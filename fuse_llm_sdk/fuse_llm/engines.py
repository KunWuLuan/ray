"""In-process vLLM engine for FuseModelDeployment.

The stock ``ray.serve.llm`` ``VLLMEngine`` forces ``distributed_executor_backend
= "ray"`` (and rejects overrides), which reserves whole GPUs per engine — unusable
for GPU time-sharing where many engines share the *same* GPUs.  This engine builds
vLLM's ``AsyncLLM`` **in-process** with
:class:`~fuse_llm.fused_ray_executor.FusedRayExecutor` and injects the
deployment-owned shared placement group, so every engine's workers claim a tiny
GPU fraction and co-reside on the same GPUs, time-sharing memory via
``sleep``/``wakeup``.  Works for ``tensor_parallel_size >= 1`` (single- or
multi-node).  All models in one deployment should use the same
``tensor_parallel_size`` (they share one PG sized to it).

Register it with :func:`~fuse_llm.deployment.set_vllm_engine_class`::

    from fuse_llm import set_vllm_engine_class, InProcessVLLMEngine
    set_vllm_engine_class(InProcessVLLMEngine)

It implements the interface FuseModelDeployment needs: ``start``, ``chat``,
``completions``, ``sleep``, ``wakeup``, ``is_sleeping``, ``collective_rpc``,
``check_health``.

NOTE: request handling is intentionally minimal (greedy/basic generation returning
OpenAI-shaped objects) — enough to drive the SDK and validated for TP>1 sleep/wake
correctness.  Full OpenAI-protocol parity (streaming SSE, tool calls, logprobs,
LoRA) is a follow-up; wrap vLLM's ``OpenAIServingChat`` here for production ingress.
"""

import os
from types import SimpleNamespace
from typing import Any, Optional


class InProcessVLLMEngine:
    """vLLM ``AsyncLLM`` engine that honours the configured executor backend."""

    def __init__(self, llm_config: Any) -> None:
        self._cfg = llm_config
        self._model_id = llm_config.model_loading_config.model_id
        self._source = llm_config.model_loading_config.model_source
        self._ek = dict(llm_config.engine_kwargs or {})
        self._eng = None
        self._tok = None
        self._rid = 0

    async def start(self) -> None:
        from vllm import AsyncEngineArgs
        from vllm.v1.engine.async_llm import AsyncLLM
        from transformers import AutoTokenizer

        from fuse_llm.fused_ray_executor import (
            FusedRayExecutor,
            get_or_create_shared_pg,
        )

        src = self._source  # a local dir or an HF repo id (offline cache honoured)
        tp = self._ek.get("tensor_parallel_size", 1)
        args = AsyncEngineArgs(
            model=src,
            enforce_eager=self._ek.get("enforce_eager", True),
            gpu_memory_utilization=self._ek.get("gpu_memory_utilization", 0.3),
            max_model_len=self._ek.get("max_model_len", 4096),
            max_num_seqs=self._ek.get("max_num_seqs", 8),
            tensor_parallel_size=tp,
            enable_sleep_mode=self._ek.get("enable_sleep_mode", True),
            distributed_executor_backend=FusedRayExecutor,
        )
        # Inject the deployment-owned shared placement group so all engines
        # co-reside on the same GPUs (fractional num_gpus). Requires running
        # inside a Ray actor (FuseServeDeployment / Serve replica) so vLLM
        # propagates RAY_ADDRESS to the EngineCore subprocess.
        cfg = args.create_engine_config()
        cfg.parallel_config.placement_group = get_or_create_shared_pg(tp)
        self._eng = AsyncLLM.from_vllm_config(cfg)
        self._tok = AutoTokenizer.from_pretrained(src)

    def _next_rid(self) -> str:
        self._rid += 1
        return f"{self._model_id}-{self._rid}"

    async def chat(self, request: Any, raw_request_info: Optional[Any] = None):
        from vllm import SamplingParams

        messages = [
            m if isinstance(m, dict) else {"role": m.role, "content": m.content}
            for m in request.messages
        ]
        prompt = self._tok.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        sp = SamplingParams(
            max_tokens=getattr(request, "max_tokens", None) or 256,
            temperature=getattr(request, "temperature", 0.0) or 0.0,
        )
        text = ""
        async for o in self._eng.generate(prompt, sp, request_id=self._next_rid()):
            text = o.outputs[0].text
        yield SimpleNamespace(
            model=self._model_id,
            choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
        )

    async def completions(self, request: Any, raw_request_info: Optional[Any] = None):
        from vllm import SamplingParams

        sp = SamplingParams(
            max_tokens=getattr(request, "max_tokens", None) or 256,
            temperature=getattr(request, "temperature", 0.0) or 0.0,
        )
        text = ""
        async for o in self._eng.generate(
            request.prompt, sp, request_id=self._next_rid()
        ):
            text = o.outputs[0].text
        yield SimpleNamespace(
            model=self._model_id, choices=[SimpleNamespace(text=text)]
        )

    async def sleep(self, **kwargs: Any) -> None:
        await self._eng.sleep(level=kwargs.get("level", 1))

    async def wakeup(self, **kwargs: Any) -> None:
        await self._eng.wake_up(tags=kwargs.get("tags"))

    async def is_sleeping(self) -> bool:
        return await self._eng.is_sleeping()

    async def collective_rpc(self, method: str, timeout=None, args=(), kwargs=None):
        return await self._eng.collective_rpc(
            method, timeout=timeout, args=args, kwargs=kwargs or {}
        )

    async def check_health(self) -> None:
        await self._eng.check_health()
