"""FuseModelDeployment: single-actor multi-model GPU time-sharing.

This module implements a Ray Serve deployment that loads multiple vLLM
engines inside a single actor, sharing one GPU.  Only one engine is
"awake" (holding GPU memory) at any given time; the others are put to
sleep via vLLM's ``sleep`` / ``wakeup`` mechanism.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, List, Optional

from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VLLMEngine resolution
# ---------------------------------------------------------------------------

_VLLM_ENGINE_CLS = None


def set_vllm_engine_class(cls):
    """Override the VLLMEngine class used by FuseModelDeployment.

    By default, the engine class is imported from
    ``ray.llm._internal.serve.engines.vllm.vllm_engine.VLLMEngine``.
    If you want to use a custom engine (e.g. for testing or a different
    vLLM version), call this function before initialising the deployment.

    Example::

        from fuse_llm import set_vllm_engine_class
        from my_custom_engine import MyVLLMEngine
        set_vllm_engine_class(MyVLLMEngine)
    """
    global _VLLM_ENGINE_CLS
    _VLLM_ENGINE_CLS = cls


def _get_vllm_engine_class():
    """Resolve the VLLMEngine class to use.

    Priority:
    1. Class set via :func:`set_vllm_engine_class`
    2. Environment variable ``FUSE_LLM_VLLM_ENGINE_CLS``
    3. Default: ``ray.llm._internal.serve.engines.vllm.vllm_engine.VLLMEngine``
    """
    global _VLLM_ENGINE_CLS

    if _VLLM_ENGINE_CLS is not None:
        return _VLLM_ENGINE_CLS

    import os

    custom_path = os.environ.get("FUSE_LLM_VLLM_ENGINE_CLS")
    if custom_path:
        from importlib import import_module

        module_path, class_name = custom_path.rsplit(".", 1)
        module = import_module(module_path)
        _VLLM_ENGINE_CLS = getattr(module, class_name)
        return _VLLM_ENGINE_CLS

    # Default: the in-process AsyncLLM engine using FusedRayExecutor + the
    # deployment's shared placement group.  (The stock ray.serve.llm VLLMEngine
    # forces distributed_executor_backend="ray" and reserves whole GPUs per
    # engine, so it cannot time-share GPUs across engines.)
    from fuse_llm.engines import InProcessVLLMEngine

    _VLLM_ENGINE_CLS = InProcessVLLMEngine
    return _VLLM_ENGINE_CLS


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ModelStats:
    """Runtime statistics for a single model inside the fuse deployment."""

    model_id: str
    state: str = "uninitialized"
    """Lifecycle state: uninitialized | awake | sleeping | draining | switching | error"""

    in_flight_requests: int = 0
    """Number of requests currently being processed by this model's engine."""

    total_requests: int = 0
    """Total number of requests processed since deployment start."""

    queued_requests: int = 0
    """Number of requests waiting for the model to become awake."""

    last_active_time: float = 0.0
    """Epoch timestamp of the last completed request."""

    request_rate_per_min: float = 0.0
    """Recent QPS estimate (updated by the controller)."""

    sleep_level: Optional[int] = None
    """vLLM sleep level used when this model was put to sleep.

    - ``None``: model is awake or was never slept.
    - ``1``: Level 1 sleep — weights offloaded to CPU RAM, KV cache
      discarded.  Wake-up must reallocate both weights and KV cache
      (``wakeup()`` with no tags wakes every component).
    - ``2``: Level 2 sleep — both weights and KV cache discarded.
      Wake-up requires three steps:
      1. ``wakeup(tags=["weights"])``  — reallocate weight memory
      2. ``collective_rpc("reload_weights")`` — load weights from disk
      3. ``wakeup(tags=["kv_cache"])``  — reallocate KV cache memory
    """


# ---------------------------------------------------------------------------
# FuseModelDeployment
# ---------------------------------------------------------------------------


class FuseModelDeployment:
    """Single-actor multi-model GPU time-sharing deployment.

    Loads multiple vLLM engines inside one Ray Serve actor.  All engines
    share the same GPU (same ``CUDA_VISIBLE_DEVICES``).  At any point in
    time only **one** engine is *awake* — the others are sleeping via
    ``sleep(level=1)`` which offloads model weights to CPU RAM and
    discards the KV cache.

    Core responsibilities
    ---------------------
    1.  **Sequential initialisation** — start engines one at a time and
        immediately sleep each one (except the default model) to avoid
        GPU OOM during startup.
    2.  **Request routing** — forward incoming requests to the engine
        identified by ``request.model``.  Requests for a sleeping model
        are queued until that model becomes active.
    3.  **In-flight tracking** — maintain a per-model counter of
        in-flight requests so the controller can make informed switch
        decisions.
    4.  **Graceful switching** — ``switch_model()`` drains in-flight
        requests on the current model (with a timeout) before sleeping
        it and waking the target model.

    Parameters
    ----------
    llm_configs:
        List of :class:`~ray.serve.llm.LLMConfig` objects, one per model.
    default_model:
        ``model_id`` of the model that should be awake after
        initialisation.  Defaults to the first config.
    drain_timeout_s:
        Default timeout (seconds) for draining in-flight requests during
        a model switch.
    """

    def __init__(
        self,
        llm_configs: List[LLMConfig],
        default_model: Optional[str] = None,
        drain_timeout_s: float = 30.0,
    ) -> None:
        self._llm_configs: Dict[str, LLMConfig] = {
            c.model_loading_config.model_id: c for c in llm_configs
        }
        if not self._llm_configs:
            raise ValueError("At least one LLMConfig must be provided")

        # Restriction check — done HERE, before any engine is loaded, so a bad
        # config fails immediately instead of deep inside vLLM after minutes of
        # model loading.  All models share ONE placement group sized to their
        # tensor_parallel_size, so every model must use the same TP.
        tp_by_model = {
            m: int((c.engine_kwargs or {}).get("tensor_parallel_size", 1))
            for m, c in self._llm_configs.items()
        }
        if len(set(tp_by_model.values())) > 1:
            raise ValueError(
                "All models in a FuseModelDeployment must use the same "
                "tensor_parallel_size — they share a single placement group "
                f"sized to it. Got per-model tensor_parallel_size: {tp_by_model}."
            )
        self._tensor_parallel_size = next(iter(tp_by_model.values()))

        # VLLMEngine instances — populated during _initialize()
        self._engines: Dict[str, Any] = {}

        # Per-model sleep level tracking.
        # Keyed by model_id; value is 1 or 2 (or None if awake).
        self._sleep_levels: Dict[str, Optional[int]] = {
            m: None for m in self._llm_configs
        }

        # Per-model statistics
        self._stats: Dict[str, ModelStats] = {}
        for model_id in self._llm_configs:
            self._stats[model_id] = ModelStats(model_id=model_id)

        # The currently awake model
        self._active_model: Optional[str] = None

        # Lock to serialise concurrent switch_model calls
        self._switch_lock = asyncio.Lock()

        # Per-model asyncio.Event — *set* when the model is awake and
        # ready to accept requests, *cleared* when draining or sleeping.
        self._ready_events: Dict[str, asyncio.Event] = {
            m: asyncio.Event() for m in self._llm_configs
        }

        # Resolve default model
        if default_model is None:
            default_model = next(iter(self._llm_configs))
        if default_model not in self._llm_configs:
            raise ValueError(
                f"default_model '{default_model}' not found in llm_configs"
            )
        self._default_model = default_model
        self._drain_timeout_s = drain_timeout_s

        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _initialize(self) -> None:
        """Sequentially start every engine, sleeping all but the default.

        Engines are started one at a time with the **default model loaded
        last**, so that at any moment during startup only a single engine is
        awake (each non-default engine sleeps and frees its GPU memory before
        the next starts).  This keeps peak GPU usage to one engine's worth,
        which is required at high ``gpu_memory_utilization`` (e.g. 0.9).

        This method is **idempotent** — calling it again after the
        first successful run is a no-op.
        """
        if self._initialized:
            return

        engine_cls = _get_vllm_engine_class()

        # Load the default model LAST so that during startup only one engine
        # is ever awake at a time: each non-default engine is put to sleep
        # (freeing its GPU memory) before the next one starts, and the default
        # is started into a free GPU and left awake.  Loading the default
        # earlier would keep it awake while later engines start, requiring two
        # engines' worth of GPU memory at once — which OOMs at high
        # ``gpu_memory_utilization`` (e.g. 0.9, where each engine wants most
        # of the GPU).
        ordered_ids = [m for m in self._llm_configs if m != self._default_model]
        ordered_ids.append(self._default_model)

        for model_id in ordered_ids:
            llm_config = self._llm_configs[model_id]
            logger.info("Initialising engine for model '%s' ...", model_id)

            # 1. Create the engine
            engine = engine_cls(llm_config)

            # 2. Start (allocates GPU memory, loads model weights)
            await engine.start()
            self._engines[model_id] = engine
            self._stats[model_id].state = "awake"
            logger.info("Engine for '%s' started.", model_id)

            # 3. Sleep immediately unless this is the default model.
            #    Use Level 1 by default during initialisation (weights
            #    kept in CPU RAM for fast wake-up).
            if model_id != self._default_model:
                await engine.sleep(level=1)
                self._sleep_levels[model_id] = 1
                self._stats[model_id].state = "sleeping"
                self._stats[model_id].sleep_level = 1
                logger.info("Engine for '%s' put to sleep (level=1).", model_id)
            else:
                # Default is loaded last and left awake — it becomes the
                # active model, holding a GPU that all other engines have
                # already freed.
                self._active_model = model_id
                self._ready_events[model_id].set()
                self._stats[model_id].last_active_time = time.time()
                logger.info("Engine for '%s' is the default active model.", model_id)

        self._initialized = True
        logger.info(
            "FuseModelDeployment initialised with %d models, active='%s'",
            len(self._engines),
            self._active_model,
        )

    # ------------------------------------------------------------------
    # Request handling
    # ------------------------------------------------------------------

    async def chat(
        self,
        request: Any,
        raw_request_info: Optional[Any] = None,
    ) -> AsyncGenerator[Any, None]:
        """Handle a chat-completion request.

        Routes to the engine identified by ``request.model``.  If that
        model is currently sleeping, the request blocks until the model
        becomes active (or times out).

        Yields response chunks compatible with the OpenAI streaming format.
        """
        model_id = getattr(request, "model", None)
        if model_id is None or model_id not in self._engines:
            raise ValueError(
                f"Unknown model '{model_id}'. Available: {list(self._engines)}"
            )

        if model_id != self._active_model:
            self._stats[model_id].queued_requests += 1
            try:
                await asyncio.wait_for(
                    self._ready_events[model_id].wait(),
                    timeout=300,
                )
            except asyncio.TimeoutError:
                self._stats[model_id].queued_requests -= 1
                raise TimeoutError(
                    f"Model '{model_id}' did not become active within 300s"
                )
            finally:
                self._stats[model_id].queued_requests -= 1

        self._stats[model_id].in_flight_requests += 1
        self._stats[model_id].total_requests += 1

        try:
            engine = self._engines[model_id]
            async for response in engine.chat(request, raw_request_info):
                yield response
        finally:
            self._stats[model_id].in_flight_requests -= 1
            self._stats[model_id].last_active_time = time.time()

    async def completions(
        self,
        request: Any,
        raw_request_info: Optional[Any] = None,
    ) -> AsyncGenerator[Any, None]:
        """Handle a completion request (same routing logic as :meth:`chat`)."""
        model_id = getattr(request, "model", None)
        if model_id is None or model_id not in self._engines:
            raise ValueError(
                f"Unknown model '{model_id}'. Available: {list(self._engines)}"
            )

        if model_id != self._active_model:
            self._stats[model_id].queued_requests += 1
            try:
                await asyncio.wait_for(
                    self._ready_events[model_id].wait(), timeout=300
                )
            except asyncio.TimeoutError:
                self._stats[model_id].queued_requests -= 1
                raise TimeoutError(
                    f"Model '{model_id}' did not become active within 300s"
                )
            finally:
                self._stats[model_id].queued_requests -= 1

        self._stats[model_id].in_flight_requests += 1
        self._stats[model_id].total_requests += 1

        try:
            engine = self._engines[model_id]
            async for response in engine.completions(request, raw_request_info):
                yield response
        finally:
            self._stats[model_id].in_flight_requests -= 1
            self._stats[model_id].last_active_time = time.time()

    # ------------------------------------------------------------------
    # Model switching
    # ------------------------------------------------------------------

    async def switch_model(
        self,
        target_model: str,
        drain_timeout: Optional[float] = None,
        sleep_level: int = 1,
    ) -> bool:
        """Switch the active model, draining in-flight requests first.

        Parameters
        ----------
        target_model:
            ``model_id`` of the model to switch to.
        drain_timeout:
            Override the default drain timeout.  ``None`` uses the
            deployment-level default.
        sleep_level:
            vLLM sleep level to use when sleeping the *current* model.
            - ``1`` (default): offload weights to CPU RAM, discard KV cache.
            - ``2``: discard both weights and KV cache.  Slower wake-up
              (weights must be reloaded from disk), but frees CPU RAM.

        Returns
        -------
        bool
            ``True`` if the switch succeeded.
        """
        async with self._switch_lock:
            if target_model == self._active_model:
                return True

            if target_model not in self._engines:
                raise ValueError(f"Unknown model: {target_model}")

            if sleep_level not in (1, 2):
                raise ValueError(
                    f"sleep_level must be 1 or 2, got {sleep_level}"
                )

            current = self._active_model
            timeout = drain_timeout if drain_timeout is not None else self._drain_timeout_s

            logger.info(
                "Switching active model: %s -> %s "
                "(drain_timeout=%.1fs, sleep_level=%d)",
                current,
                target_model,
                timeout,
                sleep_level,
            )

            # 1. Stop accepting new requests for the current model
            self._ready_events[current].clear()
            self._stats[current].state = "draining"

            # 2. Wait for in-flight requests to complete
            in_flight = self._stats[current].in_flight_requests
            if in_flight > 0:
                logger.info(
                    "Draining %d in-flight requests on '%s' ...",
                    in_flight,
                    current,
                )
                deadline = time.time() + timeout
                while time.time() < deadline:
                    if self._stats[current].in_flight_requests == 0:
                        break
                    await asyncio.sleep(0.5)

                remaining = self._stats[current].in_flight_requests
                if remaining > 0:
                    logger.warning(
                        "Drain timeout: %d requests still in-flight on '%s', "
                        "force sleeping (these requests will be interrupted)",
                        remaining,
                        current,
                    )

            # 3. Sleep current model and record the sleep level
            self._stats[current].state = "switching"
            await self._engines[current].sleep(level=sleep_level)
            self._sleep_levels[current] = sleep_level
            self._stats[current].sleep_level = sleep_level
            self._stats[current].state = "sleeping"
            logger.info(
                "Model '%s' put to sleep (level=%d).", current, sleep_level
            )

            # 4. Wake target model using the correct wake-up sequence
            self._stats[target_model].state = "switching"
            await self._wakeup_engine(target_model)
            self._stats[target_model].state = "awake"

            # 5. Update active model and release queued requests
            self._active_model = target_model
            self._ready_events[target_model].set()
            self._stats[target_model].last_active_time = time.time()

            logger.info("Switch complete: active model is now '%s'", target_model)
            return True

    async def _wakeup_engine(self, model_id: str) -> None:
        """Wake up a model using the correct sequence for its sleep level.

        - **Level 1** (weights in CPU RAM): wake **all** components.  Level-1
          sleep offloads weights to CPU RAM *and discards the KV cache*, so
          both must be reallocated on wake.  Waking only ``["weights"]``
          leaves the KV cache asleep — the engine then reports
          ``is_sleeping() == True`` and hangs on the next request.  Passing
          no tags wakes every component.
        - **Level 2** (weights discarded): three-step sequence:
          1. ``wakeup(tags=["weights"])`` — reallocate weight GPU memory
          2. ``collective_rpc("reload_weights")`` — load weights from disk
          3. ``wakeup(tags=["kv_cache"])`` — reallocate KV cache memory
        """
        engine = self._engines[model_id]
        level = self._sleep_levels.get(model_id)

        if level == 2:
            logger.info(
                "Waking up '%s' from level-2 sleep "
                "(3-step: weights -> reload -> kv_cache) ...",
                model_id,
            )
            await engine.wakeup(tags=["weights"])
            await engine.collective_rpc(method="reload_weights")
            await engine.wakeup(tags=["kv_cache"])
        else:
            logger.info(
                "Waking up '%s' from level-1 sleep "
                "(weights + kv_cache) ...",
                model_id,
            )
            await engine.wakeup()

        self._sleep_levels[model_id] = None
        self._stats[model_id].sleep_level = None

    # ------------------------------------------------------------------
    # Stats & control-plane helpers
    # ------------------------------------------------------------------

    async def get_stats(self) -> Dict[str, Any]:
        """Return runtime statistics for all models."""
        return {
            "active_model": self._active_model,
            "models": {
                model_id: {
                    "state": s.state,
                    "sleep_level": s.sleep_level,
                    "in_flight_requests": s.in_flight_requests,
                    "total_requests": s.total_requests,
                    "queued_requests": s.queued_requests,
                    "last_active_time": s.last_active_time,
                    "request_rate_per_min": s.request_rate_per_min,
                }
                for model_id, s in self._stats.items()
            },
        }

    async def sleep_model(self, model_id: str, level: int = 1) -> None:
        """Manually sleep a specific model (control-plane helper).

        Parameters
        ----------
        model_id:
            The model to sleep.
        level:
            vLLM sleep level:
            - ``1`` (default): offload weights to CPU RAM, discard KV cache.
            - ``2``: discard both weights and KV cache (frees CPU RAM,
              but wake-up requires reloading weights from disk).
        """
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        if level not in (1, 2):
            raise ValueError(f"level must be 1 or 2, got {level}")
        await self._engines[model_id].sleep(level=level)
        self._sleep_levels[model_id] = level
        self._stats[model_id].sleep_level = level
        self._stats[model_id].state = "sleeping"
        logger.info(
            "Model '%s' manually put to sleep (level=%d).", model_id, level
        )

    async def wakeup_model(self, model_id: str) -> None:
        """Manually wake a specific model (control-plane helper).

        Automatically uses the correct wake-up sequence based on the
        model's recorded sleep level.
        """
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        await self._wakeup_engine(model_id)
        self._stats[model_id].state = "awake"

    async def is_model_sleeping(self, model_id: str) -> bool:
        """Check whether a specific model is sleeping."""
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        return await self._engines[model_id].is_sleeping()

    async def check_health(self) -> None:
        """Health check — verify the active engine is responsive."""
        if self._active_model and self._active_model in self._engines:
            await self._engines[self._active_model].check_health()

    @property
    def active_model(self) -> Optional[str]:
        """The currently awake model's ``model_id``."""
        return self._active_model

    @property
    def model_ids(self) -> List[str]:
        """All model IDs managed by this deployment."""
        return list(self._llm_configs.keys())
