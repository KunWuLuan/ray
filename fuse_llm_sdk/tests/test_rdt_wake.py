"""Tests for the RDT (weight_source='rdt') level-2 wake orchestration.

Pure orchestration only — a FakeEngine records collective_rpc calls, so no GPU,
vLLM, or NIXL is required.
"""

from types import SimpleNamespace

import pytest

from fuse_llm import set_vllm_engine_class
from fuse_llm.deployment import FuseModelDeployment


class FakeEngine:
    """Engine fake that records collective_rpc invocations."""

    def __init__(self, llm_config):
        self.model_id = llm_config.model_loading_config.model_id
        self._sleeping = False
        self.rpc_calls = []  # list of (method, args)
        self.rpc_should_raise = False

    async def start(self):
        self._sleeping = False

    async def chat(self, request, raw_request_info=None):
        yield SimpleNamespace(model=self.model_id, choices=[])

    async def completions(self, request, raw_request_info=None):
        yield SimpleNamespace(model=self.model_id, choices=[])

    async def sleep(self, **kwargs):
        self._sleeping = True

    async def wakeup(self, **kwargs):
        tags = kwargs.get("tags")
        if tags is None or "kv_cache" in tags:
            self._sleeping = False

    async def is_sleeping(self):
        return self._sleeping

    async def collective_rpc(self, method, timeout=None, args=(), kwargs=None):
        self.rpc_calls.append((method, args))
        if self.rpc_should_raise:
            raise RuntimeError("simulated transport failure")
        return [{"ok": True}]

    async def check_health(self):
        return None


def _cfg(model_id):
    from ray.serve.llm import LLMConfig
    from ray.llm._internal.serve.core.configs.llm_config import ModelLoadingConfig

    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_id, model_source=f"/models/{model_id}"
        ),
        engine_kwargs=dict(tensor_parallel_size=1),
    )


@pytest.fixture(autouse=True)
def _use_fake_engine():
    set_vllm_engine_class(FakeEngine)
    yield
    set_vllm_engine_class(None)


@pytest.mark.asyncio
async def test_rdt_wake_passes_cache_handle_to_worker():
    sentinel_cache = object()
    dep = FuseModelDeployment(
        [_cfg("a"), _cfg("b")],
        default_models=["a"],
        weight_source="rdt",
        weight_cache=sentinel_cache,
    )
    await dep._initialize()
    # Sleep 'a' at level 2, then wake it via RDT.
    await dep.switch_combination(["b"], sleep_level=2)
    await dep.switch_combination(["a"], sleep_level=2)

    calls = dep._engines["a"].rpc_calls
    assert ("reload_weights", (sentinel_cache, "a")) in calls


@pytest.mark.asyncio
async def test_disk_wake_passes_no_args():
    dep = FuseModelDeployment(
        [_cfg("a"), _cfg("b")], default_models=["a"]  # weight_source defaults to "disk"
    )
    await dep._initialize()
    await dep.switch_combination(["b"], sleep_level=2)
    await dep.switch_combination(["a"], sleep_level=2)
    assert ("reload_weights", ()) in dep._engines["a"].rpc_calls


@pytest.mark.asyncio
async def test_rdt_requires_cache():
    with pytest.raises(ValueError, match="requires a weight_cache"):
        FuseModelDeployment([_cfg("a")], weight_source="rdt")


@pytest.mark.asyncio
async def test_engine_constructed_with_weight_source(monkeypatch):
    seen = {}

    class RecordingEngine(FakeEngine):
        def __init__(self, llm_config, weight_source="disk"):
            super().__init__(llm_config)
            seen[self.model_id] = weight_source

    set_vllm_engine_class(RecordingEngine)
    dep = FuseModelDeployment(
        [_cfg("a")], default_models=["a"],
        weight_source="rdt", weight_cache=object(),
    )
    await dep._initialize()
    assert seen["a"] == "rdt"
