"""Tests for combination-based awake-set orchestration in FuseModelDeployment.

These exercise the pure orchestration logic (init / switch / routing) with a
``FakeEngine`` registered via :func:`fuse_llm.set_vllm_engine_class`, so no GPU
or real vLLM is required.  Requires ``ray`` only for the ``ray.serve.llm``
import inside :mod:`fuse_llm.deployment`.
"""

from types import SimpleNamespace

import pytest

from fuse_llm import set_vllm_engine_class
from fuse_llm.deployment import FuseModelDeployment


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeEngine:
    """Minimal engine implementing the interface FuseModelDeployment needs.

    Records sleep/wake events into a *shared* ``event_log`` (class attribute)
    so tests can assert cross-engine ordering (all sleeps before all wakes).
    """

    event_log: list = []

    def __init__(self, llm_config):
        self.model_id = llm_config.model_loading_config.model_id
        self._sleeping = False
        self.reload_calls = 0

    async def start(self):
        self._sleeping = False

    async def chat(self, request, raw_request_info=None):
        yield SimpleNamespace(
            model=self.model_id,
            choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
        )

    async def completions(self, request, raw_request_info=None):
        yield SimpleNamespace(
            model=self.model_id, choices=[SimpleNamespace(text="ok")]
        )

    async def sleep(self, **kwargs):
        self._sleeping = True
        FakeEngine.event_log.append(("sleep", self.model_id))

    async def wakeup(self, **kwargs):
        # Level-1 wake (no tags) or the kv_cache step of a level-2 wake makes
        # the engine servable again.
        tags = kwargs.get("tags")
        if tags is None or "kv_cache" in tags:
            self._sleeping = False
        FakeEngine.event_log.append(("wake", self.model_id, tags))

    async def is_sleeping(self):
        return self._sleeping

    async def collective_rpc(self, method, timeout=None, args=(), kwargs=None):
        if method == "reload_weights":
            self.reload_calls += 1

    async def check_health(self):
        assert not self._sleeping, f"{self.model_id} is asleep"


def _cfg(model_id, **engine_kwargs):
    """Build an LLMConfig-shaped fake (FuseModelDeployment only reads these)."""
    return SimpleNamespace(
        model_loading_config=SimpleNamespace(model_id=model_id),
        engine_kwargs=engine_kwargs,
    )


@pytest.fixture(autouse=True)
def _use_fake_engine():
    FakeEngine.event_log = []
    set_vllm_engine_class(FakeEngine)
    yield
    set_vllm_engine_class(None)


async def _make_dep(model_ids, default_models=None):
    dep = FuseModelDeployment(
        llm_configs=[_cfg(m) for m in model_ids],
        default_models=default_models,
    )
    await dep._initialize()
    return dep


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "default_models, expected_awake",
    [
        (None, {"a"}),          # defaults to first config
        (["a"], {"a"}),
        (["a", "b"], {"a", "b"}),
        (["b", "c"], {"b", "c"}),
    ],
)
async def test_init_awake_set(default_models, expected_awake):
    dep = await _make_dep(["a", "b", "c"], default_models)
    assert set(dep.active_models) == expected_awake
    for m in ["a", "b", "c"]:
        state = dep._stats[m].state
        if m in expected_awake:
            assert state == "awake"
            assert not await dep.is_model_sleeping(m)
        else:
            assert state == "sleeping"
            assert await dep.is_model_sleeping(m)


async def test_init_rejects_unknown_default():
    with pytest.raises(ValueError):
        FuseModelDeployment(
            llm_configs=[_cfg("a")], default_models=["nope"]
        )


async def test_init_rejects_nonuniform_tp():
    with pytest.raises(ValueError):
        FuseModelDeployment(
            llm_configs=[
                _cfg("a", tensor_parallel_size=1),
                _cfg("b", tensor_parallel_size=2),
            ]
        )


# ---------------------------------------------------------------------------
# Switching
# ---------------------------------------------------------------------------


async def test_switch_sleeps_and_wakes_the_right_models():
    dep = await _make_dep(["a", "b", "c"], ["a", "b"])
    FakeEngine.event_log = []

    await dep.switch_combination(["b", "c"])

    assert set(dep.active_models) == {"b", "c"}
    # a slept, c woke, b untouched (overlap).
    assert ("sleep", "a") in FakeEngine.event_log
    assert not any(
        e[0] == "sleep" and e[1] == "b" for e in FakeEngine.event_log
    )
    assert not any(
        e[0] == "wake" and e[1] == "b" for e in FakeEngine.event_log
    )
    assert any(e[0] == "wake" and e[1] == "c" for e in FakeEngine.event_log)


async def test_switch_sleeps_before_waking():
    """Outgoing models must sleep before incoming ones wake (bounds memory)."""
    dep = await _make_dep(["a", "b"], ["a"])
    FakeEngine.event_log = []

    await dep.switch_combination(["b"])

    kinds = [e[0] for e in FakeEngine.event_log]
    last_sleep = max(i for i, k in enumerate(kinds) if k == "sleep")
    first_wake = min(i for i, k in enumerate(kinds) if k == "wake")
    assert last_sleep < first_wake


async def test_switch_to_equal_set_is_noop():
    dep = await _make_dep(["a", "b"], ["a", "b"])
    FakeEngine.event_log = []
    assert await dep.switch_combination(["b", "a"]) is True
    assert FakeEngine.event_log == []


async def test_switch_to_empty_sleeps_everything():
    dep = await _make_dep(["a", "b"], ["a", "b"])
    await dep.switch_combination([])
    assert dep.active_models == []
    assert await dep.is_model_sleeping("a")
    assert await dep.is_model_sleeping("b")


async def test_switch_unknown_model_raises():
    dep = await _make_dep(["a", "b"], ["a"])
    with pytest.raises(ValueError):
        await dep.switch_combination(["a", "ghost"])


@pytest.mark.parametrize("bad_level", [0, 3, -1])
async def test_switch_bad_sleep_level(bad_level):
    dep = await _make_dep(["a", "b"], ["a"])
    with pytest.raises(ValueError):
        await dep.switch_combination(["b"], sleep_level=bad_level)


async def test_level2_wake_sequence_reloads_weights():
    dep = await _make_dep(["a", "b"], ["a"])
    # Sleep a at level 2 by switching away with sleep_level=2 ...
    await dep.switch_combination(["b"], sleep_level=2)
    # ... then switch back; a must wake via the 3-step level-2 sequence.
    await dep.switch_combination(["a"], sleep_level=2)
    assert dep._engines["a"].reload_calls == 1
    assert not await dep.is_model_sleeping("a")


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", ["chat", "completions"])
async def test_request_for_awake_model_succeeds(method):
    dep = await _make_dep(["a", "b"], ["a"])
    req = SimpleNamespace(model="a", messages=[], prompt="hi")
    out = [r async for r in getattr(dep, method)(req)]
    assert out and out[0].model == "a"
    assert dep._stats["a"].total_requests == 1
    assert dep._stats["a"].in_flight_requests == 0


@pytest.mark.parametrize("method", ["chat", "completions"])
async def test_request_for_sleeping_model_rejected(method):
    dep = await _make_dep(["a", "b"], ["a"])
    req = SimpleNamespace(model="b", messages=[], prompt="hi")
    with pytest.raises(RuntimeError):
        _ = [r async for r in getattr(dep, method)(req)]


@pytest.mark.parametrize("method", ["chat", "completions"])
async def test_request_for_unknown_model_rejected(method):
    dep = await _make_dep(["a"], ["a"])
    req = SimpleNamespace(model="ghost", messages=[], prompt="hi")
    with pytest.raises(ValueError):
        _ = [r async for r in getattr(dep, method)(req)]


async def test_request_rejected_after_model_switched_out():
    dep = await _make_dep(["a", "b"], ["a"])
    await dep.switch_combination(["b"])
    req = SimpleNamespace(model="a", messages=[], prompt="hi")
    with pytest.raises(RuntimeError):
        _ = [r async for r in dep.chat(req)]


# ---------------------------------------------------------------------------
# Manual per-model control keeps _active_models consistent
# ---------------------------------------------------------------------------


async def test_manual_sleep_wake_updates_active_set():
    dep = await _make_dep(["a", "b"], ["a", "b"])

    await dep.sleep_model("a", level=1)
    assert "a" not in dep.active_models
    assert await dep.is_model_sleeping("a")

    await dep.wakeup_model("a")
    assert "a" in dep.active_models
    assert not await dep.is_model_sleeping("a")


async def test_check_health_covers_awake_models():
    dep = await _make_dep(["a", "b", "c"], ["a", "b"])
    # Awake engines are healthy; sleeping 'c' is excluded, so no assertion trips.
    await dep.check_health()


async def test_get_stats_reports_active_models():
    dep = await _make_dep(["a", "b", "c"], ["a", "c"])
    stats = await dep.get_stats()
    assert stats["active_models"] == ["a", "c"]
    assert set(stats["models"]) == {"a", "b", "c"}
