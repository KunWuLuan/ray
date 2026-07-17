# Fused LLM RDT Weight Loading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the disk/OSS weight reload on `FuseModelDeployment`'s level-2 wake with a NIXL RDMA pull from a hot, host-RAM weight cache, so waking a sleeping model does not touch disk.

**Architecture:** A new `WeightCacheServer` Ray actor holds each model's HF-layout `state_dict` in pinned host RAM and RDMA-serves it via RDT/NIXL. A new `RDTReloadWorkerExtension` (set as vLLM `worker_extension_cls`) runs on each vLLM worker, pulls the weights over NIXL, and calls `model.load_weights(...)`. `FuseModelDeployment` gains a `weight_source="disk"|"rdt"` toggle; when `"rdt"` its existing level-2 wake step passes the cache handle to the worker method. RDT failure falls back to disk within the same wake.

**Tech Stack:** Python, Ray (core + Serve + `ray.experimental.rdt`), vLLM V1 (`AsyncLLM`, worker extension), NIXL over eRDMA, pytest.

## Global Constraints

- Target scope v1: `tensor_parallel_size == 1` only.
- `weight_source` defaults to `"disk"` — existing behavior MUST be byte-for-byte unchanged when the flag is unset.
- RDT is an accelerator, never a hard dependency: any transport failure MUST fall back to disk reload within the same wake and still complete the switch.
- A model is marked `awake` only after `reload_weights` reports success (all named params filled) — never serve a partial/garbage model.
- All new code lives under `fuse_llm_sdk/` — no edits to Ray Serve control-plane files (`sleepable.py`, `collective_rpc.py`, `vllm_engine.py`, `protocol.py`).
- Follow existing `fuse_llm` test style: `set_vllm_engine_class(FakeEngine)`, async pytest, no GPU required for unit tests.
- Security rule (repo): new RPC-reachable methods must not weaken Ray's auth model; `WeightCacheServer` is a plain Ray actor reached only via actor handle (no new network endpoint), so no new auth surface is added — keep it that way.

---

## Task 0: Confirm the disk `reload_weights` baseline on the pod (investigation)

**Why first:** The existing level-2 wake calls `collective_rpc("reload_weights")` (`deployment.py:532`) but no `worker_extension_cls` is set today (`engines.py:64-72`) and vLLM V1 workers have no built-in `reload_weights`. Before building the RDT path we must know whether the disk baseline actually runs, because it is also the RDT path's fallback and correctness oracle.

**Files:** none (investigation only).

- [ ] **Step 1: Find the fused pod and its vLLM version**

```bash
export KUBECONFIG=~/.kube/config-rtx5000
kubectl -n default exec fuse-tp-test -- python -c "import vllm; print(vllm.__version__)"
```

- [ ] **Step 2: Check whether a `reload_weights` worker method exists**

```bash
kubectl -n default exec fuse-tp-test -- python -c "
from vllm.v1.worker.gpu_worker import Worker
print('reload_weights' in dir(Worker))
"
```
Expected: prints `True` or `False`. Record the answer.

- [ ] **Step 3: Decide the fallback source**

Record in the plan's execution notes:
- If `True`: the disk `reload_weights` exists; `RDTReloadWorkerExtension._reload_weights_from_disk` may delegate to it (`self.reload_weights_from_disk()` or equivalent worker method).
- If `False`: the disk baseline does NOT exist; `RDTReloadWorkerExtension` MUST implement the disk fallback itself using vLLM's model loader (Task 3, Step 6 provides this implementation). This is the assumed default.

- [ ] **Step 4: Commit the note**

```bash
git commit --allow-empty -m "chore(fuse_llm): record disk reload_weights baseline finding for RDT work"
```

---

## Task 1: Add `weight_source` toggle + RDT wake orchestration to `FuseModelDeployment`

**Files:**
- Modify: `fuse_llm_sdk/fuse_llm/deployment.py` (`__init__`, `_wakeup_engine`)
- Test: `fuse_llm_sdk/tests/test_rdt_wake.py` (create)

**Interfaces:**
- Consumes: `FuseModelDeployment` engine interface (`sleep`, `wakeup`, `collective_rpc`) as used today.
- Produces:
  - `FuseModelDeployment.__init__(..., weight_source: str = "disk", weight_cache: Optional[Any] = None)`
  - Level-2 RDT wake calls `engine.collective_rpc(method="reload_weights", args=(weight_cache, model_id))`; disk path keeps `args=()`.

- [ ] **Step 1: Write the failing test**

Create `fuse_llm_sdk/tests/test_rdt_wake.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_wake.py::test_rdt_wake_passes_cache_handle_to_worker -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'weight_source'`.

- [ ] **Step 3: Add the constructor params**

In `fuse_llm_sdk/fuse_llm/deployment.py`, extend `__init__` signature (after `drain_timeout_s`):

```python
    def __init__(
        self,
        llm_configs: List[LLMConfig],
        default_models: Optional[List[str]] = None,
        drain_timeout_s: float = 30.0,
        weight_source: str = "disk",
        weight_cache: Optional[Any] = None,
    ) -> None:
```

At the end of `__init__` (after `self._initialized = False`), add:

```python
        if weight_source not in ("disk", "rdt"):
            raise ValueError(
                f"weight_source must be 'disk' or 'rdt', got {weight_source!r}"
            )
        if weight_source == "rdt" and weight_cache is None:
            raise ValueError(
                "weight_source='rdt' requires a weight_cache handle"
            )
        self._weight_source = weight_source
        self._weight_cache = weight_cache
```

- [ ] **Step 4: Thread the cache handle into the level-2 wake**

In `fuse_llm_sdk/fuse_llm/deployment.py`, replace the level-2 branch of `_wakeup_engine` (currently `deployment.py:525-533`):

```python
        if level == 2:
            rpc_args = ()
            if self._weight_source == "rdt":
                rpc_args = (self._weight_cache, model_id)
            logger.info(
                "Waking up '%s' from level-2 sleep "
                "(3-step: weights -> reload[%s] -> kv_cache) ...",
                model_id,
                self._weight_source,
            )
            await engine.wakeup(tags=["weights"])
            await engine.collective_rpc(method="reload_weights", args=rpc_args)
            await engine.wakeup(tags=["kv_cache"])
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_wake.py::test_rdt_wake_passes_cache_handle_to_worker -v`
Expected: PASS.

- [ ] **Step 6: Add the disk-path-unchanged regression test**

Append to `tests/test_rdt_wake.py`:

```python
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
```

- [ ] **Step 7: Run the full test file**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_wake.py -v`
Expected: 3 passed.

- [ ] **Step 8: Verify the existing suite still passes**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_combination_switch.py -v`
Expected: all pass (disk path unchanged).

- [ ] **Step 9: Commit**

```bash
git add fuse_llm_sdk/fuse_llm/deployment.py fuse_llm_sdk/tests/test_rdt_wake.py
git commit -m "feat(fuse_llm): add weight_source toggle + RDT level-2 wake orchestration"
```

---

## Task 2: `WeightCacheServer` actor (host-RAM NIXL source)

**Files:**
- Create: `fuse_llm_sdk/fuse_llm/weight_cache.py`
- Test: `fuse_llm_sdk/tests/test_weight_cache.py` (create)

**Interfaces:**
- Produces:
  - `class WeightCacheServer` — a Ray actor class.
  - `get_weight_names(model_id: str) -> list[str]` — deterministic name order.
  - `get_weights(model_id: str) -> list[torch.Tensor]` — tensors in the SAME order as `get_weight_names`; decorated with `@ray.method(tensor_transport="nixl")`.
  - `_load_state_dict(model_source: str) -> dict[str, "torch.Tensor"]` — module-level helper (pure, testable) that loads a HF safetensors dir into a name→CPU-tensor dict.

- [ ] **Step 1: Write the failing test**

Create `fuse_llm_sdk/tests/test_weight_cache.py`:

```python
"""Tests for WeightCacheServer ordering and loading.

Runs on CPU with tiny tensors; NIXL registration is skipped when NIXL is
unavailable (the ordering/loading logic is what we assert here).
"""

import pytest

torch = pytest.importorskip("torch")

from fuse_llm.weight_cache import WeightCacheServer, _load_state_dict


def test_get_weights_order_matches_names():
    sd = {"b.weight": torch.ones(2), "a.weight": torch.zeros(3)}
    server = WeightCacheServer.__new__(WeightCacheServer)  # bypass ray actor init
    server._state_dicts = {"m": sd}
    server._names = {"m": sorted(sd.keys())}

    names = server.get_weight_names("m")
    tensors = server.get_weights("m")

    assert names == ["a.weight", "b.weight"]
    assert [t.numel() for t in tensors] == [3, 2]  # a.weight then b.weight


def test_get_weights_unknown_model_raises():
    server = WeightCacheServer.__new__(WeightCacheServer)
    server._state_dicts = {}
    server._names = {}
    with pytest.raises(KeyError):
        server.get_weights("nope")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_weight_cache.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fuse_llm.weight_cache'`.

- [ ] **Step 3: Implement `WeightCacheServer`**

Create `fuse_llm_sdk/fuse_llm/weight_cache.py`:

```python
"""WeightCacheServer: a host-RAM, NIXL-served cache of model weights.

Holds each model's HF-layout ``state_dict`` in pinned CPU memory and serves the
tensors over Ray Direct Transport (NIXL/RDMA) so a FuseModelDeployment can
reload weights on a level-2 wake without touching disk.

Loaded once per model at actor init (the single cluster-wide disk read). The
tensors are registered with NIXL so the RDMA region stays pinned for the actor's
lifetime.
"""

import logging
import os
from typing import Any, Dict, List

import ray

logger = logging.getLogger(__name__)


def _load_state_dict(model_source: str) -> Dict[str, "Any"]:
    """Load a HF-layout state_dict (name -> CPU tensor) from a local dir.

    Reads all ``*.safetensors`` shards in ``model_source``. ``model_source`` must
    be a local path (the cache node is responsible for having the files present,
    e.g. via the same OSS mount the inference pods use).
    """
    import glob

    import torch
    from safetensors.torch import load_file

    files = sorted(glob.glob(os.path.join(model_source, "*.safetensors")))
    if not files:
        raise FileNotFoundError(f"no *.safetensors under {model_source!r}")
    state: Dict[str, torch.Tensor] = {}
    for f in files:
        for name, tensor in load_file(f, device="cpu").items():
            state[name] = tensor
    return state


@ray.remote
class WeightCacheServer:
    """Ray actor holding model weights in host RAM, served over NIXL."""

    def __init__(self, sources: Dict[str, str]) -> None:
        """``sources`` maps model_id -> local model directory."""
        import torch  # noqa: F401
        from ray.experimental.rdt import register_nixl_memory

        self._state_dicts: Dict[str, Dict[str, Any]] = {}
        self._names: Dict[str, List[str]] = {}

        for model_id, source in sources.items():
            sd = _load_state_dict(source)
            names = sorted(sd.keys())
            # Pin + register each tensor with NIXL so the RDMA region persists.
            for name in names:
                t = sd[name].pin_memory()
                sd[name] = t
                try:
                    register_nixl_memory(t)
                except Exception as e:  # NIXL absent (e.g. local test node)
                    logger.warning("NIXL registration skipped for %s: %s", name, e)
            self._state_dicts[model_id] = sd
            self._names[model_id] = names
            logger.info(
                "Cached %d weight tensors for model '%s'", len(names), model_id
            )

    def get_weight_names(self, model_id: str) -> List[str]:
        return list(self._names[model_id])

    @ray.method(tensor_transport="nixl")
    def get_weights(self, model_id: str) -> List[Any]:
        sd = self._state_dicts[model_id]  # KeyError for unknown model (by design)
        return [sd[name] for name in self._names[model_id]]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_weight_cache.py -v`
Expected: 2 passed.

Note: `@ray.remote` wraps the class; `WeightCacheServer.__new__(...)` in the test bypasses the actor machinery to exercise the plain methods. If `@ray.remote` prevents `__new__` access, the test uses `WeightCacheServer.__ray_metadata__.modified_class` — but first try as written.

- [ ] **Step 5: Commit**

```bash
git add fuse_llm_sdk/fuse_llm/weight_cache.py fuse_llm_sdk/tests/test_weight_cache.py
git commit -m "feat(fuse_llm): add WeightCacheServer host-RAM NIXL weight source"
```

---

## Task 3: `RDTReloadWorkerExtension` (NIXL pull + load_weights + disk fallback)

**Files:**
- Create: `fuse_llm_sdk/fuse_llm/rdt_worker.py`
- Modify: `fuse_llm_sdk/fuse_llm/engines.py` (`start`, add a pure `_build_engine_kwargs` helper)
- Test: `fuse_llm_sdk/tests/test_rdt_worker.py` (create)

**Interfaces:**
- Consumes: `WeightCacheServer.get_weight_names` / `get_weights` (Task 2); `weight_source` toggle (Task 1).
- Produces:
  - `class RDTReloadWorkerExtension` with `reload_weights(self, server_handle, model_id)` returning `{"ok": bool, "loaded": int, "source": "rdt"|"disk"}`.
  - `InProcessVLLMEngine._build_engine_kwargs(self) -> dict` — pure helper returning the `AsyncEngineArgs` kwargs, setting `worker_extension_cls` when the config requests RDT.
  - `InProcessVLLMEngine.__init__` accepts `weight_source` off `llm_config` (see Step 7).

- [ ] **Step 1: Write the failing test for the worker extension fallback**

Create `fuse_llm_sdk/tests/test_rdt_worker.py`:

```python
"""Tests for RDTReloadWorkerExtension.

The NIXL pull + real load_weights are GPU/pod-only. Here we test the pure
control logic: success path (mocked model + server) and disk fallback on error.
"""

import pytest

from fuse_llm.rdt_worker import RDTReloadWorkerExtension


class _FakeModel:
    def __init__(self, expected_names):
        self._expected = set(expected_names)
        self.loaded = None

    def load_weights(self, weights):
        pairs = list(weights)
        self.loaded = [n for n, _ in pairs]
        return set(self.loaded)  # report everything loaded


class _FakeServerRef:
    def __init__(self, value):
        self._value = value

    # emulate ray.ObjectRef resolution via the patched ray.get


class _FakeServer:
    def __init__(self, names, tensors):
        self._names = names
        self._tensors = tensors

    def get_weight_names(self):
        return _Remote(self._names)

    def get_weights(self):
        return _Remote(self._tensors)


class _Remote:
    def __init__(self, value):
        self.value = value

    def remote(self, *a, **k):
        return self.value


def _make_ext(model, monkeypatch):
    ext = RDTReloadWorkerExtension.__new__(RDTReloadWorkerExtension)
    ext.model_runner = type("MR", (), {"model": model})()
    # patch ray.get to return the raw value the fake .remote() produced
    import fuse_llm.rdt_worker as mod

    monkeypatch.setattr(mod.ray, "get", lambda ref: ref)
    return ext


def test_reload_weights_success(monkeypatch):
    names = ["a", "b"]
    tensors = [object(), object()]
    model = _FakeModel(names)
    ext = _make_ext(model, monkeypatch)
    server = _FakeServer(names, tensors)

    monkeypatch.setattr(
        "fuse_llm.rdt_worker.torch",
        type("T", (), {"cuda": type("C", (), {"synchronize": staticmethod(lambda: None)})()})(),
    )

    result = ext.reload_weights(server, "m")
    assert result["ok"] is True
    assert result["source"] == "rdt"
    assert model.loaded == ["a", "b"]


def test_reload_weights_falls_back_to_disk(monkeypatch):
    names = ["a"]
    model = _FakeModel(names)
    ext = _make_ext(model, monkeypatch)

    class _BrokenServer:
        def get_weight_names(self):
            raise RuntimeError("nixl down")

    called = {}
    monkeypatch.setattr(
        ext, "_reload_weights_from_disk",
        lambda model_id, reason: called.setdefault("reason", reason) or {"ok": True, "loaded": 1, "source": "disk"},
    )
    result = ext.reload_weights(_BrokenServer(), "m")
    assert result["source"] == "disk"
    assert "nixl down" in called["reason"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_worker.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fuse_llm.rdt_worker'`.

- [ ] **Step 3: Implement the worker extension**

Create `fuse_llm_sdk/fuse_llm/rdt_worker.py`:

```python
"""RDTReloadWorkerExtension: vLLM worker_extension_cls for RDT weight reload.

Merged onto each vLLM worker via ``worker_extension_cls``. On a level-2 wake the
FuseModelDeployment calls ``collective_rpc("reload_weights", args=(server, id))``;
this method pulls the model's weights from a WeightCacheServer over NIXL and
loads them into the model in place. Any failure falls back to a disk reload so
the wake still completes.

The methods here run inside the vLLM worker process, which is a Ray actor, so it
is a valid RDT destination. ``self.model_runner`` is provided by vLLM.
"""

import logging

import ray

try:
    import torch
except Exception:  # torch always present on a real worker
    torch = None

logger = logging.getLogger(__name__)


class RDTReloadWorkerExtension:
    def reload_weights(self, server_handle, model_id):
        """Pull weights from the cache over NIXL and load them; fall back to disk."""
        try:
            names = ray.get(server_handle.get_weight_names.remote(model_id))
            tensors = ray.get(server_handle.get_weights.remote(model_id))  # NIXL pull
            model = self.model_runner.model
            loaded = set(model.load_weights(zip(names, tensors)))
            missing = set(names) - loaded
            if missing:
                raise RuntimeError(
                    f"{len(missing)} params not filled, e.g. {sorted(missing)[:5]}"
                )
            if torch is not None:
                torch.cuda.synchronize()
            logger.info("RDT reload of '%s' complete (%d tensors)", model_id, len(loaded))
            return {"ok": True, "loaded": len(loaded), "source": "rdt"}
        except Exception as e:
            logger.warning(
                "RDT reload of '%s' failed (%s); falling back to disk", model_id, e
            )
            return self._reload_weights_from_disk(model_id, reason=str(e))

    def _reload_weights_from_disk(self, model_id, reason=""):
        """Reload weights from the model's on-disk source into the live model.

        Uses vLLM's model loader against the worker's own model config, so the
        param buffers reallocated by ``wakeup(tags=['weights'])`` are refilled.
        """
        from vllm.model_executor.model_loader import get_model_loader

        model = self.model_runner.model
        model_config = self.model_runner.model_config
        load_config = getattr(self.model_runner, "load_config", None)
        loader = get_model_loader(load_config) if load_config else get_model_loader(None)
        loader.load_weights(model, model_config)
        if torch is not None:
            torch.cuda.synchronize()
        logger.info("Disk reload of '%s' complete (reason=%s)", model_id, reason)
        return {"ok": True, "loaded": -1, "source": "disk"}
```

Note (from Task 0): if the pod investigation found a built-in `reload_weights`
worker method, replace the body of `_reload_weights_from_disk` with a call to it
instead of `get_model_loader`. The `get_model_loader` signature is vLLM-version
sensitive — verify on the pod (Task 5) and adjust.

- [ ] **Step 4: Run the worker-extension tests**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_worker.py -v`
Expected: 2 passed.

- [ ] **Step 5: Write the failing test for engine kwargs wiring**

Append to `tests/test_rdt_worker.py`:

```python
def test_engine_sets_worker_extension_cls_for_rdt():
    from fuse_llm.engines import InProcessVLLMEngine
    from ray.serve.llm import LLMConfig
    from ray.llm._internal.serve.core.configs.llm_config import ModelLoadingConfig

    cfg = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="m", model_source="/models/m"),
        engine_kwargs=dict(tensor_parallel_size=1),
    )
    eng = InProcessVLLMEngine(cfg, weight_source="rdt")
    ek = eng._build_engine_kwargs()
    assert ek["worker_extension_cls"] == "fuse_llm.rdt_worker.RDTReloadWorkerExtension"


def test_engine_no_extension_for_disk():
    from fuse_llm.engines import InProcessVLLMEngine
    from ray.serve.llm import LLMConfig
    from ray.llm._internal.serve.core.configs.llm_config import ModelLoadingConfig

    cfg = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="m", model_source="/models/m"),
        engine_kwargs=dict(tensor_parallel_size=1),
    )
    eng = InProcessVLLMEngine(cfg)  # disk default
    ek = eng._build_engine_kwargs()
    assert "worker_extension_cls" not in ek
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_worker.py::test_engine_sets_worker_extension_cls_for_rdt -v`
Expected: FAIL — `TypeError` (no `weight_source` kwarg) or `AttributeError` (no `_build_engine_kwargs`).

- [ ] **Step 7: Refactor `engines.py` to add the wiring**

In `fuse_llm_sdk/fuse_llm/engines.py`, change `__init__` to accept `weight_source`:

```python
    def __init__(self, llm_config: Any, weight_source: str = "disk") -> None:
        self._cfg = llm_config
        self._model_id = llm_config.model_loading_config.model_id
        self._source = llm_config.model_loading_config.model_source
        self._ek = dict(llm_config.engine_kwargs or {})
        self._weight_source = weight_source
        self._eng = None
        self._tok = None
        self._rid = 0
```

Extract the kwargs-building block from `start()` into a pure helper and call it:

```python
    def _build_engine_kwargs(self) -> dict:
        from fuse_llm.fused_ray_executor import FusedRayExecutor

        tp = self._ek.get("tensor_parallel_size", 1)
        ek = dict(self._ek)
        ek.setdefault("enforce_eager", True)
        ek.setdefault("gpu_memory_utilization", 0.3)
        ek.setdefault("max_model_len", 4096)
        ek.setdefault("max_num_seqs", 8)
        ek.setdefault("enable_sleep_mode", True)
        ek["tensor_parallel_size"] = tp
        ek["distributed_executor_backend"] = FusedRayExecutor
        if self._weight_source == "rdt":
            ek["worker_extension_cls"] = (
                "fuse_llm.rdt_worker.RDTReloadWorkerExtension"
            )
        return ek
```

Then in `start()` replace the inline kwargs block with:

```python
        src = self._source
        ek = self._build_engine_kwargs()
        args = AsyncEngineArgs(model=src, **ek)
```

(Keep the rest of `start()` — `create_engine_config`, placement group injection, `AsyncLLM.from_vllm_config`, tokenizer — unchanged.)

Note: `_build_engine_kwargs` imports `FusedRayExecutor` but does not import vLLM, so it is unit-testable without a GPU. `start()` still imports vLLM lazily.

- [ ] **Step 8: Run the engine wiring tests**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_worker.py -v`
Expected: 4 passed.

- [ ] **Step 9: Commit**

```bash
git add fuse_llm_sdk/fuse_llm/rdt_worker.py fuse_llm_sdk/fuse_llm/engines.py fuse_llm_sdk/tests/test_rdt_worker.py
git commit -m "feat(fuse_llm): add RDTReloadWorkerExtension + worker_extension_cls wiring"
```

---

## Task 4: Wire `weight_source` through the deployment→engine boundary + `deploy.py` + exports

**Files:**
- Modify: `fuse_llm_sdk/fuse_llm/deployment.py` (`_initialize` engine construction)
- Modify: `fuse_llm_sdk/fuse_llm/deploy.py` (`deploy` gains cache opt-in; `FuseServeDeployment.__init__`)
- Modify: `fuse_llm_sdk/fuse_llm/__init__.py` (exports)
- Test: `fuse_llm_sdk/tests/test_rdt_wake.py` (extend)

**Interfaces:**
- Consumes: `InProcessVLLMEngine(llm_config, weight_source=...)` (Task 3); `WeightCacheServer` (Task 2).
- Produces: `deploy(..., weight_source="disk", model_sources=None)`; `WeightCacheServer` exported from `fuse_llm`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_rdt_wake.py`:

```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_wake.py::test_engine_constructed_with_weight_source -v`
Expected: FAIL — engine constructed without `weight_source`, so `seen["a"] == "disk"`.

- [ ] **Step 3: Pass `weight_source` when constructing engines**

In `fuse_llm_sdk/fuse_llm/deployment.py` `_initialize`, change the engine construction (currently `engine = engine_cls(llm_config)` around `deployment.py:275`) to:

```python
            # 1. Create the engine (pass weight_source so RDT engines register
            #    the worker extension; falls back gracefully for engines whose
            #    __init__ does not accept it, e.g. simple test fakes).
            try:
                engine = engine_cls(llm_config, weight_source=self._weight_source)
            except TypeError:
                engine = engine_cls(llm_config)
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd fuse_llm_sdk && python -m pytest tests/test_rdt_wake.py -v`
Expected: all pass (4 tests).

- [ ] **Step 5: Add `deploy.py` opt-in cache creation**

In `fuse_llm_sdk/fuse_llm/deploy.py`, add params to `deploy(...)` (after `default_sleep_level`):

```python
    weight_source: str = "disk",
    model_sources: Optional[dict] = None,
    weight_cache_options: Optional[dict] = None,
```

Near the top of `deploy`'s body, before constructing the Serve deployment, add:

```python
    weight_cache = None
    if weight_source == "rdt":
        from fuse_llm.weight_cache import WeightCacheServer

        if not model_sources:
            raise ValueError(
                "weight_source='rdt' requires model_sources={model_id: local_dir}"
            )
        opts = weight_cache_options or {}
        weight_cache = WeightCacheServer.options(**opts).remote(model_sources)
```

Pass `weight_source` and `weight_cache` into the `FuseServeDeployment` binding
(add them to `FuseServeDeployment.__init__` and forward to
`FuseModelDeployment.__init__`):

```python
    async def __init__(
        self,
        llm_configs: List[LLMConfig],
        default_models: Optional[List[str]] = None,
        drain_timeout_s: float = 30.0,
        weight_source: str = "disk",
        weight_cache: Optional[Any] = None,
    ) -> None:
        FuseModelDeployment.__init__(
            self,
            llm_configs=llm_configs,
            default_models=default_models,
            drain_timeout_s=drain_timeout_s,
            weight_source=weight_source,
            weight_cache=weight_cache,
        )
        await self._initialize()
```

And where `deploy` binds the deployment, pass `weight_source=weight_source,
weight_cache=weight_cache` into the `.bind(...)` call.

- [ ] **Step 6: Export `WeightCacheServer`**

In `fuse_llm_sdk/fuse_llm/__init__.py`, add after the engines import:

```python
from fuse_llm.weight_cache import WeightCacheServer
```

and add `"WeightCacheServer"` to `__all__`.

- [ ] **Step 7: Verify imports resolve**

Run: `cd fuse_llm_sdk && python -c "import fuse_llm; print(fuse_llm.WeightCacheServer)"`
Expected: prints the actor class (no ImportError).

- [ ] **Step 8: Run the whole unit suite**

Run: `cd fuse_llm_sdk && python -m pytest tests/ -v`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add fuse_llm_sdk/fuse_llm/deployment.py fuse_llm_sdk/fuse_llm/deploy.py fuse_llm_sdk/fuse_llm/__init__.py fuse_llm_sdk/tests/test_rdt_wake.py
git commit -m "feat(fuse_llm): thread weight_source through deploy + export WeightCacheServer"
```

---

## Task 5: Pod validation on `fuse-tp-test` (manual, GPU + eRDMA)

**Files:**
- Create: `fuse_llm_sdk/examples/rdt_wake_validation.py`

**Interfaces:**
- Consumes: everything above, running against real vLLM + NIXL on the cluster.

- [ ] **Step 1: Write the validation script**

Create `fuse_llm_sdk/examples/rdt_wake_validation.py`:

```python
"""Manual validation of RDT level-2 wake on a GPU cluster with eRDMA.

Run inside a Ray cluster where NIXL is available. Loads two TP=1 models into a
FuseModelDeployment with weight_source='rdt', switches between them at level 2,
and checks that outputs match a disk-reload wake. Prints wake latencies.
"""

import asyncio
import time

import ray

from fuse_llm import set_vllm_engine_class, InProcessVLLMEngine, WeightCacheServer
from fuse_llm.deployment import FuseModelDeployment
from fuse_llm.deploy import make_model_config

MODELS = {
    "qwen-0.6b": "/models/qwen3-0.6b",
    "qwen-2b": "/models/qwen3-2b",
}


async def main():
    set_vllm_engine_class(InProcessVLLMEngine)
    cache = WeightCacheServer.options(
        num_cpus=2, resources={"cache_node": 0.01}  # schedule off the GPU node
    ).remote(MODELS)

    cfgs = [make_model_config(m, src, tensor_parallel_size=1) for m, src in MODELS.items()]
    dep = FuseModelDeployment(
        cfgs, default_models=["qwen-0.6b"],
        weight_source="rdt", weight_cache=cache,
    )
    await dep._initialize()

    # Warm the second model once (disk load at start), then sleep at level 2.
    await dep.switch_combination(["qwen-2b"], sleep_level=2)
    t0 = time.time()
    await dep.switch_combination(["qwen-0.6b"], sleep_level=2)  # RDT wake of 0.6b
    print(f"RDT level-2 wake latency: {time.time() - t0:.3f}s")

    req = type("R", (), {"model": "qwen-0.6b", "messages": [{"role": "user", "content": "2+2="}], "max_tokens": 8, "temperature": 0.0})()
    async for r in dep.chat(req):
        print("output:", r.choices[0].message.content)


if __name__ == "__main__":
    ray.init(address="auto")
    asyncio.run(main())
```

- [ ] **Step 2: Copy the SDK + script to the pod**

```bash
export KUBECONFIG=~/.kube/config-rtx5000
kubectl -n default cp fuse_llm_sdk fuse-tp-test:/tmp/fuse_llm_sdk
```

- [ ] **Step 3: Confirm NIXL is importable on the pod**

```bash
kubectl -n default exec fuse-tp-test -- python -c "from ray.experimental.rdt import register_nixl_memory; import nixl; print('nixl ok')"
```
Expected: `nixl ok`. If it fails, RDT will fall back to disk — record and stop to install NIXL first.

- [ ] **Step 4: Run the unit suite on the pod (real Ray, still no GPU needed)**

```bash
kubectl -n default exec fuse-tp-test -- bash -lc "cd /tmp/fuse_llm_sdk && pip install -e . && python -m pytest tests/ -v"
```
Expected: all pass.

- [ ] **Step 5: Run the validation script**

```bash
kubectl -n default exec fuse-tp-test -- bash -lc "cd /tmp/fuse_llm_sdk && python examples/rdt_wake_validation.py"
```
Expected: prints a wake latency and a correct `output:` line (e.g. contains `4`). Confirms end-to-end NIXL pull + load_weights + serve.

- [ ] **Step 6: Compare against the disk path**

Edit the script's `weight_source="rdt"` → `"disk"`, re-run Step 5, and record both latencies. Expected: RDT wake latency < disk wake latency (the win); outputs identical.

- [ ] **Step 7: Commit the validation script + record results**

```bash
git add fuse_llm_sdk/examples/rdt_wake_validation.py
git commit -m "test(fuse_llm): add RDT level-2 wake pod-validation script"
```

Record measured latencies (RDT vs disk) and the NIXL/vLLM versions in the PR description.

---

## Self-Review Notes

- **Spec coverage:** WeightCacheServer (Task 2), RDTReloadWorkerExtension (Task 3), engines.py wiring (Task 3), deployment.py toggle+orchestration (Tasks 1,4), deploy.py opt-in (Task 4), error/disk fallback (Tasks 1,3), readiness coverage-check (Task 3), local unit tests (Tasks 1-4), pod validation (Task 5), open disk-baseline assumption (Task 0). All spec sections mapped.
- **Fallback path** appears in both the worker extension (Task 3, transport-level) and is exercised by a test (Task 3 Step 1); the deployment-level switch always completes because the rpc returns rather than raises on RDT failure.
- **Type consistency:** `weight_source: str`, `weight_cache`/`weight_source` param names, `reload_weights(server_handle, model_id)`, `get_weights`/`get_weight_names`, `_build_engine_kwargs` used consistently across Tasks 1-5.
- **TP=1 only** enforced by scope; no per-rank sharding code introduced.
