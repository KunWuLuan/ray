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


class _Remote:
    """Emulates a Ray ActorMethod: ``handle.method`` is an attribute whose
    ``.remote(*args)`` returns the object ref (here, the raw value)."""

    def __init__(self, value):
        self.value = value

    def remote(self, *a, **k):
        return self.value


class _RaisingRemote:
    """Emulates an ActorMethod whose ``.remote()`` fails (transport down)."""

    def __init__(self, exc):
        self._exc = exc

    def remote(self, *a, **k):
        raise self._exc


class _FakeServer:
    """Emulates a WeightCacheServer actor handle: methods are attributes with
    ``.remote()`` (matching a real ``@ray.remote`` actor)."""

    def __init__(self, names, tensors):
        self.get_weight_names = _Remote(names)
        self.get_weights = _Remote(tensors)


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

    result = ext.rdt_reload_weights(server, "m")
    assert result["ok"] is True
    assert result["source"] == "rdt"
    assert model.loaded == ["a", "b"]


def test_reload_weights_falls_back_to_disk(monkeypatch):
    names = ["a"]
    model = _FakeModel(names)
    ext = _make_ext(model, monkeypatch)

    class _BrokenServer:
        # ActorMethod whose NIXL/name fetch fails -> reload_weights must fall back.
        get_weight_names = _RaisingRemote(RuntimeError("nixl down"))

    called = {}

    def _fake_disk(model_id, reason):
        called["reason"] = reason
        return {"ok": True, "loaded": 1, "source": "disk"}

    monkeypatch.setattr(ext, "_reload_weights_from_disk", _fake_disk)
    result = ext.rdt_reload_weights(_BrokenServer(), "m")
    assert result["source"] == "disk"
    assert "nixl down" in called["reason"]


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
