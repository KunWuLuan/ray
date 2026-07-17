"""Tests for WeightCacheServer ordering and loading.

Runs on CPU with tiny tensors; NIXL registration is skipped when NIXL is
unavailable (the ordering/loading logic is what we assert here).
"""

import pytest

torch = pytest.importorskip("torch")

from fuse_llm.weight_cache import WeightCacheServer, _load_state_dict

# ``@ray.remote`` wraps the class in an ActorClass handle, so ``__new__`` on the
# handle raises. Reach through to the underlying (unwrapped) class to exercise
# the plain ``get_weights`` / ``get_weight_names`` methods on CPU without the
# actor machinery. Production behavior is unchanged.
_RawServer = WeightCacheServer.__ray_metadata__.modified_class


def test_get_weights_order_matches_names():
    sd = {"b.weight": torch.ones(2), "a.weight": torch.zeros(3)}
    server = _RawServer.__new__(_RawServer)  # bypass ray actor init
    server._state_dicts = {"m": sd}
    server._names = {"m": sorted(sd.keys())}

    names = server.get_weight_names("m")
    tensors = server.get_weights("m")

    assert names == ["a.weight", "b.weight"]
    assert [t.numel() for t in tensors] == [3, 2]  # a.weight then b.weight


def test_get_weights_unknown_model_raises():
    server = _RawServer.__new__(_RawServer)
    server._state_dicts = {}
    server._names = {}
    with pytest.raises(KeyError):
        server.get_weights("nope")
