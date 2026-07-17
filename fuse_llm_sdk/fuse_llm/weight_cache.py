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
