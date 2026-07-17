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
