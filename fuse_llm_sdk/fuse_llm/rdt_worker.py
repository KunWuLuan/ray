"""RDTReloadWorkerExtension: vLLM worker_extension_cls for RDT weight reload.

Merged onto each vLLM worker via ``worker_extension_cls``. On a level-2 wake the
FuseModelDeployment calls ``collective_rpc("rdt_reload_weights", args=(server,
id))``; this method pulls the model's weights from a WeightCacheServer over NIXL
and loads them into the model in place. Any failure falls back to the disk
reload so the wake still completes.

NOTE: the method is named ``rdt_reload_weights`` (not ``reload_weights``)
because vLLM's ``Worker`` already defines a built-in ``reload_weights`` (disk
checkpoint reload) and asserts a worker_extension_cls does NOT shadow existing
worker attributes. That built-in remains the disk path (called with no args by
the deployment when ``weight_source="disk"``), and is what our disk fallback
delegates to.

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
    def rdt_reload_weights(self, server, model_id, namespace=None):
        """Pull weights from the cache over NIXL and load them; fall back to disk.

        ``server`` is either a WeightCacheServer actor handle or its registered
        actor name (a ``str``).  A name is preferred in production because vLLM's
        ``collective_rpc`` msgpack encoder cannot serialize a raw ActorHandle;
        passing the name (resolved here via ``ray.get_actor``) avoids needing
        ``VLLM_ALLOW_INSECURE_SERIALIZATION``.  ``namespace`` must be the Ray
        namespace the cache actor was created in — vLLM's worker actors do not
        necessarily share the driver's namespace, so an explicit namespace is
        required for the name lookup to succeed.
        """
        try:
            if isinstance(server, str):
                server_handle = ray.get_actor(server, namespace=namespace)
            else:
                server_handle = server
            names = ray.get(server_handle.get_weight_names.remote(model_id))
            tensors = ray.get(server_handle.get_weights.remote(model_id))  # NIXL pull
            model = self.model_runner.model
            loaded = set(model.load_weights(zip(names, tensors)))
            # Coverage: ``load_weights`` reports the *model* param names it
            # populated (post-fusion: qkv_proj, gate_up_proj), which differ from
            # the raw HF checkpoint names we sent (q/k/v_proj, gate/up_proj) and
            # excludes tied weights (lm_head).  So comparing against the sent
            # names false-flags fused/tied models.  Mirror vLLM's own
            # reload_weights: fail only if NOTHING loaded, else warn on any model
            # params left unpopulated.
            if not loaded:
                raise RuntimeError("load_weights populated no parameters")
            try:
                expected = {n for n, _ in model.named_parameters()}
                unfilled = expected - loaded
                if unfilled:
                    logger.warning(
                        "RDT reload of '%s': %d model params not in loaded set "
                        "(tied/fused params are expected here): %s",
                        model_id, len(unfilled), sorted(unfilled)[:5],
                    )
            except Exception:  # model without named_parameters (unit-test fakes)
                pass
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

        Delegates to vLLM's built-in ``Worker.reload_weights`` (the same disk
        checkpoint reload the deployment uses for ``weight_source="disk"``),
        refilling the param buffers reallocated by ``wakeup(tags=['weights'])``.
        """
        self.reload_weights()  # built-in vLLM Worker method (disk reload)
        if torch is not None:
            torch.cuda.synchronize()
        logger.info("Disk reload of '%s' complete (reason=%s)", model_id, reason)
        return {"ok": True, "loaded": -1, "source": "disk", "fallback_reason": reason}
