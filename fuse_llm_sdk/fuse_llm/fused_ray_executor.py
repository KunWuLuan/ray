"""FusedRayExecutor: a RayExecutorV2 variant for GPU time-sharing.

The stock ``RayExecutorV2`` schedules one worker actor per placement-group
bundle claiming a **whole** GPU (``num_gpus=1``).  That makes it impossible for
several vLLM engines to share the same GPUs: each engine's actors would demand
exclusive GPUs, so N engines would need N×(GPUs-per-engine) physical GPUs.

For :class:`FuseModelDeployment`, multiple engines share the *same* GPUs and
only one is awake at a time (vLLM ``sleep``/``wakeup``).  The GPUs are owned by
the deployment via a single placement group; each engine's workers should just
*launch onto* those GPUs without exclusively reserving them.

This executor changes exactly one thing: each worker actor claims a **tiny GPU
fraction** instead of a whole GPU.  Consequences:

* Ray fractional-GPU scheduling lets many engines' actors co-reside on the same
  bundle / physical GPU (fractions sum well under 1.0).
* Ray still assigns each fractional actor a real GPU id and sets
  ``CUDA_VISIBLE_DEVICES``, so the stock GPU-discovery / worker-init path
  (Steps 6-7 of ``_init_executor``) works unchanged — no need to reimplement it.
* Fractional ``num_gpus`` is a *scheduling* hint only; it does **not** cap GPU
  memory.  Memory is governed by vLLM ``gpu_memory_utilization`` and time-shared
  across engines by the deployment's sleep/wake switching.

Usage (the deployment owns the placement group and passes it in via
``vllm_config.parallel_config.placement_group``)::

    from fuse_llm.fused_ray_executor import FusedRayExecutor
    args = AsyncEngineArgs(..., tensor_parallel_size=tp,
                           distributed_executor_backend=FusedRayExecutor)
    vllm_config = args.create_engine_config()
    vllm_config.parallel_config.placement_group = deployment_pg  # shared PG
    engine = AsyncLLM.from_vllm_config(vllm_config)
"""

from typing import Any, Dict

from vllm.platforms import current_platform
from vllm.v1.executor.ray_executor_v2 import RayExecutorV2


class FusedRayExecutor(RayExecutorV2):
    """RayExecutorV2 that co-resides many engines on shared GPUs.

    Set :attr:`per_worker_gpu_fraction` low enough that
    ``num_engines * gpus_per_engine * fraction <= 1.0`` per bundle.  The default
    (0.01) supports up to ~100 co-resident engines per GPU.
    """

    per_worker_gpu_fraction: float = 0.01

    @staticmethod
    def _get_actor_resource_kwargs() -> Dict[str, Any]:
        frac = FusedRayExecutor.per_worker_gpu_fraction
        device_key = current_platform.ray_device_key
        if device_key == "GPU":
            return {"num_gpus": frac}
        # Non-CUDA accelerators are addressed by custom resource, not num_gpus.
        return {"num_gpus": 0, "resources": {device_key: frac}}
