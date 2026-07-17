"""Deployment helper for FuseModelDeployment + TrafficAwareController.

Usage::

    from fuse_llm import deploy

    handle, controller = deploy(
        model_configs=[...],
        model_weights={"model-a": 0.7, "model-b": 0.3},
    )
"""

import logging
from typing import Any, List, Optional, Tuple

import ray
from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig

from fuse_llm.deployment import FuseModelDeployment
from fuse_llm.controller import TrafficAwareController

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model configuration helper
# ---------------------------------------------------------------------------


def make_model_config(
    model_id: str,
    model_source: str,
    max_model_len: int = 8192,
    max_num_seqs: int = 32,
    gpu_memory_utilization: float = 0.9,
    tensor_parallel_size: int = 1,
    enable_sleep_mode: bool = True,
    **extra_engine_kwargs: Any,
) -> LLMConfig:
    """Create an LLMConfig for a vLLM model in a FuseModelDeployment.

    Uses ``enforce_eager=True`` to avoid CUDA-graph conflicts when engines share
    GPUs, and ``enable_sleep_mode=True`` (required — vLLM ``sleep``/``wakeup``
    only works when the engine was started with it).

    ``tensor_parallel_size`` may be >1; the engines then share the same set of
    ``tensor_parallel_size`` GPUs and time-share memory via sleep/wake.  All
    models in one deployment should use the same ``tensor_parallel_size`` (they
    share a single placement group sized to it).

    Throughput tuning
    -----------------
    A single engine already serves many concurrent requests over one copy of
    the weights (continuous batching).  To handle more load on one GPU, raise
    ``max_num_seqs`` and ``gpu_memory_utilization`` (a bigger KV cache admits
    more concurrent sequences); to add compute beyond one GPU, raise
    ``tensor_parallel_size``.  Any other vLLM ``AsyncEngineArgs`` field can be
    passed via ``**extra_engine_kwargs`` (e.g. ``max_num_batched_tokens=8192``,
    ``enable_chunked_prefill=True``) — they are forwarded verbatim to the
    engine.

    Note: ``gpu_memory_utilization`` (hence KV-cache size, hence the throughput
    ceiling) is fixed at engine construction and is *not* re-expanded when a
    model becomes the sole awake member of a combination.  For a hot model that
    should use the whole card, give it a high ``gpu_memory_utilization`` and run
    it in a size-1 combination.

    The executor is :class:`~fuse_llm.fused_ray_executor.FusedRayExecutor`
    (set by :class:`~fuse_llm.engines.InProcessVLLMEngine`), which lets every
    engine co-reside on the deployment's shared GPUs via fractional ``num_gpus``.
    The stock ``ray.serve.llm`` ``VLLMEngine`` cannot do this (it reserves whole
    GPUs per engine), so register the in-process engine via
    :func:`~fuse_llm.deployment.set_vllm_engine_class` and run inside a Ray
    actor (the ``FuseServeDeployment`` Serve replica does both).
    """
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_id,
            model_source=model_source,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tensor_parallel_size,
            gpu_memory_utilization=gpu_memory_utilization,
            enforce_eager=True,
            max_model_len=max_model_len,
            max_num_seqs=max_num_seqs,
            enable_sleep_mode=enable_sleep_mode,
            **extra_engine_kwargs,
        ),
    )


# ---------------------------------------------------------------------------
# Serve Deployment wrapper
# ---------------------------------------------------------------------------


@serve.deployment(
    num_replicas=1,
    ray_actor_options={
        "num_gpus": 1,
    },
    health_check_period_s=30,
    health_check_timeout_s=60,
)
class FuseServeDeployment(FuseModelDeployment):
    """Ray Serve wrapper around :class:`FuseModelDeployment`."""

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

    async def check_health(self) -> None:
        """Serve health check."""
        await FuseModelDeployment.check_health(self)


# ---------------------------------------------------------------------------
# Deployment entrypoint
# ---------------------------------------------------------------------------


def deploy(
    model_configs: List[LLMConfig],
    default_models: Optional[List[str]] = None,
    app_name: str = "fuse-model",
    route_prefix: str = "/",
    drain_timeout_s: float = 30.0,
    start_controller: bool = False,
    model_weights: Optional[dict] = None,
    poll_interval_s: float = 10.0,
    min_interval_s: float = 60.0,
    queue_threshold: int = 10,
    default_sleep_level: int = 1,
    weight_source: str = "disk",
    model_sources: Optional[dict] = None,
    weight_cache_options: Optional[dict] = None,
) -> Tuple:
    """Deploy the FuseModelDeployment (multi-model GPU time-sharing).

    Switching between combinations is explicit: call
    ``handle.switch_combination.remote([...])`` (or
    :func:`fuse_llm.client.switch_combination`).

    Parameters
    ----------
    model_configs:
        List of LLMConfigs, one per model.
    default_models:
        Combination (list of ``model_id``) to keep awake after
        initialisation.  Defaults to ``[first config]``.
    app_name:
        Ray Serve application name.
    route_prefix:
        HTTP route prefix.
    drain_timeout_s:
        Timeout for draining in-flight requests during a combination switch.
    start_controller:
        Whether to start the :class:`TrafficAwareController`.  Defaults to
        ``False``.  **The controller currently targets the pre-combination
        single-model API and is pending a rewrite** — leave it off until then
        (see :mod:`fuse_llm.controller`).  When ``False`` the returned
        controller handle is ``None``.
    model_weights, poll_interval_s, min_interval_s, queue_threshold,
    default_sleep_level:
        Legacy controller tuning parameters; only used when
        ``start_controller=True``.

    Returns
    -------
    tuple
        ``(serve_handle, controller_handle_or_None)``
    """
    if not ray.is_initialized():
        ray.init()

    # 0. Optionally stand up the RDT weight cache. When weight_source='rdt',
    #    a WeightCacheServer actor holds each model's weights in host RAM and
    #    serves them over NIXL so level-2 wakes reload weights without disk.
    weight_cache = None
    if weight_source == "rdt":
        from fuse_llm.weight_cache import WeightCacheServer

        if not model_sources:
            raise ValueError(
                "weight_source='rdt' requires model_sources={model_id: local_dir}"
            )
        opts = weight_cache_options or {}
        weight_cache = WeightCacheServer.options(**opts).remote(model_sources)

    # 1. Deploy the fuse application
    app = FuseServeDeployment.bind(
        llm_configs=model_configs,
        default_models=default_models,
        drain_timeout_s=drain_timeout_s,
        weight_source=weight_source,
        weight_cache=weight_cache,
    )
    serve.run(app, name=app_name, route_prefix=route_prefix, blocking=False)
    logger.info("FuseServeDeployment deployed as '%s'", app_name)

    # 2. Get a handle to the deployment
    handle = serve.get_deployment_handle(
        deployment_name="FuseServeDeployment",
        app_name=app_name,
    )

    # 3. Optionally start the traffic-aware controller.
    #    TODO: the controller's automatic switching is built around queueing
    #    requests for a sleeping model; combinations now reject such requests
    #    instead, so its triggers no longer fire.  It needs a combination-aware
    #    rewrite before it is useful again — hence off by default.
    controller = None
    if start_controller:
        if model_weights is None:
            share = 1.0 / len(model_configs)
            model_weights = {
                c.model_loading_config.model_id: share for c in model_configs
            }
        controller = TrafficAwareController.remote(
            fuse_deployment_handle=handle,
            model_weights=model_weights,
            poll_interval_s=poll_interval_s,
            min_interval_s=min_interval_s,
            queue_threshold=queue_threshold,
            drain_timeout_s=drain_timeout_s,
            default_sleep_level=default_sleep_level,
        )
        controller.start.remote()
        logger.info("TrafficAwareController started.")

    return handle, controller
