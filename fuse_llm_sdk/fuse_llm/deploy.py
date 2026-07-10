"""Deployment helper for FuseModelDeployment + TrafficAwareController.

Usage::

    from fuse_llm import deploy

    handle, controller = deploy(
        model_configs=[...],
        model_weights={"model-a": 0.7, "model-b": 0.3},
    )
"""

import logging
from typing import List, Optional, Tuple

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
) -> LLMConfig:
    """Create an LLMConfig for a single-GPU vLLM model.

    All models in a FuseModelDeployment must use ``tensor_parallel_size=1``
    and ``enforce_eager=True`` to avoid CUDA graph conflicts when
    sharing a single GPU.
    """
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_id,
            model_source=model_source,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            gpu_memory_utilization=gpu_memory_utilization,
            enforce_eager=True,
            max_model_len=max_model_len,
            max_num_seqs=max_num_seqs,
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
        default_model: Optional[str] = None,
        drain_timeout_s: float = 30.0,
    ) -> None:
        FuseModelDeployment.__init__(
            self,
            llm_configs=llm_configs,
            default_model=default_model,
            drain_timeout_s=drain_timeout_s,
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
    model_weights: Optional[dict] = None,
    default_model: Optional[str] = None,
    app_name: str = "fuse-model",
    route_prefix: str = "/",
    poll_interval_s: float = 10.0,
    min_interval_s: float = 60.0,
    queue_threshold: int = 10,
    drain_timeout_s: float = 30.0,
    default_sleep_level: int = 1,
) -> Tuple:
    """Deploy the FuseModelDeployment and start the controller.

    Parameters
    ----------
    model_configs:
        List of LLMConfigs, one per model.
    model_weights:
        Traffic share per model.  If None, distributes equally.
    default_model:
        Model to keep awake after initialisation.
    app_name:
        Ray Serve application name.
    route_prefix:
        HTTP route prefix.
    poll_interval_s, min_interval_s, queue_threshold, drain_timeout_s:
        Controller tuning parameters.
    default_sleep_level:
        Default vLLM sleep level (1 or 2) used by the controller when
        sleeping models during automatic switching.

    Returns
    -------
    tuple
        ``(serve_handle, controller_handle)``
    """
    if model_weights is None:
        share = 1.0 / len(model_configs)
        model_weights = {
            c.model_loading_config.model_id: share for c in model_configs
        }

    if not ray.is_initialized():
        ray.init()

    # 1. Deploy the fuse application
    app = FuseServeDeployment.bind(
        llm_configs=model_configs,
        default_model=default_model,
        drain_timeout_s=drain_timeout_s,
    )
    serve.run(app, name=app_name, route_prefix=route_prefix, blocking=False)
    logger.info("FuseServeDeployment deployed as '%s'", app_name)

    # 2. Get a handle to the deployment for the controller
    handle = serve.get_deployment_handle(
        deployment_name="FuseServeDeployment",
        app_name=app_name,
    )

    # 3. Start the traffic-aware controller
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
