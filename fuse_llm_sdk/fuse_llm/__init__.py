"""Fuse LLM SDK: Multi-model GPU time-sharing for Ray Serve + vLLM.

Public API
----------
- :class:`FuseModelDeployment` — single-actor multi-model deployment
- :class:`TrafficAwareController` — traffic-aware switching controller
- :func:`deploy` — one-line deployment helper
"""

from fuse_llm.deployment import (
    FuseModelDeployment,
    ModelStats,
    set_vllm_engine_class,
)
from fuse_llm.controller import TrafficAwareController
from fuse_llm.deploy import (
    deploy,
    make_model_config,
    FuseServeDeployment,
)
from fuse_llm.engines import InProcessVLLMEngine
from fuse_llm.fused_ray_executor import FusedRayExecutor

__all__ = [
    "FuseModelDeployment",
    "FuseServeDeployment",
    "TrafficAwareController",
    "ModelStats",
    "deploy",
    "make_model_config",
    "set_vllm_engine_class",
    "InProcessVLLMEngine",
    "FusedRayExecutor",
]

__version__ = "0.1.0"
