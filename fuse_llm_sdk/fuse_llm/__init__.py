"""Fuse LLM SDK: Multi-model GPU time-sharing for Ray Serve + vLLM.

Public API
----------
- :class:`FuseModelDeployment` — single-actor multi-model deployment; a
  combination (subset) of models is awake at a time
- :func:`deploy` — one-line deployment helper
- :func:`switch_combination` — switch the awake combination (sleep/wake)
- :class:`TrafficAwareController` — legacy switching controller (pending a
  combination-aware rewrite)
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
from fuse_llm.client import switch_combination
from fuse_llm.engines import InProcessVLLMEngine
from fuse_llm.fused_ray_executor import FusedRayExecutor
from fuse_llm.weight_cache import WeightCacheServer

__all__ = [
    "FuseModelDeployment",
    "FuseServeDeployment",
    "TrafficAwareController",
    "ModelStats",
    "deploy",
    "make_model_config",
    "switch_combination",
    "set_vllm_engine_class",
    "InProcessVLLMEngine",
    "FusedRayExecutor",
    "WeightCacheServer",
]

__version__ = "0.1.0"
