"""Fuse LLM SDK: Multi-model GPU time-sharing for Ray Serve + vLLM.

Public API
----------
- :class:`FuseModelDeployment` — single-actor multi-model deployment
- :class:`TrafficAwareController` — traffic-aware switching controller
- :func:`deploy` — one-line deployment helper
"""

from fuse_llm.deployment import FuseModelDeployment, ModelStats
from fuse_llm.controller import TrafficAwareController
from fuse_llm.deploy import deploy, make_model_config, FuseServeDeployment

__all__ = [
    "FuseModelDeployment",
    "FuseServeDeployment",
    "TrafficAwareController",
    "ModelStats",
    "deploy",
    "make_model_config",
]

__version__ = "0.1.0"
