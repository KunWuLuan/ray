# Fuse LLM SDK

Multi-model GPU time-sharing SDK for Ray Serve + vLLM.

## Overview

Fuse LLM SDK allows you to load multiple vLLM models inside a single Ray Serve
actor, sharing one GPU. Only one model is "awake" (holding GPU memory) at any
given time; the others are put to sleep via vLLM's `sleep`/`wakeup` mechanism.

A traffic-aware controller automatically switches the active model based on
request patterns (QPS, queue depth).

## Installation

```bash
pip install fuse-llm
```

Requires `ray[llm]` (included as a dependency).

## Quick Start

```python
from fuse_llm import deploy

handle, controller = deploy(
    model_configs=[...],
    model_weights={"model-a": 0.7, "model-b": 0.3},
)
```

## Key Features

- **Single-actor multi-model**: All vLLM engines share one GPU process
- **Multi-GPU / TP>1 sharing**: engines can each use `tensor_parallel_size > 1`
  and share the *same* set of GPUs (see below)
- **Sleep level support**: Level 1 (CPU RAM offload) and Level 2 (disk reload)
- **Traffic-aware switching**: Controller auto-switches based on QPS ratios
- **Graceful drain**: In-flight requests complete before model switch
- **Manual override**: Force switch to a specific model when needed

## Multi-GPU (tensor parallelism) and GPU sharing

Multiple engines share the same GPUs; only one is awake at a time (sleep/wake),
so `gpu_memory_utilization` can be high (e.g. 0.9). Set `tensor_parallel_size` in
`make_model_config`.

**How sharing works.** The deployment owns **one** placement group sized to
`tensor_parallel_size` GPU bundles. Every engine runs on
`FusedRayExecutor` — a `RayExecutorV2` subclass whose workers claim a tiny GPU
*fraction*, so they all co-reside on that shared PG's GPUs (Ray fractional-GPU
scheduling). GPU memory is time-shared by sleep/wake. This is the single backend
for all topologies (TP=1, TP>1, single- or multi-node).

`InProcessVLLMEngine` (the default engine) builds vLLM's `AsyncLLM` in-process
with `FusedRayExecutor` and injects the shared PG. The stock `ray.serve.llm`
`VLLMEngine` **can't** do this — it forces `distributed_executor_backend="ray"`
and reserves whole GPUs per engine — so it isn't used.

**Requirement**: engines must be created inside a **Ray actor** (so vLLM
propagates `RAY_ADDRESS` to the EngineCore subprocess and the shared PG
resolves). `FuseServeDeployment` (a Serve replica) satisfies this automatically.
All models in one deployment should use the same `tensor_parallel_size`.

```python
from fuse_llm import make_model_config, deploy
cfgs = [make_model_config("m1", "/models/m1", tensor_parallel_size=4),
        make_model_config("m2", "/models/m2", tensor_parallel_size=4)]
handle, controller = deploy(cfgs)   # runs as a Serve replica (Ray actor)
```

Validated on 8× RTX PRO 5000: 2 engines × TP=4 (Qwen2.5-7B) sharing 4 GPUs,
sleep/wake switch **L1 ~283 ms / L2 ~845 ms**, and output is **token-for-token
identical** before sleep vs after L1 and L2 wake.
