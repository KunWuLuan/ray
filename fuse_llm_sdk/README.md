# Fuse LLM SDK

Multi-model GPU time-sharing SDK for Ray Serve + vLLM.

## Overview

Fuse LLM SDK allows you to load multiple vLLM models inside a single Ray Serve
actor, sharing one GPU (or one set of GPUs at `tensor_parallel_size > 1`). A
**combination** — an arbitrary subset of the loaded models — is "awake"
(holding GPU memory) at any given time; the rest are put to sleep via vLLM's
`sleep`/`wakeup` mechanism. As many models can be awake together as fit in GPU
memory (set `gpu_memory_utilization` low enough per model).

You switch from one combination to another explicitly with
`switch_combination([...])`: the models leaving the awake set are drained and
slept, the ones entering it are woken, and any overlap stays awake untouched.
Requests for a model that is **not** in the current combination are rejected —
wake it first.

## Installation

```bash
pip install fuse-llm
```

Requires `ray[llm]` (included as a dependency).

## Quick Start

```python
from fuse_llm import deploy, make_model_config, switch_combination

cfgs = [
    make_model_config("model-a", "/models/a", gpu_memory_utilization=0.3),
    make_model_config("model-b", "/models/b", gpu_memory_utilization=0.3),
    make_model_config("model-c", "/models/c", gpu_memory_utilization=0.3),
]

# Start with the {a, b} combination awake; c is loaded but asleep.
handle, _ = deploy(cfgs, default_models=["model-a", "model-b"])

# Requests to a and b work now; a request to c is rejected until you switch.

# Switch to the {b, c} combination: a sleeps, c wakes, b stays awake.
switch_combination(handle, ["model-b", "model-c"])
```

## Key Features

- **Single-actor multi-model**: All vLLM engines share one GPU process
- **Combinations**: run any subset of models awake at once; switch subsets via
  `switch_combination` (sleep the ones leaving, wake the ones entering)
- **Shared memory per model**: each model is one engine loaded once, so a model
  reused across combinations keeps a single CPU backup buffer — no duplication,
  and models in the overlap of two combinations aren't re-offloaded on a switch
- **Multi-GPU / TP>1 sharing**: engines can each use `tensor_parallel_size > 1`
  and share the *same* set of GPUs (see below)
- **Sleep level support**: Level 1 (CPU RAM offload) and Level 2 (disk reload)
- **Graceful drain**: In-flight requests complete before a combination switch
- **Explicit switching**: combinations are switched on demand via the handle

> **Note:** the `TrafficAwareController` (automatic QPS/queue-based switching)
> targets the previous single-active-model API and is **pending a
> combination-aware rewrite**; `deploy()` does not start it by default
> (`start_controller=False`). Use `switch_combination` for now.

## Scaling throughput (one model, more requests)

A single engine already batches many concurrent requests over **one copy of the
weights** (continuous batching) — you do **not** add throughput by running
multiple engines of the same model on one GPU (they contend for the same
compute). To serve more load:

- **On one GPU** — raise `max_num_seqs` and `gpu_memory_utilization` (a bigger
  KV cache admits more concurrent sequences). Pass any other vLLM knob through
  `make_model_config(..., max_num_batched_tokens=8192, enable_chunked_prefill=True)`.
- **Beyond one GPU** — raise `tensor_parallel_size` (one model, sharded across
  GPUs, one logical weight copy).

`gpu_memory_utilization` is fixed at engine construction, so a model does **not**
grow its KV cache when it becomes the only awake model in a combination. For a
hot model that should use the whole card, give it a high `gpu_memory_utilization`
and run it in a **size-1 combination**.

## Multi-GPU (tensor parallelism) and GPU sharing

Multiple engines share the same GPUs; a combination of them is awake at a time
(sleep/wake). Size `gpu_memory_utilization` so the intended combination fits (a
single-model combination can go high, e.g. 0.9; for N-model combinations keep it
near `1/N`). Set `tensor_parallel_size` in `make_model_config`.

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
handle, _ = deploy(cfgs)   # runs as a Serve replica (Ray actor)
```

Validated on 8× RTX PRO 5000: 2 engines × TP=4 (Qwen2.5-7B) sharing 4 GPUs,
sleep/wake switch **L1 ~283 ms / L2 ~845 ms**, and output is **token-for-token
identical** before sleep vs after L1 and L2 wake.
