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
`make_model_config`; the executor backend is chosen automatically by
`select_executor_backend`:

| topology | backend | placement group? |
|---|---|---|
| TP=1 | `uni` (in-process) | no |
| TP>1, single node | `mp` (local worker procs) | no |
| TP>1, multi-node | `FusedRayExecutor` (fractional `num_gpus` co-residence) | yes (deployment-owned) |

**Engine requirement**: the stock `ray.serve.llm` `VLLMEngine` forces
`distributed_executor_backend="ray"` (exclusive whole-GPU reservation, which
can't share) and rejects overrides. Use the in-process engine so the chosen
backend takes effect:

```python
from fuse_llm import set_vllm_engine_class, InProcessVLLMEngine, make_model_config
set_vllm_engine_class(InProcessVLLMEngine)
cfgs = [make_model_config("m1", "/models/m1", tensor_parallel_size=4),
        make_model_config("m2", "/models/m2", tensor_parallel_size=4)]
```

Validated on 8× RTX PRO 5000: 2 engines × TP=4 (Qwen2.5-7B) sharing 4 GPUs,
sleep/wake switch **L1 ~283 ms / L2 ~845 ms**, and output is **token-for-token
identical** before sleep vs after L1 and L2 wake.
