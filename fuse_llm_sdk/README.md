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
- **Sleep level support**: Level 1 (CPU RAM offload) and Level 2 (disk reload)
- **Traffic-aware switching**: Controller auto-switches based on QPS ratios
- **Graceful drain**: In-flight requests complete before model switch
- **Manual override**: Force switch to a specific model when needed
