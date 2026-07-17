# Design: RDT weight loading for FuseModelDeployment

**Date:** 2026-07-17
**Branch:** feat/fused-model-deployment
**Status:** Approved (pending spec review)

## Problem

`FuseModelDeployment` (`fuse_llm_sdk/fuse_llm/deployment.py`) runs multiple vLLM
engines in one Ray Serve actor sharing one GPU set. Only a subset of models (a
"combination") is awake at a time; the rest sleep via vLLM sleep/wake.

Two sleep tiers exist today:

- **Level 1** — weights offloaded to the serving actor's CPU RAM, KV cache
  discarded. Fast wake, but every sleeping model's weights sit in host RAM
  *inside the serving actor*.
- **Level 2** — weights and KV cache both discarded. Frees all memory, but wake
  reloads weights **from disk/OSS** (`deployment.py:525-533`), which is slow on
  this cluster (OSS-fuse is the known bottleneck).

**Goal:** give level-2 sleep its full memory-free behavior but replace the slow
disk reload on wake with a pull from a hot, shared weight cache over the
cluster RDMA fabric — so scarce inference GPUs hold no idle weights and wake
does not touch disk.

## Decisions (locked during brainstorming)

| Decision | Choice |
|---|---|
| Primary goal | Fast level-2 wake without the disk read |
| Weight source location | **Remote host RAM** on a non-inference node |
| Transport | **NIXL** (RDMA), heterogeneous CPU-source → GPU-dest |
| TP scope (v1) | **tensor_parallel_size = 1** only; TP>1 is a follow-up |
| Integration approach | **A** — redirect the existing `reload_weights` behind a `weight_source` config; standard `model.load_weights` (no zero-copy yet) |
| Deliverable | Working prototype + validation on the `fuse-tp-test` pod |

### Why remote host RAM (not remote GPU)

For multi-model time-sharing, many models sleep while a few are awake. Host RAM
is abundant, so one cache can hold many models' weights and consumes no GPU. A
remote GPU source is marginally faster (GPUDirect both ends) but limited by VRAM
and burns a whole GPU. NIXL supports both — the server just allocates its pool
on `cpu` — so this is not a lock-in.

### Feasibility evidence (verified in code + cluster)

- NIXL is registered for both device types:
  `register_tensor_transport("NIXL", ["cuda", "cpu"], ...)` (`python/ray/experimental/rdt/util.py:102`).
- The NIXL transport handles CPU tensors explicitly:
  `mem_type = "cuda" if tensor.is_cuda else "cpu"` (`nixl_tensor_transport.py:575`).
- `register_nixl_memory_pool(size, device)` accepts a `cpu` device (`util.py:314`).
- The rtx5000 cluster runs `ack-erdma-controller` + `alibabacloud-erdma-agent`
  on many nodes — **eRDMA is available**, so cross-node NIXL/UCX has a fabric.

## Architecture

Three pieces; two new, one minimal edit. All live in `fuse_llm_sdk/` — no change
to Ray Serve control-plane files.

### 1. `WeightCacheServer` (new — `fuse_llm/weight_cache.py`)

A Ray actor scheduled on a non-inference node.

- On init, for each model: load the HF-layout `state_dict` into **pinned host
  RAM once** (the single cluster-wide disk/OSS read), and pin each tensor with
  `register_nixl_memory(...)` so NIXL keeps the region registered.
- `@ray.method(tensor_transport="nixl") get_weights(model_id) -> list[torch.Tensor]`
  — returns the CPU tensors; RDT/NIXL RDMA-serves them to the caller.
- `get_weight_names(model_id) -> list[str]` — the matching name order (plain,
  tiny call).
- Tensor order returned by `get_weights` MUST match the order of
  `get_weight_names` so the worker can `zip(names, tensors)`.

### 2. `RDTReloadWorkerExtension` (new — `fuse_llm/rdt_worker.py`)

Set as vLLM's `worker_extension_cls`; its methods run **on each vLLM worker**.

```python
def reload_weights(self, server_handle, model_id):
    try:
        names = ray.get(server_handle.get_weight_names.remote(model_id))  # tiny
        tensors = ray.get(server_handle.get_weights.remote(model_id))     # NIXL pull
        loaded = self.model_runner.model.load_weights(zip(names, tensors))
        missing = set(names) - set(loaded)
        assert not missing, f"weights not filled: {sorted(missing)[:5]}..."
        torch.cuda.synchronize()
        return {"ok": True, "loaded": len(loaded)}
    except Exception as e:
        # Fall back to the disk reload path within the same wake.
        return self._reload_weights_from_disk(model_id, reason=str(e))
```

The worker fetches both the name order and the tensors from the server, so the
deployment only passes `(server, model_id)` — it never has to hold the weight
names.

Notes:
- The worker is a Ray actor (via `FusedRayExecutor`), so it is a valid RDT
  destination. Default transports auto-register lazily on first use.
- Coverage check gates readiness: a partial/garbage fill must never be reported
  as success.

### 3. `InProcessVLLMEngine` edit (`engines.py`)

In `start()`, when `weight_source == "rdt"`:

```python
ek["worker_extension_cls"] = "fuse_llm.rdt_worker.RDTReloadWorkerExtension"
```

Nothing else in the engine changes; `collective_rpc` already forwards `args`.

## Data flow (level-2 sleep → RDT wake)

```
sleep(level=2)              # fused GPU frees weights + KV; server keeps weights in host RAM
wakeup(tags=["weights"])    # reallocate GPU weight buffers (uninitialized)
collective_rpc("reload_weights", args=(server, model_id, names))
    worker:
        tensors = ray.get(server.get_weights.remote(model_id))  # NIXL RDMA: remote host RAM -> this GPU
        model.load_weights(zip(names, tensors))                 # in-place; vLLM does QKV/MLP fusing
wakeup(tags=["kv_cache"])   # reallocate KV cache -> model awake
```

## API / config surface

- `FuseModelDeployment(..., weight_source: str = "disk", weight_cache=None)`
  - `weight_source="disk"` (default) — current behavior, untouched.
  - `weight_source="rdt"` — use RDT wake; `weight_cache` is a
    `WeightCacheServer` handle (or a config to spawn one).
- The only `deployment.py` change: in `_wakeup_engine`'s level-2 branch, when
  `weight_source=="rdt"`, pass `args=(self._weight_cache, model_id)` to the
  existing `collective_rpc("reload_weights")` call. The level-1 path and the
  disk level-2 path are unchanged.
- `deploy.py` gains an opt-in flag to create + populate a `WeightCacheServer`
  from the same `LLMConfig` list.

## Error handling & fallback

- **Transport failure** (NIXL unavailable, server unreachable, transfer abort):
  the worker method falls back to disk reload within the same wake, logs a
  warning, and the switch still succeeds. RDT is an accelerator, never a hard
  dependency.
- **Readiness gate:** a model is marked `awake` only after `reload_weights`
  reports success (coverage check). Prevents serving a garbage/partial model —
  the level-2 failure mode noted in prior sleep/wake work.
- **Server lifetime:** the cache is a wake-time dependency. If it dies, wakes
  fall back to disk. (A cache pool / restart policy is out of scope for v1.)
- **One-sided abort:** NIXL is one-sided; a mid-transfer error may cause Ray to
  kill the worker. The try/except aims to catch recoverable failures; a killed
  worker triggers replica restart (acceptable for v1, logged).

## Testing & validation

### Local unit tests (no GPU)

- `_wakeup_engine` passes correct rpc args (`server, model_id, names`) when
  `weight_source=="rdt"`, and takes the disk path when `"disk"`.
- Worker-extension fallback: on a raised exception from the pull, it calls the
  disk reload path and still returns a success shape.
- `WeightCacheServer` name/tensor ordering with tiny CPU tensors (NIXL
  registration mocked) — `get_weights` order matches `get_weight_names`.

### Pod validation (`fuse-tp-test`, eRDMA present)

- Spawn a `WeightCacheServer` on a second node, load 2 models (TP=1).
- `switch_combination(..., sleep_level=2)` between them.
- Assert outputs are **identical** to a disk-reload wake (correctness).
- Measure wake latency: RDT path vs disk path (the win metric).

## Open assumption to verify first

The existing level-2 wake calls `collective_rpc("reload_weights")`
(`deployment.py:532`), but no `worker_extension_cls` is set today
(`engines.py:64-72`) and vLLM V1 workers have no built-in `reload_weights`. So
the **disk baseline itself may be a placeholder** that has never run. First
implementation step is therefore to confirm/implement a working disk
`reload_weights` (it becomes both the fallback and the correctness baseline for
the RDT path). If it does not exist, the RDT extension must provide the disk
fallback (`_reload_weights_from_disk`) rather than delegate to a pre-existing one.

## Scope / non-goals (v1)

- TP>1 per-rank sharding — follow-up.
- Zero-copy `target_buffers` into param storage (Approach C) — later perf pass.
- Quantized/fused-weight post-processing beyond what `model.load_weights` +
  `process_weights_after_loading` already handle — revisit per model.
- Cache high-availability / multi-replica cache pool.
- Full OpenAI-protocol parity in `InProcessVLLMEngine` (pre-existing gap).

## Files touched

| File | Change |
|---|---|
| `fuse_llm_sdk/fuse_llm/weight_cache.py` | **new** — `WeightCacheServer` actor |
| `fuse_llm_sdk/fuse_llm/rdt_worker.py` | **new** — `RDTReloadWorkerExtension` |
| `fuse_llm_sdk/fuse_llm/engines.py` | set `worker_extension_cls` when `rdt` |
| `fuse_llm_sdk/fuse_llm/deployment.py` | `weight_source`/`weight_cache` params; rpc args in level-2 branch |
| `fuse_llm_sdk/fuse_llm/deploy.py` | opt-in flag to spawn/populate the cache |
| `fuse_llm_sdk/tests/` | unit tests for orchestration + ordering |
