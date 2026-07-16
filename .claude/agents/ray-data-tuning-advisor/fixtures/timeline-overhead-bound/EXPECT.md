# Expected output for `timeline-overhead-bound`

Input: only a Ray timeline trace (`timeline.json`, Chrome-tracing JSON). No
ds.stats(), ray-data.log, Prometheus export, or RayCluster spec.

The subagent output MUST:

- **Aggregate the trace with a script** (python3/jq via Bash), not by reading it
  whole. From the aggregation:
  - `MapBatches(Embedder)` actors A1/A2 have a LOW `actor_busy_fraction` (~0.17–0.18:
    6 × 60ms busy over a ~2.06s span each) with large `inter_task_gap` (~340ms).
  - `MapBatches(Embedder)` is the wall-time long pole (runs to ~2.26s), while the
    upstream `FlatMap(extract_text_from_pdf)` was fully busy (`busy_fraction` ≈ 1.0)
    and finished early (~1.4s).
- **Fire S13 (GPU/actor stage is latency/overhead-bound)** as the top finding —
  the GPU op is the long pole yet mostly idle between calls.
- **NOT diagnose S2 (GPU underutilized due to upstream starvation)** as the cause:
  the upstream extract op was fully busy and finished early, so it was not starving
  the GPU; the GPU idleness is per-call overhead inside the op.
- **Recommend** raising in-flight concurrency (`concurrency`/`max_concurrency`),
  `num_cpus=` on the GPU op, larger `batch_size` (+ library-internal batch), and/or
  splitting CPU preprocessing into its own upstream stage / avoiding per-call
  recompilation.
- **Explicitly advise NOT adding GPUs** (the device is idle — more GPUs won't help
  an overhead-bound stage). KubeRay: hold GPU worker-group size; scale CPU/concurrency.
- Cite `actor_busy_fraction` and `inter_task_gap` as evidence; invent no metric.
- Emit all 5 report sections; Section 4 states the RayCluster spec was not provided.
