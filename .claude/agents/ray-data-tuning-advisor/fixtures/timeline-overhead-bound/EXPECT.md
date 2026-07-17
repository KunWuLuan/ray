# Expected output for `timeline-overhead-bound`

Input: only a Ray timeline trace (`timeline.json`, real Chrome-tracing schema).
No ds.stats(), ray-data.log, Prometheus export, or RayCluster spec.

The subagent output MUST:

- **Aggregate the trace with a script** (not by reading it whole), following the
  real Ray schema: attribute work by `args.actor_id` + `cat == "task:execute"`,
  and use **interval-union** (spans overlap under concurrency). It must NOT count
  the `task::MapWorker(MapBatches(Embedder)).submit` wrapper span as busy time
  (that would falsely inflate the fraction toward 1.0).
- From the aggregation:
  - Embedder actors A1/A2 have a LOW `actor_busy_fraction` (~0.21: 12 × 50ms
    execute over a ~2.8–2.9s active window) with large inter-execute gaps (~200ms).
  - `MapBatches(Embedder)` is the wall-time long pole (active window ends ~2.9s),
    while upstream `FlatMap(extract_text_from_pdf)` was fully busy and finished
    early (~1.4s).
- **Fire S13 (GPU/actor stage is latency/overhead-bound)** as the top finding.
- **NOT diagnose S2 (upstream starvation)**: the upstream extract op was fully
  busy and finished early, so it was not starving the GPU; the idleness is
  per-call overhead inside the embedding op.
- **Recommend** raising in-flight concurrency (`concurrency`/`max_concurrency`),
  `num_cpus=` on the GPU op, larger `batch_size` (+ library-internal batch),
  and/or splitting CPU preprocessing into its own upstream stage / avoiding
  per-call recompilation.
- **Explicitly advise NOT adding GPUs** (the device is idle — more GPUs won't help
  an overhead-bound stage). KubeRay: hold GPU worker-group size; scale CPU/concurrency.
- Cite `actor_busy_fraction` (interval-union) and the inter-execute gap; invent no metric.
- Emit all 5 report sections; Section 4 states the RayCluster spec was not provided.
