# Ray Data Tuning Playbook

The Ray Data Tuning Advisor reads this file on every run. Sections are filled in
across implementation tasks.

## Section 0 — Parsing anchor map

Locate signals by these anchors (case-sensitive substrings). If an anchor is
absent, treat that signal as unavailable — do not guess.

### In `ds.stats()` text
- Operator header: lines starting with `Operator ` — the operator name follows.
- `Remote wall time:` — per-op wall time (min/max/mean/total).
- `Remote cpu time:` — per-op CPU time.
- `UDF time:` — per-op UDF time.
- `Output num rows per block:` / `Output size bytes per block:` — block-size profile.
- `Output rows per task` — rows per task.
- `Tasks per node:` — node distribution (`N nodes used`).
- `Spilled to disk:` / `Restored from disk:` — under `Cluster memory` (whole
  cluster) and `Dataset memory` (this dataset).
- `Ray Data throughput:` / `rows/s` — dataset throughput.
- `Dataset iterator time breakdown` — iteration timers (time to first batch, blocked).

### In `ray-data.log` / console
- `Starting execution of Dataset` / `Execution plan of Dataset` — DAG.
- `Execution Progress:` and per-op lines with `Tasks:`, `Queued blocks:`,
  `Resources:`, `[backpressured:...]`, `Actors:` — periodic topology dump.
- Warning anchors (quote verbatim as evidence):
  - `has no outputs for` — idle detector.
  - `has been running or stuck in scheduling for` — hanging detector.
  - `of memory per task on average, but Ray only requests` — high-memory detector.
  - `Cluster resources are not enough to run any task` — resource starvation.
  - `is more than 4x the number of available CPU slots` — read over-parallelism.
  - `will not allow it to scale up` — actor pool cannot scale.
  - `exceeds DataContext.get_current().target_shuffle_max_block_size` — large block.
  - `hash-shuffle aggregators are ready after` / `Insufficient CPU resources in cluster for hash shuffle` / `Insufficient memory resources in cluster for hash shuffle` — shuffle health.
  - `estimated to use at least` + `driver memory` — driver memory pressure.
  - `Timed out` + `waiting for metadata from operator` — metadata fetch timeout.

### In Prometheus/Grafana export
- Column/series names begin with `ray_data_`. Key series:
  `ray_data_spilled_bytes`, `ray_data_output_bytes`, `ray_data_cpu_usage_cores`,
  `ray_data_gpu_usage_cores`, `ray_data_pool_utilization`,
  `ray_data_num_pending_actors`, `ray_data_num_idle_actors`,
  `ray_data_task_submission_backpressure_time`,
  `ray_data_task_output_backpressure_time`,
  `ray_data_object_store_memory_budget`, `ray_data_*_per_node`,
  `ray_data_cluster_{cpu,gpu,mem,object_store_memory}_utilization`.

## Section 1 — Derived indicators

Compute each from whatever inputs exist. Show the value and the inputs used.

| Indicator | Formula / source | Interpretation |
|---|---|---|
| `spill_ratio` | `ray_data_spilled_bytes` (or `Spilled to disk` MB) ÷ dataset output bytes | >0 means spilling; >0.2 is significant object-store pressure |
| `gpu_busy_fraction` | `ray_data_gpu_usage_cores` ÷ allocated GPUs (or UDF-time share on a GPU op) | <0.5 with GPUs allocated = GPU underutilized |
| `per_op_walltime_share` | each op `Remote wall time total` ÷ Σ over ops | the op with the largest share is the bottleneck |
| `submission_backpressure_share` | `ray_data_task_submission_backpressure_time` ÷ wall time | high = op throttled from launching tasks (budget/downstream) |
| `output_backpressure_share` | `ray_data_task_output_backpressure_time` ÷ wall time | high = consumer/downstream too slow |
| `pool_utilization` | `ray_data_pool_utilization`; with `num_pending`/`num_idle_actors` | ~1.0 pinned = under-provisioned; low with idle actors = over-provisioned |
| `block_size_profile` | `Output size bytes per block` / `block_size_bytes` histogram | >~512MB/block risks OOM/spill; very small blocks = task overhead |
| `read_parallelism_ratio` | read blocks ÷ cluster CPU slots | >4 triggers the over-parallelism warning |
| `tasks_per_node_skew` | `Tasks per node` max ÷ min (or `*_per_node` metrics) | >>1 indicates imbalance/locality issues |
| `time_to_first_batch` / `iter_blocked` | `ray_data_iter_time_to_first_batch_seconds` / `iter_total_blocked_seconds` | large = consumer (trainer) starvation |

Report indicators you could not compute as "unavailable (missing <input>)".

## Section 2 — Symptom taxonomy

Each entry: **Confirming signals → Root cause → Ranked remediations → Evidence
to cite**. A symptom fires only when its confirming signals are present.

### S1. Object-store spilling / OOM
- **Confirming signals:** `spill_ratio` > 0 (esp. > 0.2); `Spilled to disk` /
  `ray_data_spilled_bytes` large; high-memory-detector warning
  (`of memory per task on average, but Ray only requests`); `block_size_profile`
  large (>~512MB/block).
- **Root cause:** blocks and/or per-task working set exceed available
  object-store / heap memory, forcing spill to disk (and slowdowns / OOM risk).
- **Ranked remediations:**
  1. Reduce block size: lower `DataContext.target_max_block_size` (config) so
     each task holds less in memory. → pod: no change needed.
  2. Declare the real per-task memory: set `memory=<observed>` on the map op
     (per-op) so the scheduler packs fewer tasks per node. → KubeRay: ensure pod
     memory ÷ concurrency ≥ that value.
  3. Raise object-store memory: larger `object_store_memory` (pod). → KubeRay:
     bump worker-group pod `memory` request + `rayStartParams.object-store-memory`,
     or add a worker group / raise `maxReplicas`.
  4. Cap concurrency if upstream floods a slow op: `concurrency=` / backpressure.
- **Evidence to cite:** the exact spilled-MB figure and the high-memory warning line.

## Section 3 — Recommendation catalog (levers)

Draw remediations from these four levers, always by exact knob name:

- **Config** (`DataContext` / `ExecutionOptions`): `target_max_block_size`,
  `target_shuffle_max_block_size`, `override_num_blocks`,
  `read_op_min_num_blocks`, `execution_options.resource_limits`,
  `execution_options.exclude_resources`, `actor_pool_util_upscaling_threshold`,
  `shuffle_strategy`, `max_hash_shuffle_aggregators`, issue-detector configs.
- **Per-op call args**: `concurrency=`, `compute=` (ActorPoolStrategy),
  `num_cpus=`, `num_gpus=`, `memory=`, `batch_size=`, operator fusion,
  `.materialize()` before shuffle.
- **Pod size**: worker-group CPU/memory/GPU requests, `--num-cpus`,
  `object_store_memory`, head-node memory.
- **Autoscaling**: RayCluster autoscaler `minReplicas`/`maxReplicas` per worker
  group, actor-pool min/max size.

## Section 4 — KubeRay translation table

Every recommendation is emitted as `Ray knob → KubeRay translation`.

| Ray-level knob | KubeRay / pod expression |
|---|---|
| Raise `object_store_memory` | Increase worker-group pod `resources.requests.memory` and set `rayStartParams: { object-store-memory: "<bytes>" }`; or add a worker group / raise its `maxReplicas` |
| Raise per-task `memory=` | Ensure worker-group pod memory ÷ per-pod concurrency ≥ the requested per-task memory; enlarge pod memory request if not |
| More `num_cpus` capacity / concurrency | Raise worker-group `replicas`/`maxReplicas` or pod `resources.requests.cpu` + `rayStartParams.num-cpus` |
| More GPUs for a GPU op | Add/enlarge a GPU worker group (`nvidia.com/gpu` request) and its `minReplicas`/`maxReplicas` |
| Actor pool min/max size | Align worker-group `minReplicas`/`maxReplicas` so the pool's max actors fit; GPU actors need one GPU slot each |
| Reduce driver memory pressure | Increase head-group pod `resources.requests.memory` |
| `exclude_resources` for non-Data workloads | Reflect co-located non-Data pods; or isolate Ray Data onto a dedicated worker group |

Autoscaling note: Ray Data actor-pool scaling operates within the pods the
KubeRay autoscaler provides. If the actor pool wants more actors than the
worker group's `maxReplicas` can host, raise `maxReplicas` too — otherwise the
pool is capped regardless of `pool_utilization`.

## Section 5 — Signal-reference appendix
(Every ray_data_* metric + warning string. Filled in Task 8.)
