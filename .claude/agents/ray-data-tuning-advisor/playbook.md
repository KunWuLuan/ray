# Ray Data Tuning Playbook

The Ray Data Tuning Advisor reads this file on every run. It is the diagnostic
knowledge base: parsing anchor map (Section 0), derived indicators (Section 1),
symptom taxonomy S1–S13 (Section 2), recommendation catalog (Section 3), KubeRay
translation table (Section 4), and signal-reference appendix (Section 5).

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
  - `hash-shuffle aggregators are ready after` / `Insufficient CPU resources in cluster for hash shuffle` / `Insufficient memory resources in cluster for hash shuffle` / `exceeds the largest node's memory` — shuffle health (last = per-aggregator memory clamp/spill).
  - `estimated to use at least` + `driver memory` — driver memory pressure.
  - `Timed out` + `waiting for metadata from operator` — metadata fetch timeout.

### In Prometheus/Grafana export
- Column/series names begin with `ray_data_`. Key series:
  `ray_data_spilled_bytes`, `ray_data_output_bytes`, `ray_data_cpu_usage_cores`,
  `ray_data_gpu_usage_cores`, `ray_data_pool_utilization`,
  `ray_data_num_pending_actors`, `ray_data_num_idle_actors`,
  `ray_data_task_submission_backpressure_time`,
  `ray_data_task_output_backpressure_time`,
  `ray_data_memory_budget`, `ray_data_object_store_memory_budget`,
  `ray_data_*_per_node`,
  `ray_data_cluster_{cpu,gpu,mem,object_store_memory}_utilization`.

### In a Ray timeline trace (Chrome-tracing JSON)
Produced by `ray timeline` / `ray.timeline(filename=...)`. It is an array of
trace events (or `{"traceEvents": [...]}`); each event has `ph` (phase: `X` =
complete event with a `dur`; `M` = metadata), `ts`/`dur` (microseconds),
`pid`/`tid` (a `pid`/`tid` pair is one worker/actor track, labeled by `M`
`process_name`/`thread_name` events with the op/actor/node name), `name`, `cat`.
**Do NOT Read the whole file — it can be hundreds of MB.** Aggregate with Bash
(python3/jq): per track compute `busy = Σdur`, `span = max(ts+dur) − min(ts)`,
`busy_fraction = busy/span`, and the gaps between consecutive events. Example:

```bash
python3 - "$TRACE" <<'PY'
import json,sys,collections
ev=json.load(open(sys.argv[1])); ev=ev.get('traceEvents',ev) if isinstance(ev,dict) else ev
names={}; tr=collections.defaultdict(list)
for e in ev:
    if e.get('ph')=='M' and e.get('name') in ('process_name','thread_name'):
        names[(e['pid'],e.get('tid'))]=e.get('args',{}).get('name')
    elif e.get('ph')=='X' and 'dur' in e:
        tr[(e['pid'],e['tid'])].append((e['ts'],e['dur']))
for k,evs in tr.items():
    evs.sort(); busy=sum(d for _,d in evs); span=(evs[-1][0]+evs[-1][1])-evs[0][0]
    gaps=[evs[i+1][0]-(evs[i][0]+evs[i][1]) for i in range(len(evs)-1)]
    lbl=names.get(k) or names.get((k[0],None)) or k
    print(f"{lbl}: busy_frac={busy/span:.2f} n={len(evs)} mean_gap_us={ (sum(gaps)/len(gaps)) if gaps else 0:.0f}")
PY
```

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
| `actor_busy_fraction` | (timeline) per-actor `Σdur ÷ track wall-span` | GPU actor <~0.5 = idle between calls → overhead/upstream-bound, NOT compute-bound |
| `inter_task_gap` | (timeline) mean/median gap between consecutive events on a track | large gaps = actor starved between calls (input fetch / CPU preprocessing / recompile / warmup) |
| `op_concurrency_over_time` | (timeline) max simultaneous task spans for an op vs its configured `concurrency` | far below configured = parallelism not realized (backpressure / placement) |

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

### S2. GPU underutilized
- **Confirming signals:** `gpu_busy_fraction` < 0.5 while a GPU operator exists
  (op with `num_gpus`>0 or a low `ray_data_gpu_usage_cores`); GPU-op
  `pool_utilization` low; GPU-op wall-time share small relative to an upstream
  CPU op.
- **Root cause:** the GPU stage is starved — usually CPU-bound preprocessing
  upstream can't feed it, or `batch_size`/`concurrency` on the GPU op is too low
  to saturate the device.
- **Ranked remediations:**
  1. Scale upstream CPU preprocessing: raise its `concurrency=` / add CPU pods
     so it keeps the GPU fed. → KubeRay: raise the CPU worker-group
     `replicas`/`maxReplicas`.
  2. Increase GPU throughput per actor: raise `batch_size=` on the GPU op (larger
     batches per forward pass) and/or `max_concurrency` of the actor.
  3. Right-size the GPU actor pool: raise `concurrency=`/actor-pool max if GPUs
     are idle. → KubeRay: ensure the GPU worker group `maxReplicas` provides
     enough GPU slots.
- **Evidence to cite:** GPU usage/pool-utilization value and the upstream CPU op's
  wall-time share.
- **Disambiguate before recommending more GPUs:** if a timeline trace exists,
  check `actor_busy_fraction` of the GPU op. If it is LOW (device idle between
  calls) this is **S13 (overhead-bound)** — add concurrency/CPU/batch, NOT GPUs.
  S2 proper is when the GPU op *is* busy but its upstream can't feed it enough.

### S3. One operator dominates wall time
- **Confirming signals:** a single op's `per_op_walltime_share` ≫ others (e.g.
  > 60%).
- **Before recommending more resources:** confirm the dominating op is
  *compute-bound* — if a timeline trace shows its `actor_busy_fraction` is low
  (device/worker idle between calls), it is overhead-bound; treat as **S13**
  (add concurrency/CPU/batch) rather than throwing more CPUs/GPUs at it.
- **Root cause:** that operator is the pipeline bottleneck; everything else waits
  on it.
- **Ranked remediations:**
  1. Raise that op's `concurrency=` (more parallel tasks/actors). → KubeRay:
     ensure worker-group capacity (CPU/GPU pods) can host the added parallelism.
  2. If it is a cheap adjacent op being run separately, allow operator fusion
     (avoid an unnecessary `.materialize()` between it and its neighbor).
  3. If per-task cost is inherent, give it more resources (`num_cpus=`/`num_gpus=`)
     rather than more tasks. → KubeRay: larger pods or a dedicated worker group.
- **Evidence to cite:** the dominating op's wall-time share vs the runner-up.

### S4. Submission backpressure / budget starvation
- **Confirming signals:** `submission_backpressure_share` high;
  `ray_data_object_store_memory_budget` / `ray_data_memory_budget` near 0;
  resource-starvation warning (`Cluster resources are not enough to run any task`).
- **Root cause:** an operator cannot launch new tasks because it is out of
  resource budget (object store / memory / CPU) — the cluster is too small or
  resources are reserved elsewhere.
- **Ranked remediations:**
  1. Add cluster capacity for the starved resource (pods). → KubeRay: raise the
     relevant worker-group `replicas`/`maxReplicas` (and pod requests).
  2. Give Ray Data more of the existing cluster:
     `execution_options.resource_limits`, or remove an over-broad
     `execution_options.exclude_resources`.
  3. If object-store-bound, apply the S1 spilling remedies (smaller blocks / more
     object-store memory).
- **Evidence to cite:** the starvation warning line and the zero budget value.

### S5. Consumer / trainer starvation
- **Confirming signals:** high `output_backpressure_share`; large
  `ray_data_iter_total_blocked_seconds`; large
  `ray_data_iter_time_to_first_batch_seconds` (or the stats "Time to first batch"
  / "Total time user thread blocked").
- **Root cause:** the downstream consumer (e.g. a Ray Train loop) spends its time
  waiting on Ray Data — the pipeline can't produce batches fast enough, or
  prefetch is too shallow.
- **Ranked remediations:**
  1. Increase upstream `concurrency=` on the bottleneck op and cluster capacity.
     → KubeRay: raise worker-group `replicas`/`maxReplicas`.
  2. Increase prefetch depth (`prefetch_batches` on the iterator) so batches are
     ready before the trainer asks.
  3. Move work off the critical path: precompute/cache expensive transforms, or
     `.materialize()` a reusable stage across epochs.
- **Evidence to cite:** time-to-first-batch and total-blocked figures.

### S6. Actor pool won't scale
- **Confirming signals:** the warning `will not allow it to scale up`; or
  `pool_utilization` pinned high with `num_pending_actors` stuck > 0 and pool at
  `max_size`.
- **Root cause:** the actor pool's configured ceiling
  (`max_tasks_in_flight_per_actor / max_concurrency`) can't reach the upscaling
  threshold, or there are no pods to place new actors on.
- **Ranked remediations:**
  1. Raise `max_tasks_in_flight_per_actor` (allow more concurrent tasks per
     actor) so utilization can climb, or lower
     `actor_pool_util_upscaling_threshold`.
  2. Raise the actor pool `max_size` (via `compute=ActorPoolStrategy(max_size=)`
     / `concurrency=`). → KubeRay: raise the worker-group `maxReplicas` so the
     actors have pods (GPU actors need one GPU slot each).
- **Evidence to cite:** the "will not allow it to scale up" line, or pinned
  utilization + pending actors.

### S7. Read over-parallelization
- **Confirming signals:** the warning `is more than 4x the number of available
  CPU slots`; `read_parallelism_ratio` > 4; very many tiny read blocks.
- **Root cause:** too many read tasks/blocks create scheduling overhead that
  dominates the read phase.
- **Ranked remediations:**
  1. Lower `override_num_blocks` (or set it to `-1` to let Ray Data auto-tune).
  2. Raise `read_op_min_num_blocks` only if under-parallelized elsewhere.
  3. Ignore if the cluster is expected to autoscale up substantially (state the
     caveat).
- **Evidence to cite:** the ">4x CPU slots" warning with its two numbers.

### S8. Block-size pathology
- **Confirming signals:** `block_size_profile` far from target — blocks
  >~512MB (OOM/spill risk, ties to S1) or extremely small (task overhead
  dominates, ties to S7); large-block warning
  (`exceeds DataContext.get_current().target_shuffle_max_block_size`).
- **Root cause:** block sizing mismatched to the workload/memory budget.
- **Ranked remediations:**
  1. Too large: lower `target_max_block_size` (and `target_shuffle_max_block_size`
     before shuffles); `.materialize()` before a shuffle to prevent fusing large
     map blocks into it.
  2. Too small: raise `target_max_block_size` / reduce `override_num_blocks` so
     tasks do more work each.
- **Evidence to cite:** the per-block byte profile and any large-block warning.

### S9. Shuffle / join memory pressure
- **Confirming signals:** warnings `Insufficient memory resources in cluster for
  hash shuffle` / `Insufficient CPU resources in cluster for hash shuffle` /
  `hash-shuffle aggregators are ready after` / `exceeds the largest node's memory`
  (per-aggregator memory clamp/spill).
- **Root cause:** hash-shuffle/join aggregators need more memory/CPU than the
  cluster has, so they start slowly or spill.
- **Ranked remediations:**
  1. Reduce `DataContext.max_hash_shuffle_aggregators` or the number of
     partitions to shrink per-aggregator demand.
  2. Increase cluster memory/CPU. → KubeRay: larger worker-group pod
     `memory`/`cpu` requests or higher `maxReplicas`.
  3. Consider `shuffle_strategy` alternatives for the workload.
- **Evidence to cite:** the insufficient-resources / aggregators-not-ready lines.

### S10. Data skew / imbalance
- **Confirming signals:** `tasks_per_node_skew` ≫ 1; imbalanced `*_per_node`
  metrics.
- **Root cause:** work or data is unevenly distributed across nodes (locality
  pinning or skewed keys), so some nodes idle while others are hot.
- **Ranked remediations:**
  1. Increase parallelism / repartition so blocks spread more evenly
     (`override_num_blocks`), or use a shuffle to rebalance skewed keys.
  2. Relax locality if a single node is over-targeted
     (`execution_options.locality_with_output`).
  3. Right-size the pool so hot nodes aren't the only capacity. → KubeRay: even
     worker-group replicas across nodes.
- **Evidence to cite:** the tasks-per-node max vs mean.

### S11. Hang / stall
- **Confirming signals:** idle-detector (`has no outputs for`), hanging-detector
  (`has been running or stuck in scheduling for`), or metadata-fetch timeout
  (`waiting for metadata from operator`).
- **Root cause:** a task is stuck (slow/hung UDF, worker crash, node preemption,
  or resource starvation preventing any task from running).
- **Ranked remediations:**
  1. Inspect the stuck task's stack trace and the node's health (dashboard/logs)
     — the hanging-detector gives task_id/pid/node_id.
  2. If it's resource starvation (see S4), scale the cluster. → KubeRay: raise
     worker-group `maxReplicas`.
  3. If the UDF is legitimately slow, this is benign — tune expectations or the
     hanging-detector z-score. (Confidence caveat.)
- **Evidence to cite:** the idle/hanging lines with their durations and ids.

### S12. Driver memory pressure
- **Confirming signals:** the warning `estimated to use at least <N> of driver
  memory` (usually pull-based shuffle at scale).
- **Root cause:** the driver aggregates too much shuffle metadata/state.
- **Ranked remediations:**
  1. Switch to push-based shuffle (`RAY_DATA_DEFAULT_SHUFFLE_STRATEGY`) to reduce
     driver memory.
  2. Increase head-node memory. → KubeRay: raise the head-group pod `memory`
     request.
- **Evidence to cite:** the driver-memory estimate line.

### S13. GPU/actor stage is latency/overhead-bound (not compute-bound)
- **Confirming signals:** (timeline) a GPU/actor op is the wall-time long pole
  yet its `actor_busy_fraction` is low (<~0.5) and/or `inter_task_gap` is large;
  external GPU utilization (nvidia-smi/DCGM) is low despite `2/2 GPU` scheduled;
  `pool_utilization` moderate but measured throughput far below the model's known
  ceiling. Common with small/cheap models, tiny `batch_size`, per-call CPU
  preprocessing (e.g. tokenization) inside the UDF, or per-call `torch.compile`
  recompilation.
- **Root cause:** the device sits idle between calls — per-call overhead (input
  fetch, CPU preprocessing, small batches, recompilation) dominates, so raw GPU
  compute is NOT the limiter. Reducing actor count makes this WORSE (less
  latency-hiding and less preprocessing parallelism).
- **Ranked remediations:**
  1. Raise in-flight concurrency to hide per-call latency: more actors
     (`concurrency=`) and/or higher `max_concurrency` per actor; keep GPUs
     fractionally shared. → KubeRay: no new GPUs needed — same hardware.
  2. Give each actor enough CPU for its preprocessing: set `num_cpus=` on the GPU
     op so tokenization/decode isn't starved. → KubeRay: ensure pod CPU ≥
     actors × per-actor `num_cpus`.
  3. Amortize per-call overhead: raise `batch_size=` AND any library-internal
     batch (e.g. `SentenceTransformer.encode(batch_size=...)`, whose default
     caps the real device batch) so each call does more work.
  4. Move CPU preprocessing off the GPU actor into its own upstream CPU stage
     (tokenize/decode in a `map`/`map_batches`; feed only tensors to the GPU op).
  5. Avoid per-call recompilation: fixed shapes / `dynamic=True`, or drop
     `torch.compile` for tiny models where its warmup never amortizes.
- **Caution:** do NOT add GPUs while the device is idle — more GPUs cannot raise
  the throughput of an overhead-bound stage. → KubeRay: hold the GPU
  worker-group size; scale CPU / concurrency instead.
- **Evidence to cite:** the GPU op's `actor_busy_fraction`, `inter_task_gap`, and
  measured throughput vs the model's expected ceiling.

## Section 3 — Recommendation catalog (levers)

Draw remediations from these four levers, always by exact knob name:

- **Config** (`DataContext` / `ExecutionOptions`): `target_max_block_size`,
  `target_shuffle_max_block_size`, `override_num_blocks`,
  `read_op_min_num_blocks`, `execution_options.resource_limits`,
  `execution_options.exclude_resources`, `execution_options.locality_with_output`,
  `actor_pool_util_upscaling_threshold`,
  `max_tasks_in_flight_per_actor`,
  `shuffle_strategy`, `RAY_DATA_DEFAULT_SHUFFLE_STRATEGY` (push-based shuffle),
  `max_hash_shuffle_aggregators`, issue-detector configs.
- **Per-op call args**: `concurrency=`, `compute=` (ActorPoolStrategy),
  `num_cpus=`, `num_gpus=`, `memory=`, `batch_size=`,
  library-internal batch size (e.g. `encode(batch_size=)`),
  `prefetch_batches` (iterator prefetch depth), `max_concurrency` (actor),
  operator fusion, `.materialize()` before shuffle.
- **Pipeline shape**: split per-call CPU preprocessing (tokenize/decode) into its
  own upstream `map`/`map_batches` stage so GPU actors do only GPU work; avoid
  per-call `torch.compile` recompilation (fixed shapes / `dynamic=True`, or drop
  compile for tiny models).
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
| Overhead-bound GPU stage (S13) | Do NOT add GPUs — keep the GPU worker group as-is. Raise actor `concurrency`/`num_cpus` within existing pods; enlarge the CPU worker group if preprocessing is split into its own stage |

Autoscaling note: Ray Data actor-pool scaling operates within the pods the
KubeRay autoscaler provides. If the actor pool wants more actors than the
worker group's `maxReplicas` can host, raise `maxReplicas` too — otherwise the
pool is capped regardless of `pool_utilization`.

## Section 5 — Signal-reference appendix

Two purposes: (a) a lookup of every known signal, and (b) a **catch-all rule** so
nothing is silently dropped.

**Catch-all rule:** if the inputs contain a `ray_data_*` metric, a `WARNING`/
`ERROR` line, or a detector message that does NOT match any symptom S1–S13,
surface it under "What I couldn't assess" as an *unclassified signal* with the
verbatim line, and suggest what it might indicate. Never ignore a warning.

### Known metric families (Prometheus `ray_data_*`)
- Overview: `spilled_bytes`, `freed_bytes`, `current_bytes`, `cpu_usage_cores`,
  `gpu_usage_cores`, `output_bytes`, `output_rows`.
- Inputs/Outputs: `num_inputs_received`, `bytes_task_outputs_generated`,
  `rows_task_outputs_generated`, queue block/byte counts.
- Tasks: `num_tasks_{submitted,running,finished,failed}`,
  `task_submission_backpressure_time`, `task_output_backpressure_time`,
  `block_size_bytes` (histogram), `task_completion_time` (histogram).
- Actors: `num_{alive,restarting,pending,active,idle}_actors`,
  `pool_utilization`, `num_tasks_in_flight`.
- Object store: `obj_store_mem_{used,spilled,freed}`,
  internal in/out-queue blocks/bytes.
- Budgets: `cpu_budget`, `gpu_budget`, `memory_budget`,
  `object_store_memory_budget`, `max_bytes_to_read`.
- Iteration: `iter_time_to_first_batch_seconds`, `iter_total_blocked_seconds`,
  `iter_user_seconds`, `iter_*` timers, `iter_blocks_{local,remote}`.
- Per-node: `*_per_node`. Cluster: `cluster_{cpu,gpu,mem,object_store_memory}_utilization`.
- State/metadata: `dataset_state`, `operator_state`, `operator_queued_blocks`,
  `*_estimated_total_{blocks,rows}`.

### Ray timeline trace (Chrome-tracing JSON)
Event fields: `ph` (`X` complete / `M` metadata), `ts`, `dur`, `pid`, `tid`,
`name`, `cat`; a `pid`/`tid` pair is one worker/actor track, named via `M`
`process_name`/`thread_name`. Derived indicators: `actor_busy_fraction`,
`inter_task_gap`, `op_concurrency_over_time` (Section 1). Low `actor_busy_fraction`
+ large `inter_task_gap` on the wall-time long-pole GPU op → **S13**
(overhead-bound); high busy-fraction + full device → genuine compute-bound (S2/S3).

### Known warning strings → symptom
| Warning substring | Symptom |
|---|---|
| `has no outputs for` | S11 |
| `has been running or stuck in scheduling for` | S11 |
| `waiting for metadata from operator` | S11 |
| `of memory per task on average, but Ray only requests` | S1 |
| `Cluster resources are not enough to run any task` | S4 |
| `is more than 4x the number of available CPU slots` | S7 |
| `will not allow it to scale up` | S6 |
| `exceeds DataContext.get_current().target_shuffle_max_block_size` | S8 |
| `hash shuffle` (insufficient CPU/memory) / `aggregators are ready after` | S9 |
| `estimated to use at least` + `driver memory` | S12 |

Anything not in these tables → catch-all rule.
