# Ray Data Tuning Advisor — Design

**Date:** 2026-07-15
**Status:** Approved design, pending implementation plan
**Author:** brainstorming session

## 1. Goal

Build a Claude Code **subagent** that ingests the logs and metrics a Ray Data
job emits, diagnoses execution bottlenecks, and returns **prioritized, concrete
tuning suggestions** — spanning Ray Data configuration, pipeline modifications,
pod sizing, and autoscaling. Recommendations are expressed in **KubeRay-aware**
terms (RayCluster worker-group pod requests, `--num-cpus`, `object_store_memory`,
autoscaler `minReplicas`/`maxReplicas`) because the target environment is KubeRay
on Kubernetes/ACK.

The subagent is backed by a written **diagnostic playbook** it reads on every
run. The playbook is the curated knowledge base; the agent is the thin driver
that applies it to a specific job's telemetry.

## 2. Locked-in decisions

| Decision | Choice |
|---|---|
| Deliverable | Claude Code **subagent** (`.claude/agents/`) + playbook doc |
| Input | **Files the user provides** — `ds.stats()` text, `ray-data.log`, optional Prometheus/Grafana export. No live cluster access required. |
| Workload | **General** — infer workload shape (batch inference / training ingest / ETL) from the operators present in the stats |
| Recommendation scope | Config, pipeline, pod size, autoscaling |
| Recommendation framing | **KubeRay-aware** — Ray knob **and** its pod/worker-group translation |
| Playbook organization | **Symptom-first** (Approach A), with a signal-reference appendix for completeness |

## 3. Architecture

Two deliverable artifacts plus this spec:

- `.claude/agents/ray-data-tuning-advisor.md` — subagent definition. Frontmatter:
  `name`, `description`/when-to-use, `tools: Read, Grep, Glob, Bash`. System
  prompt encodes the single-pass flow, how to read the playbook, and the output
  report format.
- `.claude/agents/ray-data-tuning-playbook.md` — the diagnostic knowledge base:
  derived indicators, symptom taxonomy, recommendation catalog, KubeRay
  translation table, and the signal-reference appendix.
- `docs/superpowers/specs/2026-07-15-ray-data-tuning-advisor-design.md` — this spec.

Keeping the playbook separate from the agent prompt lets the knowledge base grow
without bloating the agent's context, and lets the playbook be updated as Ray
versions change without touching the agent's control flow.

### Agent flow (single pass)

```
Inputs (files) → Ingest & Parse → Compute Derived Indicators
   → Match Symptoms (playbook) → Rank & de-dupe fixes
   → Emit prioritized report (evidence + Ray knob + KubeRay translation)
```

## 4. Inputs

The agent works with any subset of the following and explicitly reports what was
missing and what to collect next time. It never fabricates a metric it did not
observe.

- **`ds.stats()` text** — per-operator wall/cpu/UDF time, block counts, rows &
  bytes per block and per task, tasks-per-node, throughput (rows/s),
  spilled/restored MB, and the iterator time breakdown.
- **`ray-data.log` (or console output)** — execution plan/DAG, periodic topology
  dumps (`format_op_state_summary`: tasks, backpressure state+policy, actor
  counts, queued blocks+bytes, per-op resource usage), and the diagnostic
  warnings (idle detector, hanging detector, high-memory detector,
  resource-starvation, large-block, hash-shuffle health, driver-memory,
  read over-parallelism).
- **Prometheus/Grafana export (optional, CSV/JSON)** — `ray_data_*` time series:
  `spilled_bytes`, `freed_bytes`, `current_bytes`, `cpu_usage_cores`,
  `gpu_usage_cores`, `pool_utilization`, `num_{alive,pending,idle,active}_actors`,
  `task_submission_backpressure_time`, `task_output_backpressure_time`,
  `{cpu,gpu,memory,object_store_memory}_budget`, `*_per_node`, and
  `data_cluster_*_utilization`.
- **Optional user-typed context** — current RayCluster spec / pod sizes /
  worker-group setup, and the optimization goal (max throughput vs min cost vs
  fix a hang).

### Parsing approach

The playbook records the anchor strings and section markers to look for (e.g.
`"Spilled to disk"`, `"UDF time"`, `"Remote wall time"`, warning message
prefixes, `format_op_state_summary` field names). The agent extracts them via
Read/Grep rather than a rigid parser, so format drift across Ray versions
degrades gracefully instead of breaking.

## 5. Derived indicators

Raw metrics do not diagnose; ratios do. The agent computes a fixed set from
whatever inputs exist:

| Indicator | Derived from | Signal |
|---|---|---|
| Spill ratio | `spilled_bytes` / dataset output bytes; `"Spilled to disk"` | object-store pressure / OOM risk |
| GPU-busy fraction | `gpu_usage_cores` vs allocated GPUs; UDF-time share | GPU starved vs saturated |
| Per-op wall-time share | `runtime_metrics()` % per op | which operator is the bottleneck |
| Submission-backpressure share | `task_submission_backpressure_time` / wall time | downstream can't keep up / budget-starved |
| Output-backpressure share | `task_output_backpressure_time` | consumer (Train/write) too slow |
| Pool utilization & headroom | `pool_utilization`, `num_pending/idle/active_actors`, min/max size | actor pool over/under-provisioned or can't scale |
| Block-size distribution | `block_size_bytes` histogram; rows/bytes per block | blocks too big (OOM) or too small (task overhead) |
| Read-parallelism ratio | read blocks vs cluster CPU slots | over-parallelized reads |
| Tasks-per-node skew | `tasks_per_node` / `*_per_node` metrics | data-locality / imbalance |
| Time-to-first-batch & iter-blocked | `iter_*` timers | trainer starvation |

## 6. Symptom taxonomy (the diagnostic heart)

The playbook is organized as one entry per symptom. Each entry has a fixed shape:

> **Confirming signals** → **Root cause** → **Ranked remediations**
> (config → pipeline → pod size → autoscaling) → **Evidence to cite**

Initial symptom set:

1. **Object-store spilling / OOM** — high spill ratio, high-memory detector
   fired, large-block warning.
2. **GPU underutilized** — GPU-busy low while GPU pods allocated; usually
   CPU-bound preprocessing upstream, or `batch_size`/`concurrency` too low.
3. **One operator dominates wall time** — a single op ≫ others; candidate for
   more concurrency, fusion, or a resource bump.
4. **Submission backpressure / budget starvation** — high submission-backpressure
   share, `data_*_budget` near zero, resource-starvation warning ("job may hang").
5. **Consumer/trainer starvation** — high output-backpressure or
   `iter_total_blocked_seconds`; long time-to-first-batch.
6. **Actor pool won't scale** — the explicit "configuration will not allow it to
   scale up" warning, or pending actors stuck / utilization pinned.
7. **Read over-parallelization** — the ">4x CPU slots" warning; too many tiny blocks.
8. **Block-size pathology** — blocks too large (OOM/spill) or too small (task
   overhead dominates).
9. **Shuffle/join memory pressure** — hash-shuffle health warning, per-aggregator
   memory clamp, insufficient CPU/memory for aggregators.
10. **Data skew / imbalance** — tasks-per-node skew, per-node metric imbalance.
11. **Hang / stall** — idle detector, hanging detector (task stuck >
    mean+z·stddev), metadata-fetch timeout.
12. **Driver memory pressure** — driver-memory warning (usually pull-based
    shuffle at scale).

A **signal-reference appendix** lists every `ray_data_*` metric and warning
string so that if a signal appears that no symptom matched, the agent still
surfaces it rather than dropping it.

## 7. Recommendation catalog & KubeRay translation

Each symptom maps to ranked fixes drawn from four levers; the playbook stores the
exact knob name so the agent never hand-waves.

- **Ray config knobs** (`DataContext` / `ExecutionOptions`): `target_max_block_size`,
  `target_shuffle_max_block_size`, `override_num_blocks` /
  `read_op_min_num_blocks`, `execution_options.resource_limits` &
  `exclude_resources`, `actor_pool_util_upscaling_threshold`, shuffle strategy,
  `max_hash_shuffle_aggregators`, issue-detector configs.
- **Per-op call knobs**: `concurrency=`, `compute=`, `num_cpus=`, `num_gpus=`,
  `memory=`, `batch_size=`, fusion hints, `.materialize()` before shuffle.
- **Pod size**: worker-group CPU/memory/GPU requests, `--num-cpus`,
  `object_store_memory` (the spill lever), head-node memory (driver-memory symptom).
- **Autoscaling**: RayCluster autoscaler `minReplicas`/`maxReplicas` per worker
  group, actor-pool min/max size, and the interaction between the two.

**KubeRay translation rule:** every recommendation is emitted as a pair — the
Ray-level knob **and** its KubeRay/pod expression. Example — spill symptom →
"raise object-store memory": Ray view = larger `object_store_memory`; KubeRay
view = bump the worker-group pod `memory` request and set
`rayStartParams.object-store-memory`, or add a worker group / raise
`maxReplicas`. The playbook carries a translation table so this is consistent.

**Guardrails:**
- Recommendations ranked by expected impact × confidence.
- Each carries an **evidence line** (the metric value / log quote that triggered it).
- Ambiguous signals carry a **caveat** (e.g. "idle op may just be a slow UDF —
  confirm before scaling").
- The agent states what it could **not** assess given missing inputs.

## 8. Output report format

```
1. Executive summary — inferred workload shape, top 1–3 bottlenecks
2. Findings (ranked) — each: Symptom · Evidence · Root cause
      · Recommendations [Ray knob → KubeRay translation] · Confidence
3. Quick-win config block — copy-pasteable DataContext/ExecutionOptions + per-op args
4. Pod & autoscaling plan — worker-group sizing / min-maxReplicas table
      (only if cluster context was provided)
5. What I couldn't assess — missing inputs + which metric/log to collect next run
```

## 9. File layout

- `.claude/agents/ray-data-tuning-advisor.md` — agent definition.
- `.claude/agents/ray-data-tuning-playbook.md` — knowledge base.
- `docs/superpowers/specs/2026-07-15-ray-data-tuning-advisor-design.md` — this spec.

## 10. Out of scope (YAGNI)

- Live cluster / Prometheus scraping (files-only for v1).
- A deterministic Python rule engine (the LLM subagent handles messy input).
- Automatically applying changes to a RayCluster (advisory only).
- Multi-stage / multi-module agent orchestration (single pass is sufficient;
  most jobs have one or two dominant bottlenecks).

## 11. Reference — telemetry source map

For the implementer, the signals in this spec come from:

- `python/ray/data/_internal/execution/interfaces/op_runtime_metrics.py` —
  `OpRuntimeMetrics` fields (INPUTS/OUTPUTS/TASKS/ACTORS/OBJECT_STORE_MEMORY/MISC).
- `python/ray/data/_internal/stats.py` — `_StatsActor` Prometheus `ray_data_*`
  gauges, `DatasetStatsSummary` / `ds.stats()` rendering, iteration metrics.
- `python/ray/data/_internal/execution/resource_manager.py` — budgets,
  `ReservationOpResourceAllocator`, resource-starvation warning.
- `python/ray/data/_internal/execution/backpressure_policy/` — backpressure
  policies and their signals.
- `python/ray/data/_internal/actor_autoscaler/` — actor-pool autoscaling signals
  and the "won't scale up" warning.
- `python/ray/data/_internal/issue_detection/` — hanging / high-memory /
  hash-shuffle health detectors.
- `python/ray/data/_internal/logging.py` — `ray-data.log` location and levels.
- `python/ray/data/_internal/execution/streaming_executor{,_state}.py` — periodic
  topology dumps, idle detector, `format_op_state_summary`.
