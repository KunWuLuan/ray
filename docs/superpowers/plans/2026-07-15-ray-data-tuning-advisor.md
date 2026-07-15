# Ray Data Tuning Advisor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Claude Code subagent that reads a Ray Data job's telemetry files and returns prioritized, KubeRay-aware tuning recommendations.

**Architecture:** A subagent definition drives a single-pass flow (ingest → derive indicators → match symptoms → rank fixes → report). A separate playbook file holds the diagnostic knowledge base (indicators, symptom taxonomy, recommendation catalog, KubeRay translation table, signal appendix). "Tests" are golden fixtures: synthetic telemetry files exhibiting a known bottleneck, against which the subagent is dispatched and its output checked for the expected diagnosis and recommendation.

**Tech Stack:** Markdown (Claude Code subagent + playbook), Claude Code Agent tool for dispatch-based verification. No compiled code, no Python runtime.

## Global Constraints

- Repo agent convention: `.claude/agents/<name>/<name>.md` (folder per agent — see `.claude/agents/README.md`). Keep playbook and fixtures in the same folder.
- **Files-only input** — never assume live cluster / Prometheus access.
- **Never fabricate a metric** the input did not contain; explicitly report what was missing.
- **Advisory only** — never instruct applying changes to a live RayCluster automatically.
- **KubeRay-aware pairing** — every recommendation states the Ray-level knob AND its KubeRay/pod translation.
- **Symptom-first** playbook organization; single-pass agent (no multi-module orchestration).
- **Version-agnostic parsing** — locate signals by anchor strings via Read/Grep; degrade gracefully if a string is absent rather than erroring.
- Deliverable file paths (locked):
  - Agent: `.claude/agents/ray-data-tuning-advisor/ray-data-tuning-advisor.md`
  - Playbook: `.claude/agents/ray-data-tuning-advisor/playbook.md`
  - Fixtures: `.claude/agents/ray-data-tuning-advisor/fixtures/<case>/` (each holds `stats.txt`, `ray-data.log`, and/or `metrics.csv` plus an `EXPECT.md` describing the required output)

---

## File Structure

- `.claude/agents/ray-data-tuning-advisor/ray-data-tuning-advisor.md` — subagent definition: frontmatter (`name`, `description`, `tools`) + system prompt (flow, playbook-read instruction, output contract).
- `.claude/agents/ray-data-tuning-advisor/playbook.md` — knowledge base. Sections: (0) Parsing anchor map, (1) Derived indicators, (2) Symptom taxonomy, (3) Recommendation catalog, (4) KubeRay translation table, (5) Signal-reference appendix.
- `.claude/agents/ray-data-tuning-advisor/fixtures/<case>/` — one folder per test case; synthetic telemetry + `EXPECT.md`.
- `.claude/agents/README.md` — modify to list the new agent.

Each task adds capability to the playbook plus a fixture that proves it, ending in a dispatch-and-verify cycle a reviewer can gate.

---

### Task 1: Agent scaffold + output contract + smoke fixture

**Files:**
- Create: `.claude/agents/ray-data-tuning-advisor/ray-data-tuning-advisor.md`
- Create: `.claude/agents/ray-data-tuning-advisor/playbook.md` (skeleton)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/smoke/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/smoke/EXPECT.md`

**Interfaces:**
- Produces: the agent name `ray-data-tuning-advisor` (dispatched via the Agent tool), and the output contract (5-section report) that all later fixtures assert against.

- [ ] **Step 1: Write the failing test (smoke fixture + expectation)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/smoke/stats.txt`:

```
Operator 1 ReadRange->Map(f): 1 tasks executed, 1 blocks produced in 0.5s
* Remote wall time: 0.5s min, 0.5s max, 0.5s mean, 0.5s total
* Remote cpu time: 0.4s min, 0.4s max, 0.4s mean, 0.4s total
* Output num rows per block: 1000 min, 1000 max, 1000 mean, 1000 total
Dataset throughput:
* Ray Data throughput: 2000 rows/s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/smoke/EXPECT.md`:

```markdown
# Expected output for `smoke`

The subagent output MUST:
- Contain all 5 report sections: Executive summary, Findings, Quick-win config, Pod & autoscaling plan, What I couldn't assess.
- Under "What I couldn't assess", note that no ray-data.log and no Prometheus export were provided.
- NOT invent any metric absent from stats.txt (e.g. no GPU numbers, no spill figures).
- Report no severe bottleneck (this is a trivial healthy job).
```

- [ ] **Step 2: Run the test to verify it fails**

Dispatch the subagent (it does not exist yet):

Run: use the Agent tool with `subagent_type: ray-data-tuning-advisor`, prompt: `Analyze the Ray Data telemetry in .claude/agents/ray-data-tuning-advisor/fixtures/smoke/ and produce tuning recommendations.`
Expected: FAIL — agent type `ray-data-tuning-advisor` is not found / not registered.

- [ ] **Step 3: Write the agent definition**

Create `.claude/agents/ray-data-tuning-advisor/ray-data-tuning-advisor.md`:

````markdown
---
name: ray-data-tuning-advisor
description: Diagnoses Ray Data execution bottlenecks from provided telemetry files (ds.stats() output, ray-data.log, optional Prometheus/Grafana export) and returns prioritized, KubeRay-aware tuning recommendations covering config, pipeline, pod sizing, and autoscaling. Use when a user shares Ray Data job logs/metrics and asks how to speed it up, cut spilling/OOM, improve GPU utilization, or fix a hang.
tools: Read, Grep, Glob, Bash
---

You are the Ray Data Tuning Advisor. You analyze telemetry that a Ray Data job
emitted and return concrete, ranked tuning recommendations. You are advisory
only: you never apply changes to a live cluster.

## Inputs

The user gives you file paths (or a directory). Accept any subset of:
- `ds.stats()` text (per-operator wall/cpu/UDF time, block/row/byte stats,
  tasks-per-node, throughput, spilled/restored MB, iterator breakdown).
- `ray-data.log` or console output (execution plan, periodic topology dumps,
  and warning lines).
- A Prometheus/Grafana export (CSV/JSON) of `ray_data_*` series.
- Optional user context: current RayCluster / worker-group / pod sizes, and the
  optimization goal (max throughput / min cost / fix a hang).

Read every file you are given. If a file type is missing, proceed with what you
have and record the gap in the final section. **Never fabricate a metric you did
not read in the inputs.**

## Method (single pass)

1. Read `.claude/agents/ray-data-tuning-advisor/playbook.md` in full. It is your
   knowledge base: parsing anchors, derived indicators, symptom taxonomy,
   recommendation catalog, KubeRay translation table, and signal appendix.
2. Ingest the provided files using the playbook's parsing anchor map (Grep/Read).
3. Compute the derived indicators from the playbook (Section 1) for whatever data
   you have.
4. Match the computed indicators and any warning lines against the symptom
   taxonomy (Section 2). A symptom fires only when its confirming signals are
   present.
5. For each fired symptom, pull its ranked remediations (Section 3) and pair each
   with its KubeRay translation (Section 4).
6. Rank findings by (expected impact × confidence). De-duplicate remediations
   that multiple symptoms suggest.
7. Emit the report in the exact format below.

## Output contract (always these 5 sections)

```
## 1. Executive summary
- Inferred workload shape (batch inference / training ingest / ETL / mixed) and why.
- Top 1–3 bottlenecks in one line each.

## 2. Findings (ranked)
For each finding:
- **Symptom:** <name from taxonomy>
- **Evidence:** <exact metric value or quoted log line from the inputs>
- **Root cause:** <one or two sentences>
- **Recommendations:** ranked list, each as `Ray knob → KubeRay translation`
- **Confidence:** high / medium / low (+ caveat if the signal is ambiguous)

## 3. Quick-win config block
Copy-pasteable DataContext / ExecutionOptions settings and per-op call args.

## 4. Pod & autoscaling plan
Worker-group sizing and autoscaler minReplicas/maxReplicas table.
Include ONLY if the user provided current cluster/pod context; otherwise state
"Not provided — supply the RayCluster spec for pod/autoscaling recommendations."

## 5. What I couldn't assess
Which inputs were missing and which metric/log to collect next run.
```

Keep recommendations grounded in the evidence you cite. When a signal is
ambiguous (e.g. an idle operator that may simply be a slow UDF), say so in the
confidence caveat rather than over-recommending.
````

- [ ] **Step 4: Write the playbook skeleton**

Create `.claude/agents/ray-data-tuning-advisor/playbook.md`:

````markdown
# Ray Data Tuning Playbook

The Ray Data Tuning Advisor reads this file on every run. Sections are filled in
across implementation tasks.

## Section 0 — Parsing anchor map
(How to locate each signal in the input files. Filled in Task 2.)

## Section 1 — Derived indicators
(Ratios computed from raw metrics. Filled in Task 2.)

## Section 2 — Symptom taxonomy
(One entry per symptom. Filled in Tasks 3–7.)

## Section 3 — Recommendation catalog
(Exact knob names per lever. Filled in Tasks 3–7.)

## Section 4 — KubeRay translation table
(Ray knob → pod/worker-group/autoscaler expression. Filled in Task 3.)

## Section 5 — Signal-reference appendix
(Every ray_data_* metric + warning string. Filled in Task 8.)
````

- [ ] **Step 5: Run the test to verify it passes**

Reload agents if needed (in Claude Code: `/reload-plugins` or restart), then
dispatch:

Run: Agent tool, `subagent_type: ray-data-tuning-advisor`, prompt: `Analyze the Ray Data telemetry in .claude/agents/ray-data-tuning-advisor/fixtures/smoke/ and produce tuning recommendations.`
Expected: PASS — output contains all 5 sections; "What I couldn't assess" notes the missing ray-data.log and Prometheus export; no GPU/spill numbers are invented; no severe bottleneck claimed. Check the output against `fixtures/smoke/EXPECT.md`.

- [ ] **Step 6: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): scaffold ray-data-tuning-advisor subagent + output contract"
```

---

### Task 2: Parsing anchor map + derived indicators

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Sections 0 and 1)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/EXPECT.md`

**Interfaces:**
- Consumes: the agent + playbook skeleton from Task 1.
- Produces: named indicators (spill_ratio, gpu_busy_fraction, per_op_walltime_share, submission_backpressure_share, output_backpressure_share, pool_utilization, block_size_profile, read_parallelism_ratio, tasks_per_node_skew, time_to_first_batch) referenced by all symptom entries in Tasks 3–7.

- [ ] **Step 1: Write the failing test (indicators fixture)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/stats.txt`:

```
Operator 1 ReadParquet: 100 tasks executed, 100 blocks produced in 20s
* Remote wall time: 0.1s min, 0.5s max, 0.2s mean, 20s total
* Output size bytes per block: 100MB min, 100MB max, 100MB mean, 10GB total
* Tasks per node: 90 min, 10 max, 50 mean, 2 nodes used
Operator 2 MapBatches(preprocess): 100 tasks executed, 100 blocks produced in 80s
* Remote wall time: 0.5s min, 1.5s max, 0.8s mean, 80s total
Cluster memory:
* Spilled to disk: 4096MB
* Restored from disk: 4096MB
Dataset throughput:
* Ray Data throughput: 5000 rows/s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/ray-data.log`:

```
2026-07-15 10:00:00	INFO streaming_executor.py:190 -- Starting execution of Dataset dataset_1.
2026-07-15 10:00:20	DEBUG streaming_executor.py:700 -- Execution Progress:
0: ReadParquet - Tasks: 8; Queued blocks: 40 (4.0GB); Resources: 8.0 CPU
1: MapBatches(preprocess) - Tasks: 16; Queued blocks: 2 (0.2GB); Resources: 16.0 CPU
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/indicators/EXPECT.md`:

```markdown
# Expected output for `indicators`

The subagent output MUST:
- Report a non-trivial spill (spill_ratio > 0; "Spilled to disk: 4096MB" cited).
- Identify MapBatches(preprocess) as the dominant operator by wall-time share
  (80s of 100s total ≈ 80%).
- Note tasks-per-node skew on ReadParquet (90 vs 10 across 2 nodes).
- Cite exact numbers from the files; invent nothing.
```

- [ ] **Step 2: Run the test to verify it fails**

Run: dispatch the agent on `fixtures/indicators/`.
Expected: FAIL — with only the skeleton, the agent has no indicator definitions, so it does not compute spill_ratio / wall-time share / node skew consistently.

- [ ] **Step 3: Fill in Section 0 (parsing anchor map)**

Replace the "## Section 0" block in `playbook.md` with:

````markdown
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
````

- [ ] **Step 4: Fill in Section 1 (derived indicators)**

Replace the "## Section 1" block in `playbook.md` with:

````markdown
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
| `tasks_per_node_skew` | `Tasks per node` max ÷ mean (or `*_per_node` metrics) | >>1 indicates imbalance/locality issues |
| `time_to_first_batch` / `iter_blocked` | `ray_data_iter_time_to_first_batch_seconds` / `iter_total_blocked_seconds` | large = consumer (trainer) starvation |

Report indicators you could not compute as "unavailable (missing <input>)".
````

- [ ] **Step 5: Run the test to verify it passes**

Run: dispatch the agent on `fixtures/indicators/`.
Expected: PASS — output cites `Spilled to disk: 4096MB`, computes a spill_ratio, names MapBatches(preprocess) as ~80% wall-time-share bottleneck, and flags the 90/10 tasks-per-node skew. Verify against `EXPECT.md`.

- [ ] **Step 6: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add parsing anchor map + derived indicators to playbook"
```

---

### Task 3: Symptom — object-store spilling/OOM + recommendation catalog + KubeRay table

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Sections 2, 3, 4)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/EXPECT.md`

**Interfaces:**
- Consumes: indicators from Task 2 (`spill_ratio`, `block_size_profile`).
- Produces: Section 4 KubeRay translation table (referenced by all later symptoms); the symptom-entry template used by Tasks 4–7.

- [ ] **Step 1: Write the failing test (spilling fixture)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/stats.txt`:

```
Operator 1 ReadImages->MapBatches(embed): 200 tasks executed, 200 blocks produced in 120s
* Remote wall time: 0.4s min, 2.0s max, 0.6s mean, 120s total
* Output size bytes per block: 600MB min, 900MB max, 700MB mean, 140GB total
Cluster memory:
* Spilled to disk: 81920MB
* Restored from disk: 81920MB
Dataset memory:
* Spilled to disk: 81920MB
Dataset throughput:
* Ray Data throughput: 1500 rows/s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/ray-data.log`:

```
2026-07-15 11:00:05	WARNING high_memory_detector.py:25 -- Operator 'MapBatches(embed)' uses 3.2GiB of memory per task on average, but Ray only requests 0.5GiB per task at the start of the pipeline. To avoid out-of-memory errors, consider setting `memory=3.2GiB` in the appropriate function or method call.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/spilling/EXPECT.md`:

```markdown
# Expected output for `spilling`

The subagent output MUST:
- Fire the **Object-store spilling / OOM** symptom as a top finding.
- Cite BOTH the `Spilled to disk: 81920MB` figure AND the high-memory-detector
  warning as evidence.
- Recommend, in ranked order: reduce `target_max_block_size` and/or set
  `memory=` on the map op (config/pipeline), and raise object-store memory.
- Pair the object-store-memory recommendation with its KubeRay translation
  (bump worker-group pod `memory` request + `rayStartParams.object-store-memory`,
  or add a worker group / raise maxReplicas).
- Note the ~700MB/block profile as contributing.
```

- [ ] **Step 2: Run the test to verify it fails**

Run: dispatch the agent on `fixtures/spilling/`.
Expected: FAIL — no symptom taxonomy yet, so the agent may note spilling but does not produce the ranked remediations with the KubeRay object-store-memory translation.

- [ ] **Step 3: Fill in Section 4 (KubeRay translation table)**

Replace the "## Section 4" block in `playbook.md` with:

````markdown
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
````

- [ ] **Step 4: Fill in Section 3 header + Section 2 template and first entry**

Replace the "## Section 3" block with:

````markdown
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
````

Replace the "## Section 2" block with the template + first entry:

````markdown
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
````

- [ ] **Step 5: Run the test to verify it passes**

Run: dispatch the agent on `fixtures/spilling/`.
Expected: PASS — S1 fires as top finding; both the 81920MB spill and the
high-memory warning are cited; ranked remediations include reducing
`target_max_block_size`, setting `memory=`, and raising object-store memory with
the KubeRay pod-memory + `rayStartParams.object-store-memory` translation. Verify
against `EXPECT.md`.

- [ ] **Step 6: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add spilling/OOM symptom, recommendation catalog, KubeRay table"
```

---

### Task 4: Symptoms — GPU underutilized + operator-dominates-walltime

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Section 2: S2, S3)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/metrics.csv`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/EXPECT.md`

**Interfaces:**
- Consumes: `gpu_busy_fraction`, `per_op_walltime_share` (Task 2).
- Produces: S2, S3 entries used by the integration fixture in Task 8.

- [ ] **Step 1: Write the failing test (gpu-underutilized fixture)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/stats.txt`:

```
Operator 1 ReadImages: 500 tasks executed, 500 blocks produced in 300s
* Remote wall time: 0.2s min, 1.0s max, 0.5s mean, 250s total
Operator 2 MapBatches(decode_resize): 500 tasks executed, 500 blocks produced in 300s
* Remote wall time: 0.3s min, 1.2s max, 0.6s mean, 300s total
Operator 3 MapBatches(gpu_infer): 500 tasks executed, 500 blocks produced in 60s
* Remote wall time: 0.05s min, 0.2s max, 0.1s mean, 50s total
Dataset throughput:
* Ray Data throughput: 800 rows/s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/metrics.csv`:

```
metric,operator,value
ray_data_gpu_usage_cores,MapBatches(gpu_infer),0.3
ray_data_cpu_usage_cores,MapBatches(decode_resize),32
ray_data_pool_utilization,MapBatches(gpu_infer),0.25
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/gpu-underutilized/EXPECT.md`:

```markdown
# Expected output for `gpu-underutilized`

The subagent output MUST:
- Fire **GPU underutilized** (S2): gpu_busy_fraction low (0.3 GPU used,
  pool_utilization 0.25) while a GPU op exists.
- Fire **One operator dominates wall time** (S3): the CPU preprocessing op
  MapBatches(decode_resize) dominates (~300s), starving the GPU op downstream.
- Root-cause the two together: GPU is starved by upstream CPU preprocessing.
- Recommend increasing CPU preprocessing parallelism/pods, and raising
  `batch_size`/`concurrency` on the GPU op; pair with KubeRay CPU worker-group
  scaling.
- Infer workload shape = batch inference (GPU).
```

- [ ] **Step 2: Run the test to verify it fails**

Run: dispatch the agent on `fixtures/gpu-underutilized/`.
Expected: FAIL — S2/S3 entries do not exist yet; the agent notes low GPU usage but does not produce the starvation root-cause + ranked CPU-scaling remediation.

- [ ] **Step 3: Add S2 and S3 to Section 2**

Append to the "## Section 2" block in `playbook.md`:

````markdown
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

### S3. One operator dominates wall time
- **Confirming signals:** a single op's `per_op_walltime_share` ≫ others (e.g.
  > 60%).
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
````

- [ ] **Step 4: Run the test to verify it passes**

Run: dispatch the agent on `fixtures/gpu-underutilized/`.
Expected: PASS — S2 and S3 both fire; the report root-causes GPU starvation to
upstream CPU preprocessing, recommends CPU scaling + `batch_size`/`concurrency`
with KubeRay CPU worker-group translation, and infers batch-inference workload.
Verify against `EXPECT.md`.

- [ ] **Step 5: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add GPU-underutilized and operator-bottleneck symptoms"
```

---

### Task 5: Symptoms — submission-backpressure/starvation + consumer/trainer starvation

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Section 2: S4, S5)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/metrics.csv`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/EXPECT.md`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/metrics.csv`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/EXPECT.md`

**Interfaces:**
- Consumes: `submission_backpressure_share`, `output_backpressure_share`,
  `time_to_first_batch`/`iter_blocked` (Task 2).
- Produces: S4, S5 entries.

- [ ] **Step 1: Write the failing tests (two fixtures)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/ray-data.log`:

```
2026-07-15 12:00:30	WARNING resource_manager.py:770 -- Cluster resources are not enough to run any task from MapBatches(embed). The job may hang forever unless the cluster scales up.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/metrics.csv`:

```
metric,operator,value
ray_data_task_submission_backpressure_time,MapBatches(embed),140
ray_data_object_store_memory_budget,MapBatches(embed),0
ray_data_memory_budget,MapBatches(embed),0
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/backpressure/EXPECT.md`:

```markdown
# Expected output for `backpressure`

The subagent output MUST:
- Fire **Submission backpressure / budget starvation** (S4).
- Cite the resource-starvation warning ("The job may hang forever unless the
  cluster scales up") AND the zero object-store/memory budget.
- Recommend scaling the cluster (add pods / raise maxReplicas) and/or raising
  resource_limits / relaxing exclude_resources; pair with KubeRay worker-group
  scaling.
- Confidence high (explicit starvation warning).
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/stats.txt`:

```
Dataset iterator time breakdown:
* Time to first batch: 45.0s
* Total time user thread blocked: 210.0s
* Total execution time: 300.0s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/metrics.csv`:

```
metric,operator,value
ray_data_task_output_backpressure_time,MapBatches(preprocess),5
ray_data_iter_total_blocked_seconds,,210
ray_data_iter_time_to_first_batch_seconds,,45
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/trainer-starvation/EXPECT.md`:

```markdown
# Expected output for `trainer-starvation`

The subagent output MUST:
- Fire **Consumer/trainer starvation** (S5): iter_total_blocked (210s of 300s)
  and time_to_first_batch (45s) are large.
- Root-cause: the training loop waits on Ray Data; the pipeline can't produce
  batches fast enough.
- Recommend raising upstream `concurrency=`/prefetch and cluster capacity; pair
  with KubeRay worker-group scaling.
- Infer workload shape = training ingest.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: dispatch the agent on `fixtures/backpressure/` and on `fixtures/trainer-starvation/`.
Expected: FAIL — S4/S5 entries not present; agent cannot produce the specific starvation diagnoses/remediations.

- [ ] **Step 3: Add S4 and S5 to Section 2**

Append to the "## Section 2" block:

````markdown
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
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: dispatch on `fixtures/backpressure/` → S4 fires with the starvation warning + zero budget cited and KubeRay scaling recommendation.
Run: dispatch on `fixtures/trainer-starvation/` → S5 fires with the iter-blocked/time-to-first-batch evidence and prefetch/concurrency recommendation; workload inferred as training ingest.
Expected: PASS both. Verify against each `EXPECT.md`.

- [ ] **Step 5: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add budget-starvation and consumer-starvation symptoms"
```

---

### Task 6: Symptoms — actor pool won't scale + read over-parallelization + block-size pathology

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Section 2: S6, S7, S8)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/actor-scale/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/actor-scale/EXPECT.md`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/read-parallelism/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/read-parallelism/EXPECT.md`

**Interfaces:**
- Consumes: `pool_utilization`, `read_parallelism_ratio`, `block_size_profile` (Task 2).
- Produces: S6, S7, S8 entries.

- [ ] **Step 1: Write the failing tests**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/actor-scale/ray-data.log`:

```
2026-07-15 13:00:10	WARNING default_actor_autoscaler.py:233 -- ⚠️  Actor Pool configuration of the MapBatches(gpu_infer) will not allow it to scale up: configured utilization threshold (80%) couldn't be reached with configured max_concurrency=1 and max_tasks_in_flight_per_actor=1 (max utilization will be max_tasks_in_flight_per_actor / max_concurrency = 100%).
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/actor-scale/EXPECT.md`:

```markdown
# Expected output for `actor-scale`

The subagent output MUST:
- Fire **Actor pool won't scale** (S6), citing the "will not allow it to scale
  up" warning verbatim.
- Explain the max_tasks_in_flight_per_actor / max_concurrency ceiling.
- Recommend raising max_tasks_in_flight_per_actor and/or lowering the upscaling
  threshold, and confirming worker-group maxReplicas can host more actors.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/read-parallelism/ray-data.log`:

```
2026-07-15 13:05:00	WARNING set_read_parallelism.py:66 -- ⚠️  The requested number of read blocks of 4000 is more than 4x the number of available CPU slots in the cluster of 200. This can lead to slowdowns during the data reading phase due to excessive task creation. Reduce the value or set override_num_blocks to -1.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/read-parallelism/EXPECT.md`:

```markdown
# Expected output for `read-parallelism`

The subagent output MUST:
- Fire **Read over-parallelization** (S7), citing the ">4x CPU slots" warning.
- Recommend lowering `override_num_blocks` (or setting it to -1) / raising
  `read_op_min_num_blocks` appropriately.
- Note that many tiny blocks add task overhead.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: dispatch on `fixtures/actor-scale/` and `fixtures/read-parallelism/`.
Expected: FAIL — S6/S7/S8 not present.

- [ ] **Step 3: Add S6, S7, S8 to Section 2**

Append to the "## Section 2" block:

````markdown
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
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: dispatch on `fixtures/actor-scale/` → S6 fires with the warning cited and the max_tasks_in_flight/threshold + maxReplicas remediation.
Run: dispatch on `fixtures/read-parallelism/` → S7 fires with the override_num_blocks remediation.
Expected: PASS both. Verify against each `EXPECT.md`.

- [ ] **Step 5: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add actor-scaling, read-parallelism, block-size symptoms"
```

---

### Task 7: Symptoms — shuffle/join memory + data skew + hang/stall + driver memory

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Section 2: S9, S10, S11, S12)
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/shuffle-memory/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/shuffle-memory/EXPECT.md`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/hang/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/hang/EXPECT.md`

**Interfaces:**
- Consumes: `tasks_per_node_skew` (Task 2) and warning anchors (Task 2 Section 0).
- Produces: S9–S12 entries.

- [ ] **Step 1: Write the failing tests**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/shuffle-memory/ray-data.log`:

```
2026-07-15 14:00:00	WARNING hash_shuffle.py:1532 -- Insufficient memory resources in cluster for hash shuffle operation. Required: 400 GiB for 64 aggregators, but cluster only has 256 GiB total memory. Consider reducing the number of partitions or increasing cluster size.
2026-07-15 14:00:01	WARNING hash_shuffle_detector.py:128 -- Only 40 out of 64 hash-shuffle aggregators are ready after 120.0 secs. This might indicate resource contention for cluster resources.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/shuffle-memory/EXPECT.md`:

```markdown
# Expected output for `shuffle-memory`

The subagent output MUST:
- Fire **Shuffle/join memory pressure** (S9), citing the insufficient-memory and
  aggregators-not-ready warnings.
- Recommend reducing `max_hash_shuffle_aggregators` / partitions OR increasing
  cluster memory; pair with KubeRay pod-memory / worker-group scaling.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/hang/ray-data.log`:

```
2026-07-15 15:00:00	WARNING streaming_executor_state.py:107 -- Operator MapBatches(embed) is running but has no outputs for 90 seconds. Execution may be slower than expected.
2026-07-15 15:00:30	WARNING hanging_detector.py:130 -- A task (task_id=abc123) of operator MapBatches(embed) (pid=42, node_id=n1, attempt=0) has been running or stuck in scheduling for 300.00s, which is longer than the average task duration + z-score * stddev of this operator (2.00 + 10 * 1.00s).
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/hang/EXPECT.md`:

```markdown
# Expected output for `hang`

The subagent output MUST:
- Fire **Hang / stall** (S11), citing BOTH the idle-detector ("no outputs for 90
  seconds") and hanging-detector ("stuck in scheduling for 300.00s") lines.
- Recommend checking the stuck task's stack trace / node health, and — if it's
  resource starvation — scaling the cluster; note a slow UDF as an alternative
  benign cause (confidence caveat).
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: dispatch on `fixtures/shuffle-memory/` and `fixtures/hang/`.
Expected: FAIL — S9–S12 not present.

- [ ] **Step 3: Add S9–S12 to Section 2**

Append to the "## Section 2" block:

````markdown
### S9. Shuffle / join memory pressure
- **Confirming signals:** warnings `Insufficient memory resources in cluster for
  hash shuffle` / `Insufficient CPU resources in cluster for hash shuffle` /
  `hash-shuffle aggregators are ready after` / per-aggregator memory clamp.
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
````

- [ ] **Step 4: Run the tests to verify they pass**

Run: dispatch on `fixtures/shuffle-memory/` → S9 fires with both warnings cited and the aggregator/pod-memory remediation.
Run: dispatch on `fixtures/hang/` → S11 fires with both stall lines cited and the stack-trace/scaling remediation + slow-UDF caveat.
Expected: PASS both. Verify against each `EXPECT.md`.

- [ ] **Step 5: Commit**

```bash
git add .claude/agents/ray-data-tuning-advisor/
git commit -m "feat(agent): add shuffle-memory, skew, hang, driver-memory symptoms"
```

---

### Task 8: Signal-reference appendix + multi-symptom integration fixture + README

**Files:**
- Modify: `.claude/agents/ray-data-tuning-advisor/playbook.md` (Section 5)
- Modify: `.claude/agents/README.md`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/integration/stats.txt`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/integration/ray-data.log`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/integration/metrics.csv`
- Create: `.claude/agents/ray-data-tuning-advisor/fixtures/integration/EXPECT.md`

**Interfaces:**
- Consumes: all symptoms S1–S12 and the output contract.
- Produces: the finished agent (no further tasks depend on it).

- [ ] **Step 1: Write the failing test (integration fixture with two co-occurring symptoms + an unmatched signal)**

Create `.claude/agents/ray-data-tuning-advisor/fixtures/integration/stats.txt`:

```
Operator 1 ReadImages: 1000 tasks executed, 1000 blocks produced in 400s
* Remote wall time: 0.2s min, 1.0s max, 0.4s mean, 400s total
* Output size bytes per block: 550MB min, 800MB max, 650MB mean, 650GB total
Operator 2 MapBatches(gpu_infer): 1000 tasks executed, 1000 blocks produced in 90s
* Remote wall time: 0.05s min, 0.2s max, 0.09s mean, 90s total
Cluster memory:
* Spilled to disk: 40960MB
* Restored from disk: 40960MB
Dataset throughput:
* Ray Data throughput: 1200 rows/s
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/integration/ray-data.log`:

```
2026-07-15 16:00:00	INFO streaming_executor.py:190 -- Starting execution of Dataset dataset_9.
2026-07-15 16:00:05	WARNING high_memory_detector.py:25 -- Operator 'ReadImages' uses 2.0GiB of memory per task on average, but Ray only requests 0.5GiB per task at the start of the pipeline.
2026-07-15 16:01:00	INFO some_new_detector.py:10 -- Operator MapBatches(gpu_infer) reports metric ray_data_future_unknown_signal=7.
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/integration/metrics.csv`:

```
metric,operator,value
ray_data_spilled_bytes,ReadImages,42949672960
ray_data_gpu_usage_cores,MapBatches(gpu_infer),0.35
ray_data_pool_utilization,MapBatches(gpu_infer),0.3
```

Create `.claude/agents/ray-data-tuning-advisor/fixtures/integration/EXPECT.md`:

```markdown
# Expected output for `integration`

The subagent output MUST:
- Fire at least TWO symptoms: S1 (spilling/OOM — 40960MB spill + high-memory
  warning + ~650MB blocks) and S2 (GPU underutilized — 0.35 GPU usage, 0.3 pool
  util, tiny GPU-op wall-time share vs the big upstream read).
- Rank findings (the more impactful one first) with evidence lines for each.
- Emit all 5 report sections.
- In "What I couldn't assess" (or as a surfaced unmatched signal), mention the
  unrecognized `ray_data_future_unknown_signal` / `some_new_detector` line rather
  than silently dropping it (signal-reference appendix behavior).
- Provide a copy-pasteable quick-win config block.
- State that pod & autoscaling specifics need the RayCluster spec (not provided),
  while still giving the KubeRay translation for object-store memory + GPU pods.
- Invent no metric not present in the inputs.
```

- [ ] **Step 2: Run the test to verify it fails**

Run: dispatch the agent on `fixtures/integration/`.
Expected: FAIL — without Section 5, the agent has no explicit instruction to
surface the unmatched `some_new_detector` / `ray_data_future_unknown_signal`
line, so it drops it.

- [ ] **Step 3: Fill in Section 5 (signal-reference appendix)**

Replace the "## Section 5" block in `playbook.md` with:

````markdown
## Section 5 — Signal-reference appendix

Two purposes: (a) a lookup of every known signal, and (b) a **catch-all rule** so
nothing is silently dropped.

**Catch-all rule:** if the inputs contain a `ray_data_*` metric, a `WARNING`/
`ERROR` line, or a detector message that does NOT match any symptom S1–S12,
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
````

- [ ] **Step 4: Add the agent to the README**

In `.claude/agents/README.md`, after the existing comment block, add:

```markdown

## Available agents

- **ray-data-tuning-advisor** (`ray-data-tuning-advisor/`) — diagnoses Ray Data
  execution bottlenecks from provided telemetry files (`ds.stats()` output,
  `ray-data.log`, optional Prometheus/Grafana export) and returns prioritized,
  KubeRay-aware tuning recommendations. See its `playbook.md` for the diagnostic
  knowledge base and `fixtures/` for worked examples.
```

- [ ] **Step 5: Run the test to verify it passes**

Run: dispatch the agent on `fixtures/integration/`.
Expected: PASS — both S1 and S2 fire and are ranked with evidence; all 5 sections
present; the unmatched `some_new_detector` / `ray_data_future_unknown_signal`
line is surfaced under "What I couldn't assess"; KubeRay translations given for
object-store memory and GPU pods; no invented metrics. Verify against `EXPECT.md`.

- [ ] **Step 6: Full regression — re-run every fixture**

Run: dispatch the agent once per fixture folder (`smoke`, `indicators`,
`spilling`, `gpu-underutilized`, `backpressure`, `trainer-starvation`,
`actor-scale`, `read-parallelism`, `shuffle-memory`, `hang`, `integration`) and
verify each still matches its `EXPECT.md`.
Expected: PASS all 11.

- [ ] **Step 7: Commit**

```bash
git add .claude/agents/
git commit -m "feat(agent): add signal appendix, integration fixture, README entry"
```

---

## Self-Review

**Spec coverage:**
- Two artifacts (agent + playbook) → Tasks 1, 3–8. ✓
- Files-only input (stats/log/prometheus) → agent def Task 1; anchors Task 2. ✓
- Derived indicators (all 10) → Task 2 Section 1. ✓
- Symptom taxonomy S1–S12 → Tasks 3 (S1), 4 (S2–S3), 5 (S4–S5), 6 (S6–S8), 7 (S9–S12). ✓
- Recommendation catalog (4 levers) → Task 3 Section 3. ✓
- KubeRay translation pairing → Task 3 Section 4, applied in every symptom. ✓
- Guardrails (evidence, confidence, caveat, missing-input honesty, no fabrication) → agent contract Task 1; caveats in S2/S11. ✓
- Output report (5 sections) → Task 1 contract; asserted by every fixture. ✓
- Signal-reference appendix + catch-all → Task 8 Section 5. ✓
- File layout (adjusted to repo `<name>/<name>.md` convention) → File Structure + Global Constraints. ✓

**Placeholder scan:** every step contains concrete file content or an exact dispatch prompt + expected result; no "TBD"/"handle edge cases"/"similar to Task N". ✓

**Type/name consistency:** symptom ids S1–S12, indicator names, `ray_data_*` metric names, and warning anchor strings are used identically across Sections 0/1/2/5 and the fixtures. Agent name `ray-data-tuning-advisor` and file paths are identical in every task. ✓

**Note on "tests":** because the deliverable is a subagent + playbook (not compiled code), each task's test is a dispatch of the subagent against a fixture folder, checked against that folder's `EXPECT.md`. The red/green cycle is real: before a symptom entry exists the fixture's specific diagnosis is absent; after it, the diagnosis appears. These checks are reviewer-gated (a human or the executing agent reads the output), not automated assertions.
