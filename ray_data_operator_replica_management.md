# Ray Data Operator 副本数管理机制

> **适用范围**：Ray Data 流式执行引擎（StreamingExecutor）
> **面向读者**：使用 Ray Data 进行大规模数据处理的工程师
> **最后更新**：2026-06-23

---

## 目录

1. [核心概念](#1-核心概念)
2. [初始副本数确定](#2-初始副本数确定)
3. [运行时扩容机制](#3-运行时扩容机制)
4. [运行时缩容机制](#4-运行时缩容机制)
5. [资源预算管理](#5-资源预算管理)
6. [反压机制](#6-反压机制)
7. [Actor 生命周期](#7-actor-生命周期)
8. [进度条与日志解读](#8-进度条与日志解读)
9. [配置参数速查表](#9-配置参数速查表)
10. [常见问题排查](#10-常见问题排查)

---

## 1. 核心概念

### 1.1 Operator 类型与副本定义

Ray Data 执行引擎基于 DAG 拓扑结构，不同 Operator 对"副本"的定义不同：

| Operator 类型 | 副本概念 | 特征 |
|---|---|---|
| `TaskPoolMapOperator` | 并发 Ray Task 数 | 无状态，每次提交即创建一个 Task |
| `ActorPoolMapOperator` | Actor 数量 | 有状态，Actor 持久运行，任务派发到已存在 Actor |
| `InputDataBuffer` | 无 | DAG 根节点，持有初始数据引用 |
| `AllToAllOperator` | Task 数 | 阻塞式物化算子（如 shuffle） |

用户通过 `ComputeStrategy` 控制类型：

```python
# TaskPool — 无状态
ds.map_batches(fn, compute=ray.data.TaskPoolStrategy(size=100))

# ActorPool — 有状态，可自动扩缩容
ds.map_batches(fn, compute=ray.data.ActorPoolStrategy(
    min_size=2, max_size=100, initial_size=10
))
```

### 1.2 执行引擎架构

```
StreamingExecutor（事件循环，每 ~100ms 一轮）
├── ResourceManager（资源预算分配）
│   └── ReservationOpResourceAllocator（两层预留模型）
├── DefaultActorAutoscaler（扩缩容决策）
│   └── _derive_target_scaling_config()（决策逻辑核心）
├── BackpressurePolicies（反压策略链）
│   ├── ResourceBudgetBackpressurePolicy
│   ├── DownstreamCapacityBackpressurePolicy
│   └── ConcurrencyCapBackpressurePolicy（已弃用）
└── ProgressManager（进度条管理）
```

---

## 2. 初始副本数确定

### 2.1 TaskPool

`TaskPoolStrategy` 通过 `size` 参数控制最大并发任务数，不指定则无上限，运行时由资源预算和反压策略动态限制。

### 2.2 ActorPool

`ActorPoolStrategy` 提供四级配置：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `size` | None | 固定大小池（min=max=initial=size） |
| `min_size` | 1 | Actor 池最小值 |
| `max_size` | inf | Actor 池最大值 |
| `initial_size` | = min_size | 初始 Actor 数 |

**配置逻辑**（`compute.py:137-177`）：

```python
if size is not None:
    min_size = max_size = initial_size = size
self.initial_size = initial_size or self.min_size
```

启动时 `ActorPoolMapOperator.start()` 触发初始扩容 `scale(delta=initial_size)`。

### 2.3 max_tasks_in_flight_per_actor 确定规则

每个 Actor 同时处理的最大任务数，遵循**三级优先级**：

1. **用户显式指定**：`ActorPoolStrategy(max_tasks_in_flight_per_actor=4)`
2. **DataContext 全局配置**：`DataContext.max_tasks_in_flight_per_actor`
3. **默认公式**：`max_concurrency × 2`（默认 max_concurrency=1，即默认值为 2）

> **关键代码**（`actor_pool_map_operator.py:227-237`）
>
> ```python
> max_tasks_in_flight_per_actor = (
>     compute_strategy.max_tasks_in_flight_per_actor
>     or self.data_context.max_tasks_in_flight_per_actor
>     or max_actor_concurrency * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
> )
> ```

乘数因子 2 可通过环境变量 `RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` 覆盖。

---

## 3. 运行时扩容机制

### 3.1 触发时机

扩容决策嵌入在事件循环中，**每轮调度循环（约 100ms）**都会调用 `actor_autoscaler.try_trigger_scaling()`。

```
事件循环（每 ~100ms 一轮）
  ① update_usages() — 刷新资源使用
  ② ray.wait(timeout=0.1) — 等待任务完成
  ③ select_operator_to_run() × N — 派发任务
  ④ actor_autoscaler.try_trigger_scaling() — 扩缩容决策  ← 每轮都调用
  ⑤ refresh_actor_state() — 检测 Actor 状态变化
```

### 3.2 扩容决策流程

`DefaultActorAutoscaler._derive_target_scaling_config()` 的完整决策链：

```
输入已全部消费？────────────── YES → 缩容到 0 (force=True)
       │ NO
current_size < min_size？───── YES → 扩容到 min_size
       │ NO
current_size > max_size？───── YES → 缩容到 max_size
       │ NO
资源超分配？─────────────────── YES → 缩容
       │ NO
未收到输入？─────────────────── YES → no-op
       │ NO
计算利用率 util = tasks_in_flight / (max_concurrency × current_size)
       │
       ├─ util >= 1.75 → 扩容（三重约束）
       ├─ util <= 0.5  → 缩容（防抖检查）
       └─ 0.5 < util < 1.75 → no-op
```

### 3.3 利用率计算

```python
util = tasks_in_flight / (max_concurrency × current_size)
```

- `current_size` = pending Actors + running Actors
- `tasks_in_flight` = 已提交但未完成的任务总数
- 该值可超过 100%，因为 `max_tasks_in_flight_per_actor` 默认 = `max_concurrency × 2`

### 3.4 三重扩容约束

实际扩容步长受三重约束中的最小值限制：

```python
max_scale_up = min(
    budget_max_scale_up,                    # ① 资源预算允许的 Actor 数
    max_upscaling_delta,                    # ② 配置上限（默认 1）
    actor_pool.max_size() - current_size    # ③ 距 max_size 的差距
)
```

| 约束 | 来源 | 说明 |
|---|---|---|
| budget_max_scale_up | ResourceManager | `budget.floordiv(per_actor_resources)` |
| max_upscaling_delta | DataContext 配置 | 默认 1，每次最多新增 1 个 Actor |
| max_size - current_size | ActorPoolStrategy | 包括 pending Actor |

扩容数量公式：

```python
delta = ceil(current_size × (util / threshold - 1))
delta = max(1, delta)          # 至少 1 个
delta = min(delta, max_scale_up)  # 不超过约束
```

---

## 4. 运行时缩容机制

### 4.1 触发条件

| 条件 | 原因 | 是否强制 |
|---|---|---|
| 所有输入已消费 | "consumed all inputs" | force=True |
| current_size > max_size | "pool exceeding max size" | 否 |
| 资源超分配 (allocation - usage < 0) | "exceeds resource allocation" | 否 |
| util <= 0.5 | "utilization of X <= Y" | 否 |

### 4.2 缩容防抖

扩容后 **10 秒内不允许缩容**（除非 `force=True`）：

```python
_ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 10

if req.delta < 0 and not req.force:
    if time.time() <= self._last_upscaled_at + 10:
        return False  # 被防抖阻止
```

防抖是**单向的**：只阻止缩容，不影响扩容。

### 4.3 缩容实现

移除优先级：
1. **优先移除 pending Actor** — 直接从 pending 列表弹出
2. **其次移除 idle Actor** — 查找 `tasks_in_flight == 0` 的 Actor

---

## 5. 资源预算管理

### 5.1 两层预留模型

默认启用 `ReservationOpResourceAllocator`（可通过 `RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION=false` 禁用）。

```
集群总资源
├── 50% 预留（均分给所有符合条件的 Operator）
│   └── 每个 Operator: limits × 0.5 / num_eligible_ops
│       ├── 50% 给任务执行
│       └── 50% 给输出队列
│
└── 50% 共享池（按下游优先分配）
    └── 从下游到上游，逐个分配
        └── 若份额不足，可向上游借调
```

### 5.2 预算计算

```
budget = max(reserved - usage, 0) + shared_share
```

- `reserved` = 该 Operator 的预留资源
- `usage` = 该 Operator 当前已用资源（含 pending Actor）
- `shared_share` = 从共享池分到的份额

### 5.3 预算如何限制扩容

```python
per_actor = actor_pool.per_actor_resource_usage()  # 如 1 CPU
budget = resource_manager.get_budget(op)            # 如 4 CPU
max_scale_up = budget.floordiv(per_actor)           # = 4
```

> **注意**：RESTARTING 状态的 Actor 资源仍计入 usage，可能占用预算而阻止扩容。

---

## 6. 反压机制

### 6.1 三种默认反压策略

| 策略 | 作用 | 影响副本 |
|---|---|---|
| ResourceBudgetBackpressurePolicy | 预算不足时阻止新任务提交 | 间接影响（阻止 tasks_in_flight 增长 → 不触发扩容） |
| DownstreamCapacityBackpressurePolicy | 下游处理能力不足时反压 | 间接影响 |
| ConcurrencyCapBackpressurePolicy | 动态调整并发上限（已弃用） | 仅影响 TaskPool |

### 6.2 反压对扩缩容的间接影响

```
反压阻止新任务派发
    ↓
tasks_in_flight 不增长
    ↓
util 不增长
    ├─ 不触发扩容
    └─ op_state.under_resource_limits = False
        → 直接阻止扩容决策（返回 no-op, reason="operator exceeding resource quota"）
```

---

## 7. Actor 生命周期

### 7.1 从创建到运行

```
扩容决策 → scale(delta=N)
    ↓
ray.remote().remote() 创建 Actor → 状态: PENDING
    ↓
异步等待 Actor 初始化完成（ready_ref 就绪）
    ↓
pending_to_running() → 加入 _running_actors → 状态: ALIVE
    ↓
可被 select_actors() 选中执行任务
```

### 7.2 Actor 状态

| 状态 | 含义 | 资源计入 |
|---|---|---|
| PENDING | 正在创建中 | `_pending_or_restarting_usage` |
| ALIVE | 已就绪，可接受任务 | `_running_usage` |
| RESTARTING | 崩溃后正在重启 | `_pending_or_restarting_usage` |

默认配置 `max_restarts=-1`（无限重启）、`max_task_retries=-1`（无限重试）。

### 7.3 Actor 选择策略

使用最小堆（`heapdict`）选择负载最轻的 Actor：
1. Peek 最小 rank（最少飞行任务）的 Actor
2. 若 `rank >= max_tasks_in_flight_per_actor`，返回 None（无可用容量）
3. 若启用 locality，优先选择数据所在节点的 Actor

---

## 8. 进度条与日志解读

### 8.1 进度条格式

```
Map(data_ingress): Tasks: 1158; Actors: 580 (running=579, restarting=0, pending=1); Queued blocks: 6841
```

| 字段 | 含义 |
|---|---|
| Tasks | 已提交的任务总数 |
| Actors | 总 Actor 数 = running + pending + restarting |
| running | ALIVE 状态的 Actor |
| pending | 正在创建中的 Actor |
| restarting | 崩溃后正在重启的 Actor |
| Queued blocks | 等待处理的输入数据块数 |

### 8.2 扩缩容日志

扩缩容日志为 **DEBUG 级别**，默认不输出。开启方式：

```python
# 方法 1：代码中开启
import logging
logging.getLogger("ray.data").setLevel(logging.DEBUG)

# 方法 2：环境变量
export RAY_LOG_LEVEL=debug
```

扩容日志格式：
```
Scaling up {cls} actor pool by {N} (reason={reason}, running={running}, util={util}, tasks_in_flight={tasks_in_flight})
```

### 8.3 Driver 日志 vs Worker 日志

| 日志类型 | 位置 | 包含内容 |
|---|---|---|
| **Driver 日志** ⭐ | `/tmp/ray/session_latest/logs/job-driver-<job_id>.log` | 扩缩容决策、资源管理、进度条、调度日志 |
| Worker 日志 | `/tmp/ray/session_latest/logs/worker-*.log` | 仅包含 map 函数内的业务日志 |

> **排查扩缩容问题查 Driver 日志**，Worker 日志只有业务代码输出。

---

## 9. 配置参数速查表

### 9.1 扩缩容相关

| 参数 | 默认值 | 环境变量 | 说明 |
|---|---|---|---|
| 扩容阈值 | 1.75 | `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD` | 利用率超过此值触发扩容 |
| 缩容阈值 | 0.5 | `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD` | 利用率低于此值触发缩容 |
| 最大扩容步长 | 1 | `RAY_DATA_DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA` | 单次决策新增 Actor 上限 |
| 缩容防抖 | 10 秒 | （硬编码） | 扩容后此时间内不缩容 |
| 等待 min Actor 超时 | -1（禁用） | `RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S` | 启动时等待最小 Actor 数的超时 |

### 9.2 资源管理相关

| 参数 | 默认值 | 环境变量 | 说明 |
|---|---|---|---|
| 资源预留开关 | True | `RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION` | 是否启用两层预留模型 |
| 预留比例 | 0.5 | `RAY_DATA_OP_RESERVATION_RATIO` | 预留资源占总量比例 |
| 下游容量反压比率 | 10.0 | `RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO` | 触发下游反压的队列比例 |

### 9.3 关键内部常量

| 常量 | 值 | 说明 |
|---|---|---|
| 事件循环周期 | 100ms | `ray.wait(timeout=0.1)` |
| 全局资源刷新间隔 | 1 秒 | `GLOBAL_LIMITS_UPDATE_INTERVAL_S` |
| max_tasks_in_flight 乘数 | 2 | `DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` |
| Actor 最大重启次数 | -1（无限） | `max_restarts` |
| Actor 最大任务重试 | -1（无限） | `max_task_retries` |

---

## 10. 常见问题排查

### 10.1 Actor 池扩容太慢

**现象**：Actor 数量从 initial_size 增长缓慢，任务积压严重。

**根因**：`MAX_UPSCALING_DELTA=1`（默认），每次决策只新增 1 个 Actor。从 initial_size=500 扩到 max_size=2000 需要 1500 个决策周期。

**解决方案**：

```python
# 方案 1（推荐）：直接设置 initial_size 为目标值
ActorPoolStrategy(
    initial_size=parallelism,  # 直接启动目标数量
    min_size=parallelism,
    max_size=parallelism
)

# 方案 2：增大扩容步长
import os
os.environ["RAY_DATA_DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA"] = "50"

# 方案 3：增大 max_tasks_in_flight_per_actor 提升单 Actor 吞吐
ActorPoolStrategy(max_tasks_in_flight_per_actor=10)
```

### 10.2 从已完成的任务日志中排查

由于扩缩容日志是 DEBUG 级别，已跑完的任务日志中不会有直接记录。可通过以下方式间接分析：

```bash
# 1. 从进度条推算扩容速率
grep "Map(data_ingress)" driver.log | awk -F'Actors: ' '{print $2}'

# 2. 搜索扩容相关日志
grep -r "Scaling up\|Scaled down\|actor pool" /tmp/ray/session_latest/logs/job-driver-*.log

# 3. 搜索资源不足警告
grep -r "starved\|resource quota\|will not allow it to scale" /tmp/ray/session_latest/logs/job-driver-*.log
```

时间推算验证：
```
已知：initial_size=500, 当前=580, 运行时间≈27min
增加量 = 580 - 500 = 80
每次 +1，共 80 次扩容
平均间隔 = 27min / 80 ≈ 20秒/次
→ 与 MAX_UPSCALING_DELTA=1 吻合
```

### 10.3 Actor 池不扩容

排查清单：

| 检查项 | 验证方法 |
|---|---|
| max_size 是否已达上限 | 进度条 `Actors: N` 是否等于 `max_size` |
| 资源预算是否为 0 | 日志中 `budget_max_scale_up` 是否为 0 |
| 集群是否有空闲资源 | Dashboard 查看 CPU 使用率 |
| 反压是否阻止 | 日志中是否有 "operator exceeding resource quota" |
| 利用率是否达阈值 | `util` 是否 >= 1.75 |
| 是否有 pending Actor 未就绪 | 进度条 `pending > 0` 表示 Actor 正在创建中 |

### 10.4 完整链路图

```
用户调用 ds.map_batches(fn, compute=ActorPoolStrategy(...))
    │
    ▼
MapOperator.create() → ActorPoolMapOperator (含 AutoscalingActorConfig)
    │
    ▼
StreamingExecutor 启动 → 构建 Topology + ResourceManager + ActorAutoscaler
    │
    ▼
ActorPoolMapOperator.start() → scale(delta=initial_size) → 创建初始 Actor
    │
    ▼
═══════════════════════════════════════════════════
  事件循环 (每 ~100ms 一轮)
═══════════════════════════════════════════════════
  ① update_usages() → ResourceManager 刷新资源使用
  ② ReservationOpResourceAllocator.update_budgets()
     ├─ 50% 预留均分给 eligible ops
     └─ 50% 共享池按下游优先分配
  ③ ray.wait(timeout=0.1) → 处理完成任务 + Actor pending→running
  ④ select_operator_to_run() × N
     ├─ 检查反压策略
     └─ dispatch_next_task() → select_actors() → actor.submit()
  ⑤ actor_autoscaler.try_trigger_scaling()
     ├─ util >= 1.75 → 三重约束 → 扩容
     └─ util <= 0.5 → 防抖检查 → 缩容
  ⑥ refresh_actor_state() → 检测 ALIVE/RESTARTING/DEAD
  ⑦ 刷新进度条
═══════════════════════════════════════════════════
  循环直到所有 Operator 完成
```

---

## 附录：关键文件索引

| 文件 | 职责 |
|---|---|
| `python/ray/data/_internal/compute.py` | ComputeStrategy 定义（TaskPool/ActorPool） |
| `python/ray/data/context.py` | DataContext 全局配置 + 默认值 |
| `python/ray/data/_internal/actor_autoscaler/default_actor_autoscaler.py` | 扩缩容决策核心逻辑 |
| `python/ray/data/_internal/actor_autoscaler/autoscaling_actor_pool.py` | ActorPool 抽象接口 + 利用率计算 |
| `python/ray/data/_internal/execution/operators/actor_pool_map_operator.py` | _ActorPool 具体实现 + Actor 生命周期 |
| `python/ray/data/_internal/execution/operators/task_pool_map_operator.py` | TaskPool 实现 |
| `python/ray/data/_internal/execution/resource_manager.py` | ResourceManager + 预算分配 |
| `python/ray/data/_internal/execution/streaming_executor.py` | 事件循环 + 调度循环 |
| `python/ray/data/_internal/execution/streaming_executor_state.py` | OpState + 进度显示格式化 |
| `python/ray/data/_internal/execution/backpressure_policy/` | 三种反压策略实现 |
