# Ray 调度机制深度解析

> 本文档基于 Ray 源码分析，系统梳理 Ray Core 的分布式调度架构，涵盖普通 Task、Actor 和 PlacementGroup 三类调度，适合对分布式系统调度感兴趣的工程师阅读。

---

## 目录

1. [为什么需要去中心化调度](#1-为什么需要去中心化调度)
2. [调度的串行性保障](#2-调度的串行性保障)
3. [资源视图与 Ray Syncer 一致性](#3-资源视图与-ray-syncer-一致性)
4. [Spillback 机制](#4-spillback-机制)
5. [Reject 后的恢复流程](#5-reject-后的恢复流程)
6. [LeasePolicy：客户端节点选择](#6-leasepolicy客户端节点选择)
7. [Actor 调度](#7-actor-调度)
8. [PlacementGroup 调度](#8-placementgroup-调度)
9. [三类调度对比](#9-三类调度对比)
10. [调度性能](#10-调度性能)
11. [测试方法](#11-测试方法)

---

## 1. 为什么需要去中心化调度

### 问题背景

Ray 集群可能有数百甚至上千个节点，如果所有调度决策都走中心化节点（如 GCS），会面临：

- **单点瓶颈**：GCS 处理能力有限，无法支撑百万级 task/sec
- **网络延迟**：每次调度都跨网络往返 GCS，增加延迟
- **单点故障**：GCS 宕机导致整个集群不可用

### Ray 的方案

Ray 将 **Task 调度** 去中心化，而 **Actor 和 PlacementGroup 调度** 保留在 GCS：

| 调度类型 | 调度位置 | 原因 |
|---------|---------|------|
| 普通 Task | 每个 Raylet 独立决策 | 吞吐量大，需要并行调度 |
| Actor | GCS 集中调度 | 需要全局状态管理（生命周期、重建） |
| PlacementGroup | GCS 集中调度 | 需要原子性（2PC） |

### 去中心化的核心设计

每个 Raylet 维护一份**全集群资源视图**（`local_view_`），通过 **Ray Syncer** 以点对点双向流式 RPC 广播，每 100ms 更新一次。Raylet 基于本地视图独立做调度决策，无需请求 GCS。

```
Node A                    Node B                    Node C
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│ local_view:  │         │ local_view:  │         │ local_view:  │
│  A: 4CPU avail│ ──────► │  A: 4CPU avail│ ──────► │  A: 4CPU avail│
│  B: 2CPU avail│ ◄────── │  B: 2CPU avail│ ◄────── │  B: 2CPU avail│
│  C: 8CPU avail│         │  C: 8CPU avail│         │  C: 8CPU avail│
└──────────────┘         └──────────────┘         └──────────────┘
        Ray Syncer 双向广播 (每 100ms)
```

---

## 2. 调度的串行性保障

Ray 通过**三层机制**保证分布式调度的串行性：

### 第一层：单线程事件循环

Raylet 和 GCS 的 `main_service` 都设置为 `running_on_single_thread=true`：

```cpp
// src/ray/raylet/main.cc
instrumented_io_context main_service{
    /*emit_metrics=*/RayConfig::instance().emit_main_service_metrics(),
    /*running_on_single_thread=*/true,
    "raylet_main_io_context"};
```

所有 RPC handler 都被 `post` 到这个单线程 io_context 执行，天然串行。

### 第二层：同步调度循环

`ClusterLeaseManager::ScheduleAndGrantLeases` 是一个纯同步 for 循环：

```cpp
void ClusterLeaseManager::ScheduleAndGrantLeases() {
    for (auto &work : leases_to_schedule_) {
        auto node = scheduler_.GetBestSchedulableNode(...);
        ScheduleOnNode(node, work);  // 同步：立即扣减资源
    }
}
```

循环内不会有异步回调插入，资源扣减是同步完成的。

### 第三层：跨 Raylet 乐观并发控制

当 Raylet A 需要把 task 调度到 Raylet B 时，A 在**自己的本地视图**中乐观扣减 B 的资源（`AllocateRemoteTaskResources`），而不是请求 B。B 在收到 task 后独立验证资源是否足够。

---

## 3. 资源视图与 Ray Syncer 一致性

### 乐观扣减会被覆盖吗？

**会。** 直接的 Ray Syncer 消息（`UpdateNode`）会无条件覆盖本地视图中的乐观扣减：

```cpp
// ClusterResourceManager::UpdateNode - 无 guard，直接覆盖
bool ClusterResourceManager::UpdateNode(...) {
    NodeResources local_view;
    local_view.available = std::move(node_resources.available);
    AddOrUpdateNode(node_id, local_view);  // 直接覆盖
    return true;
}
```

但周期性重置任务有保护：

```cpp
// 有 modified_ts guard
if (modified_ts && *modified_ts + syncer_delay < absl::Now()) {
    AddOrUpdateNode(node_id, resource);  // 只在超时后覆盖
}
```

### 为什么这样设计是安全的？

单线程事件循环保证了：**Syncer 消息只能在两次调度循环之间被处理，不会在调度循环中间插入。** 乐观扣减只需要在**一次调度循环内有效**，循环结束后即使被覆盖也没关系。

```
时间线（单线程 io_context）：
─────────────────────────────────────────────
[调度循环]──[Syncer消息处理]──[调度循环]──[Syncer消息处理]
     ↑ 乐观扣减在此有效          ↑ 覆盖发生在此时，但循环已结束
```

---

## 4. Spillback 机制

### 什么是 Spillback

当 Raylet A 资源不足时，A 不会把 task 排队等待，而是**引导客户端去另一个 Raylet B** 尝试。这不是转发，而是返回一个地址让客户端自己重试。

### 三个动作

```
Raylet A (源)                    Client                    Raylet B (目标)
    │                              │                              │
    │ 1. 乐观扣减 B 的资源        │                              │
    │    (在 A 的本地视图中)      │                              │
    │                              │                              │
    │ 2. 返回 retry_at_raylet_    │                              │
    │    address(B)               │                              │
    │ ──────────────────────────► │                              │
    │                              │                              │
    │                              │ 3. 带 grant_or_reject=true  │
    │                              │    重试到 B                  │
    │                              │ ──────────────────────────► │
    │                              │                              │
    │                              │    B 独立验证资源是否足够    │
    │                              │ ◄────────────────────────── │
    │                              │    (分配 worker / reject)    │
```

### 关键标志：`grant_or_reject`

- Spillback 请求设置 `grant_or_reject=true`
- 目标 Raylet **要么分配，要么拒绝，不能再次 spillback**
- 这防止了 spillback 链式传播

---

## 5. Reject 后的恢复流程

当目标 Raylet B 拒绝了 spillback 请求，客户端会：

```
B reject
    │
    ▼
Client 进入 reply.rejected() 分支
    │
    │ 调用 RequestNewWorkerIfNeeded(scheduling_key)
    │ 不传 raylet_address → is_spillback = false
    │
    ▼
grant_or_reject 重置为 false
    │
    ▼
LeasePolicy.GetBestNodeForLease() → 重新选节点
    │
    ▼
回到本地 Raylet A（grant_or_reject=false）
    │
    ├── A 资源够了 → 本地调度
    ├── A 资源不够 → 再次 spillback 到其他节点
    └── A 不可调度 → 排队等待
```

### 防止无限循环的机制

1. **Backpressure 限流**：`max_pending_lease_requests_per_scheduling_category` 限制并发 lease 请求（默认 = 节点数）
2. **资源视图刷新**：Ray Syncer 会更新 A 的视图，A 看到资源不足后会排队而非反复 spillback
3. **队列兜底**：`LocalLeaseManager` 最终会把 task 排入本地队列等待资源释放

---

## 6. LeasePolicy：客户端节点选择

### 职责

LeasePolicy 是**客户端侧**的初始节点选择策略，决定 CoreWorker 第一次把 `RequestWorkerLease` 发给哪个 Raylet。

### 两种实现

| 实现 | 策略 | 使用场景 |
|------|------|---------|
| `LocalLeasePolicy` | 永远选本地 Raylet | 简单场景 |
| `LocalityAwareLeasePolicy` | 选依赖对象数据最多的节点 | 默认（数据本地性优化） |

### 在调度流程中的位置

```
CoreWorker 提交 Task
    │
    ├─ 1. LeasePolicy.GetBestNodeForLease() → 选初始 Raylet（客户端侧）
    ├─ 2. 发送 RequestWorkerLease 到选中的 Raylet
    ├─ 3. Raylet 的 ClusterResourceScheduler 做调度决策（服务端侧）
    │     ├─ 资源够 → 分配 worker
    │     ├─ 资源不够 → spillback
    │     └─ 不可调度 → 排队
    └─ 4. 如果被 reject → 回到第 1 步，重新调用 LeasePolicy
```

**关键区分**：LeasePolicy 只在步骤 1 和 reject 重试时参与。Raylet 内部的调度策略（Hybrid/Spread/NodeAffinity）是服务端侧的，负责真正的资源调度决策。

---

## 7. Actor 调度

### 架构

Actor 调度由 GCS 集中负责，经过**两阶段：Lease Worker → Create Actor**。

### 状态机

```
DEPENDENCIES_UNREADY → PENDING_CREATION → ALIVE
                           │                  │
                           │                  ↓
                           │              RESTARTING
                           │                  │
                           ↓                  ↓
                          DEAD ←──────────────┘
```

### 调度流程

```
CoreWorker --CreateActor--> GCS (GcsActorManager)
                              │
    GcsActorScheduler.Schedule()
                              │
    ┌─────────────────────────┴─────────────────────┐
    │ 1. SelectForwardingNode                      │
    │    - 有资源需求 → 选 Owner 所在节点          │
    │    - 无资源需求 → 随机选 alive 节点          │
    │ 2. grant_or_reject = false                   │
    │ 3. LeaseWorkerFromNode → RPC 到 Raylet       │
    └─────────────────────────┬─────────────────────┘
                              │
                ┌─────────────┼─────────────┐
                ▼             ▼              ▼
          成功 lease      spillback       rejected
                │             │              │
    CreateActorOnWorker   grant_or_reject=true  Reschedule()
    (PushNormalTask)      去目标 Raylet       清地址,重新选节点
```

### 关键配置

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `gcs_lease_worker_retry_interval_ms` | 200ms | Lease 失败重试间隔 |
| `gcs_create_actor_retry_interval_ms` | 200ms | Actor 创建失败重试间隔 |

---

## 8. PlacementGroup 调度

### 架构

PlacementGroup 调度使用**两阶段提交（2PC）** 保证原子性。

### 调度流程

```
CoreWorker --CreatePlacementGroup--> GCS
                                      │
    GcsPlacementGroupScheduler.ScheduleUnplacedBundles()
                                      │
    ┌─────────────────────────────────┴──────────────────┐
    │ 1. ClusterResourceScheduler.SchedulePlacementGroup │
    │    一次性为所有 bundles 选节点                     │
    │ 2. AcquireBundleResources                          │
    │    乐观扣减 GCS 资源视图                           │
    └─────────────────────────────────┬──────────────────┘
                                      │
         ┌────────────────────────────┴──────────────────┐
         │ Phase 1: Prepare (并行, 每节点一个 RPC)       │
         │  raylet_client->PrepareBundleResources()      │
         │  → 各 Raylet 锁定资源                         │
         └────────────────────────────┬──────────────────┘
                                      │
                    ┌─────────────────┼───────────────┐
                    ▼                 ▼               ▼
              全部成功           部分失败        全部失败
                    │                 │               │
            Phase 2: Commit    DestroyPrepared    failure_callback
                                      │
         ┌────────────────────────────┴──────────────────┐
         │ Phase 2: Commit (并行, 每节点一个 RPC)          │
         │  raylet_client->CommitBundleResources()       │
         │  → 各 Raylet 正式占用资源                     │
         └────────────────────────────┬──────────────────┘
                                      │
                    ┌─────────────────┼───────────────┐
                    ▼                 ▼               ▼
              全部成功           部分失败        全部失败
                    │                 │               │
            success_callback    RESCHEDULING     failure_callback
                               重试失败的 bundles
```

### 为什么用 2PC？

PlacementGroup 包含多个 bundles，分布在多个节点上。需要原子性保证：要么全部 bundles 放置成功，要么全部回滚。Prepare 阶段锁定资源，Commit 阶段正式占用。如果任何节点 Prepare 失败，已 Prepare 的节点会被回滚。

### 关键配置

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `gcs_create_placement_group_retry_min_interval_ms` | 100ms | 重试最小间隔 |
| `gcs_create_placement_group_retry_max_interval_ms` | 1000ms | 重试最大间隔 |
| `gcs_create_placement_group_retry_multiplier` | 1.5 | 指数退避因子 |

---

## 9. 三类调度对比

| 维度 | 普通 Task | Actor | PlacementGroup |
|------|-----------|-------|----------------|
| **调度位置** | Raylet（去中心化） | GCS（集中） | GCS（集中） |
| **调度器** | ClusterLeaseManager | GcsActorScheduler | GcsPlacementGroupScheduler |
| **资源视图** | Raylet 本地维护全集群视图 | GCS 维护全集群视图 | GCS 维护全集群视图 |
| **选节点** | LeasePolicy + ClusterResourceScheduler | Owner 节点优先，否则随机 | ClusterResourceScheduler 批量选 |
| **资源占用** | 乐观扣减 → 目标验证 | 乐观扣减 → Raylet lease | 2PC: Prepare(锁定) → Commit(占用) |
| **Spillback** | 有 | 有（GCS 跟随 spillback） | 无（失败直接重试） |
| **失败恢复** | 客户端回退本地 | Reschedule（清地址重选） | 重新调度失败的 bundles |
| **原子性** | 不需要 | 不需要（单实体） | 需要（2PC 保证） |
| **串行保证** | Raylet 单线程事件循环 | GCS 单线程事件循环 | GCS 单线程 + 一次一个 PG |

---

## 10. 调度性能

### 官方声明

Ray 论文（[arXiv:1712.05889](https://ar5iv.labs.arxiv.org/html/1712.05889)）明确声明：

> "This allows Ray to schedule **millions of tasks per second** with **millisecond-level latencies**."

原因正是去中心化架构：N 个节点独立调度，总吞吐量 = N × 单节点吞吐。

### Microbenchmark 基准测试

Ray 每个版本发布时运行微基准测试（`release/release_logs/<version>/microbenchmark.json`）。以 2.22.0 为例：

| 测试项 | 吞吐量 (ops/sec) | 含义 |
|--------|------------------|------|
| `single_client_tasks_async` | ~8,194 | 单客户端异步提交 task |
| `single_client_tasks_sync` | ~971 | 单客户端同步提交 task |
| `multi_client_tasks_async` | ~21,743 | 多客户端异步提交 task |
| `1_1_actor_calls_async` | ~9,062 | 单 Actor 异步调用 |
| `n_n_actor_calls_async` | ~27,688 | 多对多 Actor 异步调用 |
| `placement_group_create/removal` | ~838 | Placement Group 创建/销毁 |

### 关键配置参数

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `raylet_report_resources_period_milliseconds` | 100 | Raylet 上报资源间隔 |
| `ray_syncer_message_refresh_interval_ms` | 3000 | Syncer 无消息时强制刷新 |
| `scheduler_spread_threshold` | 0.5 | Hybrid 策略 spread 阈值 |
| `handler_warning_timeout_ms` | 1000 | 事件循环 handler 超时告警 |
| `max_pending_lease_requests_per_scheduling_category` | -1 (自动=节点数) | 并发 lease 请求上限 |
| `locality_aware_leasing_enabled` | true | 是否启用数据本地性优化 |

---

## 11. 测试方法

### Python 集成测试

```bash
# 负载均衡测试
python -m pytest python/ray/tests/test_scheduling.py::test_load_balancing -v

# Hybrid 策略阈值
python -m pytest python/ray/tests/test_scheduling.py::test_hybrid_policy_threshold -v

# Spread 策略
python -m pytest python/ray/tests/test_scheduling.py -k spread -v
```

相关测试文件：
- `python/ray/tests/test_scheduling.py` — 负载均衡、Hybrid/Spread 策略
- `python/ray/tests/test_scheduling_2.py` — 节点亲和、资源抢占
- `python/ray/tests/test_label_scheduling.py` — 标签调度

### C++ 单元测试

```bash
# ClusterLeaseManager（spillback、调度循环）
bazel test //src/ray/raylet/scheduling/tests:cluster_lease_manager_test --test_output=streamed

# ClusterResourceScheduler（节点选择策略）
bazel test //src/ray/raylet/scheduling/tests:cluster_resource_scheduler_test --test_output=streamed

# LocalLeaseManager（worker 分配、公平调度）
bazel test //src/ray/raylet/scheduling/tests:local_lease_manager_test --test_output=streamed
```

---

## 附录：关键源码索引

| 组件 | 文件路径 |
|------|---------|
| Raylet 主入口 | `src/ray/raylet/main.cc` |
| GCS 主入口 | `src/ray/gcs/gcs_server_main.cc` |
| RPC handler 分发 | `src/ray/rpc/server_call.h` |
| 调度循环（Task） | `src/ray/raylet/scheduling/cluster_lease_manager.cc` |
| 本地调度（Task） | `src/ray/raylet/scheduling/local_lease_manager.cc` |
| 资源视图管理 | `src/ray/raylet/scheduling/cluster_resource_manager.cc` |
| 资源数据结构 | `src/ray/common/scheduling/cluster_resource_data.h` |
| Ray Syncer | `src/ray/ray_syncer/ray_syncer.h` / `.cc` |
| 客户端提交 | `src/ray/core_worker/task_submission/normal_task_submitter.cc` |
| LeasePolicy | `src/ray/core_worker/lease_policy.h` / `.cc` |
| Actor 调度 | `src/ray/gcs/actor/gcs_actor_scheduler.cc` |
| Actor 管理 | `src/ray/gcs/actor/gcs_actor_manager.cc` |
| PG 调度 | `src/ray/gcs/gcs_placement_group_scheduler.cc` |
| PG 管理 | `src/ray/gcs/gcs_placement_group_manager.h` |
| GCS 资源管理 | `src/ray/gcs/gcs_resource_manager.h` |
| 配置参数 | `src/ray/common/ray_config_def.h` |
