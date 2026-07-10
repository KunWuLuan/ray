# Ray 深度解析：从架构到调度到数据引擎

> 本文档整合了 Ray 架构、调度机制和 Ray Data 副本管理三个主题，按"系统架构 → 控制平面 → 数据平面 → 调度机制 → 应用层执行引擎"的递进顺序编排，适合团队技术分享使用。

---

## 目录

**第一部分：架构全景**
1. [什么是 Ray](#一什么是-ray)
2. [三层架构鸟瞰](#二三层架构鸟瞰)
3. [进程关系全景图](#三进程关系全景图)
4. [进程类型与启动顺序](#四进程类型与启动顺序)

**第二部分：控制平面 — GCS 的职责与瓶颈**
5. [GCS 八大核心职责](#五gcs-八大核心职责)
6. [单线程事件循环设计](#六单线程事件循环设计)
7. [高负载下的瓶颈分析](#七高负载下的瓶颈分析)
8. [去中心化优化](#八去中心化优化)

**第三部分：数据平面 — Object Store 与所有权模型**
9. [Object Store 在数据平面中的定位](#九object-store-在数据平面中的定位)
10. [Plasma Store：内存架构与分配](#十plasma-store内存架构与分配)
11. [对象生命周期](#十一对象生命周期)
12. [对象副本管理：单拷贝 + 按需复制](#十二对象副本管理单拷贝--按需复制)
13. [基于所有权的去中心化设计](#十三基于所有权的去中心化设计)
14. [Owner CoreWorker 与引用计数器](#十四owner-coreworker-与引用计数器)
15. [对象寻址机制](#十五对象寻址机制)
16. [跨节点传输：Push 与 Pull](#十六跨节点传输push-与-pull)
17. [内存溢出与外部存储](#十七内存溢出与外部存储)
18. [LRU 淘汰策略](#十八lru-淘汰策略)
19. [Owner 单点设计与容错](#十九owner-单点设计与容错)
20. [设计哲学：快速恢复优于高可用](#二十设计哲学快速恢复优于高可用)

**第四部分：调度机制 — 任务如何分布到集群**
21. [为什么需要去中心化调度](#二十一为什么需要去中心化调度)
22. [调度的串行性保障](#二十二调度的串行性保障)
23. [资源视图与 Ray Syncer 一致性](#二十三资源视图与-ray-syncer-一致性)
24. [Spillback 机制](#二十四spillback-机制)
25. [Reject 后的恢复流程](#二十五reject-后的恢复流程)
26. [LeasePolicy：客户端节点选择](#二十六leasepolicy客户端节点选择)
27. [Actor 调度](#二十七actor-调度)
28. [PlacementGroup 调度](#二十八placementgroup-调度)
29. [三类调度对比](#二十九三类调度对比)
30. [调度性能](#三十调度性能)
31. [测试方法](#三十一测试方法)

**第五部分：Ray Data 执行引擎 — 应用层资源管理**
32. [核心概念：Operator 类型与副本](#三十二核心概念operator-类型与副本)
33. [初始副本数确定](#三十三初始副本数确定)
34. [运行时扩容机制](#三十四运行时扩容机制)
35. [运行时缩容机制](#三十五运行时缩容机制)
36. [资源预算管理](#三十六资源预算管理)
37. [反压机制](#三十七反压机制)
38. [Actor 生命周期](#三十八actor-生命周期)
39. [进度条与日志解读](#三十九进度条与日志解读)
40. [配置参数速查表](#四十配置参数速查表)
41. [常见问题排查](#四十一常见问题排查)

**附录**
42. [关键源码索引](#附录关键源码索引)
43. [设计权衡总结](#附录设计权衡总结)

---

# 第一部分：架构全景

> **阅读提示**：这一部分的目标是建立全局认知——Ray 是什么、由哪些组件构成、它们如何协作。后续四个部分将分别深入控制平面、数据平面、调度机制和应用层执行引擎。

## 一、什么是 Ray

Ray 是一个通用的分布式计算框架，核心目标是让**单机 Python 程序可以无缝扩展到成百上千台机器**。你可以把它理解为一个"分布式操作系统"——它负责管理集群资源、调度任务、传递数据，让上层应用（训练模型、处理数据、服务推理）只需关注业务逻辑。

Ray 提供三个核心抽象：

| 抽象 | 一句话描述 | 类比 |
|---|---|---|
| **Task** | 无状态的远程函数调用 | 调用一次就结束的 RPC |
| **Actor** | 有状态的远程对象 | 常驻的微服务实例 |
| **Object** | 跨进程共享的不可变数据 | 分布式的共享内存 |

用户的代码通过 `ray.init()` 连接到 Ray 集群后，就可以用 `@ray.remote` 装饰器把普通函数和类变成分布式 Task 和 Actor，Ray 负责把它们调度到合适的节点上执行。

**核心挑战**：当集群规模达到数百甚至上千节点、每秒调度百万级 Task 时，如何保证高效和可靠？Ray 的答案是——**去中心化**。接下来我们看看这个去中心化架构是怎样组织的。

---

## 二、三层架构鸟瞰

Ray 的整体架构可以划分为三个平面，每个平面承担不同的职责，采用不同的扩展策略：

```
┌─────────────────────────────────────────────────────────┐
│                    用户应用层                              │
│    Ray Data · Ray Train · Ray Serve · RLlib · ...       │
└────────────────────────┬────────────────────────────────┘
                         │  ray.init()
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
┌──────────┐     ┌──────────────┐     ┌──────────────┐
│ 控制平面  │     │   数据平面    │     │   计算平面    │
│          │     │              │     │              │
│  GCS     │     │ Object Store │     │   Raylet     │
│  (集中)  │     │ + Owner      │     │  (去中心化)   │
│          │     │  (去中心化)   │     │  + CoreWorker │
│ Actor    │     │              │     │              │
│ PG调度   │     │ 对象元信息    │     │  Task 调度    │
│ 节点管理  │     │  对象寻址     │     │  Task 执行    │
└─────┬────┘     └──────┬───────┘     └──────┬───────┘
      │                 │                    │
      └─────────────────┼────────────────────┘
                        │
                 Ray Syncer (每100ms)
              节点间资源视图同步
```

| 平面 | 核心组件 | 职责 | 扩展策略 |
|---|---|---|---|
| **控制平面** | GCS | Actor/PG 调度、节点管理、KV 存储 | 单线程，通过去中心化减负 |
| **数据平面** | Object Store + Owner RefCounter | 对象存储与元信息管理 | 去中心化，Owner 持有元信息 |
| **计算平面** | Raylet + CoreWorker | Task 调度与执行 | 去中心化，Raylet 独立调度 |

**为什么 GCS 是集中式而 Task 调度是去中心化的？** 因为 Actor 和 PlacementGroup 需要全局状态管理（生命周期、原子性），而普通 Task 的吞吐量极大（百万级/秒），必须分散到各节点并行调度才能扛住。这个权衡贯穿了 Ray 的整个架构设计，后续各部分会反复提到。

---

## 三、进程关系全景图

理解了三层架构后，我们来看 Ray 集群中实际运行的进程及其关系。这张全景图是理解后续所有内容的基础：

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Head Node                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │GCS Server│  │ Monitor  │  │Dashboard │  │RayClient │          │
│  │          │  │          │  │API Server│  │ Server   │          │
│  └────┬─────┘  └──────────┘  └──────────┘  └──────────┘          │
│       │                                                            │
│  ┌────┴──────────────────────────────────────────┐                 │
│  │              Raylet (Head)                     │                 │
│  │  ├── Dashboard Agent (子进程)                   │                 │
│  │  └── Runtime Env Agent (子进程)                 │                 │
│  └───────────────────────────────────────────────┘                 │
│  ┌──────────────┐                                                   │
│  │ Log Monitor  │                                                   │
│  └──────────────┘                                                   │
│  ┌──────────────────────────┐                                       │
│  │ CoreWorker Driver         │ ← ray.init() 创建                     │
│  │ (Owner of most objects)   │                                      │
│  └──────────────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────┘
         │                    │
    ┌────┴────┐          ┌────┴────┐
    ▼         ▼          ▼         ▼
┌──────────────────┐                    ┌──────────────────┐
│   Worker Node 1   │                    │   Worker Node 2   │
│  ┌──────────────┐│                    │  ┌──────────────┐│
│  │   Raylet     ││◄── Ray Syncer ───► │  │   Raylet     ││
│  │  ├ Dashboard ││    每100ms同步资源   │  │  ├ Dashboard ││
│  │  └ RuntimeEnv││                    │  │  └ RuntimeEnv││
│  └──────────────┘│                    │  └──────────────┘│
│  ┌──────────────┐│                    │  ┌──────────────┐│
│  │  Log Monitor ││                    │  │  Log Monitor ││
│  └──────────────┘│                    │  └──────────────┘│
│  ┌──────────────┐│                    │  ┌──────────────┐│
│  │ CoreWorker   ││                    │  │ CoreWorker   ││
│  │  (Workers)   ││                    │  │  (Actors)    ││
│  └──────────────┘│                    │  └──────────────┘│
└──────────────────┘                    └──────────────────┘
```

图中可以看到几个关键点：

1. **GCS 只在 Head 节点运行**：它是控制平面的核心，集中负责 Actor 和 PlacementGroup 调度。
2. **每个节点都有一个 Raylet**：它是计算平面的核心，负责本节点的资源管理和 Task 调度。Raylet 之间通过 **Ray Syncer** 每 100ms 同步资源视图，无需经过 GCS。
3. **CoreWorker 是按需创建的**：不由 `ray start` 启动，而是由 `ray.init()` 创建 Driver、由 Raylet fork 创建 Worker。它们是真正执行用户代码的进程。

> **核心洞察**：Ray 的进程分为两层——`ray start` 启动的**基础设施层**（GCS、Raylet 等，长期运行），和 `ray.init()` 创建的**运行时层**（CoreWorker，随用户 Job 生命周期）。这个两层分离是理解 Ray 架构的起点。

---

## 四、进程类型与启动顺序

现在我们有了全景图，可以逐个了解每种进程的作用了。

### 4.1 进程类型一览

Ray 集群中的进程类型定义在 `python/ray/_private/ray_constants.py` 中：

| 进程类型常量 | 说明 | 所属层 |
|---|---|---|
| `PROCESS_TYPE_GCS_SERVER` | GCS Server，全局控制服务 | 基础设施层 |
| `PROCESS_TYPE_MONITOR` | Monitor，监控集群健康状态 | 基础设施层 |
| `PROCESS_TYPE_RAY_CLIENT_SERVER` | Ray Client Server，支持远程客户端连接 | 基础设施层 |
| `PROCESS_TYPE_DASHBOARD` | Dashboard/API Server，Web 管理界面 | 基础设施层 |
| `PROCESS_TYPE_RAYLET` | Raylet，每节点资源管理与任务调度核心 | 基础设施层 |
| `PROCESS_TYPE_DASHBOARD_AGENT` | Dashboard Agent，每节点指标采集 | 基础设施层 |
| `PROCESS_TYPE_RUNTIME_ENV_AGENT` | Runtime Env Agent，每节点运行时环境管理 | 基础设施层 |
| `PROCESS_TYPE_LOG_MONITOR` | Log Monitor，每节点日志收集 | 基础设施层 |
| `PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER` | CoreWorker Driver，用户脚本入口 | 运行时层 |
| `PROCESS_TYPE_PYTHON_CORE_WORKER` | CoreWorker Worker，实际执行 Task/Actor | 运行时层 |

### 4.2 Head 节点启动顺序

`ray start --head` 的启动流程（`python/ray/_private/node.py` → `start_head_processes`）：

```
1. GCS Server          — 集群元数据管理（外部 Redis 持久化）
2. Monitor             — 集群健康监控、自动扩缩容触发
3. Ray Client Server   — 远程客户端连接入口（ray.init("ray://..."）)
4. Dashboard/API Server— Web UI + REST API
5. Raylet              — 本节点资源管理 + 任务调度
   ├── Dashboard Agent   — 指标采集（作为 Raylet 子进程）
   └── Runtime Env Agent — 运行时环境准备（作为 Raylet 子进程）
6. Log Monitor         — 日志流式收集
```

### 4.3 Worker 节点启动顺序

```
1. Log Monitor
2. Raylet
   ├── Dashboard Agent
   └── Runtime Env Agent
```

> **注意**：CoreWorker 不由 `ray start` 启动，而是由 `ray.init()` 或 Raylet 按需 fork 创建。

### 4.4 运行时进程

用户代码通过 `ray.init()` 连接集群后，以下进程在运行时被创建：

- **CoreWorker Driver**：运行在调用 `ray.init()` 的机器上，与用户脚本同生共死
- **CoreWorker Worker**：由 Raylet 按需 fork，执行 Task 或作为 Actor 常驻

---

# 第二部分：控制平面 — GCS 的职责与瓶颈

> 建立了全局架构认知后，我们深入控制平面核心 —— GCS，分析它的职责、设计约束和性能瓶颈，以及 Ray 如何通过去中心化设计为 GCS 减负。

## 五、GCS 八大核心职责

GCS（Global Control Service）是 Ray 的控制平面核心，代码入口在 `src/ray/gcs/gcs_server_main.cc`：

| 职责 | 说明 |
|---|---|
| 节点管理 | 维护集群节点存活列表，节点上下线通知 |
| Actor 管理 | Actor 创建/销毁的集中调度（`GcsActorScheduler`） |
| Placement Group 调度 | 资源组的创建与调度（`GcsPlacementGroupManager`） |
| 资源管理 | 集群资源视图的维护与同步 |
| 作业管理 | Job 的提交与生命周期跟踪 |
| 内部 KV 存储 | 集群级键值存储（Dashboard、Runtime Env 等使用） |
| PubSub 发布订阅 | 消息路由服务（对象位置变更通知等） |
| 自动扩缩容状态 | Autoscaler 的状态管理 |

---

## 六、单线程事件循环设计

```cpp
// gcs_server_main.cc
instrumented_io_context main_service(
    /*enable_metrics=*/...,
    /*running_on_single_thread=*/true,   // ← 所有 RPC handler 串行执行
    "gcs_server_main_io_context");
```

所有 GCS 的 RPC handler 在**同一个线程**上串行执行，这是 GCS 性能分析的核心约束。

---

## 七、高负载下的瓶颈分析

| 瓶颈点 | 触发条件 | 影响 |
|---|---|---|
| Actor 批量创建 | 大量 Actor 同时创建 | GCS 集中调度，`Schedule()` 串行执行 |
| Placement Group 调度 | 大量 PG 创建请求 | 资源分配算法复杂度高 |
| 节点频繁上下线 | 大规模节点故障/扩容 | 节点心跳与状态更新排队 |
| KV 存储高频读写 | Dashboard、RuntimeEnv 频繁访问 | KV 操作阻塞事件循环 |
| PubSub 消息洪流 | 大量对象位置变更通知 | PubSub 路由排队 |
| GCS RPC 限流 | `gcs_max_active_rpcs_per_handler` 触发 | 请求被拒绝 |
| 持久化延迟 | Redis 写入瓶颈 | GCS 恢复变慢 |

---

## 八、去中心化优化

为减轻 GCS 负担，Ray 在以下方面做了去中心化设计：

- **普通 Task 调度**：由各节点 Raylet 独立调度，不经过 GCS
- **Spillback 机制**：Raylet 直接选择远程节点提交任务，无需 GCS 仲裁
- **Ray Syncer 协议**：节点间每 100ms 直接同步资源视图，绕过 GCS
- **对象元信息管理**：基于所有权模型，存储在 Owner CoreWorker 而非 GCS（详见第三部分）

```
                    ┌──────────────────────────┐
                    │        GCS Server         │
                    │  (单线程事件循环)           │
                    │                          │
                    │  ✓ Actor 调度（集中）      │
                    │  ✓ PG 调度（集中）         │
                    │  ✗ Task 调度（去中心化）    │
                    │  ✗ 对象元信息（去中心化）    │
                    └──────────────────────────┘
                           │           │
              ┌────────────┘           └────────────┐
              ▼                                      ▼
    ┌─────────────────┐                    ┌─────────────────┐
    │  Raylet (Node1) │◄── Ray Syncer ───►│  Raylet (Node2) │
    │  独立 Task 调度  │    每100ms同步      │  独立 Task 调度  │
    │  Spillback      │                    │  Spillback      │
    └─────────────────┘                    └─────────────────┘
```

---

# 第三部分：数据平面 — Object Store 与所有权模型

> 前两部分了解了 Ray 的进程架构和控制平面（GCS）。现在进入数据平面——对象如何存储、传输、淘汰和恢复。Ray 的数据平面由 Object Store（基于 Plasma）和 Owner CoreWorker 的引用计数器共同构成，核心设计理念是**去中心化**：元信息不存 GCS，而存 Owner；数据不做多副本，而按需复制。

## 九、Object Store 在数据平面中的定位

Object Store 是 Ray 数据平面的核心，负责在集群中存储和传输分布式对象。每个节点运行一个本地 Object Store 实例（基于 Plasma），通过共享内存（mmap）为同节点的 CoreWorker 提供零拷贝数据访问。

```
┌─────────────────────────────────────────────────────┐
│                   Raylet 进程                        │
│  ┌──────────────────────────────────────────────┐   │
│  │           ObjectManager                      │   │
│  │  ├── PullManager   (拉取管理)                 │   │
│  │  ├── PushManager    (推送管理)                 │   │
│  │  └── ObjectBufferPool (缓冲池)                │   │
│  └──────────────┬───────────────────────────────┘   │
│                 │ IPC (Unix Socket)                  │
│  ┌──────────────▼───────────────────────────────┐   │
│  │         PlasmaStore (独立线程)                 │   │
│  │  ├── ObjectLifecycleManager (生命周期管理)     │   │
│  │  │   ├── ObjectStore      (对象表)            │   │
│  │  │   └── EvictionPolicy    (LRU淘汰)          │   │
│  │  └── PlasmaAllocator      (内存分配)          │   │
│  │      ├── /dev/shm 主分配器   (共享内存)        │   │
│  │      └── fallback 分配器     (磁盘回退)        │   │
│  └──────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────┐   │
│  │         LocalObjectManager                   │   │
│  │  (主拷贝 pin/spill/restore 管理)              │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
          ▲                               ▲
          │ 零拷贝 mmap                    │ RPC (Push/Pull)
          │                               │
┌─────────┴─────────┐            ┌────────┴──────────┐
│  CoreWorker        │            │ 远端 Raylet       │
│  (同节点)           │            │ (其他节点)         │
│  PlasmaClient ─────┘            └──────────────────┘
└───────────────────┘
```

图中几个关键点：
- **PlasmaStore** 运行在 Raylet 进程的独立线程中，通过共享内存为同节点 CoreWorker 提供零拷贝访问
- **ObjectManager** 负责跨节点传输（Push/Pull）
- **LocalObjectManager** 管理主拷贝的 pin/spill/restore 生命周期

---

## 十、Plasma Store：内存架构与分配

### 10.1 两级内存分配器

PlasmaAllocator 使用两级分配策略：

| 分配器 | 内存来源 | 特点 | 用途 |
|---|---|---|---|
| **主分配器** | `/dev/shm`（Linux 共享内存） | 零拷贝，多进程共享 | 优先使用，性能最优 |
| **Fallback 分配器** | 磁盘 mmap 文件 | 性能较差，但不受 footprint limit 限制 | 主分配器 OOM 时回退 |

```cpp
// plasma_allocator.h
class PlasmaAllocator : public IAllocator {
  std::optional<Allocation> Allocate(size_t bytes) override;        // 主分配
  std::optional<Allocation> FallbackAllocate(size_t bytes) override; // 回退分配
  int64_t GetFootprintLimit() const override;                        // 内存上限
};
```

主分配器从 `/dev/shm` 预先 mmap 一大块内存，通过 `dlmalloc` 管理内部分配。Fallback 分配器从磁盘文件 mmap，不受 footprint limit 限制但计入总量统计。CoreWorker 通过 mmap 同一文件实现零拷贝读取。

### 10.2 内存容量管理

```
PlasmaStore 可用内存 = FootprintLimit - NumBytesInUse + NumBytesUnsealed
```

- `FootprintLimit`：配置的内存上限（`object_store_memory` 参数）
- `NumBytesInUse`：已密封且被引用的对象总大小
- `NumBytesUnsealed`：已创建但未密封的对象大小（不计入在用，因为可能被驱逐）

### 10.3 OOM 处理流程

当创建对象时内存不足，会触发 LRU 淘汰 → 重试分配 → Fallback 分配 → 排队等待的降级链：

```
CreateObject 请求
    │
    ▼
PlasmaAllocator::Allocate() ──失败──→ EvictionPolicy::RequireSpace()
    │                                        │
    │ 成功                                   ▼
    │                                  LRU 选择淘汰对象
    │                                        │
    │                                        ▼
    │                                  ObjectStore::DeleteObject()
    │                                        │
    │                                        ▼
    ▼                                  重试 Allocate()
CreateObject 成功                            │
                                       Allocate 成功 → 创建对象
                                       Allocate 失败 → 尝试 FallbackAllocate
                                                            │
                                                      成功 → 创建对象（性能降级）
                                                      失败 → 排队等待 (delay_on_oom_ms 后重试)
```

---

## 十一、对象生命周期

### 11.1 状态机

```
                    CreateObject
                        │
                        ▼
                ┌──────────────┐
                │   Created     │  (已创建，未密封)
                │  ref_count=1  │  (被创建者引用)
                │  不可被 Get   │
                └──────┬───────┘
                       │ SealObject
                       ▼
                ┌──────────────┐
                │    Sealed     │  (密封，不可变)
                │  ref_count≥1  │  (可被 Get)
                │  可被读取      │
                └──────┬───────┘
                       │
           ┌───────────┼───────────┐
           │           │           │
     ref_count=0    DeleteObject   Spill
     (LRU可淘汰)     (显式删除)    (溢出到磁盘)
           │           │           │
           ▼           ▼           ▼
     LRU Evict     Deleted      Spilled
     (内存回收)    (立即删除)    (数据持久化)
                                     │
                                     │ RestoreSpilledObject
                                     ▼
                                恢复到 Sealed
```

### 11.2 创建与密封

对象创建由 CoreWorker 发起，通过 IPC 请求本地 PlasmaStore：

```cpp
// plasma_store_provider.h - CoreWorker 侧接口
Status Create(metadata, data_size, object_id, owner_address, data, ...);
Status Seal(object_id);     // 密封后对象变为不可变
Status Release(object_id);  // 释放第一个引用，此后对象可被 LRU 淘汰
```

> **关键约束**：调用者创建对象后必须调用 `Release()` 释放第一个引用。在此之前，对象被 pin，不可被淘汰。

### 11.3 引用计数与 Pin 机制

PlasmaStore 维护每个对象的引用计数：

- **AddReference**：`ref_count++`，对象被 Get 时调用
- **RemoveReference**：`ref_count--`，对象被 Release 时调用
- **ref_count == 0**：对象变为可淘汰（evictable），但不立即删除
- **ref_count > 0**：对象被 pin，不可被 LRU 淘汰

Raylet 层面还有一层 pin 机制：主拷贝被 Raylet pin 住（直到 Owner 释放引用），副拷贝仅受 PlasmaStore 引用计数管理。

### 11.4 删除机制

删除有两条路径：

| 路径 | 触发条件 | 行为 |
|---|---|---|
| **显式删除** | Owner 引用计数降为 0，全局 FreeObjects 广播 | 所有节点删除该对象的所有拷贝 |
| **LRU 淘汰** | 本地 PlasmaStore 内存不足 | 仅淘汰本地 `ref_count==0` 的对象 |

显式删除的广播流程：

```
Owner CoreWorker
    │ ref_count = 0
    ▼
FlushObjectsToFree()
    │ "eagerly evict all plasma copies of the object from the cluster"
    ▼
SpreadFreeObjectsRequest ──广播──→ 所有节点 ObjectManager
    │                                    │
    ▼                                    ▼
本地 FreeObjects                     远端 FreeObjects
    │                                    │
    ▼                                    ▼
DeleteObject (Plasma)               DeleteObject (Plasma)
```

---

## 十二、对象副本管理：单拷贝 + 按需复制

### 12.1 核心结论

**Ray Object Store 的对象数据本质上是单拷贝（single-copy）设计，不存在主动的多副本复制机制。**

### 12.2 Primary Copy 与 Secondary Copy

| 类型 | 创建方式 | 管理者 | Pin 机制 | 生命周期 |
|---|---|---|---|---|
| **Primary Copy** | 对象创建时所在节点 | LocalObjectManager | Raylet pin | Owner 释放引用时删除 |
| **Secondary Copy** | 其他节点 Pull 拉取 | 仅 PlasmaStore 引用计数 | 无 Raylet pin | LRU 淘汰或全局 Free |

### 12.3 副本数量无硬性上限

`ObjectLocation` 中的 `node_ids_` 是一个 vector，**如果有 N 个节点需要访问同一对象，集群中最多存在 N 份拷贝**（1 primary + N-1 secondary）。

### 12.4 副拷贝的删除时机

副拷贝**不会在使用后立即删除**，而是保留在本地 Plasma Store 中，直到：

| 删除路径 | 触发条件 | 影响范围 |
|---|---|---|
| **LRU 淘汰** | 本地内存不足，`ref_count==0` 的对象被淘汰 | 仅删除本地副拷贝 |
| **全局 Free** | Owner 引用计数降为 0，广播 FreeObjects | 删除所有节点上的所有拷贝 |

策略总结：**"拉来就用，用完留着，内存不够再 LRU 淘汰"**。这避免了同一对象被反复拉取的开销，但可能导致集群中存在多份冗余拷贝。

### 12.5 副本管理全景图

```
Node A (Owner)              Node B                    Node C
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ PlasmaStore     │    │ PlasmaStore     │    │ PlasmaStore     │
│ ┌─────────────┐│    │ ┌─────────────┐│    │ ┌─────────────┐│
│ │ Primary Copy ││    │ │Secondary    ││    │ │Secondary    ││
│ │ (pinned by   ││    │ │ Copy        ││    │ │ Copy        ││
│ │  raylet)     ││    │ │ (ref_count  ││    │ │ (ref_count  ││
│ │              ││    │ │  may be 0)  ││    │ │  may be 0)  ││
│ └─────────────┘│    │ └─────────────┘│    │ └─────────────┘│
│ LocalObject    │    │ (无 LocalObject │    │ (无 LocalObject │
│ Manager 管理   │    │  Manager 管理)  │    │  Manager 管理)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 十三、基于所有权的去中心化设计

### 13.1 核心结论

**对象元信息不在 GCS 中，而是存储在 Owner CoreWorker 的 ReferenceCounter 中。**

GCS 在对象管理中仅承担两个辅助角色：
1. **PubSub 消息路由**：对象位置变更通知的发布/订阅中介
2. **节点存活信息**：提供节点是否存活的查询

### 13.2 证据：对象位置上报的目标

`src/ray/object_manager/ownership_object_directory.cc` 中的 `ReportObjectAdded` 方法：

```cpp
void OwnershipBasedObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const NodeID &node_id,
    const ObjectInfo &object_info) {
  const auto owner_address = GetOwnerAddressFromObjectInfo(object_info);
  auto owner_client = GetClient(owner_address);
  // 向 Owner Worker 发送位置更新，不是向 GCS
  update.set_plasma_location_update(rpc::ObjectPlasmaLocationUpdate::ADDED);
  SendObjectLocationUpdateBatchIfNeeded(worker_id, node_id, owner_address);
}
```

### 13.3 位置上报流程

```
Node B (Pull 了对象)              Node A (Owner)
    │                                  │
    │ ObjectManager::HandleObjectAdded │
    │   ReportObjectAdded               │
    │─────────RPC: UpdateObjectLocation─→│
    │                                  │
    │                           RefCounter.locations
    │                           += {NodeB}
    │                                  │
    │                           后续其他 Worker 查询时
    │                           返回 locations = {NodeA, NodeB}
```

---

## 十四、Owner CoreWorker 与引用计数器

`src/ray/core_worker/reference_counter.h` 中的 `Reference` 结构体，存储了对象的完整元信息：

```cpp
struct Reference {
  bool owned_by_us_ = false;                    // 是否是 Owner
  std::optional<rpc::Address> owner_address_;   // Owner 地址
  std::optional<NodeID> pinned_at_node_id_;     // 对象在哪个节点
  std::string spilled_url;                      // spill 到外部存储的 URL
  NodeID spilled_node_id = NodeID::Nil();       // spill 到哪个节点
  size_t local_ref_count = 0;                   // 本地引用计数
  size_t submitted_task_ref_count = 0;          // 提交任务的引用计数
  size_t lineage_ref_count = 0;                 // 血统引用计数
  absl::flat_hash_set<NodeID> locations;        // 对象位置集合
};

rpc::Address rpc_address_;  // 本 Worker 地址，用于判断是否是 Owner
ReferenceTable object_id_refs_;  // 所有跟踪对象的引用表
```

Owner 的 ReferenceCounter 还跟踪对象的各种状态计数：

```cpp
std::atomic<size_t> owned_objects_pending_creation_{0};  // 创建中
std::atomic<size_t> owned_objects_in_memory_{0};          // 在内存中
std::atomic<size_t> owned_objects_spilled_{0};            // 已溢出
std::atomic<size_t> owned_objects_in_plasma_{0};          // 在 Plasma 中
```

---

## 十五、对象寻址机制

### 15.1 核心机制：Owner 地址嵌入 ObjectRef

**ObjectRef 自身就携带了 Owner 的 RPC 地址（IP + Port + WorkerID），随引用一起在网络中传递。**

```
ObjectRef 序列化内容 = {
    binary:          对象 ID（20 字节）
    call_site:       调用位置
    owner_address:   Owner CoreWorker 的 RPC 地址  ← 关键
    object_status:   对象状态快照
}
```

### 15.2 完整寻址流程

#### 步骤 1：序列化时嵌入 Owner 地址

```python
# serialization.py - object_ref_reducer
obj, owner_address, object_status = worker.core_worker.serialize_object_ref(obj)
# ↑ 调用 C++ GetOwnershipInfo()，返回 Owner 的 RPC 地址
```

#### 步骤 2：反序列化时注册到本地

```python
# serialization.py - _object_ref_deserializer
worker.core_worker.deserialize_and_register_object_ref(
    obj_ref.binary(), outer_id, owner_address, object_status
)
# ↑ 调用 C++ RegisterOwnershipInfoAndResolveFuture()，存入本地 RefCounter
```

#### 步骤 3：访问对象时查询 Owner

```cpp
// core_worker.cc - GetLocationFromOwner
rpc::Address owner_address;
GetOwnerAddress(object_id, &owner_address);
auto client = core_worker_client_pool_->GetOrConnect(owner_address);
rpc::GetObjectLocationsOwnerRequest request;
request.set_intended_worker_id(owner_address.worker_id());
request.add_object_ids(object_id.Binary());
client->GetObjectLocationsOwner(request, callback);
```

#### 步骤 4：Owner 返回对象位置

```cpp
// core_worker.cc - HandleGetObjectLocationsOwner
reference_counter_->FillObjectInformation(object_id, object_info);
// ↑ 返回：locations(node_ids)、spilled_url、object_size、pending_creation 等
```

### 15.3 寻址流程图

```
Worker A (Owner)                    Worker B (访问者)
┌─────────────────────┐            ┌─────────────────────┐
│  ray.put() /        │            │                     │
│  task.remote()      │            │  1. 反序列化         │
│                     │            │     ObjectRef       │
│  RefCounter:        │            │     (含 A 的地址)     │
│   locations:        │ ObjectRef  │                     │
│    [Node1, Node2]   │──序列化──→ │  2. 注册 Owner 地址  │
│   spilled_url:      │            │     到本地 RefCounter│
│    "s3://..."       │            │                     │
│   object_size:1024  │            │  3. 从 RefCounter    │
│                     │  4.RPC回复  │     取 Owner 地址    │
│  HandleGetObject    │←──────────│     → 得到 A 的地址  │
│  LocationsOwner     │            │                     │
│  → 返回 locations   │──────────→│  5. 向 A 发送 RPC    │
│                     │            │     GetObject       │
│                     │            │     LocationsOwner  │
│                     │            │                     │
│                     │            │  6. 得到对象位置     │
│                     │            │     → 去对应节点     │
│                     │            │     ObjectStore 取数 │
└─────────────────────┘            └─────────────────────┘
```

### 15.4 关键设计要点

| 要点 | 说明 |
|---|---|
| **无需中心化发现** | Owner 地址随 ObjectRef 传递，不需要查 GCS |
| **直接点对点 RPC** | Worker B 直接 RPC Worker A，无中间跳转 |
| **批量查询优化** | `GetLocationFromOwner` 按 Owner 分组批量请求 |
| **PubSub 增量推送** | 订阅后，Owner 位置变更通过 PubSub 增量推送 |

---

## 十六、跨节点传输：Push 与 Pull

### 16.1 Pull 机制（拉取）

当节点需要远端对象时，通过 PullManager 发起拉取：

```
本地 CoreWorker 需要 Object X
    │
    ▼
ObjectManager::Pull(object_refs)
    │
    ▼
PullManager::Pull(...)
    │ 1. 订阅对象位置 (IObjectDirectory::SubscribeObjectLocations)
    │ 2. 等待位置回调
    ▼
OnLocationChange(object_id, client_ids, spilled_url, ...)
    │
    ├── 对象在某节点内存中 → SendPullRequest(node_id)
    │                         │
    │                         ▼
    │                    远端 ObjectManager::HandlePull
    │                    → PushManager::StartPush
    │                    → 分块发送回本地
    │
    └── 对象已 Spill 到磁盘 → RestoreSpilledObject(url)
                                 │
                                 ▼
                            从磁盘/S3 恢复到本地 Plasma
```

### 16.2 PullManager 的配额管理

PullManager 维护内存配额，防止拉取过多对象导致 OOM：

```cpp
/// The total number of bytes that we are currently pulling.
/// To avoid starvation, this is always less than the available capacity
/// in the local object store.
int64_t num_bytes_being_pulled_ = 0;
int64_t num_bytes_available_;
```

**三优先级队列**，优先满足 `ray.get` 请求（释放 worker 资源），避免任务参数拉取导致死锁：

| 优先级 | 来源 | 说明 |
|---|---|---|
| 最高 | `get_request_bundles_` | `ray.get()` 请求，允许 fallback 分配 |
| 中 | `wait_request_bundles_` | `ray.wait()` 请求 |
| 最低 | `task_argument_bundles_` | 任务参数拉取 |

### 16.3 Push 机制（推送）

PushManager 管理出站推送的限流与去重：

```cpp
class PushManager {
  const int64_t max_chunks_in_flight_;
  /// Duplicate concurrent pushes to the same destination will be suppressed.
  absl::flat_hash_map<NodeID,
      absl::flat_hash_map<ObjectID, std::list<PushState>::iterator>>
      push_state_map_;
};
```

对象被切分为固定大小的 chunk（`object_chunk_size` 配置），逐块传输以控制内存峰值。同一对象到同一节点的并发推送会被合并去重。

### 16.4 传输流程时序

```
Node A (请求方)                         Node B (持有方)
    │                                       │
    │ 1. Pull(object_refs)                  │
    │──────────SendPullRequest──────────────→│
    │                                       │ 2. HandlePull
    │                                       │    PushManager::StartPush
    │                                       │
    │ 3. ReceiveObjectChunk(chunk 0)        │
    │←──────────Push chunk 0────────────────│
    │                                       │
    │ 4. ObjectBufferPool::WriteChunk       │
    │    → PlasmaStore::Create/Seal         │
    │                                       │
    │ 5. ReceiveObjectChunk(chunk 1)        │
    │←──────────Push chunk 1────────────────│
    │                  ...                  │
    │                                       │
    │ 6. 所有 chunk 接收完毕                  │
    │    SealObject → 对象可用               │
    │                                       │
    │ 7. ReportObjectAdded (通知 Owner)     │
    │──────────────────────────────────────→ │
    │  (向 Owner 更新本地拥有该对象)          │
```

---

## 十七、内存溢出与外部存储

### 17.1 Spill 触发条件

当 PlasmaStore 内存使用超过阈值时，LocalObjectManager 触发 spill：

```cpp
/// Spill objects as much as possible as fast as possible up to the max throughput.
void SpillObjectUptoMaxThroughput() override;
```

**Spill 安全检查**：只有 `ref_count` 仅来自 raylet 的已密封 primary copy 才可 spill：

```cpp
/// Return true if the given object id has only one reference.
/// Only one reference means there's only a raylet that pins the object
/// so it is safe to spill the object.
bool IsObjectSpillable(const ObjectID &object_id);
```

### 17.2 外部存储后端

| 后端 | 配置 | 特点 |
|---|---|---|
| **本地文件系统** | `filesystem` | 默认，每个节点独立存储 |
| **分布式存储** | `s3` / `gs` / `azure` | 共享存储，任何节点可恢复 |
| **NullStorage** | — | 不 spill（开发/测试用） |

Spill 时多个对象融合为一个文件以优化性能。

### 17.3 Spill 与 Restore 流程

```
         内存压力
             │
             ▼
    LocalObjectManager::TryToSpillObjects
             │
             ├── 1. 检查 IsPlasmaObjectSpillable
             ├── 2. IO Worker 执行 spill_objects()
             │      → 写入外部存储，获得 spilled_url
             ├── 3. Unpin 主拷贝（释放 Plasma 内存）
             └── 4. 更新 Owner 的 spilled_url 元信息
             
    后续访问该对象时：
             │
             ▼
    PullManager::OnLocationChange
         spilled_url 非空
             │
             ▼
    RestoreSpilledObject(url)
         → IO Worker 从外部存储读取
         → 写入本地 PlasmaStore
         → Seal → 对象可用
```

### 17.4 Spilled 对象的删除

Spilled 对象在外部存储中不会永久保留，当 Owner 释放引用时删除。多个对象可能共享一个 spilled 文件，通过引用计数避免提前删除文件。

---

## 十八、LRU 淘汰策略

### 18.1 LRUCache 实现

PlasmaStore 使用 LRU（最近最少使用）策略管理可淘汰对象：

```cpp
class LRUCache {
  typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
  ItemList item_list_;                                      // LRU 链表
  absl::flat_hash_map<ObjectID, ItemList::iterator> item_map_;  // O(1) 查找
  int64_t capacity_;
  int64_t used_capacity_;
};
```

### 18.2 淘汰决策流程

```
PlasmaStore::CreateObject (内存不足)
    │
    ▼
EvictionPolicy::RequireSpace(size, objects_to_evict)
    │
    ├── 1. pinned_memory_bytes 不可动 (ref_count > 0 的对象)
    │
    ├── 2. LRUCache::ChooseObjectsToEvict(num_bytes_required)
    │      从 LRU 链表尾部开始选择，直到满足空间需求
    │
    └── 3. 返回待淘汰对象列表
           │
           ▼
    ObjectLifecycleManager::EvictObjects
           │
           ├── ObjectStore::DeleteObject (释放内存)
           └── EvictionPolicy::RemoveObject (从 LRU 移除)
```

### 18.3 对象访问与 LRU 交互

- `ref_count > 0` 的对象不在 LRU 中，不可被淘汰
- `ref_count == 0` 的对象在 LRU 中，按最近访问时间排序
- 淘汰从 LRU 最久未使用的对象开始

---

## 十九、Owner 单点设计与容错

### 19.1 Owner 是单点

**每个对象有且仅有一个 Owner CoreWorker，Owner 不可迁移。**

判断逻辑：ReferenceCounter 中的 `owned_by_us_` 字段，通过比较 `owner_address_` 与本地 `rpc_address_` 来确定。

### 19.2 为什么启动模块图中没有 CoreWorker

```
┌─────────────────────────────────────────────────────────────────┐
│  ray start 启动的基础设施进程                                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│  │GCS Server│ │ Monitor  │ │Dashboard │ │  Raylet  │ ...      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │
│  生命周期：集群级，长期运行                                       │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│  ray.init() 创建的运行时进程                                     │
│  ┌─────────────────┐     ┌─────────────────┐                   │
│  │ CoreWorker Driver│     │ CoreWorker Worker│ ...              │
│  │ (用户脚本进程)    │     │ (Raylet fork)    │                  │
│  └─────────────────┘     └─────────────────┘                   │
│  生命周期：用户级，随 Job/Task 生命周期                          │
└─────────────────────────────────────────────────────────────────┘
```

CoreWorker 属于第二层——由 `ray.init()` 创建的运行时进程，不是 `ray start` 启动的基础设施进程。

### 19.3 Owner 故障的容错机制

| 对象创建方式 | Owner 故障后的恢复策略 | 失败条件 |
|---|---|---|
| Task 返回值 | Lineage Reconstruction（重新提交 Task） | 血统被驱逐 / 重试次数耗尽 |
| Actor 方法返回值 | Actor 重启（GCS 重新调度） | Actor 不可恢复 |
| `ray.put()` | **永久丢失**，无恢复机制 | — |

```python
# python/ray/exceptions.py - 恢复失败的原因
REASON_MESSAGES = {
    OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED: "maximum number of task retries has been exceeded",
    OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED: "lineage has been evicted to reduce memory pressure",
    OBJECT_UNRECONSTRUCTABLE_PUT: "created by ray.put(), which has no task lineage",
    OBJECT_UNRECONSTRUCTABLE_BORROWED: "crossed an ownership boundary. Only the owner can trigger reconstruction",
}
```

### 19.4 对象丢失场景与恢复

| 场景 | 丢失的数据 | 恢复方式 | 失败条件 |
|---|---|---|---|
| 副拷贝节点宕机 | Secondary Copy | 无需恢复（其他节点或 spill 可重新拉取） | 唯一副拷贝且未 spill |
| Primary 节点宕机 | Primary Copy | 从其他节点的副拷贝 Pin 恢复 | 无副拷贝 |
| Primary + 所有副拷贝丢失 | 所有内存拷贝 | 从 Spill 恢复 | 未 Spill |
| 所有拷贝 + Spill 丢失 | 所有数据 | Lineage 重建（重新执行 Task） | 血统被驱逐 |
| Owner CoreWorker 死亡 | 元信息 + 引用 | **不可恢复**（`ray.put` 对象） | `ray.put` 创建的对象 |

### 19.5 ObjectRecoveryManager 恢复决策

```
对象访问失败（位置丢失）
    │
    ▼
RecoverObject(object_id)
    │
    ├── 1. 查询 Owner 的 locations 集合
    │      locations 非空？
    │      ├── 是 → PinExistingObjectCopy (从副拷贝恢复)
    │      │         成功 → 更新 pinned_at_node_id
    │      │         失败 → 尝试下一个 location
    │      │         全部失败 ↓
    │      │
    │   locations 为空 → 查询 spilled_url
    │      spilled_url 非空？
    │      ├── 是 → RestoreSpilledObject (从磁盘恢复)
    │      │
    │   spilled_url 为空 → ReconstructObject
    │      ├── 检查 lineage_eligibility
    │      ├── 重新提交创建该对象的 Task
    │      └── Task 完成 → 对象恢复
    │
    └── 恢复失败 → 抛出 UnreconstructableException
```

---

## 二十、设计哲学：快速恢复优于高可用

Ray 选择 **"快速恢复"（Fast Recovery）** 而非 **"高可用"（High Availability）**：
- Owner 故障 → 对象暂时不可用 → 通过血统重建快速恢复
- 而非：Owner 主从切换 → 保持服务连续

这种设计简化了系统复杂度，代价是 Owner 故障期间对象短暂不可用。

### 20.1 与传统分布式存储的对比

| 特性 | Ray Object Store | HDFS / 分布式 KV |
|---|---|---|
| 副本数 | 1（按需增加） | 3（固定） |
| 复制方式 | Pull on-demand | 主动复制 |
| 一致性 | 最终一致（Owner 单点） | 强一致（Quorum） |
| 容错 | Lineage 重建 | 副本修复 |
| 淘汰策略 | LRU | 不淘汰（持久化） |
| 访问延迟 | 本地共享内存（μs级） | 网络（ms级） |
| 适用场景 | 短生命周期中间结果 | 长期持久化数据 |

### 20.2 数据平面设计原则总结

| 原则 | 实现 |
|---|---|
| **零拷贝** | Plasma 共享内存 mmap，同节点 CoreWorker 直接读 |
| **按需复制** | 非主动多副本，消费驱动的 Pull 拉取 |
| **去中心化** | 元信息在 Owner CoreWorker，不依赖 GCS |
| **LRU 淘汰** | `ref_count==0` 的对象可被自动淘汰 |
| **Spill 降级** | 内存不足时溢出到磁盘，牺牲性能换可用性 |
| **Lineage 容错** | 重新执行 Task 恢复对象，非数据副本 |

---

# 第四部分：调度机制 — 任务如何分布到集群

> 理解了控制平面和数据平面后，我们来看调度机制——Task 和 Actor 如何被分配到集群中的各个节点。这部分是 Ray 去中心化设计的核心体现。

## 二十一、为什么需要去中心化调度

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

## 二十二、调度的串行性保障

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

## 二十三、资源视图与 Ray Syncer 一致性

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

## 二十四、Spillback 机制

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

## 二十五、Reject 后的恢复流程

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

## 二十六、LeasePolicy：客户端节点选择

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

## 二十七、Actor 调度

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

## 二十八、PlacementGroup 调度

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

## 二十九、三类调度对比

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

## 三十、调度性能

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

## 三十一、测试方法

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

# 第五部分：Ray Data 执行引擎 — 应用层资源管理

> 前四部分解释了 Ray 底层的架构和调度机制。现在我们进入应用层，看 Ray Data 如何在流式执行引擎中管理 Operator 副本数的扩缩容。这是对底层调度能力的上层封装——Ray Data 的 Actor 池调度最终会落到 Raylet 的 Actor 调度上。

## 三十二、核心概念：Operator 类型与副本

### 32.1 Operator 类型与副本定义

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

### 32.2 执行引擎架构

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

## 三十三、初始副本数确定

### 33.1 TaskPool

`TaskPoolStrategy` 通过 `size` 参数控制最大并发任务数，不指定则无上限，运行时由资源预算和反压策略动态限制。

### 33.2 ActorPool

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

### 33.3 max_tasks_in_flight_per_actor 确定规则

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

## 三十四、运行时扩容机制

### 34.1 触发时机

扩容决策嵌入在事件循环中，**每轮调度循环（约 100ms）**都会调用 `actor_autoscaler.try_trigger_scaling()`。

```
事件循环（每 ~100ms 一轮）
  ① update_usages() — 刷新资源使用
  ② ray.wait(timeout=0.1) — 等待任务完成
  ③ select_operator_to_run() × N — 派发任务
  ④ actor_autoscaler.try_trigger_scaling() — 扩缩容决策  ← 每轮都调用
  ⑤ refresh_actor_state() — 检测 Actor 状态变化
```

### 34.2 扩容决策流程

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

### 34.3 利用率计算

```python
util = tasks_in_flight / (max_concurrency × current_size)
```

- `current_size` = pending Actors + running Actors
- `tasks_in_flight` = 已提交但未完成的任务总数
- 该值可超过 100%，因为 `max_tasks_in_flight_per_actor` 默认 = `max_concurrency × 2`

### 34.4 三重扩容约束

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

## 三十五、运行时缩容机制

### 35.1 触发条件

| 条件 | 原因 | 是否强制 |
|---|---|---|
| 所有输入已消费 | "consumed all inputs" | force=True |
| current_size > max_size | "pool exceeding max size" | 否 |
| 资源超分配 (allocation - usage < 0) | "exceeds resource allocation" | 否 |
| util <= 0.5 | "utilization of X <= Y" | 否 |

### 35.2 缩容防抖

扩容后 **10 秒内不允许缩容**（除非 `force=True`）：

```python
_ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 10

if req.delta < 0 and not req.force:
    if time.time() <= self._last_upscaled_at + 10:
        return False  # 被防抖阻止
```

防抖是**单向的**：只阻止缩容，不影响扩容。

### 35.3 缩容实现

移除优先级：
1. **优先移除 pending Actor** — 直接从 pending 列表弹出
2. **其次移除 idle Actor** — 查找 `tasks_in_flight == 0` 的 Actor

---

## 三十六、资源预算管理

### 36.1 两层预留模型

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

### 36.2 预算计算

```
budget = max(reserved - usage, 0) + shared_share
```

- `reserved` = 该 Operator 的预留资源
- `usage` = 该 Operator 当前已用资源（含 pending Actor）
- `shared_share` = 从共享池分到的份额

### 36.3 预算如何限制扩容

```python
per_actor = actor_pool.per_actor_resource_usage()  # 如 1 CPU
budget = resource_manager.get_budget(op)            # 如 4 CPU
max_scale_up = budget.floordiv(per_actor)           # = 4
```

> **注意**：RESTARTING 状态的 Actor 资源仍计入 usage，可能占用预算而阻止扩容。

---

## 三十七、反压机制

### 37.1 三种默认反压策略

| 策略 | 作用 | 影响副本 |
|---|---|---|
| ResourceBudgetBackpressurePolicy | 预算不足时阻止新任务提交 | 间接影响（阻止 tasks_in_flight 增长 → 不触发扩容） |
| DownstreamCapacityBackpressurePolicy | 下游处理能力不足时反压 | 间接影响 |
| ConcurrencyCapBackpressurePolicy | 动态调整并发上限（已弃用） | 仅影响 TaskPool |

### 37.2 反压对扩缩容的间接影响

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

## 三十八、Actor 生命周期

### 38.1 从创建到运行

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

### 38.2 Actor 状态

| 状态 | 含义 | 资源计入 |
|---|---|---|
| PENDING | 正在创建中 | `_pending_or_restarting_usage` |
| ALIVE | 已就绪，可接受任务 | `_running_usage` |
| RESTARTING | 崩溃后正在重启 | `_pending_or_restarting_usage` |

默认配置 `max_restarts=-1`（无限重启）、`max_task_retries=-1`（无限重试）。

### 38.3 Actor 选择策略

使用最小堆（`heapdict`）选择负载最轻的 Actor：
1. Peek 最小 rank（最少飞行任务）的 Actor
2. 若 `rank >= max_tasks_in_flight_per_actor`，返回 None（无可用容量）
3. 若启用 locality，优先选择数据所在节点的 Actor

---

## 三十九、进度条与日志解读

### 39.1 进度条格式

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

### 39.2 扩缩容日志

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

### 39.3 Driver 日志 vs Worker 日志

| 日志类型 | 位置 | 包含内容 |
|---|---|---|
| **Driver 日志** ⭐ | `/tmp/ray/session_latest/logs/job-driver-<job_id>.log` | 扩缩容决策、资源管理、进度条、调度日志 |
| Worker 日志 | `/tmp/ray/session_latest/logs/worker-*.log` | 仅包含 map 函数内的业务日志 |

> **排查扩缩容问题查 Driver 日志**，Worker 日志只有业务代码输出。

---

## 四十、配置参数速查表

### 40.1 扩缩容相关

| 参数 | 默认值 | 环境变量 | 说明 |
|---|---|---|---|
| 扩容阈值 | 1.75 | `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD` | 利用率超过此值触发扩容 |
| 缩容阈值 | 0.5 | `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD` | 利用率低于此值触发缩容 |
| 最大扩容步长 | 1 | `RAY_DATA_DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA` | 单次决策新增 Actor 上限 |
| 缩容防抖 | 10 秒 | （硬编码） | 扩容后此时间内不缩容 |
| 等待 min Actor 超时 | -1（禁用） | `RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S` | 启动时等待最小 Actor 数的超时 |

### 40.2 资源管理相关

| 参数 | 默认值 | 环境变量 | 说明 |
|---|---|---|---|
| 资源预留开关 | True | `RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION` | 是否启用两层预留模型 |
| 预留比例 | 0.5 | `RAY_DATA_OP_RESERVATION_RATIO` | 预留资源占总量比例 |
| 下游容量反压比率 | 10.0 | `RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO` | 触发下游反压的队列比例 |

### 40.3 关键内部常量

| 常量 | 值 | 说明 |
|---|---|---|
| 事件循环周期 | 100ms | `ray.wait(timeout=0.1)` |
| 全局资源刷新间隔 | 1 秒 | `GLOBAL_LIMITS_UPDATE_INTERVAL_S` |
| max_tasks_in_flight 乘数 | 2 | `DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` |
| Actor 最大重启次数 | -1（无限） | `max_restarts` |
| Actor 最大任务重试 | -1（无限） | `max_task_retries` |

---

## 四十一、常见问题排查

### 41.1 Actor 池扩容太慢

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

### 41.2 从已完成的任务日志中排查

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

### 41.3 Actor 池不扩容

排查清单：

| 检查项 | 验证方法 |
|---|---|
| max_size 是否已达上限 | 进度条 `Actors: N` 是否等于 `max_size` |
| 资源预算是否为 0 | 日志中 `budget_max_scale_up` 是否为 0 |
| 集群是否有空闲资源 | Dashboard 查看 CPU 使用率 |
| 反压是否阻止 | 日志中是否有 "operator exceeding resource quota" |
| 利用率是否达阈值 | `util` 是否 >= 1.75 |
| 是否有 pending Actor 未就绪 | 进度条 `pending > 0` 表示 Actor 正在创建中 |

### 41.4 完整链路图

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

# 附录：关键源码索引

## 架构与控制平面

| 主题 | 文件路径 | 关键方法/结构 |
|---|---|---|
| 进程类型定义 | `python/ray/_private/ray_constants.py` | `PROCESS_TYPE_*` 常量 |
| 节点启动流程 | `python/ray/_private/node.py` | `start_head_processes`, `start_ray_processes` |
| 服务启动实现 | `python/ray/_private/services.py` | `start_raylet` 等 |
| GCS 单线程确认 | `src/ray/gcs/gcs_server_main.cc` | `main_service(running_on_single_thread=true)` |
| Actor 集中调度 | `src/ray/gcs/actor/gcs_actor_scheduler.cc` | `Schedule()`, `SelectForwardingNode()` |

## 数据平面

| 主题 | 文件路径 | 关键方法/结构 |
|---|---|---|
| PlasmaStore | `src/ray/object_manager/plasma/store.h` | `CreateObject`, `SealObjects`, `DeleteObject`, `ProcessGetRequest` |
| 对象生命周期 | `src/ray/object_manager/plasma/object_lifecycle_manager.h` | `CreateObject`, `SealObject`, `DeleteObject`, `AddReference`, `RemoveReference` |
| LRU 淘汰 | `src/ray/object_manager/plasma/eviction_policy.h` | `LRUCache`, `EvictionPolicy`, `RequireSpace`, `ChooseObjectsToEvict` |
| 内存分配 | `src/ray/object_manager/plasma/plasma_allocator.h` | `Allocate`, `FallbackAllocate`, `GetFootprintLimit` |
| 对象表 | `src/ray/object_manager/plasma/object_store.h` | `ObjectStore`, `object_table_` |
| ObjectManager | `src/ray/object_manager/object_manager.h` | `Pull`, `Push`, `FreeObjects`, `HandleNodeRemoved` |
| PullManager | `src/ray/object_manager/pull_manager.h` | `Pull`, `CancelPull`, `OnLocationChange`, `UpdatePullsBasedOnAvailableMemory` |
| PushManager | `src/ray/object_manager/push_manager.h` | `StartPush`, `OnChunkComplete`, `HandleNodeRemoved` |
| 主拷贝管理 | `src/ray/raylet/local_object_manager.h` | `PinObjectsAndWaitForFree`, `SpillObjectUptoMaxThroughput`, `AsyncRestoreSpilledObject` |
| 外部存储 | `python/ray/_private/external_storage.py` | `ExternalStorage`, `spill_objects`, `restore_spilled_objects`, `delete_spilled_objects` |
| 对象目录（所有权） | `src/ray/object_manager/ownership_object_directory.cc` | `ReportObjectAdded()` |
| 引用计数器 | `src/ray/core_worker/reference_counter.h` | `Reference` 结构体, `object_id_refs_` |
| 对象恢复 | `src/ray/core_worker/object_recovery_manager.h` | `RecoverObject`, `PinOrReconstructObject`, `ReconstructObject` |
| CoreWorker 侧接口 | `src/ray/core_worker/store_provider/plasma_store_provider.h` | `Create`, `Seal`, `Release`, `Get` |
| 对象位置 | `src/ray/core_worker/common.h` | `ObjectLocation`, `node_ids_`, `spilled_url_` |
| 对象寻址 | `src/ray/core_worker/core_worker.cc` | `GetLocationFromOwner()`, `HandleGetObjectLocationsOwner()` |
| ObjectRef 序列化 | `python/ray/_private/serialization.py` | `object_ref_reducer`, `_object_ref_deserializer` |
| ObjectRef Cython | `python/ray/_raylet.pyx` | `serialize_object_ref`, `deserialize_and_register_object_ref` |

## 调度机制

| 组件 | 文件路径 |
|---|---|
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

## Ray Data 执行引擎

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

---

## 附录：设计权衡总结

### 三层平面分离

| 平面 | 核心组件 | 职责 | 扩展策略 |
|---|---|---|---|
| 控制平面 | GCS | Actor/PG 调度、节点管理、KV 存储 | 单线程，通过去中心化减负 |
| 数据平面 | Object Store + Owner RefCounter | 对象存储与元信息管理 | 去中心化，Owner 持有元信息 |
| 计算平面 | Raylet + CoreWorker | Task 调度与执行 | 去中心化，Raylet 独立调度 |

### 去中心化设计清单

- Task 调度 → Raylet 独立调度 + Spillback
- 资源视图 → Ray Syncer 节点间直接同步
- 对象元信息 → Owner CoreWorker 持有
- 对象寻址 → ObjectRef 自带 Owner 地址
- 对象副本 → 单拷贝 + 按需 Pull 复制
- 内存管理 → LRU 淘汰 + Spill 降级
- Ray Data 扩缩容 → DefaultActorAutoscaler 基于利用率独立决策

### 权衡与代价

| 设计选择 | 优势 | 代价 |
|---|---|---|
| GCS 单线程 | 简化并发控制 | Actor/PG 调度成为瓶颈 |
| Owner 单点 | 去中心化，无需分布式一致性 | Owner 故障导致对象不可用 |
| 单拷贝 + 按需复制 | 低内存开销，系统简单 | 无高可用保证 |
| LRU 淘汰（非副本） | 最大化内存利用率 | 节点宕机时数据可能丢失 |
| 快速恢复优于高可用 | 系统简单，恢复速度快 | 故障期间服务中断 |
| `ray.put()` 无血统 | 性能好，无 Task 重放开销 | Owner 故障后对象永久丢失 |
| Spillback 乐观扣减 | 零 RPC 调度，低延迟 | 可能被 Raylet 拒绝后重试 |
| Ray Data MAX_UPSCALING_DELTA=1 | 避免过激扩容导致资源浪费 | 大规模 Actor 池扩容缓慢 |
