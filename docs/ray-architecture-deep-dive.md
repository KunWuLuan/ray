# Ray 架构深度剖析：从模块启动到对象寻址

> 本文面向团队成员，逐步深入剖析 Ray 的进程架构、控制平面（GCS）、数据平面（Object Store）和计算平面（CoreWorker）的设计与权衡。

---

## 一、Ray 启动时的模块构成

### 1.1 进程类型一览

Ray 集群中的进程类型定义在 `python/ray/_private/ray_constants.py` 中：

| 进程类型常量 | 说明 |
|---|---|
| `PROCESS_TYPE_GCS_SERVER` | GCS Server，全局控制服务 |
| `PROCESS_TYPE_MONITOR` | Monitor，监控集群健康状态 |
| `PROCESS_TYPE_RAY_CLIENT_SERVER` | Ray Client Server，支持远程客户端连接 |
| `PROCESS_TYPE_DASHBOARD` | Dashboard/API Server，Web 管理界面 |
| `PROCESS_TYPE_RAYLET` | Raylet，每节点资源管理与任务调度核心 |
| `PROCESS_TYPE_DASHBOARD_AGENT` | Dashboard Agent，每节点指标采集 |
| `PROCESS_TYPE_RUNTIME_ENV_AGENT` | Runtime Env Agent，每节点运行时环境管理 |
| `PROCESS_TYPE_LOG_MONITOR` | Log Monitor，每节点日志收集 |
| `PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER` | CoreWorker Driver，用户脚本入口 |
| `PROCESS_TYPE_PYTHON_CORE_WORKER` | CoreWorker Worker，实际执行 Task/Actor |

### 1.2 Head 节点启动顺序

`ray start --head` 的启动流程（`python/ray/_private/node.py` → `start_head_processes`）：

```
1. GCS Server          — 集群元数据管理（外部 Redis 持久化）
2. Monitor             — 集群健康监控、自动扩缩容触发
3. Ray Client Server   — 远程客户端连接入口（ray.init("ray://...")）
4. Dashboard/API Server— Web UI + REST API
5. Raylet              — 本节点资源管理 + 任务调度
   ├── Dashboard Agent   — 指标采集（作为 Raylet 子进程）
   └── Runtime Env Agent — 运行时环境准备（作为 Raylet 子进程）
6. Log Monitor         — 日志流式收集
```

### 1.3 Worker 节点启动顺序

```
1. Log Monitor
2. Raylet
   ├── Dashboard Agent
   └── Runtime Env Agent
```

> **注意**：CoreWorker 不由 `ray start` 启动，而是由 `ray.init()` 或 Raylet 按需 fork 创建。

### 1.4 运行时进程

用户代码通过 `ray.init()` 连接集群后，以下进程在运行时被创建：

- **CoreWorker Driver**：运行在调用 `ray.init()` 的机器上，与用户脚本同生共死
- **CoreWorker Worker**：由 Raylet 按需 fork，执行 Task 或作为 Actor 常驻

---

## 二、GCS 的职责与瓶颈分析

### 2.1 GCS 八大核心职责

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

### 2.2 关键设计：单线程事件循环

```cpp
// gcs_server_main.cc
instrumented_io_context main_service(
    /*enable_metrics=*/...,
    /*running_on_single_thread=*/true,   // ← 所有 RPC handler 串行执行
    "gcs_server_main_io_context");
```

所有 GCS 的 RPC handler 在**同一个线程**上串行执行，这是 GCS 性能分析的核心约束。

### 2.3 高负载下的七个潜在瓶颈

| 瓶颈点 | 触发条件 | 影响 |
|---|---|---|
| Actor 批量创建 | 大量 Actor 同时创建 | GCS 集中调度，`Schedule()` 串行执行 |
| Placement Group 调度 | 大量 PG 创建请求 | 资源分配算法复杂度高 |
| 节点频繁上下线 | 大规模节点故障/扩容 | 节点心跳与状态更新排队 |
| KV 存储高频读写 | Dashboard、Runtime Env 频繁访问 | KV 操作阻塞事件循环 |
| PubSub 消息洪流 | 大量对象位置变更通知 | PubSub 路由排队 |
| GCS RPC 限流 | `gcs_max_active_rpcs_per_handler` 触发 | 请求被拒绝 |
| 持久化延迟 | Redis 写入瓶颈 | GCS 恢复变慢 |

### 2.4 Ray 的去中心化优化

为减轻 GCS 负担，Ray 在以下方面做了去中心化设计：

- **普通 Task 调度**：由各节点 Raylet 独立调度，不经过 GCS
- **Spillback 机制**：Raylet 直接选择远程节点提交任务，无需 GCS 仲裁
- **Ray Syncer 协议**：节点间每 100ms 直接同步资源视图，绕过 GCS
- **对象元信息管理**：基于所有权模型，存储在 Owner CoreWorker 而非 GCS（详见下文）

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

## 三、Object Store 元信息管理：基于所有权的去中心化设计

### 3.1 核心结论

**对象元信息不在 GCS 中，而是存储在 Owner CoreWorker 的 ReferenceCounter 中。**

GCS 在对象管理中仅承担两个辅助角色：
1. **PubSub 消息路由**：对象位置变更通知的发布/订阅中介
2. **节点存活信息**：提供节点是否存活的查询

### 3.2 证据：对象位置上报的目标

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

### 3.3 Owner CoreWorker 中的引用计数器

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

---

## 四、Owner CoreWorker：单点设计与容错

### 4.1 Owner 是单点

**每个对象有且仅有一个 Owner CoreWorker，Owner 不可迁移。**

判断逻辑：ReferenceCounter 中的 `owned_by_us_` 字段，通过比较 `owner_address_` 与本地 `rpc_address_` 来确定。

### 4.2 为什么启动模块图中没有 CoreWorker

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

### 4.3 Owner 故障的容错机制

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

### 4.4 设计哲学：快速恢复优于高可用

Ray 选择 **"快速恢复"（Fast Recovery）** 而非 **"高可用"（High Availability）**：
- Owner 故障 → 对象暂时不可用 → 通过血统重建快速恢复
- 而非：Owner 主从切换 → 保持服务连续

这种设计简化了系统复杂度，代价是 Owner 故障期间对象短暂不可用。

---

## 五、对象寻址：如何找到元信息在哪个节点

### 5.1 核心机制：Owner 地址嵌入 ObjectRef

**ObjectRef 自身就携带了 Owner 的 RPC 地址（IP + Port + WorkerID），随引用一起在网络中传递。**

```
ObjectRef 序列化内容 = {
    binary:          对象 ID（20 字节）
    call_site:       调用位置
    owner_address:   Owner CoreWorker 的 RPC 地址  ← 关键
    object_status:   对象状态快照
}
```

### 5.2 完整寻址流程

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
// 3.1 从本地 RefCounter 取出 Owner 地址
rpc::Address owner_address;
GetOwnerAddress(object_id, &owner_address);

// 3.2 直接向 Owner CoreWorker 发送 RPC
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

### 5.3 寻址流程图

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

### 5.4 关键设计要点

| 要点 | 说明 |
|---|---|
| **无需中心化发现** | Owner 地址随 ObjectRef 传递，不需要查 GCS |
| **直接点对点 RPC** | Worker B 直接 RPC Worker A，无中间跳转 |
| **批量查询优化** | `GetLocationFromOwner` 按 Owner 分组批量请求 |
| **PubSub 增量推送** | 订阅后，Owner 位置变更通过 PubSub 增量推送 |

---

## 六、进程关系全景图

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

---

## 七、设计要点总结

### 7.1 三层平面分离

| 平面 | 核心组件 | 职责 | 扩展策略 |
|---|---|---|---|
| 控制平面 | GCS | Actor/PG 调度、节点管理、KV 存储 | 单线程，通过去中心化减负 |
| 数据平面 | Object Store + Owner RefCounter | 对象存储与元信息管理 | 去中心化，Owner 持有元信息 |
| 计算平面 | Raylet + CoreWorker | Task 调度与执行 | 去中心化，Raylet 独立调度 |

### 7.2 去中心化设计清单

- Task 调度 → Raylet 独立调度 + Spillback
- 资源视图 → Ray Syncer 节点间直接同步
- 对象元信息 → Owner CoreWorker 持有
- 对象寻址 → ObjectRef 自带 Owner 地址

### 7.3 权衡与代价

| 设计选择 | 优势 | 代价 |
|---|---|---|
| GCS 单线程 | 简化并发控制 | Actor/PG 调度成为瓶颈 |
| Owner 单点 | 去中心化，无需分布式一致性 | Owner 故障导致对象不可用 |
| 快速恢复优于高可用 | 系统简单，恢复速度快 | 故障期间服务中断 |
| `ray.put()` 无血统 | 性能好，无 Task 重放开销 | Owner 故障后对象永久丢失 |

---

## 附录：关键代码索引

| 主题 | 文件 | 关键方法/结构 |
|---|---|---|
| 进程类型定义 | `python/ray/_private/ray_constants.py` | `PROCESS_TYPE_*` 常量 |
| 节点启动流程 | `python/ray/_private/node.py` | `start_head_processes`, `start_ray_processes` |
| 服务启动实现 | `python/ray/_private/services.py` | `start_raylet` 等 |
| GCS 单线程确认 | `src/ray/gcs/gcs_server_main.cc` | `main_service(running_on_single_thread=true)` |
| Actor 集中调度 | `src/ray/gcs/actor/gcs_actor_scheduler.cc` | `Schedule()`, `SelectForwardingNode()` |
| 对象目录（所有权） | `src/ray/object_manager/ownership_object_directory.cc` | `ReportObjectAdded()` |
| 引用计数器 | `src/ray/core_worker/reference_counter.h` | `Reference` 结构体, `object_id_refs_` |
| 对象恢复 | `src/ray/core_worker/object_recovery_manager.h` | `ReconstructObject()` |
| 对象寻址 | `src/ray/core_worker/core_worker.cc` | `GetLocationFromOwner()`, `HandleGetObjectLocationsOwner()` |
| ObjectRef 序列化 | `python/ray/_private/serialization.py` | `object_ref_reducer`, `_object_ref_deserializer` |
| ObjectRef Cython | `python/ray/_raylet.pyx` | `serialize_object_ref`, `deserialize_and_register_object_ref` |
