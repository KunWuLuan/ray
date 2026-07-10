# Ray Object Store 深度剖析：从内存分配到跨节点传输

> 本文面向团队成员，逐步深入剖析 Ray Object Store 的内存架构、对象生命周期、副本管理、跨节点传输、溢出恢复与容错设计。

---

## 一、Object Store 在 Ray 架构中的定位

### 1.1 三层平面中的数据平面

| 平面 | 核心组件 | 职责 |
|---|---|---|
| 控制平面 | GCS | Actor/PG 调度、节点管理 |
| **数据平面** | **Object Store + Owner RefCounter** | **对象存储与元信息管理** |
| 计算平面 | Raylet + CoreWorker | Task 调度与执行 |

Object Store 是 Ray 数据平面的核心，负责在集群中存储和传输分布式对象。每个节点运行一个本地 Object Store 实例（基于 Plasma），通过共享内存（mmap）为同节点的 CoreWorker 提供零拷贝数据访问。

### 1.2 进程拓扑

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

### 1.3 关键代码索引

| 组件 | 文件路径 |
|---|---|
| ObjectManager | `src/ray/object_manager/object_manager.h` |
| PlasmaStore | `src/ray/object_manager/plasma/store.h` |
| ObjectStore | `src/ray/object_manager/plasma/object_store.h` |
| ObjectLifecycleManager | `src/ray/object_manager/plasma/object_lifecycle_manager.h` |
| EvictionPolicy (LRU) | `src/ray/object_manager/plasma/eviction_policy.h` |
| PlasmaAllocator | `src/ray/object_manager/plasma/plasma_allocator.h` |
| PullManager | `src/ray/object_manager/pull_manager.h` |
| PushManager | `src/ray/object_manager/push_manager.h` |
| LocalObjectManager | `src/ray/raylet/local_object_manager.h` |
| ExternalStorage | `python/ray/_private/external_storage.py` |
| ReferenceCounter | `src/ray/core_worker/reference_counter.h` |

---

## 二、Plasma Store：内存架构与分配

### 2.1 两级内存分配器

PlasmaAllocator 使用两级分配策略（见 `plasma_allocator.h`）：

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

**关键设计：**
- 主分配器从 `/dev/shm` 预先 mmap 一大块内存，通过 `dlmalloc` 管理内部分配
- Fallback 分配器从磁盘文件 mmap，不受 footprint limit 限制，但计入总量统计
- CoreWorker 通过 mmap 同一文件实现零拷贝读取

### 2.2 内存容量管理

```
PlasmaStore 可用内存 = FootprintLimit - NumBytesInUse + NumBytesUnsealed
```

其中（见 `store.h`）：
- `FootprintLimit`：配置的内存上限（`object_store_memory` 参数）
- `NumBytesInUse`：已密封且被引用的对象总大小
- `NumBytesUnsealed`：已创建但未密封的对象大小（不计入在用，因为可能被驱逐）

### 2.3 OOM 处理流程

当创建对象时内存不足：

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

### 2.4 线程模型

PlasmaStore 运行在 Raylet 进程的独立线程中，通过 `absl::Mutex` 与 Raylet 主线程同步：

```cpp
// store.h
mutable absl::Mutex mutex_;  // 保护 PlasmaStore 线程安全
// Raylet 的 LocalObjectManager 需要访问 PlasmaStore 时通过此 mutex 同步
```

---

## 三、对象生命周期

### 3.1 状态机

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

### 3.2 创建与密封

对象创建由 CoreWorker 发起，通过 IPC 请求本地 PlasmaStore：

```cpp
// plasma_store_provider.h - CoreWorker 侧接口
Status Create(metadata, data_size, object_id, owner_address, data, created_by_worker, is_mutable);
Status Seal(object_id);     // 密封后对象变为不可变
Status Release(object_id);  // 释放第一个引用，此后对象可被 LRU 淘汰
```

**关键约束（见 `plasma_store_provider.h:108-111`）：**
> *"The caller must subsequently call Release() to release the first reference to the created object. Until then, the object is pinned and cannot be evicted."*

### 3.3 引用计数与 Pin 机制

PlasmaStore 维护每个对象的引用计数（`object_lifecycle_manager.h`）：

```cpp
// 对象的引用计数规则：
// - AddReference: ref_count++，对象被 Get 时调用
// - RemoveReference: ref_count--，对象被 Release 时调用
// - ref_count == 0: 对象变为可淘汰（evictable），但不立即删除
// - ref_count > 0: 对象被 pin，不可被 LRU 淘汰
```

Raylet 层面还有一层 pin 机制（`local_object_manager.h`）：
- 主拷贝被 raylet pin 住，直到 Owner 释放引用
- 副拷贝（secondary copy）不被 raylet pin，仅受 PlasmaStore 引用计数管理

### 3.4 删除机制

删除有两条路径：

| 路径 | 触发条件 | 行为 |
|---|---|---|
| **显式删除** | Owner 引用计数降为 0，全局 FreeObjects 广播 | 所有节点删除该对象的所有拷贝 |
| **LRU 淘汰** | 本地 PlasmaStore 内存不足 | 仅淘汰本地 `ref_count==0` 的对象 |

显式删除的广播流程（见 `object_manager.h`）：

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

## 四、对象副本管理：单拷贝 + 按需复制

### 4.1 核心结论

**Ray Object Store 的对象数据本质上是单拷贝（single-copy）设计，不存在主动的多副本复制机制。**

### 4.2 Primary Copy 与 Secondary Copy

代码中明确区分了两种拷贝（见 `object_manager.h:262-264`）：

```cpp
/// Return IDs of local plasma-resident objects whose owner matches the given
/// worker or node. Includes both primary copies (also tracked by
/// LocalObjectManager) and secondary copies pulled from other nodes.
std::vector<ObjectID> GetLocalObjectsOwnedBy(const WorkerID &worker_id) const override;
```

| 类型 | 创建方式 | 管理者 | Pin 机制 | 生命周期 |
|---|---|---|---|---|
| **Primary Copy** | 对象创建时所在节点 | LocalObjectManager | Raylet pin | Owner 释放引用时删除 |
| **Secondary Copy** | 其他节点 Pull 拉取 | 仅 PlasmaStore 引用计数 | 无 Raylet pin | LRU 淘汰或全局 Free |

### 4.3 副本数量无硬性上限

`ObjectLocation` 中的 `node_ids_` 是一个 vector（见 `common.h:297-300`）：

```cpp
/// The IDs of the nodes that this object appeared on or was evicted by.
const std::vector<NodeID> node_ids_;
```

Python 侧暴露的接口也确认了这一点（`_raylet.pyx:501-502`）：

```python
# node_ids: The hex IDs of the nodes that have a copy of this object.
```

**如果有 N 个节点需要访问同一对象，集群中最多存在 N 份拷贝（1 primary + N-1 secondary）。**

### 4.4 副拷贝的删除时机

副拷贝**不会在使用后立即删除**，而是保留在本地 Plasma Store 中，直到：

| 删除路径 | 触发条件 | 影响范围 |
|---|---|---|
| **LRU 淘汰** | 本地内存不足，`ref_count==0` 的对象被淘汰 | 仅删除本地副拷贝 |
| **全局 Free** | Owner 引用计数降为 0，广播 FreeObjects | 删除所有节点上的所有拷贝 |

策略总结：**"拉来就用，用完留着，内存不够再 LRU 淘汰"**。这避免了同一对象被反复拉取的开销，但可能导致集群中存在多份冗余拷贝。

### 4.5 副本管理全景图

```
Node A (Owner)              Node B                    Node C
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ PlasmaStore     │    │ PlasmaStore     │    │ PlasmaStore     │
│                 │    │                 │    │                 │
│ ┌─────────────┐│    │ ┌─────────────┐│    │ ┌─────────────┐│
│ │ Primary Copy ││    │ │Secondary    ││    │ │Secondary    ││
│ │ (pinned by   ││    │ │ Copy        ││    │ │ Copy        ││
│ │  raylet)     ││    │ │ (ref_count  ││    │ │ (ref_count  ││
│ │              ││    │ │  may be 0)  ││    │ │  may be 0)  ││
│ └─────────────┘│    │ └─────────────┘│    │ └─────────────┘│
│                │    │                 │    │                 │
│ LocalObject    │    │ (无 LocalObject │    │ (无 LocalObject │
│ Manager 管理   │    │  Manager 管理)  │    │  Manager 管理)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 五、跨节点传输：Push 与 Pull

### 5.1 Pull 机制（拉取）

当节点需要远端对象时，通过 PullManager 发起拉取（见 `pull_manager.h`）：

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

### 5.2 PullManager 的配额管理

PullManager 维护内存配额，防止拉取过多对象导致 OOM（见 `pull_manager.h:446-466`）：

```cpp
/// The total number of bytes that we are currently pulling.
/// To avoid starvation, this is always less than the available capacity
/// in the local object store.
int64_t num_bytes_being_pulled_ = 0;

/// The total number of bytes that is available to store objects
/// that we are pulling.
int64_t num_bytes_available_;
```

**三优先级队列**（见 `pull_manager.h:456-460`）：

| 优先级 | 来源 | 说明 |
|---|---|---|
| 最高 | `get_request_bundles_` | `ray.get()` 请求，允许 fallback 分配 |
| 中 | `wait_request_bundles_` | `ray.wait()` 请求 |
| 最低 | `task_argument_bundles_` | 任务参数拉取 |

> 设计意图：优先满足 worker 的 `ray.get` 请求（释放 worker 资源），避免任务参数拉取导致死锁。

### 5.3 Push 机制（推送）

PushManager 管理出站推送的限流与去重（见 `push_manager.h`）：

```cpp
class PushManager {
  /// Max number of chunks in flight allowed.
  const int64_t max_chunks_in_flight_;

  /// Duplicate concurrent pushes to the same destination will be suppressed.
  /// (去重：同一对象到同一节点的并发推送会被合并)
  absl::flat_hash_map<NodeID,
      absl::flat_hash_map<ObjectID, std::list<PushState>::iterator>>
      push_state_map_;
};
```

**分块传输：** 对象被切分为固定大小的 chunk（`object_chunk_size` 配置），逐块传输以控制内存峰值。

### 5.4 传输流程时序

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

## 六、内存溢出与外部存储

### 6.1 Spill 触发条件

当 PlasmaStore 内存使用超过阈值时，LocalObjectManager 触发 spill（见 `local_object_manager.h`）：

```cpp
/// Spill objects as much as possible as fast as possible up to the max throughput.
void SpillObjectUptoMaxThroughput() override;

bool TryToSpillObjects();
// 溢出条件：
// 1. 对象必须是 primary copy（被 raylet pin）
// 2. 对象必须已密封 (sealed)
// 3. 对象的 ref_count 仅来自 raylet（IsPlasmaObjectSpillable）
```

**Spill 安全检查**（`store.h:75-81`）：

```cpp
/// Return true if the given object id has only one reference.
/// Only one reference means there's only a raylet that pins the object
/// so it is safe to spill the object.
bool IsObjectSpillable(const ObjectID &object_id);
```

### 6.2 外部存储后端

Ray 支持多种外部存储后端（见 `external_storage.py`）：

| 后端 | 配置 | 特点 |
|---|---|---|
| **本地文件系统** | `filesystem` | 默认，每个节点独立存储 |
| **分布式存储** | `s3` / `gs` / `azure` | 共享存储，任何节点可恢复 |
| **NullStorage** | — | 不 spill（开发/测试用） |

Spill 时多个对象融合为一个文件以优化性能（`external_storage.py:252-264`）：

```python
def spill_objects(self, object_refs, owner_addresses):
    # 轮询选择目录路径
    self._current_directory_index = (self._current_directory_index + 1) % len(
        self._directory_paths)
    directory_path = self._directory_paths[self._current_directory_index]
    filename = _get_unique_spill_filename(object_refs)
    url = f"{os.path.join(directory_path, filename)}"
    with open(url, "wb", buffering=self._buffer_size) as f:
        return self._write_multiple_objects(f, object_refs, owner_addresses, url)
```

### 6.3 Spill 与 Restore 流程

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

### 6.4 Spilled 对象的删除

Spilled 对象在外部存储中不会永久保留，当 Owner 释放引用时删除（见 `local_object_manager.h:341-353`）：

```cpp
/// A list of object id and url pairs that need to be deleted.
/// We don't instantly delete objects when it goes out of scope from external
/// storages because those objects could be still in progress of spilling.
std::queue<ObjectID> spilled_object_pending_delete_;

/// Mapping from object id to url_with_offsets.
absl::flat_hash_map<ObjectID, std::string> spilled_objects_url_;

/// Base URL -> ref_count. Multiple objects may share one spilled file.
/// We need ref count to avoid deleting the file before all objects are freed.
absl::flat_hash_map<std::string, uint64_t> url_ref_count_;
```

---

## 七、LRU 淘汰策略

### 7.1 LRUCache 实现

PlasmaStore 使用 LRU（最近最少使用）策略管理可淘汰对象（见 `eviction_policy.h`）：

```cpp
class LRUCache {
  /// A doubly-linked list containing the items in LRU order.
  typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
  ItemList item_list_;

  /// Hash table for O(1) lookup.
  absl::flat_hash_map<ObjectID, ItemList::iterator> item_map_;

  int64_t capacity_;           // 缓存容量
  int64_t used_capacity_;     // 已用容量
  int64_t num_evictions_total_;
  int64_t bytes_evicted_total_;
};
```

### 7.2 淘汰决策流程

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

### 7.3 对象访问与 LRU 交互

```cpp
// eviction_policy.h
class IEvictionPolicy {
  /// 对象创建时加入 LRU
  virtual void ObjectCreated(const ObjectID &object_id) = 0;

  /// 对象开始被使用时，从 LRU 移除（不可淘汰）
  virtual void BeginObjectAccess(const ObjectID &object_id) = 0;

  /// 对象不再被使用时，重新加入 LRU（可淘汰）
  virtual void EndObjectAccess(const ObjectID &object_id) = 0;
};
```

**关键点：**
- `ref_count > 0` 的对象不在 LRU 中，不可被淘汰
- `ref_count == 0` 的对象在 LRU 中，按最近访问时间排序
- 淘汰从 LRU 最久未使用的对象开始

---

## 八、对象元信息与位置追踪

### 8.1 去中心化的元信息管理

**对象元信息不在 GCS 中，而是存储在 Owner CoreWorker 的 ReferenceCounter 中。**

GCS 仅承担两个辅助角色：
1. PubSub 消息路由（对象位置变更通知）
2. 节点存活信息查询

### 8.2 Reference 结构体

```cpp
// reference_counter.h
struct Reference {
  bool owned_by_us_ = false;                    // 是否是 Owner
  std::optional<NodeID> pinned_at_node_id_;     // 主拷贝在哪个节点
  std::string spilled_url;                      // spill 到外部存储的 URL
  NodeID spilled_node_id = NodeID::Nil();       // spill 到哪个节点
  size_t local_ref_count = 0;                   // 本地引用计数
  size_t submitted_task_ref_count = 0;          // 提交任务的引用计数
  size_t lineage_ref_count = 0;                 // 血统引用计数
  absl::flat_hash_set<NodeID> locations;        // 对象位置集合（所有有拷贝的节点）
};
```

### 8.3 位置上报流程

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

### 8.4 对象状态分类

Owner 的 ReferenceCounter 跟踪对象的多种状态（见 `reference_counter.h:67-76`）：

```cpp
std::atomic<size_t> owned_objects_pending_creation_{0};  // 创建中
std::atomic<size_t> owned_objects_in_memory_{0};          // 在内存中
std::atomic<size_t> owned_objects_spilled_{0};            // 已溢出
std::atomic<size_t> owned_objects_in_plasma_{0};          // 在 Plasma 中

std::atomic<int64_t> owned_objects_size_in_memory_{0};    // 内存中总大小
std::atomic<int64_t> owned_objects_size_spilled_{0};      // 溢出总大小
std::atomic<int64_t> owned_objects_size_in_plasma_{0};    // Plasma 中总大小
```

---

## 九、容错与恢复

### 9.1 对象丢失的场景

| 场景 | 丢失的数据 | 恢复方式 | 失败条件 |
|---|---|---|---|
| 副拷贝节点宕机 | Secondary Copy | 无需恢复（其他节点或 spill 可重新拉取） | 唯一副拷贝且未 spill |
| Primary 节点宕机 | Primary Copy | 从其他节点的副拷贝 Pin 恢复 | 无副拷贝 |
| Primary + 所有副拷贝丢失 | 所有内存拷贝 | 从 Spill 恢复 | 未 Spill |
| 所有拷贝 + Spill 丢失 | 所有数据 | Lineage 重建（重新执行 Task） | 血统被驱逐 |
| Owner CoreWorker 死亡 | 元信息 + 引用 | **不可恢复**（`ray.put` 对象） | `ray.put` 创建的对象 |

### 9.2 ObjectRecoveryManager

恢复管理器按优先级尝试恢复（见 `object_recovery_manager.h`）：

```cpp
class ObjectRecoveryManager {
  /// Pin a new copy for a lost object from the given locations or,
  /// if that fails, attempt to reconstruct it by resubmitting the task.
  void PinOrReconstructObject(const ObjectID &object_id,
                              std::vector<rpc::Address> locations);

  /// 1. 尝试从其他节点的副拷贝 Pin 恢复
  void PinExistingObjectCopy(const ObjectID &object_id,
                             const rpc::Address &raylet_address,
                             std::vector<rpc::Address> other_locations);

  /// 2. 如果所有副拷贝都失败，通过血统重建
  void ReconstructObject(const ObjectID &object_id);
};
```

### 9.3 恢复决策流程

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

### 9.4 设计哲学：快速恢复优于高可用

| 设计选择 | 优势 | 代价 |
|---|---|---|
| 单拷贝 + 按需复制 | 低内存开销，系统简单 | 无高可用保证 |
| LRU 淘汰（非副本） | 最大化内存利用率 | 节点宕机时数据可能丢失 |
| Lineage 重建（非副本） | 无额外存储开销 | 恢复期间对象不可用 |
| `ray.put()` 无血统 | 性能好，无重放开销 | Owner 故障后永久丢失 |

---

## 十、配置参数速查

### 10.1 Object Store 内存配置

| 参数 | 默认值 | 说明 |
|---|---|---|
| `object_store_memory` | 总内存的 30%（min 75MB） | Plasma Store 内存上限 |
| `plasma_directory` | `/dev/shm` | 共享内存目录 |
| `fallback_directory` | `/tmp` | 磁盘回退目录 |
| `huge_pages` | false | 启用大页内存 |

### 10.2 传输配置

| 参数 | 默认值 | 说明 |
|---|---|---|
| `object_chunk_size` | 10485760 (10MB) | 对象分块传输大小 |
| `max_bytes_in_flight` | 104857600 (100MB) | 最大飞行字节数 |
| `push_timeout_ms` | -1 (无限) | 推送超时 |
| `pull_timeout_ms` | 60000 (60s) | 拉取重试间隔 |
| `rpc_service_threads_number` | 1 | RPC 服务线程数 |

### 10.3 Spill 配置

| 参数 | 默认值 | 说明 |
|---|---|---|
| `min_spilling_size` | 104857600 (100MB) | 最小溢出批量 |
| `max_spilling_file_size_bytes` | -1 (无限) | 单次溢出文件上限 |
| `max_fused_object_count` | 200 | 单文件最多融合对象数 |
| `max_io_workers` | 1 | 最大 IO Worker 数 |
| `free_objects_batch_size` | 32 | 批量释放对象数 |
| `free_objects_period_ms` | 1000 | 释放检查间隔 |

---

## 十一、设计要点总结

### 11.1 设计原则

| 原则 | 实现 |
|---|---|
| **零拷贝** | Plasma 共享内存 mmap，同节点 CoreWorker 直接读 |
| **按需复制** | 非主动多副本，消费驱动的 Pull 拉取 |
| **去中心化** | 元信息在 Owner CoreWorker，不依赖 GCS |
| **LRU 淘汰** | `ref_count==0` 的对象可被自动淘汰 |
| **Spill 降级** | 内存不足时溢出到磁盘，牺牲性能换可用性 |
| **Lineage 容错** | 重新执行 Task 恢复对象，非数据副本 |

### 11.2 与传统分布式存储的对比

| 特性 | Ray Object Store | HDFS / 分布式 KV |
|---|---|---|
| 副本数 | 1（按需增加） | 3（固定） |
| 复制方式 | Pull on-demand | 主动复制 |
| 一致性 | 最终一致（Owner 单点） | 强一致（Quorum） |
| 容错 | Lineage 重建 | 副本修复 |
| 淘汰策略 | LRU | 不淘汰（持久化） |
| 访问延迟 | 本地共享内存（μs级） | 网络（ms级） |
| 适用场景 | 短生命周期中间结果 | 长期持久化数据 |

### 11.3 组件协作全景图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Owner CoreWorker                            │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  ReferenceCounter                                           │   │
│  │  ├── locations: {NodeA, NodeB, ...}  ← 对象位置集合         │   │
│  │  ├── spilled_url: "s3://..."          ← Spill 位置          │   │
│  │  ├── pinned_at_node_id: NodeA        ← 主拷贝节点          │   │
│  │  └── ref_count: 3                    ← 引用计数            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  ObjectRecoveryManager                                     │   │
│  │  ├── PinExistingObjectCopy()  ← 从副拷贝恢复               │   │
│  │  └── ReconstructObject()      ← 血统重建                  │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
          │                                           │
     位置查询 RPC                              FreeObjects 广播
          │                                           │
          ▼                                           ▼
┌─────────────────────┐               ┌─────────────────────────────┐
│  Node A (Primary)   │               │  Node B (Secondary)         │
│  ┌─────────────────┐│               │  ┌─────────────────┐        │
│  │ PlasmaStore     ││               │  │ PlasmaStore     │        │
│  │ ┌─────────────┐ ││               │  │ ┌─────────────┐ │        │
│  │ │Primary Copy │ ││  ← Push/Pull→ │  │ │Secondary    │ │        │
│  │ │(pinned)     │ ││    分块传输    │  │ │Copy         │ │        │
│  │ └─────────────┘ ││               │  │ └─────────────┘ │        │
│  └─────────────────┘│               │  └─────────────────┘        │
│  ┌─────────────────┐│               │  LRU 可淘汰                   │
│  │LocalObjectMgr   ││               └─────────────────────────────┘
│  │(pin/spill/restore)│
│  └─────────────────┘│
│  ┌─────────────────┐│
│  │ ExternalStorage ││  ← Spill 到磁盘/S3
│  │ spilled_url     ││
│  └─────────────────┘│
└─────────────────────┘
```

---

## 附录：关键代码索引

| 主题 | 文件 | 关键方法/结构 |
|---|---|---|
| PlasmaStore RPC | `src/ray/object_manager/plasma/store.h` | `CreateObject`, `SealObjects`, `DeleteObject`, `ProcessGetRequest` |
| 对象生命周期 | `src/ray/object_manager/plasma/object_lifecycle_manager.h` | `CreateObject`, `SealObject`, `DeleteObject`, `AddReference`, `RemoveReference` |
| LRU 淘汰 | `src/ray/object_manager/plasma/eviction_policy.h` | `LRUCache`, `EvictionPolicy`, `RequireSpace`, `ChooseObjectsToEvict` |
| 内存分配 | `src/ray/object_manager/plasma/plasma_allocator.h` | `Allocate`, `FallbackAllocate`, `GetFootprintLimit` |
| 对象表 | `src/ray/object_manager/plasma/object_store.h` | `ObjectStore`, `object_table_` |
| Pull 管理 | `src/ray/object_manager/pull_manager.h` | `Pull`, `CancelPull`, `OnLocationChange`, `UpdatePullsBasedOnAvailableMemory` |
| Push 管理 | `src/ray/object_manager/push_manager.h` | `StartPush`, `OnChunkComplete`, `HandleNodeRemoved` |
| 主拷贝管理 | `src/ray/raylet/local_object_manager.h` | `PinObjectsAndWaitForFree`, `SpillObjectUptoMaxThroughput`, `AsyncRestoreSpilledObject` |
| 跨节点传输 | `src/ray/object_manager/object_manager.h` | `Pull`, `Push`, `FreeObjects`, `HandleNodeRemoved` |
| 外部存储 | `python/ray/_private/external_storage.py` | `ExternalStorage`, `spill_objects`, `restore_spilled_objects`, `delete_spilled_objects` |
| 引用计数 | `src/ray/core_worker/reference_counter.h` | `Reference` 结构体, `object_id_refs_` |
| 对象恢复 | `src/ray/core_worker/object_recovery_manager.h` | `RecoverObject`, `PinOrReconstructObject`, `ReconstructObject` |
| CoreWorker 侧接口 | `src/ray/core_worker/store_provider/plasma_store_provider.h` | `Create`, `Seal`, `Release`, `Get` |
| 对象位置 | `src/ray/core_worker/common.h` | `ObjectLocation`, `node_ids_`, `spilled_url_` |
