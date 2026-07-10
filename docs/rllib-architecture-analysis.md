# Ray RLlib 架构分析与详细说明文档

## 1. 概述

RLlib 是 Ray 生态系统中的分布式强化学习库，提供可扩展的 RL 训练和推理框架。它利用 Ray 的分布式计算能力，将环境采样和模型训练分布到集群中的多个节点上，支持从单 agent 到多 agent、从 on-policy 到 off-policy 的多种 RL 算法。

### 1.1 设计理念

- **分布式优先**：所有核心组件（采样、训练、推理）均可水平扩展
- **双 API 栈并存**：旧 API 栈（Policy/RolloutWorker）与新 API 栈（RLModule/EnvRunner/ConnectorV2/Learner）并存，新 API 栈为默认推荐
- **框架无关**：同时支持 PyTorch（主要支持）和 TensorFlow（旧栈支持）
- **算法可扩展**：通过继承 `Algorithm` 和 `AlgorithmConfig`，新增算法只需实现特定方法

### 1.2 支持的算法

| 算法 | 类型 | 说明 |
|------|------|------|
| PPO | On-policy | 近端策略优化，最常用的默认算法 |
| APPO | On-policy | 异步近端策略优化，支持分布式采样 |
| IMPALA | Off-policy (异步) | 重要性加权 actor-learner 架构 |
| DQN | Off-policy | 深度 Q 网络，支持经验回放 |
| SAC | Off-policy | Soft Actor-Critic，最大熵强化学习 |
| DreamerV3 | Model-based | 世界模型 + 想象空间学习 |
| MARWIL | Offline | 监督模仿 + 重要性加权 |
| BC | Offline | 行为克隆 |
| CQL | Offline | 保守 Q 学习 |
| IQL | Offline | 离线 Q 学习 |

---

## 2. 整体架构

RLlib 的架构围绕三个核心角色构建：**算法编排器（Algorithm）**、**环境采样器（EnvRunner）** 和 **学习器（Learner）**。

```
┌─────────────────────────────────────────────────────────┐
│                     Algorithm                           │
│  (训练循环编排、评估、检查点、指标聚合)                    │
│                                                         │
│  ┌──────────────┐         ┌──────────────────────┐      │
│  │ EnvRunnerGroup│        │    LearnerGroup       │      │
│  │  (N个采样Actor)│        │  (M个训练Actor/GPU)   │      │
│  └──────┬───────┘         └──────────┬───────────┘      │
│         │                            │                   │
│         ▼                            ▼                   │
│  ┌──────────────┐         ┌──────────────────────┐      │
│  │  EnvRunner    │         │      Learner          │      │
│  │ - Environment │         │ - RLModule (训练)     │      │
│  │ - RLModule    │         │ - Optimizer          │      │
│  │   (推理)      │         │ - Loss 计算           │      │
│  │ - Connectors  │         │ - Connectors         │      │
│  │ - Episode 管理│         │                      │      │
│  └──────────────┘         └──────────────────────┘      │
│                                                         │
│  数据流: EnvRunner.sample() ──→ episodes ──→           │
│         LearnerGroup.update(episodes) ──→ 训练           │
│         Learner → EnvRunner 权重同步                    │
└─────────────────────────────────────────────────────────┘
```

### 2.1 新旧 API 栈对比

| 维度 | 旧 API 栈 | 新 API 栈（默认） |
|------|-----------|-------------------|
| 模型抽象 | `Policy` (TFPolicy/TorchPolicy) | `RLModule` + `MultiRLModule` |
| 采样器 | `RolloutWorker` | `EnvRunner` (SingleAgentEnvRunner/MultiAgentEnvRunner) |
| 数据管道 | `Connector` (旧) | `ConnectorV2` (env_to_module / module_to_env / learner) |
| 训练器 | 本地 Policy.learn_on_batch() | `Learner` + `LearnerGroup` (分布式 GPU 训练) |
| 数据格式 | `SampleBatch` / `MultiAgentBatch` | `SingleAgentEpisode` / episode 列表 |
| TF 支持 | 是 | 否（仅 PyTorch） |
| 开关 | `enable_rl_module_and_learner=False` | 默认开启 |

---

## 3. 核心模块详解

### 3.1 `rllib/core/` — 新 API 核心抽象

这是新 API 栈的基础层，定义了三个核心抽象：

#### 3.1.1 RLModule (`core/rl_module/rl_module.py`)

**RLModule** 是新 API 栈中替代旧 `Policy` 的模型抽象，专注于"模型本身"而非"策略逻辑"：

```python
class RLModule(Checkpointable, abc.ABC):
    # 三个核心前向传播方法：
    def forward_inference(self, batch): ...      # 推理时
    def forward_exploration(self, batch): ...    # 探索采样时
    def _forward_train(self, batch): ...         # 训练时（抽象方法）
```

- **RLModuleSpec**：数据类，描述如何构建一个 RLModule（module_class, observation_space, action_space, model_config, catalog_class）
- **MultiRLModule**：多 agent 场景的容器，持有 `Dict[ModuleID, RLModule]` 映射，支持参数共享

关键特性：
- `inference_only=True`：构建轻量推理模块（去除 value function、target network 等）
- `learner_only=True`：仅在 Learner 上构建，不在 EnvRunner 上构建
- 通过 **Catalog** 构建子组件（encoder、head 等）

#### 3.1.2 Learner (`core/learner/learner.py`)

**Learner** 负责训练 RLModule，是新 API 栈中训练逻辑的核心：

```python
class Learner(Checkpointable):
    def build(self): ...                          # 构建 module + optimizer + connector
    def configure_optimizers(self): ...           # 配置优化器
    def configure_optimizers_for_module(self, module_id, config): ...  # 抽象方法
    def compute_losses(self, fwd_out, batch): ... # 计算损失（抽象方法）
    def update_from_batch(self, batch): ...       # 从 batch 更新
    def update_from_episodes(self, episodes): ... # 从 episodes 更新
```

关键职责：
- 持有 `MultiRLModule` 和 `LearnerConnectorPipeline`
- 管理优化器注册（`register_optimizer`），支持学习率调度
- 支持动态添加/删除 module（league-based training）
- 通过 `MetricsLogger` 记录训练指标
- 框架特定实现：`TorchLearner`（`core/learner/torch/torch_learner.py`）

#### 3.1.3 LearnerGroup (`core/learner/learner_group.py`)

**LearnerGroup** 管理多个分布式 Learner actor，提供统一的 `update()` 接口：

- 基于 Ray Train 的 `BackendExecutor` 管理分布式训练后端
- 支持 `num_learners > 1` 的数据并行训练
- 每个 Learner 获取数据的一个分片进行训练
- 训练后自动同步梯度（通过分布式后端）

#### 3.1.4 Catalog (`core/models/catalog.py`)

**Catalog** 是 RLModule 的组件工厂，描述如何构建子模块：

```python
class Catalog:
    def __init__(self, observation_space, action_space, model_config_dict): ...
    def build_encoder(self, framework): ...      # 观测编码器
    def build_pi_head(self, framework): ...      # 策略头
    def build_value_function(self, framework): ... # 价值函数
    def get_action_dist_cls(self, framework): ...  # 动作分布类
```

- 可被子类化以注入自定义组件
- 支持通过 `_determine_components_hook` 修改组件选择逻辑

#### 3.1.5 核心常量 (`core/__init__.py`)

```python
DEFAULT_MODULE_ID = "default_policy"
COMPONENT_ENV_RUNNER = "env_runner"
COMPONENT_LEARNER = "learner"
COMPONENT_LEARNER_GROUP = "learner_group"
COMPONENT_RL_MODULE = "rl_module"
COMPONENT_ENV_TO_MODULE_CONNECTOR = "env_to_module_connector"
COMPONENT_MODULE_TO_ENV_CONNECTOR = "module_to_env_connector"
```

---

### 3.2 `rllib/algorithms/` — 算法工厂与编排

#### 3.2.1 Algorithm (`algorithms/algorithm.py`)

**Algorithm** 是所有 RL 算法的基类，继承自 Ray Trainable，是整个训练循环的编排器：

```python
class Algorithm(Trainable, Checkpointable):
    def setup(self, config): ...           # 初始化 EnvRunnerGroup、LearnerGroup 等
    def step(self): ...                    # 执行一次训练迭代
    def training_step(self): ...          # 核心训练步骤（子类覆写）
    def evaluate(self): ...                # 评估
    def save_checkpoint(self): ...        # 检查点保存
    def restore(self, path): ...          # 检查点恢复
```

核心属性：
- `self.config`：AlgorithmConfig 配置对象
- `self.env_runner_group`：EnvRunnerGroup，管理采样 Actor
- `self.learner_group`：LearnerGroup，管理训练 Actor
- `self.metrics`：MetricsLogger，指标聚合
- `self.callbacks`：RLlibCallback 列表
- `self.offline_data`：离线 RL 数据源（可选）

`setup()` 流程：
1. 合并用户配置与默认配置
2. 创建 RLlibCallback
3. 创建本地 replay buffer（如需）
4. 初始化 EnvRunnerGroup（采样 worker）
5. 初始化 LearnerGroup（训练 worker）
6. 构建 offline data（如为离线 RL）

#### 3.2.2 AlgorithmConfig (`algorithms/algorithm_config.py`)

**AlgorithmConfig** 是一个 6400+ 行的配置类，使用链式 API：

```python
config = (PPOConfig()
    .environment("CartPole-v1")
    .env_runners(num_env_runners=4)
    .training(lr=5e-5, clip_param=0.2)
    .framework("torch")
    .build()
)
```

主要配置分组：
- `.environment(env, ...)`：环境配置
- `.env_runners(num_env_runners, ...)`：采样器配置
- `.training(lr, train_batch_size, ...)`：训练超参
- `.evaluation(evaluation_interval, ...)`：评估配置
- `.framework("torch"/"tf2")`：深度学习框架
- `.api_stack(enable_rl_module_and_learner=True, enable_env_runner_and_connector_v2=True)`：API 栈选择
- `.multi_agent(policies=..., policy_mapping_fn=...)`：多 agent 配置
- `.offline_data(input_=...)`：离线 RL 数据输入
- `.resources(num_gpus_per_learner=..., num_learners=...)`：资源分配
- `.reporting(metrics_num_train_results=...)`：指标报告

#### 3.2.3 算法注册 (`algorithms/registry.py`)

使用懒加载注册所有算法：

```python
ALGORITHMS = {
    "PPO": _import_ppo,
    "APPO": _import_appo,
    "DQN": _import_dqn,
    "SAC": _import_sac,
    "IMPALA": _import_impala,
    "DreamerV3": _import_dreamerv3,
    # ... BC, CQL, IQL, MARWIL
}
```

每个算法实现 3 个关键方法：
1. `get_default_config()` → 返回算法特定的 Config
2. `get_default_policy_class(config)` → 返回旧栈 Policy 类
3. `get_default_rl_module_spec()` → 返回新栈 RLModule 规格
4. `get_default_learner_class()` → 返回新栈 Learner 类
5. `training_step()` → 训练步骤实现

#### 3.2.4 PPO 算法示例 (`algorithms/ppo/ppo.py`)

PPO 的 `training_step()` 展示了新 API 栈的典型训练流程：

```
1. synchronous_parallel_sample()     → 从所有 EnvRunner 并行采样 episodes
2. learner_group.update(episodes)    → LearnerGroup 对 episodes 进行训练
   - LearnerConnectorPipeline 将 episodes 转换为训练 batch
   - RLModule.forward_train() 计算前向传播
   - compute_losses() 计算损失
   - optimizer.step() 更新参数
   - 支持 num_epochs 多轮 + minibatch
3. env_runner_group.sync_weights()   → 将更新后的权重同步到 EnvRunner
```

PPO 目录结构：
```
algorithms/ppo/
├── ppo.py                    # PPO Algorithm + PPOConfig
├── ppo_catalog.py            # PPOCatalog（构建 PPO 专用子组件）
├── ppo_tf_policy.py          # 旧栈 TF Policy
├── ppo_torch_policy.py       # 旧栈 Torch Policy
├── torch/
│   ├── default_ppo_torch_rl_module.py  # 新栈默认 RLModule
│   ├── ppo_torch_rl_module.py           # 新栈 RLModule 基类
│   ├── ppo_torch_learner.py             # 新栈 Learner
│   └── ppo_catalog.py                   # PPO Catalog (Torch)
└── tests/
```

---

### 3.3 `rllib/env/` — 环境抽象

#### 3.3.1 环境类型层次

```
gym.Env
├── MultiAgentEnv          # 多 agent 环境（step 返回 Dict[AgentID, ...]）
├── VectorEnv              # 向量化环境（多个并行子环境）
│   └── BaseEnv            # 最底层抽象（poll/send_actions 异步模型）
├── ExternalEnv            # 外部环境（通过 API 交互）
│   └── ExternalMultiAgentEnv
└── PolicyClient           # 策略客户端（远程推理）
```

#### 3.3.2 EnvRunner (新 API 栈) (`env/env_runner.py`)

```python
class EnvRunner(FaultAwareApply, abc.ABC):
    def __init__(self, *, config, **kwargs): ...
    def sample(self): ...           # 采样数据（核心方法）
    def get_metrics(self): ...      # 获取采样指标
    def set_state(self, state): ... # 设置权重/状态
    def get_state(self): ...        # 获取权重/状态
```

实现类：
- **SingleAgentEnvRunner** (`env/single_agent_env_runner.py`)：单 agent 采样循环
- **MultiAgentEnvRunner** (`env/multi_agent_env_runner.py`)：多 agent 采样循环

EnvRunner 内部持有：
- 环境实例（gym.Env 或 MultiAgentEnv）
- RLModule（inference_only=True，用于推理）
- EnvToModuleConnectorPipeline（环境数据 → RLModule 输入）
- ModuleToEnvConnectorPipeline（RLModule 输出 → 环境动作）

#### 3.3.3 EnvRunnerGroup (`env/env_runner_group.py`)

```python
class EnvRunnerGroup:
    def __init__(self, *, env_creator, config, local_env_runner=True, ...): ...
    def foreach_env_runner(self, fn): ...    # 对所有 remote worker 应用函数
    def local_env_runner(self): ...          # 本地 worker
    def sync_weights(self, from_worker_or_learner_group, ...): ...  # 权重同步
    def healthy_worker_ids(self): ...        # 健康的 worker ID
```

- 使用 `FaultTolerantActorManager` 管理 Ray actor，支持容错
- `local_env_runner=True` 时在主进程中维护一个本地 EnvRunner

#### 3.3.4 Episode 管理

新 API 栈使用 episode 对象而非 batch 来组织采样数据：

- **SingleAgentEpisode** (`env/single_agent_episode.py`)：记录单个 agent 的完整轨迹
- **MultiAgentEpisode** (`env/multi_agent_episode.py`)：记录多 agent 环境的完整轨迹

Episode 包含：observations, actions, rewards, terminated, truncated, infos 等字段。

---

### 3.4 `rllib/connectors/` — 数据流连接器

ConnectorV2 是新 API 栈的数据转换管道，在环境、RLModule 和 Learner 之间进行数据格式转换。

```
┌──────────┐    EnvToModule     ┌──────────┐    ModuleToEnv     ┌──────────┐
│  Environment │ ──────────→ │  RLModule  │ ──────────→ │  Environment │
│              │  Connector   │ (forward)   │  Connector   │  (env.step)  │
└──────────┘              └──────────┘              └──────────┘

                    LearnerConnector
                    ┌──────────────────────────┐
                    │  episodes/batch → train   │
                    │  batch (RLModule 输入)     │
                    └──────────────────────────┘
```

#### 3.4.1 ConnectorV2 基类 (`connectors/connector_v2.py`)

```python
class ConnectorV2(Checkpointable, abc.ABC):
    def __call__(self, input_, samples, **kwargs): ...  # 核心调用方法
```

- 每个 connector 是一个可调用对象，接收前一个 connector 的输出
- 多个 connector 组成 pipeline（也是 ConnectorV2）
- 支持有状态（如观测过滤器的统计量）

#### 3.4.2 三种 Pipeline 类型

| Pipeline | 所在位置 | 功能 |
|----------|---------|------|
| **EnvToModulePipeline** | EnvRunner | 环境输出 → RLModule 输入（观测预处理、滤波、RNN 序列化） |
| **ModuleToEnvPipeline** | EnvRunner | RLModule 输出 → 环境动作（从分布采样动作、探索） |
| **LearnerConnectorPipeline** | Learner | 采样数据/回放数据 → 训练 batch（batch 拼接、数据增强） |

#### 3.4.3 Connector 子目录

```
connectors/
├── connector_v2.py              # 基类
├── connector_pipeline_v2.py     # Pipeline 基类
├── env_to_module/                # 环境→模块连接器
├── module_to_env/                # 模块→环境连接器
├── learner/                      # Learner 连接器
├── common/                       # 通用连接器
├── action/                       # 动作处理连接器
├── agent/                        # Agent 级连接器
```

---

### 3.5 `rllib/policy/` — 策略抽象（旧 API 栈）

#### 3.5.1 Policy 基类 (`policy/policy.py`)

旧 API 栈的核心抽象，集模型、推理、训练于一体：

```python
class Policy(ABC):
    def compute_actions(self, obs_batch, ...): ...  # 计算动作
    def learn_on_batch(self, samples): ...           # 在 batch 上训练
    def get_weights(self): ...                       # 获取权重
    def set_weights(self, weights): ...              # 设置权重
    def postprocess_trajectory(self, ...): ...        # 轨迹后处理
```

框架特定实现：
- **TFPolicy** (`policy/tf_policy.py`) → TF1/TF2 策略
- **TorchPolicy** (`policy/torch_policy.py`) → PyTorch 策略
- **TorchPolicyV2** (`policy/torch_policy_v2.py`) → 新版 PyTorch 策略

辅助类：
- **PolicySpec**：描述如何构建一个 Policy
- **PolicyMap**：PolicyID → Policy 的映射管理
- **SampleBatch** (`policy/sample_batch.py`)：采样数据容器，包含 obs, actions, rewards 等

#### 3.5.2 SampleBatch (`policy/sample_batch.py`)

旧 API 栈的标准数据格式：

```python
class SampleBatch(dict):
    OBS = "obs"
    ACTIONS = "actions"
    REWARDS = "rewards"
    # ... 其他字段常量
```

- **MultiAgentBatch**：多 agent 批数据，`Dict[PolicyID, SampleBatch]`

---

### 3.6 `rllib/evaluation/` — 评估与采样（旧 API 栈）

#### 3.6.1 RolloutWorker (`evaluation/rollout_worker.py`)

旧 API 栈的采样器，已标记为 `@OldAPIStack`：

```python
class RolloutWorker(EnvRunnerV2):
    def sample(self): ...                # 采样
    def learn_on_batch(self, samples): ...  # 本地训练
    def get_policy(self, policy_id): ...  # 获取策略
    def for_policy(self, fn): ...        # 对所有策略应用函数
```

#### 3.6.2 其他评估组件

- **sampler.py**：`SyncSampler` / `AsyncSampler`，同步/异步采样循环
- **metrics.py**：`RolloutMetrics`，episode 级指标收集
- **postprocessing.py**：轨迹后处理（如 GAE 计算）
- **worker_set.py**：`WorkerSet`，管理多个 RolloutWorker

---

### 3.7 `rllib/execution/` — 执行操作（旧 API 栈）

旧 API 栈的训练操作函数：

| 文件 | 功能 |
|------|------|
| `rollout_ops.py` | `synchronous_parallel_sample()` — 并行采样 |
| `train_ops.py` | `train_one_step()` / `multi_gpu_train_one_step()` — 训练步骤 |
| `learner_thread.py` | 后台异步训练线程（IMPALA 用） |
| `replay_ops.py` | 经验回放操作 |
| `segment_tree.py` | 优先级回放的线段树 |
| `minibatch_buffer.py` | Minibatch 缓冲区 |

---

### 3.8 `rllib/models/` — 模型组件（旧 API 栈）

#### 3.8.1 旧栈模型体系

```
models/
├── catalog.py              # ModelCatalog — 模型工厂（旧栈）
├── modelv2.py              # ModelV2 — 模型基类（旧栈）
├── action_dist.py          # ActionDistribution — 动作分布基类
├── preprocessors/           # 观测预处理器
├── tf/                      # TensorFlow 模型实现
│   ├── layers/              # TF 自定义层
│   └── ...
├── torch/                   # PyTorch 模型实现
│   ├── modules/             # Torch 自定义模块
│   └── ...
└── tests/
```

- **ModelCatalog** (`models/catalog.py`)：根据 obs/action space 自动选择模型架构
- **MODEL_DEFAULTS**：默认模型配置（fcnet_hiddens, conv_filters 等）
- **ActionDistribution**：动作分布（Categorical, DiagGaussian, Deterministic 等）

---

### 3.9 `rllib/utils/` — 工具集

```
utils/
├── actor_manager.py          # FaultTolerantActorManager — 容错 Actor 管理
├── annotations.py            # API 注解（@PublicAPI, @DeveloperAPI, @OldAPIStack 等）
├── checkpoints.py            # Checkpointable — 检查点基类
├── debug.py                  # 调试工具
├── exploration/              # 探索策略（EpsilonGreedy, StochasticSampling 等）
├── filter.py                 # 观测过滤器（RunningMeanStd 等）
├── framework.py             # 框架导入工具（try_import_tf, try_import_torch）
├── from_config.py            # 从配置实例化对象
├── metrics/                  # 指标系统
│   ├── metrics_logger.py     # MetricsLogger — 指标记录器
│   ├── ray_metrics.py        # Ray 原生指标集成
│   └── ...
├── minibatch_utils.py        # Minibatch 迭代器
├── replay_buffers/           # 经验回放缓冲区
├── schedules/                # 学习率/超参调度器
├── spaces/                   # 空间工具
├── sgd/                      # SGD 工具
└── typing.py                 # 类型定义（AgentID, ModuleID, EpisodeType 等）
```

---

### 3.10 `rllib/offline/` — 离线 RL 数据

```
offline/
├── input_reader.py           # InputReader — 输入读取基类
├── json_reader.py            # JSON 格式读取
├── json_writer.py            # JSON 格式写入
├── dataset_reader.py         # Ray Dataset 读取
├── dataset_writer.py         # Ray Dataset 写入
├── d4rl_reader.py            # D4RL 数据集读取
├── shuffled_input.py         # 随机打乱输入
├── mixed_input.py           # 混合多源输入
├── output_writer.py          # OutputWriter — 输出写入基类
├── offline_data.py           # OfflineData — 离线数据管理
├── offline_evaluator.py      # OfflineEvaluator — 离线评估器
├── is_estimator.py           # 重要性采样估计器
├── wis_estimator.py          # 加权重要性采样估计器
├── off_policy_estimator.py   # 离策略估计器基类
├── offline_env_runner.py     # 离线 EnvRunner
├── offline_prelearner.py     # 离线预学习器
└── resource.py               # 资源管理
```

---

### 3.11 `rllib/callbacks/` — 回调系统

**RLlibCallback** (`callbacks/callbacks.py`) 提供训练全生命周期的钩子：

```python
class RLlibCallback:
    def on_algorithm_init(self, *, algorithm, ...): ...    # Algorithm 初始化后
    def on_train_result(self, *, algorithm, result, ...): ...  # 每次训练后
    def on_evaluate_start(self, *, algorithm, ...): ...    # 评估开始前
    def on_evaluate_end(self, *, algorithm, ...): ...       # 评估结束后
    # EnvRunner 级回调
    def on_episode_start(self, *, env_runner, ...): ...
    def on_episode_step(self, *, env_runner, ...): ...
    def on_episode_end(self, *, env_runner, ...): ...
```

---

## 4. 训练执行流程

### 4.1 新 API 栈完整训练流程（以 PPO 为例）

```
用户代码:
  config = PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=4).build()
  algo = config.build()
  algo.train()

内部执行:

1. Algorithm.setup(config)
   ├── 创建 AlgorithmConfig
   ├── 创建 RLlibCallback 实例
   ├── 创建 EnvRunnerGroup
   │   ├── 创建 local EnvRunner (SingleAgentEnvRunner)
   │   │   ├── 创建环境实例
   │   │   ├── 创建 RLModule (inference_only=True)
   │   │   ├── 创建 EnvToModuleConnectorPipeline
   │   │   └── 创建 ModuleToEnvConnectorPipeline
   │   └── 创建 N 个 remote EnvRunner actors (同样的结构)
   └── 创建 LearnerGroup
       ├── 创建 N 个 Learner actors (TorchLearner)
       │   ├── 构建 MultiRLModule (PPOTorchRLModule)
       │   ├── 构建 LearnerConnectorPipeline
       │   └── 配置 Optimizer (Adam)
       └── 设置分布式训练后端 (TorchConfig)

2. Algorithm.step() → training_step()
   │
   ├── [采样阶段] synchronous_parallel_sample()
   │   ├── 对所有 EnvRunner 并行调用 sample().remote()
   │   ├── 每个 EnvRunner 内部:
   │   │   while not batch_full:
   │   │     obs = env.step(action)           # 环境步进
   │   │     data = EnvToModuleConnector(obs)  # 环境数据 → 模块输入
   │   │     output = RLModule.forward_exploration(data)  # 推理
   │   │     action = ModuleToEnvConnector(output)  # 模块输出 → 动作
   │   │     episode.add(obs, action, reward)  # 记录到 episode
   │   └── 返回 episodes 列表 + metrics
   │
   ├── [训练阶段] learner_group.update(episodes)
   │   ├── 将 episodes 分发给各个 Learner
   │   ├── 每个 Learner 内部 (重复 num_epochs 次):
   │   │   ├── batch = LearnerConnector(episodes)  # episodes → train batch
   │   │   ├── 分割为 minibatches
   │   │   ├── fwd_out = RLModule.forward_train(batch)  # 前向传播
   │   │   ├── losses = compute_losses(fwd_out, batch)   # 计算损失
   │   │   ├── total_loss = sum(losses)                   # 合并损失
   │   │   ├── total_loss.backward()                      # 反向传播
   │   │   ├── optimizer.step()                           # 参数更新
   │   │   └── optimizer.zero_grad()                      # 清零梯度
   │   └── 返回 learner_results (loss 值、指标等)
   │
   └── [权重同步] env_runner_group.sync_weights(from=self.learner_group)
       └── 将 Learner 的 RLModule 权重同步到所有 EnvRunner

3. Algorithm.evaluate() (如果配置了评估)
   ├── 使用 eval_env_runner_group 采样
   └── 计算并返回评估指标

4. 返回训练结果 dict (含所有指标)
```

### 4.2 旧 API 栈训练流程

```
1. Algorithm.setup() → 创建 WorkerSet (RolloutWorker)
2. training_step() → _training_step_old_api_stack()
   ├── synchronous_parallel_sample() → 返回 SampleBatch
   ├── standardize_fields(train_batch, ["advantages"])
   ├── train_one_step(algo, train_batch) 或 multi_gpu_train_one_step()
   │   └── local_worker.learn_on_batch(train_batch)
   └── sync_weights() → 同步权重到 remote workers
```

---

## 5. 多 Agent 支持

### 5.1 多 Agent 环境接口

`MultiAgentEnv` 的 step/ reset 返回 `Dict[AgentID, observation]` 格式：

```python
class MultiAgentEnv(gym.Env):
    def reset(self) -> Tuple[Dict[AgentID, obs], Dict[AgentID, info]]: ...
    def step(self, action_dict: Dict[AgentID, action]) -> Tuple[
        Dict[AgentID, obs], Dict[AgentID, reward],
        Dict[AgentID, terminated], Dict[AgentID, truncated],
        Dict[AgentID, info]
    ]: ...
```

### 5.2 多 Agent 配置

```python
config.multi_agent(
    policies={
        "policy_1": PolicySpec(observation_space, action_space, config_overrides),
        "policy_2": PolicySpec(...),
    },
    policy_mapping_fn=lambda agent_id, episode, **kwargs: "policy_1" if ... else "policy_2",
    policies_to_train=["policy_1", "policy_2"],
)
```

### 5.3 新 API 栈多 Agent 机制

- **MultiRLModule** 持有 `Dict[ModuleID, RLModule]`
- **MultiAgentEnvRunner** 管理 `Dict[AgentID, ModuleID]` 映射
- 每个 agent 的观测路由到对应的 RLModule
- 训练时 Learner 处理 `Dict[ModuleID, train_batch]`

---

## 6. 分布式训练与资源管理

### 6.1 资源配置

```python
config.resources(
    num_learners=2,              # 分布式 Learner 数量（数据并行）
    num_gpus_per_learner=1,      # 每个 Learner 的 GPU 数
    num_cpus_per_env_runner=1,   # 每个 EnvRunner 的 CPU 数
    num_gpus_per_env_runner=0,   # 每个 EnvRunner 的 GPU 数
)
```

### 6.2 Placement Group

Algorithm 使用 Ray Placement Group 进行资源预约：
- 所有 Actor（EnvRunner、Learner、AggregatorActor）共享一个 placement group
- 确保资源分配的原子性

### 6.3 Aggregator Actor（高级）

对于大规模训练，RLlib 支持 AggregatorActor 模式：
- AggregatorActor 在 EnvRunner 和 Learner 之间预处理数据
- 减轻 Learner 的数据预处理负担
- 通过 `num_aggregator_actors_per_learner` 配置

---

## 7. 检查点与恢复

### 7.1 Checkpointable 基类

所有核心组件（Algorithm, RLModule, Learner, EnvRunner, ConnectorV2）都继承自 `Checkpointable`：

```python
class Checkpointable:
    def get_state(self, components=None) -> StateDict: ...
    def set_state(self, state: StateDict) -> None: ...
    def save_to_dir(self, checkpoint_dir) -> None: ...
    def restore_from_dir(self, checkpoint_dir) -> None: ...
```

### 7.2 检查点格式

新 API 栈检查点包含以下组件：
- `rl_module/`：RLModule 权重
- `learner/`：Learner 状态（优化器状态等）
- `env_runner/`：EnvRunner 状态
- `connector/`：Connector 状态
- `algorithm_state.pkl`：Algorithm 级状态（计数器等）

---

## 8. 指标与监控

### 8.1 MetricsLogger (`utils/metrics/metrics_logger.py`)

统一的指标记录系统，支持：
- Counter（计数器）
- Timer（计时器）
- Histogram（直方图）
- Prometheus 导出

关键指标：
- `NUM_ENV_STEPS_SAMPLED_LIFETIME`：累计采样步数
- `NUM_MODULE_STEPS_TRAINED_LIFETIME`：累计训练步数
- `ENV_RUNNER_RESULTS`：采样结果
- `LEARNER_RESULTS`：训练结果（loss 值等）

### 8.2 Ray 原生指标

所有核心组件都注册了 Ray metrics：
- `rllib_env_runner_num_env_steps_sampled_counter`
- `rllib_learner_update_inner_update_time`
- `rllib_algorithm_step_time`

---

## 9. 目录结构总览

```
rllib/
├── __init__.py                # 顶层导出（Policy, RolloutWorker, SampleBatch 等）
├── core/                      # 新 API 核心抽象
│   ├── __init__.py            # 核心常量定义
│   ├── columns.py             # 数据列名常量 (Columns.OBS, Columns.ACTIONS 等)
│   ├── rl_module/             # RLModule 抽象
│   │   ├── rl_module.py       # RLModule + RLModuleSpec
│   │   ├── multi_rl_module.py # MultiRLModule + MultiRLModuleSpec
│   │   ├── default_model_config.py  # DefaultModelConfig
│   │   └── apis/              # RLModule API mixin（InferenceOnlyAPI 等）
│   ├── learner/               # Learner 抽象
│   │   ├── learner.py         # Learner 基类
│   │   ├── learner_group.py   # LearnerGroup
│   │   ├── torch/             # TorchLearner 实现
│   │   └── differentiable_learner.py  # 可微分 Learner（元学习）
│   ├── models/                # 新栈模型
│   │   ├── catalog.py         # Catalog 组件工厂
│   │   ├── base.py            # Encoder, Head 等基类
│   │   ├── configs.py         # 模型配置类
│   │   └── torch/             # Torch 实现
│   ├── distribution/          # 动作分布抽象
│   └── testing/               # 测试工具
│
├── algorithms/                # 算法实现
│   ├── algorithm.py           # Algorithm 基类 (4975 行)
│   ├── algorithm_config.py    # AlgorithmConfig 基类 (6482 行)
│   ├── registry.py            # 算法注册表
│   ├── utils.py               # 算法工具函数
│   ├── ppo/                   # PPO 算法
│   ├── appo/                  # APPO 算法
│   ├── dqn/                   # DQN 算法
│   ├── sac/                   # SAC 算法
│   ├── impala/                # IMPALA 算法
│   ├── dreamerv3/             # DreamerV3 算法
│   ├── marwil/                # MARWIL 算法
│   ├── bc/                    # BC 算法
│   ├── cql/                   # CQL 算法
│   ├── iql/                   # IQL 算法
│   └── tqc/                   # TQC 算法
│
├── env/                       # 环境抽象
│   ├── base_env.py            # BaseEnv 最底层
│   ├── multi_agent_env.py     # MultiAgentEnv
│   ├── single_agent_env_runner.py   # 新栈单 agent 采样器
│   ├── multi_agent_env_runner.py    # 新栈多 agent 采样器
│   ├── env_runner.py          # EnvRunner 基类
│   ├── env_runner_group.py    # EnvRunnerGroup
│   ├── env_context.py         # EnvContext
│   ├── single_agent_episode.py      # Episode 数据结构
│   ├── multi_agent_episode.py       # 多 agent Episode
│   ├── env_runner_state_server.py   # 权重状态服务器
│   ├── external_env.py       # ExternalEnv
│   ├── policy_client.py      # PolicyClient
│   └── wrappers/              # 环境包装器
│
├── connectors/                # 数据流连接器
│   ├── connector_v2.py        # ConnectorV2 基类
│   ├── connector_pipeline_v2.py  # Pipeline 基类
│   ├── env_to_module/         # 环境→模块 pipeline
│   ├── module_to_env/         # 模块→环境 pipeline
│   ├── learner/               # Learner pipeline
│   ├── common/                # 通用 connector
│   ├── action/                # 动作处理
│   └── agent/                 # Agent 级 connector
│
├── policy/                    # 策略抽象（旧栈）
│   ├── policy.py              # Policy 基类
│   ├── tf_policy.py           # TFPolicy
│   ├── torch_policy.py        # TorchPolicy
│   ├── torch_policy_v2.py     # TorchPolicyV2
│   ├── sample_batch.py        # SampleBatch / MultiAgentBatch
│   ├── policy_map.py          # PolicyMap
│   ├── view_requirement.py    # ViewRequirement
│   └── rnn_sequencing.py      # RNN 序列化
│
├── evaluation/                # 评估与采样（旧栈）
│   ├── rollout_worker.py      # RolloutWorker
│   ├── sampler.py             # SyncSampler
│   ├── metrics.py             # RolloutMetrics
│   ├── postprocessing.py     # 轨迹后处理
│   ├── worker_set.py         # WorkerSet
│   └── collectors/            # 数据收集器
│
├── execution/                 # 执行操作（旧栈）
│   ├── rollout_ops.py         # 采样操作
│   ├── train_ops.py           # 训练操作
│   ├── learner_thread.py      # 异步训练线程
│   ├── replay_ops.py         # 回放操作
│   └── segment_tree.py        # 优先级回放线段树
│
├── models/                    # 模型组件（旧栈）
│   ├── catalog.py             # ModelCatalog
│   ├── modelv2.py             # ModelV2 基类
│   ├── action_dist.py         # ActionDistribution
│   ├── tf/                    # TF 模型
│   ├── torch/                 # Torch 模型
│   └── preprocessors/         # 预处理器
│
├── callbacks/                 # 回调系统
│   └── callbacks.py           # RLlibCallback
│
├── offline/                   # 离线 RL
│   ├── input_reader.py        # 输入读取基类
│   ├── output_writer.py       # 输出写入基类
│   ├── offline_data.py        # OfflineData
│   ├── offline_evaluator.py   # 离线评估器
│   └── ...                    # 各种格式读写器
│
└── utils/                     # 工具集
    ├── actor_manager.py       # 容错 Actor 管理
    ├── annotations.py         # API 注解
    ├── checkpoints.py         # 检查点基类
    ├── metrics/               # 指标系统
    ├── exploration/           # 探索策略
    ├── replay_buffers/        # 回放缓冲区
    ├── schedules/             # 调度器
    ├── spaces/                # 空间工具
    ├── sgd/                   # SGD 工具
    ├── typing.py              # 类型定义
    └── framework.py           # 框架导入工具
```

---

## 10. 扩展指南

### 10.1 自定义 RLModule

```python
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI

class MyRLModule(RLModule, ValueFunctionAPI):
    def setup(self):
        # 使用 catalog 构建子组件
        self.encoder = self.catalog.build_encoder(framework="torch")
        self.pi_head = self.catalog.build_pi_head(framework="torch")
        self.vf_head = self.catalog.build_value_function(framework="torch")

    def _forward_train(self, batch):
        encoded = self.encoder(batch[Columns.OBS])
        return {
            Columns.ACTION_DIST_INPUTS: self.pi_head(encoded),
            Columns.VALUE_FUNCTION: self.vf_head(encoded),
        }

    def _forward_exploration(self, batch):
        return self._forward_train(batch)  # 或不同逻辑

    def _forward_inference(self, batch):
        encoded = self.encoder(batch[Columns.OBS])
        return {Columns.ACTION_DIST_INPUTS: self.pi_head(encoded)}
```

### 10.2 自定义 Learner

```python
from ray.rllib.core.learner.torch.torch_learner import TorchLearner

class MyLearner(TorchLearner):
    def compute_losses(self, fwd_out, batch):
        # 计算算法特定损失
        return {DEFAULT_MODULE_ID: {"total_loss": my_loss}}

    def configure_optimizers_for_module(self, module_id, config):
        module = self.module[module_id]
        self.register_optimizer(
            module_id=module_id,
            optimizer=torch.optim.Adam(module.parameters(), lr=config.lr),
            params=list(module.parameters()),
            lr_or_lr_schedule=config.lr,
        )
```

### 10.3 自定义 Algorithm

```python
class MyAlgorithm(Algorithm):
    @classmethod
    def get_default_config(cls):
        return MyAlgorithmConfig()

    def training_step(self):
        # 自定义训练逻辑
        episodes = synchronous_parallel_sample(worker_set=self.env_runner_group)
        results = self.learner_group.update(episodes=episodes)
        self.env_runner_group.sync_weights(from_worker_or_learner_group=self.learner_group)
```

### 10.4 自定义 Connector

```python
from ray.rllib.connectors.connector_v2 import ConnectorV2

class MyObsNormalizer(ConnectorV2):
    def __call__(self, input_, samples, **kwargs):
        # 归一化观测
        obs = input_["obs"]
        input_["obs"] = (obs - self.mean) / self.std
        return input_
```

---

## 11. 总结

RLlib 的架构设计体现了以下核心原则：

1. **关注点分离**：RLModule（模型）、Learner（训练）、EnvRunner（采样）、Connector（数据转换）各司其职
2. **分布式原生**：所有核心组件都是 Ray Actor，天然支持水平扩展
3. **渐进式迁移**：新旧 API 栈共存，用户可按需切换
4. **可组合性**：通过 Catalog、Connector、Callback 等机制，算法行为可灵活定制
5. **框架无关**：核心抽象与深度学习框架解耦，同一套接口支持 PyTorch（新栈）和 TF（旧栈）

新 API 栈（RLModule + EnvRunner + ConnectorV2 + Learner）是 RLlib 的未来方向，提供了更清晰的接口边界、更好的分布式训练支持，以及更灵活的数据管道定制能力。
