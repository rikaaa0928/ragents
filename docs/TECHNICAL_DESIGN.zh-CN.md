# ragents 详细技术设计方案

状态：Draft  
版本：0.1  
日期：2026-10-14  
关联文档：
- [产品需求文档](./PRODUCT_REQUIREMENTS.zh-CN.md)
- [分阶段实施路线图](./IMPLEMENTATION_ROADMAP.zh-CN.md)

---

## 1. 方案摘要

`ragents` 是一个跨机器、跨平台的声明式 AI Agent 群管理系统。整体架构借鉴 Kubernetes 的声明式控制面与节点调谐（Watch-Reconcile）思想，将系统解耦为四个核心模块：

```text
  ragents-cli / ragents-client (SDK)
                 │
                 │  HTTP REST (Declarative Apply/Patch)
                 │  + SSE (Watch Events by Global ResourceVersion)
                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ api-server (ragents-apiserver)                                          │
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────────┐  │
│  │ Resource API    │  │ Scheduler &      │  │ Watch Broadcaster      │  │
│  │ (Node/Session/  │  │ Lease Controller │  │ (Event / Status Stream)│  │
│  │  Activation)    │  │ (Pinned/Floating)│  └───────────▲────────────┘  │
│  └────────┬────────┘  └────────┬─────────┘              │               │
│           │                    │                        │               │
│           ▼                    ▼                        │               │
│  ┌──────────────────────────────────────────────────────┴────────────┐  │
│  │ Storage Engine (StorageBackend Trait -> Default: SQLite WAL)      │  │
│  │  - Global ResourceVersion + Append-only Batches/Events + Blobs    │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────▲────────────────────────────────────────┘
                                 │
                                 │  WebSocket Multiplexed Tunnel (Daemon Active Dial)
                                 │  - Register Node & supported_models
                                 │  - Heartbeat Lease & Watch Assigned Activations
                                 │  - Commit ItemBatches / Events (with lease_epoch)
         ┌───────────────────────┴───────────────────────┐
         ▼ (Node A: Pinned / Floating)                   ▼ (Node B: Pinned / Floating)
┌─────────────────────────────────────┐         ┌─────────────────────────────────────┐
│ agent-daemon (ragents-daemon)       │         │ agent-daemon (ragents-daemon)       │
│  - Local Model Credentials Vault    │         │  - Local Model Credentials Vault    │
│    (base_url, api_key per model)    │         │    (base_url, api_key per model)    │
│  - Reconcile Loop (Spec vs Status)  │         │  - Reconcile Loop (Spec vs Status)  │
│  - Config Merger & Permission Gate  │         │  - Config Merger & Permission Gate  │
│  - CoreExecutor (InProcess/Process) │         │  - CoreExecutor (InProcess/Process) │
└──────────────────┬──────────────────┘         └──────────────────┬──────────────────┘
                   │ CoreHost Trait                                │ CoreHost Trait
                   ▼                                               ▼
        ┌─────────────────────┐                         ┌─────────────────────┐
        │ agent-core          │                         │ agent-core          │
        │ (ragents-core)      │                         │ (ragents-core)      │
        │  - OpenResponses IO │                         │  - OpenResponses IO │
        │  - ReAct Loop       │                         │  - ReAct Loop       │
        │  - Wasmtime Hooks   │                         │  - Wasmtime Hooks   │
        └─────────────────────┘                         └─────────────────────┘
```

核心运行机制：
1. **声明式交互**：客户端所有操作（创建会话、发起提问、取消任务、审批权限、修改会话配置）均表现为向 `api-server` 写入或更新声明式资源（`Session`、`Activation`）。
2. **双模式任务调度**：
   - **`Pinned`（绑定任务）**：显式指定 `node_id` 和 `workspace_path`，固定在目标机器与目录执行，永不跨机迁移。
   - **`Floating`（浮动任务）**：不指定机器与目录，由 `api-server` 按模型支持情况自动调度至健康节点，由 `agent-daemon` 生成本地临时目录；当节点异常离线时支持跨机重新调度与无缝续跑。
3. **凭证下沉与配置固化**：模型 `base_url` 与 `api_key` 由 `agent-daemon` 本地保管，仅向 `api-server` 注册 `supported_models` 列表。`agent-daemon` 执行任务前合并节点默认配置与用户声明式配置，生成内容寻址的 `ConfigRevision`（不含密钥）回写 `api-server`。
4. **不可变上下文与实时事件**：所有模型上下文以原生 `openresponses_rust::Item` 按 `ItemBatch` 原子追加到 `api-server`；客户端通过监听全局 `ResourceVersion` 驱动的 SSE 事件流实时感知任务进度。

---

## 2. 架构原则与硬性不变量

以下规则由类型系统、存储事务与状态机协议共同强制：

- **INV-001 唯一事实源（SSOT）**：所有持久化状态（`Node`、`Session`、`Activation`、`ItemBatch`、`SessionEvent`、`ConfigRevision`）仅保存在 `api-server` 的存储引擎中。`agent-daemon`、`agent-core` 与客户端均不持有唯一业务事实。
- **INV-002 上下文只追加（Append-Only Context）**：`api-server` 只提供 `commit_batch` 追加接口，严禁对已提交的 `ItemBatch` 执行更新、删除、截断或重排。
- **INV-003 原生 Open Responses 类型**：持久化和内存传输的上下文元素统一使用 `openresponses_rust::Item`，不引入第二套同构的持久化 Message/ToolCall 结构。
- **INV-004 Batch 与 Event 原子提交**：每一个 `ItemBatch`（`Input`、`ModelOutput`、`ToolOutput`）必须与其对应的 `SessionEvent` 及状态投影在同一存储事务中原子提交并分配全局 `ResourceVersion`。
- **INV-005 血缘不可变与有向无环（DAG）**：`SessionSpec.sources` 在 Session 创建时解析为固定的 `ContextPos` 闭区间 `[from, through]` 且永不可变；只允许引用创建时已存在的 Session，天然保证血缘图无环且零复制。
- **INV-006 单 Session 单活跃 Activation**：同一时刻一个 Session 最多只能有一个 `Activation` 处于执行态（`Scheduled`、`Running`、`WaitingForInteraction`、`Cancelling`）。
- **INV-007 Pinned 与 Floating 调度边界隔离**：`Pinned` 任务必须同时绑定 `node_id` 与 `workspace_path` 且永不跨节点迁移；`Floating` 任务严禁指定固定宿主路径，仅使用节点临时目录，允许跨节点调度。
- **INV-008 模型密钥不出节点**：模型的 `api_key` 和私有 `base_url` 仅存在于 `agent-daemon` 本地内存与配置文件中，严禁写入 `SessionSpec`、`ConfigRevision`、`SessionEvent` 或上传至 `api-server`。
- **INV-009 有效配置快照固化**：每次 `Activation` 启动时，`agent-daemon` 必须将合并后的有效配置（脱敏后）计算规范化 SHA-256 生成 `ConfigRevision` 并绑定至该 `Activation`，后续配置修改不影响历史解释。
- **INV-010 租约纪元防脑裂（Fencing Token）**：`agent-daemon` 对 `Activation` 的所有写入（Batch、Event、Status）必须携带当前分配的 `lease_epoch`。`api-server` 在事务内校验 `lease_epoch` 匹配，过期纪元的延迟写入一律以 `StaleLeaseEpoch` 拒绝。
- **INV-011 Agent Core 零控制面耦合**：`agent-core` 只通过 `CoreHost` 接口与宿主 `agent-daemon` 交互，严禁直接连接 `api-server` 或访问底层数据库。
- **INV-012 未知外部副作用不自动重放**：若节点在 `ToolCallStarted` 后但 `ToolCallFinished` 提交前崩溃失联，该 Activation 必须恢复为 `Interrupted`，系统绝不自动重试该工具调用。

---

## 3. Cargo Workspace 与模块结构

项目采用 Cargo Workspace 多 Crate 架构，严格隔离协议契约、容器内核、控制面服务端、守护进程与客户端：

```text
ragents/
├── Cargo.toml                  # Workspace 根配置
├── wit/
│   └── ragents-extension.wit   # WASM Component 扩展接口定义 (WIT)
├── docs/
│   ├── PRODUCT_REQUIREMENTS.zh-CN.md
│   ├── TECHNICAL_DESIGN.zh-CN.md
│   └── IMPLEMENTATION_ROADMAP.zh-CN.md
└── crates/
    ├── ragents-protocol/       # 核心协议与领域模型层：
    │   └── src/                # ID/Seq newtypes、Node/Session/Activation 声明式资源、
    │                           # Batch/Event/Config/Permission 定义、REST/SSE/WS 消息 Envelope
    │
    ├── ragents-core/           # Agent 容器内核 (SDK)：
    │   └── src/                # OpenResponses ReAct 循环、ContextProjection、
    │                           # Wasmtime WASI P2 HookManager、CoreHost & CoreExecutor 抽象
    │
    ├── ragents-apiserver/      # 控制面服务端：
    │   └── src/                # StorageBackend (SQLite WAL)、Scheduler (Pinned/Floating)、
    │                           # LeaseController、WatchHub、Axum HTTP/SSE/WS 路由
    │
    ├── ragents-client/         # API Server 客户端 SDK：
    │   └── src/                # 声明式资源 CRUD/Apply/Patch、SSE Watch 订阅流、Daemon Tunnel Client
    │
    ├── ragents-daemon/         # 节点守护进程 (kubelet)：
    │   └── src/                # 本地模型凭证库、Node 注册与心跳、Reconcile 调谐器、
    │                           # 配置合并器、PermissionController、InProcess/Subprocess 执行器驱动
    │
    └── ragents-cli/            # 命令行工具与 All-in-One 入口：
        └── src/                # `ragents up` (内嵌 apiserver+daemon)、`node`、`session`、
                                # `run`、`watch`、`cancel`、`approve` 等控制台子命令
```

---

## 4. 标识符与序号体系

### 4.1 全局唯一 ID
所有资源 ID 采用带资源前缀的 UUIDv7（按时间有序、全局唯一、文件名安全）：

```text
node_<name-or-uuid>   # 节点 ID（支持可读节点名如 node_macbook 或自动生成）
sess_<uuid-v7>        # Session ID
act_<uuid-v7>         # Activation ID
turn_<uuid-v7>        # Turn ID
evt_<uuid-v7>         # Event ID
permreq_<uuid-v7>     # Permission Request ID
blob_sha256:<hex>     # 插件/配置内容寻址引用
cfg_sha256:<hex>      # ConfigRevision 内容寻址引用
permsnap_sha256:<hex> # PermissionSnapshot 内容寻址引用
```

### 4.2 强类型序号（Newtypes）
不同维度的序号使用独立 Newtype，禁止混用：

```rust
pub struct ResourceVersion(pub u64); // api-server 全局单调递增修订号（从 1 开始），用于全局 Watch 游标
pub struct Generation(pub u64);      // 单个资源 Spec 的声明式变更代数（从 1 开始）
pub struct LeaseEpoch(pub u64);      // Activation 调度租约纪元号（从 1 开始），用于防脑裂 Fencing
pub struct BatchSeq(pub u64);        // Session 内 Batch 序号（从 0 开始连续递增）
pub struct LocalItemSeq(pub u64);    // Session 本地持久化 Item 序号（从 0 开始连续递增）
pub struct EventSeq(pub u64);        // Session 内 Event 序号（从 0 开始连续递增）
pub struct ContextPos(pub u64);      // Session 有效上下文位置（展开 Sources 后 + 本地 Item 的位置，从 0 开始）
```

---

## 5. 声明式资源模型设计

所有顶层资源均遵循统一的 `ObjectMeta + Spec + Status` 结构。

### 5.1 通用元数据 `ObjectMeta`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub id: String,
    pub generation: Generation,
    pub resource_version: ResourceVersion,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}
```

### 5.2 `Node`（节点资源）

由 `agent-daemon` 注册并周期性续租，客户端只读观察（或由管理员修改 `NodeSpec.unschedulable`）：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub metadata: ObjectMeta, // id = NodeId
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeSpec {
    pub unschedulable: bool,
    /// 节点级环境提示词（首次注册时由 daemon.toml 的 default_system_prompt 种子化）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_prompt: Option<String>,
    /// 节点级全局权限策略（AllowNode 审批在此持久化）
    #[serde(default)]
    pub permissions: PermissionPolicyLayer,
    /// 按工作目录路径持久化的权限策略（AllowWorkspace 审批在此持久化）
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub workspace_permissions: BTreeMap<String, PermissionPolicyLayer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub phase: NodePhase,                 // Ready | NotReady
    pub hostname: String,
    pub os: String,                       // macos | linux | windows
    pub arch: String,                     // aarch64 | x86_64
    pub supported_models: Vec<String>,    // 本节点本地配置支持的模型名列表（不含 api_key/base_url）
    pub installed_extensions: Vec<String>,// 本节点预装的 WASM 插件名称列表
    pub max_concurrent_runners: u32,
    pub active_runners: u32,
    pub last_heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePhase {
    Ready,
    NotReady,
}
```

### 5.3 `Session`（会话工作台资源）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub metadata: ObjectMeta, // id = SessionId
    pub spec: SessionSpec,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSpec {
    /// 调度模式（创建后不可变）：显式绑定节点+目录 (Pinned) 或 浮动自动调度 (Floating)
    pub scheduling: SchedulingMode,

    /// 上下文血缘来源（创建时解析为固定 ContextPos 闭区间，创建后不可变）
    #[serde(default)]
    pub sources: Vec<SessionSource>,

    /// 若由父 Agent 通过 session.spawn 创建，记录创建者坐标（创建后不可变）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerRef>,

    // --- 以下为可声明式演进字段（更新时 metadata.generation 递增） ---
    pub desired_phase: DesiredSessionPhase, // Open | Closed | Archived
    pub system_prompt: Option<String>,
    pub model: Option<ModelRequestSpec>,
    #[serde(default)]
    pub extensions: Vec<ExtensionSpec>,
    #[serde(default)]
    pub permissions: PermissionPolicyLayer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SchedulingMode {
    /// 绑定模式：必须显式指定节点 ID 和宿主机工作目录绝对路径，不支持跨机迁移
    Pinned {
        node_id: NodeId,
        workspace_path: PathBuf,
    },
    /// 浮动模式：不指定机器和目录，由 api-server 自动分配节点并在节点上使用临时目录，支持故障跨机切换
    Floating {
        #[serde(default)]
        node_selector: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequestSpec {
    pub name: String, // 必须匹配目标节点上报的 supported_models（若为空则使用目标节点默认模型）
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<i32>,
    pub reasoning: Option<openresponses_rust::ReasoningConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStatus {
    pub phase: SessionPhase, // Open | Closed | Archived | Corrupted
    pub observed_generation: Generation,
    pub bound_node_id: Option<NodeId>, // Pinned 模式下为固定节点；Floating 模式下为最近一次分配的节点
    pub active_activation_id: Option<ActivationId>,
    pub queued_activation_count: u32,
    pub local_item_count: u64,
    pub effective_context_item_count: u64,
    pub batch_count: u64,
    pub event_count: u64,
    pub last_error: Option<String>,
    /// 最近一次 Activation 组装的四段式 System Prompt 快照
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_prompt_parts: Option<PromptParts>,
    /// 最近一次 Activation 实际生效的工具名称列表（已剔除 disabled_tools）
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effective_tools: Vec<String>,
}
```

### 5.4 `Activation`（任务激活资源）

用户的每次提问、追问、取消指令与权限审批，都直接映射为 `Activation` 的声明式 `Spec`：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activation {
    pub metadata: ObjectMeta, // id = ActivationId
    pub spec: ActivationSpec,
    pub status: ActivationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub session_id: SessionId,
    pub submission_kind: SubmissionKind, // Normal | FollowUp | Steer { target_activation_id }
    pub input: Vec<openresponses_rust::Item>,

    /// 单次激活的可选配置覆盖（如单次指定特定模型参数）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_override: Option<ModelRequestSpec>,

    /// 单次激活禁用的工具名列表（如禁言窥屏轮次禁用 `["chatroom_speak"]`）
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_tools: Vec<String>,

    // --- 可声明式修改的控制字段 ---
    pub desired_phase: DesiredActivationPhase, // Running | Cancelled
    #[serde(default)]
    pub interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredActivationPhase {
    Running,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionResolutionSpec {
    pub decision: PermissionDecision, // Deny | AllowOnce | AllowActivation | AllowSession | AllowWorkspace | AllowNode
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub override_path: Option<String>,
    pub responded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationStatus {
    pub phase: ActivationPhase,
    pub assigned_node: Option<NodeId>,
    pub lease_epoch: LeaseEpoch,
    pub effective_config_ref: Option<ConfigRef>,
    pub effective_permission_ref: Option<PermissionSnapshotRef>,
    #[serde(default)]
    pub pending_interactions: Vec<PermissionRequest>,
    pub usage: Option<openresponses_rust::Usage>,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationPhase {
    Queued,
    Scheduled,
    Running,
    WaitingForInteraction,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}
```

### 5.5 `ItemBatch` 与 `SessionEventEnvelope`（只追加事实）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemBatch {
    pub session_id: SessionId,
    pub batch_seq: BatchSeq,
    pub first_local_item_seq: LocalItemSeq,
    pub kind: ItemBatchKind, // Input | ModelOutput | ToolOutput
    pub activation_id: ActivationId,
    pub turn_id: Option<TurnId>,
    pub response_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub items: Vec<openresponses_rust::Item>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEventEnvelope {
    pub resource_version: ResourceVersion, // 全局单调递增游标
    pub event_seq: EventSeq,               // Session 内连续递增序号
    pub event_id: EventId,
    pub session_id: SessionId,
    pub activation_id: Option<ActivationId>,
    pub turn_id: Option<TurnId>,
    pub created_at: DateTime<Utc>,
    pub event: SessionEvent,
}
```

核心 `SessionEvent` 变体：
- `SessionCreated` / `SessionSpecUpdated { generation }` / `SessionClosed` / `SessionArchived`
- `ActivationSubmitted { input_batch_seq }`
- `ActivationScheduled { node_id, lease_epoch }`
- `ActivationRescheduled { from_node, to_node, lease_epoch, reason }`（专用于 `Floating` 任务故障迁移）
- `ActivationStarted { node_id, config_ref, permission_snapshot_ref }`
- `ActivationCancellationRequested`
- `ActivationCompleted { usage }` / `ActivationFailed { error }` / `ActivationCancelled` / `ActivationInterrupted { reason }`
- `TurnStarted` / `TurnCompleted { response_id, appended_batch_seq, usage }`
- `ToolCallStarted { call_id, name }` / `ToolCallFinished { call_id, name, success, duration_ms, output_batch_seq }`
- `PermissionRequested { request }` / `PermissionResolved { request_id, decision, resulting_snapshot_ref }`
- `SessionSpawned { child_session_id, command_id }`

---

## 6. 双模式任务调度与容灾切换机制

`api-server` 内置 `Scheduler` 与 `LeaseController`，对 `Pinned` 和 `Floating` 两类任务执行严格区分的调度策略：

### 6.1 `Pinned` 任务（绑定机器 + 目录）
1. **准入校验**：
   - 创建 `Session` 时，校验 `node_id` 非空，且 `workspace_path` 为合法路径。
   - 若指定了 `model.name` 且目标节点已注册，校验该节点 `supported_models` 包含该模型。
2. **调度执行**：
   - 新提交的 `Activation` 仅能分配给 `SessionSpec` 绑定的 `node_id`。
   - `agent-daemon` 在指定的 `workspace_path` 下执行任务，并在本地 `/tmp/ragents/<session-id>` 提供辅助临时目录。
3. **故障处理（严禁切换机器）**：
   - 若目标节点当前处于 `NotReady`，新提交的 `Activation` 保持 `Queued` 状态等待该节点上线（或在客户端指定 fail-fast 时返回节点离线错误）。
   - 若任务在 `Running` 期间目标节点心跳超时（Lease 过期），`LeaseController` 将 `lease_epoch` 加 1，并将该 `Activation` 标记为 `Interrupted`，**绝不将其迁移到其他节点**。

### 6.2 `Floating` 任务（无状态临时目录 + 自动跨机容灾）
1. **准入与首次调度**：
   - 创建 `Session` 时不指定 `node_id` 和 `workspace_path`。
   - 当提交 `Activation` 时，`Scheduler` 筛选满足以下条件的节点集合：
     1. `status.phase == NodePhase::Ready` 且 `!spec.unschedulable`。
     2. 匹配 `node_selector` 标签。
     3. 若声明了 `model.name`，节点的 `supported_models` 必须包含该模型（若未声明，则节点需至少配置了一个默认模型）。
     4. `active_runners < max_concurrent_runners`。
   - 按最少活跃任务数（Least-Loaded）或亲和性（优先复用该 Session 上一轮使用的健康节点以命中本地缓存）选出目标 `node_id`，推进 `lease_epoch` 并将状态置为 `Scheduled`。
2. **节点执行与临时目录**：
   - 获派节点的 `agent-daemon` 自动创建 `/tmp/ragents/floating/<session-id>/<activation-id>` 作为本次运行的 `workspace_root` 与工作目录。
   - 从 `api-server` 拉取该 Session 的 `EffectiveContext`（包括父 `SessionSource` 切片与已提交的本地 `ItemBatch`）。
3. **节点异常时的自动切换机器（Failover / Reschedule）**：
   - **场景 A（`Scheduled` 尚未开始执行时节点掉线）**：`agent-daemon` 尚未上报 `ActivationStarted` 且未产生任何副作用。`LeaseController` 检测到节点 Lease 过期后，自动递增 `lease_epoch`，发出 `ActivationRescheduled` 事件，直接将该 `Activation` 重新分配给集群内另一台健康的匹配节点执行！
   - **场景 B（上一轮已完成，新发起下一轮 `Activation` 时原节点已掉线）**：`Scheduler` 直接将新 `Activation` 调度到新的健康节点。新节点从 `api-server` 拉取完整的历史 `ItemBatch`，用户无感跨机续聊。
   - **场景 C（`Running` 执行中途节点宕机掉线）**：
     - 若当前正处于未完成的外部 Tool 调用中，为遵守 `INV-012`（不自动重放未知外部副作用），原 `Activation` 标记为 `Interrupted`；用户或客户端发起重试/新 `Activation` 时立即在新节点上基于已提交的 `ItemBatch` 恢复执行。
     - 若当前尚未发起任何工具调用（仅在等待首轮模型响应），支持根据配置直接自动重调度至新节点。

---

## 7. 模型凭证管理与 Daemon 配置合并

### 7.1 `agent-daemon` 本地模型凭证配置
每个节点上的 `agent-daemon` 启动时加载本地配置文件（如 `~/.ragents/daemon.toml`）：

```toml
[node]
id = "node-mac-studio"
max_concurrent_runners = 8
default_system_prompt = "You are a helpful AI agent running on macOS."
default_model = "claude-sonnet-4"

[models.claude-sonnet-4]
base_url = "https://api.example.com/v1"
api_key = "sk-xxxxxxxx"
provider_model_id = "claude-sonnet-4-20250514" # 实际发往接口的 model 字段
default_temperature = 0.2
default_max_output_tokens = 8192

[models.gpt-4-1]
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"
provider_model_id = "gpt-4.1"
```

- `agent-daemon` 向 `api-server` 注册 `Node` 时，自动提取配置中的 key 列表，上报 `supported_models = ["claude-sonnet-4", "gpt-4-1"]`。
- `base_url` 和 `api_key` 永不离开 `agent-daemon` 进程。

### 7.2 配置合并流水线与 `ConfigRevision` 固化
当 `agent-daemon` 收到分配给自己的 `DispatchActivation { session, activation, cluster_system_prompt, node_spec, .. }` 时，按如下步骤合成运行配置：

1. **四段式 System Prompt (`PromptParts`) 组装**：
   - 第 1 段 `system_prompt`：来自 `api-server` 下发的集群级 `cluster_system_prompt`；
   - 第 2 段 `node_prompt`：来自 `api-server` 下发的 `node_spec.node_prompt`（首次注册时由 `daemon.toml` 的 `default_system_prompt` 种子化）；
   - 第 3 段 `client_prompt`：来自 `Session.spec.system_prompt`（支持随时声明式修改）；
   - 第 4 段 `extension_prompts`：在 `agent-core` 执行 `agent.prepare` 时收集各 WASM 扩展返回的 `append_system_prompt`（固定 `agents_md` 排首位，随后为其余扩展），最终通过 `PromptParts::assemble()` 生成确定性的 `effective_system_prompt`。
2. **模型与参数合并**：
   - 模型名优先级：`Activation.spec.model_override.name` > `Session.spec.model.name` > `daemon.default_model`。
   - 从 Daemon 本地模型表查找对应 `base_url`、`api_key`、默认 `temperature` 和 `max_output_tokens`，再用 `Session.spec` 和 `Activation.spec` 中的显式非空参数覆盖。
3. **扩展与权限合并（以 `api-server` 为唯一事实源）**：
   - 合并节点默认扩展与 `Session.spec.extensions`。
   - 合并权限策略：`node_spec.permissions (节点级)` $\rightarrow$ `node_spec.workspace_permissions[workspace_root] (工作区级)` $\rightarrow$ `Session.spec.permissions (会话级)`，并叠加启用扩展的 `allowed_dirs` 与内置默认放行目录（`.`、`/tmp`）。
4. **回写不可变快照（`AcceptActivation` & `ReportPreparedConfig`）**：
   - 在 `agent-core` 完成 `agent.prepare` 并剔除 `Activation.spec.disabled_tools` 后，立即通过 `CoreHost::report_prepared_config` 将包含完整四段式 `prompt_parts` 与 `effective_tools` 的 `ConfigRevision`（`cfg_sha256:<hex>`）回写至 `api-server`，同时更新 `SessionStatus.latest_prompt_parts` 与 `SessionStatus.effective_tools`。

---

## 8. `agent-daemon` 与 `agent-core` 交互设计

### 8.1 抽象边界：`CoreExecutor` 与 `CoreHost`
`ragents-core` 作为纯 Rust SDK Crate，通过两个核心 Trait 与 `ragents-daemon` 解耦：

```rust
/// 由 agent-daemon 实现并注入给 agent-core，供 agent-core 在 ReAct 循环中回调宿主能力
#[async_trait]
pub trait CoreHost: Send + Sync {
    /// 获取当前 Session 的有效上下文（已展开 SessionSource + 本地已提交 ItemBatch）
    async fn load_effective_context(&self) -> Result<Vec<openresponses_rust::Item>, CoreError>;

    /// 原子提交 ModelOutput Batch 与 TurnCompleted 事件
    async fn commit_model_output(
        &self,
        turn_id: &TurnId,
        response_id: Option<String>,
        items: Vec<openresponses_rust::Item>,
        usage: Option<openresponses_rust::Usage>,
    ) -> Result<BatchSeq, CoreError>;

    /// 原子提交 ToolOutput Batch 与 ToolCallFinished 事件
    async fn commit_tool_output(
        &self,
        turn_id: &TurnId,
        call_id: &str,
        tool_name: &str,
        success: bool,
        duration_ms: u64,
        items: Vec<openresponses_rust::Item>,
    ) -> Result<BatchSeq, CoreError>;

    /// 追加生命周期事件（如 TurnStarted, ToolCallStarted）
    async fn emit_event(
        &self,
        turn_id: Option<TurnId>,
        event: CoreLifecycleEvent,
    ) -> Result<(), CoreError>;

    /// 请求权限判定（若策略为 Ask，daemon 会挂起等待 api-server 的 PermissionResolutionSpec 后再返回）
    async fn check_permission(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError>;

    /// 在每个 Turn 开始前（或循环退出前）原子消费 api-server 实时消息队列
    async fn drain_realtime_messages(
        &self,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<openresponses_rust::Item>, CoreError>;

    /// 委托宿主机常驻 ProcessManager 管理前后台命令进程
    async fn manage_process(
        &self,
        extension_id: Option<&str>,
        request: ProcessActionRequest,
    ) -> Result<Option<ProcessActionResponse>, CoreError>;

    /// 在 agent.prepare 完成后，将包含四段式 PromptParts 与 effective_tools 的最终配置回写 api-server
    async fn report_prepared_config(
        &self,
        prompt_parts: PromptParts,
        effective_tools: Vec<String>,
    ) -> Result<(), CoreError>;
}

/// 由执行器实现，屏蔽进程内协程 (InProcess) 与独立子进程 (Subprocess) 的差异
#[async_trait]
pub trait CoreExecutor: Send + Sync {
    async fn execute(
        &self,
        run_spec: CoreRunSpec,
        host: Arc<dyn CoreHost>,
        cancellation: CancellationToken,
    ) -> Result<CoreRunSummary, CoreError>;
}
```

### 8.2 两种执行器模式
1. **`InProcessExecutor`（第一版默认实现）**
   - 直接在 `agent-daemon` 的 Tokio Runtime 中 `tokio::spawn` 运行 `ragents_core::run_activation(run_spec, host, cancellation)`。
   - 宿主工具与 WASM 插件通过显式传入的 `workspace_root: PathBuf` 操作文件与设置命令 `Command::current_dir(&workspace_root)`，严格禁止依赖进程全局 `std::env::set_current_dir`，从而保证同一 `agent-daemon` 进程内多个并发 Session 的目录完全隔离！
   - 优势：零进程间序列化开销，共享同一个 `wasmtime::Engine` 编译缓存，代码极其精简。
2. **`SubprocessExecutor`（后续扩展阶段实现）**
   - `agent-daemon` 通过 `tokio::process::Command::new(current_exe()).arg("internal-core-worker")` 启动独立子进程。
   - `CoreHost` 的方法调用自动映射为子进程 `stdin/stdout` 上的 JSON-RPC 请求与响应；支持 OS 进程组级 `SIGKILL` 强杀与资源隔离。

### 8.3 `agent-core` 内部 ReAct 循环与扩展机制
`agent-core` 的单次 `Activation` 执行流程：
1. 初始化 `HookManager`，加载声明的 WASM Component 扩展（`wit/ragent-extension.wit`）。
2. 执行 `agent.prepare` Hook，收集各扩展只读追加的 `append_system_prompt`（写入 `PromptParts.extension_prompts`）与注册的 `ToolDefinition` 列表。
3. 根据 `ActivationSpec.disabled_tools` 过滤工具列表，组装四段式 `PromptParts`，并调用 `host.report_prepared_config(prompt_parts, effective_tools)` 实时回写 `api-server`。
4. 进入 ReAct Turn 循环：
   - 检查 `cancellation.is_cancelled()`。
   - 通过 `host.emit_event(TurnStarted)` 发出回合开始事件，并调用 `host.drain_realtime_messages()` 原子消费 `api-server` 实时队列。
   - 通过 `host.load_effective_context()` 获取当前最新上下文，执行 `context.project.prepare` 检查是否触发自动上下文压缩（`CompactionPlan`）。
   - 使用 `ModelBackend` 发送 OpenResponses 流式请求（`stream = true`，内置瞬态错误指数退避重试），实时通过 `host.emit_event(ModelTextDelta / ModelReasoningDelta)` 推送增量文本。
   - 收到完整 `ResponseResource` 后，执行 `model.response.observe` Observer Hook，调用 `host.commit_model_output(...)` 持久化 `ModelOutput` Batch 并下发包含权威 `items` 的 `TurnCompleted` 事件。
   - 若响应包含 `Item::FunctionCall`：
     - 串行遍历每个工具调用，发出 `ToolCallStarted`。
     - 依次执行 `tool.call.prepare` 与 `permission.requirements` Hook。
     - 执行工具 Action（`tools.call`）与 `tool.result.prepare` Hook，将输出封装为 `Item::FunctionCallOutput`（即使工具执行报错也封装为错误文本输出供模型感知，不直接中断循环）。
     - 调用 `host.commit_tool_output(...)` 持久化 `ToolOutput` Batch，触发 `turn.complete` Hook，继续下一轮 Turn。
   - 若响应不含 `FunctionCall`，触发 `turn.complete` Hook，再次检查 `drain_realtime_messages()` 确认无新到达实时通知后退出循环，由 `agent-daemon` 提交 `ActivationCompleted`。
5. 无论成功、失败或取消，最终执行 `agent.error`（若出错）、`agent.shutdown` 与 `hook_manager.shutdown()` 释放资源。

---

## 9. `api-server` 通信协议设计

`api-server` 基于 `axum` 构建，在单一服务上同时提供面向客户端的 **HTTP REST + SSE** 与面向 `agent-daemon` 的 **WebSocket 双向隧道**（同时支持绑定 TCP 端口与本地 Unix Domain Socket）。

### 9.1 客户端接口（HTTP REST + SSE）

| 方法与路径 | 说明 |
| :--- | :--- |
| `GET /api/v1/nodes` | 列出集群所有节点、在线状态与各自支持的模型列表 (`supported_models`) |
| `GET /api/v1/nodes/{node_id}` | 获取单个节点详情 |
| `POST /api/v1/sessions` | 创建声明式 Session（支持内联携带初始 `ActivationSpec` 一步发起对话） |
| `GET /api/v1/sessions` | 列出 Session（支持按状态、节点、标签过滤） |
| `GET /api/v1/sessions/{session_id}` | 获取 Session 的 `Spec` 与最新 `Status` |
| `PATCH /api/v1/sessions/{session_id}` | 声明式更新 Session 的可变配置（模型、插件、权限、desired_phase） |
| `GET /api/v1/sessions/{session_id}/context` | 读取 Session 的有效上下文 `Vec<Item>` 或指定范围的 `ItemBatch` |
| `POST /api/v1/activations` | 向指定 Session 提交新的声明式 `Activation`（提问 / FollowUp / Steer） |
| `GET /api/v1/activations/{act_id}` | 获取 Activation 详情与状态 |
| `PATCH /api/v1/activations/{act_id}` | 声明式更新 Activation（如设置 `desired_phase: Cancelled` 取消任务，或提交 `interaction_responses` 批复权限） |
| `GET /api/v1/watch` | **SSE 实时事件流**。支持查询参数：`session_id`（可选，不传则监听全局）、`after_rv`（从指定全局 `ResourceVersion` 之后断点续传） |

### 9.2 Daemon 双向隧道协议（WebSocket `/api/v1/daemon/tunnel`）

为保证处于 NAT/内网后的 `agent-daemon` 无需暴露公网端口，`agent-daemon` 主动向 `api-server` 建立 WebSocket 连接，消息采用类型化 JSON Envelope：

- **Daemon $\rightarrow$ Server 消息 (`DaemonToServerMessage`)**：
  1. `RegisterNode { node_id, hostname, os, arch, labels, supported_models, default_model, installed_extensions, allowed_workspace_dirs, max_concurrent_runners, default_system_prompt, default_permissions }`
  2. `Heartbeat { node_id, active_runners }`（续期节点与活跃 Activation 的 Lease）
  3. `AcceptActivation { activation_id, lease_epoch, effective_config: ConfigRevision, effective_permission: PermissionSnapshot }`
  4. `ReportPreparedConfig { request_id, activation_id, lease_epoch, effective_config: ConfigRevision }`
  5. `CommitBatch { request_id, activation_id, lease_epoch, batch_input }`
  6. `EmitEvent { request_id, activation_id, lease_epoch, event }`
  7. `CompleteActivation { request_id, activation_id, lease_epoch, terminal_state }`
- **Server $\rightarrow$ Daemon 消息 (`ServerToDaemonMessage`)**：
  1. `Registered { node_id, lease_ttl_ms, current_rv }`
  2. `DispatchActivation { session: Session, activation: Activation, lease_epoch: LeaseEpoch, cluster_system_prompt: Option<String>, node_spec: NodeSpec }`
  3. `ReconcileActivation { activation_id, lease_epoch, desired_phase, interaction_responses }`（当客户端取消任务或批复权限时实时推送给对应 Daemon）
  4. `Ack { request_id, ok, resource_version, error }`

---

## 10. 存储引擎设计（SQLite WAL 默认实现）

### 10.1 `StorageBackend` 抽象与并发模型
`api-server` 通过 `StorageBackend` Trait 封装所有持久化事务。默认提供基于 `rusqlite` 的 `SqliteStorage`：
- 开启 `PRAGMA journal_mode = WAL;`、`PRAGMA foreign_keys = ON;`、`PRAGMA synchronous = NORMAL;`。
- 写操作通过单一写锁串行执行事务，在事务内自增 `store_meta` 中的 `global_resource_version`，写入成功后向内存 `tokio::sync::broadcast` 通道发布变更通知，唤醒所有 SSE Watch 客户端。
- 读操作支持并发读取。

### 10.2 核心数据表结构

```sql
-- 1. 全局元数据与单调递增 ResourceVersion
CREATE TABLE store_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 2. 节点注册与租约表
CREATE TABLE nodes (
    node_id          TEXT PRIMARY KEY,
    resource_version INTEGER NOT NULL,
    spec_json        TEXT NOT NULL,
    status_json      TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL
);

-- 3. 内容寻址配置快照表 (SHA-256)
CREATE TABLE configs (
    config_ref   TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

-- 4. 内容寻址权限快照表 (SHA-256)
CREATE TABLE permission_snapshots (
    permission_ref TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);

-- 5. Session 表 (含 Spec 与可重建的 Status 投影)
CREATE TABLE sessions (
    session_id       TEXT PRIMARY KEY,
    generation       INTEGER NOT NULL,
    resource_version INTEGER NOT NULL,
    scheduling_mode  TEXT NOT NULL, -- 'pinned' | 'floating'
    bound_node_id    TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    spec_json        TEXT NOT NULL,
    status_json      TEXT NOT NULL
);

-- 6. Session 血缘关系表 (支持正向展开与反向子 Session 查询)
CREATE TABLE session_sources (
    child_session_id    TEXT NOT NULL REFERENCES sessions(session_id),
    source_order        INTEGER NOT NULL,
    source_kind         TEXT NOT NULL DEFAULT 'forked_from',
    source_session_id   TEXT NOT NULL REFERENCES sessions(session_id),
    from_context_pos    INTEGER NOT NULL,
    through_context_pos INTEGER NOT NULL,
    PRIMARY KEY (child_session_id, source_order)
);
CREATE INDEX idx_session_sources_parent ON session_sources(source_session_id);

-- 6b. 三会话上下文压缩接力关系表 (sess_1 -> sess_compact -> sess_2 双向血缘与多代演进)
CREATE TABLE session_compactions (
    from_session_id        TEXT PRIMARY KEY REFERENCES sessions(session_id),
    compaction_session_id  TEXT NOT NULL UNIQUE REFERENCES sessions(session_id),
    continued_session_id   TEXT NOT NULL UNIQUE REFERENCES sessions(session_id),
    compaction_generation  INTEGER NOT NULL,
    summarized_through_pos INTEGER NOT NULL,
    kept_from_pos          INTEGER,
    kept_through_pos       INTEGER,
    tokens_before          INTEGER NOT NULL,
    created_at             TEXT NOT NULL
);
CREATE INDEX idx_session_compactions_continued ON session_compactions(continued_session_id);
CREATE INDEX idx_session_compactions_worker ON session_compactions(compaction_session_id);

-- 7. Activation 声明式任务表
CREATE TABLE activations (
    activation_id    TEXT PRIMARY KEY,
    session_id       TEXT NOT NULL REFERENCES sessions(session_id),
    generation       INTEGER NOT NULL,
    resource_version INTEGER NOT NULL,
    phase            TEXT NOT NULL,
    assigned_node    TEXT,
    lease_epoch      INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    spec_json        TEXT NOT NULL,
    status_json      TEXT NOT NULL
);
CREATE INDEX idx_activations_session ON activations(session_id);
CREATE INDEX idx_activations_node_phase ON activations(assigned_node, phase);

-- 8. 只追加 ItemBatch 表
CREATE TABLE batches (
    session_id           TEXT NOT NULL REFERENCES sessions(session_id),
    batch_seq            INTEGER NOT NULL,
    first_local_item_seq INTEGER NOT NULL,
    last_local_item_seq  INTEGER NOT NULL,
    kind                 TEXT NOT NULL,
    activation_id        TEXT NOT NULL REFERENCES activations(activation_id),
    turn_id              TEXT,
    response_id          TEXT,
    created_at           TEXT NOT NULL,
    payload_json         TEXT NOT NULL,
    PRIMARY KEY (session_id, batch_seq),
    UNIQUE (session_id, first_local_item_seq)
);

-- 9. 只追加 SessionEvent 表 (带全局 resource_version 索引支持 Watch 续传)
CREATE TABLE events (
    resource_version INTEGER PRIMARY KEY,
    session_id       TEXT NOT NULL REFERENCES sessions(session_id),
    event_seq        INTEGER NOT NULL,
    event_id         TEXT NOT NULL UNIQUE,
    kind             TEXT NOT NULL,
    activation_id    TEXT,
    turn_id          TEXT,
    batch_seq        INTEGER,
    created_at       TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    UNIQUE (session_id, event_seq)
);
CREATE INDEX idx_events_session_seq ON events(session_id, event_seq);
```
