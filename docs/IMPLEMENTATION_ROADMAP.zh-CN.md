# ragents 分阶段实施路线图

状态：Draft  
版本：0.1  
日期：2026-10-14  
关联文档：
- [产品需求文档](./PRODUCT_REQUIREMENTS.zh-CN.md)
- [详细技术方案](./TECHNICAL_DESIGN.zh-CN.md)

---

## 1. 路线图总体策略

`ragents` 作为一套全新设计的跨机器声明式 Agent 群管理系统，采用 **“契约先行、自底向上、纵向贯通、逐级验收”** 的实施路径。

总体工程纪律：
1. **全新代码基线**：本仓库作为全新系统从零构建，不保留任何历史兼容包袱或冗余双写逻辑。
2. **阶段可编译、可测试**：每个阶段（Stage）结束时必须满足：
   - `cargo fmt --all -- --check` 通过
   - `cargo clippy -- workspace --all-targets -- -D warnings` 通过
   - `cargo test --workspace` 全部通过
3. **聚焦核心链路**：前 5 个阶段（S0–S5）集中打通 **“声明式 API Server + Client SDK + Agent Daemon + Agent Core (InProcess) + 控制台 CLI (`ragents up` 及独立部署)”** 完整闭环；子进程隔离模式（`SubprocessExecutor`）与富前端/外部协议放在 S6 按需扩展。

---

## 2. 实施阶段总览

| 阶段 | 阶段名称 | 核心交付产物 | 是否阻塞下一阶段 |
| :--- | :--- | :--- | :--- |
| **S0** | Workspace 骨架与核心协议契约 | Cargo Workspace、`ragents-protocol` 声明式资源与消息模型 | 是 |
| **S1** | 控制面存储引擎与双模调度器 | `ragents-apiserver` 存储层（SQLite WAL）、Pinned/Floating 调度与 Lease 控制器 | 是 |
| **S2** | API Server 网络层与 Client SDK | `ragents-apiserver` (HTTP REST + SSE + WS) 与 `ragents-client` SDK | 是 |
| **S3** | Agent 容器内核与 WASM 扩展 | `ragents-core` (OpenResponses ReAct + Wasmtime Hooks + `InProcessExecutor`) | 是 |
| **S4** | 节点守护进程 (`agent-daemon`) | `ragents-daemon` (本地凭证管理、隧道通信、配置合并、Reconcile 调谐器) | 是 |
| **S5** | 控制台 CLI、`ragents up` 与端到端验收 | `ragents-cli` 全套控制台命令、跨机调度与故障切换端到端测试 | 是（里程碑 v0.1） |
| **S6** | 后续演进：子进程执行器与生态扩展 | `SubprocessExecutor` 单二进制自孵化、流式 Delta 增强、TUI/WebUI/协议扩展 | 否（增量演进） |

---

## 3. 各阶段详细实施步骤与验收标准

### Stage 0 (S0)：Workspace 骨架与核心协议契约 (`ragents-protocol`)

#### 3.1.1 目标
搭建完整的 Cargo Workspace 工程骨架，在 `ragents-protocol` 中固化所有声明式资源、序号类型、事件模型与通信协议数据结构。

#### 3.1.2 具体步骤
1. **初始化 Workspace**：
   - 创建根 `Cargo.toml`，配置统一依赖版本（`tokio`、`serde`、`serde_json`、`openresponses-rust`、`uuid`、`chrono`、`sha2`、`thiserror`、`anyhow`、`axum`、`rusqlite`、`wasmtime` 等）。
   - 创建 6 个成员 Crate：`ragents-protocol`、`ragents-core`、`ragents-apiserver`、`ragents-client`、`ragents-daemon`、`ragents-cli`。
   - 放入 `wit/ragents-extension.wit` 扩展接口定义。
2. **实现标识符与强类型序号 (`ragents-protocol/src/ids.rs`)**：
   - 实现 `NodeId`、`SessionId`、`ActivationId`、`TurnId`、`EventId`、`PermissionRequestId`、`ConfigRef`、`PermissionSnapshotRef`。
   - 实现 `ResourceVersion`、`Generation`、`LeaseEpoch`、`BatchSeq`、`LocalItemSeq`、`EventSeq`、`ContextPos`。
3. **实现声明式资源模型 (`ragents-protocol/src/resources/`)**：
   - `ObjectMeta`（含 `generation` 与 `resource_version`）。
   - `Node`（`NodeSpec`、`NodeStatus`，包含 `supported_models: Vec<String>` 与租约时间戳）。
   - `Session`（`SessionSpec` 含 `SchedulingMode::Pinned { node_id, workspace_path }` 与 `SchedulingMode::Floating { node_selector }`、`SessionSource`、模型/插件/权限声明；`SessionStatus`）。
   - `Activation`（`ActivationSpec` 含 `input`、`desired_phase`、`interaction_responses`；`ActivationStatus` 含 `assigned_node`、`lease_epoch`、`effective_config_ref`、`pending_interactions`）。
   - `ConfigRevision` 与 `PermissionSnapshot` 的规范化 JSON 序列化及 SHA-256 内容寻址哈希计算函数。
4. **实现事实与协议消息 (`ragents-protocol/src/events.rs`, `protocol.rs`)**：
   - `ItemBatch`（原生 `openresponses_rust::Item`）与 `SessionEventEnvelope`。
   - REST 请求/响应结构体、SSE Watch 事件帧、Daemon WebSocket 双向消息枚举（`DaemonToServerMessage` / `ServerToDaemonMessage`）。

#### 3.1.3 验收门槛
- `ragents-protocol` 零编译警告。
- 单元测试覆盖：所有声明式资源的 JSON 序列化/反序列化、SHA-256 `ConfigRef` 稳定性（不同字段顺序产出相同 Hash）、`Pinned` 与 `Floating` 模式校验逻辑。

---

### Stage 1 (S1)：控制面存储引擎与双模调度器 (`ragents-apiserver` 核心逻辑)

#### 3.2.1 目标
实现 `api-server` 底层的 SQLite WAL 存储引擎、全局 `ResourceVersion` 分配机制、上下文血缘展开、节点 Lease 巡检以及 `Pinned`/`Floating` 双模式调度器。

#### 3.2.2 具体步骤
1. **实现 SQLite 存储引擎 (`ragents-apiserver/src/store/`)**：
   - 初始化 9 张核心表（`store_meta`、`nodes`、`configs`、`permission_snapshots`、`sessions`、`session_sources`、`activations`、`batches`、`events`），开启 WAL 与外键约束。
   - 实现单写事务封装：每次写事务递增 `global_resource_version`，并在事务提交后通过内存 `broadcast::Sender<WatchEvent>` 发布实时通知。
2. **实现 Session 与上下文血缘管理**：
   - 创建 `Session`：校验 `Pinned`（必须有 `node_id` + 绝对路径 `workspace_path`）与 `Floating` 约束；校验 `SessionSource` 引用的父 Session 范围合法性。
   - 实现 `read_effective_context(session_id)`：递归展开 `SessionSource` 的固定 `ContextPos` 切片并拼接当前 Session 本地 `ItemBatch`。
   - 实现 `SessionSpec` 声明式 Patch 更新（递增 `generation`）。
3. **实现节点注册、租约管理与双模式调度器 (`ragents-apiserver/src/scheduler/`)**：
   - 实现 `register_node` 与 `heartbeat_node`：更新 `supported_models`、`active_runners` 与 `lease_expires_at`。
   - 实现 `Scheduler::schedule_pending_activations`：
     - **`Pinned` 任务**：仅匹配绑定的 `node_id`（校验节点 `Ready`、并发余量及 `supported_models`）；若节点 `NotReady`，保持 `Queued` 等待，绝不派发给其他节点。
     - **`Floating` 任务**：在所有 `Ready`、匹配 `node_selector` 且 `supported_models` 包含目标模型的节点中，优先选择亲和节点或负载最低节点，分配 `lease_epoch + 1` 并推进至 `Scheduled`。
4. **实现 `LeaseController`（超时巡检与防脑裂 Fencing）**：
   - 巡检过期节点并将其标记为 `NotReady`。
   - 对失效节点上的 `Activation`：
     - 若为 `Floating` 且处于 `Scheduled`（尚未开始运行），自动重调度至其他健康节点并记录 `ActivationRescheduled`。
     - 若处于 `Running` 或为 `Pinned` 任务，递增 `lease_epoch` 并标记为 `Interrupted`。
   - 在 `commit_batch`、`emit_event`、`complete_activation` 事务中强制校验 `lease_epoch`，拦截过期纪元写入（返回 `StaleLeaseEpoch`）。

#### 3.2.3 验收门槛
- 单元测试与存储集成测试覆盖：
  - Batch 与 Event 原子提交及 `ResourceVersion` 严格单调递增。
  - 多级 `SessionSource` 派生上下文零复制展开。
  - `Pinned` 任务在节点离线时绝不漂移；`Floating` 任务在节点离线时自动跨节点重调度或在新一轮 Activation 切换节点。
  - 旧节点恢复后携带旧 `lease_epoch` 写入被严格拒绝。

---

### Stage 2 (S2)：API Server 网络层与客户端 SDK (`ragents-apiserver` & `ragents-client`)

#### 3.3.1 目标
将 `ragents-apiserver` 封装为完整的 HTTP REST + SSE + WebSocket 服务，并在 `ragents-client` 中提供强类型异步客户端 SDK。

#### 3.3.2 具体步骤
1. **实现 `ragents-apiserver` HTTP/SSE/WS 路由 (`axum`)**：
   - 实现 REST API：`/api/v1/nodes`、`/api/v1/sessions`、`/api/v1/sessions/{id}/context`、`/api/v1/activations`。
   - 实现 SSE Watch 接口 `GET /api/v1/watch`：支持传入 `after_rv` 先从 SQLite 补齐历史事件，再无缝衔接内存实时广播流。
   - 实现 Daemon WebSocket 隧道 `GET /api/v1/daemon/tunnel`：管理在线 Daemon 连接表，当 `Scheduler` 分配任务或客户端 `PATCH` 取消/审批权限时，实时通过对应节点的 WebSocket 推送 `DispatchActivation` 或 `ReconcileActivation`。
   - 支持 TCP 监听与本地内存/Unix Socket 绑定。
2. **实现 `ragents-client` SDK**：
   - `RagentsClient`：封装声明式 Session/Activation 创建、查询、Patch（取消、权限批复）、上下文读取。
   - `WatchStream`：封装 SSE 事件流解析与 `after_rv` 游标追踪。
   - `DaemonTunnelClient`：封装面向 `agent-daemon` 的 WebSocket 连接、自动重连与请求-响应关联。

#### 3.3.3 验收门槛
- 启动内存测试 `api-server`，使用 `RagentsClient` 创建 Session 与 Activation，用模拟 `DaemonTunnelClient` 接收任务、回写 Batch 与 Event，客户端 Watch 流完整按序收到所有事件。

---

### Stage 3 (S3)：Agent 容器内核与 WASM 扩展运行时 (`ragents-core`)

#### 3.4.1 目标
实现独立于控制面的 `ragents-core` 容器内核 Crate，支持 OpenResponses 模型调用、Wasmtime WASI P2 扩展 Hook 体系、结构化权限拦截与 `InProcessExecutor`。

#### 3.4.2 具体步骤
1. **实现 `CoreHost` 与 `CoreExecutor` 接口 (`ragents-core/src/host.rs`)**：
   - 定义 `CoreRunSpec`（包含 `workspace_root`、`session_tmp_dir`、模型连接配置 `ModelEndpointConfig`、有效 `ConfigRevision` 等）。
2. **实现 OpenResponses 模型客户端与 `ContextProjection` (`ragents-core/src/model.rs`, `context.rs`)**：
   - 基于 `openresponses-rust` 发送非流式请求，解析 `Message`、`Reasoning`、`FunctionCall` 与 `Usage`。
3. **实现 Wasmtime WASI P2 扩展与 Hook 管理器 (`ragents-core/src/hooks/`)**：
   - 支持加载 `.wasm` 组件，为组件预打开受控目录（`workspace_root` 与 `session_tmp_dir`，不依赖进程全局 `cwd`）。
   - 实现 `Transform`、`Observer`、`Action` 三类 Hook 调用链（`input.prepare`、`agent.prepare`、`context.project.prepare`、`tool.call.prepare`、`permission.requirements`、`tools.call`、`tool.result.prepare`、`context.append.prepare`）。
   - 移植并构建标准扩展（`shell`、`file_editor`、`image_viewer`）。
4. **实现 ReAct 执行循环与 `InProcessExecutor` (`ragents-core/src/runner.rs`)**：
   - 串联输入准备、Turn 循环、工具权限申请（回调 `CoreHost::check_permission`）、工具调用、Batch 提交与 `CancellationToken` 响应。

#### 3.4.3 验收门槛
- 使用 Mock `CoreHost` 和 Mock 模型响应测试 `ragents-core`：无工具对话、多步工具调用、权限拒绝/允许、中途取消均符合预期，且不依赖任何数据库或网络服务。

---

### Stage 4 (S4)：节点守护进程 (`ragents-daemon`)

#### 3.5.1 目标
实现运行在各宿主机上的 `ragents-daemon`，完成本地模型密钥管理、节点注册续租、配置合并、工作目录准备（`Pinned` 固定目录 vs `Floating` 临时目录）以及驱动 `ragents-core` 执行任务。

#### 3.5.2 具体步骤
1. **实现 `DaemonConfig` 与本地模型凭证库 (`ragents-daemon/src/config.rs`)**：
   - 解析 TOML 配置或环境变量中的模型定义（`model_name -> { base_url, api_key, provider_model_id, defaults }`）。
   - 提取 `supported_models` 列表用于向 `api-server` 上报。
2. **实现配置合并器与权限门卫 (`ragents-daemon/src/merger.rs`, `permission.rs`)**：
   - 将节点默认参数与 `Session.spec`、`Activation.spec` 合并，生成脱敏的 `ConfigRevision`（SHA-256）与 `PermissionSnapshot`（SHA-256）。
   - 实现权限规则判定（`Deny` -> `Allow` -> `Default(Allow/Deny/Ask)`）；当判定为 `Ask` 时，上报 `PermissionRequested` 并异步等待 `ReconcileActivation` 下发用户的 `PermissionResolutionSpec`。
3. **实现 Daemon 调谐器与 `DaemonCoreHost` (`ragents-daemon/src/daemon.rs`)**：
   - 通过 `DaemonTunnelClient` 连接 `api-server`，发送 `RegisterNode` 并启动定时 `Heartbeat` 任务。
   - 收到 `DispatchActivation` 时：
     - 若为 `Pinned` 模式，校验并使用指定的 `workspace_path`；
     - 若为 `Floating` 模式，在本地创建 `/tmp/ragents/floating/<session-id>/<activation-id>` 临时目录。
   - 构造 `DaemonCoreHost`（携带当前 `lease_epoch`），调用 `InProcessExecutor` 启动 `ragents-core`。
   - 收到 `ReconcileActivation` 时：若 `desired_phase == Cancelled`，立即触发对应任务的 `CancellationToken`；若有新的 `interaction_responses`，唤醒挂起的权限等待通道。

#### 3.5.3 验收门槛
- 集成测试：启动一个 `api-server` 实例与两个不同配置的 `agent-daemon` 实例（如 `node-a` 与 `node-b`），验证任务自动按模型能力路由、配置合并回写、工具调用执行与结果持久化。

---

### Stage 5 (S5)：控制台 CLI、`ragents up` 与端到端验收 (`ragents-cli`)

#### 3.6.1 目标
提供完整的控制台命令行工具 `ragents`，支持一键启动本地环境（`ragents up`）或独立部署服务端/守护进程，并通过控制台完成所有声明式操作与事件监听。

#### 3.6.2 具体步骤
1. **实现服务启动命令**：
   - `ragents up`：在单进程内启动 `api-server` + 默认本地 `agent-daemon`，开箱即用。
   - `ragents apiserver`：独立启动控制面服务端。
   - `ragents daemon`：独立启动节点守护进程并连接指定 `api-server`。
2. **实现声明式资源管理与交互命令**：
   - `ragents node list / get <node-id>`：查看在线节点、状态与支持的模型列表。
   - `ragents session create`：支持创建 `Pinned`（`--node <id> --workdir <path>`）或 `Floating`（`--floating`）会话，支持 `--from-session <id:range>` 派生上下文。
   - `ragents session list / get / context / patch`：查看会话状态、完整 OpenResponses 上下文或动态修改会话模型/插件配置。
   - `ragents run <session-id> <prompt>`（或一步创建并运行）：提交声明式 `Activation`，自动订阅 SSE Watch 事件流，在终端实时打印回合开始、工具调用、权限询问提示与最终模型回复。
   - `ragents cancel <activation-id>`：声明式将 `desired_phase` 设为 `Cancelled`。
   - `ragents approve <activation-id> <request-id> --decision <allow_once|allow_session|deny>`：声明式批复权限请求。
3. **端到端（E2E）验收测试套件**：
   - **场景 1（`ragents up` 单机全链路）**：创建会话 $\rightarrow$ 运行模型调用与 `shell`/`file_editor` 扩展 $\rightarrow$ 产生物理文件修改与 `ItemBatch` 追加 $\rightarrow$ 查看历史上下文。
   - **场景 2（`Pinned` vs `Floating` 跨机调度与故障切换）**：
     - 启动 `node-1` 与 `node-2`。
     - 验证 `Pinned` 任务固定在 `node-1` 指定目录执行；停掉 `node-1` 后，`Pinned` 任务不漂移。
     - 验证 `Floating` 任务在 `node-1` 离线后，新任务或待执行任务自动切换到 `node-2` 的临时目录继续执行，且完整保留 `node-1` 阶段已提交的对话上下文。
   - **场景 3（声明式取消与权限交互）**：模拟慢速调用并执行 `ragents cancel` 验证立即终止；模拟 `Ask` 权限策略并通过 `ragents approve` 解除阻塞完成执行。

#### 3.6.3 验收门槛（Milestone v0.1 完成定义）
- 全仓通过 `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。
- 用户仅通过控制台即可完成单机 `ragents up` 或多节点跨机集群的声明式任务编排、进度监控、权限审批与故障切换。

---

### Stage 6 (S6，后续演进阶段)：子进程执行器与生态扩展

在 Milestone v0.1 稳定运行后，按优先级增量推进：
1. **`SubprocessExecutor` 单二进制自孵化模式**：
   - 实现 `ragents daemon internal-core-worker` 子进程入口，通过 Stdin/Stdout JSON-RPC 桥接 `CoreHost`。
   - 增加 `.wasm` 到 `.cwasm` 的节点本地预编译磁盘缓存，实现毫秒级子进程冷启动与 OS 进程组级强隔离。
2. **流式输出体验增强（可选）**：
   - 在 `api-server` Watch 通道增加瞬态（不落盘）Token Delta 与命令输出帧转发。
3. **多前端与外部协议适配器（按需）**：
   - 终端 TUI / WebUI 监控面板。
   - ACP（编辑器协议）、MCP（工具生态桥接）、A2A（跨框架智能体互联）适配器。
