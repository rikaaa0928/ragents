# ragents 产品需求文档 (PRD)

状态：Draft  
版本：0.1  
日期：2026-10-14  
关联文档：
- [详细技术方案](./TECHNICAL_DESIGN.zh-CN.md)
- [分阶段实施路线图](./IMPLEMENTATION_ROADMAP.zh-CN.md)

---

## 1. 产品摘要

`ragents` 是一个**跨机器、跨平台的声明式 AI Agent 群管理系统**。

整体架构对标 Kubernetes（K8s），系统中所有的交互均围绕**声明式配置（Declarative Spec）**与**状态调谐（Reconcile）**展开。系统由四个核心模块组成：

1. **`api-server`（控制面与事实存储）**：集群唯一事实源（Single Source of Truth），负责存储声明式资源（`Node`、`Session`、`Activation`、配置快照）以及只追加的上下文批次（`ItemBatch`）和生命周期事件（`SessionEvent`），负责节点调度、租约管理（Lease）与事件分发（Watch）。
2. **`api-server-client`（CLI & SDK）**：无状态客户端 SDK 与命令行工具，支持向 `api-server` 提交/更新声明式配置，并通过监听（Watch）`api-server` 事件感知任务进度、回答权限审批或渲染终端输出；同时提供 `ragents up` 便利命令一键拉起本地 All-in-One 环境。
3. **`agent-daemon`（节点守护进程，对标 `kubelet`）**：运行在各台宿主机器上，负责保管本地模型凭证（`base_url`、`api_key`）并向 `api-server` 注册本节点支持的模型列表与算力标签；监听分配给本节点的任务，合并节点级默认配置（如基础 System Prompt、默认模型参数、节点安全边界），驱动 `agent-core` 执行并将过程对话与事件回写至 `api-server`。
4. **`agent-core`（执行容器内核，对标 `container`）**：基于原生 Open Responses `Item`、ReAct 循环与 Wasmtime WASI P2 Hook/Tool 扩展机制执行具体任务，不直接感知集群网络与底层存储。

---

## 2. 产品设计原则

### 2.1 声明式驱动（Declarative First）
- 用户发起新会话、提交多轮输入、中途切换模型/插件、取消正在运行的任务、批复权限询问，本质上都是通过 Client 向 `api-server` 写入或更新声明式配置（`Spec`）。
- `agent-daemon` 通过 Watch 感知 `Spec` 与当前 `Status` 的差异，自动驱动本地 `agent-core` 向目标状态收敛，并将实际状态回写到 `Status` 与 `Event` 流中。

### 2.2 控制面集中，数据面与凭证下沉
- **对话上下文集中化**：所有 Session 的 Spec、`ItemBatch`（Open Responses `Item`）和 `SessionEvent` 统一保存在 `api-server` 的存储层中，天然支持跨机器观察、恢复与跨机器 Session 派生（Fork / Derive）。
- **模型凭证节点本地化**：模型的 `base_url` 和 `api_key` 严格由各节点的 `agent-daemon` 配置并保管，**绝不进入 `SessionSpec` 或上传至 `api-server`**。`agent-daemon` 仅向 `api-server` 注册本节点支持的模型名称列表（`supported_models`）。
- **工作目录按任务类型解耦**：区分依赖本地目录状态的任务与无本地文件依赖的纯计算/推理任务，分别提供绑定调度与浮动调度。

### 2.3 双模式任务调度（Pinned vs Floating）
系统原生支持两类具有明确语义边界的任务（Session）：
1. **绑定任务（`Pinned`：显式绑定机器 + 目录）**
   - 创建时必须显式指定目标节点（`node_id`）和目标机器上的工作目录路径（`workspace_path`）。
   - 该任务生命周期内严格固定在该节点与该目录上执行，**不支持跨机器切换或自动迁移**。
   - 若目标节点离线，任务保持等待或标记为 `Interrupted`，直至该节点恢复。
2. **浮动任务（`Floating`：不指定机器与目录，支持跨机容灾切换）**
   - 创建时不指定机器和工作目录，任务本身不依赖持久化本地文件。
   - 由 `api-server` 根据任务声明的模型需求（匹配各节点上报的 `supported_models`）和节点负载，自动指定一台健康的 `agent-daemon` 节点。
   - 目标节点的 `agent-daemon` 自动为其在本地生成隔离的临时工作目录（如 `/tmp/ragents/<session-id>`）。
   - 当原执行节点发生异常离线时，由于上下文事实（`ItemBatch`）完整保存在 `api-server` 中，该 Session 的后续激活或重调度任务**可以由 `api-server` 自动切换到其他健康机器上继续执行**。

### 2.4 上下文只追加与原生 Open Responses
- 所有上下文以 `openresponses_rust::Item` 原生结构按 `ItemBatch` 原子追加，严禁覆盖、删除或篡改已提交历史。
- 模型调用第一阶段默认采用非流式（`stream = false`）以保证跨机分布式链路简洁可靠（若评估需要也可平滑开启流式事件帧）；进度感知依靠细粒度的生命周期事件（`TurnStarted`、`ToolCallStarted`、`ToolCallFinished`、`TurnCompleted` 等）通过 SSE 实时推送到客户端。

### 2.5 渐进式交付与务实裁剪
- 第一阶段聚焦打通 **核心控制面 + Daemon + Core + 控制台 CLI** 的完整闭环与跨机调度能力。
- A2A (Agent2Agent)、ACP (Agent Client Protocol)、MCP 以及复杂的 TUI/GUI/WebUI 不纳入初期核心范围，作为后续扩展能力。
- `agent-daemon` 与 `agent-core` 之间通过统一的 `CoreExecutor` 抽象隔离：第一版优先落地实现成本低、调试直观的进程内协程模式（`InProcessExecutor`），后续无缝扩展单二进制自孵化子进程模式（`SubprocessExecutor`）。

---

## 3. 核心术语定义

| 术语 | 定义 |
| :--- | :--- |
| **`Node`** | 一台运行 `agent-daemon` 的物理机、虚拟机或容器实例。通过心跳租约（Lease）向 `api-server` 维持在线状态，并上报 `supported_models`、系统标签与并发容量。 |
| **`Session`** | 持久化的最小 Agent 工作台资源。包含调度模式（`Pinned` 或 `Floating`）、上下文来源（`SessionSource`）以及可演进的期望配置（模型、插件、权限、System Prompt）。 |
| **`Activation`** | 对某个 `Session` 发起的一次外部激活资源（如用户输入一轮指令）。一个 `Activation` 包含 1 到多个模型推理与工具执行回合（`Turn`）。 |
| **`ItemBatch`** | 一次原子提交到 `api-server` 的一组 `openresponses_rust::Item`（分为 `Input`、`ModelOutput`、`ToolOutput`）。 |
| **`SessionEvent`** | 记录 Session 与 Activation 状态变迁的不可变事实事件，分配单 Session 递增的 `EventSeq` 以及全局单调递增的 `ResourceVersion`。 |
| **`ConfigRevision`** | `agent-daemon` 将节点默认配置与 Session/Activation 声明式配置合并后生成的不可变配置快照，按规范化 JSON 的 SHA-256 内容寻址存储在 `api-server` 中。 |
| **`Lease` & `lease_epoch`** | 节点与任务激活的分布式租约及单调递增纪元号（Fencing Token），用于防止网络分区或节点假死恢复后产生双写脑裂。 |

---

## 4. 核心用户场景

### 4.1 单机快速启动（`ragents up`）
1. 用户在终端执行 `ragents up`（或在执行命令时自动拉起本地后台服务）。
2. 单个进程内同时启动嵌入式 `api-server`（SQLite 存储）与本地 `agent-daemon`（读取本地配置文件中的模型 `base_url`、`api_key` 并注册 `local` 节点）。
3. 用户即可通过 `ragents` CLI 创建会话、发起任务并实时观察控制台输出。

### 4.2 跨机器集群注册与发现
1. 在中心服务器启动 `ragents-apiserver`。
2. 在开发机 A（macOS）、服务器 B（Linux GPU）分别配置各自的模型凭证、系统默认 Prompt 与节点标签，启动 `ragents-daemon --server http://<apiserver>`。
3. 两个节点的 `agent-daemon` 分别建立与 `api-server` 的长连接隧道，上报各自支持的模型列表（如 Node A 支持 `gpt-4.1`、`claude-sonnet-4`，Node B 支持 `deepseek-r1`、`qwen-coder`）。
4. 用户在任意机器运行 `ragents node list`，即可看到集群内所有在线节点、健康状态及各自支持的模型。

### 4.3 提交绑定机器与目录的任务（`Pinned` 模式）
1. 用户希望让开发机 A 修改其本地 `/Users/dev/project-x` 目录下的代码。
2. 用户通过 CLI 提交声明式 Session 配置，指定 `scheduling: Pinned { node_id: "node-a", workspace_path: "/Users/dev/project-x" }`、模型名、启用 `file_editor` 和 `shell` 插件以及用户输入。
3. `api-server` 校验 `node-a` 支持所选模型后写入配置，并将 `Activation` 分配给 `node-a`。
4. `node-a` 上的 `agent-daemon` Watch 到任务，结合本地默认配置，在 `/Users/dev/project-x` 目录下启动 `agent-core` 执行 ReAct 循环，将每个 Turn 和工具调用结果回写 `api-server`。
5. 如果 `node-a` 断网离线，该 Session 绝不会被调度到服务器 B（因为服务器 B 没有 `/Users/dev/project-x` 的代码环境）。

### 4.4 提交浮动任务与故障跨机切换（`Floating` 模式）
1. 用户发起一个不需要本地特定项目目录的调研、分析或纯脚本计算任务，不指定 `node_id` 和工作目录，只声明所需模型 `claude-sonnet-4`。
2. `api-server` 调度器自动选择当前在线且支持 `claude-sonnet-4` 的 `node-a`。
3. `node-a` 的 `agent-daemon` 自动创建临时目录 `/tmp/ragents/sess_xxx` 并完成第一轮对话，上下文 `ItemBatch` 已持久化在 `api-server`。
4. 随后 `node-a` 关机下线（Lease 过期，节点变为 `NotReady`）。
5. 用户对该 Session 发起第二轮追问（或原未开始的任务触发重调度）：`api-server` 发现该 Session 为 `Floating` 模式且 `node-a` 已不可用，自动将新 `Activation` 调度到同样支持该模型的 `node-b`。
6. `node-b` 从 `api-server` 拉取该 Session 已提交的历史 `ItemBatch`，在本地新建临时目录 `/tmp/ragents/sess_xxx`，无缝接管并完成后续任务。

### 4.5 跨机器 Session 派生（Fork / Derive）
1. Session 1 是绑定在 `node-a` 上的代码开发会话。
2. 用户或父 Agent 从 Session 1 的固定上下文区间 `[0..25]` 派生（Fork）出一个新的 Session 2，并将 Session 2 指定为绑定在 `node-b`（或 `Floating` 模式）上运行代码审查。
3. 由于上下文存储在 `api-server` 中，Session 2 无需复制任何历史数据，在 `node-b` 上执行时直接通过 `api-server` 按需拉取来源切片并拼接本地新上下文。

### 4.6 声明式任务取消与权限审批
1. 当 `agent-core` 请求敏感能力且策略判定为 `Ask` 时，`agent-daemon` 向 `api-server` 提交 `PermissionRequested` 事件，Activation 进入 `WaitingForInteraction`。
2. 任意连接到 `api-server` 的 CLI 客户端看到等待审批提示，向 `api-server` 写入声明式权限决议（`Deny` / `AllowOnce` / `AllowSession` 等）。
3. `agent-daemon` Watch 到权限决议后恢复 `agent-core` 执行。
4. 若用户需要中途终止任务，客户端将 `Activation.spec.desired_phase` 更新为 `Cancelled`，`agent-daemon` Watch 到目标状态变更后立即触发本地 `CancellationToken` 中止 `agent-core`。

---

## 5. 功能需求清单

### 5.1 节点与模型注册管理
- **PRD-NODE-001**：`agent-daemon` 启动时必须向 `api-server` 注册 `Node` 资源，并周期性发送心跳更新租约（Lease）。
- **PRD-NODE-002**：模型 `base_url` 与 `api_key` 仅保存在 `agent-daemon` 本地配置中，`agent-daemon` 向 `api-server` 仅同步支持的模型名列表（`supported_models`）。
- **PRD-NODE-003**：`api-server` 后台巡检过期 Lease，自动将超时节点状态标记为 `NotReady`，并处理其上未完成的 Activation。

### 5.2 声明式 Session 与双模式调度
- **PRD-SCHED-001（Pinned 模式）**：当指定 `Pinned` 模式时，必须同时提供非空的 `node_id` 与绝对路径 `workspace_path`；任一缺失在创建时由 `api-server` 直接拒绝。`Pinned` Session 永不跨节点迁移。
- **PRD-SCHED-002（Floating 模式）**：当指定 `Floating` 模式时，不得绑定固定目录；`api-server` 根据模型匹配度、标签选择器与节点可用并发槽位自动分配在线 `Node`，由 `agent-daemon` 创建本地临时工作目录。
- **PRD-SCHED-003（Floating 故障切换）**：当 `Floating` Session 绑定的当前节点处于 `NotReady` 状态时：
  - 若当前无运行中 Activation，新提交的 Activation 自动重调度至其他匹配的在线 `Node`。
  - 若 Activation 仍处于 `Queued`/`Scheduled`（尚未在节点上产生未提交副作用），自动重新调度至其他在线 `Node`。
  - 若 Activation 处于 `Running` 中节点失联，先按安全规范将原 Activation 标记为 `Interrupted`（防止未知外部工具副作用重复执行），后续新 Activation 自动调度至新节点并完整继承已提交的 `ItemBatch` 上下文。

### 5.3 配置合并与固化
- **PRD-CFG-001**：客户端提交的声明式配置支持缺省部分字段（如缺省模型参数、缺省基础 System Prompt），由目标节点的 `agent-daemon` 结合节点默认配置补齐。
- **PRD-CFG-002**：`agent-daemon` 在正式开始执行 `Activation` 前，必须将合并后的有效配置（去除敏感 API Key）作为 `ConfigRevision`（按 SHA-256 内容寻址）回写至 `api-server` 并绑定到该 `Activation` 上，确保历史完全可审计。

### 5.4 上下文、血缘、扩展与权限
- **PRD-CORE-001**：采用 Open Responses 原生 `Item`、`ItemBatch` 原子追加、`SessionSource` 固定闭区间零复制派生、WASM Component Hook 生命周期（`Transform` / `Observer` / `Action`）与结构化权限判定。
- **PRD-CORE-002**：`agent-core` 与 `agent-daemon` 通过 `CoreExecutor` / `CoreHost` 接口交互，`agent-core` 不直接与 `api-server` 或 SQLite 通信。

---

## 6. 第一阶段明确不做的事项（Non-Goals）

1. **不接入外部智能体/工具协议**：第一版不实现 A2A、ACP、MCP 协议适配器。
2. **不做复杂图形界面**：第一版不开发 WebUI、桌面 GUI 或富 TUI，仅通过 CLI 控制台命令跑通创建、观察、流式事件打印、取消与审批全流程。
3. **不在 `api-server` 托管模型密钥**：`api-server` 不作为 LLM 流量代理网关，也不存储任何 LLM API Key。
4. **不自动重放未知副作用的工具调用**：节点崩溃或断联时，已经在执行中的 Tool Call 标记为 `Interrupted`，由用户或上层策略决定是否发起新 Activation。
