# ragents

**`ragents`** 是一个受 Kubernetes 声明式控制面启发的 **跨机器、跨平台 AI Agent 集群管理与安全执行基座**。

它将控制面状态协调（`ragents-apiserver`）、客户端 SDK（`ragents-client`）、节点宿主守护进程（`ragents-daemon`）与容器化安全执行内核（`ragents-core`）解耦为四个职责正交的模块，为上层多智能体应用（如 [`ragents-chat`](https://github.com/rikaaa0928/ragents-chat)）提供纯粹、通用的声明式调度与沙箱执行原语。

---

## 1. 核心设计理念与架构边界（Design Philosophy）

> **核心取舍：借鉴 Kubernetes 声明式调谐思想，但拒绝 CRD 大包大揽**
> - **为什么放弃 CRD（CustomResourceDefinition）**：在 Kubernetes 中，CRD 本质上只是让第三方组件复用 `kube-apiserver + etcd` 存储与 Watch 总线的**便利机制**，而非架构必要条件，且会引入动态 Schema、Admission Webhook 链与复杂 RBAC 等沉重负担。更重要的是，即便在 K8s 生态中，超高频变动状态与大体积数据也绝不会放入 `apiserver / etcd`，而是交由独立的配置中心或数据面服务处理。
> - **两层 SSOT 解耦模型**：`ragents-apiserver` **仅对固定、闭合的 5 个核心执行原语（`Node`、`Session`、`Activation`、`ItemBatch`、`Event`）做 SSOT**；上层业务系统（如 `ragents-chat`）自带独立的领域级 SSOT Server（管理房间、成员、消息、ACL 与文件暂存），高频业务交互与大体积数据传输由节点 WASM 插件直连上层业务 Server 旁路完成。

| 维度 | `ragents-apiserver`（核心执行控制面） | 上层业务 Server（如 `ragents-chat`）+ `.wasm` 插件 |
| :--- | :--- | :--- |
| **定位** | 仅管理 **Agent 算力节点与会话执行生命周期** 的微内核控制面 | 独立的 **领域业务 SSOT Server** + 数据面中转网关 |
| **资源模型** | **固定闭合模型**（`Node` / `Session` / `Activation`），不支持 CRD | **领域自治模型**（`Room` / `RoomAgent` / `RoomMessage` / `TokenAcl` / `FileStage`） |
| **控制信令** | 节点注册、心跳租约、激活调度、5 级权限 SSOT、按需唤醒节点插件（`NodeExtensionCall`） | 房间编排、轮流窥屏/概率插嘴调度、群管家招募 |
| **数据面与大文件** | **零参与**（不存储、不转发任何文件或业务回调） | 浏览器 $\leftrightarrow$ 业务 Server 暂存区 $\leftrightarrow$ 节点 `.wasm` 插件通过 `wasi:http` 直连流式传输 |

后续所有架构演进与功能改动均严格遵循以下 6 条核心设计理念：

### 1.1 极简、业务无关的控制面（Lean & Business-Agnostic Control Plane）
- **`ragents-apiserver` 只做纯粹的集群状态协调与事件分发**：维护 `Node`、`Session`、`Activation`、`ItemBatch`、`SessionEvent` 与 `RealtimeQueue` 的声明式状态机，负责双模调度（`Pinned` / `Floating`）、两阶段节点租约巡检（5 分钟运行期宽限 + 双向 `running_activation_ids` 对账）及基于 `ResourceVersion (rv)` 的 SSE 事件流广播。
- **严禁在 `ragents` 核心中耦合上层应用业务概念**：不在 `ragents-protocol`、`ragents-apiserver`、`ragents-daemon` 或 `ragents-core` 中硬编码任何具体业务语义（例如聊天室发言逻辑、特定模型内置工具名 `"web_search"`、节点文件管理 `ListDir`/`Upload`/`Download` 等）。
- **零重型依赖与零二进制数据中转**：`ragents-apiserver` 不依赖 `ragents-core`（Wasmtime）或 `reqwest`，不存储或转发任何文件二进制/Base64 载荷；控制面鉴权仅保留内置常量时间 Bearer Token 校验（`StaticTokenAuthnPlugin`），多租户与业务级 ACL 交由上层应用网关实现。

### 1.2 `api-server` 唯一事实源（SSOT）与 `NodeSpec` 基础/增量分层
- **配置与权限唯一事实源（Single Source of Truth）**：集群级提示词（`store_meta.cluster_system_prompt`）、节点级提示词与权限（`NodeSpec` / `NodeStatus`）、会话级提示词与权限（`SessionSpec.system_prompt`、`SessionSpec.permissions`）以及激活级工具禁用（`ActivationSpec.disabled_tools`）全部持久化在 `api-server` 中。
- **`NodeSpec` 静态基础与控制面动态增量分离**：
  - 每次 `ragents-daemon` 启动注册（`RegisterNode`）时，用本地 `daemon.toml` 的最新配置刷新 `NodeSpec.node_prompt` 与 `NodeSpec.base_permissions`（以及 `NodeStatus` 中的模型列表、已安装扩展列表、允许工作区目录）。
  - 运行时通过控制面审批追加的 `NodeSpec.granted_permissions`（`AllowNode`）、`NodeSpec.workspace_permissions`（`AllowWorkspace`）与 `NodeSpec.node_prompt_override` 跨 Daemon 重启持久保留，并支持通过 `PATCH /api/v1/nodes/{node_id}` 或 `PATCH /api/v1/sessions/{session_id}` 随时撤销非默认规则。
  - `api-server` 自动计算合并结果并维护在 `NodeStatus.effective_node_prompt` 与 `NodeStatus.effective_permissions` 中供全局统一消费。
- **敏感凭证本地独享（Zero-Trust Secret Locality）**：`ragents-daemon` 本地仅独享保管模型 `base_url`、`api_key` 及本地 `.wasm` 插件目录（`extensions_dir`），敏感密钥永不上传控制面或暴露给子进程环境变量。

### 1.3 四段式结构化 System Prompt (`PromptParts`) 与收窄的 WASM 扩展边界
- **四段式 System Prompt (`PromptParts`)**：按确定性顺序组装：
  1. `system_prompt`：集群全局级系统提示词（`api-server` 托管）；
  2. `node_prompt`：节点环境级提示词（`NodeStatus.effective_node_prompt`）；
  3. `client_prompt`：会话/角色人设提示词（`SessionSpec.system_prompt`，支持随时声明式修改，下一轮 Activation 立即生效）；
  4. `extension_prompts`：`ragents-core` WASM 扩展在 `agent.prepare` 阶段只读追加的协议/工作区指令（`agents_md` 固定排在首位，其余按扩展顺序排列）。
- **Prepared Config 实时回写对账**：在 `agent.prepare` 完成后，`ragents-core` 立即通过 `ReportPreparedConfig` 将组装好的 `PromptParts`、`effective_system_prompt` 与过滤 `disabled_tools` 后的 `effective_tools` 实时回写至 `api-server`（更新 `SessionStatus.effective_config`），确保控制面、UI 透视面板与实际发往大模型的 `instructions` / `tools` 100% 一致。
- **收窄 `ragents-core` WASM 扩展钩子**：`agent.prepare` 仅允许返回只读追加的 `ExtensionPrepareOutput { append_system_prompt, tools }`，严禁读取或覆写上层 `system_prompt` 与 `model`，严禁引入中途篡改用户 `input` 或历史消息的 Transform 钩子。

### 1.4 插件直连上层服务 + 通用无状态节点扩展调用 (`NodeExtensionCall`)
- **插件业务自治与数据直连**：上层应用的节点侧能力（如聊天室工具回调、节点目录浏览、大文件分块上传与拉取）全部封装在上层应用自己提供的 `.wasm` 插件中。
- **双触发路径**：
  1. **激活执行期（In-Activation）**：大模型在 ReAct 循环中调用工具（`hook = "tools.call"`），WASM 插件通过 `wasi:http` 直接请求上层业务服务。
  2. **节点按需唤醒（Out-of-Band `NodeExtensionCall`）**：当没有任何 Activation 运行时，上层服务可通过通用透明接口 `POST /api/v1/nodes/{node_id}/extensions/call` 唤醒目标节点上的指定 `.wasm` 插件（`hook = "node.call"`）；插件内部通过 `wasi:http` **直接与上层服务流式传输数据，彻底绕过 `ragents-apiserver`**。

### 1.5 三层目录隔离模型与最小权限内核沙箱（3-Tier Directory & Kernel Sandbox）
- **三层受控目录模型**：
  1. `workspace_root`（`.`）：Pinned 模式下绑定的项目工作目录（Floating 模式下与房间共享临时目录合一）；
  2. `extra_tmp_dirs`（如 `/tmp/ragents/rooms/<room_id>`）：房间/任务组级跨 Agent 共享临时目录；
  3. `session_tmp_dir`（`/tmp/ragents/sessions/<session_id>`）：单个 Session 独占的私有临时目录（自动注入子进程 `$TMPDIR`）。
- **最小默认权限与双层沙箱（WASI 0.2 + OS 内核沙箱）**：
  - 默认内置路径白名单**仅放行上述三层受控目录**（不放行全局 `/tmp`），`file_read` / `file_write` / `command_work_dir` / `http_hosts` 默认策略均为 `ask`。
  - 子进程命令通过 macOS Seatbelt (`/usr/bin/sandbox-exec`) 或 Linux Bubblewrap (`/usr/bin/bwrap`) 实施内核级隔离，进程退出时通过直接 POSIX `kill(-pgid, SIGKILL)` 清理整个子进程组。
  - `setuid`/`setgid` 二进制（如 `/bin/ps`）与系统执行目录（`/bin`, `/usr/bin` 等）精确按单文件粒度鉴权，不在审批时折叠为父目录。

### 1.6 严禁向前兼容代码（No Backward Compatibility）
- 所有开发迭代遵循 `AGENTS.md` 约束：严禁编写任何数据库增量迁移（`ALTER TABLE`）、历史字段回填 SQL、协议旧字段别名（`#[serde(alias = "...")]`）或前端/后端历史回退推断逻辑；数据结构变更时直接修改主建表 DDL 与协议定义。

---

## 2. 仓库结构与模块划分

```text
ragents/
├── Cargo.toml                        # Workspace 根配置
├── AGENTS.md                         # 开发规范与架构约束
├── scripts/build-extensions.sh       # 构建内置 wasm32-wasip2 工具扩展脚本
├── wit/ragent-extension.wit          # WASM 扩展 WIT 接口契约 (host.manage-process + lifecycle)
├── extensions/                       # 内置通用 WASM 工具扩展源码 (wasm32-wasip2)
│   ├── shell/                        # 宿主前后台命令执行与进程管理扩展（含 duration_ms 与尾部截断落盘）
│   ├── file_editor/                  # 文件分页读取 (offset/limit)、写入与非重叠精确替换扩展
│   ├── image_viewer/                 # 多模态图像读取扩展（历史轮次自动剥离 Base64 节省跨网带宽）
│   ├── agents_md/                    # 工作区 AGENTS.md / CLAUDE.md 自动发现与指令注入扩展
│   ├── skills/                       # Agent Skills (.agents/skills) 自动发现与按需加载扩展
│   └── compaction/                   # 三会话自动上下文压缩与多代接力摘要扩展 (sess_1 -> sess_compact -> sess_2)
├── crates/
│   ├── ragents-protocol/             # 核心声明式资源、事件、REST/SSE/WS 协议与 NodeExtensionCall 契约
│   ├── ragents-apiserver/            # 极简控制面服务：SQLite WAL 存储、双模调度器、REST/SSE/WS 隧道与静态 Token 鉴权
│   ├── ragents-client/               # 客户端 SDK (RagentsClient) 与 Daemon 隧道客户端 (DaemonTunnelHandle)
│   ├── ragents-core/                 # Agent 容器执行内核：ReAct 循环、Wasmtime WASI P2、OS 内核沙箱、SubprocessExecutor
│   ├── ragents-daemon/               # 节点守护进程：本地凭证库、配置合并、权限引擎、无状态节点扩展执行器
│   └── ragents-cli/                  # 命令行工具 `ragents`（包含 `up`、`apiserver`、`daemon`、`node`、`session`、`run`、`approve` 等）
└── docs/                             # 设计与配置文档
```

---

## 3. 编译与环境准备

### 3.1 编译主程序与内置 WASM 扩展

```bash
# 1. 构建内置 WASM 扩展组件 (shell, file_editor, image_viewer, agents_md, skills, compaction)
./scripts/build-extensions.sh

# 2. 编译 ragents 工作区所有二进制 (ragents, ragents-core-worker)
cargo build --release -p ragents-cli -p ragents-core

# 3. 运行全仓单元测试与端到端集成测试
cargo test --workspace
```

### 3.2 配置环境变量

```bash
export RAGENTS_BASE_URL="https://your-openresponses-endpoint.example.com/v1"
export RAGENTS_API_KEY="sk-your-api-key"
export RAGENTS_MODEL="gemini-3.8-flash"
export RAGENTS_AUTH_TOKEN="your-cluster-bearer-token"
```

---

## 4. 快速使用指南（CLI）

### 4.1 单机 All-in-One 模式 (`ragents up`)

```bash
./target/release/ragents up \
  --bind 127.0.0.1:8080 \
  --db .ragents/control.sqlite3 \
  --daemon-config /path/to/daemon.toml
```

### 4.2 分布式集群部署（独立启动 `apiserver` 与多台 `daemon`）

```bash
# 1. 启动中心控制面 ragents-apiserver
RAGENTS_AUTH_TOKEN="your-cluster-bearer-token" \
./target/release/ragents apiserver \
  --bind 0.0.0.0:8080 \
  --db /var/lib/ragents/apiserver.sqlite3 \
  --lease-ttl-ms 30000

# 2. 在各计算节点启动 ragents-daemon
RAGENTS_AUTH_TOKEN="your-cluster-bearer-token" \
./target/release/ragents daemon --config /etc/ragents/daemon.toml

# 3. 查看集群节点状态
./target/release/ragents --server http://127.0.0.1:8080 node list
```

### 4.3 `Pinned`（固定节点/目录）与 `Floating`（浮动调度）任务执行

```bash
# Pinned 模式：绑定指定节点与本地工作目录（extensions 仅需传名称，由节点按 extensions_dir 自动解析）
./target/release/ragents run \
  --node amd-fedora-01 \
  --workdir /root/work/src/my-project \
  --model gemini-3.8-flash \
  --extension shell \
  --extension file_editor \
  --extension agents_md \
  "请检查当前项目结构并运行测试"

# Floating 模式：自动调度至健康节点并在隔离临时目录执行
./target/release/ragents run \
  --floating \
  --model gemini-3.8-flash \
  "请推导并验证租约 Fencing Token 的安全性不变量"
```

---

## 5. 文档索引

- **[开发规范与架构约束 (AGENTS.md)](./AGENTS.md)**：严禁向前兼容、SSOT 权限持久化、四段式 System Prompt 与扩展边界规范。
- **[配置详解指南 (docs/CONFIGURATION_GUIDE.zh-CN.md)](./docs/CONFIGURATION_GUIDE.zh-CN.md)**：环境变量、`daemon.toml` 全字段说明、权限规则与扩展配置。
- **[技术方案设计 (docs/TECHNICAL_DESIGN.zh-CN.md)](./docs/TECHNICAL_DESIGN.zh-CN.md)**：声明式资源模型、SQLite 表结构、状态机与协议规范。
- **[双消息队列与后台命令进程管理系统设计 (docs/PROCESS_AND_QUEUE_DESIGN.zh-CN.md)](./docs/PROCESS_AND_QUEUE_DESIGN.zh-CN.md)**：`FollowUp` 等待队列与 `Realtime` 实时队列、后台进程生命周期管理设计。
