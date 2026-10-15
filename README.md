# ragents

**`ragents`** 是一个受 Kubernetes 声明式控制面启发的 **跨机器、跨平台 AI Agent 集群管理与安全执行系统**。

它将控制面状态协调（`api-server`）、客户端交互（`api-server-client` CLI & SDK）、节点宿主守护进程（`agent-daemon`）与容器化执行内核（`agent-core`）解耦为四个独立模块，支持在多台异构机器上统一调度、隔离执行、细粒度审计和扩展 AI Agent 任务。

---

## 1. 核心特性

1. **Kubernetes 风格声明式架构与单端口复合协议**
   - 所有集群状态收敛于 `Node`、`Session`、`Activation`、`ConfigRevision`、`PermissionSnapshot`、`ItemBatch`、`SessionEventEnvelope` 声明式资源，支持全局单调递增 `ResourceVersion (rv)` 与 `lease_epoch` Fencing Token 防脑裂校验。
   - 单一端口复用：`HTTP REST`（资源 CRUD/Patch）+ `SSE (/api/v1/watch)`（基于 `after_rv` 游标的实时与断点续传事件流）+ `WebSocket (/api/v1/daemon/tunnel)`（Daemon 主动外拨穿透 NAT 的双向控制隧道）。
2. **`Pinned`（固定节点/目录）与 `Floating`（浮动调度/容灾）双模式调度**
   - **`Pinned` 模式**：严格绑定指定 `node_id` 与绝对路径 `workspace_path`（如操作某台开发机上的本地代码仓），节点离线时任务等待或中断，**绝不跨机漂移**。
   - **`Floating` 模式**：不绑定机器与本地路径，由 `api-server` 自动调度至支持目标模型的健康节点（优先复原节点亲和性，其次按最小负载），在节点临时目录执行；当节点故障离线时支持**自动跨机重调度并无缝接续历史上下文**。
3. **节点本地模型凭证保险箱（Zero-Trust Secret Locality）**
   - 模型 `base_url` 与 `api_key` 仅保存在各节点本地的 `agent-daemon` 中，**永不上传至 `api-server` 数据库或网络层**；`agent-daemon` 仅向控制面上报该节点支持的模型名称列表（`supported_models: Vec<String>`）。
4. **纯 OpenResponses 协议与双向 ID 穿透**
   - 统一基于 `openresponses-rust` 标准格式通信；工具调用 `call_id`、消息 `Item` ID 原生贯通，且自动将 `ragents.session_id` / `ragents.activation_id` / `ragents.turn_id` 注入 OpenResponses 请求 `metadata`。
5. **子进程容器化 + WASI 沙箱 + OS 内核级命令沙箱**
   - 默认采用 `SubprocessExecutor`：每次 `Activation` 派生独立 OS 子进程（`ragents internal-core-worker`），自动剥离环境变量中的 `RAGENTS_API_KEY`，通过单 Daemon 共享的 Unix Domain Socket（`SharedIpcHub`，不可用时自动回退本地回环 TCP）与父进程通信。
   - Wasmtime 组件预编译缓存（`.cwasm` SHA-256 内容寻址，`< 1ms` 极速加载）。
   - **OS 内核级命令沙箱**：macOS 下自动启用 `/usr/bin/sandbox-exec`（Seatbelt），Linux 下自动启用 `/usr/bin/bwrap`（Bubblewrap），拦截一切未授权的工作区外文件写入与受限目录读取。
6. **三大能力边界统一鉴权（宿主 Shell + WASI 0.2 文件系统 + WASI 0.2 网络）与 5 级权限审批**
   - **最小化宿主导入**：WIT `ragent:extension/host` 仅暴露唯一的命令执行接口 `execute-command-with-timeout`；WASM 插件内部的文件读写与网络请求全部走 **WASI 0.2 标准接口**。
   - **宿主 Shell（`execute-command-with-timeout`）**：当 `uv run`、`pip install` 等命令访问工作区外部目录（如 `~/.cache/uv`）被内核沙箱拦截时，系统在**单次工具调用内部**自动捕获受阻路径（支持 `stderr` 快速提取 + macOS 内核沙箱日志兜底），支持单次命令内**多目录循环递进审批**，获批后原地自动解锁沙箱并重跑命令。
   - **WASI 0.2 文件系统（`wasi:filesystem/types::HostDescriptor`）**：直接接管 WASI 0.2 底层 `open-at`、`create-directory-at`、`unlink-file-at`、`rename-at` 等异步系统调用。无论 WASM 插件（如 `file_editor.wasm` 或任意第三方黑盒 `.wasm`）如何构造路径并调用 `std::fs`，只要触碰未授权目录，均在 WASI `open-at` 系统调用处直接挂起并发起 `FileRead` / `FileWrite` 审批，批准后当场返回文件描述符完成读写。
   - **WASI 0.2 网络（`wasi:http/outgoing-handler` & `wasi:sockets`）**：通过 Wasmtime WASI 0.2 内置的 `WasiHttpView::send_request` 与 `WasiCtxBuilder::socket_addr_check` 拦截所有出站 HTTP 与 TCP/UDP 连接，自动触发 `CapabilityRequest::Http` 权限校验与交互式域名审批（支持 `--path "*.example.com"` 通配符覆盖）。
   - 审批时统一展示 `original_path`（原始受阻文件路径或完整 URL）与 `suggested_path`（自动收敛建议目录或目标域名），支持用户通过 `--path` 手动收窄或放宽范围，并提供 **`allow-once` / `allow-activation` / `allow-session` / `allow-workspace` / `allow-node`** 5 级生效周期。
7. **`agent-daemon` 与 `api-server` 双驱动扩展体系**
   - 同时支持 **Native Rust Trait**、**WASM 组件（`.wasm`/`.cwasm`）** 与 **HTTP/UDS Webhook（`http://` / `https://` / `unix://`）**。
   - `agent-daemon` 支持拦截并改写模型请求参数（标准字段与供应商私有扩展字段）、注入自定义 Header、动态轮换 `api_key`、后处理模型响应及自定义权限策略。
   - `api-server` 支持自定义 `Authn`（客户端与节点隧道认证）、`Authz`（含基于 `enforced_label_selector` 的多租户透明隔离）以及 `Mutating / Validating Admission` 准入控制。

---

## 2. 仓库结构与模块划分

```text
ragents/
├── Cargo.toml                        # Workspace 根配置
├── scripts/build-extensions.sh       # 构建内置 wasm32-wasip2 工具扩展脚本
├── wit/ragent-extension.wit          # WASM 扩展 WIT 接口契约
├── extensions/                       # 内置 WASM 工具扩展源码
│   ├── shell/                        # 宿主终端命令执行扩展
│   ├── file_editor/                  # 文件读写与精确替换扩展
│   └── image_viewer/                 # 图像查看扩展
├── crates/
│   ├── ragents-protocol/             # 核心声明式资源、事件、REST/SSE/WS 协议与扩展契约
│   ├── ragents-apiserver/            # 控制面服务：SQLite WAL 存储、双模调度器、REST/SSE/WS 与 Authn/Authz/Admission 扩展
│   ├── ragents-client/               # 客户端 SDK (RagentsClient) 与 Daemon 隧道客户端 (DaemonTunnelHandle)
│   ├── ragents-core/                 # Agent 容器执行内核：ReAct 循环、Wasmtime WASI P2、OS 内核沙箱、SubprocessExecutor
│   ├── ragents-daemon/               # 节点守护进程：本地凭证库、多级配置合并、5 级权限引擎、DaemonInterceptor 管道
│   └── ragents-cli/                  # 命令行工具 `ragents`（包含 `up`、`apiserver`、`daemon`、`node`、`session`、`run`、`approve` 等）
└── docs/                             # 完整设计、配置与进度文档
    ├── CONFIGURATION_GUIDE.zh-CN.md
    ├── PRODUCT_REQUIREMENTS.zh-CN.md
    ├── TECHNICAL_DESIGN.zh-CN.md
    ├── EXTENSION_ARCHITECTURE.zh-CN.md
    ├── IMPLEMENTATION_ROADMAP.zh-CN.md
    └── IMPLEMENTATION_PROGRESS.zh-CN.md
```

---

## 3. 编译与环境准备

### 3.1 编译主程序与内置 WASM 扩展

```bash
# 1. 构建内置 WASM 扩展组件 (shell, file_editor, image_viewer)
./scripts/build-extensions.sh

# 2. 编译 ragents 工作区所有二进制
cargo build --bins

# 3. 运行全仓单元测试与端到端集成测试
cargo test --workspace
```

编译完成后，主入口二进制位于 `./target/debug/ragents`（Release 模式下为 `./target/release/ragents`）。

### 3.2 配置环境变量

```bash
export RAGENTS_BASE_URL="https://your-openresponses-endpoint.example.com/v1"
export RAGENTS_API_KEY="sk-your-api-key"
export RAGENTS_MODEL="gemini-3.8-flash"
```

> 详细的配置文件（`daemon.toml`）字段说明请参见 **[配置详解指南 (docs/CONFIGURATION_GUIDE.zh-CN.md)](./docs/CONFIGURATION_GUIDE.zh-CN.md)**。

---

## 4. 详细使用指南（CLI 全场景实战）

### 4.1 场景一：单机 All-in-One 模式极速运行 (`ragents up`)

如果你在本地单机使用，无需分别启动多个进程，`ragents up` 会在单个进程内自动启动内嵌的 `api-server` 和本地 `agent-daemon`：

```bash
# 1. 启动常驻 All-in-One 服务（Ctrl+C 优雅退出）
./target/debug/ragents up \
  --bind 127.0.0.1:8080 \
  --db .ragents/control.sqlite3 \
  --daemon-config /path/to/daemon.toml

# 2. 或者：启动 All-in-One 并立即在指定工作目录执行一次性任务，执行完毕自动退出
./target/debug/ragents up \
  --bind 127.0.0.1:0 \
  --db /tmp/ragents-control.sqlite3 \
  --daemon-config /path/to/daemon.toml \
  --workdir $(pwd) \
  --model gemini-3.8-flash \
  --prompt "请阅读当前仓库结构并总结每个模块的作用。"
```

---

### 4.2 场景二：分布式集群部署（独立启动 `apiserver` 与多台 `daemon`）

在跨机器或多节点场景下，分别启动控制面与各个计算节点的守护进程：

```bash
# 1. 在中心控制节点启动 api-server
./target/debug/ragents apiserver \
  --bind 0.0.0.0:8080 \
  --db /var/lib/ragents/control.sqlite3 \
  --lease-ttl-ms 30000

# 2. 在节点 A（例如 macOS 开发机）启动 agent-daemon
./target/debug/ragents daemon --config /etc/ragents/daemon-mac.toml

# 3. 在节点 B（例如 Linux 云服务器）启动 agent-daemon
./target/debug/ragents --server http://apiserver-host:8080 daemon --node-id node-linux-01

# 4. 查看当前集群已注册节点及负载、模型支持情况
./target/debug/ragents --server http://127.0.0.1:8080 node list
# 查看单个节点的详细声明式状态 JSON
./target/debug/ragents --server http://127.0.0.1:8080 node get node-mac-studio-01
```

---

### 4.3 场景三：`Pinned`（固定节点/本地目录）vs `Floating`（浮动调度）任务执行

`ragents` 严格区分两种任务调度模式：

#### 方式 A：`Pinned` 模式（必须同时指定 `--node` 和 `--workdir`）
适用于修改某台机器上的指定本地代码仓库，任务严格绑定在该机器和目录，绝不跨机迁移：

```bash
./target/debug/ragents run \
  --node node-mac-studio-01 \
  --workdir /Volumes/storage/src/my-project \
  --model gemini-3.8-flash \
  --extension shell=/Volumes/storage/src/ai/agents/ragents/extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm \
  --extension file_editor=/Volumes/storage/src/ai/agents/ragents/extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm \
  "请检查 src/main.rs 并运行 cargo check"
```

#### 方式 B：`Floating` 模式（指定 `--floating`，或同时省略 `--node` 与 `--workdir`）
适用于通用分析、检索、问答或无需绑定特定宿主机目录的任务。`api-server` 会自动选择健康的且支持该模型的节点，在节点临时目录中运行，并在节点故障时支持自动重调度：

```bash
./target/debug/ragents run \
  --floating \
  --model gemini-3.8-flash \
  "请帮我推导并验证 Raft 租约防脑裂的不变量"
```

> **提示**：`ragents run` 默认开启 `--wait=true`，会实时通过 SSE 流打印调度、Turn、工具调用、权限申请事件并在结束时输出模型回复；若希望后台异步提交立即返回，可传 `--wait=false`。

---

### 4.4 场景四：多轮对话追问、会话管理与跨会话零复制 Fork

```bash
# 1. 列出所有会话及其调度模式、所处阶段、绑定节点和上下文条目数
./target/debug/ragents session list

# 2. 在已有 Session 上发起第二轮追问（传入 --session <session_id>）
./target/debug/ragents run \
  --session sess_01a13d8f-8fe9-7504-9e8b-1a344c074450 \
  "基于刚才的分析，请继续帮我编写单元测试"

# 3. 查看某个 Session 的完整上下文消息列表 (effective_items 与 local_batches)
./target/debug/ragents session context sess_01a13d8f-8fe9-7504-9e8b-1a344c074450

# 4. 跨会话零复制血缘继承 (Fork Session)：
#    从父会话的 [0..3] 上下文切片派生一个全新的子会话（无需复制历史消息数据）
./target/debug/ragents session create \
  --floating \
  --model gemini-3.8-flash \
  --source "sess_01a13d8f-8fe9-7504-9e8b-1a344c074450:0..3" \
  --prompt "我们换一个方向继续实现刚才讨论的方案 B"

# 5. 动态更新会话的模型或状态 (Patch Session)
./target/debug/ragents session patch sess_01a13d8f-8fe9-7504-9e8b-1a344c074450 \
  --model gpt-4.1 \
  --phase closed
```

---

### 4.5 场景五：OS 沙箱拦截与交互式权限审批（支持 `--path` 目录微调与 5 级周期）

当 Agent 执行工具或外部命令（例如 `uv run --with pyyaml ...`）试图写入工作区外部目录（如 `/Users/alice/.cache/uv/sdists-v6/.git`）时：

1. **自动拦截与挂起提示**：
   终端或 `ragents watch` 会实时收到 `PermissionRequested` 事件，同时展示**原始触发路径 (`original_path`)** 与**系统自动优化的建议目录 (`suggested_path`)**：
   ```text
   [permission required] request_id=permreq_01a13dc1-828a-76f9-814e-84c2edfabc64
     original_path : /Users/alice/.cache/uv/sdists-v6/.git
     suggested_path: /Users/alice/.cache/uv (FileWrite { path: "/Users/alice/.cache/uv" })
     reason        : Some("Command `uv run ...` requires file-write access...")
     Approve default : ragents --server <url> approve act_... permreq_... --decision allow-session
     Approve custom  : ragents --server <url> approve act_... permreq_... --decision <allow-once|allow-activation|allow-session|allow-workspace|allow-node> --path <custom_dir>
   ```

2. **执行审批命令（支持 5 种生效周期与 `--path` 手动微调目录）**：
   ```bash
   # 方式 A：直接按系统建议目录 (/Users/alice/.cache/uv) 批准，在当前 Session 内生效
   ./target/debug/ragents approve <activation_id> <request_id> --decision allow-session

   # 方式 B：手动修改授权目录（例如收窄到子目录，或放宽到上级目录），并指定在当前工作目录 (allow-workspace) 级别生效
   ./target/debug/ragents approve <activation_id> <request_id> \
     --decision allow-workspace \
     --path /Users/alice/.cache/uv/sdists-v6

   # 方式 C：仅允许单次命令执行 (allow-once，命令执行完立即从沙箱白名单撤销)
   ./target/debug/ragents approve <activation_id> <request_id> --decision allow-once

   # 方式 D：在整个节点 (allow-node) 范围永久允许该缓存目录
   ./target/debug/ragents approve <activation_id> <request_id> --decision allow-node

   # 方式 E：拒绝该权限请求
   ./target/debug/ragents approve <activation_id> <request_id> --decision deny
   ```
   一旦批准，正在挂起的工具调用会在**同一次命令调用内自动解锁沙箱并重跑命令**；若命令后续还需访问其他新目录，会继续循环递进提示审批，直至命令完成。

---

### 4.6 场景六：双消息队列（FollowUp 等待队列 vs Realtime 实时队列）与后台进程管理

`ragents` 在 `api-server` 中维护每个会话的两条消息队列，并在 `agent-daemon` 中内置完整的后台命令进程管理系统：

1. **实时队列注入 (`ragents steer`)**：
   在某个 `Activation` 正在运行（例如执行多步代码测试）期间，用户或外部系统可随时向该会话的**实时队列 (Realtime Queue)** 推送一条或多条消息块。`agent-core` 会在每个 ReAct Turn 开始前（以及退出循环前）**一次性全部取出**待处理消息，并在单条 `Item::Message` 中保留多个独立的 `MessageContent` 块注入模型上下文：
   ```bash
   ./target/debug/ragents steer sess_01a13d8f-8fe9-7504-9e8b-1a344c074450 \
     "请同时检查边界条件 n=0" \
     "测试通过后先不要删除临时日志"
   ```

2. **后台进程管理 (`shell` / `process_status` / `process_logs` / `process_stop`)**：
   `extensions/shell` 提供 4 个协同工具，支持前后台联合调试（如启动后台被测 Server + 前台运行测试脚本）：
   - `shell`：支持 `wait: true | false`（默认 `true` 同步等待；`false` 异步后台启动并立即返回 `process_id` 与系统 `pid`）以及 `lifecycle: "activation" | "session"`（默认 `"activation"`，在当前 Activation 结束时自动按进程组清理；`"session"` 跨多次 Activation 持续保活）。
   - `process_status`：查询指定 `process_id` 的运行状态（`running` / `exited` / `killed` / `timed_out`）、`pid`、`exit_code` 与运行时长。
   - `process_logs`：按需拉取后台进程的 `stdout`、`stderr` 或 `combined`（标准输出+错误交织）日志，支持 `tail_lines` 与 `offset`。
   - `process_stop`：主动终止指定 `process_id` 及其整个子进程组。
   - **后台进程自然退出实时通知**：当 `wait = false` 的后台进程自行退出时，`agent-daemon` 会自动向 `api-server` 的实时队列推送一条包含 `process_id`、`pid`、`exit_code` 的系统通知，Agent 在下一个 Turn 会自动感知！

---

### 4.7 场景七：实时事件流监控与任务取消 (`watch` / `cancel`)

```bash
# 1. 实时监听全集群事件流（支持从指定 ResourceVersion 断点续传）
./target/debug/ragents watch --after-rv 0

# 2. 仅监听指定 Session 的事件流
./target/debug/ragents watch --session sess_01a13d8f-8fe9-7504-9e8b-1a344c074450

# 3. 取消正在运行的 Activation（子进程将被立即硬终止并回收）
./target/debug/ragents cancel act_01a13d8f-8fea-76c5-9f10-679fb69f27fb
```

---

## 5. Rust Client SDK 编程接入示例

除了 CLI 之外，你可以直接在 Rust 项目中引入 `ragents-client` 与 `api-server` 交互：

```rust
use openresponses_rust::Item;
use ragents_client::RagentsClient;
use ragents_protocol::*;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = RagentsClient::new("http://127.0.0.1:8080")
        .with_auth_token(Some("your-bearer-token".to_string()));

    let created = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("You are a helpful assistant.".into()),
                model: Some(ModelRequestSpec::new("gemini-3.8-flash")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("Hello from ragents SDK!")],
                model_override: None,
            }),
        })
        .await?;

    println!("Created session: {}", created.session.metadata.id);
    Ok(())
}
```

---

## 6. 文档索引

- **[配置详解指南 (CONFIGURATION_GUIDE.zh-CN.md)](./docs/CONFIGURATION_GUIDE.zh-CN.md)**：环境变量、`daemon.toml` 全字段说明、权限规则与扩展配置。
- **[产品需求文档 (PRODUCT_REQUIREMENTS.zh-CN.md)](./docs/PRODUCT_REQUIREMENTS.zh-CN.md)**：核心业务场景、双模调度与安全边界定义。
- **[技术方案设计 (TECHNICAL_DESIGN.zh-CN.md)](./docs/TECHNICAL_DESIGN.zh-CN.md)**：声明式资源模型、SQLite 表结构、状态机与协议规范。
- **[扩展架构设计 (EXTENSION_ARCHITECTURE.zh-CN.md)](./docs/EXTENSION_ARCHITECTURE.zh-CN.md)**：`agent-daemon` 与 `api-server` 的 WASM / Webhook 扩展体系设计。
- **[分阶段实施路线图 (IMPLEMENTATION_ROADMAP.zh-CN.md)](./docs/IMPLEMENTATION_ROADMAP.zh-CN.md)**：Stage 0 – Stage 7 工程演进拆解。
- **[双消息队列与后台命令进程管理系统设计 (PROCESS_AND_QUEUE_DESIGN.zh-CN.md)](./docs/PROCESS_AND_QUEUE_DESIGN.zh-CN.md)**：`FollowUp` 等待队列与 `Realtime` 实时队列、后台进程生命周期（`activation` vs `session`）与三通道日志查询设计。
- **[实施进度记录 (IMPLEMENTATION_PROGRESS.zh-CN.md)](./docs/IMPLEMENTATION_PROGRESS.zh-CN.md)**：各阶段完成状态、Commit 记录与测试验证矩阵。
