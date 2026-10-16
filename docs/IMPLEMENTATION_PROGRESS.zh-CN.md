# ragents 实施进度记录

更新日期：2026-10-14  
关联文档：
- [产品需求文档](./PRODUCT_REQUIREMENTS.zh-CN.md)
- [详细技术方案](./TECHNICAL_DESIGN.zh-CN.md)
- [分阶段实施路线图](./IMPLEMENTATION_ROADMAP.zh-CN.md)
- [扩展架构设计文档](./EXTENSION_ARCHITECTURE.zh-CN.md)
- [双消息队列与后台命令进程管理系统设计](./PROCESS_AND_QUEUE_DESIGN.zh-CN.md)

---

## 1. 阶段完成总览

目前 **S0 至 S7（含 OS 内核沙箱、WASI 0.2 原生权限统一、Daemon / API Server 扩展体系、以及双消息队列与后台命令进程管理系统）已全部落地并通过全量测试与严格 Clippy 检查**，每个阶段均已独立提交 Git Commit：

| 阶段 | 状态 | Commit | 核心完成内容 |
| :--- | :---: | :--- | :--- |
| **Docs 基线** | ✅ 已完成 | `776f05c`, `6f0a150` | 全新架构设计：PRD、详细技术方案、分阶段实施路线图、Daemon & API Server 扩展架构设计 |
| **S0：Workspace 与协议层** | ✅ 已完成 | `7c20604` | 6 个 Workspace Crate 骨架；`ragents-protocol` 声明式资源（`Node`、`Session`、`Activation`、`ConfigRevision`、`PermissionSnapshot`、`ItemBatch`、`SessionEventEnvelope`）与 REST/SSE/WebSocket 消息协议 |
| **S1：存储引擎与双模调度器** | ✅ 已完成 | `8d3aad0` | `ragents-apiserver::store`：SQLite WAL 9 张核心表、全局单调递增 `ResourceVersion`、`Pinned`（绑定节点+目录，不跨机迁移）与 `Floating`（自动调度+临时目录+跨机容灾重调度）双模式调度器、`lease_epoch` 防脑裂 Fencing Token、`SessionSource` 跨会话血缘展开 |
| **S2：API Server 网络层与 Client SDK** | ✅ 已完成 | `92bfcda` | `ragents-apiserver::server`（Axum HTTP REST + SSE `/api/v1/watch` + WebSocket `/api/v1/daemon/tunnel`）与 `ragents-client`（`RagentsClient` + `DaemonTunnelHandle`） |
| **S3：Agent 容器内核与 WASM 扩展** | ✅ 已完成 | `7b84db7` | `ragents-core`：纯 OpenResponses 模型对接（`ModelBackend` / `OpenResponsesModelBackend`）、`ContextProjection`、Wasmtime WASI P2 `HookManager`、显式 `workspace_root` 目录隔离的 `InProcessExecutor`、以及 `shell` / `file_editor` / `image_viewer` 三个标准 WASM 组件构建脚本 |
| **S4：节点守护进程 (`agent-daemon`)** | ✅ 已完成 | `871aff4` | `ragents-daemon`：节点本地模型凭证库（保管 `base_url` 与 `api_key`，仅向 `api-server` 上报 `supported_models`）、多级配置合并与 SHA-256 快照回写、权限门卫（支持 `Allow` / `Deny` / `Ask` 挂起等待审批）、调谐循环（Reconcile Loop） |
| **S5：控制台 CLI、`ragents up` 与端到端验收** | ✅ 已完成 | `8df9ae7` | `ragents-cli`：提供 `ragents up`（单进程内嵌 `apiserver` + `daemon`）、`apiserver`、`daemon`、`node`、`session`、`run`、`cancel`、`approve`、`watch` 全套命令及跨机/WASM/权限审批 E2E 验收测试 |
| **S6：子进程隔离执行器 (`SubprocessExecutor`)** | ✅ 已完成 | `701fe02`, `4735a0a` | 单二进制自孵化 `internal-core-worker` 子进程隔离执行、单 Daemon 共享 UDS Listener（`SharedIpcHub`，TCP Loopback 自动回退）多路复用 IPC、子进程环境自动剥离 `RAGENTS_API_KEY`、Wasmtime `.cwasm` 内容寻址预编译缓存 |
| **S7A：OS 内核命令沙箱与自动化多级权限审批** | ✅ 已完成 | `98cb7e2`, `5df048b`, `bb84cb6` | Host Import 强制鉴权、macOS `sandbox-exec` (Seatbelt) / Linux `bwrap` 内核沙箱、越界路径双通道（`stderr` + 内核日志）自动识别与多目录循环申请、同时展示 `original_path` 与 `suggested_path`、支持 `--path` 手动微调审批目录及 5 级生效周期（`allow-once` / `allow-activation` / `allow-session` / `allow-workspace` / `allow-node`） |
| **S7B：`agent-daemon` 拦截器扩展体系** | ✅ 已完成 | `bab6fc7` | `DaemonInterceptor` 原生 Trait + WASM 组件驱动 + HTTP/UDS Webhook 驱动，支持 `before_model_request`（修改标准模型参数、注入供应商扩展字段、注入自定义 Header、动态轮换 `api_key`）、`after_model_response` 与 `evaluate_permission` |
| **S7C：`api-server` 鉴权与准入控制扩展体系** | ✅ 已完成 | `e7149b6`, `8d95166` | `AuthnPlugin`（区分 `client_http` 与 `daemon_tunnel`）、`AuthzPlugin`（支持 `enforced_label_selector` 多租户透明隔离）、`AdmissionPlugin`（`Mutating` 变更准入与 `Validating` 校验准入）及 CLI/SDK `--auth-token` 支持 |
| **S7D：原生 WASI 0.2 权限统一与控制面 WASM 隔离** | ✅ 已完成 | `268cc28`, `c6056c1`, `e6b9efb` | 保持 WIT 最小化，直接接管 WASI 0.2 `wasi:filesystem/types::HostDescriptor`、`wasi:http/outgoing-handler` (`WasiHttpView`) 与 `wasi:sockets` (`socket_addr_check`)；控制面 WASM 插件默认开放网络、隔离宿主机 `/` 并仅挂载私有目录与 `allowed_dirs` |
| **S7E：双消息队列与后台命令进程管理系统** | ✅ 已完成 | `5877f9d`, `8dc7169` | `api-server` 双消息队列（`FollowUp` 等待队列 + `Realtime` 实时队列，支持单次全部取出并保持多个独立 `MessageContent` 块）；`agent-daemon` 托管 `ProcessManager` 支持 `wait: bool`、`lifecycle: "activation" \| "session"`、默认无超时限制（`0`）、阻塞超时按进程组强杀并保留超时前输出、`stdout`/`stderr`/`combined` 三通道日志查询、后台进程自然退出实时通知及 `ragents steer` CLI |
| **S7F：子模块拆分、工具输出保护、SSE 流式对账与 Skills 扩展** | ✅ 已完成 | `c65aea9`, `80da66e`, `acb709f`, `3800c72`, `2d1cfb7`, `3cb7262`, `891734d` | 全仓大文件按职责拆分为清晰子模块；`read_file` 支持 `offset`/`limit` 与 2000行/50KB 头部截断、`replace_in_file` 支持针对原文件的多块无重叠原子替换、`shell` 扩展支持 2000行/50KB 尾部截断与落盘；OpenResponses SSE 实时流式推送（`ModelTextDelta`/`ModelReasoningDelta`）+ `TurnCompleted { items }` 权威完整版对账兜底 + 瞬态错误指数退避重试；新增 `extensions/skills` WASM 扩展与 `ExtensionSpec.allowed_dirs` 声明式目录授权 |
| **S7G：三会话自动上下文压缩扩展 (`extensions/compaction`) 与多代接力血缘** | ✅ 已完成 | 最新提交 | 新增 `extensions/compaction` WASM 扩展（完整复刻 `pi` 的切分点选择 `find_cut_point`、平行工具调用防截断、`<conversation>` 序列化、`<read-files>` / `<modified-files>` 累积追踪与 `<previous-summary>` 增量合并更新）；在 SQLite 新增 `session_compactions` 关系表并在 `SessionStatus.compaction` 维护双向三元组血缘（`sess_1 -> sess_compact -> sess_2 -> sess_compact_2 -> sess_3`），自动迁移会话级权限与 `lifecycle = "session"` 后台进程，CLI `watch_activation_to_completion` 无缝跨会话接力跟随 |

---

## 2. 已验证的核心能力与测试矩阵

当前全仓自动化测试共覆盖以下关键不变量与端到端链路：

1. **协议与内容寻址不变量 (`crates/ragents-protocol/src/lib.rs`)**
   - `SchedulingMode::Pinned`（强制要求安全 `node_id` 与绝对路径 `workspace_path`）与 `SchedulingMode::Floating` 校验。
   - `ConfigRevision` 与 `PermissionSnapshot` 规范化 JSON SHA-256 哈希稳定性。
   - 原生 `openresponses_rust::Item` 批次序列化往返一致性。
   - `SessionSource` 固定范围校验。

2. **控制面存储、双模式调度与防脑裂 (`crates/ragents-apiserver/src/lib.rs`)**
   - **`Pinned` 不迁移保证**：当 `node_a` 租约过期而 `node_b` 在线时，`Pinned` 会话的任务转为 `Interrupted` 或保持 `Queued` 等待 `node_a`，绝不漂移至 `node_b`。
   - **`Floating` 自动故障切换与 `lease_epoch` Fencing**：当 `node_a` 租约过期时，未开始的 `Floating` 任务自动重调度至 `node_b` 并递增 `lease_epoch`；`node_a` 恢复后携带旧 `lease_epoch` 的延迟写入被 `StaleLeaseEpoch` 严格拦截。
   - **跨会话零复制血缘 (`SessionSource`)**：子会话通过 `ForkedFrom [from..=through]` 零复制继承父会话上下文切片并与本地新批次拼接。

3. **HTTP REST + SSE Watch + WebSocket Daemon 隧道 (`crates/ragents-client/tests/server_client_roundtrip.rs`)**
   - 验证 Daemon 通过 WebSocket 主动拨号注册节点及 `supported_models`、接收 `DispatchActivation`、提交 `ItemBatch` 与 `SessionEvent`，客户端通过 SSE `/api/v1/watch?after_rv=0` 实时按序接收全部事件。

4. **Agent 容器内核与 WASM 组件隔离执行 (`crates/ragents-core/src/lib.rs`)**
   - 验证 `InProcessExecutor` ReAct 循环与 `file_editor` WASM 组件加载执行；相对路径在 WASI guest `"."` 映射下精确落入 `workspace_root`，不污染宿主机全局 `cwd`。

5. **多节点集群 `Floating` 任务跨机接力 (`crates/ragents-daemon/src/lib.rs`)**
   - 启动 `node_a` 完成 `Floating` 会话第一轮对话 $\rightarrow$ `node_a` 下线超时 $\rightarrow$ 启动 `node_b` 提交第二轮追问 $\rightarrow$ `api-server` 自动将第二轮调度至 `node_b`，且 `node_b` 从 `api-server` 完整拉取到第一轮在 `node_a` 产出的历史 `ItemBatch`。

6. **真实 OpenResponses HTTP 协议 + WASM 扩展 + 声明式权限审批 (`crates/ragents-cli/tests/e2e_cluster_acceptance.rs`)**
   - 启动标准 OpenResponses HTTP 服务端（`/v1/responses`）+ `ragents-apiserver` + `ragents-daemon` + `RagentsClient`。
   - 配置 `file_write` 默认策略为 `Ask`，触发 WASM `write_file` 工具调用 $\rightarrow$ 激活进入 `WaitingForInteraction` 并推送 `PermissionRequested` 事件 $\rightarrow$ 客户端声明式调用 `respond_permission(AllowOnce)` $\rightarrow$ Daemon 收到 `ReconcileActivation` 唤醒工具完成文件写入并最终 `Succeeded`。

---

## 3. 常用构建、验证与运行命令

```bash
# 1. 编译全部 WASM 扩展 (shell, file_editor, image_viewer, skills, compaction)
./scripts/build-extensions.sh

# 2. 运行全仓格式化检查、测试与严格 Clippy
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# 3. 单机 All-in-One 快速启动与运行
cargo run -p ragents-cli -- up --prompt "Hello ragents!"

# 4. 独立启动控制面与多节点 Daemon
cargo run -p ragents-cli -- apiserver --bind 127.0.0.1:8080 --db .ragents/control.sqlite3
cargo run -p ragents-cli -- --server http://127.0.0.1:8080 daemon --node-id node_a

# 5. 通过 CLI 查看节点、创建 Pinned / Floating 会话并监听事件
cargo run -p ragents-cli -- node list
cargo run -p ragents-cli -- run --floating "Analyze this problem"
cargo run -p ragents-cli -- run --node node_a --workdir /tmp/my-proj "Edit files in /tmp/my-proj"
```
