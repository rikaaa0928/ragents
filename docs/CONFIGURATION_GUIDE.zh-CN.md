# ragents 配置详解指南（Configuration Guide）

本文档详细介绍 `ragents` 的环境变量、`agent-daemon` 配置文件（`daemon.toml`）、权限策略规则、多级配置合并机制以及 `agent-daemon` / `api-server` 扩展插件的配置方法。

---

## 1. 环境变量一览

`ragents` 严格收敛了环境变量依赖（不读取 `OPENAI_*` 或 `ROSETTA_*`），所有支持的环境变量如下：

| 环境变量名 | 作用范围 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `RAGENTS_BASE_URL` | `daemon` / `up` | `http://127.0.0.1:8000/v1` | 未指定 `daemon.toml` 时默认使用的 OpenResponses 模型服务地址（自动兼容是否带 `/v1` 后缀） |
| `RAGENTS_API_KEY` | `daemon` / `up` | `sk-placeholder` | 默认的模型 API Key，或在 `daemon.toml` 中通过 `api_key_env = "RAGENTS_API_KEY"` 引用。**在 `SubprocessExecutor` 拉起子进程时会被自动从子进程环境变量中剥离** |
| `RAGENTS_MODEL` | `daemon` / `up` | `gpt-4.1` | 未指定 `daemon.toml` 时默认注册的模型名称 |
| `RAGENTS_SERVER_URL` | `cli` / `daemon` | `http://127.0.0.1:8080` | CLI 与 Daemon 默认连接的 `api-server` 地址（可被命令行 `--server` 覆盖） |
| `RAGENTS_AUTH_TOKEN` | `cli` / `daemon` | 无 | 访问开启了 `Authn` 认证的 `api-server` 时自动携带的 `Bearer` Token（可被命令行 `--auth-token` 覆盖） |
| `RAGENTS_WORKER_BIN` | `daemon` | 当前可执行文件自身 | 自定义 `SubprocessExecutor` 使用的 worker 二进制路径（默认使用 `ragents internal-core-worker` 单二进制自孵化） |

---

## 2. `daemon.toml` 完整配置参考

`agent-daemon` 是运行在各个计算节点（开发机、云服务器、GPU/CPU 节点）上的宿主守护进程，负责管理**节点本地模型凭证**、**WASM 工具扩展**、**模型请求拦截器**以及**默认安全权限边界**。

### 2.1 完整生产级配置示例

```toml
# ============================================================================
# 1. 节点基础配置
# ============================================================================
# 节点在集群中的唯一 ID
node_id = "node-mac-studio-01"

# 控制面 api-server 地址（也可通过 CLI `--server` 或环境变量 `RAGENTS_SERVER_URL` 覆盖）
server_url = "http://127.0.0.1:8080"

# 可选：连接 api-server WebSocket 隧道的认证 Token
# auth_token = "tok_daemon_prod_01"

# 执行器隔离模式：
# - "subprocess"（默认推荐）：每次 Activation 启动独立 OS 子进程 + 共享 UDS IPC + 自动剥离 API Key
# - "in_process"：同进程异步协程模式（仅建议在轻量调试时使用）
executor_mode = "subprocess"

# 当前节点允许同时并发运行的 Activation 任务数上限
max_concurrent_runners = 4

# 单次 Activation 最大 ReAct 循环轮数限制（不填则默认无上限，直至任务完成或被取消）
# max_turns = 50

# 节点级默认系统提示词（当 Session 未指定 system_prompt 时生效）
default_system_prompt = "You are a helpful AI engineering assistant managed by ragents."

# 节点默认模型名（必须存在于下方 [models] 表中）
default_model = "gemini-3.8-flash"

# 节点管理 Floating 会话工作区及会话临时文件的本地根目录
temp_root_dir = "/tmp/ragents-node-01"

# 节点标签（用于 Floating 会话的 node_selector 亲和性调度）
[labels]
region = "cn-east"
os_family = "macos"
tier = "high-memory"

# ============================================================================
# 2. 节点本地模型凭证库（核心安全边界：base_url 与 api_key 绝不上传 api-server）
# ============================================================================
[models."gemini-3.8-flash"]
base_url = "https://your-openresponses-gateway.example.com/v1"
# 方式 A：从指定环境变量读取 API Key（推荐）
api_key_env = "RAGENTS_API_KEY"
# 方式 B：直接填写明文 Key（仅限受控机器本地配置文件）
# api_key = "sk-xxxx"
default_temperature = 0.2
default_max_output_tokens = 8192

# 可为该模型静态附加自定义 HTTP Header
[models."gemini-3.8-flash".extra_headers]
x-provider-route = "fast-lane"

[models."gpt-4.1"]
base_url = "https://your-openresponses-gateway.example.com/v1"
api_key_env = "RAGENTS_API_KEY"
default_temperature = 0.1

# ============================================================================
# 3. 节点预装 WASM 工具扩展（wasm32-wasip2 组件）
# ============================================================================
[[extensions]]
name = "shell"
path = "/Volumes/storage/src/ai/agents/ragents/extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm"
enabled = true
config = { default_timeout_seconds = 1800 }

[[extensions]]
name = "file_editor"
path = "/Volumes/storage/src/ai/agents/ragents/extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm"
enabled = true
config = {}

[[extensions]]
name = "image_viewer"
path = "/Volumes/storage/src/ai/agents/ragents/extensions/image_viewer/target/wasm32-wasip2/release/ragent_image_viewer_extension.wasm"
enabled = true
config = {}

# ============================================================================
# 4. 节点默认权限与沙箱边界策略
# ============================================================================
[default_permissions.file_read]
# 显式禁止读取的敏感目录（在 macOS Seatbelt / Linux bwrap 内核层同样强制阻断）
deny = ["/Users/yourname/.ssh", "/Users/yourname/.aws"]
# 允许直接读取的额外目录（workspace_root 与 session_tmp_dir 默认自动允许）
allow = []
# 工作区外未匹配规则时的默认动作："allow" | "deny" | "ask"
default = "ask"

[default_permissions.file_write]
deny = ["/etc", "/usr"]
# 例如可预先放行本地包管理器缓存目录，避免每次询问
# allow = ["/Users/yourname/.cache/uv"]
default = "ask"

[default_permissions.command_work_dir]
# 是否在每次执行 shell 命令前要求对工作目录进行审批
# 若不配置，在 workspace_root 内默认 allow，越出工作区自动触发 ask
# default = "ask"

[default_permissions.http_hosts]
# 允许直接访问的域名列表（支持 "*"、"api.example.com" 或 "*.example.com" 通配符）
# 若不配置 allow，默认策略为 "ask"，当 WASM 扩展调用 http-fetch 访问外部域名时会自动弹出交互式权限申请
allow = ["*"]
default = "ask"

# ============================================================================
# 5. Daemon 拦截器扩展（支持 WASM 组件与 HTTP/UDS Webhook）
# ============================================================================
[[interceptors]]
id = "model-param-and-obs-hook"
# driver 可选 "webhook" 或 "wasm"
driver = "webhook"
# 支持 http://, https://, 或本地 Unix Domain Socket unix:///path/to.sock
endpoint = "unix:///tmp/ragents-ipc/model_interceptor.sock"
# 可拦截的钩子：["before_model_request", "after_model_response", "evaluate_permission"]
hooks = ["before_model_request", "after_model_response"]
priority = 10
timeout_ms = 2000
# fail_closed（默认，插件失败则拒绝调用） | fail_open（插件失败则忽略并继续，适合可观测上报）
fail_policy = "fail_open"
config = { inject_thinking_budget = 4096 }
```

---

## 3. 多级配置合并与不可变快照机制

当一个 `Activation` 被调度到某个 `agent-daemon` 节点执行时，节点会按以下优先级合并配置（**右者覆盖左者**）：

```text
[Node 级 daemon.toml 默认配置]  ──►  [Session 级 SessionSpec 配置]  ──►  [Activation 级 model_override]
```

1. **模型选择优先级**：
   - `ActivationSpec.model_override.name` > `SessionSpec.model.name` > `DaemonConfig.default_model` > `DaemonConfig.models` 中的第一个模型。
2. **扩展列表合并**：
   - 以 `extension.name` 为键，将 `DaemonConfig.extensions` 与 `SessionSpec.extensions` 合并，同名扩展以 `SessionSpec` 为准。
3. **权限策略合并**：
   - 将 `DaemonConfig.default_permissions`、当前会话/工作目录/节点已通过 `ragents approve` 动态授予的权限（`session_grants` / `workspace_grants` / `node_grants`）以及 `SessionSpec.permissions` 合并为最终生效策略。
4. **内容寻址快照（`ConfigRevision` & `PermissionSnapshot`）**：
   - 合并完成后，`agent-daemon` 会剔除所有敏感凭证（`api_key`、`base_url`），对规范化 JSON 计算 SHA-256 摘要（生成 `cfg_sha256:...` 与 `permsnap_sha256:...`），通过 `AcceptActivation` 回写至 `api-server` 审计归档。

---

## 4. 三大能力域统一权限审批说明（Shell / WASM 文件 / WASM 网络）

`ragents` 将外部能力统一收敛为三大权限域，所有权限检查均强制收口在 `ragents-core` 的 WASM Runtime / Host Import 层：

1. **宿主提供的 Shell（`execute-command-with-timeout` + OS 内核沙箱）**：
   - **macOS**：通过 `/usr/bin/sandbox-exec`（Seatbelt）在内核层拦截未授权的 `file-write*` 与被拒的 `file-read-data`。
   - **Linux**：通过 `/usr/bin/bwrap`（Bubblewrap）只读根挂载 + 授权目录读写绑定。
   - **Windows**：不做额外 OS 沙箱限制。
   - 当命令触碰外部目录失败时，自动捕获路径、发起递进审批并在获批后原地重跑命令。
2. **WASM 文件系统（WASI `preopened_dir` 动态挂载 + `FileRead` / `FileWrite`）**：
   - 当 `file_editor.wasm`（`read_file` / `write_file` / `replace_in_file`）、`image_viewer.wasm` 或自定义 WASM 插件读写工作区外文件时，自动触发 `FileRead` / `FileWrite` 审批。
   - 获批后，`WasmPlugin` 自动将批准的目录通过 `WasiCtxBuilder::preopened_dir` 动态挂载进 WASI 容器（读权限挂载为 `DirPerms::READ`，写权限挂载为 `DirPerms::all()`），若为 `allow-once` 则在调用结束后自动卸载。
3. **WASM 网络请求（WIT `http-fetch` + `CapabilityRequest::Http`）**：
   - 当 WASM 扩展通过 `http-fetch` 发起 HTTP/HTTPS 请求时，宿主自动解析 URL 的 `scheme`、`host`、`port` 并匹配 `http_hosts` 策略。
   - 若域名未在 `allow` 列表中，自动触发 `CapabilityRequest::Http` 交互式审批。
4. **双路径展示与手动调整（`--path`）**：
   - 触发权限请求时，同时记录并展示：
     - `original_path`：实际触发拦截的具体文件路径（如 `/Users/alice/.cache/uv/sdists-v6/.git`）或完整请求 URL（如 `https://api.example.com/v1/data`）
     - `suggested_path`：系统自动向上收敛后的建议目录（如 `/Users/alice/.cache/uv`）或目标域名（如 `api.example.com`）
   - 用户在执行 `ragents approve` 时，可通过 `--path <custom_dir_or_host_pattern>` 任意收窄或放宽实际授权目录或域名通配符（如 `--path "*.example.com"`）。
5. **5 级生效周期（`--decision`）**：
   - `deny`：拒绝本次权限申请。
   - `allow-once`：仅对当前单次命令/工具调用生效，该次命令执行完毕后立即从沙箱/WASI 白名单撤销。
   - `allow-activation`：在当前 `Activation` 的所有 ReAct 轮次内有效。
   - `allow-session`：在当前 `Session` 的所有多轮对话（多个 `Activation`）内有效。
   - `allow-workspace`：在当前节点上所有绑定同一 `workspace_root` 工作目录的 `Session` 间共享有效。
   - `allow-node`：在当前 `agent-daemon` 节点运行的所有任务间全局有效。
