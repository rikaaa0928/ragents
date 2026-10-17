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
path = "./extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm"
enabled = true
# default_timeout_seconds = 0 表示默认无超时限制（与 pi 一致，支持单次调用传 timeout 秒数）
# max_output_lines / max_output_bytes 控制阻塞命令输出尾部截断阈值（超出部分自动落盘至 .ragents-tmp/）
config = { default_timeout_seconds = 0, max_output_lines = 2000, max_output_bytes = 51200 }

[[extensions]]
name = "file_editor"
path = "./extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm"
enabled = true
config = {}

[[extensions]]
name = "image_viewer"
path = "./extensions/image_viewer/target/wasm32-wasip2/release/ragent_image_viewer_extension.wasm"
enabled = true
config = {}

[[extensions]]
name = "skills"
path = "./extensions/skills/target/wasm32-wasip2/release/ragent_skills_extension.wasm"
enabled = true
# 声明式授予该扩展额外允许读取的宿主机目录（支持 ~/ 展开；会自动挂载至 WASI 并合并进 PermissionSnapshot）
allowed_dirs = ["~/.agents/skills"]
config = { include_workspace = true, include_global = true, include_agents_md = true }

[[extensions]]
name = "compaction"
path = "./extensions/compaction/target/wasm32-wasip2/release/ragent_compaction_extension.wasm"
enabled = true
# 自动上下文压缩阈值配置：
# - trigger_tokens：当上下文估算或模型返回的 Token 数达到该阈值时，自动触发三会话接力压缩（默认 64000，可在超长上下文模型质量衰减前主动提前压缩）
# - keep_recent_tokens：压缩时原样保留在 ContinuedFrom 尾部切片中的最近对话 Token 数（默认 20000，自动在 Assistant Step / User 边界切分，绝不切断平行 FunctionCall 与 FunctionCallOutput）
# - custom_instructions：可选的自定义摘要侧重点指令
config = { enabled = true, trigger_tokens = 64000, keep_recent_tokens = 20000 }

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

# 若使用 driver = "wasm"（同样适用于 api-server 的 Authn/Authz/Admission WASM 插件）：
# - 网络权限（wasi:http / wasi:sockets）默认全部放开；
# - 文件权限严禁挂载宿主机根目录 "/"，仅默认挂载插件专属临时目录，以及通过 `allowed_dirs` 显式授权的目录：
# [[interceptors]]
# id = "local-wasm-audit"
# driver = "wasm"
# path = "/etc/ragents/plugins/audit_hook.wasm"
# allowed_dirs = ["/var/log/ragents-audit", "/etc/ragents/certs"]
# hooks = ["after_model_response"]
# fail_policy = "fail_open"
```

---

## 3. 多级配置优先级、合并规则与不可变快照机制

在 `ragents` 中，**CLI 命令行参数是生成或修改声明式配置资源（`DaemonConfig`、`SessionSpec`、`ActivationSpec`）的入口**，真正的配置合并发生在 `agent-daemon` 接管任务时的 `DaemonConfig::merge_for_activation` 中。

### 3.1 总体优先级顺序（由高到低）

```text
[单次激活覆盖 ActivationSpec] ──► [会话声明 SessionSpec] ──► [节点配置 daemon.toml] ──► [环境变量兜底]
```

1. **CLI 参数与各层配置资源的映射关系**：
   - `ragents daemon --config daemon.toml` / `ragents up --daemon-config daemon.toml`：加载节点级基线配置 **`DaemonConfig`**（若未指定 `--config`，则从 `RAGENTS_BASE_URL`、`RAGENTS_API_KEY`、`RAGENTS_MODEL` 环境变量生成默认 `DaemonConfig`）。
   - `ragents session create ...` 或首次 `ragents run ...`：命令行参数（`--model`、`--system-prompt`、`--extension name=path[:dir1,dir2]`、`--node`、`--workdir`）直接组装为 **`SessionSpec`** 提交至 `api-server`。
   - `ragents run --session <existing_id> --model <override_model> <prompt>`：在已有会话上发起新一轮输入时，`--model` 写入当次激活的 **`ActivationSpec.model_override`**，仅覆盖当前这一轮 Activation。

### 3.2 各配置项的具体合并规则

1. **模型与推理参数（Model & Parameters）**：
   - **模型名称 (`model.name`)**：
     `ActivationSpec.model_override.name` $\succ$ `SessionSpec.model.name` $\succ$ `daemon.toml` 的 `default_model` $\succ$ `daemon.toml` 的 `[models]` 表中第一个模型。
     *(注：确定模型名称后，其连接的 `base_url`、`api_key`、`extra_headers` **100% 强制取自执行节点本地 `daemon.toml` 的 `[models.<name>]`**，客户端或 `SessionSpec` 绝不能覆盖节点本地凭证。)*
   - **采样与推理参数 (`temperature` / `max_output_tokens` / `reasoning`)**：
     `ActivationSpec.model_override` $\succ$ `SessionSpec.model` $\succ$ `daemon.toml` 中该模型的默认值。
2. **系统提示词（System Prompt）**：
   - 若 `SessionSpec.system_prompt`（或 CLI `--system-prompt`）非空则优先使用；否则回退使用 `daemon.toml` 的 `default_system_prompt`。
   - 随后在 `agent-core` 启动时，挂载的 WASM 扩展（如 `skills` 扩展）可通过 `agent.prepare` Hook 进一步向有效 `system_prompt` 动态追加 `<workspace_instructions>`（`AGENTS.md`）与 `<available_skills>`。
3. **WASM 扩展列表（`extensions`）—— 按扩展名 `name` 字典合并覆盖**：
   - 先加载 `daemon.toml` 中的所有 `[[extensions]]` 作为基线字典（以 `ext.name` 为 Key）。
   - 再遍历 `SessionSpec.extensions`（即 CLI `--extension` 传入的扩展）：
     - **不同名扩展**：取并集（例如 `daemon.toml` 默认配置了 `shell` 与 `file_editor`，CLI 传入 `--extension skills=...:~/.agents/skills`，最终三者同时生效）。
     - **同名扩展**：`SessionSpec` 中的同名 `ExtensionSpec` 完整覆盖 `daemon.toml` 中的对应项（可用于在特定会话中覆盖某扩展的 `config`、`allowed_dirs` 或将 `enabled` 设为 `false`）。
4. **权限策略层（`permissions`）与扩展 `allowed_dirs`**：
   - **基础策略合并**：`SessionSpec.permissions` 覆盖 `daemon.toml` 的 `default_permissions`（按 `file_read`、`file_write`、`http_hosts`、`command_work_dir` 四个维度的 `deny` / `allow` / `default` 逐项覆盖，未指定的维度继承 `daemon.toml`）。
   - **叠加启用扩展的 `allowed_dirs`**：将最终生效（`enabled = true`）的所有扩展声明的 `allowed_dirs` 自动合并追加到 `effective_permissions.file_read.allow` 中，同时在 WASI 0.2 `preopen_dirs` 与 OS 内核沙箱 `allowed_read_paths` 中注册。
   - **单规则集内部判定铁律（Deny 永远最高）**：
     在同一个规则集内，无论 `allow` 来自节点默认配置、会话配置还是扩展 `allowed_dirs`，执行判定顺序永远是：
     **① 命中 `deny` 立即拒绝 $\succ$ ② 命中 `allow`（含 `allowed_dirs` 与运行时动态授权）放行 $\succ$ ③ 命中 `default` 或工作区默认边界策略**。
5. **内容寻址审计快照（`ConfigRevision` & `PermissionSnapshot`）**：
   - 每次 `Activation` 开始执行前，`agent-daemon` 剔除所有敏感凭证（`api_key`、`base_url`），对合并后的规范化 JSON 计算 SHA-256 摘要（生成 `cfg_sha256:...` 与 `permsnap_sha256:...`），通过 `AcceptActivation` 回写至 `api-server` 持久化归档，确保每次历史运行均可精确复盘。

---

## 4. 三大能力域统一权限审批说明（Shell / WASI 0.2 文件 / WASI 0.2 网络）

`ragents` 遵循**最小宿主导入原则**：WIT `ragent:extension/host` 仅提供 `manage-process` 一个宿主进程管理函数，WASM 内部的文件读写与网络请求全部通过 **WASI 0.2 标准内置权限机制** 管控：

1. **宿主提供的命令进程管理（`manage-process` + OS 内核沙箱）**：
   - **macOS**：通过 `/usr/bin/sandbox-exec`（Seatbelt）在内核层拦截未授权的 `file-write*` 与被拒的 `file-read-data`。
   - **Linux**：通过 `/usr/bin/bwrap`（Bubblewrap）只读根挂载 + 授权目录读写绑定。
   - **Windows**：不做额外 OS 沙箱限制。
   - 当命令触碰外部目录失败时，自动捕获路径、发起递进审批并在获批后原地重跑命令。
2. **WASI 0.2 文件系统（`wasi:filesystem/types::HostDescriptor`）**：
   - 直接在 WASI 0.2 `HostDescriptor` 的 `open-at`、`create-directory-at`、`unlink-file-at`、`remove-directory-at`、`rename-at` 等异步调用处拦截权限。
   - 当任何 WASM 插件通过标准库 `std::fs` 读写工作区外或受限目录的文件时，Wasmtime 在 `open-at` 处自动挂起协程并发起 `FileRead` / `FileWrite` 审批；用户批准后当场完成 `open-at` 并返回文件句柄，无需重启或重试 WASM 函数。
3. **WASI 0.2 网络请求（`wasi:http/outgoing-handler` & `wasi:sockets`）**：
   - 通过 Wasmtime WASI 0.2 内置的 `WasiHttpView::send_request` 与 `WasiCtxBuilder::socket_addr_check` 拦截所有出站 HTTP/HTTPS 请求与底层 TCP/UDP Socket 连接。
   - 若目标域名或地址未在 `http_hosts.allow` 列表中，自动触发 `CapabilityRequest::Http` 交互式审批。
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
