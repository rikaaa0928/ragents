# ragents 三层扩展架构设计文档（`api-server`、`agent-daemon` 与 `agent-core`）

- **文档状态**：Implemented（已实现）
- **关联文档**：
  - [产品需求文档 (PRD)](./PRODUCT_REQUIREMENTS.zh-CN.md)
  - [技术方案设计文档 (Technical Design)](./TECHNICAL_DESIGN.zh-CN.md)
  - [实施路线图 (Implementation Roadmap)](./IMPLEMENTATION_ROADMAP.zh-CN.md)

---

## 1. 背景与三层扩展边界（Three-Tier Extension Boundary）

`ragents` 将扩展能力严格划分为三个互不越界的平面：**集群控制面（`api-server`）**、**节点守护面（`agent-daemon`）** 与 **沙箱执行内核（`agent-core`）**：

1. **`agent-core` WASM 工具扩展（`wit/ragent-extension.wit`）—— 仅限工具注册、提示词追加与上下文压缩**
   - `agent-core` 运行在受限的沙箱子进程（`SubprocessExecutor`）中，负责加载 `shell`、`file_editor`、`image_viewer`、`agents_md`、`skills`、`compaction`、`chatroom` 等 `wasm32-wasip2` 组件。
   - **严格收窄的 `agent.prepare` 契约**：插件在 `agent.prepare` 阶段仅可返回 `ExtensionPrepareOutput { append_system_prompt: Option<String>, tools: Vec<ToolDefinition> }`（只读追加第四段 `extension_prompts` 并注册工具），**严禁读取或篡改上层 `system_prompt`、`model` 或历史上下文**。
   - **移除中途隐式篡改钩子**：系统移除了早期设计中的 `input.prepare`、`turn.prepare`、`model.request.prepare`、`model.response.prepare`、`context.append.prepare`，并限制 `context.project.prepare` 仅用于触发三会话上下文压缩（`CompactionPlan`）。`agent.prepare` 完成后，`agent-core` 立即通过 `ReportPreparedConfig` 将组装好的四段式 `PromptParts` 与过滤 `ActivationSpec.disabled_tools` 后的 `effective_tools` 回写 `api-server`，确保 `api-server` 数据库、Web 透视面板与发往大模型的 `instructions` / `tools` 100% 一致。
2. **为什么模型参数拦截与凭证注入必须在 `agent-daemon` 而不是 `agent-core`？**
   - 在 `SubprocessExecutor` 安全隔离模型中，`agent-core` 子进程**不持有模型 API Key**，所有大模型调用均通过单节点共享 UDS 转发给父进程 `agent-daemon` 代发。
   - 因此，`agent-daemon` 是天然的 **节点级模型网关（Node-Local Model Gateway）与工作区宿主**。对模型请求参数的网关级改写（如注入特定供应商扩展参数、动态向 Vault/STS 换取短期 Token、添加自定义网关 Header、敏感信息脱敏 DLP、Token 用量与成本审计）以及节点级自定义权限评估（`evaluate_permission`）統一在 `agent-daemon` 层完成。
3. **为什么鉴权、多租户配额、输入变更与工具禁用必须在 `api-server`？**
   - `api-server` 是整个集群唯一的声明式状态入口（Single Source of Truth）。
   - 身份认证（`Authn`）、细粒度权限与多租户透明隔离（`Authz`）、声明式请求变更与校验准入（`Mutating / Validating Admission`，覆盖 `CreateSession`、`SubmitPromptTurn`、`PushRealtimeMessage` 等操作）以及激活级工具禁用（`ActivationSpec.disabled_tools`）全部在 `api-server` 入库前完成，保证落盘数据与实际执行语义完全一致。

---

## 2. 扩展方式选型对比

针对 Rust 编写的 `api-server` 与 `agent-daemon`，我们对比了业界主流的 5 种扩展形态：

| 维度 | 1. 编译期 Rust Trait | 2. C ABI 动态库 (`.so`/`.dylib`) | 3. 内嵌脚本 (Lua / JS) | 4. WASM 组件 (`wasm32-wasip2`) | 5. 外部 Webhook / UDS (`ext_proc`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **代表系统** | Axum Tower Middleware | 传统 C/C++ 插件 | Nginx-Lua / Redis | Envoy WASM / `ragents-core` | Kubernetes Admission / Envoy `ext_authz` |
| **动态加载（免重编主程序）** | 不支持 | 支持 | 支持 | **支持（含 `.cwasm` 预编译缓存）** | **支持（独立进程/远端服务）** |
| **内存与崩溃隔离** | 同进程 | 差（ABI 不稳定，易引起段错误） | 中 | **极高（沙箱内存隔离 + 燃料/超时限制）** | **极高（进程/网络级隔离）** |
| **执行性能（微秒级拦截）** | 极高（零拷贝） | 高 | 中 | **高（本机纳秒~微秒级调用）** | 中（取决于 UDS 或 HTTP RTT） |
| **访问外部数据库/Redis/内网服务** | 极易 | 易 | 难 | 较繁琐（受限于 WASI 网络能力） | **极易（任意语言生态、长连接池）** |
| **多语言编写支持** | 仅 Rust | C/Rust | 仅 Lua/JS | **Rust / Go / TS / Python / C++** | **任意语言（HTTP/UDS JSON）** |

### 2.1 核心架构决策：统一 Trait 管道 + 双运行时驱动（WASM + Webhook/UDS）

单一扩展载体无法同时完美覆盖“**高频无状态的模型参数改写**”与“**有状态的企业 IAM/Redis 远程鉴权**”。因此，`ragents` 为 `agent-daemon` 和 `api-server` 采用 **三层统一扩展架构**：

1. **底座层（Native Rust Trait 责任链）**：
   - 在 `ragents-daemon` 和 `ragents-apiserver` 内部，所有扩展点均抽象为强类型的异步 Rust Trait（如 `ModelExchangeInterceptor`、`AuthProvider`、`AdmissionInterceptor`）。
   - 如果用户将 `ragents` 作为 Rust SDK 嵌入自定义二进制，可以直接注册原生 Rust 实现，获得零序列化开销。
2. **驱动一：进程内 WASM 组件驱动（`driver = "wasm"`）—— 适合高频、低延迟、纯逻辑/参数改写**
   - 直接复用 `ragents` 已有的 Wasmtime 运行时与 `/tmp/ragents-cwasm-cache/{sha256}.cwasm` 预编译缓存（加载耗时 `< 1ms`）。
   - **典型场景**：`agent-daemon` 修改模型请求参数、注入 Header、过滤工具列表、自定义权限策略判定；`api-server` 本地 JWT 验签、请求参数标准化改写（Mutating Admission）、自定义节点调度打分。
3. **驱动二：外部 Webhook / 本地 UDS 驱动（`driver = "webhook"`）—— 适合有状态、对接企业外部系统**
   - 支持 `http://`、`https://` 以及本机低延迟 `unix:///path/to/ext.sock`，采用标准 JSON Envelope 通信（参考 Kubernetes `AdmissionReview` / `TokenReview` 与 Envoy `ext_authz`）。
   - **典型场景**：`api-server` 对接企业统一鉴权中心（SSO/IAM）、多租户配额扣减（调用计费网关）、审计日志投递；`agent-daemon` 向企业 KMS/Vault 动态换取短期模型凭证。

---

## 3. `agent-daemon` 扩展架构详细设计

### 3.1 `agent-daemon` 扩展点全景图

```text
[api-server]
    │ DispatchActivation(Activation, Session, cluster_system_prompt, node_spec)
    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              agent-daemon                                  │
│                                                                            │
│     (Shared UDS IPC) ◄──────► [agent-core 子进程]                          │
│            │                   ├── agent.prepare -> ReportPreparedConfig   │
│            │                   └── ReAct Loop (Tools / Compaction)         │
│            │                                                               │
│  1. [Hook: evaluate_permission] ──► 拦截 CheckPermission，执行自定义安全策略│
│            │                                                               │
│  2. [Hook: before_model_request] ──► ★ 拦截/修改模型请求参数、Header、路由 │
│            │                                                               │
│            ▼                                                               │
│     [OpenResponses HTTP Client] ──► 调用真实 LLM 供应商 API                 │
│            │                                                               │
│  3. [Hook: after_model_response] ──► ★ 拦截/修改模型响应、用量统计、风控审计│
└────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 核心扩展点一：模型请求与响应拦截器（`ModelExchangeInterceptor`）

这是 `agent-daemon` 最核心的扩展能力。当 `agent-core` 子进程通过 UDS 发来 `CreateModelResponse { request: CreateResponseBody }` 时，`agent-daemon` 在真正发出 HTTP 请求前后执行拦截链。

#### 3.2.1 拦截上下文契约（`ModelInterceptContext`）

拦截器不仅能拿到 `CreateResponseBody`，还能拿到当前任务的完整元信息，并允许修改请求体、端点配置（URL / Header / 动态 Token）甚至直接短路返回：

```rust
/// 传递给 before_model_request 扩展的上下文
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequestInterceptInput {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub session_metadata: BTreeMap<String, String>,
    /// 目标模型端点信息（注意：为安全起见，传给扩展时默认掩码原始 api_key，但允许扩展返回新的 override_api_key 或 extra_headers）
    pub endpoint: InterceptEndpointView,
    /// OpenResponses 原始请求体（以 JSON Value 呈现，既支持标准字段修改，也支持注入供应商私有的顶层参数）
    pub request_body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterceptEndpointView {
    pub model_id: String,
    pub base_url: String,
    pub extra_headers: BTreeMap<String, String>,
    pub timeout_ms: Option<u64>,
}

/// 扩展返回给 agent-daemon 的修改指令
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ModelRequestInterceptOutcome {
    /// 继续请求（可附带修改后的 request_body、base_url、extra_headers 或动态签发的 api_key）
    Continue {
        #[serde(default)]
        request_body: Option<serde_json::Value>,
        #[serde(default)]
        override_base_url: Option<String>,
        #[serde(default)]
        upsert_headers: BTreeMap<String, String>,
        #[serde(default)]
        override_api_key: Option<String>,
    },
    /// 短路请求，直接向 agent-core 返回构造好的 ResponseResource（可用于语义缓存命中或 Mock）
    RespondImmediate {
        response: ResponseResource,
    },
    /// 拒绝本次模型调用（例如触发 DLP 敏感词阻断或预算超限）
    Reject {
        reason: String,
    },
}
```

#### 3.2.2 为什么 `request_body` 在拦截器边界采用 `serde_json::Value`？
在 `agent-core` 内部我们使用强类型的 `openresponses_rust::CreateResponseBody`，但在 `agent-daemon` 的模型拦截器边界将其序列化为 `serde_json::Value` 有两个极大优势：
1. **标准参数修改**：可以直接修改 `temperature`、`top_p`、`max_output_tokens`、`reasoning.effort`、`truncation`、`tool_choice` 或动态增删 `tools` 数组。
2. **供应商私有扩展参数注入**：许多兼容 OpenResponses 的网关（如 Rosetta、OneAPI、vLLM、智谱/阿里/Google 代理）支持非标准顶层或嵌套字段（如 `provider_options`、`thinking_budget`、`cache_control`、`user`、`extra_body`）。使用 JSON Value 使得拦截器可以自由注入任意供应商自定义字段，并在 `OpenResponsesModelBackend` 发送 HTTP 请求时原样序列化发出。

#### 3.2.3 典型使用场景示例
- **场景 A（按模型强制覆盖参数）**：当检测到 `model == "gemini-3.8-flash"` 时，自动设置 `temperature = 0.2`，注入 `reasoning: { "effort": "medium" }`，并在 HTTP Header 中注入 `X-Client-Node: <node_id>`。
- **场景 B（企业网关动态签名）**：每次请求前，拦截器根据 `session_metadata["tenant_id"]` 生成带时间戳的 HMAC 签名放入 `upsert_headers["X-Gateway-Signature"]`。
- **场景 C（敏感信息脱敏 DLP）**：扫描 `request_body.input` 中的私钥或内部口令，替换为 `[REDACTED]` 后再发往公有云模型。

### 3.3 核心扩展点二：权限策略拦截器（`evaluate_permission`）

除模型请求与响应拦截外，`agent-daemon` 还提供节点级权限评估钩子：
- **`evaluate_permission(PermissionInterceptInput) -> PermissionInterceptOutcome`**：
  - 当 `agent-core` 请求执行 `shell` / `file_read` / `file_write` / `http` 时，可在静态规则（`Allow`/`Deny`/`Ask`）之前运行自定义策略（例如解析命令或路径参数，自动放行只读安全操作或直接拦截高危请求，减少人工 `Ask` 打扰）。
  - *(注：节点级环境提示词统一由 `api-server` 的 `NodeSpec.node_prompt` 管理并在四段式 `PromptParts` 中自动组装，不再通过 `pre_activation` 隐式篡改。)*

### 3.4 `daemon.toml` 声明式扩展配置设计

```toml
node_id = "node-prod-01"
server_url = "http://127.0.0.1:8080"

# 1. 模型参数改写拦截器（WASM 驱动，纳秒级内存执行）
[[interceptors]]
id = "model-param-tuner"
driver = "wasm"
source = "/etc/ragents/plugins/model_param_tuner.wasm"
hooks = ["before_model_request", "after_model_response"]
priority = 10
timeout_ms = 500
fail_policy = "fail_closed" # fail_closed | fail_open
config = { default_temperature = 0.2, inject_reasoning_effort = "medium" }

# 2. 动态凭据与企业审计拦截器（本地 UDS 或 HTTP Webhook 驱动）
[[interceptors]]
id = "enterprise-kms-and-dlp"
driver = "webhook"
endpoint = "unix:///var/run/enterprise-agent-sidecar.sock"
hooks = ["before_model_request", "evaluate_permission"]
priority = 20
timeout_ms = 2000
fail_policy = "fail_closed"
```

---

## 4. `api-server` 扩展架构详细设计

### 4.1 `api-server` 请求处理与扩展流水线

借鉴 Kubernetes API Server 的经典请求生命周期（`Authentication -> Authorization -> Mutating Admission -> Validating Admission -> Scheduler Extender -> Persistence -> Audit/Event Sink`），`ragents-apiserver` 的扩展管道设计如下：

```text
Client (REST / SSE) 或 Daemon (WebSocket Tunnel)
    │
    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                               api-server                                   │
│                                                                            │
│  阶段 1: [Authn 身份认证扩展] (authenticate_client / authenticate_daemon)   │
│          ├── 解析 Authorization Header / API Key / mTLS / Node Token       │
│          └── 产出 Principal { subject, tenant_id, roles, attributes }      │
│                                   │                                        │
│  阶段 2: [Authz 权限决策扩展] (authorize)                                   │
│          └── 判定 Principal 是否有权对目标 Resource 执行 Verb               │
│                                   │                                        │
│  阶段 3: [Mutating Admission 变更准入] (mutate_resource)                   │
│          └── 修改 CreateSessionRequest / PromptTurn (如注入租户标签、改模型)│
│                                   │                                        │
│  阶段 4: [Validating Admission 校验准入] (validate_resource)               │
│          └── 校验并发配额、模型白名单、Pinned 目录合规性                    │
│                                   │                                        │
│  阶段 5: [Scheduler Extender 调度扩展] (filter_and_score_nodes)            │
│          └── 针对 Floating 任务对候选 Node 进行自定义过滤与打分             │
│                                   │                                        │
│  阶段 6: [SQLite / Store 事务落盘]                                         │
│                                   │                                        │
│  阶段 7: [Event / Audit Sink 异步事件外发] (on_events_committed)           │
│          └── 异步推送至计费网关、审计系统或消息队列                         │
└────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 核心扩展点一：自定义认证与鉴权（`Authn` & `Authz`）

`api-server` 需要严格区分两类接入流量：**客户端流量（Client Plane）** 与 **守护节点流量（Node Tunnel Plane）**。

#### 4.2.1 统一身份主体模型（`Principal`）

认证插件验证通过后，向请求上下文（Axum Request Extension）注入 `Principal`：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    /// 身份类型：ClientUser / ServiceAccount / DaemonNode
    pub kind: PrincipalKind,
    /// 唯一主体标识（如 user_id 或 node_id）
    pub subject: String,
    /// 租户空间标识（用于多租户资源隔离）
    pub tenant_id: String,
    /// 角色列表（如 ["admin"]、["developer"]、["node-agent"]）
    pub roles: Vec<String>,
    /// 扩展属性（如部门、配额等级、允许使用的模型列表）
    pub claims: BTreeMap<String, String>,
}
```

#### 4.2.2 认证与鉴权请求契约（参考 K8s `TokenReview` 与 `SubjectAccessReview`）

```rust
/// 1. 认证输入（支持提取 HTTP Header、Query、路径及连接元数据）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthnRequest {
    pub scope: AuthScope, // ClientHttp | DaemonTunnel
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub remote_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthnResponse {
    Authenticated { principal: Principal },
    Unauthenticated { message: String },
    /// 当前插件不处理该凭证类型，交由链上后续认证插件尝试
    Abstain,
}

/// 2. 鉴权输入
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzRequest {
    pub principal: Principal,
    pub verb: ResourceVerb,       // Create | Get | List | Watch | Cancel | Approve | ConnectTunnel
    pub resource: ResourceKind,   // Session | Node | Permission | EventStream
    pub resource_id: Option<String>,
    pub resource_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum AuthzResponse {
    Allow {
        /// 可选：在 List/Watch 查询时自动追加的 metadata 过滤条件（实现多租户透明隔离！）
        #[serde(default)]
        enforced_label_selector: BTreeMap<String, String>,
    },
    Deny { reason: String },
}
```

**多租户透明隔离设计亮点**：
- 当租户 `tenant-a` 的用户调用 `CreateSession` 时，Mutating Admission 自动写入 `metadata["ragents.io/tenant_id"] = "tenant-a"`。
- 当该用户调用 `ListSessions`、`GetSession` 或 `Watch` 时，`AuthzResponse::Allow` 返回 `enforced_label_selector: {"ragents.io/tenant_id": "tenant-a"}`，`api-server` 在底层存储查询与 SSE 事件流推送时自动按该 selector 过滤，确保租户间绝对隔离。

### 4.3 核心扩展点二：准入控制器（Mutating & Validating Admission）

在资源真正写入 `ControlStore` 之前，准入控制器可拦截并修改/校验声明式请求：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionRequest {
    pub principal: Principal,
    pub operation: AdmissionOperation, // CreateSession | SubmitPromptTurn | PushRealtimeMessage | RegisterNode | ApprovePermission
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AdmissionResponse {
    Allowed {
        /// 若为 Mutating 阶段，可返回修改后的 payload
        #[serde(default)]
        mutated_payload: Option<serde_json::Value>,
    },
    Denied {
        code: u16,
        message: String,
    },
}
```

**典型使用场景**：
- **模型别名路由（Mutating）**：客户端传入 `--model fast`，准入插件根据当前租户配置将其改写为 `gemini-3.8-flash`。
- **企业级安全基线强制注入（Mutating）**：无论客户端请求中传了什么 `permissions`，准入插件强制在 `CreateSessionRequest.permissions` 顶部插入一条 `Deny` 规则（如禁止访问内网元数据 IP `169.254.169.254`）。
- **并发配额与余额校验（Validating）**：创建 Session 或发起新 Turn 前，调用配额插件检查该用户的点数余额（如对接 `data.mone.run` 计量接口）或当前 `Running` Session 数，不足则直接返回 `403 Quota Exceeded`。

### 4.4 核心扩展点三：调度器扩展（`SchedulerExtender`）

当 `Floating` Session 需要绑定 `Node` 时，`api-server` 先筛选出 `NodeState::Ready` 且 `supported_models` 匹配的基础候选节点列表，然后调用调度扩展：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExtenderRequest {
    pub session: Session,
    pub candidate_nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExtenderResponse {
    /// 过滤后允许调度的节点及其打分（分数最高者获胜）
    pub prioritized_nodes: Vec<ScoredNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredNode {
    pub node_id: NodeId,
    pub score: i64,
}
```

### 4.5 `apiserver.toml` 声明式扩展配置设计

```toml
bind = "0.0.0.0:8080"
db_path = "/var/lib/ragents/control.sqlite3"

# 1. 身份认证链（支持多个认证器按顺序尝试）
[[authn_plugins]]
id = "corporate-jwt-verifier"
driver = "wasm"
source = "/etc/ragents/plugins/jwt_authn.wasm"
scopes = ["client_http"]
config = { jwks_json = "...", issuer = "https://auth.example.com" }

[[authn_plugins]]
id = "daemon-cluster-token"
driver = "webhook"
endpoint = "http://127.0.0.1:9001/internal/verify-node-token"
scopes = ["daemon_tunnel"]
timeout_ms = 1000

# 2. 权限与多租户隔离插件
[authz_plugin]
id = "tenant-rbac"
driver = "wasm"
source = "/etc/ragents/plugins/tenant_rbac.wasm"
fail_policy = "fail_closed"

# 3. 准入控制链（按 priority 顺序执行）
[[admission_plugins]]
id = "default-security-rules-injector"
phase = "mutating" # mutating | validating
driver = "wasm"
source = "/etc/ragents/plugins/security_baseline.wasm"
priority = 10

[[admission_plugins]]
id = "quota-and-billing-check"
phase = "validating"
driver = "webhook"
endpoint = "https://billing.internal/api/v1/ragents/admission"
priority = 20
timeout_ms = 1500
fail_policy = "fail_closed"
```

---

## 5. WIT 接口契约与 Webhook 协议规范

为了让同一个扩展逻辑既能编译为 `.wasm` 也能跑在 HTTP/UDS 服务中，WASM 与 Webhook **共享完全一致的 JSON 数据结构**。

### 5.1 统一 WIT 契约（`wit/ragents-control-extension.wit`）

```wit
package ragents:control-extension@0.1.0;

interface types {
    record hook-invocation {
        /// 钩子名称，例如：
        /// - daemon: "before_model_request" | "after_model_response" | "pre_activation" | "evaluate_permission"
        /// - apiserver: "authn" | "authz" | "admission_mutating" | "admission_validating" | "scheduler_extender"
        hook-name: string,
        /// 插件在 toml 中配置的静态 JSON 参数
        plugin-config-json: string,
        /// 对应钩子的输入结构体序列化后的 JSON
        payload-json: string,
    }

    record hook-result {
        /// 是否执行成功
        ok: bool,
        /// 对应钩子的输出结构体序列化后的 JSON
        output-json: string,
        /// 错误信息（当 ok = false 时）
        error: option<string>,
    }
}

world control-plugin {
    use types.{hook-invocation, hook-result};

    /// 导出统一拦截入口
    export invoke-hook: func(input: hook-invocation) -> hook-result;
}
```

> **设计考量**：为什么 WIT 层使用单一的 `invoke-hook(hook-name, plugin-config-json, payload-json) -> hook-result`？
> 1. **精简统一的接口签名**：所有控制面与守护面钩子复用单一 WIT 入口函数，避免为每个钩子重复定义复杂的 WIT 类型映射。
> 2. **WASM 与 Webhook 100% 载荷同构**：Webhook 驱动只需向目标 HTTP/UDS 发送完全相同的 `{ "hook_name": "...", "plugin_config": {...}, "payload": {...} }` POST 请求即可，开发者可以用同一套数据结构编写 `.wasm` 组件与 HTTP 微服务。

---

## 6. 故障隔离、超时与降级策略（Resilience & Safety）

扩展机制绝不能拖垮宿主守护进程或控制面：

1. **严格超时控制（`timeout_ms`）**：
   - 每个拦截器调用均包裹在 `tokio::time::timeout` 中（WASM 同步调用则结合 Wasmtime epoch interruption / 线程池超时）。
2. **明确的失败开闭策略（`fail_policy`）**：
   - `fail_closed`（默认用于 `Authn`、`Authz`、`Validating Admission`、DLP 安全拦截）：当扩展超时或报错时，直接拒绝请求以确保安全。
   - `fail_open`（适合非关键遥测、可选的提示词增强或缓存查询）：当扩展超时或报错时，记录 `WARN` 日志并跳过该拦截器，继续主流程。
3. **秘钥最小暴露原则**：
   - 在 `agent-daemon` 的 `before_model_request` 中，宿主传入的 `InterceptEndpointView` **不包含** 原始明文 `api_key`（防止普通参数改写插件窃取宿主 Key）；只有当插件本身是 KMS 凭据插件时，才由插件单向通过 `override_api_key` 返回动态凭据给宿主。

---

## 7. 建议的演进与落地路线（Stage 7 规划）

如果我们接下来将这套扩展机制落地到代码中，建议分为两个紧凑的子阶段：

- **Stage 7A（`agent-daemon` 拦截器管道与模型参数改写）**：
  1. 在 `ragents-protocol` 中新增 `ModelRequestInterceptInput` / `ModelRequestInterceptOutcome` 等契约。
  2. 在 `ragents-daemon` 中实现 `DaemonInterceptorChain`（内置 Native Rust Trait + WASM 驱动 + HTTP/UDS Webhook 驱动），并在处理子进程转发来的 `CreateModelResponse` 前后挂载拦截链。
  3. 编写一个示例扩展（修改 `CreateResponseBody` 模型参数与自定义 Header）及端到端测试。
- **Stage 7B（`api-server` 鉴权与准入控制管道）**：
  1. 在 `ragents-protocol` 中新增 `Principal`、`AuthnRequest/Response`、`AuthzRequest/Response`、`AdmissionRequest/Response`。
  2. 在 `ragents-apiserver` 的 Axum 路由与 WebSocket 握手入口挂载 `Authn -> Authz -> Mutating/Validating Admission` 拦截器链，并支持多租户 `enforced_label_selector` 过滤。
  3. 在 `ragents-client` 与 `ragents-cli` 中支持携带自定义 `Bearer Token / API Key` Header。
