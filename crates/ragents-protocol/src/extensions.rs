use crate::ids::*;
use crate::resources::*;
use openresponses_rust::ResponseResource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

// ============================================================================
// 1. Common Extension Driver & Invocation Envelope
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionFailPolicy {
    /// Reject the request/operation if the extension fails or times out (safe default)
    #[default]
    FailClosed,
    /// Ignore the extension error and continue with the unmodified payload
    FailOpen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "driver", rename_all = "snake_case")]
pub enum ExtensionDriverConfig {
    /// In-process WebAssembly component (`wasm32-wasip2`, `.wasm` or `.cwasm`).
    /// For `api-server` and `agent-daemon` control plugins, network is open by default,
    /// while filesystem access is restricted to `allowed_dirs` (plus a plugin-private work dir).
    Wasm {
        path: PathBuf,
        #[serde(default)]
        allowed_dirs: Vec<PathBuf>,
    },
    /// Out-of-process HTTP (`http://`, `https://`) or Unix Domain Socket (`unix:///path/to.sock`) webhook
    Webhook { endpoint: String },
}

/// Standard JSON envelope sent to both WASM control extensions and HTTP/UDS webhooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlHookInvocation {
    pub hook_name: String,
    #[serde(default)]
    pub plugin_config: serde_json::Value,
    pub payload: serde_json::Value,
}

/// Standard JSON result returned by WASM control extensions and HTTP/UDS webhooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlHookResult {
    pub ok: bool,
    #[serde(default)]
    pub output: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// 2. Agent-Daemon Extension Contracts (Model Gateway & Node Lifecycle)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHookKind {
    BeforeModelRequest,
    AfterModelResponse,
    EvaluatePermission,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonInterceptorSpec {
    pub id: String,
    #[serde(flatten)]
    pub driver: ExtensionDriverConfig,
    #[serde(default)]
    pub hooks: Vec<DaemonHookKind>,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub fail_policy: ExtensionFailPolicy,
    #[serde(default)]
    pub config: serde_json::Value,
}

fn default_priority() -> i32 {
    100
}

fn default_timeout_ms() -> u64 {
    2000
}

/// Sanitized view of the target model endpoint passed into `before_model_request`.
/// Never exposes the node's raw `api_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterceptEndpointView {
    pub model_id: String,
    pub base_url: String,
    #[serde(default)]
    pub extra_headers: BTreeMap<String, String>,
}

/// Payload for `before_model_request` hook on `agent-daemon`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequestInterceptInput {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    #[serde(default)]
    pub session_labels: BTreeMap<String, String>,
    pub endpoint: InterceptEndpointView,
    /// OpenResponses `CreateResponseBody` serialized as JSON Value so interceptors can mutate
    /// both standard fields (`temperature`, `reasoning`, `max_output_tokens`, `tools`) and
    /// arbitrary vendor-specific parameters (`thinking_budget`, `provider_options`, `extra_body`).
    pub request_body: serde_json::Value,
}

/// Outcome returned by `before_model_request` hook on `agent-daemon`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ModelRequestInterceptOutcome {
    /// Continue calling the LLM provider with optional mutations.
    Continue {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_body: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        override_base_url: Option<String>,
        #[serde(default)]
        upsert_headers: BTreeMap<String, String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        override_api_key: Option<String>,
    },
    /// Short-circuit the LLM call and immediately return a prebuilt `ResponseResource` (e.g. cache hit / mock).
    RespondImmediate { response: Box<ResponseResource> },
    /// Reject the model call (e.g. DLP policy violation or budget limit).
    Reject { reason: String },
}

impl Default for ModelRequestInterceptOutcome {
    fn default() -> Self {
        Self::Continue {
            request_body: None,
            override_base_url: None,
            upsert_headers: BTreeMap::new(),
            override_api_key: None,
        }
    }
}

/// Payload for `after_model_response` hook on `agent-daemon`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelResponseInterceptInput {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    pub model_id: String,
    #[serde(default)]
    pub duration_ms: u64,
    pub response: ResponseResource,
}

/// Outcome returned by `after_model_response` hook on `agent-daemon`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ModelResponseInterceptOutcome {
    Continue {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<Box<ResponseResource>>,
    },
    Reject {
        reason: String,
    },
}

/// Payload for `evaluate_permission` hook on `agent-daemon`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionInterceptInput {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_id: Option<String>,
    pub capability: CapabilityRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub workspace_root: PathBuf,
}

/// Outcome returned by `evaluate_permission` hook on `agent-daemon`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum PermissionInterceptOutcome {
    /// Plugin made no decision; fall through to standard `PermissionPolicyLayer` evaluation
    Abstain,
    /// Immediately allow (with optional adjusted path)
    Allow {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        override_path: Option<PathBuf>,
    },
    /// Immediately deny
    Deny { reason: String },
    /// Force interactive user approval (`PermissionRequested`)
    Ask {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        suggested_path: Option<PathBuf>,
    },
}

// ============================================================================
// 3. API-Server Extension Contracts (Authn, Authz, Admission, Scheduler)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    ClientUser,
    ServiceAccount,
    DaemonNode,
    Anonymous,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub kind: PrincipalKind,
    pub subject: String,
    pub tenant_id: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub claims: BTreeMap<String, String>,
}

impl Principal {
    pub fn anonymous() -> Self {
        Self {
            kind: PrincipalKind::Anonymous,
            subject: "anonymous".to_string(),
            tenant_id: "default".to_string(),
            roles: Vec::new(),
            claims: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthScope {
    ClientHttp,
    DaemonTunnel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthnRequest {
    pub scope: AuthScope,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthnResponse {
    Authenticated { principal: Principal },
    Unauthenticated { message: String },
    Abstain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceVerb {
    Create,
    Get,
    List,
    Patch,
    Watch,
    ConnectTunnel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Node,
    Session,
    Activation,
    EventStream,
    DaemonTunnel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzRequest {
    pub principal: Principal,
    pub verb: ResourceVerb,
    pub resource: ResourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    #[serde(default)]
    pub resource_labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum AuthzResponse {
    Allow {
        /// Optional label selector enforced on Session/Event queries for transparent multi-tenant isolation
        #[serde(default)]
        enforced_label_selector: BTreeMap<String, String>,
    },
    Deny {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionPhase {
    Mutating,
    Validating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOperation {
    CreateSession,
    PatchSession,
    CreateActivation,
    PatchActivation,
    PushRealtimeMessage,
    RegisterNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionRequest {
    pub phase: AdmissionPhase,
    pub principal: Principal,
    pub operation: AdmissionOperation,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AdmissionResponse {
    Allowed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mutated_payload: Option<serde_json::Value>,
    },
    Denied {
        #[serde(default = "default_forbidden_code")]
        code: u16,
        message: String,
    },
}

fn default_forbidden_code() -> u16 {
    403
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExtenderRequest {
    pub session: Session,
    pub candidate_nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExtenderResponse {
    pub prioritized_nodes: Vec<ScoredNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoredNode {
    pub node_id: NodeId,
    pub score: i64,
}
