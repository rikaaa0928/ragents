use crate::events::{ItemBatch, ItemBatchKind, SessionEvent, SessionEventEnvelope};
use crate::ids::*;
use crate::resources::*;
use openresponses_rust::{Item, MessageContent, Usage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// 1. Declarative REST API Request / Response Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub spec: SessionSpec,
    /// Optional inline initial activation to start a conversation in one declarative call
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_activation: Option<InitialActivationRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialActivationRequest {
    pub input: Vec<Item>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_override: Option<ModelRequestSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    pub session: Session,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_activation: Option<Activation>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PatchSessionSpecRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_phase: Option<DesiredSessionPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRequestSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<ExtensionSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<PermissionPolicyLayer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateActivationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activation_id: Option<ActivationId>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub spec: ActivationSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PatchActivationSpecRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_phase: Option<DesiredActivationPhase>,
    #[serde(default)]
    pub interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionContextResponse {
    pub session_id: SessionId,
    pub resource_version: ResourceVersion,
    pub effective_items: Vec<Item>,
    pub local_batches: Vec<ItemBatch>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WatchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_rv: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRealtimeMessageRequest {
    pub content: Vec<MessageContent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_tools: Vec<String>,
}

impl PushRealtimeMessageRequest {
    pub fn from_text(text: impl Into<String>) -> Self {
        Self {
            content: vec![MessageContent::InputText { text: text.into() }],
            disabled_tools: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRealtimeMessageResponse {
    pub queued_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawned_activation_id: Option<ActivationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inserted_ids: Vec<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WithdrawRealtimeMessageRequest {
    #[serde(default)]
    pub message_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawRealtimeMessageResponse {
    pub removed_count: usize,
    pub remaining_count: usize,
}

// ============================================================================
// 2. Daemon <-> API Server WebSocket Tunnel Messages
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonToServerMessage {
    RegisterNode {
        request_id: String,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        #[serde(default)]
        supported_models: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        default_model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_system_prompt: Option<String>,
        #[serde(default)]
        default_permissions: PermissionPolicyLayer,
        #[serde(default)]
        installed_extensions: Vec<String>,
        #[serde(default)]
        allowed_workspace_dirs: Vec<String>,
        max_concurrent_runners: u32,
    },
    Heartbeat {
        request_id: String,
        node_id: NodeId,
        active_runners: u32,
    },
    /// Daemon claims a scheduled activation, reporting merged effective ConfigRevision & PermissionSnapshot
    AcceptActivation {
        request_id: String,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        effective_config: Box<ConfigRevision>,
        effective_permission: Box<PermissionSnapshot>,
    },
    /// Daemon commits a prepared Input, ModelOutput, or ToolOutput batch along with its pairing event
    CommitBatch {
        request_id: String,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        kind: ItemBatchKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<TurnId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        response_id: Option<String>,
        items: Vec<Item>,
        companion_event: BatchCompanionEvent,
    },
    /// Daemon emits a standalone lifecycle event (e.g. TurnStarted, ToolCallStarted, PermissionRequested)
    EmitEvent {
        request_id: String,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<TurnId>,
        event: SessionEvent,
    },
    /// Daemon requests reading effective context for a session
    FetchEffectiveContext {
        request_id: String,
        session_id: SessionId,
    },
    /// Daemon pushes a real-time notification (e.g. background process exit) into the session's real-time message queue on `api-server`
    PushRealtimeMessage {
        request_id: String,
        session_id: SessionId,
        #[serde(skip_serializing_if = "Option::is_none")]
        activation_id: Option<ActivationId>,
        content: Vec<MessageContent>,
    },
    /// Daemon drains all pending real-time messages for the session in one atomic batch at the start of a Turn
    DrainRealtimeMessages {
        request_id: String,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<TurnId>,
    },
    /// Daemon completes an activation (Succeeded, Failed, or Cancelled)
    CompleteActivation {
        request_id: String,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        #[serde(skip_serializing_if = "Option::is_none")]
        turn_id: Option<TurnId>,
        outcome: ActivationTerminalOutcome,
    },
    /// Daemon commits a 3-session context compaction rollover (`from_session -> compaction_session -> continued_session`)
    RolloverCompactedSession {
        request_id: String,
        request: Box<RolloverCompactedSessionRequest>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloverCompactedSessionRequest {
    pub from_session_id: SessionId,
    pub from_activation_id: ActivationId,
    pub lease_epoch: LeaseEpoch,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    pub summarized_through_pos: ContextPos,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kept_from_pos: Option<ContextPos>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kept_through_pos: Option<ContextPos>,
    pub tokens_before: u64,
    pub compaction_prompt: String,
    pub summary_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_usage: Option<Usage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloverCompactedSessionResponse {
    pub from_session: Session,
    pub compaction_session: Session,
    pub continued_session: Session,
    pub continued_activation: Activation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BatchCompanionEvent {
    InputSubmitted,
    TurnCompleted {
        #[serde(skip_serializing_if = "Option::is_none")]
        response_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    ToolCallFinished {
        call_id: String,
        name: String,
        success: bool,
        duration_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ActivationTerminalOutcome {
    Succeeded {
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    Failed {
        error: String,
    },
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerToDaemonMessage {
    Registered {
        request_id: String,
        node_id: NodeId,
        lease_ttl_ms: u64,
        resource_version: ResourceVersion,
        #[serde(default)]
        node_spec: NodeSpec,
    },
    Ack {
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        resource_version: Option<ResourceVersion>,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_seq: Option<BatchSeq>,
        #[serde(skip_serializing_if = "Option::is_none")]
        spawned_session_id: Option<SessionId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    EffectiveContextReply {
        request_id: String,
        ok: bool,
        #[serde(default)]
        items: Vec<Item>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    RealtimeMessagesReply {
        request_id: String,
        ok: bool,
        #[serde(default)]
        items: Vec<Item>,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_seq: Option<BatchSeq>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    RolloverCompactedSessionReply {
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        response: Option<Box<RolloverCompactedSessionResponse>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Server pushes a newly scheduled activation to this daemon
    DispatchActivation {
        session: Box<Session>,
        activation: Box<Activation>,
        lease_epoch: LeaseEpoch,
        #[serde(default)]
        cluster_system_prompt: String,
        #[serde(default)]
        node_spec: Box<NodeSpec>,
    },
    /// Server pushes declarative spec updates (e.g. desired_phase=Cancelled or permission resolutions)
    ReconcileActivation {
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        desired_phase: DesiredActivationPhase,
        #[serde(default)]
        interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
    },
    /// Server instructs daemon to clean up all background processes and temporary resources for a closed/archived session
    CleanupSession { session_id: SessionId },
    /// Server notifies daemon that a session Event was committed (useful for local observers)
    EventNotification { envelope: Box<SessionEventEnvelope> },
}
