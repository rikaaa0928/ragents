use crate::ids::*;
use crate::resources::{ManagedProcessInfo, PermissionDecision, PermissionRequest};
use chrono::{DateTime, Utc};
use openresponses_rust::{Item, Usage};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemBatchKind {
    Input,
    ModelOutput,
    ToolOutput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemBatch {
    pub session_id: SessionId,
    pub batch_seq: BatchSeq,
    pub first_local_item_seq: LocalItemSeq,
    pub kind: ItemBatchKind,
    pub activation_id: ActivationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub items: Vec<Item>,
}

impl ItemBatch {
    pub fn last_local_item_seq(&self) -> LocalItemSeq {
        let len = self.items.len() as u64;
        if len == 0 {
            self.first_local_item_seq
        } else {
            LocalItemSeq::new(self.first_local_item_seq.value() + len - 1)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEventEnvelope {
    pub resource_version: ResourceVersion,
    pub event_seq: EventSeq,
    pub event_id: EventId,
    pub session_id: SessionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activation_id: Option<ActivationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    pub created_at: DateTime<Utc>,
    pub event: SessionEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    SessionCreated,
    SessionSpecUpdated {
        generation: Generation,
    },
    SessionClosed,
    SessionArchived,

    ActivationSubmitted {
        input_batch_seq: BatchSeq,
    },
    ActivationScheduled {
        node_id: NodeId,
        lease_epoch: LeaseEpoch,
    },
    ActivationRescheduled {
        from_node: NodeId,
        to_node: NodeId,
        lease_epoch: LeaseEpoch,
        reason: String,
    },
    ActivationStarted {
        node_id: NodeId,
        config_ref: ConfigRef,
        permission_snapshot_ref: PermissionSnapshotRef,
    },
    ActivationCancellationRequested,
    ActivationCompleted {
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    ActivationFailed {
        error: String,
    },
    ActivationCancelled,
    ActivationInterrupted {
        reason: String,
    },

    TurnStarted,
    ModelTextDelta {
        delta: String,
    },
    ModelReasoningDelta {
        delta: String,
    },
    TurnCompleted {
        #[serde(skip_serializing_if = "Option::is_none")]
        response_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        appended_batch_seq: Option<BatchSeq>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },

    ToolCallStarted {
        call_id: String,
        name: String,
    },
    ToolCallFinished {
        call_id: String,
        name: String,
        success: bool,
        duration_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        output_batch_seq: Option<BatchSeq>,
    },

    PermissionRequested {
        request: PermissionRequest,
    },
    PermissionResolved {
        request_id: PermissionRequestId,
        decision: PermissionDecision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        override_path: Option<std::path::PathBuf>,
        resulting_snapshot_ref: PermissionSnapshotRef,
    },

    SessionSpawned {
        child_session_id: SessionId,
        command_id: CommandId,
    },

    BackgroundProcessStarted {
        process: ManagedProcessInfo,
    },
    BackgroundProcessExited {
        process: ManagedProcessInfo,
    },
    RealtimeMessagesDrained {
        count: usize,
        batch_seq: BatchSeq,
    },
}

impl SessionEvent {
    pub fn kind_name(&self) -> &'static str {
        match self {
            SessionEvent::SessionCreated => "session_created",
            SessionEvent::SessionSpecUpdated { .. } => "session_spec_updated",
            SessionEvent::SessionClosed => "session_closed",
            SessionEvent::SessionArchived => "session_archived",
            SessionEvent::ActivationSubmitted { .. } => "activation_submitted",
            SessionEvent::ActivationScheduled { .. } => "activation_scheduled",
            SessionEvent::ActivationRescheduled { .. } => "activation_rescheduled",
            SessionEvent::ActivationStarted { .. } => "activation_started",
            SessionEvent::ActivationCancellationRequested => "activation_cancellation_requested",
            SessionEvent::ActivationCompleted { .. } => "activation_completed",
            SessionEvent::ActivationFailed { .. } => "activation_failed",
            SessionEvent::ActivationCancelled => "activation_cancelled",
            SessionEvent::ActivationInterrupted { .. } => "activation_interrupted",
            SessionEvent::TurnStarted => "turn_started",
            SessionEvent::ModelTextDelta { .. } => "model_text_delta",
            SessionEvent::ModelReasoningDelta { .. } => "model_reasoning_delta",
            SessionEvent::TurnCompleted { .. } => "turn_completed",
            SessionEvent::ToolCallStarted { .. } => "tool_call_started",
            SessionEvent::ToolCallFinished { .. } => "tool_call_finished",
            SessionEvent::PermissionRequested { .. } => "permission_requested",
            SessionEvent::PermissionResolved { .. } => "permission_resolved",
            SessionEvent::SessionSpawned { .. } => "session_spawned",
            SessionEvent::BackgroundProcessStarted { .. } => "background_process_started",
            SessionEvent::BackgroundProcessExited { .. } => "background_process_exited",
            SessionEvent::RealtimeMessagesDrained { .. } => "realtime_messages_drained",
        }
    }

    pub fn is_ephemeral_stream_delta(&self) -> bool {
        matches!(
            self,
            SessionEvent::ModelTextDelta { .. } | SessionEvent::ModelReasoningDelta { .. }
        )
    }
}
