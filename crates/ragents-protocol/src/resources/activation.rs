use super::{
    ModelRequestSpec, ObjectMeta, PermissionRequest, PermissionResolutionSpec,
    ProtocolValidationError,
};
use crate::ids::*;
use chrono::{DateTime, Utc};
use openresponses_rust::{Item, Usage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activation {
    pub metadata: ObjectMeta,
    pub spec: ActivationSpec,
    pub status: ActivationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubmissionKind {
    #[default]
    Normal,
    FollowUp,
    Continuation,
    Steer {
        target_activation_id: ActivationId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredActivationPhase {
    #[default]
    Running,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationPhase {
    Queued,
    Scheduled,
    Running,
    WaitingForInteraction,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

impl ActivationPhase {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ActivationPhase::Succeeded
                | ActivationPhase::Failed
                | ActivationPhase::Cancelled
                | ActivationPhase::Interrupted
        )
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            ActivationPhase::Scheduled
                | ActivationPhase::Running
                | ActivationPhase::WaitingForInteraction
                | ActivationPhase::Cancelling
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub session_id: SessionId,
    #[serde(default)]
    pub submission_kind: SubmissionKind,
    pub input: Vec<Item>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_override: Option<ModelRequestSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_tools: Vec<String>,
    #[serde(default)]
    pub desired_phase: DesiredActivationPhase,
    #[serde(default)]
    pub interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
}

impl ActivationSpec {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        if !self.session_id.is_safe_identifier() {
            return Err(ProtocolValidationError::InvalidIdentifier(
                self.session_id.to_string(),
            ));
        }
        if self.input.is_empty() && !matches!(self.submission_kind, SubmissionKind::Continuation) {
            return Err(ProtocolValidationError::EmptyActivationInput);
        }
        if let Some(ref model) = self.model_override {
            model.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationStatus {
    pub phase: ActivationPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_node: Option<NodeId>,
    pub lease_epoch: LeaseEpoch,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_config_ref: Option<ConfigRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_permission_ref: Option<PermissionSnapshotRef>,
    #[serde(default)]
    pub pending_interactions: Vec<PermissionRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

impl ActivationStatus {
    pub fn initial_queued() -> Self {
        Self {
            phase: ActivationPhase::Queued,
            assigned_node: None,
            lease_epoch: LeaseEpoch::new(0),
            effective_config_ref: None,
            effective_permission_ref: None,
            pending_interactions: Vec::new(),
            usage: None,
            error: None,
            started_at: None,
            finished_at: None,
        }
    }
}
