use super::{
    default_true, empty_json_object, ObjectMeta, PermissionPolicyLayer, ProtocolValidationError,
};
use crate::ids::*;
use openresponses_rust::ReasoningConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub metadata: ObjectMeta,
    pub spec: SessionSpec,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SchedulingMode {
    /// Bound to both a specific Node and an explicit host directory; never migrates across machines.
    Pinned {
        node_id: NodeId,
        workspace_path: PathBuf,
    },
    /// Unbound to any specific machine or host directory; scheduled by api-server onto a healthy
    /// node with a temporary directory, and eligible for cross-machine failover.
    Floating {
        #[serde(default)]
        node_selector: BTreeMap<String, String>,
    },
}

impl SchedulingMode {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        match self {
            SchedulingMode::Pinned {
                node_id,
                workspace_path,
            } => {
                if !node_id.is_safe_identifier() {
                    return Err(ProtocolValidationError::InvalidIdentifier(
                        node_id.to_string(),
                    ));
                }
                let path_str = workspace_path.to_string_lossy();
                if path_str.trim().is_empty() || !workspace_path.is_absolute() {
                    return Err(ProtocolValidationError::InvalidPinnedWorkspace(
                        path_str.into_owned(),
                    ));
                }
                Ok(())
            }
            SchedulingMode::Floating { .. } => Ok(()),
        }
    }

    pub fn is_pinned(&self) -> bool {
        matches!(self, SchedulingMode::Pinned { .. })
    }

    pub fn is_floating(&self) -> bool {
        matches!(self, SchedulingMode::Floating { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionSourceKind {
    ForkedFrom,
    DerivedFrom,
    SummaryOf,
    ContinuedFrom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSource {
    pub kind: SessionSourceKind,
    pub session_id: SessionId,
    pub from_context_pos: ContextPos,
    pub through_context_pos: ContextPos,
}

impl SessionSource {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        if !self.session_id.is_safe_identifier() {
            return Err(ProtocolValidationError::InvalidIdentifier(
                self.session_id.to_string(),
            ));
        }
        if self.from_context_pos.value() > self.through_context_pos.value() {
            return Err(ProtocolValidationError::InvalidSourceRange {
                from: self.from_context_pos.value(),
                through: self.through_context_pos.value(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerRef {
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub command_id: CommandId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredSessionPhase {
    #[default]
    Open,
    Closed,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    #[default]
    Open,
    Closed,
    Archived,
    Corrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequestSpec {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningConfig>,
}

impl ModelRequestSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            temperature: None,
            max_output_tokens: None,
            reasoning: None,
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        if self.name.trim().is_empty() {
            return Err(ProtocolValidationError::EmptyModelName);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionSpec {
    pub name: String,
    pub path: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "empty_json_object")]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_dirs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSpec {
    pub scheduling: SchedulingMode,
    #[serde(default)]
    pub sources: Vec<SessionSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerRef>,
    #[serde(default)]
    pub desired_phase: DesiredSessionPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRequestSpec>,
    #[serde(default)]
    pub extensions: Vec<ExtensionSpec>,
    #[serde(default)]
    pub permissions: PermissionPolicyLayer,
}

impl SessionSpec {
    pub fn validate(&self) -> Result<(), ProtocolValidationError> {
        self.scheduling.validate()?;
        for source in &self.sources {
            source.validate()?;
        }
        if let Some(ref model) = self.model {
            model.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    #[default]
    Working,
    CompactionWorker,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SessionCompactionInfo {
    #[serde(default)]
    pub role: SessionRole,
    #[serde(default)]
    pub generation: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacted_from_session_id: Option<SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_session_id: Option<SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continued_in_session_id: Option<SessionId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub summarized_through_pos: ContextPos,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kept_from_pos: Option<ContextPos>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kept_through_pos: Option<ContextPos>,
    pub tokens_before: u64,
    pub system_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_prefix_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_summary: Option<String>,
    #[serde(default)]
    pub read_files: Vec<String>,
    #[serde(default)]
    pub modified_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStatus {
    pub phase: SessionPhase,
    pub observed_generation: Generation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_node_id: Option<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_activation_id: Option<ActivationId>,
    pub queued_activation_count: u32,
    pub local_item_count: u64,
    pub effective_context_item_count: u64,
    pub batch_count: u64,
    pub event_count: u64,
    #[serde(default)]
    pub compaction: SessionCompactionInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl SessionStatus {
    pub fn initial(bound_node_id: Option<NodeId>, effective_context_item_count: u64) -> Self {
        Self {
            phase: SessionPhase::Open,
            observed_generation: Generation::new(1),
            bound_node_id,
            active_activation_id: None,
            queued_activation_count: 0,
            local_item_count: 0,
            effective_context_item_count,
            batch_count: 0,
            event_count: 0,
            compaction: SessionCompactionInfo::default(),
            last_error: None,
        }
    }
}
