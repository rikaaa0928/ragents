use crate::error::CoreError;
use crate::model::ModelBackend;
use async_trait::async_trait;
use openresponses_rust::{Item, Usage};
use ragents_protocol::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PermissionOutcome {
    Allowed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        effective_capability: Option<CapabilityRequest>,
        #[serde(default = "default_persist_true")]
        persist_in_activation: bool,
    },
    Denied {
        reason: String,
    },
}

fn default_persist_true() -> bool {
    true
}

impl PermissionOutcome {
    pub fn allowed() -> Self {
        Self::Allowed {
            effective_capability: None,
            persist_in_activation: true,
        }
    }

    pub fn allowed_with(cap: CapabilityRequest, persist_in_activation: bool) -> Self {
        Self::Allowed {
            effective_capability: Some(cap),
            persist_in_activation,
        }
    }
}

/// Node-local OpenResponses endpoint credentials injected by `agent-daemon`.
/// Never serialized into `ConfigRevision` or sent to `api-server`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelEndpointConfig {
    pub base_url: String,
    pub api_key: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra_headers: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreRunSpec {
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub workspace_root: PathBuf,
    pub session_tmp_dir: PathBuf,
    pub effective_config: ConfigRevision,
    #[serde(default)]
    pub effective_permissions: PermissionPolicyLayer,
    pub model_endpoint: ModelEndpointConfig,
    pub max_turns: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreRunSummary {
    pub activation_id: ActivationId,
    pub final_text: String,
    pub total_usage: Option<Usage>,
    pub turns_executed: usize,
    pub last_turn_id: Option<TurnId>,
}

#[async_trait]
pub trait CoreHost: Send + Sync {
    async fn load_effective_context(&self) -> Result<Vec<Item>, CoreError>;

    async fn commit_model_output(
        &self,
        turn_id: &TurnId,
        response_id: Option<String>,
        items: Vec<Item>,
        usage: Option<Usage>,
    ) -> Result<BatchSeq, CoreError>;

    async fn commit_tool_output(
        &self,
        turn_id: &TurnId,
        call_id: &str,
        tool_name: &str,
        success: bool,
        duration_ms: u64,
        items: Vec<Item>,
    ) -> Result<BatchSeq, CoreError>;

    async fn emit_event(
        &self,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<(), CoreError>;

    async fn check_permission(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError>;

    async fn check_permission_with_original_path(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        original_path: Option<PathBuf>,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        let _ = original_path;
        self.check_permission(extension_id, capability, reason)
            .await
    }

    async fn spawn_child_session(
        &self,
        request: CreateSessionRequest,
        turn_id: Option<TurnId>,
        call_id: Option<String>,
        command_id: CommandId,
    ) -> Result<SessionId, CoreError>;
}

#[async_trait]
pub trait CoreExecutor: Send + Sync {
    async fn execute(
        &self,
        run_spec: CoreRunSpec,
        host: Arc<dyn CoreHost>,
        model_backend: Arc<dyn ModelBackend>,
        cancellation: CancellationToken,
    ) -> Result<CoreRunSummary, CoreError>;
}
