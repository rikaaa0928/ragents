use crate::ids::*;
use chrono::{DateTime, Utc};
use openresponses_rust::{Item, ReasoningConfig, Usage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolValidationError {
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error(
        "pinned scheduling requires non-empty node_id and absolute workspace_path, got path: {0}"
    )]
    InvalidPinnedWorkspace(String),
    #[error("session source range invalid: from {from} > through {through}")]
    InvalidSourceRange { from: u64, through: u64 },
    #[error("activation input items cannot be empty")]
    EmptyActivationInput,
    #[error("model name cannot be empty when specified")]
    EmptyModelName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub id: String,
    pub generation: Generation,
    pub resource_version: ResourceVersion,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl ObjectMeta {
    pub fn new(id: impl Into<String>, rv: ResourceVersion) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            generation: Generation::new(1),
            resource_version: rv,
            created_at: now,
            updated_at: now,
            labels: BTreeMap::new(),
        }
    }
}

// ============================================================================
// 1. Node Resource
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub metadata: ObjectMeta,
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    #[serde(default)]
    pub unschedulable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePhase {
    Ready,
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub phase: NodePhase,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    /// List of OpenResponses model names supported by this daemon's local config
    #[serde(default)]
    pub supported_models: Vec<String>,
    /// Default model on this node when session/activation omits model name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub installed_extensions: Vec<String>,
    pub max_concurrent_runners: u32,
    pub active_runners: u32,
    pub last_heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

// ============================================================================
// 2. Session Resource & Scheduling Modes (Pinned vs Floating)
// ============================================================================

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
}

fn default_true() -> bool {
    true
}

fn empty_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
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
            last_error: None,
        }
    }
}

// ============================================================================
// 3. Activation Resource
// ============================================================================

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
        if self.input.is_empty() {
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

// ============================================================================
// 4. Permissions & Content-Addressed Config Revisions
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CapabilityRequest {
    FileRead {
        path: PathBuf,
    },
    FileWrite {
        path: PathBuf,
    },
    Http {
        scheme: String,
        host: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        method: String,
    },
    CommandWorkDir {
        work_dir: PathBuf,
    },
}

impl CapabilityRequest {
    /// Returns a copy of this `CapabilityRequest` with its filesystem path replaced by `override_path` if provided.
    pub fn with_override_path(&self, override_path: Option<&std::path::Path>) -> Self {
        let Some(p) = override_path else {
            return self.clone();
        };
        match self {
            Self::FileRead { .. } => Self::FileRead {
                path: p.to_path_buf(),
            },
            Self::FileWrite { .. } => Self::FileWrite {
                path: p.to_path_buf(),
            },
            Self::CommandWorkDir { .. } => Self::CommandWorkDir {
                work_dir: p.to_path_buf(),
            },
            Self::Http {
                scheme,
                host: _,
                port,
                method,
            } => Self::Http {
                scheme: scheme.clone(),
                host: p.to_string_lossy().to_string(),
                port: *port,
                method: method.clone(),
            },
        }
    }

    pub fn path(&self) -> Option<&std::path::Path> {
        match self {
            Self::FileRead { path } | Self::FileWrite { path } => Some(path.as_path()),
            Self::CommandWorkDir { work_dir } => Some(work_dir.as_path()),
            Self::Http { host, .. } => Some(std::path::Path::new(host.as_str())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefaultPermissionDecision {
    Allow,
    #[default]
    Deny,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuleSet<R> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny: Option<Vec<R>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<R>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<DefaultPermissionDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PermissionPolicyLayer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_read: Option<RuleSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_write: Option<RuleSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_hosts: Option<RuleSet<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_work_dir: Option<RuleSet<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    Deny,
    /// Effective for this single check/command execution only
    AllowOnce,
    /// Effective for all turns within the current Activation
    AllowActivation,
    /// Effective for all Activations within the current Session
    AllowSession,
    /// Effective for all Sessions sharing the same workspace_root directory on the node
    #[serde(alias = "allow_project")]
    AllowWorkspace,
    /// Effective for all tasks across the entire Node (agent-daemon)
    #[serde(alias = "allow_always")]
    AllowNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: PermissionRequestId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension_id: Option<String>,
    /// Suggested / auto-optimized directory capability (e.g. `/Users/alice/.cache/uv`)
    pub capability: CapabilityRequest,
    /// Raw path that originally triggered the violation (e.g. `/Users/alice/.cache/uv/sdists-v6/.git`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionResolutionSpec {
    pub decision: PermissionDecision,
    /// Optional user-adjusted directory/file path overriding the suggested path in `PermissionRequest.capability`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub override_path: Option<PathBuf>,
    pub responded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionSnapshot {
    pub format_version: u32,
    pub permission_snapshot_ref: PermissionSnapshotRef,
    pub created_at: DateTime<Utc>,
    pub effective_policy: PermissionPolicyLayer,
}

impl PermissionSnapshot {
    pub fn from_effective_policy(policy: PermissionPolicyLayer) -> Self {
        let canonical = canonicalize_json(&serde_json::json!({
            "format_version": 1,
            "effective_policy": &policy,
        }));
        let digest = sha256_hex(&canonical);
        Self {
            format_version: 1,
            permission_snapshot_ref: PermissionSnapshotRef::new(format!(
                "permsnap_sha256:{digest}"
            )),
            created_at: Utc::now(),
            effective_policy: policy,
        }
    }
}

/// Frozen, content-addressed effective configuration merged by `agent-daemon`.
/// Strictly excludes sensitive credentials (`api_key`, `base_url`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigRevision {
    pub format_version: u32,
    pub config_ref: ConfigRef,
    pub created_at: DateTime<Utc>,
    pub system_prompt: String,
    pub model: ModelRequestSpec,
    #[serde(default)]
    pub extensions: Vec<ExtensionSpec>,
}

impl ConfigRevision {
    pub fn new_merged(
        system_prompt: String,
        model: ModelRequestSpec,
        extensions: Vec<ExtensionSpec>,
    ) -> Self {
        let canonical = canonicalize_json(&serde_json::json!({
            "format_version": 1,
            "system_prompt": &system_prompt,
            "model": &model,
            "extensions": &extensions,
        }));
        let digest = sha256_hex(&canonical);
        Self {
            format_version: 1,
            config_ref: ConfigRef::new(format!("cfg_sha256:{digest}")),
            created_at: Utc::now(),
            system_prompt,
            model,
            extensions,
        }
    }
}

pub fn canonicalize_json(value: &serde_json::Value) -> String {
    fn sort_value(val: &serde_json::Value) -> serde_json::Value {
        match val {
            serde_json::Value::Object(map) => {
                let mut sorted = serde_json::Map::new();
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for k in keys {
                    sorted.insert(k.clone(), sort_value(&map[k]));
                }
                serde_json::Value::Object(sorted)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(sort_value).collect())
            }
            other => other.clone(),
        }
    }
    let sorted = sort_value(value);
    serde_json::to_string(&sorted).unwrap_or_default()
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ============================================================================
// 5. Command Process Management Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLifecycle {
    /// Terminated automatically when the spawning `Activation` finishes (`Completed`/`Failed`/`Cancelled`)
    #[default]
    Activation,
    /// Survives across multiple `Activation`s within the same `Session`; terminated when `Session` closes or is explicitly stopped
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedProcessPhase {
    Running,
    Exited,
    Killed,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLogStream {
    Stdout,
    Stderr,
    #[default]
    Combined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessInfo {
    pub process_id: ProcessId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub command: String,
    pub lifecycle: ProcessLifecycle,
    pub phase: ManagedProcessPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ProcessActionRequest {
    Start {
        command: String,
        #[serde(default = "default_true")]
        wait: bool,
        #[serde(default)]
        lifecycle: ProcessLifecycle,
        #[serde(default)]
        timeout_ms: u64,
    },
    Status {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_id: Option<String>,
    },
    Logs {
        process_id: String,
        #[serde(default)]
        stream: ProcessLogStream,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tail_lines: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<usize>,
    },
    Stop {
        process_id: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessActionResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ManagedProcessInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes: Option<Vec<ManagedProcessInfo>>,
}
