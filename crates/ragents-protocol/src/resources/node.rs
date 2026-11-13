use super::{default_true, ObjectMeta, PermissionPolicyLayer};
use crate::ids::NodeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Daemon-reported base Node-level environment System Prompt (refreshed from `daemon.toml` on every `RegisterNode`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_prompt: Option<String>,
    /// Optional Control-Plane override for Node-level environment System Prompt (persisted across daemon restarts)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_prompt_override: Option<String>,
    /// Daemon-reported base permission policy (refreshed from `daemon.toml` on every `RegisterNode`)
    #[serde(default)]
    pub base_permissions: PermissionPolicyLayer,
    /// Control-Plane incremental node-wide permission grants (updated via `AllowNode`, preserved across daemon restarts)
    #[serde(default)]
    pub granted_permissions: PermissionPolicyLayer,
    /// Workspace-scoped permission policies keyed by normalized `workspace_path` (updated via `AllowWorkspace`, preserved across daemon restarts)
    #[serde(default)]
    pub workspace_permissions: BTreeMap<String, PermissionPolicyLayer>,
}

impl NodeSpec {
    /// Computes the effective node-level system prompt (`node_prompt_override` takes priority over `node_prompt`).
    pub fn effective_node_prompt(&self) -> String {
        self.node_prompt_override
            .clone()
            .or_else(|| self.node_prompt.clone())
            .unwrap_or_default()
    }

    /// Computes the merged node-level permission policy (`base_permissions` merged with `granted_permissions`).
    pub fn effective_permissions(&self) -> PermissionPolicyLayer {
        let mut merged = self.base_permissions.clone();
        merged.merge_from(&self.granted_permissions);
        merged
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePhase {
    Ready,
    NotReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterModelSource {
    Cluster,
    NodeLocal,
    ClusterAndNodeLocal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterModelInfo {
    pub id: String,
    pub source: ClusterModelSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_on_nodes: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_max_output_tokens: Option<i32>,
    #[serde(default)]
    pub is_default: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterModelsResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub models: Vec<ClusterModelInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refreshed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Resolved OpenResponses provider credentials/settings sent by `api-server` to `agent-daemon`
/// strictly over the authenticated `DaemonTunnel`. Never stored in `SessionSpec`, `ConfigRevision`,
/// or returned to browser clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedModelProvider {
    pub model_id: String,
    pub base_url: String,
    pub api_key: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_max_output_tokens: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub phase: NodePhase,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    /// Node-local OpenResponses model names configured/discovered on this daemon
    #[serde(default)]
    pub local_models: Vec<String>,
    /// Optional node-local default model configured on this daemon
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_default_model: Option<String>,
    /// Whether this node falls back to cluster models configured on `api-server`
    #[serde(default = "default_true")]
    pub use_cluster_models: bool,
    /// Effective list of model names runnable on this node (`local_models` + cluster models if `use_cluster_models`)
    #[serde(default)]
    pub supported_models: Vec<String>,
    /// Effective default model on this node (`local_default_model`, or inherited cluster default when `use_cluster_models` is true)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub installed_extensions: Vec<String>,
    /// Allowed parent directories for Pinned session `workspace_path` on this node (always includes `/tmp` by default)
    #[serde(default)]
    pub allowed_workspace_dirs: Vec<String>,
    /// Effective node-level environment System Prompt (merged from `NodeSpec`)
    #[serde(default)]
    pub effective_node_prompt: String,
    /// Effective node-wide permission policy (`base_permissions` merged with `granted_permissions`)
    #[serde(default)]
    pub effective_permissions: PermissionPolicyLayer,
    pub max_concurrent_runners: u32,
    pub active_runners: u32,
    pub last_heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

/// Lexically normalizes `.` and `..` components on an absolute path and resolves symlinks
/// on the longest existing ancestor prefix (e.g. `/tmp` -> `/private/tmp` on macOS)
/// while preserving any not-yet-created subdirectory suffix.
pub fn normalize_and_resolve_workspace_path(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::{Component, PathBuf};
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };

    let mut normalized = PathBuf::new();
    for comp in abs.components() {
        match comp {
            Component::Prefix(p) => normalized.push(p.as_os_str()),
            Component::RootDir => normalized.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(c) => normalized.push(c),
        }
    }

    let mut existing = normalized.as_path();
    let mut suffix_parts = Vec::new();
    while !existing.exists() {
        if let Some(name) = existing.file_name() {
            suffix_parts.push(name.to_os_string());
        }
        match existing.parent() {
            Some(p) if p != existing => existing = p,
            _ => break,
        }
    }
    let mut base = std::fs::canonicalize(existing).unwrap_or_else(|_| existing.to_path_buf());
    for part in suffix_parts.into_iter().rev() {
        base.push(part);
    }
    base
}

fn strip_macos_private_tmp(p: &std::path::Path) -> std::path::PathBuf {
    if let Ok(rest) = p.strip_prefix("/private/tmp") {
        if rest.as_os_str().is_empty() {
            std::path::PathBuf::from("/tmp")
        } else {
            std::path::Path::new("/tmp").join(rest)
        }
    } else {
        p.to_path_buf()
    }
}

/// Returns `true` if `workspace_path` is equal to or a subdirectory of at least one entry in `allowed_dirs`.
pub fn is_workspace_path_allowed(
    workspace_path: &std::path::Path,
    allowed_dirs: &[impl AsRef<std::path::Path>],
) -> bool {
    let resolved_ws = normalize_and_resolve_workspace_path(workspace_path);
    let alt_ws = strip_macos_private_tmp(&resolved_ws);
    for dir in allowed_dirs {
        let resolved_dir = normalize_and_resolve_workspace_path(dir.as_ref());
        let alt_dir = strip_macos_private_tmp(&resolved_dir);
        if resolved_ws.starts_with(&resolved_dir) || alt_ws.starts_with(&alt_dir) {
            return true;
        }
    }
    false
}

