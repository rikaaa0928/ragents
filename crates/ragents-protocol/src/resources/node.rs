use super::ObjectMeta;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    /// Allowed parent directories for Pinned session `workspace_path` on this node (always includes `/tmp` by default)
    #[serde(default)]
    pub allowed_workspace_dirs: Vec<String>,
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

/// Returns `true` if `workspace_path` is equal to or a subdirectory of at least one entry in `allowed_dirs`.
pub fn is_workspace_path_allowed(
    workspace_path: &std::path::Path,
    allowed_dirs: &[impl AsRef<std::path::Path>],
) -> bool {
    let resolved_ws = normalize_and_resolve_workspace_path(workspace_path);
    for dir in allowed_dirs {
        let resolved_dir = normalize_and_resolve_workspace_path(dir.as_ref());
        if resolved_ws.starts_with(&resolved_dir) {
            return true;
        }
    }
    false
}

