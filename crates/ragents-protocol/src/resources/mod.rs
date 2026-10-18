pub mod activation;
pub mod node;
pub mod policy;
pub mod process;
pub mod session;

pub use activation::*;
pub use node::*;
pub use policy::*;
pub use process::*;
pub use session::*;

use crate::ids::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    #[error(
        "workspace_path '{workspace_path}' is not under any allowed workspace directory on node '{node_id}' (allowed: {allowed:?})"
    )]
    DisallowedWorkspacePath {
        node_id: String,
        workspace_path: String,
        allowed: Vec<String>,
    },
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

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn empty_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}
