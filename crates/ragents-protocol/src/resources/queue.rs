use crate::ids::*;
use chrono::{DateTime, Utc};
use openresponses_rust::{Item, MessageContent};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueKind {
    Realtime,
    #[default]
    Pending,
}

impl QueueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueueKind::Realtime => "realtime",
            QueueKind::Pending => "pending",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "realtime" => Some(QueueKind::Realtime),
            "pending" => Some(QueueKind::Pending),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueToolPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enabled_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_tools: Vec<String>,
}

impl QueueToolPolicy {
    pub fn with_disabled(tools: impl IntoIterator<Item = String>) -> Self {
        Self {
            enabled_tools: Vec::new(),
            disabled_tools: tools.into_iter().collect(),
        }
    }

    pub fn with_enabled(tools: impl IntoIterator<Item = String>) -> Self {
        Self {
            enabled_tools: tools.into_iter().collect(),
            disabled_tools: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.enabled_tools.is_empty() && self.disabled_tools.is_empty()
    }

    /// Applies this single message's tool policy onto a mutable set of disabled tool names
    /// in FIFO enqueue order:
    /// 1. `disabled_tools` are added to `disabled_set`
    /// 2. `enabled_tools` are removed from `disabled_set` (explicit enable overrides prior disable)
    pub fn apply_to(&self, disabled_set: &mut BTreeSet<String>) -> bool {
        let mut changed = false;
        for name in &self.disabled_tools {
            if !name.trim().is_empty() && disabled_set.insert(name.clone()) {
                changed = true;
            }
        }
        for name in &self.enabled_tools {
            if !name.trim().is_empty() && disabled_set.remove(name) {
                changed = true;
            }
        }
        changed
    }

    pub fn effective_disabled_tools(&self) -> Vec<String> {
        let mut set = BTreeSet::new();
        self.apply_to(&mut set);
        set.into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionQueueMessage {
    pub id: i64,
    pub session_id: SessionId,
    pub queue_kind: QueueKind,
    pub content: Vec<MessageContent>,
    #[serde(default)]
    pub tool_policy: QueueToolPolicy,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DrainedRealtimeBatch {
    #[serde(default)]
    pub items: Vec<Item>,
    #[serde(default)]
    pub tool_policies: Vec<QueueToolPolicy>,
}
