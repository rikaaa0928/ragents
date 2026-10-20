use super::{ExtensionSpec, ModelRequestSpec};
use crate::ids::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;

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

impl PermissionPolicyLayer {
    pub fn grant_capability(&mut self, cap: &CapabilityRequest) {
        fn push_allow(slot: &mut Option<RuleSet<String>>, rule: String) {
            let rs = slot.get_or_insert_with(RuleSet::default);
            let allow = rs.allow.get_or_insert_with(Vec::new);
            if !allow.contains(&rule) {
                allow.push(rule);
            }
        }
        match cap {
            CapabilityRequest::FileRead { path } => {
                push_allow(&mut self.file_read, path.to_string_lossy().into_owned());
            }
            CapabilityRequest::FileWrite { path } => {
                let s = path.to_string_lossy().into_owned();
                push_allow(&mut self.file_read, s.clone());
                push_allow(&mut self.file_write, s);
            }
            CapabilityRequest::Http { host, .. } => {
                push_allow(&mut self.http_hosts, host.clone());
            }
            CapabilityRequest::CommandWorkDir { work_dir } => {
                let s = work_dir.to_string_lossy().into_owned();
                push_allow(&mut self.file_read, s.clone());
                push_allow(&mut self.command_work_dir, s);
            }
        }
    }

    pub fn merge_from(&mut self, other: &PermissionPolicyLayer) {
        fn merge_ruleset(target: &mut Option<RuleSet<String>>, src: &Option<RuleSet<String>>) {
            let Some(s) = src else { return };
            let t = target.get_or_insert_with(RuleSet::default);
            if let Some(ref deny) = s.deny {
                let list = t.deny.get_or_insert_with(Vec::new);
                for item in deny {
                    if !list.contains(item) {
                        list.push(item.clone());
                    }
                }
            }
            if let Some(ref allow) = s.allow {
                let list = t.allow.get_or_insert_with(Vec::new);
                if list.iter().any(|x| x == "*") && !allow.iter().any(|x| x == "*") {
                    *list = allow.clone();
                } else {
                    for item in allow {
                        if !list.contains(item) {
                            list.push(item.clone());
                        }
                    }
                }
            }
            if let Some(def) = s.default {
                t.default = Some(def);
            }
        }
        merge_ruleset(&mut self.file_read, &other.file_read);
        merge_ruleset(&mut self.file_write, &other.file_write);
        merge_ruleset(&mut self.http_hosts, &other.http_hosts);
        merge_ruleset(&mut self.command_work_dir, &other.command_work_dir);
    }
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
    AllowWorkspace,
    /// Effective for all tasks across the entire Node (agent-daemon)
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

/// Four-segment composable System Prompt persisted in `api-server`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptParts {
    /// 1. System-wide base prompt configured on `api-server`
    #[serde(default)]
    pub system_prompt: String,
    /// 2. Node-level environment prompt configured on `NodeSpec`
    #[serde(default)]
    pub node_prompt: String,
    /// 3. Client/Role prompt configured on `SessionSpec`
    #[serde(default)]
    pub client_prompt: String,
    /// 4. Extension-contributed prompts keyed by extension id (e.g. `agents_md`, `skills`, `chatroom`)
    #[serde(default)]
    pub extension_prompts: BTreeMap<String, String>,
}

impl PromptParts {
    /// Deterministically assembles the 4 segments from most static to most dynamic.
    pub fn assemble(&self) -> String {
        let mut sections = Vec::new();
        if !self.system_prompt.trim().is_empty() {
            sections.push(self.system_prompt.trim().to_string());
        }
        if !self.node_prompt.trim().is_empty() {
            sections.push(self.node_prompt.trim().to_string());
        }
        if !self.client_prompt.trim().is_empty() {
            sections.push(self.client_prompt.trim().to_string());
        }
        if let Some(agents_md) = self.extension_prompts.get("agents_md") {
            if !agents_md.trim().is_empty() {
                sections.push(agents_md.trim().to_string());
            }
        }
        for (ext_id, text) in &self.extension_prompts {
            if ext_id == "agents_md" {
                continue;
            }
            if !text.trim().is_empty() {
                sections.push(text.trim().to_string());
            }
        }
        if sections.is_empty() {
            "You are a helpful assistant.".to_string()
        } else {
            sections.join("\n\n")
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
    #[serde(default)]
    pub prompt_parts: PromptParts,
    pub model: ModelRequestSpec,
    #[serde(default)]
    pub extensions: Vec<ExtensionSpec>,
    #[serde(default)]
    pub effective_tools: Vec<String>,
}

impl ConfigRevision {
    pub fn new_merged(
        system_prompt: String,
        model: ModelRequestSpec,
        extensions: Vec<ExtensionSpec>,
    ) -> Self {
        let prompt_parts = PromptParts {
            client_prompt: system_prompt.clone(),
            ..Default::default()
        };
        Self::new_merged_with_parts(prompt_parts, system_prompt, model, extensions, Vec::new())
    }

    pub fn new_merged_with_parts(
        prompt_parts: PromptParts,
        system_prompt: String,
        model: ModelRequestSpec,
        extensions: Vec<ExtensionSpec>,
        effective_tools: Vec<String>,
    ) -> Self {
        let canonical = canonicalize_json(&serde_json::json!({
            "format_version": 1,
            "system_prompt": &system_prompt,
            "prompt_parts": &prompt_parts,
            "model": &model,
            "extensions": &extensions,
            "effective_tools": &effective_tools,
        }));
        let digest = sha256_hex(&canonical);
        Self {
            format_version: 1,
            config_ref: ConfigRef::new(format!("cfg_sha256:{digest}")),
            created_at: Utc::now(),
            system_prompt,
            prompt_parts,
            model,
            extensions,
            effective_tools,
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
