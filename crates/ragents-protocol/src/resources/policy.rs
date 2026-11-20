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
    pub fn is_empty(&self) -> bool {
        fn rs_empty(rs: &Option<RuleSet<String>>) -> bool {
            let Some(r) = rs else { return true };
            r.allow.as_ref().is_none_or(|v| v.is_empty())
                && r.deny.as_ref().is_none_or(|v| v.is_empty())
                && r.default.is_none()
        }
        rs_empty(&self.file_read)
            && rs_empty(&self.file_write)
            && rs_empty(&self.http_hosts)
            && rs_empty(&self.command_work_dir)
    }

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
                push_allow(&mut self.file_write, path.to_string_lossy().into_owned());
            }
            CapabilityRequest::Http { host, .. } => {
                push_allow(&mut self.http_hosts, host.clone());
            }
            CapabilityRequest::CommandWorkDir { work_dir } => {
                push_allow(
                    &mut self.command_work_dir,
                    work_dir.to_string_lossy().into_owned(),
                );
            }
        }
        self.prune_redundant();
    }

    /// Removes redundant rules within this `PermissionPolicyLayer` (e.g. duplicates, trailing-slash
    /// variants, narrower subpaths/subdomains subsumed by a broader rule in the same category and list,
    /// and `file_read.allow` rules already covered by `file_write.allow`).
    /// Returns `true` if any rule was modified or removed.
    pub fn prune_redundant(&mut self) -> bool {
        self.prune_redundant_with_write_denies(&[])
    }

    /// Variant of [`Self::prune_redundant`] that also prevents `file_write.allow` from pruning
    /// `file_read.allow` entries overlapping any external `file_write.deny` rule (e.g. from higher layers).
    pub fn prune_redundant_with_write_denies(&mut self, extra_write_denies: &[String]) -> bool {
        let mut changed = false;
        changed |= prune_ruleset_internal(&mut self.file_read, PolicyRuleKind::Path);
        changed |= prune_ruleset_internal(&mut self.file_write, PolicyRuleKind::Path);
        changed |= prune_ruleset_internal(&mut self.command_work_dir, PolicyRuleKind::Path);
        changed |= prune_ruleset_internal(&mut self.http_hosts, PolicyRuleKind::Host);
        changed |= prune_read_allows_covered_by_write(
            &mut self.file_read,
            self.file_write.as_ref(),
            extra_write_denies,
        );
        changed
    }

    /// Removes rules from `self` that are completely subsumed by rules in a strictly higher-precedence
    /// `higher` layer (same category and same `allow`/`deny` list, plus `self.file_read.allow` covered
    /// by `higher.file_write.allow`). Also normalizes/prunes `self` internally.
    /// Returns `true` if any rule was modified or removed.
    pub fn prune_subsumed_by(&mut self, higher: &PermissionPolicyLayer) -> bool {
        self.prune_subsumed_by_with_write_denies(higher, &[])
    }

    /// Variant of [`Self::prune_subsumed_by`] that accepts additional `file_write.deny` rules from
    /// sibling/ancestor layers so write-implied read pruning never strips explicit `file_read.allow`
    /// grants in write-denied subtrees.
    pub fn prune_subsumed_by_with_write_denies(
        &mut self,
        higher: &PermissionPolicyLayer,
        extra_write_denies: &[String],
    ) -> bool {
        let mut combined_write_denies: Vec<String> = extra_write_denies.to_vec();
        for src in [self.file_write.as_ref(), higher.file_write.as_ref()] {
            if let Some(denies) = src.and_then(|r| r.deny.as_ref()) {
                for d in denies {
                    if !combined_write_denies.contains(d) {
                        combined_write_denies.push(d.clone());
                    }
                }
            }
        }
        let mut changed = self.prune_redundant_with_write_denies(&combined_write_denies);
        changed |= prune_ruleset_against(&mut self.file_read, &higher.file_read, PolicyRuleKind::Path);
        changed |=
            prune_ruleset_against(&mut self.file_write, &higher.file_write, PolicyRuleKind::Path);
        changed |= prune_ruleset_against(
            &mut self.command_work_dir,
            &higher.command_work_dir,
            PolicyRuleKind::Path,
        );
        changed |=
            prune_ruleset_against(&mut self.http_hosts, &higher.http_hosts, PolicyRuleKind::Host);
        changed |= prune_read_allows_covered_by_write(
            &mut self.file_read,
            higher.file_write.as_ref(),
            &combined_write_denies,
        );
        changed
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

    /// Removes `rule` from the specified `category` (`file_read`, `file_write`, `http_hosts`, `command_work_dir`)
    /// and `list_kind` (`allow` or `deny`). Returns `true` if a matching rule was removed.
    pub fn revoke_rule(&mut self, category: &str, list_kind: &str, rule: &str) -> bool {
        let slot = match category {
            "file_read" => &mut self.file_read,
            "file_write" => &mut self.file_write,
            "http_hosts" => &mut self.http_hosts,
            "command_work_dir" => &mut self.command_work_dir,
            _ => return false,
        };
        let Some(rs) = slot.as_mut() else {
            return false;
        };
        let vec_opt = match list_kind {
            "deny" => &mut rs.deny,
            _ => &mut rs.allow,
        };
        let Some(vec) = vec_opt.as_mut() else {
            return false;
        };
        let before = vec.len();
        vec.retain(|r| r != rule);
        let removed = vec.len() < before;
        if vec.is_empty() {
            *vec_opt = None;
        }
        if rs.allow.is_none() && rs.deny.is_none() && rs.default.is_none() {
            *slot = None;
        }
        removed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyRuleKind {
    Path,
    Host,
}

fn normalize_lexical_rule_path(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed == "*" {
        return "*".to_string();
    }
    if trimmed.is_empty() || trimmed == "." {
        return ".".to_string();
    }
    use std::path::{Component, Path};
    let p = Path::new(trimmed);
    let mut is_root = false;
    let mut prefix: Option<String> = None;
    let mut parts: Vec<String> = Vec::new();
    for comp in p.components() {
        match comp {
            Component::Prefix(pre) => {
                prefix = Some(pre.as_os_str().to_string_lossy().into_owned());
            }
            Component::RootDir => {
                is_root = true;
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !parts.is_empty() && parts.last().map(|s| s.as_str()) != Some("..") {
                    parts.pop();
                } else if !is_root {
                    parts.push("..".to_string());
                }
            }
            Component::Normal(seg) => {
                parts.push(seg.to_string_lossy().into_owned());
            }
        }
    }
    let mut out = String::new();
    if let Some(pre) = prefix {
        out.push_str(&pre);
    }
    if is_root {
        out.push('/');
    }
    out.push_str(&parts.join("/"));
    if out.is_empty() {
        if is_root {
            "/".to_string()
        } else {
            ".".to_string()
        }
    } else {
        out
    }
}

fn is_protected_system_executable_rule(norm_path: &str) -> bool {
    let Some(parent) = std::path::Path::new(norm_path).parent() else {
        return false;
    };
    let parent_str = parent.to_string_lossy();
    matches!(
        parent_str.trim_end_matches('/'),
        "/bin"
            | "/sbin"
            | "/usr/bin"
            | "/usr/sbin"
            | "/usr/local/bin"
            | "/usr/local/sbin"
            | "/System/Cryptexes/App/usr/bin"
    )
}

fn path_rule_subsumes(broad: &str, narrow: &str) -> bool {
    let b = normalize_lexical_rule_path(broad);
    let n = normalize_lexical_rule_path(narrow);
    if b == "*" {
        return true;
    }
    if n == "*" {
        return false;
    }
    if b == n {
        return true;
    }
    if b == "." || n == "." {
        return false;
    }
    let bp = std::path::Path::new(&b);
    let np = std::path::Path::new(&n);
    if bp.is_absolute() != np.is_absolute() {
        return false;
    }
    if np.starts_with(bp) && !is_protected_system_executable_rule(&n) {
        return true;
    }
    false
}

fn host_rule_subsumes(broad: &str, narrow: &str) -> bool {
    let b = broad.trim();
    let n = narrow.trim();
    if b.is_empty() || n.is_empty() {
        return false;
    }
    if b == "*" {
        return true;
    }
    if n == "*" {
        return false;
    }
    if b.eq_ignore_ascii_case(n) {
        return true;
    }
    if let Some(b_suffix) = b.strip_prefix("*.") {
        let b_low = b_suffix.to_ascii_lowercase();
        let n_target = n.strip_prefix("*.").unwrap_or(n).to_ascii_lowercase();
        return n_target == b_low || n_target.ends_with(&format!(".{b_low}"));
    }
    false
}

fn rule_subsumes(kind: PolicyRuleKind, broad: &str, narrow: &str) -> bool {
    match kind {
        PolicyRuleKind::Path => path_rule_subsumes(broad, narrow),
        PolicyRuleKind::Host => host_rule_subsumes(broad, narrow),
    }
}

fn normalize_rule(kind: PolicyRuleKind, raw: &str) -> String {
    match kind {
        PolicyRuleKind::Path => normalize_lexical_rule_path(raw),
        PolicyRuleKind::Host => raw.trim().to_string(),
    }
}

fn prune_rule_list_internal(list_opt: &mut Option<Vec<String>>, kind: PolicyRuleKind) -> bool {
    let Some(list) = list_opt.as_mut() else {
        return false;
    };
    let orig = list.clone();
    let normalized: Vec<String> = list
        .iter()
        .map(|r| normalize_rule(kind, r))
        .filter(|r| !r.is_empty())
        .collect();

    let mut deduped: Vec<String> = Vec::with_capacity(normalized.len());
    for item in normalized {
        let already_eq = deduped
            .iter()
            .any(|existing| rule_subsumes(kind, existing, &item) && rule_subsumes(kind, &item, existing));
        if !already_eq {
            deduped.push(item);
        }
    }

    let kept: Vec<String> = deduped
        .iter()
        .enumerate()
        .filter(|(idx, candidate)| {
            !deduped.iter().enumerate().any(|(other_idx, other)| {
                other_idx != *idx
                    && rule_subsumes(kind, other, candidate)
                    && !rule_subsumes(kind, candidate, other)
            })
        })
        .map(|(_, item)| item.clone())
        .collect();

    let changed = kept != orig;
    if kept.is_empty() {
        *list_opt = None;
    } else {
        *list = kept;
    }
    changed
}

fn prune_ruleset_internal(slot: &mut Option<RuleSet<String>>, kind: PolicyRuleKind) -> bool {
    let Some(rs) = slot.as_mut() else {
        return false;
    };
    let mut changed = false;
    changed |= prune_rule_list_internal(&mut rs.allow, kind);
    changed |= prune_rule_list_internal(&mut rs.deny, kind);
    if rs.allow.is_none() && rs.deny.is_none() && rs.default.is_none() {
        *slot = None;
        changed = true;
    }
    changed
}

fn prune_rule_list_against(
    low_opt: &mut Option<Vec<String>>,
    high_opt: &Option<Vec<String>>,
    kind: PolicyRuleKind,
) -> bool {
    let (Some(low), Some(high)) = (low_opt.as_mut(), high_opt.as_ref()) else {
        return false;
    };
    let before = low.len();
    low.retain(|r_low| !high.iter().any(|r_high| rule_subsumes(kind, r_high, r_low)));
    let changed = low.len() < before;
    if low.is_empty() {
        *low_opt = None;
    }
    changed
}

fn prune_ruleset_against(
    low_slot: &mut Option<RuleSet<String>>,
    high_slot: &Option<RuleSet<String>>,
    kind: PolicyRuleKind,
) -> bool {
    let (Some(low_rs), Some(high_rs)) = (low_slot.as_mut(), high_slot.as_ref()) else {
        return false;
    };
    let mut changed = false;
    changed |= prune_rule_list_against(&mut low_rs.allow, &high_rs.allow, kind);
    changed |= prune_rule_list_against(&mut low_rs.deny, &high_rs.deny, kind);
    if low_rs.allow.is_none() && low_rs.deny.is_none() && low_rs.default.is_none() {
        *low_slot = None;
        changed = true;
    }
    changed
}

fn prune_read_allows_covered_by_write(
    read_slot: &mut Option<RuleSet<String>>,
    write_rs: Option<&RuleSet<String>>,
    extra_write_denies: &[String],
) -> bool {
    let (Some(read_rs), Some(w_rs)) = (read_slot.as_mut(), write_rs) else {
        return false;
    };
    let Some(ref w_allow) = w_rs.allow else {
        return false;
    };
    let Some(ref mut r_allow) = read_rs.allow else {
        return false;
    };
    let w_deny = w_rs.deny.as_deref().unwrap_or(&[]);
    let has_wildcard_write = w_allow.iter().any(|w| w.trim() == "*");
    let before = r_allow.len();
    r_allow.retain(|r| {
        if r.trim() == "." && !has_wildcard_write {
            return true;
        }
        let write_deny_interferes = w_deny
            .iter()
            .chain(extra_write_denies.iter())
            .any(|d| path_rule_subsumes(d, r) || path_rule_subsumes(r, d));
        if write_deny_interferes {
            return true;
        }
        !w_allow.iter().any(|w| path_rule_subsumes(w, r))
    });
    let changed = r_allow.len() < before;
    if r_allow.is_empty() {
        read_rs.allow = None;
    }
    if read_rs.allow.is_none() && read_rs.deny.is_none() && read_rs.default.is_none() {
        *read_slot = None;
        return true;
    }
    changed
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
