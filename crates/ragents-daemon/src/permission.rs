use ragents_protocol::*;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEvalResult {
    Allow,
    Deny,
    Ask,
}

pub fn evaluate_capability_policy(
    policy: &PermissionPolicyLayer,
    workspace_root: &Path,
    session_tmp_dir: &Path,
    capability: &CapabilityRequest,
) -> PolicyEvalResult {
    match capability {
        CapabilityRequest::FileRead { path } => evaluate_path_rules(
            policy.file_read.as_ref(),
            path,
            workspace_root,
            session_tmp_dir,
        ),
        CapabilityRequest::FileWrite { path } => evaluate_path_rules(
            policy.file_write.as_ref(),
            path,
            workspace_root,
            session_tmp_dir,
        ),
        CapabilityRequest::CommandWorkDir { work_dir } => evaluate_path_rules(
            policy.command_work_dir.as_ref(),
            work_dir,
            workspace_root,
            session_tmp_dir,
        ),
        CapabilityRequest::Http { host, .. } => {
            if let Some(ref rules) = policy.http_hosts {
                if let Some(ref denies) = rules.deny {
                    if denies.iter().any(|r| host_matches(r, host)) {
                        return PolicyEvalResult::Deny;
                    }
                }
                if let Some(ref allows) = rules.allow {
                    if allows.iter().any(|r| host_matches(r, host)) {
                        return PolicyEvalResult::Allow;
                    }
                }
                match rules.default.unwrap_or(DefaultPermissionDecision::Deny) {
                    DefaultPermissionDecision::Allow => PolicyEvalResult::Allow,
                    DefaultPermissionDecision::Deny => PolicyEvalResult::Deny,
                    DefaultPermissionDecision::Ask => PolicyEvalResult::Ask,
                }
            } else {
                PolicyEvalResult::Deny
            }
        }
    }
}

fn evaluate_path_rules(
    rules: Option<&RuleSet<String>>,
    target_path: &Path,
    workspace_root: &Path,
    session_tmp_dir: &Path,
) -> PolicyEvalResult {
    let norm_target = normalize_path(target_path, workspace_root);
    let norm_ws = normalize_path(workspace_root, workspace_root);
    let norm_tmp = normalize_path(session_tmp_dir, workspace_root);

    if let Some(rule_set) = rules {
        if let Some(ref denies) = rule_set.deny {
            for pattern in denies {
                let norm_pat = normalize_path(Path::new(pattern), workspace_root);
                if norm_target.starts_with(&norm_pat) || pattern == "*" {
                    return PolicyEvalResult::Deny;
                }
            }
        }
        if let Some(ref allows) = rule_set.allow {
            for pattern in allows {
                let norm_pat = normalize_path(Path::new(pattern), workspace_root);
                if norm_target.starts_with(&norm_pat) || pattern == "*" {
                    return PolicyEvalResult::Allow;
                }
            }
        }
        if let Some(default_decision) = rule_set.default {
            return match default_decision {
                DefaultPermissionDecision::Allow => PolicyEvalResult::Allow,
                DefaultPermissionDecision::Deny => PolicyEvalResult::Deny,
                DefaultPermissionDecision::Ask => PolicyEvalResult::Ask,
            };
        }
    }

    // Default safe boundary: allow paths inside workspace_root or session_tmp_dir;
    // paths outside workspace_root default to Ask so the user can interactively approve or deny.
    if norm_target.starts_with(&norm_ws) || norm_target.starts_with(&norm_tmp) {
        PolicyEvalResult::Allow
    } else {
        PolicyEvalResult::Ask
    }
}

pub fn capability_covered_by(
    granted: &CapabilityRequest,
    requested: &CapabilityRequest,
    workspace_root: &Path,
) -> bool {
    if granted == requested {
        return true;
    }
    match (granted, requested) {
        (CapabilityRequest::FileWrite { path: g }, CapabilityRequest::FileWrite { path: r })
        | (CapabilityRequest::FileRead { path: g }, CapabilityRequest::FileRead { path: r })
        | (
            CapabilityRequest::CommandWorkDir { work_dir: g },
            CapabilityRequest::CommandWorkDir { work_dir: r },
        ) => {
            let norm_g = normalize_path(g, workspace_root);
            let norm_r = normalize_path(r, workspace_root);
            norm_r.starts_with(&norm_g)
        }
        _ => false,
    }
}

fn host_matches(rule: &str, host: &str) -> bool {
    rule == "*" || rule.eq_ignore_ascii_case(host)
}

fn normalize_path(path: &Path, base: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    if let Ok(canon) = joined.canonicalize() {
        return canon;
    }
    let mut out = PathBuf::new();
    for comp in joined.components() {
        use std::path::Component;
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c.as_os_str()),
        }
    }
    out
}
