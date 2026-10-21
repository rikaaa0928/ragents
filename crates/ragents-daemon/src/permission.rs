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
                match rules.default.unwrap_or(DefaultPermissionDecision::Ask) {
                    DefaultPermissionDecision::Allow => PolicyEvalResult::Allow,
                    DefaultPermissionDecision::Deny => PolicyEvalResult::Deny,
                    DefaultPermissionDecision::Ask => PolicyEvalResult::Ask,
                }
            } else {
                // Default to Ask so WASM HTTP requests to unconfigured hosts trigger interactive approval
                PolicyEvalResult::Ask
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
    let require_exact = ragents_core::sandbox::is_setuid_or_setgid_binary(&norm_target)
        || ragents_core::sandbox::is_setuid_or_setgid_binary(target_path);

    let is_default_safe = !require_exact
        && (norm_target.starts_with(&norm_ws)
            || norm_target.starts_with(&norm_tmp)
            || norm_target.starts_with(Path::new("/tmp"))
            || norm_target.starts_with(Path::new("/private/tmp")));

    if let Some(rule_set) = rules {
        if let Some(ref denies) = rule_set.deny {
            for pattern in denies {
                let norm_pat = normalize_path(Path::new(pattern), workspace_root);
                if norm_target.starts_with(&norm_pat) || pattern == "*" {
                    return PolicyEvalResult::Deny;
                }
            }
        }
        if is_default_safe {
            return PolicyEvalResult::Allow;
        }
        if let Some(ref allows) = rule_set.allow {
            for pattern in allows {
                let norm_pat = normalize_path(Path::new(pattern), workspace_root);
                let matched = if require_exact {
                    norm_target == norm_pat || pattern == "*"
                } else {
                    norm_target.starts_with(&norm_pat) || pattern == "*"
                };
                if matched {
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

    // Default safe boundary: allow paths inside workspace_root, session_tmp_dir, or /tmp;
    // paths outside default to Ask so the user can interactively approve or deny.
    if is_default_safe {
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
            if ragents_core::sandbox::is_setuid_or_setgid_binary(&norm_r)
                || ragents_core::sandbox::is_setuid_or_setgid_binary(r)
            {
                norm_r == norm_g
            } else {
                norm_r.starts_with(&norm_g)
            }
        }
        (CapabilityRequest::Http { host: g, .. }, CapabilityRequest::Http { host: r, .. }) => {
            host_matches(g, r)
        }
        _ => false,
    }
}

pub fn host_matches(rule: &str, host: &str) -> bool {
    let rule = rule.trim();
    let host = host.trim();
    if rule == "*" || rule.eq_ignore_ascii_case(host) {
        return true;
    }
    if let Some(suffix) = rule.strip_prefix("*.") {
        if host.eq_ignore_ascii_case(suffix) {
            return true;
        }
        let dot_suffix = format!(".{}", suffix.to_ascii_lowercase());
        return host.to_ascii_lowercase().ends_with(&dot_suffix);
    }
    false
}

fn normalize_path(path: &Path, base: &Path) -> PathBuf {
    let path_str = path.to_string_lossy();
    let expanded_buf = if path_str == "~" || path_str.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
            let tail = path_str.strip_prefix("~/").unwrap_or("");
            if tail.is_empty() {
                PathBuf::from(home)
            } else {
                Path::new(&home).join(tail)
            }
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    };
    let joined = if expanded_buf.is_absolute() {
        expanded_buf
    } else {
        base.join(expanded_buf)
    };
    if let Ok(canon) = joined.canonicalize() {
        return canon;
    }
    let mut cleaned = PathBuf::new();
    for comp in joined.components() {
        use std::path::Component;
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                cleaned.pop();
            }
            c => cleaned.push(c.as_os_str()),
        }
    }

    // Walk up to the nearest existing ancestor so symlinks like `/tmp` -> `/private/tmp`
    // on macOS resolve consistently even when the leaf file does not exist yet.
    let mut cursor: &Path = &cleaned;
    let mut tail = Vec::new();
    while !cursor.as_os_str().is_empty() {
        if let Ok(canon_ancestor) = cursor.canonicalize() {
            let mut result = canon_ancestor;
            for part in tail.into_iter().rev() {
                result.push(part);
            }
            return result;
        }
        if let Some(name) = cursor.file_name() {
            tail.push(name.to_os_string());
        }
        match cursor.parent() {
            Some(p) if p != cursor => cursor = p,
            _ => break,
        }
    }

    cleaned
}
