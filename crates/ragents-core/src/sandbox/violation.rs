use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

pub(crate) fn closest_existing_dir(path: &Path) -> Option<PathBuf> {
    let mut cur = Some(path);
    while let Some(p) = cur {
        if p.exists() && p.is_dir() {
            return Some(p.to_path_buf());
        }
        cur = p.parent();
    }
    None
}

pub fn canonicalize_or_normalize(path: &Path) -> PathBuf {
    canonicalize_with_nonexistent_tail(path).unwrap_or_else(|| path.to_path_buf())
}

/// Normalizes a specific violated file path (such as `/Users/alice/.cache/uv/sdists-v6/.git`)
/// to its logical parent directory scope (such as `/Users/alice/.cache/uv`) so that a single
/// user approval unlocks the tool's working directory rather than prompting for every internal lockfile.
pub fn normalize_violation_to_request_dir(raw_path: &Path) -> PathBuf {
    let comps: Vec<Component<'_>> = raw_path.components().collect();

    for i in 0..comps.len() {
        let seg = comps[i].as_os_str().to_string_lossy();
        if (seg == ".cache" || seg == "Caches") && i + 1 < comps.len() {
            let dir: PathBuf = comps[..=i + 1].iter().collect();
            return canonicalize_with_nonexistent_tail(&dir).unwrap_or(dir);
        }
        if seg == ".local" && i + 2 < comps.len() {
            let next_seg = comps[i + 1].as_os_str().to_string_lossy();
            if next_seg == "share" || next_seg == "state" {
                let dir: PathBuf = comps[..=i + 2].iter().collect();
                return canonicalize_with_nonexistent_tail(&dir).unwrap_or(dir);
            }
        }
        if matches!(seg.as_ref(), ".cargo" | ".npm" | ".rustup" | ".uv") {
            let dir: PathBuf = comps[..=i].iter().collect();
            return canonicalize_with_nonexistent_tail(&dir).unwrap_or(dir);
        }
    }

    let dir = if raw_path.exists() && raw_path.is_dir() {
        raw_path.to_path_buf()
    } else if let Some(parent) = raw_path.parent() {
        if !parent.as_os_str().is_empty() {
            parent.to_path_buf()
        } else {
            raw_path.to_path_buf()
        }
    } else {
        raw_path.to_path_buf()
    };

    canonicalize_with_nonexistent_tail(&dir).unwrap_or(dir)
}

/// Fast-path extraction of absolute paths from error lines containing `Operation not permitted` / `os error 1`.
pub(crate) fn extract_eperm_paths_from_text(text: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for line in text.lines() {
        if !line.contains("Operation not permitted") && !line.contains("os error 1") {
            continue;
        }
        for candidate in split_path_candidates(line) {
            if candidate.starts_with('/') && candidate.len() > 1 && !candidate.starts_with("//") {
                let p = PathBuf::from(candidate);
                if !out.contains(&p) {
                    out.push(p);
                }
            }
        }
    }
    out
}

fn split_path_candidates(line: &str) -> Vec<&str> {
    let mut candidates = Vec::new();
    for part in line.split(['`', '\'', '"']) {
        let trimmed = part.trim().trim_end_matches(':');
        if trimmed.starts_with('/') {
            candidates.push(trimmed);
        }
    }
    if candidates.is_empty() {
        for token in line.split_whitespace() {
            let trimmed = token
                .trim_matches(|c: char| matches!(c, '`' | '\'' | '"' | ':' | '(' | ')' | ','))
                .trim_end_matches(':');
            if trimmed.starts_with('/') {
                candidates.push(trimmed);
            }
        }
    }
    candidates
}

/// Queries the macOS Unified Log for kernel sandbox violations tagged with `trace_tag`,
/// extracting `(operation, denied_path)` even when the child process swallowed the path in stderr.
pub(crate) async fn query_macos_sandbox_violations(trace_tag: &str) -> Vec<(String, String)> {
    let predicate = format!("sender == \"Sandbox\" AND eventMessage CONTAINS \"{trace_tag}\"");
    let mut cmd = tokio::process::Command::new("/usr/bin/log");
    cmd.arg("show")
        .arg("--last")
        .arg("10s")
        .arg("--style")
        .arg("compact")
        .arg("--predicate")
        .arg(predicate)
        .kill_on_drop(true);

    let output = match tokio::time::timeout(Duration::from_millis(1500), cmd.output()).await {
        Ok(Ok(out)) => out,
        _ => return Vec::new(),
    };

    let text = String::from_utf8_lossy(&output.stdout);
    let mut results = Vec::new();
    let mut seen = BTreeSet::new();
    for line in text.lines() {
        if let Some(sb_idx) = line.find("Sandbox: ") {
            let after_sb = &line[sb_idx + "Sandbox: ".len()..];
            if let Some(deny_idx) = after_sb.find(") deny(") {
                let after_deny = &after_sb[deny_idx + ") deny(".len()..];
                if let Some(close_paren) = after_deny.find(") ") {
                    let op_and_path = after_deny[close_paren + 2..].trim();
                    if let Some((op, path)) = op_and_path.split_once(' ') {
                        let key = format!("{op}:{path}");
                        if seen.insert(key) {
                            results.push((op.to_string(), path.trim().to_string()));
                        }
                    }
                }
            }
        }
    }
    results
}

pub(crate) fn expand_canonical_paths(paths: &[PathBuf]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for path in paths {
        let cleaned = clean_path(path);
        if let Some(s) = cleaned.to_str() {
            set.insert(s.to_string());
        }
        if let Some(canon) = canonicalize_with_nonexistent_tail(&cleaned) {
            if let Some(s) = canon.to_str() {
                set.insert(s.to_string());
            }
        }
    }
    set
}

fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub(crate) fn canonicalize_with_nonexistent_tail(path: &Path) -> Option<PathBuf> {
    if let Ok(canon) = path.canonicalize() {
        return Some(canon);
    }
    let mut cur = path;
    let mut tail_components = Vec::new();
    while let Some(parent) = cur.parent() {
        if let Some(name) = cur.file_name() {
            tail_components.push(name.to_os_string());
        }
        if let Ok(mut canon_parent) = parent.canonicalize() {
            for comp in tail_components.into_iter().rev() {
                canon_parent.push(comp);
            }
            return Some(canon_parent);
        }
        cur = parent;
    }
    None
}

pub(crate) fn escape_sbpl(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('"', "\\\"")
}
