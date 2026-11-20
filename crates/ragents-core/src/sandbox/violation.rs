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

/// Returns `true` if `path` exists as a regular file on a Unix platform and has its
/// `setuid` (`0o4000`) or `setgid` (`0o2000`) permission bit set.
pub fn is_setuid_or_setgid_binary(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = std::fs::metadata(path) {
            return meta.is_file() && (meta.mode() & 0o6000) != 0;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    false
}

/// Returns `true` if `dir` is a system executable directory (`/bin`, `/sbin`, `/usr/bin`,
/// `/usr/sbin`, `/usr/local/bin`, `/usr/local/sbin`) where executables must be authorized
/// individually per command rather than collapsed to the entire directory.
pub fn is_system_executable_dir(dir: &Path) -> bool {
    for candidate in [Some(dir.to_path_buf()), canonicalize_with_nonexistent_tail(dir)]
        .into_iter()
        .flatten()
    {
        let s = candidate.to_string_lossy();
        let trimmed = s.trim_end_matches('/');
        if matches!(
            trimmed,
            "/bin"
                | "/sbin"
                | "/usr/bin"
                | "/usr/sbin"
                | "/usr/local/bin"
                | "/usr/local/sbin"
                | "/System/Cryptexes/App/usr/bin"
        ) {
            return true;
        }
    }
    false
}

/// Normalizes a specific violated file path (such as `/Users/alice/.cache/uv/sdists-v6/.git`)
/// to its logical parent directory scope (such as `/Users/alice/.cache/uv`) so that a single
/// user approval unlocks the tool's working directory rather than prompting for every internal lockfile.
///
/// System executables under `/bin`, `/sbin`, `/usr/bin`, `/usr/sbin` and any binary with
/// `setuid`/`setgid` bits are never collapsed to their parent directory and are authorized
/// individually per binary.
pub fn normalize_violation_to_request_dir(raw_path: &Path) -> PathBuf {
    if is_setuid_or_setgid_binary(raw_path)
        || raw_path.parent().is_some_and(is_system_executable_dir)
    {
        return canonicalize_with_nonexistent_tail(raw_path)
            .unwrap_or_else(|| raw_path.to_path_buf());
    }

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
    } else if raw_path.exists() {
        if let Some(parent) = raw_path.parent() {
            if !parent.as_os_str().is_empty() && !is_broad_top_or_home_dir(parent) {
                parent.to_path_buf()
            } else {
                raw_path.to_path_buf()
            }
        } else {
            raw_path.to_path_buf()
        }
    } else if let Some(parent) = raw_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty() && p.exists())
    {
        if !is_broad_top_or_home_dir(parent) {
            parent.to_path_buf()
        } else {
            raw_path.to_path_buf()
        }
    } else if let Some(existing_dir) =
        closest_existing_dir(raw_path).filter(|d| !is_broad_top_or_home_dir(d))
    {
        existing_dir
    } else if let Some(parent) = raw_path.parent() {
        if !parent.as_os_str().is_empty() && !is_broad_top_or_home_dir(parent) {
            parent.to_path_buf()
        } else {
            raw_path.to_path_buf()
        }
    } else {
        raw_path.to_path_buf()
    };

    canonicalize_with_nonexistent_tail(&dir).unwrap_or(dir)
}

/// Returns `true` if `dir` is `/` or a user home directory (`/Users/<user>`, `/home/<user>`, `$HOME`)
/// or top-level mount point (`/Users`, `/home`, `/Volumes`, `/tmp`) that must never be auto-suggested as a
/// broad directory grant when a single file directly inside it (e.g. `~/.gitconfig`) is accessed.
pub(crate) fn is_broad_top_or_home_dir(dir: &Path) -> bool {
    let norm_dir = canonicalize_with_nonexistent_tail(dir).unwrap_or_else(|| dir.to_path_buf());
    if let Ok(home) = std::env::var("HOME") {
        let home_path = Path::new(&home);
        let norm_home =
            canonicalize_with_nonexistent_tail(home_path).unwrap_or_else(|| home_path.to_path_buf());
        if dir == home_path || norm_dir == home_path || norm_dir == norm_home {
            return true;
        }
    }
    for candidate in [dir, norm_dir.as_path()] {
        let normal_parts: Vec<String> = candidate
            .components()
            .filter_map(|c| match c {
                Component::Normal(s) => Some(s.to_string_lossy().to_string()),
                _ => None,
            })
            .collect();
        let is_broad = match normal_parts.len() {
            0 => true,
            1 => matches!(
                normal_parts[0].as_str(),
                "Users" | "home" | "Volumes" | "root" | "var" | "private" | "tmp"
            ),
            2 => matches!(
                (normal_parts[0].as_str(), normal_parts[1].as_str()),
                ("Users", _) | ("home", _) | ("Volumes", _) | ("private", "tmp") | ("var", "home")
            ),
            3 => normal_parts[0] == "var" && normal_parts[1] == "home",
            _ => false,
        };
        if is_broad {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtractedSandboxErrorPath {
    pub(crate) path: PathBuf,
    pub(crate) is_read_only_fs: bool,
}

/// Fast-path extraction of absolute paths from error lines containing OS sandbox errors:
/// - `Operation not permitted` / `os error 1` / `不允许的操作` (macOS Seatbelt / EPERM)
/// - `Read-only file system` / `os error 30` / `只读文件系统` (Linux `bwrap --ro-bind / /` / EROFS)
pub(crate) fn extract_sandbox_error_paths_from_text(text: &str) -> Vec<ExtractedSandboxErrorPath> {
    let mut out: Vec<ExtractedSandboxErrorPath> = Vec::new();
    for line in text.lines() {
        let is_rofs = line.contains("Read-only file system")
            || line.contains("os error 30")
            || line.contains("只读文件系统");
        let is_eperm = line.contains("Operation not permitted")
            || line.contains("os error 1")
            || line.contains("不允许的操作");
        if !is_rofs && !is_eperm {
            continue;
        }
        for candidate in split_path_candidates(line) {
            if candidate.starts_with('/') && candidate.len() > 1 && !candidate.starts_with("//") {
                let p = PathBuf::from(candidate);
                if let Some(existing) = out.iter_mut().find(|item| item.path == p) {
                    existing.is_read_only_fs |= is_rofs;
                } else {
                    out.push(ExtractedSandboxErrorPath {
                        path: p,
                        is_read_only_fs: is_rofs,
                    });
                }
            }
        }
    }
    out
}

fn split_path_candidates(line: &str) -> Vec<&str> {
    let mut candidates = Vec::new();
    for token in line.split_whitespace() {
        let trimmed = token
            .trim_matches(|c: char| {
                matches!(
                    c,
                    '`' | '\''
                        | '"'
                        | '“'
                        | '”'
                        | '‘'
                        | '’'
                        | '「'
                        | '」'
                        | ':'
                        | '：'
                        | '('
                        | ')'
                        | ','
                        | ';'
                )
            })
            .trim_end_matches(&[':', '：'][..]);
        if trimmed.starts_with('/') && trimmed.len() > 1 && !trimmed.starts_with("//") {
            candidates.push(trimmed);
        }
    }
    for (open_q, close_q) in [
        ('`', '`'),
        ('\'', '\''),
        ('"', '"'),
        ('“', '”'),
        ('‘', '’'),
        ('「', '」'),
    ] {
        let mut rest = line;
        while let Some(start) = rest.find(open_q) {
            let after_open = &rest[start + open_q.len_utf8()..];
            let Some(end) = after_open.find(close_q) else {
                break;
            };
            let inside = after_open[..end]
                .trim()
                .trim_end_matches(&[':', '：'][..]);
            if inside.starts_with('/')
                && inside.len() > 1
                && !inside.starts_with("//")
                && !inside.contains(": ")
                && !inside.contains("： ")
                && !candidates.contains(&inside)
            {
                candidates.push(inside);
            }
            rest = &after_open[end + close_q.len_utf8()..];
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
                        let trimmed_path = path.trim();
                        if op.starts_with("file-read")
                            && is_broad_top_or_home_dir(Path::new(trimmed_path))
                        {
                            continue;
                        }
                        let key = format!("{op}:{trimmed_path}");
                        if seen.insert(key) {
                            results.push((op.to_string(), trimmed_path.to_string()));
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
