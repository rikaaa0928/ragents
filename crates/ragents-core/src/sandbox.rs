use crate::hooks::runtime::ragent::extension::host::CommandOutput;
use crate::host::CoreRunSpec;
use ragents_protocol::{CapabilityRequest, PermissionPolicyLayer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

/// OS Kernel Sandbox enforcement mode for external commands executed by WASM extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// Automatically enforce OS kernel sandbox (`sandbox-exec` on macOS, `bwrap` on Linux) when available.
    #[default]
    Auto,
    /// Strictly require OS kernel sandbox; fail command execution if the OS sandbox binary is unavailable.
    Enforce,
    /// Disable OS kernel sandbox (still applies hermetic `TMPDIR` environment isolation).
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxViolation {
    pub capability: CapabilityRequest,
    pub raw_path: PathBuf,
    pub operation: String,
}

/// Shared, dynamically updatable sandbox policy state for a single activation.
#[derive(Debug)]
pub struct SandboxState {
    mode: SandboxMode,
    workspace_root: PathBuf,
    session_tmp_dir: PathBuf,
    writable_paths: Mutex<Vec<PathBuf>>,
    denied_write_paths: Mutex<Vec<PathBuf>>,
    denied_read_paths: Mutex<Vec<PathBuf>>,
    allowed_read_paths: Mutex<Vec<PathBuf>>,
}

impl SandboxState {
    pub fn from_run_spec(run_spec: &CoreRunSpec) -> Self {
        Self::from_policy(
            SandboxMode::Auto,
            &run_spec.workspace_root,
            &run_spec.session_tmp_dir,
            &run_spec.effective_permissions,
        )
    }

    pub fn from_policy(
        mode: SandboxMode,
        workspace_root: &Path,
        session_tmp_dir: &Path,
        policy: &PermissionPolicyLayer,
    ) -> Self {
        let mut writable_paths = vec![workspace_root.to_path_buf(), session_tmp_dir.to_path_buf()];
        let mut denied_write_paths = Vec::new();
        let mut denied_read_paths = Vec::new();
        let mut allowed_read_paths = Vec::new();

        let resolve = |raw: &str| -> PathBuf {
            let p = Path::new(raw);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                workspace_root.join(p)
            }
        };

        if let Some(ref fw) = policy.file_write {
            if let Some(ref allows) = fw.allow {
                for pat in allows {
                    if pat != "*" {
                        writable_paths.push(resolve(pat));
                    }
                }
            }
            if let Some(ref denies) = fw.deny {
                for pat in denies {
                    if pat != "*" {
                        denied_write_paths.push(resolve(pat));
                    }
                }
            }
        }

        if let Some(ref cwd_rules) = policy.command_work_dir {
            if let Some(ref allows) = cwd_rules.allow {
                for pat in allows {
                    if pat != "*" {
                        writable_paths.push(resolve(pat));
                    }
                }
            }
        }

        if let Some(ref fr) = policy.file_read {
            if let Some(ref denies) = fr.deny {
                for pat in denies {
                    if pat != "*" {
                        denied_read_paths.push(resolve(pat));
                    }
                }
            }
            if let Some(ref allows) = fr.allow {
                for pat in allows {
                    if pat != "*" {
                        allowed_read_paths.push(resolve(pat));
                    }
                }
            }
        }

        Self {
            mode,
            workspace_root: workspace_root.to_path_buf(),
            session_tmp_dir: session_tmp_dir.to_path_buf(),
            writable_paths: Mutex::new(writable_paths),
            denied_write_paths: Mutex::new(denied_write_paths),
            denied_read_paths: Mutex::new(denied_read_paths),
            allowed_read_paths: Mutex::new(allowed_read_paths),
        }
    }

    /// Dynamically unlocks a path (and its parent directory if it points to a non-existent file) for writing
    /// after interactive user approval (`AllowOnce` / `AllowSession` / `AllowProject` / `AllowGlobal`).
    pub fn allow_writable_path(&self, path: impl Into<PathBuf>) {
        let p = path.into();
        let mut guard = self.writable_paths.lock().expect("writable_paths poisoned");
        if p.exists() && p.is_dir() {
            guard.push(p);
        } else {
            if let Some(parent) = p.parent() {
                guard.push(parent.to_path_buf());
            }
            guard.push(p);
        }
    }

    /// Dynamically unlocks a path for reading after interactive user approval.
    pub fn allow_readable_path(&self, path: impl Into<PathBuf>) {
        let p = path.into();
        self.allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned")
            .push(p);
    }

    pub async fn execute_command(
        &self,
        work_dir: &Path,
        command: &str,
        timeout_ms: u64,
    ) -> CommandOutput {
        self.execute_command_with_violations(work_dir, command, timeout_ms)
            .await
            .0
    }

    /// Executes the command inside the OS sandbox and, if it fails with non-zero exit code,
    /// returns any detected OS sandbox violations (from fast stderr parsing and macOS kernel logs).
    pub async fn execute_command_with_violations(
        &self,
        work_dir: &Path,
        command: &str,
        timeout_ms: u64,
    ) -> (CommandOutput, Vec<SandboxViolation>) {
        let hermetic_tmp = self.session_tmp_dir.join("tmp");
        let _ = std::fs::create_dir_all(&hermetic_tmp);
        let trace_tag = format!("RAGENTS_SB_{}", Uuid::now_v7().simple());

        let mut child =
            match self.build_sandboxed_command(work_dir, command, &hermetic_tmp, &trace_tag) {
                Ok(cmd) => cmd,
                Err(err) => return (command_error(err), Vec::new()),
            };

        let output = if timeout_ms == 0 {
            child.output().await
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), child.output()).await {
                Ok(result) => result,
                Err(_) => {
                    return (
                        command_error(format!("command timed out after {timeout_ms} ms")),
                        Vec::new(),
                    )
                }
            }
        };

        match output {
            Ok(output) => {
                let exit_code = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
                let mut violations = Vec::new();

                if exit_code != 0 && self.mode != SandboxMode::Disabled {
                    // 1. Fast path: parse `Operation not permitted` paths directly from stderr/stdout
                    let combined = format!("{stderr}\n{stdout}");
                    for raw_path in extract_eperm_paths_from_text(&combined) {
                        let req_dir = normalize_violation_to_request_dir(&raw_path);
                        let cap = CapabilityRequest::FileWrite { path: req_dir };
                        if !violations
                            .iter()
                            .any(|v: &SandboxViolation| v.capability == cap)
                        {
                            violations.push(SandboxViolation {
                                capability: cap,
                                raw_path,
                                operation: "file-write".to_string(),
                            });
                        }
                    }

                    // 2. Kernel fallback path on macOS: if child process swallowed the path in stderr,
                    // query macOS Unified Log by `trace_tag`
                    if violations.is_empty() && cfg!(target_os = "macos") {
                        for (op, raw_path_str) in query_macos_sandbox_violations(&trace_tag).await {
                            let raw_path = PathBuf::from(&raw_path_str);
                            let req_dir = normalize_violation_to_request_dir(&raw_path);
                            let cap = if op.starts_with("file-read") {
                                CapabilityRequest::FileRead { path: req_dir }
                            } else {
                                CapabilityRequest::FileWrite { path: req_dir }
                            };
                            if !violations.iter().any(|v| v.capability == cap) {
                                violations.push(SandboxViolation {
                                    capability: cap,
                                    raw_path,
                                    operation: op,
                                });
                            }
                        }
                    }
                }

                (
                    CommandOutput {
                        exit_code,
                        stdout,
                        stderr,
                        error: None,
                    },
                    violations,
                )
            }
            Err(error) => (command_error(error), Vec::new()),
        }
    }

    fn build_sandboxed_command(
        &self,
        work_dir: &Path,
        command: &str,
        hermetic_tmp: &Path,
        trace_tag: &str,
    ) -> Result<tokio::process::Command, String> {
        let writable = self
            .writable_paths
            .lock()
            .expect("writable_paths poisoned")
            .clone();
        let denied_write = self
            .denied_write_paths
            .lock()
            .expect("denied_write_paths poisoned")
            .clone();
        let denied_read = self
            .denied_read_paths
            .lock()
            .expect("denied_read_paths poisoned")
            .clone();
        let allowed_read = self
            .allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned")
            .clone();

        let mut cmd = if self.mode == SandboxMode::Disabled {
            let mut c = tokio::process::Command::new("sh");
            c.arg("-c").arg(command);
            c
        } else if cfg!(target_os = "macos") && Path::new("/usr/bin/sandbox-exec").exists() {
            let profile = build_macos_seatbelt_profile(
                &writable,
                &denied_write,
                &denied_read,
                &allowed_read,
                trace_tag,
            );
            let mut c = tokio::process::Command::new("/usr/bin/sandbox-exec");
            c.arg("-p").arg(profile).arg("sh").arg("-c").arg(command);
            c
        } else if cfg!(target_os = "linux") && Path::new("/usr/bin/bwrap").exists() {
            build_linux_bwrap_command(work_dir, command, &writable, &denied_write, &denied_read)
        } else if self.mode == SandboxMode::Enforce {
            return Err(
                "OS kernel sandbox is set to Enforce, but neither macOS sandbox-exec nor Linux bwrap is available"
                    .to_string(),
            );
        } else {
            let mut c = tokio::process::Command::new("sh");
            c.arg("-c").arg(command);
            c
        };

        cmd.current_dir(work_dir)
            .env("TMPDIR", hermetic_tmp)
            .env("TMP", hermetic_tmp)
            .env("TEMP", hermetic_tmp)
            .env_remove("RAGENTS_API_KEY")
            .kill_on_drop(true);

        Ok(cmd)
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
}

/// Normalizes a specific violated file path (such as `/Users/alice/.cache/uv/sdists-v6/.git`)
/// to its logical parent directory scope (such as `/Users/alice/.cache/uv`) so that a single
/// user approval unlocks the tool's working directory rather[], prompting for every internal lockfile.
pub fn normalize_violation_to_request_dir(raw_path: &Path) -> PathBuf {
    let comps: Vec<Component<'_>> = raw_path.components().collect();

    // 1. Check well-known multi-subdirectory cache/data roots:
    //    `.../.cache/<tool>/...` -> `.../.cache/<tool>`
    //    `.../Library/Caches/<tool>/...` -> `.../Library/Caches/<tool>`
    //    `.../.local/share/<tool>/...` -> `.../.local/share/<tool>`
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

    // 2. Otherwise, if it's already an existing directory, return it; if it's a file, return its parent directory
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
fn extract_eperm_paths_from_text(text: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for line in text.lines() {
        if !line.contains("Operation not permitted") && !line.contains("os error 1") {
            continue;
        }
        // Extract absolute path tokens delimited by backticks, single/double quotes, or colons/whitespace
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

/// Generates a macOS Seatbelt (`sandbox-exec`) SBPL profile that restricts file writes to
/// `writable_paths` while enforcing `denied_write_paths` and `denied_read_paths`.
pub fn build_macos_seatbelt_profile(
    writable_paths: &[PathBuf],
    denied_write_paths: &[PathBuf],
    denied_read_paths: &[PathBuf],
    allowed_read_paths: &[PathBuf],
    trace_tag: &str,
) -> String {
    let tag = escape_sbpl(trace_tag);
    let mut profile = format!(
        "(version 1)\n\
         (allow default)\n\
         (deny file-write* (with message \"{tag}\"))\n\
         (allow file-write*\n\
           (literal \"/dev/null\")\n\
           (literal \"/dev/zero\")\n\
           (literal \"/dev/random\")\n\
           (literal \"/dev/urandom\")\n\
           (literal \"/dev/tty\")\n\
           (literal \"/dev/dtracehelper\")\n\
           (subpath \"/dev/fd\")\n"
    );

    for p in expand_canonical_paths(writable_paths) {
        profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
    }
    profile.push_str(")\n");

    let expanded_denied_write = expand_canonical_paths(denied_write_paths);
    if !expanded_denied_write.is_empty() {
        profile.push_str(&format!("(deny file-write* (with message \"{tag}\")\n"));
        for p in expanded_denied_write {
            profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
        }
        profile.push_str(")\n");
    }

    let expanded_denied_read = expand_canonical_paths(denied_read_paths);
    if !expanded_denied_read.is_empty() {
        profile.push_str(&format!("(deny file-read-data (with message \"{tag}\")\n"));
        for p in expanded_denied_read {
            profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
        }
        profile.push_str(")\n");
    }

    let expanded_allowed_read = expand_canonical_paths(allowed_read_paths);
    if !expanded_allowed_read.is_empty() {
        profile.push_str("(allow file-read-data\n");
        for p in expanded_allowed_read {
            profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
        }
        profile.push_str(")\n");
    }

    profile
}

/// Queries the macOS Unified Log for kernel sandbox violations tagged with `trace_tag`,
/// extracting `(operation, denied_path)` even when the child process swallowed the path in stderr.
async fn query_macos_sandbox_violations(trace_tag: &str) -> Vec<(String, String)> {
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

fn build_linux_bwrap_command(
    work_dir: &Path,
    command: &str,
    writable_paths: &[PathBuf],
    denied_write_paths: &[PathBuf],
    denied_read_paths: &[PathBuf],
) -> tokio::process::Command {
    let mut c = tokio::process::Command::new("/usr/bin/bwrap");
    c.arg("--ro-bind")
        .arg("/")
        .arg("/")
        .arg("--dev")
        .arg("/dev")
        .arg("--proc")
        .arg("/proc");

    for p in expand_canonical_paths(writable_paths) {
        if Path::new(&p).exists() {
            c.arg("--bind").arg(&p).arg(&p);
        }
    }
    for p in expand_canonical_paths(denied_write_paths) {
        if Path::new(&p).exists() {
            c.arg("--ro-bind").arg(&p).arg(&p);
        }
    }
    for p in expand_canonical_paths(denied_read_paths) {
        if Path::new(&p).exists() && Path::new(&p).is_dir() {
            c.arg("--tmpfs").arg(&p);
        }
    }

    c.arg("--chdir")
        .arg(work_dir)
        .arg("sh")
        .arg("-c")
        .arg(command);
    c
}

/// Expands each path into both its lexical form and its symlink-resolved canonical form
/// (even if the leaf path does not exist yet, by canonicalizing the nearest existing ancestor).
/// Crucial on macOS where `/tmp` -> `/private/tmp` and `/var` -> `/private/var`.
fn expand_canonical_paths(paths: &[PathBuf]) -> BTreeSet<String> {
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

fn canonicalize_with_nonexistent_tail(path: &Path) -> Option<PathBuf> {
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

fn escape_sbpl(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('"', "\\\"")
}

fn command_error(error: impl ToString) -> CommandOutput {
    CommandOutput {
        exit_code: -1,
        stdout: String::new(),
        stderr: String::new(),
        error: Some(error.to_string()),
    }
}
