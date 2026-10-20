mod violation;

pub use violation::{canonicalize_or_normalize, normalize_violation_to_request_dir};
pub(crate) use violation::{
    closest_existing_dir, escape_sbpl, expand_canonical_paths, extract_eperm_paths_from_text,
    query_macos_sandbox_violations,
};

use crate::host::CoreRunSpec;
use ragents_protocol::{CapabilityRequest, DefaultPermissionDecision, PermissionPolicyLayer};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub error: Option<String>,
}

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
    restrict_reads: bool,
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

        let mut restrict_reads = false;
        if let Some(ref fr) = policy.file_read {
            if let Some(ref denies) = fr.deny {
                for pat in denies {
                    if pat != "*" {
                        denied_read_paths.push(resolve(pat));
                    }
                }
            }
            let has_wildcard = fr
                .allow
                .as_ref()
                .map(|a| a.iter().any(|p| p == "*"))
                .unwrap_or(false);
            if let Some(ref allows) = fr.allow {
                for pat in allows {
                    if pat != "*" {
                        allowed_read_paths.push(resolve(pat));
                    }
                }
            }
            if !has_wildcard
                && matches!(
                    fr.default,
                    Some(DefaultPermissionDecision::Ask | DefaultPermissionDecision::Deny)
                )
            {
                restrict_reads = true;
            }
        }

        Self {
            mode,
            workspace_root: workspace_root.to_path_buf(),
            session_tmp_dir: session_tmp_dir.to_path_buf(),
            restrict_reads,
            writable_paths: Mutex::new(writable_paths),
            denied_write_paths: Mutex::new(denied_write_paths),
            denied_read_paths: Mutex::new(denied_read_paths),
            allowed_read_paths: Mutex::new(allowed_read_paths),
        }
    }

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

    pub fn allow_readable_path(&self, path: impl Into<PathBuf>) {
        let p = path.into();
        self.allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned")
            .push(p);
    }

    pub fn revoke_temporary_path(&self, path: &Path) {
        let mut w_guard = self.writable_paths.lock().expect("writable_paths poisoned");
        w_guard.retain(|p| p != path);
        if !path.is_dir() {
            if let Some(parent) = path.parent() {
                if parent != self.workspace_root && parent != self.session_tmp_dir {
                    w_guard.retain(|p| p != parent);
                }
            }
        }
        drop(w_guard);

        let mut r_guard = self
            .allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned");
        r_guard.retain(|p| p != path);
    }

    pub fn session_tmp_dir(&self) -> &Path {
        &self.session_tmp_dir
    }

    pub fn is_read_denied(&self, path: &Path) -> bool {
        let norm_target = canonicalize_or_normalize(path);
        let allowed = self
            .allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned");
        if allowed
            .iter()
            .any(|p| path.starts_with(p) || norm_target.starts_with(p))
        {
            return false;
        }
        let denied = self
            .denied_read_paths
            .lock()
            .expect("denied_read_paths poisoned");
        denied
            .iter()
            .any(|p| path.starts_with(p) || norm_target.starts_with(p))
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

    pub async fn execute_command_with_violations(
        &self,
        work_dir: &Path,
        command: &str,
        timeout_ms: u64,
    ) -> (CommandOutput, Vec<SandboxViolation>) {
        let hermetic_tmp = self.session_tmp_dir.join("tmp");
        let _ = std::fs::create_dir_all(&hermetic_tmp);
        let trace_tag = format!("RAGENTS_SB_{}", Uuid::now_v7().simple());

        let mut cmd =
            match self.build_sandboxed_command(work_dir, command, &hermetic_tmp, &trace_tag) {
                Ok(cmd) => cmd,
                Err(err) => return (command_error(err), Vec::new()),
            };

        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        #[cfg(unix)]
        {
            cmd.process_group(0);
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(error) => return (command_error(error), Vec::new()),
        };

        let os_pid = child.id();
        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();

        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut out) = stdout_pipe {
                use tokio::io::AsyncReadExt;
                let _ = out.read_to_end(&mut buf).await;
            }
            String::from_utf8_lossy(&buf).into_owned()
        });

        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut err) = stderr_pipe {
                use tokio::io::AsyncReadExt;
                let _ = err.read_to_end(&mut buf).await;
            }
            String::from_utf8_lossy(&buf).into_owned()
        });

        let mut timed_out = false;
        let wait_res = if timeout_ms == 0 {
            Some(child.wait().await)
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await {
                Ok(res) => Some(res),
                Err(_) => {
                    timed_out = true;
                    if let Some(pid) = os_pid {
                        crate::process::kill_process_group(pid).await;
                    }
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    None
                }
            }
        };

        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();

        if timed_out {
            let secs = if timeout_ms.is_multiple_of(1000) {
                format!("{}", timeout_ms / 1000)
            } else {
                format!("{:.2}", timeout_ms as f64 / 1000.0)
            };
            return (
                CommandOutput {
                    exit_code: -1,
                    stdout,
                    stderr,
                    error: Some(format!("Command timed out after {secs} seconds")),
                },
                Vec::new(),
            );
        }

        match wait_res.expect("wait_res present when not timed_out") {
            Ok(status) => {
                let exit_code = status.code().unwrap_or(-1);
                let mut violations = Vec::new();

                if exit_code != 0 && self.mode != SandboxMode::Disabled {
                    let combined = format!("{stderr}\n{stdout}");
                    let extracted = extract_eperm_paths_from_text(&combined);
                    let has_unreadable = extracted
                        .iter()
                        .any(|p| !self.is_path_readable(p) && p.exists());
                    for raw_path in extracted {
                        let is_read_violation =
                            !self.is_path_readable(&raw_path) && raw_path.exists();
                        if has_unreadable && !is_read_violation && raw_path.exists() {
                            continue;
                        }
                        let req_dir = normalize_violation_to_request_dir(&raw_path);
                        let (cap, op) = if is_read_violation {
                            (
                                CapabilityRequest::FileRead { path: req_dir },
                                "file-read-data".to_string(),
                            )
                        } else {
                            (
                                CapabilityRequest::FileWrite { path: req_dir },
                                "file-write".to_string(),
                            )
                        };
                        if !violations
                            .iter()
                            .any(|v: &SandboxViolation| v.capability == cap)
                        {
                            violations.push(SandboxViolation {
                                capability: cap,
                                raw_path,
                                operation: op,
                            });
                        }
                    }

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
                self.restrict_reads,
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

    pub fn mode(&self) -> SandboxMode {
        self.mode
    }

    pub fn build_sandboxed_command_for_process(
        &self,
        work_dir: &Path,
        command: &str,
        trace_tag: &str,
    ) -> Result<tokio::process::Command, String> {
        let hermetic_tmp = self.session_tmp_dir.join("tmp");
        let _ = std::fs::create_dir_all(&hermetic_tmp);
        self.build_sandboxed_command(work_dir, command, &hermetic_tmp, trace_tag)
    }

    pub fn is_path_writable(&self, target: &Path) -> bool {
        let norm_target = canonicalize_or_normalize(target);
        let denied = self
            .denied_write_paths
            .lock()
            .expect("denied_write_paths poisoned")
            .clone();
        for d in &denied {
            let norm_d = canonicalize_or_normalize(d);
            if norm_target.starts_with(&norm_d) {
                return false;
            }
        }
        let writable = self
            .writable_paths
            .lock()
            .expect("writable_paths poisoned")
            .clone();
        for w in &writable {
            let norm_w = canonicalize_or_normalize(w);
            if norm_target.starts_with(&norm_w) {
                return true;
            }
        }
        false
    }

    pub fn is_path_readable(&self, target: &Path) -> bool {
        let norm_target = canonicalize_or_normalize(target);
        let allowed_read = self
            .allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned")
            .clone();
        for a in &allowed_read {
            let norm_a = canonicalize_or_normalize(a);
            if norm_target.starts_with(&norm_a) {
                return true;
            }
        }
        let denied_read = self
            .denied_read_paths
            .lock()
            .expect("denied_read_paths poisoned")
            .clone();
        for d in &denied_read {
            let norm_d = canonicalize_or_normalize(d);
            if norm_target.starts_with(&norm_d) {
                return false;
            }
        }
        let writable = self
            .writable_paths
            .lock()
            .expect("writable_paths poisoned")
            .clone();
        for w in &writable {
            let norm_w = canonicalize_or_normalize(w);
            if norm_target.starts_with(&norm_w) {
                return true;
            }
        }
        false
    }

    pub fn wasi_preopen_dirs(&self) -> (Vec<PathBuf>, Vec<PathBuf>) {
        let writable = self
            .writable_paths
            .lock()
            .expect("writable_paths poisoned")
            .clone();
        let allowed_read = self
            .allowed_read_paths
            .lock()
            .expect("allowed_read_paths poisoned")
            .clone();

        let mut w_dirs = Vec::new();
        for p in writable {
            if let Some(dir) = closest_existing_dir(&p) {
                if !w_dirs.contains(&dir) {
                    w_dirs.push(dir);
                }
            }
        }
        w_dirs.sort();

        let mut r_dirs = Vec::new();
        for p in allowed_read {
            if let Some(dir) = closest_existing_dir(&p) {
                if !w_dirs.contains(&dir) && !r_dirs.contains(&dir) {
                    r_dirs.push(dir);
                }
            }
        }
        r_dirs.sort();

        (w_dirs, r_dirs)
    }
}

pub fn build_macos_seatbelt_profile(
    writable_paths: &[PathBuf],
    denied_write_paths: &[PathBuf],
    denied_read_paths: &[PathBuf],
    allowed_read_paths: &[PathBuf],
    restrict_reads: bool,
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

    if restrict_reads {
        profile.push_str(&format!(
            "(deny file-read-data (with message \"{tag}\")\n\
               (subpath \"/Users\")\n\
               (subpath \"/Volumes\")\n\
               (subpath \"/private/tmp\")\n\
               (subpath \"/tmp\")\n\
             )\n"
        ));
        let mut readable_all = writable_paths.to_vec();
        readable_all.extend_from_slice(allowed_read_paths);
        let expanded_readable = expand_canonical_paths(&readable_all);
        if !expanded_readable.is_empty() {
            profile.push_str("(allow file-read-data\n");
            for p in expanded_readable {
                profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
            }
            profile.push_str(")\n");
        }
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

fn command_error(error: impl ToString) -> CommandOutput {
    CommandOutput {
        exit_code: -1,
        stdout: String::new(),
        stderr: String::new(),
        error: Some(error.to_string()),
    }
}
