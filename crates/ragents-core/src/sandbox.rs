use crate::hooks::runtime::ragent::extension::host::CommandOutput;
use crate::host::CoreRunSpec;
use ragents_protocol::PermissionPolicyLayer;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

/// OS Kernel Sandbox enforcement mode for external commands executed by WASM extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// Automatically enforce OS kernel sandbox (`sandbox-exec` on macOS, `bwrap` on Linux) when available.
    #[default]
    Auto,
    /// Strictly require OS kernel sandbox; fail command execution if the OS sandbox binary is unavailable.
    Enforce,
    /// Disable OS kernel sandbox (still applies hermetic `TMPDIR` / `XDG_CACHE_HOME` environment isolation).
    Disabled,
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
        let hermetic_tmp = self.session_tmp_dir.join("tmp");
        let _ = std::fs::create_dir_all(&hermetic_tmp);

        let mut child = match self.build_sandboxed_command(work_dir, command, &hermetic_tmp) {
            Ok(cmd) => cmd,
            Err(err) => return command_error(err),
        };

        let output = if timeout_ms == 0 {
            child.output().await
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), child.output()).await {
                Ok(result) => result,
                Err(_) => return command_error(format!("command timed out after {timeout_ms} ms")),
            }
        };

        match output {
            Ok(output) => {
                let exit_code = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
                if exit_code != 0
                    && (stderr.contains("Operation not permitted")
                        || stdout.contains("Operation not permitted"))
                {
                    stderr.push_str(
                        "\n[RAGENTS_SANDBOX_HINT] Command failed with 'Operation not permitted' because the OS kernel sandbox blocks writing to directories outside workspace_root by default. If this command needs write access to an external directory (e.g. a package manager cache root like ~/.cache/uv), call the `request_permission` tool with capability='file_write' and the top-level directory path (e.g. '/Users/.../.cache/uv'), wait for user approval, and then retry the command.",
                    );
                }
                CommandOutput {
                    exit_code,
                    stdout,
                    stderr,
                    error: None,
                }
            }
            Err(error) => command_error(error),
        }
    }

    fn build_sandboxed_command(
        &self,
        work_dir: &Path,
        command: &str,
        hermetic_tmp: &Path,
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
            let profile =
                build_macos_seatbelt_profile(&writable, &denied_write, &denied_read, &allowed_read);
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

/// Generates a macOS Seatbelt (`sandbox-exec`) SBPL profile that restricts file writes to
/// `writable_paths` while enforcing `denied_write_paths` and `denied_read_paths`.
pub fn build_macos_seatbelt_profile(
    writable_paths: &[PathBuf],
    denied_write_paths: &[PathBuf],
    denied_read_paths: &[PathBuf],
    allowed_read_paths: &[PathBuf],
) -> String {
    let mut profile = String::from(
        "(version 1)\n\
         (allow default)\n\
         (deny file-write*)\n\
         (allow file-write*\n\
           (literal \"/dev/null\")\n\
           (literal \"/dev/zero\")\n\
           (literal \"/dev/random\")\n\
           (literal \"/dev/urandom\")\n\
           (literal \"/dev/tty\")\n\
           (subpath \"/dev/fd\")\n",
    );

    for p in expand_canonical_paths(writable_paths) {
        profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
    }
    profile.push_str(")\n");

    let expanded_denied_write = expand_canonical_paths(denied_write_paths);
    if !expanded_denied_write.is_empty() {
        profile.push_str("(deny file-write*\n");
        for p in expanded_denied_write {
            profile.push_str(&format!("  (subpath \"{}\")\n", escape_sbpl(&p)));
        }
        profile.push_str(")\n");
    }

    let expanded_denied_read = expand_canonical_paths(denied_read_paths);
    if !expanded_denied_read.is_empty() {
        profile.push_str("(deny file-read-data\n");
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
        use std::path::Component;
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
