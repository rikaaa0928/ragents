use crate::host::{CoreHost, PermissionOutcome};
use crate::sandbox::{SandboxState, SandboxViolation};
use ragents_protocol::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

pub(crate) const MAX_LOG_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const EARLY_SANDBOX_CHECK_WINDOW_MS: u64 = 150;
pub(crate) const MAX_SANDBOX_ESCALATION_ROUNDS: usize = 16;
pub(crate) const DEFAULT_SHELL_MAX_LINES: usize = 2000;
pub(crate) const DEFAULT_SHELL_MAX_BYTES: usize = 50 * 1024; // 50KB

pub(crate) struct ManagedProcessEntry {
    pub(crate) info: ManagedProcessInfo,
    pub(crate) stdout_buf: String,
    pub(crate) stderr_buf: String,
    pub(crate) combined_buf: String,
    /// Set to `true` when stopped explicitly via `Stop` or lifecycle cleanup so we don't emit
    /// redundant unsolicited exit notifications during teardown.
    pub(crate) suppressed_notification: bool,
    pub(crate) stop_notify: Arc<Notify>,
}

impl ManagedProcessEntry {
    pub(crate) fn new(info: ManagedProcessInfo, stop_notify: Arc<Notify>) -> Self {
        Self {
            info,
            stdout_buf: String::new(),
            stderr_buf: String::new(),
            combined_buf: String::new(),
            suppressed_notification: false,
            stop_notify,
        }
    }

    pub(crate) fn append_stdout(&mut self, line: &str) {
        append_capped(&mut self.stdout_buf, line);
        append_capped(&mut self.combined_buf, line);
    }

    pub(crate) fn append_stderr(&mut self, line: &str) {
        append_capped(&mut self.stderr_buf, line);
        append_capped(&mut self.combined_buf, line);
    }
}

fn append_capped(buf: &mut String, line: &str) {
    buf.push_str(line);
    buf.push('\n');
    if buf.len() > MAX_LOG_BUFFER_BYTES {
        let drain_to = buf.len() - (MAX_LOG_BUFFER_BYTES * 3 / 4);
        let safe_idx = buf
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| i >= drain_to)
            .unwrap_or(0);
        buf.drain(..safe_idx);
    }
}

pub(crate) async fn request_sandbox_violation_approval(
    host: &Arc<dyn CoreHost>,
    plugin_name: &str,
    command: &str,
    v: &SandboxViolation,
    sandbox_state: &Arc<SandboxState>,
    unlocked_paths: &mut Vec<PathBuf>,
    temporary_once_paths: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let reason = format!(
        "Command `{}` requires {} access to '{}' (suggested directory: '{}')",
        command,
        v.operation,
        v.raw_path.display(),
        v.capability
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    );
    match host
        .check_permission_with_original_path(
            Some(plugin_name),
            v.capability.clone(),
            Some(v.raw_path.clone()),
            Some(reason),
        )
        .await
    {
        Ok(PermissionOutcome::Allowed {
            effective_capability,
            persist_in_activation,
        }) => {
            let granted_cap = effective_capability.unwrap_or_else(|| v.capability.clone());
            if let Some(p) = granted_cap.path() {
                unlocked_paths.push(p.to_path_buf());
                if !persist_in_activation {
                    temporary_once_paths.push(p.to_path_buf());
                }
            }
            unlocked_paths.push(v.raw_path.clone());
            match granted_cap {
                CapabilityRequest::FileWrite { path } => {
                    sandbox_state.allow_writable_path(path);
                }
                CapabilityRequest::CommandWorkDir { work_dir } => {
                    sandbox_state.allow_writable_path(work_dir);
                }
                CapabilityRequest::FileRead { path } => {
                    sandbox_state.allow_readable_path(path);
                }
                CapabilityRequest::Http { .. } => {}
            }
            Ok(())
        }
        Ok(PermissionOutcome::Denied { reason }) => Err(format!(
            "[RAGENTS_SANDBOX_DENIED] Permission denied for {:?}: {reason}",
            v.capability
        )),
        Err(err) => Err(format!(
            "[RAGENTS_SANDBOX_ERROR] Permission request failed: {err}"
        )),
    }
}

pub(crate) fn format_exit_notification(info: &ManagedProcessInfo) -> String {
    format!(
        "[System Notification: Background Process Exited]\n- process_id: {}\n- pid: {}\n- command: {}\n- phase: {:?}\n- exit_code: {}\n- duration_ms: {}\nHint: Use `process_logs` with `process_id: \"{}\"` and `stream: \"combined\"` (or \"stdout\" / \"stderr\") to inspect its output if needed.",
        info.process_id,
        info.pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "unknown".into()),
        info.command,
        info.phase,
        info.exit_code
            .map(|c| c.to_string())
            .unwrap_or_else(|| "none".into()),
        info.duration_ms,
        info.process_id
    )
}

pub(crate) async fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    {
        let neg_pgid = format!("-{pid}");
        let _ = tokio::process::Command::new("kill")
            .arg("-TERM")
            .arg(&neg_pgid)
            .output()
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tokio::process::Command::new("kill")
            .arg("-KILL")
            .arg(&neg_pgid)
            .output()
            .await;
    }
}

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn split_lines_for_counting(content: &str) -> Vec<&str> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&str> = content.split('\n').collect();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}

fn truncate_str_to_bytes_from_end(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut start = s.len() - max_bytes;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

pub(crate) fn truncate_tail_stream(
    content: &str,
    stream_label: &str,
    session_tmp_dir: &std::path::Path,
) -> String {
    let total_bytes = content.len();
    let lines = split_lines_for_counting(content);
    let total_lines = lines.len();

    if total_lines <= DEFAULT_SHELL_MAX_LINES && total_bytes <= DEFAULT_SHELL_MAX_BYTES {
        return content.to_string();
    }

    let tmp_dir = session_tmp_dir.join("tmp");
    let _ = std::fs::create_dir_all(&tmp_dir);
    let spill_path = tmp_dir.join(format!(
        "shell-{}-{}.log",
        stream_label,
        uuid::Uuid::now_v7().simple()
    ));
    let _ = std::fs::write(&spill_path, content);
    let full_output_path = spill_path.display().to_string();

    let mut output_lines_rev: Vec<&str> = Vec::new();
    let mut output_bytes_count = 0usize;
    let mut truncated_by_lines = false;
    let mut last_line_partial = false;

    for &line in lines.iter().rev() {
        if output_lines_rev.len() >= DEFAULT_SHELL_MAX_LINES {
            truncated_by_lines = true;
            break;
        }
        let line_bytes = line.len() + usize::from(!output_lines_rev.is_empty());
        if output_bytes_count + line_bytes > DEFAULT_SHELL_MAX_BYTES {
            truncated_by_lines = false;
            if output_lines_rev.is_empty() {
                let partial = truncate_str_to_bytes_from_end(line, DEFAULT_SHELL_MAX_BYTES);
                output_lines_rev.push(partial);
                output_bytes_count = partial.len();
                last_line_partial = true;
            }
            break;
        }
        output_lines_rev.push(line);
        output_bytes_count += line_bytes;
    }

    if output_lines_rev.len() >= DEFAULT_SHELL_MAX_LINES
        && output_bytes_count <= DEFAULT_SHELL_MAX_BYTES
    {
        truncated_by_lines = true;
    }

    output_lines_rev.reverse();
    let output_lines_count = output_lines_rev.len();
    let mut text = output_lines_rev.join("\n");
    let start_line = total_lines.saturating_sub(output_lines_count) + 1;
    let end_line = total_lines;

    if last_line_partial {
        let last_line_size = format_size(lines.last().map(|l| l.len()).unwrap_or(0));
        let shown_size = format_size(output_bytes_count);
        text.push_str(&format!(
            "\n\n[Showing last {shown_size} of line {end_line} (line is {last_line_size}). Full output: {full_output_path}]"
        ));
    } else if truncated_by_lines {
        text.push_str(&format!(
            "\n\n[Showing lines {start_line}-{end_line} of {total_lines}. Full output: {full_output_path}]"
        ));
    } else {
        let limit_str = format_size(DEFAULT_SHELL_MAX_BYTES);
        text.push_str(&format!(
            "\n\n[Showing lines {start_line}-{end_line} of {total_lines} ({limit_str} limit). Full output: {full_output_path}]"
        ));
    }

    text
}

pub(crate) fn apply_synchronous_output_truncation(
    resp: &mut ProcessActionResponse,
    session_tmp_dir: &std::path::Path,
) {
    if let Some(ref stdout) = resp.stdout {
        resp.stdout = Some(truncate_tail_stream(stdout, "stdout", session_tmp_dir));
    }
    if let Some(ref stderr) = resp.stderr {
        resp.stderr = Some(truncate_tail_stream(stderr, "stderr", session_tmp_dir));
    }
}
