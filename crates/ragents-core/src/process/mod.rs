mod entry;

pub(crate) use entry::kill_process_group;

use crate::error::CoreError;
use crate::host::{CoreHost, PermissionOutcome};
use crate::sandbox::{
    extract_eperm_paths_from_text, is_setuid_or_setgid_binary, normalize_violation_to_request_dir,
    query_macos_sandbox_violations, SandboxMode, SandboxState, SandboxViolation,
};
use async_trait::async_trait;
use chrono::Utc;
use entry::{
    format_exit_notification, request_sandbox_violation_approval, ManagedProcessEntry,
    EARLY_SANDBOX_CHECK_WINDOW_MS, MAX_SANDBOX_ESCALATION_ROUNDS,
};
use openresponses_rust::MessageContent;
use ragents_protocol::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Notify;
use uuid::Uuid;

#[async_trait]
pub trait ProcessNotificationSink: Send + Sync {
    async fn on_process_started(&self, process: ManagedProcessInfo);
    async fn on_process_exited_naturally(
        &self,
        process: ManagedProcessInfo,
        realtime_notification: MessageContent,
    );
}

#[derive(Clone, Default)]
pub struct ProcessManager {
    entries: Arc<Mutex<HashMap<String, Arc<Mutex<ManagedProcessEntry>>>>>,
    sink: Option<Arc<dyn ProcessNotificationSink>>,
}

impl ProcessManager {
    pub fn new(sink: Option<Arc<dyn ProcessNotificationSink>>) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            sink,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_action(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        plugin_name: &str,
        workspace_root: &Path,
        sandbox_state: &Arc<SandboxState>,
        core_host: Option<&Arc<dyn CoreHost>>,
        request: ProcessActionRequest,
    ) -> Result<ProcessActionResponse, CoreError> {
        match request {
            ProcessActionRequest::Start {
                command,
                wait,
                lifecycle,
                timeout_ms,
            } => {
                if let Some(host) = core_host {
                    let cap = CapabilityRequest::CommandWorkDir {
                        work_dir: workspace_root.to_path_buf(),
                    };
                    let reason =
                        format!("Extension '{plugin_name}' executing shell command: {command}");
                    match host
                        .check_permission(Some(plugin_name), cap, Some(reason))
                        .await
                    {
                        Ok(PermissionOutcome::Allowed { .. }) => {}
                        Ok(PermissionOutcome::Denied { reason }) => {
                            return Ok(ProcessActionResponse {
                                ok: false,
                                exit_code: Some(-1),
                                error: Some(format!("Permission denied: {reason}")),
                                ..Default::default()
                            });
                        }
                        Err(err) => {
                            return Ok(ProcessActionResponse {
                                ok: false,
                                exit_code: Some(-1),
                                error: Some(format!("Permission check error: {err}")),
                                ..Default::default()
                            });
                        }
                    }
                }

                if wait {
                    self.run_synchronous_command(
                        plugin_name,
                        workspace_root,
                        sandbox_state,
                        core_host,
                        &command,
                        timeout_ms,
                    )
                    .await
                } else {
                    self.start_background_command(
                        session_id,
                        activation_id,
                        plugin_name,
                        workspace_root,
                        sandbox_state,
                        core_host,
                        command,
                        lifecycle,
                        timeout_ms,
                    )
                    .await
                }
            }
            ProcessActionRequest::Status { process_id } => {
                self.query_status(session_id, process_id.as_deref())
            }
            ProcessActionRequest::Logs {
                process_id,
                stream,
                tail_lines,
                offset,
            } => self.query_logs(session_id, &process_id, stream, tail_lines, offset),
            ProcessActionRequest::Stop { process_id } => {
                self.stop_process(session_id, &process_id).await
            }
        }
    }

    async fn run_synchronous_command(
        &self,
        plugin_name: &str,
        workspace_root: &Path,
        sandbox_state: &Arc<SandboxState>,
        core_host: Option<&Arc<dyn CoreHost>>,
        command: &str,
        timeout_ms: u64,
    ) -> Result<ProcessActionResponse, CoreError> {
        let mut unlocked_paths: Vec<PathBuf> = Vec::new();
        let mut temporary_once_paths: Vec<PathBuf> = Vec::new();

        let mut final_resp = ProcessActionResponse {
            ok: false,
            exit_code: Some(-1),
            error: Some(
                "sandbox multi-directory permission escalation loop exceeded maximum rounds"
                    .to_string(),
            ),
            ..Default::default()
        };

        for _round in 0..MAX_SANDBOX_ESCALATION_ROUNDS {
            let (mut output, violations) = sandbox_state
                .execute_command_with_violations(workspace_root, command, timeout_ms)
                .await;

            if output.exit_code == 0 || violations.is_empty() {
                final_resp = ProcessActionResponse {
                    ok: output.error.is_none(),
                    exit_code: Some(output.exit_code),
                    stdout: Some(output.stdout),
                    stderr: Some(output.stderr),
                    error: output.error,
                    ..Default::default()
                };
                break;
            }

            let Some(host) = core_host else {
                final_resp = ProcessActionResponse {
                    ok: output.error.is_none(),
                    exit_code: Some(output.exit_code),
                    stdout: Some(output.stdout),
                    stderr: Some(output.stderr),
                    error: output.error,
                    ..Default::default()
                };
                break;
            };

            let new_violations: Vec<_> = violations
                .into_iter()
                .filter(|v| {
                    if is_setuid_or_setgid_binary(&v.raw_path) {
                        !unlocked_paths.iter().any(|u| u == &v.raw_path)
                    } else {
                        !unlocked_paths.iter().any(|u| v.raw_path.starts_with(u))
                    }
                })
                .collect();

            if new_violations.is_empty() {
                final_resp = ProcessActionResponse {
                    ok: output.error.is_none(),
                    exit_code: Some(output.exit_code),
                    stdout: Some(output.stdout),
                    stderr: Some(output.stderr),
                    error: output.error,
                    ..Default::default()
                };
                break;
            }

            let mut all_approved = true;
            for v in new_violations {
                if let Err(denied_msg) = request_sandbox_violation_approval(
                    host,
                    plugin_name,
                    command,
                    &v,
                    sandbox_state,
                    &mut unlocked_paths,
                    &mut temporary_once_paths,
                )
                .await
                {
                    output.stderr.push_str(&format!("\n{denied_msg}"));
                    all_approved = false;
                    break;
                }
            }

            if !all_approved {
                final_resp = ProcessActionResponse {
                    ok: output.error.is_none(),
                    exit_code: Some(output.exit_code),
                    stdout: Some(output.stdout),
                    stderr: Some(output.stderr),
                    error: output.error,
                    ..Default::default()
                };
                break;
            }
        }

        for tmp_path in temporary_once_paths {
            sandbox_state.revoke_temporary_path(&tmp_path);
        }

        Ok(final_resp)
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_background_command(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        plugin_name: &str,
        workspace_root: &Path,
        sandbox_state: &Arc<SandboxState>,
        core_host: Option<&Arc<dyn CoreHost>>,
        command: String,
        lifecycle: ProcessLifecycle,
        timeout_ms: u64,
    ) -> Result<ProcessActionResponse, CoreError> {
        let mut unlocked_paths: Vec<PathBuf> = Vec::new();
        let mut temporary_once_paths: Vec<PathBuf> = Vec::new();

        for _round in 0..MAX_SANDBOX_ESCALATION_ROUNDS {
            let trace_tag = format!("RAGENTS_BG_{}", Uuid::now_v7().simple());
            let mut cmd = match sandbox_state.build_sandboxed_command_for_process(
                workspace_root,
                &command,
                &trace_tag,
            ) {
                Ok(c) => c,
                Err(err) => {
                    return Ok(ProcessActionResponse {
                        ok: false,
                        error: Some(err),
                        ..Default::default()
                    })
                }
            };

            cmd.stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(false);

            #[cfg(unix)]
            {
                cmd.process_group(0);
            }

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(err) => {
                    return Ok(ProcessActionResponse {
                        ok: false,
                        error: Some(format!("failed to spawn background process: {err}")),
                        ..Default::default()
                    })
                }
            };

            let os_pid = child.id();
            let proc_id = ProcessId::generate();
            let started_at = Utc::now();
            let stop_notify = Arc::new(Notify::new());

            let info = ManagedProcessInfo {
                process_id: proc_id.clone(),
                pid: os_pid,
                session_id: session_id.clone(),
                activation_id: activation_id.clone(),
                command: command.clone(),
                lifecycle,
                phase: ManagedProcessPhase::Running,
                exit_code: None,
                started_at,
                finished_at: None,
                duration_ms: 0,
            };

            let entry = Arc::new(Mutex::new(ManagedProcessEntry::new(
                info.clone(),
                stop_notify.clone(),
            )));

            let stdout_pipe = child.stdout.take();
            let stderr_pipe = child.stderr.take();

            let entry_out = entry.clone();
            let out_reader = tokio::spawn(async move {
                if let Some(out) = stdout_pipe {
                    let mut lines = BufReader::new(out).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        entry_out
                            .lock()
                            .expect("process entry poisoned")
                            .append_stdout(&line);
                    }
                }
            });

            let entry_err = entry.clone();
            let err_reader = tokio::spawn(async move {
                if let Some(err) = stderr_pipe {
                    let mut lines = BufReader::new(err).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        entry_err
                            .lock()
                            .expect("process entry poisoned")
                            .append_stderr(&line);
                    }
                }
            });

            tokio::time::sleep(Duration::from_millis(EARLY_SANDBOX_CHECK_WINDOW_MS)).await;

            if let Ok(Some(status)) = child.try_wait() {
                let _ = out_reader.await;
                let _ = err_reader.await;
                let exit_code = status.code().unwrap_or(-1);

                if exit_code != 0 && sandbox_state.mode() != SandboxMode::Disabled {
                    let combined = entry
                        .lock()
                        .expect("process entry poisoned")
                        .combined_buf
                        .clone();
                    let mut violations = Vec::new();
                    let extracted = extract_eperm_paths_from_text(&combined);
                    let has_unreadable = extracted
                        .iter()
                        .any(|p| !sandbox_state.is_path_readable(p) && p.exists());
                    for raw_path in extracted {
                        let is_setuid = is_setuid_or_setgid_binary(&raw_path);
                        let is_read_violation =
                            !sandbox_state.is_path_readable(&raw_path) && raw_path.exists();
                        if is_setuid && !is_read_violation {
                            continue;
                        }
                        if has_unreadable && !is_read_violation && raw_path.exists() {
                            continue;
                        }
                        let req_dir = normalize_violation_to_request_dir(&raw_path);
                        let (cap, op) = if is_setuid && is_read_violation {
                            (
                                CapabilityRequest::FileRead { path: req_dir },
                                "file-read-data + setuid/setgid exec".to_string(),
                            )
                        } else if is_read_violation {
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
                    if cfg!(target_os = "macos") {
                        for (op, raw_path_str) in query_macos_sandbox_violations(&trace_tag).await {
                            let raw_path = PathBuf::from(&raw_path_str);
                            let req_dir = normalize_violation_to_request_dir(&raw_path);
                            let cap = if op.starts_with("file-read") {
                                CapabilityRequest::FileRead { path: req_dir }
                            } else {
                                CapabilityRequest::FileWrite { path: req_dir }
                            };
                            if !violations.iter().any(|existing| existing.capability == cap) {
                                violations.push(SandboxViolation {
                                    capability: cap,
                                    raw_path,
                                    operation: op,
                                });
                            }
                        }
                    }

                    let new_violations: Vec<_> = violations
                        .into_iter()
                        .filter(|v| {
                            if is_setuid_or_setgid_binary(&v.raw_path) {
                                !unlocked_paths.iter().any(|u| u == &v.raw_path)
                            } else {
                                !unlocked_paths.iter().any(|u| v.raw_path.starts_with(u))
                            }
                        })
                        .collect();

                    if !new_violations.is_empty() {
                        if let Some(host) = core_host {
                            let mut all_approved = true;
                            for v in new_violations {
                                if request_sandbox_violation_approval(
                                    host,
                                    plugin_name,
                                    &command,
                                    &v,
                                    sandbox_state,
                                    &mut unlocked_paths,
                                    &mut temporary_once_paths,
                                )
                                .await
                                .is_err()
                                {
                                    all_approved = false;
                                    break;
                                }
                            }
                            if all_approved {
                                continue;
                            }
                        }
                    }
                }

                let finished_at = Utc::now();
                let duration_ms = (finished_at - started_at).num_milliseconds().max(0) as u64;
                let final_info = {
                    let mut guard = entry.lock().expect("process entry poisoned");
                    guard.info.phase = ManagedProcessPhase::Exited;
                    guard.info.exit_code = Some(exit_code);
                    guard.info.finished_at = Some(finished_at);
                    guard.info.duration_ms = duration_ms;
                    guard.info.clone()
                };
                self.entries
                    .lock()
                    .expect("entries poisoned")
                    .insert(proc_id.to_string(), entry);

                if let Some(ref sink) = self.sink {
                    let notif = format_exit_notification(&final_info);
                    sink.on_process_exited_naturally(
                        final_info.clone(),
                        MessageContent::InputText { text: notif },
                    )
                    .await;
                }

                return Ok(ProcessActionResponse {
                    ok: true,
                    process: Some(final_info),
                    ..Default::default()
                });
            }

            self.entries
                .lock()
                .expect("entries poisoned")
                .insert(proc_id.to_string(), entry.clone());

            if let Some(ref sink) = self.sink {
                sink.on_process_started(info.clone()).await;
            }

            let sink_clone = self.sink.clone();
            let sb_clone = sandbox_state.clone();
            let once_paths = std::mem::take(&mut temporary_once_paths);

            tokio::spawn(async move {
                let mut timed_out = false;
                let mut killed_by_stop = false;

                let wait_res = if timeout_ms > 0 {
                    tokio::select! {
                        res = child.wait() => Some(res),
                        _ = stop_notify.notified() => {
                            killed_by_stop = true;
                            None
                        }
                        _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                            timed_out = true;
                            None
                        }
                    }
                } else {
                    tokio::select! {
                        res = child.wait() => Some(res),
                        _ = stop_notify.notified() => {
                            killed_by_stop = true;
                            None
                        }
                    }
                };

                let exit_code = match wait_res {
                    Some(Ok(status)) => status.code().unwrap_or(-1),
                    Some(Err(_)) => -1,
                    None => {
                        if let Some(pid) = os_pid {
                            kill_process_group(pid).await;
                        }
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        -1
                    }
                };

                let _ = out_reader.await;
                let _ = err_reader.await;

                for p in once_paths {
                    sb_clone.revoke_temporary_path(&p);
                }

                let finished_at = Utc::now();
                let (final_info, should_notify) = {
                    let mut guard = entry.lock().expect("process entry poisoned");
                    guard.info.finished_at = Some(finished_at);
                    guard.info.duration_ms = (finished_at - guard.info.started_at)
                        .num_milliseconds()
                        .max(0) as u64;
                    guard.info.exit_code = Some(exit_code);
                    guard.info.phase = if timed_out {
                        ManagedProcessPhase::TimedOut
                    } else if killed_by_stop || guard.suppressed_notification {
                        ManagedProcessPhase::Killed
                    } else {
                        ManagedProcessPhase::Exited
                    };
                    (
                        guard.info.clone(),
                        !guard.suppressed_notification && !killed_by_stop,
                    )
                };

                if should_notify {
                    if let Some(sink) = sink_clone {
                        let notif = format_exit_notification(&final_info);
                        sink.on_process_exited_naturally(
                            final_info,
                            MessageContent::InputText { text: notif },
                        )
                        .await;
                    }
                }
            });

            return Ok(ProcessActionResponse {
                ok: true,
                process: Some(info),
                ..Default::default()
            });
        }

        for tmp_path in temporary_once_paths {
            sandbox_state.revoke_temporary_path(&tmp_path);
        }

        Ok(ProcessActionResponse {
            ok: false,
            error: Some(
                "background process sandbox permission escalation exceeded max rounds".into(),
            ),
            ..Default::default()
        })
    }

    fn find_entry(
        &self,
        session_id: &SessionId,
        id_or_pid: &str,
    ) -> Option<Arc<Mutex<ManagedProcessEntry>>> {
        let guard = self.entries.lock().expect("entries poisoned");
        if let Some(entry) = guard.get(id_or_pid) {
            let e = entry.lock().expect("process entry poisoned");
            if &e.info.session_id == session_id {
                return Some(entry.clone());
            }
        }
        if let Ok(pid_num) = id_or_pid.parse::<u32>() {
            for entry in guard.values() {
                let e = entry.lock().expect("process entry poisoned");
                if &e.info.session_id == session_id && e.info.pid == Some(pid_num) {
                    return Some(entry.clone());
                }
            }
        }
        None
    }

    fn query_status(
        &self,
        session_id: &SessionId,
        process_id: Option<&str>,
    ) -> Result<ProcessActionResponse, CoreError> {
        let now = Utc::now();
        if let Some(id_str) = process_id {
            let Some(entry) = self.find_entry(session_id, id_str) else {
                return Ok(ProcessActionResponse {
                    ok: false,
                    error: Some(format!(
                        "process '{id_str}' not found in session {session_id}"
                    )),
                    ..Default::default()
                });
            };
            let mut info = entry.lock().expect("process entry poisoned").info.clone();
            if info.phase == ManagedProcessPhase::Running {
                info.duration_ms = (now - info.started_at).num_milliseconds().max(0) as u64;
            }
            Ok(ProcessActionResponse {
                ok: true,
                exit_code: info.exit_code,
                process: Some(info),
                ..Default::default()
            })
        } else {
            let guard = self.entries.lock().expect("entries poisoned");
            let mut list = Vec::new();
            for entry in guard.values() {
                let mut info = entry.lock().expect("process entry poisoned").info.clone();
                if &info.session_id == session_id {
                    if info.phase == ManagedProcessPhase::Running {
                        info.duration_ms = (now - info.started_at).num_milliseconds().max(0) as u64;
                    }
                    list.push(info);
                }
            }
            list.sort_by_key(|i| i.started_at);
            Ok(ProcessActionResponse {
                ok: true,
                processes: Some(list),
                ..Default::default()
            })
        }
    }

    fn query_logs(
        &self,
        session_id: &SessionId,
        process_id: &str,
        stream: ProcessLogStream,
        tail_lines: Option<usize>,
        offset: Option<usize>,
    ) -> Result<ProcessActionResponse, CoreError> {
        let Some(entry) = self.find_entry(session_id, process_id) else {
            return Ok(ProcessActionResponse {
                ok: false,
                error: Some(format!(
                    "process '{process_id}' not found in session {session_id}"
                )),
                ..Default::default()
            });
        };

        let guard = entry.lock().expect("process entry poisoned");
        let raw = match stream {
            ProcessLogStream::Stdout => &guard.stdout_buf,
            ProcessLogStream::Stderr => &guard.stderr_buf,
            ProcessLogStream::Combined => &guard.combined_buf,
        };

        let sliced = if let Some(off) = offset {
            if off >= raw.len() {
                String::new()
            } else {
                let safe_idx = raw
                    .char_indices()
                    .map(|(i, _)| i)
                    .find(|&i| i >= off)
                    .unwrap_or(raw.len());
                raw[safe_idx..].to_string()
            }
        } else if let Some(n) = tail_lines {
            let lines: Vec<&str> = raw.lines().collect();
            let start = lines.len().saturating_sub(n);
            lines[start..].join("\n")
        } else {
            raw.clone()
        };

        let mut resp = ProcessActionResponse {
            ok: true,
            exit_code: guard.info.exit_code,
            process: Some(guard.info.clone()),
            ..Default::default()
        };
        match stream {
            ProcessLogStream::Stdout => resp.stdout = Some(sliced),
            ProcessLogStream::Stderr => resp.stderr = Some(sliced),
            ProcessLogStream::Combined => resp.combined = Some(sliced),
        }
        Ok(resp)
    }

    pub async fn stop_process(
        &self,
        session_id: &SessionId,
        process_id: &str,
    ) -> Result<ProcessActionResponse, CoreError> {
        let Some(entry) = self.find_entry(session_id, process_id) else {
            return Ok(ProcessActionResponse {
                ok: false,
                error: Some(format!(
                    "process '{process_id}' not found in session {session_id}"
                )),
                ..Default::default()
            });
        };

        let (notify, pid, is_running) = {
            let mut guard = entry.lock().expect("process entry poisoned");
            guard.suppressed_notification = true;
            (
                guard.stop_notify.clone(),
                guard.info.pid,
                guard.info.phase == ManagedProcessPhase::Running,
            )
        };

        if is_running {
            if let Some(p) = pid {
                kill_process_group(p).await;
            }
            notify.notify_waiters();
            for _ in 0..20 {
                if entry.lock().expect("process entry poisoned").info.phase
                    != ManagedProcessPhase::Running
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        let info = entry.lock().expect("process entry poisoned").info.clone();
        Ok(ProcessActionResponse {
            ok: true,
            exit_code: info.exit_code,
            process: Some(info),
            ..Default::default()
        })
    }

    pub async fn cleanup_activation_processes(&self, activation_id: &ActivationId) {
        let to_stop: Vec<(Arc<Notify>, Option<u32>)> = {
            let guard = self.entries.lock().expect("entries poisoned");
            let mut list = Vec::new();
            for entry in guard.values() {
                let mut e = entry.lock().expect("process entry poisoned");
                if &e.info.activation_id == activation_id
                    && e.info.lifecycle == ProcessLifecycle::Activation
                    && e.info.phase == ManagedProcessPhase::Running
                {
                    e.suppressed_notification = true;
                    list.push((e.stop_notify.clone(), e.info.pid));
                }
            }
            list
        };

        for (notify, pid) in to_stop {
            if let Some(p) = pid {
                kill_process_group(p).await;
            }
            notify.notify_waiters();
        }
    }

    pub async fn cleanup_session_processes(&self, session_id: &SessionId) {
        let to_stop: Vec<(Arc<Notify>, Option<u32>)> = {
            let guard = self.entries.lock().expect("entries poisoned");
            let mut list = Vec::new();
            for entry in guard.values() {
                let mut e = entry.lock().expect("process entry poisoned");
                if &e.info.session_id == session_id && e.info.phase == ManagedProcessPhase::Running
                {
                    e.suppressed_notification = true;
                    list.push((e.stop_notify.clone(), e.info.pid));
                }
            }
            list
        };

        for (notify, pid) in to_stop {
            if let Some(p) = pid {
                kill_process_group(p).await;
            }
            notify.notify_waiters();
        }
    }

    pub fn migrate_session_processes(
        &self,
        from_session_id: &SessionId,
        to_session_id: &SessionId,
    ) {
        let guard = self.entries.lock().expect("entries poisoned");
        for entry in guard.values() {
            let mut e = entry.lock().expect("process entry poisoned");
            if &e.info.session_id == from_session_id {
                e.info.session_id = to_session_id.clone();
            }
        }
    }
}
