use crate::error::CoreError;
use crate::host::{
    CoreExecutor, CoreHost, CoreRunSpec, CoreRunSummary, ModelEndpointConfig, PermissionOutcome,
};
use crate::model::ModelBackend;
use crate::runner::InProcessExecutor;
use async_trait::async_trait;
use openresponses_rust::{CreateResponseBody, Item, ResponseResource, Usage};
use ragents_protocol::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerToParentMessage {
    Hello {
        ipc_token: String,
    },
    LoadEffectiveContext,
    CommitModelOutput {
        turn_id: TurnId,
        response_id: Option<String>,
        items: Vec<Item>,
        usage: Option<Usage>,
    },
    CommitToolOutput {
        turn_id: TurnId,
        call_id: String,
        tool_name: String,
        success: bool,
        duration_ms: u64,
        items: Vec<Item>,
    },
    EmitEvent {
        turn_id: Option<TurnId>,
        event: Box<SessionEvent>,
    },
    CheckPermission {
        extension_id: Option<String>,
        capability: CapabilityRequest,
        reason: Option<String>,
    },
    SpawnChildSession {
        request: Box<CreateSessionRequest>,
        turn_id: Option<TurnId>,
        call_id: Option<String>,
        command_id: CommandId,
    },
    CreateModelResponse {
        request: Box<CreateResponseBody>,
    },
    Finished {
        result: Result<CoreRunSummary, String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParentToWorkerMessage {
    Start {
        run_spec: Box<CoreRunSpec>,
    },
    EffectiveContextReply {
        result: Result<Vec<Item>, String>,
    },
    CommitBatchReply {
        result: Result<BatchSeq, String>,
    },
    EmitEventReply {
        result: Result<(), String>,
    },
    CheckPermissionReply {
        result: Result<PermissionOutcome, String>,
    },
    SpawnChildSessionReply {
        result: Result<SessionId, String>,
    },
    CreateModelResponseReply {
        result: Result<Box<ResponseResource>, String>,
    },
}

/// Out-of-process container executor (`SubprocessExecutor`).
/// Spawns an isolated OS child process (`internal-core-worker`) per activation with its own
/// working directory, stripped API key environment, and hard-kill lifecycle management.
#[derive(Debug, Clone, Default)]
pub struct SubprocessExecutor {
    worker_bin: Option<PathBuf>,
}

impl SubprocessExecutor {
    pub fn new() -> Self {
        Self { worker_bin: None }
    }

    pub fn with_worker_binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.worker_bin = Some(path.into());
        self
    }

    fn resolve_worker_command(&self) -> Result<(PathBuf, bool), CoreError> {
        if let Some(ref bin) = self.worker_bin {
            return Ok((bin.clone(), true));
        }
        if let Ok(env_bin) = std::env::var("RAGENTS_WORKER_BIN") {
            let p = PathBuf::from(env_bin);
            if p.exists() {
                return Ok((p, true));
            }
        }

        let current_exe = std::env::current_exe()
            .map_err(|e| CoreError::HostError(format!("failed to resolve current_exe: {e}")))?;

        let stem = current_exe
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        // 1. Single-binary self-spawn when running inside `ragents` CLI or `ragents-core-worker`
        if stem == "ragents" || stem == "ragents-core-worker" {
            return Ok((current_exe, true));
        }

        // 2. When running inside `cargo test` (`target/debug/deps/<test_bin>`), look in `target/debug/`
        if let Some(deps_dir) = current_exe.parent() {
            if let Some(target_profile_dir) = deps_dir.parent() {
                for candidate_name in ["ragents", "ragents-core-worker"] {
                    let candidate = target_profile_dir.join(candidate_name);
                    if candidate.exists() {
                        return Ok((candidate, true));
                    }
                }
            }
        }

        Ok((current_exe, true))
    }
}

#[async_trait]
impl CoreExecutor for SubprocessExecutor {
    async fn execute(
        &self,
        run_spec: CoreRunSpec,
        host: Arc<dyn CoreHost>,
        model_backend: Arc<dyn ModelBackend>,
        cancellation: CancellationToken,
    ) -> Result<CoreRunSummary, CoreError> {
        if cancellation.is_cancelled() {
            return Err(CoreError::Cancelled);
        }

        let _ = std::fs::create_dir_all(&run_spec.workspace_root);
        let _ = std::fs::create_dir_all(&run_spec.session_tmp_dir);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| CoreError::HostError(format!("failed to bind ipc listener: {e}")))?;
        let ipc_addr = listener
            .local_addr()
            .map_err(|e| CoreError::HostError(format!("failed to read ipc local_addr: {e}")))?;
        let ipc_token = format!("tok_{}", Uuid::now_v7());

        let (worker_bin, _) = self.resolve_worker_command()?;
        let mut cmd = tokio::process::Command::new(&worker_bin);
        cmd.arg("internal-core-worker")
            .arg("--ipc-addr")
            .arg(ipc_addr.to_string())
            .arg("--ipc-token")
            .arg(&ipc_token)
            .current_dir(&run_spec.workspace_root)
            .env_remove("RAGENTS_API_KEY")
            .kill_on_drop(true);

        let mut child = cmd.spawn().map_err(|e| {
            CoreError::HostError(format!(
                "failed to spawn subprocess worker {worker_bin:?}: {e}"
            ))
        })?;

        let parent_endpoint = run_spec.model_endpoint.clone();
        let mut sanitized_spec = run_spec;
        // Strip raw API key from the spec sent into the worker subprocess memory space
        sanitized_spec.model_endpoint.api_key = String::new();

        let service_res = async {
            let (stream, _) = tokio::select! {
                res = listener.accept() => res.map_err(|e| CoreError::HostError(format!("ipc accept error: {e}")))?,
                status = child.wait() => {
                    return Err(CoreError::HostError(format!(
                        "worker subprocess exited before connecting: {status:?}"
                    )));
                }
                _ = cancellation.cancelled() => {
                    return Err(CoreError::Cancelled);
                }
            };

            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            // 1. Verify Hello token
            let first_line = tokio::select! {
                res = lines.next_line() => res
                    .map_err(|e| CoreError::HostError(format!("ipc read error: {e}")))?
                    .ok_or_else(|| CoreError::HostError("worker closed ipc before Hello".into()))?,
                _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
            };
            let hello: WorkerToParentMessage = serde_json::from_str(&first_line)?;
            match hello {
                WorkerToParentMessage::Hello { ipc_token: t } if t == ipc_token => {}
                _ => {
                    return Err(CoreError::HostError(
                        "invalid ipc handshake from subprocess worker".into(),
                    ))
                }
            }

            // 2. Send Start { run_spec }
            send_jsonl(
                &mut writer,
                &ParentToWorkerMessage::Start {
                    run_spec: Box::new(sanitized_spec),
                },
            )
            .await?;

            // 3. Proxy CoreHost & ModelBackend calls until Finished
            loop {
                let next_line = tokio::select! {
                    res = lines.next_line() => res.map_err(|e| CoreError::HostError(format!("ipc read error: {e}")))?,
                    _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                };

                let Some(line) = next_line else {
                    return Err(CoreError::HostError(
                        "worker subprocess closed ipc unexpectedly before Finished".into(),
                    ));
                };

                let msg: WorkerToParentMessage = serde_json::from_str(&line)?;
                match msg {
                    WorkerToParentMessage::Hello { .. } => {}
                    WorkerToParentMessage::LoadEffectiveContext => {
                        let res = tokio::select! {
                            r = host.load_effective_context() => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::EffectiveContextReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::CommitModelOutput {
                        turn_id,
                        response_id,
                        items,
                        usage,
                    } => {
                        let res = tokio::select! {
                            r = host.commit_model_output(&turn_id, response_id, items, usage) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::CommitBatchReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::CommitToolOutput {
                        turn_id,
                        call_id,
                        tool_name,
                        success,
                        duration_ms,
                        items,
                    } => {
                        let res = tokio::select! {
                            r = host.commit_tool_output(&turn_id, &call_id, &tool_name, success, duration_ms, items) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::CommitBatchReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::EmitEvent { turn_id, event } => {
                        let res = tokio::select! {
                            r = host.emit_event(turn_id, *event) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::EmitEventReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::CheckPermission {
                        extension_id,
                        capability,
                        reason,
                    } => {
                        let res = tokio::select! {
                            r = host.check_permission(extension_id.as_deref(), capability, reason) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::CheckPermissionReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::SpawnChildSession {
                        request,
                        turn_id,
                        call_id,
                        command_id,
                    } => {
                        let res = tokio::select! {
                            r = host.spawn_child_session(*request, turn_id, call_id, command_id) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::SpawnChildSessionReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::CreateModelResponse { request } => {
                        let res = tokio::select! {
                            r = model_backend.create_response(&parent_endpoint, *request) => r.map(Box::new).map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::CreateModelResponseReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::Finished { result } => {
                        return result.map_err(CoreError::ToolError);
                    }
                }
            }
        }
        .await;

        if service_res.is_err() {
            let _ = child.kill().await;
        }
        let _ = child.wait().await;
        service_res
    }
}

// ============================================================================
// Child Subprocess Worker Implementation
// ============================================================================

pub async fn run_subprocess_worker(ipc_addr: &str, ipc_token: &str) -> Result<(), CoreError> {
    let stream = TcpStream::connect(ipc_addr).await.map_err(|e| {
        CoreError::HostError(format!("worker failed to connect to {ipc_addr}: {e}"))
    })?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    send_jsonl(
        &mut writer,
        &WorkerToParentMessage::Hello {
            ipc_token: ipc_token.to_string(),
        },
    )
    .await?;

    let start_line = lines
        .next_line()
        .await
        .map_err(|e| CoreError::HostError(format!("worker read Start error: {e}")))?
        .ok_or_else(|| CoreError::HostError("parent closed ipc before Start".into()))?;

    let start_msg: ParentToWorkerMessage = serde_json::from_str(&start_line)?;
    let run_spec = match start_msg {
        ParentToWorkerMessage::Start { run_spec } => *run_spec,
        other => {
            return Err(CoreError::HostError(format!(
                "expected Start message, got {other:?}"
            )))
        }
    };

    let _ = std::env::set_current_dir(&run_spec.workspace_root);

    let channel = Arc::new(Mutex::new(WorkerIpcChannel { lines, writer }));
    let host_proxy: Arc<dyn CoreHost> = Arc::new(IpcCoreHostProxy {
        channel: channel.clone(),
    });
    let model_proxy: Arc<dyn ModelBackend> = Arc::new(IpcModelBackendProxy {
        channel: channel.clone(),
    });

    let in_proc = InProcessExecutor;
    let exec_res = in_proc
        .execute(run_spec, host_proxy, model_proxy, CancellationToken::new())
        .await;

    let finished_msg = WorkerToParentMessage::Finished {
        result: exec_res.map_err(|e| e.to_string()),
    };
    let mut guard = channel.lock().await;
    send_jsonl(&mut guard.writer, &finished_msg).await?;
    let _ = guard.writer.flush().await;
    Ok(())
}

struct WorkerIpcChannel {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl WorkerIpcChannel {
    async fn call(
        &mut self,
        req: &WorkerToParentMessage,
    ) -> Result<ParentToWorkerMessage, CoreError> {
        send_jsonl(&mut self.writer, req).await?;
        let line = self
            .lines
            .next_line()
            .await
            .map_err(|e| CoreError::HostError(format!("worker ipc read error: {e}")))?
            .ok_or_else(|| CoreError::Cancelled)?;
        Ok(serde_json::from_str(&line)?)
    }
}

struct IpcCoreHostProxy {
    channel: Arc<Mutex<WorkerIpcChannel>>,
}

#[async_trait]
impl CoreHost for IpcCoreHostProxy {
    async fn load_effective_context(&self) -> Result<Vec<Item>, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::LoadEffectiveContext)
            .await?
        {
            ParentToWorkerMessage::EffectiveContextReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to LoadEffectiveContext: {other:?}"
            ))),
        }
    }

    async fn commit_model_output(
        &self,
        turn_id: &TurnId,
        response_id: Option<String>,
        items: Vec<Item>,
        usage: Option<Usage>,
    ) -> Result<BatchSeq, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::CommitModelOutput {
                turn_id: turn_id.clone(),
                response_id,
                items,
                usage,
            })
            .await?
        {
            ParentToWorkerMessage::CommitBatchReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to CommitModelOutput: {other:?}"
            ))),
        }
    }

    async fn commit_tool_output(
        &self,
        turn_id: &TurnId,
        call_id: &str,
        tool_name: &str,
        success: bool,
        duration_ms: u64,
        items: Vec<Item>,
    ) -> Result<BatchSeq, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::CommitToolOutput {
                turn_id: turn_id.clone(),
                call_id: call_id.to_string(),
                tool_name: tool_name.to_string(),
                success,
                duration_ms,
                items,
            })
            .await?
        {
            ParentToWorkerMessage::CommitBatchReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to CommitToolOutput: {other:?}"
            ))),
        }
    }

    async fn emit_event(
        &self,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<(), CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::EmitEvent {
                turn_id,
                event: Box::new(event),
            })
            .await?
        {
            ParentToWorkerMessage::EmitEventReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to EmitEvent: {other:?}"
            ))),
        }
    }

    async fn check_permission(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::CheckPermission {
                extension_id: extension_id.map(String::from),
                capability,
                reason,
            })
            .await?
        {
            ParentToWorkerMessage::CheckPermissionReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to CheckPermission: {other:?}"
            ))),
        }
    }

    async fn spawn_child_session(
        &self,
        request: CreateSessionRequest,
        turn_id: Option<TurnId>,
        call_id: Option<String>,
        command_id: CommandId,
    ) -> Result<SessionId, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::SpawnChildSession {
                request: Box::new(request),
                turn_id,
                call_id,
                command_id,
            })
            .await?
        {
            ParentToWorkerMessage::SpawnChildSessionReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to SpawnChildSession: {other:?}"
            ))),
        }
    }
}

struct IpcModelBackendProxy {
    channel: Arc<Mutex<WorkerIpcChannel>>,
}

#[async_trait]
impl ModelBackend for IpcModelBackendProxy {
    async fn create_response(
        &self,
        _endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::CreateModelResponse {
                request: Box::new(request),
            })
            .await?
        {
            ParentToWorkerMessage::CreateModelResponseReply { result } => {
                result.map(|b| *b).map_err(CoreError::ResponseFailed)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to CreateModelResponse: {other:?}"
            ))),
        }
    }
}

async fn send_jsonl<W, T>(writer: &mut W, msg: &T) -> Result<(), CoreError>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|e| CoreError::HostError(format!("ipc write error: {e}")))?;
    Ok(())
}
