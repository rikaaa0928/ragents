mod protocol;
mod worker;

pub use protocol::*;
pub use worker::*;

use crate::error::CoreError;
use crate::host::{CoreExecutor, CoreHost, CoreRunSpec, CoreRunSummary};
use crate::model::ModelBackend;
use async_trait::async_trait;
use ragents_protocol::{SessionEvent, TurnId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, OnceCell};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

enum IpcListener {
    #[cfg(unix)]
    Unix {
        listener: tokio::net::UnixListener,
        socket_path: PathBuf,
    },
    Tcp {
        listener: TcpListener,
    },
}

impl IpcListener {
    async fn bind(prefer_uds: bool) -> Result<(Self, String), CoreError> {
        #[cfg(unix)]
        if prefer_uds {
            use std::sync::atomic::{AtomicU64, Ordering};
            static HUB_SEQ: AtomicU64 = AtomicU64::new(1);

            let ipc_dir = PathBuf::from("/tmp/ragents-ipc");
            if std::fs::create_dir_all(&ipc_dir).is_ok() {
                let seq = HUB_SEQ.fetch_add(1, Ordering::Relaxed);
                let sock_path = ipc_dir.join(format!("d_{}_{}.sock", std::process::id(), seq));
                let _ = std::fs::remove_file(&sock_path);
                if let Ok(listener) = tokio::net::UnixListener::bind(&sock_path) {
                    let addr_str = format!("unix:{}", sock_path.to_string_lossy());
                    return Ok((
                        Self::Unix {
                            listener,
                            socket_path: sock_path,
                        },
                        addr_str,
                    ));
                }
            }
        }
        #[cfg(not(unix))]
        let _ = prefer_uds;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| CoreError::HostError(format!("failed to bind tcp ipc listener: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| CoreError::HostError(format!("failed to read tcp ipc local_addr: {e}")))?;
        Ok((Self::Tcp { listener }, format!("tcp:{addr}")))
    }

    async fn accept(&self) -> Result<(BoxedReader, BoxedWriter), CoreError> {
        match self {
            #[cfg(unix)]
            Self::Unix { listener, .. } => {
                let (stream, _) = listener
                    .accept()
                    .await
                    .map_err(|e| CoreError::HostError(format!("uds ipc accept error: {e}")))?;
                let (r, w) = tokio::io::split(stream);
                Ok((Box::new(r), Box::new(w)))
            }
            Self::Tcp { listener } => {
                let (stream, _) = listener
                    .accept()
                    .await
                    .map_err(|e| CoreError::HostError(format!("tcp ipc accept error: {e}")))?;
                let (r, w) = tokio::io::split(stream);
                Ok((Box::new(r), Box::new(w)))
            }
        }
    }
}

/// Shared IPC Hub bound once per `SubprocessExecutor` (i.e. once per `agent-daemon`).
/// All worker subprocesses connect to the same UDS (or TCP loopback) endpoint and are
/// demultiplexed to their owning activation task via `Hello { ipc_token }`.
struct SharedIpcHub {
    ipc_addr: String,
    #[cfg(unix)]
    socket_path: Option<PathBuf>,
    pending_workers: Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<WorkerStreamPair>>>>,
    accept_task: JoinHandle<()>,
}

impl SharedIpcHub {
    async fn start(prefer_uds: bool) -> Result<Arc<Self>, CoreError> {
        let (listener, ipc_addr) = IpcListener::bind(prefer_uds).await?;
        #[cfg(unix)]
        let socket_path = match &listener {
            IpcListener::Unix { socket_path, .. } => Some(socket_path.clone()),
            IpcListener::Tcp { .. } => None,
        };

        let pending_workers: Arc<
            std::sync::Mutex<HashMap<String, oneshot::Sender<WorkerStreamPair>>>,
        > = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let pending_for_loop = pending_workers.clone();

        let accept_task = tokio::spawn(async move {
            loop {
                let (reader, writer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let pending_map = pending_for_loop.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(reader).lines();
                    if let Ok(Some(first_line)) = lines.next_line().await {
                        if let Ok(WorkerToParentMessage::Hello { ipc_token }) =
                            serde_json::from_str::<WorkerToParentMessage>(&first_line)
                        {
                            let maybe_tx = pending_map
                                .lock()
                                .expect("pending_workers mutex poisoned")
                                .remove(&ipc_token);
                            if let Some(tx) = maybe_tx {
                                let _ = tx.send((lines, writer));
                            }
                        }
                    }
                });
            }
        });

        Ok(Arc::new(Self {
            ipc_addr,
            #[cfg(unix)]
            socket_path,
            pending_workers,
            accept_task,
        }))
    }

    fn register_token(&self, token: String) -> oneshot::Receiver<WorkerStreamPair> {
        let (tx, rx) = oneshot::channel();
        self.pending_workers
            .lock()
            .expect("pending_workers mutex poisoned")
            .insert(token, tx);
        rx
    }

    fn remove_token(&self, token: &str) {
        self.pending_workers
            .lock()
            .expect("pending_workers mutex poisoned")
            .remove(token);
    }
}

impl Drop for SharedIpcHub {
    fn drop(&mut self) {
        self.accept_task.abort();
        #[cfg(unix)]
        if let Some(ref path) = self.socket_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Clone)]
pub struct SubprocessExecutor {
    worker_bin: Option<PathBuf>,
    prefer_uds: bool,
    hub: Arc<OnceCell<Arc<SharedIpcHub>>>,
}

impl std::fmt::Debug for SubprocessExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubprocessExecutor")
            .field("worker_bin", &self.worker_bin)
            .field("prefer_uds", &self.prefer_uds)
            .finish()
    }
}

impl Default for SubprocessExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl SubprocessExecutor {
    pub fn new() -> Self {
        Self {
            worker_bin: None,
            prefer_uds: true,
            hub: Arc::new(OnceCell::new()),
        }
    }

    pub fn with_worker_binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.worker_bin = Some(path.into());
        self
    }

    pub fn with_prefer_uds(mut self, prefer_uds: bool) -> Self {
        self.prefer_uds = prefer_uds;
        self.hub = Arc::new(OnceCell::new());
        self
    }

    async fn get_or_init_hub(&self) -> Result<Arc<SharedIpcHub>, CoreError> {
        let prefer_uds = self.prefer_uds;
        let hub = self
            .hub
            .get_or_try_init(|| async move { SharedIpcHub::start(prefer_uds).await })
            .await?;
        Ok(hub.clone())
    }

    pub async fn shared_ipc_addr(&self) -> Result<String, CoreError> {
        let hub = self.get_or_init_hub().await?;
        Ok(hub.ipc_addr.clone())
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

        if stem == "ragents" || stem == "ragents-core-worker" {
            return Ok((current_exe, true));
        }

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

        let hub = self.get_or_init_hub().await?;
        let ipc_token = format!("tok_{}", Uuid::now_v7());
        let worker_conn_rx = hub.register_token(ipc_token.clone());

        let (worker_bin, _) = self.resolve_worker_command()?;
        let mut cmd = tokio::process::Command::new(&worker_bin);
        cmd.arg("internal-core-worker")
            .arg("--ipc-addr")
            .arg(&hub.ipc_addr)
            .arg("--ipc-token")
            .arg(&ipc_token)
            .current_dir(&run_spec.workspace_root)
            .env_remove("RAGENTS_API_KEY")
            .kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                hub.remove_token(&ipc_token);
                return Err(CoreError::HostError(format!(
                    "failed to spawn subprocess worker {worker_bin:?}: {e}"
                )));
            }
        };

        let parent_endpoint = run_spec.model_endpoint.clone();
        let mut sanitized_spec = run_spec;
        sanitized_spec.model_endpoint.api_key = String::new();

        let service_res = async {
            let (mut lines, mut writer) = tokio::select! {
                res = worker_conn_rx => res.map_err(|_| CoreError::HostError("worker handshake channel dropped".into()))?,
                status = child.wait() => {
                    return Err(CoreError::HostError(format!(
                        "worker subprocess exited before connecting: {status:?}"
                    )));
                }
                _ = cancellation.cancelled() => {
                    return Err(CoreError::Cancelled);
                }
            };

            send_jsonl(
                &mut writer,
                &ParentToWorkerMessage::Start {
                    run_spec: Box::new(sanitized_spec),
                },
            )
            .await?;

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
                        original_path,
                        reason,
                    } => {
                        let res = tokio::select! {
                            r = host.check_permission_with_original_path(extension_id.as_deref(), capability, original_path, reason) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::CheckPermissionReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::DrainRealtimeMessages { turn_id } => {
                        let res = tokio::select! {
                            r = host.drain_realtime_messages(turn_id) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::DrainRealtimeMessagesReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::ManageProcess {
                        extension_id,
                        request,
                    } => {
                        let res = tokio::select! {
                            r = host.manage_process(extension_id.as_deref(), request) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::ManageProcessReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::RolloverCompactedSession {
                        turn_id,
                        plan,
                        compaction_prompt,
                        summary_text,
                        compaction_usage,
                    } => {
                        let res = tokio::select! {
                            r = host.rollover_compacted_session(
                                turn_id,
                                *plan,
                                compaction_prompt,
                                summary_text,
                                compaction_usage,
                            ) => r.map_err(|e| e.to_string()),
                            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                        };
                        send_jsonl(
                            &mut writer,
                            &ParentToWorkerMessage::RolloverCompactedSessionReply { result: res },
                        )
                        .await?;
                    }
                    WorkerToParentMessage::CreateModelResponse { request } => {
                        let is_compaction_worker = request
                            .metadata
                            .as_ref()
                            .and_then(|m| m.get("ragents.role"))
                            .map(|s| s.as_str())
                            == Some("compaction_worker");
                        let turn_id = request
                            .metadata
                            .as_ref()
                            .and_then(|m| m.get("ragents.turn_id"))
                            .map(TurnId::new);
                        let (delta_tx, mut delta_rx) =
                            tokio::sync::mpsc::unbounded_channel::<crate::model::ModelStreamDelta>();
                        let cb: Option<crate::model::ModelStreamCallback> = if is_compaction_worker {
                            None
                        } else {
                            Some(Arc::new(move |delta| {
                                let _ = delta_tx.send(delta);
                            }))
                        };
                        let model_fut = model_backend.create_response_streaming(
                            &parent_endpoint,
                            *request,
                            cb,
                        );
                        tokio::pin!(model_fut);
                        let res = loop {
                            tokio::select! {
                                r = &mut model_fut => {
                                    while let Ok(delta) = delta_rx.try_recv() {
                                        let ev = match delta {
                                            crate::model::ModelStreamDelta::TextDelta { delta } => SessionEvent::ModelTextDelta { delta },
                                            crate::model::ModelStreamDelta::ReasoningDelta { delta } => SessionEvent::ModelReasoningDelta { delta },
                                        };
                                        let _ = host.emit_event(turn_id.clone(), ev).await;
                                    }
                                    break r.map(Box::new).map_err(|e| e.to_string());
                                }
                                Some(delta) = delta_rx.recv() => {
                                    let ev = match delta {
                                        crate::model::ModelStreamDelta::TextDelta { delta } => SessionEvent::ModelTextDelta { delta },
                                        crate::model::ModelStreamDelta::ReasoningDelta { delta } => SessionEvent::ModelReasoningDelta { delta },
                                    };
                                    let _ = host.emit_event(turn_id.clone(), ev).await;
                                }
                                _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                            }
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

        hub.remove_token(&ipc_token);
        if service_res.is_err() {
            let _ = child.kill().await;
        }
        let _ = child.wait().await;
        service_res
    }
}
