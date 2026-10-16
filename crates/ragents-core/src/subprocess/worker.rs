use super::protocol::{
    connect_ipc_stream, send_jsonl, BoxedReader, BoxedWriter, ParentToWorkerMessage,
    WorkerToParentMessage,
};
use crate::error::CoreError;
use crate::host::{CoreExecutor, CoreHost, ModelEndpointConfig, PermissionOutcome};
use crate::model::ModelBackend;
use crate::runner::InProcessExecutor;
use async_trait::async_trait;
use openresponses_rust::{CreateResponseBody, Item, ResponseResource, Usage};
use ragents_protocol::*;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub async fn run_subprocess_worker(ipc_addr: &str, ipc_token: &str) -> Result<(), CoreError> {
    let (reader, mut writer) = connect_ipc_stream(ipc_addr).await?;
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
    lines: Lines<BufReader<BoxedReader>>,
    writer: BoxedWriter,
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
        self.check_permission_with_original_path(extension_id, capability, None, reason)
            .await
    }

    async fn check_permission_with_original_path(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        original_path: Option<PathBuf>,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::CheckPermission {
                extension_id: extension_id.map(String::from),
                capability,
                original_path,
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

    async fn drain_realtime_messages(
        &self,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<Item>, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::DrainRealtimeMessages { turn_id })
            .await?
        {
            ParentToWorkerMessage::DrainRealtimeMessagesReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to DrainRealtimeMessages: {other:?}"
            ))),
        }
    }

    async fn manage_process(
        &self,
        extension_id: Option<&str>,
        request: ProcessActionRequest,
    ) -> Result<Option<ProcessActionResponse>, CoreError> {
        let mut ch = self.channel.lock().await;
        match ch
            .call(&WorkerToParentMessage::ManageProcess {
                extension_id: extension_id.map(String::from),
                request,
            })
            .await?
        {
            ParentToWorkerMessage::ManageProcessReply { result } => {
                result.map_err(CoreError::HostError)
            }
            other => Err(CoreError::HostError(format!(
                "unexpected reply to ManageProcess: {other:?}"
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

    async fn create_response_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
        _on_delta: Option<crate::model::ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        // Parent process streams deltas directly to `CoreHost::emit_event` while executing `CreateModelResponse`.
        self.create_response(endpoint, request).await
    }
}
