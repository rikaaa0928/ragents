use crate::error::CoreError;
use crate::host::{CoreRunSpec, CoreRunSummary, PermissionOutcome};
use openresponses_rust::{CreateResponseBody, Item, ResponseResource, Usage};
use ragents_protocol::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;

pub(crate) type BoxedReader = Box<dyn AsyncRead + Unpin + Send>;
pub(crate) type BoxedWriter = Box<dyn AsyncWrite + Unpin + Send>;
pub(crate) type WorkerStreamPair = (Lines<BufReader<BoxedReader>>, BoxedWriter);

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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_path: Option<PathBuf>,
        reason: Option<String>,
    },
    DrainRealtimeMessages {
        turn_id: Option<TurnId>,
    },
    ManageProcess {
        extension_id: Option<String>,
        request: ProcessActionRequest,
    },
    RolloverCompactedSession {
        turn_id: Option<TurnId>,
        plan: Box<CompactionPlan>,
        compaction_prompt: String,
        summary_text: String,
        compaction_usage: Option<Usage>,
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
    DrainRealtimeMessagesReply {
        result: Result<Vec<Item>, String>,
    },
    ManageProcessReply {
        result: Result<Option<ProcessActionResponse>, String>,
    },
    RolloverCompactedSessionReply {
        result: Result<SessionId, String>,
    },
    CreateModelResponseReply {
        result: Result<Box<ResponseResource>, String>,
    },
}

pub(crate) async fn connect_ipc_stream(
    ipc_addr: &str,
) -> Result<(BoxedReader, BoxedWriter), CoreError> {
    #[cfg(unix)]
    if let Some(unix_path) = ipc_addr.strip_prefix("unix:") {
        let stream = tokio::net::UnixStream::connect(unix_path)
            .await
            .map_err(|e| {
                CoreError::HostError(format!("worker failed to connect to uds {unix_path}: {e}"))
            })?;
        let (r, w) = tokio::io::split(stream);
        return Ok((Box::new(r), Box::new(w)));
    }

    let tcp_addr = ipc_addr.strip_prefix("tcp:").unwrap_or(ipc_addr);
    let stream = TcpStream::connect(tcp_addr).await.map_err(|e| {
        CoreError::HostError(format!("worker failed to connect to tcp {tcp_addr}: {e}"))
    })?;
    let (r, w) = tokio::io::split(stream);
    Ok((Box::new(r), Box::new(w)))
}

pub(crate) async fn send_jsonl<W, T>(writer: &mut W, msg: &T) -> Result<(), CoreError>
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
