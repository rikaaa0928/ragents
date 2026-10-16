use crate::ClientError;
use futures::{SinkExt, StreamExt};
use openresponses_rust::Item;
use ragents_protocol::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum DaemonPushEvent {
    DispatchActivation {
        session: Box<Session>,
        activation: Box<Activation>,
        lease_epoch: LeaseEpoch,
    },
    ReconcileActivation {
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        desired_phase: DesiredActivationPhase,
        interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
    },
    Disconnected,
}

#[derive(Clone)]
pub struct DaemonTunnelHandle {
    out_tx: mpsc::UnboundedSender<DaemonToServerMessage>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<ServerToDaemonMessage>>>>,
}

type AckPayload = (Option<ResourceVersion>, Option<BatchSeq>, Option<SessionId>);

impl DaemonTunnelHandle {
    pub async fn call(
        &self,
        msg: DaemonToServerMessage,
    ) -> Result<ServerToDaemonMessage, ClientError> {
        let req_id = Self::extract_request_id(&msg).to_string();
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending mutex poisoned")
            .insert(req_id.clone(), tx);

        if self.out_tx.send(msg).is_err() {
            self.pending
                .lock()
                .expect("pending mutex poisoned")
                .remove(&req_id);
            return Err(ClientError::Tunnel("tunnel write channel closed".into()));
        }

        rx.await
            .map_err(|_| ClientError::Tunnel("tunnel response channel dropped".into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        supported_models: Vec<String>,
        default_model: Option<String>,
        installed_extensions: Vec<String>,
        max_concurrent_runners: u32,
    ) -> Result<(u64, ResourceVersion), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::RegisterNode {
                request_id,
                node_id,
                hostname,
                os,
                arch,
                labels,
                supported_models,
                default_model,
                installed_extensions,
                max_concurrent_runners,
            })
            .await?;

        match resp {
            ServerToDaemonMessage::Registered {
                lease_ttl_ms,
                resource_version,
                ..
            } => Ok((lease_ttl_ms, resource_version)),
            ServerToDaemonMessage::Ack {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "register node failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected register response: {other:?}"
            ))),
        }
    }

    pub async fn heartbeat(
        &self,
        node_id: NodeId,
        active_runners: u32,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::Heartbeat {
                request_id,
                node_id,
                active_runners,
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _)| rv.unwrap_or_default())
    }

    pub async fn accept_activation(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        effective_config: ConfigRevision,
        effective_permission: PermissionSnapshot,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::AcceptActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                effective_config: Box::new(effective_config),
                effective_permission: Box::new(effective_permission),
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _)| rv.unwrap_or_default())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_batch(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        kind: ItemBatchKind,
        turn_id: Option<TurnId>,
        response_id: Option<String>,
        items: Vec<Item>,
        companion_event: BatchCompanionEvent,
    ) -> Result<BatchSeq, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::CommitBatch {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                kind,
                turn_id,
                response_id,
                items,
                companion_event,
            })
            .await?;
        Self::expect_ack(resp).map(|(_, batch_seq, _)| batch_seq.unwrap_or_default())
    }

    pub async fn emit_event(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::EmitEvent {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                event,
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _)| rv.unwrap_or_default())
    }

    pub async fn fetch_effective_context(
        &self,
        session_id: SessionId,
    ) -> Result<Vec<Item>, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::FetchEffectiveContext {
                request_id,
                session_id,
            })
            .await?;
        match resp {
            ServerToDaemonMessage::EffectiveContextReply {
                ok: true, items, ..
            } => Ok(items),
            ServerToDaemonMessage::EffectiveContextReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "fetch context failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected context reply: {other:?}"
            ))),
        }
    }

    pub async fn push_realtime_message(
        &self,
        session_id: SessionId,
        activation_id: Option<ActivationId>,
        content: Vec<openresponses_rust::MessageContent>,
    ) -> Result<(), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::PushRealtimeMessage {
                request_id,
                session_id,
                activation_id,
                content,
            })
            .await?;
        Self::expect_ack(resp).map(|_| ())
    }

    pub async fn drain_realtime_messages(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<Item>, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::DrainRealtimeMessages {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
            })
            .await?;
        match resp {
            ServerToDaemonMessage::RealtimeMessagesReply {
                ok: true, items, ..
            } => Ok(items),
            ServerToDaemonMessage::RealtimeMessagesReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "drain realtime messages failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected realtime messages reply: {other:?}"
            ))),
        }
    }

    pub async fn complete_activation(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        outcome: ActivationTerminalOutcome,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::CompleteActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                outcome,
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _)| rv.unwrap_or_default())
    }

    pub async fn rollover_compacted_session(
        &self,
        request: RolloverCompactedSessionRequest,
    ) -> Result<RolloverCompactedSessionResponse, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::RolloverCompactedSession {
                request_id,
                request: Box::new(request),
            })
            .await?;
        match resp {
            ServerToDaemonMessage::RolloverCompactedSessionReply {
                ok: true,
                response: Some(response),
                ..
            } => Ok(*response),
            ServerToDaemonMessage::RolloverCompactedSessionReply {
                ok: false, error, ..
            } => {
                Err(ClientError::Tunnel(error.unwrap_or_else(|| {
                    "rollover compacted session failed".into()
                })))
            }
            other => Err(ClientError::Tunnel(format!(
                "unexpected rollover compacted session reply: {other:?}"
            ))),
        }
    }

    fn expect_ack(msg: ServerToDaemonMessage) -> Result<AckPayload, ClientError> {
        match msg {
            ServerToDaemonMessage::Ack {
                ok: true,
                resource_version,
                batch_seq,
                spawned_session_id,
                ..
            } => Ok((resource_version, batch_seq, spawned_session_id)),
            ServerToDaemonMessage::Ack {
                ok: false, error, ..
            } => {
                Err(ClientError::Tunnel(error.unwrap_or_else(|| {
                    "daemon tunnel operation rejected".into()
                })))
            }
            other => Err(ClientError::Tunnel(format!(
                "expected Ack message, got {other:?}"
            ))),
        }
    }

    fn extract_request_id(msg: &DaemonToServerMessage) -> &str {
        match msg {
            DaemonToServerMessage::RegisterNode { request_id, .. }
            | DaemonToServerMessage::Heartbeat { request_id, .. }
            | DaemonToServerMessage::AcceptActivation { request_id, .. }
            | DaemonToServerMessage::CommitBatch { request_id, .. }
            | DaemonToServerMessage::EmitEvent { request_id, .. }
            | DaemonToServerMessage::FetchEffectiveContext { request_id, .. }
            | DaemonToServerMessage::PushRealtimeMessage { request_id, .. }
            | DaemonToServerMessage::DrainRealtimeMessages { request_id, .. }
            | DaemonToServerMessage::CompleteActivation { request_id, .. }
            | DaemonToServerMessage::RolloverCompactedSession { request_id, .. } => request_id,
        }
    }
}

pub async fn connect_daemon_tunnel(
    ws_url: &str,
) -> Result<(DaemonTunnelHandle, mpsc::UnboundedReceiver<DaemonPushEvent>), ClientError> {
    connect_daemon_tunnel_with_token(ws_url, None).await
}

pub async fn connect_daemon_tunnel_with_token(
    ws_url: &str,
    auth_token: Option<&str>,
) -> Result<(DaemonTunnelHandle, mpsc::UnboundedReceiver<DaemonPushEvent>), ClientError> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut req = ws_url
        .into_client_request()
        .map_err(|e| ClientError::WebSocket(Box::new(e)))?;
    if let Some(tok) = auth_token {
        if let Ok(val) = format!("Bearer {tok}").parse() {
            req.headers_mut().insert("Authorization", val);
        }
    }
    let (ws_stream, _) = connect_async(req)
        .await
        .map_err(|e| ClientError::WebSocket(Box::new(e)))?;
    let (mut ws_sink, mut ws_source) = ws_stream.split();

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<DaemonToServerMessage>();
    let (push_tx, push_rx) = mpsc::unbounded_channel::<DaemonPushEvent>();
    let pending: Arc<Mutex<HashMap<String, oneshot::Sender<ServerToDaemonMessage>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Ok(text) = serde_json::to_string(&msg) {
                if ws_sink.send(WsMessage::Text(text.into())).await.is_err() {
                    break;
                }
            }
        }
    });

    let pending_reader = pending.clone();
    tokio::spawn(async move {
        while let Some(Ok(frame)) = ws_source.next().await {
            let text = match frame {
                WsMessage::Text(t) => t.to_string(),
                WsMessage::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
                WsMessage::Close(_) => break,
                _ => continue,
            };

            let incoming: ServerToDaemonMessage = match serde_json::from_str(&text) {
                Ok(m) => m,
                Err(_) => continue,
            };

            match incoming {
                ServerToDaemonMessage::Registered { ref request_id, .. }
                | ServerToDaemonMessage::Ack { ref request_id, .. }
                | ServerToDaemonMessage::EffectiveContextReply { ref request_id, .. }
                | ServerToDaemonMessage::RealtimeMessagesReply { ref request_id, .. }
                | ServerToDaemonMessage::RolloverCompactedSessionReply { ref request_id, .. } => {
                    let req_id = request_id.clone();
                    let tx = pending_reader
                        .lock()
                        .expect("pending mutex poisoned")
                        .remove(&req_id);
                    if let Some(tx) = tx {
                        let _ = tx.send(incoming);
                    }
                }
                ServerToDaemonMessage::DispatchActivation {
                    session,
                    activation,
                    lease_epoch,
                } => {
                    let _ = push_tx.send(DaemonPushEvent::DispatchActivation {
                        session,
                        activation,
                        lease_epoch,
                    });
                }
                ServerToDaemonMessage::ReconcileActivation {
                    session_id,
                    activation_id,
                    lease_epoch,
                    desired_phase,
                    interaction_responses,
                } => {
                    let _ = push_tx.send(DaemonPushEvent::ReconcileActivation {
                        session_id,
                        activation_id,
                        lease_epoch,
                        desired_phase,
                        interaction_responses,
                    });
                }
                ServerToDaemonMessage::EventNotification { .. } => {}
            }
        }
        let _ = push_tx.send(DaemonPushEvent::Disconnected);
    });

    Ok((DaemonTunnelHandle { out_tx, pending }, push_rx))
}
