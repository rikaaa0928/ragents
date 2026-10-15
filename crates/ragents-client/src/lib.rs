use chrono::Utc;
use eventsource_stream::Eventsource;
use futures::{SinkExt, Stream, StreamExt};
use openresponses_rust::Item;
use ragents_protocol::*;
use reqwest::{Client, StatusCode};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use uuid::Uuid;

pub use ragents_protocol;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("websocket error: {0}")]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("api server error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("daemon tunnel error: {0}")]
    Tunnel(String),
}

// ============================================================================
// 1. Declarative API Server Client (CLI / SDK)
// ============================================================================

#[derive(Clone)]
pub struct RagentsClient {
    base_url: String,
    http: Client,
}

impl RagentsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let raw = base_url.into();
        let trimmed = raw.trim_end_matches('/').to_string();
        Self {
            base_url: trimmed,
            http: Client::new(),
        }
    }

    pub fn with_auth_token(mut self, auth_token: Option<String>) -> Self {
        if let Some(tok) = auth_token {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(val) = reqwest::header::HeaderValue::from_str(&format!("Bearer {tok}")) {
                headers.insert(reqwest::header::AUTHORIZATION, val);
                if let Ok(client) = Client::builder().default_headers(headers).build() {
                    self.http = client;
                }
            }
        }
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn ws_tunnel_url(&self) -> String {
        let ws_base = if let Some(rest) = self.base_url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = self.base_url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            format!("ws://{}", self.base_url)
        };
        format!("{ws_base}/api/v1/daemon/tunnel")
    }

    async fn parse_response<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, ClientError> {
        let status = resp.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let text = resp.text().await.unwrap_or_default();
            let message = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(text);
            return Err(ClientError::Api {
                status: status_code,
                message,
            });
        }
        Ok(resp.json::<T>().await?)
    }

    pub async fn list_nodes(&self) -> Result<Vec<Node>, ClientError> {
        let url = format!("{}/api/v1/nodes", self.base_url);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, ClientError> {
        let url = format!("{}/api/v1/nodes/{}", self.base_url, node_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn create_session(
        &self,
        req: CreateSessionRequest,
    ) -> Result<CreateSessionResponse, ClientError> {
        let url = format!("{}/api/v1/sessions", self.base_url);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn list_sessions(&self) -> Result<Vec<Session>, ClientError> {
        let url = format!("{}/api/v1/sessions", self.base_url);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<Session>, ClientError> {
        let url = format!("{}/api/v1/sessions/{}", self.base_url, session_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn patch_session(
        &self,
        session_id: &SessionId,
        patch: PatchSessionSpecRequest,
    ) -> Result<Session, ClientError> {
        let url = format!("{}/api/v1/sessions/{}", self.base_url, session_id);
        let resp = self.http.patch(url).json(&patch).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_session_context(
        &self,
        session_id: &SessionId,
    ) -> Result<SessionContextResponse, ClientError> {
        let url = format!("{}/api/v1/sessions/{}/context", self.base_url, session_id);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn list_session_activations(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Activation>, ClientError> {
        let url = format!(
            "{}/api/v1/sessions/{}/activations",
            self.base_url, session_id
        );
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn create_activation(
        &self,
        req: CreateActivationRequest,
    ) -> Result<Activation, ClientError> {
        let url = format!("{}/api/v1/activations", self.base_url);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn push_realtime_message(
        &self,
        session_id: &SessionId,
        req: PushRealtimeMessageRequest,
    ) -> Result<PushRealtimeMessageResponse, ClientError> {
        let url = format!("{}/api/v1/sessions/{}/realtime", self.base_url, session_id);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_activation(
        &self,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, ClientError> {
        let url = format!("{}/api/v1/activations/{}", self.base_url, activation_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn patch_activation(
        &self,
        activation_id: &ActivationId,
        patch: PatchActivationSpecRequest,
    ) -> Result<Activation, ClientError> {
        let url = format!("{}/api/v1/activations/{}", self.base_url, activation_id);
        let resp = self.http.patch(url).json(&patch).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn cancel_activation(
        &self,
        activation_id: &ActivationId,
    ) -> Result<Activation, ClientError> {
        self.patch_activation(
            activation_id,
            PatchActivationSpecRequest {
                desired_phase: Some(DesiredActivationPhase::Cancelled),
                interaction_responses: BTreeMap::new(),
            },
        )
        .await
    }

    pub async fn respond_permission(
        &self,
        activation_id: &ActivationId,
        request_id: PermissionRequestId,
        decision: PermissionDecision,
    ) -> Result<Activation, ClientError> {
        self.respond_permission_with_path(activation_id, request_id, decision, None)
            .await
    }

    pub async fn respond_permission_with_path(
        &self,
        activation_id: &ActivationId,
        request_id: PermissionRequestId,
        decision: PermissionDecision,
        override_path: Option<std::path::PathBuf>,
    ) -> Result<Activation, ClientError> {
        let mut interaction_responses = BTreeMap::new();
        interaction_responses.insert(
            request_id,
            PermissionResolutionSpec {
                decision,
                override_path,
                responded_at: Utc::now(),
            },
        );
        self.patch_activation(
            activation_id,
            PatchActivationSpecRequest {
                desired_phase: None,
                interaction_responses,
            },
        )
        .await
    }

    pub async fn read_events(
        &self,
        session_id: Option<&SessionId>,
        after_rv: u64,
    ) -> Result<Vec<SessionEventEnvelope>, ClientError> {
        let mut url = format!("{}/api/v1/events?after_rv={}", self.base_url, after_rv);
        if let Some(sess_id) = session_id {
            url.push_str(&format!("&session_id={sess_id}"));
        }
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn watch_events(
        &self,
        session_id: Option<&SessionId>,
        after_rv: u64,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<SessionEventEnvelope, ClientError>> + Send>>,
        ClientError,
    > {
        let mut url = format!("{}/api/v1/watch?after_rv={}", self.base_url, after_rv);
        if let Some(sess_id) = session_id {
            url.push_str(&format!("&session_id={sess_id}"));
        }
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status: status.as_u16(),
                message: text,
            });
        }

        let byte_stream = resp.bytes_stream().eventsource();
        let mapped = byte_stream.filter_map(|item| async move {
            match item {
                Ok(ev) => {
                    if ev.data.trim().is_empty() {
                        None
                    } else {
                        Some(
                            serde_json::from_str::<SessionEventEnvelope>(&ev.data)
                                .map_err(ClientError::Json),
                        )
                    }
                }
                Err(e) => Some(Err(ClientError::Tunnel(format!("sse stream error: {e}")))),
            }
        });

        Ok(Box::pin(mapped))
    }
}

// ============================================================================
// 2. Daemon WebSocket Tunnel Client (`agent-daemon` <-> `api-server`)
// ============================================================================

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

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_child_session(
        &self,
        parent_session_id: SessionId,
        parent_activation_id: ActivationId,
        parent_turn_id: Option<TurnId>,
        call_id: Option<String>,
        command_id: CommandId,
        child_request: CreateSessionRequest,
    ) -> Result<SessionId, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::SpawnChildSession {
                request_id,
                parent_session_id,
                parent_activation_id,
                parent_turn_id,
                call_id,
                command_id,
                child_request: Box::new(child_request),
            })
            .await?;
        let (_, _, spawned_id) = Self::expect_ack(resp)?;
        spawned_id.ok_or_else(|| ClientError::Tunnel("missing spawned_session_id in Ack".into()))
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
            | DaemonToServerMessage::SpawnChildSession { request_id, .. }
            | DaemonToServerMessage::CompleteActivation { request_id, .. } => request_id,
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
                | ServerToDaemonMessage::RealtimeMessagesReply { ref request_id, .. } => {
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
