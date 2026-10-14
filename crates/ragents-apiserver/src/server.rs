use crate::store::{ReconcileNotice, ScheduledDispatch, SqliteStore, StoreError};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{
        sse::{Event as SseEvent, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use futures::{SinkExt, Stream, StreamExt};
use ragents_protocol::*;
use serde_json::json;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;

#[derive(Clone)]
pub struct ApiServerState {
    pub store: SqliteStore,
    pub lease_ttl_ms: u64,
    daemon_peers: Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<ServerToDaemonMessage>>>>,
}

impl ApiServerState {
    pub fn new(store: SqliteStore, lease_ttl_ms: u64) -> Self {
        let state = Self {
            store,
            lease_ttl_ms,
            daemon_peers: Arc::new(Mutex::new(HashMap::new())),
        };
        state.spawn_background_workers();
        state
    }

    fn spawn_background_workers(&self) {
        let mut dispatch_rx = self.store.subscribe_dispatches();
        let peers_for_dispatch = self.daemon_peers.clone();
        tokio::spawn(async move {
            while let Ok(ScheduledDispatch {
                node_id,
                session,
                activation,
                lease_epoch,
            }) = dispatch_rx.recv().await
            {
                let maybe_tx = peers_for_dispatch
                    .lock()
                    .expect("peers mutex poisoned")
                    .get(&node_id)
                    .cloned();
                if let Some(tx) = maybe_tx {
                    let _ = tx.send(ServerToDaemonMessage::DispatchActivation {
                        session: Box::new(session),
                        activation: Box::new(activation),
                        lease_epoch,
                    });
                }
            }
        });

        let mut reconcile_rx = self.store.subscribe_reconciles();
        let peers_for_reconcile = self.daemon_peers.clone();
        tokio::spawn(async move {
            while let Ok(ReconcileNotice {
                node_id,
                session_id,
                activation_id,
                lease_epoch,
                desired_phase,
                interaction_responses,
            }) = reconcile_rx.recv().await
            {
                let maybe_tx = peers_for_reconcile
                    .lock()
                    .expect("peers mutex poisoned")
                    .get(&node_id)
                    .cloned();
                if let Some(tx) = maybe_tx {
                    let _ = tx.send(ServerToDaemonMessage::ReconcileActivation {
                        session_id,
                        activation_id,
                        lease_epoch,
                        desired_phase,
                        interaction_responses,
                    });
                }
            }
        });
    }

    pub fn spawn_lease_patrol(&self, interval: Duration, cancel: CancellationToken) {
        let store = self.store.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = store.patrol_expired_leases(Utc::now());
                    }
                    _ = cancel.cancelled() => break,
                }
            }
        });
    }
}

pub fn build_router(state: ApiServerState) -> Router {
    Router::new()
        .route("/api/v1/nodes", get(handle_list_nodes))
        .route("/api/v1/nodes/{node_id}", get(handle_get_node))
        .route(
            "/api/v1/sessions",
            post(handle_create_session).get(handle_list_sessions),
        )
        .route(
            "/api/v1/sessions/{session_id}",
            get(handle_get_session).patch(handle_patch_session),
        )
        .route(
            "/api/v1/sessions/{session_id}/context",
            get(handle_get_session_context),
        )
        .route(
            "/api/v1/sessions/{session_id}/activations",
            get(handle_list_session_activations),
        )
        .route("/api/v1/activations", post(handle_create_activation))
        .route(
            "/api/v1/activations/{activation_id}",
            get(handle_get_activation).patch(handle_patch_activation),
        )
        .route("/api/v1/events", get(handle_read_events))
        .route("/api/v1/watch", get(handle_watch_events))
        .route("/api/v1/daemon/tunnel", get(handle_daemon_tunnel))
        .with_state(state)
}

pub async fn serve_tcp(
    listener: TcpListener,
    state: ApiServerState,
    cancel: CancellationToken,
) -> anyhow::Result<SocketAddr> {
    let addr = listener.local_addr()?;
    let patrol_interval = Duration::from_millis((state.lease_ttl_ms / 3).max(500));
    state.spawn_lease_patrol(patrol_interval, cancel.clone());
    let app = build_router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel.cancelled().await;
            })
            .await;
    });
    Ok(addr)
}

// ============================================================================
// HTTP REST Handlers
// ============================================================================

struct ApiError(StoreError);

impl From<StoreError> for ApiError {
    fn from(value: StoreError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Validation(_) => StatusCode::BAD_REQUEST,
            StoreError::Conflict(_)
            | StoreError::StaleLeaseEpoch { .. }
            | StoreError::UnsupportedModelOnNode { .. } => StatusCode::CONFLICT,
            StoreError::Sqlite(_) | StoreError::Json(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(json!({
            "error": self.0.to_string(),
        }));
        (status, body).into_response()
    }
}

async fn handle_list_nodes(
    State(state): State<ApiServerState>,
) -> Result<Json<Vec<Node>>, ApiError> {
    let nodes = state.store.list_nodes()?;
    Ok(Json(nodes))
}

async fn handle_get_node(
    State(state): State<ApiServerState>,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    let id = NodeId::new(node_id);
    let node = state
        .store
        .get_node(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("node {id}")))?;
    Ok(Json(node))
}

async fn handle_create_session(
    State(state): State<ApiServerState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<CreateSessionResponse>), ApiError> {
    let (resp, _) = state.store.create_session(req)?;
    Ok((StatusCode::CREATED, Json(resp)))
}

async fn handle_list_sessions(
    State(state): State<ApiServerState>,
) -> Result<Json<Vec<Session>>, ApiError> {
    let sessions = state.store.list_sessions()?;
    Ok(Json(sessions))
}

async fn handle_get_session(
    State(state): State<ApiServerState>,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    let id = SessionId::new(session_id);
    let session = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    Ok(Json(session))
}

async fn handle_patch_session(
    State(state): State<ApiServerState>,
    Path(session_id): Path<String>,
    Json(patch): Json<PatchSessionSpecRequest>,
) -> Result<Json<Session>, ApiError> {
    let id = SessionId::new(session_id);
    let session = state.store.patch_session(&id, patch)?;
    Ok(Json(session))
}

async fn handle_get_session_context(
    State(state): State<ApiServerState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionContextResponse>, ApiError> {
    let id = SessionId::new(session_id);
    let ctx = state.store.read_effective_context(&id)?;
    Ok(Json(ctx))
}

async fn handle_list_session_activations(
    State(state): State<ApiServerState>,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<Activation>>, ApiError> {
    let id = SessionId::new(session_id);
    let activations = state.store.list_session_activations(&id)?;
    Ok(Json(activations))
}

async fn handle_create_activation(
    State(state): State<ApiServerState>,
    Json(req): Json<CreateActivationRequest>,
) -> Result<(StatusCode, Json<Activation>), ApiError> {
    let (act, _) = state.store.create_activation(req)?;
    Ok((StatusCode::CREATED, Json(act)))
}

async fn handle_get_activation(
    State(state): State<ApiServerState>,
    Path(activation_id): Path<String>,
) -> Result<Json<Activation>, ApiError> {
    let id = ActivationId::new(activation_id);
    let act = state
        .store
        .get_activation(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("activation {id}")))?;
    Ok(Json(act))
}

async fn handle_patch_activation(
    State(state): State<ApiServerState>,
    Path(activation_id): Path<String>,
    Json(patch): Json<PatchActivationSpecRequest>,
) -> Result<Json<Activation>, ApiError> {
    let id = ActivationId::new(activation_id);
    let outcome = state.store.patch_activation(&id, patch)?;
    Ok(Json(outcome.activation))
}

async fn handle_read_events(
    State(state): State<ApiServerState>,
    Query(query): Query<WatchQuery>,
) -> Result<Json<Vec<SessionEventEnvelope>>, ApiError> {
    let events = state
        .store
        .read_events_after(query.session_id.as_ref(), query.after_rv.unwrap_or(0))?;
    Ok(Json(events))
}

// ============================================================================
// SSE Watch Handler (`/api/v1/watch`)
// ============================================================================

async fn handle_watch_events(
    State(state): State<ApiServerState>,
    Query(query): Query<WatchQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let live_rx = state.store.subscribe_events();
    let after_rv = query.after_rv.unwrap_or(0);
    let filter_session = query.session_id.clone();

    let historical = state
        .store
        .read_events_after(filter_session.as_ref(), after_rv)?;

    let stream = async_stream_events(historical, live_rx, filter_session, after_rv);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn async_stream_events(
    historical: Vec<SessionEventEnvelope>,
    mut live_rx: tokio::sync::broadcast::Receiver<SessionEventEnvelope>,
    filter_session: Option<SessionId>,
    after_rv: u64,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    let (tx, rx) = mpsc::unbounded_channel::<Result<SseEvent, Infallible>>();

    tokio::spawn(async move {
        let mut highest_rv = after_rv;
        for ev in historical {
            let rv = ev.resource_version.value();
            if rv > highest_rv {
                highest_rv = rv;
            }
            if let Ok(json_str) = serde_json::to_string(&ev) {
                let sse = SseEvent::default()
                    .id(rv.to_string())
                    .event(ev.event.kind_name())
                    .data(json_str);
                if tx.send(Ok(sse)).is_err() {
                    return;
                }
            }
        }

        while let Ok(ev) = live_rx.recv().await {
            let rv = ev.resource_version.value();
            if rv <= highest_rv {
                continue;
            }
            if let Some(ref target_sess) = filter_session {
                if &ev.session_id != target_sess {
                    continue;
                }
            }
            highest_rv = rv;
            if let Ok(json_str) = serde_json::to_string(&ev) {
                let sse = SseEvent::default()
                    .id(rv.to_string())
                    .event(ev.event.kind_name())
                    .data(json_str);
                if tx.send(Ok(sse)).is_err() {
                    return;
                }
            }
        }
    });

    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

// ============================================================================
// Daemon WebSocket Tunnel Handler (`/api/v1/daemon/tunnel`)
// ============================================================================

async fn handle_daemon_tunnel(
    ws: WebSocketUpgrade,
    State(state): State<ApiServerState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run_daemon_tunnel_socket(socket, state))
}

async fn run_daemon_tunnel_socket(socket: WebSocket, state: ApiServerState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerToDaemonMessage>();

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Ok(text) = serde_json::to_string(&msg) {
                if ws_sender.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
        }
    });

    let mut registered_node_id: Option<NodeId> = None;

    while let Some(Ok(frame)) = ws_receiver.next().await {
        let text = match frame {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                Ok(s) => s,
                Err(_) => continue,
            },
            Message::Close(_) => break,
            _ => continue,
        };

        let incoming: DaemonToServerMessage = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(_) => continue,
        };

        match incoming {
            DaemonToServerMessage::RegisterNode {
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
            } => {
                // Register peer channel first so any dispatches triggered during register_node are delivered
                state
                    .daemon_peers
                    .lock()
                    .expect("peers mutex poisoned")
                    .insert(node_id.clone(), out_tx.clone());
                registered_node_id = Some(node_id.clone());

                match state.store.register_node(
                    node_id.clone(),
                    hostname,
                    os,
                    arch,
                    labels,
                    supported_models,
                    default_model,
                    installed_extensions,
                    max_concurrent_runners,
                    state.lease_ttl_ms,
                ) {
                    Ok((node, _dispatches)) => {
                        let _ = out_tx.send(ServerToDaemonMessage::Registered {
                            request_id,
                            node_id,
                            lease_ttl_ms: state.lease_ttl_ms,
                            resource_version: node.metadata.resource_version,
                        });
                    }
                    Err(e) => {
                        let _ = out_tx.send(ServerToDaemonMessage::Ack {
                            request_id,
                            ok: false,
                            resource_version: None,
                            batch_seq: None,
                            spawned_session_id: None,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
            DaemonToServerMessage::Heartbeat {
                request_id,
                node_id,
                active_runners,
            } => match state
                .store
                .heartbeat_node(&node_id, active_runners, state.lease_ttl_ms)
            {
                Ok((node, _)) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: true,
                        resource_version: Some(node.metadata.resource_version),
                        batch_seq: None,
                        spawned_session_id: None,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: false,
                        resource_version: None,
                        batch_seq: None,
                        spawned_session_id: None,
                        error: Some(e.to_string()),
                    });
                }
            },
            DaemonToServerMessage::AcceptActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                effective_config,
                effective_permission,
            } => match state.store.accept_activation(
                &session_id,
                &activation_id,
                lease_epoch,
                *effective_config,
                *effective_permission,
            ) {
                Ok(act) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: true,
                        resource_version: Some(act.metadata.resource_version),
                        batch_seq: None,
                        spawned_session_id: None,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: false,
                        resource_version: None,
                        batch_seq: None,
                        spawned_session_id: None,
                        error: Some(e.to_string()),
                    });
                }
            },
            DaemonToServerMessage::CommitBatch {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                kind,
                turn_id,
                response_id,
                items,
                companion_event,
            } => match state.store.commit_batch(
                &session_id,
                &activation_id,
                lease_epoch,
                kind,
                turn_id,
                response_id,
                items,
                companion_event,
            ) {
                Ok((batch_seq, rv)) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: true,
                        resource_version: Some(rv),
                        batch_seq: Some(batch_seq),
                        spawned_session_id: None,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: false,
                        resource_version: None,
                        batch_seq: None,
                        spawned_session_id: None,
                        error: Some(e.to_string()),
                    });
                }
            },
            DaemonToServerMessage::EmitEvent {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                event,
            } => match state.store.emit_daemon_event(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
                event,
            ) {
                Ok(env) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: true,
                        resource_version: Some(env.resource_version),
                        batch_seq: None,
                        spawned_session_id: None,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: false,
                        resource_version: None,
                        batch_seq: None,
                        spawned_session_id: None,
                        error: Some(e.to_string()),
                    });
                }
            },
            DaemonToServerMessage::FetchEffectiveContext {
                request_id,
                session_id,
            } => match state.store.read_effective_context(&session_id) {
                Ok(ctx) => {
                    let _ = out_tx.send(ServerToDaemonMessage::EffectiveContextReply {
                        request_id,
                        ok: true,
                        items: ctx.effective_items,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::EffectiveContextReply {
                        request_id,
                        ok: false,
                        items: vec![],
                        error: Some(e.to_string()),
                    });
                }
            },
            DaemonToServerMessage::SpawnChildSession {
                request_id,
                parent_session_id,
                parent_activation_id,
                parent_turn_id,
                call_id,
                command_id,
                mut child_request,
            } => {
                child_request.spec.producer = Some(ProducerRef {
                    session_id: parent_session_id.clone(),
                    activation_id: parent_activation_id.clone(),
                    turn_id: parent_turn_id.clone(),
                    call_id,
                    command_id: command_id.clone(),
                });
                match state.store.create_session(*child_request) {
                    Ok((created, _)) => {
                        let child_id = SessionId::new(created.session.metadata.id);
                        let _ = out_tx.send(ServerToDaemonMessage::Ack {
                            request_id,
                            ok: true,
                            resource_version: Some(created.session.metadata.resource_version),
                            batch_seq: None,
                            spawned_session_id: Some(child_id),
                            error: None,
                        });
                    }
                    Err(e) => {
                        let _ = out_tx.send(ServerToDaemonMessage::Ack {
                            request_id,
                            ok: false,
                            resource_version: None,
                            batch_seq: None,
                            spawned_session_id: None,
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
            DaemonToServerMessage::CompleteActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                outcome,
            } => match state.store.complete_activation(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
                outcome,
            ) {
                Ok((act, _)) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: true,
                        resource_version: Some(act.metadata.resource_version),
                        batch_seq: None,
                        spawned_session_id: None,
                        error: None,
                    });
                }
                Err(e) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Ack {
                        request_id,
                        ok: false,
                        resource_version: None,
                        batch_seq: None,
                        spawned_session_id: None,
                        error: Some(e.to_string()),
                    });
                }
            },
        }
    }

    if let Some(node_id) = registered_node_id {
        state
            .daemon_peers
            .lock()
            .expect("peers mutex poisoned")
            .remove(&node_id);
    }
    writer_task.abort();
}
