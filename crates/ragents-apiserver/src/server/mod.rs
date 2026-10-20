pub mod http;
pub mod tunnel;

use crate::extensions::ApiServerExtensions;
use crate::store::{ReconcileNotice, ScheduledDispatch, SqliteStore, StoreError};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use ragents_protocol::*;
use serde_json::json;
use std::{
    collections::HashMap,
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
    pub extensions: ApiServerExtensions,
    pub(crate) daemon_peers:
        Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<ServerToDaemonMessage>>>>,
    pub(crate) shutdown: CancellationToken,
}

impl ApiServerState {
    pub fn new(store: SqliteStore, lease_ttl_ms: u64) -> Self {
        let state = Self {
            store,
            lease_ttl_ms,
            extensions: ApiServerExtensions::default(),
            daemon_peers: Arc::new(Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
        };
        state.spawn_background_workers();
        state
    }

    pub fn with_extensions(mut self, extensions: ApiServerExtensions) -> Self {
        self.extensions = extensions;
        self
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
                cluster_system_prompt,
                node_spec,
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
                        cluster_system_prompt,
                        node_spec: Box::new(node_spec),
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

        let mut event_rx = self.store.subscribe_events();
        let peers_for_cleanup = self.daemon_peers.clone();
        tokio::spawn(async move {
            while let Ok(env) = event_rx.recv().await {
                if matches!(
                    env.event,
                    SessionEvent::SessionClosed | SessionEvent::SessionArchived
                ) {
                    let senders: Vec<mpsc::UnboundedSender<ServerToDaemonMessage>> =
                        peers_for_cleanup
                            .lock()
                            .expect("peers mutex poisoned")
                            .values()
                            .cloned()
                            .collect();
                    for tx in senders {
                        let _ = tx.send(ServerToDaemonMessage::CleanupSession {
                            session_id: env.session_id.clone(),
                        });
                    }
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
        .route("/api/v1/nodes", get(http::handle_list_nodes))
        .route("/api/v1/nodes/{node_id}", get(http::handle_get_node))
        .route(
            "/api/v1/sessions",
            post(http::handle_create_session).get(http::handle_list_sessions),
        )
        .route(
            "/api/v1/sessions/{session_id}",
            get(http::handle_get_session).patch(http::handle_patch_session),
        )
        .route(
            "/api/v1/sessions/{session_id}/context",
            get(http::handle_get_session_context),
        )
        .route(
            "/api/v1/sessions/{session_id}/activations",
            get(http::handle_list_session_activations),
        )
        .route(
            "/api/v1/sessions/{session_id}/realtime",
            post(http::handle_push_realtime_message),
        )
        .route("/api/v1/activations", post(http::handle_create_activation))
        .route(
            "/api/v1/activations/{activation_id}",
            get(http::handle_get_activation).patch(http::handle_patch_activation),
        )
        .route("/api/v1/events", get(http::handle_read_events))
        .route("/api/v1/watch", get(http::handle_watch_events))
        .route("/api/v1/daemon/tunnel", get(tunnel::handle_daemon_tunnel))
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
    let state_shutdown = state.shutdown.clone();
    let cancel_for_ws = cancel.clone();
    tokio::spawn(async move {
        cancel_for_ws.cancelled().await;
        state_shutdown.cancel();
    });
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

pub(crate) enum ApiError {
    Store(StoreError),
    Http(StatusCode, String),
}

impl From<StoreError> for ApiError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<(StatusCode, String)> for ApiError {
    fn from((code, msg): (StatusCode, String)) -> Self {
        Self::Http(code, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Store(err) => {
                let status = match &err {
                    StoreError::NotFound(_) => StatusCode::NOT_FOUND,
                    StoreError::Validation(_) => StatusCode::BAD_REQUEST,
                    StoreError::Conflict(_)
                    | StoreError::StaleLeaseEpoch { .. }
                    | StoreError::UnsupportedModelOnNode { .. } => StatusCode::CONFLICT,
                    StoreError::Sqlite(_) | StoreError::Json(_) => {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                };
                let body = Json(json!({
                    "error": err.to_string(),
                }));
                (status, body).into_response()
            }
            Self::Http(status, msg) => {
                let body = Json(json!({
                    "error": msg,
                }));
                (status, body).into_response()
            }
        }
    }
}
