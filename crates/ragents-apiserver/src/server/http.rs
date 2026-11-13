use super::{ApiError, ApiServerState};
use crate::store::StoreError;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    Json,
};
use futures::Stream;
use ragents_protocol::*;
use std::convert::Infallible;
use tokio::sync::mpsc;

pub(crate) async fn handle_list_nodes(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Node>>, ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/nodes", &headers)
        .await?;
    let nodes = state.store.list_nodes()?;
    Ok(Json(nodes))
}

pub(crate) async fn handle_list_models(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<ClusterModelsResponse>, ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/models", &headers)
        .await?;
    let catalog = state.model_catalog.list_models(&state.store)?;
    Ok(Json(catalog))
}

pub(crate) async fn handle_refresh_models(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<ClusterModelsResponse>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            "/api/v1/models/refresh",
            &headers,
        )
        .await?;
    let catalog = state
        .model_catalog
        .refresh(Some(&state.store))
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(Json(catalog))
}

pub(crate) async fn handle_get_node(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/nodes/{node_id}"),
            &headers,
        )
        .await?;
    let id = NodeId::new(node_id);
    let node = state
        .store
        .get_node(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("node {id}")))?;
    Ok(Json(node))
}

pub(crate) async fn handle_patch_node(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(patch): Json<PatchNodeSpecRequest>,
) -> Result<Json<Node>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "PATCH",
            &format!("/api/v1/nodes/{node_id}"),
            &headers,
        )
        .await?;
    let id = NodeId::new(node_id);
    let updated = state.store.patch_node(&id, patch)?;
    Ok(Json(updated))
}

pub(crate) async fn handle_node_extension_call(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(req): Json<NodeExtensionCallRequest>,
) -> Result<Json<NodeExtensionCallResponse>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/nodes/{node_id}/extensions/call"),
            &headers,
        )
        .await?;
    let id = NodeId::new(node_id);
    let _node = state
        .store
        .get_node(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("node {id}")))?;

    let maybe_tx = state
        .daemon_peers
        .lock()
        .expect("peers mutex poisoned")
        .get(&id)
        .cloned();
    let Some(daemon_tx) = maybe_tx else {
        return Ok(Json(NodeExtensionCallResponse {
            ok: false,
            error: Some(format!("Node '{id}' is currently offline / not connected")),
            ..Default::default()
        }));
    };

    let request_id = format!("extreq_{}", uuid::Uuid::now_v7());
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .pending_node_ext
        .lock()
        .expect("pending_node_ext mutex poisoned")
        .insert(request_id.clone(), tx);

    if daemon_tx
        .send(ServerToDaemonMessage::NodeExtensionCall {
            request_id: request_id.clone(),
            request: req,
        })
        .is_err()
    {
        state
            .pending_node_ext
            .lock()
            .expect("pending_node_ext mutex poisoned")
            .remove(&request_id);
        return Ok(Json(NodeExtensionCallResponse {
            ok: false,
            error: Some(format!("Failed to send request to node '{id}' tunnel")),
            ..Default::default()
        }));
    }

    match tokio::time::timeout(std::time::Duration::from_secs(600), rx).await {
        Ok(Ok(resp)) => Ok(Json(resp)),
        Ok(Err(_)) => Ok(Json(NodeExtensionCallResponse {
            ok: false,
            error: Some("Node tunnel closed while waiting for extension call".to_string()),
            ..Default::default()
        })),
        Err(_) => {
            state
                .pending_node_ext
                .lock()
                .expect("pending_node_ext mutex poisoned")
                .remove(&request_id);
            Ok(Json(NodeExtensionCallResponse {
                ok: false,
                error: Some("Node extension call timed out after 600s".to_string()),
                ..Default::default()
            }))
        }
    }
}

pub(crate) async fn handle_create_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<CreateSessionResponse>), ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "POST", "/api/v1/sessions", &headers)
        .await?;
    let (resp, _) = state.store.create_session(req)?;
    Ok((StatusCode::CREATED, Json(resp)))
}

pub(crate) async fn handle_list_sessions(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Session>>, ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/sessions", &headers)
        .await?;
    let sessions = state.store.list_sessions()?;
    Ok(Json(sessions))
}

pub(crate) async fn handle_get_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let session = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    Ok(Json(session))
}

pub(crate) async fn handle_patch_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(patch): Json<PatchSessionSpecRequest>,
) -> Result<Json<Session>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "PATCH",
            &format!("/api/v1/sessions/{session_id}"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let session = state.store.patch_session(&id, patch)?;
    Ok(Json(session))
}

pub(crate) async fn handle_get_session_context(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ContextQuery>,
) -> Result<Json<SessionContextResponse>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}/context"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let _existing = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let ctx = state.store.read_effective_context_with_options(
        &id,
        query.mode,
        query.include_batches.unwrap_or(true),
    )?;
    Ok(Json(ctx))
}

pub(crate) async fn handle_query_workspace_session_history(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<SessionHistoryQueryRequest>,
) -> Result<Json<SessionHistoryQueryResponse>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/sessions/{session_id}/workspace-history"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let resp = state.store.query_workspace_session_history(&id, req)?;
    Ok(Json(resp))
}

pub(crate) async fn handle_list_session_activations(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<Activation>>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}/activations"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let _existing = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let activations = state.store.list_session_activations(&id)?;
    Ok(Json(activations))
}

pub(crate) async fn handle_create_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Json(req): Json<CreateActivationRequest>,
) -> Result<(StatusCode, Json<Activation>), ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            "/api/v1/activations",
            &headers,
        )
        .await?;
    let (act, _) = state.store.create_activation(req)?;
    Ok((StatusCode::CREATED, Json(act)))
}

pub(crate) async fn handle_push_realtime_message(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<PushRealtimeMessageRequest>,
) -> Result<(StatusCode, Json<PushRealtimeMessageResponse>), ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/sessions/{session_id}/realtime"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let resp = state
        .store
        .push_realtime_message_with_options(&id, req.content, req.disabled_tools)?;
    Ok((StatusCode::CREATED, Json(resp)))
}

pub(crate) async fn handle_withdraw_realtime_message(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<WithdrawRealtimeMessageRequest>,
) -> Result<Json<WithdrawRealtimeMessageResponse>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/sessions/{session_id}/realtime/withdraw"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id);
    let resp = state.store.withdraw_realtime_messages(&id, req)?;
    Ok(Json(resp))
}

pub(crate) async fn handle_withdraw_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
) -> Result<Json<Activation>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/activations/{activation_id}/withdraw"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id);
    let act = state.store.withdraw_queued_activation(&id)?;
    Ok(Json(act))
}

pub(crate) async fn handle_get_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
) -> Result<Json<Activation>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/activations/{activation_id}"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id);
    let act = state
        .store
        .get_activation(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("activation {id}")))?;
    Ok(Json(act))
}

pub(crate) async fn handle_patch_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
    Json(patch): Json<PatchActivationSpecRequest>,
) -> Result<Json<Activation>, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "PATCH",
            &format!("/api/v1/activations/{activation_id}"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id);
    let outcome = state.store.patch_activation(&id, patch)?;
    Ok(Json(outcome.activation))
}

pub(crate) async fn handle_read_events(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Query(query): Query<WatchQuery>,
) -> Result<Json<Vec<SessionEventEnvelope>>, ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/events", &headers)
        .await?;
    let events = state.store.read_events_after_with_mode(
        query.session_id.as_ref(),
        query.after_rv.unwrap_or(0),
        query.mode,
    )?;
    Ok(Json(events))
}

pub(crate) async fn handle_watch_events(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Query(query): Query<WatchQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/watch", &headers)
        .await?;
    let live_rx = state.store.subscribe_events();
    let after_rv = query.after_rv.unwrap_or(0);
    let filter_session = query.session_id.clone();
    let mode = query.mode;

    let historical = state.store.read_events_after_with_mode(
        filter_session.as_ref(),
        after_rv,
        mode,
    )?;

    let stream = async_stream_events(historical, live_rx, filter_session, after_rv, mode);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn async_stream_events(
    historical: Vec<SessionEventEnvelope>,
    mut live_rx: tokio::sync::broadcast::Receiver<SessionEventEnvelope>,
    filter_session: Option<SessionId>,
    after_rv: u64,
    mode: ContentFetchMode,
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

        while let Ok(mut ev) = live_rx.recv().await {
            let is_ephemeral = ev.event.is_ephemeral_stream_delta();
            let rv = ev.resource_version.value();
            if !is_ephemeral && rv <= highest_rv {
                continue;
            }
            if let Some(ref target_sess) = filter_session {
                if &ev.session_id != target_sess {
                    continue;
                }
            }
            if !is_ephemeral {
                highest_rv = rv;
            }
            if mode == ContentFetchMode::View {
                ev.apply_view_mode();
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
    });

    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}
