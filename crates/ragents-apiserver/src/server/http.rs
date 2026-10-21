use super::{ApiError, ApiServerState};
use crate::extensions::labels_match_selector;
use crate::store::{SqliteStore, StoreError};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    Json,
};
use futures::Stream;
use ragents_protocol::*;
use std::{collections::BTreeMap, convert::Infallible};
use tokio::sync::mpsc;

pub(crate) async fn handle_list_nodes(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Node>>, ApiError> {
    let principal = state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/nodes", &headers)
        .await?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::List,
            ResourceKind::Node,
            None,
            &BTreeMap::new(),
        )
        .await?;
    let nodes = state
        .store
        .list_nodes()?
        .into_iter()
        .filter(|n| labels_match_selector(&n.metadata.labels, &selector))
        .collect();
    Ok(Json(nodes))
}

pub(crate) async fn handle_get_node(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/nodes/{node_id}"),
            &headers,
        )
        .await?;
    let id = NodeId::new(node_id.clone());
    let node = state
        .store
        .get_node(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("node {id}")))?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Get,
            ResourceKind::Node,
            Some(&node_id),
            &node.metadata.labels,
        )
        .await?;
    if !labels_match_selector(&node.metadata.labels, &selector) {
        return Err(ApiError::Http(
            StatusCode::FORBIDDEN,
            "Forbidden by label selector".into(),
        ));
    }
    Ok(Json(node))
}

pub(crate) async fn handle_create_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<CreateSessionResponse>), ApiError> {
    let principal = state
        .extensions
        .authenticate(AuthScope::ClientHttp, "POST", "/api/v1/sessions", &headers)
        .await?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Create,
            ResourceKind::Session,
            req.session_id.as_ref().map(|s| s.as_str()),
            &req.labels,
        )
        .await?;
    let admitted = state
        .extensions
        .admit(&principal, AdmissionOperation::CreateSession, req)
        .await?;
    let (resp, _) = state.store.create_session(admitted)?;
    Ok((StatusCode::CREATED, Json(resp)))
}

pub(crate) async fn handle_list_sessions(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Session>>, ApiError> {
    let principal = state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/sessions", &headers)
        .await?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::List,
            ResourceKind::Session,
            None,
            &BTreeMap::new(),
        )
        .await?;
    let sessions = state
        .store
        .list_sessions()?
        .into_iter()
        .filter(|s| labels_match_selector(&s.metadata.labels, &selector))
        .collect();
    Ok(Json(sessions))
}

pub(crate) async fn handle_get_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let session = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Get,
            ResourceKind::Session,
            Some(&session_id),
            &session.metadata.labels,
        )
        .await?;
    if !labels_match_selector(&session.metadata.labels, &selector) {
        return Err(ApiError::Http(
            StatusCode::FORBIDDEN,
            "Forbidden by tenant label selector".into(),
        ));
    }
    Ok(Json(session))
}

pub(crate) async fn handle_patch_session(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(patch): Json<PatchSessionSpecRequest>,
) -> Result<Json<Session>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "PATCH",
            &format!("/api/v1/sessions/{session_id}"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let existing = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Patch,
            ResourceKind::Session,
            Some(&session_id),
            &existing.metadata.labels,
        )
        .await?;
    if !labels_match_selector(&existing.metadata.labels, &selector) {
        return Err(ApiError::Http(
            StatusCode::FORBIDDEN,
            "Forbidden by tenant label selector".into(),
        ));
    }
    let admitted = state
        .extensions
        .admit(&principal, AdmissionOperation::PatchSession, patch)
        .await?;
    let session = state.store.patch_session(&id, admitted)?;
    Ok(Json(session))
}

pub(crate) async fn handle_get_session_context(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<SessionContextResponse>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}/context"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let existing = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Get,
            ResourceKind::Session,
            Some(&session_id),
            &existing.metadata.labels,
        )
        .await?;
    if !labels_match_selector(&existing.metadata.labels, &selector) {
        return Err(ApiError::Http(
            StatusCode::FORBIDDEN,
            "Forbidden by tenant label selector".into(),
        ));
    }
    let ctx = state.store.read_effective_context(&id)?;
    Ok(Json(ctx))
}

pub(crate) async fn handle_list_session_activations(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<Activation>>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/sessions/{session_id}/activations"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let existing = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::List,
            ResourceKind::Activation,
            Some(&session_id),
            &existing.metadata.labels,
        )
        .await?;
    if !labels_match_selector(&existing.metadata.labels, &selector) {
        return Err(ApiError::Http(
            StatusCode::FORBIDDEN,
            "Forbidden by tenant label selector".into(),
        ));
    }
    let activations = state.store.list_session_activations(&id)?;
    Ok(Json(activations))
}

pub(crate) async fn handle_create_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Json(req): Json<CreateActivationRequest>,
) -> Result<(StatusCode, Json<Activation>), ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            "/api/v1/activations",
            &headers,
        )
        .await?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Create,
            ResourceKind::Activation,
            req.activation_id.as_ref().map(|a| a.as_str()),
            &req.labels,
        )
        .await?;
    let admitted = state
        .extensions
        .admit(&principal, AdmissionOperation::CreateActivation, req)
        .await?;
    let (act, _) = state.store.create_activation(admitted)?;
    Ok((StatusCode::CREATED, Json(act)))
}

pub(crate) async fn handle_push_realtime_message(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<PushRealtimeMessageRequest>,
) -> Result<(StatusCode, Json<PushRealtimeMessageResponse>), ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/sessions/{session_id}/realtime"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let sess = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Patch,
            ResourceKind::Session,
            Some(&session_id),
            &sess.metadata.labels,
        )
        .await?;
    let admitted = state
        .extensions
        .admit(&principal, AdmissionOperation::PushRealtimeMessage, req)
        .await?;
    let resp = state.store.push_realtime_message_with_options(
        &id,
        admitted.content,
        admitted.disabled_tools,
    )?;
    Ok((StatusCode::CREATED, Json(resp)))
}

pub(crate) async fn handle_withdraw_realtime_message(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(req): Json<WithdrawRealtimeMessageRequest>,
) -> Result<Json<WithdrawRealtimeMessageResponse>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/sessions/{session_id}/realtime/withdraw"),
            &headers,
        )
        .await?;
    let id = SessionId::new(session_id.clone());
    let sess = state
        .store
        .get_session(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("session {id}")))?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Patch,
            ResourceKind::Session,
            Some(&session_id),
            &sess.metadata.labels,
        )
        .await?;
    let resp = state.store.withdraw_realtime_messages(&id, req)?;
    Ok(Json(resp))
}

pub(crate) async fn handle_withdraw_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
) -> Result<Json<Activation>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "POST",
            &format!("/api/v1/activations/{activation_id}/withdraw"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id.clone());
    let existing = state
        .store
        .get_activation(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("activation {id}")))?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Patch,
            ResourceKind::Activation,
            Some(&activation_id),
            &existing.metadata.labels,
        )
        .await?;
    let act = state.store.withdraw_queued_activation(&id)?;
    Ok(Json(act))
}

pub(crate) async fn handle_get_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
) -> Result<Json<Activation>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "GET",
            &format!("/api/v1/activations/{activation_id}"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id.clone());
    let act = state
        .store
        .get_activation(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("activation {id}")))?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Get,
            ResourceKind::Activation,
            Some(&activation_id),
            &act.metadata.labels,
        )
        .await?;
    Ok(Json(act))
}

pub(crate) async fn handle_patch_activation(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Path(activation_id): Path<String>,
    Json(patch): Json<PatchActivationSpecRequest>,
) -> Result<Json<Activation>, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::ClientHttp,
            "PATCH",
            &format!("/api/v1/activations/{activation_id}"),
            &headers,
        )
        .await?;
    let id = ActivationId::new(activation_id.clone());
    let existing = state
        .store
        .get_activation(&id)?
        .ok_or_else(|| StoreError::NotFound(format!("activation {id}")))?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Patch,
            ResourceKind::Activation,
            Some(&activation_id),
            &existing.metadata.labels,
        )
        .await?;
    let admitted = state
        .extensions
        .admit(&principal, AdmissionOperation::PatchActivation, patch)
        .await?;
    let outcome = state.store.patch_activation(&id, admitted)?;
    Ok(Json(outcome.activation))
}

pub(crate) async fn handle_read_events(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Query(query): Query<WatchQuery>,
) -> Result<Json<Vec<SessionEventEnvelope>>, ApiError> {
    let principal = state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/events", &headers)
        .await?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Watch,
            ResourceKind::EventStream,
            query.session_id.as_ref().map(|s| s.as_str()),
            &BTreeMap::new(),
        )
        .await?;
    let mut events = state
        .store
        .read_events_after(query.session_id.as_ref(), query.after_rv.unwrap_or(0))?;
    if !selector.is_empty() {
        events.retain(|ev| {
            state
                .store
                .get_session(&ev.session_id)
                .ok()
                .flatten()
                .map(|s| labels_match_selector(&s.metadata.labels, &selector))
                .unwrap_or(false)
        });
    }
    Ok(Json(events))
}

pub(crate) async fn handle_watch_events(
    State(state): State<ApiServerState>,
    headers: HeaderMap,
    Query(query): Query<WatchQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let principal = state
        .extensions
        .authenticate(AuthScope::ClientHttp, "GET", "/api/v1/watch", &headers)
        .await?;
    let selector = state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::Watch,
            ResourceKind::EventStream,
            query.session_id.as_ref().map(|s| s.as_str()),
            &BTreeMap::new(),
        )
        .await?;
    let live_rx = state.store.subscribe_events();
    let after_rv = query.after_rv.unwrap_or(0);
    let filter_session = query.session_id.clone();

    let mut historical = state
        .store
        .read_events_after(filter_session.as_ref(), after_rv)?;
    if !selector.is_empty() {
        historical.retain(|ev| {
            state
                .store
                .get_session(&ev.session_id)
                .ok()
                .flatten()
                .map(|s| labels_match_selector(&s.metadata.labels, &selector))
                .unwrap_or(false)
        });
    }

    let stream = async_stream_events(
        state.store.clone(),
        historical,
        live_rx,
        filter_session,
        selector,
        after_rv,
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn async_stream_events(
    store: SqliteStore,
    historical: Vec<SessionEventEnvelope>,
    mut live_rx: tokio::sync::broadcast::Receiver<SessionEventEnvelope>,
    filter_session: Option<SessionId>,
    selector: BTreeMap<String, String>,
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
            if !selector.is_empty() {
                let matched = store
                    .get_session(&ev.session_id)
                    .ok()
                    .flatten()
                    .map(|s| labels_match_selector(&s.metadata.labels, &selector))
                    .unwrap_or(false);
                if !matched {
                    continue;
                }
            }
            if !is_ephemeral {
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
    });

    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}
