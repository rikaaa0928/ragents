use super::{ApiError, ApiServerState, DaemonPeerConnection};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::HeaderMap,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use ragents_protocol::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) async fn handle_daemon_tunnel(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<ApiServerState>,
) -> Result<Response, ApiError> {
    state
        .extensions
        .authenticate(
            AuthScope::DaemonTunnel,
            "GET",
            "/api/v1/daemon/tunnel",
            &headers,
        )
        .await?;
    Ok(ws.on_upgrade(move |socket| run_daemon_tunnel_socket(socket, state)))
}

async fn run_daemon_tunnel_socket(socket: WebSocket, state: ApiServerState) {
    let conn_id = state.next_tunnel_conn_id.fetch_add(1, Ordering::Relaxed) + 1;
    let conn_cancel = state.shutdown.child_token();
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerToDaemonMessage>();

    let writer_cancel = conn_cancel.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = writer_cancel.cancelled() => {
                    let _ = tokio::time::timeout(Duration::from_secs(2), ws_sender.close()).await;
                    break;
                }
                maybe_msg = out_rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        let _ = tokio::time::timeout(Duration::from_secs(2), ws_sender.close()).await;
                        break;
                    };
                    if let Ok(text) = serde_json::to_string(&msg) {
                        if ws_sender.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

    let mut registered_node_id: Option<NodeId> = None;

    loop {
        let next_frame = tokio::select! {
            biased;
            _ = conn_cancel.cancelled() => break,
            f = ws_receiver.next() => f,
        };
        let Some(Ok(frame)) = next_frame else {
            break;
        };
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

        handle_daemon_message(
            &state,
            conn_id,
            &conn_cancel,
            &out_tx,
            &mut registered_node_id,
            incoming,
        );
    }

    if let Some(node_id) = registered_node_id {
        let mut guard = state.daemon_peers.lock().expect("peers mutex poisoned");
        if guard
            .get(&node_id)
            .map(|existing| existing.conn_id == conn_id)
            .unwrap_or(false)
        {
            guard.remove(&node_id);
        }
    }
    conn_cancel.cancel();
    writer_task.abort();
}

fn cached_tunnel_reply(state: &ApiServerState, request_id: &str) -> Option<ServerToDaemonMessage> {
    state
        .tunnel_reply_cache
        .lock()
        .expect("tunnel_reply_cache mutex poisoned")
        .get(request_id)
}

fn cache_and_send(
    state: &ApiServerState,
    out_tx: &mpsc::UnboundedSender<ServerToDaemonMessage>,
    request_id: String,
    reply: ServerToDaemonMessage,
) {
    state
        .tunnel_reply_cache
        .lock()
        .expect("tunnel_reply_cache mutex poisoned")
        .insert(request_id, reply.clone());
    let _ = out_tx.send(reply);
}

fn handle_daemon_message(
    state: &ApiServerState,
    conn_id: u64,
    conn_cancel: &CancellationToken,
    out_tx: &mpsc::UnboundedSender<ServerToDaemonMessage>,
    registered_node_id: &mut Option<NodeId>,
    incoming: DaemonToServerMessage,
) {
    match incoming {
        DaemonToServerMessage::RegisterNode {
            request_id,
            node_id,
            hostname,
            os,
            arch,
            labels,
            local_models,
            use_cluster_models,
            default_model,
            default_system_prompt,
            default_permissions,
            installed_extensions,
            allowed_workspace_dirs,
            max_concurrent_runners,
            running_activation_ids,
        } => {
            if let Some(prev) = state
                .daemon_peers
                .lock()
                .expect("peers mutex poisoned")
                .insert(
                    node_id.clone(),
                    DaemonPeerConnection {
                        conn_id,
                        out_tx: out_tx.clone(),
                        cancel: conn_cancel.clone(),
                    },
                )
            {
                if prev.conn_id != conn_id {
                    eprintln!(
                        "[ragents-apiserver] node '{}' re-registered on conn_id={}; cancelling superseded conn_id={}",
                        node_id, conn_id, prev.conn_id
                    );
                    prev.cancel.cancel();
                }
            }
            *registered_node_id = Some(node_id.clone());

            let effective_dirs = if allowed_workspace_dirs.is_empty() {
                let mut d = vec!["/tmp".to_string()];
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();
                if !d.contains(&sys_tmp) {
                    d.push(sys_tmp);
                }
                d
            } else {
                allowed_workspace_dirs
            };

            match state.store.register_node_with_models_and_running_activations(
                node_id.clone(),
                hostname,
                os,
                arch,
                labels,
                local_models,
                use_cluster_models,
                default_model,
                default_system_prompt,
                default_permissions,
                installed_extensions,
                effective_dirs,
                max_concurrent_runners,
                state.lease_ttl_ms,
                running_activation_ids,
            ) {
                Ok((node, _dispatches)) => {
                    let _ = out_tx.send(ServerToDaemonMessage::Registered {
                        request_id,
                        node_id,
                        lease_ttl_ms: state.lease_ttl_ms,
                        resource_version: node.metadata.resource_version,
                        node_spec: node.spec,
                        cluster_default_model: state.model_catalog.effective_default_model(),
                    });
                }
                Err(e) => {
                    send_ack_err(out_tx, request_id, e.to_string());
                }
            }
        }
        DaemonToServerMessage::ResolveModelProvider { request_id, model } => {
            match state.model_catalog.resolve_provider(model.as_deref()) {
                Some(provider) => {
                    let _ = out_tx.send(ServerToDaemonMessage::ModelProviderReply {
                        request_id,
                        ok: true,
                        provider: Some(provider),
                        error: None,
                    });
                }
                None => {
                    let _ = out_tx.send(ServerToDaemonMessage::ModelProviderReply {
                        request_id,
                        ok: false,
                        provider: None,
                        error: Some(match model {
                            Some(m) => format!("cluster model '{m}' is not configured or discovered on api-server"),
                            None => "no default cluster model is configured on api-server".to_string(),
                        }),
                    });
                }
            }
        }
        DaemonToServerMessage::Heartbeat {
            request_id,
            node_id,
            active_runners,
            running_activation_ids,
        } => {
            let is_current_conn = registered_node_id.as_ref() == Some(&node_id)
                && state
                    .daemon_peers
                    .lock()
                    .expect("peers mutex poisoned")
                    .get(&node_id)
                    .map(|p| p.conn_id == conn_id)
                    .unwrap_or(false);
            if !is_current_conn {
                eprintln!(
                    "[ragents-apiserver] ignoring stale Heartbeat for node '{}' from conn_id={} (superseded)",
                    node_id, conn_id
                );
                send_ack_err(
                    out_tx,
                    request_id,
                    format!("stale tunnel connection conn_id={conn_id} for node {node_id}"),
                );
                conn_cancel.cancel();
                return;
            }
            match state.store.heartbeat_node_with_activations(
                &node_id,
                active_runners,
                running_activation_ids,
                state.lease_ttl_ms,
            ) {
                Ok((node, _)) => {
                    send_ack_ok(out_tx, request_id, Some(node.metadata.resource_version));
                }
                Err(e) => {
                    send_ack_err(out_tx, request_id, e.to_string());
                }
            }
        }
        DaemonToServerMessage::AcceptActivation {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            effective_config,
            effective_permission,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.accept_activation(
                &session_id,
                &activation_id,
                lease_epoch,
                *effective_config,
                *effective_permission,
            ) {
                Ok(act) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: true,
                    resource_version: Some(act.metadata.resource_version),
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: false,
                    resource_version: None,
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
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
            drain_realtime,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.commit_batch_with_drain(
                &session_id,
                &activation_id,
                lease_epoch,
                kind,
                turn_id,
                response_id,
                items,
                companion_event,
                drain_realtime,
            ) {
                Ok((batch_seq, rv, drained_realtime)) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: true,
                    resource_version: Some(rv),
                    batch_seq: Some(batch_seq),
                    spawned_session_id: None,
                    drained_realtime,
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: false,
                    resource_version: None,
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
        DaemonToServerMessage::BeginTurn {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            turn_id,
            include_context,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.begin_turn(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
                include_context,
            ) {
                Ok((drained_realtime, effective_items)) => ServerToDaemonMessage::BeginTurnReply {
                    request_id: request_id.clone(),
                    ok: true,
                    drained_realtime,
                    effective_items,
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::BeginTurnReply {
                    request_id: request_id.clone(),
                    ok: false,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    effective_items: None,
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
        DaemonToServerMessage::EmitEvent {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            turn_id,
            event,
        } => {
            let is_ephemeral = event.is_ephemeral_stream_delta();
            if !is_ephemeral && !request_id.is_empty() {
                if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                    let _ = out_tx.send(cached);
                    return;
                }
            }
            match state.store.emit_daemon_event(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
                event,
            ) {
                Ok(env) => {
                    if !is_ephemeral && !request_id.is_empty() {
                        let reply = ServerToDaemonMessage::Ack {
                            request_id: request_id.clone(),
                            ok: true,
                            resource_version: Some(env.resource_version),
                            batch_seq: None,
                            spawned_session_id: None,
                            drained_realtime: DrainedRealtimeBatch::default(),
                            error: None,
                        };
                        cache_and_send(state, out_tx, request_id, reply);
                    }
                }
                Err(e) => {
                    if !is_ephemeral && !request_id.is_empty() {
                        let reply = ServerToDaemonMessage::Ack {
                            request_id: request_id.clone(),
                            ok: false,
                            resource_version: None,
                            batch_seq: None,
                            spawned_session_id: None,
                            drained_realtime: DrainedRealtimeBatch::default(),
                            error: Some(e.to_string()),
                        };
                        cache_and_send(state, out_tx, request_id, reply);
                    }
                }
            }
        }
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
        DaemonToServerMessage::PushRealtimeMessage {
            request_id,
            session_id,
            activation_id: _,
            content,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.enqueue_session_message(
                &session_id,
                QueueKind::Realtime,
                EnqueueSessionMessageRequest {
                    content,
                    tool_policy: QueueToolPolicy::default(),
                    metadata: std::collections::BTreeMap::new(),
                    model_override: None,
                },
            ) {
                Ok(_) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: true,
                    resource_version: None,
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: false,
                    resource_version: None,
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
        DaemonToServerMessage::DrainRealtimeMessages {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            turn_id,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.drain_realtime_messages(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
            ) {
                Ok((drained, batch_seq)) => ServerToDaemonMessage::RealtimeMessagesReply {
                    request_id: request_id.clone(),
                    ok: true,
                    drained,
                    batch_seq,
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::RealtimeMessagesReply {
                    request_id: request_id.clone(),
                    ok: false,
                    drained: DrainedRealtimeBatch::default(),
                    batch_seq: None,
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
        DaemonToServerMessage::CompleteActivation {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            turn_id,
            outcome,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.complete_activation(
                &session_id,
                &activation_id,
                lease_epoch,
                turn_id,
                outcome,
            ) {
                Ok((act, _)) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: true,
                    resource_version: Some(act.metadata.resource_version),
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::Ack {
                    request_id: request_id.clone(),
                    ok: false,
                    resource_version: None,
                    batch_seq: None,
                    spawned_session_id: None,
                    drained_realtime: DrainedRealtimeBatch::default(),
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        }
        DaemonToServerMessage::RolloverCompactedSession {
            request_id,
            request,
        } => {
            if let Some(cached) = cached_tunnel_reply(state, &request_id) {
                let _ = out_tx.send(cached);
                return;
            }
            let reply = match state.store.rollover_compacted_session(*request) {
                Ok((response, _)) => ServerToDaemonMessage::RolloverCompactedSessionReply {
                    request_id: request_id.clone(),
                    ok: true,
                    response: Some(Box::new(response)),
                    error: None,
                },
                Err(e) => ServerToDaemonMessage::RolloverCompactedSessionReply {
                    request_id: request_id.clone(),
                    ok: false,
                    response: None,
                    error: Some(e.to_string()),
                },
            };
            cache_and_send(state, out_tx, request_id, reply);
        },
        DaemonToServerMessage::QuerySessionHistory {
            request_id,
            session_id,
            request,
        } => {
            let response = match state
                .store
                .query_workspace_session_history(&session_id, *request)
            {
                Ok(resp) => resp,
                Err(e) => SessionHistoryQueryResponse {
                    ok: false,
                    error: Some(e.to_string()),
                    ..Default::default()
                },
            };
            let _ = out_tx.send(ServerToDaemonMessage::QuerySessionHistoryReply {
                request_id,
                response: Box::new(response),
            });
        }
        DaemonToServerMessage::NodeExtensionReply {
            request_id,
            response,
        } => {
            if let Some(tx) = state
                .pending_node_ext
                .lock()
                .expect("pending_node_ext mutex poisoned")
                .remove(&request_id)
            {
                let _ = tx.send(*response);
            }
        }
    }
}

fn send_ack_ok(
    out_tx: &mpsc::UnboundedSender<ServerToDaemonMessage>,
    request_id: String,
    resource_version: Option<ResourceVersion>,
) {
    let _ = out_tx.send(ServerToDaemonMessage::Ack {
        request_id,
        ok: true,
        resource_version,
        batch_seq: None,
        spawned_session_id: None,
        drained_realtime: DrainedRealtimeBatch::default(),
        error: None,
    });
}

fn send_ack_err(
    out_tx: &mpsc::UnboundedSender<ServerToDaemonMessage>,
    request_id: String,
    error: String,
) {
    let _ = out_tx.send(ServerToDaemonMessage::Ack {
        request_id,
        ok: false,
        resource_version: None,
        batch_seq: None,
        spawned_session_id: None,
        drained_realtime: DrainedRealtimeBatch::default(),
        error: Some(error),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_handle_daemon_message_rejects_stale_conn_heartbeat() {
        let store = SqliteStore::open_in_memory().expect("open store");
        let state = ApiServerState::new(store, 30_000);
        let node_id = NodeId::new("node_fenced");

        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let cancel1 = CancellationToken::new();
        let mut reg1 = None;

        // 1. conn_id = 1 registers node_fenced
        handle_daemon_message(
            &state,
            1,
            &cancel1,
            &tx1,
            &mut reg1,
            DaemonToServerMessage::RegisterNode {
                request_id: "reg_1".into(),
                node_id: node_id.clone(),
                hostname: "h".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                labels: BTreeMap::new(),
                local_models: vec!["gpt-4.1".into()],
                use_cluster_models: true,
                default_model: Some("gpt-4.1".into()),
                default_system_prompt: None,
                default_permissions: PermissionPolicyLayer::default(),
                installed_extensions: vec![],
                allowed_workspace_dirs: vec!["/tmp".into()],
                max_concurrent_runners: 4,
                running_activation_ids: Some(vec![]),
            },
        );
        let _ = rx1.recv().await.expect("reg 1 reply");

        // 2. conn_id = 2 re-registers node_fenced -> supersedes and cancels conn_id = 1
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let cancel2 = CancellationToken::new();
        let mut reg2 = None;
        handle_daemon_message(
            &state,
            2,
            &cancel2,
            &tx2,
            &mut reg2,
            DaemonToServerMessage::RegisterNode {
                request_id: "reg_2".into(),
                node_id: node_id.clone(),
                hostname: "h".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                labels: BTreeMap::new(),
                local_models: vec!["gpt-4.1".into()],
                use_cluster_models: true,
                default_model: Some("gpt-4.1".into()),
                default_system_prompt: None,
                default_permissions: PermissionPolicyLayer::default(),
                installed_extensions: vec![],
                allowed_workspace_dirs: vec!["/tmp".into()],
                max_concurrent_runners: 4,
                running_activation_ids: Some(vec![]),
            },
        );
        let _ = rx2.recv().await.expect("reg 2 reply");
        assert!(cancel1.is_cancelled());

        // 3. Create session & accept activation on conn_id = 2
        let (created, _) = state
            .store
            .create_session(CreateSessionRequest {
                session_id: Some(SessionId::new("sess_fenced")),
                labels: BTreeMap::new(),
                spec: SessionSpec {
                    scheduling: SchedulingMode::Pinned {
                        node_id: node_id.clone(),
                        workspace_path: "/tmp".into(),
                    },
                    sources: vec![],
                    producer: None,
                    desired_phase: DesiredSessionPhase::Open,
                    system_prompt: None,
                    model: Some(ModelRequestSpec::new("gpt-4.1")),
                    extensions: vec![],
                    permissions: PermissionPolicyLayer::default(),
                    extra_tmp_dirs: vec![],
                },
                initial_message: Some(EnqueueSessionMessageRequest::from_text("go")),
            })
            .expect("create sess");
        let act_id = created.spawned_activation_id.expect("spawned act");
        let act = state.store.get_activation(&act_id).unwrap().unwrap();
        state
            .store
            .accept_activation(
                &SessionId::new("sess_fenced"),
                &act_id,
                act.status.lease_epoch,
                ConfigRevision::new_merged("sys".into(), ModelRequestSpec::new("gpt-4.1"), vec![]),
                PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default()),
            )
            .expect("accept act");

        // 4. Even if conn_id = 1 still delivers an already-buffered stale Heartbeat with empty running_activation_ids,
        //    it MUST be rejected and MUST NOT interrupt `act_id`!
        handle_daemon_message(
            &state,
            1,
            &cancel1,
            &tx1,
            &mut reg1,
            DaemonToServerMessage::Heartbeat {
                request_id: "hb_stale_1".into(),
                node_id: node_id.clone(),
                active_runners: 0,
                running_activation_ids: Some(vec![]),
            },
        );
        match rx1.recv().await.expect("stale hb reply") {
            ServerToDaemonMessage::Ack { ok, error, .. } => {
                assert!(!ok);
                assert!(error.unwrap_or_default().contains("stale tunnel connection"));
            }
            other => panic!("expected Ack error, got {other:?}"),
        }

        let act_after = state.store.get_activation(&act_id).unwrap().unwrap();
        assert_eq!(act_after.status.phase, ActivationPhase::Running);
    }
}
