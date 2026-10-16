use super::{ApiError, ApiServerState};
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
use std::collections::BTreeMap;
use tokio::sync::mpsc;

pub(crate) async fn handle_daemon_tunnel(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<ApiServerState>,
) -> Result<Response, ApiError> {
    let principal = state
        .extensions
        .authenticate(
            AuthScope::DaemonTunnel,
            "GET",
            "/api/v1/daemon/tunnel",
            &headers,
        )
        .await?;
    state
        .extensions
        .authorize(
            &principal,
            ResourceVerb::ConnectTunnel,
            ResourceKind::DaemonTunnel,
            None,
            &BTreeMap::new(),
        )
        .await?;
    Ok(ws.on_upgrade(move |socket| run_daemon_tunnel_socket(socket, state)))
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

        handle_daemon_message(&state, &out_tx, &mut registered_node_id, incoming);
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

fn handle_daemon_message(
    state: &ApiServerState,
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
            supported_models,
            default_model,
            installed_extensions,
            max_concurrent_runners,
        } => {
            state
                .daemon_peers
                .lock()
                .expect("peers mutex poisoned")
                .insert(node_id.clone(), out_tx.clone());
            *registered_node_id = Some(node_id.clone());

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
                    send_ack_err(out_tx, request_id, e.to_string());
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
                send_ack_ok(out_tx, request_id, Some(node.metadata.resource_version));
            }
            Err(e) => {
                send_ack_err(out_tx, request_id, e.to_string());
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
                send_ack_ok(out_tx, request_id, Some(act.metadata.resource_version));
            }
            Err(e) => {
                send_ack_err(out_tx, request_id, e.to_string());
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
                send_ack_err(out_tx, request_id, e.to_string());
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
                send_ack_ok(out_tx, request_id, Some(env.resource_version));
            }
            Err(e) => {
                send_ack_err(out_tx, request_id, e.to_string());
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
        DaemonToServerMessage::PushRealtimeMessage {
            request_id,
            session_id,
            activation_id: _,
            content,
        } => match state.store.push_realtime_message(&session_id, content) {
            Ok(_count) => {
                send_ack_ok(out_tx, request_id, None);
            }
            Err(e) => {
                send_ack_err(out_tx, request_id, e.to_string());
            }
        },
        DaemonToServerMessage::DrainRealtimeMessages {
            request_id,
            session_id,
            activation_id,
            lease_epoch,
            turn_id,
        } => match state.store.drain_realtime_messages(
            &session_id,
            &activation_id,
            lease_epoch,
            turn_id,
        ) {
            Ok((items, batch_seq)) => {
                let _ = out_tx.send(ServerToDaemonMessage::RealtimeMessagesReply {
                    request_id,
                    ok: true,
                    items,
                    batch_seq,
                    error: None,
                });
            }
            Err(e) => {
                let _ = out_tx.send(ServerToDaemonMessage::RealtimeMessagesReply {
                    request_id,
                    ok: false,
                    items: vec![],
                    batch_seq: None,
                    error: Some(e.to_string()),
                });
            }
        },
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
                send_ack_ok(out_tx, request_id, Some(act.metadata.resource_version));
            }
            Err(e) => {
                send_ack_err(out_tx, request_id, e.to_string());
            }
        },
        DaemonToServerMessage::RolloverCompactedSession {
            request_id,
            request,
        } => match state.store.rollover_compacted_session(*request) {
            Ok((response, _)) => {
                let _ = out_tx.send(ServerToDaemonMessage::RolloverCompactedSessionReply {
                    request_id,
                    ok: true,
                    response: Some(Box::new(response)),
                    error: None,
                });
            }
            Err(e) => {
                let _ = out_tx.send(ServerToDaemonMessage::RolloverCompactedSessionReply {
                    request_id,
                    ok: false,
                    response: None,
                    error: Some(e.to_string()),
                });
            }
        },
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
        error: Some(error),
    });
}
