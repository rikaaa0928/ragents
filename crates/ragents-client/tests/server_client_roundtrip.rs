use futures::StreamExt;
use openresponses_rust::Item;
use ragents_apiserver::{serve_tcp, ApiServerState, SqliteStore, DEFAULT_LEASE_TTL_MS};
use ragents_client::{connect_daemon_tunnel, DaemonPushEvent, RagentsClient};
use ragents_protocol::*;
use std::collections::BTreeMap;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_http_sse_and_ws_daemon_tunnel_roundtrip() {
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS);
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, cancel.clone())
        .await
        .expect("serve");
    let base_url = format!("http://{addr}");

    let client = RagentsClient::new(&base_url);
    let (tunnel, mut push_rx) = connect_daemon_tunnel(&client.ws_tunnel_url())
        .await
        .expect("connect daemon tunnel");

    let (_ttl, _rv) = tunnel
        .register_node(
            NodeId::new("node_test_1"),
            "test-host".to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            vec!["shell".to_string()],
            4,
        )
        .await
        .expect("register node");

    let nodes = client.list_nodes().await.expect("list nodes");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].status.supported_models, vec!["gpt-4.1"]);

    // Create a Floating session with initial activation
    let created = client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_e2e_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("System".to_string()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
                extra_tmp_dirs: vec![],
            },
            initial_message: Some(EnqueueSessionMessageRequest::from_text("ping over cluster")),
        })
        .await
        .expect("create session");

    // Daemon receives DispatchActivation push over WebSocket
    let push = push_rx.recv().await.expect("receive push");
    let (session, activation, lease_epoch) = match push {
        DaemonPushEvent::DispatchActivation {
            session,
            activation,
            lease_epoch,
            ..
        } => (*session, *activation, lease_epoch),
        other => panic!("expected DispatchActivation, got {other:?}"),
    };

    assert_eq!(session.metadata.id, "sess_e2e_1");
    let act_id = ActivationId::new(activation.metadata.id.clone());
    let sess_id = SessionId::new(session.metadata.id.clone());

    // Start watching SSE events from rv=0
    let mut watch_stream = client
        .watch_events(Some(&sess_id), 0)
        .await
        .expect("watch sse");

    let cfg = ConfigRevision::new_merged(
        "System".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![],
    );
    let perm = PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default());

    tunnel
        .accept_activation(sess_id.clone(), act_id.clone(), lease_epoch, cfg, perm)
        .await
        .expect("accept activation");

    tunnel
        .commit_batch(
            sess_id.clone(),
            act_id.clone(),
            lease_epoch,
            ItemBatchKind::ModelOutput,
            Some(TurnId::new("turn_1")),
            Some("resp_1".to_string()),
            vec![Item::assistant_message("pong from daemon")],
            BatchCompanionEvent::TurnCompleted {
                response_id: Some("resp_1".to_string()),
                usage: None,
            },
        )
        .await
        .expect("commit batch");

    tunnel
        .complete_activation(
            sess_id.clone(),
            act_id.clone(),
            lease_epoch,
            Some(TurnId::new("turn_1")),
            ActivationTerminalOutcome::Succeeded { usage: None },
        )
        .await
        .expect("complete activation");

    // Collect events from SSE watch until ActivationCompleted arrives
    let mut seen_kinds = Vec::new();
    while let Some(ev_res) = watch_stream.next().await {
        let env = ev_res.expect("valid sse event");
        seen_kinds.push(env.event.kind_name().to_string());
        if matches!(env.event, SessionEvent::ActivationCompleted { .. }) {
            break;
        }
    }

    assert!(seen_kinds.contains(&"session_created".to_string()));
    assert!(seen_kinds.contains(&"activation_submitted".to_string()));
    assert!(seen_kinds.contains(&"activation_scheduled".to_string()));
    assert!(seen_kinds.contains(&"activation_started".to_string()));
    assert!(seen_kinds.contains(&"turn_completed".to_string()));
    assert!(seen_kinds.contains(&"activation_completed".to_string()));

    // Verify effective context via REST
    let ctx = client
        .get_session_context(&sess_id)
        .await
        .expect("get context");
    assert_eq!(ctx.effective_items.len(), 2);
    let _ = created;
    cancel.cancel();
}

#[tokio::test]
async fn test_static_token_authn() {
    use ragents_apiserver::{ApiServerExtensions, StaticTokenAuthnPlugin};
    use ragents_client::connect_daemon_tunnel_with_token;
    use std::sync::Arc;

    let store = SqliteStore::open_in_memory().unwrap();
    let mut exts = ApiServerExtensions::new();
    exts.add_authn(Arc::new(StaticTokenAuthnPlugin::new("sk-cluster-secret")));

    let state = ApiServerState::new(store, 5000).with_extensions(exts);
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = serve_tcp(listener, state, cancel.clone()).await.unwrap();
    let base_url = format!("http://{addr}");

    // 1. Unauthenticated client request is rejected with 401
    let anon_client = RagentsClient::new(&base_url);
    assert!(anon_client.list_nodes().await.is_err());

    // 2. Wrong token is rejected with 401
    let wrong_client =
        RagentsClient::new(&base_url).with_auth_token(Some("sk-wrong".into()));
    assert!(wrong_client.list_nodes().await.is_err());

    // 3. Valid token succeeds on ClientHttp and DaemonTunnel
    let valid_client =
        RagentsClient::new(&base_url).with_auth_token(Some("sk-cluster-secret".into()));
    let nodes = valid_client.list_nodes().await.expect("valid client authn");
    assert!(nodes.is_empty());

    let ws_url = valid_client.ws_tunnel_url();
    assert!(
        connect_daemon_tunnel_with_token(&ws_url, Some("sk-wrong"))
            .await
            .is_err()
    );
    let (tunnel, _rx) = connect_daemon_tunnel_with_token(&ws_url, Some("sk-cluster-secret"))
        .await
        .expect("valid daemon tunnel authn");
    let (_, _) = tunnel
        .register_node(
            NodeId::new("node_vps"),
            "vps".into(),
            "linux".into(),
            "x86_64".into(),
            BTreeMap::new(),
            vec!["gemini-3.8-flash".into()],
            Some("gemini-3.8-flash".into()),
            vec![],
            2,
        )
        .await
        .unwrap();

    let nodes_after = valid_client.list_nodes().await.unwrap();
    assert_eq!(nodes_after.len(), 1);
    assert_eq!(nodes_after[0].metadata.id, "node_vps");

    cancel.cancel();
}

#[tokio::test]
async fn test_tunnel_reconnect_preserves_inflight_rpc_and_deduplicates_request_id() {
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS);
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, cancel.clone())
        .await
        .expect("serve");
    let base_url = format!("http://{addr}");

    let client = RagentsClient::new(&base_url);
    let (tunnel, mut push_rx) = connect_daemon_tunnel(&client.ws_tunnel_url())
        .await
        .expect("connect daemon tunnel");
    tunnel.set_auto_reconnect(true);
    assert_eq!(tunnel.current_epoch(), 0);

    tunnel
        .register_node(
            NodeId::new("node_reconn"),
            "test-host".to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            vec!["shell".to_string()],
            4,
        )
        .await
        .expect("register node");

    client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_reconn_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("System".to_string()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
                extra_tmp_dirs: vec![],
            },
            initial_message: Some(EnqueueSessionMessageRequest::from_text("hello")),
        })
        .await
        .expect("create session");

    let (sess_id, act_id, lease_epoch) = match push_rx.recv().await.expect("push") {
        DaemonPushEvent::DispatchActivation {
            session,
            activation,
            lease_epoch,
            ..
        } => (
            SessionId::new(session.metadata.id),
            ActivationId::new(activation.metadata.id),
            lease_epoch,
        ),
        other => panic!("expected DispatchActivation, got {other:?}"),
    };

    let cfg = ConfigRevision::new_merged(
        "System".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![],
    );
    let perm = PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default());
    tunnel
        .accept_activation(sess_id.clone(), act_id.clone(), lease_epoch, cfg, perm)
        .await
        .expect("accept activation");

    // Simulate a heartbeat timeout disconnecting epoch 0 right before an RPC runs,
    // with reconnect taking 1.2s (longer than the old 900ms backoff window).
    assert!(tunnel.force_disconnect_if_epoch(0));
    assert!(tunnel.is_disconnected());

    let tunnel_for_rpc = tunnel.clone();
    let sess_for_rpc = sess_id.clone();
    let act_for_rpc = act_id.clone();
    let rpc_task = tokio::spawn(async move {
        tunnel_for_rpc
            .commit_batch(
                sess_for_rpc,
                act_for_rpc,
                lease_epoch,
                ItemBatchKind::ModelOutput,
                Some(TurnId::new("turn_reconn")),
                Some("resp_reconn".to_string()),
                vec![Item::assistant_message("recovered after reconnect")],
                BatchCompanionEvent::TurnCompleted {
                    response_id: Some("resp_reconn".to_string()),
                    usage: None,
                },
            )
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (new_tunnel, _new_rx) = connect_daemon_tunnel(&client.ws_tunnel_url())
        .await
        .expect("reconnect daemon tunnel");
    new_tunnel
        .register_node_with_running_activations(
            NodeId::new("node_reconn"),
            "test-host".to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            None,
            PermissionPolicyLayer::default(),
            vec!["shell".to_string()],
            vec!["/tmp".to_string()],
            4,
            Some(vec![act_id.clone()]),
        )
        .await
        .expect("re-register node");
    tunnel.replace_connection(&new_tunnel);
    assert_eq!(tunnel.current_epoch(), 1);
    assert!(!tunnel.is_disconnected());

    // A stale heartbeat timeout from epoch 0 must NOT tear down epoch 1.
    assert!(!tunnel.force_disconnect_if_epoch(0));
    assert!(!tunnel.is_disconnected());

    let batch_seq = rpc_task
        .await
        .expect("rpc task join")
        .expect("commit_batch survives reconnect");
    assert_eq!(batch_seq.value(), 1);

    // Verify server-side request_id deduplication across reconnects using raw `call`.
    let dup_req_id = "req_dedup_fixed_001".to_string();
    let dup_msg = DaemonToServerMessage::CommitBatch {
        request_id: dup_req_id,
        session_id: sess_id.clone(),
        activation_id: act_id.clone(),
        lease_epoch,
        kind: ItemBatchKind::ToolOutput,
        turn_id: Some(TurnId::new("turn_reconn")),
        response_id: None,
        items: vec![Item::assistant_message("idempotent item")],
        companion_event: BatchCompanionEvent::ToolCallFinished {
            call_id: "call_dedup_1".to_string(),
            name: "shell".to_string(),
            success: true,
            duration_ms: 10,
            view_mode_placeholder: None,
        },
        drain_realtime: false,
    };
    let r1 = tunnel.call(dup_msg.clone()).await.expect("first call");
    let r2 = tunnel.call(dup_msg).await.expect("duplicate call");
    match (r1, r2) {
        (
            ServerToDaemonMessage::Ack {
                ok: true,
                batch_seq: b1,
                ..
            },
            ServerToDaemonMessage::Ack {
                ok: true,
                batch_seq: b2,
                ..
            },
        ) => {
            assert_eq!(b1, b2);
            assert_eq!(b1.unwrap().value(), 2);
        }
        other => panic!("unexpected replies: {other:?}"),
    }

    let ctx = client
        .get_session_context(&sess_id)
        .await
        .expect("get context");
    // Input (1) + recovered assistant (1) + idempotent item once (1) = 3
    assert_eq!(ctx.effective_items.len(), 3);

    tunnel.set_auto_reconnect(false);
    cancel.cancel();
}

#[tokio::test]
async fn test_stale_superseded_tunnel_heartbeat_cannot_interrupt_new_activation() {
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS);
    let cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, cancel.clone())
        .await
        .expect("serve");
    let base_url = format!("http://{addr}");

    let client = RagentsClient::new(&base_url);
    let (stale_tunnel, mut stale_push_rx) = connect_daemon_tunnel(&client.ws_tunnel_url())
        .await
        .expect("connect stale tunnel");

    stale_tunnel
        .register_node(
            NodeId::new("node_fence_1"),
            "test-host".to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            vec!["shell".to_string()],
            4,
        )
        .await
        .expect("register stale tunnel");

    client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_fence_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("System".to_string()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
                extra_tmp_dirs: vec![],
            },
            initial_message: Some(EnqueueSessionMessageRequest::from_text("first activation")),
        })
        .await
        .expect("create sess 1");

    let (sess1, act1, epoch1) = match stale_push_rx.recv().await.expect("push 1") {
        DaemonPushEvent::DispatchActivation {
            session,
            activation,
            lease_epoch,
            ..
        } => (
            SessionId::new(session.metadata.id),
            ActivationId::new(activation.metadata.id),
            lease_epoch,
        ),
        other => panic!("expected DispatchActivation, got {other:?}"),
    };

    let cfg = ConfigRevision::new_merged(
        "System".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![],
    );
    let perm = PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default());
    stale_tunnel
        .accept_activation(sess1.clone(), act1.clone(), epoch1, cfg.clone(), perm.clone())
        .await
        .expect("accept act1");

    // Reconnect on a brand-new tunnel WITHOUT closing `stale_tunnel` from the client side first
    // (simulating a half-open / NAT-stalled old socket whose buffered frames flush late).
    let (new_tunnel, mut new_push_rx) = connect_daemon_tunnel(&client.ws_tunnel_url())
        .await
        .expect("connect new tunnel");
    new_tunnel
        .register_node_with_running_activations(
            NodeId::new("node_fence_1"),
            "test-host".to_string(),
            "linux".to_string(),
            "x86_64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            None,
            PermissionPolicyLayer::default(),
            vec!["shell".to_string()],
            vec!["/tmp".to_string()],
            4,
            Some(vec![act1.clone()]),
        )
        .await
        .expect("register new tunnel");

    // Server should cancel & close `stale_tunnel` upon new registration.
    let stale_disc = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while let Some(ev) = stale_push_rx.recv().await {
            if matches!(ev, DaemonPushEvent::Disconnected) {
                return true;
            }
        }
        true
    })
    .await
    .expect("stale tunnel must be disconnected by server");
    assert!(stale_disc);

    // Create & accept a second activation on `new_tunnel`.
    client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_fence_2")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("System".to_string()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
                extra_tmp_dirs: vec![],
            },
            initial_message: Some(EnqueueSessionMessageRequest::from_text("second activation")),
        })
        .await
        .expect("create sess 2");

    let (sess2, act2, epoch2) = match new_push_rx.recv().await.expect("push 2") {
        DaemonPushEvent::DispatchActivation {
            session,
            activation,
            lease_epoch,
            ..
        } => (
            SessionId::new(session.metadata.id),
            ActivationId::new(activation.metadata.id),
            lease_epoch,
        ),
        other => panic!("expected DispatchActivation 2, got {other:?}"),
    };
    new_tunnel
        .accept_activation(sess2.clone(), act2.clone(), epoch2, cfg, perm)
        .await
        .expect("accept act2");

    // Even if a stale Heartbeat with only `[act1]` (missing `act2`) was sent on `stale_tunnel`
    // (or attempted right before close), it must fail / be ignored and MUST NOT interrupt `act2`.
    let _ = stale_tunnel
        .heartbeat_with_activations(NodeId::new("node_fence_1"), 1, Some(vec![act1]))
        .await;

    let act2_after = client
        .get_activation(&act2)
        .await
        .expect("get act2")
        .expect("act2 exists");
    assert_eq!(act2_after.status.phase, ActivationPhase::Running);
    assert_eq!(act2_after.status.lease_epoch, epoch2);

    new_tunnel
        .complete_activation(
            sess2,
            act2.clone(),
            epoch2,
            None,
            ActivationTerminalOutcome::Succeeded { usage: None },
        )
        .await
        .expect("complete act2 succeeds without lease_epoch mismatch");

    let act2_done = client
        .get_activation(&act2)
        .await
        .expect("get act2 done")
        .expect("act2 done exists");
    assert_eq!(act2_done.status.phase, ActivationPhase::Succeeded);

    cancel.cancel();
}
