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
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("ping over cluster")],
                model_override: None,
                disabled_tools: vec![],
            }),
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
