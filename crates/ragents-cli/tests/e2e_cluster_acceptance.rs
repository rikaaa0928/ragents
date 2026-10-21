use axum::{extract::State, routing::post, Json, Router};
use futures::StreamExt;
use openresponses_rust::{CreateResponseBody, Item, ResponseResource};
use ragents_apiserver::{serve_tcp, ApiServerState, SqliteStore, DEFAULT_LEASE_TTL_MS};
use ragents_cli::build_scheduling_mode;
use ragents_client::RagentsClient;
use ragents_core::make_openresponses_resource;
use ragents_daemon::{AgentDaemon, DaemonConfig, ExecutorMode, LocalModelConfig};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn start_mock_openresponses_http_server(
    queued_responses: Vec<ResponseResource>,
) -> (String, CancellationToken) {
    let queue = Arc::new(Mutex::new(queued_responses));
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let app = Router::new()
        .route(
            "/v1/responses",
            post(
                |State(q): State<Arc<Mutex<Vec<ResponseResource>>>>,
                 Json(_req): Json<CreateResponseBody>| async move {
                    let next = q.lock().unwrap().remove(0);
                    Json(next)
                },
            ),
        )
        .with_state(queue);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel_clone.cancelled().await;
            })
            .await;
    });

    (format!("http://{addr}/v1"), cancel)
}

#[test]
fn test_cli_build_scheduling_mode_validation() {
    // Both omitted -> Floating
    let m1 = build_scheduling_mode(None, None, false).unwrap();
    assert!(m1.is_floating());

    // Explicit --floating -> Floating
    let m2 = build_scheduling_mode(None, None, true).unwrap();
    assert!(m2.is_floating());

    // Both --node and --workdir -> Pinned
    let m3 = build_scheduling_mode(
        Some("node_1".into()),
        Some(PathBuf::from("/tmp/work")),
        false,
    )
    .unwrap();
    assert!(m3.is_pinned());

    // Only --node without --workdir -> Error
    assert!(build_scheduling_mode(Some("node_1".into()), None, false).is_err());

    // --floating combined with --node -> Error
    assert!(build_scheduling_mode(Some("node_1".into()), None, true).is_err());
}

#[tokio::test]
async fn test_e2e_openresponses_http_wasm_tool_and_interactive_permission_approval() {
    let wasm_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    if !wasm_path.exists() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let workspace_dir = tmp.path().join("project_ws");
    std::fs::create_dir_all(&workspace_dir).unwrap();
    let outside_file = tmp.path().join("approved.txt");

    // 1. Start mock OpenResponses HTTP server
    let fc_item = Item::FunctionCall {
        id: Some("fc_e2e_1".to_string()),
        call_id: "call_e2e_1".to_string(),
        name: "write_file".to_string(),
        arguments: serde_json::json!({
            "path": outside_file.to_string_lossy(),
            "content": "approved via declarative api-server!"
        })
        .to_string(),
        status: None,
    };
    let resp1 = make_openresponses_resource("resp_e2e_1", "gpt-4.1", vec![fc_item], None);
    let resp2 = make_openresponses_resource(
        "resp_e2e_2",
        "gpt-4.1",
        vec![Item::assistant_message(
            "File approved.txt has been written.",
        )],
        None,
    );
    let (llm_base_url, llm_cancel) = start_mock_openresponses_http_server(vec![resp1, resp2]).await;

    // 2. Start ragents-apiserver
    let store = SqliteStore::open(tmp.path().join("control.sqlite3")).unwrap();
    let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS);
    let cluster_cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let apiserver_addr = serve_tcp(listener, state, cluster_cancel.clone())
        .await
        .unwrap();
    let server_url = format!("http://{apiserver_addr}");

    // 3. Start ragents-daemon with local OpenResponses model credentials
    let mut models = BTreeMap::new();
    models.insert(
        "gpt-4.1".to_string(),
        LocalModelConfig {
            base_url: llm_base_url,
            api_key: Some("sk-local-node-secret".to_string()),
            api_key_env: None,
            extra_headers: BTreeMap::new(),
            default_temperature: Some(0.0),
            default_max_output_tokens: Some(1024),
        },
    );

    let daemon_cfg = DaemonConfig {
        node_id: NodeId::new("node_dev_mac"),
        server_url: server_url.clone(),
        auth_token: None,
        executor_mode: ExecutorMode::Subprocess,
        max_concurrent_runners: 4,
        max_turns: None,
        default_system_prompt: "Default node system prompt.".to_string(),
        default_model: Some("gpt-4.1".to_string()),
        temp_root_dir: tmp.path().join("daemon_tmp"),
        extensions_dir: ragents_daemon::default_extensions_dir(),
        allowed_workspace_dirs: vec![tmp.path().to_path_buf()],
        labels: BTreeMap::new(),
        auto_discover_models: false,
        models,
        extensions: vec![ExtensionSpec {
            name: "file_editor".to_string(),
            path: wasm_path.to_string_lossy().to_string(),
            enabled: true,
            config: serde_json::json!({}),
            allowed_dirs: vec![tmp.path().to_string_lossy().to_string()],
        }],
        interceptors: vec![],
        default_permissions: PermissionPolicyLayer::default(),
    };

    let daemon = AgentDaemon::new(daemon_cfg);
    daemon.start(cluster_cancel.clone()).await.unwrap();

    // 4. Client creates Pinned Session with `file_write.default = Ask`
    let client = RagentsClient::new(&server_url);
    let created = client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_e2e_perm")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_dev_mac"),
                    workspace_path: workspace_dir.clone(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer {
                    file_read: None,
                    file_write: Some(RuleSet {
                        deny: None,
                        allow: None,
                        default: Some(DefaultPermissionDecision::Ask),
                    }),
                    http_hosts: None,
                    command_work_dir: None,
                },
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("please write approved.txt")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .await
        .unwrap();

    let sess_id = SessionId::new(created.session.metadata.id);
    let act_id = ActivationId::new(created.initial_activation.unwrap().metadata.id);

    // 5. Watch SSE events: when PermissionRequested arrives, declaratively approve it!
    let mut watch = client.watch_events(Some(&sess_id), 0).await.unwrap();
    let mut approved = false;

    while let Some(Ok(env)) = watch.next().await {
        match env.event {
            SessionEvent::PermissionRequested { request } => {
                // Verify activation is in WaitingForInteraction phase
                let act_now = client.get_activation(&act_id).await.unwrap().unwrap();
                assert_eq!(act_now.status.phase, ActivationPhase::WaitingForInteraction);

                client
                    .respond_permission(&act_id, request.id, PermissionDecision::AllowOnce)
                    .await
                    .unwrap();
                approved = true;
            }
            SessionEvent::ActivationCompleted { .. } => {
                break;
            }
            SessionEvent::ActivationFailed { error } => {
                panic!("activation failed unexpectedly: {error}");
            }
            _ => {}
        }
    }

    assert!(
        approved,
        "permission prompt should have been triggered and approved"
    );
    let content = std::fs::read_to_string(&outside_file).unwrap();
    assert_eq!(content, "approved via declarative api-server!");

    cluster_cancel.cancel();
    llm_cancel.cancel();
}
