use super::*;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use futures::StreamExt;
use openresponses_rust::{CreateResponseBody, Item, ResponseResource};
use ragents_apiserver::{serve_tcp, ApiServerState, SqliteStore};
use ragents_client::RagentsClient;
use ragents_core::{make_openresponses_resource, CoreError, ModelBackend, ModelEndpointConfig};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

struct QueueModelBackend {
    tag: String,
    seen_inputs: Arc<Mutex<Vec<usize>>>,
}

#[async_trait]
impl ModelBackend for QueueModelBackend {
    async fn create_response(
        &self,
        _endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        let count = match &request.input {
            Some(openresponses_rust::Input::Items(items)) => items.len(),
            _ => 0,
        };
        self.seen_inputs.lock().unwrap().push(count);
        Ok(make_openresponses_resource(
            &format!("resp_{}", self.tag),
            request.model.as_deref().unwrap_or("gpt-4.1"),
            vec![Item::assistant_message(format!(
                "reply from {} (context_items={})",
                self.tag, count
            ))],
            None,
        ))
    }
}

fn make_daemon_cfg(
    node_id: &str,
    server_url: &str,
    models: Vec<&str>,
    temp_root: std::path::PathBuf,
) -> DaemonConfig {
    let mut model_map = BTreeMap::new();
    for m in &models {
        model_map.insert(
            m.to_string(),
            LocalModelConfig {
                base_url: format!("http://{node_id}.internal/v1"),
                api_key: Some(format!("sk-secret-{node_id}")),
                api_key_env: None,
                extra_headers: BTreeMap::new(),
                default_temperature: Some(0.1),
                default_max_output_tokens: Some(2048),
            },
        );
    }
    DaemonConfig {
        node_id: NodeId::new(node_id),
        server_url: server_url.to_string(),
        auth_token: None,
        executor_mode: ExecutorMode::Subprocess,
        max_concurrent_runners: 4,
        max_turns: None,
        default_system_prompt: format!("System prompt from {node_id}"),
        default_model: models.first().map(|s| s.to_string()),
        temp_root_dir: temp_root,
        allowed_workspace_dirs: default_allowed_workspace_dirs(),
        labels: BTreeMap::new(),
        auto_discover_models: false,
        models: model_map,
        extensions: vec![],
        interceptors: vec![],
        default_permissions: PermissionPolicyLayer::default(),
    }
}

#[tokio::test]
async fn test_multi_daemon_floating_failover_and_context_continuity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store.clone(), 5_000);
    let server_cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, server_cancel.clone())
        .await
        .expect("serve");
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    let cancel_a = CancellationToken::new();
    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let daemon_a = AgentDaemon::new(make_daemon_cfg(
        "node_a",
        &server_url,
        vec!["claude-sonnet-4"],
        tmp.path().join("node_a"),
    ))
    .with_model_backend(Arc::new(QueueModelBackend {
        tag: "node_a".to_string(),
        seen_inputs: seen_a.clone(),
    }));
    daemon_a
        .start(cancel_a.clone())
        .await
        .expect("start node_a");

    let created = client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_floating_cluster")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("claude-sonnet-4")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("hello turn 1")],
                model_override: None,
            }),
        })
        .await
        .expect("create floating session");

    let sess_id = SessionId::new(created.session.metadata.id);
    let mut watch = client
        .watch_events(Some(&sess_id), 0)
        .await
        .expect("watch events");

    while let Some(Ok(env)) = watch.next().await {
        if matches!(env.event, SessionEvent::ActivationCompleted { .. }) {
            break;
        }
    }

    cancel_a.cancel();
    store
        .patrol_expired_leases(Utc::now() + Duration::seconds(15))
        .expect("expire node_a");

    let cancel_b = CancellationToken::new();
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let daemon_b = AgentDaemon::new(make_daemon_cfg(
        "node_b",
        &server_url,
        vec!["claude-sonnet-4"],
        tmp.path().join("node_b"),
    ))
    .with_model_backend(Arc::new(QueueModelBackend {
        tag: "node_b".to_string(),
        seen_inputs: seen_b.clone(),
    }));
    daemon_b
        .start(cancel_b.clone())
        .await
        .expect("start node_b");

    let act2 = client
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: sess_id.clone(),
                submission_kind: SubmissionKind::FollowUp,
                input: vec![Item::user_message("hello turn 2 after failover")],
                model_override: None,
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .await
        .expect("create turn 2 activation");

    while let Some(Ok(env)) = watch.next().await {
        if env.activation_id.as_ref().map(|a| a.as_str()) == Some(&act2.metadata.id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    assert_eq!(*seen_a.lock().unwrap(), vec![1]);
    assert_eq!(*seen_b.lock().unwrap(), vec![3]);

    let updated_sess = client
        .get_session(&sess_id)
        .await
        .unwrap()
        .expect("session exists");
    assert_eq!(
        updated_sess.status.bound_node_id,
        Some(NodeId::new("node_b"))
    );
    assert_eq!(updated_sess.status.effective_context_item_count, 4);

    cancel_b.cancel();
    server_cancel.cancel();
}

struct InspectingRawBackend {
    captured_endpoint: Arc<Mutex<Option<ModelEndpointConfig>>>,
    captured_raw_json: Arc<Mutex<Option<serde_json::Value>>>,
}

#[async_trait]
impl ModelBackend for InspectingRawBackend {
    async fn create_response(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        let raw = serde_json::to_value(&request)?;
        self.create_response_raw(endpoint, raw).await
    }

    async fn create_response_raw(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
    ) -> Result<ResponseResource, CoreError> {
        *self.captured_endpoint.lock().unwrap() = Some(endpoint.clone());
        *self.captured_raw_json.lock().unwrap() = Some(request_json);
        Ok(make_openresponses_resource(
            "resp_intercepted",
            "gpt-4.1",
            vec![Item::assistant_message("original model answer")],
            None,
        ))
    }
}

#[tokio::test]
async fn test_daemon_model_interceptor_mutates_params_headers_and_vendor_fields() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store, 30_000);
    let server_cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = serve_tcp(listener, state, server_cancel.clone())
        .await
        .unwrap();
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    let _ = std::fs::create_dir_all("/tmp/ragents-ipc");
    let sock_path =
        std::path::PathBuf::from(format!("/tmp/ragents-ipc/ext_{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock_path);
    let uds_listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
    let sock_path_clone = sock_path.clone();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = uds_listener.accept().await {
            let (r, mut w) = stream.split();
            let mut lines = BufReader::new(r).lines();
            if let Ok(Some(line)) = lines.next_line().await {
                let inv: ControlHookInvocation = serde_json::from_str(&line).unwrap();
                let reply = match inv.hook_name.as_str() {
                    "before_model_request" => {
                        let mut input: ModelRequestInterceptInput =
                            serde_json::from_value(inv.payload).unwrap();
                        if let Some(obj) = input.request_body.as_object_mut() {
                            obj.insert("temperature".to_string(), serde_json::json!(0.15));
                            obj.insert("thinking_budget".to_string(), serde_json::json!(4096));
                        }
                        let mut headers = BTreeMap::new();
                        headers.insert(
                            "x-gateway-trace".to_string(),
                            "trace-from-uds-interceptor".to_string(),
                        );
                        let outcome = ModelRequestInterceptOutcome::Continue {
                            request_body: Some(input.request_body),
                            override_base_url: None,
                            upsert_headers: headers,
                            override_api_key: Some("sk-dynamic-rotated-token".to_string()),
                        };
                        ControlHookResult {
                            ok: true,
                            output: serde_json::to_value(outcome).unwrap(),
                            error: None,
                        }
                    }
                    "after_model_response" => {
                        let mut input: ModelResponseInterceptInput =
                            serde_json::from_value(inv.payload).unwrap();
                        input.response.output = vec![Item::assistant_message(
                            "post-processed by after_model_response interceptor",
                        )];
                        let outcome = ModelResponseInterceptOutcome::Continue {
                            response: Some(Box::new(input.response)),
                        };
                        ControlHookResult {
                            ok: true,
                            output: serde_json::to_value(outcome).unwrap(),
                            error: None,
                        }
                    }
                    _ => ControlHookResult {
                        ok: true,
                        output: serde_json::Value::Null,
                        error: None,
                    },
                };
                let mut out_str = serde_json::to_string(&reply).unwrap();
                out_str.push('\n');
                let _ = w.write_all(out_str.as_bytes()).await;
            }
        }
        let _ = std::fs::remove_file(sock_path_clone);
    });

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = DaemonConfig::default_local("node_intercept", &server_url);
    cfg.temp_root_dir = tmp.path().join("tmp");
    cfg.interceptors.push(DaemonInterceptorSpec {
        id: "uds-model-tuner".to_string(),
        driver: ExtensionDriverConfig::Webhook {
            endpoint: format!("unix://{}", sock_path.display()),
        },
        hooks: vec![
            DaemonHookKind::BeforeModelRequest,
            DaemonHookKind::AfterModelResponse,
        ],
        priority: 10,
        timeout_ms: 2000,
        fail_policy: ExtensionFailPolicy::FailClosed,
        config: serde_json::json!({}),
    });

    let captured_ep = Arc::new(Mutex::new(None));
    let captured_json = Arc::new(Mutex::new(None));
    let daemon = AgentDaemon::new(cfg).with_model_backend(Arc::new(InspectingRawBackend {
        captured_endpoint: captured_ep.clone(),
        captured_raw_json: captured_json.clone(),
    }));

    let d_cancel = CancellationToken::new();
    daemon.start(d_cancel.clone()).await.unwrap();

    let mut watch = client.watch_events(None, 0).await.unwrap();
    let created = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("test model interceptor")],
                model_override: None,
            }),
        })
        .await
        .unwrap();

    let sess_id = SessionId::new(created.session.metadata.id);
    let act_id = ActivationId::new(created.initial_activation.unwrap().metadata.id);

    while let Some(Ok(env)) = watch.next().await {
        if env.activation_id.as_ref() == Some(&act_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    let ep = captured_ep
        .lock()
        .unwrap()
        .clone()
        .expect("endpoint captured");
    assert_eq!(ep.api_key, "sk-dynamic-rotated-token");
    assert_eq!(
        ep.extra_headers.get("x-gateway-trace").map(|s| s.as_str()),
        Some("trace-from-uds-interceptor")
    );

    let raw_body = captured_json
        .lock()
        .unwrap()
        .clone()
        .expect("body captured");
    assert_eq!(raw_body.get("temperature"), Some(&serde_json::json!(0.15)));
    assert_eq!(
        raw_body.get("thinking_budget"),
        Some(&serde_json::json!(4096))
    );
    let meta = raw_body.get("metadata").expect("metadata injected");
    assert_eq!(
        meta.get("ragents.session_id").and_then(|v| v.as_str()),
        Some(sess_id.as_str())
    );
    assert_eq!(
        meta.get("ragents.activation_id").and_then(|v| v.as_str()),
        Some(act_id.as_str())
    );
    assert!(meta
        .get("ragents.turn_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .starts_with("turn_"));

    let ctx = client.get_session_context(&sess_id).await.unwrap();
    let last_text = ragents_core::extract_item_text(ctx.effective_items.last().unwrap());
    assert_eq!(
        last_text,
        "post-processed by after_model_response interceptor"
    );

    d_cancel.cancel();
    server_cancel.cancel();
}

#[tokio::test]
async fn test_background_process_lifecycle_logs_stop_and_multi_block_realtime_queue() {
    let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    if !shell_wasm_path.exists() {
        return;
    }

    let store = SqliteStore::open_in_memory().unwrap();
    let state = ApiServerState::new(store, 5000);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_cancel = CancellationToken::new();
    let addr = serve_tcp(listener, state, server_cancel.clone())
        .await
        .unwrap();
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    let base_tmp = std::env::temp_dir().join(format!("ragents-bg-proc-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    std::fs::create_dir_all(&ws_root).unwrap();

    let mut cfg = DaemonConfig::default_local("node-bg", &server_url);
    cfg.executor_mode = ExecutorMode::Subprocess;
    cfg.extensions.push(ExtensionSpec {
        name: "shell".to_string(),
        path: shell_wasm_path.to_string_lossy().to_string(),
        enabled: true,
        config: serde_json::json!({}),
        allowed_dirs: vec![],
    });

    struct BgTestBackend {
        step_counter: std::sync::atomic::AtomicUsize,
        session_id: Arc<Mutex<Option<SessionId>>>,
        client: RagentsClient,
        session_srv_proc_id: Arc<Mutex<Option<String>>>,
        act_srv_pid: Arc<Mutex<Option<u32>>>,
    }

    #[async_trait]
    impl ModelBackend for BgTestBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            let step = self
                .step_counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let items = match request.input {
                Some(openresponses_rust::Input::Items(ref v)) => v.clone(),
                _ => Vec::new(),
            };

            match step {
                0 => Ok(make_openresponses_resource(
                    "resp_bg_1",
                    "gpt-4.1",
                    vec![
                        Item::FunctionCall {
                            id: Some("fc_bg_short".into()),
                            call_id: "call_bg_short".into(),
                            name: "shell".into(),
                            arguments: serde_json::json!({
                                "command": "echo out_line_1 && echo err_line_1 >&2 && sleep 0.3 && echo out_line_2",
                                "wait": false,
                                "lifecycle": "activation"
                            })
                            .to_string(),
                            status: None,
                        },
                        Item::FunctionCall {
                            id: Some("fc_bg_act".into()),
                            call_id: "call_bg_act".into(),
                            name: "shell".into(),
                            arguments: serde_json::json!({
                                "command": "sleep 60",
                                "wait": false,
                                "lifecycle": "activation"
                            })
                            .to_string(),
                            status: None,
                        },
                        Item::FunctionCall {
                            id: Some("fc_bg_sess".into()),
                            call_id: "call_bg_sess".into(),
                            name: "shell".into(),
                            arguments: serde_json::json!({
                                "command": "echo session_daemon_ready && sleep 60",
                                "wait": false,
                                "lifecycle": "session"
                            })
                            .to_string(),
                            status: None,
                        },
                    ],
                    None,
                )),
                1 => {
                    let mut short_id = String::new();
                    for item in &items {
                        if let Item::FunctionCallOutput {
                            call_id, output, ..
                        } = item
                        {
                            let text = serde_json::to_value(output)
                                .ok()
                                .and_then(|v| v.as_str().map(String::from))
                                .unwrap_or_default();
                            if call_id == "call_bg_short" {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                    short_id = v["process_id"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .to_string();
                                }
                            } else if call_id == "call_bg_act" {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                    *self.act_srv_pid.lock().unwrap() =
                                        v["pid"].as_u64().map(|p| p as u32);
                                }
                            } else if call_id == "call_bg_sess" {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                    *self.session_srv_proc_id.lock().unwrap() =
                                        v["process_id"].as_str().map(String::from);
                                }
                            }
                        }
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(450)).await;
                    let maybe_sess = self.session_id.lock().unwrap().clone();
                    if let Some(sess_id) = maybe_sess {
                        let _ = self
                            .client
                            .push_realtime_message(
                                &sess_id,
                                PushRealtimeMessageRequest::from_text(
                                    "User steer message block #1",
                                ),
                            )
                            .await;
                        let _ = self
                            .client
                            .push_realtime_message(
                                &sess_id,
                                PushRealtimeMessageRequest::from_text(
                                    "User steer message block #2",
                                ),
                            )
                            .await;
                    }

                    Ok(make_openresponses_resource(
                        "resp_bg_2",
                        "gpt-4.1",
                        vec![
                            Item::FunctionCall {
                                id: Some("fc_logs_out".into()),
                                call_id: "call_logs_out".into(),
                                name: "process_logs".into(),
                                arguments: serde_json::json!({
                                    "process_id": short_id,
                                    "stream": "stdout"
                                })
                                .to_string(),
                                status: None,
                            },
                            Item::FunctionCall {
                                id: Some("fc_logs_err".into()),
                                call_id: "call_logs_err".into(),
                                name: "process_logs".into(),
                                arguments: serde_json::json!({
                                    "process_id": short_id,
                                    "stream": "stderr"
                                })
                                .to_string(),
                                status: None,
                            },
                        ],
                        None,
                    ))
                }
                2 => Ok(make_openresponses_resource(
                    "resp_bg_3",
                    "gpt-4.1",
                    vec![Item::assistant_message("Activation 1 done.")],
                    None,
                )),
                3 => {
                    let sess_proc = self
                        .session_srv_proc_id
                        .lock()
                        .unwrap()
                        .clone()
                        .expect("session proc id");
                    Ok(make_openresponses_resource(
                        "resp_bg_4",
                        "gpt-4.1",
                        vec![
                            Item::FunctionCall {
                                id: Some("fc_st".into()),
                                call_id: "call_st".into(),
                                name: "process_status".into(),
                                arguments: serde_json::json!({
                                    "process_id": sess_proc
                                })
                                .to_string(),
                                status: None,
                            },
                            Item::FunctionCall {
                                id: Some("fc_stop".into()),
                                call_id: "call_stop".into(),
                                name: "process_stop".into(),
                                arguments: serde_json::json!({
                                    "process_id": sess_proc
                                })
                                .to_string(),
                                status: None,
                            },
                        ],
                        None,
                    ))
                }
                _ => Ok(make_openresponses_resource(
                    "resp_bg_5",
                    "gpt-4.1",
                    vec![Item::assistant_message("Activation 2 done.")],
                    None,
                )),
            }
        }
    }

    let session_id_holder = Arc::new(Mutex::new(None));
    let session_srv_proc_id = Arc::new(Mutex::new(None));
    let act_srv_pid = Arc::new(Mutex::new(None));

    let backend = Arc::new(BgTestBackend {
        step_counter: std::sync::atomic::AtomicUsize::new(0),
        session_id: session_id_holder.clone(),
        client: client.clone(),
        session_srv_proc_id: session_srv_proc_id.clone(),
        act_srv_pid: act_srv_pid.clone(),
    });

    let daemon = AgentDaemon::new(cfg).with_model_backend(backend);
    let d_cancel = CancellationToken::new();
    daemon.start(d_cancel.clone()).await.unwrap();

    let created = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node-bg"),
                    workspace_path: ws_root.clone(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await
        .unwrap();
    let sess_id = SessionId::new(created.session.metadata.id);
    *session_id_holder.lock().unwrap() = Some(sess_id.clone());

    let act1 = client
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: sess_id.clone(),
                submission_kind: SubmissionKind::Normal,
                input: vec![Item::user_message("Start background services")],
                model_override: None,
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .await
        .unwrap();
    let act1_id = ActivationId::new(act1.metadata.id);

    let mut watch = client.watch_events(Some(&sess_id), 0).await.unwrap();
    while let Some(Ok(env)) = watch.next().await {
        if env.activation_id.as_ref() == Some(&act1_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    let ctx1 = client.get_session_context(&sess_id).await.unwrap();
    let multi_block_msg = ctx1.effective_items.iter().find(|item| {
        if let Item::Message { content, .. } = item {
            content.len() >= 2
        } else {
            false
        }
    });
    assert!(
        multi_block_msg.is_some(),
        "Expected drained real-time queue item to have multiple separate MessageContent blocks, got items: {:?}",
        ctx1.effective_items
    );
    if let Some(Item::Message { content, .. }) = multi_block_msg {
        let n = content.len();
        let second_last = serde_json::to_string(&content[n - 2]).unwrap();
        let last = serde_json::to_string(&content[n - 1]).unwrap();
        assert!(second_last.contains("User steer message block #1"));
        assert!(last.contains("User steer message block #2"));
    }
    let ctx1_json = serde_json::to_string(&ctx1.effective_items).unwrap();
    assert!(
        ctx1_json.contains("Background Process Exited"),
        "ctx1 must also contain background process exit notification: {ctx1_json}"
    );

    let maybe_act_pid = *act_srv_pid.lock().unwrap();
    if let Some(act_pid) = maybe_act_pid {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let status = std::process::Command::new("kill")
            .arg("-0")
            .arg(act_pid.to_string())
            .status();
        assert!(
            status.map(|s| !s.success()).unwrap_or(true),
            "activation-scoped background process pid {act_pid} should have been terminated"
        );
    }

    let act2 = client
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: sess_id.clone(),
                submission_kind: SubmissionKind::FollowUp,
                input: vec![Item::user_message("Check and stop session process")],
                model_override: None,
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .await
        .unwrap();
    let act2_id = ActivationId::new(act2.metadata.id);

    while let Some(Ok(env)) = watch.next().await {
        if env.activation_id.as_ref() == Some(&act2_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    let ctx2 = client.get_session_context(&sess_id).await.unwrap();
    let ctx2_json = serde_json::to_string(&ctx2.effective_items).unwrap();
    assert!(
        ctx2_json.contains("\\\"phase\\\": \\\"running\\\""),
        "ctx2: {ctx2_json}"
    );
    assert!(
        ctx2_json.contains("\\\"phase\\\": \\\"killed\\\""),
        "ctx2: {ctx2_json}"
    );
    assert!(ctx2_json.contains("out_line_1"), "ctx2: {ctx2_json}");
    assert!(ctx2_json.contains("err_line_1"), "ctx2: {ctx2_json}");

    // Now that `sess_id` is completely idle (`act2` has completed), push a realtime message
    // and verify that `push_realtime_message` automatically spawns and runs a Continuation Activation!
    let wake_resp = client
        .push_realtime_message(
            &sess_id,
            PushRealtimeMessageRequest::from_text("Wake up idle session via realtime queue!"),
        )
        .await
        .unwrap();
    let wake_act_id = wake_resp
        .spawned_activation_id
        .expect("push_realtime_message on idle session must auto-spawn an activation");

    while let Some(Ok(env)) = watch.next().await {
        if env.activation_id.as_ref() == Some(&wake_act_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    let ctx3 = client.get_session_context(&sess_id).await.unwrap();
    let ctx3_json = serde_json::to_string(&ctx3.effective_items).unwrap();
    assert!(
        ctx3_json.contains("Wake up idle session via realtime queue!"),
        "ctx3 must contain the drained realtime message that woke up the session: {ctx3_json}"
    );

    d_cancel.cancel();
    server_cancel.cancel();
    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_three_session_context_compaction_and_multi_generation_chain() {
    use std::path::PathBuf;
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compaction_wasm = manifest_dir.join(
        "../../extensions/compaction/target/wasm32-wasip2/release/ragent_compaction_extension.wasm",
    );
    let file_editor_wasm = manifest_dir.join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    assert!(
        compaction_wasm.exists(),
        "ragent_compaction_extension.wasm must be built"
    );
    assert!(
        file_editor_wasm.exists(),
        "ragent_file_editor_extension.wasm must be built"
    );

    let store = SqliteStore::open_in_memory().unwrap();
    let state = ApiServerState::new(store, 5000);
    let server_cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = serve_tcp(listener, state, server_cancel.clone())
        .await
        .unwrap();
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    struct CompactionTestBackend {
        step: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl ModelBackend for CompactionTestBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            let role = request
                .metadata
                .as_ref()
                .and_then(|m| m.get("ragents.role"))
                .map(|s| s.as_str())
                .unwrap_or("working");

            let input_json = serde_json::to_string(&request.input).unwrap_or_default();

            if role == "compaction_worker" {
                let is_incremental = input_json.contains("<previous-summary>");
                let summary_text = if is_incremental {
                    "## Goal\nBuild & verify multi-generation compacted service.\n\n## Progress\n### Done\n- [x] Read config.toml and wrote output_v1.txt\n- [x] Read schema.sql and edited output_v1.txt in generation 2"
                } else {
                    "## Goal\nBuild & verify compacted service.\n\n## Progress\n### Done\n- [x] Read config.toml and wrote output_v1.txt"
                };
                return Ok(make_openresponses_resource(
                    &format!("resp_compact_{}", uuid::Uuid::now_v7()),
                    "gpt-4.1",
                    vec![Item::assistant_message(summary_text)],
                    None,
                ));
            }

            let current = {
                let mut g = self.step.lock().unwrap();
                *g += 1;
                *g
            };

            let items = match current {
                1 => vec![
                    Item::FunctionCall {
                        id: None,
                        call_id: "call_read_1".into(),
                        name: "read_file".into(),
                        arguments: serde_json::json!({ "path": "config.toml" }).to_string(),
                        status: None,
                    },
                    Item::FunctionCall {
                        id: None,
                        call_id: "call_write_1".into(),
                        name: "write_file".into(),
                        arguments: serde_json::json!({
                            "path": "output_v1.txt",
                            "content": "initial content payload that adds enough tokens to the conversation history for compaction testing."
                        })
                        .to_string(),
                        status: None,
                    },
                ],
                2 => vec![Item::FunctionCall {
                    id: None,
                    call_id: "call_read_verify_1".into(),
                    name: "read_file".into(),
                    arguments: serde_json::json!({ "path": "output_v1.txt" }).to_string(),
                    status: None,
                }],
                3 => {
                    // At step 3, `sess_1` has compacted into `sess_2`!
                    // Verify `request.input` starts with `[Compaction Summary]` from `sess_compact_1`!
                    assert!(
                        input_json.contains("[Compaction Summary]"),
                        "step 3 on sess_2 must start with [Compaction Summary], got: {input_json}"
                    );
                    assert!(
                        input_json.contains("<read-files>\\nconfig.toml\\n</read-files>"),
                        "step 3 summary must track read-only config.toml: {input_json}"
                    );
                    assert!(
                        input_json.contains("<modified-files>\\noutput_v1.txt\\n</modified-files>"),
                        "step 3 summary must track modified output_v1.txt: {input_json}"
                    );
                    vec![Item::assistant_message(
                        "Completed phase 1 after first automatic compaction.",
                    )]
                }
                4 => vec![
                    Item::FunctionCall {
                        id: None,
                        call_id: "call_read_2".into(),
                        name: "read_file".into(),
                        arguments: serde_json::json!({ "path": "schema.sql" }).to_string(),
                        status: None,
                    },
                    Item::FunctionCall {
                        id: None,
                        call_id: "call_edit_2".into(),
                        name: "replace_in_file".into(),
                        arguments: serde_json::json!({
                            "path": "output_v1.txt",
                            "edits": [{
                                "oldText": "initial content payload",
                                "newText": "updated generation 2 payload"
                            }]
                        })
                        .to_string(),
                        status: None,
                    },
                ],
                5 => vec![Item::FunctionCall {
                    id: None,
                    call_id: "call_read_verify_2".into(),
                    name: "read_file".into(),
                    arguments: serde_json::json!({ "path": "output_v1.txt" }).to_string(),
                    status: None,
                }],
                _ => {
                    // Step 6 runs on `sess_3` (generation 2 compaction)!
                    assert!(
                        input_json.contains("edited output_v1.txt in generation 2"),
                        "step 6 on sess_3 must contain generation 2 updated summary: {input_json}"
                    );
                    assert!(
                        input_json.contains("config.toml") && input_json.contains("schema.sql"),
                        "step 6 summary must preserve cumulative read_files across generations: {input_json}"
                    );
                    vec![Item::assistant_message(
                        "Completed phase 2 on sess_3 after second consecutive compaction.",
                    )]
                }
            };

            let reported_total_tokens = if current == 2 || current == 5 {
                500
            } else {
                60
            };

            Ok(make_openresponses_resource(
                &format!("resp_work_{current}"),
                "gpt-4.1",
                items,
                Some(openresponses_rust::Usage {
                    input_tokens: reported_total_tokens - 20,
                    output_tokens: 20,
                    total_tokens: reported_total_tokens,
                    input_tokens_details: openresponses_rust::InputTokensDetails {
                        cached_tokens: 0,
                    },
                    output_tokens_details: openresponses_rust::OutputTokensDetails {
                        reasoning_tokens: 0,
                    },
                }),
            ))
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let ws_dir = tmp.path().join("ws");
    std::fs::create_dir_all(&ws_dir).unwrap();
    std::fs::write(
        ws_dir.join("config.toml"),
        "port = 8080\nmode = 'production'\ndescription = 'configuration file with enough text to contribute tokens'\n",
    )
    .unwrap();
    std::fs::write(
        ws_dir.join("schema.sql"),
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL, details TEXT);\n",
    )
    .unwrap();

    let mut cfg = DaemonConfig::default_local("node_compact", &server_url);
    cfg.temp_root_dir = tmp.path().join("tmp");
    cfg.extensions = vec![
        ExtensionSpec {
            name: "file_editor".into(),
            path: file_editor_wasm.to_string_lossy().to_string(),
            enabled: true,
            config: serde_json::json!({}),
            allowed_dirs: vec![],
        },
        ExtensionSpec {
            name: "compaction".into(),
            path: compaction_wasm.to_string_lossy().to_string(),
            enabled: true,
            config: serde_json::json!({
                "enabled": true,
                "trigger_tokens": 400,
                "keep_recent_tokens": 25
            }),
            allowed_dirs: vec![],
        },
    ];

    let daemon = AgentDaemon::new(cfg).with_model_backend(Arc::new(CompactionTestBackend {
        step: Mutex::new(0),
    }));
    let d_cancel = CancellationToken::new();
    daemon.start(d_cancel.clone()).await.unwrap();

    let mut watch = client.watch_events(None, 0).await.unwrap();

    let created = client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_gen0")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_compact"),
                    workspace_path: ws_dir.clone(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("You are a helpful coding agent.".into()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message(
                    "Please inspect config.toml and write output_v1.txt with the initial payload.",
                )],
                model_override: None,
            }),
        })
        .await
        .unwrap();

    let sess_1_id = SessionId::new(created.session.metadata.id);
    assert_eq!(sess_1_id.as_str(), "sess_gen0");

    // Wait for `SessionCompacted` on `sess_1`, and then `ActivationCompleted` on `sess_2`!
    let mut compact_1_id: Option<SessionId> = None;
    let mut sess_2_id: Option<SessionId> = None;
    let mut cont_act_1_id: Option<ActivationId> = None;

    while let Some(Ok(env)) = watch.next().await {
        if let SessionEvent::SessionCompacted {
            ref from_session_id,
            ref compaction_session_id,
            ref continued_session_id,
            ref continued_activation_id,
            compaction_generation,
            ..
        } = env.event
        {
            assert_eq!(from_session_id, &sess_1_id);
            assert_eq!(compaction_generation, 1);
            compact_1_id = Some(compaction_session_id.clone());
            sess_2_id = Some(continued_session_id.clone());
            cont_act_1_id = Some(continued_activation_id.clone());
        }
        if let Some(ref target_act) = cont_act_1_id {
            if env.activation_id.as_ref() == Some(target_act)
                && matches!(env.event, SessionEvent::ActivationCompleted { .. })
            {
                break;
            }
        }
    }

    let compact_1_id = compact_1_id.expect("compact_1_id emitted");
    let sess_2_id = sess_2_id.expect("sess_2_id emitted");

    // Verify bidirectional lineage across `sess_1 <-> compact_1 <-> sess_2`
    let s1 = client.get_session(&sess_1_id).await.unwrap().unwrap();
    let sc1 = client.get_session(&compact_1_id).await.unwrap().unwrap();
    let s2 = client.get_session(&sess_2_id).await.unwrap().unwrap();

    assert_eq!(s1.status.phase, SessionPhase::Closed);
    assert_eq!(
        s1.status.compaction.compaction_session_id.as_ref(),
        Some(&compact_1_id)
    );
    assert_eq!(
        s1.status.compaction.continued_in_session_id.as_ref(),
        Some(&sess_2_id)
    );

    assert_eq!(sc1.status.phase, SessionPhase::Closed);
    assert_eq!(sc1.status.compaction.role, SessionRole::CompactionWorker);
    assert_eq!(sc1.status.compaction.generation, 1);
    assert_eq!(
        sc1.status.compaction.compacted_from_session_id.as_ref(),
        Some(&sess_1_id)
    );
    assert_eq!(
        sc1.status.compaction.continued_in_session_id.as_ref(),
        Some(&sess_2_id)
    );

    assert_eq!(s2.status.phase, SessionPhase::Open);
    assert_eq!(s2.status.compaction.role, SessionRole::Working);
    assert_eq!(s2.status.compaction.generation, 1);
    assert_eq!(
        s2.status.compaction.compacted_from_session_id.as_ref(),
        Some(&sess_1_id)
    );
    assert_eq!(
        s2.status.compaction.compaction_session_id.as_ref(),
        Some(&compact_1_id)
    );
    assert_eq!(s2.spec.sources.len(), 2);
    assert_eq!(s2.spec.sources[0].kind, SessionSourceKind::SummaryOf);
    assert_eq!(s2.spec.sources[0].session_id, compact_1_id);
    assert_eq!(s2.spec.sources[1].kind, SessionSourceKind::ContinuedFrom);
    assert_eq!(s2.spec.sources[1].session_id, sess_1_id);

    // Now submit a second task targeting `sess_1_id` (verifying automatic compaction-chain routing to `sess_2`!)
    // which will trigger a second consecutive compaction (`sess_2 -> compact_2 -> sess_3`, generation = 2)!
    let act_phase2 = client
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: sess_1_id.clone(),
                submission_kind: SubmissionKind::Normal,
                input: vec![Item::user_message(
                    "Now read schema.sql and update output_v1.txt for phase 2.",
                )],
                model_override: None,
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .await
        .unwrap();
    // Because `sess_1` was compacted into `sess_2`, `create_activation` automatically routed it to `sess_2`!
    assert_eq!(act_phase2.spec.session_id, sess_2_id);

    let mut compact_2_id: Option<SessionId> = None;
    let mut sess_3_id: Option<SessionId> = None;
    let mut cont_act_2_id: Option<ActivationId> = None;

    while let Some(Ok(env)) = watch.next().await {
        if let SessionEvent::SessionCompacted {
            ref from_session_id,
            ref compaction_session_id,
            ref continued_session_id,
            ref continued_activation_id,
            compaction_generation,
            ..
        } = env.event
        {
            if from_session_id == &sess_2_id {
                assert_eq!(compaction_generation, 2);
                compact_2_id = Some(compaction_session_id.clone());
                sess_3_id = Some(continued_session_id.clone());
                cont_act_2_id = Some(continued_activation_id.clone());
            }
        }
        if let Some(ref target_act) = cont_act_2_id {
            if env.activation_id.as_ref() == Some(target_act)
                && matches!(env.event, SessionEvent::ActivationCompleted { .. })
            {
                break;
            }
        }
    }

    let compact_2_id = compact_2_id.expect("compact_2_id emitted");
    let sess_3_id = sess_3_id.expect("sess_3_id emitted");

    let s2_after = client.get_session(&sess_2_id).await.unwrap().unwrap();
    let sc2 = client.get_session(&compact_2_id).await.unwrap().unwrap();
    let s3 = client.get_session(&sess_3_id).await.unwrap().unwrap();

    assert_eq!(s2_after.status.phase, SessionPhase::Closed);
    assert_eq!(
        s2_after.status.compaction.continued_in_session_id.as_ref(),
        Some(&sess_3_id)
    );
    assert_eq!(sc2.status.compaction.role, SessionRole::CompactionWorker);
    assert_eq!(sc2.status.compaction.generation, 2);

    // Verify `compact_2`'s input prompt included `<previous-summary>` from `compact_1`!
    let sc2_ctx = client.get_session_context(&compact_2_id).await.unwrap();
    let sc2_prompt_json = serde_json::to_string(&sc2_ctx.effective_items[0]).unwrap();
    assert!(
        sc2_prompt_json.contains("<previous-summary>"),
        "generation 2 compaction prompt must contain <previous-summary>: {sc2_prompt_json}"
    );

    // Verify `sess_3` has generation 2 and flat 2-entry `sources` (`SummaryOf(compact_2)` + `ContinuedFrom(sess_2)`)
    assert_eq!(s3.status.phase, SessionPhase::Open);
    assert_eq!(s3.status.compaction.generation, 2);
    assert_eq!(
        s3.status.compaction.compacted_from_session_id.as_ref(),
        Some(&sess_2_id)
    );
    assert_eq!(
        s3.status.compaction.compaction_session_id.as_ref(),
        Some(&compact_2_id)
    );
    assert_eq!(s3.spec.sources.len(), 2);
    assert_eq!(s3.spec.sources[0].session_id, compact_2_id);
    assert_eq!(s3.spec.sources[1].session_id, sess_2_id);

    d_cancel.cancel();
    server_cancel.cancel();
}

#[tokio::test]
async fn test_daemon_auto_reconnect_and_session_close_process_cleanup() {
    use std::path::PathBuf;
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let shell_wasm = manifest_dir
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    assert!(
        shell_wasm.exists(),
        "ragent_shell_extension.wasm must exist"
    );

    let base_tmp = PathBuf::from("/tmp").join(format!("ragents_reconn_{}", uuid::Uuid::now_v7()));
    let ws_dir = base_tmp.join("ws");
    let db_path = base_tmp.join("state.db");
    std::fs::create_dir_all(&ws_dir).unwrap();

    let store1 = SqliteStore::open(&db_path).unwrap();
    let state1 = ApiServerState::new(store1, 5000);
    let server_cancel_1 = CancellationToken::new();
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = serve_tcp(listener1, state1, server_cancel_1.clone())
        .await
        .unwrap();
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    struct ReconnectTestBackend {
        step: Mutex<usize>,
        captured_pid: Arc<Mutex<Option<u32>>>,
    }

    #[async_trait]
    impl ModelBackend for ReconnectTestBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            if let Some(openresponses_rust::Input::Items(items)) = &request.input {
                for it in items {
                    if let Item::FunctionCallOutput {
                        output: openresponses_rust::FunctionOutput::Text(t),
                        ..
                    } = it
                    {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(t) {
                            if let Some(pid) = v.get("pid").and_then(|p| p.as_u64()) {
                                *self.captured_pid.lock().unwrap() = Some(pid as u32);
                            }
                        }
                    }
                }
            }

            let s = {
                let mut g = self.step.lock().unwrap();
                *g += 1;
                *g
            };

            let out = match s {
                1 => vec![Item::FunctionCall {
                    id: None,
                    call_id: "call_bg_srv".into(),
                    name: "shell".into(),
                    arguments: serde_json::json!({
                        "command": "sleep 60",
                        "wait": false,
                        "lifecycle": "session"
                    })
                    .to_string(),
                    status: None,
                }],
                2 => vec![Item::assistant_message(
                    "Session background process started.",
                )],
                _ => vec![Item::assistant_message(
                    "Executed activation after daemon auto-reconnected!",
                )],
            };

            Ok(make_openresponses_resource(
                &format!("resp_reconn_{s}"),
                "gpt-4.1",
                out,
                None,
            ))
        }
    }

    let captured_pid = Arc::new(Mutex::new(None));
    let mut cfg = DaemonConfig::default_local("node_reconn", &server_url);
    cfg.temp_root_dir = base_tmp.join("tmp");
    cfg.extensions = vec![ExtensionSpec {
        name: "shell".into(),
        path: shell_wasm.to_string_lossy().to_string(),
        enabled: true,
        config: serde_json::json!({}),
        allowed_dirs: vec![],
    }];

    let daemon = AgentDaemon::new(cfg).with_model_backend(Arc::new(ReconnectTestBackend {
        step: Mutex::new(0),
        captured_pid: captured_pid.clone(),
    }));
    let d_cancel = CancellationToken::new();
    daemon.start(d_cancel.clone()).await.unwrap();

    let created = client
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_reconn_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_reconn"),
                    workspace_path: ws_dir.clone(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("Start session background sleep")],
                model_override: None,
            }),
        })
        .await
        .unwrap();
    let sess_id = SessionId::new(created.session.metadata.id);
    let act1_id = ActivationId::new(created.initial_activation.unwrap().metadata.id);

    let mut watch1 = client.watch_events(Some(&sess_id), 0).await.unwrap();
    while let Some(Ok(env)) = watch1.next().await {
        if env.activation_id.as_ref() == Some(&act1_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }
    drop(watch1);

    let bg_pid = captured_pid
        .lock()
        .unwrap()
        .expect("captured session-scoped background process pid");
    let alive_before = std::process::Command::new("kill")
        .arg("-0")
        .arg(bg_pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(
        alive_before,
        "session background process {bg_pid} must be alive"
    );

    // 1. Simulate `api-server` restart: shut down `server_cancel_1`, wait briefly, then bind `addr` again!
    server_cancel_1.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let store2 = SqliteStore::open(&db_path).unwrap();
    let state2 = ApiServerState::new(store2, 5000);
    let server_cancel_2 = CancellationToken::new();
    let listener2 = TcpListener::bind(addr).await.unwrap();
    serve_tcp(listener2, state2, server_cancel_2.clone())
        .await
        .unwrap();

    // Wait briefly for `AgentDaemon` exponential-backoff loop to reconnect and re-register
    let mut watch2 = client.watch_events(Some(&sess_id), 0).await.unwrap();
    let act2 = client
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: sess_id.clone(),
                submission_kind: SubmissionKind::Normal,
                input: vec![Item::user_message(
                    "Verify daemon reconnected after server restart",
                )],
                model_override: None,
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .await
        .unwrap();
    let act2_id = ActivationId::new(act2.metadata.id);

    while let Some(Ok(env)) = watch2.next().await {
        if env.activation_id.as_ref() == Some(&act2_id)
            && matches!(env.event, SessionEvent::ActivationCompleted { .. })
        {
            break;
        }
    }

    // 2. Now close `sess_id` via `patch_session` (`DesiredSessionPhase::Closed`)
    // and verify that `AgentDaemon` receives `CleanupSession` and kills `bg_pid`!
    let patched = client
        .patch_session(
            &sess_id,
            PatchSessionSpecRequest {
                desired_phase: Some(DesiredSessionPhase::Closed),
                system_prompt: None,
                model: None,
                extensions: None,
                permissions: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(patched.status.phase, SessionPhase::Closed);

    // Wait up to 1s for the background process group to be killed and session temp dir removed
    let session_tmp = base_tmp.join("tmp").join("sessions").join(sess_id.as_str());
    let mut killed = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let still_alive = std::process::Command::new("kill")
            .arg("-0")
            .arg(bg_pid.to_string())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !still_alive && !session_tmp.exists() {
            killed = true;
            break;
        }
    }
    assert!(
        killed,
        "session-scoped background process pid {bg_pid} and session tmp dir {:?} must be cleaned up when session is closed",
        session_tmp
    );

    d_cancel.cancel();
    server_cancel_2.cancel();
    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_daemon_auto_discover_models_from_v1_models_endpoint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // 1. Spawn a lightweight local HTTP server that responds to GET /v1/models
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let mock_addr = mock_listener.local_addr().expect("mock addr");
    let mock_cancel = CancellationToken::new();
    let mock_cancel_clone = mock_cancel.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = mock_cancel_clone.cancelled() => break,
                accept_res = mock_listener.accept() => {
                    if let Ok((mut stream, _)) = accept_res {
                        let mut buf = [0u8; 2048];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        if req_str.starts_with("GET /v1/models ") {
                            let body = r#"{"object":"list","data":[{"id":"discovered-model-alpha"},{"id":"discovered-model-beta"},{"id":"configured-supplement-model"}]}"#;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        } else {
                            let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = stream.write_all(resp.as_bytes()).await;
                        }
                    }
                }
            }
        }
    });

    // 2. Start apiserver
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store, 5_000);
    let server_cancel = CancellationToken::new();
    let apiserver_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let apiserver_addr = serve_tcp(apiserver_listener, state, server_cancel.clone())
        .await
        .expect("serve");
    let server_url = format!("http://{apiserver_addr}");

    // 3. Configure daemon with auto_discover_models = true and one supplemented model ("configured-only-model" + "configured-supplement-model")
    let mut cfg = make_daemon_cfg(
        "node_autodiscover",
        &server_url,
        vec![],
        tmp.path().join("node_autodiscover"),
    );
    cfg.auto_discover_models = true;
    cfg.default_model = Some("configured-supplement-model".to_string());
    cfg.models.insert(
        "configured-supplement-model".to_string(),
        LocalModelConfig {
            base_url: format!("http://{mock_addr}"),
            api_key: Some("sk-test".to_string()),
            api_key_env: None,
            extra_headers: BTreeMap::new(),
            default_temperature: Some(0.42),
            default_max_output_tokens: Some(4096),
        },
    );
    cfg.models.insert(
        "configured-only-model".to_string(),
        LocalModelConfig {
            base_url: format!("http://{mock_addr}"),
            api_key: Some("sk-test".to_string()),
            api_key_env: None,
            extra_headers: BTreeMap::new(),
            default_temperature: Some(0.1),
            default_max_output_tokens: Some(1024),
        },
    );

    let seen_inputs = Arc::new(Mutex::new(Vec::new()));
    let daemon_cancel = CancellationToken::new();
    let daemon = AgentDaemon::new(cfg).with_model_backend(Arc::new(QueueModelBackend {
        tag: "autodiscover".to_string(),
        seen_inputs: seen_inputs.clone(),
    }));
    daemon
        .start(daemon_cancel.clone())
        .await
        .expect("start daemon");

    // 4. Verify node registration in apiserver includes BOTH discovered models and configured supplement models
    let client = RagentsClient::new(&server_url);
    let nodes = client.list_nodes().await.expect("list nodes");
    assert_eq!(nodes.len(), 1);
    let supported = &nodes[0].status.supported_models;
    assert!(supported.contains(&"discovered-model-alpha".to_string()));
    assert!(supported.contains(&"discovered-model-beta".to_string()));
    assert!(supported.contains(&"configured-supplement-model".to_string()));
    assert!(supported.contains(&"configured-only-model".to_string()));

    // 5. Verify a session requesting "discovered-model-alpha" is scheduled and executed cleanly
    let created = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("discovered-model-alpha")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("hello discovered model")],
                model_override: None,
            }),
        })
        .await
        .expect("create session on discovered model");

    let act_id = ActivationId::new(created.initial_activation.unwrap().metadata.id);
    let mut completed = false;
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if let Some(act) = client.get_activation(&act_id).await.unwrap() {
            if act.status.phase == ActivationPhase::Succeeded {
                completed = true;
                break;
            }
        }
    }
    assert!(
        completed,
        "activation on discovered model must be scheduled and succeed"
    );

    daemon_cancel.cancel();
    server_cancel.cancel();
    mock_cancel.cancel();
}

#[tokio::test]
async fn test_daemon_allowed_workspace_dirs_restriction_and_default_tmp() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SqliteStore::open_in_memory().expect("open store");
    let state = ApiServerState::new(store.clone(), 5_000);
    let server_cancel = CancellationToken::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, server_cancel.clone())
        .await
        .expect("serve");
    let server_url = format!("http://{addr}");
    let client = RagentsClient::new(&server_url);

    let mut cfg = make_daemon_cfg(
        "node_ws_guard",
        &server_url,
        vec!["gpt-4.1"],
        tmp.path().join("daemon_tmp"),
    );
    cfg.executor_mode = ExecutorMode::InProcess;
    // Configure `/a/b/c,/a/b/d` — `/tmp` must also be included automatically by default!
    cfg.allowed_workspace_dirs = parse_workspace_dirs_csv("/a/b/c,/a/b/d");

    // 1. Direct unit validation checks on DaemonConfig
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/a/b/c/agent_1"))
            .is_ok(),
        "subdirectory of /a/b/c must be allowed"
    );
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/a/b/d/project/sub"))
            .is_ok(),
        "subdirectory of /a/b/d must be allowed"
    );
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/tmp/my_agent_ws"))
            .is_ok(),
        "/tmp must be allowed by default"
    );
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/a/b/e/forbidden"))
            .is_err(),
        "/a/b/e must be rejected"
    );
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/a/b/c/../e/escape"))
            .is_err(),
        ".. path traversal escaping /a/b/c must be rejected"
    );
    assert!(
        cfg.validate_workspace_path(std::path::Path::new("/a/b/cde"))
            .is_err(),
        "sibling prefix /a/b/cde must not match /a/b/c"
    );

    // 2. Start daemon and verify `create_session` on apiserver rejects disallowed `workspace_path`
    let daemon_cancel = CancellationToken::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let daemon = AgentDaemon::new(cfg).with_model_backend(Arc::new(QueueModelBackend {
        tag: "ws_guard".to_string(),
        seen_inputs: seen,
    }));
    daemon.start(daemon_cancel.clone()).await.expect("start daemon");

    let nodes = client.list_nodes().await.expect("list nodes");
    assert_eq!(nodes.len(), 1);
    assert!(nodes[0]
        .status
        .allowed_workspace_dirs
        .contains(&"/a/b/c".to_string()));
    assert!(nodes[0]
        .status
        .allowed_workspace_dirs
        .contains(&"/a/b/d".to_string()));
    let expected_tmp = ragents_protocol::normalize_and_resolve_workspace_path(std::path::Path::new("/tmp"))
        .to_string_lossy()
        .to_string();
    assert!(nodes[0]
        .status
        .allowed_workspace_dirs
        .contains(&expected_tmp));

    // Disallowed workspace `/var/forbidden_ws` must be rejected at session creation time
    let bad_res = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_ws_guard"),
                    workspace_path: std::path::PathBuf::from("/var/forbidden_ws"),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await;
    assert!(
        bad_res.is_err(),
        "creating a session with workspace outside allowed_workspace_dirs must fail"
    );

    // Allowed workspace under `/tmp` (and `/private/tmp` on macOS) must succeed
    let ok_res = client
        .create_session(CreateSessionRequest {
            session_id: None,
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_ws_guard"),
                    workspace_path: std::path::PathBuf::from("/private/tmp/allowed_sub_ws"),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await;
    assert!(
        ok_res.is_ok(),
        "creating a session under allowed /tmp (/private/tmp) dir must succeed"
    );

    daemon_cancel.cancel();
    server_cancel.cancel();
}

