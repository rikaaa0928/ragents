pub mod config;
pub mod daemon;
pub mod interceptors;
pub mod permission;

pub use config::*;
pub use daemon::*;
pub use interceptors::*;
pub use permission::*;
pub use ragents_core;
pub use ragents_protocol;

#[cfg(test)]
mod tests {
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
            labels: BTreeMap::new(),
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

        // Start Daemon A
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

        // Submit Turn 1 on a Floating Session -> runs on node_a
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

        // Now shut down Daemon A and expire its lease, and start Daemon B!
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

        // Submit Turn 2 on the same Floating Session -> automatically switches to node_b!
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

        // Verify node_b received all 3 items (turn 1 user + turn 1 assistant from node_a + turn 2 user)!
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

        // Start a local UDS Webhook interceptor server that mutates `before_model_request` and `after_model_response`
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
                            // Mutate standard parameter (`temperature`) + inject vendor-specific field (`thinking_budget`)
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

        // Verify `before_model_request` mutations reached the underlying ModelBackend
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

        // Verify `after_model_response` mutation was committed to session context
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
        let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm",
        );
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

        let base_tmp =
            std::env::temp_dir().join(format!("ragents-bg-proc-{}", uuid::Uuid::now_v7()));
        let ws_root = base_tmp.join("ws");
        std::fs::create_dir_all(&ws_root).unwrap();

        let mut cfg = DaemonConfig::default_local("node-bg", &server_url);
        cfg.executor_mode = ExecutorMode::Subprocess;
        cfg.extensions.push(ExtensionSpec {
            name: "shell".to_string(),
            path: shell_wasm_path.to_string_lossy().to_string(),
            enabled: true,
            config: serde_json::json!({}),
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
                    0 => {
                        // Turn 1 of Activation #1:
                        // Start 3 background processes:
                        // 1) short-lived background job (wait=false, lifecycle=activation) that prints stdout+stderr and exits in ~0.3s
                        // 2) long-lived activation-scoped job (wait=false, lifecycle=activation) that sleeps 60s (should be auto-killed when Activation #1 ends)
                        // 3) long-lived session-scoped job (wait=false, lifecycle=session) that sleeps 60s (should survive into Activation #2)
                        Ok(make_openresponses_resource(
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
                        ))
                    }
                    1 => {
                        // Parse the returned `process_id` and `pid` from Turn 1 tool outputs
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
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        short_id = v["process_id"]
                                            .as_str()
                                            .unwrap_or_default()
                                            .to_string();
                                    }
                                } else if call_id == "call_bg_act" {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        *self.act_srv_pid.lock().unwrap() =
                                            v["pid"].as_u64().map(|p| p as u32);
                                    }
                                } else if call_id == "call_bg_sess" {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        *self.session_srv_proc_id.lock().unwrap() =
                                            v["process_id"].as_str().map(String::from);
                                    }
                                }
                            }
                        }

                        // Wait 450ms so `call_bg_short` has definitely finished, and push TWO real-time messages
                        // to `api-server` during Turn 2 so Turn 3 drains both at once as 2 separate `MessageContent` blocks!
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

                        // Query `process_logs` (stdout, stderr, combined) on `short_id`
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
                    2 => {
                        // Turn 3 of Activation #1: complete Activation #1
                        Ok(make_openresponses_resource(
                            "resp_bg_3",
                            "gpt-4.1",
                            vec![Item::assistant_message("Activation 1 done.")],
                            None,
                        ))
                    }
                    3 => {
                        // Turn 1 of Activation #2:
                        // Query status of the `lifecycle = session` process and then stop it via `process_stop`
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

        // 1. Verify that the drained real-time message batch in context contains a single `Item::Message`
        //    with MULTIPLE separate `MessageContent::InputText` blocks (NOT concatenated into one string!)
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

        // 2. Verify that `lifecycle = activation` process was automatically killed when Activation #1 finished
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

        // 3. Submit Activation #2 to verify `lifecycle = session` process survived Activation #1 and can be queried & stopped
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
        // Verify `process_status` in Activation #2 saw the session process as `"running"` and `process_stop` transitioned it to `"killed"`
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

        d_cancel.cancel();
        server_cancel.cancel();
        let _ = std::fs::remove_dir_all(&base_tmp);
    }
}
