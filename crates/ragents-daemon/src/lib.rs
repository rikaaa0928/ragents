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
}
