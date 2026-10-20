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

struct MockAuthn;

#[async_trait::async_trait]
impl ragents_apiserver::AuthnPlugin for MockAuthn {
    fn id(&self) -> &str {
        "mock-bearer-authn"
    }

    async fn authenticate(&self, req: &AuthnRequest) -> Result<AuthnResponse, String> {
        let Some(auth_hdr) = req.headers.get("authorization") else {
            return Ok(AuthnResponse::Unauthenticated {
                message: "Missing Authorization header".into(),
            });
        };
        match auth_hdr.as_str() {
            "Bearer tok_tenant_a" => Ok(AuthnResponse::Authenticated {
                principal: Principal {
                    kind: PrincipalKind::ClientUser,
                    subject: "alice".into(),
                    tenant_id: "tenant_a".into(),
                    roles: vec!["developer".into()],
                    claims: BTreeMap::new(),
                },
            }),
            "Bearer tok_tenant_b" => Ok(AuthnResponse::Authenticated {
                principal: Principal {
                    kind: PrincipalKind::ClientUser,
                    subject: "bob".into(),
                    tenant_id: "tenant_b".into(),
                    roles: vec!["developer".into()],
                    claims: BTreeMap::new(),
                },
            }),
            "Bearer tok_daemon_1" => Ok(AuthnResponse::Authenticated {
                principal: Principal {
                    kind: PrincipalKind::DaemonNode,
                    subject: "node_1".into(),
                    tenant_id: "system".into(),
                    roles: vec!["node-agent".into()],
                    claims: BTreeMap::new(),
                },
            }),
            _ => Ok(AuthnResponse::Unauthenticated {
                message: "Invalid bearer token".into(),
            }),
        }
    }
}

struct MockTenantAuthz;

#[async_trait::async_trait]
impl ragents_apiserver::AuthzPlugin for MockTenantAuthz {
    fn id(&self) -> &str {
        "mock-tenant-authz"
    }

    async fn authorize(&self, req: &AuthzRequest) -> Result<AuthzResponse, String> {
        if req.resource == ResourceKind::DaemonTunnel {
            if req.principal.kind == PrincipalKind::DaemonNode {
                return Ok(AuthzResponse::Allow {
                    enforced_label_selector: BTreeMap::new(),
                });
            }
            return Ok(AuthzResponse::Deny {
                reason: "Only DaemonNode principals may connect to daemon tunnel".into(),
            });
        }

        let mut selector = BTreeMap::new();
        if req.principal.kind == PrincipalKind::ClientUser {
            selector.insert(
                "ragents.io/tenant_id".to_string(),
                req.principal.tenant_id.clone(),
            );
        }
        Ok(AuthzResponse::Allow {
            enforced_label_selector: selector,
        })
    }
}

struct MockMutatingAdmission;

#[async_trait::async_trait]
impl ragents_apiserver::AdmissionPlugin for MockMutatingAdmission {
    fn id(&self) -> &str {
        "tenant-label-and-model-alias-mutator"
    }

    fn phase(&self) -> AdmissionPhase {
        AdmissionPhase::Mutating
    }

    async fn admit(&self, req: &AdmissionRequest) -> Result<AdmissionResponse, String> {
        if req.operation == AdmissionOperation::CreateSession {
            let mut create_req: CreateSessionRequest =
                serde_json::from_value(req.payload.clone()).map_err(|e| e.to_string())?;
            // 1. Stamp tenant_id label
            create_req.labels.insert(
                "ragents.io/tenant_id".to_string(),
                req.principal.tenant_id.clone(),
            );
            // 2. Rewrite model alias `fast` -> `gemini-3.8-flash`
            if let Some(ref mut m) = create_req.spec.model {
                if m.name == "fast" {
                    m.name = "gemini-3.8-flash".to_string();
                }
            }
            return Ok(AdmissionResponse::Allowed {
                mutated_payload: Some(serde_json::to_value(create_req).unwrap()),
            });
        }
        Ok(AdmissionResponse::Allowed {
            mutated_payload: None,
        })
    }
}

struct MockValidatingAdmission;

#[async_trait::async_trait]
impl ragents_apiserver::AdmissionPlugin for MockValidatingAdmission {
    fn id(&self) -> &str {
        "restricted-model-validator"
    }

    fn phase(&self) -> AdmissionPhase {
        AdmissionPhase::Validating
    }

    async fn admit(&self, req: &AdmissionRequest) -> Result<AdmissionResponse, String> {
        if req.operation == AdmissionOperation::CreateSession {
            let create_req: CreateSessionRequest =
                serde_json::from_value(req.payload.clone()).map_err(|e| e.to_string())?;
            if create_req.spec.model.as_ref().map(|m| m.name.as_str()) == Some("restricted-ultra") {
                return Ok(AdmissionResponse::Denied {
                    code: 403,
                    message: "Model 'restricted-ultra' is not permitted for your tenant".into(),
                });
            }
        }
        Ok(AdmissionResponse::Allowed {
            mutated_payload: None,
        })
    }
}

#[tokio::test]
async fn test_apiserver_authn_authz_multitenant_isolation_and_mutating_validating_admission() {
    use ragents_apiserver::ApiServerExtensions;
    use ragents_client::connect_daemon_tunnel_with_token;
    use std::sync::Arc;

    let store = SqliteStore::open_in_memory().expect("open store");
    let mut extensions = ApiServerExtensions::new();
    extensions.add_authn(Arc::new(MockAuthn));
    extensions.add_authz(Arc::new(MockTenantAuthz));
    extensions.add_admission(Arc::new(MockMutatingAdmission));
    extensions.add_admission(Arc::new(MockValidatingAdmission));

    let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS).with_extensions(extensions);
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = serve_tcp(listener, state, cancel.clone())
        .await
        .expect("serve");
    let base_url = format!("http://{addr}");

    // 1. Unauthenticated client must be rejected with 401
    let unauth_client = RagentsClient::new(&base_url);
    let err = unauth_client.list_sessions().await.unwrap_err();
    assert!(
        err.to_string().contains("401"),
        "expected 401 Unauthorized, got {err}"
    );

    // 2. Daemon with user token must be rejected from connecting to daemon tunnel (403 Forbidden),
    //    while daemon with `tok_daemon_1` succeeds
    let ws_url = unauth_client.ws_tunnel_url();
    assert!(
        connect_daemon_tunnel_with_token(&ws_url, Some("tok_tenant_a"))
            .await
            .is_err()
    );
    let (tunnel, _push_rx) = connect_daemon_tunnel_with_token(&ws_url, Some("tok_daemon_1"))
        .await
        .expect("daemon tunnel with valid daemon token");
    tunnel
        .register_node(
            NodeId::new("node_1"),
            "host".into(),
            "macos".into(),
            "aarch64".into(),
            BTreeMap::new(),
            vec!["gemini-3.8-flash".into()],
            Some("gemini-3.8-flash".into()),
            vec![],
            4,
        )
        .await
        .unwrap();

    // 3. Tenant A creates session with model alias `fast` -> Mutating Admission rewrites to `gemini-3.8-flash`
    //    and injects label `ragents.io/tenant_id = tenant_a`
    let client_a = RagentsClient::new(&base_url).with_auth_token(Some("tok_tenant_a".into()));
    let client_b = RagentsClient::new(&base_url).with_auth_token(Some("tok_tenant_b".into()));

    let created_a = client_a
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_tenant_a")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("fast")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await
        .expect("tenant_a create session");

    assert_eq!(
        created_a
            .session
            .metadata
            .labels
            .get("ragents.io/tenant_id")
            .map(|s| s.as_str()),
        Some("tenant_a")
    );
    assert_eq!(
        created_a
            .session
            .spec
            .model
            .as_ref()
            .map(|m| m.name.as_str()),
        Some("gemini-3.8-flash")
    );

    // 4. Validating Admission rejects disallowed model `restricted-ultra` with 403
    let denied_res = client_a
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
                model: Some(ModelRequestSpec::new("restricted-ultra")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await
        .unwrap_err();
    assert!(
        denied_res.to_string().contains("403"),
        "expected 403 from validating admission, got {denied_res}"
    );

    // 5. Tenant B creates its own session; verify strict multi-tenant isolation
    let _created_b = client_b
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_tenant_b")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("fast")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: None,
        })
        .await
        .expect("tenant_b create session");

    let list_a = client_a.list_sessions().await.unwrap();
    assert_eq!(list_a.len(), 1);
    assert_eq!(list_a[0].metadata.id, "sess_tenant_a");

    let list_b = client_b.list_sessions().await.unwrap();
    assert_eq!(list_b.len(), 1);
    assert_eq!(list_b[0].metadata.id, "sess_tenant_b");

    // Tenant B attempting to GET Tenant A's session must be rejected with 403 Forbidden
    let cross_tenant_err = client_b
        .get_session(&SessionId::new("sess_tenant_a"))
        .await
        .unwrap_err();
    assert!(
        cross_tenant_err.to_string().contains("403"),
        "expected 403 Forbidden on cross-tenant get_session, got {cross_tenant_err}"
    );

    cancel.cancel();
}

#[tokio::test]
async fn test_wasm_authn_extension_and_static_token_authn() {
    use ragents_apiserver::{AuthnPluginSpec, ConfiguredAuthnPlugin};
    use ragents_client::connect_daemon_tunnel_with_token;
    use std::path::PathBuf;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let authn_wasm = manifest_dir
        .join("../../extensions/authn/target/wasm32-wasip2/release/ragent_authn_extension.wasm");
    assert!(
        authn_wasm.exists(),
        "ragent_authn_extension.wasm must be built"
    );

    let store = SqliteStore::open_in_memory().unwrap();
    let mut exts = ragents_apiserver::ApiServerExtensions::new();
    exts.add_authn(std::sync::Arc::new(ConfiguredAuthnPlugin::new(
        AuthnPluginSpec {
            id: "wasm-authn".into(),
            driver: ExtensionDriverConfig::Wasm {
                path: authn_wasm,
                allowed_dirs: vec![],
            },
            scopes: vec![AuthScope::ClientHttp, AuthScope::DaemonTunnel],
            timeout_ms: 2000,
            config: serde_json::json!({
                "tokens": [
                    {
                        "token": "sk-client-only",
                        "subject": "chatroom-server",
                        "scopes": ["client_http"]
                    },
                    {
                        "token": "sk-daemon-only",
                        "subject": "node-vps",
                        "scopes": ["daemon_tunnel"]
                    }
                ]
            }),
        },
    )));

    let state = ApiServerState::new(store, 5000).with_extensions(exts);
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = serve_tcp(listener, state, cancel.clone()).await.unwrap();
    let base_url = format!("http://{addr}");

    // 1. Unauthenticated client request is rejected with 401
    let anon_client = RagentsClient::new(&base_url);
    assert!(anon_client.list_nodes().await.is_err());

    // 2. Daemon-only token used on ClientHttp is rejected with 401
    let wrong_scope_client =
        RagentsClient::new(&base_url).with_auth_token(Some("sk-daemon-only".into()));
    assert!(wrong_scope_client.list_nodes().await.is_err());

    // 3. Client-only token succeeds on ClientHttp
    let valid_client = RagentsClient::new(&base_url).with_auth_token(Some("sk-client-only".into()));
    let nodes = valid_client.list_nodes().await.expect("valid client authn");
    assert!(nodes.is_empty());

    // 4. Client-only token used on DaemonTunnel is rejected, while daemon-only token succeeds
    let ws_url = valid_client.ws_tunnel_url();
    assert!(
        connect_daemon_tunnel_with_token(&ws_url, Some("sk-client-only"))
            .await
            .is_err()
    );
    let (tunnel, _rx) = connect_daemon_tunnel_with_token(&ws_url, Some("sk-daemon-only"))
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
