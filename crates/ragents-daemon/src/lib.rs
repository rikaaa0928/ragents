pub mod config;
pub mod daemon;
pub mod permission;

pub use config::*;
pub use daemon::*;
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
                    default_temperature: Some(0.1),
                    default_max_output_tokens: Some(2048),
                },
            );
        }
        DaemonConfig {
            node_id: NodeId::new(node_id),
            server_url: server_url.to_string(),
            max_concurrent_runners: 4,
            max_turns: None,
            default_system_prompt: format!("System prompt from {node_id}"),
            default_model: models.first().map(|s| s.to_string()),
            temp_root_dir: temp_root,
            labels: BTreeMap::new(),
            models: model_map,
            extensions: vec![],
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
}
