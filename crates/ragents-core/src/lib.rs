pub mod context;
pub mod error;
pub mod hooks;
pub mod host;
pub mod model;
pub mod runner;
pub mod subprocess;

pub use context::*;
pub use error::*;
pub use hooks::*;
pub use host::*;
pub use model::*;
pub use ragents_protocol;
pub use runner::*;
pub use subprocess::*;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use openresponses_rust::{CreateResponseBody, Item, ResponseResource, Usage};
    use ragents_protocol::*;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    struct MockModelBackend {
        responses: Mutex<Vec<ResponseResource>>,
    }

    #[async_trait]
    impl ModelBackend for MockModelBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            _request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            let mut guard = self.responses.lock().unwrap();
            if guard.is_empty() {
                return Err(CoreError::ResponseFailed("no more mock responses".into()));
            }
            Ok(guard.remove(0))
        }
    }

    #[derive(Default)]
    struct MockCoreHost {
        items: Mutex<Vec<Item>>,
        events: Mutex<Vec<SessionEvent>>,
    }

    #[async_trait]
    impl CoreHost for MockCoreHost {
        async fn load_effective_context(&self) -> Result<Vec<Item>, CoreError> {
            Ok(self.items.lock().unwrap().clone())
        }

        async fn commit_model_output(
            &self,
            _turn_id: &TurnId,
            _response_id: Option<String>,
            items: Vec<Item>,
            _usage: Option<Usage>,
        ) -> Result<BatchSeq, CoreError> {
            self.items.lock().unwrap().extend(items);
            Ok(BatchSeq::new(1))
        }

        async fn commit_tool_output(
            &self,
            _turn_id: &TurnId,
            _call_id: &str,
            _tool_name: &str,
            _success: bool,
            _duration_ms: u64,
            items: Vec<Item>,
        ) -> Result<BatchSeq, CoreError> {
            self.items.lock().unwrap().extend(items);
            Ok(BatchSeq::new(2))
        }

        async fn emit_event(
            &self,
            _turn_id: Option<TurnId>,
            event: SessionEvent,
        ) -> Result<(), CoreError> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }

        async fn check_permission(
            &self,
            _extension_id: Option<&str>,
            _capability: CapabilityRequest,
            _reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            Ok(PermissionOutcome::Allowed)
        }

        async fn spawn_child_session(
            &self,
            _request: CreateSessionRequest,
            _turn_id: Option<TurnId>,
            _call_id: Option<String>,
            _command_id: CommandId,
        ) -> Result<SessionId, CoreError> {
            Ok(SessionId::generate())
        }
    }

    #[tokio::test]
    async fn test_in_process_executor_single_turn_without_tools() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let host = Arc::new(MockCoreHost {
            items: Mutex::new(vec![Item::user_message("what is 2+2?")]),
            events: Mutex::new(vec![]),
        });

        let resp = make_openresponses_resource(
            "resp_mock_1",
            "gpt-4.1",
            vec![Item::assistant_message("2+2 is 4.")],
            None,
        );

        let model_backend = Arc::new(MockModelBackend {
            responses: Mutex::new(vec![resp]),
        });

        let cfg = ConfigRevision::new_merged(
            "You are a concise math assistant.".to_string(),
            ModelRequestSpec::new("gpt-4.1"),
            vec![],
        );

        let run_spec = CoreRunSpec {
            session_id: SessionId::new("sess_test"),
            activation_id: ActivationId::new("act_test"),
            workspace_root: tmp.path().join("ws"),
            session_tmp_dir: tmp.path().join("tmp"),
            effective_config: cfg,
            model_endpoint: ModelEndpointConfig {
                base_url: "http://127.0.0.1:9999/v1".to_string(),
                api_key: "sk-local".to_string(),
            },
            max_turns: None,
        };

        let executor = InProcessExecutor;
        let summary = executor
            .execute(
                run_spec,
                host.clone(),
                model_backend,
                CancellationToken::new(),
            )
            .await
            .expect("execute activation");

        assert_eq!(summary.final_text, "2+2 is 4.");
        assert_eq!(summary.turns_executed, 1);
        assert_eq!(host.items.lock().unwrap().len(), 2);
        assert_eq!(host.events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_in_process_executor_with_wasm_file_editor_extension() {
        let wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
        );
        if !wasm_path.exists() {
            return;
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let ws_root = tmp.path().join("ws");
        std::fs::create_dir_all(&ws_root).unwrap();

        let host = Arc::new(MockCoreHost {
            items: Mutex::new(vec![Item::user_message("create hello.txt")]),
            events: Mutex::new(vec![]),
        });

        let fc_item = Item::FunctionCall {
            id: Some("fc_1".to_string()),
            call_id: "call_write_1".to_string(),
            name: "write_file".to_string(),
            arguments: serde_json::json!({
                "path": "hello.txt",
                "content": "hello from ragents-core wasm!"
            })
            .to_string(),
            status: None,
        };

        let resp1 = make_openresponses_resource("resp_1", "gpt-4.1", vec![fc_item], None);
        let resp2 = make_openresponses_resource(
            "resp_2",
            "gpt-4.1",
            vec![Item::assistant_message("Created hello.txt successfully.")],
            None,
        );

        let model_backend = Arc::new(MockModelBackend {
            responses: Mutex::new(vec![resp1, resp2]),
        });

        let cfg = ConfigRevision::new_merged(
            "You are a coding assistant.".to_string(),
            ModelRequestSpec::new("gpt-4.1"),
            vec![ExtensionSpec {
                name: "file_editor".to_string(),
                path: wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            }],
        );

        let run_spec = CoreRunSpec {
            session_id: SessionId::new("sess_wasm"),
            activation_id: ActivationId::new("act_wasm"),
            workspace_root: ws_root.clone(),
            session_tmp_dir: tmp.path().join("tmp"),
            effective_config: cfg,
            model_endpoint: ModelEndpointConfig {
                base_url: "http://127.0.0.1:9999/v1".to_string(),
                api_key: "sk-local".to_string(),
            },
            max_turns: None,
        };

        let executor = InProcessExecutor;
        let summary = executor
            .execute(
                run_spec,
                host.clone(),
                model_backend,
                CancellationToken::new(),
            )
            .await
            .expect("execute wasm tool loop");

        assert_eq!(summary.final_text, "Created hello.txt successfully.");
        assert_eq!(summary.turns_executed, 2);
        let written = std::fs::read_to_string(ws_root.join("hello.txt")).expect("read hello.txt");
        assert_eq!(written, "hello from ragents-core wasm!");
    }

    #[tokio::test]
    async fn test_subprocess_executor_with_wasm_file_editor_extension() {
        let wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
        );
        if !wasm_path.exists() {
            return;
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let ws_root = tmp.path().join("ws_subproc");
        std::fs::create_dir_all(&ws_root).unwrap();

        let host = Arc::new(MockCoreHost {
            items: Mutex::new(vec![Item::user_message("create subproc.txt")]),
            events: Mutex::new(vec![]),
        });

        let fc_item = Item::FunctionCall {
            id: Some("fc_sub_1".to_string()),
            call_id: "call_sub_1".to_string(),
            name: "write_file".to_string(),
            arguments: serde_json::json!({
                "path": "subproc.txt",
                "content": "hello from isolated subprocess executor!"
            })
            .to_string(),
            status: None,
        };

        let resp1 = make_openresponses_resource("resp_sub_1", "gpt-4.1", vec![fc_item], None);
        let resp2 = make_openresponses_resource(
            "resp_sub_2",
            "gpt-4.1",
            vec![Item::assistant_message(
                "Created subproc.txt in subprocess.",
            )],
            None,
        );

        let model_backend = Arc::new(MockModelBackend {
            responses: Mutex::new(vec![resp1, resp2]),
        });

        let cfg = ConfigRevision::new_merged(
            "You are a coding assistant.".to_string(),
            ModelRequestSpec::new("gpt-4.1"),
            vec![ExtensionSpec {
                name: "file_editor".to_string(),
                path: wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            }],
        );

        let run_spec = CoreRunSpec {
            session_id: SessionId::new("sess_subproc"),
            activation_id: ActivationId::new("act_subproc"),
            workspace_root: ws_root.clone(),
            session_tmp_dir: tmp.path().join("tmp_subproc"),
            effective_config: cfg,
            model_endpoint: ModelEndpointConfig {
                base_url: "http://127.0.0.1:9999/v1".to_string(),
                api_key: "sk-local-secret".to_string(),
            },
            max_turns: None,
        };

        let executor = SubprocessExecutor::new();
        let addr_before = executor.shared_ipc_addr().await.expect("shared ipc addr");
        #[cfg(unix)]
        assert!(addr_before.starts_with("unix:"));

        let summary = executor
            .execute(
                run_spec.clone(),
                host.clone(),
                model_backend,
                CancellationToken::new(),
            )
            .await
            .expect("execute subprocess wasm tool loop over UDS");

        assert_eq!(summary.final_text, "Created subproc.txt in subprocess.");
        assert_eq!(summary.turns_executed, 2);
        let written =
            std::fs::read_to_string(ws_root.join("subproc.txt")).expect("read subproc.txt");
        assert_eq!(written, "hello from isolated subprocess executor!");

        // Run a second task on the same executor and verify it reuses the exact same UDS listener!
        let resp_second = make_openresponses_resource(
            "resp_sub_second",
            "gpt-4.1",
            vec![Item::assistant_message("Second task on same UDS ok.")],
            None,
        );
        let summary2 = executor
            .execute(
                run_spec.clone(),
                host.clone(),
                Arc::new(MockModelBackend {
                    responses: Mutex::new(vec![resp_second]),
                }),
                CancellationToken::new(),
            )
            .await
            .expect("second task on shared UDS");
        assert_eq!(summary2.final_text, "Second task on same UDS ok.");
        assert_eq!(
            executor.shared_ipc_addr().await.unwrap(),
            addr_before,
            "must reuse the single shared UDS listener"
        );

        // Also verify TCP loopback fallback path when UDS is unavailable/disabled
        let tcp_executor = SubprocessExecutor::new().with_prefer_uds(false);
        let resp3 = make_openresponses_resource(
            "resp_sub_3",
            "gpt-4.1",
            vec![Item::assistant_message("TCP fallback ok.")],
            None,
        );
        let tcp_summary = tcp_executor
            .execute(
                run_spec,
                host,
                Arc::new(MockModelBackend {
                    responses: Mutex::new(vec![resp3]),
                }),
                CancellationToken::new(),
            )
            .await
            .expect("execute subprocess over TCP fallback");
        assert_eq!(tcp_summary.final_text, "TCP fallback ok.");
    }
}
