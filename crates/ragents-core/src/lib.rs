pub mod context;
pub mod error;
pub mod hooks;
pub mod host;
pub mod model;
pub mod runner;
pub mod sandbox;
pub mod subprocess;

pub use context::*;
pub use error::*;
pub use hooks::*;
pub use host::*;
pub use model::*;
pub use ragents_protocol;
pub use runner::*;
pub use sandbox::*;
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
        checked_capabilities: Mutex<Vec<CapabilityRequest>>,
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
            capability: CapabilityRequest,
            _reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            self.checked_capabilities.lock().unwrap().push(capability);
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
            ..Default::default()
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
            effective_permissions: PermissionPolicyLayer::default(),
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
            ..Default::default()
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
            effective_permissions: PermissionPolicyLayer::default(),
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
            ..Default::default()
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
            effective_permissions: PermissionPolicyLayer::default(),
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

    #[tokio::test]
    async fn test_os_kernel_sandbox_blocks_outside_writes_and_denied_reads_and_allows_dynamic_unlock(
    ) {
        let base_tmp =
            std::env::temp_dir().join(format!("ragents-sb-unit-{}", uuid::Uuid::now_v7()));
        let ws_root = base_tmp.join("workspace");
        let sess_tmp = base_tmp.join("session_tmp");
        let outside_dir = base_tmp.join("outside_unauthorized");
        let secret_dir = base_tmp.join("secret_ssh");

        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::create_dir_all(&sess_tmp).unwrap();
        std::fs::create_dir_all(&outside_dir).unwrap();
        std::fs::create_dir_all(&secret_dir).unwrap();
        let secret_file = secret_dir.join("id_rsa");
        std::fs::write(&secret_file, "PRIVATE_KEY_MATERIAL").unwrap();

        let policy = PermissionPolicyLayer {
            file_read: Some(RuleSet {
                allow: None,
                deny: Some(vec![secret_dir.to_string_lossy().to_string()]),
                default: Some(DefaultPermissionDecision::Allow),
            }),
            file_write: None,
            command_work_dir: None,
            http_hosts: None,
        };

        let sandbox = SandboxState::from_policy(SandboxMode::Auto, &ws_root, &sess_tmp, &policy);

        // 1. Writing inside workspace_root and $TMPDIR must succeed
        let ok_out = sandbox
            .execute_command(
                &ws_root,
                "echo inside_ws > allowed.txt && echo tmp_ok > \"$TMPDIR/cache.txt\"",
                5000,
            )
            .await;
        assert_eq!(ok_out.exit_code, 0, "stderr: {}", ok_out.stderr);
        assert!(ws_root.join("allowed.txt").exists());
        assert!(sess_tmp.join("tmp").join("cache.txt").exists());

        #[cfg(target_os = "macos")]
        {
            // 2. Writing outside workspace_root (e.g. simulating `pip install` writing to global/user dir)
            // must be blocked by the macOS kernel sandbox (`Operation not permitted`)
            let outside_file = outside_dir.join("pwned.txt");
            let blocked_write = sandbox
                .execute_command(
                    &ws_root,
                    &format!("echo pwned > '{}'", outside_file.display()),
                    5000,
                )
                .await;
            assert_ne!(
                blocked_write.exit_code, 0,
                "OS sandbox must block writes outside workspace_root"
            );
            assert!(
                !outside_file.exists(),
                "outside file must not be created by sandboxed command"
            );

            // 3. Reading a denied path (`file_read.deny`) must be blocked by the OS kernel sandbox
            let blocked_read = sandbox
                .execute_command(&ws_root, &format!("cat '{}'", secret_file.display()), 5000)
                .await;
            assert_ne!(
                blocked_read.exit_code, 0,
                "OS sandbox must block reading denied paths"
            );
            assert!(!blocked_read.stdout.contains("PRIVATE_KEY_MATERIAL"));

            // 4. Dynamically unlocking `outside_dir` (simulating user `AllowSession` approval)
            // must immediately allow subsequent commands to write to `outside_dir`
            sandbox.allow_writable_path(&outside_dir);
            let unlocked_write = sandbox
                .execute_command(
                    &ws_root,
                    &format!("echo approved_write > '{}'", outside_file.display()),
                    5000,
                )
                .await;
            assert_eq!(
                unlocked_write.exit_code, 0,
                "stderr: {}",
                unlocked_write.stderr
            );
            assert_eq!(
                std::fs::read_to_string(&outside_file).unwrap().trim(),
                "approved_write"
            );
        }

        let _ = std::fs::remove_dir_all(&base_tmp);
    }

    #[tokio::test]
    async fn test_multi_directory_auto_permission_escalation_loop_in_single_shell_call() {
        let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm",
        );
        if !shell_wasm_path.exists() {
            return;
        }

        let base_tmp =
            std::env::temp_dir().join(format!("ragents-multi-dir-{}", uuid::Uuid::now_v7()));
        let ws_root = base_tmp.join("ws");
        let sess_tmp = base_tmp.join("tmp");
        let ext1 = base_tmp.join("ext1");
        let ext2 = base_tmp.join("ext2");
        let ext3 = base_tmp.join("ext3");
        let ext4 = base_tmp.join("ext4");
        for d in [&ws_root, &sess_tmp, &ext1, &ext2, &ext3, &ext4] {
            std::fs::create_dir_all(d).unwrap();
        }

        // Single command that sequentially writes to 4 different external directories via `&&`
        let chained_cmd = format!(
            "echo d1 > '{}/a.txt' && echo d2 > '{}/b.txt' && echo d3 > '{}/c.txt' && echo d4 > '{}/d.txt'",
            ext1.display(),
            ext2.display(),
            ext3.display(),
            ext4.display()
        );

        let host = Arc::new(MockCoreHost {
            items: Mutex::new(vec![Item::user_message("run chained multi-dir command")]),
            ..Default::default()
        });

        let fc_item = Item::FunctionCall {
            id: Some("fc_multi".to_string()),
            call_id: "call_multi_1".to_string(),
            name: "shell".to_string(),
            arguments: serde_json::json!({
                "command": chained_cmd,
                "timeout_seconds": 10
            })
            .to_string(),
            status: None,
        };

        let resp1 = make_openresponses_resource("resp_m1", "gpt-4.1", vec![fc_item], None);
        let resp2 = make_openresponses_resource(
            "resp_m2",
            "gpt-4.1",
            vec![Item::assistant_message(
                "All 4 external directories written.",
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
                name: "shell".to_string(),
                path: shell_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            }],
        );

        let run_spec = CoreRunSpec {
            session_id: SessionId::new("sess_multi"),
            activation_id: ActivationId::new("act_multi"),
            workspace_root: ws_root,
            session_tmp_dir: sess_tmp,
            effective_config: cfg,
            effective_permissions: PermissionPolicyLayer::default(),
            model_endpoint: ModelEndpointConfig {
                base_url: "http://127.0.0.1:9999/v1".to_string(),
                api_key: "sk-local".to_string(),
            },
            max_turns: None,
        };

        let executor = SubprocessExecutor::new();
        let summary = executor
            .execute(
                run_spec,
                host.clone(),
                model_backend,
                CancellationToken::new(),
            )
            .await
            .expect("execute multi-directory auto permission loop");

        assert_eq!(summary.final_text, "All 4 external directories written.");
        assert_eq!(summary.turns_executed, 2);
        assert_eq!(
            std::fs::read_to_string(ext1.join("a.txt")).unwrap().trim(),
            "d1"
        );
        assert_eq!(
            std::fs::read_to_string(ext2.join("b.txt")).unwrap().trim(),
            "d2"
        );
        assert_eq!(
            std::fs::read_to_string(ext3.join("c.txt")).unwrap().trim(),
            "d3"
        );
        assert_eq!(
            std::fs::read_to_string(ext4.join("d.txt")).unwrap().trim(),
            "d4"
        );

        #[cfg(target_os = "macos")]
        {
            let caps = host.checked_capabilities.lock().unwrap();
            // 1 initial CommandWorkDir check + 4 sequential FileWrite directory approvals
            assert_eq!(caps.len(), 5, "caps: {caps:?}");
        }

        let _ = std::fs::remove_dir_all(&base_tmp);
    }
}
