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
    checked_original_paths: Mutex<Vec<Option<std::path::PathBuf>>>,
    override_path_on_approve: Mutex<Option<std::path::PathBuf>>,
    tool_durations_ms: Mutex<Vec<u64>>,
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
        duration_ms: u64,
        items: Vec<Item>,
    ) -> Result<BatchSeq, CoreError> {
        self.tool_durations_ms.lock().unwrap().push(duration_ms);
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
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        self.check_permission_with_original_path(extension_id, capability, None, reason)
            .await
    }

    async fn check_permission_with_original_path(
        &self,
        _extension_id: Option<&str>,
        capability: CapabilityRequest,
        original_path: Option<std::path::PathBuf>,
        _reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        self.checked_capabilities
            .lock()
            .unwrap()
            .push(capability.clone());
        self.checked_original_paths
            .lock()
            .unwrap()
            .push(original_path);
        let override_p = self.override_path_on_approve.lock().unwrap().clone();
        let effective = capability.with_override_path(override_p.as_deref());
        Ok(PermissionOutcome::allowed_with(effective, true))
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
    let written = std::fs::read_to_string(ws_root.join("subproc.txt")).expect("read subproc.txt");
    assert_eq!(written, "hello from isolated subprocess executor!");

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
async fn test_os_kernel_sandbox_blocks_outside_writes_and_denied_reads_and_allows_dynamic_unlock() {
    let base_tmp = std::env::temp_dir().join(format!("ragents-sb-unit-{}", uuid::Uuid::now_v7()));
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

        let blocked_read = sandbox
            .execute_command(&ws_root, &format!("cat '{}'", secret_file.display()), 5000)
            .await;
        assert_ne!(
            blocked_read.exit_code, 0,
            "OS sandbox must block reading denied paths"
        );
        assert!(!blocked_read.stdout.contains("PRIVATE_KEY_MATERIAL"));

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
    let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    if !shell_wasm_path.exists() {
        return;
    }

    let base_tmp = std::env::temp_dir().join(format!("ragents-multi-dir-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");
    let ext1 = base_tmp.join("ext1");
    let ext2 = base_tmp.join("ext2");
    let ext3 = base_tmp.join("ext3");
    let ext4 = base_tmp.join("ext4");
    for d in [&ws_root, &sess_tmp, &ext1, &ext2, &ext3, &ext4] {
        std::fs::create_dir_all(d).unwrap();
    }

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
            ..Default::default()
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
        let origs = host.checked_original_paths.lock().unwrap();
        assert_eq!(caps.len(), 5, "caps: {caps:?}");
        assert!(origs[0].is_none());
        assert!(
            origs[1]
                .as_ref()
                .unwrap()
                .to_string_lossy()
                .ends_with("ext1/a.txt"),
            "original_path must preserve exact raw file path: {:?}",
            origs[1]
        );
    }

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_wasm_external_file_dynamic_mount_and_http_fetch_permission_unification() {
    let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    let editor_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    if !shell_wasm_path.exists() || !editor_wasm_path.exists() {
        return;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: close\r\n\r\nwasm_http_ok_2026";
            let _ = stream.write_all(resp.as_bytes()).await;
        }
    });

    let base_tmp = std::env::temp_dir().join(format!("ragents-wasm-uni-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");
    let ext_dir = base_tmp.join("external_wasm_data");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&sess_tmp).unwrap();

    let ext_file = ext_dir.join("notes_outside.txt");
    let http_url = format!("http://{}/hello", addr);

    let host = Arc::new(MockCoreHost {
        items: Mutex::new(vec![Item::user_message(
            "Write to external file via WASM file_editor, read it back, and fetch HTTP via WASM.",
        )]),
        ..Default::default()
    });

    let resp1 = make_openresponses_resource(
        "resp_u1",
        "gpt-4.1",
        vec![Item::FunctionCall {
            id: Some("fc_w1".to_string()),
            call_id: "call_w1".to_string(),
            name: "write_file".to_string(),
            arguments: serde_json::json!({
                "path": ext_file.to_string_lossy(),
                "content": "hello from dynamically mounted WASI!"
            })
            .to_string(),
            status: None,
        }],
        None,
    );
    let resp2 = make_openresponses_resource(
        "resp_u2",
        "gpt-4.1",
        vec![Item::FunctionCall {
            id: Some("fc_r1".to_string()),
            call_id: "call_r1".to_string(),
            name: "read_file".to_string(),
            arguments: serde_json::json!({
                "path": ext_file.to_string_lossy(),
            })
            .to_string(),
            status: None,
        }],
        None,
    );
    let resp3 = make_openresponses_resource(
        "resp_u3",
        "gpt-4.1",
        vec![Item::assistant_message(
            "WASM file and HTTP permissions unified.",
        )],
        None,
    );

    let model_backend = Arc::new(MockModelBackend {
        responses: Mutex::new(vec![resp1, resp2, resp3]),
    });

    let cfg = ConfigRevision::new_merged(
        "You are a coding assistant.".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![
            ExtensionSpec {
                name: "shell".to_string(),
                path: shell_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            },
            ExtensionSpec {
                name: "file_editor".to_string(),
                path: editor_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_uni"),
        activation_id: ActivationId::new("act_uni"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: sess_tmp.clone(),
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        model_endpoint: ModelEndpointConfig {
            base_url: "http://127.0.0.1:9999/v1".to_string(),
            api_key: "sk-local".to_string(),
            ..Default::default()
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
        .expect("execute unified WASM file & HTTP permission test");

    assert_eq!(
        summary.final_text,
        "WASM file and HTTP permissions unified."
    );
    assert_eq!(
        std::fs::read_to_string(&ext_file).unwrap(),
        "hello from dynamically mounted WASI!"
    );

    let sandbox = Arc::new(SandboxState::from_policy(
        SandboxMode::Auto,
        &ws_root,
        &sess_tmp,
        &PermissionPolicyLayer::default(),
    ));
    let plugin = crate::hooks::runtime::WasmPlugin::load_from_file_with_host(
        "file_editor",
        &editor_wasm_path,
        &ws_root,
        &[sess_tmp],
        Some(host.clone()),
        sandbox,
    )
    .await
    .unwrap();
    let (http_status, http_body) = plugin
        .invoke_wasi_http_for_test("GET", &http_url)
        .await
        .expect("WASI 0.2 HTTP request");
    assert_eq!(http_status, 200);
    assert_eq!(http_body, "wasm_http_ok_2026");

    let caps = host.checked_capabilities.lock().unwrap();
    let origs = host.checked_original_paths.lock().unwrap();
    assert_eq!(caps.len(), 3, "caps: {caps:?}");
    assert!(matches!(caps[0], CapabilityRequest::FileWrite { .. }));
    assert!(matches!(caps[1], CapabilityRequest::FileRead { .. }));
    assert!(
        matches!(caps[2], CapabilityRequest::Http { ref host, port: Some(p), .. } if host == "127.0.0.1" && p == addr.port())
    );
    assert_eq!(origs[0].as_ref(), Some(&ext_dir));
    assert_eq!(origs[1].as_ref(), Some(&ext_file));
    assert_eq!(origs[2].as_ref(), Some(&std::path::PathBuf::from(http_url)));

    let items = host.items.lock().unwrap();
    let all_outputs = serde_json::to_string(&*items).unwrap();
    assert!(
        all_outputs.contains("hello from dynamically mounted WASI!"),
        "all_outputs: {all_outputs}"
    );

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_control_plane_wasm_plugin_allows_network_and_configured_dirs_while_blocking_host_root(
) {
    let editor_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    if !editor_wasm_path.exists() {
        return;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: close\r\n\r\ncontrol_http_ok";
            let _ = stream.write_all(resp.as_bytes()).await;
        }
    });

    let base_tmp = std::env::temp_dir().join(format!("ragents-ctrl-wasm-{}", uuid::Uuid::now_v7()));
    let allowed_dir = base_tmp.join("plugin_allowed_data");
    let forbidden_dir = base_tmp.join("host_secret_dir");
    std::fs::create_dir_all(&allowed_dir).unwrap();
    std::fs::create_dir_all(&forbidden_dir).unwrap();

    let allowed_file = allowed_dir.join("audit.log");
    let forbidden_file = forbidden_dir.join("daemon_secret.toml");
    std::fs::write(&forbidden_file, "api_key = 'sk-super-secret'").unwrap();

    let runner = crate::ControlPluginRunner::new();
    let driver = ExtensionDriverConfig::Wasm {
        path: editor_wasm_path.clone(),
        allowed_dirs: vec![allowed_dir.clone()],
    };

    let res_allowed = runner
        .invoke(
            "audit-wasm-plugin",
            &driver,
            5000,
            &ControlHookInvocation {
                hook_name: "tools.call".to_string(),
                plugin_config: serde_json::json!({}),
                payload: serde_json::json!({
                    "name": "write_file",
                    "arguments": {
                        "path": allowed_file.to_string_lossy(),
                        "content": "audit entry 1"
                    }
                }),
            },
        )
        .await
        .expect("invoke allowed write");
    assert_eq!(
        res_allowed.output.get("success"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        std::fs::read_to_string(&allowed_file).unwrap(),
        "audit entry 1"
    );

    let res_forbidden = runner
        .invoke(
            "audit-wasm-plugin",
            &driver,
            5000,
            &ControlHookInvocation {
                hook_name: "tools.call".to_string(),
                plugin_config: serde_json::json!({}),
                payload: serde_json::json!({
                    "name": "read_file",
                    "arguments": {
                        "path": forbidden_file.to_string_lossy()
                    }
                }),
            },
        )
        .await
        .expect("invoke forbidden read");
    assert_eq!(
        res_forbidden.output.get("success"),
        Some(&serde_json::json!(false)),
        "control-plane WASM plugin must not access host files outside allowed_dirs: {:?}",
        res_forbidden.output
    );

    let work_dir = base_tmp.join("work");
    std::fs::create_dir_all(&work_dir).unwrap();
    let ctrl_plugin = crate::hooks::runtime::WasmPlugin::load_from_file_with_workspace(
        "audit-wasm-plugin",
        &editor_wasm_path,
        &work_dir,
        &[allowed_dir],
    )
    .await
    .unwrap();
    let (status, body) = ctrl_plugin
        .invoke_wasi_http_for_test("GET", &format!("http://{}/jwks", addr))
        .await
        .expect("control plugin WASI HTTP allowed by default");
    assert_eq!(status, 200);
    assert_eq!(body, "control_http_ok");

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_blocking_shell_timeout_preserves_partial_output_and_kills_process_group() {
    let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    if !shell_wasm_path.exists() {
        return;
    }

    let base_tmp =
        std::env::temp_dir().join(format!("ragents-blocking-timeout-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&sess_tmp).unwrap();

    let host = Arc::new(MockCoreHost {
        items: Mutex::new(vec![Item::user_message(
            "run blocking command with timeout",
        )]),
        ..Default::default()
    });

    // Call `shell` in blocking mode (wait=true default) with `timeout: 0.3` (300ms)
    let fc_item = Item::FunctionCall {
        id: Some("fc_to_1".to_string()),
        call_id: "call_to_1".to_string(),
        name: "shell".to_string(),
        arguments: serde_json::json!({
            "command": "echo partial_stdout_before_timeout && echo partial_stderr_before_timeout >&2 && sleep 30",
            "timeout": 0.3
        })
        .to_string(),
        status: None,
    };

    let resp1 = make_openresponses_resource("resp_to_1", "gpt-4.1", vec![fc_item], None);
    let resp2 = make_openresponses_resource(
        "resp_to_2",
        "gpt-4.1",
        vec![Item::assistant_message("Command timed out as expected.")],
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
        session_id: SessionId::new("sess_to"),
        activation_id: ActivationId::new("act_to"),
        workspace_root: ws_root,
        session_tmp_dir: sess_tmp,
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        model_endpoint: ModelEndpointConfig {
            base_url: "http://127.0.0.1:9999/v1".to_string(),
            api_key: "sk-local".to_string(),
            ..Default::default()
        },
        max_turns: None,
    };

    let started = std::time::Instant::now();
    let executor = InProcessExecutor;
    let summary = executor
        .execute(
            run_spec,
            host.clone(),
            model_backend,
            CancellationToken::new(),
        )
        .await
        .expect("execute blocking timeout test");

    assert_eq!(summary.final_text, "Command timed out as expected.");
    let tool_ms = host.tool_durations_ms.lock().unwrap()[0];
    assert!(
        tool_ms < 5_000,
        "blocking timeout should have killed `sleep 30` quickly, tool_ms={tool_ms}ms (elapsed={:?})",
        started.elapsed()
    );

    let items = host.items.lock().unwrap();
    let all_outputs = serde_json::to_string(&*items).unwrap();
    assert!(
        all_outputs.contains("partial_stdout_before_timeout"),
        "partial stdout must be preserved on timeout: {all_outputs}"
    );
    assert!(
        all_outputs.contains("partial_stderr_before_timeout"),
        "partial stderr must be preserved on timeout: {all_outputs}"
    );
    assert!(
        all_outputs.contains("Command timed out after 0.30 seconds"),
        "timeout error message must be included: {all_outputs}"
    );

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_file_editor_pagination_non_overlapping_edits_and_shell_output_truncation() {
    let editor_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    let shell_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm");
    if !editor_wasm_path.exists() || !shell_wasm_path.exists() {
        return;
    }

    let base_tmp =
        std::env::temp_dir().join(format!("ragents-trunc-edit-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&sess_tmp).unwrap();

    // Create a 2,500-line file in workspace
    let big_file = ws_root.join("big.txt");
    let content = (1..=2500)
        .map(|i| format!("line-{i:04}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&big_file, &content).unwrap();

    // Create a source file for testing non-overlapping `replace_in_file`
    let code_file = ws_root.join("code.rs");
    std::fs::write(
        &code_file,
        "fn alpha() {\n    let x = 1;\n}\nfn beta() {\n    let y = 2;\n}\n",
    )
    .unwrap();

    let host = Arc::new(MockCoreHost {
        items: Mutex::new(vec![Item::user_message("test tools")]),
        ..Default::default()
    });

    // Turn 1:
    // 1. read_file on 2,500-line file without offset/limit -> auto-truncated at 2,000 lines
    // 2. read_file with offset=2490, limit=5 -> returns 5 lines and "6 more lines in file. Use offset=2495"
    // 3. replace_in_file with 2 disjoint edits
    // 4. replace_in_file with overlapping edits -> rejected
    // 5. shell command printing 2,500 lines -> tail-truncated to lines 501-2500 and spilled to temp file
    let fc1 = Item::FunctionCall {
        id: Some("fc_1".into()),
        call_id: "call_read_full".into(),
        name: "read_file".into(),
        arguments: serde_json::json!({ "path": "big.txt" }).to_string(),
        status: None,
    };
    let fc2 = Item::FunctionCall {
        id: Some("fc_2".into()),
        call_id: "call_read_page".into(),
        name: "read_file".into(),
        arguments: serde_json::json!({ "path": "big.txt", "offset": 2490, "limit": 5 }).to_string(),
        status: None,
    };
    let fc3 = Item::FunctionCall {
        id: Some("fc_3".into()),
        call_id: "call_edit_ok".into(),
        name: "replace_in_file".into(),
        arguments: serde_json::json!({
            "path": "code.rs",
            "edits": [
                { "old_text": "let y = 2;", "new_text": "let y = 20;\n    let x = 1;" },
                { "old_text": "let x = 1;", "new_text": "let x = 10;" }
            ]
        })
        .to_string(),
        status: None,
    };
    let fc4 = Item::FunctionCall {
        id: Some("fc_4".into()),
        call_id: "call_edit_overlap".into(),
        name: "replace_in_file".into(),
        arguments: serde_json::json!({
            "path": "code.rs",
            "edits": [
                { "old_text": "fn alpha() {\n    let x = 10;", "new_text": "fn alpha() {\n    let x = 100;" },
                { "old_text": "let x = 10;\n}", "new_text": "let x = 200;\n}" }
            ]
        })
        .to_string(),
        status: None,
    };
    let fc5 = Item::FunctionCall {
        id: Some("fc_5".into()),
        call_id: "call_shell_big".into(),
        name: "shell".into(),
        arguments: serde_json::json!({
            "command": "awk 'BEGIN { for (i = 1; i <= 2500; i++) printf \"out-%04d\\n\", i }'"
        })
        .to_string(),
        status: None,
    };

    let resp1 = make_openresponses_resource("r1", "gpt-4.1", vec![fc1, fc2, fc3, fc4, fc5], None);
    let resp2 = make_openresponses_resource(
        "r2",
        "gpt-4.1",
        vec![Item::assistant_message("All tool checks completed.")],
        None,
    );

    let model_backend = Arc::new(MockModelBackend {
        responses: Mutex::new(vec![resp1, resp2]),
    });

    let cfg = ConfigRevision::new_merged(
        "You are a coding assistant.".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![
            ExtensionSpec {
                name: "file_editor".to_string(),
                path: editor_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            },
            ExtensionSpec {
                name: "shell".to_string(),
                path: shell_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_trunc"),
        activation_id: ActivationId::new("act_trunc"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: sess_tmp.clone(),
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        model_endpoint: ModelEndpointConfig {
            base_url: "http://127.0.0.1:9999/v1".to_string(),
            api_key: "sk-local".to_string(),
            ..Default::default()
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
        .expect("execute truncation and edit test");
    assert_eq!(summary.final_text, "All tool checks completed.");

    // Verify non-overlapping edits matched against original file (Notice edit[0] inserted `let x = 1;`,
    // which would have broken edit[1] if edits were matched incrementally!)
    let updated_code = std::fs::read_to_string(&code_file).unwrap();
    assert_eq!(
        updated_code,
        "fn alpha() {\n    let x = 10;\n}\nfn beta() {\n    let y = 20;\n    let x = 1;\n}\n"
    );

    let items = host.items.lock().unwrap();
    let all_outputs = serde_json::to_string(&*items).unwrap();
    assert!(
        all_outputs.contains("[Showing lines 1-2000 of 2500. Use offset=2001 to continue.]"),
        "read_file must head-truncate at 2000 lines: {all_outputs}"
    );
    assert!(
        all_outputs.contains("[6 more lines in file. Use offset=2495 to continue.]"),
        "read_file with offset/limit must report remaining lines: {all_outputs}"
    );
    assert!(
        all_outputs.contains("Successfully replaced 2 block(s) in code.rs."),
        "replace_in_file must succeed on non-overlapping edits: {all_outputs}"
    );
    assert!(
        all_outputs.contains("edits[0] and edits[1] overlap in code.rs"),
        "replace_in_file must reject overlapping edits: {all_outputs}"
    );
    assert!(
        all_outputs.contains("Showing lines 501-2500 of 2500. Full output:"),
        "shell must tail-truncate at 2000 lines and spill full output to temp file: {all_outputs}"
    );
    assert!(
        !all_outputs.contains("out-0001"),
        "tail-truncated shell output must not include line 1"
    );
    assert!(
        all_outputs.contains("out-2500"),
        "tail-truncated shell output must include line 2500"
    );

    let _ = std::fs::remove_dir_all(&base_tmp);
}
