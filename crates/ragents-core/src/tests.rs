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
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
    assert_eq!(host.events.lock().unwrap().len(), 2);
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
            allowed_dirs: vec![],
            filter_env: vec![],
        }],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_wasm"),
        activation_id: ActivationId::new("act_wasm"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: tmp.path().join("tmp"),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
            allowed_dirs: vec![],
            filter_env: vec![],
        }],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_subproc"),
        activation_id: ActivationId::new("act_subproc"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: tmp.path().join("tmp_subproc"),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
            allowed_dirs: vec![],
            filter_env: vec![],
        }],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_multi"),
        activation_id: ActivationId::new("act_multi"),
        workspace_root: ws_root,
        session_tmp_dir: sess_tmp,
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
                allowed_dirs: vec![],
                filter_env: vec![],
            },
            ExtensionSpec {
                name: "file_editor".to_string(),
                path: editor_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_uni"),
        activation_id: ActivationId::new("act_uni"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: sess_tmp.clone(),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
    // Because `FileWrite` approval on `ext_dir` persists in `SandboxState` across turns within the activation,
    // subsequent `read_file` on `ext_file` succeeds without a redundant permission prompt.
    assert_eq!(caps.len(), 2, "caps: {caps:?}");
    assert!(matches!(caps[0], CapabilityRequest::FileWrite { .. }));
    assert!(
        matches!(caps[1], CapabilityRequest::Http { ref host, port: Some(p), .. } if host == "127.0.0.1" && p == addr.port())
    );
    assert_eq!(origs[0].as_ref(), Some(&ext_dir));
    assert_eq!(origs[1].as_ref(), Some(&std::path::PathBuf::from(http_url)));

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
            allowed_dirs: vec![],
            filter_env: vec![],
        }],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_to"),
        activation_id: ActivationId::new("act_to"),
        workspace_root: ws_root,
        session_tmp_dir: sess_tmp,
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
                allowed_dirs: vec![],
                filter_env: vec![],
            },
            ExtensionSpec {
                name: "shell".to_string(),
                path: shell_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_trunc"),
        activation_id: ActivationId::new("act_trunc"),
        workspace_root: ws_root.clone(),
        session_tmp_dir: sess_tmp.clone(),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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

#[tokio::test]
async fn test_openresponses_sse_streaming_deltas_and_transient_retry() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        // Attempt 1: return 503 Service Unavailable to verify transient retry
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let _ = sock
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 19\r\nConnection: close\r\n\r\nupstream overloaded",
                )
                .await;
        }
        // Attempt 2: return text/event-stream SSE with 3 text deltas + completed response
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let completed_resp = make_openresponses_resource(
                "resp_sse_1",
                "gpt-4.1",
                vec![Item::assistant_message("Hello streaming world!")],
                None,
            );
            let completed_event = serde_json::json!({
                "type": "response.completed",
                "sequence_number": 4,
                "response": completed_resp
            });
            let sse_body = format!(
                "event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"Hello \"}}\n\nevent: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"streaming \"}}\n\nevent: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"world!\"}}\n\nevent: response.completed\ndata: {}\n\ndata: [DONE]\n\n",
                serde_json::to_string(&completed_event).unwrap()
            );
            let resp_http = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                sse_body.len(),
                sse_body
            );
            let _ = sock.write_all(resp_http.as_bytes()).await;
        }
    });

    let base_tmp = std::env::temp_dir().join(format!("ragents-sse-test-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");

    let host = Arc::new(MockCoreHost {
        items: Mutex::new(vec![Item::user_message("stream greeting")]),
        ..Default::default()
    });

    let cfg = ConfigRevision::new_merged(
        "You are a helpful assistant.".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_sse"),
        activation_id: ActivationId::new("act_sse"),
        workspace_root: ws_root,
        session_tmp_dir: sess_tmp,
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
        model_endpoint: ModelEndpointConfig {
            base_url: format!("http://{addr}/v1"),
            api_key: "sk-local".to_string(),
            ..Default::default()
        },
        max_turns: None,
    };

    let executor = InProcessExecutor;
    let model_backend = Arc::new(OpenResponsesModelBackend);
    let summary = executor
        .execute(
            run_spec,
            host.clone(),
            model_backend,
            CancellationToken::new(),
        )
        .await
        .expect("execute SSE streaming with retry");

    assert_eq!(summary.final_text, "Hello streaming world!");

    let events = host.events.lock().unwrap();
    let deltas: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            SessionEvent::ModelTextDelta { delta } => Some(delta.clone()),
            _ => None,
        })
        .collect();
    assert!(!deltas.is_empty());
    assert_eq!(deltas.concat(), "Hello streaming world!");

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[tokio::test]
async fn test_wasm_skills_extension_discovers_agents_skills_and_loads_skill() {
    let skills_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../extensions/skills/target/wasm32-wasip2/release/ragent_skills_extension.wasm");
    let agents_md_wasm_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/agents_md/target/wasm32-wasip2/release/ragent_agents_md_extension.wasm",
    );
    if !skills_wasm_path.exists() || !agents_md_wasm_path.exists() {
        return;
    }

    let base_tmp =
        std::env::temp_dir().join(format!("ragents-skills-test-{}", uuid::Uuid::now_v7()));
    let ws_root = base_tmp.join("ws");
    let sess_tmp = base_tmp.join("tmp");
    let ext_skills_root = base_tmp.join("external_skills");
    let ext_skill_dir = ext_skills_root.join("demo-deploy");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&ext_skill_dir).unwrap();
    std::fs::create_dir_all(&sess_tmp).unwrap();

    std::fs::write(
        ws_root.join("AGENTS.md"),
        "Always run tests with uv run pytest.",
    )
    .unwrap();
    std::fs::write(
        ext_skill_dir.join("SKILL.md"),
        "---\nname: demo-deploy\ndescription: Deploy the demo service to staging.\n---\n# Demo Deploy Skill\nStep 1: Run `./deploy.sh --env staging`\n",
    )
    .unwrap();

    struct CapturingInstructionsBackend {
        responses: Mutex<Vec<ResponseResource>>,
        captured_instructions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ModelBackend for CapturingInstructionsBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            if let Some(inst) = request.instructions {
                self.captured_instructions.lock().unwrap().push(inst);
            }
            let mut guard = self.responses.lock().unwrap();
            if guard.is_empty() {
                return Err(CoreError::ResponseFailed("no mock responses left".into()));
            }
            Ok(guard.remove(0))
        }
    }

    let host = Arc::new(MockCoreHost {
        items: Mutex::new(vec![Item::user_message("how do I deploy demo?")]),
        ..Default::default()
    });

    let fc_load_skill = Item::FunctionCall {
        id: Some("fc_sk_1".into()),
        call_id: "call_sk_1".into(),
        name: "load_skill".into(),
        arguments: serde_json::json!({ "name": "demo-deploy" }).to_string(),
        status: None,
    };
    let resp1 = make_openresponses_resource("r_sk_1", "gpt-4.1", vec![fc_load_skill], None);
    let resp2 = make_openresponses_resource(
        "r_sk_2",
        "gpt-4.1",
        vec![Item::assistant_message(
            "Loaded demo-deploy skill: run ./deploy.sh --env staging",
        )],
        None,
    );

    let model_backend = Arc::new(CapturingInstructionsBackend {
        responses: Mutex::new(vec![resp1, resp2]),
        captured_instructions: Mutex::new(Vec::new()),
    });

    let ext_skills_str = ext_skills_root.to_string_lossy().to_string();
    let cfg = ConfigRevision::new_merged(
        "You are a coding assistant.".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![
            ExtensionSpec {
                name: "agents_md".to_string(),
                path: agents_md_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
            ExtensionSpec {
                name: "skills".to_string(),
                path: skills_wasm_path.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({
                    "include_global": false,
                    "skill_dirs": [&ext_skills_str],
                }),
                allowed_dirs: vec![ext_skills_str],
                filter_env: vec![],
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_sk"),
        activation_id: ActivationId::new("act_sk"),
        workspace_root: ws_root,
        session_tmp_dir: sess_tmp,
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
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
            model_backend.clone(),
            CancellationToken::new(),
        )
        .await
        .expect("execute skills extension test");

    assert_eq!(
        summary.final_text,
        "Loaded demo-deploy skill: run ./deploy.sh --env staging"
    );

    let instructions = model_backend.captured_instructions.lock().unwrap();
    assert!(!instructions.is_empty());
    let first_sys_prompt = &instructions[0];
    assert!(
        first_sys_prompt.contains("<workspace_instructions file=\"AGENTS.md\">"),
        "AGENTS.md should be injected into system_prompt: {first_sys_prompt}"
    );
    assert!(
        first_sys_prompt.contains("<name>demo-deploy</name>")
            && first_sys_prompt.contains("Deploy the demo service to staging."),
        "workspace .agents/skills/demo-deploy should be injected into <available_skills>: {first_sys_prompt}"
    );

    let items = host.items.lock().unwrap();
    let all_outputs = serde_json::to_string(&*items).unwrap();
    assert!(
        all_outputs.contains("Step 1: Run `./deploy.sh --env staging`"),
        "load_skill must return the full SKILL.md content: {all_outputs}"
    );
    assert!(
        host.checked_capabilities.lock().unwrap().is_empty(),
        "external skill directory declared in ExtensionSpec.allowed_dirs must not trigger interactive permission prompts"
    );

    let _ = std::fs::remove_dir_all(&base_tmp);
}

#[test]
fn test_setuid_and_system_bin_per_command_sandbox_authorization() {
    use crate::sandbox::{
        build_macos_seatbelt_profile, canonicalize_or_normalize, is_setuid_or_setgid_binary,
        normalize_violation_to_request_dir, SandboxMode, SandboxState,
    };
    use std::path::{Path, PathBuf};

    let norm_bin_sh = normalize_violation_to_request_dir(Path::new("/bin/sh"));
    assert_ne!(
        norm_bin_sh,
        PathBuf::from("/bin"),
        "/bin/sh must not be collapsed to /bin"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let base_tmp =
            std::env::temp_dir().join(format!("ragents-setuid-test-{}", uuid::Uuid::now_v7()));
        let ws_root = base_tmp.join("ws");
        let sess_tmp = base_tmp.join("tmp");
        let custom_bin_dir = base_tmp.join("custom_bin");
        std::fs::create_dir_all(&ws_root).unwrap();
        std::fs::create_dir_all(&sess_tmp).unwrap();
        std::fs::create_dir_all(&custom_bin_dir).unwrap();

        let fake_setuid_bin = custom_bin_dir.join("my_ps");
        std::fs::write(&fake_setuid_bin, b"#!/bin/sh\necho ok\n").unwrap();
        std::fs::set_permissions(&fake_setuid_bin, std::fs::Permissions::from_mode(0o4755))
            .unwrap();

        assert!(is_setuid_or_setgid_binary(&fake_setuid_bin));

        let req_path = normalize_violation_to_request_dir(&fake_setuid_bin);
        assert_eq!(
            req_path,
            canonicalize_or_normalize(&fake_setuid_bin),
            "setuid binary must be authorized as an individual file, never collapsed to parent dir"
        );

        let sandbox = SandboxState::from_policy(
            SandboxMode::Auto,
            &ws_root,
            &sess_tmp,
            &PermissionPolicyLayer::default(),
        );

        // Allowing the parent directory must NOT satisfy a setuid binary check
        sandbox.allow_readable_path(&custom_bin_dir);
        assert!(
            !sandbox.is_path_readable(&fake_setuid_bin),
            "parent directory grant must not unlock setuid binary without explicit per-binary approval"
        );

        let profile_before = build_macos_seatbelt_profile(
            &[ws_root.clone()],
            &[],
            &[],
            &[custom_bin_dir.clone()],
            true,
            "TAG_SETUID",
        );
        assert!(
            !profile_before.contains("with no-sandbox"),
            "directory grant must never emit (with no-sandbox): {profile_before}"
        );

        // Explicitly approving the setuid binary unlocks both read and process-exec (with no-sandbox)
        sandbox.allow_readable_path(&fake_setuid_bin);
        assert!(sandbox.is_path_readable(&fake_setuid_bin));

        let profile_after = build_macos_seatbelt_profile(
            &[ws_root.clone()],
            &[],
            &[],
            &[custom_bin_dir.clone(), fake_setuid_bin.clone()],
            true,
            "TAG_SETUID",
        );
        let canon_bin = canonicalize_or_normalize(&fake_setuid_bin);
        let expected_rule = format!(
            "(allow process-exec (with no-sandbox) (literal \"{}\"))",
            canon_bin.display()
        );
        assert!(
            profile_after.contains(&expected_rule),
            "expected seatbelt profile to contain `{expected_rule}`, got:\n{profile_after}"
        );

        let _ = std::fs::remove_dir_all(&base_tmp);
    }
}

#[tokio::test]
async fn test_extension_filter_env_strips_secrets_from_session_execution() {
    assert!(crate::sandbox::env_key_matches_glob("MY_EXT_*", "MY_EXT_SECRET_KEY"));
    assert!(crate::sandbox::env_key_matches_glob("*_API_TOKEN", "FOO_API_TOKEN"));
    assert!(!crate::sandbox::env_key_matches_glob("MY_EXT_*", "OTHER_SECRET"));

    std::env::set_var("RAGENTS_TEST_EXT_SECRET", "top_secret_val_1");
    std::env::set_var("RAGENTS_TEST_GLOB_KEY", "top_secret_val_2");
    std::env::set_var("RAGENTS_TEST_PUBLIC_VAR", "visible_val");

    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ConfigRevision::new_merged(
        "sys".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![ExtensionSpec {
            name: "shell".to_string(),
            path: String::new(),
            enabled: true,
            config: serde_json::json!({}),
            allowed_dirs: vec![],
            filter_env: vec![
                "RAGENTS_TEST_EXT_SECRET".to_string(),
                "RAGENTS_TEST_GLOB_*".to_string(),
            ],
        }],
    );
    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_env_filter"),
        activation_id: ActivationId::new("act_env_filter"),
        workspace_root: tmp.path().to_path_buf(),
        session_tmp_dir: tmp.path().join("tmp"),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
        model_endpoint: ModelEndpointConfig::default(),
        max_turns: None,
    };
    let sandbox = crate::sandbox::SandboxState::from_run_spec(&run_spec);
    let out = sandbox
        .execute_command(
            tmp.path(),
            "printf 'SEC=%s|GLOB=%s|PUB=%s' \"$RAGENTS_TEST_EXT_SECRET\" \"$RAGENTS_TEST_GLOB_KEY\" \"$RAGENTS_TEST_PUBLIC_VAR\"",
            5000,
        )
        .await;
    assert_eq!(out.exit_code, 0);
    assert_eq!(out.stdout, "SEC=|GLOB=|PUB=visible_val");
}

#[test]
fn test_linux_bwrap_rofs_and_multilevel_nonexistent_violation_extraction() {
    use crate::sandbox::{
        canonicalize_or_normalize, normalize_violation_to_request_dir, SandboxMode, SandboxState,
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let ws_root = tmp.path().join("ws");
    let sess_tmp = tmp.path().join("tmp");
    let ext_existing = tmp.path().join("models");
    let ext_already_writable = tmp.path().join("writable_ext");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&sess_tmp).unwrap();
    std::fs::create_dir_all(&ext_existing).unwrap();
    std::fs::create_dir_all(&ext_already_writable).unwrap();

    // Multi-level non-existent path inside an existing safe directory resolves to the closest existing safe ancestor
    let deep_missing = ext_existing.join("sub_a").join("sub_b").join("ckpt.bin");
    assert_eq!(
        normalize_violation_to_request_dir(&deep_missing),
        canonicalize_or_normalize(&ext_existing)
    );

    let mut policy = PermissionPolicyLayer::default();
    policy.file_write = Some(RuleSet {
        deny: None,
        allow: Some(vec![
            ".".to_string(),
            ext_already_writable.to_string_lossy().to_string(),
        ]),
        default: Some(DefaultPermissionDecision::Ask),
    });
    let sandbox = SandboxState::from_policy(SandboxMode::Auto, &ws_root, &sess_tmp, &policy);

    let missing_file_en = ext_existing.join("out_en.txt");
    let missing_dir_zh = ext_existing.join("new_dir_zh");
    let missing_touch_zh = ext_existing.join("sub_a").join("touch_zh.txt");
    let writable_file = ext_already_writable.join("ok.txt");

    let combined_stderr = format!(
        "cp: cannot create regular file '{}': Read-only file system\n\
         mkdir: 无法创建目录 “{}”: 只读文件系统\n\
         touch: 无法 touch '{}': 只读文件系统\n\
         touch: cannot touch '{}': Read-only file system\n",
        missing_file_en.display(),
        missing_dir_zh.display(),
        missing_touch_zh.display(),
        writable_file.display(),
    );

    let violations = sandbox.extract_violations_from_output(&combined_stderr);
    assert_eq!(
        violations.len(),
        1,
        "all three non-writable ROFS paths under ext_existing collapse to 1 FileWrite violation and skip writable_ext; got: {violations:?}"
    );
    assert_eq!(
        violations[0].capability,
        CapabilityRequest::FileWrite {
            path: canonicalize_or_normalize(&ext_existing)
        }
    );
    assert_eq!(violations[0].operation, "file-write");
}

#[tokio::test]
async fn test_permissions_extension_prompt_and_end_to_end_allow_and_deny() {
    let perms_wasm = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/permissions/target/wasm32-wasip2/release/ragent_permissions_extension.wasm",
    );
    let file_editor_wasm = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    if !perms_wasm.exists() || !file_editor_wasm.exists() {
        return;
    }

    struct DenyAwareHost {
        items: Mutex<Vec<Item>>,
        events: Mutex<Vec<SessionEvent>>,
        checked: Mutex<Vec<(Option<String>, CapabilityRequest, Option<std::path::PathBuf>, Option<String>)>>,
        deny_substring: String,
    }

    #[async_trait]
    impl CoreHost for DenyAwareHost {
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
            extension_id: Option<&str>,
            capability: CapabilityRequest,
            reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            self.check_permission_with_original_path(extension_id, capability, None, reason)
                .await
        }

        async fn check_permission_with_original_path(
            &self,
            extension_id: Option<&str>,
            capability: CapabilityRequest,
            original_path: Option<std::path::PathBuf>,
            reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            self.checked.lock().unwrap().push((
                extension_id.map(str::to_string),
                capability.clone(),
                original_path,
                reason,
            ));
            let target_str = capability
                .path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            if target_str.contains(&self.deny_substring) {
                return Ok(PermissionOutcome::Denied {
                    reason: "Denied by user interaction".to_string(),
                });
            }
            Ok(PermissionOutcome::allowed_with(capability, true))
        }
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let ws_root = tmp.path().join("ws");
    let ext_dir = tmp.path().join("external_models");
    let denied_dir = tmp.path().join("forbidden_zone");
    std::fs::create_dir_all(&ws_root).unwrap();
    std::fs::create_dir_all(&ext_dir).unwrap();
    std::fs::create_dir_all(&denied_dir).unwrap();

    let host = Arc::new(DenyAwareHost {
        items: Mutex::new(vec![Item::user_message("Request external permissions and write a file")]),
        events: Mutex::new(vec![]),
        checked: Mutex::new(vec![]),
        deny_substring: "forbidden_zone".to_string(),
    });

    let target_file = ext_dir.join("approved.txt");
    let resp1 = make_openresponses_resource(
        "resp_p1",
        "gpt-4.1",
        vec![
            Item::FunctionCall {
                id: Some("fc_p1".to_string()),
                call_id: "call_p1".to_string(),
                name: "request_permission".to_string(),
                arguments: serde_json::json!({
                    "capability": "file_write",
                    "target": ext_dir.to_string_lossy(),
                    "reason": "Need write access to external_models"
                })
                .to_string(),
                status: None,
            },
            Item::FunctionCall {
                id: Some("fc_p2".to_string()),
                call_id: "call_p2".to_string(),
                name: "request_permission".to_string(),
                arguments: serde_json::json!({
                    "capability": "http",
                    "target": "https://huggingface.co/models",
                    "reason": "Need to download model weights"
                })
                .to_string(),
                status: None,
            },
            Item::FunctionCall {
                id: Some("fc_p3".to_string()),
                call_id: "call_p3".to_string(),
                name: "request_permission".to_string(),
                arguments: serde_json::json!({
                    "capability": "file_write",
                    "target": denied_dir.to_string_lossy(),
                    "reason": "Testing denial path"
                })
                .to_string(),
                status: None,
            },
        ],
        None,
    );
    let resp2 = make_openresponses_resource(
        "resp_p2",
        "gpt-4.1",
        vec![Item::FunctionCall {
            id: Some("fc_w1".to_string()),
            call_id: "call_w1".to_string(),
            name: "write_file".to_string(),
            arguments: serde_json::json!({
                "path": target_file.to_string_lossy(),
                "content": "unlocked via request_permission"
            })
            .to_string(),
            status: None,
        }],
        None,
    );
    let resp3 = make_openresponses_resource(
        "resp_p3",
        "gpt-4.1",
        vec![Item::assistant_message("Done.")],
        None,
    );

    let captured_requests = Arc::new(Mutex::new(Vec::<CreateResponseBody>::new()));
    struct CapturingModelBackend {
        responses: Mutex<Vec<ResponseResource>>,
        captured: Arc<Mutex<Vec<CreateResponseBody>>>,
    }
    #[async_trait]
    impl ModelBackend for CapturingModelBackend {
        async fn create_response(
            &self,
            _endpoint: &ModelEndpointConfig,
            request: CreateResponseBody,
        ) -> Result<ResponseResource, CoreError> {
            self.captured.lock().unwrap().push(request);
            let mut guard = self.responses.lock().unwrap();
            Ok(guard.remove(0))
        }
    }

    let model_backend = Arc::new(CapturingModelBackend {
        responses: Mutex::new(vec![resp1, resp2, resp3]),
        captured: captured_requests.clone(),
    });

    let cfg = ConfigRevision::new_merged(
        "Base prompt.".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![
            ExtensionSpec {
                name: "permissions".to_string(),
                path: perms_wasm.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
            ExtensionSpec {
                name: "file_editor".to_string(),
                path: file_editor_wasm.to_string_lossy().to_string(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_perm_ext"),
        activation_id: ActivationId::new("act_perm_ext"),
        workspace_root: ws_root,
        session_tmp_dir: tmp.path().join("tmp"),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
        model_endpoint: ModelEndpointConfig {
            base_url: "http://127.0.0.1:9999/v1".to_string(),
            api_key: "sk-test".to_string(),
            ..Default::default()
        },
        max_turns: Some(5),
    };

    let summary = SubprocessExecutor::default()
        .execute(run_spec, host.clone(), model_backend, CancellationToken::new())
        .await
        .expect("subprocess executor should succeed");
    assert_eq!(summary.final_text, "Done.");

    // 1. `<permission_guidance>` must be present in the instructions sent to the model
    let reqs = captured_requests.lock().unwrap();
    let instr = reqs[0].instructions.as_deref().unwrap_or("");
    assert!(
        instr.contains("<permission_guidance>") && instr.contains("request_permission"),
        "expected <permission_guidance> in instructions: {instr}"
    );

    // 2. Host received exactly 3 permission checks (all from `permissions` extension);
    // `file_editor` did NOT trigger a duplicate check because `sandbox_state` synced in worker!
    let checks = host.checked.lock().unwrap();
    assert_eq!(checks.len(), 3, "checks: {checks:?}");
    assert_eq!(checks[0].0.as_deref(), Some("permissions"));
    assert_eq!(
        checks[1].1,
        CapabilityRequest::Http {
            scheme: "https".to_string(),
            host: "huggingface.co".to_string(),
            port: None,
            method: "GET".to_string(),
        }
    );
    assert!(checks[1].2.is_none(), "Http check should not set original_path");

    // 3. Verify tool outputs include GRANTED, [PERMISSION_DENIED], and written file exists
    let items_json = serde_json::to_string(&*host.items.lock().unwrap()).unwrap();
    assert!(items_json.contains("Permission GRANTED"), "{items_json}");
    assert!(items_json.contains("[PERMISSION_DENIED]"), "{items_json}");
    assert_eq!(
        std::fs::read_to_string(&target_file).unwrap(),
        "unlocked via request_permission"
    );
}

#[test]
fn test_partition_tool_call_batches_read_read_write_read() {
    use crate::runner::{partition_tool_call_batches, ToolCallBatch};

    assert_eq!(
        partition_tool_call_batches(&[true, true, false, true], 10),
        vec![
            ToolCallBatch::Concurrent(vec![0, 1]),
            ToolCallBatch::Sequential(2),
            ToolCallBatch::Concurrent(vec![3]),
        ]
    );

    assert_eq!(partition_tool_call_batches(&[], 10), vec![]);

    assert_eq!(
        partition_tool_call_batches(&[false, false], 10),
        vec![ToolCallBatch::Sequential(0), ToolCallBatch::Sequential(1)]
    );

    assert_eq!(
        partition_tool_call_batches(&[true, true, true, true, true], 2),
        vec![
            ToolCallBatch::Concurrent(vec![0, 1]),
            ToolCallBatch::Concurrent(vec![2, 3]),
            ToolCallBatch::Concurrent(vec![4]),
        ]
    );
}

#[tokio::test]
async fn test_wasm_plugin_concurrent_invoke_pooling_runs_in_parallel() {
    let sh_wasm = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/session_history/target/wasm32-wasip2/release/ragent_session_history_extension.wasm",
    );
    if !sh_wasm.exists() {
        return;
    }

    struct BarrierHost {
        barrier: tokio::sync::Barrier,
    }

    #[async_trait]
    impl CoreHost for BarrierHost {
        async fn load_effective_context(&self) -> Result<Vec<Item>, CoreError> {
            Ok(vec![])
        }
        async fn commit_model_output(
            &self,
            _turn_id: &TurnId,
            _response_id: Option<String>,
            _items: Vec<Item>,
            _usage: Option<Usage>,
        ) -> Result<BatchSeq, CoreError> {
            Ok(BatchSeq::new(1))
        }
        async fn commit_tool_output(
            &self,
            _turn_id: &TurnId,
            _call_id: &str,
            _tool_name: &str,
            _success: bool,
            _duration_ms: u64,
            _items: Vec<Item>,
        ) -> Result<BatchSeq, CoreError> {
            Ok(BatchSeq::new(1))
        }
        async fn emit_event(
            &self,
            _turn_id: Option<TurnId>,
            _event: SessionEvent,
        ) -> Result<(), CoreError> {
            Ok(())
        }
        async fn check_permission(
            &self,
            _extension_id: Option<&str>,
            _capability: CapabilityRequest,
            _reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            Ok(PermissionOutcome::allowed())
        }
        async fn query_session_history(
            &self,
            _request: SessionHistoryQueryRequest,
        ) -> Result<SessionHistoryQueryResponse, CoreError> {
            tokio::time::timeout(std::time::Duration::from_secs(3), self.barrier.wait())
                .await
                .expect("two pooled WasmPlugin invocations must reach barrier concurrently");
            Ok(SessionHistoryQueryResponse {
                ok: true,
                workspace_scope: Some("/tmp".into()),
                ..Default::default()
            })
        }
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let host: Arc<dyn CoreHost> = Arc::new(BarrierHost {
        barrier: tokio::sync::Barrier::new(2),
    });
    let sandbox = Arc::new(SandboxState::from_policy(
        SandboxMode::Auto,
        tmp.path(),
        tmp.path(),
        &PermissionPolicyLayer::default(),
    ));
    let plugin = crate::hooks::runtime::WasmPlugin::load_from_file_with_host(
        "session_history",
        &sh_wasm,
        tmp.path(),
        &[tmp.path().to_path_buf()],
        Some(host),
        sandbox,
    )
    .await
    .expect("load session_history wasm");
    plugin.initialize(&serde_json::Value::Null).await.unwrap();

    let req1 = HookRequest {
        hook: HOOK_TOOLS_CALL.into(),
        protocol_version: 1,
        invocation_id: 1,
        iteration: Some(1),
        payload: serde_json::json!({
            "call_id": "c1",
            "tool_id": "t1",
            "name": "session_history_list",
            "arguments": {}
        }),
    };
    let req2 = HookRequest {
        hook: HOOK_TOOLS_CALL.into(),
        protocol_version: 1,
        invocation_id: 2,
        iteration: Some(1),
        payload: serde_json::json!({
            "call_id": "c2",
            "tool_id": "t1",
            "name": "session_history_list",
            "arguments": {}
        }),
    };

    let (r1, r2) = tokio::join!(plugin.invoke(&req1), plugin.invoke(&req2));
    assert!(r1.is_ok(), "r1 failed: {r1:?}");
    assert!(r2.is_ok(), "r2 failed: {r2:?}");
    plugin.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_in_process_executor_batches_read_read_write_read_in_order() {
    let sh_wasm = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/session_history/target/wasm32-wasip2/release/ragent_session_history_extension.wasm",
    );
    let editor_wasm = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm",
    );
    if !sh_wasm.exists() || !editor_wasm.exists() {
        return;
    }

    struct BatchTestHost {
        items: Mutex<Vec<Item>>,
        timeline: Mutex<Vec<String>>,
        read_barrier: tokio::sync::Barrier,
    }

    #[async_trait]
    impl CoreHost for BatchTestHost {
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
            call_id: &str,
            tool_name: &str,
            _success: bool,
            _duration_ms: u64,
            items: Vec<Item>,
        ) -> Result<BatchSeq, CoreError> {
            self.timeline
                .lock()
                .unwrap()
                .push(format!("commit:{call_id}:{tool_name}"));
            self.items.lock().unwrap().extend(items);
            Ok(BatchSeq::new(2))
        }
        async fn emit_event(
            &self,
            _turn_id: Option<TurnId>,
            event: SessionEvent,
        ) -> Result<(), CoreError> {
            if let SessionEvent::ToolCallStarted { call_id, name } = event {
                self.timeline
                    .lock()
                    .unwrap()
                    .push(format!("start:{call_id}:{name}"));
            }
            Ok(())
        }
        async fn check_permission(
            &self,
            _extension_id: Option<&str>,
            _capability: CapabilityRequest,
            _reason: Option<String>,
        ) -> Result<PermissionOutcome, CoreError> {
            Ok(PermissionOutcome::allowed())
        }
        async fn query_session_history(
            &self,
            _request: SessionHistoryQueryRequest,
        ) -> Result<SessionHistoryQueryResponse, CoreError> {
            tokio::time::timeout(std::time::Duration::from_secs(3), self.read_barrier.wait())
                .await
                .expect("two reads in Batch 1 must run concurrently");
            Ok(SessionHistoryQueryResponse {
                ok: true,
                workspace_scope: Some("ws".into()),
                ..Default::default()
            })
        }
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let ws_root = tmp.path().join("ws");
    std::fs::create_dir_all(&ws_root).unwrap();
    let target_file = ws_root.join("batch_note.txt");

    let host = Arc::new(BatchTestHost {
        items: Mutex::new(vec![Item::user_message("run read, read, write, read")]),
        timeline: Mutex::new(Vec::new()),
        read_barrier: tokio::sync::Barrier::new(2),
    });

    // Turn 1: [read(session_history_list), read(session_history_list), write(write_file), read(read_file)]
    let resp1 = make_openresponses_resource(
        "resp_batch_1",
        "gpt-4.1",
        vec![
            Item::FunctionCall {
                id: None,
                call_id: "c0_read1".into(),
                name: "session_history_list".into(),
                arguments: "{}".into(),
                status: None,
            },
            Item::FunctionCall {
                id: None,
                call_id: "c1_read2".into(),
                name: "session_history_list".into(),
                arguments: "{}".into(),
                status: None,
            },
            Item::FunctionCall {
                id: None,
                call_id: "c2_write".into(),
                name: "write_file".into(),
                arguments: serde_json::json!({
                    "path": target_file.to_string_lossy(),
                    "content": "written_in_batch_2"
                })
                .to_string(),
                status: None,
            },
            Item::FunctionCall {
                id: None,
                call_id: "c3_read3".into(),
                name: "read_file".into(),
                arguments: serde_json::json!({
                    "path": target_file.to_string_lossy()
                })
                .to_string(),
                status: None,
            },
        ],
        None,
    );
    let resp2 = make_openresponses_resource(
        "resp_batch_2",
        "gpt-4.1",
        vec![Item::assistant_message("All 3 batches done.")],
        None,
    );

    let cfg = ConfigRevision::new_merged(
        "Base.".into(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![
            ExtensionSpec {
                name: "session_history".into(),
                path: sh_wasm.to_string_lossy().into_owned(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
            ExtensionSpec {
                name: "file_editor".into(),
                path: editor_wasm.to_string_lossy().into_owned(),
                enabled: true,
                config: serde_json::json!({}),
                allowed_dirs: vec![],
                filter_env: vec![],
            },
        ],
    );

    let run_spec = CoreRunSpec {
        session_id: SessionId::new("sess_batch"),
        activation_id: ActivationId::new("act_batch"),
        workspace_root: ws_root,
        session_tmp_dir: tmp.path().join("tmp"),
        extra_tmp_dirs: vec![],
        effective_config: cfg,
        effective_permissions: PermissionPolicyLayer::default(),
        disabled_tools: vec![],
        model_endpoint: ModelEndpointConfig {
            base_url: "http://127.0.0.1:9999/v1".into(),
            api_key: "sk-test".into(),
            ..Default::default()
        },
        max_turns: Some(4),
    };

    let model = Arc::new(MockModelBackend {
        responses: Mutex::new(vec![resp1, resp2]),
    });

    let summary = InProcessExecutor
        .execute(run_spec, host.clone(), model, CancellationToken::new())
        .await
        .expect("batched executor should succeed");
    assert_eq!(summary.final_text, "All 3 batches done.");

    let timeline = host.timeline.lock().unwrap().clone();
    assert_eq!(
        timeline,
        vec![
            // Batch 1 (concurrent)
            "start:c0_read1:session_history_list",
            "start:c1_read2:session_history_list",
            "commit:c0_read1:session_history_list",
            "commit:c1_read2:session_history_list",
            // Batch 2 (sequential write)
            "start:c2_write:write_file",
            "commit:c2_write:write_file",
            // Batch 3 (read after write)
            "start:c3_read3:read_file",
            "commit:c3_read3:read_file",
        ],
        "unexpected batch execution timeline: {timeline:?}"
    );

    let items = host.items.lock().unwrap();
    let outputs: Vec<(String, String)> = items
        .iter()
        .filter_map(|item| match item {
            Item::FunctionCallOutput {
                call_id, output, ..
            } => Some((call_id.clone(), format!("{output:?}"))),
            _ => None,
        })
        .collect();
    assert_eq!(outputs.len(), 4);
    assert_eq!(outputs[0].0, "c0_read1");
    assert_eq!(outputs[1].0, "c1_read2");
    assert_eq!(outputs[2].0, "c2_write");
    assert_eq!(outputs[3].0, "c3_read3");
    assert!(
        outputs[3].1.contains("written_in_batch_2"),
        "Batch 3 read_file must observe Batch 2 write_file output: {:?}",
        outputs[3]
    );
}

#[test]
fn test_sandbox_writable_implies_readable_respecting_denies() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path().join("ws");
    let ext_write = tmp.path().join("ext_write");
    let ext_write_locked = ext_write.join("locked");
    let ext_secret = ext_write.join("secret");
    let ws_secret = ws.join(".env");
    std::fs::create_dir_all(&ext_write_locked).unwrap();
    std::fs::create_dir_all(&ext_secret).unwrap();
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(&ws_secret, "SECRET=1").unwrap();

    let policy = PermissionPolicyLayer {
        file_read: Some(RuleSet {
            allow: Some(vec![".".to_string()]),
            deny: Some(vec![
                ws_secret.to_string_lossy().into_owned(),
                ext_secret.to_string_lossy().into_owned(),
            ]),
            default: Some(DefaultPermissionDecision::Ask),
        }),
        file_write: Some(RuleSet {
            allow: Some(vec![
                ".".to_string(),
                ext_write.to_string_lossy().into_owned(),
            ]),
            deny: Some(vec![ext_write_locked.to_string_lossy().into_owned()]),
            default: Some(DefaultPermissionDecision::Ask),
        }),
        ..Default::default()
    };

    let sess_tmp = tmp.path().join("sess_tmp");
    std::fs::create_dir_all(&sess_tmp).unwrap();
    let sandbox = SandboxState::from_policy_with_extra_tmp(
        SandboxMode::Auto,
        &ws,
        &sess_tmp,
        &[],
        &policy,
    );

    assert!(sandbox.is_path_writable(&ext_write.join("a.txt")));
    assert!(sandbox.is_path_readable(&ext_write.join("a.txt")));
    assert!(!sandbox.is_path_writable(&ext_write_locked.join("b.txt")));
    assert!(!sandbox.is_path_readable(&ext_write_locked.join("b.txt")));
    assert!(!sandbox.is_path_readable(&ext_secret.join("k.txt")));
    assert!(sandbox.is_read_denied(&ws_secret));
    assert!(!sandbox.is_path_readable(&ws_secret));
}


