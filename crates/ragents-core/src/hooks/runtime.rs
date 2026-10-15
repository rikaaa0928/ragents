use crate::error::CoreError;
use crate::hooks::protocol::{ExtensionMetadata, HookRequest};
use crate::host::{CoreHost, PermissionOutcome};
use crate::sandbox::{SandboxMode, SandboxState};
use ragents_protocol::{CapabilityRequest, PermissionPolicyLayer};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{
    DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView,
};

static SHARED_ENGINE: OnceLock<Engine> = OnceLock::new();

fn global_engine() -> Result<&'static Engine, CoreError> {
    if let Some(engine) = SHARED_ENGINE.get() {
        return Ok(engine);
    }
    let mut config = Config::new();
    config.async_support(true);
    let engine = Engine::new(&config).map_err(tool_error)?;
    let _ = SHARED_ENGINE.set(engine);
    SHARED_ENGINE
        .get()
        .ok_or_else(|| CoreError::ToolError("failed to initialize wasmtime engine".into()))
}

fn load_or_precompile_component(engine: &Engine, path: &Path) -> Result<Component, CoreError> {
    let wasm_bytes = std::fs::read(path).map_err(|e| {
        CoreError::ToolError(format!(
            "failed to read WebAssembly component from {path:?}: {e}"
        ))
    })?;

    let mut hasher = Sha256::new();
    hasher.update(b"wasmtime-37-component:");
    hasher.update(std::env::consts::OS.as_bytes());
    hasher.update(std::env::consts::ARCH.as_bytes());
    hasher.update(&wasm_bytes);
    let digest = format!("{:x}", hasher.finalize());

    let cache_dir = std::env::temp_dir().join("ragents-cwasm-cache");
    let _ = std::fs::create_dir_all(&cache_dir);
    let cwasm_path = cache_dir.join(format!("{digest}.cwasm"));

    if cwasm_path.exists() {
        // SAFETY: The `.cwasm` file is keyed by SHA-256 of the source `.wasm` + OS/Arch + Wasmtime version
        // and produced exclusively by `Engine::precompile_component` on this host.
        if let Ok(component) = unsafe { Component::deserialize_file(engine, &cwasm_path) } {
            return Ok(component);
        }
    }

    if let Ok(precompiled) = engine.precompile_component(&wasm_bytes) {
        let tmp_path = cache_dir.join(format!("{digest}.{}.tmp", std::process::id()));
        if std::fs::write(&tmp_path, &precompiled).is_ok() {
            let _ = std::fs::rename(&tmp_path, &cwasm_path);
        }
        if let Ok(component) = unsafe { Component::deserialize(engine, &precompiled) } {
            return Ok(component);
        }
    }

    Component::from_binary(engine, &wasm_bytes).map_err(|error| {
        CoreError::ToolError(format!(
            "failed to compile WebAssembly component from {path:?}: {error}"
        ))
    })
}

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "plugin",
    imports: { default: async },
    exports: { default: async },
});

struct HostState {
    wasi_ctx: WasiCtx,
    table: ResourceTable,
    plugin_name: String,
    workspace_root: PathBuf,
    core_host: Option<Arc<dyn CoreHost>>,
    sandbox_state: Arc<SandboxState>,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}

impl ragent::extension::host::Host for HostState {
    async fn execute_command_with_timeout(
        &mut self,
        command: String,
        timeout_ms: u64,
    ) -> ragent::extension::host::CommandOutput {
        // 1. Mandatory Host-Import Permission Check (Blind Spot 1 Fix):
        // Regardless of tool name, any WASM guest calling execute_command_with_timeout
        // MUST pass CoreHost::check_permission first.
        if let Some(ref host) = self.core_host {
            let cap = CapabilityRequest::CommandWorkDir {
                work_dir: self.workspace_root.clone(),
            };
            let reason = format!(
                "Extension '{}' executing shell command: {}",
                self.plugin_name, command
            );
            match host
                .check_permission(Some(&self.plugin_name), cap, Some(reason))
                .await
            {
                Ok(PermissionOutcome::Allowed { .. }) => {}
                Ok(PermissionOutcome::Denied { reason }) => {
                    return command_error(format!("Permission denied: {reason}"));
                }
                Err(err) => {
                    return command_error(format!("Permission check error: {err}"));
                }
            }
        }

        // 2. OS Kernel Sandboxed Execution with Multi-Directory Loop & Automatic Retry:
        // A single command (e.g. `uv run`, `pip install`, or chained `cmd1 && cmd2 && cmd3`)
        // may require multiple external directories across either the same run or sequential retries.
        // We loop as long as each retry discovers at least one NEW path not yet covered by `unlocked_paths`,
        // up to MAX_SANDBOX_ESCALATION_ROUNDS.
        const MAX_SANDBOX_ESCALATION_ROUNDS: usize = 16;
        let mut unlocked_paths: Vec<PathBuf> = Vec::new();
        let mut temporary_once_paths: Vec<PathBuf> = Vec::new();

        let mut final_output = command_error(
            "sandbox multi-directory permission escalation loop exceeded maximum rounds",
        );

        for _round in 0..MAX_SANDBOX_ESCALATION_ROUNDS {
            let (mut output, violations) = self
                .sandbox_state
                .execute_command_with_violations(&self.workspace_root, &command, timeout_ms)
                .await;

            if output.exit_code == 0 || violations.is_empty() {
                final_output = output;
                break;
            }

            let Some(ref host) = self.core_host else {
                final_output = output;
                break;
            };

            // Filter to violations whose raw_path is not already covered by a previously unlocked directory
            // (prevents infinite loops if a path is blocked by OS SIP/chmod rather than our sandbox,
            // while still prompting for sibling directories if the user narrowed `--path` on an earlier round).
            let new_violations: Vec<_> = violations
                .into_iter()
                .filter(|v| !unlocked_paths.iter().any(|u| v.raw_path.starts_with(u)))
                .collect();

            if new_violations.is_empty() {
                final_output = output;
                break;
            }

            let mut all_approved = true;
            for v in new_violations {
                let reason = format!(
                    "Command `{}` requires {} access to '{}' (suggested directory: '{}')",
                    command,
                    v.operation,
                    v.raw_path.display(),
                    v.capability
                        .path()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                );
                match host
                    .check_permission_with_original_path(
                        Some(&self.plugin_name),
                        v.capability.clone(),
                        Some(v.raw_path.clone()),
                        Some(reason),
                    )
                    .await
                {
                    Ok(PermissionOutcome::Allowed {
                        effective_capability,
                        persist_in_activation,
                    }) => {
                        let granted_cap =
                            effective_capability.unwrap_or_else(|| v.capability.clone());
                        if let Some(p) = granted_cap.path() {
                            unlocked_paths.push(p.to_path_buf());
                            if !persist_in_activation {
                                temporary_once_paths.push(p.to_path_buf());
                            }
                        }
                        unlocked_paths.push(v.raw_path.clone());
                        match granted_cap {
                            CapabilityRequest::FileWrite { path } => {
                                self.sandbox_state.allow_writable_path(path);
                            }
                            CapabilityRequest::CommandWorkDir { work_dir } => {
                                self.sandbox_state.allow_writable_path(work_dir);
                            }
                            CapabilityRequest::FileRead { path } => {
                                self.sandbox_state.allow_readable_path(path);
                            }
                            CapabilityRequest::Http { .. } => {}
                        }
                    }
                    Ok(PermissionOutcome::Denied { reason }) => {
                        output.stderr.push_str(&format!(
                            "\n[RAGENTS_SANDBOX_DENIED] Permission denied for {:?}: {reason}",
                            v.capability
                        ));
                        all_approved = false;
                        break;
                    }
                    Err(err) => {
                        output.stderr.push_str(&format!(
                            "\n[RAGENTS_SANDBOX_ERROR] Permission request failed: {err}"
                        ));
                        all_approved = false;
                        break;
                    }
                }
            }

            if !all_approved {
                final_output = output;
                break;
            }
        }

        // Revoke any paths that were approved with `AllowOnce` so they don't carry over to future commands
        for tmp_path in temporary_once_paths {
            self.sandbox_state.revoke_temporary_path(&tmp_path);
        }

        final_output
    }

    async fn http_fetch(
        &mut self,
        request: ragent::extension::host::HttpRequest,
    ) -> ragent::extension::host::HttpResponse {
        let parsed = match reqwest::Url::parse(&request.url) {
            Ok(u) => u,
            Err(err) => return http_error(format!("Invalid URL '{}': {err}", request.url)),
        };

        let scheme = parsed.scheme().to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return http_error(format!(
                "Unsupported URL scheme '{scheme}'; only http and https are allowed"
            ));
        }
        let Some(host_str) = parsed.host_str().map(|s| s.to_ascii_lowercase()) else {
            return http_error(format!("URL '{}' has no host", request.url));
        };
        let port = parsed.port_or_known_default();
        let method_str = if request.method.trim().is_empty() {
            "GET".to_string()
        } else {
            request.method.trim().to_ascii_uppercase()
        };

        // 1. Mandatory Host-Level HTTP Permission Check
        if let Some(ref host_cb) = self.core_host {
            let cap = CapabilityRequest::Http {
                scheme: scheme.clone(),
                host: host_str.clone(),
                port,
                method: method_str.clone(),
            };
            let reason = format!(
                "WASM extension '{}' requests HTTP {} '{}' (host: '{}')",
                self.plugin_name, method_str, request.url, host_str
            );
            match host_cb
                .check_permission_with_original_path(
                    Some(&self.plugin_name),
                    cap,
                    Some(PathBuf::from(&request.url)),
                    Some(reason),
                )
                .await
            {
                Ok(PermissionOutcome::Allowed {
                    effective_capability,
                    ..
                }) => {
                    if let Some(CapabilityRequest::Http {
                        host: ref approved_host,
                        ..
                    }) = effective_capability
                    {
                        if !host_matches_pattern(approved_host, &host_str) {
                            return http_error(format!(
                                "Permission denied: approved host pattern '{approved_host}' does not cover requested host '{host_str}'"
                            ));
                        }
                    }
                }
                Ok(PermissionOutcome::Denied { reason }) => {
                    return http_error(format!("Permission denied for HTTP request: {reason}"));
                }
                Err(err) => {
                    return http_error(format!("Permission check failed: {err}"));
                }
            }
        }

        // 2. Execute HTTP request via reqwest
        let mut client_builder = reqwest::Client::builder();
        if request.timeout_ms > 0 {
            client_builder =
                client_builder.timeout(std::time::Duration::from_millis(request.timeout_ms));
        }
        let client = match client_builder.build() {
            Ok(c) => c,
            Err(err) => return http_error(format!("Failed to build HTTP client: {err}")),
        };

        let method = match reqwest::Method::from_bytes(method_str.as_bytes()) {
            Ok(m) => m,
            Err(err) => return http_error(format!("Invalid HTTP method '{method_str}': {err}")),
        };

        let mut req_builder = client.request(method, parsed);
        for h in &request.headers {
            req_builder = req_builder.header(&h.name, &h.value);
        }
        if let Some(body) = request.body {
            req_builder = req_builder.body(body);
        }

        match req_builder.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let mut headers = Vec::new();
                for (k, v) in resp.headers() {
                    if let Ok(val_str) = v.to_str() {
                        headers.push(ragent::extension::host::HttpHeader {
                            name: k.as_str().to_string(),
                            value: val_str.to_string(),
                        });
                    }
                }
                match resp.text().await {
                    Ok(body) => ragent::extension::host::HttpResponse {
                        status,
                        headers,
                        body,
                        error: None,
                    },
                    Err(err) => http_error(format!("Failed to read HTTP response body: {err}")),
                }
            }
            Err(err) => http_error(format!("HTTP request failed: {err}")),
        }
    }
}

fn command_error(error: impl ToString) -> ragent::extension::host::CommandOutput {
    ragent::extension::host::CommandOutput {
        exit_code: -1,
        stdout: String::new(),
        stderr: String::new(),
        error: Some(error.to_string()),
    }
}

fn http_error(error: impl ToString) -> ragent::extension::host::HttpResponse {
    ragent::extension::host::HttpResponse {
        status: 0,
        headers: Vec::new(),
        body: String::new(),
        error: Some(error.to_string()),
    }
}

fn host_matches_pattern(rule: &str, host: &str) -> bool {
    let rule = rule.trim();
    let host = host.trim();
    if rule == "*" || rule.eq_ignore_ascii_case(host) {
        return true;
    }
    if let Some(suffix) = rule.strip_prefix("*.") {
        if host.eq_ignore_ascii_case(suffix) {
            return true;
        }
        let dot_suffix = format!(".{}", suffix.to_ascii_lowercase());
        return host.to_ascii_lowercase().ends_with(&dot_suffix);
    }
    false
}

struct PluginRuntime {
    store: Store<HostState>,
    bindings: Plugin,
    init_config: Option<String>,
    mounted_dirs: (Vec<PathBuf>, Vec<PathBuf>),
}

pub struct WasmPlugin {
    pub name: String,
    metadata: ExtensionMetadata,
    component: Component,
    linker: Arc<Linker<HostState>>,
    workspace_root: PathBuf,
    preopen_dirs: Vec<PathBuf>,
    core_host: Option<Arc<dyn CoreHost>>,
    sandbox_state: Arc<SandboxState>,
    runtime: Mutex<PluginRuntime>,
}

impl WasmPlugin {
    pub async fn load_from_file_with_workspace(
        name: impl Into<String>,
        path: &Path,
        workspace_root: &Path,
        preopen_dirs: &[PathBuf],
    ) -> Result<Self, CoreError> {
        let default_sandbox = Arc::new(SandboxState::from_policy(
            SandboxMode::Auto,
            workspace_root,
            workspace_root,
            &PermissionPolicyLayer::default(),
        ));
        Self::load_from_file_with_host(
            name,
            path,
            workspace_root,
            preopen_dirs,
            None,
            default_sandbox,
        )
        .await
    }

    pub async fn load_from_file_with_host(
        name: impl Into<String>,
        path: &Path,
        workspace_root: &Path,
        preopen_dirs: &[PathBuf],
        core_host: Option<Arc<dyn CoreHost>>,
        sandbox_state: Arc<SandboxState>,
    ) -> Result<Self, CoreError> {
        let plugin_name = name.into();
        let engine = global_engine()?;
        let component = load_or_precompile_component(engine, path)?;

        let mut linker = Linker::new(engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(tool_error)?;
        Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        let linker = Arc::new(linker);

        let mounted_dirs = sandbox_state.wasi_preopen_dirs();
        let wasi_ctx = build_wasi_ctx_with_mounts(workspace_root, preopen_dirs, &mounted_dirs);

        let host_state = HostState {
            wasi_ctx,
            table: ResourceTable::new(),
            plugin_name: plugin_name.clone(),
            workspace_root: workspace_root.to_path_buf(),
            core_host: core_host.clone(),
            sandbox_state: sandbox_state.clone(),
        };

        let mut store = Store::new(engine, host_state);
        let bindings = Plugin::instantiate_async(&mut store, &component, &linker)
            .await
            .map_err(tool_error)?;
        let metadata_json = bindings
            .ragent_extension_lifecycle()
            .call_metadata(&mut store)
            .await
            .map_err(tool_error)?;
        let metadata: ExtensionMetadata =
            serde_json::from_str(&metadata_json).map_err(|error| {
                CoreError::ToolError(format!("extension metadata is invalid: {error}"))
            })?;

        Ok(Self {
            name: plugin_name,
            metadata,
            component,
            linker,
            workspace_root: workspace_root.to_path_buf(),
            preopen_dirs: preopen_dirs.to_vec(),
            core_host,
            sandbox_state,
            runtime: Mutex::new(PluginRuntime {
                store,
                bindings,
                init_config: None,
                mounted_dirs,
            }),
        })
    }

    pub fn metadata(&self) -> &ExtensionMetadata {
        &self.metadata
    }

    pub async fn initialize(&self, config: &serde_json::Value) -> Result<(), CoreError> {
        let config_str = serde_json::to_string(config)?;
        let mut runtime = self.runtime.lock().await;
        runtime.init_config = Some(config_str.clone());
        let PluginRuntime {
            store, bindings, ..
        } = &mut *runtime;
        bindings
            .ragent_extension_lifecycle()
            .call_initialize(store, &config_str)
            .await
            .map_err(tool_error)?
            .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))
    }

    async fn ensure_wasi_mounts_up_to_date(
        &self,
        runtime: &mut PluginRuntime,
    ) -> Result<(), CoreError> {
        let current_dirs = self.sandbox_state.wasi_preopen_dirs();
        if current_dirs == runtime.mounted_dirs {
            return Ok(());
        }

        let engine = global_engine()?;
        let wasi_ctx =
            build_wasi_ctx_with_mounts(&self.workspace_root, &self.preopen_dirs, &current_dirs);
        let host_state = HostState {
            wasi_ctx,
            table: ResourceTable::new(),
            plugin_name: self.name.clone(),
            workspace_root: self.workspace_root.clone(),
            core_host: self.core_host.clone(),
            sandbox_state: self.sandbox_state.clone(),
        };
        let mut store = Store::new(engine, host_state);
        let bindings = Plugin::instantiate_async(&mut store, &self.component, &self.linker)
            .await
            .map_err(tool_error)?;
        if let Some(ref cfg) = runtime.init_config {
            bindings
                .ragent_extension_lifecycle()
                .call_initialize(&mut store, cfg)
                .await
                .map_err(tool_error)?
                .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?;
        }
        runtime.store = store;
        runtime.bindings = bindings;
        runtime.mounted_dirs = current_dirs;
        Ok(())
    }

    pub async fn invoke(&self, request: &HookRequest) -> Result<serde_json::Value, CoreError> {
        let request_str = serde_json::to_string(request)?;
        if request.hook != "tools.call" {
            let mut runtime = self.runtime.lock().await;
            self.ensure_wasi_mounts_up_to_date(&mut runtime).await?;
            let PluginRuntime {
                store, bindings, ..
            } = &mut *runtime;
            let response = bindings
                .ragent_extension_lifecycle()
                .call_invoke(store, &request_str)
                .await
                .map_err(tool_error)?
                .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?;
            return serde_json::from_str(&response).map_err(CoreError::Json);
        }

        // For `tools.call`, enforce WASM file permissions, dynamically mount approved directories
        // into WASI, and automatically escalate if a WASI file operation fails on an unmounted path.
        let mut temporary_once_paths: Vec<PathBuf> = Vec::new();
        let mut unlocked_paths: Vec<PathBuf> = Vec::new();

        let tool_name = request
            .payload
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        // 1. Pre-check file capability if the tool call includes a `path` argument
        if let Some(path_str) = request
            .payload
            .get("arguments")
            .and_then(|a| a.get("path"))
            .and_then(|v| v.as_str())
        {
            let full_path = resolve_against_workspace(&self.workspace_root, path_str);
            let is_write = matches!(tool_name, "write_file" | "replace_in_file")
                || tool_name.contains("write")
                || tool_name.contains("edit")
                || tool_name.contains("replace");

            if let Some(denied_resp) = self
                .request_and_unlock_wasm_file_access(
                    tool_name,
                    &full_path,
                    is_write,
                    &mut unlocked_paths,
                    &mut temporary_once_paths,
                )
                .await?
            {
                return Ok(denied_resp);
            }
        }

        // 2. Execute inside WASM with automatic WASI file escalation loop (for tools with custom argument names or multi-file access)
        const MAX_WASM_FILE_ESCALATION_ROUNDS: usize = 8;
        let mut final_val = serde_json::Value::Null;

        for _round in 0..MAX_WASM_FILE_ESCALATION_ROUNDS {
            let response_str = {
                let mut runtime = self.runtime.lock().await;
                self.ensure_wasi_mounts_up_to_date(&mut runtime).await?;
                let PluginRuntime {
                    store, bindings, ..
                } = &mut *runtime;
                bindings
                    .ragent_extension_lifecycle()
                    .call_invoke(store, &request_str)
                    .await
                    .map_err(tool_error)?
                    .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?
            };

            let val: serde_json::Value =
                serde_json::from_str(&response_str).map_err(CoreError::Json)?;
            let is_failed = val
                .get("payload")
                .and_then(|p| p.get("success"))
                .and_then(|s| s.as_bool())
                == Some(false);

            if !is_failed || self.core_host.is_none() {
                final_val = val;
                break;
            }

            let err_text = format!(
                "{}\n{}",
                val.get("payload")
                    .and_then(|p| p.get("output"))
                    .and_then(|s| s.as_str())
                    .unwrap_or_default(),
                val.get("payload")
                    .and_then(|p| p.get("error"))
                    .and_then(|s| s.as_str())
                    .unwrap_or_default()
            );

            let candidates = extract_wasm_file_error_paths(&err_text, &self.workspace_root);
            let new_candidates: Vec<(PathBuf, bool)> = candidates
                .into_iter()
                .filter(|(p, _)| !unlocked_paths.iter().any(|u| p.starts_with(u)))
                .collect();

            if new_candidates.is_empty() {
                final_val = val;
                break;
            }

            let mut all_approved = true;
            for (cand_path, is_write) in new_candidates {
                if let Some(denied_resp) = self
                    .request_and_unlock_wasm_file_access(
                        tool_name,
                        &cand_path,
                        is_write,
                        &mut unlocked_paths,
                        &mut temporary_once_paths,
                    )
                    .await?
                {
                    final_val = denied_resp;
                    all_approved = false;
                    break;
                }
            }

            if !all_approved {
                break;
            }
        }

        // 3. Revoke any `AllowOnce` paths so they are unmounted on subsequent calls
        for tmp_path in temporary_once_paths {
            self.sandbox_state.revoke_temporary_path(&tmp_path);
        }

        Ok(final_val)
    }

    async fn request_and_unlock_wasm_file_access(
        &self,
        tool_name: &str,
        full_path: &Path,
        is_write: bool,
        unlocked_paths: &mut Vec<PathBuf>,
        temporary_once_paths: &mut Vec<PathBuf>,
    ) -> Result<Option<serde_json::Value>, CoreError> {
        let Some(ref host) = self.core_host else {
            return Ok(None);
        };

        let in_workspace = full_path.starts_with(&self.workspace_root);
        let suggested_path = if in_workspace {
            full_path.to_path_buf()
        } else {
            crate::sandbox::normalize_violation_to_request_dir(full_path)
        };

        let cap = if is_write {
            CapabilityRequest::FileWrite {
                path: suggested_path.clone(),
            }
        } else {
            CapabilityRequest::FileRead {
                path: suggested_path.clone(),
            }
        };

        let reason = format!(
            "WASM tool '{tool_name}' requires {} access to '{}' (suggested path: '{}')",
            if is_write { "file-write" } else { "file-read" },
            full_path.display(),
            suggested_path.display()
        );

        match host
            .check_permission_with_original_path(
                Some(&self.name),
                cap.clone(),
                Some(full_path.to_path_buf()),
                Some(reason),
            )
            .await?
        {
            PermissionOutcome::Allowed {
                effective_capability,
                persist_in_activation,
            } => {
                let granted_cap = effective_capability.unwrap_or(cap);
                let approved_path = granted_cap
                    .path()
                    .map(|p| p.to_path_buf())
                    .unwrap_or(suggested_path);

                // Verify that the approved directory/path covers the requested file path
                let norm_full = crate::sandbox::canonicalize_or_normalize(full_path);
                let norm_approved = crate::sandbox::canonicalize_or_normalize(&approved_path);
                if !norm_full.starts_with(&norm_approved) && !full_path.starts_with(&approved_path)
                {
                    let msg = format!(
                        "Permission denied: approved path '{}' does not cover requested file '{}'",
                        approved_path.display(),
                        full_path.display()
                    );
                    return Ok(Some(serde_json::json!({
                        "action": "continue",
                        "payload": {
                            "success": false,
                            "output": msg,
                            "error": msg,
                        }
                    })));
                }

                // For external paths, ensure the directory exists on host (for write) and unlock in SandboxState
                // so WASI `preopened_dir` mounts it before `call_invoke`.
                let mount_dir = if approved_path.is_dir() {
                    approved_path.clone()
                } else if !in_workspace && is_write && approved_path.extension().is_none() {
                    let _ = std::fs::create_dir_all(&approved_path);
                    approved_path.clone()
                } else {
                    approved_path
                        .parent()
                        .unwrap_or(&self.workspace_root)
                        .to_path_buf()
                };

                // Also track the un-canonicalized parent directory (e.g. `/var/...` vs `/private/var/...` on macOS)
                // so WASI guest path matching succeeds regardless of symlink resolution.
                let raw_parent = full_path.parent().map(|p| p.to_path_buf());

                if is_write {
                    if !in_workspace && !mount_dir.exists() {
                        let _ = std::fs::create_dir_all(&mount_dir);
                    }
                    self.sandbox_state.allow_writable_path(mount_dir.clone());
                    if let Some(ref rp) = raw_parent {
                        if rp != &mount_dir {
                            let _ = std::fs::create_dir_all(rp);
                            self.sandbox_state.allow_writable_path(rp.clone());
                        }
                    }
                } else {
                    self.sandbox_state.allow_readable_path(mount_dir.clone());
                    if let Some(ref rp) = raw_parent {
                        if rp != &mount_dir {
                            self.sandbox_state.allow_readable_path(rp.clone());
                        }
                    }
                }

                unlocked_paths.push(mount_dir.clone());
                if let Some(ref rp) = raw_parent {
                    unlocked_paths.push(rp.clone());
                }
                unlocked_paths.push(full_path.to_path_buf());
                if !persist_in_activation {
                    temporary_once_paths.push(mount_dir);
                    if let Some(rp) = raw_parent {
                        temporary_once_paths.push(rp);
                    }
                }
                Ok(None)
            }
            PermissionOutcome::Denied { reason } => {
                let msg = format!("Permission denied: {reason}");
                Ok(Some(serde_json::json!({
                    "action": "continue",
                    "payload": {
                        "success": false,
                        "output": msg,
                        "error": msg,
                    }
                })))
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), CoreError> {
        let mut runtime = self.runtime.lock().await;
        let PluginRuntime {
            store, bindings, ..
        } = &mut *runtime;
        bindings
            .ragent_extension_lifecycle()
            .call_shutdown(store)
            .await
            .map_err(tool_error)
    }
}

fn build_wasi_ctx_with_mounts(
    workspace_root: &Path,
    preopen_dirs: &[PathBuf],
    mounted_dirs: &(Vec<PathBuf>, Vec<PathBuf>),
) -> WasiCtx {
    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.inherit_stdout().inherit_stderr();

    // Map guest "." to workspace_root so relative paths inside WASM resolve to workspace_root
    let _ = wasi_builder.preopened_dir(workspace_root, ".", DirPerms::all(), FilePerms::all());
    if let Some(ws_str) = workspace_root.to_str() {
        let _ =
            wasi_builder.preopened_dir(workspace_root, ws_str, DirPerms::all(), FilePerms::all());
    }
    for dir in preopen_dirs {
        if dir.exists() {
            if let Some(dir_str) = dir.to_str() {
                let _ = wasi_builder.preopened_dir(dir, dir_str, DirPerms::all(), FilePerms::all());
            }
        }
    }
    for w_dir in &mounted_dirs.0 {
        if w_dir.exists() {
            if let Some(dir_str) = w_dir.to_str() {
                let _ =
                    wasi_builder.preopened_dir(w_dir, dir_str, DirPerms::all(), FilePerms::all());
            }
        }
    }
    for r_dir in &mounted_dirs.1 {
        if r_dir.exists() {
            if let Some(dir_str) = r_dir.to_str() {
                let _ = wasi_builder.preopened_dir(r_dir, dir_str, DirPerms::READ, FilePerms::READ);
            }
        }
    }
    wasi_builder.build()
}

fn resolve_against_workspace(workspace_root: &Path, raw: &str) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        workspace_root.join(p)
    }
}

fn extract_wasm_file_error_paths(text: &str, workspace_root: &Path) -> Vec<(PathBuf, bool)> {
    let mut out = Vec::new();
    let lower = text.to_ascii_lowercase();
    let is_write = lower.contains("write")
        || lower.contains("create")
        || lower.contains("replace")
        || lower.contains("modify");

    for line in text.lines() {
        let mut rest = line;
        while let Some(start) = rest.find('\'') {
            let after = &rest[start + 1..];
            if let Some(end) = after.find('\'') {
                let candidate = &after[..end];
                if candidate.starts_with('/') {
                    let p = PathBuf::from(candidate);
                    if !p.starts_with(workspace_root)
                        && !out.iter().any(|(existing, _)| existing == &p)
                    {
                        out.push((p, is_write));
                    }
                }
                rest = &after[end + 1..];
            } else {
                break;
            }
        }
    }
    out
}

fn tool_error(error: impl std::fmt::Display) -> CoreError {
    CoreError::ToolError(error.to_string())
}
