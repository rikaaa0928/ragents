mod wasi_permissions;

use crate::error::CoreError;
use crate::hooks::protocol::{ExtensionMetadata, HookRequest};
use crate::host::{CoreHost, PermissionOutcome};
use crate::process::ProcessManager;
use crate::sandbox::{SandboxMode, SandboxState};
use ragents_protocol::{
    ActivationId, CapabilityRequest, PermissionPolicyLayer, ProcessActionRequest,
    ProcessActionResponse, SessionHistoryQueryRequest, SessionHistoryQueryResponse, SessionId,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use wasi_permissions::host_matches_pattern;
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::bindings::filesystem::{preopens as wasi_preopens, types as wasi_fs_types};
use wasmtime_wasi::{
    DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView,
};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

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
    hasher.update(b"wasmtime-37-component-v2:");
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

pub(crate) struct HostState {
    pub(crate) wasi_ctx: WasiCtx,
    pub(crate) http_ctx: WasiHttpCtx,
    pub(crate) table: ResourceTable,
    pub(crate) plugin_name: String,
    pub(crate) workspace_root: PathBuf,
    pub(crate) preopen_dirs: Vec<PathBuf>,
    pub(crate) core_host: Option<Arc<dyn CoreHost>>,
    pub(crate) sandbox_state: Arc<SandboxState>,
    pub(crate) local_process_manager: ProcessManager,
    pub(crate) dir_paths: HashMap<u32, PathBuf>,
    pub(crate) ws_fd_rep: Option<u32>,
    pub(crate) root_fd_rep: Option<u32>,
    pub(crate) invoke_approved_writes: Vec<PathBuf>,
    pub(crate) invoke_approved_reads: Vec<PathBuf>,
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
    async fn manage_process(&mut self, request_json: String) -> String {
        let req: ProcessActionRequest = match serde_json::from_str(&request_json) {
            Ok(r) => r,
            Err(err) => {
                return serde_json::to_string(&ProcessActionResponse {
                    ok: false,
                    error: Some(format!("invalid process action request: {err}")),
                    ..Default::default()
                })
                .unwrap_or_default();
            }
        };

        if let Some(ref host) = self.core_host {
            match host
                .manage_process(Some(&self.plugin_name), req.clone())
                .await
            {
                Ok(Some(resp)) => return serde_json::to_string(&resp).unwrap_or_default(),
                Ok(None) => {}
                Err(err) => {
                    return serde_json::to_string(&ProcessActionResponse {
                        ok: false,
                        error: Some(err.to_string()),
                        ..Default::default()
                    })
                    .unwrap_or_default();
                }
            }
        }

        let resp = match self
            .local_process_manager
            .handle_action(
                &SessionId::new("sess_local"),
                &ActivationId::new("act_local"),
                &self.plugin_name,
                &self.workspace_root,
                &self.sandbox_state,
                self.core_host.as_ref(),
                req,
            )
            .await
        {
            Ok(r) => r,
            Err(err) => ProcessActionResponse {
                ok: false,
                error: Some(err.to_string()),
                ..Default::default()
            },
        };

        serde_json::to_string(&resp).unwrap_or_default()
    }

    async fn query_session_history(&mut self, request_json: String) -> String {
        let req: SessionHistoryQueryRequest = match serde_json::from_str(&request_json) {
            Ok(r) => r,
            Err(err) => {
                return serde_json::to_string(&SessionHistoryQueryResponse {
                    ok: false,
                    error: Some(format!("invalid session history query request: {err}")),
                    ..Default::default()
                })
                .unwrap_or_default();
            }
        };

        if let Some(ref host) = self.core_host {
            match host.query_session_history(req).await {
                Ok(resp) => return serde_json::to_string(&resp).unwrap_or_default(),
                Err(err) => {
                    return serde_json::to_string(&SessionHistoryQueryResponse {
                        ok: false,
                        error: Some(err.to_string()),
                        ..Default::default()
                    })
                    .unwrap_or_default();
                }
            }
        }

        serde_json::to_string(&SessionHistoryQueryResponse {
            ok: false,
            error: Some("no CoreHost attached to query session history".into()),
            ..Default::default()
        })
        .unwrap_or_default()
    }

    async fn request_permission(&mut self, request_json: String) -> String {
        #[derive(serde::Deserialize)]
        struct HostPermissionRequest {
            capability: String,
            target: String,
            reason: String,
            #[serde(default)]
            method: Option<String>,
        }

        #[derive(serde::Serialize)]
        struct HostPermissionResponse {
            ok: bool,
            allowed: bool,
            #[serde(default)]
            denied: bool,
            #[serde(default)]
            persisted: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            granted_target: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            reason: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<String>,
        }

        let err_resp = |msg: String| -> String {
            serde_json::to_string(&HostPermissionResponse {
                ok: false,
                allowed: false,
                denied: false,
                persisted: false,
                granted_target: None,
                reason: None,
                error: Some(msg),
            })
            .unwrap_or_default()
        };

        let req: HostPermissionRequest = match serde_json::from_str(&request_json) {
            Ok(r) => r,
            Err(err) => return err_resp(format!("invalid permission request: {err}")),
        };

        let target_raw = req.target.trim();
        if target_raw.is_empty() {
            return err_resp("target must not be empty".to_string());
        }
        let reason_raw = req.reason.trim();
        if reason_raw.is_empty() {
            return err_resp("reason must not be empty".to_string());
        }

        let cap_kind = req.capability.trim().to_ascii_lowercase();
        let (cap, original_path) = match cap_kind.as_str() {
            "http" | "http_hosts" | "network" => {
                let (scheme, after_scheme) =
                    if let Some((s, rest)) = target_raw.split_once("://") {
                        (
                            if s.trim().is_empty() {
                                "https".to_string()
                            } else {
                                s.trim().to_ascii_lowercase()
                            },
                            rest,
                        )
                    } else {
                        ("https".to_string(), target_raw)
                    };
                let authority = after_scheme
                    .split(&['/', '?', '#'][..])
                    .next()
                    .unwrap_or("")
                    .trim();
                if authority.is_empty() {
                    return err_resp(format!("invalid HTTP host/URL target: '{target_raw}'"));
                }
                let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
                    if let Some((ipv6, after_bracket)) = rest.split_once(']') {
                        let parsed_port = after_bracket
                            .strip_prefix(':')
                            .and_then(|p| p.parse::<u16>().ok());
                        (ipv6.to_ascii_lowercase(), parsed_port)
                    } else {
                        (authority.to_ascii_lowercase(), None)
                    }
                } else if let Some((h, p_str)) = authority.rsplit_once(':') {
                    if let Ok(p_num) = p_str.parse::<u16>() {
                        (h.to_ascii_lowercase(), Some(p_num))
                    } else {
                        (authority.to_ascii_lowercase(), None)
                    }
                } else {
                    (authority.to_ascii_lowercase(), None)
                };
                if host.is_empty() {
                    return err_resp(format!("invalid HTTP host target: '{target_raw}'"));
                }
                let method = req
                    .method
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("GET")
                    .to_ascii_uppercase();
                (
                    CapabilityRequest::Http {
                        scheme,
                        host,
                        port,
                        method,
                    },
                    None,
                )
            }
            "file_read" | "file-read" | "read" | "file_write" | "file-write" | "write"
            | "command_work_dir" | "command-work-dir" | "work_dir" => {
                let expanded = if target_raw == "~" || target_raw.starts_with("~/") {
                    if let Ok(home) =
                        std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE"))
                    {
                        let tail = target_raw.strip_prefix("~/").unwrap_or("");
                        if tail.is_empty() {
                            PathBuf::from(home)
                        } else {
                            Path::new(&home).join(tail)
                        }
                    } else {
                        PathBuf::from(target_raw)
                    }
                } else {
                    PathBuf::from(target_raw)
                };
                let joined = if expanded.is_absolute() {
                    expanded
                } else {
                    self.workspace_root.join(expanded)
                };
                let normalized = wasi_permissions::normalize_lexical_path(&joined);
                let canon_path = crate::sandbox::canonicalize_or_normalize(&normalized);
                let is_write_or_cwd = matches!(
                    cap_kind.as_str(),
                    "file_write"
                        | "file-write"
                        | "write"
                        | "command_work_dir"
                        | "command-work-dir"
                        | "work_dir"
                );
                let cap_path = if is_write_or_cwd
                    && !canon_path.exists()
                    && !canon_path.parent().is_some_and(|p| p.exists())
                {
                    crate::sandbox::normalize_violation_to_request_dir(&canon_path)
                } else {
                    canon_path.clone()
                };
                let capability = match cap_kind.as_str() {
                    "file_read" | "file-read" | "read" => {
                        CapabilityRequest::FileRead { path: cap_path }
                    }
                    "file_write" | "file-write" | "write" => {
                        CapabilityRequest::FileWrite { path: cap_path }
                    }
                    _ => CapabilityRequest::CommandWorkDir { work_dir: cap_path },
                };
                (capability, Some(canon_path))
            }
            other => {
                return err_resp(format!(
                    "unsupported capability '{other}'; expected one of: file_read, file_write, command_work_dir, http"
                ));
            }
        };

        let Some(ref host) = self.core_host else {
            let already_allowed = match &cap {
                CapabilityRequest::FileRead { path } => self.sandbox_state.is_path_readable(path),
                CapabilityRequest::FileWrite { path }
                | CapabilityRequest::CommandWorkDir { work_dir: path } => {
                    self.sandbox_state.is_path_writable(path)
                }
                CapabilityRequest::Http { .. } => false,
            };
            if already_allowed {
                return serde_json::to_string(&HostPermissionResponse {
                    ok: true,
                    allowed: true,
                    denied: false,
                    persisted: true,
                    granted_target: cap.path().map(|p| p.display().to_string()),
                    reason: None,
                    error: None,
                })
                .unwrap_or_default();
            }
            return err_resp("no CoreHost attached to request permission".to_string());
        };

        match host
            .check_permission_with_original_path(
                Some(&self.plugin_name),
                cap.clone(),
                original_path,
                Some(reason_raw.to_string()),
            )
            .await
        {
            Ok(PermissionOutcome::Allowed {
                effective_capability,
                persist_in_activation,
            }) => {
                let granted = effective_capability.unwrap_or(cap);
                if persist_in_activation {
                    self.sandbox_state.apply_capability_grant(&granted);
                }
                serde_json::to_string(&HostPermissionResponse {
                    ok: true,
                    allowed: true,
                    denied: false,
                    persisted: persist_in_activation,
                    granted_target: granted.path().map(|p| p.display().to_string()),
                    reason: None,
                    error: None,
                })
                .unwrap_or_default()
            }
            Ok(PermissionOutcome::Denied { reason }) => {
                serde_json::to_string(&HostPermissionResponse {
                    ok: true,
                    allowed: false,
                    denied: true,
                    persisted: false,
                    granted_target: None,
                    reason: Some(reason),
                    error: None,
                })
                .unwrap_or_default()
            }
            Err(err) => err_resp(err.to_string()),
        }
    }
}

const MAX_POOLED_RUNTIMES: usize = 10;

struct PluginRuntime {
    store: Store<HostState>,
    bindings: Plugin,
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
    local_process_manager: ProcessManager,
    init_config: Mutex<Option<String>>,
    pool: Mutex<Vec<PluginRuntime>>,
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
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker).map_err(tool_error)?;

        linker.allow_shadowing(true);
        wasi_preopens::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        wasi_fs_types::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        linker.allow_shadowing(false);

        Plugin::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state).map_err(tool_error)?;

        let mut plugin = Self {
            name: plugin_name,
            metadata: ExtensionMetadata {
                id: String::new(),
                version: String::new(),
                subscriptions: Vec::new(),
                filter_env: Vec::new(),
            },
            component,
            linker: Arc::new(linker),
            workspace_root: workspace_root.to_path_buf(),
            preopen_dirs: preopen_dirs.to_vec(),
            core_host,
            sandbox_state,
            local_process_manager: ProcessManager::new(None),
            init_config: Mutex::new(None),
            pool: Mutex::new(Vec::new()),
        };

        let mut runtime = plugin.instantiate_runtime().await?;
        let metadata_json = runtime
            .bindings
            .ragent_extension_lifecycle()
            .call_metadata(&mut runtime.store)
            .await
            .map_err(tool_error)?;
        let metadata: ExtensionMetadata =
            serde_json::from_str(&metadata_json).map_err(|error| {
                CoreError::ToolError(format!("extension metadata is invalid: {error}"))
            })?;

        plugin.metadata = metadata;
        plugin.pool.lock().await.push(runtime);
        Ok(plugin)
    }

    fn build_host_state(&self) -> HostState {
        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.inherit_stdout().inherit_stderr();

        let _ = wasi_builder.preopened_dir(
            &self.workspace_root,
            ".",
            DirPerms::all(),
            FilePerms::all(),
        );
        if let Some(ws_str) = self.workspace_root.to_str() {
            wasi_builder.env("PWD", ws_str);
            let _ = wasi_builder.preopened_dir(
                &self.workspace_root,
                ws_str,
                DirPerms::all(),
                FilePerms::all(),
            );
        }

        if let Ok(home_str) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
            wasi_builder.env("HOME", &home_str);
        }

        for dir in &self.preopen_dirs {
            if dir.exists() {
                if let Some(dir_str) = dir.to_str() {
                    let _ =
                        wasi_builder.preopened_dir(dir, dir_str, DirPerms::all(), FilePerms::all());
                }
                if let Ok(canon) = dir.canonicalize() {
                    if canon != *dir {
                        if let Some(canon_str) = canon.to_str() {
                            let _ = wasi_builder.preopened_dir(
                                &canon,
                                canon_str,
                                DirPerms::all(),
                                FilePerms::all(),
                            );
                        }
                    }
                }
            }
        }
        #[cfg(unix)]
        if self.core_host.is_some() {
            let _ = wasi_builder.preopened_dir("/", "/", DirPerms::all(), FilePerms::all());
        }

        wasi_builder.allow_ip_name_lookup(true);
        let core_host_for_sock = self.core_host.clone();
        let plugin_for_sock = self.name.clone();
        wasi_builder.socket_addr_check(move |addr, addr_use| {
            let core_host = core_host_for_sock.clone();
            let plugin_name = plugin_for_sock.clone();
            Box::pin(async move {
                let Some(host_cb) = core_host else {
                    return true;
                };
                tokio::spawn(async move {
                    let scheme = match addr_use {
                        wasmtime_wasi::sockets::SocketAddrUse::TcpBind
                        | wasmtime_wasi::sockets::SocketAddrUse::TcpConnect => "tcp",
                        _ => "udp",
                    }
                    .to_string();
                    let host_str = addr.ip().to_string();
                    let port = Some(addr.port());
                    let method_str = format!("{addr_use:?}");
                    let orig = format!("{scheme}://{addr}");
                    let cap = CapabilityRequest::Http {
                        scheme,
                        host: host_str.clone(),
                        port,
                        method: method_str.clone(),
                    };
                    let reason = format!(
                        "WASI 0.2 Socket (`wasi:sockets`) from extension '{plugin_name}': {method_str} {addr}"
                    );
                    match host_cb
                        .check_permission_with_original_path(
                            Some(&plugin_name),
                            cap,
                            Some(PathBuf::from(orig)),
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
                                host_matches_pattern(approved_host, &host_str)
                            } else {
                                true
                            }
                        }
                        _ => false,
                    }
                })
                .await
                .unwrap_or(false)
            })
        });

        HostState {
            wasi_ctx: wasi_builder.build(),
            http_ctx: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            plugin_name: self.name.clone(),
            workspace_root: self.workspace_root.clone(),
            preopen_dirs: self.preopen_dirs.clone(),
            core_host: self.core_host.clone(),
            sandbox_state: self.sandbox_state.clone(),
            local_process_manager: self.local_process_manager.clone(),
            dir_paths: HashMap::new(),
            ws_fd_rep: None,
            root_fd_rep: None,
            invoke_approved_writes: Vec::new(),
            invoke_approved_reads: Vec::new(),
        }
    }

    async fn instantiate_runtime(&self) -> Result<PluginRuntime, CoreError> {
        let engine = global_engine()?;
        let host_state = self.build_host_state();
        let mut store = Store::new(engine, host_state);
        let bindings = Plugin::instantiate_async(&mut store, &self.component, &self.linker)
            .await
            .map_err(tool_error)?;
        Ok(PluginRuntime { store, bindings })
    }

    async fn acquire_runtime(&self) -> Result<PluginRuntime, CoreError> {
        if let Some(rt) = self.pool.lock().await.pop() {
            return Ok(rt);
        }
        let mut rt = self.instantiate_runtime().await?;
        let maybe_cfg = self.init_config.lock().await.clone();
        if let Some(cfg) = maybe_cfg {
            rt.bindings
                .ragent_extension_lifecycle()
                .call_initialize(&mut rt.store, &cfg)
                .await
                .map_err(tool_error)?
                .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?;
        }
        Ok(rt)
    }

    async fn release_runtime(&self, mut rt: PluginRuntime) {
        rt.store.data_mut().invoke_approved_writes.clear();
        rt.store.data_mut().invoke_approved_reads.clear();
        let mut pool = self.pool.lock().await;
        if pool.len() < MAX_POOLED_RUNTIMES {
            pool.push(rt);
        }
    }

    pub fn metadata(&self) -> &ExtensionMetadata {
        &self.metadata
    }

    pub async fn initialize(&self, config: &serde_json::Value) -> Result<(), CoreError> {
        let config = serde_json::to_string(config)?;
        *self.init_config.lock().await = Some(config.clone());
        let mut runtimes: Vec<PluginRuntime> = {
            let mut pool = self.pool.lock().await;
            pool.drain(..).collect()
        };
        if runtimes.is_empty() {
            runtimes.push(self.instantiate_runtime().await?);
        }
        let mut res = Ok(());
        for rt in &mut runtimes {
            match rt
                .bindings
                .ragent_extension_lifecycle()
                .call_initialize(&mut rt.store, &config)
                .await
                .map_err(tool_error)
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    res = Err(CoreError::ToolError(format!("{}: {error}", self.name)));
                    break;
                }
                Err(err) => {
                    res = Err(err);
                    break;
                }
            }
        }
        let mut pool = self.pool.lock().await;
        for rt in runtimes {
            if pool.len() < MAX_POOLED_RUNTIMES {
                pool.push(rt);
            }
        }
        res
    }

    pub async fn invoke(&self, request: &HookRequest) -> Result<serde_json::Value, CoreError> {
        let request = serde_json::to_string(request)?;
        let mut runtime = self.acquire_runtime().await?;
        runtime.store.data_mut().invoke_approved_writes.clear();
        runtime.store.data_mut().invoke_approved_reads.clear();
        let call_res = runtime
            .bindings
            .ragent_extension_lifecycle()
            .call_invoke(&mut runtime.store, &request)
            .await;
        match call_res {
            Ok(Ok(response)) => {
                self.release_runtime(runtime).await;
                serde_json::from_str(&response).map_err(CoreError::Json)
            }
            Ok(Err(error)) => {
                self.release_runtime(runtime).await;
                Err(CoreError::ToolError(format!("{}: {error}", self.name)))
            }
            Err(trap) => Err(tool_error(trap)),
        }
    }

    pub async fn shutdown(&self) -> Result<(), CoreError> {
        let runtimes: Vec<PluginRuntime> = {
            let mut pool = self.pool.lock().await;
            pool.drain(..).collect()
        };
        let mut first_error = None;
        for mut runtime in runtimes {
            if let Err(err) = runtime
                .bindings
                .ragent_extension_lifecycle()
                .call_shutdown(&mut runtime.store)
                .await
                .map_err(tool_error)
            {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn invoke_wasi_http_for_test(
        &self,
        method: &str,
        url: &str,
    ) -> Result<(u16, String), CoreError> {
        use http_body_util::{BodyExt, Empty};
        let mut runtime = self.acquire_runtime().await?;
        let parsed: hyper::Uri = url.parse().map_err(tool_error)?;
        let use_tls = parsed.scheme_str() == Some("https");
        let empty_body = Empty::<bytes::Bytes>::new()
            .map_err(|never| match never {})
            .boxed();
        let req = hyper::Request::builder()
            .method(method)
            .uri(parsed)
            .body(empty_body)
            .map_err(tool_error)?;
        let cfg = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls,
            connect_timeout: std::time::Duration::from_secs(10),
            first_byte_timeout: std::time::Duration::from_secs(10),
            between_bytes_timeout: std::time::Duration::from_secs(10),
        };
        let mut fut_resp = WasiHttpView::send_request(runtime.store.data_mut(), req, cfg)
            .map_err(|e| CoreError::ToolError(e.to_string()))?;
        wasmtime_wasi::p2::Pollable::ready(&mut fut_resp).await;
        let incoming = fut_resp
            .unwrap_ready()
            .map_err(tool_error)?
            .map_err(|code| CoreError::ToolError(format!("WASI HTTP error: {code:?}")))?;
        let status = incoming.resp.status().as_u16();
        let body_bytes = incoming
            .resp
            .into_body()
            .collect()
            .await
            .map_err(tool_error)?
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes).into_owned();
        self.release_runtime(runtime).await;
        Ok((status, body_str))
    }
}

fn tool_error(error: impl std::fmt::Display) -> CoreError {
    CoreError::ToolError(error.to_string())
}
