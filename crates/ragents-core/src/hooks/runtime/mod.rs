mod wasi_permissions;

use crate::error::CoreError;
use crate::hooks::protocol::{ExtensionMetadata, HookRequest};
use crate::host::{CoreHost, PermissionOutcome};
use crate::process::ProcessManager;
use crate::sandbox::{SandboxMode, SandboxState};
use ragents_protocol::{
    ActivationId, CapabilityRequest, PermissionPolicyLayer, ProcessActionRequest,
    ProcessActionResponse, SessionId,
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
}

struct PluginRuntime {
    store: Store<HostState>,
    bindings: Plugin,
}

pub struct WasmPlugin {
    pub name: String,
    metadata: ExtensionMetadata,
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
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker).map_err(tool_error)?;

        linker.allow_shadowing(true);
        wasi_preopens::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        wasi_fs_types::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        linker.allow_shadowing(false);

        Plugin::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state).map_err(tool_error)?;

        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.inherit_stdout().inherit_stderr();

        let _ = wasi_builder.preopened_dir(workspace_root, ".", DirPerms::all(), FilePerms::all());
        if let Some(ws_str) = workspace_root.to_str() {
            wasi_builder.env("PWD", ws_str);
            let _ = wasi_builder.preopened_dir(
                workspace_root,
                ws_str,
                DirPerms::all(),
                FilePerms::all(),
            );
        }

        let effective_preopen_dirs = preopen_dirs.to_vec();
        if let Ok(home_str) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
            wasi_builder.env("HOME", &home_str);
        }

        for dir in &effective_preopen_dirs {
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
        if core_host.is_some() {
            let _ = wasi_builder.preopened_dir("/", "/", DirPerms::all(), FilePerms::all());
        }

        wasi_builder.allow_ip_name_lookup(true);
        let core_host_for_sock = core_host.clone();
        let plugin_for_sock = plugin_name.clone();
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

        let host_state = HostState {
            wasi_ctx: wasi_builder.build(),
            http_ctx: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            plugin_name: plugin_name.clone(),
            workspace_root: workspace_root.to_path_buf(),
            preopen_dirs: effective_preopen_dirs,
            core_host,
            sandbox_state,
            local_process_manager: ProcessManager::new(None),
            dir_paths: HashMap::new(),
            ws_fd_rep: None,
            root_fd_rep: None,
            invoke_approved_writes: Vec::new(),
            invoke_approved_reads: Vec::new(),
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
            runtime: Mutex::new(PluginRuntime { store, bindings }),
        })
    }

    pub fn metadata(&self) -> &ExtensionMetadata {
        &self.metadata
    }

    pub async fn initialize(&self, config: &serde_json::Value) -> Result<(), CoreError> {
        let config = serde_json::to_string(config)?;
        let mut runtime = self.runtime.lock().await;
        let PluginRuntime { store, bindings } = &mut *runtime;
        bindings
            .ragent_extension_lifecycle()
            .call_initialize(store, &config)
            .await
            .map_err(tool_error)?
            .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))
    }

    pub async fn invoke(&self, request: &HookRequest) -> Result<serde_json::Value, CoreError> {
        let request = serde_json::to_string(request)?;
        let mut runtime = self.runtime.lock().await;
        let PluginRuntime { store, bindings } = &mut *runtime;
        store.data_mut().invoke_approved_writes.clear();
        store.data_mut().invoke_approved_reads.clear();
        let response = bindings
            .ragent_extension_lifecycle()
            .call_invoke(&mut *store, &request)
            .await
            .map_err(tool_error)?
            .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?;
        store.data_mut().invoke_approved_writes.clear();
        store.data_mut().invoke_approved_reads.clear();
        serde_json::from_str(&response).map_err(CoreError::Json)
    }

    pub async fn shutdown(&self) -> Result<(), CoreError> {
        let mut runtime = self.runtime.lock().await;
        let PluginRuntime { store, bindings } = &mut *runtime;
        bindings
            .ragent_extension_lifecycle()
            .call_shutdown(store)
            .await
            .map_err(tool_error)
    }

    pub async fn invoke_wasi_http_for_test(
        &self,
        method: &str,
        url: &str,
    ) -> Result<(u16, String), CoreError> {
        use http_body_util::{BodyExt, Empty};
        let mut runtime = self.runtime.lock().await;
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
        Ok((status, body_str))
    }
}

fn tool_error(error: impl std::fmt::Display) -> CoreError {
    CoreError::ToolError(error.to_string())
}
