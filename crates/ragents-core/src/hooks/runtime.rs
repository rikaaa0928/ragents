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
                Ok(PermissionOutcome::Allowed) => {}
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
        // We loop as long as each retry discovers at least one NEW directory capability to request,
        // up to MAX_SANDBOX_ESCALATION_ROUNDS.
        const MAX_SANDBOX_ESCALATION_ROUNDS: usize = 16;
        let mut requested_caps: Vec<CapabilityRequest> = Vec::new();

        for _round in 0..MAX_SANDBOX_ESCALATION_ROUNDS {
            let (mut output, violations) = self
                .sandbox_state
                .execute_command_with_violations(&self.workspace_root, &command, timeout_ms)
                .await;

            if output.exit_code == 0 || violations.is_empty() {
                return output;
            }

            let Some(ref host) = self.core_host else {
                return output;
            };

            // Filter to violations that have not already been requested & unlocked in an earlier round
            // (prevents infinite loops if a path is blocked by OS SIP/chmod rather than our sandbox).
            let new_violations: Vec<_> = violations
                .into_iter()
                .filter(|v| !requested_caps.contains(&v.capability))
                .collect();

            if new_violations.is_empty() {
                return output;
            }

            let mut all_approved = true;
            for v in new_violations {
                let reason = format!(
                    "Command `{}` requires {} access to '{}' (normalized directory scope)",
                    command,
                    v.operation,
                    v.raw_path.display()
                );
                match host
                    .check_permission(Some(&self.plugin_name), v.capability.clone(), Some(reason))
                    .await
                {
                    Ok(PermissionOutcome::Allowed) => {
                        requested_caps.push(v.capability.clone());
                        match v.capability {
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
                return output;
            }
        }

        command_error("sandbox multi-directory permission escalation loop exceeded maximum rounds")
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
        Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |state| state)
            .map_err(tool_error)?;

        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.inherit_stdout().inherit_stderr();

        // Map guest "." to workspace_root so relative paths inside WASM resolve to workspace_root
        let _ = wasi_builder.preopened_dir(workspace_root, ".", DirPerms::all(), FilePerms::all());
        if let Some(ws_str) = workspace_root.to_str() {
            let _ = wasi_builder.preopened_dir(
                workspace_root,
                ws_str,
                DirPerms::all(),
                FilePerms::all(),
            );
        }
        for dir in preopen_dirs {
            if let Some(dir_str) = dir.to_str() {
                let _ = wasi_builder.preopened_dir(dir, dir_str, DirPerms::all(), FilePerms::all());
            }
        }

        let host_state = HostState {
            wasi_ctx: wasi_builder.build(),
            table: ResourceTable::new(),
            plugin_name: plugin_name.clone(),
            workspace_root: workspace_root.to_path_buf(),
            core_host,
            sandbox_state,
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
        let response = bindings
            .ragent_extension_lifecycle()
            .call_invoke(store, &request)
            .await
            .map_err(tool_error)?
            .map_err(|error| CoreError::ToolError(format!("{}: {error}", self.name)))?;
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
}

fn tool_error(error: impl std::fmt::Display) -> CoreError {
    CoreError::ToolError(error.to_string())
}
