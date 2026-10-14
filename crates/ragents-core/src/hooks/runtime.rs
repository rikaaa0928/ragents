use crate::error::CoreError;
use crate::hooks::protocol::{ExtensionMetadata, HookRequest};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::Mutex;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{
    DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView,
};

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "plugin",
    imports: { default: async },
    exports: { default: async },
});

struct HostState {
    wasi_ctx: WasiCtx,
    table: ResourceTable,
    workspace_root: PathBuf,
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
        run_command_in_dir(&self.workspace_root, command, timeout_ms).await
    }
}

async fn run_command_in_dir(
    workspace_root: &Path,
    command: String,
    timeout_ms: u64,
) -> ragent::extension::host::CommandOutput {
    let mut child = tokio::process::Command::new("sh");
    child
        .current_dir(workspace_root)
        .arg("-c")
        .arg(command)
        .kill_on_drop(true);
    let output = if timeout_ms == 0 {
        child.output().await
    } else {
        match tokio::time::timeout(Duration::from_millis(timeout_ms), child.output()).await {
            Ok(result) => result,
            Err(_) => return command_error(format!("command timed out after {timeout_ms} ms")),
        }
    };
    match output {
        Ok(output) => ragent::extension::host::CommandOutput {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            error: None,
        },
        Err(error) => command_error(error),
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
        let mut config = Config::new();
        config.async_support(true);
        let engine = Engine::new(&config).map_err(tool_error)?;
        let component = Component::from_file(&engine, path).map_err(|error| {
            CoreError::ToolError(format!(
                "failed to load WebAssembly component from {path:?}: {error}"
            ))
        })?;

        let mut linker = Linker::new(&engine);
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
            workspace_root: workspace_root.to_path_buf(),
        };

        let mut store = Store::new(&engine, host_state);
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
            name: name.into(),
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
