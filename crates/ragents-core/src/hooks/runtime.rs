use crate::error::CoreError;
use crate::hooks::protocol::{ExtensionMetadata, HookRequest};
use crate::host::{CoreHost, PermissionOutcome};
use crate::sandbox::{SandboxMode, SandboxState};
use ragents_protocol::{CapabilityRequest, PermissionPolicyLayer};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Component as PathComponent, Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use wasmtime::component::{Component, HasSelf, Linker, Resource};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::filesystem::{WasiFilesystemCtxView, WasiFilesystemView as _};
use wasmtime_wasi::p2::bindings::filesystem::{preopens as wasi_preopens, types as wasi_fs_types};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, FsError, FsResult};
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

struct HostState {
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    table: ResourceTable,
    plugin_name: String,
    workspace_root: PathBuf,
    preopen_dirs: Vec<PathBuf>,
    core_host: Option<Arc<dyn CoreHost>>,
    sandbox_state: Arc<SandboxState>,
    /// Tracks the resolved host `PathBuf` for each WASI 0.2 `wasi:filesystem/types.descriptor` resource rep.
    dir_paths: HashMap<u32, PathBuf>,
    ws_fd_rep: Option<u32>,
    root_fd_rep: Option<u32>,
    /// Tracks paths approved during the current `invoke` call so multi-syscall operations
    /// (`create_directory_at` followed by `open_at`) don't double-prompt under `AllowOnce`.
    invoke_approved_writes: Vec<PathBuf>,
    invoke_approved_reads: Vec<PathBuf>,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}

// ============================================================================
// 1. WASI 0.2 Built-in HTTP Permission Management (`wasi:http/outgoing-handler`)
// ============================================================================

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn send_request(
        &mut self,
        request: hyper::Request<wasmtime_wasi_http::body::HyperOutgoingBody>,
        config: wasmtime_wasi_http::types::OutgoingRequestConfig,
    ) -> wasmtime_wasi_http::HttpResult<wasmtime_wasi_http::types::HostFutureIncomingResponse> {
        let core_host = self.core_host.clone();
        let plugin_name = self.plugin_name.clone();
        let scheme = if config.use_tls { "https" } else { "http" }.to_string();
        let host_str = request
            .uri()
            .host()
            .unwrap_or("unknown")
            .to_ascii_lowercase();
        let port = request
            .uri()
            .port_u16()
            .or(Some(if config.use_tls { 443 } else { 80 }));
        let method_str = request.method().as_str().to_ascii_uppercase();
        let full_url = request.uri().to_string();

        let handle = wasmtime_wasi::runtime::spawn(async move {
            if let Some(host_cb) = core_host {
                let cap = CapabilityRequest::Http {
                    scheme,
                    host: host_str.clone(),
                    port,
                    method: method_str.clone(),
                };
                let reason = format!(
                    "WASI 0.2 HTTP (`wasi:http/outgoing-handler`) from extension '{plugin_name}': {method_str} {full_url}"
                );
                match host_cb
                    .check_permission_with_original_path(
                        Some(&plugin_name),
                        cap,
                        Some(PathBuf::from(&full_url)),
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
                                return Ok(Err(
                                    wasmtime_wasi_http::bindings::http::types::ErrorCode::HttpRequestDenied,
                                ));
                            }
                        }
                    }
                    Ok(PermissionOutcome::Denied { .. }) | Err(_) => {
                        return Ok(Err(
                            wasmtime_wasi_http::bindings::http::types::ErrorCode::HttpRequestDenied,
                        ));
                    }
                }
            }
            Ok(wasmtime_wasi_http::types::default_send_request_handler(request, config).await)
        });
        Ok(wasmtime_wasi_http::types::HostFutureIncomingResponse::pending(handle))
    }
}

// ============================================================================
// 2. WASI 0.2 Built-in Filesystem Permission Management (`wasi:filesystem`)
// ============================================================================

impl HostState {
    fn resolve_fd_path(&self, fd: &Resource<wasi_fs_types::Descriptor>, rel: &str) -> PathBuf {
        let base = self
            .dir_paths
            .get(&fd.rep())
            .cloned()
            .unwrap_or_else(|| self.workspace_root.clone());
        let joined = if rel.is_empty() || rel == "." {
            base
        } else {
            base.join(rel.trim_start_matches('/'))
        };
        normalize_lexical_path(&joined)
    }

    fn remap_fd_and_resolve_path(
        &self,
        fd: Resource<wasi_fs_types::Descriptor>,
        rel: &str,
    ) -> (Resource<wasi_fs_types::Descriptor>, PathBuf) {
        if Some(fd.rep()) == self.root_fd_rep {
            let trimmed = rel.trim_start_matches('/');
            let first_seg = trimmed.split('/').next().unwrap_or("");
            let is_system_root_dir =
                !first_seg.is_empty() && Path::new("/").join(first_seg).is_dir();
            if !is_system_root_dir {
                if let Some(ws_rep) = self.ws_fd_rep {
                    let remapped_fd = Resource::new_borrow(ws_rep);
                    let full = normalize_lexical_path(&self.workspace_root.join(trimmed));
                    return (remapped_fd, full);
                }
            }
        }
        let full = self.resolve_fd_path(&fd, rel);
        (fd, full)
    }

    async fn check_wasi_fs_permission(
        &mut self,
        full_path: &Path,
        is_write: bool,
        op_name: &str,
    ) -> FsResult<()> {
        let norm_ws = crate::sandbox::canonicalize_or_normalize(&self.workspace_root);
        let norm_target = crate::sandbox::canonicalize_or_normalize(full_path);
        let in_workspace =
            full_path.starts_with(&self.workspace_root) || norm_target.starts_with(&norm_ws);

        // When `core_host` is None (control-plane WASM plugins in `api-server` or `agent-daemon`),
        // there is no interactive approval channel: strictly restrict filesystem access to
        // `workspace_root` (the plugin's private work_dir) and explicitly configured `preopen_dirs` (`allowed_dirs`).
        let Some(ref host) = self.core_host else {
            if in_workspace {
                return Ok(());
            }
            let in_allowed = self.preopen_dirs.iter().any(|dir| {
                let norm_dir = crate::sandbox::canonicalize_or_normalize(dir);
                full_path.starts_with(dir) || norm_target.starts_with(&norm_dir)
            });
            if in_allowed {
                return Ok(());
            }
            return Err(wasi_fs_types::ErrorCode::NotPermitted.into());
        };

        // If already approved earlier within this same `invoke` call (e.g. `create_directory_at` right before `open_at`),
        // skip duplicate permission check.
        if is_write {
            if self
                .invoke_approved_writes
                .iter()
                .any(|p| norm_target.starts_with(p) || full_path.starts_with(p))
            {
                return Ok(());
            }
        } else if self
            .invoke_approved_reads
            .iter()
            .any(|p| norm_target.starts_with(p) || full_path.starts_with(p))
        {
            return Ok(());
        }

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
            "WASI 0.2 filesystem (`wasi:filesystem/{op_name}`) from extension '{}' requires {} access to '{}' (suggested path: '{}')",
            self.plugin_name,
            if is_write { "file-write" } else { "file-read" },
            full_path.display(),
            suggested_path.display()
        );

        match host
            .check_permission_with_original_path(
                Some(&self.plugin_name),
                cap.clone(),
                Some(full_path.to_path_buf()),
                Some(reason),
            )
            .await
        {
            Ok(PermissionOutcome::Allowed {
                effective_capability,
                ..
            }) => {
                let granted_cap = effective_capability.unwrap_or(cap);
                let approved_path = granted_cap
                    .path()
                    .map(|p| p.to_path_buf())
                    .unwrap_or(suggested_path);
                let norm_approved = crate::sandbox::canonicalize_or_normalize(&approved_path);
                if !norm_target.starts_with(&norm_approved)
                    && !full_path.starts_with(&approved_path)
                {
                    return Err(wasi_fs_types::ErrorCode::NotPermitted.into());
                }
                if is_write {
                    self.invoke_approved_writes.push(norm_approved);
                    self.invoke_approved_writes.push(approved_path);
                } else {
                    self.invoke_approved_reads.push(norm_approved);
                    self.invoke_approved_reads.push(approved_path);
                }
                Ok(())
            }
            Ok(PermissionOutcome::Denied { .. }) | Err(_) => {
                Err(wasi_fs_types::ErrorCode::NotPermitted.into())
            }
        }
    }
}

impl wasi_preopens::Host for HostState {
    fn get_directories(
        &mut self,
    ) -> wasmtime::Result<Vec<(Resource<wasi_fs_types::Descriptor>, String)>> {
        let mut fs_view: WasiFilesystemCtxView<'_> = self.filesystem();
        let dirs = wasi_preopens::Host::get_directories(&mut fs_view)?;
        let ws = self.workspace_root.clone();
        for (res, guest_path) in &dirs {
            let host_path = if guest_path == "." {
                self.ws_fd_rep = Some(res.rep());
                ws.clone()
            } else {
                if guest_path == "/" {
                    self.root_fd_rep = Some(res.rep());
                }
                PathBuf::from(guest_path)
            };
            self.dir_paths.insert(res.rep(), host_path);
        }
        Ok(dirs)
    }
}

impl wasi_fs_types::Host for HostState {
    fn convert_error_code(&mut self, err: FsError) -> anyhow::Result<wasi_fs_types::ErrorCode> {
        wasi_fs_types::Host::convert_error_code(&mut self.filesystem(), err)
    }

    fn filesystem_error_code(
        &mut self,
        err: Resource<anyhow::Error>,
    ) -> anyhow::Result<Option<wasi_fs_types::ErrorCode>> {
        wasi_fs_types::Host::filesystem_error_code(&mut self.filesystem(), err)
    }
}

impl wasi_fs_types::HostDirectoryEntryStream for HostState {
    async fn read_directory_entry(
        &mut self,
        stream: Resource<wasi_fs_types::DirectoryEntryStream>,
    ) -> FsResult<Option<wasi_fs_types::DirectoryEntry>> {
        wasi_fs_types::HostDirectoryEntryStream::read_directory_entry(
            &mut self.filesystem(),
            stream,
        )
        .await
    }

    fn drop(
        &mut self,
        stream: Resource<wasi_fs_types::DirectoryEntryStream>,
    ) -> anyhow::Result<()> {
        wasi_fs_types::HostDirectoryEntryStream::drop(&mut self.filesystem(), stream)
    }
}

impl wasi_fs_types::HostDescriptor for HostState {
    async fn advise(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        offset: wasi_fs_types::Filesize,
        len: wasi_fs_types::Filesize,
        advice: wasi_fs_types::Advice,
    ) -> FsResult<()> {
        wasi_fs_types::HostDescriptor::advise(&mut self.filesystem(), fd, offset, len, advice).await
    }

    async fn sync_data(&mut self, fd: Resource<wasi_fs_types::Descriptor>) -> FsResult<()> {
        wasi_fs_types::HostDescriptor::sync_data(&mut self.filesystem(), fd).await
    }

    async fn get_flags(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<wasi_fs_types::DescriptorFlags> {
        wasi_fs_types::HostDescriptor::get_flags(&mut self.filesystem(), fd).await
    }

    async fn get_type(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<wasi_fs_types::DescriptorType> {
        wasi_fs_types::HostDescriptor::get_type(&mut self.filesystem(), fd).await
    }

    async fn set_size(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        size: wasi_fs_types::Filesize,
    ) -> FsResult<()> {
        wasi_fs_types::HostDescriptor::set_size(&mut self.filesystem(), fd, size).await
    }

    async fn set_times(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        atim: wasi_fs_types::NewTimestamp,
        mtim: wasi_fs_types::NewTimestamp,
    ) -> FsResult<()> {
        wasi_fs_types::HostDescriptor::set_times(&mut self.filesystem(), fd, atim, mtim).await
    }

    async fn read(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        len: wasi_fs_types::Filesize,
        offset: wasi_fs_types::Filesize,
    ) -> FsResult<(Vec<u8>, bool)> {
        wasi_fs_types::HostDescriptor::read(&mut self.filesystem(), fd, len, offset).await
    }

    async fn write(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        buf: Vec<u8>,
        offset: wasi_fs_types::Filesize,
    ) -> FsResult<wasi_fs_types::Filesize> {
        wasi_fs_types::HostDescriptor::write(&mut self.filesystem(), fd, buf, offset).await
    }

    async fn read_directory(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<Resource<wasi_fs_types::DirectoryEntryStream>> {
        if let Some(dir_path) = self.dir_paths.get(&fd.rep()).cloned() {
            self.check_wasi_fs_permission(&dir_path, false, "read-directory")
                .await?;
        }
        wasi_fs_types::HostDescriptor::read_directory(&mut self.filesystem(), fd).await
    }

    async fn sync(&mut self, fd: Resource<wasi_fs_types::Descriptor>) -> FsResult<()> {
        wasi_fs_types::HostDescriptor::sync(&mut self.filesystem(), fd).await
    }

    async fn create_directory_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        // Only check write permission if the directory does not already exist on disk
        if !full_path.exists() {
            self.check_wasi_fs_permission(&full_path, true, "create-directory-at")
                .await?;
        }
        wasi_fs_types::HostDescriptor::create_directory_at(&mut self.filesystem(), fd, path).await
    }

    async fn stat(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<wasi_fs_types::DescriptorStat> {
        wasi_fs_types::HostDescriptor::stat(&mut self.filesystem(), fd).await
    }

    async fn stat_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path_flags: wasi_fs_types::PathFlags,
        path: String,
    ) -> FsResult<wasi_fs_types::DescriptorStat> {
        let (fd, _full_path) = self.remap_fd_and_resolve_path(fd, &path);
        wasi_fs_types::HostDescriptor::stat_at(&mut self.filesystem(), fd, path_flags, path).await
    }

    async fn set_times_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path_flags: wasi_fs_types::PathFlags,
        path: String,
        atim: wasi_fs_types::NewTimestamp,
        mtim: wasi_fs_types::NewTimestamp,
    ) -> FsResult<()> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        self.check_wasi_fs_permission(&full_path, true, "set-times-at")
            .await?;
        wasi_fs_types::HostDescriptor::set_times_at(
            &mut self.filesystem(),
            fd,
            path_flags,
            path,
            atim,
            mtim,
        )
        .await
    }

    async fn link_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        old_path_flags: wasi_fs_types::PathFlags,
        old_path: String,
        new_descriptor: Resource<wasi_fs_types::Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let (fd, _full_old) = self.remap_fd_and_resolve_path(fd, &old_path);
        let (new_descriptor, full_new) = self.remap_fd_and_resolve_path(new_descriptor, &new_path);
        self.check_wasi_fs_permission(&full_new, true, "link-at")
            .await?;
        wasi_fs_types::HostDescriptor::link_at(
            &mut self.filesystem(),
            fd,
            old_path_flags,
            old_path,
            new_descriptor,
            new_path,
        )
        .await
    }

    async fn open_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path_flags: wasi_fs_types::PathFlags,
        path: String,
        oflags: wasi_fs_types::OpenFlags,
        flags: wasi_fs_types::DescriptorFlags,
    ) -> FsResult<Resource<wasi_fs_types::Descriptor>> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        let is_write = oflags
            .intersects(wasi_fs_types::OpenFlags::CREATE | wasi_fs_types::OpenFlags::TRUNCATE)
            || flags.intersects(
                wasi_fs_types::DescriptorFlags::WRITE
                    | wasi_fs_types::DescriptorFlags::MUTATE_DIRECTORY,
            );
        let is_dir_only = oflags.contains(wasi_fs_types::OpenFlags::DIRECTORY) && !is_write;

        if !is_dir_only {
            self.check_wasi_fs_permission(&full_path, is_write, "open-at")
                .await?;
        }

        let child_fd = wasi_fs_types::HostDescriptor::open_at(
            &mut self.filesystem(),
            fd,
            path_flags,
            path,
            oflags,
            flags,
        )
        .await?;
        self.dir_paths.insert(child_fd.rep(), full_path);
        Ok(child_fd)
    }

    fn drop(&mut self, fd: Resource<wasi_fs_types::Descriptor>) -> anyhow::Result<()> {
        self.dir_paths.remove(&fd.rep());
        wasi_fs_types::HostDescriptor::drop(&mut self.filesystem(), fd)
    }

    async fn readlink_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path: String,
    ) -> FsResult<String> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        self.check_wasi_fs_permission(&full_path, false, "readlink-at")
            .await?;
        wasi_fs_types::HostDescriptor::readlink_at(&mut self.filesystem(), fd, path).await
    }

    async fn remove_directory_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        self.check_wasi_fs_permission(&full_path, true, "remove-directory-at")
            .await?;
        wasi_fs_types::HostDescriptor::remove_directory_at(&mut self.filesystem(), fd, path).await
    }

    async fn rename_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        old_path: String,
        new_fd: Resource<wasi_fs_types::Descriptor>,
        new_path: String,
    ) -> FsResult<()> {
        let (fd, full_old) = self.remap_fd_and_resolve_path(fd, &old_path);
        let (new_fd, full_new) = self.remap_fd_and_resolve_path(new_fd, &new_path);
        self.check_wasi_fs_permission(&full_old, true, "rename-at")
            .await?;
        self.check_wasi_fs_permission(&full_new, true, "rename-at")
            .await?;
        wasi_fs_types::HostDescriptor::rename_at(
            &mut self.filesystem(),
            fd,
            old_path,
            new_fd,
            new_path,
        )
        .await
    }

    async fn symlink_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        src_path: String,
        dest_path: String,
    ) -> FsResult<()> {
        let (fd, full_dest) = self.remap_fd_and_resolve_path(fd, &dest_path);
        self.check_wasi_fs_permission(&full_dest, true, "symlink-at")
            .await?;
        wasi_fs_types::HostDescriptor::symlink_at(&mut self.filesystem(), fd, src_path, dest_path)
            .await
    }

    async fn unlink_file_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path: String,
    ) -> FsResult<()> {
        let (fd, full_path) = self.remap_fd_and_resolve_path(fd, &path);
        self.check_wasi_fs_permission(&full_path, true, "unlink-file-at")
            .await?;
        wasi_fs_types::HostDescriptor::unlink_file_at(&mut self.filesystem(), fd, path).await
    }

    fn read_via_stream(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        offset: wasi_fs_types::Filesize,
    ) -> FsResult<Resource<DynInputStream>> {
        wasi_fs_types::HostDescriptor::read_via_stream(&mut self.filesystem(), fd, offset)
    }

    fn write_via_stream(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        offset: wasi_fs_types::Filesize,
    ) -> FsResult<Resource<DynOutputStream>> {
        wasi_fs_types::HostDescriptor::write_via_stream(&mut self.filesystem(), fd, offset)
    }

    fn append_via_stream(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<Resource<DynOutputStream>> {
        wasi_fs_types::HostDescriptor::append_via_stream(&mut self.filesystem(), fd)
    }

    async fn is_same_object(
        &mut self,
        a: Resource<wasi_fs_types::Descriptor>,
        b: Resource<wasi_fs_types::Descriptor>,
    ) -> anyhow::Result<bool> {
        wasi_fs_types::HostDescriptor::is_same_object(&mut self.filesystem(), a, b).await
    }

    async fn metadata_hash(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
    ) -> FsResult<wasi_fs_types::MetadataHashValue> {
        wasi_fs_types::HostDescriptor::metadata_hash(&mut self.filesystem(), fd).await
    }

    async fn metadata_hash_at(
        &mut self,
        fd: Resource<wasi_fs_types::Descriptor>,
        path_flags: wasi_fs_types::PathFlags,
        path: String,
    ) -> FsResult<wasi_fs_types::MetadataHashValue> {
        wasi_fs_types::HostDescriptor::metadata_hash_at(
            &mut self.filesystem(),
            fd,
            path_flags,
            path,
        )
        .await
    }
}

// ============================================================================
// 3. Single Host Import for Shell Command Execution (`ragent:extension/host`)
// ============================================================================

impl ragent::extension::host::Host for HostState {
    async fn execute_command_with_timeout(
        &mut self,
        command: String,
        timeout_ms: u64,
    ) -> ragent::extension::host::CommandOutput {
        // 1. Mandatory Host-Import Permission Check:
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
}

fn command_error(error: impl ToString) -> ragent::extension::host::CommandOutput {
    ragent::extension::host::CommandOutput {
        exit_code: -1,
        stdout: String::new(),
        stderr: String::new(),
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

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            PathComponent::CurDir => {}
            PathComponent::ParentDir => {
                out.pop();
            }
            c => out.push(c.as_os_str()),
        }
    }
    out
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

        // Override `wasi:filesystem/preopens` and `wasi:filesystem/types` with our permission-gated
        // WASI 0.2 HostDescriptor implementation.
        linker.allow_shadowing(true);
        wasi_preopens::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        wasi_fs_types::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(tool_error)?;
        linker.allow_shadowing(false);

        Plugin::add_to_linker::<_, HasSelf<_>>(&mut linker, |state| state).map_err(tool_error)?;

        let mut wasi_builder = WasiCtxBuilder::new();
        wasi_builder.inherit_stdout().inherit_stderr();

        // Map guest "." to workspace_root and set PWD so relative paths inside WASM resolve to workspace_root.
        // Also preopen "/" so `wasi-libc` delegates all absolute path operations to our
        // permission-checked `wasi:filesystem/types::HostDescriptor`.
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
        for dir in preopen_dirs {
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
        // Only preopen "/" when `core_host` is present (agent-core tool plugins with interactive permission gating).
        // Control-plane WASM plugins (`core_host.is_none()`) never mount "/" and can only see `workspace_root` and `preopen_dirs`.
        #[cfg(unix)]
        if core_host.is_some() {
            let _ = wasi_builder.preopened_dir("/", "/", DirPerms::all(), FilePerms::all());
        }

        // Hook WASI 0.2 `wasi:sockets` built-in permission check (`socket_addr_check`)
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
            preopen_dirs: preopen_dirs.to_vec(),
            core_host,
            sandbox_state,
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

    /// Invokes WASI 0.2 `WasiHttpView::send_request` (`wasi:http/outgoing-handler`) directly on this plugin's
    /// `HostState` to verify WASI 0.2 HTTP permission enforcement without adding custom WIT host imports.
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
