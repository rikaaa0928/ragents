use super::HostState;
use crate::host::PermissionOutcome;
use ragents_protocol::CapabilityRequest;
use std::path::{Component as PathComponent, Path, PathBuf};
use wasmtime::component::Resource;
use wasmtime_wasi::filesystem::{WasiFilesystemCtxView, WasiFilesystemView as _};
use wasmtime_wasi::p2::bindings::filesystem::{preopens as wasi_preopens, types as wasi_fs_types};
use wasmtime_wasi::p2::{DynInputStream, DynOutputStream, FsError, FsResult};
use wasmtime_wasi::ResourceTable;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

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

pub(crate) fn host_matches_pattern(rule: &str, host: &str) -> bool {
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

pub(crate) fn normalize_lexical_path(path: &Path) -> PathBuf {
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
