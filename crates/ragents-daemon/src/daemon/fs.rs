use crate::config::DaemonConfig;
use base64::Engine;
use chrono::{DateTime, Utc};
use ragents_protocol::{NodeFsEntry, NodeFsOp, NodeFsRequest, NodeFsResponse, NodeFsZone};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

const DEFAULT_MAX_READ_BYTES: usize = 20 * 1024 * 1024; // 20 MB
const HARD_MAX_FILE_BYTES: usize = 50 * 1024 * 1024; // 50 MB
const MAX_TEXT_PREVIEW_BYTES: usize = 512 * 1024; // 512 KB

pub fn execute_node_fs_request(config: &DaemonConfig, req: NodeFsRequest) -> NodeFsResponse {
    let (root_dir, agent_prefix) = match resolve_zone_root(config, &req) {
        Ok(v) => v,
        Err(err) => {
            return NodeFsResponse {
                ok: false,
                error: Some(err),
                ..Default::default()
            };
        }
    };

    if req.zone == NodeFsZone::Tmp {
        if let Err(e) = std::fs::create_dir_all(&root_dir) {
            return NodeFsResponse {
                ok: false,
                error: Some(format!("failed to create temporary directory {root_dir:?}: {e}")),
                ..Default::default()
            };
        }
    } else if !root_dir.exists() {
        return NodeFsResponse {
            ok: false,
            error: Some(format!("workspace directory does not exist: {}", root_dir.display())),
            root_dir: root_dir.to_string_lossy().to_string(),
            ..Default::default()
        };
    }

    let raw_rel = match &req.op {
        NodeFsOp::ListDir { rel_path } => rel_path.as_str(),
        NodeFsOp::ReadFile { rel_path, .. } => rel_path.as_str(),
        NodeFsOp::WriteTempFile { rel_path, .. } => rel_path.as_str(),
        NodeFsOp::UploadFileToHttp { rel_path, .. } => rel_path.as_str(),
        NodeFsOp::DownloadFileFromHttp { rel_path, .. } => rel_path.as_str(),
    };

    let clean_rel = match sanitize_rel_path(raw_rel) {
        Ok(p) => p,
        Err(err) => {
            return NodeFsResponse {
                ok: false,
                error: Some(err),
                root_dir: root_dir.to_string_lossy().to_string(),
                ..Default::default()
            };
        }
    };

    let clean_rel_str = clean_rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");

    let agent_path = if agent_prefix.is_empty() {
        if clean_rel_str.is_empty() {
            ".".to_string()
        } else {
            clean_rel_str.clone()
        }
    } else if clean_rel_str.is_empty() {
        agent_prefix.clone()
    } else {
        format!("{}/{}", agent_prefix.trim_end_matches('/'), clean_rel_str)
    };

    let target_path = if clean_rel_str.is_empty() {
        root_dir.clone()
    } else {
        root_dir.join(&clean_rel)
    };

    if let Err(err) = verify_within_root(&root_dir, &target_path) {
        return NodeFsResponse {
            ok: false,
            error: Some(err),
            root_dir: root_dir.to_string_lossy().to_string(),
            resolved_rel_path: clean_rel_str,
            agent_path,
            ..Default::default()
        };
    }

    match req.op {
        NodeFsOp::ListDir { .. } => {
            if !target_path.exists() {
                return NodeFsResponse {
                    ok: true,
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    entries: Vec::new(),
                    ..Default::default()
                };
            }
            if !target_path.is_dir() {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!("path is not a directory: {}", clean_rel_str)),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            let read_dir = match std::fs::read_dir(&target_path) {
                Ok(rd) => rd,
                Err(e) => {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to read directory: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            };

            let mut entries = Vec::new();
            for entry_res in read_dir.flatten() {
                let name = entry_res.file_name().to_string_lossy().to_string();
                if name == "." || name == ".." {
                    continue;
                }
                let meta = match entry_res.metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let is_dir = meta.is_dir();
                let size = if is_dir { 0 } else { meta.len() };
                let modified_at = meta
                    .modified()
                    .ok()
                    .map(|t| DateTime::<Utc>::from(t).to_rfc3339());
                let entry_rel = if clean_rel_str.is_empty() {
                    name.clone()
                } else {
                    format!("{clean_rel_str}/{name}")
                };
                entries.push(NodeFsEntry {
                    name,
                    rel_path: entry_rel,
                    is_dir,
                    size,
                    modified_at,
                });
            }

            entries.sort_by(|a, b| {
                b.is_dir
                    .cmp(&a.is_dir)
                    .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
            });

            NodeFsResponse {
                ok: true,
                root_dir: root_dir.to_string_lossy().to_string(),
                resolved_rel_path: clean_rel_str,
                agent_path,
                entries,
                ..Default::default()
            }
        }
        NodeFsOp::ReadFile {
            offset, max_bytes, ..
        } => {
            if !target_path.exists() || !target_path.is_file() {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!("file not found: {}", clean_rel_str)),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            let meta = match std::fs::metadata(&target_path) {
                Ok(m) => m,
                Err(e) => {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to stat file: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            };
            let total_size = meta.len();
            let start_offset = offset.unwrap_or(0);
            let limit = max_bytes
                .unwrap_or(DEFAULT_MAX_READ_BYTES)
                .min(HARD_MAX_FILE_BYTES);

            let mut file = match std::fs::File::open(&target_path) {
                Ok(f) => f,
                Err(e) => {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to open file: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            };

            if start_offset > 0 {
                use std::io::{Seek, SeekFrom};
                if start_offset >= total_size {
                    return NodeFsResponse {
                        ok: true,
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        data_base64: Some(String::new()),
                        size: Some(total_size),
                        truncated: false,
                        ..Default::default()
                    };
                }
                if let Err(e) = file.seek(SeekFrom::Start(start_offset)) {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to seek file: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            }

            let mut buf = Vec::new();
            if let Err(e) = file.take(limit as u64).read_to_end(&mut buf) {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!("failed to read file: {e}")),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            let truncated = start_offset.saturating_add(buf.len() as u64) < total_size;
            let mime_type = detect_mime_type(&target_path, &buf);
            let is_text = start_offset == 0 && is_likely_utf8_text(&buf);
            let text_preview = if is_text {
                let preview_slice = &buf[..buf.len().min(MAX_TEXT_PREVIEW_BYTES)];
                Some(String::from_utf8_lossy(preview_slice).into_owned())
            } else {
                None
            };
            let data_base64 = Some(base64::engine::general_purpose::STANDARD.encode(&buf));

            NodeFsResponse {
                ok: true,
                root_dir: root_dir.to_string_lossy().to_string(),
                resolved_rel_path: clean_rel_str,
                agent_path,
                data_base64,
                size: Some(total_size),
                mime_type: Some(mime_type),
                is_text,
                text_preview,
                truncated,
                ..Default::default()
            }
        }
        NodeFsOp::WriteTempFile { data_base64, .. } => {
            if req.zone != NodeFsZone::Tmp {
                return NodeFsResponse {
                    ok: false,
                    error: Some(
                        "File upload is strictly restricted to the bound temporary directory (zone=tmp)"
                            .to_string(),
                    ),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }
            if clean_rel_str.is_empty() {
                return NodeFsResponse {
                    ok: false,
                    error: Some("target relative file path cannot be empty".to_string()),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    ..Default::default()
                };
            }

            let bytes = match base64::engine::general_purpose::STANDARD.decode(data_base64.trim()) {
                Ok(b) => b,
                Err(e) => {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("invalid base64 payload: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            };

            if bytes.len() > HARD_MAX_FILE_BYTES {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!(
                        "file size ({} bytes) exceeds 50MB limit",
                        bytes.len()
                    )),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            if let Some(parent) = target_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to create parent directory: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
                if let Err(err) = verify_within_root(&root_dir, parent) {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(err),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            }

            if let Err(e) = std::fs::write(&target_path, &bytes) {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!("failed to write temporary file: {e}")),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            let mime_type = detect_mime_type(&target_path, &bytes);
            NodeFsResponse {
                ok: true,
                root_dir: root_dir.to_string_lossy().to_string(),
                resolved_rel_path: clean_rel_str,
                agent_path,
                size: Some(bytes.len() as u64),
                mime_type: Some(mime_type),
                ..Default::default()
            }
        }
        NodeFsOp::UploadFileToHttp {
            upload_url,
            upload_token,
            ..
        } => {
            if !target_path.exists() || !target_path.is_file() {
                return NodeFsResponse {
                    ok: false,
                    error: Some(format!("file not found: {}", clean_rel_str)),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            let meta = match std::fs::metadata(&target_path) {
                Ok(m) => m,
                Err(e) => {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to stat file: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            };
            let total_size = meta.len();

            let mut header_buf = Vec::new();
            if let Ok(f) = std::fs::File::open(&target_path) {
                let _ = f.take(4096).read_to_end(&mut header_buf);
            }
            let mime_type = detect_mime_type(&target_path, &header_buf);

            let path_for_upload = target_path.clone();
            tokio::spawn(async move {
                use futures::StreamExt;
                use tokio::io::{AsyncReadExt, AsyncSeekExt};

                let client = reqwest::Client::builder()
                    .http1_only()
                    .tcp_keepalive(std::time::Duration::from_secs(15))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new());

                const CHUNK_SIZE: u64 = 4 * 1024 * 1024;
                const CONCURRENCY: usize = 8;

                if total_size <= CHUNK_SIZE {
                    let bytes = match tokio::fs::read(&path_for_upload).await {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    let mut req_builder = client
                        .put(&upload_url)
                        .header("content-type", "application/octet-stream")
                        .header("content-length", bytes.len().to_string());
                    if let Some(ref tok) = upload_token {
                        req_builder = req_builder.header("x-upload-token", tok);
                    }
                    let _ = req_builder.body(bytes).send().await;
                    return;
                }

                let mut offsets = Vec::new();
                let mut cur = 0u64;
                while cur < total_size {
                    let len = (total_size - cur).min(CHUNK_SIZE);
                    offsets.push((cur, len));
                    cur += len;
                }

                let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                futures::stream::iter(offsets)
                    .for_each_concurrent(CONCURRENCY, |(offset, len)| {
                        let client = client.clone();
                        let path = path_for_upload.clone();
                        let base_url = upload_url.clone();
                        let token = upload_token.clone();
                        let failed = failed.clone();
                        async move {
                            if failed.load(std::sync::atomic::Ordering::Relaxed) {
                                return;
                            }
                            let mut buf = vec![0u8; len as usize];
                            let read_ok = async {
                                let mut f = tokio::fs::File::open(&path).await?;
                                f.seek(std::io::SeekFrom::Start(offset)).await?;
                                f.read_exact(&mut buf).await?;
                                Ok::<(), std::io::Error>(())
                            }
                            .await;
                            if read_ok.is_err() {
                                failed.store(true, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }

                            let chunk_url = format!("{base_url}?offset={offset}");
                            let mut ok = false;
                            for _attempt in 0..3 {
                                let mut rb = client
                                    .put(&chunk_url)
                                    .header("content-type", "application/octet-stream")
                                    .header("content-length", buf.len().to_string());
                                if let Some(ref tok) = token {
                                    rb = rb.header("x-upload-token", tok);
                                }
                                if let Ok(resp) = rb.body(buf.clone()).send().await {
                                    if resp.status().is_success() {
                                        ok = true;
                                        break;
                                    }
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                            }
                            if !ok {
                                failed.store(true, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    })
                    .await;

                if !failed.load(std::sync::atomic::Ordering::Relaxed) {
                    let complete_url = format!("{upload_url}?complete=1");
                    let mut rb = client.post(&complete_url).header("content-length", "0");
                    if let Some(ref tok) = upload_token {
                        rb = rb.header("x-upload-token", tok);
                    }
                    let _ = rb.send().await;
                } else {
                    let fail_url = format!("{upload_url}?failed=1");
                    let mut rb = client.post(&fail_url).header("content-length", "0");
                    if let Some(ref tok) = upload_token {
                        rb = rb.header("x-upload-token", tok);
                    }
                    let _ = rb.send().await;
                }
            });

            NodeFsResponse {
                ok: true,
                root_dir: root_dir.to_string_lossy().to_string(),
                resolved_rel_path: clean_rel_str,
                agent_path,
                size: Some(total_size),
                mime_type: Some(mime_type),
                ..Default::default()
            }
        }
        NodeFsOp::DownloadFileFromHttp {
            download_url,
            download_token,
            expected_size,
            ..
        } => {
            if req.zone != NodeFsZone::Tmp {
                return NodeFsResponse {
                    ok: false,
                    error: Some(
                        "DownloadFileFromHttp is only permitted in the temporary directory zone"
                            .to_string(),
                    ),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            if clean_rel.as_os_str().is_empty() {
                return NodeFsResponse {
                    ok: false,
                    error: Some("relative file path cannot be empty".to_string()),
                    root_dir: root_dir.to_string_lossy().to_string(),
                    resolved_rel_path: clean_rel_str,
                    agent_path,
                    ..Default::default()
                };
            }

            if let Some(parent) = target_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(format!("failed to create parent dir: {e}")),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
                if let Err(err) = verify_within_root(&root_dir, parent) {
                    return NodeFsResponse {
                        ok: false,
                        error: Some(err),
                        root_dir: root_dir.to_string_lossy().to_string(),
                        resolved_rel_path: clean_rel_str,
                        agent_path,
                        ..Default::default()
                    };
                }
            }

            let target_for_dl = target_path.clone();
            tokio::spawn(async move {
                use futures::StreamExt;
                use tokio::io::AsyncWriteExt;

                let client = reqwest::Client::builder()
                    .tcp_keepalive(std::time::Duration::from_secs(15))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new());

                let part_path = target_for_dl.with_extension(format!(
                    "part.{}",
                    uuid::Uuid::now_v7().simple()
                ));

                let mut last_err = String::new();
                let mut success = false;

                for attempt in 0..3 {
                    if attempt > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(400 * attempt as u64))
                            .await;
                    }
                    let mut rb = client.get(&download_url);
                    if let Some(ref tok) = download_token {
                        rb = rb.header("x-distribute-token", tok);
                    }
                    let resp = match rb.send().await {
                        Ok(r) if r.status().is_success() => r,
                        Ok(r) => {
                            last_err = format!("HTTP {}", r.status());
                            continue;
                        }
                        Err(e) => {
                            last_err = e.to_string();
                            continue;
                        }
                    };

                    let mut file = match tokio::fs::File::create(&part_path).await {
                        Ok(f) => f,
                        Err(e) => {
                            last_err = e.to_string();
                            break;
                        }
                    };

                    let mut stream = resp.bytes_stream();
                    let mut received = 0u64;
                    let mut stream_ok = true;
                    while let Some(chunk_res) = stream.next().await {
                        match chunk_res {
                            Ok(chunk) => {
                                if let Err(e) = file.write_all(&chunk).await {
                                    last_err = e.to_string();
                                    stream_ok = false;
                                    break;
                                }
                                received += chunk.len() as u64;
                            }
                            Err(e) => {
                                last_err = e.to_string();
                                stream_ok = false;
                                break;
                            }
                        }
                    }

                    let _ = file.flush().await;
                    drop(file);

                    if !stream_ok {
                        let _ = tokio::fs::remove_file(&part_path).await;
                        continue;
                    }
                    if let Some(exp) = expected_size {
                        if received != exp {
                            last_err =
                                format!("size mismatch: received {received}, expected {exp}");
                            let _ = tokio::fs::remove_file(&part_path).await;
                            continue;
                        }
                    }

                    if let Err(e) = tokio::fs::rename(&part_path, &target_for_dl).await {
                        last_err = e.to_string();
                        let _ = tokio::fs::remove_file(&part_path).await;
                        break;
                    }

                    success = true;
                    break;
                }

                if success {
                    let complete_url = format!("{download_url}?complete=1");
                    let mut rb = client.post(&complete_url).header("content-length", "0");
                    if let Some(ref tok) = download_token {
                        rb = rb.header("x-distribute-token", tok);
                    }
                    let _ = rb.send().await;
                } else {
                    let _ = tokio::fs::remove_file(&part_path).await;
                    let fail_url = format!("{download_url}?failed=1");
                    let mut rb = client
                        .post(&fail_url)
                        .header("content-type", "text/plain; charset=utf-8")
                        .body(last_err);
                    if let Some(ref tok) = download_token {
                        rb = rb.header("x-distribute-token", tok);
                    }
                    let _ = rb.send().await;
                }
            });

            NodeFsResponse {
                ok: true,
                root_dir: root_dir.to_string_lossy().to_string(),
                resolved_rel_path: clean_rel_str,
                agent_path,
                size: expected_size,
                ..Default::default()
            }
        }
    }
}

fn resolve_zone_root(
    config: &DaemonConfig,
    req: &NodeFsRequest,
) -> Result<(PathBuf, String), String> {
    match req.zone {
        NodeFsZone::Workspace => {
            let ws_str = req
                .workspace_path
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    "Current room has no bound workspace_path (Floating mode only has temporary directory zone=tmp)"
                        .to_string()
                })?;
            let ws_path = PathBuf::from(ws_str);
            config
                .validate_workspace_path(&ws_path)
                .map_err(|e| e.to_string())?;
            Ok((ws_path, String::new()))
        }
        NodeFsZone::Tmp => {
            if let Some(tmp_str) = req
                .tmp_dir
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let tmp_path = PathBuf::from(tmp_str);
                let _ = std::fs::create_dir_all(&tmp_path);
                config
                    .validate_workspace_path(&tmp_path)
                    .map_err(|e| e.to_string())?;
                let prefix = tmp_str.trim_end_matches('/').to_string();
                Ok((tmp_path, prefix))
            } else if let Some(ws_str) = req
                .workspace_path
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let ws_path = PathBuf::from(ws_str);
                config
                    .validate_workspace_path(&ws_path)
                    .map_err(|e| e.to_string())?;
                let tmp_dir = ws_path.join(".ragents-tmp");
                Ok((tmp_dir, ".ragents-tmp".to_string()))
            } else {
                let tmp_dir = config.temp_root_dir.join("chatroom-tmp");
                let prefix = tmp_dir.to_string_lossy().to_string();
                Ok((tmp_dir, prefix))
            }
        }
    }
}

fn sanitize_rel_path(raw: &str) -> Result<PathBuf, String> {
    let trimmed = raw.trim().trim_start_matches('/');
    if trimmed.is_empty() || trimmed == "." {
        return Ok(PathBuf::new());
    }
    let mut clean = PathBuf::new();
    for comp in Path::new(trimmed).components() {
        match comp {
            Component::CurDir => {}
            Component::Normal(part) => clean.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(
                    "invalid relative path: path traversal ('..') is forbidden".to_string(),
                );
            }
        }
    }
    Ok(clean)
}

fn verify_within_root(root_dir: &Path, target: &Path) -> Result<(), String> {
    let canon_root = root_dir
        .canonicalize()
        .unwrap_or_else(|_| root_dir.to_path_buf());
    let mut probe = target;
    while !probe.exists() {
        if let Some(p) = probe.parent() {
            probe = p;
        } else {
            break;
        }
    }
    if probe.exists() {
        if let Ok(canon_probe) = probe.canonicalize() {
            if !canon_probe.starts_with(&canon_root) {
                return Err(format!(
                    "access denied: target resolves outside bound root directory ({})",
                    canon_root.display()
                ));
            }
        }
    }
    Ok(())
}

fn is_likely_utf8_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    let sample = &bytes[..bytes.len().min(8192)];
    if sample.contains(&0) {
        return false;
    }
    std::str::from_utf8(sample).is_ok()
}

fn detect_mime_type(path: &Path, bytes: &[u8]) -> String {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png".to_string();
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "image/jpeg".to_string();
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        return "image/webp".to_string();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_string();
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf".to_string();
    }

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "md" | "markdown" => "text/markdown; charset=utf-8",
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" | "ts" | "tsx" | "jsx" => "text/javascript; charset=utf-8",
        "rs" | "py" | "go" | "toml" | "yaml" | "yml" | "sh" | "txt" | "log" | "csv" | "sql" => {
            "text/plain; charset=utf-8"
        }
        _ => {
            if is_likely_utf8_text(bytes) {
                "text/plain; charset=utf-8"
            } else {
                "application/octet-stream"
            }
        }
    }
    .to_string()
}
