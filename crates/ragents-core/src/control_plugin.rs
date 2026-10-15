use crate::error::CoreError;
use crate::hooks::protocol::HookRequest;
use crate::hooks::runtime::WasmPlugin;
use ragents_protocol::{ControlHookInvocation, ControlHookResult, ExtensionDriverConfig};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

static INVOCATION_SEQ: AtomicU64 = AtomicU64::new(1);

/// Universal driver for executing control-plane (`api-server`) and host-plane (`agent-daemon`)
/// extensions via either Wasmtime Components (`.wasm` / `.cwasm`) or HTTP / UDS Webhooks.
#[derive(Clone, Default)]
pub struct ControlPluginRunner {
    http_client: reqwest::Client,
}

impl ControlPluginRunner {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn invoke(
        &self,
        plugin_id: &str,
        driver: &ExtensionDriverConfig,
        timeout_ms: u64,
        invocation: &ControlHookInvocation,
    ) -> Result<ControlHookResult, CoreError> {
        let fut = async {
            match driver {
                ExtensionDriverConfig::Webhook { endpoint } => {
                    self.invoke_webhook(endpoint, invocation).await
                }
                ExtensionDriverConfig::Wasm { path, allowed_dirs } => {
                    self.invoke_wasm(plugin_id, path, allowed_dirs, invocation)
                        .await
                }
            }
        };

        if timeout_ms == 0 {
            fut.await
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), fut).await {
                Ok(res) => res,
                Err(_) => Err(CoreError::HostError(format!(
                    "control extension '{plugin_id}' timed out after {timeout_ms} ms"
                ))),
            }
        }
    }

    async fn invoke_webhook(
        &self,
        endpoint: &str,
        invocation: &ControlHookInvocation,
    ) -> Result<ControlHookResult, CoreError> {
        #[cfg(unix)]
        if let Some(unix_path) = endpoint.strip_prefix("unix://") {
            return invoke_uds_webhook(unix_path, invocation).await;
        }
        #[cfg(unix)]
        if let Some(unix_path) = endpoint.strip_prefix("unix:") {
            return invoke_uds_webhook(unix_path, invocation).await;
        }

        let resp = self
            .http_client
            .post(endpoint)
            .json(invocation)
            .send()
            .await
            .map_err(|e| {
                CoreError::HostError(format!("webhook request to {endpoint} failed: {e}"))
            })?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| CoreError::HostError(format!("webhook read body failed: {e}")))?;

        if !status.is_success() {
            return Err(CoreError::HostError(format!(
                "webhook {endpoint} returned HTTP {status}: {text}"
            )));
        }

        parse_control_hook_response(&text)
    }

    async fn invoke_wasm(
        &self,
        plugin_id: &str,
        path: &Path,
        allowed_dirs: &[PathBuf],
        invocation: &ControlHookInvocation,
    ) -> Result<ControlHookResult, CoreError> {
        let safe_id: String = plugin_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let work_dir =
            std::env::temp_dir()
                .join("ragents-control-wasm-work")
                .join(if safe_id.is_empty() {
                    "default"
                } else {
                    &safe_id
                });
        let _ = std::fs::create_dir_all(&work_dir);

        let plugin =
            WasmPlugin::load_from_file_with_workspace(plugin_id, path, &work_dir, allowed_dirs)
                .await?;
        plugin.initialize(&invocation.plugin_config).await?;

        let hook_req = HookRequest {
            protocol_version: 1,
            invocation_id: INVOCATION_SEQ.fetch_add(1, Ordering::Relaxed),
            hook: invocation.hook_name.clone(),
            iteration: None,
            payload: invocation.payload.clone(),
        };

        let raw_val = plugin.invoke(&hook_req).await?;
        let _ = plugin.shutdown().await;

        // Support both `ControlHookResult` (`{ok, output, error}`) and standard `ragent-extension`
        // `{"action": "continue", "payload": ...}` formats so the same WIT world works seamlessly.
        if raw_val.get("ok").is_some() {
            let parsed: ControlHookResult = serde_json::from_value(raw_val)?;
            return Ok(parsed);
        }

        if let Some(payload) = raw_val.get("payload") {
            return Ok(ControlHookResult {
                ok: true,
                output: payload.clone(),
                error: None,
            });
        }

        Ok(ControlHookResult {
            ok: true,
            output: raw_val,
            error: None,
        })
    }
}

#[cfg(unix)]
async fn invoke_uds_webhook(
    unix_path: &str,
    invocation: &ControlHookInvocation,
) -> Result<ControlHookResult, CoreError> {
    let mut stream = tokio::net::UnixStream::connect(unix_path)
        .await
        .map_err(|e| {
            CoreError::HostError(format!(
                "failed to connect to uds webhook '{unix_path}': {e}"
            ))
        })?;

    let mut line = serde_json::to_string(invocation)?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .await
        .map_err(|e| CoreError::HostError(format!("uds webhook write failed: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| CoreError::HostError(format!("uds webhook flush failed: {e}")))?;

    let mut reader = BufReader::new(stream).lines();
    let reply_line = reader
        .next_line()
        .await
        .map_err(|e| CoreError::HostError(format!("uds webhook read failed: {e}")))?
        .ok_or_else(|| CoreError::HostError("uds webhook closed stream without response".into()))?;

    parse_control_hook_response(&reply_line)
}

fn parse_control_hook_response(raw_text: &str) -> Result<ControlHookResult, CoreError> {
    let val: serde_json::Value = serde_json::from_str(raw_text)?;
    if val.get("ok").is_some() {
        return Ok(serde_json::from_value(val)?);
    }
    Ok(ControlHookResult {
        ok: true,
        output: val,
        error: None,
    })
}
