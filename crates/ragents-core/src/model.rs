use crate::error::CoreError;
use crate::host::ModelEndpointConfig;
use async_trait::async_trait;
use futures::StreamExt;
use openresponses_rust::{CreateResponseBody, Item, MessageContent, ResponseResource, Usage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelStreamDelta {
    TextDelta { delta: String },
    ReasoningDelta { delta: String },
}

pub type ModelStreamCallback = Arc<dyn Fn(ModelStreamDelta) + Send + Sync>;

/// Abstraction over OpenResponses-compatible model endpoints.
/// All models integrated into `ragents` must conform to the OpenResponses format.
#[async_trait]
pub trait ModelBackend: Send + Sync {
    async fn create_response(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError>;

    /// Invokes the OpenResponses endpoint with a raw/mutated JSON payload (supporting vendor-specific
    /// fields injected by `agent-daemon` interceptors) and custom headers (`endpoint.extra_headers`).
    async fn create_response_raw(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
    ) -> Result<ResponseResource, CoreError> {
        let req: CreateResponseBody = serde_json::from_value(request_json)?;
        self.create_response(endpoint, req).await
    }

    async fn create_response_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        let resp = self.create_response(endpoint, request).await?;
        if let Some(ref cb) = on_delta {
            emit_synthetic_deltas_from_items(&resp.output, cb);
        }
        Ok(resp)
    }

    async fn create_response_raw_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        let resp = self.create_response_raw(endpoint, request_json).await?;
        if let Some(ref cb) = on_delta {
            emit_synthetic_deltas_from_items(&resp.output, cb);
        }
        Ok(resp)
    }
}

pub fn emit_synthetic_deltas_from_items(items: &[Item], cb: &ModelStreamCallback) {
    for item in items {
        match item {
            Item::Message { content, .. } => {
                for part in content {
                    if let MessageContent::OutputText { text, .. }
                    | MessageContent::PlainText { text } = part
                    {
                        if !text.is_empty() {
                            cb(ModelStreamDelta::TextDelta {
                                delta: text.clone(),
                            });
                        }
                    }
                }
            }
            Item::Reasoning {
                summary, content, ..
            } => {
                for part in summary {
                    if let MessageContent::SummaryText { text }
                    | MessageContent::OutputText { text, .. }
                    | MessageContent::PlainText { text } = part
                    {
                        if !text.is_empty() {
                            cb(ModelStreamDelta::ReasoningDelta {
                                delta: text.clone(),
                            });
                        }
                    }
                }
                if let Some(parts) = content {
                    for part in parts {
                        if let MessageContent::SummaryText { text }
                        | MessageContent::OutputText { text, .. }
                        | MessageContent::PlainText { text } = part
                        {
                            if !text.is_empty() {
                                cb(ModelStreamDelta::ReasoningDelta {
                                    delta: text.clone(),
                                });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

const MAX_TRANSIENT_RETRIES: u32 = 3;

fn is_transient_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

#[derive(Debug, Clone, Default)]
pub struct OpenResponsesModelBackend;

impl OpenResponsesModelBackend {
    fn build_endpoint_url(base_url: &str) -> String {
        let trimmed = base_url.trim_end_matches('/');
        if trimmed.ends_with("/v1") {
            format!("{trimmed}/responses")
        } else {
            format!("{trimmed}/v1/responses")
        }
    }

    async fn send_with_retry(
        endpoint: &ModelEndpointConfig,
        request_json: &serde_json::Value,
        accept_sse: bool,
    ) -> Result<reqwest::Response, CoreError> {
        let url = Self::build_endpoint_url(&endpoint.base_url);
        let http = reqwest::Client::new();
        let mut attempt = 0u32;

        loop {
            let mut req_builder = http
                .post(&url)
                .bearer_auth(&endpoint.api_key)
                .json(request_json);

            if accept_sse {
                req_builder = req_builder.header(
                    reqwest::header::ACCEPT,
                    "text/event-stream, application/json",
                );
            }

            for (k, v) in &endpoint.extra_headers {
                req_builder = req_builder.header(k, v);
            }

            match req_builder.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    }
                    let body_text = resp.text().await.unwrap_or_default();
                    if is_transient_status(status) && attempt < MAX_TRANSIENT_RETRIES {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(300 * (1 << (attempt - 1)))).await;
                        continue;
                    }
                    return Err(CoreError::ResponseFailed(format!(
                        "OpenResponses HTTP {status}: {body_text}"
                    )));
                }
                Err(err) => {
                    if attempt < MAX_TRANSIENT_RETRIES {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(300 * (1 << (attempt - 1)))).await;
                        continue;
                    }
                    return Err(CoreError::ResponseFailed(format!(
                        "HTTP transport error: {err}"
                    )));
                }
            }
        }
    }
}

#[async_trait]
impl ModelBackend for OpenResponsesModelBackend {
    async fn create_response(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        let raw = serde_json::to_value(&request)?;
        self.create_response_raw(endpoint, raw).await
    }

    async fn create_response_raw(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
    ) -> Result<ResponseResource, CoreError> {
        let is_stream_req = request_json
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if is_stream_req {
            return self
                .create_response_raw_streaming(endpoint, request_json, None)
                .await;
        }

        let resp = Self::send_with_retry(endpoint, &request_json, false).await?;
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();

        if content_type.contains("text/event-stream") {
            return parse_sse_response_stream(resp, &request_json, None).await;
        }

        let body_text = resp
            .text()
            .await
            .map_err(|e| CoreError::ResponseFailed(format!("HTTP read body error: {e}")))?;
        serde_json::from_str(&body_text).map_err(CoreError::Json)
    }

    async fn create_response_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        mut request: CreateResponseBody,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        request.stream = Some(true);
        let raw = serde_json::to_value(&request)?;
        self.create_response_raw_streaming(endpoint, raw, on_delta)
            .await
    }

    async fn create_response_raw_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        mut request_json: serde_json::Value,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        if let Some(obj) = request_json.as_object_mut() {
            obj.insert("stream".to_string(), serde_json::Value::Bool(true));
        }

        let resp = Self::send_with_retry(endpoint, &request_json, true).await?;
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();

        if content_type.contains("text/event-stream") {
            parse_sse_response_stream(resp, &request_json, on_delta).await
        } else {
            let body_text = resp
                .text()
                .await
                .map_err(|e| CoreError::ResponseFailed(format!("HTTP read body error: {e}")))?;
            let parsed: ResponseResource =
                serde_json::from_str(&body_text).map_err(CoreError::Json)?;
            if let Some(ref cb) = on_delta {
                emit_synthetic_deltas_from_items(&parsed.output, cb);
            }
            Ok(parsed)
        }
    }
}

async fn parse_sse_response_stream(
    resp: reqwest::Response,
    request_json: &serde_json::Value,
    on_delta: Option<ModelStreamCallback>,
) -> Result<ResponseResource, CoreError> {
    let mut byte_stream = resp.bytes_stream();
    let mut buffer = String::new();
    let mut completed_response: Option<ResponseResource> = None;
    let mut last_response_id: Option<String> = None;
    let mut last_usage: Option<Usage> = None;
    let mut collected_items: Vec<Item> = Vec::new();
    let mut in_progress_items: std::collections::BTreeMap<i64, Item> =
        std::collections::BTreeMap::new();
    let mut accumulated_text = String::new();

    while let Some(chunk_res) = byte_stream.next().await {
        let chunk = chunk_res
            .map_err(|e| CoreError::ResponseFailed(format!("SSE stream read error: {e}")))?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim_end_matches('\r').to_string();
            buffer.drain(..=newline_pos);

            let Some(data_str) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data_str.trim();
            if data.is_empty() || data == "[DONE]" {
                continue;
            }

            let Ok(val) = serde_json::from_str::<serde_json::Value>(data) else {
                continue;
            };

            let ev_type = val.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match ev_type {
                "response.output_text.delta" => {
                    if let Some(delta) = val.get("delta").and_then(|v| v.as_str()) {
                        if !delta.is_empty() {
                            accumulated_text.push_str(delta);
                            if let Some(ref cb) = on_delta {
                                cb(ModelStreamDelta::TextDelta {
                                    delta: delta.to_string(),
                                });
                            }
                        }
                    }
                }
                "response.reasoning.delta"
                | "response.reasoning_text.delta"
                | "response.reasoning_summary_text.delta" => {
                    if let Some(delta) = val.get("delta").and_then(|v| v.as_str()) {
                        if !delta.is_empty() {
                            if let Some(ref cb) = on_delta {
                                cb(ModelStreamDelta::ReasoningDelta {
                                    delta: delta.to_string(),
                                });
                            }
                        }
                    }
                }
                "response.output_item.added" => {
                    let out_idx = val
                        .get("output_index")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if let Some(item_val) = val.get("item") {
                        if let Ok(item) = serde_json::from_value::<Item>(item_val.clone()) {
                            in_progress_items.insert(out_idx, item);
                        }
                    }
                }
                "response.function_call_arguments.delta" => {
                    let out_idx = val
                        .get("output_index")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if let Some(delta) = val.get("delta").and_then(|v| v.as_str()) {
                        if let Some(Item::FunctionCall { arguments, .. }) =
                            in_progress_items.get_mut(&out_idx)
                        {
                            arguments.push_str(delta);
                        }
                    }
                }
                "response.function_call_arguments.done" => {
                    let out_idx = val
                        .get("output_index")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if let Some(args_done) = val.get("arguments").and_then(|v| v.as_str()) {
                        if let Some(Item::FunctionCall { arguments, .. }) =
                            in_progress_items.get_mut(&out_idx)
                        {
                            *arguments = args_done.to_string();
                        }
                    }
                }
                "response.output_item.done" => {
                    let out_idx = val
                        .get("output_index")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if let Some(item_val) = val.get("item") {
                        if let Ok(item) = serde_json::from_value::<Item>(item_val.clone()) {
                            in_progress_items.insert(out_idx, item.clone());
                            collected_items.push(item);
                        }
                    }
                }
                "response.created" | "response.in_progress" => {
                    if let Some(resp_val) = val.get("response") {
                        if let Some(id) = resp_val.get("id").and_then(|v| v.as_str()) {
                            last_response_id = Some(id.to_string());
                        }
                    }
                }
                "response.completed" | "response.incomplete" => {
                    if let Some(resp_val) = val.get("response") {
                        if let Some(id) = resp_val.get("id").and_then(|v| v.as_str()) {
                            last_response_id = Some(id.to_string());
                        }
                        if let Some(u_val) = resp_val.get("usage") {
                            if let Ok(u) = serde_json::from_value::<Usage>(u_val.clone()) {
                                last_usage = Some(u);
                            }
                        }
                        if let Ok(parsed) =
                            serde_json::from_value::<ResponseResource>(resp_val.clone())
                        {
                            completed_response = Some(parsed);
                        }
                    }
                }
                "response.failed" => {
                    let msg = val
                        .get("response")
                        .and_then(|r| r.get("error"))
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("Streaming response failed");
                    return Err(CoreError::ResponseFailed(msg.to_string()));
                }
                "error" => {
                    let msg = val
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .or_else(|| val.get("message"))
                        .and_then(|m| m.as_str())
                        .unwrap_or("Streaming error event");
                    return Err(CoreError::ResponseFailed(msg.to_string()));
                }
                _ => {}
            }
        }
    }

    if let Some(mut resp) = completed_response {
        if resp.output.is_empty() && !collected_items.is_empty() {
            resp.output = collected_items;
        } else if resp.output.is_empty() && !in_progress_items.is_empty() {
            resp.output = in_progress_items.into_values().collect();
        } else if resp.output.is_empty() && !accumulated_text.is_empty() {
            resp.output = vec![Item::assistant_message(accumulated_text)];
        }
        return Ok(resp);
    }

    if !collected_items.is_empty() || !in_progress_items.is_empty() || !accumulated_text.is_empty()
    {
        let items = if !collected_items.is_empty() {
            collected_items
        } else if !in_progress_items.is_empty() {
            in_progress_items.into_values().collect()
        } else {
            vec![Item::assistant_message(accumulated_text)]
        };
        let model_name = request_json
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let resp_id = last_response_id.unwrap_or_else(|| "resp_stream".to_string());
        return Ok(make_openresponses_resource(
            &resp_id, model_name, items, last_usage,
        ));
    }

    Err(CoreError::ResponseFailed(
        "SSE stream ended without a completed response or output items".into(),
    ))
}

pub fn make_openresponses_resource(
    id: &str,
    model: &str,
    output: Vec<Item>,
    usage: Option<Usage>,
) -> ResponseResource {
    let val = serde_json::json!({
        "id": id,
        "object": "response",
        "created_at": 1700000000,
        "status": "completed",
        "model": model,
        "output": output,
        "tools": [],
        "tool_choice": "auto",
        "truncation": "disabled",
        "parallel_tool_calls": true,
        "text": { "format": { "type": "text" } },
        "top_p": 1.0,
        "presence_penalty": 0.0,
        "frequency_penalty": 0.0,
        "top_logprobs": 0,
        "temperature": 1.0,
        "store": false,
        "background": false,
        "service_tier": "default",
        "metadata": {},
        "usage": usage,
    });
    serde_json::from_value(val).expect("valid OpenResponses ResponseResource")
}
