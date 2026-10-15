use crate::error::CoreError;
use crate::host::ModelEndpointConfig;
use async_trait::async_trait;
use openresponses_rust::{Client, CreateResponseBody, Item, ResponseResource, Usage};

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
}

#[derive(Debug, Clone, Default)]
pub struct OpenResponsesModelBackend;

#[async_trait]
impl ModelBackend for OpenResponsesModelBackend {
    async fn create_response(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        if endpoint.extra_headers.is_empty() {
            let client = Client::with_base_url(endpoint.api_key.clone(), endpoint.base_url.clone());
            return client
                .create_response(request)
                .await
                .map_err(CoreError::from);
        }
        let raw = serde_json::to_value(&request)?;
        self.create_response_raw(endpoint, raw).await
    }

    async fn create_response_raw(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
    ) -> Result<ResponseResource, CoreError> {
        let url = format!("{}/responses", endpoint.base_url.trim_end_matches('/'));
        let http = reqwest::Client::new();
        let mut req_builder = http
            .post(&url)
            .bearer_auth(&endpoint.api_key)
            .json(&request_json);

        for (k, v) in &endpoint.extra_headers {
            req_builder = req_builder.header(k, v);
        }

        let resp = req_builder
            .send()
            .await
            .map_err(|e| CoreError::ResponseFailed(format!("HTTP transport error: {e}")))?;
        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .map_err(|e| CoreError::ResponseFailed(format!("HTTP read body error: {e}")))?;

        if !status.is_success() {
            return Err(CoreError::ResponseFailed(format!(
                "OpenResponses HTTP {status}: {body_text}"
            )));
        }

        serde_json::from_str(&body_text).map_err(CoreError::Json)
    }
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
