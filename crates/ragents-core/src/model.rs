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
        let client = Client::with_base_url(endpoint.api_key.clone(), endpoint.base_url.clone());
        client
            .create_response(request)
            .await
            .map_err(CoreError::from)
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
