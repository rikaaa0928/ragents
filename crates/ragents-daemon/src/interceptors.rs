use async_trait::async_trait;
use openresponses_rust::{CreateResponseBody, ResponseResource};
use ragents_core::{
    emit_synthetic_deltas_from_items, ControlPluginRunner, CoreError, ModelBackend,
    ModelEndpointConfig, ModelStreamCallback,
};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Native Rust trait for `agent-daemon` extensions (Model request/response interception & permission policy hooks).
#[async_trait]
pub trait DaemonInterceptor: Send + Sync {
    fn id(&self) -> &str;

    fn priority(&self) -> i32 {
        100
    }

    fn fail_policy(&self) -> ExtensionFailPolicy {
        ExtensionFailPolicy::FailClosed
    }

    async fn before_model_request(
        &self,
        _input: &ModelRequestInterceptInput,
    ) -> Result<ModelRequestInterceptOutcome, String> {
        Ok(ModelRequestInterceptOutcome::default())
    }

    async fn after_model_response(
        &self,
        _input: &ModelResponseInterceptInput,
    ) -> Result<ModelResponseInterceptOutcome, String> {
        Ok(ModelResponseInterceptOutcome::Continue { response: None })
    }

    async fn evaluate_permission(
        &self,
        _input: &PermissionInterceptInput,
    ) -> Result<PermissionInterceptOutcome, String> {
        Ok(PermissionInterceptOutcome::Abstain)
    }
}

/// Declaratively configured interceptor driven by either a WASM Component (`.wasm`/`.cwasm`)
/// or an HTTP / UDS Webhook (`http://`, `https://`, `unix://`).
pub struct ConfiguredDaemonInterceptor {
    spec: DaemonInterceptorSpec,
    runner: ControlPluginRunner,
}

impl ConfiguredDaemonInterceptor {
    pub fn new(spec: DaemonInterceptorSpec) -> Self {
        Self {
            spec,
            runner: ControlPluginRunner::new(),
        }
    }

    fn handles_hook(&self, kind: DaemonHookKind) -> bool {
        self.spec.hooks.is_empty() || self.spec.hooks.contains(&kind)
    }
}

#[async_trait]
impl DaemonInterceptor for ConfiguredDaemonInterceptor {
    fn id(&self) -> &str {
        &self.spec.id
    }

    fn priority(&self) -> i32 {
        self.spec.priority
    }

    fn fail_policy(&self) -> ExtensionFailPolicy {
        self.spec.fail_policy
    }

    async fn before_model_request(
        &self,
        input: &ModelRequestInterceptInput,
    ) -> Result<ModelRequestInterceptOutcome, String> {
        if !self.handles_hook(DaemonHookKind::BeforeModelRequest) {
            return Ok(ModelRequestInterceptOutcome::default());
        }
        let inv = ControlHookInvocation {
            hook_name: "before_model_request".to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(input).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "interceptor returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }

    async fn after_model_response(
        &self,
        input: &ModelResponseInterceptInput,
    ) -> Result<ModelResponseInterceptOutcome, String> {
        if !self.handles_hook(DaemonHookKind::AfterModelResponse) {
            return Ok(ModelResponseInterceptOutcome::Continue { response: None });
        }
        let inv = ControlHookInvocation {
            hook_name: "after_model_response".to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(input).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "interceptor returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }

    async fn evaluate_permission(
        &self,
        input: &PermissionInterceptInput,
    ) -> Result<PermissionInterceptOutcome, String> {
        if !self.handles_hook(DaemonHookKind::EvaluatePermission) {
            return Ok(PermissionInterceptOutcome::Abstain);
        }
        let inv = ControlHookInvocation {
            hook_name: "evaluate_permission".to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(input).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "interceptor returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }
}

/// Ordered chain of `DaemonInterceptor` plugins on an `agent-daemon` node.
#[derive(Clone, Default)]
pub struct DaemonInterceptorChain {
    interceptors: Vec<Arc<dyn DaemonInterceptor>>,
}

impl DaemonInterceptorChain {
    pub fn from_specs(specs: &[DaemonInterceptorSpec]) -> Self {
        let mut list: Vec<Arc<dyn DaemonInterceptor>> = specs
            .iter()
            .cloned()
            .map(|s| Arc::new(ConfiguredDaemonInterceptor::new(s)) as Arc<dyn DaemonInterceptor>)
            .collect();
        list.sort_by_key(|i| i.priority());
        Self { interceptors: list }
    }

    pub fn push(&mut self, interceptor: Arc<dyn DaemonInterceptor>) {
        self.interceptors.push(interceptor);
        self.interceptors.sort_by_key(|i| i.priority());
    }

    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    pub async fn evaluate_permission(
        &self,
        input: &PermissionInterceptInput,
    ) -> Result<PermissionInterceptOutcome, CoreError> {
        for interceptor in &self.interceptors {
            match interceptor.evaluate_permission(input).await {
                Ok(PermissionInterceptOutcome::Abstain) => continue,
                Ok(decision) => return Ok(decision),
                Err(err) => match interceptor.fail_policy() {
                    ExtensionFailPolicy::FailOpen => continue,
                    ExtensionFailPolicy::FailClosed => {
                        return Err(CoreError::HostError(format!(
                            "daemon permission interceptor '{}' failed: {err}",
                            interceptor.id()
                        )))
                    }
                },
            }
        }
        Ok(PermissionInterceptOutcome::Abstain)
    }
}

/// Wraps an inner `ModelBackend` for a specific activation, executing `before_model_request`
/// and `after_model_response` interceptors around every OpenResponses model call.
pub struct InterceptedModelBackend {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub session_labels: BTreeMap<String, String>,
    pub chain: DaemonInterceptorChain,
    pub inner: Arc<dyn ModelBackend>,
}

#[async_trait]
impl ModelBackend for InterceptedModelBackend {
    async fn create_response(
        &self,
        endpoint: &ModelEndpointConfig,
        request: CreateResponseBody,
    ) -> Result<ResponseResource, CoreError> {
        if self.chain.is_empty() {
            return self.inner.create_response(endpoint, request).await;
        }
        let raw_json = serde_json::to_value(&request)?;
        self.create_response_raw(endpoint, raw_json).await
    }

    async fn create_response_raw(
        &self,
        endpoint: &ModelEndpointConfig,
        request_json: serde_json::Value,
    ) -> Result<ResponseResource, CoreError> {
        self.create_response_raw_streaming(endpoint, request_json, None)
            .await
    }

    async fn create_response_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        mut request: CreateResponseBody,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        if self.chain.is_empty() {
            return self
                .inner
                .create_response_streaming(endpoint, request, on_delta)
                .await;
        }
        request.stream = Some(true);
        let raw_json = serde_json::to_value(&request)?;
        self.create_response_raw_streaming(endpoint, raw_json, on_delta)
            .await
    }

    async fn create_response_raw_streaming(
        &self,
        endpoint: &ModelEndpointConfig,
        mut request_json: serde_json::Value,
        on_delta: Option<ModelStreamCallback>,
    ) -> Result<ResponseResource, CoreError> {
        let mut effective_endpoint = endpoint.clone();
        let mut model_id = request_json
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let turn_id = request_json
            .get("metadata")
            .and_then(|m| m.get("ragents.turn_id"))
            .and_then(|v| v.as_str())
            .map(TurnId::new);

        // 1. Run `before_model_request` interceptors
        let mut short_circuit_response: Option<ResponseResource> = None;

        for interceptor in &self.chain.interceptors {
            let input = ModelRequestInterceptInput {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                activation_id: self.activation_id.clone(),
                turn_id: turn_id.clone(),
                session_labels: self.session_labels.clone(),
                endpoint: InterceptEndpointView {
                    model_id: model_id.clone(),
                    base_url: effective_endpoint.base_url.clone(),
                    extra_headers: effective_endpoint.extra_headers.clone(),
                },
                request_body: request_json.clone(),
            };

            match interceptor.before_model_request(&input).await {
                Ok(ModelRequestInterceptOutcome::Continue {
                    request_body,
                    override_base_url,
                    upsert_headers,
                    override_api_key,
                }) => {
                    if let Some(new_body) = request_body {
                        if let Some(m) = new_body.get("model").and_then(|v| v.as_str()) {
                            model_id = m.to_string();
                        }
                        request_json = new_body;
                    }
                    if let Some(new_url) = override_base_url {
                        effective_endpoint.base_url = new_url;
                    }
                    for (k, v) in upsert_headers {
                        effective_endpoint.extra_headers.insert(k, v);
                    }
                    if let Some(new_key) = override_api_key {
                        effective_endpoint.api_key = new_key;
                    }
                }
                Ok(ModelRequestInterceptOutcome::RespondImmediate { response }) => {
                    short_circuit_response = Some(*response);
                    break;
                }
                Ok(ModelRequestInterceptOutcome::Reject { reason }) => {
                    return Err(CoreError::ResponseFailed(format!(
                        "Model request rejected by daemon interceptor '{}': {reason}",
                        interceptor.id()
                    )));
                }
                Err(err) => match interceptor.fail_policy() {
                    ExtensionFailPolicy::FailOpen => continue,
                    ExtensionFailPolicy::FailClosed => {
                        return Err(CoreError::ResponseFailed(format!(
                            "Daemon interceptor '{}' failed in before_model_request: {err}",
                            interceptor.id()
                        )))
                    }
                },
            }
        }

        // 2. Call underlying ModelBackend (unless short-circuited by an interceptor)
        let started_at = std::time::Instant::now();
        let mut response = match short_circuit_response {
            Some(resp) => {
                if let Some(ref cb) = on_delta {
                    emit_synthetic_deltas_from_items(&resp.output, cb);
                }
                resp
            }
            None => {
                if on_delta.is_some() {
                    self.inner
                        .create_response_raw_streaming(&effective_endpoint, request_json, on_delta)
                        .await?
                } else {
                    self.inner
                        .create_response_raw(&effective_endpoint, request_json)
                        .await?
                }
            }
        };
        let duration_ms = started_at.elapsed().as_millis() as u64;

        // 3. Run `after_model_response` interceptors
        for interceptor in &self.chain.interceptors {
            let input = ModelResponseInterceptInput {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                activation_id: self.activation_id.clone(),
                turn_id: turn_id.clone(),
                model_id: model_id.clone(),
                duration_ms,
                response: response.clone(),
            };

            match interceptor.after_model_response(&input).await {
                Ok(ModelResponseInterceptOutcome::Continue {
                    response: maybe_new_resp,
                }) => {
                    if let Some(new_resp) = maybe_new_resp {
                        response = *new_resp;
                    }
                }
                Ok(ModelResponseInterceptOutcome::Reject { reason }) => {
                    return Err(CoreError::ResponseFailed(format!(
                        "Model response rejected by daemon interceptor '{}': {reason}",
                        interceptor.id()
                    )));
                }
                Err(err) => match interceptor.fail_policy() {
                    ExtensionFailPolicy::FailOpen => continue,
                    ExtensionFailPolicy::FailClosed => {
                        return Err(CoreError::ResponseFailed(format!(
                            "Daemon interceptor '{}' failed in after_model_response: {err}",
                            interceptor.id()
                        )))
                    }
                },
            }
        }

        Ok(response)
    }
}
