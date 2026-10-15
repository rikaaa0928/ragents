use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use ragents_core::ControlPluginRunner;
use ragents_protocol::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

// ============================================================================
// 1. Native Rust Extension Traits for `api-server`
// ============================================================================

#[async_trait]
pub trait AuthnPlugin: Send + Sync {
    fn id(&self) -> &str;

    fn scopes(&self) -> &[AuthScope] {
        &[AuthScope::ClientHttp, AuthScope::DaemonTunnel]
    }

    async fn authenticate(&self, req: &AuthnRequest) -> Result<AuthnResponse, String>;
}

#[async_trait]
pub trait AuthzPlugin: Send + Sync {
    fn id(&self) -> &str;

    async fn authorize(&self, req: &AuthzRequest) -> Result<AuthzResponse, String>;
}

#[async_trait]
pub trait AdmissionPlugin: Send + Sync {
    fn id(&self) -> &str;

    fn phase(&self) -> AdmissionPhase;

    fn priority(&self) -> i32 {
        100
    }

    fn fail_policy(&self) -> ExtensionFailPolicy {
        ExtensionFailPolicy::FailClosed
    }

    async fn admit(&self, req: &AdmissionRequest) -> Result<AdmissionResponse, String>;
}

// ============================================================================
// 2. Declarative Config Specs & WASM / Webhook Drivers
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthnPluginSpec {
    pub id: String,
    #[serde(flatten)]
    pub driver: ExtensionDriverConfig,
    #[serde(default)]
    pub scopes: Vec<AuthScope>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzPluginSpec {
    pub id: String,
    #[serde(flatten)]
    pub driver: ExtensionDriverConfig,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub fail_policy: ExtensionFailPolicy,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionPluginSpec {
    pub id: String,
    pub phase: AdmissionPhase,
    #[serde(flatten)]
    pub driver: ExtensionDriverConfig,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub fail_policy: ExtensionFailPolicy,
    #[serde(default)]
    pub config: serde_json::Value,
}

fn default_priority() -> i32 {
    100
}

fn default_timeout_ms() -> u64 {
    2000
}

pub struct ConfiguredAuthnPlugin {
    spec: AuthnPluginSpec,
    runner: ControlPluginRunner,
}

impl ConfiguredAuthnPlugin {
    pub fn new(spec: AuthnPluginSpec) -> Self {
        Self {
            spec,
            runner: ControlPluginRunner::new(),
        }
    }
}

#[async_trait]
impl AuthnPlugin for ConfiguredAuthnPlugin {
    fn id(&self) -> &str {
        &self.spec.id
    }

    fn scopes(&self) -> &[AuthScope] {
        if self.spec.scopes.is_empty() {
            &[AuthScope::ClientHttp, AuthScope::DaemonTunnel]
        } else {
            &self.spec.scopes
        }
    }

    async fn authenticate(&self, req: &AuthnRequest) -> Result<AuthnResponse, String> {
        let inv = ControlHookInvocation {
            hook_name: "authn".to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(req).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "authn plugin returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }
}

pub struct ConfiguredAuthzPlugin {
    spec: AuthzPluginSpec,
    runner: ControlPluginRunner,
}

impl ConfiguredAuthzPlugin {
    pub fn new(spec: AuthzPluginSpec) -> Self {
        Self {
            spec,
            runner: ControlPluginRunner::new(),
        }
    }
}

#[async_trait]
impl AuthzPlugin for ConfiguredAuthzPlugin {
    fn id(&self) -> &str {
        &self.spec.id
    }

    async fn authorize(&self, req: &AuthzRequest) -> Result<AuthzResponse, String> {
        let inv = ControlHookInvocation {
            hook_name: "authz".to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(req).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "authz plugin returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }
}

pub struct ConfiguredAdmissionPlugin {
    spec: AdmissionPluginSpec,
    runner: ControlPluginRunner,
}

impl ConfiguredAdmissionPlugin {
    pub fn new(spec: AdmissionPluginSpec) -> Self {
        Self {
            spec,
            runner: ControlPluginRunner::new(),
        }
    }
}

#[async_trait]
impl AdmissionPlugin for ConfiguredAdmissionPlugin {
    fn id(&self) -> &str {
        &self.spec.id
    }

    fn phase(&self) -> AdmissionPhase {
        self.spec.phase
    }

    fn priority(&self) -> i32 {
        self.spec.priority
    }

    fn fail_policy(&self) -> ExtensionFailPolicy {
        self.spec.fail_policy
    }

    async fn admit(&self, req: &AdmissionRequest) -> Result<AdmissionResponse, String> {
        let hook_name = match self.spec.phase {
            AdmissionPhase::Mutating => "admission_mutating",
            AdmissionPhase::Validating => "admission_validating",
        };
        let inv = ControlHookInvocation {
            hook_name: hook_name.to_string(),
            plugin_config: self.spec.config.clone(),
            payload: serde_json::to_value(req).map_err(|e| e.to_string())?,
        };
        let res = self
            .runner
            .invoke(&self.spec.id, &self.spec.driver, self.spec.timeout_ms, &inv)
            .await
            .map_err(|e| e.to_string())?;
        if !res.ok {
            return Err(res
                .error
                .unwrap_or_else(|| "admission plugin returned ok=false".to_string()));
        }
        serde_json::from_value(res.output).map_err(|e| e.to_string())
    }
}

// ============================================================================
// 3. ApiServerExtensions Pipeline Registry
// ============================================================================

#[derive(Clone, Default)]
pub struct ApiServerExtensions {
    authn_plugins: Vec<Arc<dyn AuthnPlugin>>,
    authz_plugins: Vec<Arc<dyn AuthzPlugin>>,
    admission_plugins: Vec<Arc<dyn AdmissionPlugin>>,
}

impl ApiServerExtensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_authn(&mut self, plugin: Arc<dyn AuthnPlugin>) {
        self.authn_plugins.push(plugin);
    }

    pub fn add_authz(&mut self, plugin: Arc<dyn AuthzPlugin>) {
        self.authz_plugins.push(plugin);
    }

    pub fn add_admission(&mut self, plugin: Arc<dyn AdmissionPlugin>) {
        self.admission_plugins.push(plugin);
        self.admission_plugins.sort_by_key(|p| p.priority());
    }

    pub async fn authenticate(
        &self,
        scope: AuthScope,
        method: &str,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<Principal, (StatusCode, String)> {
        if self.authn_plugins.is_empty() {
            return Ok(Principal::anonymous());
        }

        let mut header_map = BTreeMap::new();
        for (k, v) in headers {
            if let Ok(val_str) = v.to_str() {
                header_map.insert(k.as_str().to_ascii_lowercase(), val_str.to_string());
            }
        }

        let req = AuthnRequest {
            scope,
            method: method.to_string(),
            path: path.to_string(),
            headers: header_map,
        };

        for plugin in &self.authn_plugins {
            if !plugin.scopes().contains(&scope) {
                continue;
            }
            match plugin.authenticate(&req).await {
                Ok(AuthnResponse::Authenticated { principal }) => return Ok(principal),
                Ok(AuthnResponse::Unauthenticated { message }) => {
                    return Err((StatusCode::UNAUTHORIZED, message))
                }
                Ok(AuthnResponse::Abstain) => continue,
                Err(err) => {
                    return Err((
                        StatusCode::UNAUTHORIZED,
                        format!("authn plugin '{}' error: {err}", plugin.id()),
                    ))
                }
            }
        }

        Err((
            StatusCode::UNAUTHORIZED,
            "Unauthenticated: missing or invalid credentials".to_string(),
        ))
    }

    pub async fn authorize(
        &self,
        principal: &Principal,
        verb: ResourceVerb,
        resource: ResourceKind,
        resource_id: Option<&str>,
        resource_labels: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, (StatusCode, String)> {
        if self.authz_plugins.is_empty() {
            return Ok(BTreeMap::new());
        }

        let req = AuthzRequest {
            principal: principal.clone(),
            verb,
            resource,
            resource_id: resource_id.map(String::from),
            resource_labels: resource_labels.clone(),
        };

        let mut merged_selector = BTreeMap::new();
        for plugin in &self.authz_plugins {
            match plugin.authorize(&req).await {
                Ok(AuthzResponse::Allow {
                    enforced_label_selector,
                }) => {
                    for (k, v) in enforced_label_selector {
                        merged_selector.insert(k, v);
                    }
                }
                Ok(AuthzResponse::Deny { reason }) => {
                    return Err((StatusCode::FORBIDDEN, reason));
                }
                Err(err) => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        format!("authz plugin '{}' error: {err}", plugin.id()),
                    ));
                }
            }
        }

        Ok(merged_selector)
    }

    pub async fn admit<T>(
        &self,
        principal: &Principal,
        operation: AdmissionOperation,
        payload: T,
    ) -> Result<T, (StatusCode, String)>
    where
        T: Serialize + DeserializeOwned,
    {
        if self.admission_plugins.is_empty() {
            return Ok(payload);
        }

        let mut current_val = serde_json::to_value(&payload).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("admission serialize error: {e}"),
            )
        })?;

        // 1. Mutating Admission Phase
        for plugin in self
            .admission_plugins
            .iter()
            .filter(|p| p.phase() == AdmissionPhase::Mutating)
        {
            let req = AdmissionRequest {
                phase: AdmissionPhase::Mutating,
                principal: principal.clone(),
                operation,
                payload: current_val.clone(),
            };
            match plugin.admit(&req).await {
                Ok(AdmissionResponse::Allowed { mutated_payload }) => {
                    if let Some(new_val) = mutated_payload {
                        current_val = new_val;
                    }
                }
                Ok(AdmissionResponse::Denied { code, message }) => {
                    let status = StatusCode::from_u16(code).unwrap_or(StatusCode::FORBIDDEN);
                    return Err((status, message));
                }
                Err(err) => match plugin.fail_policy() {
                    ExtensionFailPolicy::FailOpen => continue,
                    ExtensionFailPolicy::FailClosed => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("mutating admission plugin '{}' failed: {err}", plugin.id()),
                        ));
                    }
                },
            }
        }

        // 2. Validating Admission Phase
        for plugin in self
            .admission_plugins
            .iter()
            .filter(|p| p.phase() == AdmissionPhase::Validating)
        {
            let req = AdmissionRequest {
                phase: AdmissionPhase::Validating,
                principal: principal.clone(),
                operation,
                payload: current_val.clone(),
            };
            match plugin.admit(&req).await {
                Ok(AdmissionResponse::Allowed { .. }) => {}
                Ok(AdmissionResponse::Denied { code, message }) => {
                    let status = StatusCode::from_u16(code).unwrap_or(StatusCode::FORBIDDEN);
                    return Err((status, message));
                }
                Err(err) => match plugin.fail_policy() {
                    ExtensionFailPolicy::FailOpen => continue,
                    ExtensionFailPolicy::FailClosed => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!(
                                "validating admission plugin '{}' failed: {err}",
                                plugin.id()
                            ),
                        ));
                    }
                },
            }
        }

        serde_json::from_value(current_val).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("mutated payload failed schema validation: {e}"),
            )
        })
    }
}

pub fn labels_match_selector(
    labels: &BTreeMap<String, String>,
    selector: &BTreeMap<String, String>,
) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}
