use ragents_core::ModelEndpointConfig;
use ragents_protocol::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("no models configured on daemon node '{0}'")]
    NoModelsConfigured(String),
    #[error(
        "requested model '{model}' is not supported on node '{node_id}' (supported: {supported:?})"
    )]
    UnsupportedModel {
        node_id: String,
        model: String,
        supported: Vec<String>,
    },
    #[error("missing api_key for model '{0}'")]
    MissingApiKey(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalModelConfig {
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub extra_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub default_temperature: Option<f64>,
    #[serde(default)]
    pub default_max_output_tokens: Option<i32>,
}

impl LocalModelConfig {
    pub fn resolve_api_key(&self, model_name: &str) -> Result<String, DaemonConfigError> {
        if let Some(ref key) = self.api_key {
            if !key.trim().is_empty() {
                return Ok(key.clone());
            }
        }
        if let Some(ref env_var) = self.api_key_env {
            if let Ok(val) = std::env::var(env_var) {
                if !val.trim().is_empty() {
                    return Ok(val);
                }
            }
        }
        if let Ok(val) = std::env::var("RAGENTS_API_KEY") {
            if !val.trim().is_empty() {
                return Ok(val);
            }
        }
        Err(DaemonConfigError::MissingApiKey(model_name.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorMode {
    #[default]
    Subprocess,
    InProcess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    pub node_id: NodeId,
    #[serde(default = "default_server_url")]
    pub server_url: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub executor_mode: ExecutorMode,
    #[serde(default = "default_max_concurrent_runners")]
    pub max_concurrent_runners: u32,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default = "default_system_prompt")]
    pub default_system_prompt: String,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default = "default_temp_root")]
    pub temp_root_dir: PathBuf,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub models: BTreeMap<String, LocalModelConfig>,
    #[serde(default)]
    pub extensions: Vec<ExtensionSpec>,
    #[serde(default)]
    pub interceptors: Vec<DaemonInterceptorSpec>,
    #[serde(default)]
    pub default_permissions: PermissionPolicyLayer,
}

fn default_server_url() -> String {
    "http://127.0.0.1:8080".to_string()
}

fn default_max_concurrent_runners() -> u32 {
    4
}

fn default_system_prompt() -> String {
    "You are a helpful AI assistant managed by ragents.".to_string()
}

fn default_temp_root() -> PathBuf {
    std::env::temp_dir().join("ragents")
}

impl DaemonConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, DaemonConfigError> {
        let content = std::fs::read_to_string(path)?;
        let cfg: Self = toml::from_str(&content)?;
        Ok(cfg)
    }

    pub fn default_local(node_id: impl Into<NodeId>, server_url: impl Into<String>) -> Self {
        let base_url = std::env::var("RAGENTS_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8000/v1".to_string());
        let api_key =
            std::env::var("RAGENTS_API_KEY").unwrap_or_else(|_| "sk-placeholder".to_string());
        let model_name = std::env::var("RAGENTS_MODEL").unwrap_or_else(|_| "gpt-4.1".to_string());

        let mut models = BTreeMap::new();
        models.insert(
            model_name.clone(),
            LocalModelConfig {
                base_url,
                api_key: Some(api_key),
                api_key_env: None,
                extra_headers: BTreeMap::new(),
                default_temperature: None,
                default_max_output_tokens: None,
            },
        );

        Self {
            node_id: node_id.into(),
            server_url: server_url.into(),
            auth_token: std::env::var("RAGENTS_AUTH_TOKEN").ok(),
            executor_mode: ExecutorMode::Subprocess,
            max_concurrent_runners: default_max_concurrent_runners(),
            max_turns: None,
            default_system_prompt: default_system_prompt(),
            default_model: Some(model_name),
            temp_root_dir: default_temp_root(),
            labels: BTreeMap::new(),
            models,
            extensions: Vec::new(),
            interceptors: Vec::new(),
            default_permissions: PermissionPolicyLayer::default(),
        }
    }

    pub fn supported_models(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }

    pub fn installed_extension_names(&self) -> Vec<String> {
        self.extensions
            .iter()
            .filter(|e| e.enabled)
            .map(|e| e.name.clone())
            .collect()
    }

    /// Merges node-level defaults with `SessionSpec` and `ActivationSpec` to produce:
    /// 1. Content-addressed `ConfigRevision` (without secrets)
    /// 2. Content-addressed `PermissionSnapshot`
    /// 3. Node-local `ModelEndpointConfig` (`base_url` + `api_key`)
    pub fn merge_for_activation(
        &self,
        session: &Session,
        activation: &Activation,
    ) -> Result<(ConfigRevision, PermissionSnapshot, ModelEndpointConfig), DaemonConfigError> {
        let requested_model_name = activation
            .spec
            .model_override
            .as_ref()
            .map(|m| m.name.clone())
            .or_else(|| session.spec.model.as_ref().map(|m| m.name.clone()))
            .or_else(|| self.default_model.clone())
            .or_else(|| self.models.keys().next().cloned())
            .ok_or_else(|| DaemonConfigError::NoModelsConfigured(self.node_id.to_string()))?;

        let local_model = self.models.get(&requested_model_name).ok_or_else(|| {
            DaemonConfigError::UnsupportedModel {
                node_id: self.node_id.to_string(),
                model: requested_model_name.clone(),
                supported: self.supported_models(),
            }
        })?;

        let api_key = local_model.resolve_api_key(&requested_model_name)?;
        let endpoint = ModelEndpointConfig {
            base_url: local_model.base_url.clone(),
            api_key,
            extra_headers: local_model.extra_headers.clone(),
        };

        let temperature = activation
            .spec
            .model_override
            .as_ref()
            .and_then(|m| m.temperature)
            .or_else(|| session.spec.model.as_ref().and_then(|m| m.temperature))
            .or(local_model.default_temperature);

        let max_output_tokens = activation
            .spec
            .model_override
            .as_ref()
            .and_then(|m| m.max_output_tokens)
            .or_else(|| {
                session
                    .spec
                    .model
                    .as_ref()
                    .and_then(|m| m.max_output_tokens)
            })
            .or(local_model.default_max_output_tokens);

        let reasoning = activation
            .spec
            .model_override
            .as_ref()
            .and_then(|m| m.reasoning.clone())
            .or_else(|| {
                session
                    .spec
                    .model
                    .as_ref()
                    .and_then(|m| m.reasoning.clone())
            });

        let effective_model = ModelRequestSpec {
            name: requested_model_name,
            temperature,
            max_output_tokens,
            reasoning,
        };

        let effective_system_prompt = match &session.spec.system_prompt {
            Some(s) if !s.trim().is_empty() => s.clone(),
            _ => self.default_system_prompt.clone(),
        };

        // Merge daemon extensions with session extensions (session overrides by name)
        let mut ext_map: BTreeMap<String, ExtensionSpec> = BTreeMap::new();
        for ext in &self.extensions {
            ext_map.insert(ext.name.clone(), ext.clone());
        }
        for ext in &session.spec.extensions {
            ext_map.insert(ext.name.clone(), ext.clone());
        }
        let effective_extensions: Vec<ExtensionSpec> = ext_map.into_values().collect();

        let effective_permissions =
            merge_permission_layers(&self.default_permissions, &session.spec.permissions);

        let config_rev = ConfigRevision::new_merged(
            effective_system_prompt,
            effective_model,
            effective_extensions,
        );
        let perm_snap = PermissionSnapshot::from_effective_policy(effective_permissions);

        Ok((config_rev, perm_snap, endpoint))
    }
}

pub fn merge_permission_layers(
    base: &PermissionPolicyLayer,
    override_layer: &PermissionPolicyLayer,
) -> PermissionPolicyLayer {
    PermissionPolicyLayer {
        file_read: merge_rule_set(base.file_read.as_ref(), override_layer.file_read.as_ref()),
        file_write: merge_rule_set(base.file_write.as_ref(), override_layer.file_write.as_ref()),
        http_hosts: merge_rule_set(base.http_hosts.as_ref(), override_layer.http_hosts.as_ref()),
        command_work_dir: merge_rule_set(
            base.command_work_dir.as_ref(),
            override_layer.command_work_dir.as_ref(),
        ),
    }
}

fn merge_rule_set<R: Clone>(
    base: Option<&RuleSet<R>>,
    over: Option<&RuleSet<R>>,
) -> Option<RuleSet<R>> {
    match (base, over) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => Some(RuleSet {
            deny: o.deny.clone().or_else(|| b.deny.clone()),
            allow: o.allow.clone().or_else(|| b.allow.clone()),
            default: o.default.or(b.default),
        }),
    }
}
