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
    #[error(
        "workspace_path '{workspace_path}' is not under any allowed workspace directory on node '{node_id}' (allowed: {allowed:?})"
    )]
    DisallowedWorkspacePath {
        node_id: String,
        workspace_path: String,
        allowed: Vec<String>,
    },
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
    /// Allowed parent directories for Pinned session `workspace_path` (e.g. `["/a/b/c", "/a/b/d"]`).
    /// `/tmp` (and system temp dir) is always included by default.
    #[serde(default = "default_allowed_workspace_dirs")]
    pub allowed_workspace_dirs: Vec<PathBuf>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Automatically query provider `/v1/models` endpoint(s) to discover available models,
    /// using explicitly configured `models` entries as supplements/overrides (default: `true`).
    #[serde(default = "default_auto_discover_models")]
    pub auto_discover_models: bool,
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

pub fn default_allowed_workspace_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::from("/tmp")];
    let sys_tmp = std::env::temp_dir();
    if !dirs.contains(&sys_tmp) {
        dirs.push(sys_tmp);
    }
    dirs
}

pub fn parse_workspace_dirs_csv(raw: &str) -> Vec<PathBuf> {
    let mut dirs = default_allowed_workspace_dirs();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            let p = PathBuf::from(trimmed);
            if !dirs.contains(&p) {
                dirs.push(p);
            }
        }
    }
    dirs
}

fn ensure_default_tmp_in_allowed_dirs(dirs: &mut Vec<PathBuf>) {
    for d in default_allowed_workspace_dirs() {
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    }
}

fn default_auto_discover_models() -> bool {
    true
}

impl DaemonConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, DaemonConfigError> {
        let content = std::fs::read_to_string(path)?;
        let mut cfg: Self = toml::from_str(&content)?;
        if let Ok(env_base_url) = std::env::var("RAGENTS_BASE_URL") {
            if !env_base_url.trim().is_empty() {
                for m in cfg.models.values_mut() {
                    if m.base_url.trim().is_empty() || m.base_url == "${RAGENTS_BASE_URL}" {
                        m.base_url = env_base_url.clone();
                    }
                }
                if let Ok(env_model) = std::env::var("RAGENTS_MODEL") {
                    let trimmed = env_model.trim();
                    if !trimmed.is_empty() {
                        cfg.models
                            .entry(trimmed.to_string())
                            .or_insert_with(|| LocalModelConfig {
                                base_url: env_base_url.clone(),
                                api_key: None,
                                api_key_env: Some("RAGENTS_API_KEY".to_string()),
                                extra_headers: BTreeMap::new(),
                                default_temperature: None,
                                default_max_output_tokens: None,
                            });
                        if cfg.default_model.is_none() {
                            cfg.default_model = Some(trimmed.to_string());
                        }
                    }
                } else if cfg.models.is_empty() {
                    // Seed a provider entry from RAGENTS_BASE_URL so `/v1/models` discovery can run
                    // even when no explicit model name was specified in daemon.toml or RAGENTS_MODEL.
                    cfg.models.insert(
                        "gpt-4.1".to_string(),
                        LocalModelConfig {
                            base_url: env_base_url.clone(),
                            api_key: None,
                            api_key_env: Some("RAGENTS_API_KEY".to_string()),
                            extra_headers: BTreeMap::new(),
                            default_temperature: None,
                            default_max_output_tokens: None,
                        },
                    );
                }
            }
        }
        if let Ok(raw_flag) = std::env::var("RAGENTS_AUTO_DISCOVER_MODELS") {
            match raw_flag.trim().to_ascii_lowercase().as_str() {
                "false" | "0" | "no" | "off" => cfg.auto_discover_models = false,
                "true" | "1" | "yes" | "on" => cfg.auto_discover_models = true,
                _ => {}
            }
        }
        if let Ok(raw_dirs) = std::env::var("RAGENTS_ALLOWED_WORKSPACE_DIRS") {
            if !raw_dirs.trim().is_empty() {
                cfg.allowed_workspace_dirs = parse_workspace_dirs_csv(&raw_dirs);
            }
        }
        ensure_default_tmp_in_allowed_dirs(&mut cfg.allowed_workspace_dirs);
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

        let allowed_workspace_dirs = std::env::var("RAGENTS_ALLOWED_WORKSPACE_DIRS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| parse_workspace_dirs_csv(&s))
            .unwrap_or_else(default_allowed_workspace_dirs);

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
            allowed_workspace_dirs,
            labels: BTreeMap::new(),
            auto_discover_models: default_auto_discover_models(),
            models,
            extensions: Vec::new(),
            interceptors: Vec::new(),
            default_permissions: PermissionPolicyLayer::default(),
        }
    }

    pub fn effective_allowed_workspace_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = self.allowed_workspace_dirs.clone();
        ensure_default_tmp_in_allowed_dirs(&mut dirs);
        if !dirs.contains(&self.temp_root_dir) {
            dirs.push(self.temp_root_dir.clone());
        }
        dirs
    }

    pub fn effective_allowed_workspace_dir_strings(&self) -> Vec<String> {
        self.effective_allowed_workspace_dirs()
            .into_iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect()
    }

    pub fn validate_workspace_path(&self, workspace_path: &Path) -> Result<(), DaemonConfigError> {
        let allowed = self.effective_allowed_workspace_dirs();
        if is_workspace_path_allowed(workspace_path, &allowed) {
            Ok(())
        } else {
            Err(DaemonConfigError::DisallowedWorkspacePath {
                node_id: self.node_id.to_string(),
                workspace_path: workspace_path.display().to_string(),
                allowed: self.effective_allowed_workspace_dir_strings(),
            })
        }
    }

    pub fn supported_models(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }

    pub fn supported_models_with_discovered(
        &self,
        discovered: &BTreeMap<String, LocalModelConfig>,
    ) -> Vec<String> {
        let mut all: std::collections::BTreeSet<String> = self.models.keys().cloned().collect();
        for k in discovered.keys() {
            all.insert(k.clone());
        }
        all.into_iter().collect()
    }

    /// Queries `/v1/models` on each distinct provider `base_url` configured in `self.models`
    /// (or `RAGENTS_BASE_URL` if set) when `self.auto_discover_models` is `true`.
    /// Explicitly configured `self.models` entries always act as supplements/overrides.
    pub async fn discover_provider_models(&self) -> BTreeMap<String, LocalModelConfig> {
        let mut discovered = BTreeMap::new();
        if !self.auto_discover_models {
            return discovered;
        }

        let mut providers: BTreeMap<String, LocalModelConfig> = BTreeMap::new();
        for cfg in self.models.values() {
            let trimmed = cfg.base_url.trim().trim_end_matches('/').to_string();
            if trimmed.is_empty() || trimmed == "${RAGENTS_BASE_URL}" || trimmed.contains(".internal")
            {
                continue;
            }
            providers
                .entry(trimmed.clone())
                .or_insert_with(|| LocalModelConfig {
                    base_url: cfg.base_url.clone(),
                    api_key: cfg.api_key.clone(),
                    api_key_env: cfg.api_key_env.clone(),
                    extra_headers: cfg.extra_headers.clone(),
                    default_temperature: None,
                    default_max_output_tokens: None,
                });
        }

        if let Ok(env_base_url) = std::env::var("RAGENTS_BASE_URL") {
            let trimmed = env_base_url.trim().trim_end_matches('/').to_string();
            if !trimmed.is_empty() && !trimmed.contains(".internal") {
                providers
                    .entry(trimmed)
                    .or_insert_with(|| LocalModelConfig {
                        base_url: env_base_url,
                        api_key: None,
                        api_key_env: Some("RAGENTS_API_KEY".to_string()),
                        extra_headers: BTreeMap::new(),
                        default_temperature: None,
                        default_max_output_tokens: None,
                    });
            }
        }

        if providers.is_empty() {
            return discovered;
        }

        let http_client = match reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(6))
            .build()
        {
            Ok(c) => c,
            Err(_) => return discovered,
        };

        for (base_trimmed, template_cfg) in providers {
            let api_key = template_cfg.resolve_api_key("discovery").ok();
            let candidate_urls = if base_trimmed.ends_with("/v1") {
                vec![
                    format!("{base_trimmed}/models"),
                    format!("{base_trimmed}/v1/models"),
                ]
            } else {
                vec![
                    format!("{base_trimmed}/v1/models"),
                    format!("{base_trimmed}/models"),
                ]
            };

            for url in candidate_urls {
                let mut req = http_client.get(&url);
                if let Some(ref key) = api_key {
                    req = req.bearer_auth(key);
                }
                for (hk, hv) in &template_cfg.extra_headers {
                    req = req.header(hk, hv);
                }
                let Ok(resp) = req.send().await else {
                    continue;
                };
                if !resp.status().is_success() {
                    continue;
                }
                let Ok(json_val) = resp.json::<serde_json::Value>().await else {
                    continue;
                };
                let model_ids = extract_model_ids_from_v1_models_json(&json_val);
                if !model_ids.is_empty() {
                    for model_id in model_ids {
                        discovered
                            .entry(model_id)
                            .or_insert_with(|| template_cfg.clone());
                    }
                    break;
                }
            }
        }

        discovered
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
        self.merge_for_activation_with_discovered(&BTreeMap::new(), session, activation)
    }

    pub fn merge_for_activation_with_discovered(
        &self,
        discovered: &BTreeMap<String, LocalModelConfig>,
        session: &Session,
        activation: &Activation,
    ) -> Result<(ConfigRevision, PermissionSnapshot, ModelEndpointConfig), DaemonConfigError> {
        if let SchedulingMode::Pinned { workspace_path, .. } = &session.spec.scheduling {
            self.validate_workspace_path(workspace_path)?;
        }

        let requested_model_name = activation
            .spec
            .model_override
            .as_ref()
            .map(|m| m.name.clone())
            .or_else(|| session.spec.model.as_ref().map(|m| m.name.clone()))
            .or_else(|| self.default_model.clone())
            .or_else(|| self.models.keys().next().cloned())
            .or_else(|| discovered.keys().next().cloned())
            .ok_or_else(|| DaemonConfigError::NoModelsConfigured(self.node_id.to_string()))?;

        // Explicitly configured `self.models` takes priority over `/v1/models` discovered entries.
        let local_model = self
            .models
            .get(&requested_model_name)
            .or_else(|| discovered.get(&requested_model_name))
            .ok_or_else(|| DaemonConfigError::UnsupportedModel {
                node_id: self.node_id.to_string(),
                model: requested_model_name.clone(),
                supported: self.supported_models_with_discovered(discovered),
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

        let mut effective_permissions =
            merge_permission_layers(&self.default_permissions, &session.spec.permissions);
        for ext in &effective_extensions {
            if ext.enabled && !ext.allowed_dirs.is_empty() {
                let fr = effective_permissions
                    .file_read
                    .get_or_insert_with(|| RuleSet {
                        deny: None,
                        allow: Some(Vec::new()),
                        default: None,
                    });
                let allow_list = fr.allow.get_or_insert_with(Vec::new);
                for dir in &ext.allowed_dirs {
                    if !allow_list.contains(dir) {
                        allow_list.push(dir.clone());
                    }
                }
            }
        }

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

pub fn extract_model_ids_from_v1_models_json(val: &serde_json::Value) -> Vec<String> {
    let mut ids = Vec::new();
    let arr_opt = val
        .get("data")
        .and_then(|v| v.as_array())
        .or_else(|| val.get("models").and_then(|v| v.as_array()))
        .or_else(|| val.as_array());

    if let Some(arr) = arr_opt {
        for item in arr {
            let maybe_id = item
                .get("id")
                .and_then(|v| v.as_str())
                .or_else(|| item.get("name").and_then(|v| v.as_str()))
                .or_else(|| item.as_str());
            if let Some(raw_id) = maybe_id {
                let trimmed = raw_id.trim();
                if !trimmed.is_empty() && !ids.iter().any(|x| x == trimmed) {
                    ids.push(trimmed.to_string());
                }
            }
        }
    }
    ids
}

