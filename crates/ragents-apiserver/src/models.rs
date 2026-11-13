use crate::store::{ClusterModelSnapshot, SqliteStore, StoreError};
use chrono::{DateTime, Utc};
use ragents_protocol::{
    ClusterModelInfo, ClusterModelSource, ClusterModelsResponse, NodeId, NodePhase,
    ResolvedModelProvider,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::RwLock;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiserverConfigError {
    #[error("failed to read apiserver config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse apiserver config toml: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Error)]
#[error("model discovery failed: {0}")]
pub struct RefreshModelsError(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApiserverProviderConfig {
    #[serde(default)]
    pub name: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApiserverModelConfig {
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

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiserverConfig {
    #[serde(default = "default_true")]
    pub auto_discover_models: bool,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    pub providers: Vec<ApiserverProviderConfig>,
    #[serde(default)]
    pub models: BTreeMap<String, ApiserverModelConfig>,
}

impl Default for ApiserverConfig {
    fn default() -> Self {
        Self {
            auto_discover_models: true,
            default_model: None,
            providers: Vec::new(),
            models: BTreeMap::new(),
        }
    }
}

fn resolve_key(api_key: Option<&str>, api_key_env: Option<&str>) -> String {
    if let Some(inline) = api_key {
        let trimmed = inline.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("${") {
            return trimmed.to_string();
        }
    }
    if let Some(env_name) = api_key_env {
        if let Ok(val) = std::env::var(env_name.trim()) {
            if !val.trim().is_empty() {
                return val;
            }
        }
    }
    if let Ok(val) = std::env::var("RAGENTS_API_KEY") {
        if !val.trim().is_empty() {
            return val;
        }
    }
    String::new()
}

fn apply_env_overrides(cfg: &mut ApiserverConfig) {
    if let Ok(raw_flag) = std::env::var("RAGENTS_AUTO_DISCOVER_MODELS") {
        match raw_flag.trim().to_ascii_lowercase().as_str() {
            "false" | "0" | "no" | "off" => cfg.auto_discover_models = false,
            "true" | "1" | "yes" | "on" => cfg.auto_discover_models = true,
            _ => {}
        }
    }

    let env_base_urls: Vec<String> = std::env::var("RAGENTS_BASE_URL")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|part| part.trim().trim_end_matches('/').to_string())
                .filter(|part| !part.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let first_env_base = env_base_urls.first().cloned();
    if let Some(ref primary_base) = first_env_base {
        for p in &mut cfg.providers {
            if p.base_url.trim().is_empty() || p.base_url.trim() == "${RAGENTS_BASE_URL}" {
                p.base_url = primary_base.clone();
            }
        }
        for m in cfg.models.values_mut() {
            if m.base_url.trim().is_empty() || m.base_url.trim() == "${RAGENTS_BASE_URL}" {
                m.base_url = primary_base.clone();
            }
        }
        for url in &env_base_urls {
            let already = cfg
                .providers
                .iter()
                .any(|p| p.base_url.trim().trim_end_matches('/') == url);
            if !already {
                cfg.providers.push(ApiserverProviderConfig {
                    name: Some("env".to_string()),
                    base_url: url.clone(),
                    api_key: None,
                    api_key_env: Some("RAGENTS_API_KEY".to_string()),
                    extra_headers: BTreeMap::new(),
                    default_temperature: None,
                    default_max_output_tokens: None,
                });
            }
        }
    }

    if let Ok(env_model) = std::env::var("RAGENTS_MODEL") {
        let trimmed = env_model.trim();
        if !trimmed.is_empty() {
            if cfg.default_model.is_none() {
                cfg.default_model = Some(trimmed.to_string());
            }
            if let Some(ref primary_base) = first_env_base {
                cfg.models
                    .entry(trimmed.to_string())
                    .or_insert_with(|| ApiserverModelConfig {
                        base_url: primary_base.clone(),
                        api_key: None,
                        api_key_env: Some("RAGENTS_API_KEY".to_string()),
                        extra_headers: BTreeMap::new(),
                        default_temperature: None,
                        default_max_output_tokens: None,
                    });
            }
        }
    }

    // Remove entries that still contain unresolved placeholders
    cfg.providers.retain(|p| {
        let t = p.base_url.trim();
        !t.is_empty() && !t.starts_with("${")
    });
    cfg.models.retain(|_, m| {
        let t = m.base_url.trim();
        !t.is_empty() && !t.starts_with("${")
    });
}

impl ApiserverConfig {
    pub fn load_from_env() -> Self {
        if let Ok(cfg_path) = std::env::var("RAGENTS_APISERVER_CONFIG") {
            let trimmed = cfg_path.trim();
            if !trimmed.is_empty() {
                if let Ok(loaded) = Self::load_from_file(trimmed) {
                    return loaded;
                }
            }
        }
        let mut cfg = Self::default();
        apply_env_overrides(&mut cfg);
        cfg
    }

    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, ApiserverConfigError> {
        let content = std::fs::read_to_string(path)?;
        let mut cfg: Self = toml::from_str(&content)?;
        apply_env_overrides(&mut cfg);
        Ok(cfg)
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveredModelMeta {
    pub id: String,
    pub capabilities: Vec<String>,
    pub owned_by: Option<String>,
    pub provider: ApiserverProviderConfig,
}

#[derive(Debug, Default)]
struct CatalogState {
    discovered: BTreeMap<String, DiscoveredModelMeta>,
    last_refreshed_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

pub struct ClusterModelCatalog {
    config: ApiserverConfig,
    state: RwLock<CatalogState>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl ClusterModelCatalog {
    pub fn from_config(config: ApiserverConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CatalogState::default()),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn from_env() -> Self {
        Self::from_config(ApiserverConfig::load_from_env())
    }

    pub fn effective_default_model(&self) -> Option<String> {
        if let Some(ref d) = self.config.default_model {
            if !d.trim().is_empty() {
                return Some(d.clone());
            }
        }
        let guard = self.state.read().expect("catalog rwlock poisoned");
        self.config
            .models
            .keys()
            .next()
            .cloned()
            .or_else(|| guard.discovered.keys().next().cloned())
    }

    pub fn snapshot(&self) -> ClusterModelSnapshot {
        let guard = self.state.read().expect("catalog rwlock poisoned");
        let mut ids = BTreeSet::new();
        for id in self.config.models.keys() {
            ids.insert(id.clone());
        }
        for id in guard.discovered.keys() {
            ids.insert(id.clone());
        }
        let default_model = self
            .config
            .default_model
            .clone()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| ids.iter().next().cloned());
        ClusterModelSnapshot {
            default_model,
            model_ids: ids.into_iter().collect(),
        }
    }

    pub fn sync_into_store(&self, store: &SqliteStore) {
        let snap = self.snapshot();
        store.set_cluster_models(snap.default_model, snap.model_ids);
    }

    pub fn resolve_provider(&self, requested_model: Option<&str>) -> Option<ResolvedModelProvider> {
        let default_model = self.effective_default_model();
        let target_id = requested_model
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or(default_model)?;

        if let Some(cfg_model) = self.config.models.get(&target_id) {
            return Some(ResolvedModelProvider {
                model_id: target_id,
                base_url: cfg_model.base_url.clone(),
                api_key: resolve_key(
                    cfg_model.api_key.as_deref(),
                    cfg_model.api_key_env.as_deref(),
                ),
                extra_headers: cfg_model.extra_headers.clone(),
                default_temperature: cfg_model.default_temperature,
                default_max_output_tokens: cfg_model.default_max_output_tokens,
            });
        }

        let guard = self.state.read().expect("catalog rwlock poisoned");
        guard.discovered.get(&target_id).map(|meta| ResolvedModelProvider {
            model_id: target_id,
            base_url: meta.provider.base_url.clone(),
            api_key: resolve_key(
                meta.provider.api_key.as_deref(),
                meta.provider.api_key_env.as_deref(),
            ),
            extra_headers: meta.provider.extra_headers.clone(),
            default_temperature: meta.provider.default_temperature,
            default_max_output_tokens: meta.provider.default_max_output_tokens,
        })
    }

    pub fn list_models(&self, store: &SqliteStore) -> Result<ClusterModelsResponse, StoreError> {
        let nodes = store.list_nodes()?;
        let ready_cluster_nodes: Vec<NodeId> = nodes
            .iter()
            .filter(|n| n.status.phase == NodePhase::Ready && n.status.use_cluster_models)
            .map(|n| NodeId::new(n.metadata.id.clone()))
            .collect();

        let mut local_by_model: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
        for node in &nodes {
            if node.status.phase == NodePhase::Ready {
                let nid = NodeId::new(node.metadata.id.clone());
                for m in &node.status.local_models {
                    local_by_model.entry(m.clone()).or_default().push(nid.clone());
                }
            }
        }

        let guard = self.state.read().expect("catalog rwlock poisoned");
        let default_model = self.effective_default_model();

        let mut all_ids = BTreeSet::new();
        for id in self.config.models.keys() {
            all_ids.insert(id.clone());
        }
        for id in guard.discovered.keys() {
            all_ids.insert(id.clone());
        }
        for id in local_by_model.keys() {
            all_ids.insert(id.clone());
        }

        let mut models = Vec::with_capacity(all_ids.len());
        for id in all_ids {
            let static_cfg = self.config.models.get(&id);
            let disc_meta = guard.discovered.get(&id);
            let in_cluster = static_cfg.is_some() || disc_meta.is_some();
            let local_nodes = local_by_model.get(&id).cloned().unwrap_or_default();

            let source = match (in_cluster, !local_nodes.is_empty()) {
                (true, true) => ClusterModelSource::ClusterAndNodeLocal,
                (true, false) => ClusterModelSource::Cluster,
                (false, true) => ClusterModelSource::NodeLocal,
                (false, false) => continue,
            };

            let mut avail_set: BTreeSet<NodeId> = local_nodes.into_iter().collect();
            if in_cluster {
                for nid in &ready_cluster_nodes {
                    avail_set.insert(nid.clone());
                }
            }

            let capabilities = disc_meta
                .map(|m| m.capabilities.clone())
                .unwrap_or_default();
            let owned_by = disc_meta.and_then(|m| m.owned_by.clone());
            let default_temperature = static_cfg
                .and_then(|c| c.default_temperature)
                .or_else(|| disc_meta.and_then(|m| m.provider.default_temperature));
            let default_max_output_tokens = static_cfg
                .and_then(|c| c.default_max_output_tokens)
                .or_else(|| disc_meta.and_then(|m| m.provider.default_max_output_tokens));

            models.push(ClusterModelInfo {
                is_default: default_model.as_deref() == Some(id.as_str()),
                id,
                source,
                available_on_nodes: avail_set.into_iter().collect(),
                capabilities,
                owned_by,
                default_temperature,
                default_max_output_tokens,
            });
        }

        Ok(ClusterModelsResponse {
            default_model,
            models,
            last_refreshed_at: guard.last_refreshed_at,
            last_error: guard.last_error.clone(),
        })
    }

    pub async fn refresh(
        &self,
        store: Option<&SqliteStore>,
    ) -> Result<ClusterModelsResponse, RefreshModelsError> {
        let _guard = self.refresh_lock.lock().await;

        let mut provider_map: BTreeMap<String, ApiserverProviderConfig> = BTreeMap::new();
        if self.config.auto_discover_models {
            for p in &self.config.providers {
                let key = p.base_url.trim().trim_end_matches('/').to_string();
                if key.is_empty() || key.starts_with("${") || key.contains(".internal") {
                    continue;
                }
                provider_map.entry(key).or_insert_with(|| p.clone());
            }
            for m in self.config.models.values() {
                let key = m.base_url.trim().trim_end_matches('/').to_string();
                if key.is_empty() || key.starts_with("${") || key.contains(".internal") {
                    continue;
                }
                provider_map
                    .entry(key)
                    .or_insert_with(|| ApiserverProviderConfig {
                        name: None,
                        base_url: m.base_url.clone(),
                        api_key: m.api_key.clone(),
                        api_key_env: m.api_key_env.clone(),
                        extra_headers: m.extra_headers.clone(),
                        default_temperature: m.default_temperature,
                        default_max_output_tokens: m.default_max_output_tokens,
                    });
            }
        }

        if provider_map.is_empty() {
            {
                let mut state = self.state.write().expect("catalog rwlock poisoned");
                state.last_refreshed_at = Some(Utc::now());
                state.last_error = None;
            }
            if let Some(st) = store {
                self.sync_into_store(st);
                return self
                    .list_models(st)
                    .map_err(|e| RefreshModelsError(e.to_string()));
            }
            return Ok(ClusterModelsResponse::default());
        }

        let http_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| RefreshModelsError(format!("failed to build HTTP client: {e}")))?;

        let mut discovered: BTreeMap<String, DiscoveredModelMeta> = BTreeMap::new();
        let mut succeeded_providers = 0usize;
        let mut errors: Vec<String> = Vec::new();

        for (base_trimmed, provider) in provider_map {
            let api_key = resolve_key(provider.api_key.as_deref(), provider.api_key_env.as_deref());
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

            let mut last_provider_err = None;
            let mut provider_ok = false;

            for url in candidate_urls {
                let mut req = http_client.get(&url);
                if !api_key.is_empty() {
                    req = req.bearer_auth(&api_key);
                }
                for (hk, hv) in &provider.extra_headers {
                    req = req.header(hk, hv);
                }

                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        last_provider_err = Some(format!("{url}: {e}"));
                        continue;
                    }
                };
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    last_provider_err = Some(format!("{url}: HTTP {status} {body}"));
                    continue;
                }
                let json_val = match resp.json::<serde_json::Value>().await {
                    Ok(v) => v,
                    Err(e) => {
                        last_provider_err = Some(format!("{url}: invalid JSON: {e}"));
                        continue;
                    }
                };

                let entries = extract_models_from_v1_models_json(&json_val, &provider);
                for entry in entries {
                    discovered.entry(entry.id.clone()).or_insert(entry);
                }
                provider_ok = true;
                break;
            }

            if provider_ok {
                succeeded_providers += 1;
            } else if let Some(err) = last_provider_err {
                errors.push(err);
            }
        }

        if succeeded_providers == 0 && !errors.is_empty() {
            let joined = errors.join("; ");
            {
                let mut state = self.state.write().expect("catalog rwlock poisoned");
                state.last_error = Some(joined.clone());
            }
            return Err(RefreshModelsError(joined));
        }

        {
            let mut state = self.state.write().expect("catalog rwlock poisoned");
            state.discovered = discovered;
            state.last_refreshed_at = Some(Utc::now());
            state.last_error = if errors.is_empty() {
                None
            } else {
                Some(errors.join("; "))
            };
        }

        if let Some(st) = store {
            self.sync_into_store(st);
            self.list_models(st)
                .map_err(|e| RefreshModelsError(e.to_string()))
        } else {
            Ok(ClusterModelsResponse::default())
        }
    }
}

pub fn extract_models_from_v1_models_json(
    val: &serde_json::Value,
    provider: &ApiserverProviderConfig,
) -> Vec<DiscoveredModelMeta> {
    let mut out = Vec::new();
    let arr_opt = val
        .get("data")
        .and_then(|v| v.as_array())
        .or_else(|| val.get("models").and_then(|v| v.as_array()))
        .or_else(|| val.as_array());

    let Some(arr) = arr_opt else {
        return out;
    };

    for item in arr {
        let maybe_id = item
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| item.get("name").and_then(|v| v.as_str()))
            .or_else(|| item.as_str());
        let Some(raw_id) = maybe_id else {
            continue;
        };
        let id = raw_id.trim();
        if id.is_empty() || out.iter().any(|x: &DiscoveredModelMeta| x.id == id) {
            continue;
        }
        let capabilities = item
            .get("capabilities")
            .and_then(|v| v.as_array())
            .map(|caps| {
                caps.iter()
                    .filter_map(|c| c.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let owned_by = item
            .get("owned_by")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        out.push(DiscoveredModelMeta {
            id: id.to_string(),
            capabilities,
            owned_by,
            provider: provider.clone(),
        });
    }
    out
}
