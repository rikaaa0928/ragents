use crate::error::CoreError;
use crate::hooks::protocol::*;
use crate::hooks::runtime::WasmPlugin;
use futures::future::join_all;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct Subscriber {
    plugin: Arc<WasmPlugin>,
    subscription: HookSubscription,
}

pub struct HookManager {
    plugins: Vec<Arc<WasmPlugin>>,
    plugin_config: HashMap<String, Value>,
    invocation_id: AtomicU64,
    tool_id: AtomicU64,
    initialized_plugins: std::sync::Mutex<Vec<Arc<WasmPlugin>>>,
}

impl Default for HookManager {
    fn default() -> Self {
        Self::empty()
    }
}

impl HookManager {
    pub fn empty() -> Self {
        Self {
            plugins: vec![],
            plugin_config: HashMap::new(),
            invocation_id: AtomicU64::new(1),
            tool_id: AtomicU64::new(1),
            initialized_plugins: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn add_plugin_with_config(
        &mut self,
        plugin: WasmPlugin,
        config: Value,
    ) -> Result<(), CoreError> {
        let id = plugin.metadata().id.clone();
        if self.plugins.iter().any(|p| p.metadata().id == id) {
            return Err(CoreError::ToolError(format!(
                "duplicate extension id '{id}'"
            )));
        }
        self.plugin_config.insert(id, config);
        self.plugins.push(Arc::new(plugin));
        Ok(())
    }

    pub fn plugins(&self) -> &[Arc<WasmPlugin>] {
        &self.plugins
    }

    pub async fn initialize(&self) -> Result<(), CoreError> {
        {
            let mut init_guard = self.initialized_plugins.lock().unwrap();
            init_guard.clear();
        }

        for plugin in &self.plugins {
            let config_val = self
                .plugin_config
                .get(&plugin.metadata().id)
                .unwrap_or(&Value::Null);

            match plugin.initialize(config_val).await {
                Ok(()) => {
                    let mut init_guard = self.initialized_plugins.lock().unwrap();
                    init_guard.push(Arc::clone(plugin));
                }
                Err(e) => {
                    let _ = self.shutdown().await;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), CoreError> {
        let plugins_to_shutdown: Vec<Arc<WasmPlugin>> = {
            let mut guard = self.initialized_plugins.lock().unwrap();
            guard.drain(..).collect()
        };

        let mut first_error = None;
        for plugin in plugins_to_shutdown.into_iter().rev() {
            if let Err(e) = plugin.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn request(&self, hook: &str, iteration: Option<usize>, payload: Value) -> HookRequest {
        HookRequest {
            hook: hook.into(),
            protocol_version: 1,
            invocation_id: self.invocation_id.fetch_add(1, Ordering::Relaxed),
            iteration,
            payload,
        }
    }

    pub async fn transform_validated<F>(
        &self,
        hook: &str,
        iteration: Option<usize>,
        mut payload: Value,
        validate: F,
    ) -> Result<TransformResult, CoreError>
    where
        F: Fn(&Value) -> Result<(), String>,
    {
        validate(&payload).map_err(|reason| {
            CoreError::ToolError(format!(
                "core produced invalid payload for '{hook}': {reason}"
            ))
        })?;
        for sub in self.subscribers(hook, HookKind::Transform) {
            let before = payload.clone();
            let response = sub
                .plugin
                .invoke(&self.request(hook, iteration, before))
                .await
                .and_then(parse_hook_result);
            let result = match response {
                Ok(result) => result,
                Err(_) if sub.subscription.failure == HookFailurePolicy::Ignore => continue,
                Err(e) => return Err(extension_error(&sub, hook, e)),
            };
            match result.action {
                HookAction::Continue => {
                    let candidate = result
                        .payload
                        .ok_or_else(|| extension_error(&sub, hook, "continue requires payload"))?;
                    if let Err(reason) = validate(&candidate) {
                        if sub.subscription.failure == HookFailurePolicy::Ignore {
                            continue;
                        }
                        return Err(extension_error(&sub, hook, reason));
                    }
                    payload = candidate;
                }
                HookAction::Unchanged => {}
                HookAction::Reject => {
                    return Err(CoreError::HookRejected {
                        hook: hook.into(),
                        reason: result
                            .reason
                            .unwrap_or_else(|| sub.plugin.metadata().id.clone()),
                    })
                }
                HookAction::Skip => {
                    return Ok(TransformResult {
                        payload,
                        control: FlowControl::Skip,
                    })
                }
                HookAction::Stop => {
                    return Ok(TransformResult {
                        payload: result.payload.unwrap_or(payload),
                        control: FlowControl::Stop,
                    })
                }
            }
        }
        Ok(TransformResult {
            payload,
            control: FlowControl::Continue,
        })
    }

    pub async fn prepare_extensions(
        &self,
        hook: &str,
        iteration: Option<usize>,
        mut draft: AgentDraft,
    ) -> Result<
        (
            AgentDraft,
            std::collections::BTreeMap<String, String>,
            FlowControl,
        ),
        CoreError,
    > {
        let mut extension_prompts = std::collections::BTreeMap::new();
        let mut names: HashSet<String> = draft
            .tools
            .iter()
            .map(|t| t.definition.name.clone())
            .collect();

        for sub in self.subscribers(hook, HookKind::Transform) {
            let ext_id = sub.plugin.metadata().id.clone();
            let response = sub
                .plugin
                .invoke(&self.request(hook, iteration, serde_json::json!({})))
                .await
                .and_then(parse_hook_result);
            let result = match response {
                Ok(v) => v,
                Err(_) if sub.subscription.failure == HookFailurePolicy::Ignore => continue,
                Err(e) => return Err(extension_error(&sub, hook, e)),
            };
            let control = match result.action {
                HookAction::Continue => {
                    let candidate = result
                        .payload
                        .ok_or_else(|| extension_error(&sub, hook, "continue requires payload"))?;
                    let output: ExtensionPrepareOutput = match serde_json::from_value(candidate) {
                        Ok(v) => v,
                        Err(_) if sub.subscription.failure == HookFailurePolicy::Ignore => continue,
                        Err(e) => return Err(extension_error(&sub, hook, e)),
                    };
                    if let Some(append_prompt) = output.append_system_prompt {
                        let trimmed = append_prompt.trim();
                        if !trimmed.is_empty() {
                            extension_prompts.insert(ext_id.clone(), trimmed.to_string());
                            if draft.system_prompt.trim().is_empty() {
                                draft.system_prompt = trimmed.to_string();
                            } else {
                                draft.system_prompt =
                                    format!("{}\n\n{}", draft.system_prompt.trim_end(), trimmed);
                            }
                        }
                    }
                    let mut validated_tools = Vec::new();
                    let mut tool_err: Option<std::io::Error> = None;
                    for mut tool in output.tools {
                        if tool.definition.name.trim().is_empty() {
                            tool_err = Some(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "tool name must not be empty",
                            ));
                            break;
                        }
                        if names.contains(&tool.definition.name)
                            || validated_tools
                                .iter()
                                .any(|t: &ToolEntry| t.definition.name == tool.definition.name)
                        {
                            tool_err = Some(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "duplicate tool name",
                            ));
                            break;
                        }
                        let id = format!("tool-{}", self.tool_id.fetch_add(1, Ordering::Relaxed));
                        tool.id = Some(id);
                        tool.owner = Some(ext_id.clone());
                        validated_tools.push(tool);
                    }
                    if let Some(err) = tool_err {
                        if sub.subscription.failure == HookFailurePolicy::Ignore {
                            continue;
                        }
                        return Err(extension_error(&sub, hook, err));
                    }
                    for tool in validated_tools {
                        names.insert(tool.definition.name.clone());
                        draft.tools.push(tool);
                    }
                    FlowControl::Continue
                }
                HookAction::Unchanged => FlowControl::Continue,
                HookAction::Reject => {
                    return Err(CoreError::HookRejected {
                        hook: hook.into(),
                        reason: result.reason.unwrap_or(ext_id),
                    })
                }
                HookAction::Skip => FlowControl::Skip,
                HookAction::Stop => FlowControl::Stop,
            };
            if control != FlowControl::Continue {
                return Ok((draft, extension_prompts, control));
            }
        }
        Ok((draft, extension_prompts, FlowControl::Continue))
    }

    pub async fn action(
        &self,
        hook: &str,
        owner: &str,
        iteration: Option<usize>,
        payload: Value,
    ) -> Result<Value, CoreError> {
        let sub = self
            .subscribers(hook, HookKind::Action)
            .into_iter()
            .find(|s| s.plugin.metadata().id == owner)
            .ok_or_else(|| {
                CoreError::ToolError(format!("extension '{owner}' does not own action '{hook}'"))
            })?;

        let result = sub
            .plugin
            .invoke(&self.request(hook, iteration, payload))
            .await
            .and_then(parse_hook_result)
            .map_err(|e| extension_error(&sub, hook, e))?;
        match result.action {
            HookAction::Continue => result
                .payload
                .ok_or_else(|| extension_error(&sub, hook, "continue requires payload")),
            HookAction::Unchanged => Err(extension_error(
                &sub,
                hook,
                "action cannot return unchanged",
            )),
            HookAction::Reject => Err(CoreError::HookRejected {
                hook: hook.into(),
                reason: result.reason.unwrap_or_else(|| owner.into()),
            }),
            HookAction::Skip => Err(extension_error(&sub, hook, "action cannot return skip")),
            HookAction::Stop => Err(extension_error(&sub, hook, "action cannot return stop")),
        }
    }

    pub async fn observe(
        &self,
        hook: &str,
        iteration: Option<usize>,
        payload: Value,
    ) -> Result<(), CoreError> {
        let futures = self
            .subscribers(hook, HookKind::Observer)
            .into_iter()
            .map(|sub| {
                let request = self.request(hook, iteration, payload.clone());
                async move {
                    match sub.plugin.invoke(&request).await {
                        Ok(_) => Ok(()),
                        Err(_) if sub.subscription.failure == HookFailurePolicy::Ignore => Ok(()),
                        Err(e) => Err(extension_error(&sub, hook, e)),
                    }
                }
            });
        for result in join_all(futures).await {
            result?;
        }
        Ok(())
    }

    fn subscribers(&self, hook: &str, kind: HookKind) -> Vec<Subscriber> {
        let mut out = self
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin
                    .metadata()
                    .subscriptions
                    .iter()
                    .filter(move |s| s.hook == hook && s.kind == kind)
                    .map(move |s| Subscriber {
                        plugin: Arc::clone(plugin),
                        subscription: s.clone(),
                    })
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|s| s.subscription.priority);
        out
    }

    pub fn validate_subscriptions(&self) -> Result<(), CoreError> {
        let mut seen = HashSet::new();
        for plugin in &self.plugins {
            if plugin.metadata().id.trim().is_empty() {
                return Err(CoreError::ToolError(
                    "extension id must not be empty".into(),
                ));
            }
            for sub in &plugin.metadata().subscriptions {
                if !seen.insert((plugin.metadata().id.clone(), sub.hook.clone(), sub.kind)) {
                    return Err(CoreError::ToolError(format!(
                        "extension '{}' declares hook '{}' more than once",
                        plugin.metadata().id,
                        sub.hook
                    )));
                }
            }
        }
        Ok(())
    }
}

fn parse_hook_result(value: Value) -> Result<HookResult, CoreError> {
    Ok(serde_json::from_value(value)?)
}

fn extension_error(sub: &Subscriber, hook: &str, error: impl std::fmt::Display) -> CoreError {
    CoreError::ToolError(format!(
        "extension '{}' failed at '{hook}': {error}",
        sub.plugin.metadata().id
    ))
}
