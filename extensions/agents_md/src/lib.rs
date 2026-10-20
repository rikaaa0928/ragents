use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";

#[derive(Clone, Debug, Deserialize)]
struct AgentsMdConfig {
    #[serde(default = "default_filenames")]
    filenames: Vec<String>,
}

fn default_filenames() -> Vec<String> {
    vec!["AGENTS.md".to_string(), "CLAUDE.md".to_string()]
}

impl Default for AgentsMdConfig {
    fn default() -> Self {
        Self {
            filenames: default_filenames(),
        }
    }
}

thread_local! {
    static CONFIG: RefCell<AgentsMdConfig> = RefCell::new(AgentsMdConfig::default());
}

struct AgentsMdExtension;

#[derive(Serialize)]
struct Metadata<'a> {
    id: &'a str,
    version: &'a str,
    subscriptions: Vec<Subscription<'a>>,
}

#[derive(Serialize)]
struct Subscription<'a> {
    hook: &'a str,
    kind: &'a str,
    priority: i32,
    failure: &'a str,
}

#[derive(Deserialize)]
struct HookRequest {
    hook: String,
}

impl exports::ragent::extension::lifecycle::Guest for AgentsMdExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "agents_md",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![Subscription {
                hook: AGENT_PREPARE_HOOK,
                kind: "transform",
                priority: 95,
                failure: "abort",
            }],
        })
        .unwrap_or_default()
    }

    fn initialize(config: String) -> Result<(), String> {
        let value: serde_json::Value = serde_json::from_str(&config).map_err(|e| e.to_string())?;
        let parsed = if value.is_null() {
            AgentsMdConfig::default()
        } else {
            serde_json::from_value(value).map_err(|e| e.to_string())?
        };
        CONFIG.with(|c| *c.borrow_mut() = parsed);
        Ok(())
    }

    fn invoke(request: String) -> Result<String, String> {
        let request: HookRequest = serde_json::from_str(&request).map_err(|e| e.to_string())?;
        match request.hook.as_str() {
            AGENT_PREPARE_HOOK => handle_agent_prepare(),
            hook => Err(format!("unsupported hook: {hook}")),
        }
    }

    fn shutdown() {}
}

fn handle_agent_prepare() -> Result<String, String> {
    let cfg = CONFIG.with(|c| c.borrow().clone());
    for candidate in &cfg.filenames {
        if let Ok(content) = fs::read_to_string(candidate) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                let prompt = format!(
                    "<workspace_instructions file=\"{candidate}\">\n{trimmed}\n</workspace_instructions>"
                );
                return Ok(serde_json::json!({
                    "action": "continue",
                    "payload": {
                        "append_system_prompt": prompt,
                        "tools": []
                    }
                })
                .to_string());
            }
        }
    }
    Ok(serde_json::json!({ "action": "unchanged" }).to_string())
}

export!(AgentsMdExtension);
