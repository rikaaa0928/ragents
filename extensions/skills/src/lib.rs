use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";

#[derive(Clone, Debug, Default, Deserialize)]
struct SkillsConfig {
    #[serde(default)]
    skill_dirs: Vec<String>,
    #[serde(default = "default_true")]
    include_workspace: bool,
    #[serde(default = "default_true")]
    include_global: bool,
    #[serde(default = "default_true")]
    include_agents_md: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkillEntry {
    name: String,
    description: String,
    base_dir: String,
    location: String,
}

thread_local! {
    static CONFIG: RefCell<SkillsConfig> = RefCell::new(SkillsConfig {
        skill_dirs: Vec::new(),
        include_workspace: true,
        include_global: true,
        include_agents_md: true,
    });
    static DISCOVERED_SKILLS: RefCell<BTreeMap<String, SkillEntry>> = const { RefCell::new(BTreeMap::new()) };
}

struct SkillsExtension;

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
    payload: serde_json::Value,
}

#[derive(Deserialize)]
struct ToolCallRequest {
    name: String,
    arguments: serde_json::Value,
}

#[derive(Serialize)]
struct ToolResult {
    success: bool,
    output: String,
    error: Option<String>,
}

#[derive(Deserialize)]
struct LoadSkillArgs {
    name: String,
    file: Option<String>,
}

impl exports::ragent::extension::lifecycle::Guest for SkillsExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "skills",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: AGENT_PREPARE_HOOK,
                    kind: "transform",
                    priority: 90,
                    failure: "abort",
                },
                Subscription {
                    hook: TOOLS_CALL_HOOK,
                    kind: "action",
                    priority: 100,
                    failure: "abort",
                },
            ],
        })
        .unwrap_or_default()
    }

    fn initialize(config: String) -> Result<(), String> {
        let value: serde_json::Value = serde_json::from_str(&config).map_err(|e| e.to_string())?;
        let parsed = if value.is_null() {
            SkillsConfig {
                skill_dirs: Vec::new(),
                include_workspace: true,
                include_global: true,
                include_agents_md: true,
            }
        } else {
            serde_json::from_value(value).map_err(|e| e.to_string())?
        };
        CONFIG.with(|c| *c.borrow_mut() = parsed);
        Ok(())
    }

    fn invoke(request: String) -> Result<String, String> {
        let request: HookRequest = serde_json::from_str(&request).map_err(|e| e.to_string())?;
        match request.hook.as_str() {
            AGENT_PREPARE_HOOK => handle_agent_prepare(request.payload),
            TOOLS_CALL_HOOK => handle_tools_call(request.payload),
            hook => Err(format!("unsupported hook: {hook}")),
        }
    }

    fn shutdown() {}
}

fn handle_agent_prepare(mut draft: serde_json::Value) -> Result<String, String> {
    let cfg = CONFIG.with(|c| c.borrow().clone());
    let skills = discover_all_skills(&cfg);
    DISCOVERED_SKILLS.with(|s| *s.borrow_mut() = skills.clone());

    let mut prompt_additions = String::new();

    if cfg.include_agents_md {
        for candidate in ["AGENTS.md", "CLAUDE.md"] {
            if let Ok(content) = fs::read_to_string(candidate) {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    prompt_additions.push_str(&format!(
                        "\n\n<workspace_instructions file=\"{candidate}\">\n{trimmed}\n</workspace_instructions>"
                    ));
                    break;
                }
            }
        }
    }

    if !skills.is_empty() {
        prompt_additions.push_str(
            "\n\n<skills>\nThe following skills provide specialized instructions for specific tasks.\nUse `load_skill` (or `read_file`) to load a skill's file when the task matches its description.\nWhen a skill file references a relative path, resolve it against the skill directory (parent of SKILL.md / dirname of the path) and use that absolute path in tool commands.\n\n<available_skills>\n",
        );
        for skill in skills.values() {
            prompt_additions.push_str(&format!(
                "  <skill>\n    <name>{}</name>\n    <description>{}</description>\n    <location>{}</location>\n  </skill>\n",
                escape_xml(&skill.name),
                escape_xml(&skill.description),
                escape_xml(&skill.location),
            ));
        }
        prompt_additions.push_str("</available_skills>\n</skills>");

        let tools = draft
            .get_mut("tools")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or("agent.prepare payload has no tools array")?;

        tools.push(serde_json::json!({
            "enabled": true,
            "name": "load_skill",
            "description": "Load the full instructions (SKILL.md) or a supporting file from an installed agent skill listed in <available_skills>.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Exact name of the skill from <available_skills>"
                    },
                    "file": {
                        "type": "string",
                        "description": "Optional relative file path within the skill directory (defaults to 'SKILL.md')"
                    }
                },
                "required": ["name"]
            }
        }));
    }

    if !prompt_additions.is_empty() {
        if let Some(sys) = draft
            .get("system_prompt")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
        {
            draft["system_prompt"] = serde_json::Value::String(format!("{sys}{prompt_additions}"));
        }
    }

    Ok(serde_json::json!({"action": "continue", "payload": draft}).to_string())
}

fn handle_tools_call(payload: serde_json::Value) -> Result<String, String> {
    let call: ToolCallRequest = serde_json::from_value(payload).map_err(|e| e.to_string())?;
    let result = match call.name.as_str() {
        "load_skill" => match serde_json::from_value::<LoadSkillArgs>(call.arguments) {
            Ok(args) => handle_load_skill(&args.name, args.file.as_deref()),
            Err(e) => ToolResult {
                success: false,
                output: format!("invalid arguments for load_skill: {e}"),
                error: Some(e.to_string()),
            },
        },
        other => return Err(format!("unknown tool: {other}")),
    };

    serde_json::to_string(&serde_json::json!({"action": "continue", "payload": result}))
        .map_err(|e| e.to_string())
}

fn handle_load_skill(skill_name: &str, relative_file: Option<&str>) -> ToolResult {
    let maybe_entry = DISCOVERED_SKILLS.with(|s| s.borrow().get(skill_name).cloned()).or_else(|| {
        let cfg = CONFIG.with(|c| c.borrow().clone());
        let refreshed = discover_all_skills(&cfg);
        let found = refreshed.get(skill_name).cloned();
        DISCOVERED_SKILLS.with(|s| *s.borrow_mut() = refreshed);
        found
    });

    let Some(skill) = maybe_entry else {
        let available: Vec<String> =
            DISCOVERED_SKILLS.with(|s| s.borrow().keys().cloned().collect());
        let msg = format!(
            "Skill '{}' not found. Available skills: {}",
            skill_name,
            if available.is_empty() {
                "(none)".to_string()
            } else {
                available.join(", ")
            }
        );
        return ToolResult {
            success: false,
            output: msg.clone(),
            error: Some(msg),
        };
    };

    let rel = relative_file.unwrap_or("SKILL.md").trim();
    let rel_path = Path::new(rel);
    if rel_path.is_absolute()
        || rel_path
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        let msg = format!("Invalid skill file path '{rel}': must be a relative path inside the skill directory without '..'");
        return ToolResult {
            success: false,
            output: msg.clone(),
            error: Some(msg),
        };
    }

    let target_path = Path::new(&skill.base_dir).join(rel_path);
    match fs::read_to_string(&target_path) {
        Ok(content) => ToolResult {
            success: true,
            output: format!(
                "[Skill: {} | Directory: {} | File: {}]\n\n{}",
                skill.name,
                skill.base_dir,
                target_path.display(),
                content
            ),
            error: None,
        },
        Err(err) => {
            let msg = format!(
                "Failed to read skill file '{}' for skill '{}': {err}",
                target_path.display(),
                skill.name
            );
            ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            }
        }
    }
}

fn discover_all_skills(cfg: &SkillsConfig) -> BTreeMap<String, SkillEntry> {
    let mut roots: Vec<PathBuf> = Vec::new();

    // Custom configured directories (lowest to highest priority: global -> custom -> workspace)
    if cfg.include_global {
        if let Ok(home) = std::env::var("HOME") {
            if !home.trim().is_empty() {
                roots.push(Path::new(&home).join(".agents/skills"));
            }
        }
    }

    for custom in &cfg.skill_dirs {
        if !custom.trim().is_empty() {
            roots.push(PathBuf::from(custom));
        }
    }

    if cfg.include_workspace {
        if let Ok(pwd) = std::env::var("PWD") {
            if !pwd.trim().is_empty() {
                roots.push(Path::new(&pwd).join(".agents/skills"));
            } else {
                roots.push(PathBuf::from(".agents/skills"));
            }
        } else {
            roots.push(PathBuf::from(".agents/skills"));
        }
    }

    let mut discovered = BTreeMap::new();
    for root in roots {
        scan_skills_root(&root, &mut discovered);
    }
    discovered
}

fn scan_skills_root(root: &Path, out: &mut BTreeMap<String, SkillEntry>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };

    let mut subdirs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|de| de.path()))
        .filter(|p| p.is_dir())
        .collect();
    subdirs.sort();

    for skill_dir in subdirs {
        let skill_md_path = skill_dir.join("SKILL.md");
        let Ok(content) = fs::read_to_string(&skill_md_path) else {
            continue;
        };

        let fallback_name = skill_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        let (parsed_name, parsed_desc) = parse_skill_frontmatter(&content);
        let name = parsed_name
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(fallback_name);
        let description = parsed_desc.unwrap_or_default();
        if description.trim().is_empty() {
            continue;
        }

        out.insert(
            name.clone(),
            SkillEntry {
                name,
                description,
                base_dir: skill_dir.display().to_string(),
                location: skill_md_path.display().to_string(),
            },
        );
    }
}

fn parse_skill_frontmatter(content: &str) -> (Option<String>, Option<String>) {
    let normalized = content.replace("\r\n", "\n");
    let Some(rest) = normalized.strip_prefix("---\n") else {
        return (None, None);
    };
    let Some(end_idx) = rest.find("\n---") else {
        return (None, None);
    };
    let yaml_block = &rest[..end_idx];

    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let mut current_multiline_key: Option<&str> = None;

    for raw_line in yaml_block.lines() {
        let trimmed_start = raw_line.trim_start();
        let is_indented = raw_line.starts_with(' ') || raw_line.starts_with('\t');

        if is_indented {
            if let Some("description") = current_multiline_key {
                let piece = trimmed_start.trim();
                if !piece.is_empty() {
                    let desc = description.get_or_insert_with(String::new);
                    if !desc.is_empty() {
                        desc.push(' ');
                    }
                    desc.push_str(piece);
                }
            }
            continue;
        }

        current_multiline_key = None;
        if let Some(val) = raw_line.strip_prefix("name:") {
            let cleaned = unquote_yaml_scalar(val.trim());
            if !cleaned.is_empty() {
                name = Some(cleaned);
            }
        } else if let Some(val) = raw_line.strip_prefix("description:") {
            let trimmed_val = val.trim();
            if trimmed_val.is_empty() || trimmed_val == ">" || trimmed_val == "|" || trimmed_val == ">-" || trimmed_val == "|-" {
                description = Some(String::new());
                current_multiline_key = Some("description");
            } else {
                description = Some(unquote_yaml_scalar(trimmed_val));
                current_multiline_key = Some("description");
            }
        }
    }

    (name, description)
}

fn unquote_yaml_scalar(s: &str) -> String {
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        s[1..s.len() - 1].replace("\\\"", "\"")
    } else {
        s.to_string()
    }
}

fn escape_xml(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

export!(SkillsExtension);
