use serde::{Deserialize, Serialize};
use std::fs;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";

struct FileEditorExtension;

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
struct WriteFileArgs {
    path: String,
    content: String,
}

#[derive(Deserialize)]
struct ReplacementItem {
    old_str: String,
    new_str: String,
}

#[derive(Deserialize)]
struct ReplaceInFileArgs {
    path: String,
    replacements: Vec<ReplacementItem>,
}

impl exports::ragent::extension::lifecycle::Guest for FileEditorExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "file_editor",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: AGENT_PREPARE_HOOK,
                    kind: "transform",
                    priority: 100,
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

    fn initialize(_config: String) -> Result<(), String> {
        Ok(())
    }

    fn invoke(request: String) -> Result<String, String> {
        let request: HookRequest = serde_json::from_str(&request).map_err(|e| e.to_string())?;
        match request.hook.as_str() {
            AGENT_PREPARE_HOOK => {
                let mut draft = request.payload;
                let tools = draft
                    .get_mut("tools")
                    .and_then(serde_json::Value::as_array_mut)
                    .ok_or("agent.prepare payload has no tools array")?;

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "write_file",
                    "description": "全量写入文件内容（若文件不存在则创建；若父级目录不存在会自动创建）。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "目标文件路径（支持相对当前目录的相对路径）"
                            },
                            "content": {
                                "type": "string",
                                "description": "写入的文件完整内容"
                            }
                        },
                        "required": ["path", "content"]
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "replace_in_file",
                    "description": "增量编辑文件内容。按顺序在目标文件中查找 old_str 并替换为 new_str。每次替换都会严格验证 old_str 在文件中的全局唯一性，如果不唯一或不存在则报错中断；全部通过后写回原文件。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "目标文件路径"
                            },
                            "replacements": {
                                "type": "array",
                                "description": "按顺序执行的替换项列表",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_str": {
                                            "type": "string",
                                            "description": "待查找替换的旧文本（必须在文件中唯一出现）"
                                        },
                                        "new_str": {
                                            "type": "string",
                                            "description": "替换后的新文本"
                                        }
                                    },
                                    "required": ["old_str", "new_str"]
                                }
                            }
                        },
                        "required": ["path", "replacements"]
                    }
                }));

                Ok(serde_json::json!({"action": "continue", "payload": draft}).to_string())
            }
            TOOLS_CALL_HOOK => call_tool(request.payload),
            hook => Err(format!("unsupported hook: {hook}")),
        }
    }

    fn shutdown() {}
}

fn call_tool(payload: serde_json::Value) -> Result<String, String> {
    let call: ToolCallRequest = serde_json::from_value(payload).map_err(|e| e.to_string())?;
    let result = match call.name.as_str() {
        "write_file" => match serde_json::from_value::<WriteFileArgs>(call.arguments) {
            Ok(args) => handle_write_file(&args.path, &args.content),
            Err(e) => ToolResult {
                success: false,
                output: format!("invalid arguments for write_file: {e}"),
                error: Some(e.to_string()),
            },
        },
        "replace_in_file" => match serde_json::from_value::<ReplaceInFileArgs>(call.arguments) {
            Ok(args) => handle_replace_in_file(&args.path, &args.replacements),
            Err(e) => ToolResult {
                success: false,
                output: format!("invalid arguments for replace_in_file: {e}"),
                error: Some(e.to_string()),
            },
        },
        unknown => return Err(format!("unknown tool: {unknown}")),
    };

    serde_json::to_string(&serde_json::json!({"action": "continue", "payload": result}))
        .map_err(|e| e.to_string())
}

fn handle_write_file(path_str: &str, content: &str) -> ToolResult {
    let path = std::path::Path::new(path_str);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(error) = fs::create_dir_all(parent) {
                return ToolResult {
                    success: false,
                    output: format!(
                        "failed to create parent directories for '{path_str}': {error}"
                    ),
                    error: Some(error.to_string()),
                };
            }
        }
    }

    match fs::write(path, content) {
        Ok(()) => ToolResult {
            success: true,
            output: format!("successfully wrote {} bytes to '{path_str}'", content.len()),
            error: None,
        },
        Err(error) => ToolResult {
            success: false,
            output: format!("failed to write file '{path_str}': {error}"),
            error: Some(error.to_string()),
        },
    }
}

fn handle_replace_in_file(path_str: &str, replacements: &[ReplacementItem]) -> ToolResult {
    if replacements.is_empty() {
        return ToolResult {
            success: false,
            output: "replacements list must not be empty".to_string(),
            error: Some("empty replacements list".to_string()),
        };
    }

    let original_content = match fs::read_to_string(path_str) {
        Ok(c) => c,
        Err(e) => {
            return ToolResult {
                success: false,
                output: format!("failed to read file '{path_str}': {e}"),
                error: Some(e.to_string()),
            };
        }
    };

    let mut current_content = original_content;

    for (index, item) in replacements.iter().enumerate() {
        if item.old_str.is_empty() {
            return ToolResult {
                success: false,
                output: format!("replacement #{} has an empty 'old_str'", index + 1),
                error: Some("empty old_str".to_string()),
            };
        }

        let count = current_content.matches(&item.old_str).count();
        if count == 0 {
            return ToolResult {
                success: false,
                output: format!(
                    "replacement #{} failed: 'old_str' not found in '{}'.\nold_str content:\n{}",
                    index + 1,
                    path_str,
                    item.old_str
                ),
                error: Some("old_str not found".to_string()),
            };
        }

        if count > 1 {
            return ToolResult {
                success: false,
                output: format!(
                    "replacement #{} failed: 'old_str' is not unique in '{}' (matched {} times). Please include more surrounding context to disambiguate.\nold_str content:\n{}",
                    index + 1,
                    path_str,
                    count,
                    item.old_str
                ),
                error: Some(format!("old_str matched {count} times (not unique)")),
            };
        }

        current_content = current_content.replacen(&item.old_str, &item.new_str, 1);
    }

    match fs::write(path_str, current_content) {
        Ok(()) => ToolResult {
            success: true,
            output: format!(
                "successfully applied {} replacement(s) to '{}'",
                replacements.len(),
                path_str
            ),
            error: None,
        },
        Err(error) => ToolResult {
            success: false,
            output: format!("failed to write modified content to '{path_str}': {error}"),
            error: Some(error.to_string()),
        },
    }
}

export!(FileEditorExtension);
