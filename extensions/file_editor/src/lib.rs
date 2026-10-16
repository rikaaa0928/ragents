use serde::{Deserialize, Serialize};
use std::fs;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";

const DEFAULT_MAX_LINES: usize = 2000;
const DEFAULT_MAX_BYTES: usize = 50 * 1024; // 50KB

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
struct ReadFileArgs {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct WriteFileArgs {
    path: String,
    content: String,
}

#[derive(Deserialize)]
struct EditItem {
    #[serde(alias = "oldText", alias = "old_str")]
    old_text: String,
    #[serde(alias = "newText", alias = "new_str")]
    new_text: String,
}

#[derive(Deserialize)]
struct ReplaceInFileArgs {
    path: String,
    #[serde(alias = "replacements")]
    edits: Vec<EditItem>,
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
                    "name": "read_file",
                    "description": "Read the contents of a text file. Output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Path to the file to read (relative or absolute)"
                            },
                            "offset": {
                                "type": "integer",
                                "minimum": 1,
                                "description": "Line number to start reading from (1-indexed)"
                            },
                            "limit": {
                                "type": "integer",
                                "minimum": 1,
                                "description": "Maximum number of lines to read"
                            }
                        },
                        "required": ["path"]
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "write_file",
                    "description": "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Path to the file to write (relative or absolute)"
                            },
                            "content": {
                                "type": "string",
                                "description": "Content to write to the file"
                            }
                        },
                        "required": ["path", "content"]
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "replace_in_file",
                    "description": "Edit a single file using exact text replacement. Every edits[].old_text must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Path to the file to edit (relative or absolute)"
                            },
                            "edits": {
                                "type": "array",
                                "description": "One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "old_text": {
                                            "type": "string",
                                            "description": "Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].old_text in the same call."
                                        },
                                        "new_text": {
                                            "type": "string",
                                            "description": "Replacement text for this targeted edit."
                                        }
                                    },
                                    "required": ["old_text", "new_text"]
                                }
                            }
                        },
                        "required": ["path", "edits"]
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
        "read_file" => match serde_json::from_value::<ReadFileArgs>(call.arguments) {
            Ok(args) => handle_read_file(&args.path, args.offset, args.limit),
            Err(e) => ToolResult {
                success: false,
                output: format!("invalid arguments for read_file: {e}"),
                error: Some(e.to_string()),
            },
        },
        "write_file" => match serde_json::from_value::<WriteFileArgs>(call.arguments) {
            Ok(args) => handle_write_file(&args.path, &args.content),
            Err(e) => ToolResult {
                success: false,
                output: format!("invalid arguments for write_file: {e}"),
                error: Some(e.to_string()),
            },
        },
        "replace_in_file" => match serde_json::from_value::<ReplaceInFileArgs>(call.arguments) {
            Ok(args) => handle_replace_in_file(&args.path, &args.edits),
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

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

#[derive(PartialEq, Eq)]
enum TruncatedBy {
    Lines,
    Bytes,
}

struct HeadTruncationResult {
    content: String,
    truncated: bool,
    truncated_by: Option<TruncatedBy>,
    output_lines: usize,
    first_line_exceeds_limit: bool,
}

fn split_lines_for_counting<'a>(content: &'a str) -> Vec<&'a str> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&'a str> = content.split('\n').collect();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}

fn truncate_head(content: &str, max_lines: usize, max_bytes: usize) -> HeadTruncationResult {
    let total_bytes = content.len();
    let lines = split_lines_for_counting(content);
    let total_lines = lines.len();

    if total_lines <= max_lines && total_bytes <= max_bytes {
        return HeadTruncationResult {
            content: content.to_string(),
            truncated: false,
            truncated_by: None,
            output_lines: total_lines,
            first_line_exceeds_limit: false,
        };
    }

    if let Some(first_line) = lines.first() {
        if first_line.len() > max_bytes {
            return HeadTruncationResult {
                content: String::new(),
                truncated: true,
                truncated_by: Some(TruncatedBy::Bytes),
                output_lines: 0,
                first_line_exceeds_limit: true,
            };
        }
    }

    let mut output_lines_arr = Vec::new();
    let mut output_bytes_count = 0usize;
    let mut truncated_by = TruncatedBy::Lines;

    for (i, line) in lines.iter().enumerate() {
        if i >= max_lines {
            truncated_by = TruncatedBy::Lines;
            break;
        }
        let line_bytes = line.len() + usize::from(i > 0);
        if output_bytes_count + line_bytes > max_bytes {
            truncated_by = TruncatedBy::Bytes;
            break;
        }
        output_lines_arr.push(*line);
        output_bytes_count += line_bytes;
    }

    let output_lines = output_lines_arr.len();
    let output_content = output_lines_arr.join("\n");

    HeadTruncationResult {
        content: output_content,
        truncated: true,
        truncated_by: Some(truncated_by),
        output_lines,
        first_line_exceeds_limit: false,
    }
}

fn handle_read_file(path_str: &str, offset: Option<usize>, limit: Option<usize>) -> ToolResult {
    let text_content = match fs::read_to_string(path_str) {
        Ok(c) => c,
        Err(error) => {
            return ToolResult {
                success: false,
                output: format!("failed to read file '{path_str}': {error}"),
                error: Some(error.to_string()),
            };
        }
    };

    let all_lines: Vec<&str> = text_content.split('\n').collect();
    let total_file_lines = all_lines.len();
    let start_line = offset.map(|o| o.saturating_sub(1)).unwrap_or(0);
    let start_line_display = start_line + 1;

    if start_line >= all_lines.len() {
        let msg = format!(
            "Offset {} is beyond end of file ({} lines total)",
            offset.unwrap_or(1),
            total_file_lines
        );
        return ToolResult {
            success: false,
            output: msg.clone(),
            error: Some(msg),
        };
    }

    let (selected_content, user_limited_lines) = if let Some(lim) = limit {
        let end_line = (start_line + lim).min(all_lines.len());
        (
            all_lines[start_line..end_line].join("\n"),
            Some(end_line - start_line),
        )
    } else {
        (all_lines[start_line..].join("\n"), None)
    };

    let truncation = truncate_head(&selected_content, DEFAULT_MAX_LINES, DEFAULT_MAX_BYTES);
    let output_text = if truncation.first_line_exceeds_limit {
        let first_line_size = format_size(all_lines[start_line].len());
        let max_size = format_size(DEFAULT_MAX_BYTES);
        format!(
            "[Line {start_line_display} is {first_line_size}, exceeds {max_size} limit. Use shell: sed -n '{start_line_display}p' {path_str} | head -c {DEFAULT_MAX_BYTES}]"
        )
    } else if truncation.truncated {
        let end_line_display = start_line_display + truncation.output_lines.saturating_sub(1);
        let next_offset = end_line_display + 1;
        let mut out = truncation.content;
        if truncation.truncated_by == Some(TruncatedBy::Lines) {
            out.push_str(&format!(
                "\n\n[Showing lines {start_line_display}-{end_line_display} of {total_file_lines}. Use offset={next_offset} to continue.]"
            ));
        } else {
            let max_size = format_size(DEFAULT_MAX_BYTES);
            out.push_str(&format!(
                "\n\n[Showing lines {start_line_display}-{end_line_display} of {total_file_lines} ({max_size} limit). Use offset={next_offset} to continue.]"
            ));
        }
        out
    } else if let Some(limited) = user_limited_lines {
        if start_line + limited < all_lines.len() {
            let remaining = all_lines.len() - (start_line + limited);
            let next_offset = start_line + limited + 1;
            format!(
                "{}\n\n[{remaining} more lines in file. Use offset={next_offset} to continue.]",
                truncation.content
            )
        } else {
            truncation.content
        }
    } else {
        truncation.content
    };

    ToolResult {
        success: true,
        output: output_text,
        error: None,
    }
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
            output: format!("Successfully wrote {} bytes to {path_str}.", content.len()),
            error: None,
        },
        Err(error) => ToolResult {
            success: false,
            output: format!("failed to write file '{path_str}': {error}"),
            error: Some(error.to_string()),
        },
    }
}

fn split_bom(raw: &str) -> (&str, &str) {
    if let Some(stripped) = raw.strip_prefix('\u{feff}') {
        ("\u{feff}", stripped)
    } else {
        ("", raw)
    }
}

fn detect_line_ending(content: &str) -> &'static str {
    let crlf_idx = content.find("\r\n");
    let lf_idx = content.find('\n');
    match (crlf_idx, lf_idx) {
        (Some(c), Some(l)) if c < l => "\r\n",
        _ => "\n",
    }
}

fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn restore_line_endings(text: &str, ending: &str) -> String {
    if ending == "\r\n" {
        text.replace('\n', "\r\n")
    } else {
        text.to_string()
    }
}

#[derive(Clone, Debug)]
struct MatchedEdit {
    edit_index: usize,
    match_index: usize,
    match_length: usize,
    new_text: String,
}

fn handle_replace_in_file(path_str: &str, edits: &[EditItem]) -> ToolResult {
    if edits.is_empty() {
        let msg = "Edit tool input is invalid. edits must contain at least one replacement."
            .to_string();
        return ToolResult {
            success: false,
            output: msg.clone(),
            error: Some(msg),
        };
    }

    let raw_content = match fs::read_to_string(path_str) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Could not edit file: {path_str}. {e}");
            return ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            };
        }
    };

    let (bom, content_no_bom) = split_bom(&raw_content);
    let original_ending = detect_line_ending(content_no_bom);
    let normalized_content = normalize_to_lf(content_no_bom);

    let normalized_edits: Vec<(String, String)> = edits
        .iter()
        .map(|e| (normalize_to_lf(&e.old_text), normalize_to_lf(&e.new_text)))
        .collect();

    let total_edits = normalized_edits.len();
    for (i, (old_text, _)) in normalized_edits.iter().enumerate() {
        if old_text.is_empty() {
            let msg = if total_edits == 1 {
                format!("old_text must not be empty in {path_str}.")
            } else {
                format!("edits[{i}].old_text must not be empty in {path_str}.")
            };
            return ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            };
        }
    }

    let mut matched_edits = Vec::with_capacity(total_edits);
    for (i, (old_text, new_text)) in normalized_edits.iter().enumerate() {
        let mut matches = normalized_content.match_indices(old_text.as_str());
        let Some((first_idx, _)) = matches.next() else {
            let msg = if total_edits == 1 {
                format!("Could not find the exact text in {path_str}. The old_text must match exactly including all whitespace and newlines.")
            } else {
                format!("Could not find edits[{i}] in {path_str}. The old_text must match exactly including all whitespace and newlines.")
            };
            return ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            };
        };

        let extra_matches = matches.count();
        if extra_matches > 0 {
            let occurrences = 1 + extra_matches;
            let msg = if total_edits == 1 {
                format!("Found {occurrences} occurrences of the text in {path_str}. The text must be unique. Please provide more context to make it unique.")
            } else {
                format!("Found {occurrences} occurrences of edits[{i}] in {path_str}. Each old_text must be unique. Please provide more context to make it unique.")
            };
            return ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            };
        }

        matched_edits.push(MatchedEdit {
            edit_index: i,
            match_index: first_idx,
            match_length: old_text.len(),
            new_text: new_text.clone(),
        });
    }

    matched_edits.sort_by_key(|m| m.match_index);
    for i in 1..matched_edits.len() {
        let prev = &matched_edits[i - 1];
        let curr = &matched_edits[i];
        if prev.match_index + prev.match_length > curr.match_index {
            let msg = format!(
                "edits[{}] and edits[{}] overlap in {path_str}. Merge them into one edit or target disjoint regions.",
                prev.edit_index, curr.edit_index
            );
            return ToolResult {
                success: false,
                output: msg.clone(),
                error: Some(msg),
            };
        }
    }

    let mut new_content = normalized_content.clone();
    for edit in matched_edits.iter().rev() {
        let start = edit.match_index;
        let end = start + edit.match_length;
        new_content.replace_range(start..end, &edit.new_text);
    }

    if new_content == normalized_content {
        let msg = if total_edits == 1 {
            format!("No changes made to {path_str}. The replacement produced identical content.")
        } else {
            format!("No changes made to {path_str}. The replacements produced identical content.")
        };
        return ToolResult {
            success: false,
            output: msg.clone(),
            error: Some(msg),
        };
    }

    let final_content = format!(
        "{bom}{}",
        restore_line_endings(&new_content, original_ending)
    );
    match fs::write(path_str, final_content) {
        Ok(()) => ToolResult {
            success: true,
            output: format!("Successfully replaced {total_edits} block(s) in {path_str}."),
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
