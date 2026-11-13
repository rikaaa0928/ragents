use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";

struct SessionHistoryExtension;

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
struct HostHistoryResponse {
    ok: bool,
    error: Option<String>,
    workspace_scope: Option<String>,
    #[serde(default)]
    sessions: Vec<SessionSummary>,
    #[serde(default)]
    matches: Vec<SearchMatch>,
    slice: Option<SessionSlice>,
}

#[derive(Deserialize)]
struct SessionSummary {
    session_id: String,
    relation: String,
    phase: String,
    model: Option<String>,
    generation: u32,
    compacted_from_session_id: Option<String>,
    continued_in_session_id: Option<String>,
    created_at: String,
    updated_at: String,
    effective_item_count: u64,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    first_user_preview: Option<String>,
    latest_activity_preview: Option<String>,
}

#[derive(Deserialize)]
struct SearchMatch {
    session_id: String,
    relation: String,
    generation: u32,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    item_index: usize,
    item_type: String,
    role: Option<String>,
    tool_name: Option<String>,
    call_id: Option<String>,
    snippet: String,
    total_chars: usize,
}

#[derive(Deserialize)]
struct SessionSlice {
    session_id: String,
    relation: String,
    phase: String,
    generation: u32,
    compacted_from_session_id: Option<String>,
    continued_in_session_id: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    total_items: usize,
    start_index: usize,
    end_index: usize,
    #[serde(default)]
    items: Vec<SliceItem>,
}

#[derive(Deserialize)]
struct SliceItem {
    item_index: usize,
    item_type: String,
    role: Option<String>,
    tool_name: Option<String>,
    call_id: Option<String>,
    content: String,
    total_chars: usize,
    truncated: bool,
}

fn format_labels_tag(labels: &BTreeMap<String, String>) -> String {
    let mut parts = Vec::new();
    if let Some(name) = labels
        .get("ragents.chat.agent_name")
        .filter(|s| !s.trim().is_empty())
    {
        parts.push(format!("agent={name}"));
    }
    if let Some(role) = labels
        .get("ragents.chat.role_title")
        .or_else(|| labels.get("ragents.chat.role"))
        .filter(|s| !s.trim().is_empty())
    {
        parts.push(format!("role={role}"));
    }
    if let Some(room) = labels
        .get("ragents.chat.room_name")
        .or_else(|| labels.get("ragents.chat.room_id"))
        .filter(|s| !s.trim().is_empty())
    {
        parts.push(format!("room={room}"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" [{}]", parts.join(", "))
    }
}

fn format_list_response(resp: &HostHistoryResponse) -> String {
    let scope = resp.workspace_scope.as_deref().unwrap_or("unknown");
    if resp.sessions.is_empty() {
        return format!(
            "Workspace Scope: {scope}\nNo matching historical or sibling sessions found in this workspace."
        );
    }
    let mut out = format!(
        "Workspace Scope: {scope} | Found {} session(s):\n",
        resp.sessions.len()
    );
    for s in &resp.sessions {
        let label_tag = format_labels_tag(&s.labels);
        let model_str = s.model.as_deref().unwrap_or("-");
        out.push_str(&format!(
            "\n- session_id: {}{}\n  relation: {} | phase: {} | gen: {} | model: {} | items: {} (indices 0..={})\n  updated_at: {} (created_at: {})\n",
            s.session_id,
            label_tag,
            s.relation,
            s.phase,
            s.generation,
            model_str,
            s.effective_item_count,
            s.effective_item_count.saturating_sub(1),
            s.updated_at,
            s.created_at,
        ));
        if s.compacted_from_session_id.is_some() || s.continued_in_session_id.is_some() {
            out.push_str(&format!(
                "  lineage: compacted_from={} -> continued_in={}\n",
                s.compacted_from_session_id.as_deref().unwrap_or("-"),
                s.continued_in_session_id.as_deref().unwrap_or("-"),
            ));
        }
        if let Some(ref first) = s.first_user_preview {
            out.push_str(&format!("  first_user: {first}\n"));
        }
        if let Some(ref latest) = s.latest_activity_preview {
            out.push_str(&format!("  latest_activity: {latest}\n"));
        }
    }
    out.push_str(
        "\nTip: Use `session_history_search` to search by keyword or `session_history_read` to inspect a slice of items.",
    );
    out
}

fn format_search_response(resp: &HostHistoryResponse) -> String {
    let scope = resp.workspace_scope.as_deref().unwrap_or("unknown");
    if resp.matches.is_empty() {
        return format!("Workspace Scope: {scope}\nNo matches found.");
    }
    let mut out = format!(
        "Workspace Scope: {scope} | Found {} match(es):\n",
        resp.matches.len()
    );
    for m in &resp.matches {
        let label_tag = format_labels_tag(&m.labels);
        let role_tag = m
            .role
            .as_ref()
            .map(|r| format!(", role={r}"))
            .unwrap_or_default();
        let tool_tag = m
            .tool_name
            .as_ref()
            .map(|t| format!(" | tool={t}"))
            .unwrap_or_default();
        let call_tag = m
            .call_id
            .as_ref()
            .map(|c| format!(" | call_id={c}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "\n- [session_id: {} | item_index: {}]{} (relation={}, gen={}, type={}{}{}{}, total_chars={})\n  snippet: {}\n",
            m.session_id,
            m.item_index,
            label_tag,
            m.relation,
            m.generation,
            m.item_type,
            role_tag,
            tool_tag,
            call_tag,
            m.total_chars,
            m.snippet,
        ));
    }
    out.push_str(
        "\nTip: Call `session_history_read(session_id=\"...\", around_index=<item_index>, context_window=5)` to read surrounding context.",
    );
    out
}

fn format_read_response(resp: &HostHistoryResponse) -> String {
    let Some(ref slice) = resp.slice else {
        return "Empty session slice response.".to_string();
    };
    let label_tag = format_labels_tag(&slice.labels);
    let mut out = format!(
        "Session: {}{} (relation={}, phase={}, gen={})\nTotal Items: {} | Showing range: [{}..={}] ({} item(s) rendered)\n",
        slice.session_id,
        label_tag,
        slice.relation,
        slice.phase,
        slice.generation,
        slice.total_items,
        slice.start_index,
        slice.end_index,
        slice.items.len(),
    );
    if slice.compacted_from_session_id.is_some() || slice.continued_in_session_id.is_some() {
        out.push_str(&format!(
            "Lineage: compacted_from={} -> continued_in={}\n",
            slice.compacted_from_session_id.as_deref().unwrap_or("-"),
            slice.continued_in_session_id.as_deref().unwrap_or("-"),
        ));
    }

    if slice.items.is_empty() {
        out.push_str("\n(No items in this range)");
        return out;
    }

    for item in &slice.items {
        let role_part = item
            .role
            .as_ref()
            .map(|r| format!(" | role={r}"))
            .unwrap_or_default();
        let tool_part = item
            .tool_name
            .as_ref()
            .map(|t| format!(" | tool={t}"))
            .unwrap_or_default();
        let call_part = item
            .call_id
            .as_ref()
            .map(|c| format!(" | call_id={c}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "\n--- [item_index: {} | {}{}{}{} | chars={}] ---\n{}\n",
            item.item_index,
            item.item_type,
            role_part,
            tool_part,
            call_part,
            item.total_chars,
            item.content,
        ));
        if item.truncated {
            out.push_str(&format!(
                "[... truncated (total {} chars); call session_history_read(session_id=\"{}\", from_index={}, limit=1, max_chars_per_item=30000) to view full item]\n",
                item.total_chars, slice.session_id, item.item_index
            ));
        }
    }
    out
}

impl exports::ragent::extension::lifecycle::Guest for SessionHistoryExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "session_history",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: AGENT_PREPARE_HOOK,
                    kind: "transform",
                    priority: 85,
                    failure: "ignore",
                },
                Subscription {
                    hook: TOOLS_CALL_HOOK,
                    kind: "action",
                    priority: 85,
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
        let req: HookRequest = serde_json::from_str(&request).map_err(|e| e.to_string())?;
        match req.hook.as_str() {
            AGENT_PREPARE_HOOK => {
                let tools = vec![
                    serde_json::json!({
                        "enabled": true,
                        "name": "session_history_list",
                        "description": "List other sessions in the same workspace directory, including your own historical/compacted ancestor sessions (`self_history`) and other agents' sessions (`sibling_session`). Returns session IDs, agent/room labels, compaction lineage, item counts, and activity previews.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "include_current_session": {
                                    "type": "boolean",
                                    "default": false,
                                    "description": "Whether to include your own currently active session in the list (default false)."
                                },
                                "include_same_lineage_only": {
                                    "type": "boolean",
                                    "default": false,
                                    "description": "If true, only list your own historical/compacted sessions (`self_history`) and exclude other agents' sessions."
                                },
                                "limit": {
                                    "type": "integer",
                                    "default": 30,
                                    "description": "Maximum number of sessions to return (default 30, max 100), ordered by most recently updated."
                                }
                            },
                            "additionalProperties": false
                        }
                    }),
                    serde_json::json!({
                        "enabled": true,
                        "name": "session_history_search",
                        "description": "Search conversation messages, tool calls, and tool outputs across sessions in the same workspace directory (or within a specific `session_id`). Returns matching `session_id`, 0-based `item_index`, and text snippets.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Case-insensitive keyword(s) to search for (space-separated tokens must all appear in the item)."
                                },
                                "session_id": {
                                    "type": "string",
                                    "description": "Optional target session_id to search within. If omitted, searches across all sessions in the same workspace."
                                },
                                "item_types": {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "enum": ["user_message", "assistant_message", "reasoning", "function_call", "function_call_output"]
                                    },
                                    "description": "Optional filter by item types."
                                },
                                "tool_name": {
                                    "type": "string",
                                    "description": "Optional filter for `function_call` / `function_call_output` items by tool name (e.g. 'shell', 'replace_in_file', 'chatroom_speak')."
                                },
                                "include_current_session": {
                                    "type": "boolean",
                                    "default": false,
                                    "description": "Whether to also search inside your own currently active session (default false)."
                                },
                                "limit": {
                                    "type": "integer",
                                    "default": 20,
                                    "description": "Maximum number of matches to return (default 20, max 50)."
                                }
                            },
                            "required": ["query"],
                            "additionalProperties": false
                        }
                    }),
                    serde_json::json!({
                        "enabled": true,
                        "name": "session_history_read",
                        "description": "Read a slice of conversation/tool history items from a target session in the same workspace. Supports reading around a specific `around_index` (from search results), reading forward from `from_index`, or reading the latest tail items when neither index is specified.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "session_id": {
                                    "type": "string",
                                    "description": "Target `session_id` to inspect (obtained from `session_history_list` or `session_history_search`)."
                                },
                                "around_index": {
                                    "type": "integer",
                                    "description": "Optional 0-based `item_index` to center the view around (returns `[around_index - context_window ..= around_index + context_window]`)."
                                },
                                "context_window": {
                                    "type": "integer",
                                    "default": 5,
                                    "description": "Number of items before and after `around_index` to include (default 5, max 25)."
                                },
                                "from_index": {
                                    "type": "integer",
                                    "description": "Optional 0-based starting `item_index`. If neither `around_index` nor `from_index` is provided, returns the most recent `limit` items at the tail of the session."
                                },
                                "limit": {
                                    "type": "integer",
                                    "default": 20,
                                    "description": "Maximum number of items to return when using `from_index` or tail mode (default 20, max 50)."
                                },
                                "max_chars_per_item": {
                                    "type": "integer",
                                    "default": 2000,
                                    "description": "Maximum characters per item before truncation (default 2000, max 30000). Set `limit=1, max_chars_per_item=30000` to read a large tool output in full."
                                },
                                "include_reasoning": {
                                    "type": "boolean",
                                    "default": false,
                                    "description": "Whether to include internal model reasoning items in the slice (default false)."
                                }
                            },
                            "required": ["session_id"],
                            "additionalProperties": false
                        }
                    }),
                ];

                let append_prompt = "<session_history_guidance>\nYou have access to `session_history_list`, `session_history_search`, and `session_history_read` to inspect other sessions sharing the same workspace directory—including your own pre-compaction/previous sessions (`self_history`) and other agents' sessions (`sibling_session`). When you need details that were compacted away or want to inspect commands, file edits, or errors from another agent/session in this workspace, list or search the workspace sessions first and then read the relevant item slice.\n</session_history_guidance>";

                Ok(serde_json::json!({
                    "action": "continue",
                    "payload": {
                        "append_system_prompt": append_prompt,
                        "tools": tools,
                    }
                })
                .to_string())
            }
            TOOLS_CALL_HOOK => {
                let call: ToolCallRequest =
                    serde_json::from_value(req.payload).map_err(|e| e.to_string())?;
                let args = call.arguments;
                let host_req = match call.name.as_str() {
                    "session_history_list" => serde_json::json!({
                        "action": "list",
                        "include_current_session": args.get("include_current_session").and_then(|v| v.as_bool()).unwrap_or(false),
                        "include_same_lineage_only": args.get("include_same_lineage_only").and_then(|v| v.as_bool()).unwrap_or(false),
                        "limit": args.get("limit").and_then(|v| v.as_u64()).unwrap_or(30),
                    }),
                    "session_history_search" => {
                        let Some(query) = args.get("query").and_then(|v| v.as_str()) else {
                            return Ok(serde_json::json!({
                                "action": "continue",
                                "payload": ToolResult {
                                    success: false,
                                    output: "Missing required parameter 'query'".into(),
                                    error: Some("Missing required parameter 'query'".into()),
                                }
                            })
                            .to_string());
                        };
                        serde_json::json!({
                            "action": "search",
                            "query": query,
                            "session_id": args.get("session_id").and_then(|v| v.as_str()),
                            "item_types": args.get("item_types").and_then(|v| v.as_array()).cloned().unwrap_or_default(),
                            "tool_name": args.get("tool_name").and_then(|v| v.as_str()),
                            "include_current_session": args.get("include_current_session").and_then(|v| v.as_bool()).unwrap_or(false),
                            "limit": args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20),
                        })
                    }
                    "session_history_read" => {
                        let Some(session_id) = args.get("session_id").and_then(|v| v.as_str()) else {
                            return Ok(serde_json::json!({
                                "action": "continue",
                                "payload": ToolResult {
                                    success: false,
                                    output: "Missing required parameter 'session_id'".into(),
                                    error: Some("Missing required parameter 'session_id'".into()),
                                }
                            })
                            .to_string());
                        };
                        serde_json::json!({
                            "action": "read",
                            "session_id": session_id,
                            "from_index": args.get("from_index").and_then(|v| v.as_u64()),
                            "around_index": args.get("around_index").and_then(|v| v.as_u64()),
                            "context_window": args.get("context_window").and_then(|v| v.as_u64()).unwrap_or(5),
                            "limit": args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20),
                            "max_chars_per_item": args.get("max_chars_per_item").and_then(|v| v.as_u64()).unwrap_or(2000),
                            "include_reasoning": args.get("include_reasoning").and_then(|v| v.as_bool()).unwrap_or(false),
                        })
                    }
                    _ => {
                        return Ok(serde_json::json!({
                            "action": "unchanged"
                        })
                        .to_string());
                    }
                };

                let raw_resp = ragent::extension::host::query_session_history(&host_req.to_string());
                let parsed: HostHistoryResponse = match serde_json::from_str(&raw_resp) {
                    Ok(r) => r,
                    Err(e) => {
                        let msg = format!("Failed to parse host session history response: {e}");
                        return Ok(serde_json::json!({
                            "action": "continue",
                            "payload": ToolResult {
                                success: false,
                                output: msg.clone(),
                                error: Some(msg),
                            }
                        })
                        .to_string());
                    }
                };

                if !parsed.ok {
                    let err_msg = parsed
                        .error
                        .unwrap_or_else(|| "Unknown session history error".into());
                    return Ok(serde_json::json!({
                        "action": "continue",
                        "payload": ToolResult {
                            success: false,
                            output: err_msg.clone(),
                            error: Some(err_msg),
                        }
                    })
                    .to_string());
                }

                let formatted = match call.name.as_str() {
                    "session_history_list" => format_list_response(&parsed),
                    "session_history_search" => format_search_response(&parsed),
                    "session_history_read" => format_read_response(&parsed),
                    _ => String::new(),
                };

                Ok(serde_json::json!({
                    "action": "continue",
                    "payload": ToolResult {
                        success: true,
                        output: formatted,
                        error: None,
                    }
                })
                .to_string())
            }
            _ => Ok(serde_json::json!({ "action": "unchanged" }).to_string()),
        }
    }

    fn shutdown() {}
}

export!(SessionHistoryExtension);
