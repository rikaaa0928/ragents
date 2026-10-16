use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Mutex;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const COMPACTION_SUMMARY_PREFIX: &str = "[Compaction Summary]";
const TOOL_RESULT_MAX_CHARS: usize = 2000;

const SUMMARIZATION_SYSTEM_PROMPT: &str = "You are a context summarization assistant. Your task is to read a conversation between a user and an AI coding assistant, then produce a structured summary following the exact format specified. Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.";

const SUMMARIZATION_PROMPT: &str = "The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work.\n\nUse this exact structure:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or \"(none)\" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or \"(none)\" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.";

const UPDATE_SUMMARIZATION_PROMPT: &str = "The messages above are NEW conversation messages to incorporate into the existing summary provided in <previous-summary> tags.\n\nUpdate the existing structured summary with new information. RULES:\n- PRESERVE all existing information from the previous summary\n- ADD new progress, decisions, and context from the new messages\n- UPDATE the Progress section: move items from \"In Progress\" to \"Done\" when completed\n- UPDATE \"Next Steps\" based on what was accomplished\n- PRESERVE exact file paths, function names, and error messages\n- If something is no longer relevant, you may remove it\n\nUse this exact structure:\n\n## Goal\n[Preserve existing goals, add new ones if the task expanded]\n\n## Constraints & Preferences\n- [Preserve existing, add new ones discovered]\n\n## Progress\n### Done\n- [x] [Include previously done items AND newly completed items]\n\n### In Progress\n- [ ] [Current work - update based on progress]\n\n### Blocked\n- [Current blockers - remove if resolved]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale] (preserve all previous, add new)\n\n## Next Steps\n1. [Update based on current state]\n\n## Critical Context\n- [Preserve important context, add new if needed]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.";

const TURN_PREFIX_SUMMARIZATION_PROMPT: &str = "This is the early part of a turn that was too large to keep. The (more recent)Suffix of this turn is retained in the conversation.\n\nSummarize this prefix to provide context for the retained suffix:\n\n## Original Request\n[What did the user ask for in this turn?]\n\n## Early Progress\n- [Key decisions and work done in the prefix]\n\n## Context for Suffix\n- [Information needed to understand the retained recent messages]\n\nBe concise. Preserve exact file paths, function names, and error messages.";

#[derive(Debug, Clone, Deserialize)]
struct CompactionConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_trigger_tokens")]
    trigger_tokens: u64,
    #[serde(default = "default_keep_recent_tokens")]
    keep_recent_tokens: u64,
    #[serde(default = "default_reserve_tokens")]
    #[allow(dead_code)]
    reserve_tokens: u64,
    #[serde(default)]
    custom_instructions: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_trigger_tokens() -> u64 {
    64_000
}

fn default_keep_recent_tokens() -> u64 {
    20_000
}

fn default_reserve_tokens() -> u64 {
    8_192
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            trigger_tokens: default_trigger_tokens(),
            keep_recent_tokens: default_keep_recent_tokens(),
            reserve_tokens: default_reserve_tokens(),
            custom_instructions: None,
        }
    }
}

#[derive(Debug, Default)]
struct CompactionRuntimeState {
    config: CompactionConfig,
    last_observed_total_tokens: Option<u64>,
    last_observed_item_count: usize,
}

static STATE: Mutex<Option<CompactionRuntimeState>> = Mutex::new(None);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionPlan {
    summarized_through_pos: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    kept_from_pos: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kept_through_pos: Option<u64>,
    tokens_before: u64,
    system_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_prefix_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_summary: Option<String>,
    #[serde(default)]
    read_files: Vec<String>,
    #[serde(default)]
    modified_files: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct FileOps {
    read: BTreeSet<String>,
    written: BTreeSet<String>,
    edited: BTreeSet<String>,
}

struct CutPointResult {
    first_kept_index: usize,
    turn_start_index: Option<usize>,
    is_split_turn: bool,
}

pub fn estimate_item_tokens(item: &Value) -> u64 {
    let mut chars: usize = 0;
    match item.get("type").and_then(Value::as_str).unwrap_or("") {
        "message" => {
            if let Some(arr) = item.get("content").and_then(Value::as_array) {
                for block in arr {
                    match block.get("type").and_then(Value::as_str).unwrap_or("") {
                        "input_text" | "output_text" | "plain_text" | "summary_text" => {
                            if let Some(t) = block.get("text").and_then(Value::as_str) {
                                chars += t.len();
                            }
                        }
                        "refusal" => {
                            if let Some(r) = block.get("refusal").and_then(Value::as_str) {
                                chars += r.len();
                            }
                        }
                        "input_image" => {
                            chars += 4800;
                        }
                        _ => {}
                    }
                }
            }
        }
        "reasoning" => {
            for field in ["summary", "content"] {
                if let Some(arr) = item.get(field).and_then(Value::as_array) {
                    for block in arr {
                        if let Some(t) = block.get("text").and_then(Value::as_str) {
                            chars += t.len();
                        }
                    }
                }
            }
        }
        "function_call" => {
            chars += item
                .get("name")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0);
            chars += item
                .get("arguments")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0);
        }
        "function_call_output" => match item.get("output") {
            Some(Value::String(s)) => chars += s.len(),
            Some(Value::Array(arr)) => {
                for block in arr {
                    if let Some(t) = block.get("text").and_then(Value::as_str) {
                        chars += t.len();
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
    (chars as u64).div_ceil(4)
}

fn extract_message_text(item: &Value) -> String {
    let mut out = String::new();
    if let Some(arr) = item.get("content").and_then(Value::as_array) {
        for block in arr {
            if let Some(t) = block.get("text").and_then(Value::as_str) {
                out.push_str(t);
            } else if let Some(r) = block.get("refusal").and_then(Value::as_str) {
                out.push_str(r);
            }
        }
    }
    out
}

fn is_user_message(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("message")
        && item.get("role").and_then(Value::as_str) == Some("user")
}

fn is_valid_cut_point(items: &[Value], index: usize, start_index: usize) -> bool {
    let item = &items[index];
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    match item_type {
        "message" => {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("");
            if role == "user" || role == "system" || role == "developer" {
                return true;
            }
            // Assistant message is a valid cut point if it starts a new assistant step
            if index <= start_index {
                return true;
            }
            let prev = &items[index - 1];
            let prev_type = prev.get("type").and_then(Value::as_str).unwrap_or("");
            prev_type == "function_call_output" || is_user_message(prev)
        }
        "reasoning" | "function_call" => {
            // Valid cut point only at the start of an assistant step (never in the middle of parallel function_calls)
            if index <= start_index {
                return true;
            }
            let prev = &items[index - 1];
            let prev_type = prev.get("type").and_then(Value::as_str).unwrap_or("");
            prev_type == "function_call_output" || is_user_message(prev)
        }
        _ => false,
    }
}

fn parse_previous_compaction_summary(item: &Value) -> Option<(String, FileOps)> {
    if item.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    let raw = extract_message_text(item);
    let trimmed = raw.trim();
    if !trimmed.starts_with(COMPACTION_SUMMARY_PREFIX) {
        return None;
    }

    let mut body = trimmed[COMPACTION_SUMMARY_PREFIX.len()..].trim().to_string();
    let mut ops = FileOps::default();

    if let Some((before, after)) = extract_xml_tag(&body, "modified-files") {
        for line in after.lines().map(str::trim).filter(|l| !l.is_empty()) {
            ops.edited.insert(line.to_string());
        }
        body = before;
    }
    if let Some((before, after)) = extract_xml_tag(&body, "read-files") {
        for line in after.lines().map(str::trim).filter(|l| !l.is_empty()) {
            ops.read.insert(line.to_string());
        }
        body = before;
    }

    Some((body.trim().to_string(), ops))
}

fn extract_xml_tag(text: &str, tag: &str) -> Option<(String, String)> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.rfind(&open)?;
    let end = text[start..].find(&close)? + start;
    let inner = text[start + open.len()..end].trim().to_string();
    let mut remaining = String::new();
    remaining.push_str(text[..start].trim_end());
    let tail = text[end + close.len()..].trim();
    if !tail.is_empty() {
        remaining.push('\n');
        remaining.push_str(tail);
    }
    Some((remaining, inner))
}

fn find_turn_start_index(items: &[Value], entry_index: usize, start_index: usize) -> Option<usize> {
    for i in (start_index..=entry_index).rev() {
        if is_user_message(&items[i]) {
            return Some(i);
        }
    }
    None
}

fn find_cut_point(
    items: &[Value],
    start_index: usize,
    end_index: usize,
    keep_recent_tokens: u64,
) -> CutPointResult {
    let mut cut_points = Vec::new();
    for i in start_index..end_index {
        if is_valid_cut_point(items, i, start_index) {
            cut_points.push(i);
        }
    }
    if cut_points.is_empty() {
        return CutPointResult {
            first_kept_index: start_index,
            turn_start_index: None,
            is_split_turn: false,
        };
    }

    let mut accumulated_tokens = 0u64;
    let mut cut_index = cut_points[0];

    for i in (start_index..end_index).rev() {
        accumulated_tokens += estimate_item_tokens(&items[i]);
        if let Some(&cp) = cut_points.iter().rev().find(|&& idx| idx <= i) {
            cut_index = cp;
        }
        if accumulated_tokens >= keep_recent_tokens {
            break;
        }
    }

    let is_user = is_user_message(&items[cut_index]);
    let turn_start_index = if is_user {
        None
    } else {
        find_turn_start_index(items, cut_index, start_index)
    };

    CutPointResult {
        first_kept_index: cut_index,
        turn_start_index,
        is_split_turn: !is_user && turn_start_index.is_some(),
    }
}

fn extract_file_ops_from_items(items: &[Value], ops: &mut FileOps) {
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("function_call") {
            let name = item.get("name").and_then(Value::as_str).unwrap_or("");
            let args_str = item.get("arguments").and_then(Value::as_str).unwrap_or("{}");
            if let Ok(args) = serde_json::from_str::<Value>(args_str) {
                if let Some(path) = args.get("path").and_then(Value::as_str) {
                    match name {
                        "read_file" | "read" => {
                            ops.read.insert(path.to_string());
                        }
                        "write_file" | "write" => {
                            ops.written.insert(path.to_string());
                        }
                        "replace_in_file" | "edit" => {
                            ops.edited.insert(path.to_string());
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn compute_file_lists(ops: &FileOps) -> (Vec<String>, Vec<String>) {
    let modified: BTreeSet<String> = ops.edited.union(&ops.written).cloned().collect();
    let read_only: Vec<String> = ops
        .read
        .iter()
        .filter(|p| !modified.contains(*p))
        .cloned()
        .collect();
    (read_only, modified.into_iter().collect())
}

fn truncate_for_summary(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }
    let truncated = &text[..text.floor_char_boundary(max_chars)];
    let remaining = text.len().saturating_sub(truncated.len());
    format!("{truncated}\n... [{remaining} more chars truncated]")
}

fn serialize_conversation(items: &[Value]) -> String {
    let mut parts = Vec::new();
    for item in items {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "message" => {
                let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                let text = extract_message_text(item);
                if !text.is_empty() {
                    let label = match role {
                        "user" => "[User]",
                        "assistant" => "[Assistant]",
                        "system" | "developer" => "[System]",
                        _ => "[Message]",
                    };
                    parts.push(format!("{label}: {text}"));
                }
            }
            "reasoning" => {
                let mut thinking = String::new();
                for field in ["summary", "content"] {
                    if let Some(arr) = item.get(field).and_then(Value::as_array) {
                        for block in arr {
                            if let Some(t) = block.get("text").and_then(Value::as_str) {
                                thinking.push_str(t);
                            }
                        }
                    }
                }
                if !thinking.is_empty() {
                    parts.push(format!("[Assistant thinking]: {thinking}"));
                }
            }
            "function_call" => {
                let name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
                let args = item.get("arguments").and_then(Value::as_str).unwrap_or("{}");
                parts.push(format!("[Assistant tool calls]: {name}({args})"));
            }
            "function_call_output" => {
                let raw = match item.get("output") {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Array(arr)) => arr
                        .iter()
                        .filter_map(|b| b.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n"),
                    _ => String::new(),
                };
                if !raw.is_empty() {
                    parts.push(format!(
                        "[Tool result]: {}",
                        truncate_for_summary(&raw, TOOL_RESULT_MAX_CHARS)
                    ));
                }
            }
            _ => {}
        }
    }
    parts.join("\n\n")
}

fn build_history_prompt(
    messages: &[Value],
    previous_summary: Option<&str>,
    custom_instructions: Option<&str>,
) -> Option<String> {
    if messages.is_empty() {
        return None;
    }
    let conversation_text = serialize_conversation(messages);
    if conversation_text.trim().is_empty() {
        return None;
    }
    let mut base_prompt = if previous_summary.is_some() {
        UPDATE_SUMMARIZATION_PROMPT.to_string()
    } else {
        SUMMARIZATION_PROMPT.to_string()
    };
    if let Some(custom) = custom_instructions.filter(|s| !s.trim().is_empty()) {
        base_prompt.push_str(&format!("\n\nAdditional focus: {custom}"));
    }

    let mut prompt = format!("<conversation>\n{conversation_text}\n</conversation>\n\n");
    if let Some(prev) = previous_summary.filter(|s| !s.trim().is_empty()) {
        prompt.push_str(&format!(
            "<previous-summary>\n{prev}\n</previous-summary>\n\n"
        ));
    }
    prompt.push_str(&base_prompt);
    Some(prompt)
}

fn build_turn_prefix_prompt(messages: &[Value]) -> Option<String> {
    if messages.is_empty() {
        return None;
    }
    let conversation_text = serialize_conversation(messages);
    if conversation_text.trim().is_empty() {
        return None;
    }
    Some(format!(
        "<conversation>\n{conversation_text}\n</conversation>\n\n{TURN_PREFIX_SUMMARIZATION_PROMPT}"
    ))
}

struct CompactionExtension;

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
    payload: Value,
}

impl exports::ragent::extension::lifecycle::Guest for CompactionExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "compaction",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: "model.response.observe",
                    kind: "observer",
                    priority: 50,
                    failure: "ignore",
                },
                Subscription {
                    hook: "context.project.prepare",
                    kind: "transform",
                    priority: 10,
                    failure: "abort",
                },
            ],
        })
        .unwrap_or_default()
    }

    fn initialize(config_json: String) -> Result<(), String> {
        let cfg = if config_json.trim().is_empty() || config_json.trim() == "null" {
            CompactionConfig::default()
        } else {
            serde_json::from_str::<CompactionConfig>(&config_json).unwrap_or_default()
        };
        let mut guard = STATE.lock().map_err(|e| e.to_string())?;
        *guard = Some(CompactionRuntimeState {
            config: cfg,
            last_observed_total_tokens: None,
            last_observed_item_count: 0,
        });
        Ok(())
    }

    fn invoke(request_json: String) -> Result<String, String> {
        let invocation: HookRequest =
            serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        match invocation.hook.as_str() {
            "model.response.observe" => {
                if let Some(total) = invocation
                    .payload
                    .get("usage")
                    .and_then(|u| u.get("total_tokens"))
                    .and_then(Value::as_u64)
                {
                    if let Ok(mut guard) = STATE.lock() {
                        if let Some(ref mut st) = *guard {
                            st.last_observed_total_tokens = Some(total);
                        }
                    }
                }
                Ok(json!({ "action": "unchanged" }).to_string())
            }
            "context.project.prepare" => {
                let items: Vec<Value> = match serde_json::from_value(invocation.payload) {
                    Ok(v) => v,
                    Err(_) => return Ok(json!({ "action": "unchanged" }).to_string()),
                };

                let mut guard = match STATE.lock() {
                    Ok(g) => g,
                    Err(_) => return Ok(json!({ "action": "unchanged" }).to_string()),
                };
                let Some(ref mut st) = *guard else {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                };

                if !st.config.enabled || items.len() < 3 {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                let estimated_all: u64 = items.iter().map(estimate_item_tokens).sum();
                let tokens_before = match st.last_observed_total_tokens {
                    Some(obs)
                        if st.last_observed_item_count > 0
                            && items.len() >= st.last_observed_item_count =>
                    {
                        let trailing: u64 = items[st.last_observed_item_count..]
                            .iter()
                            .map(estimate_item_tokens)
                            .sum();
                        (obs + trailing).max(estimated_all)
                    }
                    _ => estimated_all,
                };
                st.last_observed_item_count = items.len();

                if tokens_before < st.config.trigger_tokens {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                // Detect if `items[0]` is a previous compaction summary from an earlier generation
                let (start_index, previous_summary, mut file_ops) =
                    match parse_previous_compaction_summary(&items[0]) {
                        Some((prev_sum, prev_ops)) => (1usize, Some(prev_sum), prev_ops),
                        None => (0usize, None, FileOps::default()),
                    };

                if items.len() <= start_index + 1 {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                // Ensure `keep_recent_tokens` is strictly less than `trigger_tokens` so custom small thresholds work seamlessly
                let effective_keep_recent = st
                    .config
                    .keep_recent_tokens
                    .min((st.config.trigger_tokens / 2).max(1));

                let cut_point =
                    find_cut_point(&items, start_index, items.len(), effective_keep_recent);

                // If `first_kept_index <= start_index` and not a split turn, there are no older items to summarize
                if cut_point.first_kept_index <= start_index && !cut_point.is_split_turn {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                let history_end = if cut_point.is_split_turn {
                    cut_point
                        .turn_start_index
                        .unwrap_or(cut_point.first_kept_index)
                } else {
                    cut_point.first_kept_index
                };

                let messages_to_summarize = &items[start_index..history_end];
                let turn_prefix_messages = if cut_point.is_split_turn {
                    &items[history_end..cut_point.first_kept_index]
                } else {
                    &[][..]
                };

                if messages_to_summarize.is_empty() && turn_prefix_messages.is_empty() {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                extract_file_ops_from_items(
                    &items[start_index..cut_point.first_kept_index],
                    &mut file_ops,
                );
                let (read_files, modified_files) = compute_file_lists(&file_ops);

                let history_prompt = build_history_prompt(
                    messages_to_summarize,
                    previous_summary.as_deref(),
                    st.config.custom_instructions.as_deref(),
                );
                let turn_prefix_prompt = build_turn_prefix_prompt(turn_prefix_messages);

                if history_prompt.is_none() && turn_prefix_prompt.is_none() {
                    return Ok(json!({ "action": "unchanged" }).to_string());
                }

                let summarized_through_pos = (cut_point.first_kept_index.saturating_sub(1)) as u64;
                let (kept_from_pos, kept_through_pos) = if cut_point.first_kept_index < items.len()
                {
                    (
                        Some(cut_point.first_kept_index as u64),
                        Some((items.len() - 1) as u64),
                    )
                } else {
                    (None, None)
                };

                let plan = CompactionPlan {
                    summarized_through_pos,
                    kept_from_pos,
                    kept_through_pos,
                    tokens_before,
                    system_prompt: SUMMARIZATION_SYSTEM_PROMPT.to_string(),
                    history_prompt,
                    turn_prefix_prompt,
                    previous_summary,
                    read_files,
                    modified_files,
                };

                Ok(json!({
                    "action": "stop",
                    "payload": plan,
                    "reason": "context_compaction_triggered"
                })
                .to_string())
            }
            _ => Ok(json!({ "action": "unchanged" }).to_string()),
        }
    }

    fn shutdown() {}
}

export!(CompactionExtension);
