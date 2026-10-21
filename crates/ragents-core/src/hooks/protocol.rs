use openresponses_rust::{FunctionOutput, MessageContent};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const HOOK_AGENT_PREPARE: &str = "agent.prepare";
pub const HOOK_CONTEXT_PROJECT_PREPARE: &str = "context.project.prepare";
pub const HOOK_MODEL_RESPONSE_OBSERVE: &str = "model.response.observe";
pub const HOOK_TOOL_CALL_PREPARE: &str = "tool.call.prepare";
pub const HOOK_PERMISSION_REQUIREMENTS: &str = "permission.requirements";
pub const HOOK_TOOLS_CALL: &str = "tools.call";
pub const HOOK_TOOL_RESULT_PREPARE: &str = "tool.result.prepare";
pub const HOOK_TURN_COMPLETE: &str = "turn.complete";
pub const HOOK_AGENT_ERROR: &str = "agent.error";
pub const HOOK_AGENT_SHUTDOWN: &str = "agent.shutdown";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum HookKind {
    Transform,
    Action,
    Observer,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum HookFailurePolicy {
    #[default]
    Abort,
    Ignore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HookSubscription {
    pub hook: String,
    pub kind: HookKind,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub failure: HookFailurePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionMetadata {
    pub id: String,
    pub version: String,
    #[serde(default)]
    pub subscriptions: Vec<HookSubscription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookRequest {
    pub hook: String,
    pub protocol_version: u32,
    pub invocation_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iteration: Option<usize>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookAction {
    Continue,
    Unchanged,
    Reject,
    Skip,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookResult {
    pub action: HookAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowControl {
    Continue,
    Skip,
    Stop,
}

#[derive(Debug, Clone)]
pub struct TransformResult {
    pub payload: serde_json::Value,
    pub control: FlowControl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDraft {
    pub name: String,
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<openresponses_rust::ReasoningConfig>,
}

impl ModelDraft {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            temperature: None,
            max_output_tokens: None,
            reasoning: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtensionPrepareOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub append_system_prompt: Option<String>,
    #[serde(default)]
    pub tools: Vec<ToolEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDraft {
    pub system_prompt: String,
    pub model: ModelDraft,
    #[serde(default)]
    pub tools: Vec<ToolEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(flatten)]
    pub definition: ToolDefinition,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub call_id: String,
    pub tool_id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolOutput {
    Text(String),
    Parts(Vec<MessageContent>),
}

impl ToolOutput {
    pub fn to_display_string(&self) -> String {
        match self {
            ToolOutput::Text(text) => text.clone(),
            ToolOutput::Parts(parts) => {
                let texts: Vec<&str> = parts
                    .iter()
                    .filter_map(|part| match part {
                        MessageContent::InputText { text }
                        | MessageContent::PlainText { text }
                        | MessageContent::OutputText { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                if texts.is_empty() {
                    format!("[{} content part(s)]", parts.len())
                } else {
                    texts.join("\n")
                }
            }
        }
    }

    pub fn to_function_output(&self) -> FunctionOutput {
        match self {
            ToolOutput::Text(text) => FunctionOutput::Text(text.clone()),
            ToolOutput::Parts(parts) => FunctionOutput::Content(parts.clone()),
        }
    }
}

impl Serialize for ToolOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ToolOutput::Text(s) => serializer.serialize_str(s),
            ToolOutput::Parts(vec) => vec.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ToolOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(s) = value.as_str() {
            return Ok(ToolOutput::Text(s.to_string()));
        }
        if value.is_array() {
            let parts: Vec<MessageContent> =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            return Ok(ToolOutput::Parts(parts));
        }
        Err(serde::de::Error::custom(
            "ToolOutput must be a string or an array of MessageContent",
        ))
    }
}

impl From<String> for ToolOutput {
    fn from(s: String) -> Self {
        ToolOutput::Text(s)
    }
}

impl From<&str> for ToolOutput {
    fn from(s: &str) -> Self {
        ToolOutput::Text(s.to_string())
    }
}

impl From<Vec<MessageContent>> for ToolOutput {
    fn from(v: Vec<MessageContent>) -> Self {
        ToolOutput::Parts(v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub success: bool,
    pub output: ToolOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ToolResult {
    pub fn ok(output: impl Into<ToolOutput>) -> Self {
        Self {
            success: true,
            output: output.into(),
            error: None,
        }
    }

    pub fn err(error: impl Into<String>) -> Self {
        let error = error.into();
        Self {
            success: false,
            output: ToolOutput::Text(error.clone()),
            error: Some(error),
        }
    }
}
