use serde::{Deserialize, Serialize};
use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";
const FALLBACK_TIMEOUT_SECONDS: u64 = 30 * 60;

thread_local! {
    static DEFAULT_TIMEOUT_SECONDS: Cell<u64> = const { Cell::new(FALLBACK_TIMEOUT_SECONDS) };
}

struct ShellExtension;

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

#[derive(Default, Deserialize)]
struct ShellConfig {
    default_timeout_seconds: Option<u64>,
}

impl exports::ragent::extension::lifecycle::Guest for ShellExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "shell",
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

    fn initialize(config: String) -> Result<(), String> {
        let value: serde_json::Value = serde_json::from_str(&config).map_err(|e| e.to_string())?;
        let config = if value.is_null() {
            ShellConfig::default()
        } else {
            serde_json::from_value(value).map_err(|e| e.to_string())?
        };
        let timeout = config
            .default_timeout_seconds
            .unwrap_or(FALLBACK_TIMEOUT_SECONDS);
        timeout_millis(timeout)?;
        DEFAULT_TIMEOUT_SECONDS.with(|current| current.set(timeout));
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
                    "name": "shell",
                    "description": "在宿主系统上执行 shell 命令。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "command": {
                                "type": "string",
                                "description": "需要执行的终端命令"
                            },
                            "timeout_seconds": {
                                "type": "integer",
                                "minimum": 0,
                                "default": default_timeout_seconds(),
                                "description": "命令执行超时时间（秒），0 表示禁用超时。未填写时使用 Shell 扩展配置的默认值；若未配置则为 1800 秒（30 分钟）。建议根据实际情况填写一个合理的兜底值。"
                            }
                        },
                        "required": ["command"]
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
    if call.name != "shell" {
        return Err(format!("unknown tool: {}", call.name));
    }

    let command = call
        .arguments
        .get("command")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing string argument 'command'")?;
    let timeout_seconds = match call.arguments.get("timeout_seconds") {
        Some(value) => value
            .as_u64()
            .ok_or("'timeout_seconds' must be a non-negative integer")?,
        None => default_timeout_seconds(),
    };
    let timeout_ms = timeout_millis(timeout_seconds)?;
    let result = ragent::extension::host::execute_command_with_timeout(command, timeout_ms);

    let success = result.exit_code == 0 && result.error.is_none();
    let output = if let Some(error) = &result.error {
        format!("failed to execute command: {error}")
    } else {
        format!(
            "exit_code: {}\nstdout:\n{}\nstderr:\n{}",
            result.exit_code, result.stdout, result.stderr
        )
    };
    let error = (!success).then(|| {
        result
            .error
            .unwrap_or_else(|| format!("command exited with status {}", result.exit_code))
    });

    let result = ToolResult {
        success,
        output,
        error,
    };
    serde_json::to_string(&serde_json::json!({"action": "continue", "payload": result}))
        .map_err(|e| e.to_string())
}

export!(ShellExtension);

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS.with(Cell::get)
}

fn timeout_millis(seconds: u64) -> Result<u64, String> {
    seconds
        .checked_mul(1_000)
        .ok_or_else(|| "timeout is too large".into())
}
