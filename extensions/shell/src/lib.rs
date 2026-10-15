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

#[derive(Deserialize)]
struct HostProcessResponse {
    ok: bool,
    error: Option<String>,
    exit_code: Option<i32>,
    stdout: Option<String>,
    stderr: Option<String>,
    combined: Option<String>,
    process: Option<serde_json::Value>,
    processes: Option<serde_json::Value>,
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
                    "description": "在宿主系统上执行终端命令。支持同步阻塞等待完成（wait=true，默认）或后台异步启动进程并立即返回 process_id 与真实 pid（wait=false）。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "command": {
                                "type": "string",
                                "description": "需要执行的终端命令"
                            },
                            "wait": {
                                "type": "boolean",
                                "default": true,
                                "description": "是否等待命令执行结束。默认为 true（阻塞等待并返回 stdout/stderr/exit_code）；若为 false 则后台运行并立即返回 process_id 和 pid。"
                            },
                            "lifecycle": {
                                "type": "string",
                                "enum": ["activation", "session"],
                                "default": "activation",
                                "description": "后台任务的最大生命周期。activation（默认）：当前 activation 结束后自动清理仍在运行的进程；session：跨 activation 保持运行，直至被主动停止或 session 关闭。"
                            },
                            "timeout_seconds": {
                                "type": "integer",
                                "minimum": 0,
                                "default": default_timeout_seconds(),
                                "description": "命令最大运行超时时间（秒），0 表示禁用超时。默认 1800 秒。"
                            }
                        },
                        "required": ["command"]
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "process_status",
                    "description": "查询由 shell(wait=false) 启动的后台任务进程的运行状态、真实 PID、返回码（exit_code）与运行时长。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "process_id": {
                                "type": "string",
                                "description": "可选的目标进程 ID（如 proc_... 或数字 PID）。若省略则列出当前会话所有托管进程。"
                            }
                        }
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "process_logs",
                    "description": "查询后台任务进程的输出日志，支持单独查看 stdout、stderr 或按时间合并的 combined (stdout+stderr)。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "process_id": {
                                "type": "string",
                                "description": "目标进程 ID（如 proc_... 或数字 PID）"
                            },
                            "stream": {
                                "type": "string",
                                "enum": ["stdout", "stderr", "combined"],
                                "default": "combined",
                                "description": "要查询的日志流：stdout、stderr 或 combined（默认）"
                            },
                            "tail_lines": {
                                "type": "integer",
                                "minimum": 1,
                                "default": 200,
                                "description": "返回最新 N 行日志（默认 200 行）"
                            },
                            "offset": {
                                "type": "integer",
                                "minimum": 0,
                                "description": "可选的字节偏移量，用于增量读取新增日志"
                            }
                        },
                        "required": ["process_id"]
                    }
                }));

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "process_stop",
                    "description": "主动停止（终止）指定的后台任务进程及其子进程组。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "process_id": {
                                "type": "string",
                                "description": "需要停止的目标进程 ID（如 proc_... 或数字 PID）"
                            }
                        },
                        "required": ["process_id"]
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
        "shell" => handle_shell(&call.arguments)?,
        "process_status" => handle_process_status(&call.arguments)?,
        "process_logs" => handle_process_logs(&call.arguments)?,
        "process_stop" => handle_process_stop(&call.arguments)?,
        other => return Err(format!("unknown tool: {other}")),
    };

    serde_json::to_string(&serde_json::json!({"action": "continue", "payload": result}))
        .map_err(|e| e.to_string())
}

fn handle_shell(args: &serde_json::Value) -> Result<ToolResult, String> {
    let command = args
        .get("command")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing string argument 'command'")?;
    let wait = args
        .get("wait")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let lifecycle = args
        .get("lifecycle")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("activation");
    let timeout_seconds = match args.get("timeout_seconds") {
        Some(value) => value
            .as_u64()
            .ok_or("'timeout_seconds' must be a non-negative integer")?,
        None => default_timeout_seconds(),
    };
    let timeout_ms = timeout_millis(timeout_seconds)?;

    let req_json = serde_json::json!({
        "action": "start",
        "command": command,
        "wait": wait,
        "lifecycle": lifecycle,
        "timeout_ms": timeout_ms,
    })
    .to_string();

    let raw_resp = ragent::extension::host::manage_process(&req_json);
    let resp: HostProcessResponse =
        serde_json::from_str(&raw_resp).map_err(|e| format!("invalid host response: {e}"))?;

    if wait {
        let exit_code = resp.exit_code.unwrap_or(-1);
        let stdout = resp.stdout.unwrap_or_default();
        let stderr = resp.stderr.unwrap_or_default();
        let success = resp.ok && exit_code == 0 && resp.error.is_none();
        let output = if let Some(ref error) = resp.error {
            format!("failed to execute command: {error}")
        } else {
            format!("exit_code: {exit_code}\nstdout:\n{stdout}\nstderr:\n{stderr}")
        };
        let error = (!success).then(|| {
            resp.error
                .unwrap_or_else(|| format!("command exited with status {exit_code}"))
        });
        Ok(ToolResult {
            success,
            output,
            error,
        })
    } else if resp.ok {
        let proc_json = resp.process.unwrap_or(serde_json::Value::Null);
        let output = serde_json::to_string_pretty(&proc_json).unwrap_or_default();
        Ok(ToolResult {
            success: true,
            output,
            error: None,
        })
    } else {
        let err = resp
            .error
            .unwrap_or_else(|| "failed to start background process".to_string());
        Ok(ToolResult {
            success: false,
            output: err.clone(),
            error: Some(err),
        })
    }
}

fn handle_process_status(args: &serde_json::Value) -> Result<ToolResult, String> {
    let process_id = args.get("process_id").and_then(serde_json::Value::as_str);
    let req_json = serde_json::json!({
        "action": "status",
        "process_id": process_id,
    })
    .to_string();

    let raw_resp = ragent::extension::host::manage_process(&req_json);
    let resp: HostProcessResponse =
        serde_json::from_str(&raw_resp).map_err(|e| format!("invalid host response: {e}"))?;
    if !resp.ok {
        let err = resp.error.unwrap_or_else(|| "status query failed".into());
        return Ok(ToolResult {
            success: false,
            output: err.clone(),
            error: Some(err),
        });
    }

    let payload = if let Some(p) = resp.process {
        p
    } else {
        resp.processes.unwrap_or(serde_json::json!([]))
    };
    Ok(ToolResult {
        success: true,
        output: serde_json::to_string_pretty(&payload).unwrap_or_default(),
        error: None,
    })
}

fn handle_process_logs(args: &serde_json::Value) -> Result<ToolResult, String> {
    let process_id = args
        .get("process_id")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing string argument 'process_id'")?;
    let stream = args
        .get("stream")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("combined");
    let tail_lines = args.get("tail_lines").and_then(serde_json::Value::as_u64);
    let offset = args.get("offset").and_then(serde_json::Value::as_u64);

    let req_json = serde_json::json!({
        "action": "logs",
        "process_id": process_id,
        "stream": stream,
        "tail_lines": tail_lines,
        "offset": offset,
    })
    .to_string();

    let raw_resp = ragent::extension::host::manage_process(&req_json);
    let resp: HostProcessResponse =
        serde_json::from_str(&raw_resp).map_err(|e| format!("invalid host response: {e}"))?;
    if !resp.ok {
        let err = resp.error.unwrap_or_else(|| "logs query failed".into());
        return Ok(ToolResult {
            success: false,
            output: err.clone(),
            error: Some(err),
        });
    }

    let text = match stream {
        "stdout" => resp.stdout.unwrap_or_default(),
        "stderr" => resp.stderr.unwrap_or_default(),
        _ => resp
            .combined
            .or(resp.stdout)
            .or(resp.stderr)
            .unwrap_or_default(),
    };
    Ok(ToolResult {
        success: true,
        output: text,
        error: None,
    })
}

fn handle_process_stop(args: &serde_json::Value) -> Result<ToolResult, String> {
    let process_id = args
        .get("process_id")
        .and_then(serde_json::Value::as_str)
        .ok_or("missing string argument 'process_id'")?;
    let req_json = serde_json::json!({
        "action": "stop",
        "process_id": process_id,
    })
    .to_string();

    let raw_resp = ragent::extension::host::manage_process(&req_json);
    let resp: HostProcessResponse =
        serde_json::from_str(&raw_resp).map_err(|e| format!("invalid host response: {e}"))?;
    if !resp.ok {
        let err = resp.error.unwrap_or_else(|| "stop process failed".into());
        return Ok(ToolResult {
            success: false,
            output: err.clone(),
            error: Some(err),
        });
    }

    let output = serde_json::to_string_pretty(&resp.process.unwrap_or(serde_json::Value::Null))
        .unwrap_or_default();
    Ok(ToolResult {
        success: true,
        output,
        error: None,
    })
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
