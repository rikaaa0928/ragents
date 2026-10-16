use serde::{Deserialize, Serialize};
use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";
const FALLBACK_TIMEOUT_SECONDS: u64 = 0;
const MAX_TIMEOUT_MS: u64 = 2_147_483_647;
const DEFAULT_MAX_LINES: usize = 2000;
const DEFAULT_MAX_BYTES: usize = 50 * 1024; // 50KB

thread_local! {
    static DEFAULT_TIMEOUT_SECONDS: Cell<u64> = const { Cell::new(FALLBACK_TIMEOUT_SECONDS) };
    static MAX_OUTPUT_LINES: Cell<usize> = const { Cell::new(DEFAULT_MAX_LINES) };
    static MAX_OUTPUT_BYTES: Cell<usize> = const { Cell::new(DEFAULT_MAX_BYTES) };
    static SPILL_SEQ: Cell<u64> = const { Cell::new(1) };
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
    max_output_lines: Option<usize>,
    max_output_bytes: Option<usize>,
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
        MAX_OUTPUT_LINES.with(|c| c.set(config.max_output_lines.unwrap_or(DEFAULT_MAX_LINES)));
        MAX_OUTPUT_BYTES.with(|c| c.set(config.max_output_bytes.unwrap_or(DEFAULT_MAX_BYTES)));
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
                    "description": "Execute a shell command on the host system. Supports synchronous blocking execution (wait=true, default) or asynchronous background execution returning process_id and pid (wait=false). For blocking commands, output is truncated to the last 2000 lines or 50KB (whichever is hit first); if truncated, full output is saved to a temp file.",
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
                            "timeout": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Timeout in seconds (optional, no default timeout)"
                            },
                            "timeout_seconds": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Timeout in seconds (optional, no default timeout; alias of timeout)"
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
    let timeout_ms = match args.get("timeout").or_else(|| args.get("timeout_seconds")) {
        Some(value) if !value.is_null() => {
            let secs = value
                .as_f64()
                .ok_or("Invalid timeout: must be a finite number of seconds")?;
            if !secs.is_finite() || secs < 0.0 {
                return Err("Invalid timeout: must be a finite non-negative number of seconds".into());
            }
            if secs == 0.0 {
                0
            } else {
                let ms = (secs * 1000.0).round() as u64;
                if ms > MAX_TIMEOUT_MS {
                    return Err(format!(
                        "Invalid timeout: maximum is {} seconds",
                        MAX_TIMEOUT_MS / 1000
                    ));
                }
                ms.max(1)
            }
        }
        _ => timeout_millis(default_timeout_seconds())?,
    };

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
        let raw_stdout = resp.stdout.unwrap_or_default();
        let raw_stderr = resp.stderr.unwrap_or_default();
        let stdout = truncate_tail_stream(&raw_stdout, "stdout");
        let stderr = truncate_tail_stream(&raw_stderr, "stderr");
        let success = resp.ok && exit_code == 0 && resp.error.is_none();
        let output = if let Some(ref error) = resp.error {
            format!("exit_code: {exit_code}\nstdout:\n{stdout}\nstderr:\n{stderr}\nerror: {error}")
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

fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn split_lines_for_counting(content: &str) -> Vec<&str> {
    if content.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&str> = content.split('\n').collect();
    if content.ends_with('\n') {
        lines.pop();
    }
    lines
}

fn truncate_str_to_bytes_from_end(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut start = s.len() - max_bytes;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

fn truncate_tail_stream(content: &str, stream_label: &str) -> String {
    let max_lines = MAX_OUTPUT_LINES.with(Cell::get);
    let max_bytes = MAX_OUTPUT_BYTES.with(Cell::get);
    let total_bytes = content.len();
    let lines = split_lines_for_counting(content);
    let total_lines = lines.len();

    if total_lines <= max_lines && total_bytes <= max_bytes {
        return content.to_string();
    }

    let seq = SPILL_SEQ.with(|c| {
        let cur = c.get();
        c.set(cur + 1);
        cur
    });
    let spill_dir = std::path::Path::new(".ragents-tmp");
    let _ = std::fs::create_dir_all(spill_dir);
    let spill_rel = format!(".ragents-tmp/shell-{stream_label}-{seq}.log");
    let _ = std::fs::write(&spill_rel, content);

    let mut output_lines_rev: Vec<&str> = Vec::new();
    let mut output_bytes_count = 0usize;
    let mut truncated_by_lines = false;
    let mut last_line_partial = false;

    for &line in lines.iter().rev() {
        if output_lines_rev.len() >= max_lines {
            truncated_by_lines = true;
            break;
        }
        let line_bytes = line.len() + usize::from(!output_lines_rev.is_empty());
        if output_bytes_count + line_bytes > max_bytes {
            truncated_by_lines = false;
            if output_lines_rev.is_empty() {
                let partial = truncate_str_to_bytes_from_end(line, max_bytes);
                output_lines_rev.push(partial);
                output_bytes_count = partial.len();
                last_line_partial = true;
            }
            break;
        }
        output_lines_rev.push(line);
        output_bytes_count += line_bytes;
    }

    if output_lines_rev.len() >= max_lines && output_bytes_count <= max_bytes {
        truncated_by_lines = true;
    }

    output_lines_rev.reverse();
    let output_lines_count = output_lines_rev.len();
    let mut text = output_lines_rev.join("\n");
    let start_line = total_lines.saturating_sub(output_lines_count) + 1;
    let end_line = total_lines;

    if last_line_partial {
        let last_line_size = format_size(lines.last().map(|l| l.len()).unwrap_or(0));
        let shown_size = format_size(output_bytes_count);
        text.push_str(&format!(
            "\n\n[Showing last {shown_size} of line {end_line} (line is {last_line_size}). Full output: {spill_rel}]"
        ));
    } else if truncated_by_lines {
        text.push_str(&format!(
            "\n\n[Showing lines {start_line}-{end_line} of {total_lines}. Full output: {spill_rel}]"
        ));
    } else {
        let limit_str = format_size(max_bytes);
        text.push_str(&format!(
            "\n\n[Showing lines {start_line}-{end_line} of {total_lines} ({limit_str} limit). Full output: {spill_rel}]"
        ));
    }

    text
}
