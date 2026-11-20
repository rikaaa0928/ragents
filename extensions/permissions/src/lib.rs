use serde::{Deserialize, Serialize};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";

struct PermissionsExtension;

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
struct RequestPermissionArgs {
    capability: String,
    target: String,
    reason: String,
    #[serde(default)]
    method: Option<String>,
}

#[derive(Deserialize)]
struct HostPermissionResponse {
    ok: bool,
    allowed: bool,
    #[serde(default)]
    denied: bool,
    #[serde(default)]
    persisted: bool,
    granted_target: Option<String>,
    reason: Option<String>,
    error: Option<String>,
}

impl exports::ragent::extension::lifecycle::Guest for PermissionsExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "permissions",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: AGENT_PREPARE_HOOK,
                    kind: "transform",
                    priority: 88,
                    failure: "ignore",
                },
                Subscription {
                    hook: TOOLS_CALL_HOOK,
                    kind: "action",
                    priority: 88,
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
                let tools = vec![serde_json::json!({
                    "enabled": true,
                    "concurrency_safe": false,
                    "name": "request_permission",
                    "description": "Explicitly request user approval to access a filesystem path/directory or HTTP host outside your default authorized workspace (`.`, room shared temp dir, and `$TMPDIR`). Use this when an operation (such as `shell`, container mounts, or creating new external directories) failed with `Read-only file system` / `Permission denied` / `Operation not permitted`, or when pre-authorizing external paths/hosts before running complex commands. Do NOT call this for paths inside `.`, the room temp dir, or `$TMPDIR`.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "capability": {
                                "type": "string",
                                "enum": ["file_read", "file_write", "command_work_dir", "http"],
                                "description": "Capability type to request: `file_read` (read external path/dir), `file_write` (write/create external path/dir; automatically includes read access on the granted path unless read is explicitly denied), `command_work_dir` (execute commands in an external working dir), or `http` (allow WASI HTTP access to an external host)."
                            },
                            "target": {
                                "type": "string",
                                "description": "Target filesystem path (e.g. `/var/home/user/models/project` or `~/models`) for file/command capabilities, or hostname/URL (e.g. `api.github.com` or `https://api.github.com`) for `http`."
                            },
                            "reason": {
                                "type": "string",
                                "description": "Clear, user-facing explanation of why this access is needed for the current task."
                            },
                            "method": {
                                "type": "string",
                                "default": "GET",
                                "description": "Optional HTTP method when `capability` is `http` (e.g. `GET`, `POST`)."
                            }
                        },
                        "required": ["capability", "target", "reason"],
                        "additionalProperties": false
                    }
                })];

                let append_prompt = "<permission_guidance>\n\
Your workspace directory (`.`), room shared temporary directory (if any), and `$TMPDIR` are authorized for read/write by default—operate on them directly without calling `request_permission`.\n\
External paths and external HTTP hosts default to interactive approval (`ask`):\n\
1. Do NOT stop or give up simply because a required path is outside `.` or because an initial command/tool reports `Permission denied`, `Operation not permitted`, `Read-only file system`, or sandbox access errors.\n\
2. File/image tools (`read_file`, `write_file`, `replace_in_file`, `view_image`) and `shell` automatically trigger an interactive approval prompt on unauthorized paths—attempt the operation directly, or call `request_permission` to explicitly request access when `shell`/container/compound workflows, nonexistent external directories, or HTTP hosts are restricted or need pre-authorization.\n\
3. `file_write` automatically includes read access (`file_read`) for the granted path (unless read is explicitly denied by policy): when you already have or are requesting `file_write`, do NOT request `file_read` separately for the same path.\n\
4. Only stop or switch approaches for that target if the user explicitly denies approval (`Denied by user interaction` / `[PERMISSION_DENIED]`) or policy blocks it (`Denied by permission policy`).\n\
</permission_guidance>";

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
                if call.name != "request_permission" {
                    return Ok(serde_json::json!({
                        "action": "unchanged"
                    })
                    .to_string());
                }

                let args: RequestPermissionArgs = match serde_json::from_value(call.arguments) {
                    Ok(a) => a,
                    Err(e) => {
                        let msg = format!("Invalid arguments for request_permission: {e}");
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

                let host_req = serde_json::json!({
                    "capability": args.capability,
                    "target": args.target,
                    "reason": args.reason,
                    "method": args.method,
                });

                let raw_resp = ragent::extension::host::request_permission(&host_req.to_string());
                let parsed: HostPermissionResponse = match serde_json::from_str(&raw_resp) {
                    Ok(r) => r,
                    Err(e) => {
                        let msg = format!("Failed to parse host permission response: {e}");
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
                    let msg = parsed
                        .error
                        .unwrap_or_else(|| "Unknown permission request error".into());
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

                if parsed.allowed {
                    let scope_note = if parsed.persisted {
                        "persisted for current activation/scope"
                    } else {
                        "granted"
                    };
                    let granted = parsed
                        .granted_target
                        .as_deref()
                        .unwrap_or(args.target.as_str());
                    let output = format!(
                        "Permission GRANTED ({scope_note}) for `{}` on `{}` (approved target: `{}`). You may now proceed with your operation.",
                        args.capability, args.target, granted
                    );
                    return Ok(serde_json::json!({
                        "action": "continue",
                        "payload": ToolResult {
                            success: true,
                            output,
                            error: None,
                        }
                    })
                    .to_string());
                }

                let deny_reason = parsed
                    .reason
                    .unwrap_or_else(|| "Denied by user or permission policy".into());
                let denied_msg = format!(
                    "[PERMISSION_DENIED] Permission DENIED for `{}` on `{}`: {deny_reason}\nDo NOT retry this same restricted target unless the user instructs otherwise; switch to an alternative approach within authorized directories or inform the user.",
                    args.capability, args.target
                );
                Ok(serde_json::json!({
                    "action": "continue",
                    "payload": ToolResult {
                        success: false,
                        output: denied_msg,
                        error: Some(if parsed.denied {
                            deny_reason
                        } else {
                            "permission_denied".into()
                        }),
                    }
                })
                .to_string())
            }
            hook => Err(format!("unsupported hook: {hook}")),
        }
    }

    fn shutdown() {}
}

export!(PermissionsExtension);
