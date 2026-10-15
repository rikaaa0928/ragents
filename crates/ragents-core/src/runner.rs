use crate::context::ContextProjection;
use crate::error::CoreError;
use crate::hooks::*;
use crate::host::{
    CoreExecutor, CoreHost, CoreRunSpec, CoreRunSummary, ModelEndpointConfig, PermissionOutcome,
};
use crate::model::ModelBackend;
use crate::sandbox::SandboxState;
use async_trait::async_trait;
use openresponses_rust::{
    CreateResponseBody, InputTokensDetails, Item, MessageContent, OutputTokensDetails,
    ReasoningConfig, ResponseResource, ResponseStatus, Tool, ToolChoiceParam, Usage,
};
use ragents_protocol::{CapabilityRequest, SessionEvent, TurnId};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq)]
pub struct AgentFunctionCall {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentCoreStepResult {
    pub output_items: Vec<Item>,
    pub function_calls: Vec<AgentFunctionCall>,
    pub response_id: Option<String>,
    pub usage: Option<Usage>,
    pub text: String,
    pub reasoning_text: String,
}

pub struct AgentCore;

impl AgentCore {
    #[allow(clippy::too_many_arguments)]
    pub async fn step(
        instructions: &str,
        projection: &ContextProjection,
        model_name: &str,
        temperature: Option<f64>,
        max_output_tokens: Option<i32>,
        reasoning: Option<ReasoningConfig>,
        tools: Vec<Tool>,
        endpoint: &ModelEndpointConfig,
        model_backend: &dyn ModelBackend,
    ) -> Result<AgentCoreStepResult, CoreError> {
        let input = projection.to_openresponses_input();
        let has_tools = !tools.is_empty();

        let request = CreateResponseBody {
            input: Some(input),
            model: Some(model_name.to_string()),
            instructions: Some(instructions.to_string()),
            tools: if has_tools { Some(tools) } else { None },
            tool_choice: if has_tools {
                Some(ToolChoiceParam::default())
            } else {
                None
            },
            temperature,
            max_output_tokens,
            reasoning,
            stream: Some(false),
            ..Default::default()
        };

        let response: ResponseResource = model_backend.create_response(endpoint, request).await?;

        if let Some(err) = response.error {
            return Err(CoreError::ResponseFailed(err.message));
        }
        if response.status == ResponseStatus::Failed {
            return Err(CoreError::ResponseFailed(
                "Response status is failed".into(),
            ));
        }

        let output_items = response.output;
        let mut function_calls = Vec::new();
        let mut text = String::new();
        let mut reasoning_text = String::new();

        for item in &output_items {
            match item {
                Item::Message { content, .. } => {
                    for part in content {
                        match part {
                            MessageContent::OutputText {
                                text: part_text, ..
                            }
                            | MessageContent::PlainText { text: part_text } => {
                                text.push_str(part_text);
                            }
                            MessageContent::Refusal { refusal } => {
                                text.push_str(refusal);
                            }
                            _ => {}
                        }
                    }
                }
                Item::Reasoning {
                    summary, content, ..
                } => {
                    for part in summary {
                        match part {
                            MessageContent::SummaryText { text: part_text }
                            | MessageContent::OutputText {
                                text: part_text, ..
                            }
                            | MessageContent::PlainText { text: part_text } => {
                                reasoning_text.push_str(part_text);
                            }
                            _ => {}
                        }
                    }
                    if reasoning_text.is_empty() {
                        if let Some(content_parts) = content {
                            for part in content_parts {
                                match part {
                                    MessageContent::SummaryText { text: part_text }
                                    | MessageContent::OutputText {
                                        text: part_text, ..
                                    }
                                    | MessageContent::PlainText { text: part_text } => {
                                        reasoning_text.push_str(part_text);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Item::FunctionCall {
                    call_id,
                    name,
                    arguments,
                    ..
                } => {
                    function_calls.push(AgentFunctionCall {
                        call_id: call_id.clone(),
                        name: name.clone(),
                        arguments: arguments.clone(),
                    });
                }
                _ => {}
            }
        }

        Ok(AgentCoreStepResult {
            output_items,
            function_calls,
            response_id: Some(response.id),
            usage: response.usage,
            text,
            reasoning_text,
        })
    }
}

/// First-edition in-process coroutine executor for `ragents-core`.
/// Strictly isolates workspace paths per activation without mutating process-global `cwd`.
#[derive(Debug, Clone, Default)]
pub struct InProcessExecutor;

#[async_trait]
impl CoreExecutor for InProcessExecutor {
    async fn execute(
        &self,
        run_spec: CoreRunSpec,
        host: Arc<dyn CoreHost>,
        model_backend: Arc<dyn ModelBackend>,
        cancellation: CancellationToken,
    ) -> Result<CoreRunSummary, CoreError> {
        let _ = std::fs::create_dir_all(&run_spec.workspace_root);
        let _ = std::fs::create_dir_all(&run_spec.session_tmp_dir);

        let mut hook_manager = HookManager::empty();
        let sandbox_state = Arc::new(SandboxState::from_run_spec(&run_spec));
        let preopen_dirs = vec![
            run_spec.workspace_root.clone(),
            run_spec.session_tmp_dir.clone(),
        ];

        for ext in &run_spec.effective_config.extensions {
            if ext.enabled {
                let p = Path::new(&ext.path);
                let full_path = if p.is_absolute() {
                    p.to_path_buf()
                } else {
                    run_spec.workspace_root.join(p)
                };
                if !full_path.exists() {
                    let _ = hook_manager.shutdown().await;
                    return Err(CoreError::ToolError(format!(
                        "Configured extension '{}' does not exist at {:?}",
                        ext.name, full_path
                    )));
                }
                let plugin = WasmPlugin::load_from_file_with_host(
                    &ext.name,
                    &full_path,
                    &run_spec.workspace_root,
                    &preopen_dirs,
                    Some(host.clone()),
                    sandbox_state.clone(),
                )
                .await?;
                hook_manager.add_plugin_with_config(plugin, ext.config.clone())?;
            }
        }

        hook_manager.validate_subscriptions()?;
        tokio::select! {
            res = hook_manager.initialize() => res?,
            _ = cancellation.cancelled() => {
                let _ = hook_manager.shutdown().await;
                return Err(CoreError::Cancelled);
            }
        };

        let result = run_react_loop(
            &run_spec,
            &hook_manager,
            host.as_ref(),
            model_backend.as_ref(),
            &sandbox_state,
            &cancellation,
        )
        .await;

        let _ = hook_manager.shutdown().await;
        result
    }
}

async fn run_react_loop(
    run_spec: &CoreRunSpec,
    hook_manager: &HookManager,
    host: &dyn CoreHost,
    model_backend: &dyn ModelBackend,
    sandbox_state: &SandboxState,
    cancellation: &CancellationToken,
) -> Result<CoreRunSummary, CoreError> {
    let cfg = &run_spec.effective_config;
    let initial_draft = AgentDraft {
        system_prompt: cfg.system_prompt.clone(),
        model: ModelDraft {
            name: cfg.model.name.clone(),
            temperature: cfg.model.temperature,
            max_output_tokens: cfg.model.max_output_tokens,
            reasoning: cfg.model.reasoning.clone(),
        },
        tools: vec![],
        context: None,
    };

    let (draft, _) = tokio::select! {
        res = hook_manager.transform_agent_draft(HOOK_AGENT_PREPARE, None, initial_draft) => res?,
        _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
    };

    let active_tools = draft
        .tools
        .iter()
        .filter(|t| t.enabled)
        .cloned()
        .collect::<Vec<_>>();

    let mut request_tools = active_tools
        .iter()
        .map(|t| {
            Tool::function(t.definition.name.clone())
                .with_description(t.definition.description.clone())
                .with_parameters(t.definition.parameters.clone())
        })
        .collect::<Vec<_>>();

    if !active_tools.is_empty()
        && !request_tools.iter().any(|t| match t {
            Tool::Function { name, .. } => name == "request_permission",
            _ => false,
        })
    {
        request_tools.push(
            Tool::function("request_permission".to_string())
                .with_description(
                    "Request user permission to read/write an external directory or file outside workspace_root (e.g., when a shell command fails with 'Operation not permitted' because a tool like uv/pip/npm needs write access to ~/.cache/uv)."
                        .to_string(),
                )
                .with_parameters(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "capability": {
                            "type": "string",
                            "enum": ["file_write", "file_read", "command_work_dir"],
                            "description": "Capability type to request (usually 'file_write' for external caches/directories)."
                        },
                        "path": {
                            "type": "string",
                            "description": "Absolute path of the directory or file to unlock (prefer the top-level directory such as '/Users/.../.cache/uv' rather than an internal lockfile)."
                        },
                        "reason": {
                            "type": "string",
                            "description": "Explanation shown to the user for why this permission is needed."
                        }
                    },
                    "required": ["capability", "path", "reason"]
                })),
        );
    }

    let mut iteration = 0usize;
    let mut final_text = String::new();
    let mut accumulated_usage: Option<Usage> = None;
    let mut last_turn_id: Option<TurnId> = None;

    loop {
        if cancellation.is_cancelled() {
            return Err(CoreError::Cancelled);
        }
        if let Some(max_turns) = run_spec.max_turns {
            if iteration >= max_turns {
                break;
            }
        }
        iteration += 1;

        let turn_id = TurnId::generate();
        last_turn_id = Some(turn_id.clone());

        host.emit_event(Some(turn_id.clone()), SessionEvent::TurnStarted)
            .await?;

        let raw_items = host.load_effective_context().await?;
        let projected_items =
            apply_context_projection_hook(hook_manager, iteration, raw_items).await?;
        let projection = ContextProjection::new(projected_items);

        let step = tokio::select! {
            res = AgentCore::step(
                &draft.system_prompt,
                &projection,
                &draft.model.name,
                draft.model.temperature,
                draft.model.max_output_tokens,
                draft.model.reasoning.clone(),
                request_tools.clone(),
                &run_spec.model_endpoint,
                model_backend,
            ) => res?,
            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
        };

        if let Some(ref usage) = step.usage {
            accumulated_usage = Some(match accumulated_usage {
                Some(ref acc) => add_usage(acc, usage),
                None => usage.clone(),
            });
        }

        if !step.text.is_empty() {
            final_text = step.text.clone();
        }

        host.commit_model_output(
            &turn_id,
            step.response_id.clone(),
            step.output_items.clone(),
            step.usage.clone(),
        )
        .await?;

        if !step.function_calls.is_empty() {
            for call in &step.function_calls {
                if cancellation.is_cancelled() {
                    return Err(CoreError::Cancelled);
                }

                host.emit_event(
                    Some(turn_id.clone()),
                    SessionEvent::ToolCallStarted {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                    },
                )
                .await?;

                let started_at = Instant::now();
                let (tool_output_item, success) = tokio::select! {
                    res = execute_tool_call(
                        run_spec,
                        call,
                        &active_tools,
                        hook_manager,
                        host,
                        sandbox_state,
                        iteration,
                    ) => res?,
                    _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                };

                let duration_ms = started_at.elapsed().as_millis() as u64;
                host.commit_tool_output(
                    &turn_id,
                    &call.call_id,
                    &call.name,
                    success,
                    duration_ms,
                    vec![tool_output_item],
                )
                .await?;
            }
        } else {
            break;
        }
    }

    Ok(CoreRunSummary {
        activation_id: run_spec.activation_id.clone(),
        final_text,
        total_usage: accumulated_usage,
        turns_executed: iteration,
        last_turn_id,
    })
}

async fn apply_context_projection_hook(
    hook_manager: &HookManager,
    iteration: usize,
    items: Vec<Item>,
) -> Result<Vec<Item>, CoreError> {
    let payload = serde_json::to_value(&items)?;
    let transformed = hook_manager
        .transform_validated(
            HOOK_CONTEXT_PROJECT_PREPARE,
            Some(iteration),
            payload,
            |_| Ok(()),
        )
        .await?;
    Ok(serde_json::from_value(transformed.payload).unwrap_or(items))
}

async fn execute_tool_call(
    run_spec: &CoreRunSpec,
    call: &AgentFunctionCall,
    active_tools: &[ToolEntry],
    hook_manager: &HookManager,
    host: &dyn CoreHost,
    sandbox_state: &SandboxState,
    iteration: usize,
) -> Result<(Item, bool), CoreError> {
    if call.name == "request_permission" {
        return execute_builtin_request_permission(run_spec, call, host, sandbox_state).await;
    }

    let tool = match active_tools.iter().find(|t| t.definition.name == call.name) {
        Some(t) => t,
        None => {
            let err_item = Item::FunctionCallOutput {
                id: None,
                call_id: call.call_id.clone(),
                output: ToolResult::err(format!("Tool not found: {}", call.name))
                    .output
                    .to_function_output(),
                status: None,
            };
            return Ok((err_item, false));
        }
    };

    let arguments_val: serde_json::Value = serde_json::from_str(&call.arguments)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

    let initial_req = ToolCallRequest {
        call_id: call.call_id.clone(),
        tool_id: tool.id.clone().unwrap_or_default(),
        name: call.name.clone(),
        arguments: arguments_val,
    };

    let transformed = hook_manager
        .transform_validated(
            HOOK_TOOL_CALL_PREPARE,
            Some(iteration),
            serde_json::to_value(&initial_req)?,
            |_| Ok(()),
        )
        .await?;

    let call_req: ToolCallRequest =
        serde_json::from_value(transformed.payload).unwrap_or(initial_req);

    let owner = tool
        .owner
        .as_deref()
        .ok_or_else(|| CoreError::ToolError("Tool has no owner".into()))?;

    // Check required capabilities before invoking tool action (INV-013)
    let required_caps = infer_capabilities_for_tool_call(run_spec, &call_req);
    for (cap, reason) in required_caps {
        match host
            .check_permission(Some(owner), cap.clone(), Some(reason))
            .await?
        {
            PermissionOutcome::Allowed {
                effective_capability,
                persist_in_activation,
            } => {
                if persist_in_activation {
                    let granted_cap = effective_capability.unwrap_or(cap);
                    match granted_cap {
                        CapabilityRequest::FileWrite { path } => {
                            sandbox_state.allow_writable_path(path);
                        }
                        CapabilityRequest::CommandWorkDir { work_dir } => {
                            sandbox_state.allow_writable_path(work_dir);
                        }
                        CapabilityRequest::FileRead { path } => {
                            sandbox_state.allow_readable_path(path);
                        }
                        CapabilityRequest::Http { .. } => {}
                    }
                }
            }
            PermissionOutcome::Denied { reason } => {
                let denied_res = ToolResult::err(format!("Permission denied: {reason}"));
                let output_item = Item::FunctionCallOutput {
                    id: None,
                    call_id: call.call_id.clone(),
                    output: denied_res.output.to_function_output(),
                    status: None,
                };
                return Ok((output_item, false));
            }
        }
    }

    let action_result = hook_manager
        .action(
            HOOK_TOOLS_CALL,
            owner,
            Some(iteration),
            serde_json::to_value(call_req)?,
        )
        .await;

    let tool_result: ToolResult = match action_result {
        Ok(val) => serde_json::from_value(val).unwrap_or_else(|e| ToolResult::err(e.to_string())),
        Err(e) => ToolResult::err(e.to_string()),
    };

    let success = tool_result.success;

    let res_transformed = hook_manager
        .transform_validated(
            HOOK_TOOL_RESULT_PREPARE,
            Some(iteration),
            serde_json::to_value(&tool_result)?,
            |_| Ok(()),
        )
        .await?;

    let final_tool_res: ToolResult =
        serde_json::from_value(res_transformed.payload).unwrap_or(tool_result);

    let output_item = Item::FunctionCallOutput {
        id: None,
        call_id: call.call_id.clone(),
        output: final_tool_res.output.to_function_output(),
        status: None,
    };

    Ok((output_item, success))
}

fn infer_capabilities_for_tool_call(
    run_spec: &CoreRunSpec,
    call_req: &ToolCallRequest,
) -> Vec<(CapabilityRequest, String)> {
    let mut caps = Vec::new();
    let resolve_path = |raw: &str| {
        let p = Path::new(raw);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            run_spec.workspace_root.join(p)
        }
    };

    if let Some(path_str) = call_req.arguments.get("path").and_then(|v| v.as_str()) {
        let full_path = resolve_path(path_str);
        if call_req.name == "write_file" || call_req.name == "replace_in_file" {
            caps.push((
                CapabilityRequest::FileWrite { path: full_path },
                format!("Tool '{}' writing file '{}'", call_req.name, path_str),
            ));
        } else if call_req.name == "view_image" || call_req.name == "read_file" {
            caps.push((
                CapabilityRequest::FileRead { path: full_path },
                format!("Tool '{}' reading file '{}'", call_req.name, path_str),
            ));
        }
    }

    // Note: Standard shell command execution (`execute_command_with_timeout`) is now
    // mandatorily checked inside the WASM Host Import layer (`runtime.rs`).
    // Here we only pre-check if the tool call arguments explicitly request an alternate `work_dir`.
    if let Some(work_dir_str) = call_req.arguments.get("work_dir").and_then(|v| v.as_str()) {
        let work_dir = resolve_path(work_dir_str);
        caps.push((
            CapabilityRequest::CommandWorkDir { work_dir },
            format!(
                "Tool '{}' requesting custom work_dir '{}'",
                call_req.name, work_dir_str
            ),
        ));
    }

    caps
}

async fn execute_builtin_request_permission(
    run_spec: &CoreRunSpec,
    call: &AgentFunctionCall,
    host: &dyn CoreHost,
    sandbox_state: &SandboxState,
) -> Result<(Item, bool), CoreError> {
    let args: serde_json::Value = serde_json::from_str(&call.arguments)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
    let cap_kind = args
        .get("capability")
        .and_then(|v| v.as_str())
        .unwrap_or("file_write");
    let raw_path = match args.get("path").and_then(|v| v.as_str()) {
        Some(p) if !p.trim().is_empty() => p.trim(),
        _ => {
            let err_item = Item::FunctionCallOutput {
                id: None,
                call_id: call.call_id.clone(),
                output: ToolResult::err("Missing required string argument 'path'".to_string())
                    .output
                    .to_function_output(),
                status: None,
            };
            return Ok((err_item, false));
        }
    };
    let reason = args
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("Agent requested external path access")
        .to_string();

    let resolved_path = {
        let p = Path::new(raw_path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            run_spec.workspace_root.join(p)
        }
    };

    let cap = match cap_kind {
        "file_read" => CapabilityRequest::FileRead {
            path: resolved_path.clone(),
        },
        "command_work_dir" => CapabilityRequest::CommandWorkDir {
            work_dir: resolved_path.clone(),
        },
        _ => CapabilityRequest::FileWrite {
            path: resolved_path.clone(),
        },
    };

    match host
        .check_permission(
            Some("builtin:request_permission"),
            cap.clone(),
            Some(reason),
        )
        .await?
    {
        PermissionOutcome::Allowed {
            effective_capability,
            persist_in_activation,
        } => {
            let granted_cap = effective_capability.unwrap_or(cap);
            let final_path_str = granted_cap
                .path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| resolved_path.display().to_string());
            if persist_in_activation {
                match granted_cap {
                    CapabilityRequest::FileWrite { path } => {
                        sandbox_state.allow_writable_path(path)
                    }
                    CapabilityRequest::CommandWorkDir { work_dir } => {
                        sandbox_state.allow_writable_path(work_dir)
                    }
                    CapabilityRequest::FileRead { path } => sandbox_state.allow_readable_path(path),
                    CapabilityRequest::Http { .. } => {}
                }
            }
            let ok_res = ToolResult::ok(format!(
                "Permission GRANTED for '{}' on '{}'. The OS kernel sandbox has unlocked this path; you may now retry your command.",
                cap_kind,
                final_path_str
            ));
            Ok((
                Item::FunctionCallOutput {
                    id: None,
                    call_id: call.call_id.clone(),
                    output: ok_res.output.to_function_output(),
                    status: None,
                },
                true,
            ))
        }
        PermissionOutcome::Denied { reason } => {
            let err_res = ToolResult::err(format!("Permission DENIED: {reason}"));
            Ok((
                Item::FunctionCallOutput {
                    id: None,
                    call_id: call.call_id.clone(),
                    output: err_res.output.to_function_output(),
                    status: None,
                },
                false,
            ))
        }
    }
}

pub fn add_usage(a: &Usage, b: &Usage) -> Usage {
    Usage {
        input_tokens: a.input_tokens + b.input_tokens,
        output_tokens: a.output_tokens + b.output_tokens,
        total_tokens: a.total_tokens + b.total_tokens,
        input_tokens_details: InputTokensDetails {
            cached_tokens: a.input_tokens_details.cached_tokens
                + b.input_tokens_details.cached_tokens,
        },
        output_tokens_details: OutputTokensDetails {
            reasoning_tokens: a.output_tokens_details.reasoning_tokens
                + b.output_tokens_details.reasoning_tokens,
        },
    }
}
