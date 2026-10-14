use crate::context::ContextProjection;
use crate::error::CoreError;
use crate::hooks::*;
use crate::host::{
    CoreExecutor, CoreHost, CoreRunSpec, CoreRunSummary, ModelEndpointConfig, PermissionOutcome,
};
use crate::model::ModelBackend;
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
                let plugin = WasmPlugin::load_from_file_with_workspace(
                    &ext.name,
                    &full_path,
                    &run_spec.workspace_root,
                    &preopen_dirs,
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

    let request_tools = active_tools
        .iter()
        .map(|t| {
            Tool::function(t.definition.name.clone())
                .with_description(t.definition.description.clone())
                .with_parameters(t.definition.parameters.clone())
        })
        .collect::<Vec<_>>();

    let max_turns = run_spec.max_turns.max(1);
    let mut iteration = 0usize;
    let mut final_text = String::new();
    let mut accumulated_usage: Option<Usage> = None;
    let mut last_turn_id: Option<TurnId> = None;

    loop {
        if cancellation.is_cancelled() {
            return Err(CoreError::Cancelled);
        }
        if iteration >= max_turns {
            break;
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
    iteration: usize,
) -> Result<(Item, bool), CoreError> {
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
            .check_permission(Some(owner), cap, Some(reason))
            .await?
        {
            PermissionOutcome::Allowed => {}
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

    if call_req.name == "run_command" || call_req.name == "shell" || call_req.name == "bash" {
        let work_dir = call_req
            .arguments
            .get("work_dir")
            .and_then(|v| v.as_str())
            .map(resolve_path)
            .unwrap_or_else(|| run_spec.workspace_root.clone());
        caps.push((
            CapabilityRequest::CommandWorkDir { work_dir },
            format!("Tool '{}' executing shell command", call_req.name),
        ));
    }

    caps
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
