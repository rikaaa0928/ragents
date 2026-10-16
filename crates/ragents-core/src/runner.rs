use crate::context::ContextProjection;
use crate::error::CoreError;
use crate::hooks::*;
use crate::host::{
    CoreExecutor, CoreHost, CoreRunSpec, CoreRunSummary, ModelEndpointConfig, PermissionOutcome,
};
use crate::model::{ModelBackend, ModelStreamCallback, ModelStreamDelta};
use crate::sandbox::SandboxState;
use async_trait::async_trait;
use openresponses_rust::{
    CreateResponseBody, InputTokensDetails, Item, MessageContent, OutputTokensDetails,
    ReasoningConfig, ResponseResource, ResponseStatus, Tool, ToolChoiceParam, Usage,
};
use ragents_protocol::{CapabilityRequest, CompactionPlan, SessionEvent, TurnId};
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
    pub fn build_request(
        instructions: &str,
        projection: &ContextProjection,
        model_name: &str,
        temperature: Option<f64>,
        max_output_tokens: Option<i32>,
        reasoning: Option<ReasoningConfig>,
        tools: Vec<Tool>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> CreateResponseBody {
        let input = projection.to_openresponses_input();
        let has_tools = !tools.is_empty();

        CreateResponseBody {
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
            metadata,
            stream: Some(true),
            ..Default::default()
        }
    }

    pub fn parse_output_items(
        output_items: Vec<Item>,
        response_id: Option<String>,
        usage: Option<Usage>,
    ) -> AgentCoreStepResult {
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

        AgentCoreStepResult {
            output_items,
            function_calls,
            response_id,
            usage,
            text,
            reasoning_text,
        }
    }

    pub async fn execute_request_streaming(
        request: CreateResponseBody,
        endpoint: &ModelEndpointConfig,
        model_backend: &dyn ModelBackend,
        host: &dyn CoreHost,
        turn_id: &TurnId,
    ) -> Result<AgentCoreStepResult, CoreError> {
        let (delta_tx, mut delta_rx) = tokio::sync::mpsc::unbounded_channel::<ModelStreamDelta>();
        let cb: ModelStreamCallback = Arc::new(move |delta| {
            let _ = delta_tx.send(delta);
        });

        let model_fut = model_backend.create_response_streaming(endpoint, request, Some(cb));
        tokio::pin!(model_fut);

        let response: ResponseResource = loop {
            tokio::select! {
                res = &mut model_fut => {
                    let r = res?;
                    while let Ok(delta) = delta_rx.try_recv() {
                        let ev = match delta {
                            ModelStreamDelta::TextDelta { delta } => SessionEvent::ModelTextDelta { delta },
                            ModelStreamDelta::ReasoningDelta { delta } => SessionEvent::ModelReasoningDelta { delta },
                        };
                        let _ = host.emit_event(Some(turn_id.clone()), ev).await;
                    }
                    break r;
                }
                Some(delta) = delta_rx.recv() => {
                    let ev = match delta {
                        ModelStreamDelta::TextDelta { delta } => SessionEvent::ModelTextDelta { delta },
                        ModelStreamDelta::ReasoningDelta { delta } => SessionEvent::ModelReasoningDelta { delta },
                    };
                    let _ = host.emit_event(Some(turn_id.clone()), ev).await;
                }
            }
        };

        if let Some(err) = response.error {
            return Err(CoreError::ResponseFailed(err.message));
        }
        if response.status == ResponseStatus::Failed {
            return Err(CoreError::ResponseFailed(
                "Response status is failed".into(),
            ));
        }

        Ok(Self::parse_output_items(
            response.output,
            Some(response.id),
            response.usage,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn step(
        instructions: &str,
        projection: &ContextProjection,
        model_name: &str,
        temperature: Option<f64>,
        max_output_tokens: Option<i32>,
        reasoning: Option<ReasoningConfig>,
        tools: Vec<Tool>,
        metadata: Option<std::collections::HashMap<String, String>>,
        endpoint: &ModelEndpointConfig,
        model_backend: &dyn ModelBackend,
    ) -> Result<AgentCoreStepResult, CoreError> {
        let request = Self::build_request(
            instructions,
            projection,
            model_name,
            temperature,
            max_output_tokens,
            reasoning,
            tools,
            metadata,
        );
        let response: ResponseResource = model_backend.create_response(endpoint, request).await?;
        if let Some(err) = response.error {
            return Err(CoreError::ResponseFailed(err.message));
        }
        if response.status == ResponseStatus::Failed {
            return Err(CoreError::ResponseFailed(
                "Response status is failed".into(),
            ));
        }
        Ok(Self::parse_output_items(
            response.output,
            Some(response.id),
            response.usage,
        ))
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
        let base_preopen_dirs = vec![
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
                let mut ext_preopen_dirs = base_preopen_dirs.clone();
                for dir_str in &ext.allowed_dirs {
                    let expanded = expand_tilde_path(dir_str, &run_spec.workspace_root);
                    if !ext_preopen_dirs.contains(&expanded) {
                        ext_preopen_dirs.push(expanded.clone());
                    }
                    sandbox_state.allow_readable_path(expanded);
                }
                let plugin = WasmPlugin::load_from_file_with_host(
                    &ext.name,
                    &full_path,
                    &run_spec.workspace_root,
                    &ext_preopen_dirs,
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
            &cancellation,
        )
        .await;

        if let Err(ref err) = result {
            let _ = hook_manager
                .observe(
                    HOOK_AGENT_ERROR,
                    None,
                    serde_json::json!({
                        "session_id": run_spec.session_id,
                        "activation_id": run_spec.activation_id,
                        "error": err.to_string(),
                    }),
                )
                .await;
        }

        let _ = hook_manager
            .observe(
                HOOK_AGENT_SHUTDOWN,
                None,
                serde_json::json!({
                    "session_id": run_spec.session_id,
                    "activation_id": run_spec.activation_id,
                    "ok": result.is_ok(),
                }),
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

    let _ = hook_manager
        .transform_validated(
            HOOK_INPUT_PREPARE,
            None,
            serde_json::json!({
                "session_id": run_spec.session_id,
                "activation_id": run_spec.activation_id,
            }),
            |_| Ok(()),
        )
        .await?;

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

        let turn_prep = hook_manager
            .transform_validated(
                HOOK_TURN_PREPARE,
                Some(iteration),
                serde_json::json!({
                    "session_id": run_spec.session_id,
                    "activation_id": run_spec.activation_id,
                    "turn_id": turn_id,
                    "iteration": iteration,
                }),
                |_| Ok(()),
            )
            .await?;
        match turn_prep.control {
            FlowControl::Continue => {}
            FlowControl::Skip => continue,
            FlowControl::Stop => break,
        }

        // Drain all pending real-time messages (background process exit notifications, steer messages)
        // in one atomic batch before loading the effective context for this Turn.
        let _ = host.drain_realtime_messages(Some(turn_id.clone())).await?;

        let raw_items = host.load_effective_context().await?;
        let projected_items =
            match apply_context_projection_hook(hook_manager, iteration, raw_items).await? {
                ContextProjectionOutcome::Projected(items) => items,
                ContextProjectionOutcome::CompactionRequested(plan) => {
                    let (compaction_prompt, summary_text, compaction_usage) =
                        execute_compaction_plan(
                            &plan,
                            run_spec,
                            &draft,
                            model_backend,
                            &turn_id,
                            cancellation,
                        )
                        .await?;
                    if let Some(ref u) = compaction_usage {
                        accumulated_usage = Some(match accumulated_usage {
                            Some(ref acc) => add_usage(acc, u),
                            None => u.clone(),
                        });
                    }
                    let continued_session_id = host
                        .rollover_compacted_session(
                            Some(turn_id.clone()),
                            plan,
                            compaction_prompt,
                            summary_text,
                            accumulated_usage.clone(),
                        )
                        .await?;
                    return Ok(CoreRunSummary {
                        activation_id: run_spec.activation_id.clone(),
                        final_text,
                        total_usage: accumulated_usage,
                        turns_executed: iteration.saturating_sub(1),
                        last_turn_id,
                        compacted_into_session_id: Some(continued_session_id),
                    });
                }
            };

        host.emit_event(Some(turn_id.clone()), SessionEvent::TurnStarted)
            .await?;

        let projection = ContextProjection::new(projected_items);

        let mut turn_metadata = std::collections::HashMap::new();
        turn_metadata.insert(
            "ragents.session_id".to_string(),
            run_spec.session_id.to_string(),
        );
        turn_metadata.insert(
            "ragents.activation_id".to_string(),
            run_spec.activation_id.to_string(),
        );
        turn_metadata.insert("ragents.turn_id".to_string(), turn_id.to_string());

        let initial_request = AgentCore::build_request(
            &draft.system_prompt,
            &projection,
            &draft.model.name,
            draft.model.temperature,
            draft.model.max_output_tokens,
            draft.model.reasoning.clone(),
            request_tools.clone(),
            Some(turn_metadata),
        );
        let prepared_request =
            apply_model_request_hook(hook_manager, iteration, initial_request).await?;

        let raw_step = tokio::select! {
            res = AgentCore::execute_request_streaming(
                prepared_request,
                &run_spec.model_endpoint,
                model_backend,
                host,
                &turn_id,
            ) => res?,
            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
        };

        if let Some(ref usage) = raw_step.usage {
            accumulated_usage = Some(match accumulated_usage {
                Some(ref acc) => add_usage(acc, usage),
                None => usage.clone(),
            });
        }

        hook_manager
            .observe(
                HOOK_MODEL_RESPONSE_OBSERVE,
                Some(iteration),
                serde_json::json!({
                    "session_id": run_spec.session_id,
                    "activation_id": run_spec.activation_id,
                    "turn_id": turn_id,
                    "iteration": iteration,
                    "response_id": raw_step.response_id,
                    "usage": raw_step.usage,
                    "accumulated_usage": accumulated_usage,
                    "output_items": raw_step.output_items,
                }),
            )
            .await?;

        let prepared_output_items =
            apply_model_response_hook(hook_manager, iteration, raw_step.output_items).await?;
        let step = AgentCore::parse_output_items(
            prepared_output_items,
            raw_step.response_id,
            raw_step.usage,
        );

        if !step.text.is_empty() {
            final_text = step.text.clone();
        }

        let model_items_to_commit =
            apply_context_append_hook(hook_manager, iteration, step.output_items.clone()).await?;

        host.commit_model_output(
            &turn_id,
            step.response_id.clone(),
            model_items_to_commit,
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
                        call,
                        &active_tools,
                        hook_manager,
                        host,
                        iteration,
                    ) => res?,
                    _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
                };

                let duration_ms = started_at.elapsed().as_millis() as u64;
                let tool_items_to_commit =
                    apply_context_append_hook(hook_manager, iteration, vec![tool_output_item])
                        .await?;
                host.commit_tool_output(
                    &turn_id,
                    &call.call_id,
                    &call.name,
                    success,
                    duration_ms,
                    tool_items_to_commit,
                )
                .await?;
            }

            hook_manager
                .observe(
                    HOOK_TURN_COMPLETE,
                    Some(iteration),
                    serde_json::json!({
                        "session_id": run_spec.session_id,
                        "activation_id": run_spec.activation_id,
                        "turn_id": turn_id,
                        "iteration": iteration,
                        "response_id": step.response_id,
                        "usage": step.usage,
                        "accumulated_usage": accumulated_usage,
                        "function_calls_count": step.function_calls.len(),
                    }),
                )
                .await?;
        } else {
            hook_manager
                .observe(
                    HOOK_TURN_COMPLETE,
                    Some(iteration),
                    serde_json::json!({
                        "session_id": run_spec.session_id,
                        "activation_id": run_spec.activation_id,
                        "turn_id": turn_id,
                        "iteration": iteration,
                        "response_id": step.response_id,
                        "usage": step.usage,
                        "accumulated_usage": accumulated_usage,
                        "function_calls_count": 0,
                    }),
                )
                .await?;

            // Before finishing the activation, check if any real-time messages arrived during the final turn.
            // If so, continue into another Turn so the agent can process the notification.
            let late_realtime = host.drain_realtime_messages(None).await?;
            if late_realtime.is_empty() {
                break;
            }
        }
    }

    Ok(CoreRunSummary {
        activation_id: run_spec.activation_id.clone(),
        final_text,
        total_usage: accumulated_usage,
        turns_executed: iteration,
        last_turn_id,
        compacted_into_session_id: None,
    })
}

enum ContextProjectionOutcome {
    Projected(Vec<Item>),
    CompactionRequested(CompactionPlan),
}

async fn apply_context_projection_hook(
    hook_manager: &HookManager,
    iteration: usize,
    items: Vec<Item>,
) -> Result<ContextProjectionOutcome, CoreError> {
    let payload = serde_json::to_value(&items)?;
    let transformed = hook_manager
        .transform_validated(
            HOOK_CONTEXT_PROJECT_PREPARE,
            Some(iteration),
            payload,
            |_| Ok(()),
        )
        .await?;
    if transformed.control == FlowControl::Stop {
        if let Ok(plan) = serde_json::from_value::<CompactionPlan>(transformed.payload.clone()) {
            return Ok(ContextProjectionOutcome::CompactionRequested(plan));
        }
    }
    Ok(ContextProjectionOutcome::Projected(
        serde_json::from_value(transformed.payload).unwrap_or(items),
    ))
}

async fn execute_compaction_plan(
    plan: &CompactionPlan,
    run_spec: &CoreRunSpec,
    draft: &AgentDraft,
    model_backend: &dyn ModelBackend,
    turn_id: &TurnId,
    cancellation: &CancellationToken,
) -> Result<(String, String, Option<Usage>), CoreError> {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "ragents.session_id".to_string(),
        run_spec.session_id.to_string(),
    );
    metadata.insert(
        "ragents.activation_id".to_string(),
        run_spec.activation_id.to_string(),
    );
    metadata.insert("ragents.turn_id".to_string(), turn_id.to_string());
    metadata.insert("ragents.role".to_string(), "compaction_worker".to_string());

    let mut combined_prompt = String::new();
    let mut history_summary: Option<String> = None;
    let mut turn_prefix_summary: Option<String> = None;
    let mut total_usage: Option<Usage> = None;

    if let Some(ref hp) = plan.history_prompt {
        combined_prompt.push_str(hp);
        let proj = ContextProjection::new(vec![Item::user_message(hp.clone())]);
        let step = tokio::select! {
            res = AgentCore::step(
                &plan.system_prompt,
                &proj,
                &draft.model.name,
                Some(0.0),
                draft.model.max_output_tokens,
                None,
                vec![],
                Some(metadata.clone()),
                &run_spec.model_endpoint,
                model_backend,
            ) => res?,
            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
        };
        if let Some(ref u) = step.usage {
            total_usage = Some(match total_usage {
                Some(ref acc) => add_usage(acc, u),
                None => u.clone(),
            });
        }
        history_summary = Some(step.text.trim().to_string());
    }

    if let Some(ref tpp) = plan.turn_prefix_prompt {
        if !combined_prompt.is_empty() {
            combined_prompt.push_str("\n\n---\n\n");
        }
        combined_prompt.push_str(tpp);
        let proj = ContextProjection::new(vec![Item::user_message(tpp.clone())]);
        let step = tokio::select! {
            res = AgentCore::step(
                &plan.system_prompt,
                &proj,
                &draft.model.name,
                Some(0.0),
                draft.model.max_output_tokens,
                None,
                vec![],
                Some(metadata),
                &run_spec.model_endpoint,
                model_backend,
            ) => res?,
            _ = cancellation.cancelled() => return Err(CoreError::Cancelled),
        };
        if let Some(ref u) = step.usage {
            total_usage = Some(match total_usage {
                Some(ref acc) => add_usage(acc, u),
                None => u.clone(),
            });
        }
        turn_prefix_summary = Some(step.text.trim().to_string());
    }

    let body = match (history_summary, turn_prefix_summary) {
        (Some(h), Some(t)) => format!("{h}\n\n---\n\n**Turn Context (split turn):**\n\n{t}"),
        (Some(h), None) => h,
        (None, Some(t)) => {
            if let Some(ref prev) = plan.previous_summary {
                format!("{prev}\n\n---\n\n**Turn Context (split turn):**\n\n{t}")
            } else {
                format!("**Turn Context (split turn):**\n\n{t}")
            }
        }
        (None, None) => plan.previous_summary.clone().unwrap_or_default(),
    };

    let mut formatted = format!("[Compaction Summary]\n\n{}", body.trim());
    if !plan.read_files.is_empty() {
        formatted.push_str(&format!(
            "\n\n<read-files>\n{}\n</read-files>",
            plan.read_files.join("\n")
        ));
    }
    if !plan.modified_files.is_empty() {
        formatted.push_str(&format!(
            "\n\n<modified-files>\n{}\n</modified-files>",
            plan.modified_files.join("\n")
        ));
    }

    Ok((combined_prompt, formatted, total_usage))
}

async fn apply_model_request_hook(
    hook_manager: &HookManager,
    iteration: usize,
    request: CreateResponseBody,
) -> Result<CreateResponseBody, CoreError> {
    let payload = serde_json::to_value(&request)?;
    let transformed = hook_manager
        .transform_validated(HOOK_MODEL_REQUEST_PREPARE, Some(iteration), payload, |_| {
            Ok(())
        })
        .await?;
    Ok(serde_json::from_value(transformed.payload).unwrap_or(request))
}

async fn apply_model_response_hook(
    hook_manager: &HookManager,
    iteration: usize,
    items: Vec<Item>,
) -> Result<Vec<Item>, CoreError> {
    let payload = serde_json::to_value(&items)?;
    let transformed = hook_manager
        .transform_validated(
            HOOK_MODEL_RESPONSE_PREPARE,
            Some(iteration),
            payload,
            |_| Ok(()),
        )
        .await?;
    Ok(serde_json::from_value(transformed.payload).unwrap_or(items))
}

async fn apply_context_append_hook(
    hook_manager: &HookManager,
    iteration: usize,
    items: Vec<Item>,
) -> Result<Vec<Item>, CoreError> {
    let payload = serde_json::to_value(&items)?;
    let transformed = hook_manager
        .transform_validated(
            HOOK_CONTEXT_APPEND_PREPARE,
            Some(iteration),
            payload,
            |_| Ok(()),
        )
        .await?;
    Ok(serde_json::from_value(transformed.payload).unwrap_or(items))
}

async fn execute_tool_call(
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

    let perm_req_transformed = hook_manager
        .transform_validated(
            HOOK_PERMISSION_REQUIREMENTS,
            Some(iteration),
            serde_json::json!({
                "call_id": call_req.call_id,
                "tool_id": call_req.tool_id,
                "name": call_req.name,
                "arguments": call_req.arguments,
                "requirements": [],
            }),
            |_| Ok(()),
        )
        .await?;
    if let Some(reqs) = perm_req_transformed
        .payload
        .get("requirements")
        .and_then(|v| v.as_array())
    {
        for req_val in reqs {
            if let Ok(cap) = serde_json::from_value::<CapabilityRequest>(req_val.clone()) {
                if let PermissionOutcome::Denied { reason } =
                    host.check_permission(Some(owner), cap, None).await?
                {
                    let err_item = Item::FunctionCallOutput {
                        id: None,
                        call_id: call.call_id.clone(),
                        output: ToolResult::err(format!("Permission denied: {reason}"))
                            .output
                            .to_function_output(),
                        status: None,
                    };
                    return Ok((err_item, false));
                }
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

fn expand_tilde_path(raw: &str, workspace_root: &Path) -> std::path::PathBuf {
    let trimmed = raw.trim();
    if trimmed == "~" || trimmed.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
            let tail = trimmed.strip_prefix("~/").unwrap_or("");
            if tail.is_empty() {
                return std::path::PathBuf::from(home);
            }
            return Path::new(&home).join(tail);
        }
    }
    let p = Path::new(trimmed);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        workspace_root.join(p)
    }
}
