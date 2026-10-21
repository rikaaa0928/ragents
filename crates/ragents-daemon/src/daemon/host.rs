use crate::interceptors::DaemonInterceptorChain;
use crate::permission::{capability_covered_by, evaluate_capability_policy, PolicyEvalResult};
use async_trait::async_trait;
use chrono::Utc;
use openresponses_rust::{Item, MessageContent, Usage};
use ragents_client::DaemonTunnelHandle;
use ragents_core::{
    CoreError, CoreHost, PermissionOutcome, ProcessManager, ProcessNotificationSink, SandboxState,
};
use ragents_protocol::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(crate) struct DaemonProcessSink {
    pub(crate) tunnel: DaemonTunnelHandle,
    pub(crate) lease_epochs: Arc<Mutex<HashMap<ActivationId, LeaseEpoch>>>,
}

#[async_trait]
impl ProcessNotificationSink for DaemonProcessSink {
    async fn on_process_started(&self, process: ManagedProcessInfo) {
        let maybe_epoch = self
            .lease_epochs
            .lock()
            .expect("lease_epochs poisoned")
            .get(&process.activation_id)
            .copied();
        if let Some(epoch) = maybe_epoch {
            let _ = self
                .tunnel
                .emit_event(
                    process.session_id.clone(),
                    process.activation_id.clone(),
                    epoch,
                    None,
                    SessionEvent::BackgroundProcessStarted { process },
                )
                .await;
        }
    }

    async fn on_process_exited_naturally(
        &self,
        process: ManagedProcessInfo,
        realtime_notification: MessageContent,
    ) {
        let _ = self
            .tunnel
            .push_realtime_message(
                process.session_id.clone(),
                Some(process.activation_id.clone()),
                vec![realtime_notification],
            )
            .await;

        let maybe_epoch = self
            .lease_epochs
            .lock()
            .expect("lease_epochs poisoned")
            .get(&process.activation_id)
            .copied();
        if let Some(epoch) = maybe_epoch {
            let _ = self
                .tunnel
                .emit_event(
                    process.session_id.clone(),
                    process.activation_id.clone(),
                    epoch,
                    None,
                    SessionEvent::BackgroundProcessExited { process },
                )
                .await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct DaemonCoreHost {
    pub(crate) node_id: NodeId,
    pub(crate) session_id: SessionId,
    pub(crate) activation_id: ActivationId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) workspace_root: PathBuf,
    pub(crate) norm_workspace_key: PathBuf,
    pub(crate) session_tmp_dir: PathBuf,
    pub(crate) effective_config: ConfigRevision,
    pub(crate) effective_permission: PermissionSnapshot,
    pub(crate) effective_policy: PermissionPolicyLayer,
    pub(crate) interceptor_chain: DaemonInterceptorChain,
    pub(crate) activation_grants: Arc<Mutex<Vec<CapabilityRequest>>>,
    pub(crate) session_grants: Arc<Mutex<HashMap<SessionId, Vec<CapabilityRequest>>>>,
    pub(crate) workspace_grants: Arc<Mutex<HashMap<PathBuf, Vec<CapabilityRequest>>>>,
    pub(crate) node_grants: Arc<Mutex<Vec<CapabilityRequest>>>,
    pub(crate) sandbox_state: Arc<SandboxState>,
    pub(crate) process_manager: Arc<ProcessManager>,
    pub(crate) tunnel: DaemonTunnelHandle,
    pub(crate) permission_waiters:
        Arc<Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionResolutionSpec>>>>,
    pub(crate) cancellation: CancellationToken,
}

impl DaemonCoreHost {
    fn is_covered_by_grants(&self, capability: &CapabilityRequest) -> bool {
        if self
            .activation_grants
            .lock()
            .expect("activation_grants mutex poisoned")
            .iter()
            .any(|g| capability_covered_by(g, capability, &self.workspace_root))
        {
            return true;
        }
        if self
            .session_grants
            .lock()
            .expect("session_grants mutex poisoned")
            .get(&self.session_id)
            .map(|grants| {
                grants
                    .iter()
                    .any(|g| capability_covered_by(g, capability, &self.workspace_root))
            })
            .unwrap_or(false)
        {
            return true;
        }
        if self
            .workspace_grants
            .lock()
            .expect("workspace_grants mutex poisoned")
            .get(&self.norm_workspace_key)
            .map(|grants| {
                grants
                    .iter()
                    .any(|g| capability_covered_by(g, capability, &self.workspace_root))
            })
            .unwrap_or(false)
        {
            return true;
        }
        if self
            .node_grants
            .lock()
            .expect("node_grants mutex poisoned")
            .iter()
            .any(|g| capability_covered_by(g, capability, &self.workspace_root))
        {
            return true;
        }
        false
    }

    fn sync_sandbox_with_grants(&self) {
        for g in self
            .activation_grants
            .lock()
            .expect("activation_grants mutex poisoned")
            .iter()
        {
            self.sandbox_state.apply_capability_grant(g);
        }
        if let Some(s_grants) = self
            .session_grants
            .lock()
            .expect("session_grants mutex poisoned")
            .get(&self.session_id)
        {
            for g in s_grants {
                self.sandbox_state.apply_capability_grant(g);
            }
        }
        if let Some(w_grants) = self
            .workspace_grants
            .lock()
            .expect("workspace_grants mutex poisoned")
            .get(&self.norm_workspace_key)
        {
            for g in w_grants {
                self.sandbox_state.apply_capability_grant(g);
            }
        }
        for g in self
            .node_grants
            .lock()
            .expect("node_grants mutex poisoned")
            .iter()
        {
            self.sandbox_state.apply_capability_grant(g);
        }
    }
}

#[async_trait]
impl CoreHost for DaemonCoreHost {
    async fn report_prepared_config(
        &self,
        extension_prompts: std::collections::BTreeMap<String, String>,
        effective_tools: Vec<String>,
    ) -> Result<(), CoreError> {
        let mut prompt_parts = self.effective_config.prompt_parts.clone();
        prompt_parts.extension_prompts = extension_prompts;
        let system_prompt = prompt_parts.assemble();
        let updated_cfg = ConfigRevision::new_merged_with_parts(
            prompt_parts,
            system_prompt,
            self.effective_config.model.clone(),
            self.effective_config.extensions.clone(),
            effective_tools,
        );
        self.tunnel
            .accept_activation(
                self.session_id.clone(),
                self.activation_id.clone(),
                self.lease_epoch,
                updated_cfg,
                self.effective_permission.clone(),
            )
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))?;
        Ok(())
    }

    async fn load_effective_context(&self) -> Result<Vec<Item>, CoreError> {
        self.tunnel
            .fetch_effective_context(self.session_id.clone())
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))
    }

    async fn commit_model_output(
        &self,
        turn_id: &TurnId,
        response_id: Option<String>,
        items: Vec<Item>,
        usage: Option<Usage>,
    ) -> Result<BatchSeq, CoreError> {
        self.tunnel
            .commit_batch(
                self.session_id.clone(),
                self.activation_id.clone(),
                self.lease_epoch,
                ItemBatchKind::ModelOutput,
                Some(turn_id.clone()),
                response_id.clone(),
                items,
                BatchCompanionEvent::TurnCompleted { response_id, usage },
            )
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))
    }

    async fn commit_tool_output(
        &self,
        turn_id: &TurnId,
        call_id: &str,
        tool_name: &str,
        success: bool,
        duration_ms: u64,
        items: Vec<Item>,
    ) -> Result<BatchSeq, CoreError> {
        self.tunnel
            .commit_batch(
                self.session_id.clone(),
                self.activation_id.clone(),
                self.lease_epoch,
                ItemBatchKind::ToolOutput,
                Some(turn_id.clone()),
                None,
                items,
                BatchCompanionEvent::ToolCallFinished {
                    call_id: call_id.to_string(),
                    name: tool_name.to_string(),
                    success,
                    duration_ms,
                },
            )
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))
    }

    async fn emit_event(
        &self,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<(), CoreError> {
        self.tunnel
            .emit_event(
                self.session_id.clone(),
                self.activation_id.clone(),
                self.lease_epoch,
                turn_id,
                event,
            )
            .await
            .map(|_| ())
            .map_err(|e| CoreError::HostError(e.to_string()))
    }

    async fn check_permission(
        &self,
        extension_id: Option<&str>,
        capability: CapabilityRequest,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        self.check_permission_with_original_path(extension_id, capability, None, reason)
            .await
    }

    async fn check_permission_with_original_path(
        &self,
        extension_id: Option<&str>,
        mut capability: CapabilityRequest,
        original_path: Option<PathBuf>,
        reason: Option<String>,
    ) -> Result<PermissionOutcome, CoreError> {
        if self.is_covered_by_grants(&capability) {
            return Ok(PermissionOutcome::allowed());
        }
        if let Some(ref orig) = original_path {
            let exact_cap = capability.with_override_path(Some(orig.as_path()));
            if self.is_covered_by_grants(&exact_cap) {
                return Ok(PermissionOutcome::allowed_with(exact_cap, true));
            }
        }

        let mut forced_eval: Option<PolicyEvalResult> = None;
        if !self.interceptor_chain.is_empty() {
            let p_input = PermissionInterceptInput {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                activation_id: self.activation_id.clone(),
                extension_id: extension_id.map(String::from),
                capability: capability.clone(),
                original_path: original_path.clone(),
                reason: reason.clone(),
                workspace_root: self.workspace_root.clone(),
            };
            match self.interceptor_chain.evaluate_permission(&p_input).await? {
                PermissionInterceptOutcome::Abstain => {}
                PermissionInterceptOutcome::Allow { override_path } => {
                    let effective = capability.with_override_path(override_path.as_deref());
                    return Ok(PermissionOutcome::allowed_with(effective, true));
                }
                PermissionInterceptOutcome::Deny { reason } => {
                    return Ok(PermissionOutcome::Denied { reason });
                }
                PermissionInterceptOutcome::Ask { suggested_path } => {
                    capability = capability.with_override_path(suggested_path.as_deref());
                    forced_eval = Some(PolicyEvalResult::Ask);
                }
            }
        }

        let eval = forced_eval.unwrap_or_else(|| {
            let primary = evaluate_capability_policy(
                &self.effective_policy,
                &self.workspace_root,
                &self.session_tmp_dir,
                &capability,
            );
            if primary == PolicyEvalResult::Ask {
                if let Some(ref orig) = original_path {
                    let exact_cap = capability.with_override_path(Some(orig.as_path()));
                    let exact_eval = evaluate_capability_policy(
                        &self.effective_policy,
                        &self.workspace_root,
                        &self.session_tmp_dir,
                        &exact_cap,
                    );
                    if exact_eval != PolicyEvalResult::Ask {
                        return exact_eval;
                    }
                }
            }
            primary
        });

        match eval {
            PolicyEvalResult::Allow => {
                if let Some(ref orig) = original_path {
                    let exact_cap = capability.with_override_path(Some(orig.as_path()));
                    return Ok(PermissionOutcome::allowed_with(exact_cap, true));
                }
                Ok(PermissionOutcome::allowed())
            }
            PolicyEvalResult::Deny => Ok(PermissionOutcome::Denied {
                reason: format!("Denied by permission policy for {capability:?}"),
            }),
            PolicyEvalResult::Ask => {
                let req_id = PermissionRequestId::generate();
                let (tx, rx) = oneshot::channel();
                self.permission_waiters
                    .lock()
                    .expect("waiters mutex poisoned")
                    .insert(req_id.clone(), tx);

                let req = PermissionRequest {
                    id: req_id.clone(),
                    session_id: self.session_id.clone(),
                    activation_id: self.activation_id.clone(),
                    extension_id: extension_id.map(String::from),
                    capability: capability.clone(),
                    original_path,
                    reason,
                    created_at: Utc::now(),
                };

                self.tunnel
                    .emit_event(
                        self.session_id.clone(),
                        self.activation_id.clone(),
                        self.lease_epoch,
                        None,
                        SessionEvent::PermissionRequested { request: req },
                    )
                    .await
                    .map_err(|e| CoreError::HostError(e.to_string()))?;

                let res_spec = tokio::select! {
                    res = rx => res.map_err(|_| CoreError::Cancelled)?,
                    _ = self.cancellation.cancelled() => return Err(CoreError::Cancelled),
                };

                let effective_cap =
                    capability.with_override_path(res_spec.override_path.as_deref());

                match res_spec.decision {
                    PermissionDecision::Deny => Ok(PermissionOutcome::Denied {
                        reason: "Denied by user interaction".to_string(),
                    }),
                    PermissionDecision::AllowOnce => {
                        Ok(PermissionOutcome::allowed_with(effective_cap, false))
                    }
                    PermissionDecision::AllowActivation => {
                        self.sandbox_state.apply_capability_grant(&effective_cap);
                        self.activation_grants
                            .lock()
                            .expect("activation_grants mutex poisoned")
                            .push(effective_cap.clone());
                        Ok(PermissionOutcome::allowed_with(effective_cap, true))
                    }
                    PermissionDecision::AllowSession => {
                        self.sandbox_state.apply_capability_grant(&effective_cap);
                        self.session_grants
                            .lock()
                            .expect("session_grants mutex poisoned")
                            .entry(self.session_id.clone())
                            .or_default()
                            .push(effective_cap.clone());
                        Ok(PermissionOutcome::allowed_with(effective_cap, true))
                    }
                    PermissionDecision::AllowWorkspace => {
                        self.sandbox_state.apply_capability_grant(&effective_cap);
                        self.workspace_grants
                            .lock()
                            .expect("workspace_grants mutex poisoned")
                            .entry(self.norm_workspace_key.clone())
                            .or_default()
                            .push(effective_cap.clone());
                        Ok(PermissionOutcome::allowed_with(effective_cap, true))
                    }
                    PermissionDecision::AllowNode => {
                        self.sandbox_state.apply_capability_grant(&effective_cap);
                        self.node_grants
                            .lock()
                            .expect("node_grants mutex poisoned")
                            .push(effective_cap.clone());
                        Ok(PermissionOutcome::allowed_with(effective_cap, true))
                    }
                }
            }
        }
    }

    async fn drain_realtime_messages(
        &self,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<Item>, CoreError> {
        self.tunnel
            .drain_realtime_messages(
                self.session_id.clone(),
                self.activation_id.clone(),
                self.lease_epoch,
                turn_id,
            )
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))
    }

    async fn manage_process(
        &self,
        extension_id: Option<&str>,
        request: ProcessActionRequest,
    ) -> Result<Option<ProcessActionResponse>, CoreError> {
        self.sync_sandbox_with_grants();
        let self_arc: Arc<dyn CoreHost> = Arc::new(self.clone());
        let resp = self
            .process_manager
            .handle_action(
                &self.session_id,
                &self.activation_id,
                extension_id.unwrap_or("shell"),
                &self.workspace_root,
                &self.sandbox_state,
                Some(&self_arc),
                request,
            )
            .await?;
        Ok(Some(resp))
    }

    async fn rollover_compacted_session(
        &self,
        turn_id: Option<TurnId>,
        plan: CompactionPlan,
        compaction_prompt: String,
        summary_text: String,
        compaction_usage: Option<Usage>,
    ) -> Result<SessionId, CoreError> {
        let resp = self
            .tunnel
            .rollover_compacted_session(RolloverCompactedSessionRequest {
                from_session_id: self.session_id.clone(),
                from_activation_id: self.activation_id.clone(),
                lease_epoch: self.lease_epoch,
                turn_id,
                summarized_through_pos: plan.summarized_through_pos,
                kept_from_pos: plan.kept_from_pos,
                kept_through_pos: plan.kept_through_pos,
                tokens_before: plan.tokens_before,
                compaction_prompt,
                summary_text,
                compaction_usage,
            })
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))?;

        let continued_session_id = SessionId::new(resp.continued_session.metadata.id);

        // Migrate session-scoped (and current activation-scoped) permission grants to `continued_session_id`
        {
            let act_grants = self
                .activation_grants
                .lock()
                .expect("activation_grants mutex poisoned")
                .clone();
            let mut sess_map = self
                .session_grants
                .lock()
                .expect("session_grants mutex poisoned");
            let mut migrated = sess_map.get(&self.session_id).cloned().unwrap_or_default();
            for g in act_grants {
                if !migrated.contains(&g) {
                    migrated.push(g);
                }
            }
            if !migrated.is_empty() {
                sess_map.insert(continued_session_id.clone(), migrated);
            }
        }

        // Migrate any running `lifecycle = "session"` background processes to `continued_session_id`
        self.process_manager
            .migrate_session_processes(&self.session_id, &continued_session_id);

        Ok(continued_session_id)
    }
}
