use crate::config::{DaemonConfig, ExecutorMode};
use crate::permission::{capability_covered_by, evaluate_capability_policy, PolicyEvalResult};
use async_trait::async_trait;
use chrono::Utc;
use openresponses_rust::{Item, Usage};
use ragents_client::{
    connect_daemon_tunnel, ClientError, DaemonPushEvent, DaemonTunnelHandle, RagentsClient,
};
use ragents_core::{
    CoreError, CoreExecutor, CoreHost, CoreRunSpec, InProcessExecutor, ModelBackend,
    OpenResponsesModelBackend, PermissionOutcome, SubprocessExecutor,
};
use ragents_protocol::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

struct ActiveActivationControl {
    cancel: CancellationToken,
    permission_waiters:
        Arc<Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionDecision>>>>,
}

#[derive(Clone)]
pub struct AgentDaemon {
    config: Arc<DaemonConfig>,
    executor: Arc<dyn CoreExecutor>,
    model_backend: Arc<dyn ModelBackend>,
    session_grants: Arc<Mutex<HashMap<SessionId, Vec<CapabilityRequest>>>>,
}

impl AgentDaemon {
    pub fn new(config: DaemonConfig) -> Self {
        let executor: Arc<dyn CoreExecutor> = match config.executor_mode {
            ExecutorMode::Subprocess => Arc::new(SubprocessExecutor::new()),
            ExecutorMode::InProcess => Arc::new(InProcessExecutor),
        };
        Self {
            config: Arc::new(config),
            executor,
            model_backend: Arc::new(OpenResponsesModelBackend),
            session_grants: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_executor(mut self, executor: Arc<dyn CoreExecutor>) -> Self {
        self.executor = executor;
        self
    }

    pub fn with_model_backend(mut self, model_backend: Arc<dyn ModelBackend>) -> Self {
        self.model_backend = model_backend;
        self
    }

    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }

    /// Connects to `api-server`, registers the node, starts heartbeat and reconcile loop in the background.
    pub async fn start(&self, shutdown: CancellationToken) -> Result<(), ClientError> {
        let client = RagentsClient::new(&self.config.server_url);
        let ws_url = client.ws_tunnel_url();
        let (tunnel, mut push_rx) = connect_daemon_tunnel(&ws_url).await?;

        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        let os = std::env::consts::OS.to_string();
        let arch = std::env::consts::ARCH.to_string();

        let (lease_ttl_ms, _rv) = tunnel
            .register_node(
                self.config.node_id.clone(),
                hostname,
                os,
                arch,
                self.config.labels.clone(),
                self.config.supported_models(),
                self.config.default_model.clone(),
                self.config.installed_extension_names(),
                self.config.max_concurrent_runners,
            )
            .await?;

        let active_runners = Arc::new(AtomicU32::new(0));
        let active_tasks: Arc<Mutex<HashMap<ActivationId, ActiveActivationControl>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Spawn periodic heartbeat task
        let hb_tunnel = tunnel.clone();
        let hb_node_id = self.config.node_id.clone();
        let hb_runners = active_runners.clone();
        let hb_shutdown = shutdown.clone();
        let hb_interval = Duration::from_millis((lease_ttl_ms / 3).max(500));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let count = hb_runners.load(Ordering::Relaxed);
                        let _ = hb_tunnel.heartbeat(hb_node_id.clone(), count).await;
                    }
                    _ = hb_shutdown.cancelled() => break,
                }
            }
        });

        // Spawn reconcile & dispatch loop
        let daemon = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_push = push_rx.recv() => {
                        let Some(push) = maybe_push else { break; };
                        match push {
                            DaemonPushEvent::DispatchActivation {
                                session,
                                activation,
                                lease_epoch,
                            } => {
                                daemon.handle_dispatch(
                                    *session,
                                    *activation,
                                    lease_epoch,
                                    tunnel.clone(),
                                    active_runners.clone(),
                                    active_tasks.clone(),
                                    shutdown.clone(),
                                );
                            }
                            DaemonPushEvent::ReconcileActivation {
                                activation_id,
                                desired_phase,
                                interaction_responses,
                                ..
                            } => {
                                let maybe_ctrl = active_tasks
                                    .lock()
                                    .expect("active_tasks mutex poisoned")
                                    .get(&activation_id)
                                    .map(|c| (c.cancel.clone(), c.permission_waiters.clone()));

                                if let Some((cancel, waiters)) = maybe_ctrl {
                                    if desired_phase == DesiredActivationPhase::Cancelled {
                                        cancel.cancel();
                                    }
                                    for (req_id, res_spec) in interaction_responses {
                                        if let Some(tx) = waiters
                                            .lock()
                                            .expect("waiters mutex poisoned")
                                            .remove(&req_id)
                                        {
                                            let _ = tx.send(res_spec.decision);
                                        }
                                    }
                                }
                            }
                            DaemonPushEvent::Disconnected => break,
                        }
                    }
                    _ = shutdown.cancelled() => break,
                }
            }
        });

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_dispatch(
        &self,
        session: Session,
        activation: Activation,
        lease_epoch: LeaseEpoch,
        tunnel: DaemonTunnelHandle,
        active_runners: Arc<AtomicU32>,
        active_tasks: Arc<Mutex<HashMap<ActivationId, ActiveActivationControl>>>,
        daemon_shutdown: CancellationToken,
    ) {
        let daemon = self.clone();
        tokio::spawn(async move {
            let sess_id = SessionId::new(session.metadata.id.clone());
            let act_id = ActivationId::new(activation.metadata.id.clone());

            let (effective_config, effective_permission, model_endpoint) =
                match daemon.config.merge_for_activation(&session, &activation) {
                    Ok(merged) => merged,
                    Err(err) => {
                        let _ = tunnel
                            .complete_activation(
                                sess_id,
                                act_id,
                                lease_epoch,
                                None,
                                ActivationTerminalOutcome::Failed {
                                    error: err.to_string(),
                                },
                            )
                            .await;
                        return;
                    }
                };

            // Determine workspace directory based on Pinned vs Floating mode
            let workspace_root = match &session.spec.scheduling {
                SchedulingMode::Pinned { workspace_path, .. } => workspace_path.clone(),
                SchedulingMode::Floating { .. } => daemon
                    .config
                    .temp_root_dir
                    .join("floating")
                    .join(sess_id.as_str())
                    .join(act_id.as_str()),
            };
            let session_tmp_dir = daemon
                .config
                .temp_root_dir
                .join("sessions")
                .join(sess_id.as_str());

            let task_cancel = daemon_shutdown.child_token();
            if activation.spec.desired_phase == DesiredActivationPhase::Cancelled {
                task_cancel.cancel();
            }

            let permission_waiters: Arc<
                Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionDecision>>>,
            > = Arc::new(Mutex::new(HashMap::new()));

            active_tasks
                .lock()
                .expect("active_tasks mutex poisoned")
                .insert(
                    act_id.clone(),
                    ActiveActivationControl {
                        cancel: task_cancel.clone(),
                        permission_waiters: permission_waiters.clone(),
                    },
                );
            active_runners.fetch_add(1, Ordering::Relaxed);

            if tunnel
                .accept_activation(
                    sess_id.clone(),
                    act_id.clone(),
                    lease_epoch,
                    effective_config.clone(),
                    effective_permission.clone(),
                )
                .await
                .is_err()
            {
                active_runners.fetch_sub(1, Ordering::Relaxed);
                active_tasks
                    .lock()
                    .expect("active_tasks mutex poisoned")
                    .remove(&act_id);
                return;
            }

            let prior_grants = daemon
                .session_grants
                .lock()
                .expect("session_grants mutex poisoned")
                .get(&sess_id)
                .cloned()
                .unwrap_or_default();

            let mut runtime_permissions = effective_permission.effective_policy.clone();
            for g in &prior_grants {
                match g {
                    CapabilityRequest::FileWrite { path } => {
                        let rule_set = runtime_permissions
                            .file_write
                            .get_or_insert_with(Default::default);
                        rule_set
                            .allow
                            .get_or_insert_with(Vec::new)
                            .push(path.to_string_lossy().to_string());
                    }
                    CapabilityRequest::FileRead { path } => {
                        let rule_set = runtime_permissions
                            .file_read
                            .get_or_insert_with(Default::default);
                        rule_set
                            .allow
                            .get_or_insert_with(Vec::new)
                            .push(path.to_string_lossy().to_string());
                    }
                    CapabilityRequest::CommandWorkDir { work_dir } => {
                        let rule_set = runtime_permissions
                            .command_work_dir
                            .get_or_insert_with(Default::default);
                        rule_set
                            .allow
                            .get_or_insert_with(Vec::new)
                            .push(work_dir.to_string_lossy().to_string());
                    }
                    CapabilityRequest::Http { .. } => {}
                }
            }

            let host = Arc::new(DaemonCoreHost {
                session_id: sess_id.clone(),
                activation_id: act_id.clone(),
                lease_epoch,
                workspace_root: workspace_root.clone(),
                session_tmp_dir: session_tmp_dir.clone(),
                effective_policy: runtime_permissions.clone(),
                session_grants: daemon.session_grants.clone(),
                tunnel: tunnel.clone(),
                permission_waiters,
                cancellation: task_cancel.clone(),
            });

            let run_spec = CoreRunSpec {
                session_id: sess_id.clone(),
                activation_id: act_id.clone(),
                workspace_root,
                session_tmp_dir,
                effective_config,
                effective_permissions: runtime_permissions,
                model_endpoint,
                max_turns: daemon.config.max_turns,
            };

            let exec_res = daemon
                .executor
                .execute(run_spec, host, daemon.model_backend.clone(), task_cancel)
                .await;

            active_runners.fetch_sub(1, Ordering::Relaxed);
            active_tasks
                .lock()
                .expect("active_tasks mutex poisoned")
                .remove(&act_id);

            match exec_res {
                Ok(summary) => {
                    let _ = tunnel
                        .complete_activation(
                            sess_id,
                            act_id,
                            lease_epoch,
                            summary.last_turn_id,
                            ActivationTerminalOutcome::Succeeded {
                                usage: summary.total_usage,
                            },
                        )
                        .await;
                }
                Err(CoreError::Cancelled) => {
                    let _ = tunnel
                        .complete_activation(
                            sess_id,
                            act_id,
                            lease_epoch,
                            None,
                            ActivationTerminalOutcome::Cancelled,
                        )
                        .await;
                }
                Err(err) => {
                    let _ = tunnel
                        .complete_activation(
                            sess_id,
                            act_id,
                            lease_epoch,
                            None,
                            ActivationTerminalOutcome::Failed {
                                error: err.to_string(),
                            },
                        )
                        .await;
                }
            }
        });
    }
}

struct DaemonCoreHost {
    session_id: SessionId,
    activation_id: ActivationId,
    lease_epoch: LeaseEpoch,
    workspace_root: PathBuf,
    session_tmp_dir: PathBuf,
    effective_policy: PermissionPolicyLayer,
    session_grants: Arc<Mutex<HashMap<SessionId, Vec<CapabilityRequest>>>>,
    tunnel: DaemonTunnelHandle,
    permission_waiters:
        Arc<Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionDecision>>>>,
    cancellation: CancellationToken,
}

#[async_trait]
impl CoreHost for DaemonCoreHost {
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
        if self
            .session_grants
            .lock()
            .expect("session_grants mutex poisoned")
            .get(&self.session_id)
            .map(|grants| {
                grants
                    .iter()
                    .any(|g| capability_covered_by(g, &capability, &self.workspace_root))
            })
            .unwrap_or(false)
        {
            return Ok(PermissionOutcome::Allowed);
        }

        let eval = evaluate_capability_policy(
            &self.effective_policy,
            &self.workspace_root,
            &self.session_tmp_dir,
            &capability,
        );

        match eval {
            PolicyEvalResult::Allow => Ok(PermissionOutcome::Allowed),
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

                let decision = tokio::select! {
                    res = rx => res.map_err(|_| CoreError::Cancelled)?,
                    _ = self.cancellation.cancelled() => return Err(CoreError::Cancelled),
                };

                match decision {
                    PermissionDecision::Deny => Ok(PermissionOutcome::Denied {
                        reason: "Denied by user interaction".to_string(),
                    }),
                    PermissionDecision::AllowOnce => Ok(PermissionOutcome::Allowed),
                    PermissionDecision::AllowSession
                    | PermissionDecision::AllowProject
                    | PermissionDecision::AllowAlways => {
                        self.session_grants
                            .lock()
                            .expect("session_grants mutex poisoned")
                            .entry(self.session_id.clone())
                            .or_default()
                            .push(capability);
                        Ok(PermissionOutcome::Allowed)
                    }
                }
            }
        }
    }

    async fn spawn_child_session(
        &self,
        request: CreateSessionRequest,
        turn_id: Option<TurnId>,
        call_id: Option<String>,
        command_id: CommandId,
    ) -> Result<SessionId, CoreError> {
        self.tunnel
            .spawn_child_session(
                self.session_id.clone(),
                self.activation_id.clone(),
                turn_id,
                call_id,
                command_id,
                request,
            )
            .await
            .map_err(|e| CoreError::HostError(e.to_string()))
    }
}
