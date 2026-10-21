mod host;

use crate::config::{DaemonConfig, ExecutorMode, LocalModelConfig};
use crate::interceptors::{DaemonInterceptor, DaemonInterceptorChain, InterceptedModelBackend};
use host::{DaemonCoreHost, DaemonProcessSink};
use ragents_client::{
    connect_daemon_tunnel_with_token, ClientError, DaemonPushEvent, DaemonTunnelHandle,
    RagentsClient,
};
use ragents_core::{
    CoreError, CoreExecutor, CoreRunSpec, InProcessExecutor, ModelBackend,
    OpenResponsesModelBackend, ProcessManager, SandboxMode, SandboxState, SubprocessExecutor,
};
use ragents_protocol::*;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

struct ActiveActivationControl {
    session_id: SessionId,
    cancel: CancellationToken,
    permission_waiters:
        Arc<Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionResolutionSpec>>>>,
}

#[derive(Clone)]
pub struct AgentDaemon {
    config: Arc<DaemonConfig>,
    discovered_models: Arc<Mutex<BTreeMap<String, LocalModelConfig>>>,
    executor: Arc<dyn CoreExecutor>,
    model_backend: Arc<dyn ModelBackend>,
    interceptor_chain: DaemonInterceptorChain,
    /// Session-scoped permission grants (`AllowSession`)
    session_grants: Arc<Mutex<HashMap<SessionId, Vec<CapabilityRequest>>>>,
    /// Workspace-scoped permission grants (`AllowWorkspace`, keyed by canonicalized `workspace_root`)
    workspace_grants: Arc<Mutex<HashMap<PathBuf, Vec<CapabilityRequest>>>>,
    /// Node/Daemon-scoped permission grants (`AllowNode`)
    node_grants: Arc<Mutex<Vec<CapabilityRequest>>>,
}

impl AgentDaemon {
    pub fn new(config: DaemonConfig) -> Self {
        let executor: Arc<dyn CoreExecutor> = match config.executor_mode {
            ExecutorMode::Subprocess => Arc::new(SubprocessExecutor::new()),
            ExecutorMode::InProcess => Arc::new(InProcessExecutor),
        };
        let interceptor_chain = DaemonInterceptorChain::from_specs(&config.interceptors);
        Self {
            config: Arc::new(config),
            discovered_models: Arc::new(Mutex::new(BTreeMap::new())),
            executor,
            model_backend: Arc::new(OpenResponsesModelBackend),
            interceptor_chain,
            session_grants: Arc::new(Mutex::new(HashMap::new())),
            workspace_grants: Arc::new(Mutex::new(HashMap::new())),
            node_grants: Arc::new(Mutex::new(Vec::new())),
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

    pub fn with_interceptor(mut self, interceptor: Arc<dyn DaemonInterceptor>) -> Self {
        self.interceptor_chain.push(interceptor);
        self
    }

    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }

    pub fn effective_supported_models(&self) -> Vec<String> {
        let disc = self
            .discovered_models
            .lock()
            .expect("discovered_models mutex poisoned");
        self.config.supported_models_with_discovered(&disc)
    }

    pub fn effective_default_model(&self) -> Option<String> {
        self.config
            .default_model
            .clone()
            .or_else(|| self.effective_supported_models().into_iter().next())
    }

    pub fn sync_grants_from_node_spec(&self, node_spec: &NodeSpec) {
        {
            let mut n_guard = self.node_grants.lock().expect("node_grants mutex poisoned");
            for cap in policy_layer_to_capability_grants(&node_spec.permissions) {
                if !n_guard.contains(&cap) {
                    n_guard.push(cap);
                }
            }
        }
        {
            let mut w_guard = self
                .workspace_grants
                .lock()
                .expect("workspace_grants mutex poisoned");
            for (ws_str, pol) in &node_spec.workspace_permissions {
                let raw_path = PathBuf::from(ws_str);
                let canon_path = raw_path
                    .canonicalize()
                    .unwrap_or_else(|_| raw_path.clone());
                let grants = policy_layer_to_capability_grants(pol);
                for key in [raw_path, canon_path] {
                    let entry = w_guard.entry(key).or_default();
                    for cap in &grants {
                        if !entry.contains(cap) {
                            entry.push(cap.clone());
                        }
                    }
                }
            }
        }
    }

    pub fn sync_session_grants(&self, session_id: &SessionId, policy: &PermissionPolicyLayer) {
        let grants = policy_layer_to_capability_grants(policy);
        if grants.is_empty() {
            return;
        }
        let mut s_guard = self
            .session_grants
            .lock()
            .expect("session_grants mutex poisoned");
        let entry = s_guard.entry(session_id.clone()).or_default();
        for cap in grants {
            if !entry.contains(&cap) {
                entry.push(cap);
            }
        }
    }

    /// Connects to `api-server`, registers the node, starts heartbeat and reconcile loop in the background.
    pub async fn start(&self, shutdown: CancellationToken) -> Result<(), ClientError> {
        if self.config.auto_discover_models {
            let discovered = self.config.discover_provider_models().await;
            if !discovered.is_empty() {
                *self
                    .discovered_models
                    .lock()
                    .expect("discovered_models mutex poisoned") = discovered;
            }
        }

        let client = RagentsClient::new(&self.config.server_url)
            .with_auth_token(self.config.auth_token.clone());
        let ws_url = client.ws_tunnel_url();
        let (tunnel, mut push_rx) =
            connect_daemon_tunnel_with_token(&ws_url, self.config.auth_token.as_deref()).await?;

        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        let os = std::env::consts::OS.to_string();
        let arch = std::env::consts::ARCH.to_string();

        let (lease_ttl_ms, _rv, initial_node_spec) = tunnel
            .register_node_with_seed(
                self.config.node_id.clone(),
                hostname,
                os,
                arch,
                self.config.labels.clone(),
                self.effective_supported_models(),
                self.effective_default_model(),
                Some(self.config.default_system_prompt.clone()),
                self.config.default_permissions.clone(),
                self.config.installed_extension_names(),
                self.config.effective_allowed_workspace_dir_strings(),
                self.config.max_concurrent_runners,
            )
            .await?;
        self.sync_grants_from_node_spec(&initial_node_spec);

        let active_runners = Arc::new(AtomicU32::new(0));
        let active_tasks: Arc<Mutex<HashMap<ActivationId, ActiveActivationControl>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let lease_epochs: Arc<Mutex<HashMap<ActivationId, LeaseEpoch>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let process_manager = Arc::new(ProcessManager::new(Some(Arc::new(DaemonProcessSink {
            tunnel: tunnel.clone(),
            lease_epochs: lease_epochs.clone(),
        }))));

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

        let daemon = self.clone();
        let ws_url_for_loop = ws_url.clone();
        tokio::spawn(async move {
            loop {
                let disconnected = tokio::select! {
                    maybe_push = push_rx.recv() => {
                        match maybe_push {
                            Some(DaemonPushEvent::DispatchActivation {
                                session,
                                activation,
                                lease_epoch,
                                cluster_system_prompt,
                                node_spec,
                            }) => {
                                daemon.handle_dispatch(
                                    *session,
                                    *activation,
                                    lease_epoch,
                                    cluster_system_prompt,
                                    *node_spec,
                                    tunnel.clone(),
                                    active_runners.clone(),
                                    active_tasks.clone(),
                                    lease_epochs.clone(),
                                    process_manager.clone(),
                                    shutdown.clone(),
                                );
                                false
                            }
                            Some(DaemonPushEvent::ReconcileActivation {
                                activation_id,
                                desired_phase,
                                interaction_responses,
                                ..
                            }) => {
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
                                            let _ = tx.send(res_spec);
                                        }
                                    }
                                }
                                false
                            }
                            Some(DaemonPushEvent::CleanupSession { session_id }) => {
                                let cancels: Vec<CancellationToken> = active_tasks
                                    .lock()
                                    .expect("active_tasks mutex poisoned")
                                    .values()
                                    .filter(|c| c.session_id == session_id)
                                    .map(|c| c.cancel.clone())
                                    .collect();
                                for c in cancels {
                                    c.cancel();
                                }
                                process_manager.cleanup_session_processes(&session_id).await;
                                daemon
                                    .session_grants
                                    .lock()
                                    .expect("session_grants mutex poisoned")
                                    .remove(&session_id);
                                let _ = std::fs::remove_dir_all(
                                    daemon
                                        .config
                                        .temp_root_dir
                                        .join("sessions")
                                        .join(session_id.as_str()),
                                );
                                let _ = std::fs::remove_dir_all(
                                    daemon
                                        .config
                                        .temp_root_dir
                                        .join("floating")
                                        .join(session_id.as_str()),
                                );
                                false
                            }
                            Some(DaemonPushEvent::Disconnected) | None => true,
                        }
                    }
                    _ = shutdown.cancelled() => break,
                };

                if disconnected {
                    let mut backoff_ms = 100u64;
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                            _ = shutdown.cancelled() => return,
                        }
                        if let Ok((new_tunnel, new_rx)) = connect_daemon_tunnel_with_token(
                            &ws_url_for_loop,
                            daemon.config.auth_token.as_deref(),
                        )
                        .await
                        {
                            let hostname = std::env::var("HOSTNAME")
                                .unwrap_or_else(|_| "localhost".to_string());
                            let os = std::env::consts::OS.to_string();
                            let arch = std::env::consts::ARCH.to_string();
                            if let Ok((_, _, reconnected_node_spec)) = new_tunnel
                                .register_node_with_seed(
                                    daemon.config.node_id.clone(),
                                    hostname,
                                    os,
                                    arch,
                                    daemon.config.labels.clone(),
                                    daemon.effective_supported_models(),
                                    daemon.effective_default_model(),
                                    Some(daemon.config.default_system_prompt.clone()),
                                    daemon.config.default_permissions.clone(),
                                    daemon.config.installed_extension_names(),
                                    daemon.config.effective_allowed_workspace_dir_strings(),
                                    daemon.config.max_concurrent_runners,
                                )
                                .await
                            {
                                daemon.sync_grants_from_node_spec(&reconnected_node_spec);
                                let count = active_runners.load(Ordering::Relaxed);
                                let _ = new_tunnel
                                    .heartbeat(daemon.config.node_id.clone(), count)
                                    .await;
                                tunnel.replace_connection(&new_tunnel);
                                push_rx = new_rx;
                                break;
                            }
                        }
                        backoff_ms = (backoff_ms * 2).min(2000);
                    }
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
        cluster_system_prompt: String,
        node_spec: NodeSpec,
        tunnel: DaemonTunnelHandle,
        active_runners: Arc<AtomicU32>,
        active_tasks: Arc<Mutex<HashMap<ActivationId, ActiveActivationControl>>>,
        lease_epochs: Arc<Mutex<HashMap<ActivationId, LeaseEpoch>>>,
        process_manager: Arc<ProcessManager>,
        daemon_shutdown: CancellationToken,
    ) {
        let daemon = self.clone();
        tokio::spawn(async move {
            let sess_id = SessionId::new(session.metadata.id.clone());
            let act_id = ActivationId::new(activation.metadata.id.clone());
            daemon.sync_grants_from_node_spec(&node_spec);
            daemon.sync_session_grants(&sess_id, &session.spec.permissions);

            let (effective_config, effective_permission, model_endpoint) = {
                let disc = daemon
                    .discovered_models
                    .lock()
                    .expect("discovered_models mutex poisoned")
                    .clone();
                match daemon.config.merge_for_activation_with_context(
                    &disc,
                    &cluster_system_prompt,
                    &node_spec,
                    &session,
                    &activation,
                ) {
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
                }
            };

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
                Mutex<HashMap<PermissionRequestId, oneshot::Sender<PermissionResolutionSpec>>>,
            > = Arc::new(Mutex::new(HashMap::new()));

            active_tasks
                .lock()
                .expect("active_tasks mutex poisoned")
                .insert(
                    act_id.clone(),
                    ActiveActivationControl {
                        session_id: sess_id.clone(),
                        cancel: task_cancel.clone(),
                        permission_waiters: permission_waiters.clone(),
                    },
                );
            lease_epochs
                .lock()
                .expect("lease_epochs poisoned")
                .insert(act_id.clone(), lease_epoch);
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
                lease_epochs
                    .lock()
                    .expect("lease_epochs poisoned")
                    .remove(&act_id);
                return;
            }

            let norm_ws_key = workspace_root
                .canonicalize()
                .unwrap_or_else(|_| workspace_root.clone());

            let mut prior_grants = daemon
                .session_grants
                .lock()
                .expect("session_grants mutex poisoned")
                .get(&sess_id)
                .cloned()
                .unwrap_or_default();
            if let Some(ws_list) = daemon
                .workspace_grants
                .lock()
                .expect("workspace_grants mutex poisoned")
                .get(&norm_ws_key)
            {
                prior_grants.extend(ws_list.iter().cloned());
            }
            prior_grants.extend(
                daemon
                    .node_grants
                    .lock()
                    .expect("node_grants mutex poisoned")
                    .iter()
                    .cloned(),
            );

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

            let sandbox_state = Arc::new(SandboxState::from_policy(
                SandboxMode::Auto,
                &workspace_root,
                &session_tmp_dir,
                &runtime_permissions,
            ));

            let host = Arc::new(DaemonCoreHost {
                node_id: daemon.config.node_id.clone(),
                session_id: sess_id.clone(),
                activation_id: act_id.clone(),
                lease_epoch,
                workspace_root: workspace_root.clone(),
                norm_workspace_key: norm_ws_key,
                session_tmp_dir: session_tmp_dir.clone(),
                effective_config: effective_config.clone(),
                effective_permission: effective_permission.clone(),
                effective_policy: runtime_permissions.clone(),
                interceptor_chain: daemon.interceptor_chain.clone(),
                activation_grants: Arc::new(Mutex::new(Vec::new())),
                session_grants: daemon.session_grants.clone(),
                workspace_grants: daemon.workspace_grants.clone(),
                node_grants: daemon.node_grants.clone(),
                sandbox_state,
                process_manager: process_manager.clone(),
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
                disabled_tools: activation.spec.disabled_tools.clone(),
                model_endpoint,
                max_turns: daemon.config.max_turns,
            };

            let effective_model_backend: Arc<dyn ModelBackend> =
                if daemon.interceptor_chain.is_empty() {
                    daemon.model_backend.clone()
                } else {
                    Arc::new(InterceptedModelBackend {
                        node_id: daemon.config.node_id.clone(),
                        session_id: sess_id.clone(),
                        activation_id: act_id.clone(),
                        session_labels: session.metadata.labels.clone(),
                        chain: daemon.interceptor_chain.clone(),
                        inner: daemon.model_backend.clone(),
                    })
                };

            let exec_res = daemon
                .executor
                .execute(run_spec, host, effective_model_backend, task_cancel)
                .await;

            process_manager.cleanup_activation_processes(&act_id).await;

            active_runners.fetch_sub(1, Ordering::Relaxed);
            active_tasks
                .lock()
                .expect("active_tasks mutex poisoned")
                .remove(&act_id);
            lease_epochs
                .lock()
                .expect("lease_epochs poisoned")
                .remove(&act_id);

            match exec_res {
                Ok(summary) => {
                    if summary.compacted_into_session_id.is_none() {
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

fn policy_layer_to_capability_grants(policy: &PermissionPolicyLayer) -> Vec<CapabilityRequest> {
    let mut grants = Vec::new();
    if let Some(ref fr) = policy.file_read {
        if let Some(ref allows) = fr.allow {
            for pat in allows {
                if pat != "." && pat != "*" {
                    grants.push(CapabilityRequest::FileRead {
                        path: PathBuf::from(pat),
                    });
                }
            }
        }
    }
    if let Some(ref fw) = policy.file_write {
        if let Some(ref allows) = fw.allow {
            for pat in allows {
                if pat != "." && pat != "*" {
                    grants.push(CapabilityRequest::FileWrite {
                        path: PathBuf::from(pat),
                    });
                }
            }
        }
    }
    if let Some(ref cwd) = policy.command_work_dir {
        if let Some(ref allows) = cwd.allow {
            for pat in allows {
                if pat != "." && pat != "*" {
                    grants.push(CapabilityRequest::CommandWorkDir {
                        work_dir: PathBuf::from(pat),
                    });
                }
            }
        }
    }
    if let Some(ref hh) = policy.http_hosts {
        if let Some(ref allows) = hh.allow {
            for pat in allows {
                if pat != "*" {
                    grants.push(CapabilityRequest::Http {
                        scheme: "https".to_string(),
                        host: pat.clone(),
                        port: None,
                        method: "GET".to_string(),
                    });
                }
            }
        }
    }
    grants
}
