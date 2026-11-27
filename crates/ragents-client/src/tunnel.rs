use crate::ClientError;
use futures::{SinkExt, StreamExt};
use openresponses_rust::Item;
use ragents_protocol::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_tungstenite::{client_async_tls_with_config, tungstenite::Message as WsMessage};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum DaemonPushEvent {
    DispatchActivation {
        session: Box<Session>,
        activation: Box<Activation>,
        lease_epoch: LeaseEpoch,
        cluster_system_prompt: String,
        node_spec: Box<NodeSpec>,
        cluster_default_model: Option<String>,
        cluster_model_provider: Option<ResolvedModelProvider>,
    },
    ReconcileActivation {
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        desired_phase: DesiredActivationPhase,
        interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
    },
    CleanupSession {
        session_id: SessionId,
    },
    NodeExtensionCall {
        request_id: String,
        request: NodeExtensionCallRequest,
    },
    Disconnected,
}

struct ActiveConnection {
    epoch: u64,
    out_tx: mpsc::UnboundedSender<DaemonToServerMessage>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<ServerToDaemonMessage>>>>,
    push_tx: mpsc::UnboundedSender<DaemonPushEvent>,
    disconnected: Arc<AtomicBool>,
    cancel: CancellationToken,
}

#[derive(Clone)]
pub struct DaemonTunnelHandle {
    inner: Arc<Mutex<ActiveConnection>>,
    auto_reconnect: Arc<AtomicBool>,
    state_notify: Arc<Notify>,
}

type AckPayload = (
    Option<ResourceVersion>,
    Option<BatchSeq>,
    Option<SessionId>,
    DrainedRealtimeBatch,
);

type PendingReplyMap = Arc<Mutex<HashMap<String, oneshot::Sender<ServerToDaemonMessage>>>>;
type PreparedSingleCall = (
    String,
    PendingReplyMap,
    oneshot::Receiver<ServerToDaemonMessage>,
);

impl DaemonTunnelHandle {
    /// Enables or disables waiting for `replace_connection` when an in-flight RPC encounters
    /// a disconnect. Long-lived `AgentDaemon` loops enable this; one-shot clients leave it off.
    pub fn set_auto_reconnect(&self, enabled: bool) {
        self.auto_reconnect.store(enabled, Ordering::Release);
        self.state_notify.notify_waiters();
    }

    pub fn current_epoch(&self) -> u64 {
        self.inner.lock().expect("tunnel mutex poisoned").epoch
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner
            .lock()
            .expect("tunnel mutex poisoned")
            .disconnected
            .load(Ordering::Acquire)
    }

    /// Atomically swaps the underlying WebSocket connection and pending map to a newly reconnected tunnel
    /// so all existing `DaemonTunnelHandle` clones (heartbeat, ProcessManager, active hosts) seamlessly
    /// use the new connection.
    pub fn replace_connection(&self, other: &DaemonTunnelHandle) {
        let new_conn = {
            let guard = other.inner.lock().expect("other tunnel mutex poisoned");
            ActiveConnection {
                epoch: 0,
                out_tx: guard.out_tx.clone(),
                pending: guard.pending.clone(),
                push_tx: guard.push_tx.clone(),
                disconnected: guard.disconnected.clone(),
                cancel: guard.cancel.clone(),
            }
        };
        let (old_pending, old_cancel, old_disc) = {
            let mut guard = self.inner.lock().expect("self tunnel mutex poisoned");
            let next_epoch = guard.epoch.wrapping_add(1);
            let old = std::mem::replace(
                &mut *guard,
                ActiveConnection {
                    epoch: next_epoch,
                    ..new_conn
                },
            );
            (old.pending, old.cancel, old.disconnected)
        };
        old_disc.store(true, Ordering::Release);
        old_cancel.cancel();
        old_pending.lock().expect("old pending mutex poisoned").clear();
        self.state_notify.notify_waiters();
    }

    /// Proactively clears pending RPC waiters, closes the underlying socket tasks, and notifies
    /// the daemon reconnect loop that the current WebSocket connection is unhealthy.
    pub fn force_disconnect(&self) {
        self.force_disconnect_internal(None);
    }

    /// Same as `force_disconnect`, but only triggers if the handle is still on `expected_epoch`
    /// so a stale heartbeat timeout from a previous socket cannot tear down a freshly reconnected one.
    pub fn force_disconnect_if_epoch(&self, expected_epoch: u64) -> bool {
        self.force_disconnect_internal(Some(expected_epoch))
    }

    fn force_disconnect_internal(&self, expected_epoch: Option<u64>) -> bool {
        let (pending, push_tx, cancel, disc) = {
            let guard = self.inner.lock().expect("tunnel mutex poisoned");
            if let Some(exp) = expected_epoch {
                if guard.epoch != exp {
                    return false;
                }
            }
            (
                guard.pending.clone(),
                guard.push_tx.clone(),
                guard.cancel.clone(),
                guard.disconnected.clone(),
            )
        };
        disc.store(true, Ordering::Release);
        cancel.cancel();
        pending.lock().expect("pending mutex poisoned").clear();
        let _ = push_tx.send(DaemonPushEvent::Disconnected);
        self.state_notify.notify_waiters();
        true
    }

    pub async fn call(
        &self,
        msg: DaemonToServerMessage,
    ) -> Result<ServerToDaemonMessage, ClientError> {
        self.call_with_timeout(msg, std::time::Duration::from_secs(120), 5)
            .await
    }

    pub async fn call_with_timeout(
        &self,
        msg: DaemonToServerMessage,
        timeout: std::time::Duration,
        max_attempts: usize,
    ) -> Result<ServerToDaemonMessage, ClientError> {
        self.call_with_timeout_internal(msg, timeout, max_attempts, true)
            .await
    }

    async fn call_with_timeout_internal(
        &self,
        msg: DaemonToServerMessage,
        timeout: std::time::Duration,
        max_attempts: usize,
        allow_reconnect_wait: bool,
    ) -> Result<ServerToDaemonMessage, ClientError> {
        let req_id = Self::extract_request_id(&msg).to_string();
        let max_attempts = max_attempts.max(1);
        let overall_deadline = tokio::time::Instant::now() + timeout;

        for attempt in 1..=max_attempts {
            let (out_tx, pending, conn_disc) = loop {
                let (is_disc, out_tx, pending, conn_disc) = {
                    let guard = self.inner.lock().expect("tunnel mutex poisoned");
                    (
                        guard.disconnected.load(Ordering::Acquire),
                        guard.out_tx.clone(),
                        guard.pending.clone(),
                        guard.disconnected.clone(),
                    )
                };
                if !is_disc {
                    break (out_tx, pending, conn_disc);
                }
                if !allow_reconnect_wait || !self.auto_reconnect.load(Ordering::Acquire) {
                    return Err(ClientError::Tunnel("tunnel disconnected".into()));
                }
                let remaining =
                    overall_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(ClientError::Tunnel("tunnel call timed out".into()));
                }
                let notified = self.state_notify.notified();
                {
                    let guard = self.inner.lock().expect("tunnel mutex poisoned");
                    if !guard.disconnected.load(Ordering::Acquire)
                        || !self.auto_reconnect.load(Ordering::Acquire)
                    {
                        continue;
                    }
                }
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    return Err(ClientError::Tunnel("tunnel call timed out".into()));
                }
            };

            let (tx, rx) = oneshot::channel();
            pending
                .lock()
                .expect("pending mutex poisoned")
                .insert(req_id.clone(), tx);

            if conn_disc.load(Ordering::Acquire) || out_tx.send(msg.clone()).is_err() {
                pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&req_id);
                if attempt < max_attempts {
                    if !allow_reconnect_wait || !self.auto_reconnect.load(Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(300 * attempt as u64))
                            .await;
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    continue;
                }
                return Err(ClientError::Tunnel("tunnel write channel closed".into()));
            }

            let remaining =
                overall_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&req_id);
                return Err(ClientError::Tunnel("tunnel call timed out".into()));
            }

            match tokio::time::timeout(remaining, rx).await {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(_)) => {
                    if attempt < max_attempts {
                        if !allow_reconnect_wait || !self.auto_reconnect.load(Ordering::Acquire) {
                            if conn_disc.load(Ordering::Acquire) {
                                return Err(ClientError::Tunnel(
                                    "tunnel response channel dropped".into(),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                        } else {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        continue;
                    }
                    return Err(ClientError::Tunnel(
                        "tunnel response channel dropped".into(),
                    ));
                }
                Err(_) => {
                    pending
                        .lock()
                        .expect("pending mutex poisoned")
                        .remove(&req_id);
                    return Err(ClientError::Tunnel("tunnel call timed out".into()));
                }
            }
        }

        Err(ClientError::Tunnel("tunnel call failed".into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        supported_models: Vec<String>,
        default_model: Option<String>,
        installed_extensions: Vec<String>,
        max_concurrent_runners: u32,
    ) -> Result<(u64, ResourceVersion), ClientError> {
        let mut default_dirs = vec!["/tmp".to_string()];
        let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();
        if !default_dirs.contains(&sys_tmp) {
            default_dirs.push(sys_tmp);
        }
        self.register_node_with_workspace_dirs(
            node_id,
            hostname,
            os,
            arch,
            labels,
            supported_models,
            default_model,
            installed_extensions,
            default_dirs,
            max_concurrent_runners,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node_with_workspace_dirs(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        supported_models: Vec<String>,
        default_model: Option<String>,
        installed_extensions: Vec<String>,
        allowed_workspace_dirs: Vec<String>,
        max_concurrent_runners: u32,
    ) -> Result<(u64, ResourceVersion), ClientError> {
        self.register_node_with_seed(
            node_id,
            hostname,
            os,
            arch,
            labels,
            supported_models,
            default_model,
            None,
            PermissionPolicyLayer::default(),
            installed_extensions,
            allowed_workspace_dirs,
            max_concurrent_runners,
        )
        .await
        .map(|(ttl, rv, _)| (ttl, rv))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node_with_seed(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        supported_models: Vec<String>,
        default_model: Option<String>,
        default_system_prompt: Option<String>,
        default_permissions: PermissionPolicyLayer,
        installed_extensions: Vec<String>,
        allowed_workspace_dirs: Vec<String>,
        max_concurrent_runners: u32,
    ) -> Result<(u64, ResourceVersion, NodeSpec), ClientError> {
        self.register_node_with_running_activations(
            node_id,
            hostname,
            os,
            arch,
            labels,
            supported_models,
            default_model,
            default_system_prompt,
            default_permissions,
            installed_extensions,
            allowed_workspace_dirs,
            max_concurrent_runners,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node_with_running_activations(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        local_models: Vec<String>,
        default_model: Option<String>,
        default_system_prompt: Option<String>,
        default_permissions: PermissionPolicyLayer,
        installed_extensions: Vec<String>,
        allowed_workspace_dirs: Vec<String>,
        max_concurrent_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
    ) -> Result<(u64, ResourceVersion, NodeSpec), ClientError> {
        self.register_node_with_models_and_running_activations(
            node_id,
            hostname,
            os,
            arch,
            labels,
            local_models,
            true,
            default_model,
            default_system_prompt,
            default_permissions,
            installed_extensions,
            allowed_workspace_dirs,
            max_concurrent_runners,
            running_activation_ids,
        )
        .await
        .map(|(ttl, rv, spec, _)| (ttl, rv, spec))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node_with_models_and_running_activations(
        &self,
        node_id: NodeId,
        hostname: String,
        os: String,
        arch: String,
        labels: BTreeMap<String, String>,
        local_models: Vec<String>,
        use_cluster_models: bool,
        default_model: Option<String>,
        default_system_prompt: Option<String>,
        default_permissions: PermissionPolicyLayer,
        installed_extensions: Vec<String>,
        allowed_workspace_dirs: Vec<String>,
        max_concurrent_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
    ) -> Result<(u64, ResourceVersion, NodeSpec, Option<String>), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::RegisterNode {
                request_id,
                node_id,
                hostname,
                os,
                arch,
                labels,
                local_models,
                use_cluster_models,
                default_model,
                default_system_prompt,
                default_permissions,
                installed_extensions,
                allowed_workspace_dirs,
                max_concurrent_runners,
                running_activation_ids,
            })
            .await?;

        match resp {
            ServerToDaemonMessage::Registered {
                lease_ttl_ms,
                resource_version,
                node_spec,
                cluster_default_model,
                ..
            } => Ok((
                lease_ttl_ms,
                resource_version,
                node_spec,
                cluster_default_model,
            )),
            ServerToDaemonMessage::Ack {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "register node failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected register response: {other:?}"
            ))),
        }
    }

    pub async fn resolve_model_provider(
        &self,
        model: Option<String>,
    ) -> Result<ResolvedModelProvider, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::ResolveModelProvider { request_id, model })
            .await?;
        match resp {
            ServerToDaemonMessage::ModelProviderReply {
                ok: true,
                provider: Some(provider),
                ..
            } => Ok(provider),
            ServerToDaemonMessage::ModelProviderReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "resolve model provider failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected resolve model provider reply: {other:?}"
            ))),
        }
    }

    pub async fn heartbeat(
        &self,
        node_id: NodeId,
        active_runners: u32,
    ) -> Result<ResourceVersion, ClientError> {
        self.heartbeat_with_activations(node_id, active_runners, None)
            .await
    }

    pub async fn heartbeat_with_activations(
        &self,
        node_id: NodeId,
        active_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
    ) -> Result<ResourceVersion, ClientError> {
        self.heartbeat_with_activations_timeout(
            node_id,
            active_runners,
            running_activation_ids,
            std::time::Duration::from_secs(45),
        )
        .await
    }

    /// Synchronously registers the pending reply waiter and pushes a single-attempt RPC
    /// into the active connection's `out_tx`, without awaiting. Callers can hold an external lock
    /// (such as `active_tasks`) across this synchronous step before `.await`ing `await_single_call`.
    fn enqueue_single_call(
        &self,
        request_id: String,
        msg: DaemonToServerMessage,
    ) -> Result<PreparedSingleCall, ClientError> {
        let (is_disc, out_tx, pending, conn_disc) = {
            let guard = self.inner.lock().expect("tunnel mutex poisoned");
            (
                guard.disconnected.load(Ordering::Acquire),
                guard.out_tx.clone(),
                guard.pending.clone(),
                guard.disconnected.clone(),
            )
        };
        if is_disc {
            return Err(ClientError::Tunnel("tunnel disconnected".into()));
        }
        let (tx, rx) = oneshot::channel();
        pending
            .lock()
            .expect("pending mutex poisoned")
            .insert(request_id.clone(), tx);
        if conn_disc.load(Ordering::Acquire) || out_tx.send(msg).is_err() {
            pending
                .lock()
                .expect("pending mutex poisoned")
                .remove(&request_id);
            return Err(ClientError::Tunnel("tunnel write channel closed".into()));
        }
        Ok((request_id, pending, rx))
    }

    async fn await_single_call(
        prepared: Result<PreparedSingleCall, ClientError>,
        timeout: std::time::Duration,
    ) -> Result<ServerToDaemonMessage, ClientError> {
        let (request_id, pending, rx) = prepared?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(ClientError::Tunnel(
                "tunnel response channel dropped".into(),
            )),
            Err(_) => {
                pending
                    .lock()
                    .expect("pending mutex poisoned")
                    .remove(&request_id);
                Err(ClientError::Tunnel("tunnel call timed out".into()))
            }
        }
    }

    /// Synchronously enqueues a `Heartbeat` with the given snapshot and returns a future for its reply.
    pub fn start_heartbeat_with_activations_timeout(
        &self,
        node_id: NodeId,
        active_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
        timeout: std::time::Duration,
    ) -> impl std::future::Future<Output = Result<ResourceVersion, ClientError>> + Send {
        let request_id = format!("req_{}", Uuid::now_v7());
        let prepared = self.enqueue_single_call(
            request_id.clone(),
            DaemonToServerMessage::Heartbeat {
                request_id,
                node_id,
                active_runners,
                running_activation_ids,
            },
        );
        async move {
            let resp = Self::await_single_call(prepared, timeout).await?;
            Self::expect_ack(resp).map(|(rv, _, _, _)| rv.unwrap_or_default())
        }
    }

    pub async fn heartbeat_with_activations_timeout(
        &self,
        node_id: NodeId,
        active_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
        timeout: std::time::Duration,
    ) -> Result<ResourceVersion, ClientError> {
        self.start_heartbeat_with_activations_timeout(
            node_id,
            active_runners,
            running_activation_ids,
            timeout,
        )
        .await
    }

    pub async fn accept_activation(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        effective_config: ConfigRevision,
        effective_permission: PermissionSnapshot,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::AcceptActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                effective_config: Box::new(effective_config),
                effective_permission: Box::new(effective_permission),
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _, _)| rv.unwrap_or_default())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_batch(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        kind: ItemBatchKind,
        turn_id: Option<TurnId>,
        response_id: Option<String>,
        items: Vec<Item>,
        companion_event: BatchCompanionEvent,
    ) -> Result<BatchSeq, ClientError> {
        self.commit_batch_with_drain(
            session_id,
            activation_id,
            lease_epoch,
            kind,
            turn_id,
            response_id,
            items,
            companion_event,
            false,
        )
        .await
        .map(|(seq, _)| seq)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_batch_with_drain(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        kind: ItemBatchKind,
        turn_id: Option<TurnId>,
        response_id: Option<String>,
        items: Vec<Item>,
        companion_event: BatchCompanionEvent,
        drain_realtime: bool,
    ) -> Result<(BatchSeq, DrainedRealtimeBatch), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::CommitBatch {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                kind,
                turn_id,
                response_id,
                items,
                companion_event,
                drain_realtime,
            })
            .await?;
        Self::expect_ack(resp)
            .map(|(_, batch_seq, _, drained)| (batch_seq.unwrap_or_default(), drained))
    }

    pub async fn begin_turn(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: TurnId,
        include_context: bool,
    ) -> Result<(DrainedRealtimeBatch, Option<Vec<Item>>), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::BeginTurn {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                include_context,
            })
            .await?;
        match resp {
            ServerToDaemonMessage::BeginTurnReply {
                ok: true,
                drained_realtime,
                effective_items,
                ..
            } => Ok((drained_realtime, effective_items)),
            ServerToDaemonMessage::BeginTurnReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "begin turn failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected begin turn reply: {other:?}"
            ))),
        }
    }

    pub async fn emit_event(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<ResourceVersion, ClientError> {
        if event.is_ephemeral_stream_delta() {
            let out_tx = {
                let guard = self.inner.lock().expect("tunnel mutex poisoned");
                guard.out_tx.clone()
            };
            let _ = out_tx.send(DaemonToServerMessage::EmitEvent {
                request_id: String::new(),
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                event,
            });
            return Ok(ResourceVersion::new(0));
        }

        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::EmitEvent {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                event,
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _, _)| rv.unwrap_or_default())
    }

    pub async fn fetch_effective_context(
        &self,
        session_id: SessionId,
    ) -> Result<Vec<Item>, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::FetchEffectiveContext {
                request_id,
                session_id,
            })
            .await?;
        match resp {
            ServerToDaemonMessage::EffectiveContextReply {
                ok: true, items, ..
            } => Ok(items),
            ServerToDaemonMessage::EffectiveContextReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "fetch context failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected context reply: {other:?}"
            ))),
        }
    }

    pub async fn push_realtime_message(
        &self,
        session_id: SessionId,
        activation_id: Option<ActivationId>,
        content: Vec<openresponses_rust::MessageContent>,
    ) -> Result<(), ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::PushRealtimeMessage {
                request_id,
                session_id,
                activation_id,
                content,
            })
            .await?;
        Self::expect_ack(resp).map(|_| ())
    }

    pub async fn drain_realtime_messages(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
    ) -> Result<DrainedRealtimeBatch, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::DrainRealtimeMessages {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
            })
            .await?;
        match resp {
            ServerToDaemonMessage::RealtimeMessagesReply {
                ok: true, drained, ..
            } => Ok(drained),
            ServerToDaemonMessage::RealtimeMessagesReply {
                ok: false, error, ..
            } => Err(ClientError::Tunnel(
                error.unwrap_or_else(|| "drain realtime messages failed".into()),
            )),
            other => Err(ClientError::Tunnel(format!(
                "unexpected realtime messages reply: {other:?}"
            ))),
        }
    }

    pub async fn complete_activation(
        &self,
        session_id: SessionId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        outcome: ActivationTerminalOutcome,
    ) -> Result<ResourceVersion, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::CompleteActivation {
                request_id,
                session_id,
                activation_id,
                lease_epoch,
                turn_id,
                outcome,
            })
            .await?;
        Self::expect_ack(resp).map(|(rv, _, _, _)| rv.unwrap_or_default())
    }

    pub async fn rollover_compacted_session(
        &self,
        request: RolloverCompactedSessionRequest,
    ) -> Result<RolloverCompactedSessionResponse, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::RolloverCompactedSession {
                request_id,
                request: Box::new(request),
            })
            .await?;
        match resp {
            ServerToDaemonMessage::RolloverCompactedSessionReply {
                ok: true,
                response: Some(response),
                ..
            } => Ok(*response),
            ServerToDaemonMessage::RolloverCompactedSessionReply {
                ok: false, error, ..
            } => {
                Err(ClientError::Tunnel(error.unwrap_or_else(|| {
                    "rollover compacted session failed".into()
                })))
            }
            other => Err(ClientError::Tunnel(format!(
                "unexpected rollover compacted session reply: {other:?}"
            ))),
        }
    }

    pub async fn query_session_history(
        &self,
        session_id: SessionId,
        request: SessionHistoryQueryRequest,
    ) -> Result<SessionHistoryQueryResponse, ClientError> {
        let request_id = format!("req_{}", Uuid::now_v7());
        let resp = self
            .call(DaemonToServerMessage::QuerySessionHistory {
                request_id,
                session_id,
                request: Box::new(request),
            })
            .await?;
        match resp {
            ServerToDaemonMessage::QuerySessionHistoryReply { response, .. } => Ok(*response),
            other => Err(ClientError::Tunnel(format!(
                "unexpected query session history reply: {other:?}"
            ))),
        }
    }

    pub fn send_node_extension_reply(
        &self,
        request_id: String,
        response: NodeExtensionCallResponse,
    ) {
        let out_tx = {
            let guard = self.inner.lock().expect("tunnel mutex poisoned");
            guard.out_tx.clone()
        };
        let _ = out_tx.send(DaemonToServerMessage::NodeExtensionReply {
            request_id,
            response: Box::new(response),
        });
    }

    fn expect_ack(msg: ServerToDaemonMessage) -> Result<AckPayload, ClientError> {
        match msg {
            ServerToDaemonMessage::Ack {
                ok: true,
                resource_version,
                batch_seq,
                spawned_session_id,
                drained_realtime,
                ..
            } => Ok((
                resource_version,
                batch_seq,
                spawned_session_id,
                drained_realtime,
            )),
            ServerToDaemonMessage::Ack {
                ok: false, error, ..
            } => {
                Err(ClientError::Tunnel(error.unwrap_or_else(|| {
                    "daemon tunnel operation rejected".into()
                })))
            }
            other => Err(ClientError::Tunnel(format!(
                "expected Ack message, got {other:?}"
            ))),
        }
    }

    fn extract_request_id(msg: &DaemonToServerMessage) -> &str {
        match msg {
            DaemonToServerMessage::RegisterNode { request_id, .. }
            | DaemonToServerMessage::ResolveModelProvider { request_id, .. }
            | DaemonToServerMessage::Heartbeat { request_id, .. }
            | DaemonToServerMessage::AcceptActivation { request_id, .. }
            | DaemonToServerMessage::CommitBatch { request_id, .. }
            | DaemonToServerMessage::BeginTurn { request_id, .. }
            | DaemonToServerMessage::EmitEvent { request_id, .. }
            | DaemonToServerMessage::FetchEffectiveContext { request_id, .. }
            | DaemonToServerMessage::PushRealtimeMessage { request_id, .. }
            | DaemonToServerMessage::DrainRealtimeMessages { request_id, .. }
            | DaemonToServerMessage::CompleteActivation { request_id, .. }
            | DaemonToServerMessage::RolloverCompactedSession { request_id, .. }
            | DaemonToServerMessage::QuerySessionHistory { request_id, .. }
            | DaemonToServerMessage::NodeExtensionReply { request_id, .. } => request_id,
        }
    }
}

pub async fn connect_daemon_tunnel(
    ws_url: &str,
) -> Result<(DaemonTunnelHandle, mpsc::UnboundedReceiver<DaemonPushEvent>), ClientError> {
    connect_daemon_tunnel_with_token(ws_url, None).await
}

pub async fn connect_daemon_tunnel_with_token(
    ws_url: &str,
    auth_token: Option<&str>,
) -> Result<(DaemonTunnelHandle, mpsc::UnboundedReceiver<DaemonPushEvent>), ClientError> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut req = ws_url
        .into_client_request()
        .map_err(|e| ClientError::WebSocket(Box::new(e)))?;
    if let Some(tok) = auth_token {
        if let Ok(val) = format!("Bearer {tok}").parse() {
            req.headers_mut().insert("Authorization", val);
        }
    }
    let uri = req.uri().clone();
    let raw_host = uri
        .host()
        .ok_or_else(|| ClientError::Tunnel("websocket url missing host".into()))?;
    let host = raw_host.trim_start_matches('[').trim_end_matches(']');
    let port = uri
        .port_u16()
        .or_else(|| match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or_else(|| ClientError::Tunnel("unsupported websocket url scheme".into()))?;
    let tcp_stream = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| ClientError::Tunnel(format!("websocket tcp connect failed: {e}")))?;
    let _ = tcp_stream.set_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(15))
        .with_interval(std::time::Duration::from_secs(5));
    let _ = socket2::SockRef::from(&tcp_stream).set_tcp_keepalive(&keepalive);

    crate::ensure_rustls_crypto_provider();
    let (ws_stream, _) = client_async_tls_with_config(req, tcp_stream, None, None)
        .await
        .map_err(|e| ClientError::WebSocket(Box::new(e)))?;
    let (mut ws_sink, mut ws_source) = ws_stream.split();

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<DaemonToServerMessage>();
    let (push_tx, push_rx) = mpsc::unbounded_channel::<DaemonPushEvent>();
    let pending: Arc<Mutex<HashMap<String, oneshot::Sender<ServerToDaemonMessage>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let disconnected = Arc::new(AtomicBool::new(false));
    let cancel = CancellationToken::new();

    let writer_cancel = cancel.clone();
    let writer_disc = disconnected.clone();
    let writer_pending = pending.clone();
    let writer_push_tx = push_tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = writer_cancel.cancelled() => {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        ws_sink.close(),
                    )
                    .await;
                    break;
                }
                maybe_msg = out_rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            ws_sink.close(),
                        )
                        .await;
                        break;
                    };
                    if let Ok(text) = serde_json::to_string(&msg) {
                        if ws_sink.send(WsMessage::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
        writer_disc.store(true, Ordering::Release);
        writer_pending.lock().expect("pending mutex poisoned").clear();
        let _ = writer_push_tx.send(DaemonPushEvent::Disconnected);
    });

    let push_tx_for_handle = push_tx.clone();
    let pending_reader = pending.clone();
    let reader_cancel = cancel.clone();
    let reader_disc = disconnected.clone();
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                biased;
                _ = reader_cancel.cancelled() => break,
                next_frame = ws_source.next() => match next_frame {
                    Some(Ok(frame)) => frame,
                    _ => break,
                }
            };
            let text = match frame {
                WsMessage::Text(t) => t.to_string(),
                WsMessage::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
                WsMessage::Close(_) => break,
                _ => continue,
            };

            let incoming: ServerToDaemonMessage = match serde_json::from_str(&text) {
                Ok(m) => m,
                Err(_) => continue,
            };

            match incoming {
                ServerToDaemonMessage::Registered { ref request_id, .. }
                | ServerToDaemonMessage::ModelProviderReply { ref request_id, .. }
                | ServerToDaemonMessage::Ack { ref request_id, .. }
                | ServerToDaemonMessage::BeginTurnReply { ref request_id, .. }
                | ServerToDaemonMessage::EffectiveContextReply { ref request_id, .. }
                | ServerToDaemonMessage::RealtimeMessagesReply { ref request_id, .. }
                | ServerToDaemonMessage::RolloverCompactedSessionReply { ref request_id, .. }
                | ServerToDaemonMessage::QuerySessionHistoryReply { ref request_id, .. } => {
                    let req_id = request_id.clone();
                    let tx = pending_reader
                        .lock()
                        .expect("pending mutex poisoned")
                        .remove(&req_id);
                    if let Some(tx) = tx {
                        let _ = tx.send(incoming);
                    }
                }
                ServerToDaemonMessage::DispatchActivation {
                    session,
                    activation,
                    lease_epoch,
                    cluster_system_prompt,
                    node_spec,
                    cluster_default_model,
                    cluster_model_provider,
                } => {
                    let _ = push_tx.send(DaemonPushEvent::DispatchActivation {
                        session,
                        activation,
                        lease_epoch,
                        cluster_system_prompt,
                        node_spec,
                        cluster_default_model,
                        cluster_model_provider,
                    });
                }
                ServerToDaemonMessage::ReconcileActivation {
                    session_id,
                    activation_id,
                    lease_epoch,
                    desired_phase,
                    interaction_responses,
                } => {
                    let _ = push_tx.send(DaemonPushEvent::ReconcileActivation {
                        session_id,
                        activation_id,
                        lease_epoch,
                        desired_phase,
                        interaction_responses,
                    });
                }
                ServerToDaemonMessage::CleanupSession { session_id } => {
                    let _ = push_tx.send(DaemonPushEvent::CleanupSession { session_id });
                }
                ServerToDaemonMessage::NodeExtensionCall {
                    request_id,
                    request,
                } => {
                    let _ = push_tx.send(DaemonPushEvent::NodeExtensionCall {
                        request_id,
                        request,
                    });
                }
                ServerToDaemonMessage::EventNotification { .. } => {}
            }
        }
        reader_disc.store(true, Ordering::Release);
        pending_reader
            .lock()
            .expect("pending mutex poisoned")
            .clear();
        let _ = push_tx.send(DaemonPushEvent::Disconnected);
    });

    Ok((
        DaemonTunnelHandle {
            inner: Arc::new(Mutex::new(ActiveConnection {
                epoch: 0,
                out_tx,
                pending,
                push_tx: push_tx_for_handle,
                disconnected,
                cancel,
            })),
            auto_reconnect: Arc::new(AtomicBool::new(false)),
            state_notify: Arc::new(Notify::new()),
        },
        push_rx,
    ))
}
