use super::{ReconcileNotice, ScheduledDispatch, SqliteStore, StoreError};
use chrono::{DateTime, Duration, Utc};
use ragents_protocol::*;
use rusqlite::{params, OptionalExtension};
use std::collections::BTreeMap;

/// Grace period (5 minutes) after a node's heartbeat lease expires (`NodePhase::NotReady`)
/// before already-running activations (`Running` / `WaitingForInteraction` / `Cancelling`)
/// are forcefully marked `Interrupted`. If the node reconnects within this window and still
/// reports the activation in `running_activation_ids`, execution continues seamlessly.
pub const DEFAULT_RUNNING_ACTIVATION_GRACE_MS: u64 = 300_000;

impl SqliteStore {
    #[allow(clippy::too_many_arguments)]
    pub fn register_node(
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
        lease_ttl_ms: u64,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
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
            lease_ttl_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_node_with_workspace_dirs(
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
        lease_ttl_ms: u64,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
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
            lease_ttl_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_node_with_seed(
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
        lease_ttl_ms: u64,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
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
            lease_ttl_ms,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_node_with_running_activations(
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
        lease_ttl_ms: u64,
        running_activation_ids: Option<Vec<ActivationId>>,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
        if !node_id.is_safe_identifier() {
            return Err(StoreError::Validation(
                ProtocolValidationError::InvalidIdentifier(node_id.to_string()),
            ));
        }

        let mut events_to_publish = Vec::new();
        let mut reconcile_notices = Vec::new();
        let mut dispatches;
        let node = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;
            let rv = Self::next_resource_version(&tx)?;
            let now = Utc::now();
            let expires_at = now + Duration::milliseconds(lease_ttl_ms as i64);

            if let Some(ref reported_ids) = running_activation_ids {
                Self::reconcile_node_running_activations_in_tx(
                    &tx,
                    &node_id,
                    reported_ids,
                    true,
                    now,
                    &mut events_to_publish,
                    &mut reconcile_notices,
                )?;
            }

            let active_count: u32 = tx.query_row(
                "SELECT COUNT(*) FROM activations WHERE assigned_node = ?1 AND phase IN ('scheduled', 'running', 'waiting_for_interaction', 'cancelling')",
                params![node_id.as_str()],
                |r| r.get(0),
            )?;

            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT spec_json, status_json FROM nodes WHERE node_id = ?1",
                    params![node_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            // Refresh daemon-owned base configuration (`node_prompt` and `base_permissions`) on every registration
            // while preserving control-plane overrides (`node_prompt_override`) and interactive grants (`granted_permissions`, `workspace_permissions`).
            let spec = match existing {
                Some((spec_json, _)) => {
                    let mut prev = serde_json::from_str::<NodeSpec>(&spec_json)?;
                    prev.node_prompt = default_system_prompt;
                    prev.base_permissions = default_permissions;
                    prev
                }
                None => NodeSpec {
                    unschedulable: false,
                    node_prompt: default_system_prompt,
                    node_prompt_override: None,
                    base_permissions: default_permissions,
                    granted_permissions: PermissionPolicyLayer::default(),
                    workspace_permissions: BTreeMap::new(),
                },
            };

            let mut meta = ObjectMeta::new(node_id.as_str(), rv);
            meta.labels = labels;

            let effective_node_prompt = spec.effective_node_prompt();
            let effective_permissions = spec.effective_permissions();

            let status = NodeStatus {
                phase: NodePhase::Ready,
                hostname,
                os,
                arch,
                supported_models,
                default_model,
                installed_extensions,
                allowed_workspace_dirs,
                effective_node_prompt: effective_node_prompt.clone(),
                effective_permissions,
                max_concurrent_runners: max_concurrent_runners.max(1),
                active_runners: active_count,
                last_heartbeat_at: now,
                lease_expires_at: expires_at,
            };

            let node = Node {
                metadata: meta,
                spec,
                status,
            };

            tx.execute(
                "INSERT INTO nodes (node_id, resource_version, spec_json, status_json, lease_expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(node_id) DO UPDATE SET
                    resource_version = excluded.resource_version,
                    spec_json = excluded.spec_json,
                    status_json = excluded.status_json,
                    lease_expires_at = excluded.lease_expires_at",
                params![
                    node_id.as_str(),
                    rv.value() as i64,
                    serde_json::to_string(&node.spec)?,
                    serde_json::to_string(&node.status)?,
                    expires_at.to_rfc3339(),
                ],
            )?;

            // Synchronize `latest_prompt_parts.node_prompt` for sessions currently bound to this node
            // so the Inspector immediately displays the refreshed `effective_node_prompt` after daemon restart.
            let bound_session_ids: Vec<String> = {
                let mut stmt =
                    tx.prepare("SELECT session_id FROM sessions WHERE bound_node_id = ?1")?;
                let rows = stmt.query_map(params![node_id.as_str()], |r| r.get(0))?;
                let mut ids = Vec::new();
                for r in rows {
                    ids.push(r?);
                }
                ids
            };
            for sid_str in bound_session_ids {
                let sid = SessionId::new(sid_str);
                if let Some(mut sess) = Self::load_session_in_tx(&tx, &sid)? {
                    if let Some(ref mut parts) = sess.status.latest_prompt_parts {
                        if parts.node_prompt != effective_node_prompt {
                            parts.node_prompt = effective_node_prompt.clone();
                            Self::save_session_in_tx(&tx, &mut sess)?;
                        }
                    }
                }
            }

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;

            // Also re-emit dispatches for any activations already in `scheduled` phase assigned to `node_id`
            // (in case the daemon disconnected right before receiving `DispatchActivation`).
            let scheduled_rows: Vec<(String, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT activation_id, session_id FROM activations WHERE assigned_node = ?1 AND phase = 'scheduled' ORDER BY created_at ASC",
                )?;
                let rows = stmt.query_map(params![node_id.as_str()], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?;
                let mut list = Vec::new();
                for r in rows {
                    list.push(r?);
                }
                list
            };
            for (act_id_str, sess_id_str) in scheduled_rows {
                if dispatches
                    .iter()
                    .any(|d| d.activation.metadata.id == act_id_str)
                {
                    continue;
                }
                let act_id = ActivationId::new(act_id_str);
                let sess_id = SessionId::new(sess_id_str);
                if let (Some(act), Some(sess)) = (
                    Self::load_activation_in_tx(&tx, &act_id)?,
                    Self::load_session_in_tx(&tx, &sess_id)?,
                ) {
                    let lease_epoch = act.status.lease_epoch;
                    let cluster_system_prompt = Self::load_cluster_system_prompt_in_tx(&tx)?;
                    dispatches.push(ScheduledDispatch {
                        node_id: node_id.clone(),
                        session: sess,
                        activation: act,
                        lease_epoch,
                        cluster_system_prompt,
                        node_spec: node.spec.clone(),
                    });
                }
            }

            tx.commit()?;
            node
        };

        for notice in reconcile_notices {
            let _ = self.reconcile_tx.send(notice);
        }
        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((node, dispatches))
    }

    pub fn heartbeat_node(
        &self,
        node_id: &NodeId,
        active_runners: u32,
        lease_ttl_ms: u64,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
        self.heartbeat_node_with_activations(node_id, active_runners, None, lease_ttl_ms)
    }

    pub fn heartbeat_node_with_activations(
        &self,
        node_id: &NodeId,
        active_runners: u32,
        running_activation_ids: Option<Vec<ActivationId>>,
        lease_ttl_ms: u64,
    ) -> Result<(Node, Vec<ScheduledDispatch>), StoreError> {
        let mut events_to_publish = Vec::new();
        let mut reconcile_notices = Vec::new();
        let dispatches;
        let node = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let (spec_json, status_json): (String, String) = tx
                .query_row(
                    "SELECT spec_json, status_json FROM nodes WHERE node_id = ?1",
                    params![node_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(format!("node {node_id}")))?;

            let rv = Self::next_resource_version(&tx)?;
            let now = Utc::now();
            let expires_at = now + Duration::milliseconds(lease_ttl_ms as i64);

            if let Some(ref reported_ids) = running_activation_ids {
                Self::reconcile_node_running_activations_in_tx(
                    &tx,
                    node_id,
                    reported_ids,
                    false,
                    now,
                    &mut events_to_publish,
                    &mut reconcile_notices,
                )?;
            }

            let spec: NodeSpec = serde_json::from_str(&spec_json)?;
            let mut status: NodeStatus = serde_json::from_str(&status_json)?;
            status.phase = NodePhase::Ready;
            status.active_runners = active_runners;
            status.last_heartbeat_at = now;
            status.lease_expires_at = expires_at;

            tx.execute(
                "UPDATE nodes SET resource_version = ?1, status_json = ?2, lease_expires_at = ?3 WHERE node_id = ?4",
                params![
                    rv.value() as i64,
                    serde_json::to_string(&status)?,
                    expires_at.to_rfc3339(),
                    node_id.as_str(),
                ],
            )?;

            let node = Node {
                metadata: ObjectMeta::new(node_id.as_str(), rv),
                spec,
                status,
            };

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            tx.commit()?;
            node
        };

        for notice in reconcile_notices {
            let _ = self.reconcile_tx.send(notice);
        }
        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((node, dispatches))
    }

    fn reconcile_node_running_activations_in_tx(
        tx: &rusqlite::Transaction<'_>,
        node_id: &NodeId,
        reported_ids: &[ActivationId],
        is_register: bool,
        now: DateTime<Utc>,
        events_out: &mut Vec<SessionEventEnvelope>,
        reconcile_out: &mut Vec<ReconcileNotice>,
    ) -> Result<(), StoreError> {
        let server_active_ids: Vec<String> = {
            let mut stmt = tx.prepare(
                "SELECT activation_id FROM activations
                 WHERE assigned_node = ?1
                   AND phase IN ('running', 'waiting_for_interaction', 'cancelling')",
            )?;
            let rows = stmt.query_map(params![node_id.as_str()], |r| r.get::<_, String>(0))?;
            let mut ids = Vec::new();
            for r in rows {
                ids.push(r?);
            }
            ids
        };

        // 1. Check activations that api-server thinks are running on `node_id`
        for act_id_str in server_active_ids {
            let act_id = ActivationId::new(act_id_str);
            let Some(mut act) = Self::load_activation_in_tx(tx, &act_id)? else {
                continue;
            };
            if reported_ids.iter().any(|r| r == &act_id) {
                // Still running on daemon: on reconnect (`RegisterNode`), re-push any pending
                // cancellation or permission interaction responses that arrived while offline.
                if is_register
                    && (act.spec.desired_phase == DesiredActivationPhase::Cancelled
                        || !act.spec.interaction_responses.is_empty())
                {
                    reconcile_out.push(ReconcileNotice {
                        node_id: node_id.clone(),
                        session_id: act.spec.session_id.clone(),
                        activation_id: act_id,
                        lease_epoch: act.status.lease_epoch,
                        desired_phase: act.spec.desired_phase,
                        interaction_responses: act.spec.interaction_responses.clone(),
                    });
                }
            } else {
                // Node is online and reported its active activations, and `act_id` is gone
                // (e.g. daemon restarted or worker crashed). Mark Interrupted immediately!
                let mut session = Self::load_session_in_tx(tx, &act.spec.session_id)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!("session {}", act.spec.session_id))
                    })?;
                act.status.lease_epoch = act.status.lease_epoch.next();
                let reason = format!(
                    "Node {} reported activation is no longer running (was in {:?} phase)",
                    node_id, act.status.phase
                );
                act.status.phase = ActivationPhase::Interrupted;
                act.status.error = Some(reason.clone());
                act.status.finished_at = Some(now);
                if session.status.active_activation_id.as_ref() == Some(&act_id) {
                    session.status.active_activation_id = None;
                }
                session.status.last_error = Some(reason.clone());

                let ev = Self::append_event_in_tx(
                    tx,
                    &session.metadata.id.clone().into(),
                    Some(act_id.clone()),
                    None,
                    None,
                    SessionEvent::ActivationInterrupted { reason },
                )?;
                session.status.event_count = ev.event_seq.value() + 1;
                events_out.push(ev);

                Self::save_activation_in_tx(tx, &mut act)?;
                Self::save_session_in_tx(tx, &mut session)?;
            }
        }

        // 2. Check activations that the daemon claims it is running: if any is already terminal
        // or not assigned to this node, immediately instruct the daemon to cancel & kill it.
        for reported_id in reported_ids {
            let act_opt = Self::load_activation_in_tx(tx, reported_id)?;
            let should_kill = match &act_opt {
                None => true,
                Some(act) => {
                    act.status.assigned_node.as_ref() != Some(node_id)
                        || act.status.phase.is_terminal()
                        || act.spec.desired_phase == DesiredActivationPhase::Cancelled
                }
            };
            if should_kill
                && !reconcile_out
                    .iter()
                    .any(|r| &r.activation_id == reported_id)
            {
                reconcile_out.push(ReconcileNotice {
                    node_id: node_id.clone(),
                    session_id: act_opt
                        .as_ref()
                        .map(|a| a.spec.session_id.clone())
                        .unwrap_or_else(|| SessionId::new("orphan")),
                    activation_id: reported_id.clone(),
                    lease_epoch: act_opt
                        .as_ref()
                        .map(|a| a.status.lease_epoch)
                        .unwrap_or_else(|| LeaseEpoch::new(0)),
                    desired_phase: DesiredActivationPhase::Cancelled,
                    interaction_responses: BTreeMap::new(),
                });
            }
        }

        Ok(())
    }

    pub fn list_nodes(&self) -> Result<Vec<Node>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT node_id, resource_version, spec_json, status_json FROM nodes ORDER BY node_id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let node_id: String = row.get(0)?;
            let rv: i64 = row.get(1)?;
            let spec_json: String = row.get(2)?;
            let status_json: String = row.get(3)?;
            Ok((node_id, rv as u64, spec_json, status_json))
        })?;

        let mut nodes = Vec::new();
        for r in rows {
            let (node_id, rv, spec_json, status_json) = r?;
            let spec: NodeSpec = serde_json::from_str(&spec_json)?;
            let status: NodeStatus = serde_json::from_str(&status_json)?;
            let mut meta = ObjectMeta::new(node_id, ResourceVersion::new(rv));
            meta.updated_at = status.last_heartbeat_at;
            nodes.push(Node {
                metadata: meta,
                spec,
                status,
            });
        }
        Ok(nodes)
    }

    pub fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let row: Option<(i64, String, String)> = conn
            .query_row(
                "SELECT resource_version, spec_json, status_json FROM nodes WHERE node_id = ?1",
                params![node_id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;

        match row {
            Some((rv, spec_json, status_json)) => {
                let spec: NodeSpec = serde_json::from_str(&spec_json)?;
                let status: NodeStatus = serde_json::from_str(&status_json)?;
                let mut meta = ObjectMeta::new(node_id.as_str(), ResourceVersion::new(rv as u64));
                meta.updated_at = status.last_heartbeat_at;
                Ok(Some(Node {
                    metadata: meta,
                    spec,
                    status,
                }))
            }
            None => Ok(None),
        }
    }

    /// Two-stage lease patrol:
    /// 1. When `now > lease_expires_at` (e.g. 30s):
    ///    - Marks `Ready` nodes as `NotReady`.
    ///    - Unstarted `Scheduled` activations on expired nodes are immediately rescheduled (`Floating`)
    ///      or marked `Interrupted` (`Pinned`).
    ///    - Already-executing (`Running` / `WaitingForInteraction` / `Cancelling`) activations remain
    ///      alive during `running_grace_ms` (default 5 minutes) so cross-internet network blips can
    ///      recover seamlessly upon daemon reconnect.
    /// 2. When `now > lease_expires_at + running_grace_ms`:
    ///    - Marks remaining `Running` / `WaitingForInteraction` / `Cancelling` activations on the
    ///      long-dead node as `Interrupted` and bumps `lease_epoch`.
    pub fn patrol_expired_leases(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<ScheduledDispatch>, StoreError> {
        self.patrol_expired_leases_with_grace(now, DEFAULT_RUNNING_ACTIVATION_GRACE_MS)
    }

    pub fn patrol_expired_leases_with_grace(
        &self,
        now: DateTime<Utc>,
        running_grace_ms: u64,
    ) -> Result<Vec<ScheduledDispatch>, StoreError> {
        let mut events_to_publish = Vec::new();
        let dispatches;
        {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let expired_nodes: Vec<(String, String, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT node_id, status_json, lease_expires_at FROM nodes WHERE lease_expires_at < ?1",
                )?;
                let rows = stmt.query_map(params![now.to_rfc3339()], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?;
                let mut list = Vec::new();
                for r in rows {
                    list.push(r?);
                }
                list
            };

            let grace_cutoff = now - Duration::milliseconds(running_grace_ms as i64);

            for (node_id_str, status_json, lease_expires_str) in expired_nodes {
                let mut status: NodeStatus = serde_json::from_str(&status_json)?;
                if status.phase == NodePhase::Ready {
                    status.phase = NodePhase::NotReady;
                    status.active_runners = 0;
                    let rv = Self::next_resource_version(&tx)?;
                    tx.execute(
                        "UPDATE nodes SET resource_version = ?1, status_json = ?2 WHERE node_id = ?3",
                        params![
                            rv.value() as i64,
                            serde_json::to_string(&status)?,
                            node_id_str,
                        ],
                    )?;
                }

                let dead_node = NodeId::new(node_id_str);
                let lease_expired_at = DateTime::parse_from_rfc3339(&lease_expires_str)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or(now);
                let grace_exceeded = lease_expired_at <= grace_cutoff;

                let phase_query = if grace_exceeded {
                    "SELECT activation_id FROM activations
                     WHERE assigned_node = ?1
                       AND phase IN ('scheduled', 'running', 'waiting_for_interaction', 'cancelling')"
                } else {
                    "SELECT activation_id FROM activations
                     WHERE assigned_node = ?1
                       AND phase = 'scheduled'"
                };

                let affected: Vec<String> = {
                    let mut stmt = tx.prepare(phase_query)?;
                    let rows =
                        stmt.query_map(params![dead_node.as_str()], |r| r.get::<_, String>(0))?;
                    let mut ids = Vec::new();
                    for r in rows {
                        ids.push(r?);
                    }
                    ids
                };

                for act_id_str in affected {
                    let act_id = ActivationId::new(act_id_str);
                    let mut act = Self::load_activation_in_tx(&tx, &act_id)?
                        .ok_or_else(|| StoreError::NotFound(format!("activation {act_id}")))?;
                    let mut session = Self::load_session_in_tx(&tx, &act.spec.session_id)?
                        .ok_or_else(|| {
                            StoreError::NotFound(format!("session {}", act.spec.session_id))
                        })?;

                    let bumped_epoch = act.status.lease_epoch.next();
                    act.status.lease_epoch = bumped_epoch;

                    if session.spec.scheduling.is_floating()
                        && act.status.phase == ActivationPhase::Scheduled
                    {
                        act.status.phase = ActivationPhase::Queued;
                        act.status.assigned_node = None;
                        session.status.active_activation_id = None;
                        session.status.queued_activation_count =
                            session.status.queued_activation_count.saturating_add(1);
                        Self::save_activation_in_tx(&tx, &mut act)?;
                        Self::save_session_in_tx(&tx, &mut session)?;
                    } else {
                        let reason = format!(
                            "Node {} lease expired while activation was in {:?} phase",
                            dead_node, act.status.phase
                        );
                        act.status.phase = ActivationPhase::Interrupted;
                        act.status.error = Some(reason.clone());
                        act.status.finished_at = Some(now);
                        if session.status.active_activation_id.as_ref() == Some(&act_id) {
                            session.status.active_activation_id = None;
                        }
                        session.status.last_error = Some(reason.clone());

                        let ev = Self::append_event_in_tx(
                            &tx,
                            &session.metadata.id.clone().into(),
                            Some(act_id.clone()),
                            None,
                            None,
                            SessionEvent::ActivationInterrupted { reason },
                        )?;
                        session.status.event_count = ev.event_seq.value() + 1;
                        events_to_publish.push(ev);

                        Self::save_activation_in_tx(&tx, &mut act)?;
                        Self::save_session_in_tx(&tx, &mut session)?;
                    }
                }
            }

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            tx.commit()?;
        }

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok(dispatches)
    }
}
