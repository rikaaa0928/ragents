use super::{ScheduledDispatch, SqliteStore, StoreError};
use chrono::{DateTime, Duration, Utc};
use ragents_protocol::*;
use rusqlite::{params, OptionalExtension};
use std::collections::BTreeMap;

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
        if !node_id.is_safe_identifier() {
            return Err(StoreError::Validation(
                ProtocolValidationError::InvalidIdentifier(node_id.to_string()),
            ));
        }

        let mut events_to_publish = Vec::new();
        let mut dispatches;
        let node = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;
            let rv = Self::next_resource_version(&tx)?;
            let now = Utc::now();
            let expires_at = now + Duration::milliseconds(lease_ttl_ms as i64);

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

            let spec = match existing {
                Some((spec_json, _)) => serde_json::from_str::<NodeSpec>(&spec_json)?,
                None => NodeSpec::default(),
            };

            let mut meta = ObjectMeta::new(node_id.as_str(), rv);
            meta.labels = labels;

            let status = NodeStatus {
                phase: NodePhase::Ready,
                hostname,
                os,
                arch,
                supported_models,
                default_model,
                installed_extensions,
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
                    dispatches.push(ScheduledDispatch {
                        node_id: node_id.clone(),
                        session: sess,
                        activation: act,
                        lease_epoch,
                    });
                }
            }

            tx.commit()?;
            node
        };

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
        let mut events_to_publish = Vec::new();
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

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((node, dispatches))
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

    /// Marks nodes whose lease has expired as `NotReady`, and handles their active activations:
    /// - `Floating` sessions with `Scheduled` (unstarted) activations are automatically rescheduled
    ///   onto another healthy node (or queued if none available right now).
    /// - `Pinned` sessions (or already `Running` activations with potential side effects) have their
    ///   `lease_epoch` bumped and are marked `Interrupted`.
    pub fn patrol_expired_leases(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<ScheduledDispatch>, StoreError> {
        let mut events_to_publish = Vec::new();
        let dispatches;
        {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let expired_nodes: Vec<(String, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT node_id, status_json FROM nodes WHERE lease_expires_at < ?1",
                )?;
                let rows = stmt.query_map(params![now.to_rfc3339()], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?;
                let mut list = Vec::new();
                for r in rows {
                    list.push(r?);
                }
                list
            };

            let mut newly_not_ready = Vec::new();
            for (node_id_str, status_json) in expired_nodes {
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
                    newly_not_ready.push(NodeId::new(node_id_str));
                }
            }

            for dead_node in &newly_not_ready {
                let affected: Vec<String> = {
                    let mut stmt = tx.prepare(
                        "SELECT activation_id FROM activations
                         WHERE assigned_node = ?1
                           AND phase IN ('scheduled', 'running', 'waiting_for_interaction', 'cancelling')",
                    )?;
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
                        session.status.active_activation_id = None;
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
