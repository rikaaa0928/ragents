use super::{PatchActivationOutcome, ReconcileNotice, ScheduledDispatch, SqliteStore, StoreError};
use chrono::{DateTime, Utc};
use openresponses_rust::Item;
use ragents_protocol::*;
use rusqlite::params;
use std::collections::BTreeMap;

impl SqliteStore {
    pub fn create_activation(
        &self,
        req: CreateActivationRequest,
    ) -> Result<(Activation, Vec<ScheduledDispatch>), StoreError> {
        req.spec.validate()?;
        let mut events_to_publish = Vec::new();
        let dispatches;
        let activation = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let act = Self::create_activation_in_tx(&tx, req, &mut events_to_publish)?;
            let act_id = ActivationId::new(act.metadata.id.clone());
            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            let updated_act = Self::load_activation_in_tx(&tx, &act_id)?.unwrap_or(act);
            tx.commit()?;
            updated_act
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((activation, dispatches))
    }

    pub(crate) fn create_activation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        req: CreateActivationRequest,
        events_out: &mut Vec<SessionEventEnvelope>,
    ) -> Result<Activation, StoreError> {
        let session_id = req.spec.session_id.clone();
        let mut session = Self::load_session_in_tx(tx, &session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

        if session.status.phase != SessionPhase::Open {
            return Err(StoreError::Conflict(format!(
                "session {session_id} is not open (phase: {:?})",
                session.status.phase
            )));
        }

        if let Some(ref model_override) = req.spec.model_override {
            model_override.validate()?;
            if let SchedulingMode::Pinned { ref node_id, .. } = session.spec.scheduling {
                if let Some(node) = Self::load_node_in_tx(tx, node_id)? {
                    if !node.status.supported_models.contains(&model_override.name) {
                        return Err(StoreError::UnsupportedModelOnNode {
                            node_id: node_id.to_string(),
                            model: model_override.name.clone(),
                            supported: node.status.supported_models,
                        });
                    }
                }
            }
        }

        let activation_id = req.activation_id.unwrap_or_else(ActivationId::generate);
        let rv = Self::next_resource_version(tx)?;
        let mut meta = ObjectMeta::new(activation_id.as_str(), rv);
        meta.labels = req.labels;

        let input_items = req.spec.input.clone();
        let status = ActivationStatus::initial_queued();
        let activation = Activation {
            metadata: meta,
            spec: req.spec,
            status,
        };

        tx.execute(
            "INSERT INTO activations (
                activation_id, session_id, generation, resource_version, phase,
                assigned_node, lease_epoch, created_at, updated_at, spec_json, status_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                activation_id.as_str(),
                session_id.as_str(),
                activation.metadata.generation.value() as i64,
                activation.metadata.resource_version.value() as i64,
                "queued",
                Option::<String>::None,
                0i64,
                activation.metadata.created_at.to_rfc3339(),
                activation.metadata.updated_at.to_rfc3339(),
                serde_json::to_string(&activation.spec)?,
                serde_json::to_string(&activation.status)?,
            ],
        )?;

        let batch_seq = BatchSeq::new(session.status.batch_count);
        let first_seq = LocalItemSeq::new(session.status.local_item_count);
        let item_count = input_items.len() as u64;
        let batch = ItemBatch {
            session_id: session_id.clone(),
            batch_seq,
            first_local_item_seq: first_seq,
            kind: ItemBatchKind::Input,
            activation_id: activation_id.clone(),
            turn_id: None,
            response_id: None,
            created_at: Utc::now(),
            items: input_items,
        };

        tx.execute(
            "INSERT INTO batches (
                session_id, batch_seq, first_local_item_seq, last_local_item_seq,
                kind, activation_id, turn_id, response_id, created_at, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                session_id.as_str(),
                batch_seq.value() as i64,
                first_seq.value() as i64,
                batch.last_local_item_seq().value() as i64,
                "input",
                activation_id.as_str(),
                Option::<String>::None,
                Option::<String>::None,
                batch.created_at.to_rfc3339(),
                serde_json::to_string(&batch)?,
            ],
        )?;

        let ev = Self::append_event_in_tx(
            tx,
            &session_id,
            Some(activation_id),
            None,
            Some(batch_seq),
            SessionEvent::ActivationSubmitted {
                input_batch_seq: batch_seq,
            },
        )?;
        events_out.push(ev.clone());

        session.status.batch_count += 1;
        session.status.local_item_count += item_count;
        session.status.effective_context_item_count += item_count;
        session.status.queued_activation_count += 1;
        session.status.event_count = ev.event_seq.value() + 1;
        Self::save_session_in_tx(tx, &mut session)?;

        Ok(activation)
    }

    pub fn accept_activation(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        effective_config: ConfigRevision,
        effective_permission: PermissionSnapshot,
    ) -> Result<Activation, StoreError> {
        let mut events_to_publish = Vec::new();
        let activation = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            tx.execute(
                "INSERT OR IGNORE INTO configs (config_ref, created_at, payload_json) VALUES (?1, ?2, ?3)",
                params![
                    effective_config.config_ref.as_str(),
                    effective_config.created_at.to_rfc3339(),
                    serde_json::to_string(&effective_config)?,
                ],
            )?;

            tx.execute(
                "INSERT OR IGNORE INTO permission_snapshots (permission_ref, created_at, payload_json) VALUES (?1, ?2, ?3)",
                params![
                    effective_permission.permission_snapshot_ref.as_str(),
                    effective_permission.created_at.to_rfc3339(),
                    serde_json::to_string(&effective_permission)?,
                ],
            )?;

            let node_id = act
                .status
                .assigned_node
                .clone()
                .unwrap_or_else(|| NodeId::new("unknown"));

            act.status.phase = ActivationPhase::Running;
            act.status.effective_config_ref = Some(effective_config.config_ref.clone());
            act.status.effective_permission_ref =
                Some(effective_permission.permission_snapshot_ref.clone());
            act.status.started_at = Some(Utc::now());

            let ev = Self::append_event_in_tx(
                &tx,
                session_id,
                Some(activation_id.clone()),
                None,
                None,
                SessionEvent::ActivationStarted {
                    node_id,
                    config_ref: effective_config.config_ref,
                    permission_snapshot_ref: effective_permission.permission_snapshot_ref,
                },
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);

            Self::save_activation_in_tx(&tx, &mut act)?;
            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;
            act
        };

        self.publish_events(events_to_publish);
        Ok(activation)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_batch(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        kind: ItemBatchKind,
        turn_id: Option<TurnId>,
        response_id: Option<String>,
        items: Vec<Item>,
        companion: BatchCompanionEvent,
    ) -> Result<(BatchSeq, ResourceVersion), StoreError> {
        let mut events_to_publish = Vec::new();
        let (batch_seq, rv) = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            let appended_batch_seq = if !items.is_empty() {
                let b_seq = BatchSeq::new(session.status.batch_count);
                let first_seq = LocalItemSeq::new(session.status.local_item_count);
                let item_count = items.len() as u64;
                let batch = ItemBatch {
                    session_id: session_id.clone(),
                    batch_seq: b_seq,
                    first_local_item_seq: first_seq,
                    kind,
                    activation_id: activation_id.clone(),
                    turn_id: turn_id.clone(),
                    response_id: response_id.clone(),
                    created_at: Utc::now(),
                    items,
                };
                let kind_str = match kind {
                    ItemBatchKind::Input => "input",
                    ItemBatchKind::ModelOutput => "model_output",
                    ItemBatchKind::ToolOutput => "tool_output",
                };
                tx.execute(
                    "INSERT INTO batches (
                        session_id, batch_seq, first_local_item_seq, last_local_item_seq,
                        kind, activation_id, turn_id, response_id, created_at, payload_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        session_id.as_str(),
                        b_seq.value() as i64,
                        first_seq.value() as i64,
                        batch.last_local_item_seq().value() as i64,
                        kind_str,
                        activation_id.as_str(),
                        turn_id.as_ref().map(|t| t.as_str()),
                        response_id.as_deref(),
                        batch.created_at.to_rfc3339(),
                        serde_json::to_string(&batch)?,
                    ],
                )?;
                session.status.batch_count += 1;
                session.status.local_item_count += item_count;
                session.status.effective_context_item_count += item_count;
                Some(b_seq)
            } else {
                None
            };

            let session_event = match companion {
                BatchCompanionEvent::InputSubmitted => SessionEvent::ActivationSubmitted {
                    input_batch_seq: appended_batch_seq.unwrap_or(BatchSeq::new(0)),
                },
                BatchCompanionEvent::TurnCompleted {
                    response_id: resp_id,
                    usage,
                } => SessionEvent::TurnCompleted {
                    response_id: resp_id,
                    appended_batch_seq,
                    usage,
                },
                BatchCompanionEvent::ToolCallFinished {
                    call_id,
                    name,
                    success,
                    duration_ms,
                } => SessionEvent::ToolCallFinished {
                    call_id,
                    name,
                    success,
                    duration_ms,
                    output_batch_seq: appended_batch_seq,
                },
            };

            let ev = Self::append_event_in_tx(
                &tx,
                session_id,
                Some(activation_id.clone()),
                turn_id,
                appended_batch_seq,
                session_event,
            )?;
            let committed_rv = ev.resource_version;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);

            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;
            (appended_batch_seq.unwrap_or(BatchSeq::new(0)), committed_rv)
        };

        self.publish_events(events_to_publish);
        Ok((batch_seq, rv))
    }

    pub fn emit_daemon_event(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        event: SessionEvent,
    ) -> Result<SessionEventEnvelope, StoreError> {
        if event.is_ephemeral_stream_delta() {
            let act = self
                .get_activation(activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;
            let envelope = SessionEventEnvelope {
                resource_version: ResourceVersion::new(0),
                event_seq: EventSeq::new(0),
                event_id: EventId::generate(),
                session_id: session_id.clone(),
                activation_id: Some(activation_id.clone()),
                turn_id,
                created_at: Utc::now(),
                event,
            };
            self.publish_events(vec![envelope.clone()]);
            return Ok(envelope);
        }

        let mut events_to_publish = Vec::new();
        let envelope = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            if let SessionEvent::PermissionRequested { ref request } = event {
                act.status.phase = ActivationPhase::WaitingForInteraction;
                act.status.pending_interactions.push(request.clone());
                Self::save_activation_in_tx(&tx, &mut act)?;
            }

            let ev = Self::append_event_in_tx(
                &tx,
                session_id,
                Some(activation_id.clone()),
                turn_id,
                None,
                event,
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            Self::save_session_in_tx(&tx, &mut session)?;
            events_to_publish.push(ev.clone());
            tx.commit()?;
            ev
        };

        self.publish_events(events_to_publish);
        Ok(envelope)
    }

    pub fn complete_activation(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
        outcome: ActivationTerminalOutcome,
    ) -> Result<(Activation, Vec<ScheduledDispatch>), StoreError> {
        let mut events_to_publish = Vec::new();
        let dispatches;
        let activation = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            if let Some(ref node_id) = act.status.assigned_node {
                if let Some(mut node) = Self::load_node_in_tx(&tx, node_id)? {
                    node.status.active_runners = node.status.active_runners.saturating_sub(1);
                    tx.execute(
                        "UPDATE nodes SET status_json = ?1 WHERE node_id = ?2",
                        params![serde_json::to_string(&node.status)?, node_id.as_str()],
                    )?;
                }
            }

            act.status.finished_at = Some(Utc::now());
            act.status.pending_interactions.clear();

            let terminal_event = match outcome {
                ActivationTerminalOutcome::Succeeded { usage } => {
                    act.status.phase = ActivationPhase::Succeeded;
                    act.status.usage = usage.clone();
                    SessionEvent::ActivationCompleted { usage }
                }
                ActivationTerminalOutcome::Failed { error } => {
                    act.status.phase = ActivationPhase::Failed;
                    act.status.error = Some(error.clone());
                    session.status.last_error = Some(error.clone());
                    SessionEvent::ActivationFailed { error }
                }
                ActivationTerminalOutcome::Cancelled => {
                    act.status.phase = ActivationPhase::Cancelled;
                    SessionEvent::ActivationCancelled
                }
            };

            if session.status.active_activation_id.as_ref() == Some(activation_id) {
                session.status.active_activation_id = None;
            }

            let ev = Self::append_event_in_tx(
                &tx,
                session_id,
                Some(activation_id.clone()),
                turn_id,
                None,
                terminal_event,
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);

            Self::save_activation_in_tx(&tx, &mut act)?;
            Self::save_session_in_tx(&tx, &mut session)?;

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            tx.commit()?;
            act
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((activation, dispatches))
    }

    pub fn patch_activation(
        &self,
        activation_id: &ActivationId,
        patch: PatchActivationSpecRequest,
    ) -> Result<PatchActivationOutcome, StoreError> {
        let mut events_to_publish = Vec::new();
        let outcome = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            let sess_id = act.spec.session_id.clone();
            let mut session = Self::load_session_in_tx(&tx, &sess_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {sess_id}")))?;

            let mut should_notify_daemon = false;

            for (req_id, res_spec) in patch.interaction_responses {
                if act
                    .status
                    .pending_interactions
                    .iter()
                    .any(|p| p.id == req_id)
                {
                    act.status.pending_interactions.retain(|p| p.id != req_id);
                    act.spec
                        .interaction_responses
                        .insert(req_id.clone(), res_spec.clone());
                    let snap =
                        PermissionSnapshot::from_effective_policy(session.spec.permissions.clone());
                    let ev = Self::append_event_in_tx(
                        &tx,
                        &sess_id,
                        Some(activation_id.clone()),
                        None,
                        None,
                        SessionEvent::PermissionResolved {
                            request_id: req_id,
                            decision: res_spec.decision,
                            override_path: res_spec.override_path.clone(),
                            resulting_snapshot_ref: snap.permission_snapshot_ref,
                        },
                    )?;
                    session.status.event_count = ev.event_seq.value() + 1;
                    events_to_publish.push(ev);
                    should_notify_daemon = true;
                }
            }

            if act.status.phase == ActivationPhase::WaitingForInteraction
                && act.status.pending_interactions.is_empty()
            {
                act.status.phase = ActivationPhase::Running;
            }

            let mut newly_scheduled = Vec::new();
            if let Some(DesiredActivationPhase::Cancelled) = patch.desired_phase {
                act.spec.desired_phase = DesiredActivationPhase::Cancelled;
                if act.status.phase == ActivationPhase::Queued {
                    act.status.phase = ActivationPhase::Cancelled;
                    act.status.finished_at = Some(Utc::now());
                    session.status.queued_activation_count =
                        session.status.queued_activation_count.saturating_sub(1);
                    let ev = Self::append_event_in_tx(
                        &tx,
                        &sess_id,
                        Some(activation_id.clone()),
                        None,
                        None,
                        SessionEvent::ActivationCancelled,
                    )?;
                    session.status.event_count = ev.event_seq.value() + 1;
                    events_to_publish.push(ev);
                } else if act.status.phase.is_active()
                    && act.status.phase != ActivationPhase::Cancelling
                {
                    act.status.phase = ActivationPhase::Cancelling;
                    let ev = Self::append_event_in_tx(
                        &tx,
                        &sess_id,
                        Some(activation_id.clone()),
                        None,
                        None,
                        SessionEvent::ActivationCancellationRequested,
                    )?;
                    session.status.event_count = ev.event_seq.value() + 1;
                    events_to_publish.push(ev);
                    should_notify_daemon = true;
                }
            }

            act.metadata.generation = act.metadata.generation.next();
            Self::save_activation_in_tx(&tx, &mut act)?;
            Self::save_session_in_tx(&tx, &mut session)?;

            if act.status.phase == ActivationPhase::Cancelled {
                newly_scheduled = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            }

            let reconcile_notice = if should_notify_daemon {
                act.status
                    .assigned_node
                    .clone()
                    .map(|node_id| ReconcileNotice {
                        node_id,
                        session_id: sess_id,
                        activation_id: activation_id.clone(),
                        lease_epoch: act.status.lease_epoch,
                        desired_phase: act.spec.desired_phase,
                        interaction_responses: act.spec.interaction_responses.clone(),
                    })
            } else {
                None
            };

            tx.commit()?;
            PatchActivationOutcome {
                activation: act,
                reconcile_notice,
                newly_scheduled,
            }
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(outcome.newly_scheduled.clone());
        if let Some(ref notice) = outcome.reconcile_notice {
            let _ = self.reconcile_tx.send(notice.clone());
        }
        Ok(outcome)
    }

    pub fn get_activation(
        &self,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Self::load_activation_from_conn(&conn, activation_id)
    }

    pub fn list_session_activations(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Activation>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT activation_id, generation, resource_version, created_at, updated_at, spec_json, status_json
             FROM activations WHERE session_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![session_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        let mut list = Vec::new();
        for r in rows {
            let (id, gen, rv, created_str, updated_str, spec_json, status_json) = r?;
            let spec: ActivationSpec = serde_json::from_str(&spec_json)?;
            let status: ActivationStatus = serde_json::from_str(&status_json)?;
            list.push(Activation {
                metadata: ObjectMeta {
                    id,
                    generation: Generation::new(gen as u64),
                    resource_version: ResourceVersion::new(rv as u64),
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    updated_at: DateTime::parse_from_rfc3339(&updated_str)
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    labels: BTreeMap::new(),
                },
                spec,
                status,
            });
        }
        Ok(list)
    }
}
