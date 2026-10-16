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
        mut req: CreateActivationRequest,
        events_out: &mut Vec<SessionEventEnvelope>,
    ) -> Result<Activation, StoreError> {
        let mut session_id = req.spec.session_id.clone();
        let mut session = Self::load_session_in_tx(tx, &session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

        // If `session` was compacted into a continuation session chain, automatically follow to the active head session
        let mut follow_hops = 0usize;
        while session.status.phase == SessionPhase::Closed {
            if let Some(ref next_sid) = session.status.compaction.continued_in_session_id {
                if follow_hops >= 32 {
                    break;
                }
                follow_hops += 1;
                let next_sess = Self::load_session_in_tx(tx, next_sid)?
                    .ok_or_else(|| StoreError::NotFound(format!("session {next_sid}")))?;
                session_id = next_sid.clone();
                req.spec.session_id = session_id.clone();
                session = next_sess;
            } else {
                break;
            }
        }

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
        let item_count = input_items.len() as u64;
        if !input_items.is_empty() {
            let first_seq = LocalItemSeq::new(session.status.local_item_count);
            let batch = ItemBatch {
                session_id: session_id.clone(),
                batch_seq,
                first_local_item_seq: first_seq,
                kind: ItemBatchKind::Input,
                activation_id: activation_id.clone(),
                turn_id: None,
                response_id: None,
                created_at: Utc::now(),
                items: input_items.clone(),
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
            session.status.batch_count += 1;
            session.status.local_item_count += item_count;
            session.status.effective_context_item_count += item_count;
        }

        let ev = Self::append_event_in_tx(
            tx,
            &session_id,
            Some(activation_id),
            None,
            (!input_items.is_empty()).then_some(batch_seq),
            SessionEvent::ActivationSubmitted {
                input_batch_seq: batch_seq,
                items: input_items,
            },
        )?;
        events_out.push(ev.clone());

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

            let committed_items = items.clone();
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
                    items: committed_items,
                },
                BatchCompanionEvent::TurnCompleted {
                    response_id: resp_id,
                    usage,
                } => SessionEvent::TurnCompleted {
                    response_id: resp_id,
                    appended_batch_seq,
                    usage,
                    items: committed_items,
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
                    items: committed_items,
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

    pub fn rollover_compacted_session(
        &self,
        req: RolloverCompactedSessionRequest,
    ) -> Result<(RolloverCompactedSessionResponse, Vec<ScheduledDispatch>), StoreError> {
        let mut events_to_publish = Vec::new();
        let dispatches;
        let response = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut from_act = Self::load_activation_in_tx(&tx, &req.from_activation_id)?
                .ok_or_else(|| {
                    StoreError::NotFound(format!("activation {}", req.from_activation_id))
                })?;
            Self::verify_fencing(&from_act, req.lease_epoch)?;

            let mut from_session = Self::load_session_in_tx(&tx, &req.from_session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {}", req.from_session_id)))?;

            let next_gen = from_session.status.compaction.generation + 1;
            let compaction_session_id =
                SessionId::new(format!("sess_compact_{}", uuid::Uuid::now_v7()));
            let continued_session_id = SessionId::generate();

            // 1. Create the intermediate CompactionWorker session (`compaction_session`)
            let compact_rv = Self::next_resource_version(&tx)?;
            let mut compact_meta = ObjectMeta::new(compaction_session_id.as_str(), compact_rv);
            compact_meta.labels = from_session.metadata.labels.clone();
            compact_meta
                .labels
                .insert("ragents.role".to_string(), "compaction_worker".to_string());

            let compact_spec = SessionSpec {
                scheduling: from_session.spec.scheduling.clone(),
                sources: vec![SessionSource {
                    kind: SessionSourceKind::SummaryOf,
                    session_id: req.from_session_id.clone(),
                    from_context_pos: ContextPos::new(0),
                    through_context_pos: req.summarized_through_pos,
                }],
                producer: Some(ProducerRef {
                    session_id: req.from_session_id.clone(),
                    activation_id: req.from_activation_id.clone(),
                    turn_id: req.turn_id.clone(),
                    call_id: None,
                    command_id: CommandId::generate(),
                }),
                desired_phase: DesiredSessionPhase::Closed,
                system_prompt: None,
                model: from_session.spec.model.clone(),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            };

            let mut compact_status =
                SessionStatus::initial(from_session.status.bound_node_id.clone(), 0);
            compact_status.phase = SessionPhase::Closed;
            compact_status.compaction = SessionCompactionInfo {
                role: SessionRole::CompactionWorker,
                generation: next_gen,
                compacted_from_session_id: Some(req.from_session_id.clone()),
                compaction_session_id: None,
                continued_in_session_id: Some(continued_session_id.clone()),
            };

            let scheduling_mode_str = if from_session.spec.scheduling.is_pinned() {
                "pinned"
            } else {
                "floating"
            };

            let mut compaction_session = Session {
                metadata: compact_meta,
                spec: compact_spec,
                status: compact_status,
            };

            tx.execute(
                "INSERT INTO sessions (
                    session_id, generation, resource_version, scheduling_mode, bound_node_id,
                    created_at, updated_at, labels_json, spec_json, status_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    compaction_session_id.as_str(),
                    compaction_session.metadata.generation.value() as i64,
                    compaction_session.metadata.resource_version.value() as i64,
                    scheduling_mode_str,
                    compaction_session
                        .status
                        .bound_node_id
                        .as_ref()
                        .map(|n| n.as_str()),
                    compaction_session.metadata.created_at.to_rfc3339(),
                    compaction_session.metadata.updated_at.to_rfc3339(),
                    serde_json::to_string(&compaction_session.metadata.labels)?,
                    serde_json::to_string(&compaction_session.spec)?,
                    serde_json::to_string(&compaction_session.status)?,
                ],
            )?;

            tx.execute(
                "INSERT INTO session_sources (
                    child_session_id, source_order, source_kind, source_session_id,
                    from_context_pos, through_context_pos
                ) VALUES (?1, 0, 'summary_of', ?2, 0, ?3)",
                params![
                    compaction_session_id.as_str(),
                    req.from_session_id.as_str(),
                    req.summarized_through_pos.value() as i64,
                ],
            )?;

            // Record the compaction worker's synthetic activation & 2 batches (0: prompt input, 1: summary output)
            let compact_act_id = ActivationId::new(format!("act_compact_{}", uuid::Uuid::now_v7()));
            let compact_act_rv = Self::next_resource_version(&tx)?;
            let compact_act = Activation {
                metadata: ObjectMeta::new(compact_act_id.as_str(), compact_act_rv),
                spec: ActivationSpec {
                    session_id: compaction_session_id.clone(),
                    submission_kind: SubmissionKind::Normal,
                    input: vec![Item::user_message(req.compaction_prompt.clone())],
                    model_override: None,
                    desired_phase: DesiredActivationPhase::Running,
                    interaction_responses: BTreeMap::new(),
                },
                status: ActivationStatus {
                    phase: ActivationPhase::Succeeded,
                    assigned_node: from_act.status.assigned_node.clone(),
                    lease_epoch: LeaseEpoch::new(1),
                    effective_config_ref: from_act.status.effective_config_ref.clone(),
                    effective_permission_ref: from_act.status.effective_permission_ref.clone(),
                    pending_interactions: vec![],
                    usage: req.compaction_usage.clone(),
                    error: None,
                    started_at: Some(Utc::now()),
                    finished_at: Some(Utc::now()),
                },
            };
            tx.execute(
                "INSERT INTO activations (
                    activation_id, session_id, generation, resource_version, phase,
                    assigned_node, lease_epoch, created_at, updated_at, spec_json, status_json
                ) VALUES (?1, ?2, ?3, ?4, 'succeeded', ?5, 1, ?6, ?7, ?8, ?9)",
                params![
                    compact_act_id.as_str(),
                    compaction_session_id.as_str(),
                    compact_act.metadata.generation.value() as i64,
                    compact_act.metadata.resource_version.value() as i64,
                    compact_act
                        .status
                        .assigned_node
                        .as_ref()
                        .map(|n| n.as_str()),
                    compact_act.metadata.created_at.to_rfc3339(),
                    compact_act.metadata.updated_at.to_rfc3339(),
                    serde_json::to_string(&compact_act.spec)?,
                    serde_json::to_string(&compact_act.status)?,
                ],
            )?;

            // Note: compaction_session has spec.sources = [], so its local_batches are its effective_items:
            // pos 0 = Input prompt, pos 1 = Summary output!
            compaction_session.spec.sources.clear();
            let compact_batch_0 = ItemBatch {
                session_id: compaction_session_id.clone(),
                batch_seq: BatchSeq::new(0),
                first_local_item_seq: LocalItemSeq::new(0),
                kind: ItemBatchKind::Input,
                activation_id: compact_act_id.clone(),
                turn_id: None,
                response_id: None,
                created_at: Utc::now(),
                items: vec![Item::user_message(req.compaction_prompt)],
            };
            let compact_batch_1 = ItemBatch {
                session_id: compaction_session_id.clone(),
                batch_seq: BatchSeq::new(1),
                first_local_item_seq: LocalItemSeq::new(1),
                kind: ItemBatchKind::ModelOutput,
                activation_id: compact_act_id.clone(),
                turn_id: None,
                response_id: None,
                created_at: Utc::now(),
                items: vec![Item::assistant_message(req.summary_text)],
            };
            for (b_seq, kind_str, b) in [
                (0i64, "input", &compact_batch_0),
                (1i64, "model_output", &compact_batch_1),
            ] {
                tx.execute(
                    "INSERT INTO batches (
                        session_id, batch_seq, first_local_item_seq, last_local_item_seq,
                        kind, activation_id, turn_id, response_id, created_at, payload_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, ?8)",
                    params![
                        compaction_session_id.as_str(),
                        b_seq,
                        b_seq,
                        b_seq,
                        kind_str,
                        compact_act_id.as_str(),
                        b.created_at.to_rfc3339(),
                        serde_json::to_string(b)?,
                    ],
                )?;
            }
            compaction_session.status.batch_count = 2;
            compaction_session.status.local_item_count = 2;
            compaction_session.status.effective_context_item_count = 2;
            Self::save_session_in_tx(&tx, &mut compaction_session)?;

            // 2. Create the new continuation working session (`continued_session`)
            let mut continued_sources = vec![SessionSource {
                kind: SessionSourceKind::SummaryOf,
                session_id: compaction_session_id.clone(),
                from_context_pos: ContextPos::new(1),
                through_context_pos: ContextPos::new(1),
            }];
            let mut inherited_count = 1u64;
            if let (Some(k_from), Some(k_through)) = (req.kept_from_pos, req.kept_through_pos) {
                if k_from.value() <= k_through.value() {
                    continued_sources.push(SessionSource {
                        kind: SessionSourceKind::ContinuedFrom,
                        session_id: req.from_session_id.clone(),
                        from_context_pos: k_from,
                        through_context_pos: k_through,
                    });
                    inherited_count += k_through.value() - k_from.value() + 1;
                }
            }

            let cont_rv = Self::next_resource_version(&tx)?;
            let mut cont_meta = ObjectMeta::new(continued_session_id.as_str(), cont_rv);
            cont_meta.labels = from_session.metadata.labels.clone();
            let cont_spec = SessionSpec {
                scheduling: from_session.spec.scheduling.clone(),
                sources: continued_sources,
                producer: Some(ProducerRef {
                    session_id: req.from_session_id.clone(),
                    activation_id: req.from_activation_id.clone(),
                    turn_id: req.turn_id.clone(),
                    call_id: None,
                    command_id: CommandId::generate(),
                }),
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: from_session.spec.system_prompt.clone(),
                model: from_session.spec.model.clone(),
                extensions: from_session.spec.extensions.clone(),
                permissions: from_session.spec.permissions.clone(),
            };
            let mut cont_status =
                SessionStatus::initial(from_session.status.bound_node_id.clone(), inherited_count);
            cont_status.compaction = SessionCompactionInfo {
                role: SessionRole::Working,
                generation: next_gen,
                compacted_from_session_id: Some(req.from_session_id.clone()),
                compaction_session_id: Some(compaction_session_id.clone()),
                continued_in_session_id: None,
            };

            let mut continued_session = Session {
                metadata: cont_meta,
                spec: cont_spec,
                status: cont_status,
            };

            tx.execute(
                "INSERT INTO sessions (
                    session_id, generation, resource_version, scheduling_mode, bound_node_id,
                    created_at, updated_at, labels_json, spec_json, status_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    continued_session_id.as_str(),
                    continued_session.metadata.generation.value() as i64,
                    continued_session.metadata.resource_version.value() as i64,
                    scheduling_mode_str,
                    continued_session
                        .status
                        .bound_node_id
                        .as_ref()
                        .map(|n| n.as_str()),
                    continued_session.metadata.created_at.to_rfc3339(),
                    continued_session.metadata.updated_at.to_rfc3339(),
                    serde_json::to_string(&continued_session.metadata.labels)?,
                    serde_json::to_string(&continued_session.spec)?,
                    serde_json::to_string(&continued_session.status)?,
                ],
            )?;

            for (order, src) in continued_session.spec.sources.iter().enumerate() {
                let kind_str = match src.kind {
                    SessionSourceKind::ForkedFrom => "forked_from",
                    SessionSourceKind::DerivedFrom => "derived_from",
                    SessionSourceKind::SummaryOf => "summary_of",
                    SessionSourceKind::ContinuedFrom => "continued_from",
                };
                tx.execute(
                    "INSERT INTO session_sources (
                        child_session_id, source_order, source_kind, source_session_id,
                        from_context_pos, through_context_pos
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        continued_session_id.as_str(),
                        order as i64,
                        kind_str,
                        src.session_id.as_str(),
                        src.from_context_pos.value() as i64,
                        src.through_context_pos.value() as i64,
                    ],
                )?;
            }

            let cont_created_ev = Self::append_event_in_tx(
                &tx,
                &continued_session_id,
                None,
                None,
                None,
                SessionEvent::SessionCreated,
            )?;
            continued_session.status.event_count = cont_created_ev.event_seq.value() + 1;
            Self::save_session_in_tx(&tx, &mut continued_session)?;
            events_to_publish.push(cont_created_ev);

            // Migrate any unconsumed realtime messages from `from_session` to `continued_session`
            tx.execute(
                "UPDATE realtime_messages SET session_id = ?1 WHERE session_id = ?2",
                params![continued_session_id.as_str(), req.from_session_id.as_str()],
            )?;

            // 3. Complete `from_activation` and close `from_session`, linking it to `compaction_session` & `continued_session`
            if let Some(ref node_id) = from_act.status.assigned_node {
                if let Some(mut node) = Self::load_node_in_tx(&tx, node_id)? {
                    node.status.active_runners = node.status.active_runners.saturating_sub(1);
                    tx.execute(
                        "UPDATE nodes SET status_json = ?1 WHERE node_id = ?2",
                        params![serde_json::to_string(&node.status)?, node_id.as_str()],
                    )?;
                }
            }
            from_act.status.phase = ActivationPhase::Succeeded;
            from_act.status.finished_at = Some(Utc::now());
            from_act.status.usage = req.compaction_usage;
            Self::save_activation_in_tx(&tx, &mut from_act)?;

            from_session.status.active_activation_id = None;
            from_session.spec.desired_phase = DesiredSessionPhase::Closed;
            from_session.status.phase = SessionPhase::Closed;
            from_session.status.compaction.compaction_session_id =
                Some(compaction_session_id.clone());
            from_session.status.compaction.continued_in_session_id =
                Some(continued_session_id.clone());

            // 4. Create the continuation Activation on `continued_session`
            let cont_act = Self::create_activation_in_tx(
                &tx,
                CreateActivationRequest {
                    activation_id: None,
                    labels: BTreeMap::new(),
                    spec: ActivationSpec {
                        session_id: continued_session_id.clone(),
                        submission_kind: SubmissionKind::Continuation,
                        input: vec![],
                        model_override: from_act.spec.model_override.clone(),
                        desired_phase: DesiredActivationPhase::Running,
                        interaction_responses: BTreeMap::new(),
                    },
                },
                &mut events_to_publish,
            )?;
            let cont_act_id = ActivationId::new(cont_act.metadata.id.clone());

            let compacted_ev = Self::append_event_in_tx(
                &tx,
                &req.from_session_id,
                Some(req.from_activation_id.clone()),
                req.turn_id,
                None,
                SessionEvent::SessionCompacted {
                    from_session_id: req.from_session_id.clone(),
                    compaction_session_id: compaction_session_id.clone(),
                    continued_session_id: continued_session_id.clone(),
                    continued_activation_id: cont_act_id.clone(),
                    compaction_generation: next_gen,
                    summarized_through_pos: req.summarized_through_pos,
                    kept_from_pos: req.kept_from_pos,
                    kept_through_pos: req.kept_through_pos,
                    tokens_before: req.tokens_before,
                },
            )?;
            from_session.status.event_count = compacted_ev.event_seq.value() + 1;
            Self::save_session_in_tx(&tx, &mut from_session)?;
            events_to_publish.push(compacted_ev);

            tx.execute(
                "INSERT INTO session_compactions (
                    from_session_id, compaction_session_id, continued_session_id,
                    compaction_generation, summarized_through_pos, kept_from_pos,
                    kept_through_pos, tokens_before, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    req.from_session_id.as_str(),
                    compaction_session_id.as_str(),
                    continued_session_id.as_str(),
                    next_gen as i64,
                    req.summarized_through_pos.value() as i64,
                    req.kept_from_pos.map(|p| p.value() as i64),
                    req.kept_through_pos.map(|p| p.value() as i64),
                    req.tokens_before as i64,
                    Utc::now().to_rfc3339(),
                ],
            )?;

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            continued_session = Self::load_session_in_tx(&tx, &continued_session_id)?.unwrap();
            let updated_cont_act =
                Self::load_activation_in_tx(&tx, &cont_act_id)?.unwrap_or(cont_act);
            tx.commit()?;

            RolloverCompactedSessionResponse {
                from_session,
                compaction_session,
                continued_session,
                continued_activation: updated_cont_act,
            }
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((response, dispatches))
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
