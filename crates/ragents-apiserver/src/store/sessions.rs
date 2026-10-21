use super::{ScheduledDispatch, SqliteStore, StoreError};
use chrono::{DateTime, Utc};
use openresponses_rust::Item;
use ragents_protocol::*;
use rusqlite::{params, Connection};
use std::collections::BTreeMap;

impl SqliteStore {
    pub fn create_session(
        &self,
        req: CreateSessionRequest,
    ) -> Result<(CreateSessionResponse, Vec<ScheduledDispatch>), StoreError> {
        req.spec.validate()?;
        let session_id = req.session_id.unwrap_or_else(SessionId::generate);
        if !session_id.is_safe_identifier() {
            return Err(StoreError::Validation(
                ProtocolValidationError::InvalidIdentifier(session_id.to_string()),
            ));
        }
        if let Some(ref init_act) = req.initial_activation {
            if init_act.input.is_empty() {
                return Err(StoreError::Validation(
                    ProtocolValidationError::EmptyActivationInput,
                ));
            }
            if let Some(ref m) = init_act.model_override {
                m.validate()?;
            }
        }

        let mut events_to_publish = Vec::new();
        let dispatches;
        let response = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            if Self::load_session_in_tx(&tx, &session_id)?.is_some() {
                return Err(StoreError::Conflict(format!(
                    "session {session_id} already exists"
                )));
            }

            let bound_node_id = match &req.spec.scheduling {
                SchedulingMode::Pinned {
                    node_id,
                    workspace_path,
                } => {
                    if let Some(node) = Self::load_node_in_tx(&tx, node_id)? {
                        if let Some(ref model_spec) = req.spec.model {
                            if !node.status.supported_models.contains(&model_spec.name) {
                                return Err(StoreError::UnsupportedModelOnNode {
                                    node_id: node_id.to_string(),
                                    model: model_spec.name.clone(),
                                    supported: node.status.supported_models,
                                });
                            }
                        }
                        if !node.status.allowed_workspace_dirs.is_empty()
                            && !is_workspace_path_allowed(
                                workspace_path,
                                &node.status.allowed_workspace_dirs,
                            )
                        {
                            return Err(StoreError::Validation(
                                ProtocolValidationError::DisallowedWorkspacePath {
                                    node_id: node_id.to_string(),
                                    workspace_path: workspace_path.display().to_string(),
                                    allowed: node.status.allowed_workspace_dirs,
                                },
                            ));
                        }
                    }
                    Some(node_id.clone())
                }
                SchedulingMode::Floating { .. } => None,
            };

            let mut inherited_item_count = 0u64;
            for src in &req.spec.sources {
                let parent = Self::load_session_in_tx(&tx, &src.session_id)?.ok_or_else(|| {
                    StoreError::NotFound(format!("source session {}", src.session_id))
                })?;
                if src.through_context_pos.value() >= parent.status.effective_context_item_count {
                    return Err(StoreError::Conflict(format!(
                        "source {} range [{}..={}] exceeds parent effective context count {}",
                        src.session_id,
                        src.from_context_pos.value(),
                        src.through_context_pos.value(),
                        parent.status.effective_context_item_count
                    )));
                }
                let slice_len = src.through_context_pos.value() - src.from_context_pos.value() + 1;
                inherited_item_count += slice_len;
            }

            let rv = Self::next_resource_version(&tx)?;
            let mut meta = ObjectMeta::new(session_id.as_str(), rv);
            meta.labels = req.labels;

            let mut status = SessionStatus::initial(bound_node_id.clone(), inherited_item_count);
            status.phase = match req.spec.desired_phase {
                DesiredSessionPhase::Open => SessionPhase::Open,
                DesiredSessionPhase::Closed => SessionPhase::Closed,
                DesiredSessionPhase::Archived => SessionPhase::Archived,
            };
            let cluster_prompt = Self::load_cluster_system_prompt_in_tx(&tx)?;
            let node_prompt = match &bound_node_id {
                Some(nid) => Self::load_node_in_tx(&tx, nid)?
                    .map(|n| n.status.effective_node_prompt)
                    .unwrap_or_default(),
                None => String::new(),
            };
            status.latest_prompt_parts = Some(PromptParts {
                system_prompt: cluster_prompt,
                node_prompt,
                client_prompt: req.spec.system_prompt.clone().unwrap_or_default(),
                extension_prompts: BTreeMap::new(),
            });

            let scheduling_mode_str = if req.spec.scheduling.is_pinned() {
                "pinned"
            } else {
                "floating"
            };

            let mut session = Session {
                metadata: meta,
                spec: req.spec,
                status,
            };

            tx.execute(
                "INSERT INTO sessions (
                    session_id, generation, resource_version, scheduling_mode, bound_node_id,
                    created_at, updated_at, labels_json, spec_json, status_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    session_id.as_str(),
                    session.metadata.generation.value() as i64,
                    session.metadata.resource_version.value() as i64,
                    scheduling_mode_str,
                    bound_node_id.as_ref().map(|n| n.as_str()),
                    session.metadata.created_at.to_rfc3339(),
                    session.metadata.updated_at.to_rfc3339(),
                    serde_json::to_string(&session.metadata.labels)?,
                    serde_json::to_string(&session.spec)?,
                    serde_json::to_string(&session.status)?,
                ],
            )?;

            for (order, src) in session.spec.sources.iter().enumerate() {
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
                        session_id.as_str(),
                        order as i64,
                        kind_str,
                        src.session_id.as_str(),
                        src.from_context_pos.value() as i64,
                        src.through_context_pos.value() as i64,
                    ],
                )?;
            }

            let created_ev = Self::append_event_in_tx(
                &tx,
                &session_id,
                None,
                None,
                None,
                SessionEvent::SessionCreated,
            )?;
            session.status.event_count = created_ev.event_seq.value() + 1;
            Self::save_session_in_tx(&tx, &mut session)?;
            events_to_publish.push(created_ev);

            let initial_activation = if let Some(init_req) = req.initial_activation {
                let act = Self::create_activation_in_tx(
                    &tx,
                    CreateActivationRequest {
                        activation_id: None,
                        labels: BTreeMap::new(),
                        spec: ActivationSpec {
                            session_id: session_id.clone(),
                            submission_kind: SubmissionKind::Normal,
                            input: init_req.input,
                            model_override: init_req.model_override,
                            disabled_tools: init_req.disabled_tools,
                            desired_phase: DesiredActivationPhase::Running,
                            interaction_responses: BTreeMap::new(),
                        },
                    },
                    &mut events_to_publish,
                )?;
                Some(act)
            } else {
                None
            };

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            session = Self::load_session_in_tx(&tx, &session_id)?.unwrap();
            let updated_initial_act = match initial_activation {
                Some(a) => Self::load_activation_in_tx(&tx, &ActivationId::new(a.metadata.id))?,
                None => None,
            };

            tx.commit()?;
            CreateSessionResponse {
                session,
                initial_activation: updated_initial_act,
            }
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((response, dispatches))
    }

    pub fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Self::load_session_from_conn(&conn, session_id)
    }

    pub fn list_sessions(&self) -> Result<Vec<Session>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT session_id, generation, resource_version, created_at, updated_at, labels_json, spec_json, status_json
             FROM sessions ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut sessions = Vec::new();
        for r in rows {
            let (id, gen, rv, created_str, updated_str, labels_json, spec_json, status_json) = r?;
            let labels: BTreeMap<String, String> =
                serde_json::from_str(&labels_json).unwrap_or_default();
            let spec: SessionSpec = serde_json::from_str(&spec_json)?;
            let status: SessionStatus = serde_json::from_str(&status_json)?;
            sessions.push(Session {
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
                    labels,
                },
                spec,
                status,
            });
        }
        Ok(sessions)
    }

    pub fn patch_session(
        &self,
        session_id: &SessionId,
        patch: PatchSessionSpecRequest,
    ) -> Result<Session, StoreError> {
        let mut events_to_publish = Vec::new();
        let mut reconcile_notices = Vec::new();
        let session = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut target_sid = session_id.clone();
            let mut session = Self::load_session_in_tx(&tx, &target_sid)?
                .ok_or_else(|| StoreError::NotFound(format!("session {target_sid}")))?;

            let mut hops = 0usize;
            while session.status.phase == SessionPhase::Closed {
                if let Some(next_sid) = session.status.compaction.continued_in_session_id.clone() {
                    if hops >= 32 {
                        break;
                    }
                    hops += 1;
                    session = Self::load_session_in_tx(&tx, &next_sid)?
                        .ok_or_else(|| StoreError::NotFound(format!("session {next_sid}")))?;
                    target_sid = next_sid;
                } else {
                    break;
                }
            }

            if let Some(ref model) = patch.model {
                model.validate()?;
                if let SchedulingMode::Pinned { ref node_id, .. } = session.spec.scheduling {
                    if let Some(node) = Self::load_node_in_tx(&tx, node_id)? {
                        if !node.status.supported_models.contains(&model.name) {
                            return Err(StoreError::UnsupportedModelOnNode {
                                node_id: node_id.to_string(),
                                model: model.name.clone(),
                                supported: node.status.supported_models,
                            });
                        }
                    }
                }
                session.spec.model = Some(model.clone());
            }
            if let Some(prompt) = patch.system_prompt {
                if let Some(ref mut parts) = session.status.latest_prompt_parts {
                    parts.client_prompt = prompt.clone();
                } else {
                    let cluster_prompt = Self::load_cluster_system_prompt_in_tx(&tx)?;
                    let node_prompt = match &session.status.bound_node_id {
                        Some(nid) => Self::load_node_in_tx(&tx, nid)?
                            .map(|n| n.status.effective_node_prompt)
                            .unwrap_or_default(),
                        None => String::new(),
                    };
                    session.status.latest_prompt_parts = Some(PromptParts {
                        system_prompt: cluster_prompt,
                        node_prompt,
                        client_prompt: prompt.clone(),
                        extension_prompts: BTreeMap::new(),
                    });
                }
                session.spec.system_prompt = Some(prompt);
            }
            if let Some(exts) = patch.extensions {
                session.spec.extensions = exts;
            }
            if let Some(perms) = patch.permissions {
                session.spec.permissions = perms;
            }

            let mut phase_transition_event: Option<SessionEvent> = None;
            if let Some(desired_phase) = patch.desired_phase {
                if desired_phase != DesiredSessionPhase::Open {
                    // Cancel all Queued activations for this session
                    let queued_ids: Vec<String> = {
                        let mut stmt = tx.prepare(
                            "SELECT activation_id FROM activations WHERE session_id = ?1 AND phase = 'queued'",
                        )?;
                        let rows = stmt.query_map(params![target_sid.as_str()], |r| r.get(0))?;
                        let mut list = Vec::new();
                        for r in rows {
                            list.push(r?);
                        }
                        list
                    };
                    for q_id_str in queued_ids {
                        let q_id = ActivationId::new(q_id_str);
                        if let Some(mut q_act) = Self::load_activation_in_tx(&tx, &q_id)? {
                            q_act.spec.desired_phase = DesiredActivationPhase::Cancelled;
                            q_act.status.phase = ActivationPhase::Cancelled;
                            q_act.status.finished_at = Some(Utc::now());
                            Self::save_activation_in_tx(&tx, &mut q_act)?;
                            let cancel_ev = Self::append_event_in_tx(
                                &tx,
                                &target_sid,
                                Some(q_id),
                                None,
                                None,
                                SessionEvent::ActivationCancelled,
                            )?;
                            session.status.event_count = cancel_ev.event_seq.value() + 1;
                            events_to_publish.push(cancel_ev);
                        }
                    }
                    session.status.queued_activation_count = 0;

                    // If there is an active activation, request cancellation and notify the assigned daemon
                    if let Some(active_id) = session.status.active_activation_id.clone() {
                        if let Some(mut act) = Self::load_activation_in_tx(&tx, &active_id)? {
                            if act.status.phase.is_active() {
                                act.spec.desired_phase = DesiredActivationPhase::Cancelled;
                                act.status.phase = ActivationPhase::Cancelling;
                                Self::save_activation_in_tx(&tx, &mut act)?;
                                let cancel_req_ev = Self::append_event_in_tx(
                                    &tx,
                                    &target_sid,
                                    Some(active_id.clone()),
                                    None,
                                    None,
                                    SessionEvent::ActivationCancellationRequested,
                                )?;
                                session.status.event_count = cancel_req_ev.event_seq.value() + 1;
                                events_to_publish.push(cancel_req_ev);
                                if let Some(ref node_id) = act.status.assigned_node {
                                    reconcile_notices.push(super::ReconcileNotice {
                                        node_id: node_id.clone(),
                                        session_id: target_sid.clone(),
                                        activation_id: active_id,
                                        lease_epoch: act.status.lease_epoch,
                                        desired_phase: DesiredActivationPhase::Cancelled,
                                        interaction_responses: act
                                            .spec
                                            .interaction_responses
                                            .clone(),
                                    });
                                }
                            }
                        }
                        session.status.active_activation_id = None;
                    }
                }
                session.spec.desired_phase = desired_phase;
                let new_phase = match desired_phase {
                    DesiredSessionPhase::Open => SessionPhase::Open,
                    DesiredSessionPhase::Closed => SessionPhase::Closed,
                    DesiredSessionPhase::Archived => SessionPhase::Archived,
                };
                if session.status.phase != new_phase {
                    session.status.phase = new_phase;
                    phase_transition_event = match new_phase {
                        SessionPhase::Closed => Some(SessionEvent::SessionClosed),
                        SessionPhase::Archived => Some(SessionEvent::SessionArchived),
                        SessionPhase::Open | SessionPhase::Corrupted => None,
                    };
                }
            }

            let new_gen = session.metadata.generation.next();
            session.metadata.generation = new_gen;
            session.status.observed_generation = new_gen;

            let ev = Self::append_event_in_tx(
                &tx,
                &target_sid,
                None,
                None,
                None,
                SessionEvent::SessionSpecUpdated {
                    generation: new_gen,
                },
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);

            if let Some(phase_ev_kind) = phase_transition_event {
                let phase_ev =
                    Self::append_event_in_tx(&tx, &target_sid, None, None, None, phase_ev_kind)?;
                session.status.event_count = phase_ev.event_seq.value() + 1;
                events_to_publish.push(phase_ev);
            }

            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;
            session
        };

        for notice in reconcile_notices {
            let _ = self.reconcile_tx.send(notice);
        }
        self.publish_events(events_to_publish);
        Ok(session)
    }

    pub fn read_effective_context(
        &self,
        session_id: &SessionId,
    ) -> Result<SessionContextResponse, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let session = Self::load_session_from_conn(&conn, session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;
        let effective_items = Self::resolve_effective_items_from_conn(&conn, session_id)?;
        let local_batches = Self::read_local_batches_from_conn(&conn, session_id)?;
        Ok(SessionContextResponse {
            session_id: session_id.clone(),
            resource_version: session.metadata.resource_version,
            effective_items,
            local_batches,
        })
    }

    pub(crate) fn resolve_effective_items_from_conn(
        conn: &Connection,
        session_id: &SessionId,
    ) -> Result<Vec<Item>, StoreError> {
        let session = Self::load_session_from_conn(conn, session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;
        let mut effective = Vec::new();
        for src in &session.spec.sources {
            let parent_items = Self::resolve_effective_items_from_conn(conn, &src.session_id)?;
            let from = src.from_context_pos.value() as usize;
            let through = src.through_context_pos.value() as usize;
            if through >= parent_items.len() || from > through {
                return Err(StoreError::Conflict(format!(
                    "corrupted source slice [{}..={}] for parent {}",
                    from, through, src.session_id
                )));
            }
            effective.extend_from_slice(&parent_items[from..=through]);
        }
        let local_batches = Self::read_local_batches_from_conn(conn, session_id)?;
        for batch in local_batches {
            effective.extend(batch.items);
        }
        Ok(effective)
    }

    pub(crate) fn read_local_batches_from_conn(
        conn: &Connection,
        session_id: &SessionId,
    ) -> Result<Vec<ItemBatch>, StoreError> {
        let mut stmt = conn.prepare(
            "SELECT payload_json FROM batches WHERE session_id = ?1 ORDER BY batch_seq ASC",
        )?;
        let rows = stmt.query_map(params![session_id.as_str()], |r| r.get::<_, String>(0))?;
        let mut batches = Vec::new();
        for r in rows {
            let payload_json = r?;
            let batch: ItemBatch = serde_json::from_str(&payload_json)?;
            batches.push(batch);
        }
        Ok(batches)
    }
}
