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
                SchedulingMode::Pinned { node_id, .. } => {
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
                tx.execute(
                    "INSERT INTO session_sources (
                        child_session_id, source_order, source_session_id,
                        from_context_pos, through_context_pos
                    ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        session_id.as_str(),
                        order as i64,
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
        let session = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

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
                session.spec.system_prompt = Some(prompt);
            }
            if let Some(exts) = patch.extensions {
                session.spec.extensions = exts;
            }
            if let Some(perms) = patch.permissions {
                session.spec.permissions = perms;
            }
            if let Some(desired_phase) = patch.desired_phase {
                if desired_phase != DesiredSessionPhase::Open
                    && session.status.active_activation_id.is_some()
                {
                    return Err(StoreError::Conflict(format!(
                        "cannot close or archive session {session_id} while an activation is active"
                    )));
                }
                session.spec.desired_phase = desired_phase;
                session.status.phase = match desired_phase {
                    DesiredSessionPhase::Open => SessionPhase::Open,
                    DesiredSessionPhase::Closed => SessionPhase::Closed,
                    DesiredSessionPhase::Archived => SessionPhase::Archived,
                };
            }

            let new_gen = session.metadata.generation.next();
            session.metadata.generation = new_gen;
            session.status.observed_generation = new_gen;

            let ev = Self::append_event_in_tx(
                &tx,
                session_id,
                None,
                None,
                None,
                SessionEvent::SessionSpecUpdated {
                    generation: new_gen,
                },
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);

            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;
            session
        };

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
