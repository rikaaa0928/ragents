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
                SchedulingMode::Floating { .. } => req
                    .labels
                    .get("ragents.io/preferred-node")
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| NodeId::new(s.trim())),
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
        Self::list_sessions_from_conn(&conn)
    }

    pub(crate) fn list_sessions_from_conn(conn: &Connection) -> Result<Vec<Session>, StoreError> {
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
            if let Some(extra_tmp) = patch.extra_tmp_dirs {
                session.spec.extra_tmp_dirs = extra_tmp;
            }
            if let Some(bound_opt) = patch.bound_node_id {
                if session.spec.scheduling.is_floating() {
                    session.status.bound_node_id = bound_opt;
                }
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
        self.read_effective_context_with_options(session_id, ContentFetchMode::Full, true)
    }

    pub fn read_effective_context_with_options(
        &self,
        session_id: &SessionId,
        mode: ContentFetchMode,
        include_batches: bool,
    ) -> Result<SessionContextResponse, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let session = Self::load_session_from_conn(&conn, session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;
        let effective_items =
            Self::resolve_effective_items_with_mode(&conn, session_id, mode)?;
        let local_batches = if include_batches {
            Self::read_local_batches_with_mode(&conn, session_id, mode)?
        } else {
            Vec::new()
        };
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
        Self::resolve_effective_items_with_mode(conn, session_id, ContentFetchMode::Full)
    }

    pub(crate) fn resolve_effective_items_with_mode(
        conn: &Connection,
        session_id: &SessionId,
        mode: ContentFetchMode,
    ) -> Result<Vec<Item>, StoreError> {
        let session = Self::load_session_from_conn(conn, session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;
        let mut effective = Vec::new();
        for src in &session.spec.sources {
            let parent_items = Self::resolve_effective_items_with_mode(conn, &src.session_id, mode)?;
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
        let local_batches = Self::read_local_batches_with_mode(conn, session_id, mode)?;
        for batch in local_batches {
            effective.extend(batch.items);
        }
        let last_user_msg_idx = effective
            .iter()
            .rposition(|item| {
                matches!(
                    item,
                    Item::Message {
                        role: openresponses_rust::MessageRole::User,
                        ..
                    }
                )
            })
            .unwrap_or(0);
        for (idx, item) in effective.iter_mut().enumerate() {
            if idx < last_user_msg_idx {
                if let Item::FunctionCallOutput {
                    output: openresponses_rust::FunctionOutput::Content(parts),
                    ..
                } = item
                {
                    for part in parts.iter_mut() {
                        if let openresponses_rust::MessageContent::InputImage {
                            image_url: Some(ref url),
                            ..
                        } = part
                        {
                            if url.starts_with("data:") {
                                *part = openresponses_rust::MessageContent::InputText {
                                    text: "[Historical image data omitted in subsequent turns to save bandwidth; call view_image again if you need to re-inspect this image.]".to_string(),
                                };
                            }
                        }
                    }
                }
            }
        }
        Ok(effective)
    }

    pub(crate) fn read_local_batches_from_conn(
        conn: &Connection,
        session_id: &SessionId,
    ) -> Result<Vec<ItemBatch>, StoreError> {
        Self::read_local_batches_with_mode(conn, session_id, ContentFetchMode::Full)
    }

    pub(crate) fn read_local_batches_with_mode(
        conn: &Connection,
        session_id: &SessionId,
        mode: ContentFetchMode,
    ) -> Result<Vec<ItemBatch>, StoreError> {
        let mut stmt = conn.prepare(
            "SELECT payload_json FROM batches WHERE session_id = ?1 ORDER BY batch_seq ASC",
        )?;
        let rows = stmt.query_map(params![session_id.as_str()], |r| r.get::<_, String>(0))?;
        let mut batches = Vec::new();
        for r in rows {
            let payload_json = r?;
            let mut batch: ItemBatch = serde_json::from_str(&payload_json)?;
            if mode == ContentFetchMode::View {
                batch.apply_view_mode();
            }
            batches.push(batch);
        }
        Ok(batches)
    }

    pub fn query_workspace_session_history(
        &self,
        caller_session_id: &SessionId,
        req: SessionHistoryQueryRequest,
    ) -> Result<SessionHistoryQueryResponse, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let caller = Self::load_session_from_conn(&conn, caller_session_id)?
            .ok_or_else(|| StoreError::NotFound(format!("session {caller_session_id}")))?;
        let all_sessions = Self::list_sessions_from_conn(&conn)?;

        let (workspace_scope, scoped_sessions) =
            compute_scoped_workspace_sessions(&caller, &all_sessions);

        match req {
            SessionHistoryQueryRequest::List {
                include_current_session,
                include_same_lineage_only,
                limit,
            } => {
                let clamped_limit = limit.clamp(1, 100);
                let mut summaries = Vec::new();
                for (sess, relation) in scoped_sessions {
                    if !include_current_session
                        && relation == WorkspaceSessionRelation::SelfCurrent
                    {
                        continue;
                    }
                    if include_same_lineage_only
                        && relation == WorkspaceSessionRelation::SiblingSession
                    {
                        continue;
                    }
                    let sid = SessionId::new(sess.metadata.id.clone());
                    let items =
                        Self::resolve_effective_items_with_mode(&conn, &sid, ContentFetchMode::View)
                            .unwrap_or_default();
                    let (first_user_preview, latest_activity_preview) =
                        extract_session_previews(&items);
                    summaries.push(WorkspaceSessionSummary {
                        session_id: sid,
                        relation,
                        phase: sess.status.phase,
                        model: sess.spec.model.as_ref().map(|m| m.name.clone()),
                        generation: sess.status.compaction.generation,
                        compacted_from_session_id: sess
                            .status
                            .compaction
                            .compacted_from_session_id
                            .clone(),
                        continued_in_session_id: sess
                            .status
                            .compaction
                            .continued_in_session_id
                            .clone(),
                        created_at: sess.metadata.created_at,
                        updated_at: sess.metadata.updated_at,
                        effective_item_count: items.len() as u64,
                        labels: sess.metadata.labels.clone(),
                        first_user_preview,
                        latest_activity_preview,
                    });
                    if summaries.len() >= clamped_limit {
                        break;
                    }
                }
                Ok(SessionHistoryQueryResponse {
                    ok: true,
                    error: None,
                    workspace_scope: Some(workspace_scope),
                    sessions: summaries,
                    matches: Vec::new(),
                    slice: None,
                })
            }
            SessionHistoryQueryRequest::Search {
                query,
                session_id,
                item_types,
                tool_name,
                include_current_session,
                limit,
            } => {
                let trimmed_query = query.trim();
                if trimmed_query.is_empty() {
                    return Ok(SessionHistoryQueryResponse {
                        ok: false,
                        error: Some("search query must not be empty".into()),
                        workspace_scope: Some(workspace_scope),
                        ..Default::default()
                    });
                }
                let query_lower = trimmed_query.to_lowercase();
                let tokens: Vec<String> = query_lower
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
                let allowed_types: Vec<String> =
                    item_types.iter().map(|t| t.trim().to_lowercase()).collect();
                let filter_tool_lower = tool_name
                    .as_ref()
                    .map(|t| t.trim().to_lowercase())
                    .filter(|t| !t.is_empty());
                let clamped_limit = limit.clamp(1, 50);

                if let Some(ref target_sid) = session_id {
                    if !scoped_sessions
                        .iter()
                        .any(|(s, _)| s.metadata.id == target_sid.as_str())
                    {
                        return Ok(SessionHistoryQueryResponse {
                            ok: false,
                            error: Some(format!(
                                "session {target_sid} does not exist or is outside the current workspace scope ({workspace_scope})"
                            )),
                            workspace_scope: Some(workspace_scope),
                            ..Default::default()
                        });
                    }
                }

                let mut matches = Vec::new();
                'sess_loop: for (sess, relation) in scoped_sessions {
                    let sid = SessionId::new(sess.metadata.id.clone());
                    if let Some(ref target_sid) = session_id {
                        if &sid != target_sid {
                            continue;
                        }
                    } else if !include_current_session
                        && relation == WorkspaceSessionRelation::SelfCurrent
                    {
                        continue;
                    }

                    let items =
                        Self::resolve_effective_items_with_mode(&conn, &sid, ContentFetchMode::View)
                            .unwrap_or_default();
                    let call_tools = build_call_id_tool_map(&items);

                    for (idx, item) in items.iter().enumerate() {
                        let parsed = inspect_item(item, &call_tools);
                        if !allowed_types.is_empty()
                            && !allowed_types.iter().any(|t| {
                                t == &parsed.item_type
                                    || parsed.role.as_ref().is_some_and(|r| t == r)
                            })
                        {
                            continue;
                        }
                        if let Some(ref want_tool) = filter_tool_lower {
                            let matches_tool = parsed
                                .tool_name
                                .as_ref()
                                .is_some_and(|tn| tn.to_lowercase().contains(want_tool));
                            if !matches_tool {
                                continue;
                            }
                        }

                        let combined_lower = match &parsed.tool_name {
                            Some(tn) => format!(
                                "{} {}",
                                tn.to_lowercase(),
                                parsed.content.to_lowercase()
                            ),
                            None => parsed.content.to_lowercase(),
                        };

                        if !tokens.iter().all(|tok| combined_lower.contains(tok)) {
                            continue;
                        }

                        let total_chars = parsed.content.chars().count();
                        let snippet = build_search_snippet(&parsed.content, &query_lower, &tokens, 240);
                        matches.push(WorkspaceSessionSearchMatch {
                            session_id: sid.clone(),
                            relation,
                            generation: sess.status.compaction.generation,
                            labels: sess.metadata.labels.clone(),
                            item_index: idx,
                            item_type: parsed.item_type,
                            role: parsed.role,
                            tool_name: parsed.tool_name,
                            call_id: parsed.call_id,
                            snippet,
                            total_chars,
                        });
                        if matches.len() >= clamped_limit {
                            break 'sess_loop;
                        }
                    }
                }

                Ok(SessionHistoryQueryResponse {
                    ok: true,
                    error: None,
                    workspace_scope: Some(workspace_scope),
                    sessions: Vec::new(),
                    matches,
                    slice: None,
                })
            }
            SessionHistoryQueryRequest::Read {
                session_id,
                from_index,
                around_index,
                context_window,
                limit,
                max_chars_per_item,
                include_reasoning,
            } => {
                let Some((sess, relation)) = scoped_sessions
                    .into_iter()
                    .find(|(s, _)| s.metadata.id == session_id.as_str())
                else {
                    return Ok(SessionHistoryQueryResponse {
                        ok: false,
                        error: Some(format!(
                            "session {session_id} does not exist or is outside the current workspace scope ({workspace_scope})"
                        )),
                        workspace_scope: Some(workspace_scope),
                        ..Default::default()
                    });
                };

                let items = Self::resolve_effective_items_with_mode(
                    &conn,
                    &session_id,
                    ContentFetchMode::View,
                )?;
                let total_items = items.len();
                let call_tools = build_call_id_tool_map(&items);
                let clamped_limit = limit.clamp(1, 50);
                let clamped_max_chars = max_chars_per_item.clamp(200, 30_000);

                if total_items == 0 {
                    return Ok(SessionHistoryQueryResponse {
                        ok: true,
                        error: None,
                        workspace_scope: Some(workspace_scope),
                        sessions: Vec::new(),
                        matches: Vec::new(),
                        slice: Some(WorkspaceSessionSlice {
                            session_id,
                            relation,
                            phase: sess.status.phase,
                            generation: sess.status.compaction.generation,
                            compacted_from_session_id: sess
                                .status
                                .compaction
                                .compacted_from_session_id,
                            continued_in_session_id: sess
                                .status
                                .compaction
                                .continued_in_session_id,
                            labels: sess.metadata.labels,
                            total_items: 0,
                            start_index: 0,
                            end_index: 0,
                            items: Vec::new(),
                        }),
                    });
                }

                let (start_index, end_index) = if let Some(center) = around_index {
                    let center = center.min(total_items - 1);
                    let win = context_window.min(25);
                    let start = center.saturating_sub(win);
                    let end = (center + win).min(total_items - 1);
                    (start, end)
                } else if let Some(from) = from_index {
                    let start = from.min(total_items - 1);
                    let end = (start + clamped_limit - 1).min(total_items - 1);
                    (start, end)
                } else {
                    let end = total_items - 1;
                    let start = total_items.saturating_sub(clamped_limit);
                    (start, end)
                };

                let mut slice_items = Vec::new();
                for (idx, item) in items
                    .iter()
                    .enumerate()
                    .take(end_index + 1)
                    .skip(start_index)
                {
                    let parsed = inspect_item(item, &call_tools);
                    if parsed.item_type == "reasoning"
                        && !include_reasoning
                        && around_index != Some(idx)
                        && !(from_index == Some(idx) && clamped_limit == 1)
                    {
                        continue;
                    }
                    let total_chars = parsed.content.chars().count();
                    let (content, truncated) = truncate_chars(&parsed.content, clamped_max_chars);
                    slice_items.push(WorkspaceSessionSliceItem {
                        item_index: idx,
                        item_type: parsed.item_type,
                        role: parsed.role,
                        tool_name: parsed.tool_name,
                        call_id: parsed.call_id,
                        content,
                        total_chars,
                        truncated,
                    });
                }

                Ok(SessionHistoryQueryResponse {
                    ok: true,
                    error: None,
                    workspace_scope: Some(workspace_scope),
                    sessions: Vec::new(),
                    matches: Vec::new(),
                    slice: Some(WorkspaceSessionSlice {
                        session_id,
                        relation,
                        phase: sess.status.phase,
                        generation: sess.status.compaction.generation,
                        compacted_from_session_id: sess
                            .status
                            .compaction
                            .compacted_from_session_id,
                        continued_in_session_id: sess
                            .status
                            .compaction
                            .continued_in_session_id,
                        labels: sess.metadata.labels,
                        total_items,
                        start_index,
                        end_index,
                        items: slice_items,
                    }),
                })
            }
        }
    }
}

fn normalize_path_key(path: &std::path::Path) -> String {
    let raw = path.to_string_lossy();
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn compute_scoped_workspace_sessions(
    caller: &Session,
    all_sessions: &[Session],
) -> (String, Vec<(Session, WorkspaceSessionRelation)>) {
    use std::collections::{HashMap, HashSet};

    let by_id: HashMap<&str, &Session> = all_sessions
        .iter()
        .map(|s| (s.metadata.id.as_str(), s))
        .collect();

    // 1. Transitive closure of compaction/source lineage from `caller`
    let mut lineage_ids: HashSet<String> = HashSet::new();
    let mut stack = vec![caller.metadata.id.clone()];
    while let Some(curr_id) = stack.pop() {
        if !lineage_ids.insert(curr_id.clone()) {
            continue;
        }
        if let Some(curr) = by_id.get(curr_id.as_str()) {
            if let Some(ref prev) = curr.status.compaction.compacted_from_session_id {
                stack.push(prev.to_string());
            }
            if let Some(ref next) = curr.status.compaction.continued_in_session_id {
                stack.push(next.to_string());
            }
            for src in &curr.spec.sources {
                stack.push(src.session_id.to_string());
            }
        }
        for s in all_sessions {
            if s.status
                .compaction
                .compacted_from_session_id
                .as_ref()
                .is_some_and(|id| id.as_str() == curr_id)
                || s.status
                    .compaction
                    .continued_in_session_id
                    .as_ref()
                    .is_some_and(|id| id.as_str() == curr_id)
                || s.spec
                    .sources
                    .iter()
                    .any(|src| src.session_id.as_str() == curr_id)
            {
                stack.push(s.metadata.id.clone());
            }
        }
    }

    let caller_agent_id = caller
        .metadata
        .labels
        .get("ragents.chat.agent_id")
        .filter(|v| !v.trim().is_empty());
    let caller_room_id = caller
        .metadata
        .labels
        .get("ragents.chat.room_id")
        .filter(|v| !v.trim().is_empty());

    let workspace_scope = match &caller.spec.scheduling {
        SchedulingMode::Pinned {
            node_id,
            workspace_path,
        } => format!("{}:{}", node_id, normalize_path_key(workspace_path)),
        SchedulingMode::Floating { .. } => {
            if let Some(first_dir) = caller.spec.extra_tmp_dirs.first() {
                format!("floating:{}", normalize_path_key(first_dir))
            } else if let Some(room_id) = caller_room_id {
                format!("room:{room_id}")
            } else {
                format!("lineage:{}", caller.metadata.id)
            }
        }
    };

    let mut matched: Vec<(Session, WorkspaceSessionRelation)> = Vec::new();
    for cand in all_sessions {
        if cand.status.compaction.role == SessionRole::CompactionWorker {
            continue;
        }
        let is_self_current = cand.metadata.id == caller.metadata.id;
        let is_lineage = lineage_ids.contains(&cand.metadata.id);
        let is_same_agent = caller_agent_id.is_some_and(|aid| {
            cand.metadata
                .labels
                .get("ragents.chat.agent_id")
                .is_some_and(|cid| cid == aid)
        });
        let is_same_room = caller_room_id.is_some_and(|rid| {
            cand.metadata
                .labels
                .get("ragents.chat.room_id")
                .is_some_and(|crid| crid == rid)
        });

        let is_same_workspace = match (&caller.spec.scheduling, &cand.spec.scheduling) {
            (
                SchedulingMode::Pinned {
                    node_id: n1,
                    workspace_path: p1,
                },
                SchedulingMode::Pinned {
                    node_id: n2,
                    workspace_path: p2,
                },
            ) => n1 == n2 && normalize_path_key(p1) == normalize_path_key(p2),
            (SchedulingMode::Floating { .. }, SchedulingMode::Floating { .. }) => {
                match (
                    caller.spec.extra_tmp_dirs.first(),
                    cand.spec.extra_tmp_dirs.first(),
                ) {
                    (Some(d1), Some(d2)) => normalize_path_key(d1) == normalize_path_key(d2),
                    _ => false,
                }
            }
            _ => false,
        };

        if !(is_self_current || is_lineage || is_same_workspace || is_same_room) {
            continue;
        }

        let relation = if is_self_current {
            WorkspaceSessionRelation::SelfCurrent
        } else if is_lineage || is_same_agent {
            WorkspaceSessionRelation::SelfHistory
        } else {
            WorkspaceSessionRelation::SiblingSession
        };

        matched.push((cand.clone(), relation));
    }

    matched.sort_by(|(a, _), (b, _)| {
        b.metadata
            .updated_at
            .cmp(&a.metadata.updated_at)
            .then_with(|| {
                b.status
                    .compaction
                    .generation
                    .cmp(&a.status.compaction.generation)
            })
            .then_with(|| b.metadata.created_at.cmp(&a.metadata.created_at))
    });

    (workspace_scope, matched)
}

struct InspectedItem {
    item_type: String,
    role: Option<String>,
    tool_name: Option<String>,
    call_id: Option<String>,
    content: String,
}

fn build_call_id_tool_map(items: &[Item]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for item in items {
        if let Item::FunctionCall { call_id, name, .. } = item {
            map.insert(call_id.clone(), name.clone());
        }
    }
    map
}

fn extract_message_contents(parts: &[openresponses_rust::MessageContent]) -> String {
    let mut out = Vec::new();
    for part in parts {
        match part {
            openresponses_rust::MessageContent::InputText { text }
            | openresponses_rust::MessageContent::OutputText { text, .. }
            | openresponses_rust::MessageContent::PlainText { text }
            | openresponses_rust::MessageContent::SummaryText { text } => {
                if !text.trim().is_empty() {
                    out.push(text.as_str());
                }
            }
            openresponses_rust::MessageContent::Refusal { refusal } => {
                if !refusal.trim().is_empty() {
                    out.push(refusal.as_str());
                }
            }
            _ => {}
        }
    }
    out.join("\n")
}

fn inspect_item(
    item: &Item,
    call_tools: &std::collections::HashMap<String, String>,
) -> InspectedItem {
    match item {
        Item::Message { role, content, .. } => {
            let role_str = match role {
                openresponses_rust::MessageRole::User => "user",
                openresponses_rust::MessageRole::Assistant => "assistant",
                openresponses_rust::MessageRole::System => "system",
                openresponses_rust::MessageRole::Developer => "developer",
            };
            InspectedItem {
                item_type: format!("{role_str}_message"),
                role: Some(role_str.to_string()),
                tool_name: None,
                call_id: None,
                content: extract_message_contents(content),
            }
        }
        Item::Reasoning {
            summary, content, ..
        } => {
            let mut text = extract_message_contents(summary);
            if text.trim().is_empty() {
                if let Some(c) = content {
                    text = extract_message_contents(c);
                }
            }
            InspectedItem {
                item_type: "reasoning".to_string(),
                role: Some("assistant".to_string()),
                tool_name: None,
                call_id: None,
                content: text,
            }
        }
        Item::FunctionCall {
            call_id,
            name,
            arguments,
            ..
        } => InspectedItem {
            item_type: "function_call".to_string(),
            role: Some("assistant".to_string()),
            tool_name: Some(name.clone()),
            call_id: Some(call_id.clone()),
            content: arguments.clone(),
        },
        Item::FunctionCallOutput {
            call_id, output, ..
        } => {
            let text = match output {
                openresponses_rust::FunctionOutput::Text(s) => s.clone(),
                openresponses_rust::FunctionOutput::Content(parts) => {
                    extract_message_contents(parts)
                }
            };
            InspectedItem {
                item_type: "function_call_output".to_string(),
                role: Some("tool".to_string()),
                tool_name: call_tools.get(call_id).cloned(),
                call_id: Some(call_id.clone()),
                content: text,
            }
        }
        other => InspectedItem {
            item_type: "extension_item".to_string(),
            role: None,
            tool_name: None,
            call_id: None,
            content: serde_json::to_string(other).unwrap_or_default(),
        },
    }
}

fn single_line_preview(text: &str, max_chars: usize) -> String {
    let collapsed: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let (s, truncated) = truncate_chars(&collapsed, max_chars);
    if truncated {
        format!("{s}...")
    } else {
        s
    }
}

fn extract_session_previews(items: &[Item]) -> (Option<String>, Option<String>) {
    let call_tools = build_call_id_tool_map(items);
    let mut first_user = None;
    let mut fallback_first_user = None;
    for item in items {
        let parsed = inspect_item(item, &call_tools);
        if parsed.item_type == "user_message" && !parsed.content.trim().is_empty() {
            let trimmed = parsed.content.trim();
            if trimmed.starts_with("<context_compaction_summary>")
                || trimmed.starts_with("[Context Compaction Summary]")
            {
                if fallback_first_user.is_none() {
                    fallback_first_user = Some(single_line_preview(trimmed, 140));
                }
                continue;
            }
            first_user = Some(single_line_preview(trimmed, 140));
            break;
        }
    }
    let first_user = first_user.or(fallback_first_user);

    let mut latest = None;
    for item in items.iter().rev() {
        let parsed = inspect_item(item, &call_tools);
        if parsed.item_type == "reasoning" || parsed.content.trim().is_empty() {
            continue;
        }
        let prefix = match parsed.item_type.as_str() {
            "user_message" => "[user] ".to_string(),
            "assistant_message" => "[assistant] ".to_string(),
            "function_call" => format!(
                "[call:{}] ",
                parsed.tool_name.as_deref().unwrap_or("tool")
            ),
            "function_call_output" => format!(
                "[out:{}] ",
                parsed.tool_name.as_deref().unwrap_or("tool")
            ),
            _ => String::new(),
        };
        latest = Some(format!(
            "{prefix}{}",
            single_line_preview(parsed.content.trim(), 130)
        ));
        break;
    }

    (first_user, latest)
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    let mut chars = text.chars();
    let collected: String = chars.by_ref().take(max_chars).collect();
    let has_more = chars.next().is_some();
    (collected, has_more)
}

fn build_search_snippet(
    content: &str,
    query_lower: &str,
    tokens: &[String],
    window_chars: usize,
) -> String {
    let content_chars: Vec<char> = content.chars().collect();
    if content_chars.len() <= window_chars {
        return content.to_string();
    }
    let lower_chars: Vec<char> = content
        .chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect();

    let find_subslice = |needle: &str| -> Option<usize> {
        let n_chars: Vec<char> = needle.chars().collect();
        if n_chars.is_empty() || n_chars.len() > lower_chars.len() {
            return None;
        }
        lower_chars
            .windows(n_chars.len())
            .position(|w| w == n_chars.as_slice())
    };

    let match_pos = find_subslice(query_lower)
        .or_else(|| tokens.iter().find_map(|t| find_subslice(t)))
        .unwrap_or(0);

    let half = window_chars / 2;
    let start = match_pos.saturating_sub(half);
    let end = (start + window_chars).min(content_chars.len());
    let start = end.saturating_sub(window_chars);

    let mut out = String::new();
    if start > 0 {
        out.push_str("...");
    }
    let slice: String = content_chars[start..end].iter().collect();
    out.push_str(&slice);
    if end < content_chars.len() {
        out.push_str("...");
    }
    out
}
