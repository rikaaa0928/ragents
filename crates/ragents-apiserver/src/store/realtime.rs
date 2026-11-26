use super::{ScheduledDispatch, SqliteStore, StoreError};
use chrono::{DateTime, Utc};
use openresponses_rust::{Item, MessageContent};
use ragents_protocol::*;
use rusqlite::{params, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

impl SqliteStore {
    pub(crate) fn resolve_open_session_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<(SessionId, Session), StoreError> {
        let mut target_sid = session_id.clone();
        let mut session = Self::load_session_in_tx(tx, &target_sid)?
            .ok_or_else(|| StoreError::NotFound(format!("session {target_sid}")))?;
        let mut hops = 0usize;
        while session.status.phase == SessionPhase::Closed {
            if let Some(next_sid) = session.status.compaction.continued_in_session_id.clone() {
                if hops >= 32 {
                    break;
                }
                hops += 1;
                session = Self::load_session_in_tx(tx, &next_sid)?
                    .ok_or_else(|| StoreError::NotFound(format!("session {next_sid}")))?;
                target_sid = next_sid;
            } else {
                break;
            }
        }
        Ok((target_sid, session))
    }

    pub(crate) fn queue_counts_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<(usize, usize), StoreError> {
        let rt: i64 = tx.query_row(
            "SELECT COUNT(*) FROM session_queue_messages WHERE session_id = ?1 AND queue_kind = 'realtime'",
            params![session_id.as_str()],
            |r| r.get(0),
        )?;
        let pd: i64 = tx.query_row(
            "SELECT COUNT(*) FROM session_queue_messages WHERE session_id = ?1 AND queue_kind = 'pending'",
            params![session_id.as_str()],
            |r| r.get(0),
        )?;
        Ok((rt.max(0) as usize, pd.max(0) as usize))
    }

    fn parse_queue_row(
        id: i64,
        session_id: String,
        queue_kind_str: String,
        content_json: String,
        tool_policy_json: String,
        metadata_json: String,
        created_at_str: String,
    ) -> Result<SessionQueueMessage, StoreError> {
        let queue_kind = QueueKind::parse(&queue_kind_str)
            .ok_or_else(|| StoreError::Conflict(format!("invalid queue_kind '{queue_kind_str}'")))?;
        let content: Vec<MessageContent> = serde_json::from_str(&content_json)?;
        let tool_policy: QueueToolPolicy = serde_json::from_str(&tool_policy_json)?;
        let metadata: BTreeMap<String, String> = serde_json::from_str(&metadata_json)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        Ok(SessionQueueMessage {
            id,
            session_id: SessionId::new(session_id),
            queue_kind,
            content,
            tool_policy,
            metadata,
            created_at,
        })
    }

    pub(crate) fn insert_queue_message_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
        queue_kind: QueueKind,
        req: &EnqueueSessionMessageRequest,
    ) -> Result<SessionQueueMessage, StoreError> {
        if req.content.is_empty() {
            return Err(StoreError::Validation(
                ProtocolValidationError::EmptyActivationInput,
            ));
        }
        if let Some(ref model_override) = req.model_override {
            model_override.validate()?;
            if let Some(session) = Self::load_session_in_tx(tx, session_id)? {
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
        }

        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let content_json = serde_json::to_string(&req.content)?;
        let tool_policy_json = serde_json::to_string(&req.tool_policy)?;
        let metadata_json = serde_json::to_string(&req.metadata)?;
        let model_override_json = match &req.model_override {
            Some(m) => Some(serde_json::to_string(m)?),
            None => None,
        };

        tx.execute(
            "INSERT INTO session_queue_messages (
                session_id, queue_kind, content_json, tool_policy_json,
                metadata_json, model_override_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_id.as_str(),
                queue_kind.as_str(),
                content_json,
                tool_policy_json,
                metadata_json,
                model_override_json,
                now_str,
            ],
        )?;
        let id = tx.last_insert_rowid();
        Ok(SessionQueueMessage {
            id,
            session_id: session_id.clone(),
            queue_kind,
            content: req.content.clone(),
            tool_policy: req.tool_policy.clone(),
            metadata: req.metadata.clone(),
            created_at: now,
        })
    }

    pub fn enqueue_session_message(
        &self,
        session_id: &SessionId,
        queue_kind: QueueKind,
        req: EnqueueSessionMessageRequest,
    ) -> Result<(EnqueueSessionMessageResponse, Vec<ScheduledDispatch>), StoreError> {
        let mut events_to_publish = Vec::new();
        let dispatches;
        let response = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let (target_sid, mut session) = Self::resolve_open_session_in_tx(&tx, session_id)?;
            if session.status.phase != SessionPhase::Open {
                return Err(StoreError::Conflict(format!(
                    "session {target_sid} is not open (phase: {:?})",
                    session.status.phase
                )));
            }

            let message = Self::insert_queue_message_in_tx(&tx, &target_sid, queue_kind, &req)?;

            let (rt_before, pd_before) = Self::queue_counts_in_tx(&tx, &target_sid)?;
            session.status.realtime_queue_count = rt_before as u32;
            session.status.pending_queue_count = pd_before as u32;
            Self::save_session_in_tx(&tx, &mut session)?;

            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;

            let spawned_activation_id = dispatches
                .iter()
                .find(|d| d.session.metadata.id == target_sid.as_str())
                .map(|d| ActivationId::new(d.activation.metadata.id.clone()));

            let (rt_after, pd_after) = Self::queue_counts_in_tx(&tx, &target_sid)?;
            if spawned_activation_id.is_none() {
                if let Some(mut sess_reload) = Self::load_session_in_tx(&tx, &target_sid)? {
                    let ev = Self::append_event_in_tx(
                        &tx,
                        &target_sid,
                        None,
                        None,
                        None,
                        SessionEvent::SessionQueueUpdated {
                            realtime_count: rt_after,
                            pending_count: pd_after,
                        },
                    )?;
                    sess_reload.status.event_count = ev.event_seq.value() + 1;
                    sess_reload.status.realtime_queue_count = rt_after as u32;
                    sess_reload.status.pending_queue_count = pd_after as u32;
                    Self::save_session_in_tx(&tx, &mut sess_reload)?;
                    events_to_publish.push(ev);
                }
            }

            tx.commit()?;
            EnqueueSessionMessageResponse {
                message,
                target_session_id: target_sid,
                queue_kind,
                realtime_count: rt_after,
                pending_count: pd_after,
                spawned_activation_id,
            }
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches.clone());
        Ok((response, dispatches))
    }

    pub fn list_session_queues(
        &self,
        session_id: &SessionId,
    ) -> Result<SessionQueuesResponse, StoreError> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;
        let (target_sid, _) = Self::resolve_open_session_in_tx(&tx, session_id)?;

        let mut stmt = tx.prepare(
            "SELECT id, session_id, queue_kind, content_json, tool_policy_json, metadata_json, created_at
             FROM session_queue_messages
             WHERE session_id = ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![target_sid.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;

        let mut realtime = Vec::new();
        let mut pending = Vec::new();
        for r in rows {
            let (id, sid, kind_s, c_json, tp_json, meta_json, created_at) = r?;
            let msg = Self::parse_queue_row(id, sid, kind_s, c_json, tp_json, meta_json, created_at)?;
            match msg.queue_kind {
                QueueKind::Realtime => realtime.push(msg),
                QueueKind::Pending => pending.push(msg),
            }
        }
        drop(stmt);
        tx.commit()?;

        Ok(SessionQueuesResponse {
            session_id: target_sid,
            realtime,
            pending,
        })
    }

    pub fn withdraw_session_queue_message(
        &self,
        session_id: &SessionId,
        message_id: i64,
    ) -> Result<WithdrawSessionQueueMessageResponse, StoreError> {
        let mut events_to_publish = Vec::new();
        let response = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let (target_sid, mut session) = Self::resolve_open_session_in_tx(&tx, session_id)?;
            let existing = tx
                .query_row(
                    "SELECT id, session_id, queue_kind, content_json, tool_policy_json, metadata_json, created_at
                     FROM session_queue_messages
                     WHERE session_id = ?1 AND id = ?2",
                    params![target_sid.as_str(), message_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                        ))
                    },
                )
                .optional()?;

            let Some((id, sid, kind_s, c_json, tp_json, meta_json, created_at)) = existing else {
                return Err(StoreError::Conflict(format!(
                    "queue message {message_id} for session {target_sid} was already consumed or does not exist"
                )));
            };
            let removed_message =
                Self::parse_queue_row(id, sid, kind_s, c_json, tp_json, meta_json, created_at)?;

            let deleted = tx.execute(
                "DELETE FROM session_queue_messages WHERE session_id = ?1 AND id = ?2",
                params![target_sid.as_str(), message_id],
            )?;
            if deleted == 0 {
                return Err(StoreError::Conflict(format!(
                    "queue message {message_id} could not be withdrawn"
                )));
            }

            let (rt_count, pd_count) = Self::queue_counts_in_tx(&tx, &target_sid)?;
            session.status.realtime_queue_count = rt_count as u32;
            session.status.pending_queue_count = pd_count as u32;

            let ev = Self::append_event_in_tx(
                &tx,
                &target_sid,
                None,
                None,
                None,
                SessionEvent::SessionQueueUpdated {
                    realtime_count: rt_count,
                    pending_count: pd_count,
                },
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_to_publish.push(ev);
            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;

            WithdrawSessionQueueMessageResponse {
                removed_message,
                session_id: target_sid,
                realtime_count: rt_count,
                pending_count: pd_count,
            }
        };

        self.publish_events(events_to_publish);
        Ok(response)
    }

    /// Atomically drains ALL queued real-time messages for `session_id` in one batch
    /// inside an already-running `Activation`, preserving each message's `MessageContent`
    /// blocks and returning their `tool_policies` in FIFO order.
    pub fn drain_realtime_messages(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
    ) -> Result<(DrainedRealtimeBatch, Option<BatchSeq>), StoreError> {
        let mut events_to_publish = Vec::new();
        let (batch, batch_seq) = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            let (drained, maybe_b_seq) = Self::drain_realtime_messages_in_tx(
                &tx,
                session_id,
                activation_id,
                turn_id,
                &mut session,
                &mut events_to_publish,
            )?;
            if maybe_b_seq.is_some() {
                Self::save_session_in_tx(&tx, &mut session)?;
                tx.commit()?;
            }
            (drained, maybe_b_seq)
        };

        self.publish_events(events_to_publish);
        Ok((batch, batch_seq))
    }

    pub(crate) fn drain_realtime_messages_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
        activation_id: &ActivationId,
        turn_id: Option<TurnId>,
        session: &mut Session,
        events_to_publish: &mut Vec<SessionEventEnvelope>,
    ) -> Result<(DrainedRealtimeBatch, Option<BatchSeq>), StoreError> {
        let mut blocks: Vec<MessageContent> = Vec::new();
        let mut tool_policies: Vec<QueueToolPolicy> = Vec::new();
        {
            let mut stmt = tx.prepare(
                "SELECT content_json, tool_policy_json
                 FROM session_queue_messages
                 WHERE session_id = ?1 AND queue_kind = 'realtime'
                 ORDER BY id ASC",
            )?;
            let rows = stmt.query_map(params![session_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (content_json, policy_json) = row?;
                let row_blocks: Vec<MessageContent> = serde_json::from_str(&content_json)?;
                let pol: QueueToolPolicy = serde_json::from_str(&policy_json)?;
                blocks.extend(row_blocks);
                tool_policies.push(pol);
            }
        }

        if blocks.is_empty() {
            return Ok((DrainedRealtimeBatch::default(), None));
        }

        tx.execute(
            "DELETE FROM session_queue_messages WHERE session_id = ?1 AND queue_kind = 'realtime'",
            params![session_id.as_str()],
        )?;

        let block_count = blocks.len();
        let item = Item::user_message_with_content(blocks);
        let items = vec![item];

        let b_seq = BatchSeq::new(session.status.batch_count);
        let first_seq = LocalItemSeq::new(session.status.local_item_count);
        let batch = ItemBatch {
            session_id: session_id.clone(),
            batch_seq: b_seq,
            first_local_item_seq: first_seq,
            kind: ItemBatchKind::Input,
            activation_id: activation_id.clone(),
            turn_id: turn_id.clone(),
            response_id: None,
            view_mode_placeholder: None,
            created_at: Utc::now(),
            items: items.clone(),
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
                "input",
                activation_id.as_str(),
                turn_id.as_ref().map(|t| t.as_str()),
                Option::<String>::None,
                batch.created_at.to_rfc3339(),
                serde_json::to_string(&batch)?,
            ],
        )?;
        session.status.batch_count += 1;
        session.status.local_item_count += 1;
        session.status.effective_context_item_count += 1;

        let (_, pending_count) = Self::queue_counts_in_tx(tx, session_id)?;
        session.status.realtime_queue_count = 0;
        session.status.pending_queue_count = pending_count as u32;

        let ev = Self::append_event_in_tx(
            tx,
            session_id,
            Some(activation_id.clone()),
            turn_id,
            Some(b_seq),
            SessionEvent::RealtimeMessagesDrained {
                count: block_count,
                batch_seq: b_seq,
            },
        )?;
        session.status.event_count = ev.event_seq.value() + 1;
        events_to_publish.push(ev);

        let q_ev = Self::append_event_in_tx(
            tx,
            session_id,
            Some(activation_id.clone()),
            None,
            None,
            SessionEvent::SessionQueueUpdated {
                realtime_count: 0,
                pending_count,
            },
        )?;
        session.status.event_count = q_ev.event_seq.value() + 1;
        events_to_publish.push(q_ev);

        Ok((
            DrainedRealtimeBatch {
                items,
                tool_policies,
            },
            Some(b_seq),
        ))
    }

    /// Inspects the next dispatchable queue unit (`realtime` batch or head 1 `pending` item)
    /// without removing it from SQLite, returning its `model_override` if set.
    pub(crate) fn peek_next_queue_model_override_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<Option<ModelRequestSpec>, StoreError> {
        let rt_models: Vec<Option<String>> = {
            let mut stmt = tx.prepare(
                "SELECT model_override_json FROM session_queue_messages
                 WHERE session_id = ?1 AND queue_kind = 'realtime'
                 ORDER BY id ASC",
            )?;
            let rows = stmt.query_map(params![session_id.as_str()], |r| r.get::<_, Option<String>>(0))?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r?);
            }
            out
        };
        if !rt_models.is_empty() {
            for json_opt in rt_models.into_iter().rev() {
                if let Some(json) = json_opt {
                    return Ok(Some(serde_json::from_str(&json)?));
                }
            }
            return Ok(None);
        }

        let pd_model: Option<Option<String>> = tx
            .query_row(
                "SELECT model_override_json FROM session_queue_messages
                 WHERE session_id = ?1 AND queue_kind = 'pending'
                 ORDER BY id ASC LIMIT 1",
                params![session_id.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        match pd_model.flatten() {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    /// Pops either ALL `realtime` messages (priority 1) or ONLY the head 1 `pending` message
    /// (priority 2) to launch the next independent `Activation` on an idle session.
    pub(crate) fn pop_next_activation_unit_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<
        Option<(
            SubmissionKind,
            Vec<Item>,
            QueueToolPolicy,
            Option<ModelRequestSpec>,
        )>,
        StoreError,
    > {
        // Priority 1: drain all queued `realtime` messages together
        let rt_rows: Vec<(i64, String, String, Option<String>)> = {
            let mut stmt = tx.prepare(
                "SELECT id, content_json, tool_policy_json, model_override_json
                 FROM session_queue_messages
                 WHERE session_id = ?1 AND queue_kind = 'realtime'
                 ORDER BY id ASC",
            )?;
            let rows = stmt.query_map(params![session_id.as_str()], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })?;
            let mut list = Vec::new();
            for r in rows {
                list.push(r?);
            }
            list
        };

        if !rt_rows.is_empty() {
            let mut blocks = Vec::new();
            let mut disabled_set = BTreeSet::new();
            let mut model_override = None;
            for (_, content_json, policy_json, model_json) in rt_rows {
                let row_blocks: Vec<MessageContent> = serde_json::from_str(&content_json)?;
                let pol: QueueToolPolicy = serde_json::from_str(&policy_json)?;
                pol.apply_to(&mut disabled_set);
                if let Some(m_json) = model_json {
                    model_override = Some(serde_json::from_str(&m_json)?);
                }
                blocks.extend(row_blocks);
            }
            tx.execute(
                "DELETE FROM session_queue_messages WHERE session_id = ?1 AND queue_kind = 'realtime'",
                params![session_id.as_str()],
            )?;
            let folded_policy = QueueToolPolicy::with_disabled(disabled_set);
            return Ok(Some((
                SubmissionKind::Continuation,
                vec![Item::user_message_with_content(blocks)],
                folded_policy,
                model_override,
            )));
        }

        // Priority 2: pop ONLY the head 1 `pending` message (1 message = 1 independent Activation)
        let pd_row: Option<(i64, String, String, Option<String>)> = tx
            .query_row(
                "SELECT id, content_json, tool_policy_json, model_override_json
                 FROM session_queue_messages
                 WHERE session_id = ?1 AND queue_kind = 'pending'
                 ORDER BY id ASC LIMIT 1",
                params![session_id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;

        if let Some((row_id, content_json, policy_json, model_json)) = pd_row {
            tx.execute(
                "DELETE FROM session_queue_messages WHERE id = ?1",
                params![row_id],
            )?;
            let row_blocks: Vec<MessageContent> = serde_json::from_str(&content_json)?;
            let pol: QueueToolPolicy = serde_json::from_str(&policy_json)?;
            let model_override = match model_json {
                Some(m_json) => Some(serde_json::from_str(&m_json)?),
                None => None,
            };
            return Ok(Some((
                SubmissionKind::Normal,
                vec![Item::user_message_with_content(row_blocks)],
                pol,
                model_override,
            )));
        }

        Ok(None)
    }
}
