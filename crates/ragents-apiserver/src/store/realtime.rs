use super::{SqliteStore, StoreError};
use chrono::Utc;
use openresponses_rust::Item;
use ragents_protocol::*;
use rusqlite::params;
use std::collections::BTreeMap;

impl SqliteStore {
    pub fn push_realtime_message(
        &self,
        session_id: &SessionId,
        content: Vec<openresponses_rust::MessageContent>,
    ) -> Result<PushRealtimeMessageResponse, StoreError> {
        self.push_realtime_message_with_options(session_id, content, Vec::new())
    }

    pub fn push_realtime_message_with_options(
        &self,
        session_id: &SessionId,
        content: Vec<openresponses_rust::MessageContent>,
        disabled_tools: Vec<String>,
    ) -> Result<PushRealtimeMessageResponse, StoreError> {
        let mut events_to_publish = Vec::new();
        let mut dispatches = Vec::new();
        let response = {
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

            if session.status.phase != SessionPhase::Open {
                return Err(StoreError::Conflict(format!(
                    "session {target_sid} is not open (phase: {:?})",
                    session.status.phase
                )));
            }

            let now = Utc::now().to_rfc3339();
            for block in content {
                tx.execute(
                    "INSERT INTO realtime_messages (session_id, content_json, created_at) VALUES (?1, ?2, ?3)",
                    params![target_sid.as_str(), serde_json::to_string(&block)?, &now],
                )?;
            }

            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM realtime_messages WHERE session_id = ?1",
                params![target_sid.as_str()],
                |row| row.get(0),
            )?;

            // If the session is currently idle (no active or queued activation), automatically wake it up
            // by creating and scheduling a Continuation activation so the realtime message is immediately processed.
            let mut spawned_activation_id = None;
            if count > 0
                && session.status.active_activation_id.is_none()
                && session.status.queued_activation_count == 0
            {
                let wake_act = Self::create_activation_in_tx(
                    &tx,
                    CreateActivationRequest {
                        activation_id: None,
                        labels: BTreeMap::new(),
                        spec: ActivationSpec {
                            session_id: target_sid.clone(),
                            submission_kind: SubmissionKind::Continuation,
                            input: vec![],
                            model_override: None,
                            disabled_tools,
                            desired_phase: DesiredActivationPhase::Running,
                            interaction_responses: BTreeMap::new(),
                        },
                    },
                    &mut events_to_publish,
                )?;
                spawned_activation_id = Some(ActivationId::new(wake_act.metadata.id));
                dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
            }

            tx.commit()?;
            PushRealtimeMessageResponse {
                queued_count: count as usize,
                target_session_id: Some(target_sid),
                spawned_activation_id,
            }
        };

        self.publish_events(events_to_publish);
        self.publish_dispatches(dispatches);
        Ok(response)
    }

    /// Atomically drains ALL queued real-time messages for `session_id` in one batch,
    /// preserving each message as its own separate `MessageContent` block inside `Item::Message.content`
    /// (never concatenating multiple messages into a single string), and commits the batch to `batches`.
    pub fn drain_realtime_messages(
        &self,
        session_id: &SessionId,
        activation_id: &ActivationId,
        lease_epoch: LeaseEpoch,
        turn_id: Option<TurnId>,
    ) -> Result<(Vec<Item>, Option<BatchSeq>), StoreError> {
        let mut events_to_publish = Vec::new();
        let (items, batch_seq) = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;

            let act = Self::load_activation_in_tx(&tx, activation_id)?
                .ok_or_else(|| StoreError::NotFound(format!("activation {activation_id}")))?;
            Self::verify_fencing(&act, lease_epoch)?;

            let mut session = Self::load_session_in_tx(&tx, session_id)?
                .ok_or_else(|| StoreError::NotFound(format!("session {session_id}")))?;

            let mut blocks: Vec<openresponses_rust::MessageContent> = Vec::new();
            {
                let mut stmt = tx.prepare(
                    "SELECT content_json FROM realtime_messages WHERE session_id = ?1 ORDER BY id ASC",
                )?;
                let rows = stmt.query_map(params![session_id.as_str()], |row| {
                    let s: String = row.get(0)?;
                    Ok(s)
                })?;
                for row in rows {
                    let json_str = row?;
                    let block: openresponses_rust::MessageContent =
                        serde_json::from_str(&json_str)?;
                    blocks.push(block);
                }
            }

            if blocks.is_empty() {
                return Ok((Vec::new(), None));
            }

            tx.execute(
                "DELETE FROM realtime_messages WHERE session_id = ?1",
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

            let ev = Self::append_event_in_tx(
                &tx,
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

            Self::save_session_in_tx(&tx, &mut session)?;
            tx.commit()?;
            (items, Some(b_seq))
        };

        self.publish_events(events_to_publish);
        Ok((items, batch_seq))
    }
}
