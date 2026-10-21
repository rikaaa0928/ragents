use super::{ScheduledDispatch, SqliteStore, StoreError};
use ragents_protocol::*;
use rusqlite::params;
use std::collections::BTreeMap;

impl SqliteStore {
    /// Dual-mode Scheduler:
    /// - `Pinned`: Strictly binds to `node_id`. Never migrates.
    /// - `Floating`: Selects a healthy `Ready` node matching `node_selector` and `supported_models`
    ///   (preferring previous `bound_node_id` for cache affinity, then least-loaded).
    pub(crate) fn schedule_pending_in_tx(
        tx: &rusqlite::Transaction<'_>,
        events_out: &mut Vec<SessionEventEnvelope>,
    ) -> Result<Vec<ScheduledDispatch>, StoreError> {
        let queued_act_ids: Vec<(String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT activation_id, session_id FROM activations
                 WHERE phase = 'queued' ORDER BY created_at ASC",
            )?;
            let rows =
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            let mut list = Vec::new();
            for r in rows {
                list.push(r?);
            }
            list
        };

        let mut dispatches = Vec::new();

        for (act_id_str, sess_id_str) in queued_act_ids {
            let sess_id = SessionId::new(sess_id_str);
            let mut session = match Self::load_session_in_tx(tx, &sess_id)? {
                Some(s) => s,
                None => continue,
            };

            if session.status.active_activation_id.is_some()
                || session.status.phase != SessionPhase::Open
            {
                continue;
            }

            let act_id = ActivationId::new(act_id_str);
            let mut activation = match Self::load_activation_in_tx(tx, &act_id)? {
                Some(a) if a.status.phase == ActivationPhase::Queued => a,
                _ => continue,
            };

            let required_model = activation
                .spec
                .model_override
                .as_ref()
                .map(|m| m.name.as_str())
                .or_else(|| session.spec.model.as_ref().map(|m| m.name.as_str()));

            let previous_assigned = session.status.bound_node_id.clone();

            let target_node_id = match &session.spec.scheduling {
                SchedulingMode::Pinned { node_id, .. } => {
                    let node = match Self::load_node_in_tx(tx, node_id)? {
                        Some(n) => n,
                        None => continue,
                    };
                    if !Self::node_can_run(&node, required_model, &BTreeMap::new()) {
                        continue;
                    }
                    node_id.clone()
                }
                SchedulingMode::Floating { node_selector } => {
                    let candidates = Self::find_candidate_nodes_in_tx(
                        tx,
                        required_model,
                        node_selector,
                        previous_assigned.as_ref(),
                    )?;
                    match candidates.into_iter().next() {
                        Some(chosen) => chosen,
                        None => continue,
                    }
                }
            };

            let mut target_node_spec = NodeSpec::default();
            if let Some(mut node) = Self::load_node_in_tx(tx, &target_node_id)? {
                target_node_spec = node.spec.clone();
                node.status.active_runners = node.status.active_runners.saturating_add(1);
                tx.execute(
                    "UPDATE nodes SET status_json = ?1 WHERE node_id = ?2",
                    params![
                        serde_json::to_string(&node.status)?,
                        target_node_id.as_str()
                    ],
                )?;
            }
            let cluster_system_prompt = Self::load_cluster_system_prompt_in_tx(tx)?;

            let was_rescheduled_from = if session.spec.scheduling.is_floating() {
                previous_assigned.filter(|prev| prev != &target_node_id)
            } else {
                None
            };

            let input_items = activation.spec.input.clone();
            let batch_seq = BatchSeq::new(session.status.batch_count);
            let item_count = input_items.len() as u64;
            if !input_items.is_empty() {
                let first_seq = LocalItemSeq::new(session.status.local_item_count);
                let batch = ItemBatch {
                    session_id: sess_id.clone(),
                    batch_seq,
                    first_local_item_seq: first_seq,
                    kind: ItemBatchKind::Input,
                    activation_id: act_id.clone(),
                    turn_id: None,
                    response_id: None,
                    created_at: chrono::Utc::now(),
                    items: input_items.clone(),
                };

                tx.execute(
                    "INSERT INTO batches (
                        session_id, batch_seq, first_local_item_seq, last_local_item_seq,
                        kind, activation_id, turn_id, response_id, created_at, payload_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        sess_id.as_str(),
                        batch_seq.value() as i64,
                        first_seq.value() as i64,
                        batch.last_local_item_seq().value() as i64,
                        "input",
                        act_id.as_str(),
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

            let submitted_ev = Self::append_event_in_tx(
                tx,
                &sess_id,
                Some(act_id.clone()),
                None,
                (!input_items.is_empty()).then_some(batch_seq),
                SessionEvent::ActivationSubmitted {
                    input_batch_seq: batch_seq,
                    items: input_items,
                },
            )?;
            session.status.event_count = submitted_ev.event_seq.value() + 1;
            events_out.push(submitted_ev);

            let next_epoch = activation.status.lease_epoch.next();
            activation.status.lease_epoch = next_epoch;
            activation.status.phase = ActivationPhase::Scheduled;
            activation.status.assigned_node = Some(target_node_id.clone());

            session.status.active_activation_id = Some(act_id.clone());
            session.status.bound_node_id = Some(target_node_id.clone());
            session.status.queued_activation_count =
                session.status.queued_activation_count.saturating_sub(1);
            let existing_ext_prompts = session
                .status
                .latest_prompt_parts
                .as_ref()
                .map(|p| p.extension_prompts.clone())
                .unwrap_or_default();
            session.status.latest_prompt_parts = Some(PromptParts {
                system_prompt: cluster_system_prompt.clone(),
                node_prompt: target_node_spec.node_prompt.clone().unwrap_or_default(),
                client_prompt: session.spec.system_prompt.clone().unwrap_or_default(),
                extension_prompts: existing_ext_prompts,
            });

            let sched_event = if let Some(from_node) = was_rescheduled_from {
                SessionEvent::ActivationRescheduled {
                    from_node,
                    to_node: target_node_id.clone(),
                    lease_epoch: next_epoch,
                    reason: "Floating session scheduled onto available healthy node".to_string(),
                }
            } else {
                SessionEvent::ActivationScheduled {
                    node_id: target_node_id.clone(),
                    lease_epoch: next_epoch,
                }
            };

            let ev = Self::append_event_in_tx(
                tx,
                &sess_id,
                Some(act_id.clone()),
                None,
                None,
                sched_event,
            )?;
            session.status.event_count = ev.event_seq.value() + 1;
            events_out.push(ev);

            Self::save_activation_in_tx(tx, &mut activation)?;
            Self::save_session_in_tx(tx, &mut session)?;

            dispatches.push(ScheduledDispatch {
                node_id: target_node_id,
                session,
                activation,
                lease_epoch: next_epoch,
                cluster_system_prompt,
                node_spec: target_node_spec,
            });
        }

        Ok(dispatches)
    }

    pub(crate) fn node_can_run(
        node: &Node,
        required_model: Option<&str>,
        selector: &BTreeMap<String, String>,
    ) -> bool {
        if node.spec.unschedulable || node.status.phase != NodePhase::Ready {
            return false;
        }
        if node.status.active_runners >= node.status.max_concurrent_runners {
            return false;
        }
        for (k, v) in selector {
            if node.metadata.labels.get(k) != Some(v) {
                return false;
            }
        }
        match required_model {
            Some(model_name) => node.status.supported_models.iter().any(|m| m == model_name),
            None => node.status.default_model.is_some() || !node.status.supported_models.is_empty(),
        }
    }

    pub(crate) fn find_candidate_nodes_in_tx(
        tx: &rusqlite::Transaction<'_>,
        required_model: Option<&str>,
        selector: &BTreeMap<String, String>,
        preferred_node: Option<&NodeId>,
    ) -> Result<Vec<NodeId>, StoreError> {
        let mut stmt = tx.prepare(
            "SELECT node_id, resource_version, spec_json, status_json FROM nodes ORDER BY node_id ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;

        let mut matching: Vec<(NodeId, bool, u32)> = Vec::new();
        for r in rows {
            let (id_str, rv, spec_json, status_json) = r?;
            let spec: NodeSpec = serde_json::from_str(&spec_json)?;
            let status: NodeStatus = serde_json::from_str(&status_json)?;
            let node_id = NodeId::new(id_str.clone());
            let node = Node {
                metadata: ObjectMeta::new(id_str, ResourceVersion::new(rv as u64)),
                spec,
                status,
            };
            if Self::node_can_run(&node, required_model, selector) {
                let is_preferred = preferred_node == Some(&node_id);
                matching.push((node_id, is_preferred, node.status.active_runners));
            }
        }

        matching.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        Ok(matching.into_iter().map(|(id, _, _)| id).collect())
    }
}
