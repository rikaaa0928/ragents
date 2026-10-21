mod activations;
mod nodes;
mod realtime;
mod scheduler;
mod schema;
mod sessions;

use chrono::{DateTime, Utc};
use ragents_protocol::*;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("validation error: {0}")]
    Validation(#[from] ProtocolValidationError),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("stale lease epoch for activation {activation_id}: expected {expected}, got {got}")]
    StaleLeaseEpoch {
        activation_id: String,
        expected: u64,
        got: u64,
    },
    #[error("node {node_id} does not support model '{model}' (supported: {supported:?})")]
    UnsupportedModelOnNode {
        node_id: String,
        model: String,
        supported: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct ScheduledDispatch {
    pub node_id: NodeId,
    pub session: Session,
    pub activation: Activation,
    pub lease_epoch: LeaseEpoch,
    pub cluster_system_prompt: String,
    pub node_spec: NodeSpec,
}

#[derive(Debug, Clone)]
pub struct ReconcileNotice {
    pub node_id: NodeId,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub lease_epoch: LeaseEpoch,
    pub desired_phase: DesiredActivationPhase,
    pub interaction_responses: BTreeMap<PermissionRequestId, PermissionResolutionSpec>,
}

#[derive(Debug, Clone)]
pub struct PatchActivationOutcome {
    pub activation: Activation,
    pub reconcile_notice: Option<ReconcileNotice>,
    pub newly_scheduled: Vec<ScheduledDispatch>,
}

#[derive(Clone)]
pub struct SqliteStore {
    pub(crate) conn: Arc<Mutex<Connection>>,
    pub(crate) event_tx: broadcast::Sender<SessionEventEnvelope>,
    pub(crate) dispatch_tx: broadcast::Sender<ScheduledDispatch>,
    pub(crate) reconcile_tx: broadcast::Sender<ReconcileNotice>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        if let Some(parent) = path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self, StoreError> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )?;

        Self::init_schema(&conn)?;

        if let Ok(env_prompt) = std::env::var("RAGENTS_SYSTEM_PROMPT") {
            let trimmed = env_prompt.trim();
            if !trimmed.is_empty() {
                let current: String = conn
                    .query_row(
                        "SELECT value FROM store_meta WHERE key = 'cluster_system_prompt'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or_default();
                if current.trim().is_empty() {
                    let _ = conn.execute(
                        "UPDATE store_meta SET value = ?1 WHERE key = 'cluster_system_prompt'",
                        params![trimmed],
                    );
                }
            }
        }

        let (event_tx, _) = broadcast::channel(4096);
        let (dispatch_tx, _) = broadcast::channel(1024);
        let (reconcile_tx, _) = broadcast::channel(1024);

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_tx,
            dispatch_tx,
            reconcile_tx,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEventEnvelope> {
        self.event_tx.subscribe()
    }

    pub fn subscribe_dispatches(&self) -> broadcast::Receiver<ScheduledDispatch> {
        self.dispatch_tx.subscribe()
    }

    pub fn subscribe_reconciles(&self) -> broadcast::Receiver<ReconcileNotice> {
        self.reconcile_tx.subscribe()
    }

    pub(crate) fn next_resource_version(
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<ResourceVersion, StoreError> {
        let current_str: String = tx.query_row(
            "SELECT value FROM store_meta WHERE key = 'global_resource_version'",
            [],
            |row| row.get(0),
        )?;
        let next = current_str.parse::<u64>().unwrap_or(0).saturating_add(1);
        tx.execute(
            "UPDATE store_meta SET value = ?1 WHERE key = 'global_resource_version'",
            params![next.to_string()],
        )?;
        Ok(ResourceVersion::new(next))
    }

    pub fn current_resource_version(&self) -> Result<ResourceVersion, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let current_str: String = conn.query_row(
            "SELECT value FROM store_meta WHERE key = 'global_resource_version'",
            [],
            |row| row.get(0),
        )?;
        Ok(ResourceVersion::new(
            current_str.parse::<u64>().unwrap_or(0),
        ))
    }

    pub(crate) fn load_cluster_system_prompt_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<String, StoreError> {
        let val: Option<String> = tx
            .query_row(
                "SELECT value FROM store_meta WHERE key = 'cluster_system_prompt'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(val.unwrap_or_default())
    }

    pub fn get_cluster_system_prompt(&self) -> Result<String, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let val: Option<String> = conn
            .query_row(
                "SELECT value FROM store_meta WHERE key = 'cluster_system_prompt'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(val.unwrap_or_default())
    }

    pub fn set_cluster_system_prompt(&self, prompt: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "INSERT INTO store_meta (key, value) VALUES ('cluster_system_prompt', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![prompt],
        )?;
        Ok(())
    }

    pub(crate) fn append_event_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
        activation_id: Option<ActivationId>,
        turn_id: Option<TurnId>,
        batch_seq: Option<BatchSeq>,
        event: SessionEvent,
    ) -> Result<SessionEventEnvelope, StoreError> {
        let rv = Self::next_resource_version(tx)?;
        let next_event_seq: u64 = tx.query_row(
            "SELECT COALESCE(MAX(event_seq) + 1, 0) FROM events WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| row.get(0),
        )?;
        let envelope = SessionEventEnvelope {
            resource_version: rv,
            event_seq: EventSeq::new(next_event_seq),
            event_id: EventId::generate(),
            session_id: session_id.clone(),
            activation_id: activation_id.clone(),
            turn_id: turn_id.clone(),
            created_at: Utc::now(),
            event,
        };
        let payload_json = serde_json::to_string(&envelope)?;
        tx.execute(
            "INSERT INTO events (
                resource_version, session_id, event_seq, event_id, kind,
                activation_id, turn_id, batch_seq, created_at, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                rv.value() as i64,
                session_id.as_str(),
                next_event_seq as i64,
                envelope.event_id.as_str(),
                envelope.event.kind_name(),
                activation_id.as_ref().map(|a| a.as_str()),
                turn_id.as_ref().map(|t| t.as_str()),
                batch_seq.map(|b| b.value() as i64),
                envelope.created_at.to_rfc3339(),
                payload_json,
            ],
        )?;
        Ok(envelope)
    }

    pub(crate) fn publish_events(&self, events: Vec<SessionEventEnvelope>) {
        for ev in events {
            let _ = self.event_tx.send(ev);
        }
    }

    pub(crate) fn publish_dispatches(&self, dispatches: Vec<ScheduledDispatch>) {
        for d in dispatches {
            let _ = self.dispatch_tx.send(d);
        }
    }

    pub fn read_events_after(
        &self,
        session_id: Option<&SessionId>,
        after_rv: u64,
    ) -> Result<Vec<SessionEventEnvelope>, StoreError> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut events = Vec::new();
        if let Some(sess_id) = session_id {
            let mut stmt = conn.prepare(
                "SELECT payload_json FROM events
                 WHERE session_id = ?1 AND resource_version > ?2
                 ORDER BY resource_version ASC",
            )?;
            let rows = stmt.query_map(params![sess_id.as_str(), after_rv as i64], |r| {
                r.get::<_, String>(0)
            })?;
            for r in rows {
                events.push(serde_json::from_str(&r?)?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT payload_json FROM events
                 WHERE resource_version > ?1
                 ORDER BY resource_version ASC",
            )?;
            let rows = stmt.query_map(params![after_rv as i64], |r| r.get::<_, String>(0))?;
            for r in rows {
                events.push(serde_json::from_str(&r?)?);
            }
        }
        Ok(events)
    }

    pub(crate) fn verify_fencing(
        act: &Activation,
        lease_epoch: LeaseEpoch,
    ) -> Result<(), StoreError> {
        if act.status.lease_epoch != lease_epoch || !act.status.phase.is_active() {
            return Err(StoreError::StaleLeaseEpoch {
                activation_id: act.metadata.id.clone(),
                expected: act.status.lease_epoch.value(),
                got: lease_epoch.value(),
            });
        }
        Ok(())
    }

    pub(crate) fn load_node_in_tx(
        tx: &rusqlite::Transaction<'_>,
        node_id: &NodeId,
    ) -> Result<Option<Node>, StoreError> {
        let row: Option<(i64, String, String)> = tx
            .query_row(
                "SELECT resource_version, spec_json, status_json FROM nodes WHERE node_id = ?1",
                params![node_id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        match row {
            Some((rv, spec_json, status_json)) => Ok(Some(Node {
                metadata: ObjectMeta::new(node_id.as_str(), ResourceVersion::new(rv as u64)),
                spec: serde_json::from_str(&spec_json)?,
                status: serde_json::from_str(&status_json)?,
            })),
            None => Ok(None),
        }
    }

    pub(crate) fn save_node_spec_in_tx(
        tx: &rusqlite::Transaction<'_>,
        node: &mut Node,
    ) -> Result<(), StoreError> {
        let rv = Self::next_resource_version(tx)?;
        node.metadata.resource_version = rv;
        node.status.effective_node_prompt = node.spec.effective_node_prompt();
        node.status.effective_permissions = node.spec.effective_permissions();
        tx.execute(
            "UPDATE nodes SET resource_version = ?1, spec_json = ?2, status_json = ?3 WHERE node_id = ?4",
            params![
                rv.value() as i64,
                serde_json::to_string(&node.spec)?,
                serde_json::to_string(&node.status)?,
                node.metadata.id,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn load_session_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<Option<Session>, StoreError> {
        Self::load_session_from_conn(tx, session_id)
    }

    pub(crate) fn load_session_from_conn(
        conn: &Connection,
        session_id: &SessionId,
    ) -> Result<Option<Session>, StoreError> {
        let row: Option<(i64, i64, String, String, String, String, String)> = conn
            .query_row(
                "SELECT generation, resource_version, created_at, updated_at, labels_json, spec_json, status_json
                 FROM sessions WHERE session_id = ?1",
                params![session_id.as_str()],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .optional()?;
        match row {
            Some((gen, rv, created_str, updated_str, labels_json, spec_json, status_json)) => {
                let labels: BTreeMap<String, String> =
                    serde_json::from_str(&labels_json).unwrap_or_default();
                Ok(Some(Session {
                    metadata: ObjectMeta {
                        id: session_id.to_string(),
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
                    spec: serde_json::from_str(&spec_json)?,
                    status: serde_json::from_str(&status_json)?,
                }))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn save_session_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session: &mut Session,
    ) -> Result<(), StoreError> {
        let rv = Self::next_resource_version(tx)?;
        session.metadata.resource_version = rv;
        session.metadata.updated_at = Utc::now();
        tx.execute(
            "UPDATE sessions SET
                generation = ?1,
                resource_version = ?2,
                bound_node_id = ?3,
                updated_at = ?4,
                labels_json = ?5,
                spec_json = ?6,
                status_json = ?7
             WHERE session_id = ?8",
            params![
                session.metadata.generation.value() as i64,
                rv.value() as i64,
                session.status.bound_node_id.as_ref().map(|n| n.as_str()),
                session.metadata.updated_at.to_rfc3339(),
                serde_json::to_string(&session.metadata.labels)?,
                serde_json::to_string(&session.spec)?,
                serde_json::to_string(&session.status)?,
                session.metadata.id,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn load_activation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, StoreError> {
        Self::load_activation_from_conn(tx, activation_id)
    }

    pub(crate) fn load_activation_from_conn(
        conn: &Connection,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, StoreError> {
        let row: Option<(i64, i64, String, String, String, String)> = conn
            .query_row(
                "SELECT generation, resource_version, created_at, updated_at, spec_json, status_json
                 FROM activations WHERE activation_id = ?1",
                params![activation_id.as_str()],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .optional()?;
        match row {
            Some((gen, rv, created_str, updated_str, spec_json, status_json)) => {
                Ok(Some(Activation {
                    metadata: ObjectMeta {
                        id: activation_id.to_string(),
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
                    spec: serde_json::from_str(&spec_json)?,
                    status: serde_json::from_str(&status_json)?,
                }))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn save_activation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        act: &mut Activation,
    ) -> Result<(), StoreError> {
        let rv = Self::next_resource_version(tx)?;
        act.metadata.resource_version = rv;
        act.metadata.updated_at = Utc::now();
        let phase_str = match act.status.phase {
            ActivationPhase::Queued => "queued",
            ActivationPhase::Scheduled => "scheduled",
            ActivationPhase::Running => "running",
            ActivationPhase::WaitingForInteraction => "waiting_for_interaction",
            ActivationPhase::Cancelling => "cancelling",
            ActivationPhase::Succeeded => "succeeded",
            ActivationPhase::Failed => "failed",
            ActivationPhase::Cancelled => "cancelled",
            ActivationPhase::Interrupted => "interrupted",
        };
        tx.execute(
            "UPDATE activations SET
                generation = ?1,
                resource_version = ?2,
                phase = ?3,
                assigned_node = ?4,
                lease_epoch = ?5,
                updated_at = ?6,
                spec_json = ?7,
                status_json = ?8
             WHERE activation_id = ?9",
            params![
                act.metadata.generation.value() as i64,
                rv.value() as i64,
                phase_str,
                act.status.assigned_node.as_ref().map(|n| n.as_str()),
                act.status.lease_epoch.value() as i64,
                act.metadata.updated_at.to_rfc3339(),
                serde_json::to_string(&act.spec)?,
                serde_json::to_string(&act.status)?,
                act.metadata.id,
            ],
        )?;
        Ok(())
    }
}
