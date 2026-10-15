use chrono::{DateTime, Duration, Utc};
use openresponses_rust::Item;
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
    conn: Arc<Mutex<Connection>>,
    event_tx: broadcast::Sender<SessionEventEnvelope>,
    dispatch_tx: broadcast::Sender<ScheduledDispatch>,
    reconcile_tx: broadcast::Sender<ReconcileNotice>,
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

    fn init_schema(conn: &Connection) -> Result<(), StoreError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS store_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            INSERT OR IGNORE INTO store_meta (key, value) VALUES ('schema_version', '1');
            INSERT OR IGNORE INTO store_meta (key, value) VALUES ('global_resource_version', '0');

            CREATE TABLE IF NOT EXISTS nodes (
                node_id          TEXT PRIMARY KEY,
                resource_version INTEGER NOT NULL,
                spec_json        TEXT NOT NULL,
                status_json      TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS configs (
                config_ref   TEXT PRIMARY KEY,
                created_at   TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS permission_snapshots (
                permission_ref TEXT PRIMARY KEY,
                created_at     TEXT NOT NULL,
                payload_json   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id       TEXT PRIMARY KEY,
                generation       INTEGER NOT NULL,
                resource_version INTEGER NOT NULL,
                scheduling_mode  TEXT NOT NULL,
                bound_node_id    TEXT,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                labels_json      TEXT NOT NULL DEFAULT '{}',
                spec_json        TEXT NOT NULL,
                status_json      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS session_sources (
                child_session_id    TEXT NOT NULL REFERENCES sessions(session_id),
                source_order        INTEGER NOT NULL,
                source_session_id   TEXT NOT NULL REFERENCES sessions(session_id),
                from_context_pos    INTEGER NOT NULL,
                through_context_pos INTEGER NOT NULL,
                PRIMARY KEY (child_session_id, source_order)
            );
            CREATE INDEX IF NOT EXISTS idx_session_sources_parent ON session_sources(source_session_id);

            CREATE TABLE IF NOT EXISTS activations (
                activation_id    TEXT PRIMARY KEY,
                session_id       TEXT NOT NULL REFERENCES sessions(session_id),
                generation       INTEGER NOT NULL,
                resource_version INTEGER NOT NULL,
                phase            TEXT NOT NULL,
                assigned_node    TEXT,
                lease_epoch      INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                spec_json        TEXT NOT NULL,
                status_json      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_activations_session ON activations(session_id);
            CREATE INDEX IF NOT EXISTS idx_activations_node_phase ON activations(assigned_node, phase);

            CREATE TABLE IF NOT EXISTS batches (
                session_id           TEXT NOT NULL REFERENCES sessions(session_id),
                batch_seq            INTEGER NOT NULL,
                first_local_item_seq INTEGER NOT NULL,
                last_local_item_seq  INTEGER NOT NULL,
                kind                 TEXT NOT NULL,
                activation_id        TEXT NOT NULL REFERENCES activations(activation_id),
                turn_id              TEXT,
                response_id          TEXT,
                created_at           TEXT NOT NULL,
                payload_json         TEXT NOT NULL,
                PRIMARY KEY (session_id, batch_seq),
                UNIQUE (session_id, first_local_item_seq)
            );

            CREATE TABLE IF NOT EXISTS events (
                resource_version INTEGER PRIMARY KEY,
                session_id       TEXT NOT NULL REFERENCES sessions(session_id),
                event_seq        INTEGER NOT NULL,
                event_id         TEXT NOT NULL UNIQUE,
                kind             TEXT NOT NULL,
                activation_id    TEXT,
                turn_id          TEXT,
                batch_seq        INTEGER,
                created_at       TEXT NOT NULL,
                payload_json     TEXT NOT NULL,
                UNIQUE (session_id, event_seq)
            );
            CREATE INDEX IF NOT EXISTS idx_events_session_seq ON events(session_id, event_seq);

            CREATE TABLE IF NOT EXISTS realtime_messages (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id   TEXT NOT NULL REFERENCES sessions(session_id),
                content_json TEXT NOT NULL,
                created_at   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_realtime_messages_session ON realtime_messages(session_id, id);",
        )?;
        Ok(())
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

    fn next_resource_version(
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

    fn append_event_in_tx(
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

    fn publish_events(&self, events: Vec<SessionEventEnvelope>) {
        for ev in events {
            let _ = self.event_tx.send(ev);
        }
    }

    fn publish_dispatches(&self, dispatches: Vec<ScheduledDispatch>) {
        for d in dispatches {
            let _ = self.dispatch_tx.send(d);
        }
    }

    // ========================================================================
    // 1. Node Registration, Heartbeat & Lease Expiration
    // ========================================================================

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
        let dispatches;
        let node = {
            let mut conn = self.conn.lock().expect("store mutex poisoned");
            let tx = conn.transaction()?;
            let rv = Self::next_resource_version(&tx)?;
            let now = Utc::now();
            let expires_at = now + Duration::milliseconds(lease_ttl_ms as i64);

            // Count currently active activations on this node
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

            // Schedule any pending activations now that this node is Ready
            dispatches = Self::schedule_pending_in_tx(&tx, &mut events_to_publish)?;
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

            // Handle active activations on expired nodes
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

                    // Bump fencing lease_epoch so any delayed message from dead_node is rejected
                    let bumped_epoch = act.status.lease_epoch.next();
                    act.status.lease_epoch = bumped_epoch;

                    if session.spec.scheduling.is_floating()
                        && act.status.phase == ActivationPhase::Scheduled
                    {
                        // Floating task hasn't started executing yet -> safe to return to Queued and reschedule!
                        act.status.phase = ActivationPhase::Queued;
                        act.status.assigned_node = None;
                        session.status.active_activation_id = None;
                        session.status.queued_activation_count =
                            session.status.queued_activation_count.saturating_add(1);
                        Self::save_activation_in_tx(&tx, &mut act)?;
                        Self::save_session_in_tx(&tx, &mut session)?;
                    } else {
                        // Pinned session OR already running activation -> mark Interrupted (INV-007 & INV-012)
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

    // ========================================================================
    // 2. Session & Context Lineage Management
    // ========================================================================

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

            // If Pinned and target node is already registered, validate model compatibility
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

            // Validate SessionSources and calculate inherited context length
            let mut inherited_item_count = 0u64;
            for (idx, src) in req.spec.sources.iter().enumerate() {
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
                let _ = idx;
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

    fn resolve_effective_items_from_conn(
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

    fn read_local_batches_from_conn(
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

    // ========================================================================
    // 3. Activation Lifecycle & Dual-Mode Scheduler (Pinned vs Floating)
    // ========================================================================

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

    fn create_activation_in_tx(
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

        // Validate model_override against Pinned node if applicable
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

        // Atomically commit the Input ItemBatch & ActivationSubmitted event
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

    /// Dual-mode Scheduler:
    /// - `Pinned`: Strictly binds to `node_id`. Never migrates.
    /// - `Floating`: Selects a healthy `Ready` node matching `node_selector` and `supported_models`
    ///   (preferring previous `bound_node_id` for cache affinity, then least-loaded).
    fn schedule_pending_in_tx(
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

            // INV-006: Single active Activation per Session
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

            // Increment node's active_runners count in DB
            if let Some(mut node) = Self::load_node_in_tx(tx, &target_node_id)? {
                node.status.active_runners = node.status.active_runners.saturating_add(1);
                tx.execute(
                    "UPDATE nodes SET status_json = ?1 WHERE node_id = ?2",
                    params![
                        serde_json::to_string(&node.status)?,
                        target_node_id.as_str()
                    ],
                )?;
            }

            let was_rescheduled_from = if session.spec.scheduling.is_floating() {
                previous_assigned.filter(|prev| prev != &target_node_id)
            } else {
                None
            };

            let next_epoch = activation.status.lease_epoch.next();
            activation.status.lease_epoch = next_epoch;
            activation.status.phase = ActivationPhase::Scheduled;
            activation.status.assigned_node = Some(target_node_id.clone());

            session.status.active_activation_id = Some(act_id.clone());
            session.status.bound_node_id = Some(target_node_id.clone());
            session.status.queued_activation_count =
                session.status.queued_activation_count.saturating_sub(1);

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
            });
        }

        Ok(dispatches)
    }

    fn node_can_run(
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

    fn find_candidate_nodes_in_tx(
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

        // Sort: preferred affinity first, then lowest active_runners, then node_id
        matching.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        Ok(matching.into_iter().map(|(id, _, _)| id).collect())
    }

    // ========================================================================
    // 4. Daemon Execution Callbacks (with lease_epoch Fencing) & Client Patch
    // ========================================================================

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

    pub fn push_realtime_message(
        &self,
        session_id: &SessionId,
        content: Vec<openresponses_rust::MessageContent>,
    ) -> Result<usize, StoreError> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;

        if Self::load_session_in_tx(&tx, session_id)?.is_none() {
            return Err(StoreError::NotFound(format!("session {session_id}")));
        }

        let now = Utc::now().to_rfc3339();
        for block in content {
            tx.execute(
                "INSERT INTO realtime_messages (session_id, content_json, created_at) VALUES (?1, ?2, ?3)",
                params![session_id.as_str(), serde_json::to_string(&block)?, &now],
            )?;
        }

        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM realtime_messages WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| row.get(0),
        )?;
        tx.commit()?;
        Ok(count as usize)
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

            // 1. Handle declarative permission resolutions
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

            // 2. Handle declarative cancellation (desired_phase = Cancelled)
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

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    fn verify_fencing(act: &Activation, lease_epoch: LeaseEpoch) -> Result<(), StoreError> {
        if act.status.lease_epoch != lease_epoch || !act.status.phase.is_active() {
            return Err(StoreError::StaleLeaseEpoch {
                activation_id: act.metadata.id.clone(),
                expected: act.status.lease_epoch.value(),
                got: lease_epoch.value(),
            });
        }
        Ok(())
    }

    fn load_node_in_tx(
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

    fn load_session_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_id: &SessionId,
    ) -> Result<Option<Session>, StoreError> {
        Self::load_session_from_conn(tx, session_id)
    }

    fn load_session_from_conn(
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

    fn save_session_in_tx(
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

    fn load_activation_in_tx(
        tx: &rusqlite::Transaction<'_>,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, StoreError> {
        Self::load_activation_from_conn(tx, activation_id)
    }

    fn load_activation_from_conn(
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

    fn save_activation_in_tx(
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
