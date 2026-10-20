use super::{SqliteStore, StoreError};
use rusqlite::Connection;

impl SqliteStore {
    pub(crate) fn init_schema(conn: &Connection) -> Result<(), StoreError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS store_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

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
                source_kind         TEXT NOT NULL DEFAULT 'forked_from',
                source_session_id   TEXT NOT NULL REFERENCES sessions(session_id),
                from_context_pos    INTEGER NOT NULL,
                through_context_pos INTEGER NOT NULL,
                PRIMARY KEY (child_session_id, source_order)
            );
            CREATE INDEX IF NOT EXISTS idx_session_sources_parent ON session_sources(source_session_id);

            CREATE TABLE IF NOT EXISTS session_compactions (
                from_session_id        TEXT PRIMARY KEY REFERENCES sessions(session_id),
                compaction_session_id  TEXT NOT NULL UNIQUE REFERENCES sessions(session_id),
                continued_session_id   TEXT NOT NULL UNIQUE REFERENCES sessions(session_id),
                compaction_generation  INTEGER NOT NULL,
                summarized_through_pos INTEGER NOT NULL,
                kept_from_pos          INTEGER,
                kept_through_pos       INTEGER,
                tokens_before          INTEGER NOT NULL,
                created_at             TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_session_compactions_continued ON session_compactions(continued_session_id);
            CREATE INDEX IF NOT EXISTS idx_session_compactions_worker ON session_compactions(compaction_session_id);

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
}
