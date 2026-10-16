use super::default_true;
use crate::ids::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLifecycle {
    /// Terminated automatically when the spawning `Activation` finishes (`Completed`/`Failed`/`Cancelled`)
    #[default]
    Activation,
    /// Survives across multiple `Activation`s within the same `Session`; terminated when `Session` closes or is explicitly stopped
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedProcessPhase {
    Running,
    Exited,
    Killed,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLogStream {
    Stdout,
    Stderr,
    #[default]
    Combined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessInfo {
    pub process_id: ProcessId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub command: String,
    pub lifecycle: ProcessLifecycle,
    pub phase: ManagedProcessPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ProcessActionRequest {
    Start {
        command: String,
        #[serde(default = "default_true")]
        wait: bool,
        #[serde(default)]
        lifecycle: ProcessLifecycle,
        #[serde(default)]
        timeout_ms: u64,
    },
    Status {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_id: Option<String>,
    },
    Logs {
        process_id: String,
        #[serde(default)]
        stream: ProcessLogStream,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tail_lines: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<usize>,
    },
    Stop {
        process_id: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessActionResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ManagedProcessInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes: Option<Vec<ManagedProcessInfo>>,
}
