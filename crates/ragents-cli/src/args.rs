use clap::{Parser, Subcommand, ValueEnum};
use ragents_apiserver::DEFAULT_LEASE_TTL_MS;
use ragents_protocol::{DesiredSessionPhase, PermissionDecision};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "ragents",
    version,
    about = "Declarative cross-machine AI agent cluster management system"
)]
pub struct Cli {
    /// API server base URL (overrides RAGENTS_SERVER_URL env var)
    #[arg(long, global = true)]
    pub server: Option<String>,

    /// Optional Bearer auth token for API server authentication (overrides RAGENTS_AUTH_TOKEN env var)
    #[arg(long, global = true)]
    pub auth_token: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start embedded api-server + local agent-daemon in a single process (All-in-One mode)
    Up {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = ".ragents/control.sqlite3")]
        db: PathBuf,
        #[arg(long, default_value = "node_local")]
        node_id: String,
        #[arg(long)]
        daemon_config: Option<PathBuf>,
        /// Optional WASM Authn plugin path (`ragent_authn_extension.wasm`) for api-server
        #[arg(long)]
        authn_wasm: Option<PathBuf>,
        /// Optional JSON config for the Authn WASM plugin
        #[arg(long)]
        authn_config: Option<String>,
        /// Optional working directory for Pinned one-shot run
        #[arg(long)]
        workdir: Option<PathBuf>,
        /// Optional one-shot prompt to run immediately after starting `up`
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long)]
        model: Option<String>,
        /// Override automatic `/v1/models` discovery on the local daemon (default: true)
        #[arg(long)]
        auto_discover_models: Option<bool>,
    },
    /// Run standalone control-plane api-server
    Apiserver {
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long, default_value = ".ragents/control.sqlite3")]
        db: PathBuf,
        #[arg(long, default_value_t = DEFAULT_LEASE_TTL_MS)]
        lease_ttl_ms: u64,
        /// Optional WASM Authn plugin path (`ragent_authn_extension.wasm`)
        #[arg(long)]
        authn_wasm: Option<PathBuf>,
        /// Optional JSON config for the Authn WASM plugin
        #[arg(long)]
        authn_config: Option<String>,
    },
    /// Run standalone node agent-daemon (kubelet)
    Daemon {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, default_value = "node_local")]
        node_id: String,
        /// Override automatic `/v1/models` discovery on the daemon (default: true)
        #[arg(long)]
        auto_discover_models: Option<bool>,
    },
    /// Inspect cluster nodes and their supported models
    Node {
        #[command(subcommand)]
        cmd: NodeSubcommand,
    },
    /// Declarative session management (Pinned & Floating modes, lineage fork)
    Session {
        #[command(subcommand)]
        cmd: SessionSubcommand,
    },
    /// Submit a declarative user prompt activation and watch progress
    Run {
        /// Existing session ID (if omitted, creates a new session automatically)
        #[arg(long)]
        session: Option<String>,
        /// Target node ID (required together with --workdir for Pinned mode)
        #[arg(long)]
        node: Option<String>,
        /// Target workspace directory path on the node (required together with --node for Pinned mode)
        #[arg(long)]
        workdir: Option<PathBuf>,
        /// Explicitly mark session as Floating (no node/workdir binding)
        #[arg(long, default_value_t = false)]
        floating: bool,
        /// Model name (must be in target node's supported_models)
        #[arg(long)]
        model: Option<String>,
        /// System prompt override
        #[arg(long)]
        system_prompt: Option<String>,
        /// Extensions in `name=path` format
        #[arg(long = "extension")]
        extensions: Vec<String>,
        /// Wait and stream events until activation finishes
        #[arg(long, default_value_t = true)]
        wait: bool,
        /// User prompt text
        prompt: String,
    },
    /// Push one or more real-time message blocks into a session's in-turn Realtime Queue
    Steer {
        session_id: String,
        /// One or more text blocks to enqueue into the session's Realtime Queue
        #[arg(required = true)]
        messages: Vec<String>,
    },
    /// Declaratively cancel an active or queued activation (`desired_phase = Cancelled`)
    Cancel { activation_id: String },
    /// Declaratively respond to a permission request on an activation
    Approve {
        activation_id: String,
        request_id: String,
        #[arg(long, value_enum, default_value_t = CliPermissionDecision::AllowOnce)]
        decision: CliPermissionDecision,
        /// Optional custom directory/file path to approve instead of the suggested directory
        #[arg(long)]
        path: Option<PathBuf>,
    },
    /// Watch real-time cluster or session events via SSE
    Watch {
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = 0)]
        after_rv: u64,
    },
    /// Internal subprocess worker entrypoint for SubprocessExecutor (single-binary re-exec)
    #[command(hide = true, name = "internal-core-worker")]
    InternalCoreWorker {
        #[arg(long)]
        ipc_addr: String,
        #[arg(long)]
        ipc_token: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum NodeSubcommand {
    List,
    Get { node_id: String },
}

#[derive(Debug, Subcommand)]
pub enum SessionSubcommand {
    Create {
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        node: Option<String>,
        #[arg(long)]
        workdir: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        floating: bool,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        system_prompt: Option<String>,
        /// Extensions in `name=path` format
        #[arg(long = "extension")]
        extensions: Vec<String>,
        /// Inherit context slice in `session_id:from..through` format
        #[arg(long = "source")]
        sources: Vec<String>,
        /// Optional initial user prompt
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, default_value_t = true)]
        wait: bool,
    },
    List,
    Get {
        session_id: String,
    },
    Context {
        session_id: String,
    },
    Patch {
        session_id: String,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        system_prompt: Option<String>,
        #[arg(long, value_enum)]
        phase: Option<CliDesiredSessionPhase>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliPermissionDecision {
    Deny,
    AllowOnce,
    AllowActivation,
    AllowSession,
    #[value(alias = "allow-project")]
    AllowWorkspace,
    #[value(alias = "allow-always")]
    AllowNode,
}

impl From<CliPermissionDecision> for PermissionDecision {
    fn from(value: CliPermissionDecision) -> Self {
        match value {
            CliPermissionDecision::Deny => PermissionDecision::Deny,
            CliPermissionDecision::AllowOnce => PermissionDecision::AllowOnce,
            CliPermissionDecision::AllowActivation => PermissionDecision::AllowActivation,
            CliPermissionDecision::AllowSession => PermissionDecision::AllowSession,
            CliPermissionDecision::AllowWorkspace => PermissionDecision::AllowWorkspace,
            CliPermissionDecision::AllowNode => PermissionDecision::AllowNode,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliDesiredSessionPhase {
    Open,
    Closed,
    Archived,
}

impl From<CliDesiredSessionPhase> for DesiredSessionPhase {
    fn from(value: CliDesiredSessionPhase) -> Self {
        match value {
            CliDesiredSessionPhase::Open => DesiredSessionPhase::Open,
            CliDesiredSessionPhase::Closed => DesiredSessionPhase::Closed,
            CliDesiredSessionPhase::Archived => DesiredSessionPhase::Archived,
        }
    }
}
