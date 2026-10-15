use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use openresponses_rust::Item;
use ragents_apiserver::{serve_tcp, ApiServerState, SqliteStore, DEFAULT_LEASE_TTL_MS};
use ragents_client::RagentsClient;
use ragents_core::{extract_item_text, run_subprocess_worker};
use ragents_daemon::{AgentDaemon, DaemonConfig};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

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
        /// Optional working directory for Pinned one-shot run
        #[arg(long)]
        workdir: Option<PathBuf>,
        /// Optional one-shot prompt to run immediately after starting `up`
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long)]
        model: Option<String>,
    },
    /// Run standalone control-plane api-server
    Apiserver {
        #[arg(long, default_value = "0.0.0.0:8080")]
        bind: String,
        #[arg(long, default_value = ".ragents/control.sqlite3")]
        db: PathBuf,
        #[arg(long, default_value_t = DEFAULT_LEASE_TTL_MS)]
        lease_ttl_ms: u64,
    },
    /// Run standalone node agent-daemon (kubelet)
    Daemon {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, default_value = "node_local")]
        node_id: String,
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

pub async fn run_cli(cli: Cli) -> Result<()> {
    let explicit_server = cli
        .server
        .or_else(|| std::env::var("RAGENTS_SERVER_URL").ok());
    let server_url = explicit_server
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let client = RagentsClient::new(&server_url);

    match cli.command {
        Commands::Up {
            bind,
            db,
            node_id,
            daemon_config,
            workdir,
            prompt,
            model,
        } => {
            let store = SqliteStore::open(&db)?;
            let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS);
            let cancel = CancellationToken::new();
            let listener = TcpListener::bind(&bind).await?;
            let addr = serve_tcp(listener, state, cancel.clone()).await?;
            let local_server_url = format!("http://{addr}");

            let mut d_cfg = match daemon_config {
                Some(path) => DaemonConfig::load_from_file(path)?,
                None => DaemonConfig::default_local(NodeId::new(&node_id), &local_server_url),
            };
            d_cfg.server_url = local_server_url.clone();
            let registered_node_id = d_cfg.node_id.clone();

            let daemon = AgentDaemon::new(d_cfg);
            daemon.start(cancel.clone()).await?;

            println!(
                "ragents up: api-server listening at {local_server_url}, daemon '{}' registered",
                daemon.config().node_id
            );

            if let Some(prompt_text) = prompt {
                let up_client = RagentsClient::new(&local_server_url);
                let scheduling = match workdir {
                    Some(w) => {
                        let abs_w = if w.is_absolute() {
                            w
                        } else {
                            std::env::current_dir()?.join(w)
                        };
                        SchedulingMode::Pinned {
                            node_id: registered_node_id,
                            workspace_path: abs_w,
                        }
                    }
                    None => SchedulingMode::Floating {
                        node_selector: BTreeMap::new(),
                    },
                };
                let created = up_client
                    .create_session(CreateSessionRequest {
                        session_id: None,
                        labels: BTreeMap::new(),
                        spec: SessionSpec {
                            scheduling,
                            sources: vec![],
                            producer: None,
                            desired_phase: DesiredSessionPhase::Open,
                            system_prompt: None,
                            model: model.map(ModelRequestSpec::new),
                            extensions: vec![],
                            permissions: PermissionPolicyLayer::default(),
                        },
                        initial_activation: Some(InitialActivationRequest {
                            input: vec![Item::user_message(prompt_text)],
                            model_override: None,
                        }),
                    })
                    .await?;
                let sess_id = SessionId::new(created.session.metadata.id);
                let act_id = ActivationId::new(
                    created
                        .initial_activation
                        .expect("initial activation created")
                        .metadata
                        .id,
                );
                watch_activation_to_completion(&up_client, &sess_id, &act_id).await?;
                cancel.cancel();
            } else {
                tokio::signal::ctrl_c().await?;
                println!("Shutting down ragents up...");
                cancel.cancel();
            }
            Ok(())
        }
        Commands::Apiserver {
            bind,
            db,
            lease_ttl_ms,
        } => {
            let store = SqliteStore::open(&db)?;
            let state = ApiServerState::new(store, lease_ttl_ms);
            let cancel = CancellationToken::new();
            let listener = TcpListener::bind(&bind).await?;
            let addr = serve_tcp(listener, state, cancel.clone()).await?;
            println!("ragents-apiserver listening on http://{addr}");
            tokio::signal::ctrl_c().await?;
            cancel.cancel();
            Ok(())
        }
        Commands::Daemon { config, node_id } => {
            let mut d_cfg = match config {
                Some(path) => DaemonConfig::load_from_file(path)?,
                None => DaemonConfig::default_local(NodeId::new(&node_id), &server_url),
            };
            if let Some(ref exp_srv) = explicit_server {
                d_cfg.server_url = exp_srv.clone();
            }
            let cancel = CancellationToken::new();
            let daemon = AgentDaemon::new(d_cfg);
            daemon.start(cancel.clone()).await?;
            println!(
                "ragents-daemon '{}' connected to {}",
                daemon.config().node_id,
                daemon.config().server_url
            );
            tokio::signal::ctrl_c().await?;
            cancel.cancel();
            Ok(())
        }
        Commands::Node { cmd } => match cmd {
            NodeSubcommand::List => {
                let nodes = client.list_nodes().await?;
                for n in nodes {
                    println!(
                        "{}\tphase={:?}\trunners={}/{}\tmodels={:?}",
                        n.metadata.id,
                        n.status.phase,
                        n.status.active_runners,
                        n.status.max_concurrent_runners,
                        n.status.supported_models
                    );
                }
                Ok(())
            }
            NodeSubcommand::Get { node_id } => {
                let node = client
                    .get_node(&NodeId::new(&node_id))
                    .await?
                    .ok_or_else(|| anyhow!("Node '{node_id}' not found"))?;
                println!("{}", serde_json::to_string_pretty(&node)?);
                Ok(())
            }
        },
        Commands::Session { cmd } => match cmd {
            SessionSubcommand::Create {
                id,
                node,
                workdir,
                floating,
                model,
                system_prompt,
                extensions,
                sources,
                prompt,
                wait,
            } => {
                let scheduling = build_scheduling_mode(node, workdir, floating)?;
                let parsed_exts = parse_extension_specs(&extensions)?;
                let parsed_sources = parse_session_sources(&sources)?;

                let created = client
                    .create_session(CreateSessionRequest {
                        session_id: id.map(SessionId::new),
                        labels: BTreeMap::new(),
                        spec: SessionSpec {
                            scheduling,
                            sources: parsed_sources,
                            producer: None,
                            desired_phase: DesiredSessionPhase::Open,
                            system_prompt,
                            model: model.map(ModelRequestSpec::new),
                            extensions: parsed_exts,
                            permissions: PermissionPolicyLayer::default(),
                        },
                        initial_activation: prompt.map(|p| InitialActivationRequest {
                            input: vec![Item::user_message(p)],
                            model_override: None,
                        }),
                    })
                    .await?;

                let sess_id = SessionId::new(created.session.metadata.id.clone());
                println!("Created session: {sess_id}");

                if let Some(act) = created.initial_activation {
                    let act_id = ActivationId::new(act.metadata.id);
                    println!("Submitted initial activation: {act_id}");
                    if wait {
                        watch_activation_to_completion(&client, &sess_id, &act_id).await?;
                    }
                }
                Ok(())
            }
            SessionSubcommand::List => {
                let sessions = client.list_sessions().await?;
                for s in sessions {
                    let mode = if s.spec.scheduling.is_pinned() {
                        "pinned"
                    } else {
                        "floating"
                    };
                    println!(
                        "{}\tmode={}\tphase={:?}\tnode={}\titems={}",
                        s.metadata.id,
                        mode,
                        s.status.phase,
                        s.status
                            .bound_node_id
                            .as_ref()
                            .map(|n| n.as_str())
                            .unwrap_or("-"),
                        s.status.effective_context_item_count
                    );
                }
                Ok(())
            }
            SessionSubcommand::Get { session_id } => {
                let session = client
                    .get_session(&SessionId::new(&session_id))
                    .await?
                    .ok_or_else(|| anyhow!("Session '{session_id}' not found"))?;
                println!("{}", serde_json::to_string_pretty(&session)?);
                Ok(())
            }
            SessionSubcommand::Context { session_id } => {
                let ctx = client
                    .get_session_context(&SessionId::new(&session_id))
                    .await?;
                println!("{}", serde_json::to_string_pretty(&ctx)?);
                Ok(())
            }
            SessionSubcommand::Patch {
                session_id,
                model,
                system_prompt,
                phase,
            } => {
                let updated = client
                    .patch_session(
                        &SessionId::new(&session_id),
                        PatchSessionSpecRequest {
                            desired_phase: phase.map(Into::into),
                            system_prompt,
                            model: model.map(ModelRequestSpec::new),
                            extensions: None,
                            permissions: None,
                        },
                    )
                    .await?;
                println!("{}", serde_json::to_string_pretty(&updated)?);
                Ok(())
            }
        },
        Commands::Run {
            session,
            node,
            workdir,
            floating,
            model,
            system_prompt,
            extensions,
            wait,
            prompt,
        } => {
            let (sess_id, act_id) = if let Some(existing_sess_id) = session {
                let s_id = SessionId::new(existing_sess_id);
                let act = client
                    .create_activation(CreateActivationRequest {
                        activation_id: None,
                        labels: BTreeMap::new(),
                        spec: ActivationSpec {
                            session_id: s_id.clone(),
                            submission_kind: SubmissionKind::Normal,
                            input: vec![Item::user_message(prompt)],
                            model_override: model.map(ModelRequestSpec::new),
                            desired_phase: DesiredActivationPhase::Running,
                            interaction_responses: BTreeMap::new(),
                        },
                    })
                    .await?;
                (s_id, ActivationId::new(act.metadata.id))
            } else {
                let scheduling = build_scheduling_mode(node, workdir, floating)?;
                let parsed_exts = parse_extension_specs(&extensions)?;
                let created = client
                    .create_session(CreateSessionRequest {
                        session_id: None,
                        labels: BTreeMap::new(),
                        spec: SessionSpec {
                            scheduling,
                            sources: vec![],
                            producer: None,
                            desired_phase: DesiredSessionPhase::Open,
                            system_prompt,
                            model: model.map(ModelRequestSpec::new),
                            extensions: parsed_exts,
                            permissions: PermissionPolicyLayer::default(),
                        },
                        initial_activation: Some(InitialActivationRequest {
                            input: vec![Item::user_message(prompt)],
                            model_override: None,
                        }),
                    })
                    .await?;
                let s_id = SessionId::new(created.session.metadata.id);
                let a_id = ActivationId::new(
                    created
                        .initial_activation
                        .expect("initial activation")
                        .metadata
                        .id,
                );
                (s_id, a_id)
            };

            println!("session={sess_id} activation={act_id}");
            if wait {
                watch_activation_to_completion(&client, &sess_id, &act_id).await?;
            }
            Ok(())
        }
        Commands::Cancel { activation_id } => {
            let act = client
                .cancel_activation(&ActivationId::new(&activation_id))
                .await?;
            println!(
                "Activation {} desired_phase={:?} status.phase={:?}",
                act.metadata.id, act.spec.desired_phase, act.status.phase
            );
            Ok(())
        }
        Commands::Approve {
            activation_id,
            request_id,
            decision,
            path,
        } => {
            let act = client
                .respond_permission_with_path(
                    &ActivationId::new(&activation_id),
                    PermissionRequestId::new(&request_id),
                    decision.into(),
                    path.clone(),
                )
                .await?;
            println!(
                "Activation {} permission {} resolved (decision={:?}, override_path={:?}), status.phase={:?}",
                act.metadata.id, request_id, decision, path, act.status.phase
            );
            Ok(())
        }
        Commands::Watch { session, after_rv } => {
            let sess_id = session.map(SessionId::new);
            let mut stream = client.watch_events(sess_id.as_ref(), after_rv).await?;
            while let Some(ev) = stream.next().await {
                let env = ev?;
                println!("{}", serde_json::to_string(&env)?);
            }
            Ok(())
        }
        Commands::InternalCoreWorker {
            ipc_addr,
            ipc_token,
        } => {
            run_subprocess_worker(&ipc_addr, &ipc_token).await?;
            Ok(())
        }
    }
}

pub fn build_scheduling_mode(
    node: Option<String>,
    workdir: Option<PathBuf>,
    floating: bool,
) -> Result<SchedulingMode> {
    if floating {
        if node.is_some() || workdir.is_some() {
            bail!("--floating cannot be combined with --node or --workdir");
        }
        return Ok(SchedulingMode::Floating {
            node_selector: BTreeMap::new(),
        });
    }

    match (node, workdir) {
        (Some(n), Some(w)) => {
            let abs_w = if w.is_absolute() {
                w
            } else {
                std::env::current_dir()?.join(w)
            };
            let mode = SchedulingMode::Pinned {
                node_id: NodeId::new(n),
                workspace_path: abs_w,
            };
            mode.validate()?;
            Ok(mode)
        }
        (None, None) => Ok(SchedulingMode::Floating {
            node_selector: BTreeMap::new(),
        }),
        _ => bail!(
            "Pinned task requires BOTH --node <node_id> and --workdir <path>; omit both for Floating mode"
        ),
    }
}

fn parse_extension_specs(raw_exts: &[String]) -> Result<Vec<ExtensionSpec>> {
    let mut specs = Vec::new();
    for raw in raw_exts {
        let (name, path) = raw
            .split_once('=')
            .ok_or_else(|| anyhow!("Extension must be in 'name=path' format, got '{raw}'"))?;
        specs.push(ExtensionSpec {
            name: name.trim().to_string(),
            path: path.trim().to_string(),
            enabled: true,
            config: serde_json::json!({}),
        });
    }
    Ok(specs)
}

fn parse_session_sources(raw_sources: &[String]) -> Result<Vec<SessionSource>> {
    let mut sources = Vec::new();
    for raw in raw_sources {
        let (sess_part, range_part) = raw.split_once(':').ok_or_else(|| {
            anyhow!("Source must be in 'session_id:from..through' format, got '{raw}'")
        })?;
        let (from_str, through_str) = range_part
            .split_once("..")
            .ok_or_else(|| anyhow!("Source range must be 'from..through', got '{range_part}'"))?;
        let from: u64 = from_str.parse().context("invalid from_context_pos")?;
        let through: u64 = through_str.parse().context("invalid through_context_pos")?;
        let src = SessionSource {
            kind: SessionSourceKind::ForkedFrom,
            session_id: SessionId::new(sess_part.trim()),
            from_context_pos: ContextPos::new(from),
            through_context_pos: ContextPos::new(through),
        };
        src.validate()?;
        sources.push(src);
    }
    Ok(sources)
}

pub async fn watch_activation_to_completion(
    client: &RagentsClient,
    session_id: &SessionId,
    activation_id: &ActivationId,
) -> Result<()> {
    let mut stream = client.watch_events(Some(session_id), 0).await?;
    while let Some(item) = stream.next().await {
        let env = item?;
        if env.activation_id.as_ref() != Some(activation_id) {
            continue;
        }
        match &env.event {
            SessionEvent::ActivationScheduled {
                node_id,
                lease_epoch,
            } => {
                println!(
                    "[event] scheduled on node={} (epoch={})",
                    node_id, lease_epoch
                );
            }
            SessionEvent::ActivationRescheduled {
                from_node,
                to_node,
                lease_epoch,
                ..
            } => {
                println!(
                    "[event] rescheduled from={} to={} (epoch={})",
                    from_node, to_node, lease_epoch
                );
            }
            SessionEvent::ActivationStarted { node_id, .. } => {
                println!("[event] started on node={node_id}");
            }
            SessionEvent::TurnStarted => {
                println!("[event] turn started");
            }
            SessionEvent::ToolCallStarted { name, call_id } => {
                println!("[event] tool call started: {name} ({call_id})");
            }
            SessionEvent::ToolCallFinished {
                name,
                success,
                duration_ms,
                ..
            } => {
                println!("[event] tool call finished: {name} (success={success}, {duration_ms}ms)");
            }
            SessionEvent::PermissionRequested { request } => {
                let suggested = request
                    .capability
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| format!("{:?}", request.capability));
                let orig = request
                    .original_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| suggested.clone());
                println!(
                    "[permission required] request_id={}\n  original_path : {}\n  suggested_path: {} ({:?})\n  reason        : {:?}\n  Approve default : ragents --server <url> approve {} {} --decision allow-session\n  Approve custom  : ragents --server <url> approve {} {} --decision <allow-once|allow-activation|allow-session|allow-workspace|allow-node> --path <custom_dir>",
                    request.id,
                    orig,
                    suggested,
                    request.capability,
                    request.reason,
                    activation_id,
                    request.id,
                    activation_id,
                    request.id
                );
            }
            SessionEvent::PermissionResolved {
                request_id,
                decision,
                override_path,
                ..
            } => {
                println!(
                    "[event] permission resolved: request_id={} decision={:?} override_path={:?}",
                    request_id, decision, override_path
                );
            }
            SessionEvent::ActivationCompleted { .. } => {
                let ctx = client.get_session_context(session_id).await?;
                if let Some(last_msg) = ctx
                    .effective_items
                    .iter()
                    .rev()
                    .find(|item| matches!(item, Item::Message { .. }))
                {
                    let text = extract_item_text(last_msg);
                    if !text.is_empty() {
                        println!("\n{text}");
                    }
                }
                break;
            }
            SessionEvent::ActivationFailed { error } => {
                bail!("Activation failed: {error}");
            }
            SessionEvent::ActivationCancelled => {
                println!("[event] activation cancelled");
                break;
            }
            SessionEvent::ActivationInterrupted { reason } => {
                bail!("Activation interrupted: {reason}");
            }
            _ => {}
        }
    }
    Ok(())
}
