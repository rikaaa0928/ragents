use crate::args::{Cli, Commands, NodeSubcommand, SessionSubcommand};
use crate::helpers::{
    build_scheduling_mode, parse_extension_specs, parse_session_sources,
    watch_activation_to_completion,
};
use anyhow::{anyhow, Result};
use futures::StreamExt;
use openresponses_rust::Item;
use ragents_apiserver::{
    serve_tcp, ApiServerExtensions, ApiServerState, AuthnPluginSpec, ConfiguredAuthnPlugin,
    SqliteStore, StaticTokenAuthnPlugin, DEFAULT_LEASE_TTL_MS,
};
use ragents_client::RagentsClient;
use ragents_core::run_subprocess_worker;
use ragents_daemon::{AgentDaemon, DaemonConfig};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

fn build_apiserver_extensions(
    auth_token: &Option<String>,
    authn_wasm: Option<PathBuf>,
    authn_config: Option<String>,
) -> Result<ApiServerExtensions> {
    let mut exts = ApiServerExtensions::new();
    if let Some(wasm_path) = authn_wasm {
        let cfg_val = match authn_config {
            Some(raw) => serde_json::from_str(&raw)
                .map_err(|e| anyhow!("invalid --authn-config JSON: {e}"))?,
            None => match auth_token {
                Some(tok) => serde_json::json!({ "token": tok }),
                None => serde_json::json!({}),
            },
        };
        let plugin = ConfiguredAuthnPlugin::new(AuthnPluginSpec {
            id: "wasm-authn".to_string(),
            driver: ExtensionDriverConfig::Wasm {
                path: wasm_path,
                allowed_dirs: vec![],
            },
            scopes: vec![AuthScope::ClientHttp, AuthScope::DaemonTunnel],
            timeout_ms: 2000,
            config: cfg_val,
        });
        exts.add_authn(Arc::new(plugin));
    } else if let Some(ref tok) = auth_token {
        if !tok.trim().is_empty() {
            exts.add_authn(Arc::new(StaticTokenAuthnPlugin::new(tok.clone())));
        }
    }
    Ok(exts)
}

pub async fn run_cli(cli: Cli) -> Result<()> {
    let explicit_server = cli
        .server
        .or_else(|| std::env::var("RAGENTS_SERVER_URL").ok());
    let server_url = explicit_server
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let auth_token = cli
        .auth_token
        .or_else(|| std::env::var("RAGENTS_AUTH_TOKEN").ok());
    let client = RagentsClient::new(&server_url).with_auth_token(auth_token.clone());

    match cli.command {
        Commands::Up {
            bind,
            db,
            node_id,
            daemon_config,
            authn_wasm,
            authn_config,
            workdir,
            prompt,
            model,
        } => {
            let store = SqliteStore::open(&db)?;
            let exts = build_apiserver_extensions(&auth_token, authn_wasm, authn_config)?;
            let state = ApiServerState::new(store, DEFAULT_LEASE_TTL_MS).with_extensions(exts);
            let cancel = CancellationToken::new();
            let listener = TcpListener::bind(&bind).await?;
            let addr = serve_tcp(listener, state, cancel.clone()).await?;
            let local_server_url = format!("http://{addr}");

            let mut d_cfg = match daemon_config {
                Some(path) => DaemonConfig::load_from_file(path)?,
                None => DaemonConfig::default_local(NodeId::new(&node_id), &local_server_url),
            };
            d_cfg.server_url = local_server_url.clone();
            if d_cfg.auth_token.is_none() {
                d_cfg.auth_token = auth_token.clone();
            }
            let registered_node_id = d_cfg.node_id.clone();

            let daemon = AgentDaemon::new(d_cfg);
            daemon.start(cancel.clone()).await?;

            println!(
                "ragents up: api-server listening at {local_server_url}, daemon '{}' registered",
                daemon.config().node_id
            );

            if let Some(prompt_text) = prompt {
                let up_client =
                    RagentsClient::new(&local_server_url).with_auth_token(auth_token.clone());
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
            authn_wasm,
            authn_config,
        } => {
            let store = SqliteStore::open(&db)?;
            let exts = build_apiserver_extensions(&auth_token, authn_wasm, authn_config)?;
            let state = ApiServerState::new(store, lease_ttl_ms).with_extensions(exts);
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
            if auth_token.is_some() {
                d_cfg.auth_token = auth_token;
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
                        "{}\tmode={}\tphase={:?}\trole={:?}\tgen={}\tnode={}\titems={}",
                        s.metadata.id,
                        mode,
                        s.status.phase,
                        s.status.compaction.role,
                        s.status.compaction.generation,
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
                (act.spec.session_id, ActivationId::new(act.metadata.id))
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
        Commands::Steer {
            session_id,
            messages,
        } => {
            let sess_id = SessionId::new(&session_id);
            let blocks = messages
                .into_iter()
                .map(|text| openresponses_rust::MessageContent::InputText { text })
                .collect();
            let resp = client
                .push_realtime_message(&sess_id, PushRealtimeMessageRequest { content: blocks })
                .await?;
            let effective_sid = resp.target_session_id.as_ref().unwrap_or(&sess_id);
            if let Some(ref act_id) = resp.spawned_activation_id {
                println!(
                    "Enqueued real-time message block(s) into idle session {} and automatically spawned activation {} (total pending: {})",
                    effective_sid, act_id, resp.queued_count
                );
            } else {
                println!(
                    "Enqueued real-time message block(s) into session {} (total pending: {})",
                    effective_sid, resp.queued_count
                );
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
