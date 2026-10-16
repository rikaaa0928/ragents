use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use openresponses_rust::Item;
use ragents_client::RagentsClient;
use ragents_core::extract_item_text;
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::path::PathBuf;

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

pub(crate) fn parse_extension_specs(raw_exts: &[String]) -> Result<Vec<ExtensionSpec>> {
    let mut specs = Vec::new();
    for raw in raw_exts {
        let (name, rest) = raw.split_once('=').ok_or_else(|| {
            anyhow!("Extension must be in 'name=path[:dir1,dir2]' format, got '{raw}'")
        })?;
        let (path, allowed_dirs) = match rest.split_once(':') {
            Some((p, dirs_str)) => {
                let dirs = dirs_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                (p, dirs)
            }
            None => (rest, Vec::new()),
        };
        specs.push(ExtensionSpec {
            name: name.trim().to_string(),
            path: path.trim().to_string(),
            enabled: true,
            config: serde_json::json!({}),
            allowed_dirs,
        });
    }
    Ok(specs)
}

pub(crate) fn parse_session_sources(raw_sources: &[String]) -> Result<Vec<SessionSource>> {
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
    let mut rendered_any_turn_text = false;
    let mut turn_streamed_text = String::new();
    let mut turn_streaming_line_open = false;

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
                turn_streamed_text.clear();
                turn_streaming_line_open = false;
                println!("[event] turn started");
            }
            SessionEvent::ModelReasoningDelta { delta } => {
                use std::io::Write;
                print!("{delta}");
                let _ = std::io::stdout().flush();
                turn_streaming_line_open = true;
            }
            SessionEvent::ModelTextDelta { delta } => {
                use std::io::Write;
                print!("{delta}");
                let _ = std::io::stdout().flush();
                turn_streamed_text.push_str(delta);
                rendered_any_turn_text = true;
                turn_streaming_line_open = true;
            }
            SessionEvent::TurnCompleted { items, .. } => {
                let mut authoritative_text = String::new();
                for it in items {
                    if matches!(it, Item::Message { .. }) {
                        let t = extract_item_text(it);
                        if !t.is_empty() {
                            if !authoritative_text.is_empty() {
                                authoritative_text.push('\n');
                            }
                            authoritative_text.push_str(&t);
                        }
                    }
                }

                if !authoritative_text.is_empty() {
                    if turn_streamed_text.is_empty() {
                        println!("{authoritative_text}");
                        rendered_any_turn_text = true;
                        turn_streaming_line_open = false;
                    } else if authoritative_text == turn_streamed_text {
                        if turn_streaming_line_open {
                            println!();
                            turn_streaming_line_open = false;
                        }
                    } else if let Some(missing_suffix) =
                        authoritative_text.strip_prefix(&turn_streamed_text)
                    {
                        println!("{missing_suffix}");
                        rendered_any_turn_text = true;
                        turn_streaming_line_open = false;
                    } else {
                        if turn_streaming_line_open {
                            println!();
                        }
                        println!("{authoritative_text}");
                        rendered_any_turn_text = true;
                        turn_streaming_line_open = false;
                    }
                } else if turn_streaming_line_open {
                    println!();
                    turn_streaming_line_open = false;
                }
                turn_streamed_text.clear();
            }
            SessionEvent::ToolCallStarted { name, call_id } => {
                if turn_streaming_line_open {
                    println!();
                    turn_streaming_line_open = false;
                }
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
            SessionEvent::BackgroundProcessStarted { process } => {
                println!(
                    "[event] background process started: id={} pid={:?} lifecycle={:?} cmd={}",
                    process.process_id, process.pid, process.lifecycle, process.command
                );
            }
            SessionEvent::BackgroundProcessExited { process } => {
                println!(
                    "[event] background process exited: id={} pid={:?} phase={:?} exit_code={:?} ({}ms)",
                    process.process_id,
                    process.pid,
                    process.phase,
                    process.exit_code,
                    process.duration_ms
                );
            }
            SessionEvent::RealtimeMessagesDrained { count, batch_seq } => {
                println!(
                    "[event] drained {} real-time message block(s) into context (batch_seq={})",
                    count,
                    batch_seq.value()
                );
            }
            SessionEvent::ActivationCompleted { .. } => {
                if turn_streaming_line_open {
                    println!();
                }
                if !rendered_any_turn_text {
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
