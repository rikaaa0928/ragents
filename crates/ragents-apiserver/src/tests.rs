use super::*;
use chrono::{Duration, Utc};
use openresponses_rust::Item;
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn register_test_node(
    store: &SqliteStore,
    node_id: &str,
    models: Vec<&str>,
    lease_ttl_ms: u64,
) -> (Node, Vec<ScheduledDispatch>) {
    store
        .register_node(
            NodeId::new(node_id),
            format!("{node_id}.local"),
            "macos".to_string(),
            "aarch64".to_string(),
            BTreeMap::new(),
            models.into_iter().map(String::from).collect(),
            Some("gpt-4.1".to_string()),
            vec!["shell".to_string(), "file_editor".to_string()],
            4,
            lease_ttl_ms,
        )
        .expect("register node")
}

#[test]
fn test_pinned_session_never_migrates_across_nodes() {
    let store = SqliteStore::open_in_memory().expect("open store");
    register_test_node(&store, "node_a", vec!["gpt-4.1"], 10_000);
    register_test_node(&store, "node_b", vec!["gpt-4.1"], 60_000);

    let (created, dispatches) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_pinned_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_a"),
                    workspace_path: PathBuf::from("/tmp/workspace/proj"),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: Some("Pinned prompt".to_string()),
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("edit main.rs")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create pinned session");

    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].node_id, NodeId::new("node_a"));
    let act_id = ActivationId::new(
        created
            .initial_activation
            .as_ref()
            .unwrap()
            .metadata
            .id
            .clone(),
    );

    let future_time = Utc::now() + Duration::seconds(20);
    let rescheduled = store
        .patrol_expired_leases(future_time)
        .expect("patrol leases");

    assert!(rescheduled.is_empty());
    let act = store.get_activation(&act_id).unwrap().unwrap();
    assert_eq!(act.status.phase, ActivationPhase::Interrupted);

    let (act2, dispatches2) = store
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: SessionId::new("sess_pinned_1"),
                submission_kind: SubmissionKind::FollowUp,
                input: vec![Item::user_message("try again")],
                model_override: None,
                disabled_tools: vec![],
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .expect("create second activation");
    assert!(dispatches2.is_empty());
    assert_eq!(act2.status.phase, ActivationPhase::Queued);
}

#[test]
fn test_floating_session_failover_and_fencing_token() {
    let store = SqliteStore::open_in_memory().expect("open store");
    register_test_node(&store, "node_a", vec!["claude-sonnet-4"], 10_000);

    let (created, dispatches) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_float_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("claude-sonnet-4")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("analyze algorithm")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create floating session");

    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].node_id, NodeId::new("node_a"));
    assert_eq!(dispatches[0].lease_epoch, LeaseEpoch::new(1));
    let act_id = ActivationId::new(
        created
            .initial_activation
            .as_ref()
            .unwrap()
            .metadata
            .id
            .clone(),
    );

    register_test_node(&store, "node_b", vec!["claude-sonnet-4"], 120_000);
    let future_time = Utc::now() + Duration::seconds(20);
    let failover_dispatches = store
        .patrol_expired_leases(future_time)
        .expect("patrol expired node_a");

    assert_eq!(failover_dispatches.len(), 1);
    assert_eq!(failover_dispatches[0].node_id, NodeId::new("node_b"));
    let new_epoch = failover_dispatches[0].lease_epoch;
    assert!(new_epoch.value() > 1);

    let stale_err = store
        .commit_batch(
            &SessionId::new("sess_float_1"),
            &act_id,
            LeaseEpoch::new(1),
            ItemBatchKind::ModelOutput,
            Some(TurnId::new("turn_1")),
            Some("resp_stale".to_string()),
            vec![Item::assistant_message("zombie output")],
            BatchCompanionEvent::TurnCompleted {
                response_id: Some("resp_stale".to_string()),
                usage: None,
            },
        )
        .expect_err("stale lease_epoch must fail");
    assert!(matches!(stale_err, StoreError::StaleLeaseEpoch { .. }));

    let cfg = ConfigRevision::new_merged(
        "Default prompt".to_string(),
        ModelRequestSpec::new("claude-sonnet-4"),
        vec![],
    );
    let perm = PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default());
    store
        .accept_activation(
            &SessionId::new("sess_float_1"),
            &act_id,
            new_epoch,
            cfg,
            perm,
        )
        .expect("node_b accept activation");

    store
        .commit_batch(
            &SessionId::new("sess_float_1"),
            &act_id,
            new_epoch,
            ItemBatchKind::ModelOutput,
            Some(TurnId::new("turn_1")),
            Some("resp_ok".to_string()),
            vec![Item::assistant_message("completed on node_b")],
            BatchCompanionEvent::TurnCompleted {
                response_id: Some("resp_ok".to_string()),
                usage: None,
            },
        )
        .expect("node_b commit batch");

    let (completed_act, _) = store
        .complete_activation(
            &SessionId::new("sess_float_1"),
            &act_id,
            new_epoch,
            Some(TurnId::new("turn_1")),
            ActivationTerminalOutcome::Succeeded { usage: None },
        )
        .expect("node_b complete activation");
    assert_eq!(completed_act.status.phase, ActivationPhase::Succeeded);
}

#[test]
fn test_cross_session_source_lineage_context_resolution() {
    let store = SqliteStore::open_in_memory().expect("open store");
    register_test_node(&store, "node_a", vec!["gpt-4.1"], 60_000);

    let (parent_res, dispatches) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_parent")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("turn 1 user")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create parent");

    let act_id = ActivationId::new(parent_res.initial_activation.unwrap().metadata.id);
    let epoch = dispatches[0].lease_epoch;
    store
        .commit_batch(
            &SessionId::new("sess_parent"),
            &act_id,
            epoch,
            ItemBatchKind::ModelOutput,
            Some(TurnId::new("t1")),
            Some("r1".to_string()),
            vec![Item::assistant_message("turn 1 assistant")],
            BatchCompanionEvent::TurnCompleted {
                response_id: Some("r1".to_string()),
                usage: None,
            },
        )
        .expect("commit parent output");

    let (_child_res, _) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_child")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![SessionSource {
                    kind: SessionSourceKind::ForkedFrom,
                    session_id: SessionId::new("sess_parent"),
                    from_context_pos: ContextPos::new(0),
                    through_context_pos: ContextPos::new(1),
                }],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("child follow-up")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create child session");

    let child_ctx = store
        .read_effective_context(&SessionId::new("sess_child"))
        .expect("read child effective context");
    assert_eq!(child_ctx.effective_items.len(), 3);
}

#[test]
fn test_deferred_queued_activation_batch_and_atomic_withdrawal() {
    let store = SqliteStore::open_in_memory().expect("open store");
    register_test_node(&store, "node_a", vec!["gpt-4.1"], 60_000);

    let (created, dispatches) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_withdraw_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Floating {
                    node_selector: BTreeMap::new(),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("turn 1 running")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create session");

    let act1_id = ActivationId::new(created.initial_activation.unwrap().metadata.id);
    let epoch1 = dispatches[0].lease_epoch;
    assert!( store.withdraw_queued_activation(&act1_id).is_err(), "scheduled activation cannot be withdrawn" );

    // Submit a second activation while act1 is still active -> act2 stays Queued
    let (act2, dispatches2) = store
        .create_activation(CreateActivationRequest {
            activation_id: None,
            labels: BTreeMap::new(),
            spec: ActivationSpec {
                session_id: SessionId::new("sess_withdraw_1"),
                submission_kind: SubmissionKind::FollowUp,
                input: vec![Item::user_message("turn 2 queued to withdraw")],
                model_override: None,
                disabled_tools: vec![],
                desired_phase: DesiredActivationPhase::Running,
                interaction_responses: BTreeMap::new(),
            },
        })
        .expect("create queued activation");
    assert!(dispatches2.is_empty());
    assert_eq!(act2.status.phase, ActivationPhase::Queued);
    let act2_id = ActivationId::new(act2.metadata.id);

    // While act2 is Queued, its input must NOT be in effective_items yet!
    let ctx_while_queued = store
        .read_effective_context(&SessionId::new("sess_withdraw_1"))
        .expect("read context");
    assert_eq!(ctx_while_queued.effective_items.len(), 1);

    // Withdraw act2 while Queued -> succeeds and leaves zero residue in effective_items
    let withdrawn = store
        .withdraw_queued_activation(&act2_id)
        .expect("withdraw queued activation");
    assert_eq!(withdrawn.status.phase, ActivationPhase::Cancelled);

    let ctx_after_withdraw = store
        .read_effective_context(&SessionId::new("sess_withdraw_1"))
        .expect("read context after withdraw");
    assert_eq!(ctx_after_withdraw.effective_items.len(), 1);

    // Also test realtime message push + withdraw before drain
    let rt_pushed = store
        .push_realtime_message(
            &SessionId::new("sess_withdraw_1"),
            vec![openresponses_rust::MessageContent::InputText {
                text: "urgent realtime".to_string(),
            }],
        )
        .expect("push realtime");
    assert_eq!(rt_pushed.queued_count, 1);
    assert_eq!(rt_pushed.inserted_ids.len(), 1);

    let rt_withdrawn = store
        .withdraw_realtime_messages(
            &SessionId::new("sess_withdraw_1"),
            WithdrawRealtimeMessageRequest {
                message_ids: rt_pushed.inserted_ids,
            },
        )
        .expect("withdraw realtime");
    assert_eq!(rt_withdrawn.removed_count, 1);
    assert_eq!(rt_withdrawn.remaining_count, 0);

    let (drained, _) = store
        .drain_realtime_messages(
            &SessionId::new("sess_withdraw_1"),
            &act1_id,
            epoch1,
            None,
        )
        .expect("drain realtime");
    assert!(drained.is_empty());
}

#[test]
fn test_running_activation_grace_period_and_reconnect_reconciliation() {
    let store = SqliteStore::open_in_memory().expect("open store");
    register_test_node(&store, "node_wan", vec!["gpt-4.1"], 10_000);

    let (created, dispatches) = store
        .create_session(CreateSessionRequest {
            session_id: Some(SessionId::new("sess_wan_1")),
            labels: BTreeMap::new(),
            spec: SessionSpec {
                scheduling: SchedulingMode::Pinned {
                    node_id: NodeId::new("node_wan"),
                    workspace_path: PathBuf::from("/tmp/workspace/wan"),
                },
                sources: vec![],
                producer: None,
                desired_phase: DesiredSessionPhase::Open,
                system_prompt: None,
                model: Some(ModelRequestSpec::new("gpt-4.1")),
                extensions: vec![],
                permissions: PermissionPolicyLayer::default(),
            },
            initial_activation: Some(InitialActivationRequest {
                input: vec![Item::user_message("long running task")],
                model_override: None,
                disabled_tools: vec![],
            }),
        })
        .expect("create session");

    let act_id = ActivationId::new(
        created
            .initial_activation
            .as_ref()
            .unwrap()
            .metadata
            .id
            .clone(),
    );
    let epoch1 = dispatches[0].lease_epoch;

    let cfg = ConfigRevision::new_merged(
        "prompt".to_string(),
        ModelRequestSpec::new("gpt-4.1"),
        vec![],
    );
    let perm = PermissionSnapshot::from_effective_policy(PermissionPolicyLayer::default());
    store
        .accept_activation(&SessionId::new("sess_wan_1"), &act_id, epoch1, cfg, perm)
        .expect("accept activation");

    // 1. After 20s (lease expired, within 5-minute running grace period):
    // Node becomes NotReady, but Running activation stays Running!
    let t_blip = Utc::now() + Duration::seconds(20);
    store
        .patrol_expired_leases(t_blip)
        .expect("patrol at 20s blip");
    let node = store
        .get_node(&NodeId::new("node_wan"))
        .unwrap()
        .unwrap();
    assert_eq!(node.status.phase, NodePhase::NotReady);
    let act = store.get_activation(&act_id).unwrap().unwrap();
    assert_eq!(act.status.phase, ActivationPhase::Running);
    assert_eq!(act.status.lease_epoch, epoch1);

    // 2. Node reconnects and still reports `act_id` in `running_activation_ids`:
    // Node returns to Ready and activation remains Running with original lease_epoch.
    store
        .register_node_with_running_activations(
            NodeId::new("node_wan"),
            "node_wan.local".to_string(),
            "macos".to_string(),
            "aarch64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            None,
            PermissionPolicyLayer::default(),
            vec![],
            vec!["/tmp".to_string()],
            4,
            10_000,
            Some(vec![act_id.clone()]),
        )
        .expect("reconnect with running act_id");
    let act_after_reconnect = store.get_activation(&act_id).unwrap().unwrap();
    assert_eq!(act_after_reconnect.status.phase, ActivationPhase::Running);
    assert_eq!(act_after_reconnect.status.lease_epoch, epoch1);

    // 3. Node restarts and registers with empty `running_activation_ids`:
    // Activation is immediately marked Interrupted without waiting for grace timeout.
    store
        .register_node_with_running_activations(
            NodeId::new("node_wan"),
            "node_wan.local".to_string(),
            "macos".to_string(),
            "aarch64".to_string(),
            BTreeMap::new(),
            vec!["gpt-4.1".to_string()],
            Some("gpt-4.1".to_string()),
            None,
            PermissionPolicyLayer::default(),
            vec![],
            vec!["/tmp".to_string()],
            4,
            10_000,
            Some(vec![]),
        )
        .expect("reconnect after daemon restart");
    let act_after_restart = store.get_activation(&act_id).unwrap().unwrap();
    assert_eq!(
        act_after_restart.status.phase,
        ActivationPhase::Interrupted
    );
}

