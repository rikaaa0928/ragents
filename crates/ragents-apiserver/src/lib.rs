pub mod server;
pub mod store;

pub use ragents_protocol;
pub use server::*;
pub use store::*;

#[cfg(test)]
mod tests {
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

        // Create Pinned Session bound to node_a and /workspace/proj
        let (created, dispatches) = store
            .create_session(CreateSessionRequest {
                session_id: Some(SessionId::new("sess_pinned_1")),
                labels: BTreeMap::new(),
                spec: SessionSpec {
                    scheduling: SchedulingMode::Pinned {
                        node_id: NodeId::new("node_a"),
                        workspace_path: PathBuf::from("/workspace/proj"),
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

        // Expire node_a lease (node_b is still healthy!)
        let future_time = Utc::now() + Duration::seconds(20);
        let rescheduled = store
            .patrol_expired_leases(future_time)
            .expect("patrol leases");

        // Pinned session must NOT reschedule onto node_b!
        assert!(rescheduled.is_empty());
        let act = store.get_activation(&act_id).unwrap().unwrap();
        assert_eq!(act.status.phase, ActivationPhase::Interrupted);

        // Submitting a new activation while node_a is NotReady keeps it Queued (never sent to node_b)
        let (act2, dispatches2) = store
            .create_activation(CreateActivationRequest {
                activation_id: None,
                labels: BTreeMap::new(),
                spec: ActivationSpec {
                    session_id: SessionId::new("sess_pinned_1"),
                    submission_kind: SubmissionKind::FollowUp,
                    input: vec![Item::user_message("try again")],
                    model_override: None,
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

        // 1. Create Floating session -> scheduled onto node_a
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

        // 2. Register node_b with longer lease, then let node_a expire while activation is Scheduled
        register_test_node(&store, "node_b", vec!["claude-sonnet-4"], 120_000);
        let future_time = Utc::now() + Duration::seconds(20);
        let failover_dispatches = store
            .patrol_expired_leases(future_time)
            .expect("patrol expired node_a");

        // Floating scheduled task automatically fails over to node_b with bumped lease_epoch!
        assert_eq!(failover_dispatches.len(), 1);
        assert_eq!(failover_dispatches[0].node_id, NodeId::new("node_b"));
        let new_epoch = failover_dispatches[0].lease_epoch;
        assert!(new_epoch.value() > 1);

        // 3. Stale write from zombie node_a with lease_epoch=1 must be rejected!
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

        // 4. Node B accepts activation and commits with valid new_epoch
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

        // Fork child session referencing [0..=1] of sess_parent
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
                }),
            })
            .expect("create child session");

        let child_ctx = store
            .read_effective_context(&SessionId::new("sess_child"))
            .expect("read child effective context");
        assert_eq!(child_ctx.effective_items.len(), 3);
    }
}
