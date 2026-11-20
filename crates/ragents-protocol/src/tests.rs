use super::*;
use openresponses_rust::Item;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[test]
fn test_scheduling_mode_validation() {
    let valid_pinned = SchedulingMode::Pinned {
        node_id: NodeId::new("node_macbook"),
        workspace_path: PathBuf::from("/Users/dev/project"),
    };
    assert!(valid_pinned.validate().is_ok());

    let relative_pinned = SchedulingMode::Pinned {
        node_id: NodeId::new("node_macbook"),
        workspace_path: PathBuf::from("relative/path"),
    };
    assert!(matches!(
        relative_pinned.validate(),
        Err(ProtocolValidationError::InvalidPinnedWorkspace(_))
    ));

    let bad_node_id = SchedulingMode::Pinned {
        node_id: NodeId::new("../bad_id"),
        workspace_path: PathBuf::from("/Users/dev/project"),
    };
    assert!(matches!(
        bad_node_id.validate(),
        Err(ProtocolValidationError::InvalidIdentifier(_))
    ));

    let floating = SchedulingMode::Floating {
        node_selector: BTreeMap::new(),
    };
    assert!(floating.validate().is_ok());
}

#[test]
fn test_config_revision_content_addressed_sha256_stability() {
    let ext1 = ExtensionSpec {
        name: "shell".to_string(),
        path: "extensions/shell.wasm".to_string(),
        enabled: true,
        config: serde_json::json!({"b": 2, "a": 1}),
        allowed_dirs: vec![],
        filter_env: vec![],
    };
    let ext2 = ExtensionSpec {
        name: "shell".to_string(),
        path: "extensions/shell.wasm".to_string(),
        enabled: true,
        config: serde_json::json!({"a": 1, "b": 2}),
        allowed_dirs: vec![],
        filter_env: vec![],
    };

    let rev1 = ConfigRevision::new_merged(
        "You are helpful.".to_string(),
        ModelRequestSpec {
            name: "gpt-4.1".to_string(),
            temperature: Some(0.2),
            max_output_tokens: Some(4096),
            reasoning: None,
            built_in_tools: vec![],
        },
        vec![ext1],
    );

    let rev2 = ConfigRevision::new_merged(
        "You are helpful.".to_string(),
        ModelRequestSpec {
            name: "gpt-4.1".to_string(),
            temperature: Some(0.2),
            max_output_tokens: Some(4096),
            reasoning: None,
            built_in_tools: vec![],
        },
        vec![ext2],
    );

    assert_eq!(rev1.config_ref, rev2.config_ref);
    assert!(rev1.config_ref.as_str().starts_with("cfg_sha256:"));
}

#[test]
fn test_openresponses_item_batch_roundtrip() {
    let item = Item::user_message("hello ragents cluster");
    let batch = ItemBatch {
        session_id: SessionId::generate(),
        batch_seq: BatchSeq::new(0),
        first_local_item_seq: LocalItemSeq::new(0),
        kind: ItemBatchKind::Input,
        activation_id: ActivationId::generate(),
        turn_id: None,
        response_id: None,
        view_mode_placeholder: None,
        created_at: chrono::Utc::now(),
        items: vec![item],
    };

    assert_eq!(batch.last_local_item_seq(), LocalItemSeq::new(0));
    let json = serde_json::to_string(&batch).expect("serialize batch");
    let decoded: ItemBatch = serde_json::from_str(&json).expect("deserialize batch");
    assert_eq!(batch, decoded);
}

#[test]
fn test_session_source_validation() {
    let valid_src = SessionSource {
        kind: SessionSourceKind::ForkedFrom,
        session_id: SessionId::new("sess_123"),
        from_context_pos: ContextPos::new(0),
        through_context_pos: ContextPos::new(10),
    };
    assert!(valid_src.validate().is_ok());

    let invalid_src = SessionSource {
        kind: SessionSourceKind::ForkedFrom,
        session_id: SessionId::new("sess_123"),
        from_context_pos: ContextPos::new(12),
        through_context_pos: ContextPos::new(5),
    };
    assert!(matches!(
        invalid_src.validate(),
        Err(ProtocolValidationError::InvalidSourceRange {
            from: 12,
            through: 5
        })
    ));
}

#[test]
fn test_permission_policy_layer_prunes_redundant_and_subsumed_rules() {
    // 1. Same-layer pruning: broader directory subsumes narrower subdirectory; sibling prefix preserved.
    let mut layer = PermissionPolicyLayer::default();
    layer.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/src/minimax/ComfyUI/"),
    });
    layer.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/src/minimax2"),
    });
    layer.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/src/minimax"),
    });
    assert_eq!(
        layer.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(
            &[
                "/var/home/rikaaa0928/src/minimax2".to_string(),
                "/var/home/rikaaa0928/src/minimax".to_string()
            ][..]
        )
    );

    // 2. Cross-layer pruning: AllowWorkspace subsumes AllowSession in matching category only.
    let mut workspace = PermissionPolicyLayer::default();
    workspace.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/models/readonly_sub"),
    });
    workspace.grant_capability(&CapabilityRequest::FileWrite {
        path: PathBuf::from("/var/home/rikaaa0928/models/anima"),
    });
    workspace.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/src/minimax"),
    });

    let mut session = PermissionPolicyLayer {
        file_read: Some(RuleSet {
            allow: Some(vec![
                "/var/home/rikaaa0928/src/minimax/ComfyUI".to_string(),
                "/var/home/rikaaa0928/src/other".to_string(),
            ]),
            deny: Some(vec!["/var/home/rikaaa0928/src/minimax/secret".to_string()]),
            default: None,
        }),
        file_write: Some(RuleSet {
            allow: Some(vec!["/var/home/rikaaa0928/src/minimax/out".to_string()]),
            deny: None,
            default: None,
        }),
        ..Default::default()
    };

    assert!(session.prune_subsumed_by(&workspace));
    // file_read minimax/ComfyUI is removed; other is kept; deny is never removed by allow!
    assert_eq!(
        session.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/var/home/rikaaa0928/src/other".to_string()][..])
    );
    assert_eq!(
        session.file_read.as_ref().and_then(|r| r.deny.as_deref()),
        Some(&["/var/home/rikaaa0928/src/minimax/secret".to_string()][..])
    );
    // workspace only granted FileRead on minimax, NOT FileWrite, so session file_write must remain!
    assert_eq!(
        session.file_write.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/var/home/rikaaa0928/src/minimax/out".to_string()][..])
    );

    // 3. AllowNode subsumes matching category in AllowWorkspace and preserves other categories.
    let mut node_grant = PermissionPolicyLayer::default();
    node_grant.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/var/home/rikaaa0928/models"),
    });
    assert!(workspace.prune_subsumed_by(&node_grant));
    assert_eq!(
        workspace.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/var/home/rikaaa0928/src/minimax".to_string()][..])
    );
    assert_eq!(
        workspace.file_write.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/var/home/rikaaa0928/models/anima".to_string()][..])
    );

    // 4. System executable protection and HTTP wildcard subsumption.
    let mut sess_bins = PermissionPolicyLayer::default();
    sess_bins.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/usr/bin/sudo"),
    });
    sess_bins.grant_capability(&CapabilityRequest::Http {
        scheme: "https".to_string(),
        host: "api.sub.example.com".to_string(),
        port: None,
        method: "GET".to_string(),
    });
    sess_bins.grant_capability(&CapabilityRequest::Http {
        scheme: "https".to_string(),
        host: "other.org".to_string(),
        port: None,
        method: "GET".to_string(),
    });

    let mut parent_node = PermissionPolicyLayer::default();
    parent_node.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/usr/bin"),
    });
    parent_node.grant_capability(&CapabilityRequest::Http {
        scheme: "https".to_string(),
        host: "*.example.com".to_string(),
        port: None,
        method: "GET".to_string(),
    });

    assert!(sess_bins.prune_subsumed_by(&parent_node));
    // /usr/bin parent must NOT remove /usr/bin/sudo (requires exact setuid match)
    assert_eq!(
        sess_bins.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/usr/bin/sudo".to_string()][..])
    );
    // *.example.com subsumes api.sub.example.com but preserves other.org
    assert_eq!(
        sess_bins.http_hosts.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["other.org".to_string()][..])
    );

    // Exact /usr/bin/sudo at higher level DOES subsume /usr/bin/sudo at lower level.
    parent_node.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/usr/bin/sudo"),
    });
    assert!(sess_bins.prune_subsumed_by(&parent_node));
    assert!(sess_bins.file_read.is_none());
}

#[test]
fn test_write_subsumes_read_and_respects_write_deny_on_merge_and_prune() {
    // 1. grant_capability(FileWrite) does NOT duplicate into file_read and prunes narrower file_read.
    let mut layer = PermissionPolicyLayer::default();
    layer.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/data/models/sub"),
    });
    layer.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/data/other"),
    });
    layer.grant_capability(&CapabilityRequest::FileWrite {
        path: PathBuf::from("/data/models"),
    });
    assert_eq!(
        layer.file_write.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/data/models".to_string()][..])
    );
    assert_eq!(
        layer.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/data/other".to_string()][..])
    );

    // 2. Higher FileWrite prunes lower FileRead, while higher FileRead NEVER prunes lower FileWrite.
    let mut higher = PermissionPolicyLayer::default();
    higher.grant_capability(&CapabilityRequest::FileWrite {
        path: PathBuf::from("/srv/shared"),
    });
    higher.grant_capability(&CapabilityRequest::FileRead {
        path: PathBuf::from("/srv/readonly"),
    });

    let mut lower = PermissionPolicyLayer {
        file_read: Some(RuleSet {
            allow: Some(vec![
                "/srv/shared/logs".to_string(),
                "/srv/locked/readme.txt".to_string(),
            ]),
            deny: None,
            default: None,
        }),
        file_write: Some(RuleSet {
            allow: Some(vec!["/srv/readonly/out".to_string()]),
            deny: None,
            default: None,
        }),
        ..Default::default()
    };
    assert!(lower.prune_subsumed_by(&higher));
    assert_eq!(
        lower.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/srv/locked/readme.txt".to_string()][..])
    );
    assert_eq!(
        lower.file_write.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/srv/readonly/out".to_string()][..])
    );

    // 3. Multi-layer merge with file_write.deny on a later layer must NOT lose lower file_read inside write-denied subtree!
    let mut node_layer = PermissionPolicyLayer::default();
    node_layer.grant_capability(&CapabilityRequest::FileWrite {
        path: PathBuf::from("/data"),
    });
    let mut ws_layer = PermissionPolicyLayer {
        file_read: Some(RuleSet {
            allow: Some(vec![
                "/data/normal/file".to_string(),
                "/data/locked/readme".to_string(),
            ]),
            deny: None,
            default: None,
        }),
        ..Default::default()
    };
    let session_layer = PermissionPolicyLayer {
        file_write: Some(RuleSet {
            allow: None,
            deny: Some(vec!["/data/locked".to_string()]),
            default: None,
        }),
        ..Default::default()
    };

    // Merging across layers preserves ws_layer's read until final prune sees session's write deny.
    let mut merged = node_layer.clone();
    merged.merge_from(&ws_layer);
    merged.merge_from(&session_layer);
    merged.prune_redundant();
    assert_eq!(
        merged.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/data/locked/readme".to_string()][..])
    );

    // And pruning ws_layer with extra_write_denies from session/node preserves /data/locked/readme!
    assert!(ws_layer.prune_subsumed_by_with_write_denies(&node_layer, &["/data/locked".to_string()]));
    assert_eq!(
        ws_layer.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&["/data/locked/readme".to_string()][..])
    );

    // 4. NodeSpec::effective_permissions merges base + granted and prunes redundant file_read under file_write.
    let node_spec = NodeSpec {
        base_permissions: PermissionPolicyLayer {
            file_read: Some(RuleSet {
                allow: Some(vec![".".to_string(), "/models/sub".to_string()]),
                deny: None,
                default: Some(DefaultPermissionDecision::Ask),
            }),
            file_write: Some(RuleSet {
                allow: Some(vec![".".to_string()]),
                deny: None,
                default: Some(DefaultPermissionDecision::Ask),
            }),
            ..Default::default()
        },
        granted_permissions: {
            let mut g = PermissionPolicyLayer::default();
            g.grant_capability(&CapabilityRequest::FileWrite {
                path: PathBuf::from("/models"),
            });
            g
        },
        ..Default::default()
    };
    let eff = node_spec.effective_permissions();
    assert_eq!(
        eff.file_read.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&[".".to_string()][..])
    );
    assert_eq!(
        eff.file_write.as_ref().and_then(|r| r.allow.as_deref()),
        Some(&[".".to_string(), "/models".to_string()][..])
    );
}

