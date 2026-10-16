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
    };
    let ext2 = ExtensionSpec {
        name: "shell".to_string(),
        path: "extensions/shell.wasm".to_string(),
        enabled: true,
        config: serde_json::json!({"a": 1, "b": 2}),
        allowed_dirs: vec![],
    };

    let rev1 = ConfigRevision::new_merged(
        "You are helpful.".to_string(),
        ModelRequestSpec {
            name: "gpt-4.1".to_string(),
            temperature: Some(0.2),
            max_output_tokens: Some(4096),
            reasoning: None,
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
