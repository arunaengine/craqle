use craqle::*;

fn policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/tests/prevalidated".to_string()],
    }
}

fn actor(seed: u8) -> ActorId {
    ActorId::from_bytes([seed; 32])
}

fn doc(graph: &GraphId, file_count: usize) -> String {
    let root = graph.as_str();
    let mut entries = vec![
        serde_json::json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
            "about": {"@id": root}
        }),
        serde_json::json!({
            "@id": root,
            "@type": "Dataset",
            "name": "Prevalidated Dataset",
            "description": "Prevalidated apply test",
            "datePublished": "2026-06-10",
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
            "hasPart": (0..file_count)
                .map(|idx| serde_json::json!({"@id": format!("./data/file-{idx}.raw")}))
                .collect::<Vec<_>>()
        }),
    ];
    for idx in 0..file_count {
        entries.push(serde_json::json!({
            "@id": format!("./data/file-{idx}.raw"),
            "@type": "File",
            "name": format!("file-{idx}.raw")
        }));
    }
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": entries
    })
    .to_string()
}

#[test]
fn prevalidated_apply_matches_checked_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let checked = CraqleNode::open(tmp.path().join("checked")).unwrap();
    let trusted = CraqleNode::open(tmp.path().join("trusted")).unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/prevalidated-equivalence");
    let jsonld = doc(&graph, 2);

    let checked_batch = checked
        .apply_rocrate_document_checked_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph.clone(),
            &jsonld,
            policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(7)),
        )
        .unwrap();
    let trusted_batch = trusted
        .apply_rocrate_document_prevalidated_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph.clone(),
            &jsonld,
            policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(7)),
        )
        .unwrap();

    assert_eq!(
        format!("{:?}", checked_batch.ops),
        format!("{:?}", trusted_batch.ops)
    );
    assert_eq!(checked_batch.actor, trusted_batch.actor);
    assert_eq!(checked_batch.counter, trusted_batch.counter);

    let checked_export = checked.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
    let trusted_export = trusted.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
    assert_eq!(checked_export, trusted_export);
    assert_eq!(
        checked.graph_fingerprint(&graph).unwrap(),
        trusted.graph_fingerprint(&graph).unwrap()
    );
    assert!(trusted.graph_violations(&graph).unwrap().is_empty());
    assert!(!trusted.graph_diagnostics(&graph).unwrap().has_orphans());
}

#[test]
fn prevalidated_apply_replaces_existing_graph() {
    let tmp = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(tmp.path()).unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/prevalidated-replace");

    node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
        &AllowAllAuthorizer,
        graph.clone(),
        &doc(&graph, 1),
        policy(),
        CraqleRequestDurability::WalAlreadyDurable,
        Some(actor(1)),
    )
    .unwrap();
    node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
        &AllowAllAuthorizer,
        graph.clone(),
        &doc(&graph, 3),
        policy(),
        CraqleRequestDurability::WalAlreadyDurable,
        Some(actor(2)),
    )
    .unwrap();

    let export = node.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
    assert!(export.contains("./data/file-2.raw"));
    assert!(node.graph_violations(&graph).unwrap().is_empty());
}

#[test]
fn prevalidated_apply_rejects_structurally_invalid_jsonld() {
    let tmp = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(tmp.path()).unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/prevalidated-structural");

    let missing_graph = r#"{"@context": "https://w3id.org/ro/crate/1.2/context"}"#;
    assert!(
        node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph.clone(),
            missing_graph,
            policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(3)),
        )
        .is_err()
    );
    assert!(
        node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph.clone(),
            "not json",
            policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(3)),
        )
        .is_err()
    );
    assert!(!node.contains_graph(&graph).unwrap());
}

#[test]
fn checked_apply_still_validates_semantics() {
    let tmp = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(tmp.path()).unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/prevalidated-checked-guard");

    // No metadata descriptor / root dataset: checked path must reject.
    let invalid = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {"@id": "./data/loose.raw", "@type": "File", "name": "loose"}
        ]
    })
    .to_string();
    assert!(
        node.apply_rocrate_document_checked_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph.clone(),
            &invalid,
            policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(4)),
        )
        .is_err()
    );
}

#[test]
fn prevalidated_create_matches_checked_create() {
    let tmp = tempfile::tempdir().unwrap();
    let checked = CraqleNode::open(tmp.path().join("checked")).unwrap();
    let trusted = CraqleNode::open(tmp.path().join("trusted")).unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/prevalidated-create");
    let request = || {
        CreateCrateRequest::new(
            graph.clone(),
            "Scaffold Dataset",
            "Scaffold description",
            "2026-06-10",
            Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            policy(),
        )
    };

    let checked_batch = checked
        .create_crate_with_durability_as(
            &AllowAllAuthorizer,
            request(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(5)),
        )
        .unwrap();
    let trusted_batch = trusted
        .create_crate_prevalidated_with_durability_as(
            &AllowAllAuthorizer,
            request(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(actor(5)),
        )
        .unwrap();

    assert_eq!(
        format!("{:?}", checked_batch.ops),
        format!("{:?}", trusted_batch.ops)
    );
    assert_eq!(
        checked.graph_fingerprint(&graph).unwrap(),
        trusted.graph_fingerprint(&graph).unwrap()
    );
    assert!(trusted.graph_violations(&graph).unwrap().is_empty());
}
