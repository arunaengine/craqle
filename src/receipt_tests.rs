//! Checks receipt coverage and accepted failures through the node facade.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::time::Duration;

use crate::{
    AllowAllAuthorizer, CraqleError, CraqleNode, CreateCrateRequest, EncodedTerm, GraphId,
    GraphPolicy, MaterializedQuadChange, MutationId, MutationLookup, MutationReceipt,
    MutationRequest, MutationStatus, PersistenceOutcome, RepairOutcome, ShutdownState,
    SourceOutcome, UpdateError,
};

fn create(node: &CraqleNode, graph: &GraphId) -> crate::Batch {
    node.create_crate(
        &AllowAllAuthorizer,
        CreateCrateRequest::new(
            graph.clone(),
            "receipt fixture",
            "receiptneedle",
            "2026-09-20",
            None,
            GraphPolicy::default(),
        ),
    )
    .unwrap()
}

fn lookup(receipt: &MutationReceipt) -> MutationLookup {
    MutationLookup {
        graph: receipt.graph.clone(),
        id: receipt.id,
        admission_sequence: Some(receipt.admission_sequence),
    }
}

#[test]
fn merge_covers_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let source = CraqleNode::open(directory.path().join("source")).unwrap();
    let replica = CraqleNode::open(directory.path().join("replica")).unwrap();
    let graph = GraphId::new("urn:test:receipt-merge");
    let batch = create(&source, &graph);
    assert!(replica.merge_batch(&batch).unwrap().applied);
    #[cfg(feature = "search")]
    replica.flush_search_updates().unwrap();
    let MutationStatus::Known(receipt) = replica.replication.receipt_for_batch(&batch).unwrap()
    else {
        panic!("accepted batch has no receipt");
    };
    let MutationStatus::Known(receipt) = replica
        .mutation_status(&AllowAllAuthorizer, lookup(&receipt))
        .unwrap()
    else {
        panic!("accepted batch lost its receipt");
    };
    assert_eq!(receipt.source, SourceOutcome::Applied);
    assert_eq!(receipt.persistence, PersistenceOutcome::FullySynced);
    assert_eq!(receipt.repairs.query_view, RepairOutcome::Complete);
    #[cfg(feature = "search")]
    assert_eq!(receipt.repairs.search, RepairOutcome::Complete);
    assert_eq!(
        source.graph_snapshot(&graph).unwrap(),
        replica.graph_snapshot(&graph).unwrap()
    );
}

#[test]
fn persistence_reports_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let mut node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:receipt-persistence");
    create(&node, &graph);
    assert_eq!(
        node.shutdown(Duration::from_secs(180)).unwrap(),
        ShutdownState::Complete
    );
    let mut request = MutationRequest {
        id: MutationId([73; 32]),
        admission_sequence: None,
        graph: graph.clone(),
        changes: vec![MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&graph.0),
            predicate: EncodedTerm("<http://schema.org/keywords>".to_owned()),
            object: EncodedTerm("\"acceptedneedle\"".to_owned()),
        }],
    };
    let prepared = node
        .apply_mutation(&AllowAllAuthorizer, request.clone())
        .unwrap();
    request.admission_sequence = Some(prepared.admission_sequence);
    let accepted = node.replication.apply_mutation(request).unwrap();
    assert_eq!(accepted.source, SourceOutcome::Applied);
    let MutationStatus::Known(accepted) = node
        .replication
        .mutation_status(&lookup(&accepted))
        .unwrap()
    else {
        panic!("accepted mutation has no receipt");
    };
    let expected = node.graph_snapshot(&graph).unwrap();

    // The worker is stopped, so only the receipt update can consume this fault.
    node.store.arm_commit_failure();
    let error = node.persist_receipt(accepted.clone()).unwrap_err();
    let CraqleError::Update(UpdateError::Accepted { receipt, .. }) = error else {
        panic!("post-source failure lost its accepted outcome: {error:?}");
    };
    assert_eq!(receipt.id, accepted.id);
    assert_eq!(receipt.source, SourceOutcome::Applied);
    assert_eq!(receipt.persistence, PersistenceOutcome::Pending);
    assert_eq!(node.graph_snapshot(&graph).unwrap(), expected);

    // A failed completion never claims sync; explicitly persist before reopening.
    node.persist_fjall().unwrap();
    drop(node);
    let reopened = CraqleNode::open(directory.path()).unwrap();
    let MutationStatus::Known(receipt) = reopened
        .mutation_status(&AllowAllAuthorizer, lookup(&accepted))
        .unwrap()
    else {
        panic!("accepted receipt disappeared after explicit persistence");
    };
    assert_eq!(receipt.persistence, PersistenceOutcome::Pending);
    assert_eq!(reopened.graph_snapshot(&graph).unwrap(), expected);
    assert_eq!(
        reopened.persist_receipt(receipt).unwrap().persistence,
        PersistenceOutcome::FullySynced
    );
}

#[cfg(feature = "shacl-core")]
#[test]
fn receipts_hide_dependents() {
    use crate::{
        GrantAuthorizer, PermissionGrant, PermissionLevel, ShaclBinding, ShaclBindingOptions,
        ShaclWritePolicy,
    };

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let data = GraphId::new("urn:test:receipt-hidden-data");
    let shapes = GraphId::new("urn:test:receipt-visible-shapes");
    create(&node, &data);
    let statements = [
        (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.w3.org/ns/shacl#NodeShape",
        ),
        (
            "http://www.w3.org/ns/shacl#targetClass",
            "http://schema.org/Dataset",
        ),
    ];
    node.apply_unchecked(
        &shapes,
        statements
            .into_iter()
            .map(|(predicate, object)| MaterializedQuadChange::Insert {
                graph: shapes.clone(),
                subject: EncodedTerm("<urn:test:receipt-shape>".to_owned()),
                predicate: EncodedTerm(format!("<{predicate}>")),
                object: EncodedTerm(format!("<{object}>")),
            })
            .collect(),
    )
    .unwrap();
    node.set_graph_policy(
        &AllowAllAuthorizer,
        &shapes,
        GraphPolicy {
            public: false,
            permission_paths: vec!["/shapes".to_owned()],
        },
    )
    .unwrap();
    node.bind_shacl(
        &AllowAllAuthorizer,
        &ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy: ShaclWritePolicy::Advisory,
            validation_options: ShaclBindingOptions::default(),
        },
    )
    .unwrap();
    let receipt = node
        .replication
        .delete_graph(&shapes, false)
        .unwrap()
        .unwrap();
    assert!(receipt.repair_graphs.contains(&data));
    node.persist_receipt(receipt.clone()).unwrap();
    assert!(
        node.store
            .mutation_receipt(&receipt.id)
            .unwrap()
            .unwrap()
            .repair_graphs
            .contains(&data)
    );

    let auth = GrantAuthorizer::new(vec![PermissionGrant::new(
        "/shapes",
        PermissionLevel::Write,
    )]);
    let MutationStatus::Known(outbound) = node.mutation_status(&auth, lookup(&receipt)).unwrap()
    else {
        panic!("authorized shapes receipt is missing");
    };
    assert!(!outbound.repair_graphs.contains(&data));
    assert!(!format!("{outbound:?}").contains(data.as_str()));
    assert!(
        !serde_json::to_string(&outbound)
            .unwrap()
            .contains(data.as_str())
    );
}
