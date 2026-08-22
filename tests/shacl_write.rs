#![cfg(feature = "shacl-core")]

mod support;

use crate::support::TestWriteExt as _;

use std::sync::{Arc, Barrier, mpsc};

use craqle::{
    ActorId, AllowAllAuthorizer, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId,
    GraphPolicy, GraphReplicaSnapshot, MaterializedQuadChange, QueryIndexStatus, ShaclBinding,
    ShaclBindingOptions, ShaclBindingStatus, ShaclBlockingSeverity, ShaclValidationState,
    ShaclWritePolicy, UpdateError, VectorClock,
};

use crate::support::with_watchdog;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROP: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const SH_TARGET: &str = "http://www.w3.org/ns/shacl#targetNode";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_MIN: &str = "http://www.w3.org/ns/shacl#minCount";
const SH_MAX: &str = "http://www.w3.org/ns/shacl#maxCount";
const SH_SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const FOCUS: &str = "urn:test:shacl-write-focus";
const VALUE: &str = "urn:test:shacl-write-value";
#[cfg(feature = "search")]
const NAME: &str = "http://schema.org/name";

type State = (
    GraphReplicaSnapshot,
    VectorClock,
    QueryIndexStatus,
    QueryIndexStatus,
    Vec<ShaclBindingStatus>,
);

fn open_node() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x79; 32])),
    )
    .unwrap();
    (directory, node)
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

#[cfg(feature = "search")]
fn literal(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
}

fn number(value: u8) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\"^^<{XSD_INTEGER}>"))
}

fn add(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

fn del(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

fn limit_shape(
    node: &CraqleNode,
    graph: &GraphId,
    kind: &str,
    count: u8,
    path: &str,
    target: &str,
) {
    node.apply_changes_unchecked(
        graph,
        vec![
            add(graph, "urn:test:shacl-write-shape", RDF_TYPE, iri(SH_NODE)),
            add(graph, "urn:test:shacl-write-shape", SH_TARGET, iri(target)),
            add(
                graph,
                "urn:test:shacl-write-shape",
                SH_PROPERTY,
                iri("urn:test:shacl-write-property"),
            ),
            add(
                graph,
                "urn:test:shacl-write-property",
                RDF_TYPE,
                iri(SH_PROP),
            ),
            add(graph, "urn:test:shacl-write-property", SH_PATH, iri(path)),
            add(graph, "urn:test:shacl-write-property", kind, number(count)),
        ],
    )
    .unwrap();
}

fn bind(
    node: &CraqleNode,
    data: &GraphId,
    shapes: &GraphId,
    policy: ShaclWritePolicy,
) -> ShaclBindingStatus {
    node.bind_shacl(
        &craqle::AllowAllAuthorizer,
        &ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy,
            validation_options: ShaclBindingOptions::default(),
        },
    )
    .unwrap()
}

fn status(node: &CraqleNode, graph: &GraphId) -> ShaclBindingStatus {
    let mut statuses = node
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, graph)
        .unwrap();
    assert_eq!(statuses.len(), 1);
    statuses.pop().unwrap()
}

fn state(node: &CraqleNode, graph: &GraphId) -> State {
    node.ensure_query_indexes();
    (
        node.graph_snapshot(graph).unwrap(),
        node.vector_clock(graph).unwrap(),
        node.query_index_status_fast().unwrap(),
        node.query_index_status().unwrap(),
        node.shacl_binding_statuses(&craqle::AllowAllAuthorizer, graph)
            .unwrap(),
    )
}

#[test]
fn enforce_insert_atomic() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-insert-data");
    let shapes = GraphId::new("urn:test:shacl-write-insert-shapes");
    limit_shape(&node, &shapes, SH_MAX, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(&data, vec![add(&data, FOCUS, VALUE, iri("urn:test:one"))])
        .unwrap();
    let bound = bind(&node, &data, &shapes, ShaclWritePolicy::Enforce);
    assert_eq!(bound.state, ShaclValidationState::Valid);

    let before = state(&node, &data);
    let error = node
        .apply_changes(
            &AllowAllAuthorizer,
            &data,
            vec![add(&data, FOCUS, VALUE, iri("urn:test:two"))],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Update(UpdateError::ShaclValidationFailed(_))
    ));
    assert_eq!(state(&node, &data), before);
}

#[test]
fn new_graph_atomic() {
    let (_directory, node) = open_node();
    let graph = GraphId::new("urn:test:shacl-write-invalid-new");
    let mut changes = node
        .validate_create_crate(
            &AllowAllAuthorizer,
            craqle::CreateCrateRequest::new(
                graph.clone(),
                "Invalid new graph",
                "Atomic validation",
                "2026-08-22",
                None,
                GraphPolicy::default(),
            ),
        )
        .unwrap();
    changes.push(add(
        &graph,
        "urn:test:shacl-write-invalid",
        VALUE,
        iri("urn:test:value"),
    ));
    let error = node
        .apply_changes(&AllowAllAuthorizer, &graph, changes)
        .unwrap_err();

    assert!(matches!(
        error,
        CraqleError::Update(UpdateError::ValidationFailed(_))
    ));
    assert!(!node.contains_graph(&graph).unwrap());
    assert!(node.graph_snapshot(&graph).unwrap().quads.is_empty());
    assert!(node.vector_clock(&graph).unwrap().0.is_empty());
    assert!(
        node.shacl_binding_statuses(&AllowAllAuthorizer, &graph)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn enforce_delete_atomic() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-delete-data");
    let shapes = GraphId::new("urn:test:shacl-write-delete-shapes");
    limit_shape(&node, &shapes, SH_MIN, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(&data, vec![add(&data, FOCUS, VALUE, iri("urn:test:one"))])
        .unwrap();
    let bound = bind(&node, &data, &shapes, ShaclWritePolicy::Enforce);
    assert_eq!(bound.state, ShaclValidationState::Valid);

    let before = state(&node, &data);
    let error = node
        .apply_changes(
            &AllowAllAuthorizer,
            &data,
            vec![del(&data, FOCUS, VALUE, iri("urn:test:one"))],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Update(UpdateError::ShaclValidationFailed(_))
    ));
    assert_eq!(state(&node, &data), before);
}

#[test]
fn enforce_valid_status() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-valid-data");
    let shapes = GraphId::new("urn:test:shacl-write-valid-shapes");
    limit_shape(&node, &shapes, SH_MAX, 2, VALUE, FOCUS);
    node.apply_changes_unchecked(&data, vec![add(&data, FOCUS, VALUE, iri("urn:test:one"))])
        .unwrap();
    let bound = bind(&node, &data, &shapes, ShaclWritePolicy::Enforce);
    assert_eq!(bound.state, ShaclValidationState::Valid);

    let before = node.graph_snapshot(&data).unwrap();
    node.apply_changes(
        &AllowAllAuthorizer,
        &data,
        vec![add(&data, FOCUS, VALUE, iri("urn:test:two"))],
    )
    .unwrap();
    let current = status(&node, &data);
    let report = current.report.as_ref().unwrap();
    assert_eq!(current.state, ShaclValidationState::Valid);
    assert!(report.conforms);
    assert!(!report.statistics.stopped_early);
    assert_ne!(current.data_version, bound.data_version);
    assert_ne!(node.graph_snapshot(&data).unwrap(), before);
    // `shacl_binding_statuses` reports final states only when their stored
    // versions still match the current graph and shape dependencies.
    assert_eq!(current, status(&node, &data));
}

#[test]
fn checked_writes_serialize() {
    with_watchdog("checked SHACL write serialization", || {
        let (_directory, node) = open_node();
        let data = GraphId::new("urn:test:shacl-write-race-data");
        let shapes = GraphId::new("urn:test:shacl-write-race-shapes");
        limit_shape(&node, &shapes, SH_MAX, 1, VALUE, FOCUS);
        node.apply_changes_unchecked(
            &data,
            vec![add(&data, FOCUS, RDF_TYPE, iri("http://schema.org/Thing"))],
        )
        .unwrap();
        assert_eq!(
            bind(&node, &data, &shapes, ShaclWritePolicy::Enforce).state,
            ShaclValidationState::Valid
        );

        let before = node.vector_clock(&data).unwrap();
        let node = Arc::new(node);
        let start = Arc::new(Barrier::new(3));
        let (sent, received) = mpsc::channel();
        let mut tasks = Vec::new();
        for object in ["urn:test:race-one", "urn:test:race-two"] {
            let node = Arc::clone(&node);
            let start = Arc::clone(&start);
            let sent = sent.clone();
            let data = data.clone();
            tasks.push(std::thread::spawn(move || {
                start.wait();
                sent.send(node.apply_changes(
                    &AllowAllAuthorizer,
                    &data,
                    vec![add(&data, FOCUS, VALUE, iri(object))],
                ))
                .unwrap();
            }));
        }
        drop(sent);
        start.wait();
        let results = [received.recv().unwrap(), received.recv().unwrap()];
        for task in tasks {
            task.join().unwrap();
        }

        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "results: {results:?}"
        );
        assert!(results.iter().any(|result| {
            matches!(
                result,
                Err(CraqleError::Update(UpdateError::ShaclValidationFailed(_)))
            )
        }));
        let batch = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(batch.base_clock, before);
        assert!(node.vector_clock(&data).unwrap().contains(&craqle::Dot {
            actor: batch.actor,
            counter: batch.counter,
        }));
        let values: Vec<_> = node
            .graph_snapshot(&data)
            .unwrap()
            .quads
            .into_iter()
            .filter(|quad| quad.subject == iri(FOCUS) && quad.predicate == iri(VALUE))
            .collect();
        assert_eq!(values.len(), 1);
        assert_eq!(status(&node, &data).state, ShaclValidationState::Valid);
    });
}

#[test]
fn advisory_invalid_commits() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-advisory-data");
    let shapes = GraphId::new("urn:test:shacl-write-advisory-shapes");
    limit_shape(&node, &shapes, SH_MAX, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(&data, vec![add(&data, FOCUS, VALUE, iri("urn:test:one"))])
        .unwrap();
    assert_eq!(
        bind(&node, &data, &shapes, ShaclWritePolicy::Enforce).state,
        ShaclValidationState::Valid
    );
    let change = add(&data, FOCUS, VALUE, iri("urn:test:two"));
    let enforced = match node
        .apply_changes(&AllowAllAuthorizer, &data, vec![change.clone()])
        .unwrap_err()
    {
        CraqleError::Update(UpdateError::ShaclValidationFailed(mut reports)) => {
            assert_eq!(reports.len(), 1);
            reports.pop().unwrap()
        }
        error => panic!("unexpected Enforce error: {error:?}"),
    };
    node.unbind_shacl(&AllowAllAuthorizer, &data, &shapes)
        .unwrap();
    assert_eq!(
        bind(&node, &data, &shapes, ShaclWritePolicy::Advisory).state,
        ShaclValidationState::Valid
    );

    node.apply_changes(&AllowAllAuthorizer, &data, vec![change])
        .unwrap();
    let current = status(&node, &data);
    let report = current.report.as_ref().unwrap();
    assert_eq!(current.state, ShaclValidationState::Invalid);
    assert!(!report.conforms);
    assert_eq!(report.results.len(), 1);
    assert!(!report.statistics.stopped_early);
    assert_eq!(report.conforms, enforced.conforms);
    assert_eq!(report.results, enforced.results);
    assert!(
        node.graph_snapshot(&data)
            .unwrap()
            .quads
            .iter()
            .any(|quad| quad.object == iri("urn:test:two"))
    );
}

#[test]
fn shacl_blocking_severity() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-severity-data");
    let shapes = GraphId::new("urn:test:shacl-severity-shapes");
    limit_shape(&node, &shapes, SH_MAX, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(
        &shapes,
        vec![add(
            &shapes,
            "urn:test:shacl-write-property",
            SH_SEVERITY,
            iri("http://www.w3.org/ns/shacl#Warning"),
        )],
    )
    .unwrap();
    let first = add(&data, FOCUS, VALUE, iri("urn:test:severity-one"));
    let second = add(&data, FOCUS, VALUE, iri("urn:test:severity-two"));
    node.apply_changes_unchecked(&data, vec![first]).unwrap();

    let initial = bind(&node, &data, &shapes, ShaclWritePolicy::Enforce);
    assert_eq!(
        initial.binding.validation_options.blocking_severity,
        ShaclBlockingSeverity::ViolationOnly
    );
    node.apply_changes(&AllowAllAuthorizer, &data, vec![second.clone()])
        .unwrap();
    let warning = status(&node, &data).report.unwrap();
    assert!(!warning.conforms);
    assert!(warning.accepted_by_write_policy);

    let stricter = node
        .bind_shacl(
            &AllowAllAuthorizer,
            &ShaclBinding {
                data_graph: data.clone(),
                shapes_graph: shapes.clone(),
                policy: ShaclWritePolicy::Enforce,
                validation_options: ShaclBindingOptions {
                    blocking_severity: ShaclBlockingSeverity::WarningOrViolation,
                    ..ShaclBindingOptions::default()
                },
            },
        )
        .unwrap();
    assert_eq!(
        stricter.binding.validation_options.blocking_severity,
        ShaclBlockingSeverity::WarningOrViolation
    );
    assert!(!stricter.report.unwrap().accepted_by_write_policy);
    node.apply_changes(
        &AllowAllAuthorizer,
        &data,
        vec![del(&data, FOCUS, VALUE, iri("urn:test:severity-two"))],
    )
    .unwrap();
    assert!(matches!(
        node.apply_changes(&AllowAllAuthorizer, &data, vec![second.clone()]),
        Err(CraqleError::Update(UpdateError::ShaclValidationFailed(_)))
    ));

    node.bind_shacl(
        &AllowAllAuthorizer,
        &ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy: ShaclWritePolicy::Advisory,
            validation_options: ShaclBindingOptions {
                blocking_severity: ShaclBlockingSeverity::AnyResult,
                ..ShaclBindingOptions::default()
            },
        },
    )
    .unwrap();
    node.apply_changes(&AllowAllAuthorizer, &data, vec![second])
        .unwrap();
    let advisory = status(&node, &data).report.unwrap();
    assert!(!advisory.conforms);
    assert!(!advisory.accepted_by_write_policy);

    let custom_data = GraphId::new("urn:test:shacl-custom-severity-data");
    let custom_shapes = GraphId::new("urn:test:shacl-custom-severity-shapes");
    limit_shape(&node, &custom_shapes, SH_MAX, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(
        &custom_shapes,
        vec![add(
            &custom_shapes,
            "urn:test:shacl-write-property",
            SH_SEVERITY,
            iri("urn:test:custom-severity"),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &custom_data,
        vec![add(&custom_data, FOCUS, VALUE, iri("urn:test:custom-one"))],
    )
    .unwrap();
    bind(
        &node,
        &custom_data,
        &custom_shapes,
        ShaclWritePolicy::Enforce,
    );
    assert!(matches!(
        node.apply_changes(
            &AllowAllAuthorizer,
            &custom_data,
            vec![add(&custom_data, FOCUS, VALUE, iri("urn:test:custom-two"),)],
        ),
        Err(CraqleError::Update(UpdateError::ShaclValidationFailed(_)))
    ));
}

#[test]
fn disabled_keeps_rocrate() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-disabled-data");
    let shapes = GraphId::new("urn:test:shacl-write-disabled-shapes");
    node.create_crate(
        &AllowAllAuthorizer,
        craqle::CreateCrateRequest::new(
            data.clone(),
            "Disabled binding",
            "RO-Crate rules remain checked.",
            "2026-08-20",
            None,
            GraphPolicy::default(),
        ),
    )
    .unwrap();
    limit_shape(&node, &shapes, SH_MAX, 0, VALUE, FOCUS);
    assert_eq!(
        bind(&node, &data, &shapes, ShaclWritePolicy::Disabled).state,
        ShaclValidationState::Pending
    );

    let before = state(&node, &data);
    let error = node
        .apply_changes(
            &AllowAllAuthorizer,
            &data,
            vec![del(
                &data,
                data.as_str(),
                RDF_TYPE,
                iri("http://schema.org/Dataset"),
            )],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Update(UpdateError::ValidationFailed(_))
    ));
    assert_eq!(state(&node, &data), before);
}

#[cfg(feature = "search")]
fn hits(node: &CraqleNode, query: &str) -> Vec<(String, String)> {
    node.search(
        &AllowAllAuthorizer,
        craqle::SearchRequest { query, limit: 10 },
    )
    .unwrap()
    .into_iter()
    .map(|hit| (hit.graph_id, hit.subject_iri))
    .collect()
}

#[cfg(feature = "search")]
#[test]
fn enforce_search_atomic() {
    let (_directory, node) = open_node();
    let data = GraphId::new("urn:test:shacl-write-search-data");
    let shapes = GraphId::new("urn:test:shacl-write-search-shapes");
    limit_shape(&node, &shapes, SH_MAX, 1, VALUE, FOCUS);
    node.apply_changes_unchecked(&data, vec![add(&data, FOCUS, VALUE, iri("urn:test:one"))])
        .unwrap();
    assert_eq!(
        bind(&node, &data, &shapes, ShaclWritePolicy::Enforce).state,
        ShaclValidationState::Valid
    );
    node.flush_search_updates().unwrap();
    let before = hits(&node, "writeatomicprobe");
    assert!(before.is_empty());

    let error = node
        .apply_changes(
            &AllowAllAuthorizer,
            &data,
            vec![
                add(&data, FOCUS, VALUE, iri("urn:test:two")),
                add(&data, FOCUS, NAME, literal("writeatomicprobe")),
            ],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Update(UpdateError::ShaclValidationFailed(_))
    ));
    node.flush_search_updates().unwrap();
    assert_eq!(hits(&node, "writeatomicprobe"), before);
}
