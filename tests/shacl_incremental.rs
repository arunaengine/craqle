#![cfg(feature = "shacl-core")]

use craqle::{
    ActorId, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange,
    ShaclBinding, ShaclBindingOptions, ShaclCompileOptions, ShaclValidationOptions,
    ShaclValidationState, UpdateError, ValidationPolicy,
};
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};

const SHAPES: &str = r#"
<urn:test:incremental-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:incremental-shape> <http://www.w3.org/ns/shacl#targetSubjectsOf> <urn:test:value> .
<urn:test:incremental-shape> <http://www.w3.org/ns/shacl#property> <urn:test:incremental-property> .
<urn:test:incremental-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:incremental-property> <http://www.w3.org/ns/shacl#minCount> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const CLASS_SHAPES: &str = r#"
<urn:test:class-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:class-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:class-focus> .
<urn:test:class-shape> <http://www.w3.org/ns/shacl#property> <urn:test:class-property> .
<urn:test:class-property> <http://www.w3.org/ns/shacl#path> <urn:test:class-value> .
<urn:test:class-property> <http://www.w3.org/ns/shacl#class> <urn:test:RequiredClass> .
"#;

const MAX_SHAPES: &str = r#"
<urn:test:max-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:max-shape> <http://www.w3.org/ns/shacl#targetSubjectsOf> <urn:test:value> .
<urn:test:max-shape> <http://www.w3.org/ns/shacl#property> <urn:test:max-property> .
<urn:test:max-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:max-property> <http://www.w3.org/ns/shacl#maxCount> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

fn node() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x71; 32])),
    )
    .unwrap();
    (directory, node)
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn insert_shape_text(node: &CraqleNode, graph: &GraphId, shapes: &str) {
    let parsed =
        OxigraphInMemory::from_str(shapes, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .unwrap();
    let changes = parsed
        .quads()
        .map(|quad| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object),
        })
        .collect();
    node.apply_changes_unchecked(graph, changes).unwrap();
}

fn insert_shapes(node: &CraqleNode, graph: &GraphId) {
    insert_shape_text(node, graph, SHAPES);
}

fn change(
    graph: &GraphId,
    insert: bool,
    subject: &str,
    predicate: &str,
    object: &str,
) -> MaterializedQuadChange {
    let fields = (graph.clone(), iri(subject), iri(predicate), iri(object));
    if insert {
        MaterializedQuadChange::Insert {
            graph: fields.0,
            subject: fields.1,
            predicate: fields.2,
            object: fields.3,
        }
    } else {
        MaterializedQuadChange::Delete {
            graph: fields.0,
            subject: fields.1,
            predicate: fields.2,
            object: fields.3,
        }
    }
}

#[test]
fn delta_matches_full() {
    // Direct changes stay local while unrelated predicates execute no shapes.
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:incremental-data");
    let shapes = GraphId::new("urn:test:incremental-shapes");
    insert_shapes(&node, &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:focus",
            "urn:test:value",
            "urn:test:one",
        )],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    let baseline = node.validate_shacl(&data, &schema, &options).unwrap();
    assert_eq!(baseline.results.len(), 1);

    let unrelated = vec![change(
        &data,
        true,
        "urn:test:other",
        "urn:test:unrelated",
        "urn:test:value",
    )];
    let unchanged = node
        .validate_shacl_delta(&data, &schema, &unrelated, &options)
        .unwrap();
    assert_eq!(unchanged.results, baseline.results);
    assert_eq!(unchanged.statistics.shapes_executed, 0);
    assert_eq!(unchanged.statistics.full_graph_fallbacks, 0);
    assert_eq!(unchanged.statistics.read.candidate_quads, 0);

    let relevant = vec![change(
        &data,
        true,
        "urn:test:focus",
        "urn:test:value",
        "urn:test:two",
    )];
    let incremental = node
        .validate_shacl_delta(&data, &schema, &relevant, &options)
        .unwrap();
    node.apply_changes_unchecked(&data, relevant).unwrap();
    let full = node.validate_shacl(&data, &schema, &options).unwrap();
    assert_eq!(incremental.results, full.results);
    assert!(incremental.conforms);
    assert_eq!(incremental.statistics.full_graph_fallbacks, 0);
    assert!(incremental.statistics.read.candidate_quads < 20);
}

#[test]
fn generated_delta_matches() {
    // Seeded insert/delete sequences must match full reports at every step.
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:incremental-generated-data");
    let shapes = GraphId::new("urn:test:incremental-generated-shapes");
    insert_shapes(&node, &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:seed",
            "urn:test:unrelated",
            "urn:test:seed-value",
        )],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    node.validate_shacl(&data, &schema, &options).unwrap();

    let mut state = 0x9e37_79b9_u64;
    for step in 0..64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let relevant = state & 3 != 0;
        let insert = state & 4 == 0;
        let subject = format!("urn:test:focus-{}", (state >> 8) % 8);
        let object = format!("urn:test:value-{}", (state >> 16) % 4);
        let predicate = if relevant {
            "urn:test:value"
        } else {
            "urn:test:unrelated"
        };
        let changes = vec![change(&data, insert, &subject, predicate, &object)];
        let incremental = node
            .validate_shacl_delta(&data, &schema, &changes, &options)
            .unwrap_or_else(|error| panic!("incremental step {step} failed: {error}"));
        node.apply_changes_unchecked(&data, changes).unwrap();
        let full = node
            .validate_shacl(&data, &schema, &options)
            .unwrap_or_else(|error| panic!("full step {step} failed: {error}"));
        assert_eq!(incremental.results, full.results, "step {step}");
    }
}

#[test]
fn missing_graph_errors() {
    // Validation must not treat a missing named graph as an empty graph.
    let (_directory, node) = node();
    let shapes = GraphId::new("urn:test:missing-shapes");
    insert_shapes(&node, &shapes);
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let error = node
        .validate_shacl(
            &GraphId::new("urn:test:missing-data"),
            &schema,
            &ShaclValidationOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(craqle::ShaclError::DataGraphNotFound { .. })
    ));
}

#[test]
fn class_delta_matches() {
    // A value-node type change must recheck the owning property-shape focus.
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:class-data");
    let shapes = GraphId::new("urn:test:class-shapes");
    insert_shape_text(&node, &shapes, CLASS_SHAPES);
    let value = change(
        &data,
        true,
        "urn:test:class-focus",
        "urn:test:class-value",
        "urn:test:class-object",
    );
    let value_type = change(
        &data,
        true,
        "urn:test:class-object",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "urn:test:RequiredClass",
    );
    node.apply_changes_unchecked(&data, vec![value, value_type.clone()])
        .unwrap();
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    assert!(
        node.validate_shacl(&data, &schema, &options)
            .unwrap()
            .conforms
    );
    let deletion = match value_type {
        MaterializedQuadChange::Insert {
            graph,
            subject,
            predicate,
            object,
        } => MaterializedQuadChange::Delete {
            graph,
            subject,
            predicate,
            object,
        },
        MaterializedQuadChange::Delete { .. } => unreachable!(),
    };
    let incremental = node
        .validate_shacl_delta(&data, &schema, std::slice::from_ref(&deletion), &options)
        .unwrap();
    node.apply_changes_unchecked(&data, vec![deletion]).unwrap();
    let full = node.validate_shacl(&data, &schema, &options).unwrap();
    assert_eq!(incremental.results, full.results);
    assert!(!incremental.conforms);
    assert_eq!(incremental.statistics.full_graph_fallbacks, 1);
}

#[test]
fn batch_state_matches() {
    // Insert then delete in one authored batch must commit the validated absence.
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:batch-data");
    let shapes = GraphId::new("urn:test:batch-shapes");
    insert_shape_text(&node, &shapes, MAX_SHAPES);
    node.apply_changes_unchecked(
        &data,
        vec![
            change(
                &data,
                true,
                "urn:test:batch-focus",
                "urn:test:value",
                "urn:test:one",
            ),
            change(
                &data,
                true,
                "urn:test:batch-focus",
                "urn:test:value",
                "urn:test:two",
            ),
        ],
    )
    .unwrap();
    node.bind_shacl(&ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy: ValidationPolicy::Enforce,
        validation_options: ShaclBindingOptions::default(),
    })
    .unwrap();
    let transient = change(
        &data,
        true,
        "urn:test:batch-focus",
        "urn:test:value",
        "urn:test:three",
    );
    let removal = match transient.clone() {
        MaterializedQuadChange::Insert {
            graph,
            subject,
            predicate,
            object,
        } => MaterializedQuadChange::Delete {
            graph,
            subject,
            predicate,
            object,
        },
        MaterializedQuadChange::Delete { .. } => unreachable!(),
    };
    node.apply_changes(&data, vec![transient, removal]).unwrap();
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    assert!(
        node.validate_shacl(&data, &schema, &ShaclValidationOptions::default())
            .unwrap()
            .conforms
    );
    assert!(
        !node
            .graph_snapshot(&data)
            .unwrap()
            .quads
            .iter()
            .any(|quad| quad.object == iri("urn:test:three"))
    );
}

#[test]
fn policy_report_persistence() {
    // Enforce rejects atomically while advisory reports survive restart.
    let (directory, node) = node();
    let data = GraphId::new("urn:test:policy-data");
    let shapes = GraphId::new("urn:test:policy-shapes");
    insert_shapes(&node, &shapes);
    let second_value = change(
        &data,
        true,
        "urn:test:policy-focus",
        "urn:test:value",
        "urn:test:two",
    );
    node.apply_changes_unchecked(
        &data,
        vec![
            change(
                &data,
                true,
                "urn:test:policy-focus",
                "urn:test:value",
                "urn:test:one",
            ),
            second_value.clone(),
        ],
    )
    .unwrap();
    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy: ValidationPolicy::Enforce,
        validation_options: ShaclBindingOptions::default(),
    };
    let bound = node.bind_shacl(&binding).unwrap();
    assert_eq!(bound.state, ShaclValidationState::Valid);
    let before = node.graph_snapshot(&data).unwrap();
    let before_index = node.query_index_status_fast().unwrap();
    let rejected = node
        .apply_changes(
            &data,
            vec![match second_value.clone() {
                MaterializedQuadChange::Insert {
                    graph,
                    subject,
                    predicate,
                    object,
                } => MaterializedQuadChange::Delete {
                    graph,
                    subject,
                    predicate,
                    object,
                },
                MaterializedQuadChange::Delete { .. } => unreachable!(),
            }],
        )
        .unwrap_err();
    assert!(matches!(
        rejected,
        CraqleError::Update(UpdateError::ShaclValidationFailed(_))
    ));
    assert_eq!(node.graph_snapshot(&data).unwrap(), before);
    assert_eq!(node.query_index_status_fast().unwrap(), before_index);

    node.unbind_shacl(&data, &shapes).unwrap();
    let advisory = ShaclBinding {
        policy: ValidationPolicy::Advisory,
        ..binding
    };
    node.bind_shacl(&advisory).unwrap();
    node.apply_changes(
        &data,
        vec![match second_value {
            MaterializedQuadChange::Insert {
                graph,
                subject,
                predicate,
                object,
            } => MaterializedQuadChange::Delete {
                graph,
                subject,
                predicate,
                object,
            },
            MaterializedQuadChange::Delete { .. } => unreachable!(),
        }],
    )
    .unwrap();
    let statuses = node.shacl_binding_statuses(&data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert_eq!(statuses[0].report.as_ref().unwrap().results.len(), 1);

    drop(node);
    let reopened = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x72; 32])),
    )
    .unwrap();
    let statuses = reopened.shacl_binding_statuses(&data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert_eq!(statuses[0].report.as_ref().unwrap().results.len(), 1);
}
