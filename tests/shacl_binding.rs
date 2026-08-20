#![cfg(feature = "shacl-core")]

use craqle::{
    ActorId, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange, ShaclBinding,
    ShaclBindingOptions, ShaclValidationState, ValidationPolicy,
};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROP: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const SH_TARGET: &str = "http://www.w3.org/ns/shacl#targetNode";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_MIN: &str = "http://www.w3.org/ns/shacl#minCount";
const SH_MAX: &str = "http://www.w3.org/ns/shacl#maxCount";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn node() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x75; 32])),
    )
    .unwrap();
    (directory, node)
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn int(value: u8) -> EncodedTerm {
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

#[test]
fn imports_settle_invalid() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:binding-data");
    let root = GraphId::new("urn:test:binding-root");
    let imported = GraphId::new("urn:test:binding-import");
    let focus = "urn:test:binding-focus";
    let value = "urn:test:binding-value";
    let shape = "urn:test:binding-shape";
    let minimum = "urn:test:binding-minimum";

    node.apply_changes_unchecked(
        &root,
        vec![add(
            &root,
            "urn:test:binding-ontology",
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &imported,
        vec![
            add(&imported, shape, RDF_TYPE, iri(SH_NODE)),
            add(&imported, shape, SH_TARGET, iri(focus)),
            add(&imported, shape, SH_PROPERTY, iri(minimum)),
            add(&imported, minimum, RDF_TYPE, iri(SH_PROP)),
            add(&imported, minimum, SH_PATH, iri(value)),
            add(&imported, minimum, SH_MIN, int(1)),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![
            add(&data, focus, value, iri("urn:test:binding-object")),
            add(
                &data,
                focus,
                "urn:test:binding-unrelated",
                iri("urn:test:binding-other"),
            ),
        ],
    )
    .unwrap();

    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: root.clone(),
        policy: ValidationPolicy::Advisory,
        validation_options: ShaclBindingOptions {
            allow_local_imports: true,
            ..ShaclBindingOptions::default()
        },
    };
    assert_eq!(
        node.bind_shacl(&craqle::AllowAllAuthorizer, &binding)
            .unwrap()
            .state,
        ShaclValidationState::Valid
    );

    let strict = "urn:test:binding-strict";
    node.apply_changes_unchecked(
        &imported,
        vec![
            add(&imported, shape, SH_PROPERTY, iri(strict)),
            add(&imported, strict, RDF_TYPE, iri(SH_PROP)),
            add(&imported, strict, SH_PATH, iri(value)),
            add(&imported, strict, SH_MAX, int(0)),
        ],
    )
    .unwrap();

    let statuses = node
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);

    node.apply_changes(
        &data,
        vec![del(
            &data,
            focus,
            "urn:test:binding-unrelated",
            iri("urn:test:binding-other"),
        )],
    )
    .unwrap();
    let statuses = node
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert_eq!(statuses[0].report.as_ref().unwrap().results.len(), 1);
}

#[test]
fn deleted_shapes_block() {
    for imported_shapes in [false, true] {
        let (_directory, node) = node();
        let data = GraphId::new("urn:test:deleted-data");
        let root = GraphId::new("urn:test:deleted-root");
        let imported = GraphId::new("urn:test:deleted-import");
        let shapes = if imported_shapes { &imported } else { &root };
        let focus = "urn:test:deleted-focus";
        let value = "urn:test:deleted-value";
        let shape = "urn:test:deleted-shape";
        let minimum = "urn:test:deleted-minimum";

        if imported_shapes {
            node.apply_changes_unchecked(
                &root,
                vec![add(
                    &root,
                    "urn:test:deleted-ontology",
                    OWL_IMPORTS,
                    iri(imported.as_str()),
                )],
            )
            .unwrap();
        }
        node.apply_changes_unchecked(
            shapes,
            vec![
                add(shapes, shape, RDF_TYPE, iri(SH_NODE)),
                add(shapes, shape, SH_TARGET, iri(focus)),
                add(shapes, shape, SH_PROPERTY, iri(minimum)),
                add(shapes, minimum, RDF_TYPE, iri(SH_PROP)),
                add(shapes, minimum, SH_PATH, iri(value)),
                add(shapes, minimum, SH_MIN, int(1)),
            ],
        )
        .unwrap();
        node.apply_changes_unchecked(
            &data,
            vec![add(&data, focus, value, iri("urn:test:deleted-object"))],
        )
        .unwrap();

        let binding = ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: root.clone(),
            policy: ValidationPolicy::Enforce,
            validation_options: ShaclBindingOptions {
                allow_local_imports: imported_shapes,
                ..ShaclBindingOptions::default()
            },
        };
        assert_eq!(
            node.bind_shacl(&craqle::AllowAllAuthorizer, &binding)
                .unwrap()
                .state,
            ShaclValidationState::Valid
        );

        node.delete_graph_unchecked(shapes).unwrap();
        let statuses = node
            .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
            .unwrap();
        assert_eq!(statuses.len(), 1);
        assert!(matches!(
            statuses[0].state,
            ShaclValidationState::Pending | ShaclValidationState::Failed
        ));

        assert!(
            node.apply_changes(
                &data,
                vec![add(
                    &data,
                    focus,
                    "urn:test:deleted-unrelated",
                    iri("urn:test:deleted-other"),
                )],
            )
            .is_err()
        );
        let statuses = node
            .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
            .unwrap();
        assert_eq!(statuses.len(), 1);
        assert!(matches!(
            statuses[0].state,
            ShaclValidationState::Pending | ShaclValidationState::Failed
        ));
    }
}

#[test]
fn import_data_settles() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:import-data");
    let root = GraphId::new("urn:test:import-root");
    let imported = GraphId::new("urn:test:import-shapes");
    let focus = "urn:test:import-focus";
    let value = "urn:test:import-value";
    let shape = "urn:test:import-shape";
    let minimum = "urn:test:import-minimum";

    node.apply_changes_unchecked(
        &root,
        vec![add(
            &root,
            "urn:test:import-ontology",
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &imported,
        vec![
            add(
                &imported,
                "urn:test:import-ontology",
                OWL_IMPORTS,
                iri(data.as_str()),
            ),
            add(&imported, shape, RDF_TYPE, iri(SH_NODE)),
            add(&imported, shape, SH_TARGET, iri(focus)),
            add(&imported, shape, SH_PROPERTY, iri(minimum)),
            add(&imported, minimum, RDF_TYPE, iri(SH_PROP)),
            add(&imported, minimum, SH_PATH, iri(value)),
            add(&imported, minimum, SH_MIN, int(1)),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![add(&data, focus, value, iri("urn:test:import-object"))],
    )
    .unwrap();

    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: root,
        policy: ValidationPolicy::Enforce,
        validation_options: ShaclBindingOptions {
            allow_local_imports: true,
            ..ShaclBindingOptions::default()
        },
    };
    assert_eq!(
        node.bind_shacl(&craqle::AllowAllAuthorizer, &binding)
            .unwrap()
            .state,
        ShaclValidationState::Valid
    );

    node.apply_changes_unchecked(
        &imported,
        vec![add(
            &imported,
            focus,
            "urn:test:import-unchecked",
            iri("urn:test:import-other"),
        )],
    )
    .unwrap();
    let statuses = node
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Valid);

    assert!(
        node.apply_changes(
            &data,
            vec![add(
                &data,
                focus,
                "urn:test:import-enforced",
                iri("urn:test:import-new"),
            )],
        )
        .is_err()
    );
    let statuses = node
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Valid);
    assert!(statuses[0].report.is_some());
}

#[test]
fn data_binding_cleanup() {
    let (directory, node) = node();
    let data = GraphId::new("urn:test:delete-binding-data");
    let root = GraphId::new("urn:test:delete-binding-root");
    let imported = GraphId::new("urn:test:delete-binding-import");
    let focus = "urn:test:delete-binding-focus";
    let value = "urn:test:delete-binding-value";
    let shape = "urn:test:delete-binding-shape";
    let property = "urn:test:delete-binding-property";

    node.apply_changes_unchecked(
        &root,
        vec![add(
            &root,
            "urn:test:delete-binding-ontology",
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &imported,
        vec![
            add(&imported, shape, RDF_TYPE, iri(SH_NODE)),
            add(&imported, shape, SH_TARGET, iri(focus)),
            add(&imported, shape, SH_PROPERTY, iri(property)),
            add(&imported, property, RDF_TYPE, iri(SH_PROP)),
            add(&imported, property, SH_PATH, iri(value)),
            add(&imported, property, SH_MAX, int(0)),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            focus,
            value,
            iri("urn:test:delete-binding-value"),
        )],
    )
    .unwrap();
    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: root.clone(),
        policy: ValidationPolicy::Advisory,
        validation_options: ShaclBindingOptions {
            allow_local_imports: true,
            ..ShaclBindingOptions::default()
        },
    };
    let old = node
        .bind_shacl(&craqle::AllowAllAuthorizer, &binding)
        .unwrap();
    assert_eq!(old.state, ShaclValidationState::Invalid);
    assert!(old.report.as_ref().is_some_and(|report| !report.conforms));

    node.delete_graph_unchecked(&data).unwrap();
    assert!(!node.contains_graph(&data).unwrap());
    drop(node);

    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x75; 32])),
    )
    .unwrap();
    // Each dependency write would find a dangling reverse record if deletion
    // had left one behind. Reopening also proves the deleted queue cannot replay.
    node.apply_changes_unchecked(
        &root,
        vec![add(
            &root,
            "urn:test:delete-binding-root-note",
            "urn:test:delete-binding-note",
            iri("urn:test:delete-binding-root-value"),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &imported,
        vec![add(
            &imported,
            "urn:test:delete-binding-import-note",
            "urn:test:delete-binding-note",
            iri("urn:test:delete-binding-import-value"),
        )],
    )
    .unwrap();

    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            "urn:test:delete-binding-recreated",
            "urn:test:delete-binding-note",
            iri("urn:test:delete-binding-recreated-value"),
        )],
    )
    .unwrap();
    assert!(node.contains_graph(&data).unwrap());
    assert!(
        node.shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
            .unwrap()
            .is_empty()
    );

    let fresh = node
        .bind_shacl(&craqle::AllowAllAuthorizer, &binding)
        .unwrap();
    assert_eq!(fresh.state, ShaclValidationState::Valid);
    assert!(fresh.report.as_ref().is_some_and(|report| report.conforms));
}
