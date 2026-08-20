#![cfg(feature = "shacl-core")]

mod support;

use craqle::{
    EncodedTerm, GraphId, MaterializedQuadChange, ShaclBinding, ShaclBindingOptions,
    ShaclValidationState, ValidationPolicy,
};

use crate::support::setup_network;

#[test]
fn remote_violation_converges() {
    let (_directory, net) = setup_network(2);
    let data = GraphId::new("urn:test:remote-policy-data");
    let shapes = GraphId::new("urn:test:remote-policy-shapes");
    let add = |graph: &GraphId, subject: &str, predicate: &str, object: &str| {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm(subject.to_string()),
            predicate: EncodedTerm(predicate.to_string()),
            object: EncodedTerm(object.to_string()),
        }
    };

    net.peer(0)
        .apply_changes_unchecked(
            &shapes,
            vec![
                add(
                    &shapes,
                    "<urn:test:remote-shape>",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<http://www.w3.org/ns/shacl#NodeShape>",
                ),
                add(
                    &shapes,
                    "<urn:test:remote-shape>",
                    "<http://www.w3.org/ns/shacl#targetSubjectsOf>",
                    "<urn:test:remote-value>",
                ),
                add(
                    &shapes,
                    "<urn:test:remote-shape>",
                    "<http://www.w3.org/ns/shacl#property>",
                    "<urn:test:remote-property>",
                ),
                add(
                    &shapes,
                    "<urn:test:remote-property>",
                    "<http://www.w3.org/ns/shacl#path>",
                    "<urn:test:remote-value>",
                ),
                add(
                    &shapes,
                    "<urn:test:remote-property>",
                    "<http://www.w3.org/ns/shacl#minCount>",
                    "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                ),
            ],
        )
        .unwrap();
    let second = add(
        &data,
        "<urn:test:remote-focus>",
        "<urn:test:remote-value>",
        "<urn:test:remote-two>",
    );
    net.peer(0)
        .apply_changes_unchecked(
            &data,
            vec![
                add(
                    &data,
                    "<urn:test:remote-focus>",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<urn:test:remote-class>",
                ),
                add(
                    &data,
                    "<urn:test:remote-focus>",
                    "<urn:test:remote-value>",
                    "<urn:test:remote-one>",
                ),
                second.clone(),
            ],
        )
        .unwrap();
    net.sync_until_converged(10).unwrap();
    let enforce = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy: ValidationPolicy::Enforce,
        validation_options: ShaclBindingOptions::default(),
    };
    assert_eq!(
        net.peer(0)
            .bind_shacl(&craqle::AllowAllAuthorizer, &enforce)
            .unwrap()
            .state,
        ShaclValidationState::Valid
    );
    assert_eq!(
        net.peer(1)
            .bind_shacl(
                &craqle::AllowAllAuthorizer,
                &ShaclBinding {
                    policy: ValidationPolicy::Advisory,
                    ..enforce.clone()
                }
            )
            .unwrap()
            .state,
        ShaclValidationState::Valid
    );

    let delete = match second {
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
    net.peer(1).apply_changes(&data, vec![delete]).unwrap();
    net.sync_until_converged(10).unwrap();

    assert_eq!(
        net.peer(0).graph_snapshot(&data).unwrap(),
        net.peer(1).graph_snapshot(&data).unwrap()
    );
    let left = net
        .peer(0)
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    let right = net
        .peer(1)
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(right.len(), 1);
    assert_eq!(left[0].binding.policy, ValidationPolicy::Enforce);
    assert_eq!(right[0].binding.policy, ValidationPolicy::Advisory);
    assert_eq!(left[0].state, ShaclValidationState::Invalid);
    assert_eq!(right[0].state, ShaclValidationState::Invalid);
    assert!(left[0].error.is_none());
    assert!(right[0].error.is_none());
    let left_report = left[0].report.as_ref().unwrap();
    let right_report = right[0].report.as_ref().unwrap();
    assert!(!left_report.conforms);
    assert!(!right_report.conforms);
    assert_eq!(left_report.results, right_report.results);
}

#[test]
fn remove_keeps_dot() {
    let (_directory, mut net) = setup_network(2);
    let data = GraphId::new("urn:test:dot-data");
    let shapes = GraphId::new("urn:test:dot-shapes");
    let add = |graph: &GraphId, subject: &str, predicate: &str, object: &str| {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm(subject.to_string()),
            predicate: EncodedTerm(predicate.to_string()),
            object: EncodedTerm(object.to_string()),
        }
    };
    let value = add(
        &data,
        "<urn:test:dot-focus>",
        "<urn:test:dot-value>",
        "<urn:test:dot-object>",
    );

    net.peer(0)
        .apply_changes_unchecked(
            &shapes,
            vec![
                add(
                    &shapes,
                    "<urn:test:dot-shape>",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<http://www.w3.org/ns/shacl#NodeShape>",
                ),
                add(
                    &shapes,
                    "<urn:test:dot-shape>",
                    "<http://www.w3.org/ns/shacl#targetNode>",
                    "<urn:test:dot-focus>",
                ),
                add(
                    &shapes,
                    "<urn:test:dot-shape>",
                    "<http://www.w3.org/ns/shacl#property>",
                    "<urn:test:dot-property>",
                ),
                add(
                    &shapes,
                    "<urn:test:dot-property>",
                    "<http://www.w3.org/ns/shacl#path>",
                    "<urn:test:dot-value>",
                ),
                add(
                    &shapes,
                    "<urn:test:dot-property>",
                    "<http://www.w3.org/ns/shacl#minCount>",
                    "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                ),
            ],
        )
        .unwrap();
    net.sync_until_converged(10).unwrap();
    net.peer(0)
        .apply_changes_unchecked(
            &data,
            vec![add(
                &data,
                "<urn:test:dot-seed>",
                "<urn:test:dot-seed-value>",
                "<urn:test:dot-seed-object>",
            )],
        )
        .unwrap();
    net.sync_until_converged(10).unwrap();

    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy: ValidationPolicy::Advisory,
        validation_options: ShaclBindingOptions::default(),
    };
    net.peer(0)
        .bind_shacl(&craqle::AllowAllAuthorizer, &binding)
        .unwrap();
    net.peer(1)
        .bind_shacl(&craqle::AllowAllAuthorizer, &binding)
        .unwrap();

    net.partition(0, 1);
    net.peer(0)
        .apply_changes_unchecked(&data, vec![value.clone()])
        .unwrap();
    net.peer(1)
        .apply_changes_unchecked(&data, vec![value.clone()])
        .unwrap();
    let delete = match value {
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
    net.peer(0).apply_changes(&data, vec![delete]).unwrap();

    net.heal(0, 1);
    net.sync_until_converged(10).unwrap();

    let left_snapshot = net.peer(0).graph_snapshot(&data).unwrap();
    let right_snapshot = net.peer(1).graph_snapshot(&data).unwrap();
    assert_eq!(left_snapshot, right_snapshot);
    let quad = left_snapshot
        .quads
        .iter()
        .find(|quad| {
            quad.subject.0 == "<urn:test:dot-focus>"
                && quad.predicate.0 == "<urn:test:dot-value>"
                && quad.object.0 == "<urn:test:dot-object>"
        })
        .unwrap();
    assert_eq!(quad.dots.len(), 1);

    let left = net
        .peer(0)
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    let right = net
        .peer(1)
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(right.len(), 1);
    assert_eq!(left[0].state, ShaclValidationState::Valid);
    assert_eq!(right[0].state, ShaclValidationState::Valid);
    assert!(left[0].error.is_none());
    assert!(right[0].error.is_none());
    let left_report = left[0].report.as_ref().unwrap();
    let right_report = right[0].report.as_ref().unwrap();
    assert!(left_report.conforms);
    assert!(right_report.conforms);
    assert_eq!(left_report.results, right_report.results);
}

#[test]
fn import_change_settles() {
    let (_directory, net) = setup_network(2);
    let data = GraphId::new("urn:test:import-settles-data");
    let shapes = GraphId::new("urn:test:import-settles-shapes");
    let imported = GraphId::new("urn:test:import-settles-imported");
    let add = |graph: &GraphId, subject: &str, predicate: &str, object: &str| {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm(subject.to_string()),
            predicate: EncodedTerm(predicate.to_string()),
            object: EncodedTerm(object.to_string()),
        }
    };

    net.peer(0)
        .apply_changes_unchecked(
            &shapes,
            vec![add(
                &shapes,
                "<urn:test:import-settles-ontology>",
                "<http://www.w3.org/2002/07/owl#imports>",
                &format!("<{}>", imported.as_str()),
            )],
        )
        .unwrap();
    net.peer(0)
        .apply_changes_unchecked(
            &imported,
            vec![
                add(
                    &imported,
                    "<urn:test:import-settles-shape>",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<http://www.w3.org/ns/shacl#NodeShape>",
                ),
                add(
                    &imported,
                    "<urn:test:import-settles-shape>",
                    "<http://www.w3.org/ns/shacl#targetNode>",
                    "<urn:test:import-settles-focus>",
                ),
                add(
                    &imported,
                    "<urn:test:import-settles-shape>",
                    "<http://www.w3.org/ns/shacl#property>",
                    "<urn:test:import-settles-property>",
                ),
                add(
                    &imported,
                    "<urn:test:import-settles-property>",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<http://www.w3.org/ns/shacl#PropertyShape>",
                ),
                add(
                    &imported,
                    "<urn:test:import-settles-property>",
                    "<http://www.w3.org/ns/shacl#path>",
                    "<urn:test:import-settles-value>",
                ),
                add(
                    &imported,
                    "<urn:test:import-settles-property>",
                    "<http://www.w3.org/ns/shacl#minCount>",
                    "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                ),
            ],
        )
        .unwrap();
    net.peer(0)
        .apply_changes_unchecked(
            &data,
            vec![add(
                &data,
                "<urn:test:import-settles-focus>",
                "<urn:test:import-settles-value>",
                "<urn:test:import-settles-object>",
            )],
        )
        .unwrap();
    net.sync_until_converged(10).unwrap();

    let binding = ShaclBinding {
        data_graph: data.clone(),
        shapes_graph: shapes.clone(),
        policy: ValidationPolicy::Advisory,
        validation_options: ShaclBindingOptions {
            allow_local_imports: true,
            ..ShaclBindingOptions::default()
        },
    };
    assert_eq!(
        net.peer(1)
            .bind_shacl(&craqle::AllowAllAuthorizer, &binding)
            .unwrap()
            .state,
        ShaclValidationState::Valid
    );

    net.peer(0)
        .apply_changes_unchecked(
            &imported,
            vec![add(
                &imported,
                "<urn:test:import-settles-property>",
                "<http://www.w3.org/ns/shacl#maxCount>",
                "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            )],
        )
        .unwrap();
    net.sync_until_converged(10).unwrap();

    let status = net
        .peer(1)
        .shacl_binding_statuses(&craqle::AllowAllAuthorizer, &data)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(status.state, ShaclValidationState::Invalid);
    assert!(status.error.is_none());
    assert!(!status.report.unwrap().conforms);
}
