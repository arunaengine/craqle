#![cfg(feature = "shacl-core")]

mod support;

use craqle::{
    CraqleError, CreateCrateRequest, EncodedTerm, GraphId, MaterializedQuadChange, NewDataEntity,
    RoCrateError, ShaclBinding, ShaclBindingOptions, ShaclValidationState, UpdateError,
    ValidationPolicy,
};

use crate::support::{benchmark_rocrate_document, public_policy, setup_network, writer_auth};

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const SHACL: &str = "<http://www.w3.org/ns/shacl#";
const HAS_PART: &str = "<http://schema.org/hasPart>";

fn add(graph: &GraphId, subject: &str, predicate: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm(subject.to_string()),
        predicate: EncodedTerm(predicate.to_string()),
        object: EncodedTerm(object.to_string()),
    }
}

#[test]
fn checked_bulk_enforces() {
    let (_directory, net) = setup_network(1);
    let node = net.peer(0);
    let data = GraphId::new("urn:test:bulk-data");
    let shapes = GraphId::new("urn:test:bulk-shapes");
    let writer = writer_auth();

    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            data.clone(),
            "Bulk enforcement",
            "Checked batches validate before commit.",
            "2026-08-20",
            None,
            public_policy(),
        ),
    )
    .unwrap();
    node.apply_changes_unchecked(
        &shapes,
        vec![
            add(
                &shapes,
                "<urn:test:bulk-shape>",
                RDF_TYPE,
                &format!("{SHACL}NodeShape>"),
            ),
            add(
                &shapes,
                "<urn:test:bulk-shape>",
                &format!("{SHACL}targetSubjectsOf>"),
                HAS_PART,
            ),
            add(
                &shapes,
                "<urn:test:bulk-shape>",
                &format!("{SHACL}property>"),
                "<urn:test:bulk-property>",
            ),
            add(
                &shapes,
                "<urn:test:bulk-property>",
                &format!("{SHACL}path>"),
                HAS_PART,
            ),
            add(
                &shapes,
                "<urn:test:bulk-property>",
                &format!("{SHACL}maxCount>"),
                "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ),
        ],
    )
    .unwrap();
    assert_eq!(
        node.bind_shacl(&ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes,
            policy: ValidationPolicy::Enforce,
            validation_options: ShaclBindingOptions::default(),
        })
        .unwrap()
        .state,
        ShaclValidationState::Valid
    );

    let snapshot = node.graph_snapshot(&data).unwrap();
    let clock = node.vector_clock(&data).unwrap();
    let index = node.query_index_status_fast().unwrap();
    let error = node
        .append_new_root_data_entities(
            &writer,
            &data,
            vec![NewDataEntity {
                entity_id: "./blocked.txt".to_string(),
                entity_type: "http://schema.org/MediaObject".to_string(),
                name: "Blocked".to_string(),
                additional_triples: Vec::new(),
            }],
        )
        .unwrap_err();
    match error {
        CraqleError::RoCrate(RoCrateError::Update(UpdateError::ShaclValidationFailed(reports))) => {
            assert_eq!(reports.len(), 1);
            assert!(!reports[0].conforms);
            assert_eq!(reports[0].results.len(), 1);
            assert!(!reports[0].statistics.stopped_early);
        }
        other => panic!("expected checked SHACL rejection, got {other:?}"),
    }
    assert_eq!(node.graph_snapshot(&data).unwrap(), snapshot);
    assert_eq!(node.vector_clock(&data).unwrap(), clock);
    assert_eq!(node.query_index_status_fast().unwrap(), index);
}

#[test]
fn import_enforce_empty() {
    let (_directory, net) = setup_network(1);
    let node = net.peer(0);
    let data = GraphId::new("urn:test:bulk-import-data");
    let shapes = GraphId::new("urn:test:bulk-import-shapes");
    let writer = writer_auth();

    node.apply_changes_unchecked(
        &data,
        vec![add(
            &data,
            "<urn:test:seed>",
            "<urn:test:seed-predicate>",
            "<urn:test:seed-object>",
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &data,
        vec![MaterializedQuadChange::Delete {
            graph: data.clone(),
            subject: EncodedTerm("<urn:test:seed>".to_string()),
            predicate: EncodedTerm("<urn:test:seed-predicate>".to_string()),
            object: EncodedTerm("<urn:test:seed-object>".to_string()),
        }],
    )
    .unwrap();
    node.import_graph_policy(&data, public_policy()).unwrap();
    node.apply_changes_unchecked(
        &shapes,
        vec![
            add(
                &shapes,
                "<urn:test:import-shape>",
                RDF_TYPE,
                &format!("{SHACL}NodeShape>"),
            ),
            add(
                &shapes,
                "<urn:test:import-shape>",
                &format!("{SHACL}targetSubjectsOf>"),
                HAS_PART,
            ),
            add(
                &shapes,
                "<urn:test:import-shape>",
                &format!("{SHACL}property>"),
                "<urn:test:import-property>",
            ),
            add(
                &shapes,
                "<urn:test:import-property>",
                &format!("{SHACL}path>"),
                HAS_PART,
            ),
            add(
                &shapes,
                "<urn:test:import-property>",
                &format!("{SHACL}maxCount>"),
                "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ),
        ],
    )
    .unwrap();
    assert_eq!(
        node.bind_shacl(&ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes,
            policy: ValidationPolicy::Enforce,
            validation_options: ShaclBindingOptions::default(),
        })
        .unwrap()
        .state,
        ShaclValidationState::Valid
    );

    let snapshot = node.graph_snapshot(&data).unwrap();
    let clock = node.vector_clock(&data).unwrap();
    let index = node.query_index_status_fast().unwrap();
    let document = benchmark_rocrate_document(&data, 1, "import", "Imported");
    let error = node
        .apply_rocrate_document(&writer, data.clone(), &document)
        .unwrap_err();
    match error {
        CraqleError::RoCrate(RoCrateError::Update(UpdateError::ShaclValidationFailed(reports))) => {
            assert_eq!(reports.len(), 1);
            assert!(!reports[0].conforms);
            assert_eq!(reports[0].results.len(), 1);
        }
        other => panic!("expected automatic SHACL rejection, got {other:?}"),
    }
    assert_eq!(node.graph_snapshot(&data).unwrap(), snapshot);
    assert_eq!(node.vector_clock(&data).unwrap(), clock);
    assert_eq!(node.query_index_status_fast().unwrap(), index);
}
