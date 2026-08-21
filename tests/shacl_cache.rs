#![cfg(feature = "shacl-core")]

mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    ActorId, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange,
    QueryCancellation, ShaclCompileOptions, ShaclError, ShaclValidationOptions,
};

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const NODE_SHAPE: &str = "<http://www.w3.org/ns/shacl#NodeShape>";
const TARGET_NODE: &str = "<http://www.w3.org/ns/shacl#targetNode>";
const PROPERTY: &str = "<http://www.w3.org/ns/shacl#property>";
const PATH: &str = "<http://www.w3.org/ns/shacl#path>";
const ONE_OR_MORE: &str = "<http://www.w3.org/ns/shacl#oneOrMorePath>";
const DATATYPE: &str = "<http://www.w3.org/ns/shacl#datatype>";
const STRING: &str = "<http://www.w3.org/2001/XMLSchema#string>";
const FOCUS: &str = "<urn:test:cache:focus>";
const EDGE: &str = "<urn:test:cache:edge>";

fn node() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x72; 32])),
    )
    .unwrap();
    (directory, node)
}

fn insert(node: &CraqleNode, graph: &GraphId, triples: &[(&str, &str, &str)]) {
    let changes = triples
        .iter()
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm((*subject).to_owned()),
                predicate: EncodedTerm((*predicate).to_owned()),
                object: EncodedTerm((*object).to_owned()),
            },
        )
        .collect();
    node.apply_changes_unchecked(graph, changes).unwrap();
}

fn setup() -> (
    tempfile::TempDir,
    CraqleNode,
    GraphId,
    craqle::CompiledShaclSchema,
) {
    let (directory, node) = node();
    let data = GraphId::new("urn:test:cache:data");
    let shapes = GraphId::new("urn:test:cache:shapes");
    insert(
        &node,
        &shapes,
        &[
            ("<urn:test:cache:shape>", RDF_TYPE, NODE_SHAPE),
            ("<urn:test:cache:shape>", TARGET_NODE, FOCUS),
            ("<urn:test:cache:shape>", PROPERTY, "_:property"),
            ("_:property", PATH, "_:path"),
            ("_:path", ONE_OR_MORE, EDGE),
            ("_:property", DATATYPE, STRING),
        ],
    );
    insert(
        &node,
        &data,
        &[
            (FOCUS, EDGE, "<urn:test:cache:one>"),
            ("<urn:test:cache:one>", EDGE, "<urn:test:cache:two>"),
        ],
    );
    let schema = node
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &shapes,
            &ShaclCompileOptions::default(),
        )
        .unwrap();
    (directory, node, data, schema)
}

fn change(graph: &GraphId) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm("<urn:test:cache:other>".to_owned()),
        predicate: EncodedTerm("<urn:test:cache:unrelated>".to_owned()),
        object: EncodedTerm("<urn:test:cache:value>".to_owned()),
    }
}

#[test]
fn cache_cancelled() {
    let (_directory, node, data, schema) = setup();
    let baseline = node
        .validate_shacl(
            &craqle::AllowAllAuthorizer,
            &data,
            &schema,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert!(!baseline.conforms);

    let cancellation = QueryCancellation::new();
    cancellation.cancel();
    let error = node
        .validate_shacl_delta(
            &craqle::AllowAllAuthorizer,
            &data,
            &schema,
            &[],
            &ShaclValidationOptions {
                cancellation,
                ..ShaclValidationOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::ValidationCancelled)
    ));
}

#[test]
fn cache_limits() {
    let (_directory, node, data, schema) = setup();
    let baseline = node
        .validate_shacl(
            &craqle::AllowAllAuthorizer,
            &data,
            &schema,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert!(!baseline.conforms);
    let changes = [change(&data)];

    let error = node
        .validate_shacl_delta(
            &craqle::AllowAllAuthorizer,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions {
                max_path_edges: 1,
                ..ShaclValidationOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::PathBudgetExceeded { limit: 1 })
    ));

    let error = node
        .validate_shacl_delta(
            &craqle::AllowAllAuthorizer,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions {
                max_path_depth: 1,
                ..ShaclValidationOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::PathDepthExceeded { limit: 1 })
    ));
}
