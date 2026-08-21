#![cfg(feature = "shacl-core")]

mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    ActorId, AllowAllAuthorizer, CompiledShaclSchema, CraqleError, CraqleNode, CraqleOptions,
    EncodedTerm, GraphId, MaterializedQuadChange, ShaclBinding, ShaclBindingOptions,
    ShaclCompileOptions, ShaclError, ShaclExecutionMode, ShaclValidationOptions,
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

const GLOBAL_SHAPES: &str = r#"
<urn:test:global-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:global-shape> <http://www.w3.org/ns/shacl#targetSubjectsOf> <urn:test:walk> .
<urn:test:global-shape> <http://www.w3.org/ns/shacl#property> <urn:test:global-property> .
<urn:test:global-property> <http://www.w3.org/ns/shacl#path> _:global-path .
_:global-path <http://www.w3.org/ns/shacl#oneOrMorePath> <urn:test:walk> .
<urn:test:global-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const ZERO_SHAPES: &str = r#"
<urn:test:zero-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:zero-shape> <http://www.w3.org/ns/shacl#targetSubjectsOf> <urn:test:zero-value> .
"#;

const IMPORT_ROOT: &str = r#"
<urn:test:incremental-import-root> <http://www.w3.org/2002/07/owl#imports> <urn:test:incremental-import> .
"#;

const TARGET_NODE_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const TARGET_CLASS_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetClass> <urn:test:MatrixClass> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const TARGET_OBJECTS_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetObjectsOf> <urn:test:rel> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const INVERSE_PATH_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> _:inverse .
_:inverse <http://www.w3.org/ns/shacl#inversePath> <urn:test:rel> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const SEQUENCE_PATH_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> _:sequence .
_:sequence <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <urn:test:first> .
_:sequence <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:tail .
_:tail <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <urn:test:second> .
_:tail <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const ZERO_OR_ONE_PATH_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> _:path .
_:path <http://www.w3.org/ns/shacl#zeroOrOnePath> <urn:test:rel> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#maxCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const ZERO_OR_MORE_PATH_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> _:path .
_:path <http://www.w3.org/ns/shacl#zeroOrMorePath> <urn:test:rel> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#maxCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const ONE_OR_MORE_PATH_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> _:path .
_:path <http://www.w3.org/ns/shacl#oneOrMorePath> <urn:test:rel> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

const LOGICAL_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#and> _:and .
_:and <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <urn:test:matrix-left> .
_:and <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:tail .
_:tail <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <urn:test:matrix-right> .
_:tail <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
<urn:test:matrix-left> <http://www.w3.org/ns/shacl#class> <urn:test:LeftClass> .
<urn:test:matrix-right> <http://www.w3.org/ns/shacl#class> <urn:test:RightClass> .
"#;

const QUALIFIED_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#qualifiedValueShape> <urn:test:qualified> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#qualifiedMinCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:test:qualified> <http://www.w3.org/ns/shacl#class> <urn:test:GoodClass> .
"#;

const CLOSED_SHAPES: &str = r#"
<urn:test:matrix-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#closed> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
<urn:test:matrix-shape> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property> .
<urn:test:matrix-property> <http://www.w3.org/ns/shacl#path> <urn:test:allowed> .
"#;

const SHARED_DEPENDENCY_SHAPES: &str = r#"
<urn:test:matrix-shape-a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape-a> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape-a> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property-a> .
<urn:test:matrix-property-a> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property-a> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:test:matrix-shape-b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:matrix-shape-b> <http://www.w3.org/ns/shacl#targetNode> <urn:test:matrix-focus> .
<urn:test:matrix-shape-b> <http://www.w3.org/ns/shacl#property> <urn:test:matrix-property-b> .
<urn:test:matrix-property-b> <http://www.w3.org/ns/shacl#path> <urn:test:value> .
<urn:test:matrix-property-b> <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

struct ModeCase {
    name: &'static str,
    shapes: &'static str,
    seed: &'static [(&'static str, &'static str, &'static str)],
    changes: &'static [(bool, &'static str, &'static str, &'static str)],
}

const AUTH: AllowAllAuthorizer = AllowAllAuthorizer;

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

fn mode_options(execution_mode: ShaclExecutionMode) -> ShaclValidationOptions {
    ShaclValidationOptions {
        execution_mode,
        ..ShaclValidationOptions::default()
    }
}

fn assert_mode_case(case: &ModeCase) {
    let (_directory, node) = node();
    let data = GraphId::new(&format!("urn:test:auto-matrix:{}:data", case.name));
    let shapes = GraphId::new(&format!("urn:test:auto-matrix:{}:shapes", case.name));
    insert_shape_text(&node, &shapes, case.shapes);
    node.apply_changes_unchecked(
        &data,
        case.seed
            .iter()
            .map(|(subject, predicate, object)| change(&data, true, subject, predicate, object))
            .collect(),
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let changes = case
        .changes
        .iter()
        .map(|(insert, subject, predicate, object)| {
            change(&data, *insert, subject, predicate, object)
        })
        .collect::<Vec<_>>();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    let delta = node.validate_shacl_delta(
        &AUTH,
        &data,
        &schema,
        &changes,
        &mode_options(ShaclExecutionMode::ForceDelta),
    );
    if let Ok(delta) = &delta {
        assert_eq!(delta.conforms, full.conforms, "{}", case.name);
        assert_eq!(delta.results, full.results, "{}", case.name);
        assert_eq!(
            delta.statistics.selected_mode,
            ShaclExecutionMode::ForceDelta,
            "{}",
            case.name
        );
    } else {
        assert!(matches!(
            delta,
            Err(CraqleError::Shacl(
                ShaclError::DeltaExecutionUnavailable { .. }
            ))
        ));
    }
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::Auto),
        )
        .unwrap();
    assert_eq!(auto.conforms, full.conforms, "{}", case.name);
    assert_eq!(auto.results, full.results, "{}", case.name);
    assert_ne!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::Auto,
        "{}",
        case.name
    );
    eprintln!(
        "case={} estimated_delta_work={} estimated_full_work={} selected={:?} \
         candidate_reads={} focus_nodes={} constraints_evaluated={}",
        case.name,
        auto.statistics.estimated_delta_work,
        auto.statistics.estimated_full_work,
        auto.statistics.selected_mode,
        auto.statistics.read.candidate_quads,
        auto.statistics.focus_nodes,
        auto.statistics.constraints_evaluated,
    );
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
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    let delta_options = mode_options(ShaclExecutionMode::ForceDelta);
    let baseline = node
        .validate_shacl(&AUTH, &data, &schema, &options)
        .unwrap();
    assert_eq!(baseline.results.len(), 1);

    let unrelated = vec![change(
        &data,
        true,
        "urn:test:other",
        "urn:test:unrelated",
        "urn:test:value",
    )];
    let unchanged = node
        .validate_shacl_delta(&AUTH, &data, &schema, &unrelated, &delta_options)
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
        .validate_shacl_delta(&AUTH, &data, &schema, &relevant, &delta_options)
        .unwrap();
    node.apply_changes_unchecked(&data, relevant).unwrap();
    let full = node
        .validate_shacl(&AUTH, &data, &schema, &options)
        .unwrap();
    assert_eq!(incremental.results, full.results);
    assert!(incremental.conforms);
    assert_eq!(incremental.statistics.full_graph_fallbacks, 0);
    assert!(incremental.statistics.read.candidate_quads < 20);
}

#[test]
fn modes_match_reports() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:modes-data");
    let shapes = GraphId::new("urn:test:modes-shapes");
    insert_shapes(&node, &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:modes-focus",
            "urn:test:value",
            "urn:test:one",
        )],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let changes = vec![change(
        &data,
        true,
        "urn:test:modes-focus",
        "urn:test:value",
        "urn:test:two",
    )];
    let delta = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    assert_eq!(delta.results, full.results);
    assert_eq!(delta.conforms, full.conforms);
    assert_eq!(
        delta.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert_eq!(full.statistics.selected_mode, ShaclExecutionMode::ForceFull);
    let error = node
        .validate_shacl(
            &AUTH,
            &data,
            &schema,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::DeltaExecutionUnavailable { .. })
    ));
}

#[test]
fn auto_force_mode_correctness_matrix() {
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let cases = [
        ModeCase {
            name: "target-node-direct",
            shapes: TARGET_NODE_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "target-class-insert",
            shapes: TARGET_CLASS_SHAPES,
            seed: &[("urn:test:matrix-focus", RDF_TYPE, "urn:test:MatrixClass")],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "target-class-delete",
            shapes: TARGET_CLASS_SHAPES,
            seed: &[
                ("urn:test:matrix-focus", RDF_TYPE, "urn:test:MatrixClass"),
                (
                    "urn:test:matrix-focus",
                    "urn:test:value",
                    "urn:test:value-1",
                ),
            ],
            changes: &[(
                false,
                "urn:test:matrix-focus",
                RDF_TYPE,
                "urn:test:MatrixClass",
            )],
        },
        ModeCase {
            name: "target-subjects-of",
            shapes: SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-2",
            )],
        },
        ModeCase {
            name: "target-objects-of",
            shapes: TARGET_OBJECTS_SHAPES,
            seed: &[(
                "urn:test:matrix-source",
                "urn:test:rel",
                "urn:test:matrix-focus",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "inverse-path",
            shapes: INVERSE_PATH_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-source",
                "urn:test:rel",
                "urn:test:matrix-focus",
            )],
        },
        ModeCase {
            name: "sequence-path",
            shapes: SEQUENCE_PATH_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:first",
                "urn:test:matrix-middle",
            )],
            changes: &[(
                true,
                "urn:test:matrix-middle",
                "urn:test:second",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "zero-or-one-path",
            shapes: ZERO_OR_ONE_PATH_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:rel",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "zero-or-more-path",
            shapes: ZERO_OR_MORE_PATH_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:rel",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "one-or-more-path",
            shapes: ONE_OR_MORE_PATH_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:rel",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "logical-nested",
            shapes: LOGICAL_SHAPES,
            seed: &[("urn:test:matrix-focus", RDF_TYPE, "urn:test:LeftClass")],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                RDF_TYPE,
                "urn:test:RightClass",
            )],
        },
        ModeCase {
            name: "qualified-value",
            shapes: QUALIFIED_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:matrix-member",
            )],
            changes: &[(
                true,
                "urn:test:matrix-member",
                RDF_TYPE,
                "urn:test:GoodClass",
            )],
        },
        ModeCase {
            name: "closed-shape",
            shapes: CLOSED_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:allowed",
                "urn:test:value-1",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:extra",
                "urn:test:value-2",
            )],
        },
        ModeCase {
            name: "unrelated-predicate",
            shapes: SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
            changes: &[(
                true,
                "urn:test:other",
                "urn:test:unrelated",
                "urn:test:value-2",
            )],
        },
        ModeCase {
            name: "shared-dependencies",
            shapes: SHARED_DEPENDENCY_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:seed",
                "urn:test:seed-value",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "duplicate-insert",
            shapes: TARGET_NODE_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
            changes: &[(
                true,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
        },
        ModeCase {
            name: "absent-delete",
            shapes: TARGET_NODE_SHAPES,
            seed: &[(
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:value-1",
            )],
            changes: &[(
                false,
                "urn:test:matrix-focus",
                "urn:test:value",
                "urn:test:missing",
            )],
        },
    ];

    for case in &cases {
        assert_mode_case(case);
    }
}

#[test]
fn auto_handles_skewed_high_frequency_predicates() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:auto-skew:data");
    let shapes = GraphId::new("urn:test:auto-skew:shapes");
    insert_shape_text(&node, &shapes, TARGET_NODE_SHAPES);
    let mut seed = vec![change(
        &data,
        true,
        "urn:test:matrix-focus",
        "urn:test:seed",
        "urn:test:seed-value",
    )];
    for index in 0..2_000 {
        seed.push(change(
            &data,
            true,
            &format!("urn:test:skew-subject:{index}"),
            "urn:test:common",
            &format!("urn:test:skew-object:{index}"),
        ));
    }
    node.apply_changes_unchecked(&data, seed).unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let changes = vec![change(
        &data,
        true,
        "urn:test:matrix-focus",
        "urn:test:value",
        "urn:test:value-1",
    )];
    let delta = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::Auto),
        )
        .unwrap();
    assert_eq!(delta.results, full.results);
    assert_eq!(auto.results, full.results);
    assert_eq!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert!(auto.statistics.estimated_delta_work < auto.statistics.estimated_full_work);
    assert!(auto.statistics.read.candidate_quads < 100);
}

#[test]
fn imported_shape_change_invalidates_then_reestimates() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:auto-import:data");
    let root = GraphId::new("urn:test:incremental-import-root");
    let imported = GraphId::new("urn:test:incremental-import");
    insert_shape_text(&node, &imported, TARGET_NODE_SHAPES);
    insert_shape_text(&node, &root, IMPORT_ROOT);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:matrix-focus",
            "urn:test:seed",
            "urn:test:seed-value",
        )],
    )
    .unwrap();
    let compile_options = ShaclCompileOptions {
        allow_local_imports: true,
        ..ShaclCompileOptions::default()
    };
    let old_schema = node.compile_shacl(&AUTH, &root, &compile_options).unwrap();
    insert_shape_text(
        &node,
        &imported,
        r#"
<urn:test:imported-extra> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:test:imported-extra> <http://www.w3.org/ns/shacl#targetNode> <urn:test:unrelated-focus> .
"#,
    );
    let changes = vec![change(
        &data,
        true,
        "urn:test:matrix-focus",
        "urn:test:value",
        "urn:test:value-1",
    )];
    let stale = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &old_schema,
            &changes,
            &mode_options(ShaclExecutionMode::Auto),
        )
        .unwrap_err();
    assert!(matches!(
        stale,
        CraqleError::Shacl(ShaclError::SchemaChangedDuringValidation { .. })
    ));

    let schema = node.compile_shacl(&AUTH, &root, &compile_options).unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let delta = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::Auto),
        )
        .unwrap();
    assert_eq!(delta.results, full.results);
    assert_eq!(auto.results, full.results);
    assert_ne!(auto.statistics.selected_mode, ShaclExecutionMode::Auto);
}

#[test]
fn cold_delta_limit() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:cold-limit-data");
    let shapes = GraphId::new("urn:test:cold-limit-shapes");
    insert_shapes(&node, &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![
            change(
                &data,
                true,
                "urn:test:cold-limit-one",
                "urn:test:value",
                "urn:test:one",
            ),
            change(
                &data,
                true,
                "urn:test:cold-limit-two",
                "urn:test:value",
                "urn:test:one",
            ),
        ],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let changes = [
        change(
            &data,
            true,
            "urn:test:cold-limit-one",
            "urn:test:value",
            "urn:test:two",
        ),
        change(
            &data,
            true,
            "urn:test:cold-limit-two",
            "urn:test:value",
            "urn:test:two",
        ),
    ];
    let mut delta_options = mode_options(ShaclExecutionMode::ForceDelta);
    delta_options.max_results = 1;
    let error = node
        .validate_shacl_delta(&AUTH, &data, &schema, &changes, &delta_options)
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::DeltaExecutionUnavailable { .. })
    ));

    let mut full_options = mode_options(ShaclExecutionMode::ForceFull);
    full_options.max_results = 1;
    let full = node
        .validate_shacl_delta(&AUTH, &data, &schema, &changes, &full_options)
        .unwrap();
    assert!(full.conforms);
    assert!(full.results.is_empty());
}

#[test]
fn cold_auto_full() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:cold-auto-data");
    let shapes = GraphId::new("urn:test:cold-auto-shapes");
    insert_shapes(&node, &shapes);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:cold-auto-focus",
            "urn:test:value",
            "urn:test:one",
        )],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let changes = [change(
        &data,
        true,
        "urn:test:cold-auto-focus",
        "urn:test:value",
        "urn:test:two",
    )];
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    assert_eq!(auto.statistics.selected_mode, ShaclExecutionMode::ForceFull);
    assert_eq!(auto.results, full.results);
    assert_eq!(auto.conforms, full.conforms);
}

#[test]
fn zero_auto_delta() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:zero-auto-data");
    let shapes = GraphId::new("urn:test:zero-auto-shapes");
    insert_shapes(&node, &shapes);
    insert_shape_text(&node, &shapes, ZERO_SHAPES);
    let seed = (0..16)
        .map(|index| {
            change(
                &data,
                true,
                &format!("urn:test:zero-auto-focus-{index}"),
                "urn:test:value",
                &format!("urn:test:zero-auto-value-{index}"),
            )
        })
        .collect();
    node.apply_changes_unchecked(&data, seed).unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &[change(
                &data,
                true,
                "urn:test:zero-auto-focus-0",
                "urn:test:value",
                "urn:test:zero-auto-next",
            )],
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert!(auto.statistics.estimated_full_work > 0);
    assert!(auto.statistics.read.qv_counter_reads >= 4);
}

#[test]
fn auto_selects_paths() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:auto-data");
    let shapes = GraphId::new("urn:test:auto-shapes");
    insert_shapes(&node, &shapes);
    let seed = (0..128)
        .map(|index| {
            change(
                &data,
                true,
                &format!("urn:test:auto-focus-{index}"),
                "urn:test:value",
                &format!("urn:test:auto-value-{index}"),
            )
        })
        .collect();
    node.apply_changes_unchecked(&data, seed).unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let small = [change(
        &data,
        true,
        "urn:test:auto-focus-0",
        "urn:test:value",
        "urn:test:auto-small",
    )];
    let small = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &small,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(
        small.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert!(small.statistics.estimated_delta_work < small.statistics.estimated_full_work);

    let unrelated = [change(
        &data,
        true,
        "urn:test:auto-other",
        "urn:test:unrelated",
        "urn:test:auto-value",
    )];
    let unrelated = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &unrelated,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(
        unrelated.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );

    for count in [100usize, 1_000] {
        let changes = (0..count)
            .map(|index| {
                change(
                    &data,
                    true,
                    &format!("urn:test:auto-large-{count}-{index}"),
                    "urn:test:value",
                    &format!("urn:test:auto-large-value-{index}"),
                )
            })
            .collect::<Vec<_>>();
        let auto = node
            .validate_shacl_delta(
                &AUTH,
                &data,
                &schema,
                &changes,
                &ShaclValidationOptions::default(),
            )
            .unwrap();
        let full = node
            .validate_shacl_delta(
                &AUTH,
                &data,
                &schema,
                &changes,
                &mode_options(ShaclExecutionMode::ForceFull),
            )
            .unwrap();
        assert_eq!(auto.statistics.selected_mode, ShaclExecutionMode::ForceFull);
        assert!(auto.statistics.estimated_delta_work > auto.statistics.estimated_full_work);
        assert_eq!(auto.results, full.results, "batch {count}");
    }
}

fn auto_graph(
    label: &str,
    rows: usize,
) -> (tempfile::TempDir, CraqleNode, GraphId, CompiledShaclSchema) {
    let (directory, node) = node();
    let data = GraphId::new(&format!("urn:test:auto-size-{label}"));
    let shapes = GraphId::new(&format!("urn:test:auto-size-{label}-shapes"));
    insert_shapes(&node, &shapes);
    let seed = (0..rows)
        .map(|index| {
            change(
                &data,
                true,
                &format!("urn:test:auto-size-{label}-{index}"),
                "urn:test:noise",
                "urn:test:auto-noise",
            )
        })
        .collect();
    node.apply_changes_unchecked(&data, seed).unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    (directory, node, data, schema)
}

#[test]
fn auto_scales() {
    let (_small_dir, small_node, small_data, small_schema) = auto_graph("small", 128);
    let (_large_dir, large_node, large_data, large_schema) = auto_graph("large", 16_384);
    let changes = |data: &GraphId, count: usize| {
        (0..count)
            .map(|index| {
                change(
                    data,
                    true,
                    &format!("urn:test:auto-change-{count}-{index}"),
                    "urn:test:value",
                    "urn:test:auto-value",
                )
            })
            .collect::<Vec<_>>()
    };

    let small = small_node
        .validate_shacl_delta(
            &AUTH,
            &small_data,
            &small_schema,
            &changes(&small_data, 100),
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    let large_delta = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &changes(&large_data, 100),
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap();
    let large_full = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &changes(&large_data, 100),
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    let large = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &changes(&large_data, 100),
            &ShaclValidationOptions::default(),
        )
        .unwrap();

    assert_eq!(large_delta.results, large_full.results);
    assert_eq!(large_delta.conforms, large_full.conforms);
    assert_eq!(large.results, large_delta.results);
    assert_eq!(large.conforms, large_delta.conforms);
    assert!(large.statistics.estimated_full_work > small.statistics.estimated_full_work);
    assert_eq!(
        large.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );

    let batch = changes(&large_data, 1_000);
    let large_batch_delta = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &batch,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap();
    let large_batch_full = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &batch,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    let large_batch = large_node
        .validate_shacl_delta(
            &AUTH,
            &large_data,
            &large_schema,
            &batch,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(large_batch_delta.results, large_batch_full.results);
    assert_eq!(large_batch_delta.conforms, large_batch_full.conforms);
    assert_eq!(large_batch.results, large_batch_delta.results);
    assert_eq!(large_batch.conforms, large_batch_delta.conforms);
    assert_eq!(
        large_batch.statistics.selected_mode,
        ShaclExecutionMode::ForceFull
    );
}

#[test]
fn duplicate_auto_delta() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:duplicate-data");
    let shapes = GraphId::new("urn:test:duplicate-shapes");
    insert_shape_text(&node, &shapes, CLASS_SHAPES);
    let value = change(
        &data,
        true,
        "urn:test:duplicate-focus",
        "urn:test:class-value",
        "urn:test:duplicate-object",
    );
    node.apply_changes_unchecked(
        &data,
        vec![
            value.clone(),
            change(
                &data,
                true,
                "urn:test:duplicate-object",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "urn:test:RequiredClass",
            ),
        ],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let baseline = node
        .validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let changes = vec![value; 100];
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    assert_eq!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert_eq!(auto.results, baseline.results);
    assert_eq!(auto.conforms, baseline.conforms);
    assert_eq!(auto.results, full.results);
    assert_eq!(auto.conforms, full.conforms);
}

#[test]
fn absent_noop_delta() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:absent-noop-data");
    let shapes = GraphId::new("urn:test:absent-noop-shapes");
    insert_shape_text(&node, &shapes, CLASS_SHAPES);
    node.apply_changes_unchecked(
        &data,
        vec![
            change(
                &data,
                true,
                "urn:test:absent-noop-focus",
                "urn:test:class-value",
                "urn:test:absent-noop-object",
            ),
            change(
                &data,
                true,
                "urn:test:absent-noop-object",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "urn:test:RequiredClass",
            ),
        ],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let baseline = node
        .validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let changes = [
        change(
            &data,
            true,
            "urn:test:absent-noop-focus",
            "urn:test:class-value",
            "urn:test:absent-noop-next",
        ),
        change(
            &data,
            false,
            "urn:test:absent-noop-focus",
            "urn:test:class-value",
            "urn:test:absent-noop-next",
        ),
    ];
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    assert_eq!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert_eq!(auto.statistics.shapes_executed, 0);
    assert_eq!(auto.statistics.constraints_evaluated, 0);
    assert_eq!(auto.statistics.read.index_seeks, 1);
    assert_eq!(auto.results, baseline.results);
    assert_eq!(auto.conforms, baseline.conforms);
    assert_eq!(auto.results, full.results);
    assert_eq!(auto.conforms, full.conforms);
}

#[test]
fn present_noop_delta() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:present-noop-data");
    let shapes = GraphId::new("urn:test:present-noop-shapes");
    insert_shape_text(&node, &shapes, CLASS_SHAPES);
    node.apply_changes_unchecked(
        &data,
        vec![
            change(
                &data,
                true,
                "urn:test:present-noop-focus",
                "urn:test:class-value",
                "urn:test:present-noop-object",
            ),
            change(
                &data,
                true,
                "urn:test:present-noop-object",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "urn:test:RequiredClass",
            ),
        ],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let baseline = node
        .validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let changes = [
        change(
            &data,
            false,
            "urn:test:present-noop-focus",
            "urn:test:class-value",
            "urn:test:present-noop-object",
        ),
        change(
            &data,
            true,
            "urn:test:present-noop-focus",
            "urn:test:class-value",
            "urn:test:present-noop-object",
        ),
    ];
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    let full = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &changes,
            &mode_options(ShaclExecutionMode::ForceFull),
        )
        .unwrap();
    assert_eq!(
        auto.statistics.selected_mode,
        ShaclExecutionMode::ForceDelta
    );
    assert_eq!(auto.statistics.shapes_executed, 0);
    assert_eq!(auto.statistics.constraints_evaluated, 0);
    assert_eq!(auto.statistics.read.index_seeks, 1);
    assert_eq!(auto.results, baseline.results);
    assert_eq!(auto.conforms, baseline.conforms);
    assert_eq!(auto.results, full.results);
    assert_eq!(auto.conforms, full.conforms);
}

#[test]
fn schema_changes_error() {
    let (_directory, node) = node();
    let data = GraphId::new("urn:test:global-data");
    let shapes = GraphId::new("urn:test:global-shapes");
    insert_shape_text(&node, &shapes, GLOBAL_SHAPES);
    node.apply_changes_unchecked(
        &data,
        vec![change(
            &data,
            true,
            "urn:test:global-focus",
            "urn:test:walk",
            "urn:test:global-value",
        )],
    )
    .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let global = [change(
        &data,
        true,
        "urn:test:global-focus",
        "urn:test:walk",
        "urn:test:global-next",
    )];
    let global = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            &global,
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(
        global.statistics.selected_mode,
        ShaclExecutionMode::ForceFull
    );
    assert!(global.statistics.estimated_delta_work > global.statistics.estimated_full_work);
    assert!(global.statistics.estimated_affected_shapes >= 2);

    let imported = GraphId::new("urn:test:incremental-import");
    let root = GraphId::new("urn:test:incremental-import-root");
    insert_shape_text(&node, &imported, SHAPES);
    insert_shape_text(&node, &root, IMPORT_ROOT);
    let imported_schema = node
        .compile_shacl(
            &AUTH,
            &root,
            &ShaclCompileOptions {
                allow_local_imports: true,
                ..ShaclCompileOptions::default()
            },
        )
        .unwrap();
    let root_change = [change(
        &root,
        true,
        "urn:test:root-change",
        "urn:test:value",
        "urn:test:value",
    )];
    let root_error = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &imported_schema,
            &root_change,
            &ShaclValidationOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        root_error,
        CraqleError::Shacl(ShaclError::ShapesGraphMutationUnsupported { .. })
    ));
    let import_change = [change(
        &imported,
        true,
        "urn:test:import-change",
        "urn:test:value",
        "urn:test:value",
    )];
    let import_error = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &imported_schema,
            &import_change,
            &mode_options(ShaclExecutionMode::ForceDelta),
        )
        .unwrap_err();
    assert!(matches!(
        import_error,
        CraqleError::Shacl(ShaclError::DeltaExecutionUnavailable { .. })
    ));
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
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    let delta_options = mode_options(ShaclExecutionMode::ForceDelta);
    node.validate_shacl(&AUTH, &data, &schema, &options)
        .unwrap();

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
            .validate_shacl_delta(&AUTH, &data, &schema, &changes, &delta_options)
            .unwrap_or_else(|error| panic!("incremental step {step} failed: {error}"));
        node.apply_changes_unchecked(&data, changes).unwrap();
        let full = node
            .validate_shacl(&AUTH, &data, &schema, &options)
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
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let error = node
        .validate_shacl(
            &AUTH,
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
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    let options = ShaclValidationOptions::default();
    let delta_options = mode_options(ShaclExecutionMode::ForceDelta);
    assert!(
        node.validate_shacl(&AUTH, &data, &schema, &options)
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
    let auto = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            std::slice::from_ref(&deletion),
            &ShaclValidationOptions::default(),
        )
        .unwrap();
    assert_eq!(auto.statistics.selected_mode, ShaclExecutionMode::ForceFull);
    let incremental = node
        .validate_shacl_delta(
            &AUTH,
            &data,
            &schema,
            std::slice::from_ref(&deletion),
            &delta_options,
        )
        .unwrap();
    node.apply_changes_unchecked(&data, vec![deletion]).unwrap();
    let full = node
        .validate_shacl(&AUTH, &data, &schema, &options)
        .unwrap();
    assert_eq!(incremental.results, full.results);
    assert_eq!(auto.results, incremental.results);
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
    node.bind_shacl(
        &AUTH,
        &ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy: ValidationPolicy::Enforce,
            validation_options: ShaclBindingOptions::default(),
        },
    )
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
    node.apply_changes(&AUTH, &data, vec![transient, removal])
        .unwrap();
    let schema = node
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .unwrap();
    assert!(
        node.validate_shacl(&AUTH, &data, &schema, &ShaclValidationOptions::default(),)
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
    let bound = node.bind_shacl(&AUTH, &binding).unwrap();
    assert_eq!(bound.state, ShaclValidationState::Valid);
    let before = node.graph_snapshot(&data).unwrap();
    let before_index = node.query_index_status_fast().unwrap();
    let rejected = node
        .apply_changes(
            &AUTH,
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

    node.unbind_shacl(&AUTH, &data, &shapes).unwrap();
    let advisory = ShaclBinding {
        policy: ValidationPolicy::Advisory,
        ..binding
    };
    node.bind_shacl(&AUTH, &advisory).unwrap();
    node.apply_changes(
        &AUTH,
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
    let statuses = node.shacl_binding_statuses(&AUTH, &data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert_eq!(statuses[0].report.as_ref().unwrap().results.len(), 1);

    drop(node);
    let reopened = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x72; 32])),
    )
    .unwrap();
    let statuses = reopened.shacl_binding_statuses(&AUTH, &data).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, ShaclValidationState::Invalid);
    assert_eq!(statuses[0].report.as_ref().unwrap().results.len(), 1);
}
