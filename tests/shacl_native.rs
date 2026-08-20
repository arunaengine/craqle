#![cfg(feature = "shacl-core")]

use craqle::{
    ActorId, CraqleError, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange,
    QueryCancellation, SearchStorage, ShaclCompileOptions, ShaclError, ShaclValidationOptions,
};
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_core::SHACLPath;
use rudof_rdf::rdf_core::term::Object;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};
use shacl::ir::IRSchema;
use shacl::rdf::ShaclParser;
use shacl::types::Severity;
use shacl::validator::ShaclValidationMode;
use shacl::validator::processor::{GraphValidation, ShaclProcessor};
use shacl::validator::store::Graph;

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const RDF_FIRST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
const RDF_REST: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
const RDF_NIL: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
const SH: &str = "http://www.w3.org/ns/shacl#";

fn iri(value: &str) -> String {
    format!("<{value}>")
}

fn sh(local: &str) -> String {
    iri(&format!("{SH}{local}"))
}

fn literal(value: &str) -> String {
    EncodedTerm::from_term(&oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
        value,
    )))
    .0
}

fn integer(value: i64) -> String {
    format!("\"{value}\"^^<http://www.w3.org/2001/XMLSchema#integer>")
}

fn node() -> (tempfile::TempDir, CraqleNode) {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        database.path(),
        CraqleOptions::new()
            .with_actor(ActorId::from_bytes([0x6B; 32]))
            .with_search_storage(SearchStorage::Memory),
    )
    .unwrap();
    (database, node)
}

fn insert(node: &CraqleNode, graph: &GraphId, triples: &[(&str, &str, &str)]) {
    node.apply_changes_unchecked(
        graph,
        triples
            .iter()
            .map(
                |(subject, predicate, object)| MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm((*subject).to_owned()),
                    predicate: EncodedTerm((*predicate).to_owned()),
                    object: EncodedTerm((*object).to_owned()),
                },
            )
            .collect(),
    )
    .unwrap();
}

fn insert_ntriples(node: &CraqleNode, graph: &GraphId, text: &str) {
    let parsed =
        OxigraphInMemory::from_str(text, &RDFFormat::NTriples, None, &ReaderMode::Strict).unwrap();
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

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NormalizedResult {
    focus: String,
    value: Option<String>,
    path: Option<String>,
    source: String,
    component: String,
    severity: String,
    messages: Vec<(Option<String>, String)>,
}

fn rudof_validate(shape_text: &str, data_text: &str) -> shacl::validator::report::ValidationReport {
    let shape_graph =
        OxigraphInMemory::from_str(shape_text, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .unwrap();
    let mut parser = ShaclParser::new(shape_graph);
    let schema: IRSchema = parser.parse().unwrap().try_into().unwrap();
    let data_graph =
        OxigraphInMemory::from_str(data_text, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .unwrap();
    let mut validator: GraphValidation = Graph::from(data_graph).into();
    validator
        .validate(&schema, &ShaclValidationMode::Native)
        .unwrap()
}

fn normalize_native(report: &craqle::ShaclValidationReport) -> Vec<NormalizedResult> {
    report
        .results
        .iter()
        .map(|result| NormalizedResult {
            focus: result.focus_node.0.clone(),
            value: result.value.as_ref().map(|value| value.0.clone()),
            path: result.result_path.clone(),
            source: result.source_shape.0.clone(),
            component: format!("<{}>", result.source_constraint_component),
            severity: result.severity.0.clone(),
            messages: result
                .messages
                .iter()
                .map(|message| (message.language.clone(), message.text.clone()))
                .collect(),
        })
        .collect()
}

fn normalize_rudof(report: &shacl::validator::report::ValidationReport) -> Vec<NormalizedResult> {
    let mut results = report
        .results()
        .iter()
        .map(|result| {
            let mut messages = result
                .message()
                .iter()
                .map(|(language, message)| {
                    (language.as_ref().map(ToString::to_string), message.clone())
                })
                .collect::<Vec<_>>();
            messages.sort();
            NormalizedResult {
                focus: encode_rudof_object(result.focus_node()),
                value: result.value().map(encode_rudof_object),
                path: result.path().map(normalize_path),
                source: encode_rudof_object(result.source().unwrap()),
                component: encode_rudof_object(result.constraint_component()),
                severity: severity_term(result.severity()),
                messages,
            }
        })
        .collect::<Vec<_>>();
    results.sort();
    results
}

fn encode_rudof_object(object: &Object) -> String {
    match object {
        Object::Iri(iri) => format!("<{}>", iri.as_str()),
        Object::BlankNode(label) => format!("_:{label}"),
        Object::Literal(literal) => {
            let value = if let Some(language) = literal.lang() {
                oxrdf::Literal::new_language_tagged_literal(
                    literal.lexical_form(),
                    language.to_string(),
                )
                .unwrap()
            } else {
                let datatype = literal.datatype();
                let datatype = datatype.get_iri().unwrap();
                oxrdf::Literal::new_typed_literal(
                    literal.lexical_form(),
                    oxrdf::NamedNode::new_unchecked(datatype.as_str()),
                )
            };
            EncodedTerm::from_term(&oxrdf::Term::Literal(value)).0
        }
        Object::Triple { .. } => panic!("RDF-star is outside CraqleFastV1"),
    }
}

fn normalize_path(path: &SHACLPath) -> String {
    match path {
        SHACLPath::Predicate { pred } => format!("<{}>", pred.as_str()),
        SHACLPath::Alternative { paths } => format!(
            "({})",
            paths
                .iter()
                .map(normalize_path)
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        SHACLPath::Sequence { paths } => format!(
            "({})",
            paths
                .iter()
                .map(normalize_path)
                .collect::<Vec<_>>()
                .join(" / ")
        ),
        SHACLPath::Inverse { path } => format!("^{}", normalize_path(path)),
        SHACLPath::ZeroOrMore { path } => format!("{}*", normalize_path(path)),
        SHACLPath::OneOrMore { path } => format!("{}+", normalize_path(path)),
        SHACLPath::ZeroOrOne { path } => format!("{}?", normalize_path(path)),
    }
}

fn severity_term(severity: &Severity) -> String {
    let local = match severity {
        Severity::Trace => "Trace",
        Severity::Debug => "Debug",
        Severity::Info => "Info",
        Severity::Warning => "Warning",
        Severity::Violation => "Violation",
        Severity::Generic(iri) => return format!("<{}>", iri.as_str()),
    };
    format!("<{SH}{local}>")
}

fn assert_native_matches_rudof(shape_text: &str, data_text: &str) {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:matrix:shapes");
    let data = GraphId::new("urn:test:shacl:native:matrix:data");
    insert_ntriples(&node, &shapes, shape_text);
    insert_ntriples(&node, &data, data_text);
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let native = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let rudof = rudof_validate(shape_text, data_text);
    assert_eq!(native.conforms, rudof.conforms());
    let mut native = normalize_native(&native);
    let mut rudof = normalize_rudof(&rudof);
    for result in &mut native {
        result.messages.clear();
    }
    for result in &mut rudof {
        result.messages.clear();
    }
    assert_eq!(native, rudof);
}

#[test]
fn native_cardinality_and_has_value_use_bounded_direct_reads() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:cardinality:shapes");
    let data = GraphId::new("urn:test:shacl:native:cardinality:data");
    let focus = iri("urn:test:focus");
    let predicate = iri("urn:test:value");
    let node_shape = sh("NodeShape");
    let target_node = sh("targetNode");
    let property = sh("property");
    let path = sh("path");
    let min_count = sh("minCount");
    let max_count = sh("maxCount");
    let has_value = sh("hasValue");
    insert(
        &node,
        &shapes,
        &[
            ("_:node", RDF_TYPE, &node_shape),
            ("_:node", &target_node, &focus),
            ("_:node", &property, "_:property"),
            ("_:property", &path, &predicate),
            ("_:property", &min_count, &integer(2)),
            ("_:property", &max_count, &integer(2)),
            ("_:property", &has_value, &literal("required")),
        ],
    );
    insert(
        &node,
        &data,
        &[
            (&focus, &predicate, &literal("one")),
            (&focus, &predicate, &literal("two")),
            (&focus, &predicate, &literal("three")),
        ],
    );

    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let report = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    assert!(!report.conforms);
    assert_eq!(report.results.len(), 2);
    assert!(report.results.iter().any(|result| {
        result
            .source_constraint_component
            .ends_with("MaxCountConstraintComponent")
    }));
    assert!(report.results.iter().any(|result| {
        result
            .source_constraint_component
            .ends_with("HasValueConstraintComponent")
    }));
    assert!(report.statistics.path_candidate_quads <= 5);
    assert_eq!(report.statistics.full_graph_fallbacks, 0);

    assert!(
        !node
            .conforms_shacl(&data, &schema, &ShaclValidationOptions::default())
            .unwrap()
    );
}

#[test]
fn native_value_constraints_emit_complete_deterministic_results() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:values:shapes");
    let data = GraphId::new("urn:test:shacl:native:values:data");
    let focus = iri("urn:test:value-focus");
    let predicate = iri("urn:test:numeric");
    let node_shape = sh("NodeShape");
    let target_node = sh("targetNode");
    let property = sh("property");
    let path = sh("path");
    let datatype = sh("datatype");
    let min_inclusive = sh("minInclusive");
    let max_exclusive = sh("maxExclusive");
    insert(
        &node,
        &shapes,
        &[
            ("_:node", RDF_TYPE, &node_shape),
            ("_:node", &target_node, &focus),
            ("_:node", &property, "_:property"),
            ("_:property", &path, &predicate),
            (
                "_:property",
                &datatype,
                "<http://www.w3.org/2001/XMLSchema#integer>",
            ),
            ("_:property", &min_inclusive, &integer(10)),
            ("_:property", &max_exclusive, &integer(20)),
        ],
    );
    insert(
        &node,
        &data,
        &[
            (&focus, &predicate, &integer(9)),
            (&focus, &predicate, &integer(20)),
            (&focus, &predicate, &literal("not-an-integer")),
        ],
    );

    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let first = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let second = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    assert_eq!(first.results, second.results);
    assert_eq!(first.results.len(), 5);
    assert!(second.statistics.shape_compile_cache_hit);
    assert!(first.results.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn native_inverse_sequence_and_repeating_paths_enforce_budgets() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:paths:shapes");
    let data = GraphId::new("urn:test:shacl:native:paths:data");
    let focus = iri("urn:test:path-focus");
    let p1 = iri("urn:test:p1");
    let p2 = iri("urn:test:p2");
    let node_shape = sh("NodeShape");
    let target_node = sh("targetNode");
    let property = sh("property");
    let path = sh("path");
    let one_or_more = sh("oneOrMorePath");
    let min_count = sh("minCount");
    insert(
        &node,
        &shapes,
        &[
            ("_:node", RDF_TYPE, &node_shape),
            ("_:node", &target_node, &focus),
            ("_:node", &property, "_:property"),
            ("_:property", &path, "_:repeating"),
            ("_:repeating", &one_or_more, "_:sequence"),
            ("_:sequence", RDF_FIRST, &p1),
            ("_:sequence", RDF_REST, "_:tail"),
            ("_:tail", RDF_FIRST, &p2),
            ("_:tail", RDF_REST, RDF_NIL),
            ("_:property", &min_count, &integer(1)),
            ("_:property", &sh("in"), "_:allowed"),
            ("_:allowed", RDF_FIRST, &iri("urn:test:b")),
            ("_:allowed", RDF_REST, RDF_NIL),
        ],
    );
    insert(
        &node,
        &data,
        &[
            (&focus, &p1, &iri("urn:test:a")),
            (&iri("urn:test:a"), &p2, &iri("urn:test:b")),
            (&iri("urn:test:b"), &p1, &iri("urn:test:c")),
            (&iri("urn:test:c"), &p2, &focus),
        ],
    );

    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    assert!(
        node.conforms_shacl(&data, &schema, &ShaclValidationOptions::default())
            .unwrap()
    );

    let error = node
        .validate_shacl(
            &data,
            &schema,
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
        .validate_shacl(
            &data,
            &schema,
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

#[test]
fn native_validation_cancellation_and_result_limits_are_errors() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:limits:shapes");
    let data = GraphId::new("urn:test:shacl:native:limits:data");
    let focus = iri("urn:test:limit-focus");
    let predicate = iri("urn:test:required");
    let node_shape = sh("NodeShape");
    let target_node = sh("targetNode");
    let property = sh("property");
    let path = sh("path");
    let min_count = sh("minCount");
    insert(
        &node,
        &shapes,
        &[
            ("_:node", RDF_TYPE, &node_shape),
            ("_:node", &target_node, &focus),
            ("_:node", &property, "_:first"),
            ("_:node", &property, "_:second"),
            ("_:first", &path, &predicate),
            ("_:first", &min_count, &integer(1)),
            ("_:second", &path, &iri("urn:test:also-required")),
            ("_:second", &min_count, &integer(1)),
        ],
    );
    insert(
        &node,
        &data,
        &[(&focus, &iri("urn:test:present"), &literal("value"))],
    );
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();

    let error = node
        .validate_shacl(
            &data,
            &schema,
            &ShaclValidationOptions {
                max_results: 1,
                ..ShaclValidationOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::ResultLimitExceeded { limit: 1 })
    ));

    let cancellation = QueryCancellation::new();
    cancellation.cancel();
    let error = node
        .validate_shacl(
            &data,
            &schema,
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
fn native_targets_and_closed_shapes_report_only_the_violating_terms() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:targets:shapes");
    let data = GraphId::new("urn:test:shacl:native:targets:data");
    let class = iri("urn:test:TargetClass");
    let class_focus = iri("urn:test:class-focus");
    let subject_focus = iri("urn:test:subject-focus");
    let object_focus = iri("urn:test:object-focus");
    let target_predicate = iri("urn:test:target-predicate");
    let required = iri("urn:test:required-property");
    let node_shape = sh("NodeShape");
    let target_class = sh("targetClass");
    let target_subjects = sh("targetSubjectsOf");
    let target_objects = sh("targetObjectsOf");
    let property = sh("property");
    let path = sh("path");
    let min_count = sh("minCount");
    for (shape, target, target_value) in [
        ("_:classShape", target_class.as_str(), class.as_str()),
        (
            "_:subjectShape",
            target_subjects.as_str(),
            target_predicate.as_str(),
        ),
        (
            "_:objectShape",
            target_objects.as_str(),
            target_predicate.as_str(),
        ),
    ] {
        insert(
            &node,
            &shapes,
            &[
                (shape, RDF_TYPE, &node_shape),
                (shape, target, target_value),
                (shape, &property, &format!("{shape}Property")),
                (&format!("{shape}Property"), &path, &required),
                (&format!("{shape}Property"), &min_count, &integer(1)),
            ],
        );
    }
    insert(
        &node,
        &data,
        &[
            (&class_focus, RDF_TYPE, &class),
            (&subject_focus, &target_predicate, &object_focus),
        ],
    );

    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let report = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    let focus_nodes = report
        .results
        .iter()
        .map(|result| result.focus_node.0.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        focus_nodes,
        std::collections::BTreeSet::from([
            class_focus.as_str(),
            subject_focus.as_str(),
            object_focus.as_str(),
        ])
    );

    let closed_shapes = GraphId::new("urn:test:shacl:native:closed:shapes");
    let closed_data = GraphId::new("urn:test:shacl:native:closed:data");
    let focus = iri("urn:test:closed-focus");
    let allowed = iri("urn:test:allowed");
    let rejected = iri("urn:test:rejected");
    let target_node = sh("targetNode");
    let closed = sh("closed");
    let ignored = sh("ignoredProperties");
    insert(
        &node,
        &closed_shapes,
        &[
            ("_:shape", RDF_TYPE, &node_shape),
            ("_:shape", &target_node, &focus),
            (
                "_:shape",
                &closed,
                "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            ),
            ("_:shape", &ignored, "_:ignored"),
            ("_:ignored", RDF_FIRST, RDF_TYPE),
            ("_:ignored", RDF_REST, RDF_NIL),
            ("_:shape", &property, "_:allowedProperty"),
            ("_:allowedProperty", &path, &allowed),
        ],
    );
    insert(
        &node,
        &closed_data,
        &[
            (&focus, RDF_TYPE, &class),
            (&focus, &allowed, &literal("kept")),
            (&focus, &rejected, &literal("reported")),
        ],
    );
    let schema = node
        .compile_shacl(&closed_shapes, &ShaclCompileOptions::default())
        .unwrap();
    let report = node
        .validate_shacl(&closed_data, &schema, &ShaclValidationOptions::default())
        .unwrap();
    assert_eq!(report.results.len(), 1);
    assert_eq!(
        report.results[0].result_path.as_deref(),
        Some(rejected.as_str())
    );
    assert_eq!(
        report.results[0].value.as_ref().unwrap().0,
        literal("reported")
    );
}

#[test]
fn native_report_matches_rudof_for_supported_named_shape_fixture() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:differential:shapes");
    let data = GraphId::new("urn:test:shacl:native:differential:data");
    let node_shape = iri("urn:test:differential-node-shape");
    let property_shape = iri("urn:test:differential-property-shape");
    let focus = iri("urn:test:differential-focus");
    let predicate = iri("urn:test:differential-value");
    let node_shape_type = sh("NodeShape");
    let property_shape_type = sh("PropertyShape");
    let target_node = sh("targetNode");
    let property = sh("property");
    let path = sh("path");
    let min_count = sh("minCount");
    let severity = sh("severity");
    let warning = sh("Warning");
    let message = sh("message");
    insert(
        &node,
        &shapes,
        &[
            (&node_shape, RDF_TYPE, &node_shape_type),
            (&node_shape, &target_node, &focus),
            (&node_shape, &property, &property_shape),
            (&property_shape, RDF_TYPE, &property_shape_type),
            (&property_shape, &path, &predicate),
            (&property_shape, &min_count, &integer(2)),
            (&property_shape, &severity, &warning),
            (&property_shape, &message, &literal("at least two values")),
        ],
    );
    insert(&node, &data, &[(&focus, &predicate, &literal("only one"))]);
    let schema = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap();
    let native = node
        .validate_shacl(&data, &schema, &ShaclValidationOptions::default())
        .unwrap();

    let shape_text = format!(
        "{node_shape} {RDF_TYPE} {node_shape_type} .\n\
         {node_shape} {target_node} {focus} .\n\
         {node_shape} {property} {property_shape} .\n\
         {property_shape} {RDF_TYPE} {property_shape_type} .\n\
         {property_shape} {path} {predicate} .\n\
         {property_shape} {min_count} {} .\n\
         {property_shape} {severity} {warning} .\n\
         {property_shape} {message} {} .\n",
        integer(2),
        literal("at least two values")
    );
    let data_text = format!("{focus} {predicate} {} .\n", literal("only one"));
    let rudof = rudof_validate(&shape_text, &data_text);

    assert_eq!(native.conforms, rudof.conforms());
    assert_eq!(normalize_native(&native), normalize_rudof(&rudof));
}

#[test]
fn native_string_language_class_and_in_constraints_match_rudof() {
    let root = iri("urn:test:matrix:root");
    let text_shape = iri("urn:test:matrix:text-shape");
    let language_shape = iri("urn:test:matrix:language-shape");
    let class_shape = iri("urn:test:matrix:class-shape");
    let in_shape = iri("urn:test:matrix:in-shape");
    let focus = iri("urn:test:matrix:focus");
    let text = iri("urn:test:matrix:text");
    let language = iri("urn:test:matrix:language");
    let reference = iri("urn:test:matrix:reference");
    let choice = iri("urn:test:matrix:choice");
    let class = iri("urn:test:matrix:Class");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} {text_shape} .\n\
         {root} {} {language_shape} .\n\
         {root} {} {class_shape} .\n\
         {root} {} {in_shape} .\n\
         {text_shape} {RDF_TYPE} {} .\n\
         {text_shape} {} {text} .\n\
         {text_shape} {} {} .\n\
         {text_shape} {} {} .\n\
         {text_shape} {} {} .\n\
         {text_shape} {} {} .\n\
         {text_shape} {} {} .\n\
         {language_shape} {RDF_TYPE} {} .\n\
         {language_shape} {} {language} .\n\
         {language_shape} {} {} .\n\
         {language_shape} {} _:languages .\n\
         _:languages {RDF_FIRST} {} .\n\
         _:languages {RDF_REST} {RDF_NIL} .\n\
         {class_shape} {RDF_TYPE} {} .\n\
         {class_shape} {} {reference} .\n\
         {class_shape} {} {class} .\n\
         {in_shape} {RDF_TYPE} {} .\n\
         {in_shape} {} {choice} .\n\
         {in_shape} {} _:choices .\n\
         _:choices {RDF_FIRST} {} .\n\
         _:choices {RDF_REST} _:choiceTail .\n\
         _:choiceTail {RDF_FIRST} {} .\n\
         _:choiceTail {RDF_REST} {RDF_NIL} .\n\
         {text_shape} {} {} .\n\
         {language_shape} {} {} .\n\
         {class_shape} {} {} .\n\
         {in_shape} {} {} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("nodeKind"),
        sh("Literal"),
        sh("minLength"),
        integer(4),
        sh("maxLength"),
        integer(6),
        sh("pattern"),
        literal("^A"),
        sh("flags"),
        literal("i"),
        sh("PropertyShape"),
        sh("path"),
        sh("uniqueLang"),
        "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
        sh("languageIn"),
        literal("en"),
        sh("PropertyShape"),
        sh("path"),
        sh("class"),
        sh("PropertyShape"),
        sh("path"),
        sh("in"),
        literal("one"),
        literal("two"),
        sh("message"),
        literal("text constraint"),
        sh("message"),
        literal("language constraint"),
        sh("message"),
        literal("class constraint"),
        sh("message"),
        literal("in constraint"),
    );
    let data_text = format!(
        "{focus} {text} {} .\n\
         {focus} {language} \"first\"@en .\n\
         {focus} {language} \"second\"@EN .\n\
         {focus} {reference} {} .\n\
         {focus} {choice} {} .\n",
        literal("xy"),
        iri("urn:test:matrix:untyped"),
        literal("three"),
    );
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_numeric_boundaries_match_rudof() {
    let root = iri("urn:test:numeric:root");
    let property = iri("urn:test:numeric:property-shape");
    let focus = iri("urn:test:numeric:focus");
    let predicate = iri("urn:test:numeric:value");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} {property} .\n\
         {property} {RDF_TYPE} {} .\n\
         {property} {} {predicate} .\n\
         {property} {} {} .\n\
         {property} {} {} .\n\
         {property} {} {} .\n\
         {property} {} {} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("datatype"),
        iri("http://www.w3.org/2001/XMLSchema#integer"),
        sh("minInclusive"),
        integer(10),
        sh("maxExclusive"),
        integer(20),
        sh("message"),
        literal("numeric constraint"),
    );
    let data_text = format!(
        "{focus} {predicate} {} .\n\
         {focus} {predicate} {} .\n\
         {focus} {predicate} {} .\n",
        integer(9),
        integer(20),
        literal("not-an-integer"),
    );
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_property_pair_constraints_match_rudof() {
    let root = iri("urn:test:pairs:root");
    let set_shape = iri("urn:test:pairs:set-shape");
    let less_shape = iri("urn:test:pairs:less-shape");
    let less_equal_shape = iri("urn:test:pairs:less-equal-shape");
    let focus = iri("urn:test:pairs:focus");
    let set = iri("urn:test:pairs:set");
    let equals = iri("urn:test:pairs:equals");
    let disjoint = iri("urn:test:pairs:disjoint");
    let low = iri("urn:test:pairs:low");
    let high = iri("urn:test:pairs:high");
    let low_equal = iri("urn:test:pairs:low-equal");
    let high_equal = iri("urn:test:pairs:high-equal");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} {set_shape} .\n\
         {root} {} {less_shape} .\n\
         {root} {} {less_equal_shape} .\n\
         {set_shape} {RDF_TYPE} {} .\n\
         {set_shape} {} {set} .\n\
         {set_shape} {} {equals} .\n\
         {set_shape} {} {disjoint} .\n\
         {less_shape} {RDF_TYPE} {} .\n\
         {less_shape} {} {low} .\n\
         {less_shape} {} {high} .\n\
         {less_equal_shape} {RDF_TYPE} {} .\n\
         {less_equal_shape} {} {low_equal} .\n\
         {less_equal_shape} {} {high_equal} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("equals"),
        sh("disjoint"),
        sh("PropertyShape"),
        sh("path"),
        sh("lessThan"),
        sh("PropertyShape"),
        sh("path"),
        sh("lessThanOrEquals"),
    );
    let data_text = format!(
        "{focus} {set} {} .\n\
         {focus} {set} {} .\n\
         {focus} {equals} {} .\n\
         {focus} {equals} {} .\n\
         {focus} {disjoint} {} .\n\
         {focus} {low} {} .\n\
         {focus} {high} {} .\n\
         {focus} {low_equal} {} .\n\
         {focus} {high_equal} {} .\n",
        integer(1),
        integer(2),
        integer(2),
        integer(3),
        integer(2),
        integer(5),
        integer(3),
        integer(3),
        integer(3),
    );
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_logical_and_qualified_constraints_match_rudof() {
    let root = iri("urn:test:logical:root");
    let property = iri("urn:test:logical:property");
    let class_a_shape = iri("urn:test:logical:class-a-shape");
    let class_b_shape = iri("urn:test:logical:class-b-shape");
    let focus = iri("urn:test:logical:focus");
    let value = iri("urn:test:logical:value");
    let predicate = iri("urn:test:logical:predicate");
    let class_a = iri("urn:test:logical:ClassA");
    let class_b = iri("urn:test:logical:ClassB");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} {property} .\n\
         {property} {RDF_TYPE} {} .\n\
         {property} {} {predicate} .\n\
         {property} {} _:andList .\n\
         {property} {} _:orList .\n\
         {property} {} {class_a_shape} .\n\
         {property} {} _:xoneList .\n\
         {property} {} {class_a_shape} .\n\
         _:andList {RDF_FIRST} {class_a_shape} .\n\
         _:andList {RDF_REST} _:andTail .\n\
         _:andTail {RDF_FIRST} {class_b_shape} .\n\
         _:andTail {RDF_REST} {RDF_NIL} .\n\
         _:orList {RDF_FIRST} {class_a_shape} .\n\
         _:orList {RDF_REST} _:orTail .\n\
         _:orTail {RDF_FIRST} {class_b_shape} .\n\
         _:orTail {RDF_REST} {RDF_NIL} .\n\
         _:xoneList {RDF_FIRST} {class_a_shape} .\n\
         _:xoneList {RDF_REST} _:xoneTail .\n\
         _:xoneTail {RDF_FIRST} {class_b_shape} .\n\
         _:xoneTail {RDF_REST} {RDF_NIL} .\n\
         {class_a_shape} {RDF_TYPE} {} .\n\
         {class_a_shape} {} {class_a} .\n\
         {class_b_shape} {RDF_TYPE} {} .\n\
         {class_b_shape} {} {class_b} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("and"),
        sh("or"),
        sh("not"),
        sh("xone"),
        sh("node"),
        sh("NodeShape"),
        sh("class"),
        sh("NodeShape"),
        sh("class"),
    );
    let data_text = format!(
        "{focus} {predicate} {value} .\n\
         {value} {RDF_TYPE} {class_a} .\n"
    );
    assert_native_matches_rudof(&shape_text, &data_text);

    let qualified_root = iri("urn:test:qualified:root");
    let qualified_property = iri("urn:test:qualified:property");
    let qualified_focus = iri("urn:test:qualified:focus");
    let qualified_predicate = iri("urn:test:qualified:value");
    let qualified_shape_text = format!(
        "{qualified_root} {RDF_TYPE} {} .\n\
         {qualified_root} {} {qualified_focus} .\n\
         {qualified_root} {} {qualified_property} .\n\
         {qualified_property} {RDF_TYPE} {} .\n\
         {qualified_property} {} {qualified_predicate} .\n\
         {qualified_property} {} {class_a_shape} .\n\
         {qualified_property} {} {} .\n\
         {qualified_property} {} {} .\n\
         {class_a_shape} {RDF_TYPE} {} .\n\
         {class_a_shape} {} {class_a} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("qualifiedValueShape"),
        sh("qualifiedMinCount"),
        integer(2),
        sh("qualifiedMaxCount"),
        integer(2),
        sh("NodeShape"),
        sh("class"),
    );
    let other_value = iri("urn:test:qualified:other-value");
    let qualified_data_text = format!(
        "{qualified_focus} {qualified_predicate} {value} .\n\
         {qualified_focus} {qualified_predicate} {other_value} .\n\
         {value} {RDF_TYPE} {class_a} .\n\
         {other_value} {RDF_TYPE} {class_b} .\n"
    );
    assert_native_matches_rudof(&qualified_shape_text, &qualified_data_text);
}

#[test]
fn native_all_core_path_forms_match_rudof() {
    let root = iri("urn:test:paths:root");
    let focus = iri("urn:test:paths:focus");
    let inverse_shape = iri("urn:test:paths:inverse-shape");
    let sequence_shape = iri("urn:test:paths:sequence-shape");
    let alternative_shape = iri("urn:test:paths:alternative-shape");
    let zero_one_shape = iri("urn:test:paths:zero-one-shape");
    let zero_more_shape = iri("urn:test:paths:zero-more-shape");
    let one_more_shape = iri("urn:test:paths:one-more-shape");
    let p1 = iri("urn:test:paths:p1");
    let p2 = iri("urn:test:paths:p2");
    let p3 = iri("urn:test:paths:p3");
    let p4 = iri("urn:test:paths:p4");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} {inverse_shape} .\n\
         {root} {} {sequence_shape} .\n\
         {root} {} {alternative_shape} .\n\
         {root} {} {zero_one_shape} .\n\
         {root} {} {zero_more_shape} .\n\
         {root} {} {one_more_shape} .\n\
         {inverse_shape} {RDF_TYPE} {} .\n\
         {inverse_shape} {} _:inversePath .\n\
         _:inversePath {} {p1} .\n\
         {inverse_shape} {} {} .\n\
         {sequence_shape} {RDF_TYPE} {} .\n\
         {sequence_shape} {} _:sequence .\n\
         _:sequence {RDF_FIRST} {p1} .\n\
         _:sequence {RDF_REST} _:sequenceTail .\n\
         _:sequenceTail {RDF_FIRST} {p2} .\n\
         _:sequenceTail {RDF_REST} {RDF_NIL} .\n\
         {sequence_shape} {} {} .\n\
         {alternative_shape} {RDF_TYPE} {} .\n\
         {alternative_shape} {} _:alternativePath .\n\
         _:alternativePath {} _:alternatives .\n\
         _:alternatives {RDF_FIRST} {p3} .\n\
         _:alternatives {RDF_REST} _:alternativeTail .\n\
         _:alternativeTail {RDF_FIRST} {p4} .\n\
         _:alternativeTail {RDF_REST} {RDF_NIL} .\n\
         {alternative_shape} {} {} .\n\
         {zero_one_shape} {RDF_TYPE} {} .\n\
         {zero_one_shape} {} _:zeroOnePath .\n\
         _:zeroOnePath {} {p4} .\n\
         {zero_one_shape} {} {} .\n\
         {zero_more_shape} {RDF_TYPE} {} .\n\
         {zero_more_shape} {} _:zeroMorePath .\n\
         _:zeroMorePath {} {p3} .\n\
         {zero_more_shape} {} {} .\n\
         {one_more_shape} {RDF_TYPE} {} .\n\
         {one_more_shape} {} _:oneMorePath .\n\
         _:oneMorePath {} {p3} .\n\
         {one_more_shape} {} {} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("inversePath"),
        sh("minCount"),
        integer(2),
        sh("PropertyShape"),
        sh("path"),
        sh("minCount"),
        integer(2),
        sh("PropertyShape"),
        sh("path"),
        sh("alternativePath"),
        sh("minCount"),
        integer(2),
        sh("PropertyShape"),
        sh("path"),
        sh("zeroOrOnePath"),
        sh("minCount"),
        integer(2),
        sh("PropertyShape"),
        sh("path"),
        sh("zeroOrMorePath"),
        sh("maxCount"),
        integer(1),
        sh("PropertyShape"),
        sh("path"),
        sh("oneOrMorePath"),
        sh("minCount"),
        integer(2),
    );
    let source = iri("urn:test:paths:source");
    let middle = iri("urn:test:paths:middle");
    let end = iri("urn:test:paths:end");
    let alternative = iri("urn:test:paths:alternative");
    let data_text = format!(
        "{source} {p1} {focus} .\n\
         {focus} {p1} {middle} .\n\
         {middle} {p2} {end} .\n\
         {focus} {p3} {alternative} .\n"
    );
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_all_core_target_forms_match_rudof() {
    let required = iri("urn:test:targets:required");
    let class = iri("urn:test:targets:Class");
    let implicit_class = iri("urn:test:targets:ImplicitClass");
    let relation = iri("urn:test:targets:relation");
    let node_focus = iri("urn:test:targets:node-focus");
    let class_focus = iri("urn:test:targets:class-focus");
    let subject_focus = iri("urn:test:targets:subject-focus");
    let object_focus = iri("urn:test:targets:object-focus");
    let implicit_focus = iri("urn:test:targets:implicit-focus");
    let mut shape_text = String::new();
    for (index, target, target_value) in [
        (0, sh("targetNode"), node_focus.clone()),
        (1, sh("targetClass"), class.clone()),
        (2, sh("targetSubjectsOf"), relation.clone()),
        (3, sh("targetObjectsOf"), relation.clone()),
    ] {
        let shape = iri(&format!("urn:test:targets:shape-{index}"));
        let property = iri(&format!("urn:test:targets:property-{index}"));
        shape_text.push_str(&format!(
            "{shape} {RDF_TYPE} {} .\n\
             {shape} {target} {target_value} .\n\
             {shape} {} {property} .\n\
             {property} {RDF_TYPE} {} .\n\
             {property} {} {required} .\n\
             {property} {} {} .\n",
            sh("NodeShape"),
            sh("property"),
            sh("PropertyShape"),
            sh("path"),
            sh("minCount"),
            integer(1),
        ));
    }
    let implicit_property = iri("urn:test:targets:implicit-property");
    shape_text.push_str(&format!(
        "{implicit_class} {RDF_TYPE} {} .\n\
         {implicit_class} {RDF_TYPE} <http://www.w3.org/2000/01/rdf-schema#Class> .\n\
         {implicit_class} {} {implicit_property} .\n\
         {implicit_property} {RDF_TYPE} {} .\n\
         {implicit_property} {} {required} .\n\
         {implicit_property} {} {} .\n",
        sh("NodeShape"),
        sh("property"),
        sh("PropertyShape"),
        sh("path"),
        sh("minCount"),
        integer(1),
    ));
    let data_text = format!(
        "{class_focus} {RDF_TYPE} {class} .\n\
         {subject_focus} {relation} {object_focus} .\n\
         {implicit_focus} {RDF_TYPE} {implicit_class} .\n\
         {node_focus} <urn:test:targets:present> {} .\n",
        literal("value")
    );
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_deactivated_shapes_are_skipped() {
    let root = iri("urn:test:deactivated:shape");
    let focus = iri("urn:test:deactivated:focus");
    let shape_text = format!(
        "{root} {RDF_TYPE} {} .\n\
         {root} {} {focus} .\n\
         {root} {} \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n\
         {root} {} {} .\n",
        sh("NodeShape"),
        sh("targetNode"),
        sh("deactivated"),
        sh("class"),
        iri("urn:test:deactivated:MissingClass"),
    );
    let data_text = format!("{focus} <urn:test:present> {} .\n", literal("value"));
    assert_native_matches_rudof(&shape_text, &data_text);
}

#[test]
fn native_logical_cycles_are_rejected_during_compilation() {
    let (_database, node) = node();
    let shapes = GraphId::new("urn:test:shacl:native:cycle:shapes");
    let shape = iri("urn:test:cycle:shape");
    let focus = iri("urn:test:cycle:focus");
    insert(
        &node,
        &shapes,
        &[
            (&shape, RDF_TYPE, &sh("NodeShape")),
            (&shape, &sh("targetNode"), &focus),
            (&shape, &sh("not"), &shape),
        ],
    );
    let error = node
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::IllFormedShapes { .. })
    ));
}
