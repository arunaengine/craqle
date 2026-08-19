#![cfg(feature = "shacl-core")]

use oxrdf::{Literal, NamedNode, Term, Triple};
use rocraters::ro_crate::context::RoCrateContext;
use rocraters::ro_crate::rdf::{RdfError, RdfGraph, ResolvedContext, rdf_graph_to_rocrate};
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};
use shacl::ir::IRSchema;
use shacl::rdf::ShaclParser;
use shacl::validator::ShaclValidationMode;
use shacl::validator::processor::{GraphValidation, ShaclProcessor};
use shacl::validator::store::Graph;

#[test]
fn rocrate_rdf_star_term_is_rejected() {
    let quoted = Triple::new(
        NamedNode::new_unchecked("urn:craqle:test:quoted-subject"),
        NamedNode::new_unchecked("urn:craqle:test:quoted-predicate"),
        Literal::new_simple_literal("quoted-value"),
    );
    let mut graph = RdfGraph::new(ResolvedContext::new(RoCrateContext::ReferenceContext(
        "https://w3id.org/ro/crate/1.3/context".to_string(),
    )));
    graph.insert(Triple::new(
        NamedNode::new_unchecked("urn:craqle:test:subject"),
        NamedNode::new_unchecked("urn:craqle:test:predicate"),
        Term::from(quoted),
    ));

    assert!(matches!(
        rdf_graph_to_rocrate(graph),
        Err(RdfError::UnsupportedRdfStarTerm)
    ));
}

#[test]
fn rudof_native_validation_runs_without_sparql() {
    let shapes = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:craqle:test:shape> a sh:NodeShape ;
            sh:targetNode <urn:craqle:test:focus> ;
            sh:property [
                sh:path <urn:craqle:test:required> ;
                sh:minCount 1
            ] .
    "#;
    let data = "<urn:craqle:test:focus> <urn:craqle:test:present> \"value\" .";

    let shapes_graph =
        OxigraphInMemory::from_str(shapes, &RDFFormat::Turtle, None, &ReaderMode::Strict)
            .expect("parse fixed test shapes");
    let mut parser = ShaclParser::new(shapes_graph);
    let schema: IRSchema = parser
        .parse()
        .expect("parse fixed test shapes through Rudof")
        .try_into()
        .expect("compile fixed test shapes into Rudof IR");
    let data_graph =
        OxigraphInMemory::from_str(data, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .expect("parse fixed test data");
    let mut validator: GraphValidation = Graph::from(data_graph).into();
    let report = validator
        .validate(&schema, &ShaclValidationMode::Native)
        .expect("run Rudof Native validation without the sparql feature");

    assert!(!report.conforms());
    assert_eq!(report.results().len(), 1);
}
