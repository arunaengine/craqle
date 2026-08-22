use craqle::{
    AllowAllAuthorizer, CraqleErrorKind, CraqleNode, CreateCrateRequest, EncodedTerm, GraphId,
    GraphPolicy, MaterializedQuadChange, UpdateOptions,
};
use oxrdf::{Literal, NamedNode, Term, Triple};

fn quoted_term() -> Term {
    Term::from(Triple::new(
        NamedNode::new_unchecked("urn:test:rdf-star:subject"),
        NamedNode::new_unchecked("urn:test:rdf-star:predicate"),
        Literal::new_simple_literal("value"),
    ))
}

#[test]
fn rdf_star_rejection() {
    let quoted = quoted_term();
    let conversion = EncodedTerm::from_term(&quoted).unwrap_err();
    assert_eq!(conversion.kind(), CraqleErrorKind::Unsupported);

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:rdf-star:graph");
    node.create_crate(
        &AllowAllAuthorizer,
        CreateCrateRequest::new(
            graph.clone(),
            "RDF-star rejection",
            "All public term conversions fail explicitly.",
            "2026-08-22",
            None,
            GraphPolicy::default(),
        ),
    )
    .unwrap();
    let before = node.graph_snapshot(&graph).unwrap();

    let encoded_quoted = EncodedTerm(quoted.to_string());
    let error = node
        .apply_changes(
            &AllowAllAuthorizer,
            &graph,
            vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:rdf-star:write",
                )),
                predicate: EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:rdf-star:value",
                )),
                object: encoded_quoted,
            }],
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Unsupported);
    assert_eq!(node.graph_snapshot(&graph).unwrap(), before);

    let error = node
        .add_data_entity_with_triples(
            &AllowAllAuthorizer,
            &graph,
            "./quoted.dat",
            "http://schema.org/MediaObject",
            "Quoted term",
            vec![(NamedNode::new_unchecked("urn:test:rdf-star:value"), quoted)],
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Unsupported);
    assert_eq!(node.graph_snapshot(&graph).unwrap(), before);

    let error = node
        .query(
            &AllowAllAuthorizer,
            "SELECT * WHERE { <<( <urn:s> <urn:p> <urn:o> )>> <urn:q> ?value }",
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Unsupported);

    let error = node
        .apply_sparql_update_with_options(
            &AllowAllAuthorizer,
            "INSERT DATA { GRAPH <urn:test:rdf-star:graph> { <urn:s> <urn:p> <<( <urn:a> <urn:b> <urn:c> )>> } }",
            &UpdateOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Unsupported);
    assert_eq!(node.graph_snapshot(&graph).unwrap(), before);
}
