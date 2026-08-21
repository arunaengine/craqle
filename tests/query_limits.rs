mod support;

use std::time::Duration;

use crate::support::TestWriteExt as _;
use craqle::{
    AllowAllAuthorizer, CraqleErrorKind, CraqleNode, EncodedTerm, GraphId, MaterializedQuadChange,
    QueryExecutionOptions, QueryFastPathMode, QueryLimits, QueryResults, UpdateLimits,
    UpdateOptions,
};

fn insert(graph: &GraphId, subject: &str, predicate: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm(format!("<{subject}>")),
        predicate: EncodedTerm(format!("<{predicate}>")),
        object: EncodedTerm(format!("<{object}>")),
    }
}

fn fixture() -> (tempfile::TempDir, CraqleNode, GraphId) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:query-limits");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:s:1", "urn:p", "urn:o:1"),
            insert(&graph, "urn:s:2", "urn:p", "urn:o:2"),
            insert(&graph, "urn:s:1", "urn:q", "urn:x:1"),
            insert(&graph, "urn:s:2", "urn:q", "urn:x:2"),
            insert(&graph, "urn:a", "urn:next", "urn:b"),
            insert(&graph, "urn:b", "urn:next", "urn:c"),
            insert(&graph, "urn:c", "urn:next", "urn:a"),
        ],
    )
    .unwrap();
    (directory, node, graph)
}

fn expect_query_limit(
    node: &CraqleNode,
    graph: &GraphId,
    query: &str,
    configure: impl FnOnce(&mut QueryExecutionOptions),
) {
    let prepared = node.prepare_query(query).unwrap();
    let mut options = QueryExecutionOptions::default();
    configure(&mut options);
    let error = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(graph),
            &prepared,
            &options,
        )
        .unwrap_err();
    assert_eq!(
        error.kind(),
        CraqleErrorKind::QueryLimit,
        "{query}: {error:?}"
    );
}

#[test]
fn query_limits() {
    let (_directory, node, graph) = fixture();
    let production = QueryLimits::production();
    assert_eq!(production, QueryLimits::default());
    assert!(production.deadline.is_some());

    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s ?o WHERE { ?s <urn:p> ?o }",
        |options| options.limits.max_result_rows = 1,
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s ?o WHERE { ?s <urn:p> ?o }",
        |options| {
            options.fast_paths = QueryFastPathMode::Disabled;
            options.limits.max_result_cells = 1;
        },
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s ?o WHERE { ?s <urn:p> ?o }",
        |options| options.limits.max_result_bytes = 1,
    );
    expect_query_limit(&node, &graph, "ASK { ?s <urn:p> ?o }", |options| {
        options.limits.max_result_rows = 0;
    });
    expect_query_limit(
        &node,
        &graph,
        "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?o }",
        |options| options.limits.max_result_rows = 0,
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT DISTINCT ?s WHERE { ?s <urn:p> ?o }",
        |options| {
            options.fast_paths = QueryFastPathMode::Disabled;
            options.limits.max_hash_entries = 1;
        },
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s WHERE { ?s <urn:p> ?o ; <urn:q> ?x }",
        |options| options.limits.max_intermediate_rows = 1,
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?o . ?s <urn:q> ?x }",
        |options| options.limits.max_hash_bytes = 1,
    );

    for query in [
        "SELECT ?s WHERE { ?s <urn:p> ?o FILTER EXISTS { ?s <urn:q> ?x } }",
        "SELECT ?s WHERE { ?s <urn:p> ?o FILTER NOT EXISTS { ?s <urn:q> ?x } }",
        "SELECT ?s WHERE { ?s <urn:p> ?o MINUS { ?s <urn:q> ?x } }",
        "ASK { ?a <urn:next> ?b . ?b <urn:next> ?c . ?c <urn:next> ?a }",
    ] {
        expect_query_limit(&node, &graph, query, |options| {
            options.limits.max_intermediate_rows = 1;
        });
    }

    expect_query_limit(
        &node,
        &graph,
        "SELECT ?o WHERE { <urn:a> <urn:next>+ ?o }",
        |options| {
            options.fast_paths = QueryFastPathMode::Disabled;
            options.limits.max_property_path_edges = 1;
        },
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?o WHERE { <urn:a> <urn:next>+ ?o }",
        |options| options.limits.max_property_path_depth = 1,
    );
    expect_query_limit(
        &node,
        &graph,
        "CONSTRUCT { ?s <urn:copy> ?o } WHERE { ?s <urn:p> ?o }",
        |options| options.limits.max_graph_triples = 1,
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s WHERE { ?s <urn:p> ?o }",
        |options| options.limits.max_query_bytes = 1,
    );
    expect_query_limit(
        &node,
        &graph,
        "SELECT ?s WHERE { ?s <urn:p> ?o }",
        |options| options.limits.deadline = Some(Duration::ZERO),
    );

    let results = node
        .query_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            "SELECT ?s WHERE { ?s <urn:p> ?o }",
        )
        .unwrap();
    assert!(matches!(results, QueryResults::Solutions(rows) if rows.len() == 2));
}

#[test]
fn update_limits() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:update-limits");
    node.create_crate(
        &AllowAllAuthorizer,
        craqle::CreateCrateRequest::new(
            graph.clone(),
            "Update limits",
            "Bound update materialization.",
            "2026-08-22",
            None,
            craqle::GraphPolicy::default(),
        ),
    )
    .unwrap();
    let before = node.graph_snapshot(&graph).unwrap();
    let production = UpdateLimits::production();
    assert_eq!(production, UpdateLimits::default());
    assert!(production.deadline.is_some());

    let mut change_limits = production.clone();
    change_limits.max_changes = 1;
    let mut binding_limits = production.clone();
    binding_limits.max_materialized_bindings = 0;
    let mut graph_limits = production.clone();
    graph_limits.max_graphs = 1;
    let mut byte_limits = production.clone();
    byte_limits.max_update_bytes = 1;
    let mut deadline_limits = production;
    deadline_limits.deadline = Some(Duration::ZERO);
    let cases = [
        (
            "INSERT DATA { GRAPH <urn:test:update-limits> { <urn:a> <urn:p> <urn:o> . <urn:b> <urn:p> <urn:o> } }",
            change_limits,
        ),
        (
            "INSERT { GRAPH <urn:test:update-limits> { <urn:test:update-limits> <urn:copy> ?name } } WHERE { GRAPH <urn:test:update-limits> { <urn:test:update-limits> <http://schema.org/name> ?name } }",
            binding_limits,
        ),
        (
            "INSERT DATA { GRAPH <urn:test:update-one> { <urn:a> <urn:p> <urn:o> } GRAPH <urn:test:update-two> { <urn:b> <urn:p> <urn:o> } }",
            graph_limits,
        ),
        (
            "INSERT DATA { GRAPH <urn:test:update-limits> { <urn:a> <urn:p> <urn:o> } }",
            byte_limits,
        ),
        (
            "INSERT DATA { GRAPH <urn:test:update-limits> { <urn:a> <urn:p> <urn:o> } }",
            deadline_limits,
        ),
    ];
    for (update, limits) in cases {
        let mut options = UpdateOptions::default();
        options.limits = limits;
        let error = node
            .apply_sparql_update_with_options(&AllowAllAuthorizer, update, &options)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            CraqleErrorKind::QueryLimit,
            "{update}: {error:?}"
        );
        assert_eq!(node.graph_snapshot(&graph).unwrap(), before);
    }
}
