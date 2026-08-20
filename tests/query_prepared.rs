use craqle::{
    CraqleError, CraqleNode, DenyAllAuthorizer, EncodedTerm, GraphId, JoinKind, JoinMode,
    MaterializedQuadChange, QueryExecutionOptions, QueryResults,
};

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn insert(graph: &GraphId, subject: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri("urn:test:prepared:p"),
        object: iri(object),
    }
}

#[test]
fn prepared_queries_reuse_parsing_but_read_fresh_state() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:prepared:graph");
    node.apply_changes_unchecked(
        &graph,
        vec![insert(
            &graph,
            "urn:test:prepared:s1",
            "urn:test:prepared:o1",
        )],
    )
    .unwrap();

    let sparql = "SELECT ?s ?o WHERE { ?s <urn:test:prepared:p> ?o }";
    let old = node
        .query_graphs(std::slice::from_ref(&graph), sparql)
        .unwrap();
    let diagnostic = node
        .query_graphs_with_statistics(std::slice::from_ref(&graph), sparql)
        .unwrap();
    assert_eq!(diagnostic.results, old);
    assert_eq!(diagnostic.statistics.result_rows, 1);
    assert_eq!(diagnostic.statistics.result_cells, 2);
    assert!(
        diagnostic
            .statistics
            .time_to_first_internal_result
            .is_some()
    );
    assert!(diagnostic.statistics.intermediate_rows > 0);
    assert_eq!(diagnostic.statistics.qv_admission_checks, 1);
    assert_eq!(diagnostic.statistics.qv_header_reads, 1);
    assert_eq!(diagnostic.statistics.qv_counter_reads, 1);
    assert!(!diagnostic.statistics.plan_fingerprint.is_empty());
    assert!(!diagnostic.statistics.selected_access_paths.is_empty());

    let prepared = node.prepare_query(sparql).unwrap();
    let first = node
        .execute_prepared_graphs(
            std::slice::from_ref(&graph),
            &prepared,
            &QueryExecutionOptions::default(),
        )
        .unwrap();
    assert_eq!(first.results, old);
    assert_eq!(first.statistics.parse_time, std::time::Duration::ZERO);

    node.apply_changes_unchecked(
        &graph,
        vec![insert(
            &graph,
            "urn:test:prepared:s2",
            "urn:test:prepared:o2",
        )],
    )
    .unwrap();
    let second = node
        .execute_prepared_graphs(
            std::slice::from_ref(&graph),
            &prepared,
            &QueryExecutionOptions::default(),
        )
        .unwrap();
    assert_eq!(second.statistics.result_rows, 2);
    assert_eq!(second.statistics.result_cells, 4);
    assert_eq!(
        first.statistics.plan_fingerprint,
        second.statistics.plan_fingerprint
    );
    assert!(matches!(second.results, QueryResults::Solutions(rows) if rows.len() == 2));
}

#[test]
fn prepared_query_cancellation_is_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:prepared:cancel");
    node.apply_changes_unchecked(
        &graph,
        vec![insert(&graph, "urn:test:prepared:s", "urn:test:prepared:o")],
    )
    .unwrap();
    let prepared = node
        .prepare_query("SELECT ?s WHERE { ?s <urn:test:prepared:p> ?o }")
        .unwrap();
    let options = QueryExecutionOptions::default();
    options.cancellation.cancel();

    let error = node
        .execute_prepared_graphs(std::slice::from_ref(&graph), &prepared, &options)
        .unwrap_err();
    assert!(matches!(error, CraqleError::QueryCancelled), "{error:?}");
}

#[test]
fn diagnostic_authorization_matches_the_existing_query_api() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:prepared:denied");
    node.apply_changes_unchecked(
        &graph,
        vec![insert(&graph, "urn:test:prepared:s", "urn:test:prepared:o")],
    )
    .unwrap();
    let query = "SELECT ?s WHERE { ?s <urn:test:prepared:p> ?o }";
    let authorizer = DenyAllAuthorizer;

    let old = node.query(&authorizer, query).unwrap();
    let diagnostic = node.query_with_statistics(&authorizer, query).unwrap();
    assert_eq!(diagnostic.results, old);
    assert!(matches!(diagnostic.results, QueryResults::Solutions(rows) if rows.is_empty()));
}

#[test]
fn forced_hash_and_lateral_join_results_are_identical() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:prepared:joins");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:test:prepared:s", "urn:test:prepared:o"),
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: iri("urn:test:prepared:s"),
                predicate: iri("urn:test:prepared:q"),
                object: iri("urn:test:prepared:x"),
            },
        ],
    )
    .unwrap();
    let prepared = node
        .prepare_query(
            "SELECT ?s ?o ?x WHERE { ?s <urn:test:prepared:p> ?o . ?s <urn:test:prepared:q> ?x }",
        )
        .unwrap();
    let mut lateral_options = QueryExecutionOptions::default();
    lateral_options.join_mode = JoinMode::ForceLateral;
    let lateral = node
        .execute_prepared_graphs(std::slice::from_ref(&graph), &prepared, &lateral_options)
        .unwrap();
    let mut hash_options = QueryExecutionOptions::default();
    hash_options.join_mode = JoinMode::ForceHash;
    let hash = node
        .execute_prepared_graphs(std::slice::from_ref(&graph), &prepared, &hash_options)
        .unwrap();

    assert_eq!(hash.results, lateral.results);
    assert_eq!(
        lateral.statistics.planned_joins[0].physical_operator,
        JoinKind::IndexedLateral
    );
    assert_eq!(
        hash.statistics.planned_joins[0].physical_operator,
        JoinKind::Hash
    );

    let graph_query = node
        .prepare_query(
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s <urn:test:prepared:p> ?o . ?s <urn:test:prepared:q> ?x } }",
        )
        .unwrap();
    let graph_lateral = node
        .execute_prepared_graphs(std::slice::from_ref(&graph), &graph_query, &lateral_options)
        .unwrap();
    let graph_hash = node
        .execute_prepared_graphs(std::slice::from_ref(&graph), &graph_query, &hash_options)
        .unwrap();
    assert_eq!(graph_hash.results, graph_lateral.results);
    assert!(matches!(
        graph_hash.results,
        QueryResults::Solutions(rows)
            if rows.len() == 1 && rows[0].get("g") == Some(&iri(graph.as_str()))
    ));
}
