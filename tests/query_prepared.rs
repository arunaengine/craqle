mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CraqleError, CraqleErrorKind, CraqleNode,
    DenyAllAuthorizer, EncodedTerm, GraphId, GraphPolicy, JoinKind, JoinMode,
    MaterializedQuadChange, QueryExecutionOptions, QueryLogicalOperator, QueryPhysicalOperator,
    QueryPlan, QueryResults,
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
fn prepared_reads_fresh() {
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
        .query_in_graphs(&AllowAllAuthorizer, std::slice::from_ref(&graph), sparql)
        .unwrap();
    let diagnostic = node
        .query_in_graphs_with_options(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            sparql,
            &QueryExecutionOptions::default(),
        )
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
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &QueryExecutionOptions::default(),
        )
        .unwrap();
    assert_eq!(first.results, old);
    assert_eq!(first.statistics.parse_time, std::time::Duration::ZERO);

    let first_query_id_generation = first.statistics.query_id_generation.unwrap();
    let rebuilt = node.rebuild_query_indexes().unwrap();
    assert!(rebuilt.query_id_generation > first_query_id_generation);
    let after_rebuild = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &QueryExecutionOptions::default(),
        )
        .unwrap();
    assert_eq!(after_rebuild.results, old);
    assert_eq!(
        after_rebuild.statistics.query_id_generation,
        Some(rebuilt.query_id_generation)
    );

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
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
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
fn prepared_cancellation() {
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
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &options,
        )
        .unwrap_err();
    assert!(matches!(error, CraqleError::QueryCancelled), "{error:?}");
}

#[test]
fn diagnostic_access_matches() {
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
fn explicit_graph_query_authorization_fails_the_whole_request() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let readable = GraphId::new("urn:test:explicit-auth:readable");
    let hidden = GraphId::new("urn:test:explicit-auth:hidden");
    let missing = GraphId::new("urn:test:explicit-auth:missing");
    node.apply_changes_unchecked(
        &readable,
        vec![insert(
            &readable,
            "urn:test:explicit-auth:s:readable",
            "urn:test:explicit-auth:o:readable",
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &hidden,
        vec![insert(
            &hidden,
            "urn:test:explicit-auth:s:hidden",
            "urn:test:explicit-auth:o:hidden",
        )],
    )
    .unwrap();

    let readable_for_auth = readable.clone();
    let auth = move |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
        if graph == &readable_for_auth && action == Action::Read {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    };
    let sparql = "SELECT ?s WHERE { ?s <urn:test:prepared:p> ?o }";
    assert!(matches!(
        node.query_in_graphs(&auth, std::slice::from_ref(&readable), sparql),
        Ok(QueryResults::Solutions(rows)) if rows.len() == 1
    ));

    for graphs in [
        std::slice::from_ref(&hidden),
        std::slice::from_ref(&missing),
        &[readable.clone(), hidden.clone()],
    ] {
        let error = node.query_in_graphs(&auth, graphs, sparql).unwrap_err();
        assert_eq!(CraqleErrorKind::Unauthorized, error.kind());
    }

    let mixed = [readable.clone(), hidden.clone()];
    let prepared = node.prepare_query(sparql).unwrap();
    let options = QueryExecutionOptions::default();
    assert_eq!(
        CraqleErrorKind::Unauthorized,
        node.query_in_graphs_with_options(&auth, &mixed, sparql, &options)
            .unwrap_err()
            .kind()
    );
    assert_eq!(
        CraqleErrorKind::Unauthorized,
        node.execute_prepared_in_graphs(&auth, &mixed, &prepared, &options)
            .unwrap_err()
            .kind()
    );
    assert_eq!(
        CraqleErrorKind::Unauthorized,
        node.explain_prepared_in_graphs(&auth, &mixed, &prepared, &options)
            .unwrap_err()
            .kind()
    );
    assert_eq!(
        CraqleErrorKind::Unauthorized,
        node.analyze_prepared_in_graphs(&auth, &mixed, &prepared, &options)
            .unwrap_err()
            .kind()
    );

    let fts = node
        .prepare_query(
            "SELECT ?s WHERE { SERVICE <urn:craqle:fts> { \
             ?s <urn:craqle:fts:query> \"authorization\" } }",
        )
        .unwrap();
    assert_eq!(
        CraqleErrorKind::Unauthorized,
        node.execute_prepared_in_graphs(&auth, &mixed, &fts, &options)
            .unwrap_err()
            .kind()
    );
}

#[test]
fn forced_join_results() {
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
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &lateral_options,
        )
        .unwrap();
    let mut hash_options = QueryExecutionOptions::default();
    hash_options.join_mode = JoinMode::ForceHash;
    let hash = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &hash_options,
        )
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
    let explained = node
        .explain_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &hash_options,
        )
        .unwrap();
    assert_eq!(
        explained.root.logical_operator,
        QueryLogicalOperator::Select
    );
    assert_eq!(
        explained.root.physical_operator,
        QueryPhysicalOperator::Generic
    );
    assert!(explained.root.actual_rows.is_none());
    assert!(explained.root.children.iter().any(|node| {
        node.physical_operator == QueryPhysicalOperator::PlannedJoin(JoinKind::Hash)
    }));
    let serialized = serde_json::to_string(&explained).unwrap();
    assert_eq!(
        serde_json::from_str::<QueryPlan>(&serialized).unwrap(),
        explained
    );

    let analyzed = node
        .analyze_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &prepared,
            &hash_options,
        )
        .unwrap();
    assert_eq!(analyzed.root.actual_rows, Some(1));
    assert_eq!(analyzed.root.output_rows, 1);
    assert!(analyzed.root.index_seeks > 0);
    assert!(analyzed.root.candidate_rows > 0);
    assert!(!analyzed.root.access_paths.is_empty());
    assert!(analyzed.root.elapsed_time > std::time::Duration::ZERO);

    let graph_query = node
        .prepare_query(
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s <urn:test:prepared:p> ?o . ?s <urn:test:prepared:q> ?x } }",
        )
        .unwrap();
    let graph_lateral = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &graph_query,
            &lateral_options,
        )
        .unwrap();
    let graph_hash = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &graph_query,
            &hash_options,
        )
        .unwrap();
    assert_eq!(graph_hash.results, graph_lateral.results);
    assert!(matches!(
        graph_hash.results,
        QueryResults::Solutions(rows)
            if rows.len() == 1 && rows[0].get("g") == Some(&iri(graph.as_str()))
    ));
}
