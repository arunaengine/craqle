#![allow(clippy::result_large_err)]

mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CraqleErrorKind, CraqleNode, EncodedTerm,
    GraphId, GraphPolicy, MaterializedQuadChange, QueryCancellation, QueryLimits, QueryOptions,
    QueryResults, UpdateLimits, UpdateOptions,
};

/// Wide enough that the pairwise product dwarfs the row count, small enough
/// that a rejected plan costs nothing.
const SIDE: usize = 40;

fn insert(graph: &GraphId, subject: &str, predicate: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm(format!("<{subject}>")),
        predicate: EncodedTerm(format!("<{predicate}>")),
        object: EncodedTerm(format!("<{object}>")),
    }
}

fn rows_limit(rows: usize) -> QueryLimits {
    let mut limits = QueryLimits::production();
    limits.max_intermediate_rows = rows;
    limits
}

fn path_limits(edges: usize, depth: usize) -> QueryLimits {
    let mut limits = QueryLimits::production();
    limits.max_property_path_edges = edges;
    limits.max_property_path_depth = depth;
    limits
}

fn query_options(limits: QueryLimits) -> QueryOptions {
    let mut options = QueryOptions::default();
    options.limits = limits;
    options
}

fn values_list(count: usize) -> String {
    (0..count)
        .map(|index| format!("\"value-{index:04}\""))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Two independent `VALUES` tables: `SIDE` rows each, `SIDE * SIDE` pairs.
fn product() -> String {
    format!(
        "VALUES ?a {{ {} }} VALUES ?b {{ {} }}",
        values_list(SIDE),
        values_list(SIDE)
    )
}

fn solution_rows(results: &QueryResults) -> usize {
    match results {
        QueryResults::Solutions(rows) => rows.len(),
        other => panic!("expected solutions, got {other:?}"),
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    node: CraqleNode,
    graph: GraphId,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();
        let graph = GraphId::new("urn:test:query-budgets");
        let mut changes = Vec::new();
        // Every subject shares one object, so the self join multiplies.
        for index in 0..SIDE {
            changes.push(insert(
                &graph,
                &format!("urn:s:{index:04}"),
                "urn:p",
                "urn:shared",
            ));
        }
        for index in 0..SIDE {
            changes.push(insert(
                &graph,
                &format!("urn:n:{index:04}"),
                "urn:next",
                &format!("urn:n:{:04}", index + 1),
            ));
        }
        node.apply_changes_unchecked(&graph, changes).unwrap();
        Self {
            _directory: directory,
            node,
            graph,
        }
    }

    fn run(&self, query: &str, limits: QueryLimits) -> craqle::Result<QueryResults> {
        let prepared = self.node.prepare_query(query)?;
        let options = query_options(limits);
        self.node
            .execute_prepared_in_graphs(
                &AllowAllAuthorizer,
                std::slice::from_ref(&self.graph),
                &prepared,
                &options,
            )
            .map(|execution| execution.results)
    }

    fn expect_limit(&self, query: &str, limits: QueryLimits) {
        match self.run(query, limits) {
            Err(error) => assert_eq!(error.kind(), CraqleErrorKind::QueryLimit, "{error}"),
            Ok(results) => panic!(
                "completed with {} rows instead of reporting a limit",
                solution_rows(&results)
            ),
        }
    }
}

/// BUDGET-01: two independent `VALUES` tables ordered by an expression over
/// both columns. The pairwise product is the work; the row counts are not.
#[test]
fn independent_values_product() {
    let fixture = Fixture::new();
    let query = format!(
        "SELECT ?a ?b WHERE {{ {} }} ORDER BY (CONCAT(?a, ?b))",
        product()
    );
    fixture.expect_limit(&query, rows_limit(SIDE * 4));
}

/// BUDGET-02: a store join whose output multiplicity exceeds the quads read.
/// The inner side is rescanned per outer row, so every output row does pass a
/// charged pull point.
#[test]
fn join_multiplicity_charged() {
    let fixture = Fixture::new();
    let query = "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?o . ?t <urn:p> ?o }";

    fixture.expect_limit(query, rows_limit(4));
    fixture.expect_limit(query, rows_limit(SIDE * 4));

    let results = fixture
        .run(query, rows_limit(SIDE * SIDE * 4))
        .expect("a bound above the charged pulls must complete");
    let QueryResults::Solutions(rows) = &results else {
        panic!("expected solutions, got {results:?}");
    };
    let count = rows[0].get("count").expect("?count must be bound");
    assert!(
        count.0.starts_with(&format!("\"{}\"", SIDE * SIDE)),
        "the join really did produce the larger product: {count:?}"
    );
}

/// BUDGET-03: an aggregate hides the intermediate cardinality from every
/// result-shaped limit.
#[test]
fn aggregate_hides_rows() {
    let fixture = Fixture::new();
    fixture.expect_limit(
        &format!("SELECT (COUNT(*) AS ?count) WHERE {{ {} }}", product()),
        rows_limit(SIDE * 4),
    );
}

/// BUDGET-03: `DISTINCT` collapses the product before it reaches a result.
#[test]
fn distinct_hides_rows() {
    let fixture = Fixture::new();
    fixture.expect_limit(
        &format!("SELECT DISTINCT ?a WHERE {{ {} }}", product()),
        rows_limit(SIDE * 4),
    );
}

/// BUDGET-03: `ORDER BY` with `LIMIT` sorts the whole product for one row.
#[test]
fn order_by_hides_rows() {
    let fixture = Fixture::new();
    fixture.expect_limit(
        &format!("SELECT ?a WHERE {{ {} }} ORDER BY ?b LIMIT 1", product()),
        rows_limit(SIDE * 4),
    );
}

/// BUDGET-04: long strings and repeated expression calls over a product that
/// collapses to a single result row.
#[test]
fn expression_cost_charged() {
    let fixture = Fixture::new();
    let long = "x".repeat(4096);
    let query = format!(
        "SELECT (COUNT(*) AS ?count) WHERE {{ {} FILTER(STRLEN(CONCAT(?a, \"{long}\", ?b)) > 0) }}",
        product()
    );
    fixture.expect_limit(&query, rows_limit(SIDE * 4));
}

/// BUDGET-05: property-path expansion is charged per traversed edge, and the
/// declared nesting depth is rejected before evaluation.
#[test]
fn deep_path_bounds() {
    let fixture = Fixture::new();
    let path = "SELECT ?o WHERE { <urn:n:0000> <urn:next>* ?o }";
    fixture.expect_limit(path, path_limits(4, 64));
    fixture.expect_limit(path, path_limits(1_000_000, 1));
}

/// BUDGET-07: a cancellation raised while the evaluation is already running.
/// The authorizer runs inside the request, so no thread or sleep is needed.
#[test]
fn cancel_during_evaluation() {
    let fixture = Fixture::new();
    let cancellation = QueryCancellation::new();
    let mut options = QueryOptions::default();
    options.cancellation = cancellation.clone();
    let prepared = fixture
        .node
        .prepare_query("SELECT ?s ?o WHERE { ?s <urn:p> ?o }")
        .unwrap();
    let authorizer =
        move |_: &GraphId, _: &GraphPolicy, _: Action| -> Result<(), AuthorizationError> {
            cancellation.cancel();
            Ok(())
        };
    let error = fixture
        .node
        .execute_prepared_in_graphs(
            &authorizer,
            std::slice::from_ref(&fixture.graph),
            &prepared,
            &options,
        )
        .expect_err("a cancelled request must not report a truncated result");
    assert_eq!(error.kind(), CraqleErrorKind::Cancelled, "{error}");
}

/// BUDGET-08: an update whose read side hides its intermediate cardinality
/// must be rejected before it materializes, with the source left untouched and
/// no state that spoils the next request.
#[test]
fn update_read_budget() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:update-budgets");
    node.create_crate(
        &AllowAllAuthorizer,
        craqle::CreateCrateRequest::new(
            graph.clone(),
            "Update budgets",
            "Bound the update read side.",
            "2026-09-12",
            None,
            GraphPolicy::default(),
        ),
    )
    .unwrap();
    let before = node.graph_snapshot(&graph).unwrap();

    let update = format!(
        "INSERT {{ GRAPH <{root}> {{ <{root}> <urn:copy> ?a }} }} WHERE {{ \
         SELECT DISTINCT ?a WHERE {{ {} }} }}",
        product(),
        root = graph.as_str()
    );
    let mut limits = UpdateLimits::production();
    limits.max_materialized_bindings = SIDE * 4;
    let mut options = UpdateOptions::default();
    options.limits = limits;
    match node.apply_sparql_update_with_options(&AllowAllAuthorizer, &update, &options) {
        Err(error) => assert_eq!(error.kind(), CraqleErrorKind::QueryLimit, "{error}"),
        Ok(_) => panic!("the hidden product was materialized instead of reported"),
    }
    assert_eq!(node.graph_snapshot(&graph).unwrap(), before);

    node.apply_sparql_update_with_options(
        &AllowAllAuthorizer,
        &format!(
            "INSERT DATA {{ GRAPH <{root}> {{ <{root}> <urn:copy> \"kept\" }} }}",
            root = graph.as_str()
        ),
        &UpdateOptions::default(),
    )
    .expect("a rejected update must not spoil the next one");
    assert_ne!(node.graph_snapshot(&graph).unwrap(), before);
}

/// Unlimited execution of the same shapes stays complete and correct.
#[test]
fn unlimited_stays_complete() {
    let fixture = Fixture::new();
    let results = fixture
        .run(
            "SELECT ?s ?o WHERE { ?s <urn:p> ?o }",
            QueryLimits::production(),
        )
        .unwrap();
    assert_eq!(solution_rows(&results), SIDE);
}

#[cfg(feature = "search")]
mod fts {
    use super::*;
    use craqle::{CraqleOptions, SearchStorage};

    /// More hits than the bound under test, so the rewrite alone exceeds it.
    const CRATES: usize = 30;

    fn seeded_node(directory: &tempfile::TempDir) -> CraqleNode {
        let node = CraqleNode::open_with_options(
            directory.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        for index in 0..CRATES {
            let graph = GraphId::new(&format!("urn:test:fts-budget:{index:04}"));
            node.apply_changes_unchecked(
                &graph,
                vec![MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm(format!("<{}>", graph.as_str())),
                    predicate: EncodedTerm("<http://schema.org/name>".to_owned()),
                    object: EncodedTerm("\"Proteomics Atlas\"".to_owned()),
                }],
            )
            .unwrap();
        }
        node.flush_search_updates().unwrap();
        node
    }

    /// BUDGET-06: the FTS rewrite runs before the first result, and its hits
    /// are the request's first intermediate rows.
    #[test]
    fn fts_rewrite_charged() {
        let directory = tempfile::tempdir().unwrap();
        let node = seeded_node(&directory);
        let query = format!(
            "SELECT ?s WHERE {{ SERVICE <urn:craqle:fts> {{ \
             ?s fts:query \"proteomics\" . ?s fts:limit {CRATES} . }} }}"
        );
        let prepared = node.prepare_query(&query).unwrap();
        let options = query_options(rows_limit(CRATES / 3));
        let error = node
            .execute_prepared(&AllowAllAuthorizer, &prepared, &options)
            .expect_err("rewrite hits must be charged before evaluation");
        assert_eq!(error.kind(), CraqleErrorKind::QueryLimit, "{error}");
    }
}
