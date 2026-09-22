//! Checks that unreadable graph policy fails default-union queries instead of hiding rows.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use crate::{
    Action, AuthorizationError, CraqleErrorKind, CraqleNode, CraqleOptions, EncodedTerm, GraphId,
    GraphPolicy, MaterializedQuadChange, QueryOptions, QueryRequest, QueryResults, Result,
    SearchStorage, vocab,
};

const ALLOWED: &str = "urn:test:policy:allowed";
const DENIED: &str = "urn:test:policy:denied";
const FAILING: &str = "urn:test:policy:failing";
const ROWS: &str = "SELECT ?s WHERE { ?s <http://schema.org/keywords> \"race\" }";

/// Reads public graphs and denies private ones, as a real policy decision.
fn public_only(
    graph: &GraphId,
    policy: &GraphPolicy,
    action: Action,
) -> std::result::Result<(), AuthorizationError> {
    if policy.public {
        Ok(())
    } else {
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_owned(),
        })
    }
}

fn fixture() -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_search_storage(SearchStorage::Memory),
    )
    .unwrap();
    for (graph, public) in [(ALLOWED, true), (DENIED, false), (FAILING, true)] {
        let graph = GraphId::new(graph);
        let policy = GraphPolicy {
            public,
            permission_paths: Vec::new(),
        };
        node.store.set_graph_policy(&graph, &policy).unwrap();
        node.apply_unchecked(
            &graph,
            vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: EncodedTerm("\"race\"".to_string()),
            }],
        )
        .unwrap();
    }
    (directory, node)
}

fn subjects(results: &QueryResults) -> Vec<String> {
    let QueryResults::Solutions(rows) = results else {
        panic!("expected solutions, got {results:?}");
    };
    let mut subjects: Vec<String> = rows
        .iter()
        .map(|row| row["s"].0.trim_matches(['<', '>']).to_owned())
        .collect();
    subjects.sort();
    subjects
}

fn count(results: &QueryResults) -> String {
    let QueryResults::Solutions(rows) = results else {
        panic!("expected solutions, got {results:?}");
    };
    rows[0]["n"].0.clone()
}

/// Every entry point that evaluates snapshot policy for the default union.
fn run_all(node: &CraqleNode, sparql: &str) -> Vec<(&'static str, Result<QueryResults>)> {
    let auth = &public_only;
    let options = QueryOptions::default();
    let cheap = QueryOptions {
        collect_plan_statistics: false,
        ..QueryOptions::default()
    };
    let prepared = node.prepare_query(sparql).unwrap();
    let request = |options| QueryRequest { sparql, options };
    vec![
        ("query", node.query(auth, sparql)),
        (
            "options",
            node.query_with_options(auth, request(&options))
                .map(|execution| execution.results),
        ),
        (
            "results only",
            node.query_with_options(auth, request(&cheap))
                .map(|execution| execution.results),
        ),
        (
            "statistics",
            node.query_with_statistics(auth, sparql)
                .map(|execution| execution.results),
        ),
        (
            "prepared",
            node.execute_prepared(auth, &prepared, &cheap)
                .map(|execution| execution.results),
        ),
        (
            "analyze",
            node.analyze_prepared(auth, &prepared, &options)
                .map(|_| QueryResults::Boolean(true)),
        ),
    ]
}

fn assert_failed(label: &str, result: Result<QueryResults>) {
    let error = result.expect_err(label);
    assert_eq!(CraqleErrorKind::Storage, error.kind(), "{label}: {error}");
    let message = error.to_string();
    assert!(
        !message.contains(DENIED) && !message.contains(FAILING),
        "{label}: {message}"
    );
}

#[test]
fn denied_stays_hidden() {
    let (_directory, node) = fixture();
    for (label, result) in run_all(&node, ROWS) {
        let results = result.unwrap_or_else(|error| panic!("{label}: {error}"));
        if label != "analyze" {
            assert_eq!(vec![ALLOWED, FAILING], subjects(&results), "{label}");
        }
    }
    let ask = format!("ASK {{ <{DENIED}> <http://schema.org/keywords> \"race\" }}");
    assert!(matches!(
        node.query(&public_only, &ask).unwrap(),
        QueryResults::Boolean(false)
    ));
}

#[test]
fn policy_failure_fails() {
    let (_directory, node) = fixture();
    node.store.fail_policy_reads(Some(GraphId::new(FAILING)));
    for (label, result) in run_all(&node, ROWS) {
        assert_failed(label, result);
    }
    let counted = "SELECT (COUNT(*) AS ?n) WHERE { ?s <http://schema.org/keywords> \"race\" }";
    for (label, result) in run_all(&node, counted) {
        assert_failed(label, result);
    }
    let ask = format!("ASK {{ <{FAILING}> <http://schema.org/keywords> \"race\" }}");
    for (label, result) in run_all(&node, &ask) {
        assert_failed(label, result);
    }

    node.store.fail_policy_reads(None);
    let counted = node.query(&public_only, counted).unwrap();
    assert!(count(&counted).starts_with("\"2\""), "{counted:?}");
}

#[test]
fn early_limit_completes() {
    let (_directory, node) = fixture();
    node.store.fail_policy_reads(Some(GraphId::new(FAILING)));
    let limited = format!("{ROWS} LIMIT 1");
    // A limit met before the failing graph is a complete answer; otherwise the query fails.
    for (label, result) in run_all(&node, &limited) {
        match result {
            Ok(results) if label != "analyze" => {
                assert_eq!(vec![ALLOWED], subjects(&results), "{label}");
            }
            Ok(_) => {}
            Err(error) => assert_eq!(CraqleErrorKind::Storage, error.kind(), "{label}"),
        }
    }
}

#[test]
fn explicit_scope_fails() {
    let (_directory, node) = fixture();
    node.store.fail_policy_reads(Some(GraphId::new(FAILING)));
    let graphs = [GraphId::new(ALLOWED), GraphId::new(FAILING)];
    let error = node
        .query_in_graphs(&public_only, &graphs, ROWS)
        .expect_err("explicit scope with unreadable policy");
    assert_ne!(CraqleErrorKind::Unauthorized, error.kind(), "{error}");
    let allowed = node
        .query_in_graphs(&public_only, &graphs[..1], ROWS)
        .unwrap();
    assert_eq!(vec![ALLOWED], subjects(&allowed));
}

/// Graph-level native plans hide denied graphs and fail on unreadable policy.
#[test]
fn graph_level_policy() {
    let (_directory, node) = fixture();
    let distinct =
        "SELECT DISTINCT ?s WHERE { GRAPH ?g { ?s <http://schema.org/keywords> \"race\" } }";
    let counted = "SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { \
                   ?s <http://schema.org/keywords> \"race\" } }";
    for (label, result) in run_all(&node, distinct) {
        let results = result.unwrap_or_else(|error| panic!("{label}: {error}"));
        if label != "analyze" {
            assert_eq!(vec![ALLOWED, FAILING], subjects(&results), "{label}");
        }
    }
    let runs = || crate::sparql::GRAPH_DISTINCT_RUNS.with(std::cell::Cell::get);
    let before = runs();
    let visible = node.query(&public_only, counted).unwrap();
    assert!(count(&visible).starts_with("\"2\""), "{visible:?}");
    assert_eq!(runs(), before + 1, "the count must use the native plan");

    node.store.fail_policy_reads(Some(GraphId::new(FAILING)));
    for sparql in [distinct, counted] {
        for (label, result) in run_all(&node, sparql) {
            assert_failed(label, result);
        }
    }
}
