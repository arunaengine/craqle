//! Checks that one explicit graph scope reads only that graph and keeps union semantics.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "../support.rs"]
mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    AllowAllAuthorizer, CraqleNode, EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange,
    QueryExecution, QueryOptions, QueryResults,
};

const TARGET: &str = "urn:test:scope:target";
const SHARED: &str = "urn:test:scope:shared";
const UNRELATED: &str = "urn:test:scope:unrelated";
const NAME: &str = "urn:test:scope:name";
const KNOWS: &str = "urn:test:scope:knows";

/// Queries whose default-graph answer over the target alone has a `GRAPH` oracle.
const QUERIES: [(&str, &str); 6] = [
    (
        "multi-valued",
        "SELECT ?s ?name WHERE { ?s <urn:test:scope:name> ?name }",
    ),
    (
        "join",
        "SELECT ?s ?o ?name WHERE { ?s <urn:test:scope:knows> ?o . ?o <urn:test:scope:name> ?name }",
    ),
    (
        "count",
        "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:test:scope:name> ?name }",
    ),
    (
        "ordered",
        "SELECT ?name WHERE { ?s <urn:test:scope:name> ?name } ORDER BY DESC(?name) LIMIT 3",
    ),
    (
        "deleted",
        "ASK { <urn:test:scope:s:gone> <urn:test:scope:name> \"gone\" }",
    ),
    (
        "shared",
        "SELECT ?s WHERE { ?s <urn:test:scope:knows> <urn:test:scope:s:1> }",
    ),
];

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn quad(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

/// Target data, a graph sharing some of its triples, and `unrelated` rows elsewhere.
fn fixture(unrelated: usize) -> (tempfile::TempDir, CraqleNode) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let [target, shared, other] = [TARGET, SHARED, UNRELATED].map(GraphId::new);
    for graph in [&target, &shared, &other] {
        node.import_graph_policy(graph, GraphPolicy::default())
            .unwrap();
    }
    let mut changes = Vec::new();
    for index in 0..6 {
        let subject = format!("urn:test:scope:s:{index}");
        for value in ["alpha", "beta"] {
            let name = EncodedTerm(format!("\"{value} {index}\""));
            changes.push(quad(&target, &subject, NAME, name));
        }
        let next = format!("urn:test:scope:s:{}", (index + 1) % 6);
        changes.push(quad(&target, &subject, KNOWS, iri(&next)));
    }
    changes.push(quad(
        &target,
        "urn:test:scope:s:gone",
        NAME,
        EncodedTerm("\"gone\"".into()),
    ));
    node.apply_changes_unchecked(&target, changes).unwrap();
    node.apply_changes_unchecked(
        &target,
        vec![MaterializedQuadChange::Delete {
            graph: target.clone(),
            subject: iri("urn:test:scope:s:gone"),
            predicate: iri(NAME),
            object: EncodedTerm("\"gone\"".into()),
        }],
    )
    .unwrap();
    // The same triples in a second graph must not duplicate default-union rows.
    let copies = vec![
        quad(
            &shared,
            "urn:test:scope:s:0",
            KNOWS,
            iri("urn:test:scope:s:1"),
        ),
        quad(
            &shared,
            "urn:test:scope:s:gone",
            NAME,
            EncodedTerm("\"gone\"".into()),
        ),
    ];
    node.apply_changes_unchecked(&shared, copies).unwrap();
    let noise = (0..unrelated)
        .flat_map(|index| {
            let subject = format!("urn:test:scope:noise:{index}");
            let name = EncodedTerm(format!("\"noise {index}\""));
            [
                quad(&other, &subject, NAME, name),
                quad(&other, &subject, KNOWS, iri("urn:test:scope:s:1")),
            ]
        })
        .collect();
    node.apply_changes_unchecked(&other, noise).unwrap();
    (directory, node)
}

/// Every solution as sorted `name=term` cells, keeping duplicate rows and, if asked, order.
fn canonical(results: QueryResults, ordered: bool) -> Vec<String> {
    let QueryResults::Solutions(rows) = results else {
        return vec![format!("{results:?}")];
    };
    let mut rows: Vec<String> = rows
        .into_iter()
        .map(|row| {
            let mut cells: Vec<String> = row
                .into_iter()
                .map(|(name, term)| format!("{name}={}", term.0))
                .collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    if !ordered {
        rows.sort();
    }
    rows
}

fn scoped(node: &CraqleNode, graphs: &[&str], query: &str) -> QueryExecution {
    let graphs: Vec<GraphId> = graphs.iter().map(|graph| GraphId::new(graph)).collect();
    let mut options = QueryOptions::default();
    options.collect_costs = true;
    node.query_in_graphs_with_options(&AllowAllAuthorizer, &graphs, query, &options)
        .unwrap()
}

/// The same query with every default-graph pattern placed inside `GRAPH <target>`.
fn oracle(node: &CraqleNode, query: &str) -> QueryResults {
    let open = query.find('{').unwrap();
    let close = query.rfind('}').unwrap();
    let rewritten = format!(
        "{} {{ GRAPH <{TARGET}> {} }}{}",
        &query[..open],
        &query[open..=close],
        &query[close + 1..]
    );
    node.query(&AllowAllAuthorizer, &rewritten).unwrap()
}

#[test]
fn single_scope_matches() {
    let (_directory, node) = fixture(40);
    for (label, query) in QUERIES {
        let ordered = label == "ordered";
        let expected = canonical(oracle(&node, query), ordered);
        for graphs in [&[TARGET][..], &[TARGET, TARGET][..]] {
            let actual = canonical(scoped(&node, graphs, query).results, ordered);
            assert_eq!(expected, actual, "{label} over {graphs:?}");
        }
    }
    let count = canonical(scoped(&node, &[TARGET], QUERIES[2].1).results, false);
    assert_eq!(
        vec!["n=\"12\"^^<http://www.w3.org/2001/XMLSchema#integer>"],
        count
    );
}

#[test]
fn union_stays_distinct() {
    let (_directory, node) = fixture(40);
    let shared = QUERIES[5].1;
    let union = canonical(scoped(&node, &[TARGET, SHARED], shared).results, false);
    assert_eq!(vec!["s=<urn:test:scope:s:0>"], union);
    let deleted = canonical(
        scoped(&node, &[TARGET, SHARED], QUERIES[4].1).results,
        false,
    );
    assert_eq!(vec!["Boolean(true)"], deleted);
    let named = "SELECT ?g WHERE { GRAPH ?g { <urn:test:scope:s:0> <urn:test:scope:knows> ?o } }";
    let copies = canonical(scoped(&node, &[TARGET, SHARED], named).results, false);
    assert_eq!(2, copies.len(), "named graph copies stay separate");
}

#[test]
fn unrelated_rows_unread() {
    let (_small_directory, small) = fixture(4);
    let (_large_directory, large) = fixture(4_000);
    for (label, query) in QUERIES {
        let small_run = scoped(&small, &[TARGET], query);
        let large_run = scoped(&large, &[TARGET], query);
        assert_eq!(
            canonical(small_run.results.clone(), false),
            canonical(large_run.results.clone(), false),
            "{label}"
        );
        let work = |run: &QueryExecution| {
            (
                run.statistics.candidate_quads,
                run.statistics.qv_keys_read,
                run.statistics.source_keys_read,
            )
        };
        assert_eq!(
            work(&small_run),
            work(&large_run),
            "{label}: unrelated rows changed the scoped read work"
        );
    }
}
