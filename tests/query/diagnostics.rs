//! Checks that authorized query diagnostics do not reveal denied graph sizes.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "../support.rs"]
mod support;

use std::time::Duration;

use crate::support::TestWriteExt as _;
use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CraqleNode, EncodedTerm, GraphId, GraphPolicy,
    JoinMode, MaterializedQuadChange, QueryExecution, QueryExecutionStatistics, QueryOptions,
    QueryPhysicalOperator, QueryPlan, QueryPlanNode, QueryRequest,
};

const QUERY: &str = "SELECT ?left ?right ?key WHERE { \
     ?left <urn:test:diag:left> ?key . ?right <urn:test:diag:right> ?key }";
/// A single-pattern count that ordinary options may admit to a fast path.
const COUNT: &str = "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:test:diag:right> ?key }";

struct Fixture {
    _directory: tempfile::TempDir,
    node: CraqleNode,
    readable: GraphId,
}

fn iri(value: String) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn rows(graph: &GraphId, count: usize) -> Vec<MaterializedQuadChange> {
    let mut changes = Vec::new();
    for index in 0..count {
        for side in ["left", "right"] {
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: iri(format!("{}:{side}:{index}", graph.as_str())),
                predicate: iri(format!("urn:test:diag:{side}")),
                object: iri(format!("urn:test:diag:key:{}", index % 4)),
            });
        }
    }
    changes
}

/// Identical readable data next to a denied graph of the given size.
fn fixture(denied_rows: usize) -> Fixture {
    skewed_fixture(denied_rows, 0)
}

/// Like [`fixture`], with `right_only` extra denied rows that skew one join side.
fn skewed_fixture(denied_rows: usize, right_only: usize) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let readable = GraphId::new("urn:test:diag:readable");
    let denied = GraphId::new("urn:test:diag:denied");
    for (graph, count) in [(&readable, 8), (&denied, denied_rows)] {
        node.import_graph_policy(graph, GraphPolicy::default())
            .unwrap();
        node.apply_changes_unchecked(graph, rows(graph, count))
            .unwrap();
    }
    let skew = (0..right_only)
        .map(|index| MaterializedQuadChange::Insert {
            graph: denied.clone(),
            subject: iri(format!("urn:test:diag:skew:{index}")),
            predicate: iri("urn:test:diag:right".to_owned()),
            object: iri(format!("urn:test:diag:key:{}", index % 4)),
        })
        .collect();
    node.apply_changes_unchecked(&denied, skew).unwrap();
    Fixture {
        _directory: directory,
        node,
        readable,
    }
}

fn reader(
    graph: &GraphId,
    _policy: &GraphPolicy,
    action: Action,
) -> Result<(), AuthorizationError> {
    if graph.as_str() == "urn:test:diag:readable" && action == Action::Read {
        Ok(())
    } else {
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_owned(),
        })
    }
}

/// Pins the join so that only counts, not the chosen plan, can differ.
fn planner_options() -> QueryOptions {
    let mut options = QueryOptions::default();
    options.fast_paths = craqle::QueryFastPathMode::Disabled;
    options.join_mode = JoinMode::ForceLateral;
    options.collect_costs = true;
    options
}

/// Removes elapsed times, which may differ under shared resources.
fn untimed_node(mut node: QueryPlanNode) -> QueryPlanNode {
    node.elapsed_time = Duration::ZERO;
    node.children = node.children.into_iter().map(untimed_node).collect();
    node
}

fn untimed_plan(plan: QueryPlan) -> QueryPlan {
    QueryPlan {
        fingerprint: plan.fingerprint,
        root: untimed_node(plan.root),
    }
}

fn untimed_stats(mut stats: QueryExecutionStatistics) -> QueryExecutionStatistics {
    stats.parse_time = Duration::ZERO;
    stats.rewrite_time = Duration::ZERO;
    stats.planning_time = Duration::ZERO;
    stats.execution_time = Duration::ZERO;
    stats.result_collection_time = Duration::ZERO;
    stats.time_to_first_internal_result =
        stats.time_to_first_internal_result.map(|_| Duration::ZERO);
    stats.plan = untimed_plan(stats.plan);
    stats
}

/// Automatic join selection and fast-path admission, as ordinary callers run them.
fn automatic_options() -> QueryOptions {
    let mut options = QueryOptions::default();
    options.collect_costs = true;
    options
}

/// Every caller-visible output of one fixture, in a comparable form.
fn observe(fixture: &Fixture, options: &QueryOptions, query: &str) -> Vec<(String, String)> {
    let node = &fixture.node;
    let graphs = std::slice::from_ref(&fixture.readable);
    let options = options.clone();
    let prepared = node.prepare_query(query).unwrap();
    let execution = |label: &str, run: craqle::Result<QueryExecution>| {
        let run = run.unwrap();
        let mut rows = support::solution_rows(run.results)
            .into_iter()
            .map(|row| {
                format!(
                    "{:?}",
                    row.into_iter()
                        .collect::<std::collections::BTreeMap<_, _>>()
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        vec![
            (format!("{label}:rows"), rows.join("\n")),
            (
                format!("{label}:statistics"),
                format!("{:?}", untimed_stats(run.statistics)),
            ),
        ]
    };
    let plan = |label: &str, plan: craqle::Result<QueryPlan>| {
        vec![(
            label.to_owned(),
            format!("{:?}", untimed_plan(plan.unwrap())),
        )]
    };
    let request = QueryRequest {
        sparql: query,
        options: &options,
    };
    [
        execution("options", node.query_with_options(&reader, request)),
        execution("statistics", node.query_with_statistics(&reader, query)),
        execution(
            "graphs",
            node.query_in_graphs_with_options(&reader, graphs, query, &options),
        ),
        execution(
            "prepared",
            node.execute_prepared(&reader, &prepared, &options),
        ),
        execution(
            "prepared_graphs",
            node.execute_prepared_in_graphs(&reader, graphs, &prepared, &options),
        ),
        plan(
            "explain",
            node.explain_prepared(&reader, &prepared, &options),
        ),
        plan(
            "explain_graphs",
            node.explain_prepared_in_graphs(&reader, graphs, &prepared, &options),
        ),
        plan(
            "analyze",
            node.analyze_prepared(&reader, &prepared, &options),
        ),
        plan(
            "analyze_graphs",
            node.analyze_prepared_in_graphs(&reader, graphs, &prepared, &options),
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn assert_same(small: &[(String, String)], large: &[(String, String)]) {
    assert_eq!(small.len(), large.len());
    let mut differences = Vec::new();
    for ((label, small), (_, large)) in small.iter().zip(large) {
        if small != large {
            differences.push(format!("{label}\n  small: {small}\n  large: {large}"));
        }
    }
    assert!(
        differences.is_empty(),
        "denied data changed caller-visible output:\n{}",
        differences.join("\n")
    );
}

#[test]
fn denied_sizes_hidden() {
    let options = planner_options();
    assert_same(
        &observe(&fixture(2), &options, QUERY),
        &observe(&fixture(3_000), &options, QUERY),
    );
}

#[test]
fn automatic_plan_hidden() {
    let options = automatic_options();
    for query in [QUERY, COUNT] {
        let small = observe(&skewed_fixture(2, 0), &options, query);
        assert_same(&small, &observe(&skewed_fixture(3_000, 0), &options, query));
        assert_same(&small, &observe(&skewed_fixture(2, 6_000), &options, query));
    }
}

#[test]
fn default_counts_withheld() {
    let fixture = fixture(3_000);
    let restricted = fixture
        .node
        .query_with_statistics(&reader, QUERY)
        .unwrap()
        .statistics;
    let full = fixture
        .node
        .query_in_graphs_with_options(
            &AllowAllAuthorizer,
            std::slice::from_ref(&fixture.readable),
            QUERY,
            &QueryOptions::default(),
        )
        .unwrap()
        .statistics;
    assert!(restricted.details_withheld());
    assert_eq!(restricted.result_rows, 16);
    assert!(restricted.planned_joins.is_empty());
    assert!(restricted.selected_access_paths.is_empty());
    assert!(!restricted.intermediate_rows_available);
    assert_eq!(restricted.plan.root.estimated_rows, None);
    assert_eq!(restricted.candidate_quads, 0);
    assert!(restricted.plan.root.children.is_empty());
    assert_eq!(restricted.plan.root.output_rows, 16);
    assert_eq!(
        restricted.plan.root.physical_operator,
        QueryPhysicalOperator::Withheld
    );
    assert_eq!(restricted.plan_fingerprint, restricted.plan.fingerprint);

    // A caller that reads every graph keeps the complete physical diagnostics.
    assert!(!full.details_withheld());
    assert_eq!(full.result_rows, 16);
    assert!(full.planned_joins[0].estimated_left_rows > 0);
    assert!(full.plan.root.estimated_rows.is_some());
    assert!(full.candidate_quads > 0);
    assert!(!full.plan.root.children.is_empty());
    assert_ne!(full.plan_fingerprint, restricted.plan_fingerprint);
}
