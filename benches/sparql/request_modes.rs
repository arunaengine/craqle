//! Measures results-only and diagnostic query modes, and planning next to denied data.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::env;
use std::time::{Duration, Instant};

#[path = "../support.rs"]
mod support;

use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, Authorizer, CraqleNode, EncodedTerm, GraphId,
    GraphPolicy, MaterializedQuadChange, PreparedQuery, QueryExecutionStatistics, QueryOptions,
    QueryResults,
};
use serde_json::json;
use support::BenchWriteExt as _;
use support::fixture::{binary_blake3, repository_commit};

const QUERY: &str = "SELECT ?left ?right ?key WHERE { \
     ?left <urn:bench:modes:left> ?key . ?right <urn:bench:modes:right> ?key }";
const READABLE: &str = "urn:bench:modes:readable";
const DENIED: &str = "urn:bench:modes:denied";
const LOAD_BATCH: usize = 1_000;

struct Fixture {
    node: CraqleNode,
    _directory: tempfile::TempDir,
    readable: GraphId,
    prepared: PreparedQuery,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Plain,
    Statistics,
    Graphs,
    GraphsDetailed,
    Prepared { detailed: bool },
    PreparedGraphs { detailed: bool },
}

const MODES: [Mode; 8] = [
    Mode::Plain,
    Mode::Statistics,
    Mode::Graphs,
    Mode::GraphsDetailed,
    Mode::Prepared { detailed: false },
    Mode::Prepared { detailed: true },
    Mode::PreparedGraphs { detailed: false },
    Mode::PreparedGraphs { detailed: true },
];

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Plain => "query",
            Self::Statistics => "query_with_statistics",
            Self::Graphs => "query_in_graphs",
            Self::GraphsDetailed => "query_in_graphs_with_options",
            Self::Prepared { detailed: false } => "execute_prepared_results",
            Self::Prepared { detailed: true } => "execute_prepared_detailed",
            Self::PreparedGraphs { detailed: false } => "execute_prepared_in_graphs_results",
            Self::PreparedGraphs { detailed: true } => "execute_prepared_in_graphs_detailed",
        }
    }

    fn detailed(self) -> bool {
        match self {
            Self::Plain | Self::Graphs => false,
            Self::Statistics | Self::GraphsDetailed => true,
            Self::Prepared { detailed } | Self::PreparedGraphs { detailed } => detailed,
        }
    }

    fn parses(self) -> bool {
        !matches!(self, Self::Prepared { .. } | Self::PreparedGraphs { .. })
    }
}

struct Sample {
    elapsed: Duration,
    results: QueryResults,
    statistics: Option<QueryExecutionStatistics>,
}

fn reader(
    graph: &GraphId,
    _policy: &GraphPolicy,
    action: Action,
) -> Result<(), AuthorizationError> {
    if graph.as_str() == READABLE && action == Action::Read {
        Ok(())
    } else {
        Err(AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_owned(),
        })
    }
}

fn load(node: &CraqleNode, graph: &GraphId, rows: usize) {
    let keys = (rows / 4).max(1);
    let mut changes = Vec::with_capacity(LOAD_BATCH);
    for index in 0..rows {
        for side in ["left", "right"] {
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm(format!("<{}:{side}:{index}>", graph.as_str())),
                predicate: EncodedTerm(format!("<urn:bench:modes:{side}>")),
                object: EncodedTerm(format!("<urn:bench:modes:key:{}>", index % keys)),
            });
        }
        if changes.len() >= LOAD_BATCH {
            node.apply_bulk_unchecked(graph, std::mem::take(&mut changes))
                .expect("load benchmark rows");
        }
    }
    if !changes.is_empty() {
        node.apply_bulk_unchecked(graph, changes)
            .expect("load benchmark rows");
    }
}

impl Fixture {
    fn new(readable_rows: usize, denied_rows: usize) -> Self {
        let directory = tempfile::tempdir().expect("benchmark directory");
        let node = CraqleNode::open(directory.path()).expect("open benchmark node");
        let readable = GraphId::new(READABLE);
        let denied = GraphId::new(DENIED);
        for (graph, rows) in [(&readable, readable_rows), (&denied, denied_rows)] {
            node.set_graph_policy(&AllowAllAuthorizer, graph, GraphPolicy::default())
                .expect("benchmark policy");
            load(&node, graph, rows);
        }
        let prepared = node.prepare_query(QUERY).expect("prepare benchmark query");
        Self {
            node,
            _directory: directory,
            readable,
            prepared,
        }
    }

    fn run(&self, mode: Mode, auth: &dyn Authorizer) -> Sample {
        let graphs = std::slice::from_ref(&self.readable);
        let mut options = QueryOptions::default();
        options.collect_plan_statistics = mode.detailed();
        let started = Instant::now();
        let (results, statistics) = match mode {
            Mode::Plain => (self.node.query(auth, QUERY).expect("query"), None),
            Mode::Graphs => (
                self.node
                    .query_in_graphs(auth, graphs, QUERY)
                    .expect("graph query"),
                None,
            ),
            Mode::Statistics => {
                let run = self
                    .node
                    .query_with_statistics(auth, QUERY)
                    .expect("statistics query");
                (run.results, Some(run.statistics))
            }
            Mode::GraphsDetailed => {
                let run = self
                    .node
                    .query_in_graphs_with_options(auth, graphs, QUERY, &options)
                    .expect("graph statistics query");
                (run.results, Some(run.statistics))
            }
            Mode::Prepared { .. } => {
                let run = self
                    .node
                    .execute_prepared(auth, &self.prepared, &options)
                    .expect("prepared query");
                (run.results, Some(run.statistics))
            }
            Mode::PreparedGraphs { .. } => {
                let run = self
                    .node
                    .execute_prepared_in_graphs(auth, graphs, &self.prepared, &options)
                    .expect("prepared graph query");
                (run.results, Some(run.statistics))
            }
        };
        Sample {
            elapsed: started.elapsed(),
            results,
            statistics,
        }
    }
}

/// Order-independent digest of a solution multiset.
fn digest(results: &QueryResults) -> (String, usize) {
    let QueryResults::Solutions(rows) = results else {
        panic!("benchmark query must return solutions");
    };
    let mut rows = rows
        .iter()
        .map(|row| {
            let mut cells = row
                .iter()
                .map(|(name, term)| format!("{name}={}", term.0))
                .collect::<Vec<_>>();
            cells.sort();
            cells.join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    let count = rows.len();
    (
        blake3::hash(rows.join("\n").as_bytes())
            .to_hex()
            .to_string(),
        count,
    )
}

fn nanos(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], part: usize) -> u64 {
    sorted[(sorted.len() - 1) * part / 100]
}

fn env_list(name: &str, defaults: &[usize]) -> Vec<usize> {
    env::var(name).map_or_else(
        |_| defaults.to_vec(),
        |value| {
            value
                .split(',')
                .map(|item| {
                    item.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("{name} must list unsigned integers"))
                })
                .collect()
        },
    )
}

/// Runs every mode in a rotated order per round and prints one record per mode.
fn measure(fixture: &Fixture, scenario: serde_json::Value, auth: &dyn Authorizer) {
    let rounds = env_list("CRAQLE_MODES_ROUNDS", &[30])[0];
    let mut samples = vec![Vec::with_capacity(rounds); MODES.len()];
    let mut expected = None;
    let mut last = vec![None; MODES.len()];
    for round in 0..rounds.saturating_add(1) {
        for offset in 0..MODES.len() {
            let index = (round + offset) % MODES.len();
            let sample = fixture.run(MODES[index], auth);
            let identity = digest(&sample.results);
            assert_eq!(
                expected.get_or_insert_with(|| identity.clone()),
                &identity,
                "{} changed the result multiset",
                MODES[index].label()
            );
            // Round zero only warms caches and checks results.
            if round > 0 {
                samples[index].push(nanos(sample.elapsed));
            }
            last[index] = sample.statistics;
        }
    }
    let (digest, rows) = expected.expect("at least one sample");
    for (index, mode) in MODES.iter().enumerate() {
        let mut sorted = samples[index].clone();
        sorted.sort_unstable();
        let statistics = last[index].as_ref();
        println!(
            "{}",
            json!({
                "record": "request_mode",
                "scenario": scenario,
                "mode": mode.label(),
                "detailed_statistics": mode.detailed(),
                "includes_parse": mode.parses(),
                "result_rows": rows,
                "result_digest": digest,
                "median_ns": percentile(&sorted, 50),
                "p90_ns": percentile(&sorted, 90),
                "samples_ns": samples[index],
                "planning_ns": statistics.map(|stats| nanos(stats.planning_time)),
                "joins": statistics.map(|stats| stats
                    .planned_joins
                    .iter()
                    .map(|join| format!("{:?}", join.physical_operator))
                    .collect::<Vec<_>>()),
                "estimated_rows": statistics.and_then(|stats| stats.plan.root.estimated_rows),
            })
        );
    }
}

/// Storage work of one explicit single-graph run, outside any timed interval.
fn scoped_work(fixture: &Fixture, denied: usize) {
    let mut options = QueryOptions::default();
    options.collect_costs = true;
    options.collect_plan_statistics = false;
    let run = fixture
        .node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&fixture.readable),
            &fixture.prepared,
            &options,
        )
        .expect("scoped counter run");
    let stats = run.statistics;
    println!(
        "{}",
        json!({
            "record": "scoped_work",
            "denied_rows": denied,
            "result_rows": stats.result_rows,
            "candidate_quads": stats.candidate_quads,
            "qv_keys_read": stats.qv_keys_read,
            "qv_bytes_read": stats.qv_bytes_read,
            "source_keys_read": stats.source_keys_read,
            "forward_mapping_reads": stats.forward_mapping_reads,
            "reverse_mapping_reads": stats.reverse_mapping_reads,
            "planner_point_reads": stats.planner_point_reads,
            "planning_ns": nanos(stats.planning_time),
            "access_paths": format!("{:?}", stats.selected_access_paths),
        })
    );
}

fn main() {
    let readable = env_list("CRAQLE_MODES_READABLE", &[2_000])[0];
    let denied = env_list("CRAQLE_MODES_DENIED", &[0, 2_000, 20_000]);
    println!(
        "{}",
        json!({
            "record": "request_mode_provenance",
            "commit": repository_commit(),
            "binary_blake3": binary_blake3(),
            "readable_rows": readable,
            "denied_rows": &denied,
            "query": QUERY,
            "fixture": "deterministic keys = rows / 4",
            "transport": "local_in_process",
        })
    );
    for denied in denied {
        let fixture = Fixture::new(readable, denied);
        let scenario = |reader: &str| json!({ "denied_rows": denied, "reader": reader });
        measure(&fixture, scenario("restricted"), &reader);
        scoped_work(&fixture, denied);
        if denied == 0 {
            measure(&fixture, scenario("allow_all"), &AllowAllAuthorizer);
        }
    }
}
