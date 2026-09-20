//! Compares ordinary SELECT plans across asymmetric, skewed, and scoped joins.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::env;
use std::hint::black_box;
use std::time::Instant;

#[path = "../support.rs"]
mod support;

use craqle::{
    ActorId, AllowAllAuthorizer, CraqleErrorKind, CraqleNode, CraqleOptions, EncodedTerm, GraphId,
    JoinKind, JoinMode, MaterializedQuadChange, PreparedQuery, QueryExecution,
    QueryFastPathMode as FastPathMode, QueryLimits, QueryOptions, QueryResults,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use support::BenchWriteExt as _;
use support::fixture::{binary_blake3, env_duration, repository_commit};

const DEFAULT_BATCH: usize = 1_000;
const DEFAULT_CAP: usize = 100_000;
const FIXTURE_SEED: u64 = 0x504c_414e_5155_414c;

#[derive(Clone, Copy, Debug)]
enum Distribution {
    Asymmetric,
    Skewed,
}

#[derive(Clone, Copy, Debug)]
enum Scope {
    Primary,
    Both,
}

#[derive(Clone, Copy)]
struct CaseSpec {
    distribution: Distribution,
    scope: Scope,
    left_rows: usize,
    right_rows: usize,
    keys: usize,
    hot_left: usize,
    hot_right: usize,
    limit: Option<usize>,
    result_cap: usize,
}

struct InsertSpec<'a> {
    graph: &'a GraphId,
    subject: &'a EncodedTerm,
    side: &'a str,
    key: usize,
}

struct Fixture {
    node: CraqleNode,
    _database: tempfile::TempDir,
    graphs: Vec<GraphId>,
    query: PreparedQuery,
    options: QueryOptions,
    expected: Vec<(String, String, String)>,
    digest: String,
    label: String,
    spec: CaseSpec,
}

struct ColdSample {
    execution: QueryExecution,
    wall_ns: u64,
    result_digest: String,
}

impl Fixture {
    fn new(spec: CaseSpec, mode: JoinMode) -> Self {
        assert!(spec.left_rows > 0 && spec.right_rows > 0 && spec.keys > 0);
        let database = tempfile::tempdir().expect("create plan-quality database");
        let node = CraqleNode::open_with_options(
            database.path(),
            CraqleOptions::new().with_actor(ActorId::from_bytes([0x50; 32])),
        )
        .expect("open plan-quality node");
        let primary = GraphId::new("urn:craqle:bench:plan-quality:primary");
        let secondary = GraphId::new("urn:craqle:bench:plan-quality:secondary");
        let mut primary_changes = Vec::new();
        let mut secondary_changes = Vec::new();
        let mut left_buckets = vec![Vec::new(); spec.keys];
        let mut right_primary = vec![Vec::new(); spec.keys];
        let mut right_all = vec![Vec::new(); spec.keys];
        for index in 0..spec.left_rows {
            let key = row_key(spec, true, index);
            let subject = iri(format!("urn:craqle:bench:plan-quality:left:{index:010}"));
            primary_changes.push(insert(InsertSpec {
                graph: &primary,
                subject: &subject,
                side: "left",
                key,
            }));
            left_buckets[key].push(subject.0);
        }
        for index in 0..spec.right_rows {
            let key = row_key(spec, false, index);
            let subject = iri(format!("urn:craqle:bench:plan-quality:right:{index:010}"));
            let graph = if index % 2 == 0 {
                right_primary[key].push(subject.0.clone());
                &primary
            } else {
                &secondary
            };
            right_all[key].push(subject.0.clone());
            let change = insert(InsertSpec {
                graph,
                subject: &subject,
                side: "right",
                key,
            });
            if graph == &primary {
                primary_changes.push(change);
            } else {
                secondary_changes.push(change);
            }
        }
        let duplicate = iri("urn:craqle:bench:plan-quality:left:0000000000");
        secondary_changes.push(insert(InsertSpec {
            graph: &secondary,
            subject: &duplicate,
            side: "left",
            key: row_key(spec, true, 0),
        }));
        apply_batches(&node, &primary, primary_changes);
        apply_batches(&node, &secondary, secondary_changes);
        node.persist_fjall().expect("persist plan-quality fixture");
        let right_buckets = match spec.scope {
            Scope::Primary => &right_primary,
            Scope::Both => &right_all,
        };
        let expected_size = bag_size(&left_buckets, right_buckets);
        assert!(expected_size <= spec.result_cap);
        let mut expected = expected_rows(&left_buckets, right_buckets, expected_size);
        assert_eq!(expected.len(), expected_size);
        expected.sort();
        let graphs = match spec.scope {
            Scope::Primary => vec![primary.clone()],
            Scope::Both => vec![primary.clone(), secondary.clone()],
        };
        let suffix = spec
            .limit
            .map_or_else(String::new, |limit| format!(" LIMIT {limit}"));
        let query = node
            .prepare_query(&format!(
                "SELECT ?left ?right ?key WHERE {{ \
                 ?left <urn:craqle:bench:plan-quality:left> ?key . \
                 ?right <urn:craqle:bench:plan-quality:right> ?key }}{suffix}"
            ))
            .expect("prepare plan-quality query");
        let mut limits = QueryLimits::production();
        limits.max_result_rows = spec.result_cap;
        limits.max_result_cells = spec.result_cap.saturating_mul(3);
        let mut options = QueryOptions::default();
        options.join_mode = mode;
        options.fast_paths = FastPathMode::Disabled;
        options.collect_costs = false;
        options.collect_plan_statistics = false;
        options.limits = limits;
        let label = format!(
            "{:?}_{:?}_{}",
            spec.distribution,
            spec.scope,
            spec.limit
                .map_or("full".to_owned(), |limit| format!("limit_{limit}"))
        )
        .to_lowercase();
        let digest = fixture_digest(spec);
        Self {
            node,
            _database: database,
            graphs,
            query,
            options,
            expected,
            digest,
            label,
            spec,
        }
    }

    fn execute(&self) -> craqle::Result<QueryExecution> {
        self.node.execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            &self.graphs,
            &self.query,
            &self.options,
        )
    }

    fn cold_sample(&self) -> ColdSample {
        let started = Instant::now();
        let execution = self.execute().unwrap_or_else(|error| {
            print_error(self, &error.to_string());
            panic!("plan-quality execution failed: {error}");
        });
        let wall_ns = nanos(started.elapsed());
        let result_digest = validate_results(&execution.results, &self.expected, self.spec.limit);
        assert!(!execution.statistics.intermediate_rows_available);
        assert_eq!(execution.statistics.fast_path, None);
        ColdSample {
            execution,
            wall_ns,
            result_digest,
        }
    }

    fn validate_limit(&self) {
        if self.expected.is_empty() || self.spec.limit.is_some() {
            return;
        }
        let mut options = self.options.clone();
        options.limits.max_result_rows = self.expected.len() - 1;
        let error = self
            .node
            .execute_prepared_in_graphs(&AllowAllAuthorizer, &self.graphs, &self.query, &options)
            .expect_err("result limit below the complete bag must reject");
        assert_eq!(error.kind(), CraqleErrorKind::QueryLimit);
    }
}

fn iri(value: impl Into<String>) -> EncodedTerm {
    EncodedTerm(format!("<{}>", value.into()))
}

fn row_key(spec: CaseSpec, left: bool, index: usize) -> usize {
    match spec.distribution {
        Distribution::Asymmetric => index % spec.keys,
        Distribution::Skewed => {
            let hot = if left { spec.hot_left } else { spec.hot_right };
            if index < hot {
                0
            } else {
                1 + (index - hot) % spec.keys.saturating_sub(1).max(1)
            }
        }
    }
}

fn insert(spec: InsertSpec<'_>) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: spec.graph.clone(),
        subject: spec.subject.clone(),
        predicate: iri(format!("urn:craqle:bench:plan-quality:{}", spec.side)),
        object: iri(format!(
            "urn:craqle:bench:plan-quality:key:{:010}",
            spec.key
        )),
    }
}

fn apply_batches(node: &CraqleNode, graph: &GraphId, changes: Vec<MaterializedQuadChange>) {
    let batch = env_usize("CRAQLE_PLAN_QUALITY_BATCH", DEFAULT_BATCH).max(1);
    for chunk in changes.chunks(batch) {
        node.apply_bulk_unchecked(graph, chunk.to_vec())
            .expect("load plan-quality fixture");
    }
}

fn expected_rows(
    left: &[Vec<String>],
    right: &[Vec<String>],
    expected_size: usize,
) -> Vec<(String, String, String)> {
    let mut rows = Vec::with_capacity(expected_size);
    for (key, (left, right)) in left.iter().zip(right).enumerate() {
        let key = format!("<urn:craqle:bench:plan-quality:key:{key:010}>");
        for left in left {
            for right in right {
                rows.push((left.clone(), right.clone(), key.clone()));
            }
        }
    }
    rows
}

fn bag_size(left: &[Vec<String>], right: &[Vec<String>]) -> usize {
    left.iter().zip(right).fold(0usize, |total, (left, right)| {
        let bucket = left
            .len()
            .checked_mul(right.len())
            .expect("plan-quality bucket cardinality overflow");
        total
            .checked_add(bucket)
            .expect("plan-quality result cardinality overflow")
    })
}

fn validate_results(
    results: &QueryResults,
    expected: &[(String, String, String)],
    limit: Option<usize>,
) -> String {
    let QueryResults::Solutions(rows) = results else {
        panic!("plan-quality SELECT must return solutions");
    };
    let mut actual: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row.get("left").expect("left binding").0.clone(),
                row.get("right").expect("right binding").0.clone(),
                row.get("key").expect("key binding").0.clone(),
            )
        })
        .collect();
    actual.sort();
    if let Some(limit) = limit {
        assert_eq!(actual.len(), limit.min(expected.len()));
        let mut remaining = HashMap::new();
        for row in expected {
            *remaining.entry(row.clone()).or_insert(0usize) += 1;
        }
        for row in &actual {
            let count = remaining
                .get_mut(row)
                .expect("limited row must belong to the complete result bag");
            assert!(
                *count > 0,
                "limited row multiplicity exceeds the complete bag"
            );
            *count -= 1;
        }
    } else {
        assert_eq!(actual, expected);
    }
    let bytes = serde_json::to_vec(&actual).expect("serialize plan-quality bag");
    blake3::hash(&bytes).to_hex().to_string()
}

fn fixture_digest(spec: CaseSpec) -> String {
    blake3::hash(
        format!(
            "plan-quality-v1:{FIXTURE_SEED}:{:?}:{:?}:{}:{}:{}:{}:{}:{}:{}:{}",
            spec.distribution,
            spec.scope,
            spec.left_rows,
            spec.right_rows,
            spec.keys,
            spec.hot_left,
            spec.hot_right,
            spec.limit.map_or(0, |limit| limit as u64),
            spec.result_cap,
            env_usize("CRAQLE_PLAN_QUALITY_BATCH", DEFAULT_BATCH)
        )
        .as_bytes(),
    )
    .to_hex()
    .to_string()
}

fn print_sample(fixture: &Fixture, mode: JoinMode, sample: &ColdSample) {
    let stats = &sample.execution.statistics;
    let join = stats.planned_joins.first().expect("join plan");
    match mode {
        JoinMode::ForceHash => assert_eq!(join.physical_operator, JoinKind::Hash),
        JoinMode::ForceLateral => assert_eq!(join.physical_operator, JoinKind::IndexedLateral),
        JoinMode::Auto => {}
        JoinMode::ForcePropertyStar => unreachable!("property-star mode is not a join control"),
    }
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "plan_quality",
            "status": "ok",
            "case": &fixture.label,
            "mode": format!("{mode:?}"),
            "fixture": {
                "digest": &fixture.digest,
                "distribution": format!("{:?}", fixture.spec.distribution),
                "scope": format!("{:?}", fixture.spec.scope),
                "graph_count": fixture.graphs.len(),
                "left_rows": fixture.spec.left_rows,
                "right_rows": fixture.spec.right_rows,
                "keys": fixture.spec.keys,
                "hot_left": fixture.spec.hot_left,
                "hot_right": fixture.spec.hot_right,
                "limit": fixture.spec.limit,
                "result_cap": fixture.spec.result_cap,
            },
            "plan": {
                "fingerprint": &stats.plan_fingerprint,
                "joins": &stats.planned_joins,
                "fast_paths": "disabled",
                "plan_statistics": false,
            },
            "timing": {
                "wall_ns": sample.wall_ns,
                "planning_ns": nanos(stats.planning_time),
                "execution_ns": nanos(stats.execution_time),
                "collection_ns": nanos(stats.result_collection_time),
            },
            "work": {
                "index_seeks": stats.index_seeks,
                "candidate_quads": stats.candidate_quads,
                "matching_quads": stats.matching_quads,
                "result_rows": stats.result_rows,
                "result_cells": stats.result_cells,
                "intermediate_rows": null,
            },
            "result": {
                "multiset_blake3": &sample.result_digest,
                "rows": fixture
                    .spec
                    .limit
                    .unwrap_or(fixture.expected.len())
                    .min(fixture.expected.len()),
                "complete": fixture.spec.limit.is_none(),
                "limited_validity_verified": fixture.spec.limit.is_some(),
                "limit_rejection_verified": fixture.spec.limit.is_none(),
            },
        }))
        .expect("serialize plan-quality sample")
    );
}

fn print_error(fixture: &Fixture, error: &str) {
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "plan_quality",
            "status": "error",
            "case": &fixture.label,
            "fixture_digest": &fixture.digest,
            "error": error,
            "counted_as_timing": false,
        }))
        .expect("serialize plan-quality error")
    );
}

fn nanos(value: std::time::Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("invalid {name}")))
        .unwrap_or(default)
}
