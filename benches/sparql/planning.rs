//! Measures planner statistics work across unrelated and relevant writes.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::env;
use std::time::{Duration, Instant};

#[path = "../support.rs"]
mod support;

use craqle::{
    ActorId, AllowAllAuthorizer, CraqleNode, CraqleOptions, EncodedTerm, GraphId, JoinKind,
    JoinMode, MaterializedQuadChange, PlannedJoin, PreparedQuery, QueryExecutionStatistics,
    QueryFastPathMode as FastPathMode, QueryLimits, QueryOptions, QueryResults,
};
use serde_json::json;
use support::BenchWriteExt as _;
use support::fixture::{binary_blake3, repository_commit};

const DEFAULT_BATCH: usize = 1_000;
const DEFAULT_CAP: usize = 4_096;
const FIXTURE_SEED: u64 = 0x504c_414e_5354_4154;
#[derive(Clone, Copy)]
struct ScenarioSpec {
    relevant: usize,
    unrelated: usize,
    result_cap: usize,
}
#[derive(Clone, Copy)]
struct PhaseRun {
    label: &'static str,
    mutation_kind: &'static str,
    source_case: &'static str,
    mutation_ns: u64,
    expected_cache: bool,
}
struct InsertSpec<'a> {
    graph: &'a GraphId,
    subject: EncodedTerm,
    predicate: &'a str,
    object: EncodedTerm,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct PlanIdentity {
    fingerprint: String,
    joins: Vec<PlannedJoin>,
}
struct PhaseSample {
    phase: PhaseRun,
    wall_ns: u64,
    digest: String,
    rows: usize,
    plan: PlanIdentity,
    stats: QueryExecutionStatistics,
}
struct Fixture {
    node: CraqleNode,
    _database: tempfile::TempDir,
    graph: GraphId,
    other_graph: GraphId,
    query: PreparedQuery,
    options: QueryOptions,
    expected: Vec<(String, String, String)>,
    spec: ScenarioSpec,
    digest: String,
}
impl Fixture {
    fn new(spec: ScenarioSpec) -> Self {
        assert!(spec.relevant > 0);
        assert!(spec.relevant <= spec.result_cap);
        let database = tempfile::tempdir().expect("create planning database");
        let node = deterministic_node(database.path());
        let graph = GraphId::new("urn:craqle:bench:planning:main");
        let other_graph = GraphId::new("urn:craqle:bench:planning:other");
        let mut changes = Vec::with_capacity(
            spec.relevant
                .saturating_mul(2)
                .saturating_add(spec.unrelated),
        );
        let mut expected = Vec::with_capacity(spec.relevant);
        for index in 0..spec.relevant {
            let subject = iri(format!("urn:craqle:bench:planning:subject:{index:010}"));
            let left = iri(format!("urn:craqle:bench:planning:left:{index:010}"));
            let right = iri(format!("urn:craqle:bench:planning:right:{index:010}"));
            changes.push(insert(InsertSpec {
                graph: &graph,
                subject: subject.clone(),
                predicate: "urn:craqle:bench:planning:left",
                object: left.clone(),
            }));
            changes.push(insert(InsertSpec {
                graph: &graph,
                subject: subject.clone(),
                predicate: "urn:craqle:bench:planning:right",
                object: right.clone(),
            }));
            expected.push((subject.0, left.0, right.0));
        }
        for index in 0..spec.unrelated {
            changes.push(insert(InsertSpec {
                graph: &graph,
                subject: iri(format!("urn:craqle:bench:planning:noise:{index:010}")),
                predicate: "urn:craqle:bench:planning:noise",
                object: iri(format!("urn:craqle:bench:planning:noise-value:{index:010}")),
            }));
        }
        apply_batches(&node, &graph, changes);
        node.persist_fjall().expect("persist planning fixture");
        expected.sort();
        let query = node
            .prepare_query(
                "SELECT ?subject ?left ?right WHERE { \
                 ?subject <urn:craqle:bench:planning:left> ?left . \
                 ?subject <urn:craqle:bench:planning:right> ?right }",
            )
            .expect("prepare planning query");
        let mut limits = QueryLimits::production();
        limits.max_result_rows = spec.result_cap;
        limits.max_result_cells = spec.result_cap.saturating_mul(3);
        let mut options = QueryOptions::default();
        options.join_mode = JoinMode::ForceHash;
        options.fast_paths = FastPathMode::Disabled;
        options.collect_costs = true;
        options.limits = limits;
        let digest = blake3::hash(
            format!(
                "planning-v1:{FIXTURE_SEED}:{}:{}:{}",
                spec.relevant, spec.unrelated, spec.result_cap
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string();
        Self {
            node,
            _database: database,
            graph,
            other_graph,
            query,
            options,
            expected,
            spec,
            digest,
        }
    }

    fn execute(&self, phase: PhaseRun) -> PhaseSample {
        let started = Instant::now();
        let execution = self
            .node
            .execute_prepared_in_graphs(
                &AllowAllAuthorizer,
                std::slice::from_ref(&self.graph),
                &self.query,
                &self.options,
            )
            .unwrap_or_else(|error| {
                print_error(self, phase, &error.to_string());
                panic!("planning phase {} failed: {error}", phase.label);
            });
        let wall_ns = nanos(started.elapsed());
        let (digest, rows) = validate_results(&execution.results, &self.expected);
        let stats = execution.statistics;
        assert_eq!(rows, self.spec.relevant);
        assert_eq!(stats.planned_joins.len(), 1);
        assert_eq!(stats.planned_joins[0].physical_operator, JoinKind::Hash);
        let plan = PlanIdentity {
            fingerprint: stats.plan_fingerprint.clone(),
            joins: stats.planned_joins.clone(),
        };
        PhaseSample {
            phase,
            wall_ns,
            digest,
            rows,
            plan,
            stats,
        }
    }

    fn unrelated_write(&mut self) -> u64 {
        let change = insert(InsertSpec {
            graph: &self.graph,
            subject: iri("urn:craqle:bench:planning:phase-noise"),
            predicate: "urn:craqle:bench:planning:noise",
            object: iri("urn:craqle:bench:planning:phase-noise-value"),
        });
        measure_write(&self.node, &self.graph, change)
    }

    fn duplicate_write(&mut self) -> u64 {
        let change = insert(InsertSpec {
            graph: &self.graph,
            subject: iri("urn:craqle:bench:planning:subject:0000000000"),
            predicate: "urn:craqle:bench:planning:left",
            object: iri("urn:craqle:bench:planning:left:0000000000"),
        });
        measure_write(&self.node, &self.graph, change)
    }

    fn replace_left(&mut self) -> u64 {
        let subject = iri("urn:craqle:bench:planning:subject:0000000000");
        let old = iri("urn:craqle:bench:planning:left:0000000000");
        let new = iri("urn:craqle:bench:planning:left:replacement");
        let changes = vec![
            MaterializedQuadChange::Delete {
                graph: self.graph.clone(),
                subject: subject.clone(),
                predicate: iri("urn:craqle:bench:planning:left"),
                object: old.clone(),
            },
            insert(InsertSpec {
                graph: &self.graph,
                subject: subject.clone(),
                predicate: "urn:craqle:bench:planning:left",
                object: new.clone(),
            }),
        ];
        let elapsed = measure_changes(&self.node, &self.graph, changes);
        let row = self
            .expected
            .iter_mut()
            .find(|row| row.0 == subject.0 && row.1 == old.0)
            .expect("replacement source row exists");
        row.1 = new.0;
        self.expected.sort();
        elapsed
    }

    fn delete_noise(&mut self) -> u64 {
        let change = MaterializedQuadChange::Delete {
            graph: self.graph.clone(),
            subject: iri("urn:craqle:bench:planning:phase-noise"),
            predicate: iri("urn:craqle:bench:planning:noise"),
            object: iri("urn:craqle:bench:planning:phase-noise-value"),
        };
        measure_write(&self.node, &self.graph, change)
    }

    fn other_write(&mut self) -> u64 {
        let change = insert(InsertSpec {
            graph: &self.other_graph,
            subject: iri("urn:craqle:bench:planning:other-subject"),
            predicate: "urn:craqle:bench:planning:noise",
            object: iri("urn:craqle:bench:planning:other-value"),
        });
        measure_write(&self.node, &self.other_graph, change)
    }
}

fn deterministic_node(path: &std::path::Path) -> CraqleNode {
    CraqleNode::open_with_options(
        path,
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x50; 32])),
    )
    .expect("open deterministic planning node")
}

fn iri(value: impl Into<String>) -> EncodedTerm {
    EncodedTerm(format!("<{}>", value.into()))
}

fn insert(spec: InsertSpec<'_>) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: spec.graph.clone(),
        subject: spec.subject,
        predicate: iri(spec.predicate),
        object: spec.object,
    }
}

fn apply_batches(node: &CraqleNode, graph: &GraphId, changes: Vec<MaterializedQuadChange>) {
    let batch = env_usize("CRAQLE_PLAN_LOAD_BATCH", DEFAULT_BATCH).max(1);
    for chunk in changes.chunks(batch) {
        node.apply_bulk_unchecked(graph, chunk.to_vec())
            .expect("load planning fixture");
    }
}

fn measure_write(node: &CraqleNode, graph: &GraphId, change: MaterializedQuadChange) -> u64 {
    measure_changes(node, graph, vec![change])
}

fn measure_changes(
    node: &CraqleNode,
    graph: &GraphId,
    changes: Vec<MaterializedQuadChange>,
) -> u64 {
    let started = Instant::now();
    node.apply_bulk_unchecked(graph, changes)
        .expect("apply planning phase write");
    nanos(started.elapsed())
}

fn validate_results(
    results: &QueryResults,
    expected: &[(String, String, String)],
) -> (String, usize) {
    let QueryResults::Solutions(rows) = results else {
        panic!("planning query must return solutions");
    };
    let mut actual: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row.get("subject").expect("subject binding").0.clone(),
                row.get("left").expect("left binding").0.clone(),
                row.get("right").expect("right binding").0.clone(),
            )
        })
        .collect();
    actual.sort();
    assert_eq!(
        actual, expected,
        "planning phase changed the complete result bag"
    );
    let bytes = serde_json::to_vec(&actual).expect("serialize planning results");
    (blake3::hash(&bytes).to_hex().to_string(), rows.len())
}

fn run_phase(
    fixture: &Fixture,
    phase: PhaseRun,
    baseline: &mut Option<PlanIdentity>,
) -> PhaseSample {
    let sample = fixture.execute(phase);
    if let Some(expected) = baseline {
        assert_eq!(
            &sample.plan, expected,
            "phase {} changed the controlled physical plan",
            phase.label
        );
    } else {
        *baseline = Some(sample.plan.clone());
    }
    sample
}

fn print_sample(fixture: &Fixture, sample: &PhaseSample) {
    let stats = &sample.stats;
    let estimate_source = if stats.planner_index_entries == 0 && stats.planner_cache_hits > 0 {
        "warmed_ndv_cache"
    } else {
        "index_scan_or_fallback"
    };
    if sample.phase.expected_cache {
        assert_eq!(stats.planner_index_entries, 0);
        assert!(stats.planner_cache_hits > 0);
    }
    let physical_joins: Vec<_> = sample
        .plan
        .joins
        .iter()
        .map(|join| join.physical_operator)
        .collect();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "planning_cost",
            "status": "ok",
            "phase": sample.phase.label,
            "phase_type": sample.phase.mutation_kind,
            "source_case": sample.phase.source_case,
            "fixture": {
                "digest": &fixture.digest,
                "relevant_rows": fixture.spec.relevant,
                "unrelated_rows": fixture.spec.unrelated,
                "result_cap": fixture.spec.result_cap,
                "actor_hex": "50".repeat(32),
                "seed": FIXTURE_SEED,
                "sync": null,
            },
            "plan": {
                "fingerprint": &sample.plan.fingerprint,
                "physical_joins": physical_joins,
                "joins": &sample.plan.joins,
                "fast_paths": "disabled",
                "forced_join": "hash",
                "estimate_source": estimate_source,
                "same_plan_verified": true,
            },
            "timing": {
                "wall_ns": sample.wall_ns,
                "mutation_ns": sample.phase.mutation_ns,
                "parse_ns": nanos(stats.parse_time),
                "rewrite_ns": nanos(stats.rewrite_time),
                "planning_ns": nanos(stats.planning_time),
                "execution_ns": nanos(stats.execution_time),
                "collection_ns": nanos(stats.result_collection_time),
            },
            "work": {
                "planner_index_entries": stats.planner_index_entries,
                "planner_point_reads": stats.planner_point_reads,
                "planner_cache_hits": stats.planner_cache_hits,
                "planner_cache_misses": stats.planner_cache_misses,
                "forward_mapping_reads": stats.forward_mapping_reads,
                "forward_mapping_bytes": stats.forward_mapping_bytes,
                "reverse_mapping_reads": stats.reverse_mapping_reads,
                "reverse_mapping_bytes": stats.reverse_mapping_bytes,
            },
            "result": {
                "multiset_blake3": &sample.digest,
                "rows": sample.rows,
                "complete": true,
            },
        }))
        .expect("serialize planning sample")
    );
}

fn print_error(fixture: &Fixture, phase: PhaseRun, error: &str) {
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "planning_cost",
            "status": "error",
            "phase": phase.label,
            "phase_type": phase.mutation_kind,
            "source_case": phase.source_case,
            "fixture_digest": &fixture.digest,
            "error": error,
            "counted_as_timing": false,
        }))
        .expect("serialize planning error")
    );
}

fn nanos(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(default)
}

fn env_list(name: &str, defaults: &[usize]) -> Vec<usize> {
    match env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|item| {
                item.trim()
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("{name} must contain comma-separated integers"))
            })
            .collect(),
        Err(env::VarError::NotPresent) => defaults.to_vec(),
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn run_scenario(spec: ScenarioSpec) {
    let mut fixture = Fixture::new(spec);
    let mut baseline = None;
    let phases = [
        PhaseRun {
            label: "first_cold_stats",
            mutation_kind: "none",
            source_case: "cold_fixture",
            mutation_ns: 0,
            expected_cache: false,
        },
        PhaseRun {
            label: "warm_storage",
            mutation_kind: "none",
            source_case: "warmed_fixture",
            mutation_ns: 0,
            expected_cache: true,
        },
        PhaseRun {
            label: "repeat_warm_stats",
            mutation_kind: "none",
            source_case: "warmed_stats",
            mutation_ns: 0,
            expected_cache: true,
        },
    ];
    for phase in phases {
        let sample = run_phase(&fixture, phase, &mut baseline);
        print_sample(&fixture, &sample);
    }
    let mut live_rows = fixture
        .node
        .query_index_status_fast()
        .unwrap()
        .source_live_quads;
    for (label, mutation_kind, source_case, mutate, delta) in [
        (
            "after_unrelated_write",
            "insert",
            "unrelated_predicate",
            Fixture::unrelated_write as fn(&mut Fixture) -> u64,
            1_i64,
        ),
        (
            "after_relevant_duplicate",
            "duplicate_insert",
            "queried_predicate_existing_fact",
            Fixture::duplicate_write,
            0,
        ),
        (
            "after_relevant_replacement",
            "replace",
            "queried_predicate_object",
            Fixture::replace_left,
            0,
        ),
        (
            "after_delete",
            "delete",
            "unrelated_predicate",
            Fixture::delete_noise,
            -1,
        ),
        (
            "after_other_graph_write",
            "insert",
            "other_graph_unrelated_predicate",
            Fixture::other_write,
            1,
        ),
    ] {
        let mutation_ns = mutate(&mut fixture);
        live_rows = live_rows
            .checked_add_signed(delta)
            .expect("valid fixture delta");
        assert_eq!(
            fixture
                .node
                .query_index_status_fast()
                .unwrap()
                .source_live_quads,
            live_rows,
            "mutation phase {label} did not change the intended facts"
        );
        let phase = PhaseRun {
            label,
            mutation_kind,
            source_case,
            mutation_ns,
            expected_cache: false,
        };
        let sample = run_phase(&fixture, phase, &mut baseline);
        print_sample(&fixture, &sample);
    }
}

fn main() {
    let relevant = env_list("CRAQLE_PLAN_RELEVANT", &[512, 1_024]);
    let unrelated = env_list("CRAQLE_PLAN_UNRELATED", &[0, 1_000, 10_000]);
    let result_cap = env_usize("CRAQLE_PLAN_RESULT_CAP", DEFAULT_CAP);
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "planning_cost_provenance",
            "commit": repository_commit(),
            "binary_blake3": binary_blake3(),
            "relevant_rows": &relevant,
            "unrelated_rows": &unrelated,
            "result_cap": result_cap,
            "load_batch": env_usize("CRAQLE_PLAN_LOAD_BATCH", DEFAULT_BATCH),
            "seed": FIXTURE_SEED,
            "transport": "local_in_process",
            "sync": null,
            "statistics": "request_local_enabled",
            "limits": "production_with_configured_result_cap",
        }))
        .expect("serialize planning provenance")
    );
    for relevant in relevant {
        for &unrelated in &unrelated {
            run_scenario(ScenarioSpec {
                relevant,
                unrelated,
                result_cap,
            });
        }
    }
}
