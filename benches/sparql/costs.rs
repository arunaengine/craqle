//! Separates native query execution from general join algorithm costs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::env;
use std::hint::black_box;
use std::time::Instant;

#[cfg(feature = "allocation-metrics")]
#[path = "../allocation.rs"]
mod allocation;

#[path = "../support.rs"]
mod support;

use craqle::{
    ActorId, AllowAllAuthorizer, CraqleNode, CraqleOptions, EncodedTerm, GraphId, JoinKind,
    JoinMode, MaterializedQuadChange, PreparedQuery, QueryExecution,
    QueryFastPathKind as FastPathKind, QueryFastPathMode as FastPathMode, QueryLimits,
    QueryOptions, QueryResults,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use support::BenchWriteExt as _;
use support::fixture::{binary_blake3, env_duration, repository_commit};

const DEFAULT_ROWS: usize = 1_000;
const DEFAULT_KEYS: usize = 10;
const DEFAULT_CAP: usize = 100_000;
const DEFAULT_BATCH: usize = 1_000;
const STAR_PROPERTIES: usize = 4;
const FIXTURE_SEED: u64 = 0x5155_4552_5943_4f53;

#[derive(Clone, Copy, Debug)]
enum Arm {
    NativeHashCount,
    GenericHashCount,
    GenericLateralCount,
    SelectHash,
    SelectLateral,
    NativeStar,
    GenericStar,
}

#[derive(Clone, Copy)]
struct JoinSpec {
    rows: usize,
    keys: usize,
    result_cap: usize,
}

#[derive(Clone, Copy)]
struct StarSpec {
    multiplicity: usize,
    limit: Option<usize>,
    result_cap: usize,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Self::NativeHashCount => "native_hash_count",
            Self::GenericHashCount => "generic_hash_count",
            Self::GenericLateralCount => "generic_lateral_count",
            Self::SelectHash => "generic_hash_select",
            Self::SelectLateral => "generic_lateral_select",
            Self::NativeStar => "native_property_star",
            Self::GenericStar => "generic_star",
        }
    }

    fn options(self, collect_costs: bool) -> QueryOptions {
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        options.collect_costs = collect_costs;
        options.collect_plan_statistics = env_bool("CRAQLE_COST_PLAN_STATS", true);
        match self {
            Self::NativeHashCount => {
                options.join_mode = JoinMode::ForceHash;
                options.fast_paths = FastPathMode::Auto;
            }
            Self::GenericHashCount | Self::SelectHash => {
                options.join_mode = JoinMode::ForceHash;
                options.fast_paths = FastPathMode::Disabled;
            }
            Self::GenericLateralCount | Self::SelectLateral => {
                options.join_mode = JoinMode::ForceLateral;
                options.fast_paths = FastPathMode::Disabled;
            }
            Self::GenericStar => {
                options.join_mode = JoinMode::Auto;
                options.fast_paths = FastPathMode::Disabled;
            }
            Self::NativeStar => {
                options.join_mode = JoinMode::ForcePropertyStar;
                options.fast_paths = FastPathMode::Auto;
            }
        }
        options
    }
}

struct Fixture {
    node: CraqleNode,
    _database: tempfile::TempDir,
    graph: GraphId,
    query: PreparedQuery,
    options: QueryOptions,
    arm: Arm,
    case: String,
    fixture_digest: String,
    expected_rows: usize,
    input_rows: usize,
    distinct_keys: usize,
    star_multiplicity: Option<usize>,
    join_matches: Option<usize>,
    limited: bool,
    collect_costs: bool,
}

impl Fixture {
    fn join(spec: &JoinSpec, arm: Arm, collect_costs: bool) -> Self {
        let JoinSpec {
            rows,
            keys: distinct_keys,
            result_cap,
        } = *spec;
        assert!(rows > 0 && distinct_keys > 0 && distinct_keys <= rows);
        let join_matches = join_cardinality(rows, distinct_keys);
        let database = tempfile::tempdir().expect("create query-cost database");
        let node = deterministic_node(database.path());
        let graph = GraphId::new("urn:craqle:bench:query-cost:join");
        let batch = env_usize("CRAQLE_COST_LOAD_BATCH", DEFAULT_BATCH).max(1);
        for start in (0..rows).step_by(batch) {
            let end = start.saturating_add(batch).min(rows);
            let mut changes = Vec::with_capacity((end - start) * 2);
            for index in start..end {
                let key = iri(format!(
                    "urn:craqle:bench:query-cost:key:{:010}",
                    index % distinct_keys
                ));
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: iri(format!("urn:craqle:bench:query-cost:left:{index:010}")),
                    predicate: iri("urn:craqle:bench:query-cost:left"),
                    object: key.clone(),
                });
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: iri(format!("urn:craqle:bench:query-cost:right:{index:010}")),
                    predicate: iri("urn:craqle:bench:query-cost:right"),
                    object: key,
                });
            }
            node.apply_bulk_unchecked(&graph, changes)
                .expect("load query-cost join fixture");
        }
        node.persist_fjall().expect("persist query-cost fixture");
        let select_limit = (join_matches > result_cap).then_some(result_cap);
        let sparql = match arm {
            Arm::NativeHashCount | Arm::GenericHashCount | Arm::GenericLateralCount => {
                "SELECT (COUNT(*) AS ?count) WHERE { \
                 ?left <urn:craqle:bench:query-cost:left> ?key . \
                 ?right <urn:craqle:bench:query-cost:right> ?key }"
                    .to_owned()
            }
            Arm::SelectHash | Arm::SelectLateral => format!(
                "SELECT ?left ?right ?key WHERE {{ \
                 ?left <urn:craqle:bench:query-cost:left> ?key . \
                 ?right <urn:craqle:bench:query-cost:right> ?key }}{}",
                select_limit.map_or_else(String::new, |limit| format!(" LIMIT {limit}"))
            ),
            Arm::NativeStar | Arm::GenericStar => unreachable!("star uses a separate fixture"),
        };
        let query = node
            .prepare_query(&sparql)
            .expect("prepare query-cost join");
        let expected_rows = match arm {
            Arm::NativeHashCount | Arm::GenericHashCount | Arm::GenericLateralCount => 1,
            Arm::SelectHash | Arm::SelectLateral => select_limit.unwrap_or(join_matches),
            Arm::NativeStar | Arm::GenericStar => unreachable!("star uses a separate fixture"),
        };
        let query_kind = match arm {
            Arm::NativeHashCount | Arm::GenericHashCount | Arm::GenericLateralCount => "count",
            Arm::SelectHash | Arm::SelectLateral => "select",
            Arm::NativeStar | Arm::GenericStar => unreachable!("star uses a separate fixture"),
        };
        Self {
            node,
            _database: database,
            graph,
            query,
            options: arm.options(collect_costs),
            arm,
            case: format!("join_{query_kind}_rows_{rows}_keys_{distinct_keys}"),
            fixture_digest: blake3::hash(
                format!("query-cost-join-v1:{FIXTURE_SEED}:{rows}:{distinct_keys}").as_bytes(),
            )
            .to_hex()
            .to_string(),
            expected_rows,
            input_rows: rows,
            distinct_keys,
            star_multiplicity: None,
            join_matches: Some(join_matches),
            limited: matches!(arm, Arm::SelectHash | Arm::SelectLateral) && select_limit.is_some(),
            collect_costs,
        }
    }

    fn star(spec: &StarSpec, arm: Arm, collect_costs: bool) -> Self {
        let StarSpec {
            multiplicity,
            limit,
            result_cap,
        } = *spec;
        assert!(multiplicity > 0);
        assert!(matches!(arm, Arm::NativeStar | Arm::GenericStar));
        let full_rows = multiplicity.pow(STAR_PROPERTIES as u32);
        let expected_rows = limit.map_or(full_rows, |limit| limit.min(full_rows));
        assert!(
            expected_rows <= result_cap,
            "property-star output exceeds CRAQLE_COST_RESULT_CAP"
        );
        let database = tempfile::tempdir().expect("create property-star database");
        let node = deterministic_node(database.path());
        let graph = GraphId::new("urn:craqle:bench:query-cost:star");
        let subject = iri("urn:craqle:bench:query-cost:star:subject");
        let mut changes = Vec::with_capacity(STAR_PROPERTIES * multiplicity);
        for property in 0..STAR_PROPERTIES {
            for value in 0..multiplicity {
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: subject.clone(),
                    predicate: iri(format!("urn:craqle:bench:query-cost:star:p{property}")),
                    object: iri(format!(
                        "urn:craqle:bench:query-cost:star:p{property}:v{value}"
                    )),
                });
            }
        }
        node.apply_bulk_unchecked(&graph, changes)
            .expect("load property-star fixture");
        node.persist_fjall().expect("persist property-star fixture");
        let suffix = limit.map_or_else(String::new, |limit| format!(" LIMIT {limit}"));
        let sparql = format!(
            "SELECT ?v0 ?v1 ?v2 ?v3 WHERE {{ \
             {} <urn:craqle:bench:query-cost:star:p0> ?v0 ; \
                <urn:craqle:bench:query-cost:star:p1> ?v1 ; \
                <urn:craqle:bench:query-cost:star:p2> ?v2 ; \
                <urn:craqle:bench:query-cost:star:p3> ?v3 \
             }}{suffix}",
            subject.0,
        );
        let query = node
            .prepare_query(&sparql)
            .expect("prepare property-star query");
        Self {
            node,
            _database: database,
            graph,
            query,
            options: arm.options(collect_costs),
            arm,
            case: format!(
                "property_star_m{multiplicity}_{}",
                limit.map_or("full".to_owned(), |value| format!("limit_{value}"))
            ),
            fixture_digest: blake3::hash(
                format!("query-cost-star-v1:{FIXTURE_SEED}:{STAR_PROPERTIES}:{multiplicity}")
                    .as_bytes(),
            )
            .to_hex()
            .to_string(),
            expected_rows,
            input_rows: STAR_PROPERTIES * multiplicity,
            distinct_keys: 1,
            star_multiplicity: Some(multiplicity),
            join_matches: None,
            limited: limit.is_some(),
            collect_costs,
        }
    }

    fn execute(&self) -> craqle::Result<QueryExecution> {
        self.node.execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&self.graph),
            &self.query,
            &self.options,
        )
    }
}

struct ColdSample {
    execution: QueryExecution,
    wall_ns: u64,
    digest: String,
    rows: usize,
    cells: usize,
    allocation: Option<AllocationData>,
}

#[derive(Clone, Copy, serde::Serialize)]
struct AllocationData {
    allocations: u64,
    allocated_bytes: u64,
    peak_delta_bytes: usize,
}

fn deterministic_node(path: &std::path::Path) -> CraqleNode {
    CraqleNode::open_with_options(
        path,
        CraqleOptions::new().with_actor(ActorId::from_bytes([0x51; 32])),
    )
    .expect("open deterministic query-cost node")
}

fn iri(value: impl Into<String>) -> EncodedTerm {
    EncodedTerm(format!("<{}>", value.into()))
}

fn join_cardinality(rows: usize, keys: usize) -> usize {
    let short = rows / keys;
    let long_buckets = rows % keys;
    let short_buckets = keys - long_buckets;
    long_buckets
        .checked_mul((short + 1).checked_pow(2).expect("long bucket square"))
        .and_then(|long| {
            short_buckets
                .checked_mul(short.checked_pow(2).expect("short bucket square"))
                .and_then(|short| long.checked_add(short))
        })
        .expect("join cardinality fits usize")
}

fn env_usize(name: &str, default: usize) -> usize {
    let value = env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        })
        .unwrap_or(default);
    assert!(value > 0, "{name} must be positive");
    value
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name).as_deref() {
        Ok("1" | "true" | "on") => true,
        Ok("0" | "false" | "off") => false,
        Ok(_) => panic!("{name} must be true or false"),
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn env_list(name: &str, defaults: &[usize]) -> Vec<usize> {
    match env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|item| {
                item.trim()
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("{name} must be comma-separated integers"))
            })
            .collect(),
        Err(env::VarError::NotPresent) => defaults.to_vec(),
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn nanos(value: std::time::Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn result_shape(results: &QueryResults) -> (String, usize, usize) {
    match results {
        QueryResults::Solutions(rows) => {
            let mut canonical: Vec<Vec<(String, String)>> = rows
                .iter()
                .map(|row| {
                    let mut values: Vec<_> = row
                        .iter()
                        .map(|(name, value)| (name.clone(), value.0.clone()))
                        .collect();
                    values.sort();
                    values
                })
                .collect();
            canonical.sort();
            let bytes = serde_json::to_vec(&canonical).expect("serialize canonical results");
            let cells = canonical.iter().map(Vec::len).sum();
            (blake3::hash(&bytes).to_hex().to_string(), rows.len(), cells)
        }
        QueryResults::Boolean(value) => {
            let bytes = [u8::from(*value)];
            (blake3::hash(&bytes).to_hex().to_string(), 1, 1)
        }
        QueryResults::Graph(quads) => {
            let bytes = serde_json::to_vec(quads).expect("serialize canonical graph results");
            (
                blake3::hash(&bytes).to_hex().to_string(),
                quads.len(),
                quads.len() * 3,
            )
        }
    }
}

fn cold_sample(fixture: &Fixture) -> ColdSample {
    #[cfg(feature = "allocation-metrics")]
    let allocation = allocation::AllocationInterval::begin();
    let started = Instant::now();
    let result = fixture.execute();
    let wall_ns = nanos(started.elapsed());
    #[cfg(feature = "allocation-metrics")]
    let allocation = {
        let sample = allocation.finish();
        Some(AllocationData {
            allocations: sample.allocations,
            allocated_bytes: sample.allocated_bytes,
            peak_delta_bytes: sample.peak_delta_bytes,
        })
    };
    #[cfg(not(feature = "allocation-metrics"))]
    let allocation = None;
    let execution = match result {
        Ok(execution) => execution,
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "record": "query_cost",
                    "phase": "cold",
                    "status": "error",
                    "case": &fixture.case,
                    "arm": fixture.arm.label(),
                    "plan_statistics": fixture.options.collect_plan_statistics,
                    "error": error.to_string(),
                    "counted_as_timing": false,
                }))
                .expect("serialize error record")
            );
            panic!("query-cost arm failed: {}", fixture.arm.label());
        }
    };
    let (digest, rows, cells) = result_shape(&execution.results);
    assert_eq!(rows, fixture.expected_rows, "unexpected result cardinality");
    ColdSample {
        execution,
        wall_ns,
        digest,
        rows,
        cells,
        allocation,
    }
}

fn assert_operator(arm: Arm, sample: &ColdSample) {
    let statistics = &sample.execution.statistics;
    match arm {
        Arm::NativeHashCount => {
            assert_eq!(statistics.fast_path, Some(FastPathKind::HashJoinCount));
            assert_eq!(
                statistics.planned_joins[0].physical_operator,
                JoinKind::Hash
            );
        }
        Arm::GenericHashCount | Arm::SelectHash => {
            assert_eq!(statistics.fast_path, None);
            assert_eq!(
                statistics.planned_joins[0].physical_operator,
                JoinKind::Hash
            );
        }
        Arm::GenericLateralCount | Arm::SelectLateral => {
            assert_eq!(statistics.fast_path, None);
            assert_eq!(
                statistics.planned_joins[0].physical_operator,
                JoinKind::IndexedLateral
            );
        }
        Arm::NativeStar => {
            assert_eq!(statistics.fast_path, Some(FastPathKind::PropertyStar));
        }
        Arm::GenericStar => {
            assert_eq!(statistics.fast_path, None);
        }
    }
}

fn print_sample(fixture: &Fixture, sample: &ColdSample) {
    let stats = &sample.execution.statistics;
    let join = stats.planned_joins.first();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "query_cost",
            "phase": "cold",
            "status": "ok",
            "case": &fixture.case,
            "arm": fixture.arm.label(),
            "costs_enabled": fixture.collect_costs,
            "plan_statistics": fixture.options.collect_plan_statistics,
            "fixture": {
                "digest": &fixture.fixture_digest,
                "input_rows": fixture.input_rows,
                "distinct_keys": fixture.distinct_keys,
                "full_join_rows": fixture.join_matches,
                "star_multiplicity": fixture.star_multiplicity,
                "limited": fixture.limited,
                "sync": null,
                "actor_hex": "51".repeat(32),
                "seed": FIXTURE_SEED,
            },
            "execution": {
                "prepared": true,
                "limits": "trusted_unbounded_under_external_result_cap",
                "join": join.map(|value| format!("{:?}", value.physical_operator)),
                "fast_path": stats.fast_path.map(|value| format!("{value:?}")),
                "wall_ns": sample.wall_ns,
                "parse_ns": nanos(stats.parse_time),
                "rewrite_ns": nanos(stats.rewrite_time),
                "planning_ns": nanos(stats.planning_time),
                "execution_ns": nanos(stats.execution_time),
                "collection_ns": nanos(stats.result_collection_time),
                "first_internal_ns": stats.time_to_first_internal_result.map(nanos),
            },
            "work": {
                "index_seeks": stats.index_seeks,
                "qv_admission_checks": stats.qv_admission_checks,
                "qv_header_reads": stats.qv_header_reads,
                "qv_counter_reads": stats.qv_counter_reads,
                "qv_keys": stats.qv_keys_read,
                "qv_bytes": stats.qv_bytes_read,
                "source_keys": stats.source_keys_read,
                "source_bytes": stats.source_bytes_read,
                "reverse_mapping_reads": stats.reverse_mapping_reads,
                "reverse_mapping_bytes": stats.reverse_mapping_bytes,
                "forward_mapping_reads": stats.forward_mapping_reads,
                "forward_mapping_bytes": stats.forward_mapping_bytes,
                "planner_index_entries": stats.planner_index_entries,
                "planner_point_reads": stats.planner_point_reads,
                "planner_cache_hits": stats.planner_cache_hits,
                "planner_cache_misses": stats.planner_cache_misses,
                "candidate_quads": stats.candidate_quads,
                "matching_quads": stats.matching_quads,
                "graph_checks": stats.graphs_considered,
                "orphan_checks": stats.orphan_checks,
                "key_fields": stats.key_fields_extracted,
                "authoritative_decodes": stats.authoritative_terms_decoded,
                "result_decodes": stats.result_terms_decoded,
                "quad_builds": stats.encoded_quad_constructions,
                "intermediate_rows_available": stats.intermediate_rows_available,
                "intermediate_rows": stats
                    .intermediate_rows_available
                    .then_some(stats.intermediate_rows),
                "result_rows": sample.rows,
                "result_cells": sample.cells,
            },
            "allocation": sample.allocation,
            "result": {
                "multiset_blake3": &sample.digest,
                "rows": sample.rows,
                "cells": sample.cells,
                "cross_arm_exact": !fixture.limited,
                "limited_validation": fixture.limited.then_some("cardinality_unique_fixture_members"),
            },
        }))
        .expect("serialize query-cost record")
    );
}

fn validate_group(fixtures: &[Fixture], samples: &[ColdSample]) {
    assert_eq!(fixtures.len(), samples.len());
    let expected = &samples[0].digest;
    for (fixture, sample) in fixtures.iter().zip(samples) {
        assert_eq!(fixture.fixture_digest, fixtures[0].fixture_digest);
        assert_operator(fixture.arm, sample);
        assert_eq!(sample.rows, samples[0].rows);
        assert_eq!(sample.cells, samples[0].cells);
        if fixture.limited {
            if let Some(multiplicity) = fixture.star_multiplicity {
                assert_limited(&sample.execution.results, multiplicity);
            } else {
                assert_join_limit(
                    &sample.execution.results,
                    fixture.input_rows,
                    fixture.distinct_keys,
                );
            }
        } else {
            assert_eq!(
                &sample.digest, expected,
                "query arms returned different multisets"
            );
        }
        if matches!(
            fixture.arm,
            Arm::NativeHashCount | Arm::GenericHashCount | Arm::GenericLateralCount
        ) {
            assert_count(
                &sample.execution.results,
                fixture.join_matches.expect("COUNT has join cardinality"),
            );
        }
        print_sample(fixture, sample);
    }
}

fn assert_count(results: &QueryResults, expected: usize) {
    let QueryResults::Solutions(rows) = results else {
        panic!("COUNT must return solutions");
    };
    let [row] = rows.as_slice() else {
        panic!("COUNT must return one row");
    };
    let value = row
        .get("count")
        .and_then(EncodedTerm::to_term)
        .and_then(|term| match term {
            oxrdf::Term::Literal(literal) => literal.value().parse::<usize>().ok(),
            _ => None,
        })
        .expect("COUNT result is an integer literal");
    assert_eq!(
        value, expected,
        "COUNT result differs from fixture cardinality"
    );
}

fn assert_limited(results: &QueryResults, multiplicity: usize) {
    let QueryResults::Solutions(rows) = results else {
        panic!("limited property star must return solutions");
    };
    let mut unique = std::collections::HashSet::new();
    for row in rows {
        let mut tuple = Vec::with_capacity(STAR_PROPERTIES);
        for property in 0..STAR_PROPERTIES {
            let name = format!("v{property}");
            let value = row.get(&name).expect("limited star binds every variable");
            let valid = (0..multiplicity).any(|index| {
                value.0 == format!("<urn:craqle:bench:query-cost:star:p{property}:v{index}>")
            });
            assert!(valid, "limited star returned a value outside its fixture");
            tuple.push(value.0.clone());
        }
        assert!(
            unique.insert(tuple),
            "limited star returned a duplicate row"
        );
    }
}

fn assert_join_limit(results: &QueryResults, rows: usize, keys: usize) {
    let QueryResults::Solutions(values) = results else {
        panic!("limited join must return solutions");
    };
    let mut unique = std::collections::HashSet::new();
    for row in values {
        let left = iri_suffix(row.get("left").expect("left binding"), "left");
        let right = iri_suffix(row.get("right").expect("right binding"), "right");
        let key = iri_suffix(row.get("key").expect("key binding"), "key");
        assert!(left < rows && right < rows && key < keys);
        assert_eq!(left % keys, key);
        assert_eq!(right % keys, key);
        assert!(
            unique.insert((left, right, key)),
            "limited join returned a duplicate"
        );
    }
}

fn iri_suffix(term: &EncodedTerm, domain: &str) -> usize {
    term.0
        .strip_prefix(&format!("<urn:craqle:bench:query-cost:{domain}:"))
        .and_then(|value| value.strip_suffix('>'))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("invalid {domain} fixture term: {}", term.0))
}

fn benchmark_group(c: &mut Criterion, fixtures: Vec<Fixture>, throughput: usize) {
    let samples: Vec<_> = fixtures.iter().map(cold_sample).collect();
    validate_group(&fixtures, &samples);
    if env::var("CRAQLE_COST_DIAGNOSTIC_ONLY").as_deref() == Ok("1") {
        return;
    }
    let sample_size = env_usize("CRAQLE_BENCH_SAMPLE_SIZE", 10);
    assert!(
        sample_size >= 10,
        "CRAQLE_BENCH_SAMPLE_SIZE must be at least 10"
    );
    let warmup = env_duration("CRAQLE_BENCH_WARMUP_SECS", 1);
    let measurement = env_duration("CRAQLE_BENCH_MEASUREMENT_SECS", 3);
    let mut group = c.benchmark_group(format!("query_costs/{}", fixtures[0].case));
    group.sample_size(sample_size);
    group.warm_up_time(warmup);
    group.measurement_time(measurement);
    group.throughput(Throughput::Elements(throughput as u64));
    for fixture in fixtures {
        group.bench_with_input(
            BenchmarkId::new(
                fixture.arm.label(),
                if fixture.collect_costs {
                    "costs_on"
                } else {
                    "costs_off"
                },
            ),
            &fixture,
            |b, fixture| {
                b.iter(|| {
                    let execution = fixture
                        .execute()
                        .unwrap_or_else(|error| panic!("warm arm failed: {error}"));
                    black_box(execution)
                })
            },
        );
    }
    group.finish();
}

fn query_costs(c: &mut Criterion) {
    let rows = env_usize("CRAQLE_COST_ROWS", DEFAULT_ROWS);
    let small_keys = env_usize("CRAQLE_COST_KEYS", DEFAULT_KEYS).min(rows).max(1);
    let result_cap = env_usize("CRAQLE_COST_RESULT_CAP", DEFAULT_CAP);
    let mut key_counts = vec![small_keys, rows];
    key_counts.sort_unstable();
    key_counts.dedup();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "record": "query_cost_provenance",
            "commit": repository_commit(),
            "binary_blake3": binary_blake3(),
            "rows": rows,
            "key_counts": &key_counts,
            "result_cap": result_cap,
            "plan_statistics": env_bool("CRAQLE_COST_PLAN_STATS", true),
            "seed": FIXTURE_SEED,
            "sync": null,
            "transport": "local_in_process",
            "limits": "trusted_unbounded_under_external_result_cap",
            "allocation": if cfg!(feature = "allocation-metrics") {
                "process_wide_interval_includes_fjall_workers"
            } else {
                "disabled_build_pair_required"
            },
            "cgroup_peak": "recorded_externally",
        }))
        .expect("serialize query-cost provenance")
    );

    for keys in key_counts {
        let spec = JoinSpec {
            rows,
            keys,
            result_cap,
        };
        let matches = join_cardinality(rows, keys);
        benchmark_group(
            c,
            [
                Arm::NativeHashCount,
                Arm::GenericHashCount,
                Arm::GenericLateralCount,
            ]
            .into_iter()
            .flat_map(|arm| {
                [false, true].map(move |collect_costs| Fixture::join(&spec, arm, collect_costs))
            })
            .collect(),
            matches,
        );
        benchmark_group(
            c,
            [Arm::SelectHash, Arm::SelectLateral]
                .into_iter()
                .flat_map(|arm| {
                    [false, true].map(move |collect_costs| Fixture::join(&spec, arm, collect_costs))
                })
                .collect(),
            matches.min(result_cap),
        );
    }

    let star_values = env_list("CRAQLE_COST_STAR_MULTS", &[1, 5, 20]);
    for multiplicity in star_values {
        let expected = multiplicity.pow(STAR_PROPERTIES as u32).min(20);
        let spec = StarSpec {
            multiplicity,
            limit: Some(20),
            result_cap,
        };
        benchmark_group(
            c,
            [Arm::NativeStar, Arm::GenericStar]
                .into_iter()
                .flat_map(|arm| {
                    [false, true].map(move |collect_costs| Fixture::star(&spec, arm, collect_costs))
                })
                .collect(),
            expected,
        );
    }
    let full = env_usize("CRAQLE_COST_STAR_FULL_MULT", 5);
    let spec = StarSpec {
        multiplicity: full,
        limit: None,
        result_cap,
    };
    benchmark_group(
        c,
        [Arm::NativeStar, Arm::GenericStar]
            .into_iter()
            .flat_map(|arm| {
                [false, true].map(move |collect_costs| Fixture::star(&spec, arm, collect_costs))
            })
            .collect(),
        full.pow(STAR_PROPERTIES as u32),
    );
}

criterion_group!(benches, query_costs);
criterion_main!(benches);
