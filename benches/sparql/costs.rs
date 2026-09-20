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
