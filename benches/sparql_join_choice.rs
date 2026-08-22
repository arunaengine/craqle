use std::env;
use std::hint::black_box;

#[path = "support/allocation.rs"]
mod allocation;
#[path = "support/mod.rs"]
mod support;

use support::BenchWriteExt as _;

use allocation::AllocationInterval;
use craqle::{
    ActorId, AllowAllAuthorizer, CraqleNode, CraqleOptions, EncodedTerm, GraphId, JoinKind,
    JoinMode, MaterializedQuadChange, QueryExecution, QueryFastPathKind, QueryOptions,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use support::fixture::{binary_blake3, env_duration, repository_commit};

const LOAD_BATCH_SIZE: usize = 1_000;

fn iri(value: impl Into<String>) -> EncodedTerm {
    EncodedTerm(format!("<{}>", value.into()))
}

struct JoinFixture {
    node: CraqleNode,
    _database: tempfile::TempDir,
    graph: GraphId,
    query: craqle::PreparedQuery,
    rows: usize,
    distinct_keys: usize,
}

impl JoinFixture {
    fn new(rows: usize, distinct_keys: usize) -> Self {
        assert!(distinct_keys > 0 && distinct_keys <= rows);
        let database = tempfile::tempdir().expect("create join benchmark database");
        let node = CraqleNode::open_with_options(
            database.path(),
            CraqleOptions::new().with_actor(ActorId::from_bytes([0x4a; 32])),
        )
        .expect("open join benchmark node");
        let graph = GraphId::new("urn:craqle:bench:join");
        for start in (0..rows).step_by(LOAD_BATCH_SIZE) {
            let end = (start + LOAD_BATCH_SIZE).min(rows);
            let mut changes = Vec::with_capacity((end - start) * 2);
            for index in start..end {
                let key = index % distinct_keys;
                let subject = iri(format!("urn:craqle:bench:join:s:{key:010}"));
                let object = iri(format!("urn:craqle:bench:join:v:{index:010}"));
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: subject.clone(),
                    predicate: iri("urn:craqle:bench:join:left"),
                    object: object.clone(),
                });
                changes.push(MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject,
                    predicate: iri("urn:craqle:bench:join:right"),
                    object,
                });
            }
            node.apply_changes_unchecked(&graph, changes)
                .expect("load join benchmark batch");
        }
        let query = node
            .prepare_query(
                "SELECT (COUNT(*) AS ?count) WHERE { \
                 ?s <urn:craqle:bench:join:left> ?key . \
                 ?s <urn:craqle:bench:join:right> ?key }",
            )
            .expect("prepare join benchmark query");
        Self {
            node,
            _database: database,
            graph,
            query,
            rows,
            distinct_keys,
        }
    }

    fn run(&self, mode: JoinMode) -> QueryExecution {
        let mut options = QueryOptions::default();
        options.join_mode = mode;
        self.node
            .execute_prepared_in_graphs(
                &AllowAllAuthorizer,
                std::slice::from_ref(&self.graph),
                &self.query,
                &options,
            )
            .expect("execute forced join benchmark")
    }
}

fn sparql_join_choice(c: &mut Criterion) {
    let rows = env::var("CRAQLE_JOIN_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let distinct_keys = env::var("CRAQLE_JOIN_KEYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let fixture = JoinFixture::new(rows, distinct_keys);
    let lateral_interval = AllocationInterval::begin();
    let lateral = fixture.run(JoinMode::ForceLateral);
    let lateral_allocations = lateral_interval.finish();
    let hash_interval = AllocationInterval::begin();
    let hash = fixture.run(JoinMode::ForceHash);
    let hash_allocations = hash_interval.finish();
    let automatic_interval = AllocationInterval::begin();
    let automatic = fixture.run(JoinMode::Auto);
    let automatic_allocations = automatic_interval.finish();
    assert_eq!(lateral.results, hash.results);
    assert_eq!(hash.results, automatic.results);
    assert_eq!(
        lateral.statistics.planned_joins[0].physical_operator,
        JoinKind::IndexedLateral
    );
    assert_eq!(
        hash.statistics.planned_joins[0].physical_operator,
        JoinKind::Hash
    );
    assert_eq!(
        automatic.statistics.planned_joins[0].physical_operator,
        JoinKind::Hash
    );
    assert_eq!(
        hash.statistics.fast_path,
        Some(QueryFastPathKind::HashJoinCount)
    );
    assert_eq!(
        automatic.statistics.fast_path,
        Some(QueryFastPathKind::HashJoinCount)
    );
    for (mode, execution, allocations) in [
        ("ForceLateral", &lateral, lateral_allocations),
        ("ForceHash", &hash, hash_allocations),
        ("Auto", &automatic, automatic_allocations),
    ] {
        let statistics = &execution.statistics;
        eprintln!(
            "sparql_join_choice rows={} distinct_keys={} mode={mode} logical_operator={:?} \
             physical_operator={:?} join_operator={:?} fast_path={:?} plan_fingerprint={} access_path={:?} \
             estimated_rows={:?} actual_rows={:?} plan_candidates={} plan_output_rows={} \
             plan_elapsed_ns={} index_seeks={} qv_admission_checks={} qv_header_reads={} qv_counter_reads={} \
             qv_trusted={} fallback_reason={} source_keys={} source_bytes={} qv_keys={} \
             qv_bytes={} candidate_quads={} matching_quads={} graph_checks={} orphan_checks={} \
             duplicate_groups={} duplicate_copies_skipped={} term_decodes={} intermediate_rows={} \
             result_rows={} result_cells={} parse_ns={} rewrite_ns={} planning_ns={} execution_ns={} \
             collection_ns={} first_internal_ns={:?} allocations={} allocated_bytes={} \
             peak_live_delta_bytes={}",
            fixture.rows,
            fixture.distinct_keys,
            statistics.plan.root.logical_operator,
            statistics.plan.root.physical_operator,
            statistics.planned_joins[0].physical_operator,
            statistics.fast_path,
            statistics.plan_fingerprint,
            statistics.selected_access_paths,
            statistics.plan.root.estimated_rows,
            statistics.plan.root.actual_rows,
            statistics.plan.root.candidate_rows,
            statistics.plan.root.output_rows,
            statistics.plan.root.elapsed_time.as_nanos(),
            statistics.index_seeks,
            statistics.qv_admission_checks,
            statistics.qv_header_reads,
            statistics.qv_counter_reads,
            statistics.qv_trusted,
            statistics.fallback_reason.as_deref().unwrap_or("none"),
            statistics.source_keys_read,
            statistics.source_bytes_read,
            statistics.qv_keys_read,
            statistics.qv_bytes_read,
            statistics.candidate_quads,
            statistics.matching_quads,
            statistics.graphs_considered,
            statistics.orphan_checks,
            statistics.duplicate_groups,
            statistics.duplicate_copies_skipped,
            statistics.terms_decoded,
            statistics.intermediate_rows,
            statistics.result_rows,
            statistics.result_cells,
            statistics.parse_time.as_nanos(),
            statistics.rewrite_time.as_nanos(),
            statistics.planning_time.as_nanos(),
            statistics.execution_time.as_nanos(),
            statistics.result_collection_time.as_nanos(),
            statistics
                .time_to_first_internal_result
                .map(|duration| duration.as_nanos()),
            allocations.allocations,
            allocations.allocated_bytes,
            allocations.peak_live_delta_bytes,
        );
    }

    let sample_size = match env::var("CRAQLE_BENCH_SAMPLE_SIZE") {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("CRAQLE_BENCH_SAMPLE_SIZE must be an integer")),
        Err(env::VarError::NotPresent) => 10,
        Err(env::VarError::NotUnicode(_)) => {
            panic!("CRAQLE_BENCH_SAMPLE_SIZE must be valid UTF-8")
        }
    };
    assert!(
        sample_size >= 10,
        "CRAQLE_BENCH_SAMPLE_SIZE must be at least 10"
    );
    let warm_up = env_duration("CRAQLE_BENCH_WARMUP_SECS", 1);
    let measurement = env_duration("CRAQLE_BENCH_MEASUREMENT_SECS", 5);
    let mut fixture_hasher = blake3::Hasher::new();
    fixture_hasher.update(b"craqle-sparql-join-choice-v1");
    fixture_hasher.update(&(rows as u64).to_le_bytes());
    fixture_hasher.update(&(distinct_keys as u64).to_le_bytes());
    fixture_hasher.update(&(LOAD_BATCH_SIZE as u64).to_le_bytes());
    eprintln!(
        "sparql_join_choice provenance: commit={} binary_blake3={} fixture_digest={} \
         rows={} distinct_keys={} sample_size={} warmup_secs={} measurement_secs={}",
        repository_commit(),
        binary_blake3(),
        fixture_hasher.finalize().to_hex(),
        rows,
        distinct_keys,
        sample_size,
        warm_up.as_secs(),
        measurement.as_secs(),
    );
    let mut group = c.benchmark_group("sparql_join_choice");
    group.sample_size(sample_size);
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);
    group.throughput(Throughput::Elements(rows as u64));
    for (label, mode) in [
        ("forced_lateral", JoinMode::ForceLateral),
        ("forced_hash", JoinMode::ForceHash),
        ("automatic", JoinMode::Auto),
    ] {
        group.bench_function(label, |b| b.iter(|| black_box(fixture.run(mode))));
    }
    group.finish();
}

criterion_group!(benches, sparql_join_choice);
criterion_main!(benches);
