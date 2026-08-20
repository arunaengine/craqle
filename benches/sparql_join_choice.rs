use std::env;
use std::hint::black_box;
use std::time::Duration;

use craqle::{
    ActorId, CraqleNode, CraqleOptions, EncodedTerm, GraphId, JoinKind, JoinMode,
    MaterializedQuadChange, QueryExecution, QueryExecutionOptions, QueryFastPathKind,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

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
        let mut options = QueryExecutionOptions::default();
        options.join_mode = mode;
        self.node
            .execute_prepared_graphs(std::slice::from_ref(&self.graph), &self.query, &options)
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
    let lateral = fixture.run(JoinMode::ForceLateral);
    let hash = fixture.run(JoinMode::ForceHash);
    let automatic = fixture.run(JoinMode::Auto);
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
    eprintln!(
        "sparql_join_choice rows={} distinct_keys={} lateral_ns={} hash_ns={} auto_ns={} lateral_seeks={} hash_seeks={} lateral_candidates={} hash_candidates={}",
        fixture.rows,
        fixture.distinct_keys,
        lateral.statistics.execution_time.as_nanos(),
        hash.statistics.execution_time.as_nanos(),
        automatic.statistics.execution_time.as_nanos(),
        lateral.statistics.index_seeks,
        hash.statistics.index_seeks,
        lateral.statistics.candidate_quads,
        hash.statistics.candidate_quads,
    );

    let mut group = c.benchmark_group("sparql_join_choice");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
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
