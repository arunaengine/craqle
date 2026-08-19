//! Persistent-index write-cost benchmark.
//!
//! Fixture construction is deliberately kept outside Criterion's measured
//! closures. Every local case receives a new database per iteration, while
//! the replicated case receives a new two-peer Irokle-backed cluster. The
//! canonical CRDT state is checked before the benchmark starts, and the
//! resulting database bytes are printed as a paired-commit comparison aid.

use std::collections::HashSet;
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use craqle::{
    ActorId, Batch, CraqleFjallPersistMode, CraqleNode, CraqleOptions, CreateCrateRequest,
    EncodedTerm, GrantAuthorizer, GraphId, GraphPolicy, MaterializedQuadChange, PermissionGrant,
    PermissionLevel, SearchStorage,
};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};

#[path = "../tests/support/sim.rs"]
mod sim;
#[path = "support/mod.rs"]
mod support;

use sim::CraqleCluster;
use support::{
    CORPUS_VERSION, CorpusConfig, DEFAULT_SEED, DeterministicCorpus, GRAPHS_1, ObjectSpec,
    PredicateKind, QUADS_10K, QuadSpec,
};

const LOCAL_GRAPH: &str = "urn:craqle:bench:index-write-cost:local";
const MERGE_GRAPH: &str = "urn:craqle:bench:index-write-cost:merge";
const CONCURRENT_WRITERS: usize = 4;
const CONCURRENT_ROWS_PER_WRITER: usize = 100;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Triple {
    subject: EncodedTerm,
    predicate: EncodedTerm,
    object: EncodedTerm,
}

impl Triple {
    fn change(&self, graph: &GraphId, insert: bool) -> MaterializedQuadChange {
        if insert {
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: self.subject.clone(),
                predicate: self.predicate.clone(),
                object: self.object.clone(),
            }
        } else {
            MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: self.subject.clone(),
                predicate: self.predicate.clone(),
                object: self.object.clone(),
            }
        }
    }
}

/// Keep the deterministic corpus as the one source of write terms used by
/// every case. A one-graph, no-duplicate configuration gives each ordinal a
/// stable row while still exercising the corpus generator used by the read
/// benchmarks.
fn write_corpus() -> Arc<Vec<Triple>> {
    let config = CorpusConfig::new(QUADS_10K, GRAPHS_1, 0, DEFAULT_SEED)
        .expect("supported write benchmark corpus");
    let triples: Vec<_> = DeterministicCorpus::new(config)
        .expect("validated write benchmark corpus")
        .iter()
        .map(triple_from_spec)
        .collect();
    let unique: HashSet<_> = triples.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        triples.len(),
        "write corpus rows must be unique"
    );
    Arc::new(triples)
}

fn triple_from_spec(spec: QuadSpec) -> Triple {
    Triple {
        subject: EncodedTerm(format!(
            "<urn:craqle:bench:index-write-cost:subject:{:016x}>",
            spec.subject
        )),
        predicate: predicate_term(spec.predicate),
        object: object_term(spec.object),
    }
}

fn predicate_term(predicate: PredicateKind) -> EncodedTerm {
    let suffix = match predicate {
        PredicateKind::Type => "type".to_owned(),
        PredicateKind::Common(index) => format!("common:{index}"),
        PredicateKind::Rare(index) => format!("rare:{index}"),
        PredicateKind::Chain => "chain".to_owned(),
    };
    EncodedTerm(format!(
        "<urn:craqle:bench:index-write-cost:predicate:{suffix}>"
    ))
}

fn object_term(object: ObjectSpec) -> EncodedTerm {
    match object {
        ObjectSpec::Iri(value) => EncodedTerm(format!(
            "<urn:craqle:bench:index-write-cost:object:{value:016x}>"
        )),
        ObjectSpec::Literal(value) => EncodedTerm(format!("\"write-literal-{value:016x}\"")),
    }
}

fn node_options(actor_byte: u8) -> CraqleOptions {
    CraqleOptions::new()
        .with_actor(ActorId::from_bytes([actor_byte; 32]))
        .with_search_storage(SearchStorage::Memory)
        .with_graph_store_persist_mode(CraqleFjallPersistMode::Buffer)
}

struct LocalFixture {
    // Drop the node before TempDir closes the store it owns.
    node: Arc<CraqleNode>,
    database: tempfile::TempDir,
    graph: GraphId,
    changes: Vec<MaterializedQuadChange>,
}

fn local_fixture(changes: Vec<MaterializedQuadChange>) -> LocalFixture {
    let database = tempfile::tempdir().expect("create local write benchmark database");
    let node = CraqleNode::open_with_options(database.path(), node_options(0xA5))
        .expect("open local write benchmark node");
    let graph = GraphId::new(LOCAL_GRAPH);
    assert_eq!(node.graph_snapshot(&graph).unwrap().quads.len(), 0);
    node.flush_search_updates()
        .expect("settle local fixture search work");
    LocalFixture {
        node: Arc::new(node),
        database,
        graph,
        changes,
    }
}

fn changes_for(
    graph: &GraphId,
    triples: &[Triple],
    range: std::ops::Range<usize>,
    insert: bool,
) -> Vec<MaterializedQuadChange> {
    range
        .map(|index| triples[index].change(graph, insert))
        .collect()
}

fn delete_fixture(triples: &[Triple]) -> LocalFixture {
    let mut fixture = local_fixture(Vec::new());
    fixture
        .node
        .apply_changes_unchecked(
            &fixture.graph,
            vec![triples[0].change(&fixture.graph, true)],
        )
        .expect("seed live row for delete benchmark");
    assert_row_count(&fixture.node, &fixture.graph, 1);
    fixture
        .node
        .flush_search_updates()
        .expect("settle delete benchmark setup");
    fixture.changes = vec![triples[0].change(&fixture.graph, false)];
    fixture
}

fn concurrent_fixture(triples: &[Triple]) -> ConcurrentFixture {
    let database = tempfile::tempdir().expect("create concurrent write benchmark database");
    let node = Arc::new(
        CraqleNode::open_with_options(database.path(), node_options(0xA6))
            .expect("open concurrent write benchmark node"),
    );
    let graph = GraphId::new(LOCAL_GRAPH);
    let batches = (0..CONCURRENT_WRITERS)
        .map(|writer| {
            let start = writer * CONCURRENT_ROWS_PER_WRITER;
            changes_for(
                &graph,
                triples,
                start..start + CONCURRENT_ROWS_PER_WRITER,
                true,
            )
        })
        .collect();
    node.flush_search_updates()
        .expect("settle concurrent fixture search work");
    ConcurrentFixture {
        node,
        database,
        graph,
        batches,
    }
}

struct ConcurrentFixture {
    node: Arc<CraqleNode>,
    database: tempfile::TempDir,
    graph: GraphId,
    batches: Vec<Vec<MaterializedQuadChange>>,
}

struct MergeFixture {
    // CraqleCluster must drop before its backing TempDir.
    cluster: CraqleCluster,
    database: tempfile::TempDir,
    graph: GraphId,
    expected_before: usize,
}

fn merge_fixture(_triples: &[Triple]) -> MergeFixture {
    let database = tempfile::tempdir().expect("create replicated write benchmark database");
    let cluster =
        CraqleCluster::new_with_options(2, database.path(), |peer| node_options(0xB0 + peer as u8))
            .expect("open replicated write benchmark cluster");
    let graph = GraphId::new(MERGE_GRAPH);
    let benchmark_auth = GrantAuthorizer::new(vec![PermissionGrant::new(
        "/bench/**",
        PermissionLevel::Write,
    )]);
    cluster
        .peer(0)
        .create_crate(
            &benchmark_auth,
            CreateCrateRequest::new(
                graph.clone(),
                "Index write merge benchmark",
                "Deterministic public replicated-merge fixture",
                "2025-01-01",
                None,
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/bench/public".to_owned()],
                },
            ),
        )
        .expect("create merge benchmark crate");
    cluster
        .sync_until_converged(10)
        .expect("sync merge benchmark baseline");
    cluster
        .flush_search_updates()
        .expect("settle baseline search work");
    let baseline = cluster.peer(1).graph_snapshot(&graph).unwrap();
    let rdf_type = EncodedTerm("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned());
    let subject = baseline
        .quads
        .iter()
        .find(|quad| quad.predicate == rdf_type)
        .map(|quad| quad.subject.clone())
        .expect("merge baseline must contain a typed subject");
    let expected_before = baseline.quads.len();
    assert!(
        expected_before > 0,
        "merge baseline must contain crate rows"
    );

    cluster
        .peer(0)
        .insert_quads(
            &graph,
            vec![(
                subject,
                EncodedTerm("<urn:craqle:bench:index-write-cost:predicate:replicated>".to_owned()),
                EncodedTerm("\"replicated-write\"".to_owned()),
            )],
        )
        .expect("stage one remote merge row");
    for peer in 0..cluster.peer_count() {
        cluster
            .peer(peer)
            .flush_search_updates()
            .expect("settle merge peer setup");
    }
    assert_row_count(cluster.peer(0), &graph, expected_before + 1);
    assert_row_count(cluster.peer(1), &graph, expected_before);
    MergeFixture {
        cluster,
        database,
        graph,
        expected_before,
    }
}

fn assert_row_count(node: &CraqleNode, graph: &GraphId, expected: usize) {
    let snapshot = node
        .graph_snapshot(graph)
        .expect("read write benchmark graph snapshot");
    assert_eq!(snapshot.quads.len(), expected, "unexpected live row count");
}

fn apply_local_fixture(mut fixture: LocalFixture, bulk: bool) -> (LocalFixture, Batch) {
    let changes = std::mem::take(&mut fixture.changes);
    let result = if bulk {
        fixture
            .node
            .apply_changes_bulk_unchecked(&fixture.graph, changes)
    } else {
        fixture
            .node
            .apply_changes_unchecked(&fixture.graph, changes)
    }
    .expect("apply local write benchmark changes");
    if bulk {
        fixture
            .node
            .rebuild_graph_diagnostics(&fixture.graph)
            .expect("rebuild bulk local write diagnostics");
    }
    (fixture, result)
}

fn apply_concurrent_fixture(fixture: ConcurrentFixture) -> (ConcurrentFixture, Vec<Batch>) {
    let ConcurrentFixture {
        node,
        database,
        graph,
        batches,
    } = fixture;
    let start = Arc::new(Barrier::new(batches.len()));
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(batches.len());
        for changes in batches {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            let start = Arc::clone(&start);
            handles.push(scope.spawn(move || {
                start.wait();
                node.apply_changes_bulk_unchecked(&graph, changes)
                    .expect("apply concurrent local write batch")
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("join concurrent local write"))
            .collect()
    });
    node.rebuild_graph_diagnostics(&graph)
        .expect("rebuild concurrent local write diagnostics");
    (
        ConcurrentFixture {
            node,
            database,
            graph,
            batches: Vec::new(),
        },
        results,
    )
}

fn apply_merge_fixture(fixture: MergeFixture) -> (MergeFixture, usize) {
    let moved = fixture
        .cluster
        .sync_pair(0, 1)
        .expect("sync replicated merge batch");
    (fixture, moved)
}

fn settle_and_size(node: &CraqleNode, database: &tempfile::TempDir) -> u64 {
    node.flush_search_updates()
        .expect("settle local search work");
    node.persist_fjall()
        .expect("persist local benchmark database");
    directory_bytes(&database.path().join("store"))
}

fn directory_bytes(path: &Path) -> u64 {
    let metadata = fs::symlink_metadata(path).expect("stat benchmark database path");
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read benchmark database directory")
        .map(|entry| directory_bytes(&entry.expect("read benchmark database entry").path()))
        .sum()
}

fn assert_local_contract(
    label: &str,
    fixture: LocalFixture,
    expected_rows: usize,
    bulk: bool,
) -> u64 {
    let LocalFixture {
        node,
        database,
        graph,
        changes,
    } = fixture;
    let result = if bulk {
        node.apply_changes_bulk_unchecked(&graph, changes)
    } else {
        node.apply_changes_unchecked(&graph, changes)
    }
    .expect("apply untimed local write contract");
    black_box(result);
    if bulk {
        node.rebuild_graph_diagnostics(&graph)
            .expect("rebuild untimed bulk diagnostics");
    }
    assert_row_count(&node, &graph, expected_rows);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case={label} corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows={expected_rows} db_bytes={bytes} persistence=buffer \
         write_path=raw_unchecked_changes search_storage=memory"
    );
    drop(node);
    drop(database);
    bytes
}

fn assert_delete_contract(triples: &[Triple]) -> u64 {
    let fixture = delete_fixture(triples);
    let LocalFixture {
        node,
        database,
        graph,
        changes,
    } = fixture;
    assert_row_count(&node, &graph, 1);
    black_box(
        node.apply_changes_unchecked(&graph, changes)
            .expect("apply untimed delete contract"),
    );
    assert_row_count(&node, &graph, 0);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case=single_delete corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows=0 db_bytes={bytes} setup_live_rows=1 persistence=buffer \
         write_path=raw_unchecked_changes search_storage=memory"
    );
    drop(node);
    drop(database);
    bytes
}

fn assert_concurrent_contract(triples: &[Triple]) -> u64 {
    let fixture = concurrent_fixture(triples);
    let ConcurrentFixture {
        node,
        database,
        graph,
        batches,
    } = fixture;
    let expected_rows = batches.len() * CONCURRENT_ROWS_PER_WRITER;
    let start = Arc::new(Barrier::new(batches.len()));
    std::thread::scope(|scope| {
        for changes in batches {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            let start = Arc::clone(&start);
            scope.spawn(move || {
                start.wait();
                node.apply_changes_bulk_unchecked(&graph, changes)
                    .expect("apply untimed concurrent contract");
            });
        }
    });
    node.rebuild_graph_diagnostics(&graph)
        .expect("rebuild untimed concurrent diagnostics");
    assert_row_count(&node, &graph, expected_rows);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case=concurrent_local_writes corpus_version={CORPUS_VERSION} \
         seed={DEFAULT_SEED:#x} rows={expected_rows} writers={CONCURRENT_WRITERS} \
         rows_per_writer={CONCURRENT_ROWS_PER_WRITER} db_bytes={bytes} persistence=buffer \
         write_path=raw_unchecked_changes search_storage=memory"
    );
    drop(node);
    drop(database);
    bytes
}

fn assert_merge_contract(triples: &[Triple]) -> u64 {
    let fixture = merge_fixture(triples);
    let MergeFixture {
        cluster,
        database,
        graph,
        expected_before,
    } = fixture;
    let moved = cluster
        .sync_pair(0, 1)
        .expect("apply untimed replicated merge contract");
    assert!(moved > 0, "replicated merge must move the staged row");
    cluster
        .flush_search_updates()
        .expect("settle merge search work");
    assert_row_count(cluster.peer(1), &graph, expected_before + 1);
    for peer in 0..2 {
        cluster
            .peer(peer)
            .persist_fjall()
            .expect("persist merge benchmark database");
    }
    let bytes = directory_bytes(&database.path().join("peer_0/store"))
        + directory_bytes(&database.path().join("peer_1/store"));
    println!(
        "index_write_cost case=replicated_merge corpus_version={CORPUS_VERSION} \
         seed={DEFAULT_SEED:#x} rows_added=1 db_bytes={bytes} peers=2 \
         persistence=buffer path=CraqleCluster::sync_pair search_storage=memory"
    );
    drop(cluster);
    drop(database);
    bytes
}

fn env_duration(name: &str, default_seconds: u64) -> Duration {
    match env::var(name) {
        Ok(value) => Duration::from_secs(
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("{name} must be an integer number of seconds")),
        ),
        Err(env::VarError::NotPresent) => Duration::from_secs(default_seconds),
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn env_sample_size() -> usize {
    match env::var("CRAQLE_BENCH_SAMPLE_SIZE") {
        Ok(value) => {
            let sample_size = value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("CRAQLE_BENCH_SAMPLE_SIZE must be an integer"));
            assert!(
                sample_size >= 10,
                "CRAQLE_BENCH_SAMPLE_SIZE must be at least 10"
            );
            sample_size
        }
        Err(env::VarError::NotPresent) => 10,
        Err(env::VarError::NotUnicode(_)) => {
            panic!("CRAQLE_BENCH_SAMPLE_SIZE must be valid UTF-8")
        }
    }
}

fn index_write_cost_benchmarks(c: &mut Criterion) {
    let triples = write_corpus();
    let sample_size = env_sample_size();
    println!(
        "index_write_cost metadata: corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows={} local_cases=single_insert,single_delete,batch_100,batch_10000,\
         concurrent_local_writes replicated_case=replicated_merge \
         setup=iter_batched_per_iteration persistence=buffer \
         local_write_path=raw_unchecked_changes replicated_path=CraqleCluster::sync_pair \
         search_storage=memory sample_size={sample_size}",
        triples.len()
    );

    // These are deliberately untimed. They prove the operation contracts and
    // provide one comparable database-size sample for each case.
    assert_local_contract(
        "single_insert",
        local_fixture(changes_for(
            &GraphId::new(LOCAL_GRAPH),
            &triples,
            0..1,
            true,
        )),
        1,
        false,
    );
    assert_delete_contract(&triples);
    assert_local_contract(
        "batch_100",
        local_fixture(changes_for(
            &GraphId::new(LOCAL_GRAPH),
            &triples,
            0..100,
            true,
        )),
        100,
        true,
    );
    assert_local_contract(
        "batch_10000",
        local_fixture(changes_for(
            &GraphId::new(LOCAL_GRAPH),
            &triples,
            0..10_000,
            true,
        )),
        10_000,
        true,
    );
    assert_concurrent_contract(&triples);
    assert_merge_contract(&triples);

    let mut group = c.benchmark_group("index_write_cost");
    group.sample_size(sample_size);
    group.warm_up_time(env_duration("CRAQLE_BENCH_WARMUP_SECS", 1));
    group.measurement_time(env_duration("CRAQLE_BENCH_MEASUREMENT_SECS", 5));

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_insert", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture(changes_for(&graph, &triples, 0..1, true))
            },
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_delete", |b| {
        b.iter_batched(
            || delete_fixture(&triples),
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(100));
    group.bench_function("batch_100", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture(changes_for(&graph, &triples, 0..100, true))
            },
            |fixture| apply_local_fixture(fixture, true),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("batch_10000", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture(changes_for(&graph, &triples, 0..10_000, true))
            },
            |fixture| apply_local_fixture(fixture, true),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(
        (CONCURRENT_WRITERS * CONCURRENT_ROWS_PER_WRITER) as u64,
    ));
    group.bench_function("concurrent_local_writes", |b| {
        b.iter_batched(
            || concurrent_fixture(&triples),
            apply_concurrent_fixture,
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("replicated_merge", |b| {
        b.iter_batched(
            || merge_fixture(&triples),
            apply_merge_fixture,
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, index_write_cost_benchmarks);
criterion_main!(benches);
