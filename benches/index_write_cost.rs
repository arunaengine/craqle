//! Persistent-index write-cost benchmark.
//!
//! Fixture construction is deliberately kept outside Criterion's measured
//! closures. Every local case receives a new database per iteration, while
//! the replicated case receives a new two-peer Irokle-backed cluster. The
//! canonical CRDT state is checked before the benchmark starts, and the
//! resulting database bytes are printed as a paired-commit comparison aid.

use std::collections::HashMap;
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

#[path = "support/allocation.rs"]
mod allocation;
#[path = "../tests/support/sim.rs"]
mod sim;
#[path = "support/mod.rs"]
mod support;

use allocation::AllocationInterval;
use sim::CraqleCluster;
use support::{
    CORPUS_VERSION, CorpusConfig, DEFAULT_SEED, DeterministicCorpus, GRAPHS_1, ObjectSpec,
    PredicateKind, QUADS_10K, QuadSpec,
};

const LOCAL_GRAPH: &str = "urn:craqle:bench:index-write-cost:local";
const MERGE_GRAPH: &str = "urn:craqle:bench:index-write-cost:merge";
const CONCURRENT_WRITERS: usize = 4;
const CONCURRENT_ROWS_PER_WRITER: usize = 100;
const LOAD_BATCH_SIZE: usize = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Triple {
    graph: u32,
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

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(default)
}

fn write_config() -> CorpusConfig {
    CorpusConfig::new(
        env_usize("CRAQLE_BENCH_QUADS", QUADS_10K),
        env_usize("CRAQLE_BENCH_GRAPHS", GRAPHS_1),
        env_u8("CRAQLE_BENCH_DUPLICATE_PERCENT", 0),
        DEFAULT_SEED,
    )
    .unwrap_or_else(|error| panic!("invalid write benchmark corpus: {error}"))
}

fn write_corpus(config: CorpusConfig) -> Arc<Vec<Triple>> {
    let rows = config.quads.min(QUADS_10K);
    let triples: Vec<_> = DeterministicCorpus::new(config)
        .expect("validated write benchmark corpus")
        .iter()
        .take(rows)
        .map(operation_triple)
        .collect();
    Arc::new(triples)
}

fn hash_frame(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn corpus_hash(config: CorpusConfig) -> String {
    let mut hasher = blake3::Hasher::new();
    hash_frame(&mut hasher, b"craqle-index-write/v2");
    hash_frame(&mut hasher, CORPUS_VERSION.as_bytes());
    hash_frame(&mut hasher, &config.seed.to_le_bytes());
    hash_frame(&mut hasher, &(config.quads as u64).to_le_bytes());
    hash_frame(&mut hasher, &(config.graphs as u64).to_le_bytes());
    hash_frame(&mut hasher, &[config.duplicate_percent]);
    for spec in DeterministicCorpus::new(config)
        .expect("validated write benchmark corpus")
        .iter()
    {
        let triple = triple_from_spec(spec);
        hash_frame(&mut hasher, &spec.graph.to_le_bytes());
        hash_frame(&mut hasher, triple.subject.0.as_bytes());
        hash_frame(&mut hasher, triple.predicate.0.as_bytes());
        hash_frame(&mut hasher, triple.object.0.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn triple_from_spec(spec: QuadSpec) -> Triple {
    Triple {
        graph: spec.graph,
        subject: EncodedTerm(format!(
            "<urn:craqle:bench:index-write-cost:subject:{:016x}>",
            spec.subject
        )),
        predicate: predicate_term(spec.predicate),
        object: object_term(spec.object),
    }
}

fn operation_triple(spec: QuadSpec) -> Triple {
    let mut triple = triple_from_spec(spec);
    triple.subject = EncodedTerm(format!(
        "<urn:craqle:bench:index-write-cost:operation:{:016x}>",
        spec.ordinal
    ));
    triple
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

fn local_options(actor_byte: u8, mode: CraqleFjallPersistMode) -> CraqleOptions {
    node_options(actor_byte).with_graph_store_persist_mode(mode)
}

struct LocalFixture {
    // Drop the node before TempDir closes the store it owns.
    node: Arc<CraqleNode>,
    database: tempfile::TempDir,
    graph: GraphId,
    graphs: Vec<GraphId>,
    changes: Vec<MaterializedQuadChange>,
}

fn preload_corpus(node: &CraqleNode, config: CorpusConfig, base: &GraphId) -> Vec<GraphId> {
    let graphs: Vec<_> = (0..config.graphs)
        .map(|index| graph_for(base, index as u32))
        .collect();
    let mut partitions = (0..graphs.len())
        .map(|_| Vec::new())
        .collect::<Vec<Vec<MaterializedQuadChange>>>();
    for spec in DeterministicCorpus::new(config)
        .expect("validated write benchmark corpus")
        .iter()
    {
        let graph_index = spec.graph as usize;
        partitions[graph_index].push(MaterializedQuadChange::Insert {
            graph: graphs[graph_index].clone(),
            subject: triple_from_spec(spec).subject,
            predicate: predicate_term(spec.predicate),
            object: object_term(spec.object),
        });
        if partitions[graph_index].len() >= LOAD_BATCH_SIZE {
            let changes = std::mem::take(&mut partitions[graph_index]);
            node.apply_changes_bulk_unchecked(&graphs[graph_index], changes)
                .expect("preload graph-scoped write corpus");
        }
    }
    for (index, changes) in partitions.into_iter().enumerate() {
        if !changes.is_empty() {
            node.apply_changes_bulk_unchecked(&graphs[index], changes)
                .expect("finish graph-scoped write corpus preload");
        }
    }
    for graph in &graphs {
        node.rebuild_graph_diagnostics(graph)
            .expect("rebuild preloaded graph diagnostics");
    }
    node.ensure_query_indexes();
    node.flush_search_updates()
        .expect("settle preloaded search work");
    node.persist_fjall()
        .expect("persist preloaded write corpus");
    graphs
}

fn local_fixture_mode(
    config: CorpusConfig,
    changes: Vec<MaterializedQuadChange>,
    mode: CraqleFjallPersistMode,
) -> LocalFixture {
    let database = tempfile::tempdir().expect("create local write benchmark database");
    let node = CraqleNode::open_with_options(database.path(), local_options(0xA5, mode))
        .expect("open local write benchmark node");
    let graph = GraphId::new(LOCAL_GRAPH);
    assert_eq!(node.graph_snapshot(&graph).unwrap().quads.len(), 0);
    let graphs = preload_corpus(&node, config, &graph);
    LocalFixture {
        node: Arc::new(node),
        database,
        graph,
        graphs,
        changes,
    }
}

fn local_fixture(config: CorpusConfig, changes: Vec<MaterializedQuadChange>) -> LocalFixture {
    local_fixture_mode(config, changes, CraqleFjallPersistMode::Buffer)
}

fn local_fixture_union_proof_state(
    config: CorpusConfig,
    changes: Vec<MaterializedQuadChange>,
    mode: CraqleFjallPersistMode,
    invalidate: bool,
) -> LocalFixture {
    let fixture = local_fixture_mode(config, changes, mode);
    let spec = DeterministicCorpus::new(config)
        .expect("validated write benchmark corpus")
        .iter()
        .next()
        .expect("write benchmark corpus is non-empty");
    let duplicate = GraphId::new("urn:craqle:bench:index-write-cost:union-proof-unknown");
    let warm = operation_triple(spec);
    fixture
        .node
        .apply_changes_unchecked(&duplicate, vec![warm.change(&duplicate, true)])
        .expect("warm union-proof write state");
    fixture
        .node
        .apply_changes_unchecked(&duplicate, vec![warm.change(&duplicate, false)])
        .expect("restore the warmed union-proof write state");
    if invalidate {
        let triple = triple_from_spec(spec);
        fixture
            .node
            .apply_changes_unchecked(&duplicate, vec![triple.change(&duplicate, true)])
            .expect("create a duplicate union row");
        fixture
            .node
            .apply_changes_unchecked(&duplicate, vec![triple.change(&duplicate, false)])
            .expect("remove the duplicate union row");
    }
    fixture
        .node
        .flush_search_updates()
        .expect("settle union-proof benchmark search work");
    fixture
        .node
        .persist_fjall()
        .expect("persist union-proof benchmark setup");
    fixture
}

fn changes_for(
    graph: &GraphId,
    triples: &[Triple],
    range: std::ops::Range<usize>,
    insert: bool,
) -> Vec<MaterializedQuadChange> {
    range
        .map(|index| {
            let target = graph_for(graph, triples[index].graph);
            triples[index].change(&target, insert)
        })
        .collect()
}

fn graph_for(base: &GraphId, index: u32) -> GraphId {
    if index == 0 {
        base.clone()
    } else {
        GraphId::new(&format!("{}:{index}", base.as_str()))
    }
}

fn change_groups(
    changes: Vec<MaterializedQuadChange>,
) -> Vec<(GraphId, Vec<MaterializedQuadChange>)> {
    let mut groups = HashMap::<GraphId, Vec<MaterializedQuadChange>>::new();
    for change in changes {
        let graph = match &change {
            MaterializedQuadChange::Insert { graph, .. }
            | MaterializedQuadChange::Delete { graph, .. } => graph.clone(),
        };
        groups.entry(graph).or_default().push(change);
    }
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    groups
}

fn delete_fixture(config: CorpusConfig, triples: &[Triple]) -> LocalFixture {
    let mut fixture = local_fixture(config, Vec::new());
    let graph = graph_for(&fixture.graph, triples[0].graph);
    fixture
        .node
        .apply_changes_unchecked(&graph, vec![triples[0].change(&graph, true)])
        .expect("seed live row for delete benchmark");
    assert_rows(&fixture.node, &fixture.graphs, config.quads + 1);
    fixture
        .node
        .flush_search_updates()
        .expect("settle delete benchmark setup");
    fixture.node.ensure_query_indexes();
    fixture.changes = vec![triples[0].change(&graph, false)];
    fixture
}

fn concurrent_fixture(config: CorpusConfig, triples: &[Triple]) -> ConcurrentFixture {
    let database = tempfile::tempdir().expect("create concurrent write benchmark database");
    let node = Arc::new(
        CraqleNode::open_with_options(
            database.path(),
            local_options(0xA6, CraqleFjallPersistMode::Buffer),
        )
        .expect("open concurrent write benchmark node"),
    );
    let graph = GraphId::new(LOCAL_GRAPH);
    let graphs = preload_corpus(&node, config, &graph);
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
    ConcurrentFixture {
        node,
        database,
        graphs,
        batches,
    }
}

struct ConcurrentFixture {
    node: Arc<CraqleNode>,
    database: tempfile::TempDir,
    graphs: Vec<GraphId>,
    batches: Vec<Vec<MaterializedQuadChange>>,
}

struct MergeFixture {
    // CraqleCluster must drop before its backing TempDir.
    cluster: CraqleCluster,
    database: tempfile::TempDir,
    graph: GraphId,
    expected_before: usize,
    fixture_hash: String,
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
    let fixture_hash = blake3::hash(
        &postcard::to_allocvec(&baseline).expect("encode replicated benchmark fixture"),
    )
    .to_hex()
    .to_string();
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
        cluster.peer(peer).ensure_query_indexes();
    }
    assert_row_count(cluster.peer(0), &graph, expected_before + 1);
    assert_row_count(cluster.peer(1), &graph, expected_before);
    MergeFixture {
        cluster,
        database,
        graph,
        expected_before,
        fixture_hash,
    }
}

fn assert_row_count(node: &CraqleNode, graph: &GraphId, expected: usize) {
    let snapshot = node
        .graph_snapshot(graph)
        .expect("read write benchmark graph snapshot");
    assert_eq!(snapshot.quads.len(), expected, "unexpected live row count");
}

fn assert_rows(node: &CraqleNode, graphs: &[GraphId], expected: usize) {
    let rows: usize = graphs
        .iter()
        .map(|graph| {
            node.graph_snapshot(graph)
                .expect("read write benchmark graph snapshot")
                .quads
                .len()
        })
        .sum();
    assert_eq!(rows, expected, "unexpected live row count");
}

fn apply_local(node: &CraqleNode, changes: Vec<MaterializedQuadChange>, bulk: bool) -> Batch {
    let mut result = None;
    for (graph, changes) in change_groups(changes) {
        let batch = if bulk {
            node.apply_changes_bulk_unchecked(&graph, changes)
        } else {
            node.apply_changes_unchecked(&graph, changes)
        }
        .expect("apply local write benchmark changes");
        if bulk {
            node.rebuild_graph_diagnostics(&graph)
                .expect("rebuild local write diagnostics");
        }
        result = Some(batch);
    }
    result.expect("write benchmark changes must not be empty")
}

fn apply_local_fixture(mut fixture: LocalFixture, bulk: bool) -> (LocalFixture, Batch) {
    let changes = std::mem::take(&mut fixture.changes);
    let result = apply_local(&fixture.node, changes, bulk);
    (fixture, result)
}

fn apply_durable(fixture: LocalFixture, bulk: bool) -> (LocalFixture, Batch) {
    let (fixture, batch) = apply_local_fixture(fixture, bulk);
    fixture
        .node
        .persist_fjall()
        .expect("persist durable write benchmark operation");
    (fixture, batch)
}

fn apply_concurrent_fixture(fixture: ConcurrentFixture) -> (ConcurrentFixture, Vec<Batch>) {
    let ConcurrentFixture {
        node,
        database,
        graphs,
        batches,
    } = fixture;
    let start = Arc::new(Barrier::new(batches.len()));
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(batches.len());
        for changes in batches {
            let node = Arc::clone(&node);
            let start = Arc::clone(&start);
            handles.push(scope.spawn(move || {
                start.wait();
                apply_local(&node, changes, true)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("join concurrent local write"))
            .collect()
    });
    (
        ConcurrentFixture {
            node,
            database,
            graphs,
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
    fixture_hash: &str,
) -> u64 {
    let LocalFixture {
        node,
        database,
        graphs,
        changes,
        ..
    } = fixture;
    let changed_rows = changes.len();
    let before = directory_bytes(&database.path().join("store"));
    let allocation = AllocationInterval::begin();
    let result = apply_local(&node, changes, bulk);
    black_box(result);
    let allocation = allocation.finish();
    assert_rows(&node, &graphs, expected_rows);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case={label} corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows={expected_rows} db_bytes={bytes} db_growth={} persistence=buffer \
         write_path=raw_unchecked_changes validation_reads=0 \
         source_keys_written={changed_rows} qv_keys_written={} \
         key_count_scope=logical_quad_rows_and_qv_orders \
         fixture_hash={fixture_hash} fixture_scope=preloaded_corpus \
         allocation_scope={} \
         allocations={} allocated_bytes={} peak_live_delta_bytes={} search_storage=memory",
        bytes.saturating_sub(before),
        changed_rows.saturating_mul(3),
        if bulk {
            "untimed_apply_and_diagnostics"
        } else {
            "untimed_apply"
        },
        allocation.allocations,
        allocation.allocated_bytes,
        allocation.peak_live_delta_bytes,
    );
    drop(node);
    drop(database);
    bytes
}

fn assert_delete_contract(config: CorpusConfig, triples: &[Triple], fixture_hash: &str) -> u64 {
    let fixture = delete_fixture(config, triples);
    let LocalFixture {
        node,
        database,
        graphs,
        changes,
        ..
    } = fixture;
    assert_rows(&node, &graphs, config.quads + 1);
    let before = directory_bytes(&database.path().join("store"));
    let allocation = AllocationInterval::begin();
    black_box(apply_local(&node, changes, false));
    let allocation = allocation.finish();
    assert_rows(&node, &graphs, config.quads);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case=single_delete corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows={} db_bytes={bytes} db_growth={} setup_live_rows={} persistence=buffer \
         write_path=raw_unchecked_changes validation_reads=0 \
         source_keys_written=1 qv_keys_written=3 \
         key_count_scope=logical_quad_rows_and_qv_orders \
         fixture_hash={fixture_hash} fixture_scope=preloaded_corpus allocation_scope=untimed_apply \
         allocations={} allocated_bytes={} peak_live_delta_bytes={} search_storage=memory",
        config.quads,
        bytes.saturating_sub(before),
        config.quads + 1,
        allocation.allocations,
        allocation.allocated_bytes,
        allocation.peak_live_delta_bytes,
    );
    drop(node);
    drop(database);
    bytes
}

fn assert_concurrent_contract(config: CorpusConfig, triples: &[Triple], fixture_hash: &str) -> u64 {
    let fixture = concurrent_fixture(config, triples);
    let ConcurrentFixture {
        node,
        database,
        graphs,
        batches,
    } = fixture;
    let changed_rows: usize = batches.iter().map(Vec::len).sum();
    let expected_rows = config.quads + batches.len() * CONCURRENT_ROWS_PER_WRITER;
    let before = directory_bytes(&database.path().join("store"));
    let start = Arc::new(Barrier::new(batches.len()));
    let allocation = AllocationInterval::begin();
    std::thread::scope(|scope| {
        for changes in batches {
            let node = Arc::clone(&node);
            let start = Arc::clone(&start);
            scope.spawn(move || {
                start.wait();
                apply_local(&node, changes, true);
            });
        }
    });
    let allocation = allocation.finish();
    assert_rows(&node, &graphs, expected_rows);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case=concurrent_local_writes corpus_version={CORPUS_VERSION} \
         seed={DEFAULT_SEED:#x} rows={expected_rows} writers={CONCURRENT_WRITERS} \
         rows_per_writer={CONCURRENT_ROWS_PER_WRITER} db_bytes={bytes} db_growth={} \
         persistence=buffer write_path=raw_unchecked_changes validation_reads=0 \
         source_keys_written={changed_rows} qv_keys_written={} \
         key_count_scope=logical_quad_rows_and_qv_orders \
         fixture_hash={fixture_hash} fixture_scope=preloaded_corpus \
         allocation_scope=untimed_apply_and_diagnostics \
         allocations={} allocated_bytes={} peak_live_delta_bytes={} search_storage=memory",
        bytes.saturating_sub(before),
        changed_rows.saturating_mul(3),
        allocation.allocations,
        allocation.allocated_bytes,
        allocation.peak_live_delta_bytes,
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
        fixture_hash,
    } = fixture;
    let before = directory_bytes(&database.path().join("peer_0/store"))
        + directory_bytes(&database.path().join("peer_1/store"));
    let allocation = AllocationInterval::begin();
    let moved = cluster
        .sync_pair(0, 1)
        .expect("apply untimed replicated merge contract");
    let allocation = allocation.finish();
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
         seed={DEFAULT_SEED:#x} rows_added=1 db_bytes={bytes} db_growth={} peers=2 \
         persistence=buffer path=CraqleCluster::sync_pair validation_reads=0 \
         source_keys_written=1 qv_keys_written=3 \
         key_count_scope=logical_quad_rows_and_qv_orders \
         fixture_hash={fixture_hash} fixture_scope=crate_metadata allocation_scope=untimed_sync_pair \
         allocations={} allocated_bytes={} peak_live_delta_bytes={} search_storage=memory",
        bytes.saturating_sub(before),
        allocation.allocations,
        allocation.allocated_bytes,
        allocation.peak_live_delta_bytes,
    );
    drop(cluster);
    drop(database);
    bytes
}

fn assert_sync_data(config: CorpusConfig, triples: &[Triple], fixture_hash: &str) -> u64 {
    let graph = GraphId::new(LOCAL_GRAPH);
    let fixture = local_fixture_mode(
        config,
        changes_for(&graph, triples, 0..1, true),
        CraqleFjallPersistMode::SyncData,
    );
    let before = directory_bytes(&fixture.database.path().join("store"));
    let allocation = AllocationInterval::begin();
    let (fixture, result) = apply_durable(fixture, false);
    let allocation = allocation.finish();
    black_box(result);
    assert_rows(&fixture.node, &fixture.graphs, config.quads + 1);
    let bytes = settle_and_size(&fixture.node, &fixture.database);
    println!(
        "index_write_cost case=single_insert_sync_data corpus_version={CORPUS_VERSION} \
         seed={DEFAULT_SEED:#x} rows={} db_bytes={bytes} db_growth={} persistence=sync_data \
         write_path=raw_unchecked_changes validation_reads=0 source_keys_written=1 \
         qv_keys_written=3 key_count_scope=logical_quad_rows_and_qv_orders \
         fixture_hash={fixture_hash} fixture_scope=preloaded_corpus \
         allocation_scope=untimed_apply_plus_persist allocations={} allocated_bytes={} \
         peak_live_delta_bytes={} search_storage=memory",
        config.quads + 1,
        bytes.saturating_sub(before),
        allocation.allocations,
        allocation.allocated_bytes,
        allocation.peak_live_delta_bytes,
    );
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
    let config = write_config();
    let triples = write_corpus(config);
    let fixture_hash = corpus_hash(config);
    let sample_size = env_sample_size();
    println!(
        "index_write_cost metadata: corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         corpus_quads={} corpus_graphs={} duplicate_percent={} operation_rows={} \
         fixture_hash={fixture_hash} local_cases=single_insert,single_insert_union_proof_current,\
         single_insert_union_proof_unknown,single_delete,batch_100,batch_10000,\
         single_insert_sync_data,single_insert_sync_data_union_proof_current,\
         single_insert_sync_data_union_proof_unknown,concurrent_local_writes \
         replicated_case=replicated_merge \
         replicated_fixture_scope=crate_metadata \
         setup=iter_batched_per_iteration preload=streamed_bounded_buffer persistence=buffer \
         durable_case=single_insert_sync_data durable_persistence=sync_data \
         durable_timed_scope=apply_plus_persist \
         local_write_path=raw_unchecked_changes replicated_path=CraqleCluster::sync_pair \
         key_count_scope=logical_quad_rows_and_qv_orders \
         validation_reads=0 search_storage=memory sample_size={sample_size} \
         criterion_semantics=sample_size_samples_per_case_batch_size_per_iteration",
        config.quads,
        config.graphs,
        config.duplicate_percent,
        triples.len()
    );

    // These are deliberately untimed. They prove the operation contracts and
    // provide one comparable database-size sample for each case.
    assert_local_contract(
        "single_insert",
        local_fixture(
            config,
            changes_for(&GraphId::new(LOCAL_GRAPH), &triples, 0..1, true),
        ),
        config.quads + 1,
        false,
        &fixture_hash,
    );
    assert_delete_contract(config, &triples, &fixture_hash);
    assert_local_contract(
        "batch_100",
        local_fixture(
            config,
            changes_for(&GraphId::new(LOCAL_GRAPH), &triples, 0..100, true),
        ),
        config.quads + 100,
        true,
        &fixture_hash,
    );
    assert_local_contract(
        "batch_10000",
        local_fixture(
            config,
            changes_for(&GraphId::new(LOCAL_GRAPH), &triples, 0..10_000, true),
        ),
        config.quads + triples.len(),
        true,
        &fixture_hash,
    );
    assert_sync_data(config, &triples, &fixture_hash);
    assert_concurrent_contract(config, &triples, &fixture_hash);
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
                local_fixture(config, changes_for(&graph, &triples, 0..1, true))
            },
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("single_insert_union_proof_current", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture_union_proof_state(
                    config,
                    changes_for(&graph, &triples, 0..1, true),
                    CraqleFjallPersistMode::Buffer,
                    false,
                )
            },
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("single_insert_union_proof_unknown", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture_union_proof_state(
                    config,
                    changes_for(&graph, &triples, 0..1, true),
                    CraqleFjallPersistMode::Buffer,
                    true,
                )
            },
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_delete", |b| {
        b.iter_batched(
            || delete_fixture(config, &triples),
            |fixture| apply_local_fixture(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(100));
    group.bench_function("batch_100", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture(config, changes_for(&graph, &triples, 0..100, true))
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
                local_fixture(config, changes_for(&graph, &triples, 0..10_000, true))
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
            || concurrent_fixture(config, &triples),
            apply_concurrent_fixture,
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_insert_sync_data", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture_mode(
                    config,
                    changes_for(&graph, &triples, 0..1, true),
                    CraqleFjallPersistMode::SyncData,
                )
            },
            |fixture| apply_durable(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("single_insert_sync_data_union_proof_current", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture_union_proof_state(
                    config,
                    changes_for(&graph, &triples, 0..1, true),
                    CraqleFjallPersistMode::SyncData,
                    false,
                )
            },
            |fixture| apply_durable(fixture, false),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("single_insert_sync_data_union_proof_unknown", |b| {
        b.iter_batched(
            || {
                let graph = GraphId::new(LOCAL_GRAPH);
                local_fixture_union_proof_state(
                    config,
                    changes_for(&graph, &triples, 0..1, true),
                    CraqleFjallPersistMode::SyncData,
                    true,
                )
            },
            |fixture| apply_durable(fixture, false),
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
