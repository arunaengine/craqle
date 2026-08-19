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
    ActorId, Batch, CraqleNode, CraqleOptions, CreateCrateRequest, EncodedTerm, GrantAuthorizer,
    GraphId, GraphPolicy, MaterializedQuadChange, PermissionGrant, PermissionLevel, SearchStorage,
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
    cluster
        .peer(0)
        .flush_search_updates()
        .expect("settle merge benchmark setup");
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
    assert_row_count(&node, &graph, expected_rows);
    let bytes = settle_and_size(&node, &database);
    println!(
        "index_write_cost case={label} corpus_version={CORPUS_VERSION} seed={DEFAULT_SEED:#x} \
         rows={expected_rows} db_bytes={bytes} search_storage=memory"
    );
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
