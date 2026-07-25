//! WS0 concurrency guarantees, exercised end to end through the public
//! `CraqleNode` API.
//!
//! The guard-level proofs — parallel `add_quad` + `commit` keeping the dot set
//! intact, the self-guarding store functions not deadlocking, and the
//! `#[cfg(test)]` corrupt-index hook showing that the detecting commit repairs
//! the index — live in `src/internal/store.rs`'s unit tests, because
//! `craqle::store` is a private module and `GraphCommitGuard` is `pub(crate)`.
//! Integration tests cannot name either. What they can do is show the same
//! guarantees from the outside: concurrent writers lose nothing, and concurrent
//! graph-lifecycle calls make progress.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use craqle::{CraqleNode, EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange, vocab};

/// Generous enough that a slow machine never trips it, short enough that a real
/// deadlock fails the run instead of hanging it.
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(180);

fn public_policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["*".to_string()],
    }
}

fn named(iri: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(iri))
}

/// Write triples verbatim. These tests are about write concurrency, not about
/// crate structure, so they skip the structural validation that would reject a
/// bare `schema:name` triple with no surrounding crate.
fn write_unchecked(
    node: &CraqleNode,
    graph: &GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
) {
    let changes = triples
        .into_iter()
        .map(|(subject, predicate, object)| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject,
            predicate,
            object,
        })
        .collect();
    node.apply_changes_unchecked(graph, changes).unwrap();
}

/// Concurrent writers on one graph must all land: every insert gets its own
/// dot, and the graph clock accounts for every committed batch (G1, G2).
///
/// Today the local write path is still serialized by `ReplicationEngine`'s
/// engine-wide `local_commit_lock`. WS1-T2 deletes that lock in favour of the
/// per-graph commit guard, so this test is the end-to-end regression guard for
/// that swap: if the guard is not adopted at every read→write site, the counter
/// mint and the dot-set read-modify-write interleave and adds are lost.
#[test]
fn parallel_inserts_on_one_graph_lose_nothing() {
    const WRITERS: usize = 6;
    const INSERTS_PER_WRITER: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:concurrency:one-graph");
    let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
    node.import_graph_policy(&graph, public_policy()).unwrap();

    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            scope.spawn(move || {
                let name = EncodedTerm::from_named_node(&vocab::schema_name());
                for index in 0..INSERTS_PER_WRITER {
                    write_unchecked(
                        &node,
                        &graph,
                        vec![(
                            named(&format!("urn:parallel:w{writer}-e{index}")),
                            name.clone(),
                            EncodedTerm(format!("\"writer {writer} entity {index}\"")),
                        )],
                    );
                }
            });
        }
    });

    let expected = (WRITERS * INSERTS_PER_WRITER) as u64;
    let (count, _, _) = node.graph_fingerprint(&graph).unwrap();
    assert_eq!(expected, count, "concurrent inserts lost quads");

    let snapshot = node.graph_snapshot(&graph).unwrap();
    assert_eq!(expected as usize, snapshot.quads.len());
    assert!(
        snapshot.quads.iter().all(|quad| quad.dots.len() == 1),
        "each insert must contribute exactly one dot"
    );

    // Every dot is unique, and the clock covers all of them.
    let mut dots: Vec<(craqle::ActorId, u64)> = snapshot
        .quads
        .iter()
        .flat_map(|quad| quad.dots.iter().map(|dot| (dot.actor, dot.counter)))
        .collect();
    dots.sort();
    let unique = dots.len();
    dots.dedup();
    assert_eq!(unique, dots.len(), "two inserts shared a dot");

    let clock = node.vector_clock(&graph).unwrap();
    let highest: u64 = clock.0.values().copied().max().unwrap_or(0);
    assert!(
        highest >= expected,
        "the graph clock must cover every committed batch: {highest} < {expected}"
    );
}

/// Writers on different graphs share the 64 commit-lock shards, so they can
/// contend even though they are logically independent. That contention must
/// only serialize them, never lose writes.
#[test]
fn parallel_inserts_across_graphs_lose_nothing() {
    const GRAPHS: usize = 8;
    const INSERTS_PER_GRAPH: usize = 15;

    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
    let graphs: Vec<GraphId> = (0..GRAPHS)
        .map(|index| GraphId::new(&format!("urn:test:concurrency:multi-{index}")))
        .collect();
    for graph in &graphs {
        node.import_graph_policy(graph, public_policy()).unwrap();
    }

    std::thread::scope(|scope| {
        for graph in &graphs {
            let node = Arc::clone(&node);
            scope.spawn(move || {
                let name = EncodedTerm::from_named_node(&vocab::schema_name());
                for index in 0..INSERTS_PER_GRAPH {
                    write_unchecked(
                        &node,
                        graph,
                        vec![(
                            named(&format!("urn:multi:{}-e{index}", graph.as_str())),
                            name.clone(),
                            EncodedTerm(format!("\"entity {index}\"")),
                        )],
                    );
                }
            });
        }
    });

    for graph in &graphs {
        let (count, _, _) = node.graph_fingerprint(graph).unwrap();
        assert_eq!(INSERTS_PER_GRAPH as u64, count, "lost quads in {graph}");
    }
}

/// The self-guarding node operations take a graph commit guard internally.
/// Calling them concurrently — including on graphs that collide on a lock
/// shard — must make progress rather than deadlock on the non-reentrant mutex.
#[test]
fn concurrent_graph_lifecycle_calls_do_not_deadlock() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 10;

    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
    let (tx, rx) = mpsc::channel();

    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let node = Arc::clone(&node);
            let tx = tx.clone();
            std::thread::spawn(move || {
                // Two threads per graph, so each graph's shard is contended.
                let graph = GraphId::new(&format!("urn:test:concurrency:lifecycle-{}", thread % 4));
                let name = EncodedTerm::from_named_node(&vocab::schema_name());
                for round in 0..ROUNDS {
                    node.import_graph_policy(&graph, public_policy()).unwrap();
                    write_unchecked(
                        &node,
                        &graph,
                        vec![(
                            named(&format!("urn:lifecycle:t{thread}-r{round}")),
                            name.clone(),
                            EncodedTerm(format!("\"round {round}\"")),
                        )],
                    );
                    let _ = node.graph_diagnostics(&graph).unwrap();
                    let _ = node.graph_policy(&graph).unwrap();
                    node.delete_graph_unchecked(&graph).unwrap();
                }
                tx.send(thread).unwrap();
            })
        })
        .collect();
    drop(tx);

    for _ in 0..THREADS {
        rx.recv_timeout(PROGRESS_TIMEOUT)
            .expect("concurrent graph-lifecycle calls deadlocked");
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

/// Reads must stay consistent with writes while both run: a diagnostics read
/// never observes a set that disagrees with the graph it is reading, and never
/// blocks writers indefinitely.
#[test]
fn concurrent_reads_and_writes_stay_consistent() {
    const WRITES: usize = 60;

    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:concurrency:read-write");
    let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
    node.import_graph_policy(&graph, public_policy()).unwrap();

    std::thread::scope(|scope| {
        let writer = {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            scope.spawn(move || {
                let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
                let media_object = EncodedTerm::from_named_node(&vocab::schema_media_object());
                for index in 0..WRITES {
                    write_unchecked(
                        &node,
                        &graph,
                        vec![(
                            named(&format!("urn:rw:orphan-{index}")),
                            rdf_type.clone(),
                            media_object.clone(),
                        )],
                    );
                }
            })
        };

        for _ in 0..3 {
            let node = Arc::clone(&node);
            let graph = graph.clone();
            scope.spawn(move || {
                for _ in 0..WRITES {
                    let diagnostics = node.graph_diagnostics(&graph).unwrap();
                    let (count, _, _) = node.graph_fingerprint(&graph).unwrap();
                    // Every entity written by the writer is an orphan, so the
                    // reported orphan count can never exceed the quad count.
                    assert!(
                        diagnostics.orphaned_entities.len() as u64 <= count.max(1),
                        "diagnostics reported more orphans than the graph has quads"
                    );
                }
            });
        }

        writer.join().unwrap();
    });

    assert_eq!(
        WRITES,
        node.graph_diagnostics(&graph).unwrap().orphaned_entities.len(),
        "the final diagnostics must describe the final graph state"
    );
}
