//! WS0 concurrency guarantees, exercised end to end through the public
//! `CraqleNode` API.
//!
//! The guard-level proofs — parallel `insert_quad` + `commit` keeping the dot set
//! intact, the self-guarding store functions not deadlocking, and the
//! `#[cfg(test)]` corrupt-index hook showing that the detecting commit repairs
//! the index — live in `src/internal/store.rs`'s unit tests, because
//! `craqle::store` is a private module and `GraphCommitGuard` is `pub(crate)`.
//! Integration tests cannot name either. What they can do is show the same
//! guarantees from the outside: concurrent writers lose nothing, and concurrent
//! graph-lifecycle calls make progress.

mod support;

use std::sync::Arc;
use std::sync::mpsc;

use craqle::{CraqleNode, EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange, vocab};

use crate::support::{WATCHDOG_TIMEOUT as PROGRESS_TIMEOUT, with_watchdog};

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
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject,
                predicate,
                object,
            },
        )
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
fn parallel_inserts_persist() {
    with_watchdog("parallel_inserts_persist", || {
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

        // Exact, not `max >= expected`: counters are minted contiguously from 1
        // per actor, so the clock entries sum to the number of dots. Taking the
        // maximum only catches a lost advance when the *highest* counter is the
        // one that was lost — every other interleaving slips through.
        let clock = node.vector_clock(&graph).unwrap();
        let covered: u64 = clock.0.values().sum();
        assert_eq!(
            unique as u64, covered,
            "the graph clock must account for every minted dot exactly once: {clock:?}"
        );
    });
}

/// Writers on different graphs share the 64 commit-lock shards, so they can
/// contend even though they are logically independent. That contention must
/// only serialize them, never lose writes.
#[test]
fn multigraph_inserts_persist() {
    with_watchdog("multigraph_inserts_persist", || {
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
            let clock = node.vector_clock(graph).unwrap();
            let covered: u64 = clock.0.values().sum();
            assert_eq!(
                INSERTS_PER_GRAPH as u64, covered,
                "shard contention must not cost {graph} a clock entry: {clock:?}"
            );
        }
    });
}

/// The self-guarding node operations take a graph commit guard internally.
/// Calling them concurrently — including on graphs that collide on a lock
/// shard — must make progress rather than deadlock on the non-reentrant mutex.
#[test]
fn lifecycle_never_deadlocks() {
    with_watchdog("lifecycle_never_deadlocks", || {
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
                    let graph =
                        GraphId::new(&format!("urn:test:concurrency:lifecycle-{}", thread % 4));
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
    });
}

/// fjall applies a write batch item by item, so a reader can see a commit's
/// quads while the same batch's clock key has not landed. A freshness check
/// reading the durable clock then matches the *previous* orphan record and
/// serves a pre-write orphan set as current (G6). One wide batch holds that
/// gap open long enough to catch it reliably; `reads_stay_consistent` only
/// catches it a few times in a hundred runs.
#[test]
fn diagnostics_never_lag() {
    with_watchdog("diagnostics_never_lag", || {
        const PER_BATCH: usize = 400;
        const ROUNDS: usize = 5;

        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:concurrency:wide-batch");
        let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
        node.import_graph_policy(&graph, public_policy()).unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    while !done.load(std::sync::atomic::Ordering::Relaxed) {
                        // One quad per entity and one orphan per quad, so the
                        // orphan set a diagnostics read returns is the quad
                        // count of the graph it read.
                        let (before, _, _) = node.graph_fingerprint(&graph).unwrap();
                        let orphaned = node.graph_diagnostics(&graph).unwrap().orphaned_entities;
                        let (after, _, _) = node.graph_fingerprint(&graph).unwrap();
                        let observed = orphaned.len() as u64;
                        assert!(
                            (before..=after).contains(&observed),
                            "diagnostics read {observed} orphans, outside the \
                             {before}..={after} quads the graph held during the read"
                        );
                    }
                });
            }

            let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
            let media_object = EncodedTerm::from_named_node(&vocab::schema_media_object());
            for round in 0..ROUNDS {
                let triples = (0..PER_BATCH)
                    .map(|index| {
                        (
                            named(&format!("urn:wide:r{round}-e{index}")),
                            rdf_type.clone(),
                            media_object.clone(),
                        )
                    })
                    .collect();
                write_unchecked(&node, &graph, triples);
            }
            done.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        assert_eq!(
            PER_BATCH * ROUNDS,
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .len(),
            "the final diagnostics must describe the final graph state"
        );
    });
}

/// A fingerprint races wide batches: each batch commits atomically behind the
/// index lock, so every observed quad count must be a whole number of batches.
/// A scan bypassing that lock read torn counts mid-batch.
#[test]
fn fingerprints_never_tear() {
    with_watchdog("fingerprints_never_tear", || {
        const PER_BATCH: usize = 400;
        const ROUNDS: usize = 8;

        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:concurrency:fingerprint-tear");
        let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
        node.import_graph_policy(&graph, public_policy()).unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    while !done.load(std::sync::atomic::Ordering::Relaxed) {
                        let (count, _, _) = node.graph_fingerprint(&graph).unwrap();
                        assert_eq!(
                            0,
                            count % PER_BATCH as u64,
                            "fingerprint observed a torn batch: {count} quads"
                        );
                    }
                });
            }

            let name = EncodedTerm::from_named_node(&vocab::schema_name());
            for round in 0..ROUNDS {
                let triples = (0..PER_BATCH)
                    .map(|index| {
                        (
                            named(&format!("urn:tear:r{round}-e{index}")),
                            name.clone(),
                            EncodedTerm(format!("\"entity {index}\"")),
                        )
                    })
                    .collect();
                write_unchecked(&node, &graph, triples);
            }
            done.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let (count, _, _) = node.graph_fingerprint(&graph).unwrap();
        assert_eq!((PER_BATCH * ROUNDS) as u64, count);
    });
}

/// A snapshot races wide batches: it must be internally consistent, meaning
/// its clock and its quads describe the same commit. With one dot minted per
/// batch, the clock total times the batch size must equal the quad count.
#[test]
fn snapshots_never_tear() {
    with_watchdog("snapshots_never_tear", || {
        const PER_BATCH: usize = 400;
        const ROUNDS: usize = 16;

        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:concurrency:snapshot-tear");
        let node = Arc::new(CraqleNode::open(dir.path()).unwrap());
        node.import_graph_policy(&graph, public_policy()).unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    while !done.load(std::sync::atomic::Ordering::Relaxed) {
                        let snapshot = node.graph_snapshot(&graph).unwrap();
                        let quads = snapshot.quads.len() as u64;
                        let batches: u64 = snapshot.clock.0.values().sum();
                        assert_eq!(
                            batches * PER_BATCH as u64,
                            quads,
                            "snapshot clock covers {batches} batches but holds {quads} quads"
                        );
                    }
                });
            }

            let name = EncodedTerm::from_named_node(&vocab::schema_name());
            for round in 0..ROUNDS {
                let triples = (0..PER_BATCH)
                    .map(|index| {
                        (
                            named(&format!("urn:snap:r{round}-e{index}")),
                            name.clone(),
                            EncodedTerm(format!("\"entity {index}\"")),
                        )
                    })
                    .collect();
                write_unchecked(&node, &graph, triples);
            }
            done.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let snapshot = node.graph_snapshot(&graph).unwrap();
        assert_eq!(PER_BATCH * ROUNDS, snapshot.quads.len());
    });
}

/// Reads must stay consistent with writes while both run: a diagnostics read
/// never observes a set that disagrees with the graph it is reading, and never
/// blocks writers indefinitely.
#[test]
fn reads_stay_consistent() {
    with_watchdog("reads_stay_consistent", || {
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
                        // Every write contributes exactly one quad and exactly
                        // one orphan, so the orphan set a diagnostics read
                        // returns *is* the quad count of the graph it read.
                        // The writer only ever grows that count, so sandwiching
                        // the read between two fingerprints pins the equality
                        // without pinning which instant was observed. A bound of
                        // `<= count` alone is satisfied by arbitrarily stale
                        // diagnostics, including an empty set.
                        let (before, _, _) = node.graph_fingerprint(&graph).unwrap();
                        let orphaned = node.graph_diagnostics(&graph).unwrap().orphaned_entities;
                        let (after, _, _) = node.graph_fingerprint(&graph).unwrap();
                        let observed = orphaned.len() as u64;
                        assert!(
                            (before..=after).contains(&observed),
                            "diagnostics read {observed} orphans, outside the \
                             {before}..={after} quads the graph held during the read"
                        );
                    }
                });
            }

            writer.join().unwrap();
        });

        assert_eq!(
            WRITES,
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .len(),
            "the final diagnostics must describe the final graph state"
        );
    });
}
