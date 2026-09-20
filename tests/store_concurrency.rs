//! Verifies public-node concurrency remains lossless and makes progress.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

mod support;

use crate::support::TestWriteExt as _;

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

/// Writes bare triples so structural validation does not mask concurrency.
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

/// Parallel inserts must keep unique dots and complete graph-clock coverage.
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

        // Contiguous actor counters make their sum the exact minted-dot count.
        // A maximum would miss lost intermediate advances.
        let clock = node.vector_clock(&graph).unwrap();
        let covered: u64 = clock.0.values().sum();
        assert_eq!(
            unique as u64, covered,
            "the graph clock must account for every minted dot exactly once: {clock:?}"
        );
    });
}

/// Commit-shard contention across graphs may serialize but never lose writes.
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
/// Concurrent calls on colliding graph-lock shards must still make progress.
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
                    let name = EncodedTerm::from_named_node(&vocab::schema_name());
                    for round in 0..ROUNDS {
                        let graph = GraphId::new(&format!(
                            "urn:test:concurrency:lifecycle-{thread}-{round}"
                        ));
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

/// A wide Fjall batch exposes any gap between quad publication and the clock
/// used to validate diagnostics freshness.
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
                        // Each entity contributes one quad and one orphan.
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

/// Races a fingerprint against wide batches: every observed quad count must
/// be a whole number of batches, since each batch commits atomically.
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

/// Races snapshots against wide batches: with one dot minted per batch, the
/// clock total times the batch size must equal the quad count observed.
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

/// Concurrent diagnostics must match an observed graph state without blocking writes.
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
                        // Fingerprints bracket the growing graph state observed by
                        // diagnostics; a one-sided bound would permit stale emptiness.
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
