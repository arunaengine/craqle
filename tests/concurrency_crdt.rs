mod support;

/// Concurrency guarantees of the write path (finding K1).
///
/// Every test here drives *parallel* writers at one graph. Before the store's
/// per-graph commit guard was adopted by `ReplicationEngine`, each of these
/// races could mint a duplicate dot, drop an add, or lose a vector-clock entry.
#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    use craqle::MaterializedQuadChange as Change;
    use craqle::*;

    use crate::support::*;

    /// Generous enough that a slow machine never trips it, short enough that a
    /// lock-order regression fails the run instead of hanging it.
    const PROGRESS_TIMEOUT: Duration = Duration::from_secs(180);

    const WRITERS: usize = 8;
    /// Writes per thread. More rounds means a wider window for a lost update.
    const ROUNDS: usize = 8;
    const TOTAL_WRITES: usize = WRITERS * ROUNDS;

    fn keyword_change(graph: &GraphId, keyword: &str) -> (EncodedTerm, EncodedTerm, EncodedTerm) {
        (
            EncodedTerm::from_named_node(&graph.0),
            EncodedTerm::from_named_node(&vocab::schema_keywords()),
            literal_term(keyword),
        )
    }

    /// Every writer thread inserts `ROUNDS` distinct keywords into the same
    /// graph in parallel. Returns every replication batch that was committed.
    fn insert_keywords_in_parallel(node: &CraqleNode, graph: &GraphId, prefix: &str) -> Vec<Batch> {
        std::thread::scope(|scope| {
            let writers: Vec<_> = (0..WRITERS)
                .map(|writer| {
                    scope.spawn(move || {
                        (0..ROUNDS)
                            .map(|round| {
                                node.insert_quads(
                                    graph,
                                    vec![keyword_change(
                                        graph,
                                        &format!("{prefix}-{writer}-{round}"),
                                    )],
                                )
                                .unwrap()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            writers
                .into_iter()
                .flat_map(|writer| writer.join().unwrap())
                .collect()
        })
    }

    fn keyword_quads(node: &CraqleNode, graph: &GraphId) -> Vec<SnapshotQuadState> {
        let keywords = EncodedTerm::from_named_node(&vocab::schema_keywords());
        node.graph_snapshot(graph)
            .unwrap()
            .quads
            .into_iter()
            .filter(|quad| quad.predicate == keywords)
            .collect()
    }

    /// K1a — two commits on one graph must never mint the same `(actor, counter)`.
    ///
    /// Each parallel insert is its own batch and therefore carries exactly one
    /// dot; a duplicated counter would show up as two quads sharing a dot.
    #[test]
    fn dots_unique_under_parallel_same_graph_writes() {
        with_watchdog("dots_unique_under_parallel_same_graph_writes", || {
            let (_tmp, net) = setup_network(1);
            let graph = GraphId::new("urn:test:crdt-dots");
            create_test_crate(&net, 0, &graph);

            insert_keywords_in_parallel(net.peer(0), &graph, "dot");

            let quads = keyword_quads(net.peer(0), &graph);
            assert_eq!(quads.len(), TOTAL_WRITES, "one keyword quad per write");

            let mut seen = HashSet::new();
            for quad in &quads {
                assert_eq!(quad.dots.len(), 1, "single-change batch carries one dot");
                assert!(
                    seen.insert((quad.dots[0].actor, quad.dots[0].counter)),
                    "two distinct adds share dot {:?}",
                    quad.dots[0]
                );
            }
        });
    }

    /// K1b — no add may be lost when writers race on the same graph.
    #[test]
    fn no_lost_adds_under_parallel_insert() {
        with_watchdog("no_lost_adds_under_parallel_insert", || {
            let (_tmp, net) = setup_network(1);
            let graph = GraphId::new("urn:test:crdt-adds");
            create_test_crate(&net, 0, &graph);

            insert_keywords_in_parallel(net.peer(0), &graph, "add");

            let objects: HashSet<String> = keyword_quads(net.peer(0), &graph)
                .into_iter()
                .map(|quad| quad.object.0)
                .collect();
            for writer in 0..WRITERS {
                for round in 0..ROUNDS {
                    let expected = literal_term(&format!("add-{writer}-{round}")).0;
                    assert!(objects.contains(&expected), "lost add for {expected}");
                }
            }
        });
    }

    /// K1c — every applied batch's dot ends up in the graph's vector clock (G2).
    ///
    /// A read-modify-write race on the clock silently drops the losing writer's
    /// counter, leaving the clock behind the log head.
    #[test]
    fn vector_clock_not_lost_under_concurrent_commits() {
        with_watchdog("vector_clock_not_lost_under_concurrent_commits", || {
            let (_tmp, net) = setup_network(1);
            let graph = GraphId::new("urn:test:crdt-clock");
            create_test_crate(&net, 0, &graph);

            let batches = insert_keywords_in_parallel(net.peer(0), &graph, "clock");
            let clock = net.peer(0).vector_clock(&graph).unwrap();

            let mut minted = HashSet::new();
            for batch in &batches {
                let dot = Dot {
                    actor: batch.actor,
                    counter: batch.counter,
                };
                assert!(
                    minted.insert(dot),
                    "two batches minted the same dot {dot:?}"
                );
                assert!(
                    clock.contains(&dot),
                    "clock lost the dot of an applied batch: {dot:?}"
                );
            }
            assert_eq!(minted.len(), TOTAL_WRITES);

            // The clock must also cover every dot actually written to the store.
            for quad in net.peer(0).graph_snapshot(&graph).unwrap().quads {
                for dot in quad.dots {
                    assert!(clock.contains(&dot), "clock is missing live dot {dot:?}");
                }
            }
        });
    }

    /// G1 — a `Remove` deletes exactly the dots it witnessed and no others.
    ///
    /// Both peers add the same triple while partitioned, so it carries two dots.
    /// Peer 0 then deletes it *before* healing, witnessing only its own dot, so
    /// peer 1's concurrent add must survive the merge on both replicas.
    #[test]
    fn remove_kills_only_witnessed_dots() {
        with_watchdog("remove_kills_only_witnessed_dots", || {
            let (_tmp, mut net) = setup_network(2);
            let graph = GraphId::new("urn:test:crdt-witness");
            create_test_crate(&net, 0, &graph);
            net.sync_until_converged(10).unwrap();

            net.partition(0, 1);
            keyword_insert(net.peer(0), &graph, "contested");
            keyword_insert(net.peer(1), &graph, "contested");

            // Still partitioned: peer 0 has only ever observed its own add.
            keyword_delete(net.peer(0), &graph, "contested");
            assert!(
                !graph_has_keyword(&net, 0, &graph, "contested"),
                "the witnessed add must be gone locally"
            );

            net.heal(0, 1);
            net.sync_until_converged(10).unwrap();

            for peer in 0..2 {
                assert!(
                    graph_has_keyword(&net, peer, &graph, "contested"),
                    "peer {peer} killed a dot it never witnessed"
                );
            }
        });
    }

    /// Concurrent local writes in sync mode still converge across peers, and
    /// both replicas agree on the derived diagnostics (G4, G6).
    ///
    /// Convergence alone is not the guarantee: two peers that both lost the same
    /// add agree perfectly. The quad count is asserted against the state before
    /// the writes plus one quad per write, so completeness is pinned as well.
    #[test]
    fn sync_mode_concurrent_writes_converge() {
        with_watchdog("sync_mode_concurrent_writes_converge", || {
            let (_tmp, net) = setup_network(2);
            let graph = GraphId::new("urn:test:crdt-converge");
            create_test_crate(&net, 0, &graph);
            net.sync_until_converged(10).unwrap();
            let (seeded, _, _) = net.peer(0).graph_fingerprint(&graph).unwrap();

            insert_keywords_in_parallel(net.peer(0), &graph, "converge");
            net.sync_until_converged(20).unwrap();

            let fingerprint = net.peer(0).graph_fingerprint(&graph).unwrap();
            assert_eq!(
                fingerprint,
                net.peer(1).graph_fingerprint(&graph).unwrap(),
                "peers diverged after concurrent local writes"
            );
            assert_eq!(
                seeded + TOTAL_WRITES as u64,
                fingerprint.0,
                "the peers converged on an incomplete graph"
            );
            assert_eq!(
                net.peer(0).graph_diagnostics(&graph).unwrap(),
                net.peer(1).graph_diagnostics(&graph).unwrap(),
                "peers disagree on derived diagnostics"
            );
            assert!(!net.peer(0).graph_diagnostics(&graph).unwrap().has_orphans());
        });
    }

    /// K1d — the in-memory query indexes must equal the durable quad state after
    /// a concurrent mix of inserts and deletes, **without** a restart to repair
    /// them.
    #[test]
    fn index_matches_store_after_concurrent_churn() {
        with_watchdog("index_matches_store_after_concurrent_churn", || {
            let (_tmp, net) = setup_network(1);
            let graph = GraphId::new("urn:test:crdt-churn");
            create_test_crate(&net, 0, &graph);
            let node = net.peer(0);

            // Seed the keywords the deleting threads will race against.
            for writer in 0..WRITERS {
                for round in 0..ROUNDS {
                    keyword_insert(node, &graph, &format!("churn-{writer}-{round}"));
                }
            }

            std::thread::scope(|scope| {
                for writer in 0..WRITERS {
                    let graph = &graph;
                    scope.spawn(move || {
                        for round in 0..ROUNDS {
                            keyword_delete(node, graph, &format!("churn-{writer}-{round}"));
                            keyword_insert(node, graph, &format!("fresh-{writer}-{round}"));
                        }
                    });
                }
            });

            let keywords = EncodedTerm::from_named_node(&vocab::schema_keywords());
            let stored: HashSet<String> = node
                .graph_snapshot(&graph)
                .unwrap()
                .quads
                .into_iter()
                .filter(|quad| quad.predicate == keywords)
                .map(|quad| quad.object.0)
                .collect();

            let query = format!(
                "SELECT ?k WHERE {{ GRAPH <{}> {{ <{}> schema:keywords ?k }} }}",
                graph.as_str(),
                graph.as_str()
            );
            let indexed: HashSet<String> =
                solution_rows(node.query(&GrantAuthorizer::default(), &query).unwrap())
                    .into_iter()
                    .map(|row| row["k"].0.clone())
                    .collect();

            assert_eq!(
                indexed, stored,
                "query indexes drifted from the durable quad state"
            );
            assert_eq!(
                stored.len(),
                TOTAL_WRITES,
                "each churn cycle leaves exactly one keyword"
            );
        });
    }

    // ── F2: validation runs outside the commit guard ────────────────────────

    /// Racing pairs. Each pair needs both writers to validate before either
    /// commits, so a handful of pairs is not enough to be sure of hitting it.
    const RACE_ROUNDS: usize = 24;
    /// Filler entities, purely to widen the validation window.
    const FILLER_ENTITIES: usize = 400;

    /// An owned node: a detached racing thread outlives the test body, so it
    /// cannot borrow a cluster peer.
    fn standalone_node(dir: &tempfile::TempDir) -> Arc<CraqleNode> {
        Arc::new(
            CraqleNode::open_with_options(
                dir.path(),
                CraqleOptions::new().with_search_storage(SearchStorage::Memory),
            )
            .unwrap(),
        )
    }

    fn entity(graph: &GraphId, name: &str) -> EncodedTerm {
        EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(format!(
            "{}/{name}",
            graph.as_str()
        )))
    }

    fn insert(graph: &GraphId, triple: (EncodedTerm, EncodedTerm, EncodedTerm)) -> Change {
        Change::Insert {
            graph: graph.clone(),
            subject: triple.0,
            predicate: triple.1,
            object: triple.2,
        }
    }

    /// `root ─hasPart→ p1 ─hasPart→ x ←hasPart─ p2 ←hasPart─ root`, so cutting
    /// either edge into `x` on its own leaves it reachable.
    fn two_parent_fixture(graph: &GraphId, round: usize) -> Vec<Change> {
        let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let name = EncodedTerm::from_named_node(&vocab::schema_name());
        let dataset = EncodedTerm::from_named_node(&vocab::schema_dataset());
        let media = EncodedTerm::from_named_node(&vocab::schema_media_object());
        let root = EncodedTerm::from_named_node(&graph.0);
        let child = entity(graph, &format!("x-{round}"));

        let mut changes = vec![
            insert(graph, (child.clone(), rdf_type.clone(), media)),
            insert(
                graph,
                (
                    child.clone(),
                    name.clone(),
                    literal_term(&format!("child {round}")),
                ),
            ),
        ];
        for parent in 0..2 {
            let parent = entity(graph, &format!("p{parent}-{round}"));
            changes.extend([
                insert(graph, (parent.clone(), rdf_type.clone(), dataset.clone())),
                insert(
                    graph,
                    (
                        parent.clone(),
                        name.clone(),
                        literal_term(&format!("parent {round}")),
                    ),
                ),
                insert(graph, (root.clone(), has_part.clone(), parent.clone())),
                insert(graph, (parent, has_part.clone(), child.clone())),
            ]);
        }
        changes
    }

    fn cut_parent_edge(graph: &GraphId, round: usize, parent: usize) -> Change {
        Change::Delete {
            graph: graph.clone(),
            subject: entity(graph, &format!("p{parent}-{round}")),
            predicate: EncodedTerm::from_named_node(&vocab::schema_has_part()),
            object: entity(graph, &format!("x-{round}")),
        }
    }

    /// The orphans the graph state actually implies: a child of the fixture is
    /// orphaned exactly when both of its `hasPart` edges are gone.
    fn expected_orphans(node: &CraqleNode, graph: &GraphId) -> BTreeSet<String> {
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let edges: HashSet<(String, String)> = node
            .graph_snapshot(graph)
            .unwrap()
            .quads
            .into_iter()
            .filter(|quad| quad.predicate == has_part)
            .map(|quad| (quad.subject.0, quad.object.0))
            .collect();

        let mut orphans = BTreeSet::new();
        for round in 0..RACE_ROUNDS {
            let child = entity(graph, &format!("x-{round}"));
            let linked = (0..2).any(|parent| {
                let parent = entity(graph, &format!("p{parent}-{round}"));
                edges.contains(&(parent.0, child.0.clone()))
            });
            if !linked {
                orphans.insert(child.0.trim_matches(['<', '>']).to_string());
            }
        }
        orphans
    }

    /// F2 — two validated writes race, each cutting one of an entity's two
    /// parents. Validation runs before the commit guard, so both can pass and
    /// the entity ends up unreachable; the persisted diagnostics must describe
    /// the orphan they left behind rather than the clean graph each writer was
    /// promised (G6).
    #[test]
    fn racing_validated_deletes_record_the_orphan_they_create() {
        let dir = tempfile::tempdir().unwrap();
        let node = standalone_node(&dir);
        let graph = GraphId::new("urn:test:f2-orphan-race");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "race crate",
                "description",
                "2025-01-01",
                None,
                public_policy(),
            ),
        )
        .unwrap();

        let mut fixture: Vec<Change> = (0..RACE_ROUNDS)
            .flat_map(|round| two_parent_fixture(&graph, round))
            .collect();
        let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let media = EncodedTerm::from_named_node(&vocab::schema_media_object());
        let root = EncodedTerm::from_named_node(&graph.0);
        for filler in 0..FILLER_ENTITIES {
            let subject = entity(&graph, &format!("filler-{filler}"));
            fixture.push(insert(
                &graph,
                (subject.clone(), rdf_type.clone(), media.clone()),
            ));
            fixture.push(insert(&graph, (root.clone(), has_part.clone(), subject)));
        }
        node.apply_changes_unchecked(&graph, fixture).unwrap();
        assert!(
            !node.graph_diagnostics(&graph).unwrap().has_orphans(),
            "the fixture must start orphan-free"
        );

        let (tx, rx) = mpsc::channel();
        for round in 0..RACE_ROUNDS {
            let start = Arc::new(Barrier::new(2));
            for parent in 0..2 {
                let node = Arc::clone(&node);
                let graph = graph.clone();
                let start = Arc::clone(&start);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    start.wait();
                    // A racer that loses the interleaving is rejected by the
                    // reachability rule; what matters is the record the winners
                    // leave behind.
                    let _ =
                        node.apply_changes(&graph, vec![cut_parent_edge(&graph, round, parent)]);
                    tx.send(()).unwrap();
                });
            }
        }
        drop(tx);
        for _ in 0..(RACE_ROUNDS * 2) {
            rx.recv_timeout(PROGRESS_TIMEOUT)
                .expect("a racing validated write never finished");
        }

        let expected = expected_orphans(&node, &graph);
        assert!(
            !expected.is_empty(),
            "no round interleaved validation with a commit, so this run proved nothing"
        );
        let recorded: BTreeSet<String> = node
            .graph_diagnostics(&graph)
            .unwrap()
            .orphaned_entities
            .into_iter()
            .collect();
        assert_eq!(
            recorded, expected,
            "the persisted orphan set must describe the graph the writes actually left"
        );
    }

    // ── F3: a read that writes destroys the search re-queue baseline ────────

    /// F3 — the search worker reads a graph's diagnostics to know which
    /// subjects to hide. If that read also persisted what it recomputed, it
    /// would move the baseline `rebuild_graph_diagnostics` diffs against
    /// *without* indexing anything, and an entity a bulk write re-linked —
    /// never named by the write, so never enqueued by it — would stay out of
    /// the index forever (G6, G7).
    #[test]
    #[cfg(feature = "search")]
    fn a_worker_read_between_a_bulk_relink_and_its_rebuild_keeps_search_correct() {
        let dir = tempfile::tempdir().unwrap();
        let node = standalone_node(&dir);
        let graph = GraphId::new("urn:test:f3-requeue-baseline");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "requeue crate",
                "description",
                "2025-01-01",
                None,
                public_policy(),
            ),
        )
        .unwrap();

        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let root = EncodedTerm::from_named_node(&graph.0);
        let child = entity(&graph, "salamander.dat");
        let link = Change::Insert {
            graph: graph.clone(),
            subject: root.clone(),
            predicate: has_part.clone(),
            object: child.clone(),
        };
        node.apply_changes_unchecked(
            &graph,
            vec![
                insert(
                    &graph,
                    (
                        child.clone(),
                        EncodedTerm::from_named_node(&vocab::rdf_type()),
                        EncodedTerm::from_named_node(&vocab::schema_media_object()),
                    ),
                ),
                insert(
                    &graph,
                    (
                        child.clone(),
                        EncodedTerm::from_named_node(&vocab::schema_name()),
                        literal_term("salamander"),
                    ),
                ),
                link.clone(),
            ],
        )
        .unwrap();
        node.flush_search_updates().unwrap();
        assert_eq!(1, search_hits(&node, "salamander"), "seeded and searchable");

        // Orphan it, so it is correctly absent from the index.
        node.apply_changes_unchecked(
            &graph,
            vec![Change::Delete {
                graph: graph.clone(),
                subject: root,
                predicate: has_part,
                object: child,
            }],
        )
        .unwrap();
        node.flush_search_updates().unwrap();
        assert_eq!(0, search_hits(&node, "salamander"), "orphans are hidden");

        // Re-link it through the bulk path, which defers diagnostics and
        // enqueues only the subjects it names — the root, never the child.
        node.apply_changes_bulk_unchecked(&graph, vec![link])
            .unwrap();
        // The worker wins the race to the diagnostics read.
        node.flush_search_updates().unwrap();
        node.rebuild_graph_diagnostics(&graph).unwrap();
        node.flush_search_updates().unwrap();

        assert!(
            !node.graph_diagnostics(&graph).unwrap().has_orphans(),
            "the re-linked entity is reachable again"
        );
        assert_eq!(
            1,
            search_hits(&node, "salamander"),
            "the re-linked entity must be searchable again"
        );
    }

    #[cfg(feature = "search")]
    fn search_hits(node: &CraqleNode, query: &str) -> usize {
        node.search(&writer_auth(), SearchRequest { query, limit: 10 })
            .unwrap()
            .len()
    }

    fn graph_has_keyword(
        net: &sim::CraqleCluster,
        peer: usize,
        graph: &GraphId,
        keyword: &str,
    ) -> bool {
        let expected = literal_term(keyword).0;
        let keywords = EncodedTerm::from_named_node(&vocab::schema_keywords());
        net.peer(peer)
            .graph_snapshot(graph)
            .unwrap()
            .quads
            .into_iter()
            .any(|quad| quad.predicate == keywords && quad.object.0 == expected)
    }
}
