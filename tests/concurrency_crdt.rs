mod support;

/// Concurrency guarantees of the write path (finding K1).
///
/// Every test here drives *parallel* writers at one graph. Before the store's
/// per-graph commit guard was adopted by `ReplicationEngine`, each of these
/// races could mint a duplicate dot, drop an add, or lose a vector-clock entry.
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use craqle::*;

    use crate::support::*;

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
    }

    /// K1b — no add may be lost when writers race on the same graph.
    #[test]
    fn no_lost_adds_under_parallel_insert() {
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
    }

    /// K1c — every applied batch's dot ends up in the graph's vector clock (G2).
    ///
    /// A read-modify-write race on the clock silently drops the losing writer's
    /// counter, leaving the clock behind the log head.
    #[test]
    fn vector_clock_not_lost_under_concurrent_commits() {
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
    }

    /// G1 — a `Remove` deletes exactly the dots it witnessed and no others.
    ///
    /// Both peers add the same triple while partitioned, so it carries two dots.
    /// Peer 0 then deletes it *before* healing, witnessing only its own dot, so
    /// peer 1's concurrent add must survive the merge on both replicas.
    #[test]
    fn remove_kills_only_witnessed_dots() {
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
    }

    /// Concurrent local writes in sync mode still converge across peers, and
    /// both replicas agree on the derived diagnostics (G4, G6).
    #[test]
    fn sync_mode_concurrent_writes_converge() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crdt-converge");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        insert_keywords_in_parallel(net.peer(0), &graph, "converge");
        net.sync_until_converged(20).unwrap();

        assert_eq!(
            net.peer(0).graph_fingerprint(&graph).unwrap(),
            net.peer(1).graph_fingerprint(&graph).unwrap(),
            "peers diverged after concurrent local writes"
        );
        assert_eq!(
            net.peer(0).graph_diagnostics(&graph).unwrap(),
            net.peer(1).graph_diagnostics(&graph).unwrap(),
            "peers disagree on derived diagnostics"
        );
        assert!(!net.peer(0).graph_diagnostics(&graph).unwrap().has_orphans());
    }

    /// K1d — the in-memory query indexes must equal the durable quad state after
    /// a concurrent mix of inserts and deletes, **without** a restart to repair
    /// them.
    #[test]
    fn index_matches_store_after_concurrent_churn() {
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
