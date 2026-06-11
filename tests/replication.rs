mod support;

#[cfg(test)]
mod tests {
    use craqle::*;
    use irokle::Event;
    use proptest::prelude::*;

    use crate::support::*;

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, irokle::Event)]
    #[irokle(type_id = "craqle.graph.v1")]
    struct PoisonEvent {
        junk: Vec<u64>,
    }

    #[test]
    fn test_graph_delete_replicates_to_peers() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-delete");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();
        assert!(net.peer(1).contains_graph(&graph).unwrap());

        net.peer(0).delete_graph_unchecked(&graph).unwrap();
        assert!(!net.peer(0).contains_graph(&graph).unwrap());
        net.sync_until_converged(10).unwrap();
        assert!(!net.peer(1).contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_graph_delete_survives_reopen_without_resurrection() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let open = || {
            CraqleNode::open_with_options(
                dir.path(),
                CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
            )
            .unwrap()
        };
        let graph = GraphId::new("urn:test:crate-delete-reopen");

        let node = open();
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Doomed",
                "To be deleted",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();
        node.delete_graph_unchecked(&graph).unwrap();
        assert!(!node.contains_graph(&graph).unwrap());
        drop(node);

        let node = open();
        assert!(!node.contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_poison_event_does_not_brick_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:crate-poison");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Poisoned",
                "Has a bad record",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        let topic_id = node.irokle_topic_id(&graph).unwrap().unwrap();
        irokle
            .open_topic::<PoisonEvent>(topic_id)
            .unwrap()
            .publish(PoisonEvent { junk: vec![99; 99] })
            .unwrap();

        node.reconcile_irokle().unwrap();
        assert!(node.contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_cross_graph_injection_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph_a = GraphId::new("urn:test:crate-bound");
        let graph_b = GraphId::new("urn:test:crate-injected");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph_a.clone(),
                "Bound",
                "Legit graph",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        let topic_id = node.irokle_topic_id(&graph_a).unwrap().unwrap();
        irokle
            .open_topic::<CraqleGraphEvent>(topic_id)
            .unwrap()
            .publish(CraqleGraphEvent::QuadChanges {
                graph: graph_b.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: graph_b.clone(),
                    subject: EncodedTerm::from_named_node(&graph_b.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: literal_term("injected"),
                }],
            })
            .unwrap();

        node.reconcile_irokle().unwrap();
        assert!(!node.contains_graph(&graph_b).unwrap());
        assert!(node.contains_graph(&graph_a).unwrap());
    }

    #[test]
    fn test_durable_write_before_reconcile_does_not_fork_graph_topic() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-topic-fork");
        create_test_crate(&net, 0, &graph);
        let topic_a = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();

        // Deliver A's irokle ops to B without letting B's craqle layer apply
        // them, so B still has no graph->topic binding when it writes.
        let summary = net.irokle(1).sync_summary(topic_a).unwrap();
        let data = net
            .irokle(0)
            .plan_sync_data(net.irokle(1).peer_id(), &summary)
            .unwrap();
        let ack = net
            .irokle(1)
            .receive_sync_data_from(net.irokle(0).peer_id(), data)
            .unwrap();
        let _ = net.irokle(0).apply_sync_ack(&ack);
        assert!(net.peer(1).irokle_topic_id(&graph).unwrap().is_none());

        create_test_crate(&net, 1, &graph);
        let topic_b = net.peer(1).irokle_topic_id(&graph).unwrap().unwrap();
        assert_eq!(topic_a, topic_b, "both nodes must agree on the graph topic");

        net.sync_until_converged(10).unwrap();
        let state0 = graph_state(&net, 0, &graph);
        assert_eq!(state0, graph_state(&net, 1, &graph));
        assert!(!state0.is_empty());

        keyword_insert(net.peer(1), &graph, "post-fork-keyword");
        net.sync_until_converged(10).unwrap();
        assert!(
            graph_state(&net, 0, &graph)
                .iter()
                .any(|(_, _, object)| object.contains("post-fork-keyword")),
            "B-side change must replicate to A over the shared topic"
        );

        for peer in 0..2 {
            let topics: Vec<_> = net
                .irokle(peer)
                .list_topics()
                .unwrap()
                .into_iter()
                .filter(|topic| topic.event_type_id == CraqleGraphEvent::TYPE_ID)
                .collect();
            assert_eq!(topics.len(), 1, "no second topic may exist for the graph");
            assert_eq!(topics[0].topic_id, topic_a);
        }
    }

    #[test]
    fn test_deterministic_actor_writes_are_identical_across_nodes() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let node_a = CraqleNode::open(dir_a.path()).unwrap();
        let node_b = CraqleNode::open(dir_b.path()).unwrap();
        let graph = GraphId::new("urn:test:crate-deterministic");
        let actor = ActorId::from_bytes([7u8; 32]);
        let request = || {
            CreateCrateRequest::new(
                graph.clone(),
                "Det",
                "Deterministic materialization",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            )
        };

        for node in [&node_a, &node_b] {
            node.create_crate_with_durability_as(
                &writer_auth(),
                request(),
                CraqleRequestDurability::WalAlreadyDurable,
                Some(actor),
            )
            .unwrap();
        }

        let normalize = |snapshot: GraphReplicaSnapshot| {
            let mut quads = snapshot.quads;
            quads.sort_by(|a, b| {
                (&a.subject.0, &a.predicate.0, &a.object.0).cmp(&(
                    &b.subject.0,
                    &b.predicate.0,
                    &b.object.0,
                ))
            });
            (snapshot.clock, quads)
        };
        assert_eq!(
            normalize(node_a.graph_snapshot(&graph).unwrap()),
            normalize(node_b.graph_snapshot(&graph).unwrap()),
        );
    }

    #[test]
    fn test_single_peer_create_crate() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");
        create_test_crate(&net, 0, &graph);

        // Verify the crate was created
        assert!(net.peer(0).contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_add_add_convergence() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create crate on peer 0, sync to peer 1
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Both peers add different entities (offline)
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/file_a.csv",
            "http://schema.org/MediaObject",
            "File A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/file_b.csv",
            "http://schema.org/MediaObject",
            "File B",
            vec![],
        )
        .unwrap();

        // Sync
        net.sync_until_converged(10).unwrap();

        // Both peers should have both entities
        let f0 = net.peer(0).vector_clock(&graph).unwrap();
        let f1 = net.peer(1).vector_clock(&graph).unwrap();
        assert_eq!(f0, f1, "vector clocks must match after convergence");
    }

    #[test]
    fn test_three_peer_convergence() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate1");

        // Create on peer 0, sync to all
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Partition: isolate peer 2
        net.partition(0, 2);
        net.partition(1, 2);

        // Peer 0 and 1 make changes
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/entity_a.csv",
            "http://schema.org/MediaObject",
            "Entity A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/entity_b.csv",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();

        // Peer 2 (isolated) makes a change
        let mgr2 = manager(net.peer(2));
        mgr2.add_data_entity(
            &graph,
            "data/entity_c.csv",
            "http://schema.org/MediaObject",
            "Entity C",
            vec![],
        )
        .unwrap();

        // Sync 0 and 1
        net.sync_until_converged(10).unwrap();

        // Heal partition
        net.heal(0, 2);
        net.heal(1, 2);

        // Full sync
        net.sync_until_converged(10).unwrap();

        // All three peers must converge
        let f0 = net.peer(0).vector_clock(&graph).unwrap();
        let f1 = net.peer(1).vector_clock(&graph).unwrap();
        let f2 = net.peer(2).vector_clock(&graph).unwrap();
        assert_eq!(f0, f1);
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_idempotent_batch_replay() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create and sync
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let f_before = net.peer(1).vector_clock(&graph).unwrap();

        // Sync again (duplicate delivery)
        net.sync_until_converged(10).unwrap();

        let f_after = net.peer(1).vector_clock(&graph).unwrap();
        assert_eq!(f_before, f_after, "duplicate sync must be idempotent");
    }

    #[test]
    fn test_concurrent_same_field_update_keeps_both_values() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = manager(net.peer(0));
        mgr.create_crate(
            graph.clone(),
            "Original",
            "Concurrent title updates",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        let update0 = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:name \"Original\" }} }} INSERT {{ GRAPH <{}> {{ ?root schema:name \"Peer 0 Title\" }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name \"Original\" . }} }}",
            graph.as_str(),
            graph.as_str(),
            graph.as_str()
        );
        let update1 = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:name \"Original\" }} }} INSERT {{ GRAPH <{}> {{ ?root schema:name \"Peer 1 Title\" }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name \"Original\" . }} }}",
            graph.as_str(),
            graph.as_str(),
            graph.as_str()
        );

        net.peer_mut(0).update(&update0).unwrap();
        net.peer_mut(1).update(&update1).unwrap();
        net.sync_until_converged(10).unwrap();

        let query = format!(
            "SELECT ?name WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name ?name . }} }}",
            graph.as_str()
        );
        let results = net
            .peer(0)
            .query(&GrantAuthorizer::default(), &query)
            .unwrap();
        let mut names: Vec<String> = solution_rows(results)
            .iter()
            .map(|binding| binding_literal(binding.get("name").unwrap()))
            .collect();
        names.sort();

        assert_eq!(names, vec!["Peer 0 Title", "Peer 1 Title"]);
    }

    #[test]
    fn test_concurrent_metadata_editing_scenario() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-metadata");

        let mgr0 = manager(net.peer(0));
        mgr0.create_crate(
            graph.clone(),
            "Original Dataset",
            "Original description",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        mgr0.update_property(
            &graph,
            graph.as_str(),
            "schema:name",
            None,
            "Updated Dataset v2",
        )
        .unwrap();
        let mgr1 = manager(net.peer(1));
        mgr1.update_property(
            &graph,
            graph.as_str(),
            "schema:description",
            None,
            "Improved description with more detail",
        )
        .unwrap();

        net.sync_until_converged(10).unwrap();

        let exported = mgr1.export_jsonld(&graph).unwrap();
        assert!(exported.contains("Updated Dataset v2"));
        assert!(exported.contains("Improved description with more detail"));
        assert!(violation_messages(&net, 0, &graph).is_empty());
        assert!(violation_messages(&net, 1, &graph).is_empty());

        assert!(!reindex_and_search(&net, 0, "updated").is_empty());
        assert!(!reindex_and_search(&net, 1, "improved").is_empty());
    }

    #[test]
    fn test_concurrent_entity_addition_scenario() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-entities");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let mgr0 = manager(net.peer(0));
        let mgr1 = manager(net.peer(1));
        mgr0.add_data_entity(
            &graph,
            "results.csv",
            "http://schema.org/MediaObject",
            "Results CSV",
            vec![],
        )
        .unwrap();
        mgr1.add_data_entity(
            &graph,
            "analysis.py",
            "http://schema.org/MediaObject",
            "Analysis Script",
            vec![],
        )
        .unwrap();

        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(state.iter().any(|(s, _, _)| s.contains("results.csv")));
        assert!(state.iter().any(|(s, _, _)| s.contains("analysis.py")));
        assert!(
            state
                .iter()
                .any(|(_, p, o)| p.contains("hasPart") && o.contains("results.csv"))
        );
        assert!(
            state
                .iter()
                .any(|(_, p, o)| p.contains("hasPart") && o.contains("analysis.py"))
        );
        assert!(violation_messages(&net, 0, &graph).is_empty());
    }

    #[test]
    fn test_observed_remove_removes_quad_everywhere() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-observed-remove");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        keyword_insert(net.peer(0), &graph, "observed-keyword");
        net.sync_until_converged(10).unwrap();
        keyword_delete(net.peer(1), &graph, "observed-keyword");
        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(
            !state
                .iter()
                .any(|(_, _, object)| object.contains("observed-keyword"))
        );
        assert_eq!(state, graph_state(&net, 1, &graph));
    }

    #[test]
    fn test_concurrent_remove_is_add_wins() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-add-wins");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        keyword_insert(net.peer(0), &graph, "race-keyword");
        keyword_delete(net.peer(1), &graph, "race-keyword");
        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(
            state
                .iter()
                .any(|(_, _, object)| object.contains("race-keyword"))
        );
        assert_eq!(state, graph_state(&net, 1, &graph));
    }

    #[test]
    fn test_out_of_order_delivery_within_actor_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-out-of-order");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        net.peer_mut(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-one"),
                )],
            )
            .unwrap();
        net.peer_mut(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-two"),
                )],
            )
            .unwrap();

        let topic_id = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
        let summary = net.irokle(1).sync_summary(topic_id).unwrap();
        let mut data = net
            .irokle(0)
            .plan_sync_data(net.irokle(1).peer_id(), &summary)
            .unwrap();
        assert_eq!(data.ops.len(), 2);
        data.ops.reverse();

        let ack = net
            .irokle(1)
            .receive_sync_data_from(net.irokle(0).peer_id(), data)
            .unwrap();
        let _ = net.irokle(0).apply_sync_ack(&ack);
        net.peer(1).reconcile_irokle().unwrap();

        assert_eq!(graph_state(&net, 0, &graph), graph_state(&net, 1, &graph));
    }

    #[test]
    fn test_three_peer_partition_scenario() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-partition");
        manager(net.peer(0))
            .create_crate(
                graph.clone(),
                "Partitioned Crate",
                "Original description",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.partition(0, 2);
        net.partition(1, 2);

        manager(net.peer(0))
            .add_data_entity(
                &graph,
                "entity-a.txt",
                "http://schema.org/MediaObject",
                "Entity A",
                vec![],
            )
            .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "entity-b.txt",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr2 = manager(net.peer(2));
        mgr2.add_data_entity(
            &graph,
            "entity-c.txt",
            "http://schema.org/MediaObject",
            "Entity C",
            vec![],
        )
        .unwrap();
        mgr2.update_property(
            &graph,
            graph.as_str(),
            "schema:description",
            None,
            "Updated by isolated peer",
        )
        .unwrap();

        net.heal(0, 2);
        net.heal(1, 2);
        net.sync_until_converged(20).unwrap();

        for peer in 0..3 {
            let exported = manager(net.peer(peer)).export_jsonld(&graph).unwrap();
            assert!(exported.contains("Entity A"));
            assert!(exported.contains("Entity B"));
            assert!(exported.contains("Entity C"));
            assert!(exported.contains("Updated by isolated peer"));
        }
    }

    #[derive(Debug, Clone)]
    enum RandomOp {
        Add { peer: usize, keyword: u8 },
        Remove { peer: usize, keyword: u8 },
        SyncAll,
    }

    fn random_op_strategy() -> impl Strategy<Value = RandomOp> {
        prop_oneof![
            (0usize..3, 0u8..6).prop_map(|(peer, keyword)| RandomOp::Add { peer, keyword }),
            (0usize..3, 0u8..6).prop_map(|(peer, keyword)| RandomOp::Remove { peer, keyword }),
            Just(RandomOp::SyncAll),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn prop_crdt_converges_under_random_ops(ops in prop::collection::vec(random_op_strategy(), 1..40)) {
            let (_tmp, net) = setup_network(3);
            let graph = GraphId::new("urn:test:crate-proptest");
            create_test_crate(&net, 0, &graph);
            net.sync_until_converged(10).unwrap();

            for op in ops {
                match op {
                    RandomOp::Add { peer, keyword } => {
                        keyword_insert(net.peer(peer), &graph, &format!("kw-{keyword}"));
                    }
                    RandomOp::Remove { peer, keyword } => {
                        keyword_delete(net.peer(peer), &graph, &format!("kw-{keyword}"));
                    }
                    RandomOp::SyncAll => net.sync_until_converged(20).unwrap(),
                }
            }

            net.sync_until_converged(100).unwrap();

            let state0 = graph_state(&net, 0, &graph);
            let clock0 = net.peer(0).vector_clock(&graph).unwrap();
            for peer in 1..3 {
                prop_assert_eq!(state0.clone(), graph_state(&net, peer, &graph));
                prop_assert_eq!(clock0.clone(), net.peer(peer).vector_clock(&graph).unwrap());
            }
        }
    }
}
