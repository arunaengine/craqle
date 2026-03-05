mod support;

#[cfg(test)]
mod tests {
    use craqle::*;
    use proptest::prelude::*;

    use crate::support::*;

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

        mgr0.update_property(&graph, "./", "schema:name", None, "Updated Dataset v2")
            .unwrap();
        let mgr1 = manager(net.peer(1));
        mgr1.update_property(
            &graph,
            "./",
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
                    EncodedTerm::from_named_node(&vocab::root_entity()),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-one"),
                )],
            )
            .unwrap();
        net.peer_mut(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&vocab::root_entity()),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-two"),
                )],
            )
            .unwrap();

        let batches = net
            .peer(0)
            .catchup_batches(&graph, &net.peer(1).vector_clock(&graph).unwrap())
            .unwrap();
        assert_eq!(batches.len(), 2);
        let clock_before = net.peer(1).vector_clock(&graph).unwrap();

        net.deliver_batch_to_peer(1, batches[1].clone()).unwrap();
        assert_eq!(net.peer(1).vector_clock(&graph).unwrap(), clock_before);

        net.deliver_batch_to_peer(1, batches[0].clone()).unwrap();
        assert_eq!(graph_state(&net, 0, &graph), graph_state(&net, 1, &graph));
    }

    #[test]
    fn test_duplicate_batch_replay_explicit() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-duplicate");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        net.peer_mut(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&vocab::root_entity()),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("duplicate-keyword"),
                )],
            )
            .unwrap();
        let batch = net
            .peer(0)
            .catchup_batches(&graph, &net.peer(1).vector_clock(&graph).unwrap())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        net.deliver_batch_to_peer(1, batch.clone()).unwrap();
        let clock = net.peer(1).vector_clock(&graph).unwrap();
        let state = graph_state(&net, 1, &graph);
        net.deliver_batch_to_peer(1, batch).unwrap();
        assert_eq!(clock, net.peer(1).vector_clock(&graph).unwrap());
        assert_eq!(state, graph_state(&net, 1, &graph));
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
            "./",
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

    #[test]
    fn test_snapshot_bootstrap_scenario() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-snapshot");
        net.partition(0, 2);
        net.partition(1, 2);
