// Integration tests are in tests/ directory

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use aruna_core::*;
    use aruna_shacl::GraphSnapshot;
    use aruna_sync::SyncNetwork;
    use proptest::prelude::*;

    fn setup_network(peers: usize) -> (tempfile::TempDir, SyncNetwork) {
        let tmp = tempfile::tempdir().unwrap();
        let net = SyncNetwork::new(peers, tmp.path()).unwrap();
        (tmp, net)
    }

    fn create_test_crate(net: &SyncNetwork, peer: usize, graph: &GraphId) {
        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(peer).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Test Dataset",
            "A test dataset",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
    }

    fn binding_literal(term: &EncodedTerm) -> String {
        match term.to_term() {
            Some(oxrdf::Term::Literal(literal)) => literal.value().to_string(),
            Some(other) => panic!("expected literal binding, got {other}"),
            None => panic!("failed to decode binding {}", term.0),
        }
    }

    fn binding_i64(term: &EncodedTerm) -> i64 {
        binding_literal(term).parse::<i64>().unwrap()
    }

    fn solution_rows(
        results: aruna_sparql::QueryResults,
    ) -> Vec<std::collections::HashMap<String, EncodedTerm>> {
        match results {
            aruna_sparql::QueryResults::Solutions(rows) => rows,
            other => panic!("expected solution bindings, got {other:?}"),
        }
    }

    fn literal_term(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    fn graph_state(
        net: &SyncNetwork,
        peer: usize,
        graph: &GraphId,
    ) -> BTreeSet<(String, String, String)> {
        let store = &net.peer(peer).store;
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = store.lookup_term(&graph_term).unwrap() else {
            return BTreeSet::new();
        };

        store
            .quads_for_pattern(Some(graph_id), None, None, None)
            .unwrap()
            .into_iter()
            .map(|quad| {
                (
                    store.decode_term(quad.subject).unwrap().0,
                    store.decode_term(quad.predicate).unwrap().0,
                    store.decode_term(quad.object).unwrap().0,
                )
            })
            .collect()
    }

    fn graph_contains(net: &SyncNetwork, peer: usize, graph: &GraphId, subject: &str) -> bool {
        graph_state(net, peer, graph)
            .iter()
            .any(|(s, _, _)| s.contains(subject))
    }

    fn violation_messages(net: &SyncNetwork, peer: usize, graph: &GraphId) -> Vec<String> {
        let snapshot = GraphSnapshot::from_store(&net.peer(peer).store, graph).unwrap();
        aruna_shacl::post_merge_check(&snapshot)
            .into_iter()
            .map(|violation| violation.to_string())
            .collect()
    }

    fn keyword_insert(peer: &aruna_sync::PeerNode, graph: &GraphId, keyword: &str) {
        peer.engine
            .local_insert_quads(
                graph,
                vec![(
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                    literal_term(keyword),
                )],
            )
            .unwrap();
    }

    fn keyword_delete(peer: &aruna_sync::PeerNode, graph: &GraphId, keyword: &str) {
        peer.engine
            .local_apply_changes(
                graph,
                vec![MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    predicate: EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                    object: literal_term(keyword),
                }],
            )
            .unwrap();
    }

    fn reindex_and_search(net: &SyncNetwork, peer: usize, query: &str) -> Vec<String> {
        net.reindex_search().unwrap();
        net.peer(peer)
            .search
            .search(query, 10)
            .unwrap()
            .into_iter()
            .map(|hit| hit.subject_iri)
            .collect()
    }

    fn bulk_media_object_changes(
        graph: &GraphId,
        start: usize,
        count: usize,
        keyword: &str,
    ) -> Vec<MaterializedQuadChange> {
        let mut changes = Vec::with_capacity(count * 6);
        for idx in start..start + count {
            let entity = format!("./bulk/entity-{idx:06}.dat");
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                predicate: EncodedTerm::from_named_node(&aruna_core::vocab::schema_has_part()),
                object: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            });
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
                predicate: EncodedTerm::from_named_node(&aruna_core::vocab::rdf_type()),
                object: EncodedTerm::from_named_node(&aruna_core::vocab::schema_media_object()),
            });
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
                predicate: EncodedTerm::from_named_node(&aruna_core::vocab::schema_name()),
                object: literal_term(&format!("Proteomics sample {idx}")),
            });
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
                predicate: EncodedTerm::from_named_node(&aruna_core::vocab::schema_description()),
                object: literal_term(&format!("{keyword} heavy benchmark record {idx}")),
            });
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
                predicate: EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                object: literal_term(keyword),
            });
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
                predicate: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                    "http://schema.org/identifier",
                )),
                object: literal_term(&format!("BENCH-{idx:06}")),
            });
        }
        changes
    }

    #[test]
    fn test_single_peer_create_crate() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");
        create_test_crate(&net, 0, &graph);

        // Verify the crate was created
        assert!(net.peer(0).store.contains_graph(&graph).unwrap());
    }

    #[test]
    fn test_add_add_convergence() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create crate on peer 0, sync to peer 1
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Both peers add different entities (offline)
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.add_data_entity(
            &graph,
            "data/file_a.csv",
            "http://schema.org/MediaObject",
            "File A",
            vec![],
        )
        .unwrap();

        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
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
        let f0 = net.peer(0).store.get_frontier(&graph).unwrap();
        let f1 = net.peer(1).store.get_frontier(&graph).unwrap();
        assert_eq!(f0, f1, "frontiers must match after convergence");
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
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.add_data_entity(
            &graph,
            "data/entity_a.csv",
            "http://schema.org/MediaObject",
            "Entity A",
            vec![],
        )
        .unwrap();

        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        mgr1.add_data_entity(
            &graph,
            "data/entity_b.csv",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();

        // Peer 2 (isolated) makes a change
        let mgr2 = aruna_rocrate::RoCrateManager::new(net.peer(2).engine.clone());
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
        let f0 = net.peer(0).store.get_frontier(&graph).unwrap();
        let f1 = net.peer(1).store.get_frontier(&graph).unwrap();
        let f2 = net.peer(2).store.get_frontier(&graph).unwrap();
        assert_eq!(f0, f1);
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_idempotent_batch_replay() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create and sync
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let f_before = net.peer(1).store.get_frontier(&graph).unwrap();

        // Sync again (duplicate delivery)
        net.sync_until_converged(10).unwrap();

        let f_after = net.peer(1).store.get_frontier(&graph).unwrap();
        assert_eq!(f_before, f_after, "duplicate sync must be idempotent");
    }

    #[test]
    fn test_search_after_create() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Microbial Genomics Study",
            "Analysis of microbial communities",
            "2025-03-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        // Reindex
        let _ = net
            .peer(0)
            .search
            .reindex_from_store(&net.peer(0).store, &graph);
        net.peer(0).search.commit().unwrap();

        // Search
        let hits = net.peer(0).search.search("genomics", 10).unwrap();
        assert!(!hits.is_empty(), "should find 'genomics' in crate name");
    }

    #[test]
    fn test_export_import_roundtrip() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Test Crate",
            "A test crate for roundtrip",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        let jsonld = mgr.export_jsonld(&graph).unwrap();
        assert!(jsonld.contains("@context"));
        assert!(jsonld.contains("Test Crate"));
    }

    #[test]
    fn test_import_jsonld_roundtrip() {
        let (_tmp, net) = setup_network(1);
        let source = GraphId::new("urn:test:crate-source");
        let imported = GraphId::new("urn:test:crate-imported");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            source.clone(),
            "Imported Crate",
            "Roundtrip through JSON-LD import",
            "2025-02-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        mgr.add_data_entity(
            &source,
            "data/example.txt",
            "http://schema.org/MediaObject",
            "Example File",
            vec![],
        )
        .unwrap();

        let jsonld = mgr.export_jsonld(&source).unwrap();
        mgr.import_jsonld(imported.clone(), &jsonld).unwrap();

        let exported = mgr.export_jsonld(&imported).unwrap();
        assert!(exported.contains("Imported Crate"));
        assert!(exported.contains("Example File"));
        assert!(net.peer(0).store.contains_graph(&imported).unwrap());
    }

    #[test]
    fn test_update_property_rewrites_rocrate() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Original Name",
            "Original description",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        mgr.update_property(
            &graph,
            "./",
            "schema:description",
            Some("Original description"),
            "Updated description",
        )
        .unwrap();

        let exported = mgr.export_jsonld(&graph).unwrap();
        assert!(exported.contains("Updated description"));
        assert!(!exported.contains("Original description"));
    }

    #[test]
    fn test_concurrent_same_field_update_keeps_both_values() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
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
        let results = net.peer(0).engine.sparql().query(&query).unwrap();
        let mut names: Vec<String> = solution_rows(results)
            .iter()
            .map(|binding| binding_literal(binding.get("name").unwrap()))
            .collect();
        names.sort();

        assert_eq!(names, vec!["Peer 0 Title", "Peer 1 Title"]);
    }

    #[test]
    fn test_network_reindex_search_commits() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph,
            "Proteomics Dataset",
            "Search helper should commit indexed documents",
            "2025-03-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        net.reindex_search().unwrap();
        let hits = net.peer(0).search.search("proteomics", 10).unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_shacl_prevents_orphan_creation() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        // Create a valid crate first
        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Test",
            "Test",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        // Try to add a file entity WITHOUT hasPart link (should be rejected)
        let orphan_quads = vec![
            (
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("orphan.txt")),
                EncodedTerm::from_named_node(&aruna_core::vocab::rdf_type()),
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                    "http://schema.org/MediaObject",
                )),
            ),
            (
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("orphan.txt")),
                EncodedTerm::from_named_node(&aruna_core::vocab::schema_name()),
                EncodedTerm("\"Orphan File\"".into()),
            ),
        ];

        match net.peer(0).engine.local_insert_quads(&graph, orphan_quads) {
            Err(aruna_repl::UpdateError::ValidationFailed(violations)) => {
                assert!(violations.iter().any(|violation| {
                    matches!(violation, CrateViolation::OrphanedDataEntity { .. })
                }));
            }
            other => panic!("expected orphan validation failure, got {other:?}"),
        }

        mgr.add_data_entity(
            &graph,
            "linked.txt",
            "http://schema.org/MediaObject",
            "Linked File",
            vec![],
        )
        .unwrap();

        assert!(graph_contains(&net, 0, &graph, "linked.txt"));
    }

    #[test]
    fn test_concurrent_metadata_editing_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-metadata");

        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
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
        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
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
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-entities");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
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
    fn test_orphaned_entity_after_concurrent_edit_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-orphans");
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());

        mgr0.create_crate(
            graph.clone(),
            "Orphan Test",
            "Concurrent structure changes",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        mgr0.add_data_entity(
            &graph,
            "data/",
            "http://schema.org/Dataset",
            "Data Directory",
            vec![],
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        mgr0.add_data_entity_under(
            &graph,
            "data/",
            "data/results.csv",
            "http://schema.org/MediaObject",
            "Nested Results",
            vec![],
        )
        .unwrap();

        let remove_link = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:hasPart ?data . ?data rdf:type ?type . ?data schema:name ?name }} }} WHERE {{ GRAPH <{}> {{ ?root schema:datePublished ?date . ?root schema:hasPart ?data . ?data rdf:type ?type . ?data schema:name ?name . FILTER(STR(?data) = \"./data/\") }} }}",
            graph.as_str(),
            graph.as_str()
        );
        net.peer_mut(1).update(&remove_link).unwrap();
        net.sync_until_converged(10).unwrap();

        let violations = violation_messages(&net, 0, &graph);
        assert!(
            violations
                .iter()
                .any(|msg| msg.contains("<./data/results.csv>")
                    || msg.contains("./data/results.csv"))
        );
        assert!(violations.iter().any(|msg| msg.contains("<./data/>")
            || msg.contains("./data/")
            || msg.contains("missing rdf:type")));
    }

    #[test]
    fn test_shacl_prevents_root_destruction_scenario() {
        let (_tmp, mut net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-root-guard");
        create_test_crate(&net, 0, &graph);

        let delete_root = format!(
            "DELETE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:datePublished ?date . }} }}",
            graph.as_str(),
            graph.as_str()
        );

        match net.peer_mut(0).update(&delete_root) {
            Err(aruna_repl::UpdateError::ValidationFailed(violations)) => {
                assert!(
                    violations.iter().any(|violation| matches!(
                        violation,
                        CrateViolation::MissingRootDataEntity
                    ))
                );
            }
            other => panic!("expected root-destruction validation failure, got {other:?}"),
        }
    }

    #[test]
    fn test_observed_remove_removes_quad_everywhere() {
        let (_tmp, mut net) = setup_network(2);
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
        let (_tmp, mut net) = setup_network(2);
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
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                    literal_term("kw-one"),
                )],
            )
            .unwrap();
        net.peer_mut(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                    literal_term("kw-two"),
                )],
            )
            .unwrap();

        let batches = net.drain_peer_outbox(0);
        assert_eq!(batches.len(), 2);
        let frontier_before = net.peer(1).store.get_frontier(&graph).unwrap();

        net.deliver_batch_to_peer(1, batches[1].clone()).unwrap();
        assert_eq!(
            net.peer(1).store.get_frontier(&graph).unwrap(),
            frontier_before
        );

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
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&aruna_core::vocab::schema_keywords()),
                    literal_term("duplicate-keyword"),
                )],
            )
            .unwrap();
        let batch = net.drain_peer_outbox(0).pop().unwrap();

        net.deliver_batch_to_peer(1, batch.clone()).unwrap();
        let frontier = net.peer(1).store.get_frontier(&graph).unwrap();
        let state = graph_state(&net, 1, &graph);
        net.deliver_batch_to_peer(1, batch).unwrap();
        assert_eq!(frontier, net.peer(1).store.get_frontier(&graph).unwrap());
        assert_eq!(state, graph_state(&net, 1, &graph));
    }

    #[test]
    fn test_three_peer_partition_scenario() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-partition");
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.create_crate(
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

        mgr0.add_data_entity(
            &graph,
            "entity-a.txt",
            "http://schema.org/MediaObject",
            "Entity A",
            vec![],
        )
        .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        mgr1.add_data_entity(
            &graph,
            "entity-b.txt",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr2 = aruna_rocrate::RoCrateManager::new(net.peer(2).engine.clone());
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
            let exported = aruna_rocrate::RoCrateManager::new(net.peer(peer).engine.clone())
                .export_jsonld(&graph)
                .unwrap();
            assert!(exported.contains("Entity A"));
            assert!(exported.contains("Entity B"));
            assert!(exported.contains("Entity C"));
            assert!(exported.contains("Updated by isolated peer"));
        }
    }

    #[test]
    fn test_search_after_concurrent_edits_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-search-edit");
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.create_crate(
            graph.clone(),
            "Microbial Genomics Study",
            "Microbial sequencing",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        mgr0.update_property(
            &graph,
            "./",
            "schema:name",
            None,
            "Microbial Proteomics Study",
        )
        .unwrap();
        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        mgr1.add_data_entity(
            &graph,
            "assembly.txt",
            "http://schema.org/MediaObject",
            "Assembly Notes",
            vec![(
                oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal("metagenomic assembly")),
            )],
        )
        .unwrap();

        net.sync_until_converged(10).unwrap();

        let proteomics = reindex_and_search(&net, 0, "proteomics");
        let metagenomic = reindex_and_search(&net, 0, "metagenomic");
        let genomics = reindex_and_search(&net, 0, "genomics");

        assert!(proteomics.iter().any(|subject| subject == "./"));
        assert!(
            metagenomic
                .iter()
                .any(|subject| subject.contains("assembly.txt"))
        );
        assert!(genomics.is_empty());
    }

    #[test]
    fn test_snapshot_bootstrap_scenario() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-snapshot");
        net.partition(0, 2);
        net.partition(1, 2);

        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.create_crate(
            graph.clone(),
            "Snapshot Crate",
            "Bootstrap scenario",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        for idx in 0..5 {
            mgr0.add_data_entity(
                &graph,
                &format!("data/file-{idx}.txt"),
                "http://schema.org/MediaObject",
                &format!("File {idx}"),
                vec![],
            )
            .unwrap();
        }
        net.sync_pair(0, 1).unwrap();

        let snapshot = net.snapshot_graph(0, &graph).unwrap();
        net.load_snapshot(2, &snapshot).unwrap();

        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        mgr1.add_data_entity(
            &graph,
            "data/new-after-snapshot.txt",
            "http://schema.org/MediaObject",
            "Late File",
            vec![],
        )
        .unwrap();
        net.sync_pair(0, 1).unwrap();

        net.heal(0, 2);
        net.heal(1, 2);
        net.sync_until_converged(20).unwrap();

        let state0 = graph_state(&net, 0, &graph);
        assert_eq!(state0, graph_state(&net, 1, &graph));
        assert_eq!(state0, graph_state(&net, 2, &graph));
    }

    #[test]
    fn test_full_rocrate_lifecycle_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-lifecycle");
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr0.create_crate(
            graph.clone(),
            "Experiment Results",
            "Data from experiment X",
            "2025-01-15",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        mgr0.add_data_entity(
            &graph,
            "data/sample1.fastq",
            "http://schema.org/MediaObject",
            "Sample 1",
            vec![],
        )
        .unwrap();
        mgr0.add_data_entity(
            &graph,
            "data/sample2.fastq",
            "http://schema.org/MediaObject",
            "Sample 2",
            vec![],
        )
        .unwrap();
        mgr0.add_data_entity(
            &graph,
            "analysis/pipeline.nf",
            "http://schema.org/MediaObject",
            "Pipeline",
            vec![],
        )
        .unwrap();
        mgr0.add_contextual_entity(
            &graph,
            "#alice",
            "http://schema.org/Person",
            "Alice Example",
            vec![(
                oxrdf::NamedNode::new_unchecked("http://schema.org/identifier"),
                oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                    "https://orcid.org/0000-0001-2345-6789",
                )),
            )],
        )
        .unwrap();
        net.peer(0)
            .engine
            .local_insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "http://schema.org/creator",
                    )),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("#alice")),
                )],
            )
            .unwrap();
        mgr0.add_contextual_entity(
            &graph,
            "#lab",
            "http://schema.org/Organization",
            "Example Lab",
            vec![],
        )
        .unwrap();
        net.peer(0)
            .engine
            .local_insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "http://schema.org/publisher",
                    )),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("#lab")),
                )],
            )
            .unwrap();
        mgr0.add_contextual_entity(
            &graph,
            "#grant",
            "http://schema.org/Grant",
            "Grant ABC",
            vec![],
        )
        .unwrap();
        net.peer(0)
            .engine
            .local_insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&aruna_core::vocab::root_entity()),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "http://schema.org/funder",
                    )),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("#grant")),
                )],
            )
            .unwrap();

        net.sync_until_converged(10).unwrap();

        let mgr1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        mgr1.add_data_entity(
            &graph,
            "results/report.pdf",
            "http://schema.org/MediaObject",
            "Report",
            vec![],
        )
        .unwrap();
        mgr1.update_property(
            &graph,
            "./",
            "schema:description",
            None,
            "Data from experiment X with updated notes",
        )
        .unwrap();

        mgr0.add_data_entity_under(
            &graph,
            "./",
            "results/figures/",
            "http://schema.org/Dataset",
            "Figures Directory",
            vec![],
        )
        .unwrap();
        mgr0.add_data_entity_under(
            &graph,
            "results/figures/",
            "results/figures/fig1.png",
            "http://schema.org/MediaObject",
            "Figure 1",
            vec![],
        )
        .unwrap();

        net.sync_until_converged(20).unwrap();

        let exported = mgr1.export_jsonld(&graph).unwrap();
        let json: serde_json::Value = serde_json::from_str(&exported).unwrap();
        assert!(json["@graph"].as_array().unwrap().len() >= 8);
        assert!(exported.contains("Alice Example"));
        assert!(exported.contains("results/report.pdf"));
        assert!(exported.contains("results/figures/fig1.png"));

        let experiment_hits = reindex_and_search(&net, 0, "experiment");
        let report_hits = reindex_and_search(&net, 1, "report");
        assert!(experiment_hits.iter().any(|subject| subject == "./"));
        assert!(
            report_hits
                .iter()
                .any(|subject| subject.contains("report.pdf"))
        );
    }

    #[test]
    fn test_sparql_integrated_fts_uses_tantivy_hits() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-fts");
        let mgr0 = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());

        mgr0.create_crate(
            graph.clone(),
            "Integrated FTS Crate",
            "SPARQL should see Tantivy hits",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        mgr0.add_data_entity(
            &graph,
            "data/proteomics-01.tsv",
            "http://schema.org/MediaObject",
            "Proteomics Table",
            vec![(
                oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                    "proteomics peptide quantification",
                )),
            )],
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        let query = format!(
            r#"
            SELECT ?s ?g ?score ?name
            WHERE {{
              SERVICE <urn:aruna:fts> {{
                ?s fts:query "proteomics" .
                ?s fts:score ?score .
                ?s fts:graph ?g .
                ?s fts:name ?name .
                ?s fts:limit 10 .
              }}
              GRAPH ?g {{ ?s schema:name ?name }}
              FILTER(?g = <{}>)
            }}
            ORDER BY DESC(?score)
            "#,
            graph.as_str()
        );

        let rows = solution_rows(net.peer(1).engine.sparql().query(&query).unwrap());
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|row| {
            row.get("s")
                .is_some_and(|value| value.0.contains("proteomics-01.tsv"))
        }));
        assert!(rows[0].contains_key("score"));
    }

    #[test]
    fn test_benchmark_exports_can_omit_or_page_data_entities() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-export-page");
        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());

        mgr.create_crate(
            graph.clone(),
            "Benchmark Export Crate",
            "Testing summary and paged exports",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        for idx in 0..5 {
            mgr.add_data_entity(
                &graph,
                &format!("data/page-{idx}.dat"),
                "http://schema.org/MediaObject",
                &format!("Page {idx}"),
                vec![],
            )
            .unwrap();
        }

        let summary = mgr.export_jsonld_summary(&graph).unwrap();
        assert!(summary.contains("Benchmark Export Crate"));
        assert!(!summary.contains("page-0.dat"));

        let page = mgr.export_jsonld_page(&graph, 1, 2).unwrap();
        assert_eq!(page.total_data_entities, 5);
        assert_eq!(page.returned_data_entities, 2);
        assert_eq!(page.next_offset, Some(3));
        assert_eq!(page.next_cursor.as_deref(), Some("./data/page-2.dat"));
        assert!(page.jsonld.contains("page-1.dat") || page.jsonld.contains("page-2.dat"));
    }

    #[test]
    fn test_benchmark_export_cursor_pages_can_resume() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-export-cursor");
        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());

        mgr.create_crate(
            graph.clone(),
            "Cursor Export Crate",
            "Testing cursor-based partial export",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        for idx in 0..5 {
            mgr.add_data_entity(
                &graph,
                &format!("data/cursor-{idx}.dat"),
                "http://schema.org/MediaObject",
                &format!("Cursor {idx}"),
                vec![],
            )
            .unwrap();
        }

        let first_page = mgr.export_jsonld_page_after(&graph, None, 2).unwrap();
        assert_eq!(first_page.total_data_entities, 5);
        assert_eq!(first_page.returned_data_entities, 2);
        assert_eq!(first_page.next_offset, None);
        assert_eq!(
            first_page.next_cursor.as_deref(),
            Some("./data/cursor-1.dat")
        );
        assert!(first_page.jsonld.contains("cursor-0.dat"));
        assert!(first_page.jsonld.contains("cursor-1.dat"));

        let second_page = mgr
            .export_jsonld_page_after(&graph, first_page.next_cursor.as_deref(), 2)
            .unwrap();
        assert_eq!(second_page.total_data_entities, 5);
        assert_eq!(second_page.returned_data_entities, 2);
        assert_eq!(
            second_page.next_cursor.as_deref(),
            Some("./data/cursor-3.dat")
        );
        assert!(second_page.jsonld.contains("cursor-2.dat"));
        assert!(second_page.jsonld.contains("cursor-3.dat"));
        assert!(!second_page.jsonld.contains("cursor-0.dat"));
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
            let (_tmp, mut net) = setup_network(3);
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
            let frontier0 = net.peer(0).store.get_frontier(&graph).unwrap();
            for peer in 1..3 {
                prop_assert_eq!(state0.clone(), graph_state(&net, peer, &graph));
                prop_assert_eq!(frontier0.clone(), net.peer(peer).store.get_frontier(&graph).unwrap());
            }
        }
    }

    #[test]
    #[ignore = "performance smoke check"]
    fn performance_store_and_sync_smoke() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-perf");
        create_test_crate(&net, 0, &graph);

        let start = Instant::now();
        for idx in 0..1_000 {
            keyword_insert(net.peer(0), &graph, &format!("perf-{idx}"));
        }
        let insert_elapsed = start.elapsed();

        let sync_start = Instant::now();
        net.sync_until_converged(50).unwrap();
        let sync_elapsed = sync_start.elapsed();

        let search_start = Instant::now();
        let hits = reindex_and_search(&net, 0, "perf");
        let search_elapsed = search_start.elapsed();

        println!(
            "perf: 1000 inserts in {:?}, convergence in {:?}, reindex+search in {:?}",
            insert_elapsed, sync_elapsed, search_elapsed
        );
        assert!(!hits.is_empty());
    }

    #[test]
    #[ignore = "heavy real-world graph smoke test"]
    fn heavy_real_world_graph_with_integrated_fts() {
        let entity_count = std::env::var("ARUNA_HEAVY_ENTITY_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100_000);
        let chunk_size = std::env::var("ARUNA_HEAVY_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2_000);

        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-heavy");
        let mgr = aruna_rocrate::RoCrateManager::new(net.peer(0).engine.clone());
        mgr.create_crate(
            graph.clone(),
            "Heavy Proteomics Graph",
            "Large graph benchmark",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        let load_start = Instant::now();
        for start in (0..entity_count).step_by(chunk_size) {
            let batch_count = usize::min(chunk_size, entity_count - start);
            net.peer(0)
                .engine
                .local_apply_changes_unchecked(
                    &graph,
                    bulk_media_object_changes(&graph, start, batch_count, "proteomics"),
                )
                .unwrap();
        }
        let load_elapsed = load_start.elapsed();

        let sync_start = Instant::now();
        net.sync_until_converged(200).unwrap();
        let sync_elapsed = sync_start.elapsed();

        let mgr_peer1 = aruna_rocrate::RoCrateManager::new(net.peer(1).engine.clone());
        let summary_start = Instant::now();
        let summary = mgr_peer1.export_jsonld_summary(&graph).unwrap();
        let summary_elapsed = summary_start.elapsed();

        let page_start = Instant::now();
        let page = mgr_peer1.export_jsonld_page(&graph, 0, 1000).unwrap();
        let page_elapsed = page_start.elapsed();

        let cursor_page_start = Instant::now();
        let cursor_page = mgr_peer1
            .export_jsonld_page_after(&graph, None, 1000)
            .unwrap();
        let cursor_page_elapsed = cursor_page_start.elapsed();

        let count_query = format!(
            "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
            graph.as_str()
        );
        let count_rows = solution_rows(net.peer(1).engine.sparql().query(&count_query).unwrap());
        assert_eq!(
            binding_i64(count_rows[0].get("count").unwrap()),
            entity_count as i64
        );

        let fts_query = format!(
            r#"
            SELECT ?s ?score
            WHERE {{
              SERVICE <urn:aruna:fts> {{
                ?s fts:query "proteomics" .
                ?s fts:score ?score .
                ?s fts:graph <{}> .
                ?s fts:limit 25 .
              }}
            }}
            ORDER BY DESC(?score)
            "#,
            graph.as_str()
        );

        let fts_start = Instant::now();
        let rows = solution_rows(net.peer(1).engine.sparql().query(&fts_query).unwrap());
        let fts_elapsed = fts_start.elapsed();

        println!(
            "heavy graph: {} entities (~{} triples) loaded in {:?}, synced in {:?}, summary export in {:?}, offset page export in {:?}, cursor page export in {:?}, fts in {:?}",
            entity_count,
            entity_count * 6 + 8,
            load_elapsed,
            sync_elapsed,
            summary_elapsed,
            page_elapsed,
            cursor_page_elapsed,
            fts_elapsed
        );
        assert!(summary.contains("Heavy Proteomics Graph"));
        assert_eq!(page.total_data_entities, entity_count);
        assert_eq!(page.returned_data_entities, usize::min(1000, entity_count));
        assert_eq!(cursor_page.total_data_entities, entity_count);
        assert_eq!(
            cursor_page.returned_data_entities,
            usize::min(1000, entity_count)
        );
        assert!(!rows.is_empty());
    }
}
