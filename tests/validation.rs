mod support;

#[cfg(test)]
mod tests {
    use craqle::*;

    use crate::support::*;

    #[test]
    fn test_rules_prevent_orphan_creation() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");
        let writer = writer_auth();

        // Create a valid crate first
        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Test",
                    "Test",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();

        // Try to add a file entity WITHOUT hasPart link (should be rejected)
        let orphan_quads = vec![
            (
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("orphan.txt")),
                EncodedTerm::from_named_node(&vocab::rdf_type()),
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                    "http://schema.org/MediaObject",
                )),
            ),
            (
                EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("orphan.txt")),
                EncodedTerm::from_named_node(&vocab::schema_name()),
                EncodedTerm("\"Orphan File\"".into()),
            ),
        ];

        match net.peer(0).insert_quads(&graph, orphan_quads) {
            Err(craqle::CraqleError::Update(UpdateError::ValidationFailed(violations))) => {
                assert!(
                    violations
                        .iter()
                        .any(|violation| violation.code == "orphaned_data_entity")
                );
            }
            other => panic!("expected orphan validation failure, got {other:?}"),
        }

        net.peer(0)
            .add_data_entity(
                &writer,
                &graph,
                "linked.txt",
                "http://schema.org/MediaObject",
                "Linked File",
            )
            .unwrap();

        assert!(graph_contains(&net, 0, &graph, "linked.txt"));
    }

    #[test]
    fn test_orphaned_entity_after_concurrent_edit_scenario() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-orphans");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Orphan Test",
                    "Concurrent structure changes",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.peer(0)
            .add_data_entity(
                &writer,
                &graph,
                "data/",
                "http://schema.org/Dataset",
                "Data Directory",
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .append_new_data_entities_under(
                &writer,
                &graph,
                "data/",
                vec![NewDataEntity {
                    entity_id: "data/results.csv".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "Nested Results".to_string(),
                    additional_triples: vec![],
                }],
            )
            .unwrap();

        let remove_link = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:hasPart ?data . ?data rdf:type ?type . ?data schema:name ?name }} }} WHERE {{ GRAPH <{}> {{ ?root schema:datePublished ?date . ?root schema:hasPart ?data . ?data rdf:type ?type . ?data schema:name ?name . FILTER(STR(?data) = \"./data/\") }} }}",
            graph.as_str(),
            graph.as_str()
        );
        net.peer_mut(1)
            .apply_sparql_update(&writer_auth(), &remove_link)
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let violations = violation_messages(&net, 0, &graph);
        assert!(!violations.is_empty());

        let diagnostics = net.peer(0).graph_diagnostics(&graph).unwrap();
        assert!(diagnostics.has_orphans());
        assert!(
            diagnostics
                .orphaned_entities
                .iter()
                .any(|entity| entity == "./data/")
        );
        assert!(
            diagnostics
                .orphaned_entities
                .iter()
                .any(|entity| entity == "./data/results.csv")
        );

        let exported = net
            .peer(0)
            .export_rocrate(&GrantAuthorizer::default(), &graph)
            .unwrap();
        assert!(!exported.contains("data/results.csv"));
        assert!(!exported.contains("\"Data Directory\""));

        let query = format!(
            "SELECT ?s WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
            graph.as_str()
        );
        let rows = solution_rows(
            net.peer(0)
                .query(&GrantAuthorizer::default(), &query)
                .unwrap(),
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn test_rules_prevent_root_destruction_scenario() {
        let (_tmp, mut net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-root-guard");
        create_test_crate(&net, 0, &graph);

        let delete_root = format!(
            "DELETE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:datePublished ?date . }} }}",
            graph.as_str(),
            graph.as_str()
        );

        match net
            .peer_mut(0)
            .apply_sparql_update(&writer_auth(), &delete_root)
        {
            Err(craqle::CraqleError::Update(UpdateError::ValidationFailed(violations))) => {
                assert!(
                    violations
                        .iter()
                        .any(|violation| violation.code == "missing_root_data_entity")
                );
            }
            other => panic!("expected root-destruction validation failure, got {other:?}"),
        }
    }

    #[test]
    fn alternate_parent_accepted() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-multi-parent");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Multi Parent",
                    "Alternate path reachability",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.peer(0)
            .add_data_entity(
                &writer,
                &graph,
                "primary/",
                "http://schema.org/Dataset",
                "Primary",
            )
            .unwrap();
        net.peer(0)
            .add_data_entity(
                &writer,
                &graph,
                "secondary/",
                "http://schema.org/Dataset",
                "Secondary",
            )
            .unwrap();
        net.peer(0)
            .append_new_data_entities_under(
                &writer,
                &graph,
                "primary/",
                vec![NewDataEntity {
                    entity_id: "shared/file.txt".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "Shared File".to_string(),
                    additional_triples: vec![],
                }],
            )
            .unwrap();

        net.peer(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("./secondary/")),
                    EncodedTerm::from_named_node(&vocab::schema_has_part()),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "./shared/file.txt",
                    )),
                )],
            )
            .unwrap();

        net.peer(0)
            .apply_changes(
                &graph,
                vec![MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "./primary/",
                    )),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_has_part()),
                    object: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "./shared/file.txt",
                    )),
                }],
            )
            .unwrap();

        assert!(graph_contains(&net, 0, &graph, "shared/file.txt"));
        let diagnostics = net.peer(0).graph_diagnostics(&graph).unwrap();
        assert!(!diagnostics.has_orphans());
    }

    fn iri(value: &str) -> EncodedTerm {
        EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(value))
    }

    fn insert(graph: &GraphId, triple: (&str, EncodedTerm, EncodedTerm)) -> MaterializedQuadChange {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: iri(triple.0),
            predicate: triple.1,
            object: triple.2,
        }
    }

    fn media_object_triples(graph: &GraphId, entity: &str) -> Vec<MaterializedQuadChange> {
        vec![
            insert(
                graph,
                (
                    entity,
                    EncodedTerm::from_named_node(&vocab::rdf_type()),
                    iri("http://schema.org/MediaObject"),
                ),
            ),
            insert(
                graph,
                (
                    entity,
                    EncodedTerm::from_named_node(&vocab::schema_name()),
                    EncodedTerm(format!("\"{entity}\"")),
                ),
            ),
        ]
    }

    fn seeded_crate(net: &sim::CraqleCluster, graph: &GraphId) {
        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Reachability",
                    "Reachability fixtures",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
    }

    /// K3 — validating a write at the bottom of a very deep `hasPart` chain must
    /// not overflow the stack. The reachability walk climbs one frame per link,
    /// so a recursive implementation dies well before this depth.
    #[test]
    fn deep_chain_terminates() {
        const DEPTH: usize = 10_000;

        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-deep-chain");
        seeded_crate(&net, &graph);

        // Build the chain with the trusted bulk path so the chain itself is not
        // what is under test.
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let mut changes = Vec::with_capacity(DEPTH * 3);
        for link in 0..DEPTH {
            let entity = format!("./link-{link}");
            let parent = if link == 0 {
                graph.as_str().to_string()
            } else {
                format!("./link-{}", link - 1)
            };
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: iri(&parent),
                predicate: has_part.clone(),
                object: iri(&entity),
            });
            changes.extend(media_object_triples(&graph, &entity));
        }
        net.peer(0)
            .apply_changes_bulk_unchecked(&graph, changes)
            .unwrap();
        net.peer(0).rebuild_graph_diagnostics(&graph).unwrap();
        assert!(!net.peer(0).graph_diagnostics(&graph).unwrap().has_orphans());

        // One *validated* write at the deep end: the orphan check has to walk
        // every link back up to the root.
        let deepest = format!("./link-{}", DEPTH - 1);
        let leaf = "./deep-leaf.txt";
        let mut tail = vec![MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: iri(&deepest),
            predicate: has_part.clone(),
            object: iri(leaf),
        }];
        tail.extend(media_object_triples(&graph, leaf));
        net.peer(0).apply_changes(&graph, tail).unwrap();

        assert!(graph_contains(&net, 0, &graph, "deep-leaf.txt"));
        assert!(!net.peer(0).graph_diagnostics(&graph).unwrap().has_orphans());
    }

    /// W4 — the delta index resolves a triple last-writer-wins, so deleting and
    /// re-inserting the root type in one change set leaves the root intact.
    #[test]
    fn reinsert_passes_validation() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-reinsert");
        seeded_crate(&net, &graph);

        let root_type = (
            EncodedTerm::from_named_node(&graph.0),
            EncodedTerm::from_named_node(&vocab::rdf_type()),
            iri("http://schema.org/Dataset"),
        );
        let changes = vec![
            MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: root_type.0.clone(),
                predicate: root_type.1.clone(),
                object: root_type.2.clone(),
            },
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: root_type.0.clone(),
                predicate: root_type.1.clone(),
                object: root_type.2.clone(),
            },
        ];

        net.peer(0).apply_changes(&graph, changes).unwrap();
        assert!(
            net.peer(0).graph_violations(&graph).unwrap().is_empty(),
            "delete-then-reinsert must leave the root data entity in place"
        );
    }

    /// The mirror image: insert-then-delete of the same triple removes it, so
    /// validation must reject the change set.
    #[test]
    fn delete_fails_validation() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-reinsert-reverse");
        seeded_crate(&net, &graph);

        let root = EncodedTerm::from_named_node(&graph.0);
        let rdf_type = EncodedTerm::from_named_node(&vocab::rdf_type());
        let dataset = iri("http://schema.org/Dataset");
        let changes = vec![
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: root.clone(),
                predicate: rdf_type.clone(),
                object: dataset.clone(),
            },
            MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: root,
                predicate: rdf_type,
                object: dataset,
            },
        ];

        match net.peer(0).apply_changes(&graph, changes) {
            Err(craqle::CraqleError::Update(UpdateError::ValidationFailed(violations))) => {
                assert!(
                    violations
                        .iter()
                        .any(|violation| violation.code == "missing_root_data_entity"),
                    "expected the last delete to win, got {violations:?}"
                );
            }
            other => panic!("expected root-destruction validation failure, got {other:?}"),
        }
    }

    /// A `hasPart` cycle that is not attached to the root is unreachable: the
    /// walk must not call the members reachable just because they reach each
    /// other. Pinned on both the validated path and the recomputed diagnostics.
    #[test]
    fn detached_cycle_orphaned() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-cycle");
        seeded_crate(&net, &graph);

        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let mut cycle = vec![
            insert(&graph, ("./cycle-a", has_part.clone(), iri("./cycle-b"))),
            insert(&graph, ("./cycle-b", has_part.clone(), iri("./cycle-a"))),
        ];
        cycle.extend(media_object_triples(&graph, "./cycle-a"));
        cycle.extend(media_object_triples(&graph, "./cycle-b"));

        // The validated path rejects it outright.
        match net.peer(0).apply_changes(&graph, cycle.clone()) {
            Err(craqle::CraqleError::Update(UpdateError::ValidationFailed(violations))) => {
                assert!(
                    violations
                        .iter()
                        .any(|violation| violation.code == "orphaned_data_entity"),
                    "a detached cycle is not reachable, got {violations:?}"
                );
            }
            other => panic!("expected an orphan violation, got {other:?}"),
        }

        // Forced in through the trusted bulk path, both members are reported as
        // orphans by the recomputed diagnostics.
        net.peer(0)
            .apply_changes_bulk_unchecked(&graph, cycle)
            .unwrap();
        net.peer(0).rebuild_graph_diagnostics(&graph).unwrap();
        let orphans = net
            .peer(0)
            .graph_diagnostics(&graph)
            .unwrap()
            .orphaned_entities;
        for member in ["./cycle-a", "./cycle-b"] {
            assert!(
                orphans.iter().any(|entity| entity == member),
                "{member} should be orphaned, got {orphans:?}"
            );
        }
    }

    /// The same cycle *is* reachable once the root links into it, and the walk
    /// must terminate rather than loop between the two members.
    #[test]
    fn attached_cycle_reachable() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-cycle-attached");
        seeded_crate(&net, &graph);

        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let mut cycle = vec![
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: has_part.clone(),
                object: iri("./ring-a"),
            },
            insert(&graph, ("./ring-a", has_part.clone(), iri("./ring-b"))),
            insert(&graph, ("./ring-b", has_part.clone(), iri("./ring-a"))),
        ];
        cycle.extend(media_object_triples(&graph, "./ring-a"));
        cycle.extend(media_object_triples(&graph, "./ring-b"));

        net.peer(0).apply_changes(&graph, cycle).unwrap();
        assert!(!net.peer(0).graph_diagnostics(&graph).unwrap().has_orphans());
    }
}
