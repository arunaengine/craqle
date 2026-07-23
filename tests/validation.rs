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
        net.peer_mut(1).update(&remove_link).unwrap();
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

        match net.peer_mut(0).update(&delete_root) {
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
    fn test_reachability_allows_alternate_parent_path() {
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
}
