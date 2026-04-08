mod support;

#[cfg(test)]
mod tests {
    use craqle::*;

    use crate::support::*;

    #[test]
    fn test_export_import_roundtrip() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = manager(net.peer(0));
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

        let mgr = manager(net.peer(0));
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
        assert!(net.peer(0).contains_graph(&imported).unwrap());
    }

    #[test]
    fn test_import_jsonld_updates_search_without_manual_reindex() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:import-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        let jsonld =
            benchmark_rocrate_document(&graph, 250, "import-bulk-keyword", "Imported Dataset");
        node.apply_rocrate_document_with_policy(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let hits = node.search(&reader, "DOC-000123", 10).unwrap();
        assert!(
            hits.iter().any(|hit| hit.graph_id == graph.as_str()
                && hit.subject_iri == "./bulk/entity-000123.dat")
        );

        let keyword_hits = node.search(&reader, "import-bulk-keyword", 10).unwrap();
        assert!(!keyword_hits.is_empty());
    }

    #[test]
    fn test_trusted_bootstrap_import_updates_search_without_manual_reindex() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:trusted-bootstrap-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        let jsonld =
            benchmark_rocrate_document(&graph, 250, "trusted-bulk-keyword", "Trusted Dataset");
        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let hits = node.search(&reader, "DOC-000123", 10).unwrap();
        assert!(
            hits.iter().any(|hit| hit.graph_id == graph.as_str()
                && hit.subject_iri == "./bulk/entity-000123.dat")
        );

        let keyword_hits = node.search(&reader, "trusted-bulk-keyword", 10).unwrap();
        assert!(!keyword_hits.is_empty());
    }

    #[test]
    fn test_import_jsonld_with_policy_rejects_invalid_new_graph() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:invalid-bootstrap-import");
        let writer = writer_auth();

        let invalid = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                    "about": { "@id": graph.as_str() }
                },
                {
                    "@id": "./data/file.txt",
                    "@type": "MediaObject",
                    "name": "Orphaned file"
                }
            ]
        });

        let err = node
            .apply_rocrate_document_checked_with_policy(
                &writer,
                graph,
                &invalid.to_string(),
                public_policy(),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            CraqleError::RoCrate(RoCrateError::Update(UpdateError::ValidationFailed(_)))
        ));
    }

    #[test]
    fn test_trusted_bootstrap_rejects_non_empty_graph() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:trusted-bootstrap-non-empty");
        let writer = writer_auth();

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Existing Dataset",
                "Existing graph",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        let jsonld =
            benchmark_rocrate_document(&graph, 5, "trusted-existing-keyword", "Trusted Existing");
        let err = node
            .bootstrap_rocrate_document(&writer, graph, &jsonld, public_policy())
            .unwrap_err();

        assert!(matches!(
            err,
            CraqleError::RoCrate(RoCrateError::InvalidGraph(_))
        ));
    }

    #[test]
    fn test_batched_append_updates_search_without_manual_reindex() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:append-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Append Search Dataset",
                "Batched append search refresh",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        node.append_new_root_data_entities(
            &writer,
            &graph,
            benchmark_media_object_entities(
                0,
                250,
                "append-bulk-keyword",
                "Append Entity",
                "append record",
                "APPEND",
            ),
        )
        .unwrap();

        let hits = node.search(&reader, "APPEND-000123", 10).unwrap();
        assert!(
            hits.iter().any(|hit| hit.graph_id == graph.as_str()
                && hit.subject_iri == "./bulk/entity-000123.dat")
        );

        let keyword_hits = node.search(&reader, "append-bulk-keyword", 10).unwrap();
        assert!(!keyword_hits.is_empty());
    }

    #[test]
    fn test_graph_reindex_marker_refreshes_search_results() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:graph-reindex-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Graph Reindex Dataset",
                "Graph-level reindex marker test",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        node.append_new_root_data_entities(
            &writer,
            &graph,
            benchmark_media_object_entities(
                0,
                25,
                "graph-reindex-keyword",
                "Graph Reindex Entity",
                "graph reindex record",
                "REINDEX",
            ),
        )
        .unwrap();

        node.reindex_search().unwrap();

        let hits = node.search(&reader, "REINDEX-000010", 10).unwrap();
        assert!(
            hits.iter().any(|hit| hit.graph_id == graph.as_str()
                && hit.subject_iri == "./bulk/entity-000010.dat")
        );
    }

    #[test]
    fn test_update_property_rewrites_rocrate() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = manager(net.peer(0));
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
            graph.as_str(),
            "schema:description",
            Some("Original description"),
            "Updated description",
        )
        .unwrap();
        net.peer(0)
            .insert_quads(
                &graph,
                vec![
                    (
                        EncodedTerm::from_named_node(&graph.0),
                        EncodedTerm::from_named_node(&vocab::schema_keywords()),
                        literal_term("kw-a"),
                    ),
                    (
                        EncodedTerm::from_named_node(&graph.0),
                        EncodedTerm::from_named_node(&vocab::schema_keywords()),
                        literal_term("kw-b"),
                    ),
                ],
            )
            .unwrap();
        mgr.update_property(
            &graph,
            graph.as_str(),
            "schema:keywords",
            Some("kw-a"),
            "kw-a-updated",
        )
        .unwrap();

        let exported = mgr.export_jsonld(&graph).unwrap();
        assert!(exported.contains("Updated description"));
        assert!(!exported.contains("Original description"));
        assert!(exported.contains("kw-a-updated"));
        assert!(exported.contains("kw-b"));
        assert!(!exported.contains("\"kw-a\""));
    }

    #[test]
    fn test_import_export_preserves_language_and_typed_value_objects() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:value-objects");
        let mgr = manager(net.peer(0));

        let jsonld = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                    "about": { "@id": graph.as_str() }
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Typed Demo",
                    "description": "Checks typed literal fidelity",
                    "datePublished": "2025-03-01",
                    "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
                    "hasPart": { "@id": "./data/file.txt" },
                    "comment": {
                        "@value": "bonjour",
                        "@language": "fr"
                    }
                },
                {
                    "@id": "./data/file.txt",
                    "@type": "MediaObject",
                    "name": "File",
                    "measurement": {
                        "@value": "42",
                        "@type": "http://example.org/datatype/custom-int"
                    }
                }
            ]
        });

        mgr.import_jsonld(graph.clone(), &jsonld.to_string())
            .unwrap();
        let exported = mgr.export_jsonld(&graph).unwrap();
        let exported_json: serde_json::Value = serde_json::from_str(&exported).unwrap();
        let graph_entries = exported_json["@graph"].as_array().unwrap();
        let root = graph_entries
            .iter()
            .find(|entry| entry["@id"] == graph.as_str())
            .unwrap();
        let file = graph_entries
            .iter()
            .find(|entry| entry["@id"] == "./data/file.txt")
            .unwrap();

        assert_eq!(root["comment"]["@language"], "fr");
        assert_eq!(root["comment"]["@value"], "bonjour");
        assert_eq!(
            file["measurement"]["@type"],
            "http://example.org/datatype/custom-int"
        );
        assert_eq!(file["measurement"]["@value"], "42");
    }

    #[test]
    fn test_full_rocrate_lifecycle_scenario() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-lifecycle");
        let mgr0 = manager(net.peer(0));
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
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
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
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
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
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "http://schema.org/funder",
                    )),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("#grant")),
                )],
            )
            .unwrap();

        net.sync_until_converged(10).unwrap();

        let mgr1 = manager(net.peer(1));
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
            graph.as_str(),
            "schema:description",
            None,
            "Data from experiment X with updated notes",
        )
        .unwrap();

        mgr0.add_data_entity_under(
            &graph,
            graph.as_str(),
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
        assert!(
            experiment_hits
                .iter()
                .any(|subject| subject == graph.as_str())
        );
        assert!(
            report_hits
                .iter()
                .any(|subject| subject.contains("report.pdf"))
        );
    }

    #[test]
    fn test_benchmark_exports_can_omit_or_page_data_entities() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-export-page");
        let mgr = manager(net.peer(0));

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
    fn test_benchmark_exports_include_linked_contextual_entities() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-export-contextual");
        let mgr = manager(net.peer(0));

        mgr.create_crate(
            graph.clone(),
            "Contextual Export Crate",
            "Summary and page exports should retain contextual entities",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        mgr.add_contextual_entity(
            &graph,
            "#alice",
            "http://schema.org/Person",
            "Alice Example",
            vec![],
        )
        .unwrap();
        net.peer(0)
            .insert_quads(
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                        "http://schema.org/creator",
                    )),
                    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("#alice")),
                )],
            )
            .unwrap();
        mgr.add_data_entity(
            &graph,
            "data/sample.dat",
            "http://schema.org/MediaObject",
            "Sample Data",
            vec![(
                oxrdf::NamedNode::new_unchecked("http://schema.org/creator"),
                oxrdf::Term::NamedNode(oxrdf::NamedNode::new_unchecked("#alice")),
            )],
        )
        .unwrap();

        let summary = mgr.export_jsonld_summary(&graph).unwrap();
        assert!(summary.contains("Contextual Export Crate"));
        assert!(summary.contains("Alice Example"));
        assert!(!summary.contains("sample.dat"));

        let page = mgr.export_jsonld_page_after(&graph, None, 1).unwrap();
        assert!(page.jsonld.contains("sample.dat"));
        assert!(page.jsonld.contains("Alice Example"));
    }

    #[test]
    fn test_benchmark_export_cursor_pages_can_resume() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate-export-cursor");
        let mgr = manager(net.peer(0));

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
}
