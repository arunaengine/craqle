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

        let jsonld = benchmark_rocrate_document(250, "import-bulk-keyword", "Imported Dataset");
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

        let jsonld = benchmark_rocrate_document(250, "trusted-bulk-keyword", "Trusted Dataset");
        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let hits = node.search(&reader, "DOC-000123", 10).unwrap();
        assert!(
            hits.iter().any(|hit| hit.graph_id == graph.as_str()
                && hit.subject_iri == "./bulk/entity-000123.dat")
