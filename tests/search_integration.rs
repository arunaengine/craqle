mod support;

#[cfg(test)]
mod tests {
    use craqle::*;

    use crate::support::*;

    #[test]
    fn test_search_after_create() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Microbial Genomics Study",
                    "Analysis of microbial communities",
                    "2025-03-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.flush_search_updates().unwrap();

        let hits = net
            .peer(0)
            .search(&GrantAuthorizer::default(), "genomics", 10)
            .unwrap();
        assert!(!hits.is_empty(), "should find 'genomics' in crate name");
    }

    #[test]
    fn test_reindex_search_keeps_results_available() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph,
                    "Proteomics Dataset",
                    "Search helper should commit indexed documents",
                    "2025-03-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();

        net.reindex_search().unwrap();
        let hits = net
            .peer(0)
            .search(&GrantAuthorizer::default(), "proteomics", 10)
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_search_after_concurrent_edits_scenario() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-search-edit");
        let writer = writer_auth();
        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Microbial Genomics Study",
                    "Microbial sequencing",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .update_property(
                &writer,
                &graph,
                graph.as_str(),
                "schema:name",
                None,
                "Microbial Proteomics Study",
            )
            .unwrap();
        net.peer(1)
            .add_data_entity_with_triples(
                &writer,
                &graph,
                "assembly.txt",
                "http://schema.org/MediaObject",
                "Assembly Notes",
                vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                    oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                        "metagenomic assembly",
                    )),
                )],
            )
            .unwrap();

        net.sync_until_converged(10).unwrap();

        let proteomics = reindex_and_search(&net, 0, "proteomics");
        let metagenomic = reindex_and_search(&net, 0, "metagenomic");
        let genomics = reindex_and_search(&net, 0, "genomics");

        assert!(proteomics.iter().any(|subject| subject == graph.as_str()));
        assert!(
            metagenomic
                .iter()
                .any(|subject| subject.contains("assembly.txt"))
        );
        assert!(genomics.is_empty());
    }

    #[test]
    fn test_sparql_integrated_fts_uses_tantivy_hits() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-fts");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Integrated FTS Crate",
                    "SPARQL should see Tantivy hits",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.peer(0)
            .add_data_entity_with_triples(
                &writer,
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
          SERVICE <urn:craqle:fts> {{
            ?s fts:query "proteomics" .
            ?s fts:score ?score .
            ?s fts:graph ?g .
            ?s fts:limit 10 .
          }}
          GRAPH ?g {{ ?s schema:name ?name }}
          FILTER(?g = <{}>)
        }}
        ORDER BY DESC(?score)
        "#,
            graph.as_str()
        );

        let rows = solution_rows(
            net.peer(1)
                .query(&GrantAuthorizer::default(), &query)
                .unwrap(),
        );
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|row| {
            row.get("s")
                .is_some_and(|value| value.0.contains("proteomics-01.tsv"))
        }));
        assert!(rows[0].contains_key("score"));
    }

    #[test]
    fn test_search_index_persists_across_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:crate-fts-persist");

        {
            let node = CraqleNode::open(tmp.path().join("peer0")).unwrap();
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Persisted Search Crate",
                    "Committed Tantivy index should survive restart",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
            node.add_data_entity_with_triples(
                &writer_auth(),
                &graph,
                "data/proteomics-01.tsv",
                "http://schema.org/MediaObject",
                "Proteomics Table",
                vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                    oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                        "persisted proteomics peptide quantification",
                    )),
                )],
            )
            .unwrap();
            node.flush_search_updates().unwrap();

            let hits = node
                .search(&GrantAuthorizer::default(), "proteomics", 10)
                .unwrap();
            assert!(
                hits.iter()
                    .any(|hit| hit.subject_iri.contains("proteomics-01.tsv"))
            );
        }

        let reopened = CraqleNode::open(tmp.path().join("peer0")).unwrap();
        reopened.flush_search_updates().unwrap();
        let hits = reopened
            .search(&GrantAuthorizer::default(), "proteomics", 10)
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri.contains("proteomics-01.tsv"))
        );
    }

    #[test]
    fn test_remote_batch_sync_updates_search_without_reindex() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:remote-batch-search");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Remote Batch Dataset",
                    "Receiver should update search directly",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .append_new_root_data_entities(
                &writer,
                &graph,
                benchmark_media_object_entities(
                    0,
                    50,
                    "remote-batch-keyword",
                    "Remote Batch Entity",
                    "remote batch record",
                    "RBATCH",
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let hits = net
            .peer(1)
            .search(&GrantAuthorizer::default(), "RBATCH-000049", 10)
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri.contains("entity-000049.dat"))
        );
    }

    #[test]
    fn test_graph_delete_removes_search_results_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:delete-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Deleted Search Dataset",
                "This should disappear from search",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();
        node.flush_search_updates().unwrap();
        assert!(!node.search(&reader, "deleted", 10).unwrap().is_empty());

        node.delete_graph(&writer, &graph).unwrap();
        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Replacement Search Dataset",
                "Only replacement text should be searchable",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();
        node.flush_search_updates().unwrap();

        assert!(node.search(&reader, "deleted", 10).unwrap().is_empty());
        assert!(!node.search(&reader, "replacement", 10).unwrap().is_empty());
    }
}
