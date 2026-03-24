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
                    "https://creativecommons.org/licenses/by/4.0/",
                    public_policy(),
                ),
            )
            .unwrap();

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
                    "https://creativecommons.org/licenses/by/4.0/",
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
                    "https://creativecommons.org/licenses/by/4.0/",
                    public_policy(),
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .update_property(
                &writer,
                &graph,
                "./",
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

