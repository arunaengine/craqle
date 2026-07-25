mod support;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 6;
    const DEFAULT_ENTITIES_PER_GRAPH: usize = 8_000;
    const DEFAULT_CONTEXTUALS_PER_GRAPH: usize = 6;
    const DEFAULT_BATCH_SIZE: usize = 2_000;

    #[derive(Debug, Clone, Copy)]
    struct ColdStartConfig {
        graph_count: usize,
        entities_per_graph: usize,
        contextuals_per_graph: usize,
        batch_size: usize,
    }

    impl ColdStartConfig {
        fn from_env() -> Self {
            Self {
                graph_count: env_usize("CRAQLE_COLD_START_GRAPH_COUNT", DEFAULT_GRAPH_COUNT),
                entities_per_graph: env_usize(
                    "CRAQLE_COLD_START_ENTITIES_PER_GRAPH",
                    DEFAULT_ENTITIES_PER_GRAPH,
                ),
                contextuals_per_graph: env_usize(
                    "CRAQLE_COLD_START_CONTEXTUALS_PER_GRAPH",
                    DEFAULT_CONTEXTUALS_PER_GRAPH,
                ),
                batch_size: env_usize("CRAQLE_COLD_START_BATCH_SIZE", DEFAULT_BATCH_SIZE),
            }
        }

        fn total_entities(self) -> usize {
            self.graph_count * self.entities_per_graph
        }
    }

    #[test]
    #[ignore = "release-only cold-start reopen and rebuild profile"]
    fn cold_start_reopen_rebuild_to_query_ready() {
        let config = ColdStartConfig::from_env();
        assert!(config.graph_count > 0, "graph_count must be > 0");
        assert!(
            config.entities_per_graph > 0,
            "entities_per_graph must be > 0"
        );
        assert!(config.batch_size > 0, "batch_size must be > 0");
        let tmp = tempfile::tempdir().unwrap();
        let peer_dir = tmp.path().join("peer_0");
        let reader = GrantAuthorizer::default();
        let mut graphs = Vec::with_capacity(config.graph_count);

        {
            let node = CraqleNode::open(&peer_dir).unwrap();

            let seed_start = Instant::now();
            for graph_idx in 0..config.graph_count {
                let graph = GraphId::new(&format!("urn:perf:cold-start:{graph_idx:02}"));
                graphs.push(graph.clone());

                node.create_crate(
                    &writer_auth(),
                    CreateCrateRequest::new(
                        graph.clone(),
                        format!("Cold Start Graph {graph_idx}"),
                        "Cold-start reopen and search rebuild profile",
                        "2026-03-30",
                        Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                        public_policy(),
                    ),
                )
                .unwrap();

                attach_contextual_entities(
                    &node,
                    &writer_auth(),
                    &graph,
                    &format!("{graph_idx:02}"),
                    config.contextuals_per_graph,
                    "Perf",
                );

                let keyword = format!("cold-start-keyword-{graph_idx:02}");
                for start in (0..config.entities_per_graph).step_by(config.batch_size) {
                    let batch_count =
                        usize::min(config.batch_size, config.entities_per_graph - start);
                    append_benchmark_media_objects(
                        &node,
                        &writer_auth(),
                        &graph,
                        start,
                        batch_count,
                        &keyword,
                    );
                }
            }
            let seed_elapsed = seed_start.elapsed();

            let warm_reindex_start = Instant::now();
            node.reindex_search().unwrap();
            let warm_reindex_elapsed = warm_reindex_start.elapsed();

            let warm_query = node
                .query(
                    &reader,
                    &format!(
                        "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
                        graphs[0].as_str()
                    ),
                )
                .unwrap();
            let warm_rows = solution_rows(warm_query);
            assert_eq!(
                binding_i64(warm_rows[0].get("count").unwrap()),
                config.entities_per_graph as i64
            );

            println!(
                "prepared cold-start corpus: {} graphs x {} entities ({} total) seeded in {:?}, warm fts reindex {:?}",
                config.graph_count,
                config.entities_per_graph,
                config.total_entities(),
                seed_elapsed,
                warm_reindex_elapsed,
            );
        }

        fs::remove_dir_all(peer_dir.join("search")).unwrap();

        let reopen_start = Instant::now();
        let reopened = CraqleNode::open(&peer_dir).unwrap();
        let reopen_elapsed = reopen_start.elapsed();

        let summary_start = Instant::now();
        let summary = reopened
            .export_rocrate_summary(&reader, &graphs[0])
            .unwrap();
        let summary_elapsed = summary_start.elapsed();
        assert!(summary.contains("Cold Start Graph 0"));
        assert!(summary.contains("Perf Person"));

        let count_query = format!(
            "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
            graphs[0].as_str()
        );
        let count_start = Instant::now();
        let count_rows = solution_rows(reopened.query(&reader, &count_query).unwrap());
        let count_elapsed = count_start.elapsed();
        assert_eq!(
            binding_i64(count_rows[0].get("count").unwrap()),
            config.entities_per_graph as i64
        );

        let search_term = format!("cold-start-keyword-{:02}", config.graph_count / 2);
        let expected_graph_id = format!("urn:perf:cold-start:{:02}", config.graph_count / 2);
        let search_start = Instant::now();
        let hits = reopened
            .search(
                &reader,
                SearchRequest {
                    query: &search_term,
                    limit: 10,
                },
            )
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(!hits.is_empty());
        assert!(peer_dir.join("search").exists());
        assert!(hits.iter().all(|hit| hit.graph_id == expected_graph_id));

        println!(
            "cold start reopen: {} graphs x {} entities ({} total), open+rebuild {:?}, first summary {:?}, first count {:?}, first search {:?}",
            config.graph_count,
            config.entities_per_graph,
            config.total_entities(),
            reopen_elapsed,
            summary_elapsed,
            count_elapsed,
            search_elapsed,
        );
    }
}
