mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    const PAGE_SIZE: usize = 1_000;
    const DEFAULT_CRATE_COUNT: usize = 24;
    const DEFAULT_ENTITIES_PER_CRATE: usize = 50_000;
    const DEFAULT_CONTEXTUALS_PER_CRATE: usize = 6;
    const DEFAULT_BATCH_SIZE: usize = 10_000;
    const DEFAULT_QUERY_SAMPLES: usize = 12;

    #[derive(Debug, Clone, Copy)]
    struct PerfConfig {
        crate_count: usize,
        entities_per_crate: usize,
        contextuals_per_crate: usize,
        batch_size: usize,
        query_samples: usize,
    }

    impl PerfConfig {
        fn from_env() -> Self {
            Self {
                crate_count: env_usize("CRAQLE_PERF_CRATE_COUNT", DEFAULT_CRATE_COUNT),
                entities_per_crate: env_usize(
                    "CRAQLE_PERF_ENTITIES_PER_CRATE",
                    DEFAULT_ENTITIES_PER_CRATE,
                ),
                contextuals_per_crate: env_usize(
                    "CRAQLE_PERF_CONTEXTUALS_PER_CRATE",
                    DEFAULT_CONTEXTUALS_PER_CRATE,
                ),
                batch_size: env_usize("CRAQLE_PERF_BATCH_SIZE", DEFAULT_BATCH_SIZE),
                query_samples: env_usize("CRAQLE_PERF_QUERY_SAMPLES", DEFAULT_QUERY_SAMPLES),
            }
        }

        fn total_entities(self) -> usize {
            self.crate_count * self.entities_per_crate
        }
    }

    #[test]
    #[ignore = "release-only large latency scenario"]
    fn large_multi_crate_latency_profile() {
        let config = PerfConfig::from_env();
        let (_tmp, net) = setup_network(2);
        let reader = GrantAuthorizer::default();

        let mut graphs = Vec::with_capacity(config.crate_count);
        let load_start = Instant::now();
        for crate_idx in 0..config.crate_count {
            let graph = GraphId::new(&format!("urn:perf:crate-{crate_idx:02}"));
            graphs.push(graph.clone());

            net.peer(0)
                .create_crate(
                    &writer_auth(),
                    CreateCrateRequest::new(
                        graph.clone(),
                        format!("Large Latency Crate {crate_idx}"),
                        "Release latency profile for very large RO-Crates",
                        "2025-01-01",
                        "https://creativecommons.org/licenses/by/4.0/",
                        public_policy(),
                    ),
                )
                .unwrap();

            attach_contextual_entities(
                net.peer(0),
                &writer_auth(),
                &graph,
                &format!("{crate_idx:02}"),
                config.contextuals_per_crate,
                "Perf",
            );
            keyword_insert(net.peer(0), &graph, "common-keyword");

            let crate_keyword = format!("crate-keyword-{crate_idx:02}");
            for start in (0..config.entities_per_crate).step_by(config.batch_size) {
                let batch_count = usize::min(config.batch_size, config.entities_per_crate - start);
                append_benchmark_media_objects(
                    net.peer(0),
                    &writer_auth(),
                    &graph,
                    start,
                    batch_count,
                    &crate_keyword,
                );
            }
        }
        let load_elapsed = load_start.elapsed();

        let sync_start = Instant::now();
        net.sync_until_converged(400).unwrap();
        let sync_elapsed = sync_start.elapsed();

        let reindex_start = Instant::now();
        net.reindex_search().unwrap();
        let reindex_elapsed = reindex_start.elapsed();

        let mut summary_latencies = Vec::new();
        let mut page_start_latencies = Vec::new();
        let mut page_deep_latencies = Vec::new();
        let mut count_latencies = Vec::new();
        let mut root_query_latencies = Vec::new();
        let mut point_query_latencies = Vec::new();
        let mut fts_latencies = Vec::new();

        for sample in 0..config.query_samples {
            let graph = &graphs[sample % graphs.len()];
            let graph_index = sample % graphs.len();

            let summary_start = Instant::now();
            let summary = net.peer(1).export_rocrate_summary(&reader, graph).unwrap();
            summary_latencies.push(summary_start.elapsed());
            assert!(summary.contains("Large Latency Crate"));
            assert!(summary.contains("Perf Person"));

            let page_start = Instant::now();
            let first_page = net
                .peer(1)
                .export_rocrate_page_after(&reader, graph, None, PAGE_SIZE)
                .unwrap();
            page_start_latencies.push(page_start.elapsed());
            assert_eq!(
                first_page.returned_data_entities,
                usize::min(PAGE_SIZE, config.entities_per_crate)
            );

            if config.entities_per_crate > PAGE_SIZE {
                let deep_cursor =
                    format!("./bulk/entity-{:06}.dat", config.entities_per_crate / 2 - 1);
                let page_deep_start = Instant::now();
                let deep_page = net
                    .peer(1)
                    .export_rocrate_page_after(&reader, graph, Some(&deep_cursor), PAGE_SIZE)
                    .unwrap();
                page_deep_latencies.push(page_deep_start.elapsed());
                assert!(!deep_page.jsonld.is_empty());
            }

            let count_query = format!(
                "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
                graph.as_str()
            );
            let count_start = Instant::now();
            let count_rows = solution_rows(net.peer(1).query(&reader, &count_query).unwrap());
            count_latencies.push(count_start.elapsed());
            assert_eq!(
                binding_i64(count_rows[0].get("count").unwrap()),
                config.entities_per_crate as i64
            );
