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
                        Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
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
                let deep_cursor = net
                    .peer(1)
                    .export_rocrate_page_after(&reader, graph, None, config.entities_per_crate / 2)
                    .unwrap()
                    .next_cursor
                    .expect("midpoint page must have a continuation cursor");
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

            let root_query = format!(
                "SELECT ?root ?name ?description WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name ?name . ?root schema:description ?description . }} }}",
                graph.as_str()
            );
            let root_start = Instant::now();
            let root_rows = solution_rows(net.peer(1).query(&reader, &root_query).unwrap());
            root_query_latencies.push(root_start.elapsed());
            assert_eq!(
                binding_literal(root_rows[0].get("name").unwrap()),
                format!("Large Latency Crate {graph_index}")
            );

            let entity_idx = sample_entity_index(sample, config.entities_per_crate);
            let identifier = format!("BENCH-{entity_idx:06}");
            let point_query = format!(
                "SELECT ?s ?name WHERE {{ GRAPH <{}> {{ ?s <http://schema.org/identifier> \"{}\" . ?s schema:name ?name . }} }}",
                graph.as_str(),
                identifier,
            );
            let point_start = Instant::now();
            let point_rows = solution_rows(net.peer(1).query(&reader, &point_query).unwrap());
            point_query_latencies.push(point_start.elapsed());
            assert!(!point_rows.is_empty());

            let fts_query = format!(
                r#"
            SELECT ?s ?score
            WHERE {{
              SERVICE <urn:craqle:fts> {{
                ?s fts:query "crate-keyword-{graph_index:02}" .
                ?s fts:score ?score .
                ?s fts:graph <{}> .
                ?s fts:limit 10 .
              }}
            }}
            ORDER BY DESC(?score)
            "#,
                graph.as_str(),
            );
            let fts_start = Instant::now();
            let fts_rows = solution_rows(net.peer(1).query(&reader, &fts_query).unwrap());
            fts_latencies.push(fts_start.elapsed());
            assert!(!fts_rows.is_empty());
        }

        let target_graph = &graphs[0];
        let insert_start = Instant::now();
        net.peer(0)
            .add_data_entity_with_triples(
                &writer_auth(),
                target_graph,
                "data/inserted-release-profile.dat",
                "http://schema.org/MediaObject",
                "Inserted Release Profile Data",
                vec![
                    (
                        oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                        oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                            "insert latency profile entity",
                        )),
                    ),
                    (
                        oxrdf::NamedNode::new_unchecked("http://schema.org/keywords"),
                        oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal("insert-latency")),
                    ),
                ],
            )
            .unwrap();
        let insert_elapsed = insert_start.elapsed();

        let insert_sync_start = Instant::now();
        net.sync_until_converged(100).unwrap();
        let insert_sync_elapsed = insert_sync_start.elapsed();

        let update_start = Instant::now();
        net.peer(0)
            .update_property(
                &writer_auth(),
                target_graph,
                target_graph.as_str(),
                "schema:description",
                None,
                "Release latency profile with updated description",
            )
            .unwrap();
        let update_elapsed = update_start.elapsed();

        let update_sync_start = Instant::now();
        net.sync_until_converged(100).unwrap();
        let update_sync_elapsed = update_sync_start.elapsed();

        let partial_update_start = Instant::now();
        net.peer(0)
            .update_property(
                &writer_auth(),
                target_graph,
                target_graph.as_str(),
                "http://schema.org/keywords",
                Some("common-keyword"),
                "common-keyword-updated",
            )
            .unwrap();
        let partial_update_elapsed = partial_update_start.elapsed();

        let partial_sync_start = Instant::now();
        net.sync_until_converged(100).unwrap();
        let partial_sync_elapsed = partial_sync_start.elapsed();

        let post_write_reindex_start = Instant::now();
        net.reindex_search().unwrap();
        let post_write_reindex_elapsed = post_write_reindex_start.elapsed();

        println!(
            "large perf config: {} crates x {} entities ({} total), {} contextual entities per crate, batch {}",
            config.crate_count,
            config.entities_per_crate,
            config.total_entities(),
            config.contextuals_per_crate,
            config.batch_size,
        );
        println!(
            "load {:?}, sync {:?}, initial fts reindex {:?}",
            load_elapsed, sync_elapsed, reindex_elapsed,
        );
        println!("{}", format_stats("summary export", &summary_latencies));
        println!(
            "{}",
            format_stats("cursor page start", &page_start_latencies)
        );
        if !page_deep_latencies.is_empty() {
            println!("{}", format_stats("cursor page deep", &page_deep_latencies));
        }
        println!("{}", format_stats("sparql count", &count_latencies));
        println!(
            "{}",
            format_stats("root summary query", &root_query_latencies)
        );
        println!(
            "{}",
            format_stats("point identifier query", &point_query_latencies)
        );
        println!("{}", format_stats("fts fixed graph", &fts_latencies));
        println!(
            "write latencies: insert {:?} + sync {:?}, update {:?} + sync {:?}, partial update {:?} + sync {:?}, post-write fts reindex {:?}",
            insert_elapsed,
            insert_sync_elapsed,
            update_elapsed,
            update_sync_elapsed,
            partial_update_elapsed,
            partial_sync_elapsed,
            post_write_reindex_elapsed,
        );
    }

    fn sample_entity_index(sample: usize, total: usize) -> usize {
        ((sample as u64 * 1_103_515_245 + 12_345) % total as u64) as usize
    }
}
