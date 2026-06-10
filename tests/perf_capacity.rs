mod support;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    const DEFAULT_SINGLE_GRAPH_COUNT: usize = 1;
    const DEFAULT_SINGLE_GRAPH_ENTITIES: usize = 2_500_000;
    const DEFAULT_SINGLE_GRAPH_RAMP_ENTITIES: &[usize] =
        &[100_000, 250_000, 500_000, 1_000_000, 2_500_000];
    const DEFAULT_SINGLE_SUMMARY_SAMPLES: usize = 25;

    const DEFAULT_MANY_GRAPH_COUNT: usize = 1_000;
    const DEFAULT_MANY_GRAPH_ENTITIES: usize = 10_000;
    const DEFAULT_MANY_SUMMARY_SAMPLES: usize = 1_000;

    const DEFAULT_CONTEXTUALS_PER_GRAPH: usize = 6;
    const DEFAULT_BATCH_SIZE: usize = 10_000;
    const DEFAULT_BREAKDOWN_ENTITIES: usize = 100_000;

    #[derive(Debug, Clone, Copy)]
    struct CapacityConfig {
        graph_count: usize,
        entities_per_graph: usize,
        contextuals_per_graph: usize,
        batch_size: usize,
        summary_samples: usize,
    }

    #[derive(Debug, Clone, Copy)]
    struct BreakdownConfig {
        entities_per_graph: usize,
        contextuals_per_graph: usize,
        batch_size: usize,
    }

    impl BreakdownConfig {
        fn from_env() -> Self {
            Self {
                entities_per_graph: env_usize(
                    "CRAQLE_CAPACITY_BREAKDOWN_ENTITIES",
                    DEFAULT_BREAKDOWN_ENTITIES,
                ),
                contextuals_per_graph: env_usize(
                    "CRAQLE_CAPACITY_BREAKDOWN_CONTEXTUALS",
                    DEFAULT_CONTEXTUALS_PER_GRAPH,
                ),
                batch_size: env_usize("CRAQLE_CAPACITY_BREAKDOWN_BATCH_SIZE", DEFAULT_BATCH_SIZE),
            }
        }
    }

    #[derive(Debug, Default)]
    struct LoadBreakdown {
        change_build_latencies: Vec<Duration>,
        apply_latencies: Vec<Duration>,
        batch_bytes: Vec<u64>,
    }

    impl CapacityConfig {
        fn from_env(
            prefix: &str,
            graph_count: usize,
            entities_per_graph: usize,
            summary_samples: usize,
        ) -> Self {
            Self {
                graph_count: env_usize(&format!("{prefix}_GRAPH_COUNT"), graph_count),
                entities_per_graph: env_usize(
                    &format!("{prefix}_ENTITIES_PER_GRAPH"),
                    entities_per_graph,
                ),
                contextuals_per_graph: env_usize(
                    &format!("{prefix}_CONTEXTUALS_PER_GRAPH"),
                    DEFAULT_CONTEXTUALS_PER_GRAPH,
                ),
                batch_size: env_usize(&format!("{prefix}_BATCH_SIZE"), DEFAULT_BATCH_SIZE),
                summary_samples: env_usize(&format!("{prefix}_SUMMARY_SAMPLES"), summary_samples),
            }
        }

        fn total_entities(self) -> usize {
            self.graph_count * self.entities_per_graph
        }

        fn approximate_triples(self) -> u64 {
            let base_triples = 8u64;
            let contextual_links = self.contextuals_per_graph.min(3) as u64;
            let contextual_triples = (self.contextuals_per_graph as u64) * 2;
            let entity_triples = (self.entities_per_graph as u64) * 6;
            (self.graph_count as u64)
                * (base_triples + contextual_links + contextual_triples + entity_triples)
        }
    }

    #[test]
    #[ignore = "release-only 2.5M single-graph capacity profile"]
    fn single_graph_2_5_million_summary_and_disk_profile() {
        let config = CapacityConfig::from_env(
            "CRAQLE_CAPACITY_SINGLE",
            DEFAULT_SINGLE_GRAPH_COUNT,
            DEFAULT_SINGLE_GRAPH_ENTITIES,
            DEFAULT_SINGLE_SUMMARY_SAMPLES,
        );
        run_summary_and_disk_profile("single_graph_2_5m", config);
    }

    #[test]
    #[ignore = "release-only single-graph capacity ramp profile"]
    fn single_graph_capacity_ramp_profile() {
        let base = CapacityConfig::from_env(
            "CRAQLE_CAPACITY_SINGLE",
            DEFAULT_SINGLE_GRAPH_COUNT,
            DEFAULT_SINGLE_GRAPH_ENTITIES,
            1,
        );
        let counts = env_usize_list(
            "CRAQLE_CAPACITY_SINGLE_ENTITY_STEPS",
            DEFAULT_SINGLE_GRAPH_RAMP_ENTITIES,
        );

        for entities_per_graph in counts {
            let mut config = base;
            config.entities_per_graph = entities_per_graph;
            run_summary_and_disk_profile(
                &format!("single_graph_ramp_{entities_per_graph}"),
                config,
            );
        }
    }

    #[test]
    #[ignore = "release-only single-graph step breakdown profile"]
    fn single_graph_step_breakdown_profile() {
        let config = BreakdownConfig::from_env();

        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:capacity:breakdown:graph-0000");
        let load = seed_graph(
            net.peer(0),
            &graph,
            0,
            config.entities_per_graph,
            config.contextuals_per_graph,
            config.batch_size,
            "capacity-breakdown",
        );

        let diagnostics_start = Instant::now();
        net.peer(0).rebuild_graph_diagnostics(&graph).unwrap();
        let diagnostics_elapsed = diagnostics_start.elapsed();

        let fingerprint_start = Instant::now();
        let fingerprint = net.peer(0).graph_fingerprint(&graph).unwrap();
        let fingerprint_elapsed = fingerprint_start.elapsed();

        let pair_sync_start = Instant::now();
        let pair_sync_ops = net.sync_pair(0, 1).unwrap();
        let pair_sync_elapsed = pair_sync_start.elapsed();

        let peer0_reindex_start = Instant::now();
        net.peer(0).reindex_search().unwrap();
        let peer0_reindex_elapsed = peer0_reindex_start.elapsed();

        let peer1_reindex_start = Instant::now();
        net.peer(1).reindex_search().unwrap();
        let peer1_reindex_elapsed = peer1_reindex_start.elapsed();

        let (_tmp_sync, net_sync) = setup_network(2);
        let sync_graph = GraphId::new("urn:capacity:breakdown:sync-graph-0000");
        let sync_load = seed_graph(
            net_sync.peer(0),
            &sync_graph,
            0,
            config.entities_per_graph,
            config.contextuals_per_graph,
            config.batch_size,
            "capacity-breakdown-sync",
        );

        let sync_start = Instant::now();
        net_sync.sync_until_converged(50).unwrap();
        let sync_elapsed = sync_start.elapsed();

        println!(
            "breakdown config: {} entities, {} contextuals, batch {}",
            config.entities_per_graph, config.contextuals_per_graph, config.batch_size,
        );
        println!(
            "seed load: build {}, apply {}, mean batch payload {}",
            format_stats("change build", &load.change_build_latencies),
            format_stats("apply batch", &load.apply_latencies),
            format_bytes(mean_u64(&load.batch_bytes)),
        );
        println!(
            "post-load steps: diagnostics {:?}, fingerprint {:?} ({} quads), pair sync {:?} ({} ops), peer0 fts {:?}, peer1 fts {:?}",
            diagnostics_elapsed,
            fingerprint_elapsed,
            fingerprint.0,
            pair_sync_elapsed,
            pair_sync_ops,
            peer0_reindex_elapsed,
            peer1_reindex_elapsed,
        );
        println!(
            "sync path: build {}, apply {}, sync {:?}",
            format_stats("sync change build", &sync_load.change_build_latencies),
            format_stats("sync apply batch", &sync_load.apply_latencies),
            sync_elapsed,
        );
    }

    #[test]
    #[ignore = "release-only many-graph capacity profile"]
    fn many_small_graphs_summary_and_disk_profile() {
        let config = CapacityConfig::from_env(
            "CRAQLE_CAPACITY_MANY",
            DEFAULT_MANY_GRAPH_COUNT,
            DEFAULT_MANY_GRAPH_ENTITIES,
            DEFAULT_MANY_SUMMARY_SAMPLES,
        );
        run_summary_and_disk_profile("many_graphs", config);
    }

    fn run_summary_and_disk_profile(label: &str, config: CapacityConfig) {
        let tmp = tempfile::tempdir().unwrap();
        let cluster = CraqleCluster::new(1, tmp.path()).unwrap();
        let node = cluster.peer(0);
        let reader = GrantAuthorizer::default();

        let mut graphs = Vec::with_capacity(config.graph_count);
        let load_start = Instant::now();
        for graph_idx in 0..config.graph_count {
            let graph = GraphId::new(&format!("urn:capacity:graph-{graph_idx:04}"));
            graphs.push(graph.clone());

            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    format!("Capacity Graph {graph_idx}"),
                    "Summary and storage footprint capacity profile",
                    "2025-01-01",
                    "https://creativecommons.org/licenses/by/4.0/",
                    public_policy(),
                ),
            )
            .unwrap();

            attach_contextual_entities(
                node,
                &writer_auth(),
                &graph,
                &format!("{graph_idx:04}"),
                config.contextuals_per_graph,
                "Perf",
            );

            let graph_keyword = format!("capacity-keyword-{graph_idx:04}");
            for start in (0..config.entities_per_graph).step_by(config.batch_size) {
                let batch_count = usize::min(config.batch_size, config.entities_per_graph - start);
                append_benchmark_media_objects(
                    node,
                    &writer_auth(),
                    &graph,
                    start,
                    batch_count,
                    &graph_keyword,
                );
            }

            node.rebuild_graph_diagnostics(&graph).unwrap();
        }
        let load_elapsed = load_start.elapsed();

        let reindex_start = Instant::now();
        cluster.reindex_search().unwrap();
        let reindex_elapsed = reindex_start.elapsed();

        let peer_root = tmp.path().join("peer_0");
        let store_bytes = dir_size_bytes(&peer_root.join("store"));
        let search_bytes = dir_size_bytes(&peer_root.join("search"));
        let peer_total_bytes = dir_size_bytes(&peer_root);

        let sample_count = usize::max(1, config.summary_samples);
        let mut summary_latencies = Vec::with_capacity(sample_count);
        for sample in 0..sample_count {
            let graph = &graphs[sample % graphs.len()];
            let start = Instant::now();
            let summary = node.export_rocrate_summary(&reader, graph).unwrap();
            summary_latencies.push(start.elapsed());

            assert!(summary.contains("Capacity Graph"));
            assert!(summary.contains("Perf Person"));
            assert!(!summary.contains("./bulk/entity-"));
        }

        println!(
            "{}: {} graphs x {} entities ({} total, ~{} triples) loaded in {:?}, fts reindex in {:?}",
            label,
            config.graph_count,
            config.entities_per_graph,
            config.total_entities(),
            config.approximate_triples(),
            load_elapsed,
            reindex_elapsed,
        );
        println!("{}", format_stats("summary export", &summary_latencies));
        println!(
            "disk usage: peer {}, store {}, search {}, bytes/entity {:.2}, bytes/triple {:.2}",
            format_bytes(peer_total_bytes),
            format_bytes(store_bytes),
            format_bytes(search_bytes),
            peer_total_bytes as f64 / config.total_entities() as f64,
            peer_total_bytes as f64 / config.approximate_triples() as f64,
        );
    }

    fn seed_graph(
        node: &CraqleNode,
        graph: &GraphId,
        graph_idx: usize,
        entities_per_graph: usize,
        contextuals_per_graph: usize,
        batch_size: usize,
        keyword: &str,
    ) -> LoadBreakdown {
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                format!("Capacity Graph {graph_idx}"),
                "Summary and storage footprint capacity profile",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        attach_contextual_entities(
            node,
            &writer_auth(),
            graph,
            &format!("{graph_idx:04}"),
            contextuals_per_graph,
            "Perf",
        );

        let mut breakdown = LoadBreakdown::default();
        for start in (0..entities_per_graph).step_by(batch_size) {
            let batch_count = usize::min(batch_size, entities_per_graph - start);

            let build_start = Instant::now();
            let entities = benchmark_media_object_entities(
                start,
                batch_count,
                keyword,
                "Proteomics sample",
                "benchmark record",
                "BENCH",
            );
            breakdown.change_build_latencies.push(build_start.elapsed());

            let apply_start = Instant::now();
            let report = node
                .append_new_root_data_entities(&writer_auth(), graph, entities)
                .unwrap();
            breakdown.apply_latencies.push(apply_start.elapsed());
            breakdown
                .batch_bytes
                .push(postcard::to_allocvec(&report.batch).unwrap().len() as u64);
        }

        breakdown
    }
}
