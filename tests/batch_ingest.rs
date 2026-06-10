mod support;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use anyhow::{Result, ensure};
    use craqle::{CraqleNode, CreateCrateRequest, GrantAuthorizer, GraphId};

    use crate::support::{
        CraqleCluster, benchmark_media_object_entities, dir_size_bytes, env_bool, env_usize,
        format_bytes, format_stats, public_policy_for, sum_durations, writer_auth_for,
    };

    const DEFAULT_TOTAL_ENTITIES: usize = 250_000;
    const DEFAULT_BATCH_SIZE: usize = 10_000;
    const DEFAULT_PEER_COUNT: usize = 2;
    const DEFAULT_SYNC_EVERY_BATCHES: usize = 0;
    const DEFAULT_QUERY_SAMPLES: usize = 3;
    const DEFAULT_PAGE_SIZE: usize = 1_000;
    const DEFAULT_MANUAL_COMPACT: bool = false;

    struct Config {
        data_dir: PathBuf,
        total_entities: usize,
        batch_size: usize,
        peer_count: usize,
        sync_every_batches: usize,
        query_samples: usize,
        page_size: usize,
        manual_compact: bool,
    }

    #[derive(Default)]
    struct ReplicationRound {
        message_count: usize,
        drain_elapsed: Duration,
        apply_elapsed: Duration,
    }

    #[test]
    #[ignore = "release-only batch ingest workflow profile"]
    fn batch_ingest_workflow_profile() {
        run_batch_ingest_workflow().unwrap();
    }

    fn run_batch_ingest_workflow() -> Result<()> {
        let config = Config::from_env();
        ensure!(
            config.peer_count == 1 || config.peer_count == 2,
            "batch ingest workflow currently supports 1 or 2 peers"
        );

        std::fs::create_dir_all(&config.data_dir)?;

        let cluster = CraqleCluster::new(config.peer_count, &config.data_dir)?;
        let writer = writer_auth_for("/demo/**");
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new(&format!(
            "urn:batch-ingest:{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));

        cluster.peer(0).create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Batch Ingest Demo",
                "RO-Crate batch ingest workflow",
                "2026-03-27",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy_for("/demo/batch-ingest"),
            ),
        )?;

        if config.peer_count > 1 {
            let _ = replicate_pending(&cluster)?;
        }

        let total_batches = config.total_entities.div_ceil(config.batch_size);
        let mut build_latencies = Vec::with_capacity(total_batches);
        let mut apply_latencies = Vec::with_capacity(total_batches);
        let mut replication_rounds = Vec::new();

        println!("data dir: {}", config.data_dir.display());
        println!(
            "graph: {}, entities: {}, batch size: {}, peers: {}, sync every: {} batch(es)",
            graph.as_str(),
            config.total_entities,
            config.batch_size,
            config.peer_count,
            config.sync_every_batches,
        );

        let load_start = Instant::now();
        for (batch_idx, start) in (0..config.total_entities)
            .step_by(config.batch_size)
            .enumerate()
        {
            let batch_count = usize::min(config.batch_size, config.total_entities - start);

            let build_start = Instant::now();
            let entities = benchmark_media_object_entities(
                start,
                batch_count,
                "batch-ingest",
                "Batch Entity",
                "record",
                "BATCH",
            );
            let build_elapsed = build_start.elapsed();
            build_latencies.push(build_elapsed);

            let apply_start = Instant::now();
            cluster
                .peer(0)
                .append_new_root_data_entities(&writer, &graph, entities)?;
            let apply_elapsed = apply_start.elapsed();
            apply_latencies.push(apply_elapsed);

            let mut sync_elapsed = None;
            if config.peer_count > 1
                && config.sync_every_batches > 0
                && (batch_idx + 1) % config.sync_every_batches == 0
            {
                let round = replicate_pending(&cluster)?;
                sync_elapsed = Some(round.drain_elapsed + round.apply_elapsed);
                replication_rounds.push(round);
            }

            println!(
                "batch {:>4}/{:>4} entities [{:>7}..{:>7}) build {:?} apply {:?}{}",
                batch_idx + 1,
                total_batches,
                start,
                start + batch_count,
                build_elapsed,
                apply_elapsed,
                sync_elapsed
                    .map(|elapsed| format!(" sync {:?}", elapsed))
                    .unwrap_or_default(),
            );
        }

        if config.peer_count > 1
            && (config.sync_every_batches == 0
                || !total_batches.is_multiple_of(config.sync_every_batches))
        {
            let round = replicate_pending(&cluster)?;
            replication_rounds.push(round);
        }

        let load_elapsed = load_start.elapsed();
        let local_commit_total = sum_durations(&build_latencies) + sum_durations(&apply_latencies);
        let replication_drain_total = sum_durations(
            &replication_rounds
                .iter()
                .map(|round| round.drain_elapsed)
                .collect::<Vec<_>>(),
        );
        let replication_apply_total = sum_durations(
            &replication_rounds
                .iter()
                .map(|round| round.apply_elapsed)
                .collect::<Vec<_>>(),
        );
        let replication_total = replication_drain_total + replication_apply_total;

        let local_diagnostics_start = Instant::now();
        cluster.peer(0).rebuild_graph_diagnostics(&graph)?;
        let local_diagnostics_elapsed = local_diagnostics_start.elapsed();

        let local_summary_latencies =
            sample_summary_query(cluster.peer(0), &reader, &graph, config.query_samples)?;
        let local_page_latencies = sample_page_query(
            cluster.peer(0),
            &reader,
            &graph,
            config.page_size,
            config.query_samples,
        )?;

        let replica_summary_latencies = if config.peer_count > 1 {
            Some(sample_summary_query(
                cluster.peer(1),
                &reader,
                &graph,
                config.query_samples,
            )?)
        } else {
            None
        };
        let replica_page_latencies = if config.peer_count > 1 {
            Some(sample_page_query(
                cluster.peer(1),
                &reader,
                &graph,
                config.page_size,
                config.query_samples,
            )?)
        } else {
            None
        };

        let local_page =
            cluster
                .peer(0)
                .export_rocrate_page_after(&reader, &graph, None, config.page_size)?;
        let replica_page = if config.peer_count > 1 {
            Some(cluster.peer(1).export_rocrate_page_after(
                &reader,
                &graph,
                None,
                config.page_size,
            )?)
        } else {
            None
        };

        let local_disk = peer_disk_usage(&config.data_dir, 0)?;
        let replica_disk = if config.peer_count > 1 {
            Some(peer_disk_usage(&config.data_dir, 1)?)
        } else {
            None
        };
        let mut local_compaction_elapsed = None;
        let mut replica_compaction_elapsed = None;
        let mut local_compacted_disk = None;
        let mut replica_compacted_disk = None;

        if config.manual_compact {
            let start = Instant::now();
            cluster.peer(0).manual_compact_store()?;
            local_compaction_elapsed = Some(start.elapsed());
            local_compacted_disk = Some(peer_disk_usage(&config.data_dir, 0)?);

            if config.peer_count > 1 {
                let start = Instant::now();
                cluster.peer(1).manual_compact_store()?;
                replica_compaction_elapsed = Some(start.elapsed());
                replica_compacted_disk = Some(peer_disk_usage(&config.data_dir, 1)?);
            }
        }

        println!("workflow total: {:?}", load_elapsed);
        println!("local batch creation total: {:?}", local_commit_total);
        println!("local diagnostics rebuild: {:?}", local_diagnostics_elapsed);
        if !replication_rounds.is_empty() {
            println!(
                "replication total: {:?} (drain {:?} + apply {:?})",
                replication_total, replication_drain_total, replication_apply_total,
            );
            println!(
                "replication messages: {} round(s), {} total message(s)",
                replication_rounds.len(),
                replication_rounds
                    .iter()
                    .map(|round| round.message_count)
                    .sum::<usize>(),
            );
        }
        println!("{}", format_stats("build", &build_latencies));
        println!("{}", format_stats("apply", &apply_latencies));
        if !replication_rounds.is_empty() {
            println!(
                "{}",
                format_stats(
                    "replication drain",
                    &replication_rounds
                        .iter()
                        .map(|round| round.drain_elapsed)
                        .collect::<Vec<_>>(),
                )
            );
            println!(
                "{}",
                format_stats(
                    "replication apply",
                    &replication_rounds
                        .iter()
                        .map(|round| round.apply_elapsed)
                        .collect::<Vec<_>>(),
                )
            );
        }

        println!(
            "peer0 disk: total {}, store {}, search {}",
            format_bytes(local_disk.total_bytes),
            format_bytes(local_disk.store_bytes),
            format_bytes(local_disk.search_bytes),
        );
        if let Some(replica_disk) = replica_disk {
            println!(
                "peer1 disk: total {}, store {}, search {}",
                format_bytes(replica_disk.total_bytes),
                format_bytes(replica_disk.store_bytes),
                format_bytes(replica_disk.search_bytes),
            );
        }
        if let (Some(elapsed), Some(disk)) = (local_compaction_elapsed, local_compacted_disk) {
            println!(
                "peer0 manual compaction: {:?}, compacted disk total {}, store {}, search {}",
                elapsed,
                format_bytes(disk.total_bytes),
                format_bytes(disk.store_bytes),
                format_bytes(disk.search_bytes),
            );
        }
        if let (Some(elapsed), Some(disk)) = (replica_compaction_elapsed, replica_compacted_disk) {
            println!(
                "peer1 manual compaction: {:?}, compacted disk total {}, store {}, search {}",
                elapsed,
                format_bytes(disk.total_bytes),
                format_bytes(disk.store_bytes),
                format_bytes(disk.search_bytes),
            );
        }

        println!(
            "{}",
            format_stats("peer0 summary", &local_summary_latencies)
        );
        println!(
            "{}",
            format_stats("peer0 page(1000)", &local_page_latencies)
        );
        println!(
            "peer0 page result: total entities {}, returned {}",
            local_page.total_data_entities, local_page.returned_data_entities,
        );
        if let Some(latencies) = replica_summary_latencies.as_ref() {
            println!("{}", format_stats("peer1 summary", latencies));
        }
        if let Some(latencies) = replica_page_latencies.as_ref() {
            println!("{}", format_stats("peer1 page(1000)", latencies));
        }
        if let Some(page) = replica_page {
            println!(
                "peer1 page result: total entities {}, returned {}",
                page.total_data_entities, page.returned_data_entities,
            );
        }

        Ok(())
    }

    impl Config {
        fn from_env() -> Self {
            let default_dir = PathBuf::from(format!(
                "target/batch-ingest/{}",
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            ));

            Self {
                data_dir: std::env::var("CRAQLE_BATCH_INGEST_DATA_DIR")
                    .map(PathBuf::from)
                    .unwrap_or(default_dir),
                total_entities: env_usize(
                    "CRAQLE_BATCH_INGEST_TOTAL_ENTITIES",
                    DEFAULT_TOTAL_ENTITIES,
                ),
                batch_size: env_usize("CRAQLE_BATCH_INGEST_BATCH_SIZE", DEFAULT_BATCH_SIZE),
                peer_count: env_usize("CRAQLE_BATCH_INGEST_PEERS", DEFAULT_PEER_COUNT),
                sync_every_batches: env_usize(
                    "CRAQLE_BATCH_INGEST_SYNC_EVERY_BATCHES",
                    DEFAULT_SYNC_EVERY_BATCHES,
                ),
                query_samples: env_usize(
                    "CRAQLE_BATCH_INGEST_QUERY_SAMPLES",
                    DEFAULT_QUERY_SAMPLES,
                ),
                page_size: env_usize("CRAQLE_BATCH_INGEST_PAGE_SIZE", DEFAULT_PAGE_SIZE),
                manual_compact: env_bool(
                    "CRAQLE_BATCH_INGEST_MANUAL_COMPACT",
                    DEFAULT_MANUAL_COMPACT,
                ),
            }
        }
    }

    fn replicate_pending(cluster: &CraqleCluster) -> Result<ReplicationRound> {
        if cluster.peer_count() < 2 {
            return Ok(ReplicationRound::default());
        }

        let apply_start = Instant::now();
        let message_count = cluster.sync_pair(0, 1)?;
        let apply_elapsed = apply_start.elapsed();

        Ok(ReplicationRound {
            message_count,
            drain_elapsed: Duration::default(),
            apply_elapsed,
        })
    }

    fn sample_summary_query(
        node: &CraqleNode,
        reader: &GrantAuthorizer,
        graph: &GraphId,
        samples: usize,
    ) -> Result<Vec<Duration>> {
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let summary = node.export_rocrate_summary(reader, graph)?;
            latencies.push(start.elapsed());
            ensure!(!summary.is_empty(), "summary export should not be empty");
        }
        Ok(latencies)
    }

    fn sample_page_query(
        node: &CraqleNode,
        reader: &GrantAuthorizer,
        graph: &GraphId,
        page_size: usize,
        samples: usize,
    ) -> Result<Vec<Duration>> {
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let page = node.export_rocrate_page_after(reader, graph, None, page_size)?;
            latencies.push(start.elapsed());
            ensure!(
                page.returned_data_entities > 0,
                "page export should return data entities"
            );
        }
        Ok(latencies)
    }

    struct PeerDiskUsage {
        total_bytes: u64,
        store_bytes: u64,
        search_bytes: u64,
    }

    fn peer_disk_usage(root: &std::path::Path, peer_index: usize) -> Result<PeerDiskUsage> {
        let peer_dir = root.join(format!("peer_{peer_index}"));
        let store_dir = peer_dir.join("store");
        let search_dir = peer_dir.join("search");
        Ok(PeerDiskUsage {
            total_bytes: dir_size_bytes(&peer_dir),
            store_bytes: dir_size_bytes(&store_dir),
            search_bytes: dir_size_bytes(&search_dir),
        })
    }
}
