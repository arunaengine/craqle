mod support;

#[cfg(test)]
mod tests {
    #[cfg(feature = "iroh")]
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    #[test]
    #[ignore = "diagnostic probe for Craqle + Irokle write latency"]
    fn small_rocrate_write_latency_probe() {
        let samples = env_usize("CRAQLE_IROKLE_WRITE_PROBE_SAMPLES", 5);
        assert!(samples > 0, "samples must be > 0");

        let plain_dir = probe_tempdir("craqle-plain-write-probe");
        let plain = CraqleNode::open(plain_dir.path()).unwrap();
        probe_node("plain-craqle", &plain, None, samples);

        let memory_dir = probe_tempdir("craqle-irokle-memory-write-probe");
        let memory_irokle = irokle::Irokle::builder().build().unwrap();
        let memory_peer = irokle::Irokle::builder().build().unwrap().peer_id();
        let memory = CraqleNode::open_with_options(
            memory_dir.path(),
            CraqleOptions::new().with_irokle(memory_irokle, CraqleIrokleOptions::new()),
        )
        .unwrap();
        probe_node("irokle-memory", &memory, Some(memory_peer), samples);

        let durable_root = probe_tempdir("craqle-irokle-fjall-write-probe");
        let durable_irokle = irokle::Irokle::builder()
            .with_fjall_path_and_persist_mode(
                durable_root.path().join("irokle"),
                fjall::PersistMode::Buffer,
            )
            .unwrap()
            .build()
            .unwrap();
        let durable_peer = irokle::Irokle::builder().build().unwrap().peer_id();
        let durable = CraqleNode::open_with_options(
            durable_root.path().join("craqle"),
            CraqleOptions::new().with_irokle(durable_irokle, CraqleIrokleOptions::new()),
        )
        .unwrap();
        probe_node("irokle-fjall", &durable, Some(durable_peer), samples);
    }

    #[cfg(feature = "iroh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "diagnostic probe for Aruna-like net-backed Irokle write latency"]
    async fn aruna_like_net_backed_write_latency_probe() {
        let samples = env_usize("CRAQLE_IROKLE_NET_WRITE_PROBE_SAMPLES", 10);
        let peer_count = env_usize("CRAQLE_IROKLE_NET_WRITE_PROBE_PEERS", 2);
        let entity_count = env_usize("CRAQLE_IROKLE_NET_WRITE_PROBE_ENTITIES", 1);
        assert!(samples > 0, "samples must be > 0");
        assert!(peer_count > 0, "peer_count must be > 0");
        assert!(entity_count > 0, "entity_count must be > 0");

        probe_net_node(
            "iroh-fjall-all-async",
            None,
            irokle::WriteConcern::AsyncReplication,
            samples,
            peer_count,
            entity_count,
        )
        .await;

        probe_net_node(
            "iroh-fjall-craqle-local",
            None,
            irokle::WriteConcern::Local,
            samples,
            peer_count,
            entity_count,
        )
        .await;

        probe_net_node(
            "iroh-fjall-node-local",
            Some(irokle::WriteConcern::Local),
            irokle::WriteConcern::Local,
            samples,
            peer_count,
            entity_count,
        )
        .await;

        probe_aruna_service_node(
            "aruna-service-style-syncall",
            fjall::PersistMode::SyncAll,
            samples,
            peer_count,
            entity_count,
        )
        .await;

        probe_aruna_service_node(
            "aruna-service-style-buffer",
            fjall::PersistMode::Buffer,
            samples,
            peer_count,
            entity_count,
        )
        .await;
    }

    fn probe_node(label: &str, node: &CraqleNode, peer: Option<irokle::PeerId>, samples: usize) {
        let writer = writer_auth();
        let mut create_latencies = Vec::with_capacity(samples);
        let mut apply_latencies = Vec::with_capacity(samples);
        let mut peer_latencies = Vec::with_capacity(samples);

        for sample in 0..samples {
            let create_graph = GraphId::new(&format!("urn:perf:irokle:{label}:create:{sample}"));
            let create_start = Instant::now();
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    create_graph.clone(),
                    "Small Write Probe",
                    "Small RO-Crate write latency probe",
                    "2026-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
            create_latencies.push(create_start.elapsed());

            if let Some(peer) = peer {
                let peer_start = Instant::now();
                node.add_irokle_peer(&create_graph, peer).unwrap();
                peer_latencies.push(peer_start.elapsed());
            }

            let apply_graph = GraphId::new(&format!("urn:perf:irokle:{label}:apply:{sample}"));
            let jsonld = small_probe_rocrate(&apply_graph, 1);
            let apply_start = Instant::now();
            node.apply_rocrate_document_checked_with_policy(
                &writer,
                apply_graph,
                &jsonld,
                public_policy(),
            )
            .unwrap();
            apply_latencies.push(apply_start.elapsed());
        }

        println!(
            "{label}: create {}, strict_apply {}, add_peer {}",
            format_stats("create", &create_latencies),
            format_stats("strict_apply", &apply_latencies),
            format_stats("add_peer", &peer_latencies),
        );
    }

    #[cfg(feature = "iroh")]
    async fn probe_net_node(
        label: &str,
        node_write_concern: Option<irokle::WriteConcern>,
        craqle_write_concern: irokle::WriteConcern,
        samples: usize,
        peer_count: usize,
        entity_count: usize,
    ) {
        let root = probe_tempdir("craqle-iroh-net-write-probe");
        let mut peer_nodes = Vec::with_capacity(peer_count);
        let mut peers = Vec::with_capacity(peer_count);
        for peer_idx in 0..peer_count {
            let peer_node = net_backed_irokle_node(
                root.path().join(format!("irokle-peer-{peer_idx}")),
                Some(irokle::WriteConcern::Local),
            )
            .await;
            peers.push(peer_node.peer_id());
            peer_nodes.push(peer_node);
        }

        let irokle_node =
            net_backed_irokle_node(root.path().join("irokle-main"), node_write_concern).await;
        let node = CraqleNode::open_with_options(
            root.path().join("craqle"),
            CraqleOptions::new().with_irokle(
                irokle_node.clone(),
                CraqleIrokleOptions::new().with_write_concern(craqle_write_concern),
            ),
        )
        .unwrap();

        probe_node_with_peers(label, &node, &peers, samples, entity_count);

        irokle_node.shutdown_iroh().await;
        for peer_node in peer_nodes {
            peer_node.shutdown_iroh().await;
        }
    }

    #[cfg(feature = "iroh")]
    async fn net_backed_irokle_node(
        path: impl AsRef<std::path::Path>,
        write_concern: Option<irokle::WriteConcern>,
    ) -> irokle::Irokle<irokle::FjallStorage> {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let builder = irokle::Irokle::builder()
            .with_net(endpoint)
            .with_iroh_runtime_config(irokle::net::IrohRuntimeConfig {
                connect_timeout: Duration::from_millis(50),
                sync_io_timeout: Duration::from_millis(50),
                resync_interval: Duration::from_secs(60),
                resync_initial_backoff: Duration::from_millis(50),
                resync_max_backoff: Duration::from_millis(100),
                ..irokle::net::IrohRuntimeConfig::default()
            })
            .with_fjall_path_and_persist_mode(path, fjall::PersistMode::Buffer)
            .unwrap();
        let builder = match write_concern {
            Some(write_concern) => builder.with_write_concern(write_concern),
            None => builder,
        };
        builder.build().unwrap()
    }

    #[allow(dead_code)]
    fn probe_node_with_peers(
        label: &str,
        node: &CraqleNode,
        peers: &[irokle::PeerId],
        samples: usize,
        entity_count: usize,
    ) {
        let writer = writer_auth();
        let mut create_latencies = Vec::with_capacity(samples);
        let mut apply_latencies = Vec::with_capacity(samples);
        let mut peer_latencies = Vec::with_capacity(samples * peers.len());

        for sample in 0..samples {
            let create_graph = GraphId::new(&format!("urn:perf:iroh:{label}:create:{sample}"));
            let create_start = Instant::now();
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    create_graph.clone(),
                    "Aruna-like Net Write Probe",
                    "Network-backed Irokle write latency probe",
                    "2026-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
            create_latencies.push(create_start.elapsed());

            for peer in peers {
                let peer_start = Instant::now();
                node.add_irokle_peer(&create_graph, *peer).unwrap();
                peer_latencies.push(peer_start.elapsed());
            }

            let apply_graph = GraphId::new(&format!("urn:perf:iroh:{label}:apply:{sample}"));
            let jsonld = small_probe_rocrate(&apply_graph, entity_count);
            let apply_start = Instant::now();
            node.apply_rocrate_document_checked_with_policy(
                &writer,
                apply_graph,
                &jsonld,
                public_policy(),
            )
            .unwrap();
            apply_latencies.push(apply_start.elapsed());
        }

        println!(
            "{label}: peers={}, entities={}, create {}, strict_apply {}, add_peer {}",
            peers.len(),
            entity_count,
            format_stats("create", &create_latencies),
            format_stats("strict_apply", &apply_latencies),
            format_stats("add_peer", &peer_latencies),
        );
    }

    #[cfg(feature = "iroh")]
    async fn probe_aruna_service_node(
        label: &str,
        persist_mode: fjall::PersistMode,
        samples: usize,
        peer_count: usize,
        entity_count: usize,
    ) {
        let root = probe_tempdir("craqle-aruna-service-write-probe");
        let mut peer_nodes = Vec::with_capacity(peer_count);
        let mut peers = Vec::with_capacity(peer_count);
        for peer_idx in 0..peer_count {
            let peer_node =
                aruna_style_irokle_node(root.path().join(format!("irokle-peer-{peer_idx}")), None)
                    .await;
            peers.push(peer_node.peer_id());
            peer_nodes.push(peer_node);
        }

        let (irokle_node, net) =
            aruna_style_irokle_service(root.path().join("irokle-main"), persist_mode).await;
        let node = CraqleNode::open_with_options(
            root.path().join("craqle"),
            CraqleOptions::new().with_irokle(irokle_node.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();

        probe_node_with_peers_and_recheck(label, &node, &net, &peers, samples, entity_count);

        net.shutdown().await;
        for peer_node in peer_nodes {
            peer_node.shutdown_iroh().await;
        }
    }

    #[cfg(feature = "iroh")]
    async fn aruna_style_irokle_service(
        path: impl AsRef<std::path::Path>,
        persist_mode: fjall::PersistMode,
    ) -> (
        irokle::Irokle<irokle::FjallStorage>,
        Arc<irokle::net::IrohNet<irokle::FjallStorage>>,
    ) {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let node = irokle::Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .with_fjall_path_and_persist_mode(path, persist_mode)
            .unwrap()
            .build()
            .unwrap();
        let net = Arc::new(
            irokle::net::IrohNet::new_with_config(endpoint, node.clone(), probe_iroh_runtime())
                .unwrap(),
        );
        net.start_configured_resync_loop().unwrap();
        (node, net)
    }

    #[cfg(feature = "iroh")]
    async fn aruna_style_irokle_node(
        path: impl AsRef<std::path::Path>,
        write_concern: Option<irokle::WriteConcern>,
    ) -> irokle::Irokle<irokle::FjallStorage> {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .bind()
            .await
            .unwrap();
        let builder = irokle::Irokle::builder()
            .with_iroh_secret_key(endpoint.secret_key())
            .with_net(endpoint)
            .with_iroh_runtime_config(probe_iroh_runtime())
            .with_fjall_path_and_persist_mode(path, fjall::PersistMode::Buffer)
            .unwrap();
        let builder = match write_concern {
            Some(write_concern) => builder.with_write_concern(write_concern),
            None => builder,
        };
        builder.build().unwrap()
    }

    #[cfg(feature = "iroh")]
    fn probe_node_with_peers_and_recheck(
        label: &str,
        node: &CraqleNode,
        net: &irokle::net::IrohNet<irokle::FjallStorage>,
        peers: &[irokle::PeerId],
        samples: usize,
        entity_count: usize,
    ) {
        let writer = writer_auth();
        let mut create_latencies = Vec::with_capacity(samples);
        let mut apply_latencies = Vec::with_capacity(samples);
        let mut peer_latencies = Vec::with_capacity(samples * peers.len());
        let mut recheck_latencies = Vec::with_capacity(samples);

        for sample in 0..samples {
            let create_graph = GraphId::new(&format!("urn:perf:aruna:{label}:create:{sample}"));
            let create_start = Instant::now();
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    create_graph.clone(),
                    "Aruna Service Style Probe",
                    "Aruna-style Irokle service write latency probe",
                    "2026-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
            create_latencies.push(create_start.elapsed());

            for peer in peers {
                let peer_start = Instant::now();
                node.add_irokle_peer(&create_graph, *peer).unwrap();
                peer_latencies.push(peer_start.elapsed());
            }

            if let Some(topic_id) = node.irokle_topic_id(&create_graph).unwrap() {
                let recheck_start = Instant::now();
                net.schedule_topic_recheck(topic_id).unwrap();
                recheck_latencies.push(recheck_start.elapsed());
            }

            let apply_graph = GraphId::new(&format!("urn:perf:aruna:{label}:apply:{sample}"));
            let jsonld = small_probe_rocrate(&apply_graph, entity_count);
            let apply_start = Instant::now();
            node.apply_rocrate_document_checked_with_policy(
                &writer,
                apply_graph,
                &jsonld,
                public_policy(),
            )
            .unwrap();
            apply_latencies.push(apply_start.elapsed());
        }

        println!(
            "{label}: peers={}, entities={}, create {}, strict_apply {}, add_peer {}, schedule_recheck {}",
            peers.len(),
            entity_count,
            format_stats("create", &create_latencies),
            format_stats("strict_apply", &apply_latencies),
            format_stats("add_peer", &peer_latencies),
            format_stats("schedule_recheck", &recheck_latencies),
        );
    }

    #[cfg(feature = "iroh")]
    fn probe_iroh_runtime() -> irokle::net::IrohRuntimeConfig {
        irokle::net::IrohRuntimeConfig {
            connect_timeout: Duration::from_millis(50),
            sync_io_timeout: Duration::from_millis(50),
            resync_interval: Duration::from_secs(60),
            resync_initial_backoff: Duration::from_millis(50),
            resync_max_backoff: Duration::from_millis(100),
            ..irokle::net::IrohRuntimeConfig::default()
        }
    }

    fn small_probe_rocrate(graph: &GraphId, entity_count: usize) -> String {
        benchmark_rocrate_document(
            graph,
            entity_count,
            "small-write-probe",
            "Small Write Probe Import",
        )
    }

    fn probe_tempdir(prefix: &str) -> tempfile::TempDir {
        match std::env::var_os("CRAQLE_IROKLE_WRITE_PROBE_ROOT") {
            Some(root) => tempfile::Builder::new()
                .prefix(prefix)
                .tempdir_in(root)
                .unwrap(),
            None => tempfile::Builder::new().prefix(prefix).tempdir().unwrap(),
        }
    }

    fn format_stats(label: &str, values: &[Duration]) -> String {
        if values.is_empty() {
            return format!("{label}: n=0");
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[((sorted.len() * 95) / 100).min(sorted.len() - 1)];
        let max = *sorted.last().unwrap();
        let avg = sorted.iter().sum::<Duration>() / sorted.len() as u32;
        format!(
            "{label}: n={}, avg={avg:?}, p50={p50:?}, p95={p95:?}, max={max:?}",
            sorted.len()
        )
    }
}
