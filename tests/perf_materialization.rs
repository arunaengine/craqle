mod support;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    const DEFAULT_PRELOAD_DOCS: usize = 2_000;
    const DEFAULT_DOCS: usize = 1_000;
    const DEFAULT_WIDTH: usize = 16;

    fn production_shaped_doc(graph: &GraphId) -> String {
        let root = graph.as_str();
        format!(
            r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "{root}"}}
    }},
    {{
      "@id": "{root}",
      "@type": "Dataset",
      "name": "Materialized Dataset {root}",
      "description": "Replicated metadata record for {root}",
      "datePublished": "2026-06-10",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}},
      "hasPart": [
        {{"@id": "./data/file-0.raw"}},
        {{"@id": "./data/file-1.raw"}}
      ]
    }},
    {{
      "@id": "./data/file-0.raw",
      "@type": "File",
      "name": "file-0.raw",
      "contentSize": "1048576",
      "encodingFormat": "application/octet-stream"
    }},
    {{
      "@id": "./data/file-1.raw",
      "@type": "File",
      "name": "file-1.raw",
      "contentSize": "2097152",
      "encodingFormat": "application/octet-stream"
    }}
  ]
}}"#
        )
    }

    fn doc_graph(nonce: u64, idx: usize) -> GraphId {
        GraphId::new(&format!("https://w3id.org/aruna/{nonce:016x}{idx:08}"))
    }

    fn doc_policy() -> GraphPolicy {
        GraphPolicy {
            public: true,
            permission_paths: vec!["/realm/group".to_string()],
        }
    }

    fn deterministic_actor(nonce: u64, idx: usize) -> ActorId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&nonce.to_be_bytes());
        bytes[8..16].copy_from_slice(&(idx as u64).to_be_bytes());
        ActorId::from_bytes(bytes)
    }

    fn open_materialization_node(path: &std::path::Path) -> CraqleNode {
        let irokle = irokle::Irokle::builder().build().unwrap();
        CraqleNode::open_with_options(
            path,
            CraqleOptions::new().with_irokle(irokle, CraqleIrokleOptions::new()),
        )
        .unwrap()
    }

    fn apply_doc(node: &CraqleNode, nonce: u64, idx: usize) -> Duration {
        let graph = doc_graph(nonce, idx);
        let jsonld = production_shaped_doc(&graph);
        let started = Instant::now();
        node.apply_rocrate_document_checked_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph,
            &jsonld,
            doc_policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(deterministic_actor(nonce, idx)),
        )
        .unwrap();
        started.elapsed()
    }

    fn apply_doc_trusted(node: &CraqleNode, nonce: u64, idx: usize) -> Duration {
        let graph = doc_graph(nonce, idx);
        let jsonld = production_shaped_doc(&graph);
        let started = Instant::now();
        node.apply_rocrate_document_checked_with_policy_and_durability_as(
            &AllowAllAuthorizer,
            graph,
            &jsonld,
            doc_policy(),
            CraqleRequestDurability::WalAlreadyDurable,
            Some(deterministic_actor(nonce, idx)),
        )
        .unwrap();
        started.elapsed()
    }

    fn create_doc(node: &CraqleNode, nonce: u64, idx: usize) -> Duration {
        let graph = doc_graph(nonce, idx);
        let request = CreateCrateRequest::new(
            graph,
            format!("Scaffold Dataset {nonce:x}-{idx}"),
            "Materialized scaffold record",
            "2026-06-10",
            Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            doc_policy(),
        );
        let started = Instant::now();
        node.create_crate_with_durability_as(
            &AllowAllAuthorizer,
            request,
            CraqleRequestDurability::WalAlreadyDurable,
            Some(deterministic_actor(nonce, idx)),
        )
        .unwrap();
        started.elapsed()
    }

    fn create_doc_trusted(node: &CraqleNode, nonce: u64, idx: usize) -> Duration {
        let graph = doc_graph(nonce, idx);
        let request = CreateCrateRequest::new(
            graph,
            format!("Scaffold Dataset {nonce:x}-{idx}"),
            "Materialized scaffold record",
            "2026-06-10",
            Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            doc_policy(),
        );
        let started = Instant::now();
        node.create_crate_with_durability_as(
            &AllowAllAuthorizer,
            request,
            CraqleRequestDurability::WalAlreadyDurable,
            Some(deterministic_actor(nonce, idx)),
        )
        .unwrap();
        started.elapsed()
    }

    struct PersistLoop {
        stop: Arc<std::sync::atomic::AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl PersistLoop {
        fn start(node: Arc<CraqleNode>) -> Self {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let loop_stop = stop.clone();
            let handle = std::thread::spawn(move || {
                while !loop_stop.load(Ordering::Acquire) {
                    node.persist_fjall().unwrap();
                }
            });
            Self {
                stop,
                handle: Some(handle),
            }
        }
    }

    impl Drop for PersistLoop {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn summarize(label: &str, mut latencies: Vec<Duration>, wall: Duration) {
        latencies.sort();
        let count = latencies.len().max(1);
        let total: Duration = latencies.iter().sum();
        let mean = total / count as u32;
        let p50 = latencies[(count / 2).min(count - 1)];
        let p95 = latencies[(count * 95 / 100).min(count - 1)];
        let max = latencies.last().copied().unwrap_or_default();
        println!(
            "{label}: n={count} mean={:.3}ms p50={:.3}ms p95={:.3}ms max={:.3}ms wall={:.3}s rate={:.0}/s",
            mean.as_secs_f64() * 1e3,
            p50.as_secs_f64() * 1e3,
            p95.as_secs_f64() * 1e3,
            max.as_secs_f64() * 1e3,
            wall.as_secs_f64(),
            count as f64 / wall.as_secs_f64(),
        );
    }

    fn run_concurrent<F>(
        node: &Arc<CraqleNode>,
        nonce: u64,
        docs: usize,
        width: usize,
        apply: F,
    ) -> (Vec<Duration>, Duration)
    where
        F: Fn(&CraqleNode, u64, usize) -> Duration + Copy + Send + Sync,
    {
        let next = Arc::new(AtomicUsize::new(0));
        let wall_started = Instant::now();
        let latencies = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..width {
                let node = node.clone();
                let next = next.clone();
                handles.push(scope.spawn(move || {
                    let mut local = Vec::new();
                    loop {
                        let idx = next.fetch_add(1, Ordering::Relaxed);
                        if idx >= docs {
                            return local;
                        }
                        local.push(apply(&node, nonce, idx));
                    }
                }));
            }
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        (latencies, wall_started.elapsed())
    }

    #[test]
    #[ignore = "release-only remote materialization latency profile"]
    fn remote_materialization_profile() {
        let preload = env_usize("CRAQLE_MAT_PRELOAD", DEFAULT_PRELOAD_DOCS);
        let docs = env_usize("CRAQLE_MAT_DOCS", DEFAULT_DOCS);
        let width = env_usize("CRAQLE_MAT_WIDTH", DEFAULT_WIDTH);
        let tmp = tempfile::tempdir().unwrap();
        let node = Arc::new(open_materialization_node(tmp.path()));

        // Preload: simulate an already-populated store.
        let preload_started = Instant::now();
        for idx in 0..preload {
            apply_doc(&node, 0xface, idx);
        }
        println!(
            "preload: {preload} docs in {:.2}s",
            preload_started.elapsed().as_secs_f64()
        );

        // Stage breakdown on a small sample (plan-only vs full apply).
        let mut plan_total = Duration::ZERO;
        let mut parse_total = Duration::ZERO;
        for idx in 0..100 {
            let graph = doc_graph(0xbeef, idx);
            let jsonld = production_shaped_doc(&graph);
            let parse_started = Instant::now();
            let _: serde_json::Value = serde_json::from_str(&jsonld).unwrap();
            parse_total += parse_started.elapsed();
            let plan_started = Instant::now();
            node.validate_rocrate_document_checked_with_policy(
                &AllowAllAuthorizer,
                graph,
                &jsonld,
                doc_policy(),
            )
            .unwrap();
            plan_total += plan_started.elapsed();
        }
        println!(
            "stage parse(json)={:.3}ms/doc plan+validate={:.3}ms/doc",
            parse_total.as_secs_f64() * 10.0,
            plan_total.as_secs_f64() * 10.0,
        );

        // Sequential apply.
        let wall_started = Instant::now();
        let latencies: Vec<Duration> = (0..docs).map(|idx| apply_doc(&node, 0xcafe, idx)).collect();
        let first100: Vec<Duration> = latencies.iter().take(100).copied().collect();
        let last100: Vec<Duration> = latencies.iter().rev().take(100).copied().collect();
        summarize("sequential", latencies, wall_started.elapsed());
        summarize("sequential-first100", first100, Duration::from_secs(1));
        summarize("sequential-last100", last100, Duration::from_secs(1));

        // Concurrent apply at production drain width.
        let (latencies, wall) = run_concurrent(&node, 0xdead, docs, width, apply_doc);
        summarize(&format!("concurrent-w{width}"), latencies, wall);

        // Scaffold create path (queue's CreateCrate effect).
        let wall_started = Instant::now();
        let latencies: Vec<Duration> = (0..docs)
            .map(|idx| create_doc(&node, 0xc0de, idx))
            .collect();
        summarize("scaffold-sequential", latencies, wall_started.elapsed());
        let (latencies, wall) = run_concurrent(&node, 0xd0de, docs, width, create_doc);
        summarize(&format!("scaffold-concurrent-w{width}"), latencies, wall);

        // Width-16 apply while a deferred-persist loop runs (as aruna does).
        {
            let _persist = PersistLoop::start(node.clone());
            let (latencies, wall) = run_concurrent(&node, 0xfade, docs, width, apply_doc);
            summarize(
                &format!("concurrent-w{width}+persist-loop"),
                latencies,
                wall,
            );
        }

        // Search flush cost after the run (deferred, off the apply path).
        let flush_started = Instant::now();
        node.flush_search_updates().unwrap();
        println!(
            "search flush after run: {:.2}s",
            flush_started.elapsed().as_secs_f64()
        );

        // Cold reopen: fjall block cache empty, derived indexes rebuilding.
        drop(node);
        let reopen_started = Instant::now();
        let node = Arc::new(open_materialization_node(tmp.path()));
        println!(
            "cold reopen: {:.2}s",
            reopen_started.elapsed().as_secs_f64()
        );
        let (latencies, wall) = run_concurrent(&node, 0xab1e, docs, width, apply_doc);
        summarize(&format!("cold-concurrent-w{width}"), latencies, wall);
    }

    #[test]
    #[ignore = "release-only trusted (pre-validated) materialization latency profile"]
    fn remote_materialization_trusted_profile() {
        let preload = env_usize("CRAQLE_MAT_PRELOAD", DEFAULT_PRELOAD_DOCS);
        let docs = env_usize("CRAQLE_MAT_DOCS", DEFAULT_DOCS);
        let width = env_usize("CRAQLE_MAT_WIDTH", DEFAULT_WIDTH);
        let tmp = tempfile::tempdir().unwrap();
        let node = Arc::new(open_materialization_node(tmp.path()));

        for idx in 0..preload {
            apply_doc_trusted(&node, 0xface, idx);
        }

        let wall_started = Instant::now();
        let latencies: Vec<Duration> = (0..docs)
            .map(|idx| apply_doc_trusted(&node, 0xcafe, idx))
            .collect();
        summarize("trusted-sequential", latencies, wall_started.elapsed());

        let (latencies, wall) = run_concurrent(&node, 0xdead, docs, width, apply_doc_trusted);
        summarize(&format!("trusted-concurrent-w{width}"), latencies, wall);

        let wall_started = Instant::now();
        let latencies: Vec<Duration> = (0..docs)
            .map(|idx| create_doc_trusted(&node, 0xc0de, idx))
            .collect();
        summarize(
            "trusted-scaffold-sequential",
            latencies,
            wall_started.elapsed(),
        );
        let (latencies, wall) = run_concurrent(&node, 0xd0de, docs, width, create_doc_trusted);
        summarize(
            &format!("trusted-scaffold-concurrent-w{width}"),
            latencies,
            wall,
        );

        {
            let _persist = PersistLoop::start(node.clone());
            let (latencies, wall) = run_concurrent(&node, 0xfade, docs, width, apply_doc_trusted);
            summarize(
                &format!("trusted-concurrent-w{width}+persist-loop"),
                latencies,
                wall,
            );
        }

        let flush_started = Instant::now();
        node.flush_search_updates().unwrap();
        println!(
            "search flush after run: {:.2}s",
            flush_started.elapsed().as_secs_f64()
        );
    }
}
