mod support;

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{Duration, Instant};

    use craqle::*;
    use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

    use crate::support::{env_usize, public_policy, writer_auth};

    #[test]
    #[ignore = "diagnostic probe for Craqle/Fjall durability policy latency"]
    fn fjall_durability_policy_metadata_probe() {
        let samples = env_usize("CRAQLE_FJALL_DURABILITY_PROBE_SAMPLES", 5);
        let subwrites = env_usize("CRAQLE_FJALL_DURABILITY_PROBE_SUBWRITES", 4);
        assert!(samples > 0, "samples must be > 0");
        assert!(subwrites > 0, "subwrites must be > 0");

        let root = probe_tempdir("craqle-fjall-durability-probe");

        let node_dir = root.path().join("craqle-node");
        let node = CraqleNode::open(&node_dir).unwrap();
        probe_craqle_metadata_ops("craqle-node-central-syncdata", &node, samples);

        probe_low_level_fjall(
            "low-level-syncall-per-subwrite",
            &root.path().join("low-level-syncall"),
            samples,
            subwrites,
            PersistMode::SyncAll,
            None,
        );
        probe_low_level_fjall(
            "low-level-buffer-no-central-persist",
            &root.path().join("low-level-buffer-no-persist"),
            samples,
            subwrites,
            PersistMode::Buffer,
            None,
        );
        probe_low_level_fjall(
            "low-level-buffer-central-syncdata",
            &root.path().join("low-level-buffer-syncdata"),
            samples,
            subwrites,
            PersistMode::Buffer,
            Some(PersistMode::SyncData),
        );
    }

    fn probe_craqle_metadata_ops(label: &str, node: &CraqleNode, samples: usize) {
        let writer = writer_auth();
        let mut create_latencies = Vec::with_capacity(samples);
        let mut policy_latencies = Vec::with_capacity(samples);

        for sample in 0..samples {
            let graph = GraphId::new(&format!("urn:perf:fjall-durability:crate:{sample}"));

            let create_start = Instant::now();
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    format!("Fjall Durability Probe {sample}"),
                    "Craqle metadata durability policy probe",
                    "2026-01-01",
                    "https://creativecommons.org/licenses/by/4.0/",
                    public_policy(),
                ),
            )
            .unwrap();
            create_latencies.push(create_start.elapsed());

            let policy = GraphPolicy {
                public: sample % 2 == 0,
                permission_paths: vec!["/tests/public".to_string()],
            };
            let policy_start = Instant::now();
            node.set_graph_policy(&writer, &graph, policy).unwrap();
            policy_latencies.push(policy_start.elapsed());
        }

        println!(
            "{label}: {}, {}",
            format_latency_stats("create_crate", &create_latencies),
            format_latency_stats("set_graph_policy", &policy_latencies),
        );
    }

    fn probe_low_level_fjall(
        label: &str,
        path: &Path,
        samples: usize,
        subwrites: usize,
        subwrite_persist: PersistMode,
        central_persist: Option<PersistMode>,
    ) {
        let db = Database::builder(path).open().unwrap();
        let graphs = db
            .keyspace("graphs", KeyspaceCreateOptions::default)
            .unwrap();
        let log = db.keyspace("log", KeyspaceCreateOptions::default).unwrap();
        let mut latencies = Vec::with_capacity(samples);

        for sample in 0..samples {
            let start = Instant::now();
            for subwrite in 0..subwrites {
                commit_metadata_subwrite(&db, &graphs, &log, sample, subwrite, subwrite_persist);
            }
            if let Some(mode) = central_persist {
                db.persist(mode).unwrap();
            }
            latencies.push(start.elapsed());
        }

        println!(
            "{label}: subwrites={}, {}",
            subwrites,
            format_latency_stats("logical_op", &latencies),
        );
    }

    fn commit_metadata_subwrite(
        db: &Database,
        graphs: &Keyspace,
        log: &Keyspace,
        sample: usize,
        subwrite: usize,
        persist_mode: PersistMode,
    ) {
        let mut batch = db.batch().durability(Some(persist_mode));
        let sequence = ((sample as u64) << 32) | subwrite as u64;
        batch.insert(
            graphs,
            format!("graph-policy:{sample:08}:{subwrite:02}"),
            format!("public=true;path=/tests/public;seq={sequence}"),
        );
        batch.insert(
            log,
            sequence.to_be_bytes(),
            format!("metadata-op:{sequence}"),
        );
        batch.commit().unwrap();
    }

    fn probe_tempdir(prefix: &str) -> tempfile::TempDir {
        match std::env::var_os("CRAQLE_FJALL_DURABILITY_PROBE_ROOT") {
            Some(root) => tempfile::Builder::new()
                .prefix(prefix)
                .tempdir_in(root)
                .unwrap(),
            None => tempfile::Builder::new().prefix(prefix).tempdir().unwrap(),
        }
    }

    fn format_latency_stats(label: &str, values: &[Duration]) -> String {
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
