mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    const DEFAULT_TOTAL_ENTITIES: usize = 300_000;
    const DEFAULT_BATCH_SIZES: &[usize] = &[10_000, 50_000, 100_000];
    const DEFAULT_SYNC_EVERY_BATCHES: usize = 1;

    #[test]
    #[ignore = "release-only batched ingest workflow profile"]
    fn batched_ingest_workflow_profile() {
        let total_entities = env_usize(
            "CRAQLE_BATCH_PROFILE_TOTAL_ENTITIES",
            DEFAULT_TOTAL_ENTITIES,
        );
        let sync_every_batches = env_usize(
            "CRAQLE_BATCH_PROFILE_SYNC_EVERY_BATCHES",
            DEFAULT_SYNC_EVERY_BATCHES,
        );
        let batch_sizes = env_usize_list("CRAQLE_BATCH_PROFILE_BATCH_SIZES", DEFAULT_BATCH_SIZES);
        assert!(total_entities > 0, "total_entities must be > 0");
        assert!(
            batch_sizes.iter().all(|size| *size > 0),
            "batch_sizes must be > 0"
        );

        for batch_size in batch_sizes {
            let (_tmp, net) = setup_network(2);
            let graph = GraphId::new(&format!("urn:batch-profile:{batch_size}"));

            net.peer(0)
                .create_crate(
                    &writer_auth(),
                    CreateCrateRequest::new(
                        graph.clone(),
                        format!("Batch Profile {batch_size}"),
                        "Batched RO-Crate ingest workflow",
                        "2026-03-27",
                        "https://creativecommons.org/licenses/by/4.0/",
                        public_policy(),
                    ),
                )
                .unwrap();
            net.sync_until_converged(10).unwrap();
            assert!(!net.peer(1).vector_clock(&graph).unwrap().0.is_empty());

            let mut build_latencies = Vec::new();
            let mut local_apply_latencies = Vec::new();
            let mut sync_latencies = Vec::new();

            let local_start = Instant::now();
            let total_batches = total_entities.div_ceil(batch_size);
            for (batch_index, start) in (0..total_entities).step_by(batch_size).enumerate() {
                let batch_count = usize::min(batch_size, total_entities - start);

                let build_start = Instant::now();
                let entities = benchmark_media_object_entities(
                    start,
                    batch_count,
                    "batch-profile",
                    "Proteomics sample",
                    "benchmark record",
                    "BENCH",
                );
                build_latencies.push(build_start.elapsed());

                let apply_start = Instant::now();
                net.peer(0)
                    .append_new_root_data_entities(&writer_auth(), &graph, entities)
                    .unwrap();
                local_apply_latencies.push(apply_start.elapsed());

                if sync_every_batches > 0 && (batch_index + 1) % sync_every_batches == 0 {
                    let sync_start = Instant::now();
                    net.sync_until_converged(100).unwrap();
                    sync_latencies.push(sync_start.elapsed());
                }
            }

            if sync_every_batches == 0 || total_batches % sync_every_batches != 0 {
                let sync_start = Instant::now();
                net.sync_until_converged(100).unwrap();
                sync_latencies.push(sync_start.elapsed());
            }
            let local_elapsed = local_start.elapsed();
            let local_commit_total =
                sum_durations(&build_latencies) + sum_durations(&local_apply_latencies);
            let replication_total = sum_durations(&sync_latencies);

            assert_eq!(
                net.peer(0).graph_fingerprint(&graph).unwrap(),
                net.peer(1).graph_fingerprint(&graph).unwrap()
            );
            let count_query = format!(
                "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
                graph.as_str()
            );
            let count_rows = solution_rows(
                net.peer(1)
                    .query(&GrantAuthorizer::default(), &count_query)
                    .unwrap(),
            );
            assert_eq!(
                binding_i64(count_rows[0].get("count").unwrap()),
                total_entities as i64
            );

            println!(
                "batch workflow size {}: total entities {}, workflow total {:?}, local batch creation {:?}, replication wait {:?}",
                batch_size, total_entities, local_elapsed, local_commit_total, replication_total,
            );
            println!("{}", format_stats("build", &build_latencies));
            println!("{}", format_stats("local apply", &local_apply_latencies));
            println!("{}", format_stats("sync", &sync_latencies));
        }
    }
}
