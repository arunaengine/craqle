//! Measures single write latency on a store that already holds many RO-Crate graphs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use craqle::*;

    const DEFAULT_GRAPHS: usize = 5_000;
    const DEFAULT_SAMPLES: usize = 200;

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn graph_id(index: usize) -> GraphId {
        GraphId::new(&format!("https://w3id.org/aruna/write-{index:06}"))
    }

    fn create_graph(node: &CraqleNode, index: usize, durability: CraqleRequestDurability) {
        let request = CreateCrateRequest::new(
            graph_id(index),
            format!("Write latency dataset {index}"),
            format!("Write latency corpus graph {index}"),
            "2026-01-01",
            Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            GraphPolicy {
                public: true,
                permission_paths: vec![format!("/realm/g/group/meta/doc-{index}")],
            },
        );
        node.create_crate_with_durability_as(&AllowAllAuthorizer, request, durability, None)
            .unwrap();
    }

    fn report(label: &str, mut samples: Vec<Duration>) {
        samples.sort();
        let at = |pct: usize| samples[(samples.len() - 1) * pct / 100];
        println!(
            "{label}: n={} p50={:?} p95={:?} p99={:?} max={:?}",
            samples.len(),
            at(50),
            at(95),
            at(99),
            samples.last().copied().unwrap_or_default()
        );
    }

    fn timed(samples: usize, mut run: impl FnMut(usize)) -> Vec<Duration> {
        (0..samples)
            .map(|sample| {
                let started = Instant::now();
                run(sample);
                started.elapsed()
            })
            .collect()
    }

    #[test]
    #[ignore = "release-only write latency profile"]
    fn write_latency_profile() {
        let graphs = env_usize("CRAQLE_PERF_WRITE_GRAPHS", DEFAULT_GRAPHS);
        let samples = env_usize("CRAQLE_PERF_WRITE_SAMPLES", DEFAULT_SAMPLES);
        let entities = env_usize("CRAQLE_PERF_WRITE_ENTITIES", 0);
        assert!(
            graphs > 0 && (1..=graphs).contains(&samples),
            "the profile needs at least one graph and between 1 and {graphs} samples"
        );
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_graph_store_persist_mode(CraqleFjallPersistMode::Buffer),
        )
        .unwrap();

        let seeded = Instant::now();
        let seed = timed(graphs, |index| {
            create_graph(&node, index, CraqleRequestDurability::WalAlreadyDurable)
        });
        println!("seeded {graphs} graphs in {:?}", seeded.elapsed());
        report(
            "seed create (last samples)",
            seed[graphs.saturating_sub(samples)..].to_vec(),
        );

        let mut next = graphs;
        report(
            "create wal",
            timed(samples, |_| {
                create_graph(&node, next, CraqleRequestDurability::WalAlreadyDurable);
                next += 1;
            }),
        );
        report(
            "create durable",
            timed(samples, |_| {
                create_graph(&node, next, CraqleRequestDurability::Durable);
                next += 1;
            }),
        );

        let policy = |sample: usize| GraphPolicy {
            public: true,
            permission_paths: vec![format!("/realm/g/group/meta/doc-{sample}")],
        };
        // Each updated crate first holds `entities` files; the timed update adds one more.
        let with_files = |graph: &GraphId, sample: usize, count: usize| {
            let exported = node.export_rocrate(&AllowAllAuthorizer, graph).unwrap();
            let mut document: serde_json::Value = serde_json::from_str(&exported).unwrap();
            let files = (0..count)
                .map(|file| format!("./data/file-{sample}-{file}.csv"))
                .collect::<Vec<_>>();
            let items = document["@graph"].as_array_mut().unwrap();
            items.extend(files.iter().map(|id| {
                serde_json::json!({"@id": id, "@type": "File", "name": id, "encodingFormat": "text/csv"})
            }));
            let root = items
                .iter_mut()
                .find(|entity| entity["@id"] == graph.as_str())
                .unwrap();
            root["hasPart"] = files
                .iter()
                .map(|id| serde_json::json!({"@id": id}))
                .collect();
            document.to_string()
        };
        let apply = |graph: &GraphId, sample: usize, jsonld: &str| {
            node.apply_rocrate_document_checked_with_policy_and_durability_as(
                &AllowAllAuthorizer,
                graph.clone(),
                jsonld,
                policy(sample),
                CraqleRequestDurability::WalAlreadyDurable,
                None,
            )
            .unwrap();
        };
        let documents = (0..samples)
            .map(|sample| {
                let graph = graph_id(sample * graphs / samples);
                if entities > 0 {
                    apply(&graph, sample, &with_files(&graph, sample, entities));
                }
                let jsonld = with_files(&graph, sample, entities + 1);
                (graph, jsonld)
            })
            .collect::<Vec<_>>();
        report(
            "apply update",
            timed(samples, |sample| {
                let (graph, jsonld) = &documents[sample];
                apply(graph, sample, jsonld);
            }),
        );
        report(
            "patch entity",
            timed(samples, |sample| {
                let request = PatchEntityRequest {
                    entity: CreateEntityRequest {
                        graph: documents[sample].0.clone(),
                        entity_id: format!("./data/patched-{sample}.txt"),
                        entity_type: "File".to_string(),
                        name: format!("Patched file {sample}"),
                        additional_triples: Vec::new(),
                    },
                    replaced_predicates: Vec::new(),
                };
                node.patch_data_with(
                    &AllowAllAuthorizer,
                    request,
                    CraqleRequestDurability::WalAlreadyDurable,
                    None,
                )
                .unwrap();
            }),
        );
        report(
            "set policy",
            timed(samples, |sample| {
                node.set_graph_policy(
                    &AllowAllAuthorizer,
                    &graph_id(sample),
                    GraphPolicy {
                        public: false,
                        permission_paths: vec![format!("/realm/g/group/meta/doc-{sample}")],
                    },
                )
                .unwrap();
            }),
        );
        report(
            "delete graph",
            timed(samples, |sample| {
                node.delete_graph(&AllowAllAuthorizer, &graph_id(graphs - 1 - sample))
                    .unwrap();
            }),
        );
    }
}
