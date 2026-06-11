mod support;

/// Faithful reproduction of the aruna production cluster query workload:
/// aruna-shaped graphs (50/50 scaffold / RO-Crate, bench.py payload shapes),
/// `https://w3id.org/aruna/<ULID>` graph IRIs, applied through the normal
/// checked apply path with sprinkled deletes, then reopened cold. Runs the
/// exact cluster bench queries through `query_graphs_with` with a predicate
/// mirroring aruna's registry lookup (IRI tail parse + binary search).
#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 40_000;
    const DELETE_EVERY: usize = 50;
    const DEFAULT_SAMPLES: usize = 5;
    const DEFAULT_THREADS: usize = 8;

    const ASK_QUERY: &str = "ASK WHERE { ?d a <http://schema.org/Dataset> }";
    const SELECT_DATASETS: &str = "SELECT ?d ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                                   <http://schema.org/name> ?name . } LIMIT 25";
    const SELECT_FILES: &str = "SELECT ?f ?name WHERE { ?f a <http://schema.org/File> ; \
                                <http://schema.org/name> ?name . } LIMIT 100";

    const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    fn ulid_like(idx: usize) -> String {
        // Deterministic 26-char Crockford string, ordered by idx.
        let mut out = vec![b'0'; 26];
        let mut rest = idx;
        for slot in (10..26).rev() {
            out[slot] = CROCKFORD[rest % 32];
            rest /= 32;
        }
        let mut seed = idx.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for slot in 0..10 {
            out[slot] = CROCKFORD[(seed % 32) as usize];
            seed /= 32;
        }
        // Keep lexicographic order aligned with idx for binary search.
        out[..10].copy_from_slice(b"01HZBENCH0");
        String::from_utf8(out).unwrap()
    }

    fn graph_iri(idx: usize) -> String {
        format!("https://w3id.org/aruna/{}", ulid_like(idx))
    }

    fn doc_path(idx: usize) -> String {
        format!("bench/w{}/doc-{}", idx % 8, idx)
    }

    fn rocrate_jsonld(ds: &str, name: &str) -> String {
        serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": ds},
                },
                {
                    "@id": ds,
                    "@type": "Dataset",
                    "name": name,
                    "description": "k8s benchmark RO-Crate",
                    "datePublished": "2026-06-11",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                    "hasPart": [{"@id": format!("{ds}/file-1")}],
                },
                {
                    "@id": format!("{ds}/file-1"),
                    "@type": "File",
                    "name": format!("file-of-{name}"),
                    "contentSize": "1024",
                },
            ],
        })
        .to_string()
    }

    fn load_corpus(node: &CraqleNode, graph_count: usize) -> Vec<String> {
        let writer = writer_auth_for("/bench/**");
        let policy = public_policy_for("/bench/public");
        let started = Instant::now();
        let mut live = Vec::with_capacity(graph_count);
        for idx in 0..graph_count {
            let iri = graph_iri(idx);
            let graph = GraphId::new(&iri);
            let path = doc_path(idx);
            if idx % 2 == 0 {
                node.create_crate(
                    &writer,
                    CreateCrateRequest::new(
                        graph.clone(),
                        format!("Bench {path}"),
                        "k8s benchmark document",
                        "2026-06-11",
                        "https://creativecommons.org/licenses/by/4.0/",
                        policy.clone(),
                    ),
                )
                .unwrap();
            } else {
                let ds = format!("urn:bench:{path}");
                let jsonld = rocrate_jsonld(&ds, &format!("Bench Dataset {path}"));
                node.apply_rocrate_document_checked_with_policy(
                    &writer,
                    graph.clone(),
                    &jsonld,
                    policy.clone(),
                )
                .unwrap();
            }
            if idx % DELETE_EVERY == DELETE_EVERY - 1 {
                node.delete_graph_unchecked(&graph).unwrap();
            } else {
                live.push(iri);
            }
        }
        println!(
            "aruna-shaped corpus: {graph_count} graphs ({} live after deletes), loaded in {:?}",
            live.len(),
            started.elapsed(),
        );
        live.sort();
        live
    }

    /// Mirrors aruna's lazy registry predicate: extract the IRI tail, validate
    /// it as a ULID-shaped id, then binary-search the registry snapshot.
    fn registry_visible(registry: &[String], graph: &GraphId) -> bool {
        let iri = graph.as_str();
        let Some(tail) = iri.rsplit('/').next() else {
            return false;
        };
        if tail.len() != 26 || !tail.bytes().all(|b| CROCKFORD.contains(&b.to_ascii_uppercase())) {
            return false;
        }
        registry.binary_search_by(|entry| entry.as_str().cmp(iri)).is_ok()
    }

    fn measure(label: &str, samples: usize, mut run: impl FnMut() -> usize) -> Vec<Duration> {
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let _rows = run();
            latencies.push(start.elapsed());
        }
        println!("{}", format_stats(label, &latencies));
        latencies
    }

    fn measure_concurrent(
        label: &str,
        threads: usize,
        samples: usize,
        run: impl Fn() -> usize + Sync,
    ) -> Vec<Duration> {
        let wall_start = Instant::now();
        let latencies = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        let mut latencies = Vec::with_capacity(samples);
                        for _ in 0..samples {
                            let start = Instant::now();
                            let _rows = run();
                            latencies.push(start.elapsed());
                        }
                        latencies
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let wall = wall_start.elapsed();
        let qps = latencies.len() as f64 / wall.as_secs_f64();
        println!("{} [wall {:?}, {:.1} qps]", format_stats(label, &latencies), wall, qps);
        latencies
    }

    fn run_query_suite(node: &CraqleNode, registry: &[String], samples: usize, threads: usize) {
        let visible = |graph: &GraphId| registry_visible(registry, graph);

        let ask = || {
            let result = node.query_graphs_with(visible, ASK_QUERY).unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        };
        let select_datasets = || {
            let rows = solution_rows(node.query_graphs_with(visible, SELECT_DATASETS).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        };
        let select_files = || {
            let rows = solution_rows(node.query_graphs_with(visible, SELECT_FILES).unwrap());
            assert_eq!(rows.len(), 100);
            rows.len()
        };
        let mut needle_idx = registry.len() * 3 / 4;
        if needle_idx % DELETE_EVERY == DELETE_EVERY - 1 {
            needle_idx -= 1;
        }
        let needle = doc_path(needle_idx);
        let filter_contains = format!(
            "SELECT ?d ?name WHERE {{ ?d <http://schema.org/name> ?name . \
             FILTER(CONTAINS(?name, \"doc-{needle_idx}\")) }}"
        );
        let filter_scan = || {
            let rows = solution_rows(node.query_graphs_with(visible, &filter_contains).unwrap());
            assert!(
                rows.iter().any(|row| row
                    .get("name")
                    .is_some_and(|name| name.0.contains(&needle))),
                "needle row missing",
            );
            rows.len()
        };

        measure("cold ASK dataset", 1, ask);
        measure("cold SELECT datasets LIMIT 25", 1, select_datasets);
        measure("cold SELECT files LIMIT 100", 1, select_files);
        measure("cold FILTER CONTAINS scan", 1, filter_scan);

        measure("warm ASK dataset", samples, ask);
        measure("warm SELECT datasets LIMIT 25", samples, select_datasets);
        measure("warm SELECT files LIMIT 100", samples, select_files);
        measure("warm FILTER CONTAINS scan", samples, filter_scan);

        measure_concurrent("conc SELECT datasets LIMIT 25", threads, samples, select_datasets);
        measure_concurrent("conc SELECT files LIMIT 100", threads, samples, select_files);
    }

    #[test]
    #[ignore = "release-only scan cost breakdown"]
    fn aruna_shaped_corpus_scan_breakdown() {
        let graph_count = env_usize("CRAQLE_ARUNA_GRAPH_COUNT", DEFAULT_GRAPH_COUNT);
        let samples = env_usize("CRAQLE_ARUNA_QUERY_SAMPLES", DEFAULT_SAMPLES);
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let registry = load_corpus(&node, graph_count);
        let visible = |graph: &GraphId| registry_visible(registry.as_slice(), graph);
        let all = |_: &GraphId| true;

        let count_q = "SELECT (COUNT(*) AS ?c) WHERE { ?d <http://schema.org/name> ?name }";
        let contains_q = "SELECT ?d ?name WHERE { ?d <http://schema.org/name> ?name . \
                          FILTER(CONTAINS(?name, \"doc-0\")) }";
        let count_type_q =
            "SELECT (COUNT(*) AS ?c) WHERE { ?d a <http://schema.org/Dataset> }";

        measure("COUNT names (predicate registry)", samples, || {
            solution_rows(node.query_graphs_with(visible, count_q).unwrap()).len()
        });
        measure("COUNT names (predicate all)", samples, || {
            solution_rows(node.query_graphs_with(all, count_q).unwrap()).len()
        });
        measure("COUNT type quads (predicate registry)", samples, || {
            solution_rows(node.query_graphs_with(visible, count_type_q).unwrap()).len()
        });
        measure("CONTAINS scan (predicate registry)", samples, || {
            solution_rows(node.query_graphs_with(visible, contains_q).unwrap()).len()
        });
        measure("CONTAINS scan (predicate all)", samples, || {
            solution_rows(node.query_graphs_with(all, contains_q).unwrap()).len()
        });
    }

    #[test]
    #[ignore = "release-only aruna-corpus query-path reproduction"]
    fn aruna_shaped_corpus_select_latency() {
        let graph_count = env_usize("CRAQLE_ARUNA_GRAPH_COUNT", DEFAULT_GRAPH_COUNT);
        let samples = env_usize("CRAQLE_ARUNA_QUERY_SAMPLES", DEFAULT_SAMPLES);
        let threads = env_usize("CRAQLE_ARUNA_CONCURRENCY", DEFAULT_THREADS);
        assert!(graph_count > 0);

        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let registry = load_corpus(&node, graph_count);

        println!("--- pre-reopen (write-warm store) ---");
        run_query_suite(&node, &registry, samples, threads);

        drop(node);
        let reopen_started = Instant::now();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        node.ensure_query_indexes();
        println!("--- post-reopen (cold caches), reopen took {:?} ---", reopen_started.elapsed());
        run_query_suite(&node, &registry, samples, threads);
    }
}
