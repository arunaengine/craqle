mod support;

/// Query-shape matrix for the craqle plan optimizer over the aruna-shaped
/// 40k corpus: every shape is measured with the optimizer off (raw sparopt
/// plan, the BEFORE column) and on (craqle-owned plan, the AFTER column),
/// sequentially and 8-way concurrent for the headline shapes.
#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 40_000;
    const DELETE_EVERY: usize = 50;
    const DEFAULT_SAMPLES: usize = 5;
    const DEFAULT_THREADS: usize = 8;
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    fn ulid_like(idx: usize) -> String {
        let mut out = vec![b'0'; 26];
        let mut rest = idx;
        for slot in (10..26).rev() {
            out[slot] = CROCKFORD[rest % 32];
            rest /= 32;
        }
        out[..10].copy_from_slice(b"01HZBENCH0");
        String::from_utf8(out).unwrap()
    }

    fn graph_iri(idx: usize) -> String {
        format!("https://w3id.org/aruna/{}", ulid_like(idx))
    }

    fn doc_path(idx: usize) -> String {
        format!("bench/w{}/doc-{}", idx % 8, idx)
    }

    fn term_iri(iri: &str) -> EncodedTerm {
        EncodedTerm(format!("<{iri}>"))
    }

    fn term_str(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    /// Aruna-shaped graphs through the fast apply path: even idx mirrors the
    /// scaffold crate (Dataset root, no parts), odd idx mirrors the bench.py
    /// RO-Crate payload (Dataset root + File part). Every DELETE_EVERY-th
    /// graph is deleted again, like the cluster repro.
    fn load_corpus(node: &CraqleNode, graph_count: usize) -> Vec<String> {
        let started = Instant::now();
        let mut live = Vec::with_capacity(graph_count);
        for idx in 0..graph_count {
            let iri = graph_iri(idx);
            let graph = GraphId::new(&iri);
            let path = doc_path(idx);
            let root = term_iri(&iri);
            let insert = |subject: &EncodedTerm, predicate: &str, object: EncodedTerm| {
                MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: subject.clone(),
                    predicate: term_iri(predicate),
                    object,
                }
            };

            let mut changes = vec![
                insert(&root, RDF_TYPE, term_iri("http://schema.org/Dataset")),
                insert(
                    &root,
                    "http://schema.org/datePublished",
                    term_str("2026-06-11"),
                ),
                insert(
                    &root,
                    "http://schema.org/license",
                    term_iri("https://creativecommons.org/licenses/by/4.0/"),
                ),
            ];
            if idx % 2 == 0 {
                changes.push(insert(
                    &root,
                    "http://schema.org/name",
                    term_str(&format!("Bench {path}")),
                ));
                changes.push(insert(
                    &root,
                    "http://schema.org/description",
                    term_str("k8s benchmark document"),
                ));
            } else {
                let name = format!("Bench Dataset {path}");
                let file = term_iri(&format!("{iri}/file-1"));
                changes.push(insert(&root, "http://schema.org/name", term_str(&name)));
                changes.push(insert(
                    &root,
                    "http://schema.org/description",
                    term_str("k8s benchmark RO-Crate"),
                ));
                changes.push(insert(&root, "http://schema.org/hasPart", file.clone()));
                changes.push(insert(&file, RDF_TYPE, term_iri("http://schema.org/File")));
                changes.push(insert(
                    &file,
                    "http://schema.org/name",
                    term_str(&format!("file-of-{name}")),
                ));
                changes.push(insert(
                    &file,
                    "http://schema.org/contentSize",
                    term_str("1024"),
                ));
            }
            node.apply_changes_unchecked(&graph, changes).unwrap();
            if idx % DELETE_EVERY == DELETE_EVERY - 1 {
                node.delete_graph_unchecked(&graph).unwrap();
            } else {
                live.push(iri);
            }
        }
        println!(
            "matrix corpus: {graph_count} aruna-shaped graphs ({} live), loaded in {:?}",
            live.len(),
            started.elapsed(),
        );
        live.sort();
        live
    }

    fn registry_visible(registry: &[String], graph: &GraphId) -> bool {
        let iri = graph.as_str();
        let Some(tail) = iri.rsplit('/').next() else {
            return false;
        };
        if tail.len() != 26
            || !tail
                .bytes()
                .all(|b| CROCKFORD.contains(&b.to_ascii_uppercase()))
        {
            return false;
        }
        registry
            .binary_search_by(|entry| entry.as_str().cmp(iri))
            .is_ok()
    }

    struct Shape {
        label: &'static str,
        sparql: String,
        expect_rows: Option<usize>,
        headline: bool,
        /// Inherently corpus-bound (full scan); used for the regression guard
        /// instead of the index-driven latency target.
        corpus_bound: bool,
    }

    fn shape(
        label: &'static str,
        sparql: String,
        expect_rows: Option<usize>,
        headline: bool,
        corpus_bound: bool,
    ) -> Shape {
        Shape {
            label,
            sparql,
            expect_rows,
            headline,
            corpus_bound,
        }
    }

    fn matrix(live: &[String]) -> Vec<Shape> {
        // A specific RO-Crate dataset around 3/4 of the corpus (odd idx, not
        // deleted), anchoring the selective shapes.
        let mut needle_idx = (live.len() * 3 / 4) | 1;
        while needle_idx % DELETE_EVERY == DELETE_EVERY - 1 {
            needle_idx += 2;
        }
        let needle = format!("Bench Dataset {}", doc_path(needle_idx));
        let dataset_count_per_25 = 25;

        vec![
            shape(
                "S01 ask_dataset",
                "ASK WHERE { ?d a <http://schema.org/Dataset> }".into(),
                None,
                false,
                false,
            ),
            shape(
                "S02 datasets_limit25",
                "SELECT ?d ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name . } LIMIT 25"
                    .into(),
                Some(dataset_count_per_25),
                true,
                false,
            ),
            shape(
                "S03 bgp2_selective_last",
                format!(
                    "SELECT ?d WHERE {{ ?d a <http://schema.org/Dataset> . \
                     ?d <http://schema.org/name> \"{needle}\" }}"
                ),
                Some(1),
                true,
                false,
            ),
            shape(
                "S04 bgp2_selective_first",
                format!(
                    "SELECT ?d WHERE {{ ?d <http://schema.org/name> \"{needle}\" . \
                     ?d a <http://schema.org/Dataset> }}"
                ),
                Some(1),
                true,
                false,
            ),
            shape(
                "S05 optional_multi_limit25",
                "SELECT ?d ?name ?desc ?date WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name . OPTIONAL { \
                 ?d <http://schema.org/description> ?desc . \
                 ?d <http://schema.org/datePublished> ?date } } LIMIT 25"
                    .into(),
                Some(25),
                true,
                false,
            ),
            shape(
                "S06 filter_eq_name",
                format!(
                    "SELECT ?d ?name WHERE {{ ?d <http://schema.org/name> ?name . \
                     FILTER(?name = \"{needle}\") }}"
                ),
                Some(1),
                true,
                false,
            ),
            shape(
                "S07 union_limit50",
                "SELECT ?x ?name WHERE { { ?x a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name } UNION { ?x a <http://schema.org/File> ; \
                 <http://schema.org/name> ?name } } LIMIT 50"
                    .into(),
                Some(50),
                false,
                false,
            ),
            shape(
                "S08 graph_var_limit25",
                "SELECT ?g ?d ?name WHERE { GRAPH ?g { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name } } LIMIT 25"
                    .into(),
                Some(25),
                false,
                false,
            ),
            shape(
                "S09 filter_exists_limit25",
                "SELECT ?d ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name . FILTER EXISTS { \
                 ?d <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> } } LIMIT 25"
                    .into(),
                Some(25),
                true,
                false,
            ),
            shape(
                "S10 filter_not_exists_limit25",
                "SELECT ?d ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name . FILTER NOT EXISTS { \
                 ?d <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> } } LIMIT 25"
                    .into(),
                Some(25),
                false,
                false,
            ),
            shape(
                "S11 chain_limit100",
                "SELECT ?d ?fn WHERE { ?d a <http://schema.org/Dataset> . \
                 ?d <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> . \
                 ?f <http://schema.org/name> ?fn } LIMIT 100"
                    .into(),
                Some(100),
                true,
                false,
            ),
            shape(
                "S12 chain_anchored_worst_order",
                format!(
                    "SELECT ?fn WHERE {{ ?f <http://schema.org/name> ?fn . \
                     ?f a <http://schema.org/File> . ?d <http://schema.org/hasPart> ?f . \
                     ?d a <http://schema.org/Dataset> . \
                     ?d <http://schema.org/name> \"{needle}\" }}"
                ),
                Some(1),
                true,
                false,
            ),
            shape(
                "S13 distinct_limit25",
                "SELECT DISTINCT ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name } LIMIT 25"
                    .into(),
                Some(25),
                false,
                false,
            ),
            shape(
                "S14 order_by_selective_limit10",
                format!(
                    "SELECT ?d ?fn WHERE {{ ?d <http://schema.org/name> \"{needle}\" . \
                     ?d <http://schema.org/hasPart> ?f . ?f <http://schema.org/name> ?fn }} \
                     ORDER BY ?fn LIMIT 10"
                ),
                Some(1),
                false,
                false,
            ),
            shape(
                "S15 order_by_corpus_bound_limit10",
                "SELECT ?d ?name WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?name } ORDER BY ?name LIMIT 10"
                    .into(),
                Some(10),
                false,
                true,
            ),
            shape(
                "S16 filter_contains_scan",
                format!(
                    "SELECT ?d ?name WHERE {{ ?d <http://schema.org/name> ?name . \
                     FILTER(CONTAINS(?name, \"doc-{needle_idx}\")) }}"
                ),
                None,
                false,
                true,
            ),
        ]
    }

    fn percentile(samples: &mut [Duration], pct: f64) -> Duration {
        samples.sort_unstable();
        let idx = ((samples.len() as f64 - 1.0) * pct).round() as usize;
        samples[idx]
    }

    fn run_samples(samples: usize, mut run: impl FnMut() -> usize) -> (Vec<Duration>, usize) {
        let rows = run();
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let got = run();
            latencies.push(start.elapsed());
            assert_eq!(got, rows, "row count must be stable across samples");
        }
        (latencies, rows)
    }

    fn run_concurrent(
        threads: usize,
        samples: usize,
        run: impl Fn() -> usize + Sync,
    ) -> (Vec<Duration>, Duration) {
        let _ = run();
        let wall_start = Instant::now();
        let latencies = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        let mut latencies = Vec::with_capacity(samples);
                        for _ in 0..samples {
                            let start = Instant::now();
                            let _ = run();
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
        (latencies, wall_start.elapsed())
    }

    #[test]
    #[ignore = "release-only optimizer before/after matrix"]
    fn query_plan_matrix_before_after() {
        let graph_count = env_usize("CRAQLE_MATRIX_GRAPH_COUNT", DEFAULT_GRAPH_COUNT);
        let samples = env_usize("CRAQLE_MATRIX_SAMPLES", DEFAULT_SAMPLES);
        let threads = env_usize("CRAQLE_MATRIX_CONCURRENCY", DEFAULT_THREADS);

        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let registry = load_corpus(&node, graph_count);
        node.ensure_query_indexes();
        let visible = |graph: &GraphId| registry_visible(&registry, graph);

        let count_rows = |sparql: &str, optimize: bool| -> usize {
            match node
                .query_graphs_with_planner(visible, sparql, optimize)
                .unwrap()
            {
                QueryResults::Solutions(rows) => rows.len(),
                QueryResults::Boolean(value) => {
                    assert!(value, "ASK shapes must hold on the corpus");
                    1
                }
                QueryResults::Graph(triples) => triples.len(),
            }
        };

        println!("\n=== sequential matrix (p50 of {samples} warm samples) ===");
        println!(
            "{:<34} {:>12} {:>12} {:>9}  rows",
            "shape", "before", "after", "speedup"
        );
        let mut violations = Vec::new();
        for shape in matrix(&registry) {
            let (mut before, rows_before) =
                run_samples(samples, || count_rows(&shape.sparql, false));
            let (mut after, rows_after) = run_samples(samples, || count_rows(&shape.sparql, true));
            assert_eq!(
                rows_before, rows_after,
                "{}: row count diverged between modes",
                shape.label
            );
            if let Some(expected) = shape.expect_rows {
                assert_eq!(rows_after, expected, "{}: unexpected row count", shape.label);
            }
            let before_p50 = percentile(&mut before, 0.5);
            let after_p50 = percentile(&mut after, 0.5);
            let speedup = before_p50.as_secs_f64() / after_p50.as_secs_f64().max(1e-9);
            println!(
                "{:<34} {:>12} {:>12} {:>8.1}x  {}",
                shape.label,
                format!("{before_p50:?}"),
                format!("{after_p50:?}"),
                speedup,
                rows_after,
            );
            if shape.corpus_bound {
                // Full scans must not regress materially.
                if after_p50 > before_p50.mul_f64(1.3) + Duration::from_millis(5) {
                    violations.push(format!(
                        "{}: corpus-bound shape regressed {before_p50:?} -> {after_p50:?}",
                        shape.label
                    ));
                }
            } else if after_p50 > Duration::from_millis(10) {
                violations.push(format!(
                    "{}: index-driven shape too slow after optimization: {after_p50:?}",
                    shape.label
                ));
            }
        }

        println!("\n=== 8-way concurrent headline shapes (optimizer on) ===");
        for shape in matrix(&registry).into_iter().filter(|s| s.headline) {
            let (mut latencies, wall) =
                run_concurrent(threads, samples, || count_rows(&shape.sparql, true));
            let total = latencies.len();
            let p50 = percentile(&mut latencies, 0.5);
            let p99 = percentile(&mut latencies, 0.99);
            let qps = total as f64 / wall.as_secs_f64();
            println!(
                "{:<34} p50 {:>10} p99 {:>10}  {:>8.1} qps",
                shape.label,
                format!("{p50:?}"),
                format!("{p99:?}"),
                qps,
            );
            if p50 > Duration::from_millis(10) {
                violations.push(format!(
                    "{}: concurrent p50 too slow: {p50:?}",
                    shape.label
                ));
            }
        }

        // Written-order independence: both orders of the same BGP must be
        // within noise of each other with the optimizer on.
        let shapes = matrix(&registry);
        let worst = shapes.iter().find(|s| s.label.contains("S03")).unwrap();
        let best = shapes.iter().find(|s| s.label.contains("S04")).unwrap();
        let (mut worst_lat, _) = run_samples(samples * 4, || count_rows(&worst.sparql, true));
        let (mut best_lat, _) = run_samples(samples * 4, || count_rows(&best.sparql, true));
        let worst_p50 = percentile(&mut worst_lat, 0.5);
        let best_p50 = percentile(&mut best_lat, 0.5);
        println!(
            "\nwritten-order independence: worst-order p50 {worst_p50:?} vs best-order p50 {best_p50:?}"
        );
        if worst_p50 > best_p50.mul_f64(3.0) + Duration::from_millis(2) {
            violations.push(format!(
                "written-order dependence: {worst_p50:?} vs {best_p50:?}"
            ));
        }

        assert!(violations.is_empty(), "matrix violations:\n{}", violations.join("\n"));
    }
}
