mod support;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use craqle::*;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 50_000;
    const DEFAULT_FILES_PER_GRAPH: usize = 3;
    const DEFAULT_QUERY_SAMPLES: usize = 5;
    const NEEDLE: &str = "needle-7";
    const NEEDLE_EVERY: usize = 10_000;

    fn term_iri(iri: &str) -> EncodedTerm {
        EncodedTerm(format!("<{iri}>"))
    }

    fn term_str(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    fn crate_changes(graph: &GraphId, graph_idx: usize, files_per_graph: usize) -> Vec<MaterializedQuadChange> {
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let root = term_iri(graph.as_str());
        let name = if graph_idx % NEEDLE_EVERY == 0 {
            format!("Bench Dataset {graph_idx:05} {NEEDLE}")
        } else {
            format!("Bench Dataset {graph_idx:05}")
        };

        let insert = |subject: &EncodedTerm, predicate: &str, object: EncodedTerm| {
            MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: term_iri(predicate),
                object,
            }
        };

        let mut changes = vec![
            insert(&root, rdf_type, term_iri("http://schema.org/Dataset")),
            insert(&root, "http://schema.org/name", term_str(&name)),
            insert(
                &root,
                "http://schema.org/description",
                term_str(&format!("Synthetic RO-Crate {graph_idx:05} for query profiling")),
            ),
            insert(&root, "http://schema.org/datePublished", term_str("2026-01-01")),
            insert(
                &root,
                "http://schema.org/license",
                term_iri("https://creativecommons.org/licenses/by/4.0/"),
            ),
        ];

        for file_idx in 0..files_per_graph {
            let file = term_iri(&format!("{}/data/file-{file_idx}.dat", graph.as_str()));
            changes.push(insert(&file, rdf_type, term_iri("http://schema.org/MediaObject")));
            changes.push(insert(
                &file,
                "http://schema.org/name",
                term_str(&format!("file-{graph_idx:05}-{file_idx}.dat")),
            ));
            changes.push(insert(&file, "http://schema.org/contentSize", term_str("1024")));
            changes.push(insert(
                &file,
                "http://schema.org/encodingFormat",
                term_str("text/plain"),
            ));
            changes.push(insert(&root, "http://schema.org/hasPart", file.clone()));
        }

        changes
    }

    fn measure(
        label: &str,
        samples: usize,
        mut run: impl FnMut() -> usize,
    ) -> Vec<Duration> {
        let _ = run();
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let _rows = run();
            latencies.push(start.elapsed());
        }
        println!("{}", format_stats(label, &latencies));
        latencies
    }

    fn load_corpus(node: &CraqleNode, graph_count: usize, files_per_graph: usize) -> Vec<GraphId> {
        let load_start = Instant::now();
        let mut graphs = Vec::with_capacity(graph_count);
        for graph_idx in 0..graph_count {
            let graph = GraphId::new(&format!("urn:bench:crate:{graph_idx:05}"));
            node.apply_changes_unchecked(&graph, crate_changes(&graph, graph_idx, files_per_graph))
                .unwrap();
            graphs.push(graph);
        }
        let load_elapsed = load_start.elapsed();
        node.flush_search_updates().unwrap();
        println!(
            "multigraph corpus: {graph_count} graphs x ~{} quads, loaded in {load_elapsed:?}",
            5 + files_per_graph * 5,
        );
        graphs
    }

    fn measure_concurrent(
        label: &str,
        threads: usize,
        samples: usize,
        run: impl Fn() -> usize + Sync,
    ) -> Vec<Duration> {
        let _ = run();
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
        println!(
            "{} [wall {:?}, {:.1} qps]",
            format_stats(label, &latencies),
            wall,
            qps,
        );
        latencies
    }

    /// 90%-visible predicate: hides graphs whose index ends in 7, so needle
    /// graphs (idx % NEEDLE_EVERY == 0) stay visible.
    fn ninety_percent_visible(graph: &GraphId) -> bool {
        !graph.as_str().ends_with('7')
    }

    #[test]
    #[ignore = "release-only multi-graph query-path profile"]
    fn query_graphs_latency_across_many_graphs() {
        let graph_count = env_usize("CRAQLE_MULTI_GRAPH_COUNT", DEFAULT_GRAPH_COUNT);
        let files_per_graph = env_usize("CRAQLE_MULTI_FILES_PER_GRAPH", DEFAULT_FILES_PER_GRAPH);
        let samples = env_usize("CRAQLE_MULTI_QUERY_SAMPLES", DEFAULT_QUERY_SAMPLES);
        assert!(graph_count > 0);

        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();

        let graphs = load_corpus(&node, graph_count, files_per_graph);

        measure("trivial ASK (graph list)", samples, || {
            let result = node.query_graphs(&graphs, "ASK { ?s ?p ?o }").unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        });
        measure("trivial ASK (predicate all)", samples, || {
            let result = node
                .query_graphs_with(|_: &GraphId| true, "ASK { ?s ?p ?o }")
                .unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        });
        measure("trivial ASK (predicate 90%)", samples, || {
            let result = node
                .query_graphs_with(ninety_percent_visible, "ASK { ?s ?p ?o }")
                .unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        });

        let select_limited = "SELECT ?s ?name WHERE { ?s schema:name ?name } LIMIT 25";
        measure("SELECT name LIMIT 25 (graph list)", samples, || {
            let rows = solution_rows(node.query_graphs(&graphs, select_limited).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        });
        measure("SELECT name LIMIT 25 (predicate all)", samples, || {
            let rows =
                solution_rows(node.query_graphs_with(|_: &GraphId| true, select_limited).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        });
        measure("SELECT name LIMIT 25 (predicate 90%)", samples, || {
            let rows = solution_rows(
                node.query_graphs_with(ninety_percent_visible, select_limited)
                    .unwrap(),
            );
            assert_eq!(rows.len(), 25);
            rows.len()
        });

        let expected_needles = graph_count.div_ceil(NEEDLE_EVERY);
        let filter_scan = format!(
            "SELECT ?s ?name WHERE {{ ?s schema:name ?name . FILTER(CONTAINS(?name, \"{NEEDLE}\")) }}"
        );
        measure("FILTER CONTAINS scan (graph list)", samples, || {
            let rows = solution_rows(node.query_graphs(&graphs, &filter_scan).unwrap());
            assert_eq!(rows.len(), expected_needles);
            rows.len()
        });
        measure("FILTER CONTAINS scan (predicate all)", samples, || {
            let rows =
                solution_rows(node.query_graphs_with(|_: &GraphId| true, &filter_scan).unwrap());
            assert_eq!(rows.len(), expected_needles);
            rows.len()
        });
        measure("FILTER CONTAINS scan (predicate 90%)", samples, || {
            let rows = solution_rows(
                node.query_graphs_with(ninety_percent_visible, &filter_scan)
                    .unwrap(),
            );
            assert_eq!(rows.len(), expected_needles);
            rows.len()
        });

        let graph_bound = "SELECT ?g ?name WHERE { GRAPH ?g { ?s rdf:type schema:Dataset . ?s schema:name ?name } } LIMIT 25";
        measure("GRAPH-bound type+name LIMIT 25 (graph list)", samples, || {
            let rows = solution_rows(node.query_graphs(&graphs, graph_bound).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        });
        measure("GRAPH-bound type+name LIMIT 25 (predicate all)", samples, || {
            let rows =
                solution_rows(node.query_graphs_with(|_: &GraphId| true, graph_bound).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        });
    }

    #[test]
    #[ignore = "release-only concurrent multi-graph query-path profile"]
    fn query_graphs_concurrent_latency_across_many_graphs() {
        let graph_count = env_usize("CRAQLE_MULTI_GRAPH_COUNT", 40_000);
        let files_per_graph = env_usize("CRAQLE_MULTI_FILES_PER_GRAPH", DEFAULT_FILES_PER_GRAPH);
        let samples = env_usize("CRAQLE_MULTI_QUERY_SAMPLES", DEFAULT_QUERY_SAMPLES);
        let threads = env_usize("CRAQLE_MULTI_CONCURRENCY", 8);
        assert!(graph_count > 0);

        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();

        let graphs = load_corpus(&node, graph_count, files_per_graph);
        let expected_needles = graph_count.div_ceil(NEEDLE_EVERY);

        let ask_query = "ASK { ?s ?p ?o }";
        let select_query = "SELECT ?s ?name WHERE { ?s schema:name ?name } LIMIT 25";
        let scan_query = format!(
            "SELECT ?s ?name WHERE {{ ?s schema:name ?name . FILTER(CONTAINS(?name, \"{NEEDLE}\")) }}"
        );
        let graph_bound_query = "SELECT ?g ?name WHERE { GRAPH ?g { ?s rdf:type schema:Dataset . ?s schema:name ?name } } LIMIT 25";

        let ask_list = || {
            let result = node.query_graphs(&graphs, ask_query).unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        };
        let ask_pred = || {
            let result = node
                .query_graphs_with(ninety_percent_visible, ask_query)
                .unwrap();
            assert_eq!(result, QueryResults::Boolean(true));
            1
        };
        let select_limited = || {
            let rows = solution_rows(node.query_graphs(&graphs, select_query).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        };
        let select_limited_pred = || {
            let rows = solution_rows(
                node.query_graphs_with(ninety_percent_visible, select_query)
                    .unwrap(),
            );
            assert_eq!(rows.len(), 25);
            rows.len()
        };
        let filter_scan = || {
            let rows = solution_rows(node.query_graphs(&graphs, &scan_query).unwrap());
            assert_eq!(rows.len(), expected_needles);
            rows.len()
        };
        let filter_scan_pred = || {
            let rows = solution_rows(
                node.query_graphs_with(ninety_percent_visible, &scan_query)
                    .unwrap(),
            );
            assert_eq!(rows.len(), expected_needles);
            rows.len()
        };
        let graph_bound = || {
            let rows = solution_rows(node.query_graphs(&graphs, graph_bound_query).unwrap());
            assert_eq!(rows.len(), 25);
            rows.len()
        };

        measure("seq trivial ASK (graph list)", samples, ask_list);
        measure_concurrent("conc trivial ASK (graph list)", threads, samples, ask_list);
        measure("seq trivial ASK (predicate 90%)", samples, ask_pred);
        measure_concurrent("conc trivial ASK (predicate 90%)", threads, samples, ask_pred);

        measure("seq SELECT name LIMIT 25 (graph list)", samples, select_limited);
        measure_concurrent(
            "conc SELECT name LIMIT 25 (graph list)",
            threads,
            samples,
            select_limited,
        );
        measure(
            "seq SELECT name LIMIT 25 (predicate 90%)",
            samples,
            select_limited_pred,
        );
        measure_concurrent(
            "conc SELECT name LIMIT 25 (predicate 90%)",
            threads,
            samples,
            select_limited_pred,
        );

        measure("seq FILTER CONTAINS scan (graph list)", samples, filter_scan);
        measure_concurrent(
            "conc FILTER CONTAINS scan (graph list)",
            threads,
            samples,
            filter_scan,
        );
        measure(
            "seq FILTER CONTAINS scan (predicate 90%)",
            samples,
            filter_scan_pred,
        );
        measure_concurrent(
            "conc FILTER CONTAINS scan (predicate 90%)",
            threads,
            samples,
            filter_scan_pred,
        );

        measure("seq GRAPH-bound LIMIT 25 (graph list)", samples, graph_bound);
        measure_concurrent(
            "conc GRAPH-bound LIMIT 25 (graph list)",
            threads,
            samples,
            graph_bound,
        );
    }
}
