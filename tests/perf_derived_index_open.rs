mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 50_000;
    const DEFAULT_FILES_PER_GRAPH: usize = 3;

    fn term_iri(iri: &str) -> EncodedTerm {
        EncodedTerm(format!("<{iri}>"))
    }

    fn term_str(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    fn crate_changes(
        graph: &GraphId,
        graph_idx: usize,
        files_per_graph: usize,
    ) -> Vec<MaterializedQuadChange> {
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let root = term_iri(graph.as_str());

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
            insert(
                &root,
                "http://schema.org/name",
                term_str(&format!("Bench Dataset {graph_idx:05}")),
            ),
            insert(
                &root,
                "http://schema.org/description",
                term_str(&format!("Synthetic RO-Crate {graph_idx:05}")),
            ),
            insert(
                &root,
                "http://schema.org/datePublished",
                term_str("2026-01-01"),
            ),
            insert(
                &root,
                "http://schema.org/license",
                term_iri("https://creativecommons.org/licenses/by/4.0/"),
            ),
        ];

        for file_idx in 0..files_per_graph {
            let file = term_iri(&format!("{}/data/file-{file_idx}.dat", graph.as_str()));
            changes.push(insert(
                &file,
                rdf_type,
                term_iri("http://schema.org/MediaObject"),
            ));
            changes.push(insert(
                &file,
                "http://schema.org/name",
                term_str(&format!("file-{graph_idx:05}-{file_idx}.dat")),
            ));
            changes.push(insert(
                &file,
                "http://schema.org/contentSize",
                term_str("1024"),
            ));
            changes.push(insert(
                &file,
                "http://schema.org/encodingFormat",
                term_str("text/plain"),
            ));
            changes.push(insert(&root, "http://schema.org/hasPart", file.clone()));
        }

        changes
    }

    #[test]
    #[ignore = "release-only derived-index open/readiness profile"]
    fn derived_index_readiness_after_reopen() {
        let graph_count = env_usize("CRAQLE_MULTI_GRAPH_COUNT", DEFAULT_GRAPH_COUNT);
        let files_per_graph = env_usize("CRAQLE_MULTI_FILES_PER_GRAPH", DEFAULT_FILES_PER_GRAPH);
        let quads_per_graph = 5 + files_per_graph * 5;

        let tmp = tempfile::tempdir().unwrap();
        let mut graphs = Vec::with_capacity(graph_count);

        {
            let node = CraqleNode::open_with_options(
                tmp.path(),
                CraqleOptions::new().with_search_storage(SearchStorage::Memory),
            )
            .unwrap();
            let load_start = Instant::now();
            for graph_idx in 0..graph_count {
                let graph = GraphId::new(&format!("urn:bench:crate:{graph_idx:05}"));
                node.apply_changes_unchecked(
                    &graph,
                    crate_changes(&graph, graph_idx, files_per_graph),
                )
                .unwrap();
                graphs.push(graph);
            }
            println!(
                "corpus: {graph_count} graphs x {quads_per_graph} quads ({} total) loaded in {:?}",
                graph_count * quads_per_graph,
                load_start.elapsed(),
            );
        }

        let open_start = Instant::now();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let open_elapsed = open_start.elapsed();

        let ready_start = Instant::now();
        node.ensure_query_indexes();
        let ready_elapsed = ready_start.elapsed();

        let first_query_start = Instant::now();
        let rows = solution_rows(
            node.query_in_graphs(
                &AllowAllAuthorizer,
                &graphs,
                "SELECT ?s ?name WHERE { ?s schema:name ?name } LIMIT 25",
            )
            .unwrap(),
        );
        let first_query_elapsed = first_query_start.elapsed();
        assert_eq!(rows.len(), 25);

        let second_query_start = Instant::now();
        let rows = solution_rows(
            node.query_in_graphs(
                &AllowAllAuthorizer,
                &graphs,
                "SELECT ?s ?name WHERE { ?s schema:name ?name } LIMIT 25",
            )
            .unwrap(),
        );
        let second_query_elapsed = second_query_start.elapsed();
        assert_eq!(rows.len(), 25);

        println!(
            "reopen: open {open_elapsed:?}, index readiness {ready_elapsed:?}, first cross-graph query {first_query_elapsed:?}, second {second_query_elapsed:?}"
        );
    }
}
