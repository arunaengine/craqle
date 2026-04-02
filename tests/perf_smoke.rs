mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    #[test]
    #[ignore = "performance smoke check"]
    fn performance_store_and_sync_smoke() {
        let (_tmp, net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-perf");
        create_test_crate(&net, 0, &graph);

        let start = Instant::now();
        for idx in 0..1_000 {
            keyword_insert(net.peer(0), &graph, &format!("perf-{idx}"));
        }
        let insert_elapsed = start.elapsed();

        let sync_start = Instant::now();
        net.sync_until_converged(50).unwrap();
        let sync_elapsed = sync_start.elapsed();

        let search_start = Instant::now();
        let hits = reindex_and_search(&net, 0, "perf");
        let search_elapsed = search_start.elapsed();

        println!(
            "perf: 1000 inserts in {:?}, convergence in {:?}, reindex+search in {:?}",
            insert_elapsed, sync_elapsed, search_elapsed
        );
        assert!(!hits.is_empty());
    }

    #[test]
    #[ignore = "heavy real-world graph smoke test"]
    fn heavy_real_world_graph_with_integrated_fts() {
        let entity_count = std::env::var("CRAQLE_HEAVY_ENTITY_COUNT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100_000);
        let chunk_size = std::env::var("CRAQLE_HEAVY_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2_000);

        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-heavy");
        let mgr = manager(net.peer(0));
        mgr.create_crate(
            graph.clone(),
            "Heavy Proteomics Graph",
            "Large graph benchmark",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();

        let load_start = Instant::now();
        for start in (0..entity_count).step_by(chunk_size) {
            let batch_count = usize::min(chunk_size, entity_count - start);
            append_benchmark_media_objects(
                net.peer(0),
                &writer_auth(),
                &graph,
                start,
                batch_count,
                "proteomics",
            );
        }
        let load_elapsed = load_start.elapsed();

        let sync_start = Instant::now();
        net.sync_until_converged(200).unwrap();
        let sync_elapsed = sync_start.elapsed();

        let mgr_peer1 = manager(net.peer(1));
        let summary_start = Instant::now();
        let summary = mgr_peer1.export_jsonld_summary(&graph).unwrap();
        let summary_elapsed = summary_start.elapsed();

        let page_start = Instant::now();
        let page = mgr_peer1.export_jsonld_page(&graph, 0, 1000).unwrap();
        let page_elapsed = page_start.elapsed();

        let cursor_page_start = Instant::now();
        let cursor_page = mgr_peer1
            .export_jsonld_page_after(&graph, None, 1000)
            .unwrap();
        let cursor_page_elapsed = cursor_page_start.elapsed();

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
            entity_count as i64
        );

        let fts_reindex_start = Instant::now();
        net.reindex_search().unwrap();
        let fts_reindex_elapsed = fts_reindex_start.elapsed();

        let fts_query = format!(
            r#"
        SELECT ?s ?score
        WHERE {{
          SERVICE <urn:craqle:fts> {{
            ?s fts:query "proteomics" .
            ?s fts:score ?score .
            ?s fts:graph <{}> .
            ?s fts:limit 25 .
          }}
        }}
        ORDER BY DESC(?score)
        "#,
            graph.as_str()
        );

        let fts_start = Instant::now();
        let rows = solution_rows(
            net.peer(1)
                .query(&GrantAuthorizer::default(), &fts_query)
                .unwrap(),
        );
        let fts_elapsed = fts_start.elapsed();

        println!(
            "heavy graph: {} entities (~{} triples) loaded in {:?}, synced in {:?}, summary export in {:?}, offset page export in {:?}, cursor page export in {:?}, fts reindex in {:?}, fts query in {:?}",
            entity_count,
            entity_count * 6 + 8,
            load_elapsed,
            sync_elapsed,
            summary_elapsed,
            page_elapsed,
            cursor_page_elapsed,
            fts_reindex_elapsed,
            fts_elapsed
        );
        assert!(summary.contains("Heavy Proteomics Graph"));
        assert_eq!(page.total_data_entities, entity_count);
        assert_eq!(page.returned_data_entities, usize::min(1000, entity_count));
        assert_eq!(cursor_page.total_data_entities, entity_count);
        assert_eq!(
            cursor_page.returned_data_entities,
            usize::min(1000, entity_count)
        );
        assert!(!rows.is_empty());
    }
}
