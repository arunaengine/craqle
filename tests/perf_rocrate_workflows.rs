mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;

    use crate::support::*;

    const DEFAULT_ENTITY_COUNT: usize = 25_000;
    const DEFAULT_BATCH_SIZE: usize = 5_000;

    #[test]
    #[ignore = "release-only large RO-Crate workflow comparison"]
    fn compare_large_rocrate_import_and_batched_append() {
        let entity_count = env_usize("CRAQLE_ROCRATE_WORKFLOW_ENTITY_COUNT", DEFAULT_ENTITY_COUNT);
        let batch_size = env_usize("CRAQLE_ROCRATE_WORKFLOW_BATCH_SIZE", DEFAULT_BATCH_SIZE);
        let include_validated_baseline =
            env_bool("CRAQLE_ROCRATE_WORKFLOW_INCLUDE_VALIDATED_BASELINE", true);
        assert!(entity_count > 0, "entity_count must be > 0");
        assert!(batch_size > 0, "batch_size must be > 0");

        run_import_workflow(entity_count, include_validated_baseline);
        run_replace_workflow(entity_count);
        run_append_like_replace_workflow(entity_count);
        run_incremental_update_workflow(entity_count);
        run_batched_append_workflow(entity_count, batch_size);
    }

    fn run_import_workflow(entity_count: usize, include_validated_baseline: bool) {
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:perf:workflow:import");
        let probe = usize::min(entity_count.saturating_sub(1), 123);

        let build_start = Instant::now();
        let jsonld = benchmark_rocrate_document(
            &graph,
            entity_count,
            "workflow-import-keyword",
            "Workflow Import Dataset",
        );
        let build_elapsed = build_start.elapsed();

        let planner_dir = tempfile::tempdir().unwrap();
        let planner = CraqleNode::open(planner_dir.path()).unwrap();
        let plan_start = Instant::now();
        let planned = planner
            .preview_rocrate_update(&writer, &graph, &jsonld)
            .unwrap();
        let plan_elapsed = plan_start.elapsed();

        let validated_apply_elapsed = if include_validated_baseline {
            let validated_dir = tempfile::tempdir().unwrap();
            let validated = CraqleNode::open(validated_dir.path()).unwrap();
            let validated_apply_start = Instant::now();
            validated.apply_changes(&graph, planned.clone()).unwrap();
            Some(validated_apply_start.elapsed())
        } else {
            None
        };

        let dir = tempfile::tempdir().unwrap();
        let strict_node = CraqleNode::open(dir.path()).unwrap();
        let strict_import_start = Instant::now();
        strict_node
            .apply_rocrate_document_checked_with_policy(
                &writer,
                graph.clone(),
                &jsonld,
                public_policy(),
            )
            .unwrap();
        let strict_import_elapsed = strict_import_start.elapsed();

        let trusted_dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(trusted_dir.path()).unwrap();
        let import_start = Instant::now();
        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();
        let import_elapsed = import_start.elapsed();

        let count_query = format!(
            "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
            graph.as_str()
        );
        let count_rows = solution_rows(node.query(&reader, &count_query).unwrap());
        assert_eq!(
            binding_i64(count_rows[0].get("count").unwrap()),
            entity_count as i64
        );

        let search_start = Instant::now();
        let hits = node
            .search(&reader, &format!("DOC-{probe:06}"), 10)
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri == format!("./bulk/entity-{probe:06}.dat"))
        );

        println!(
            "all-at-once import: {} entities, document build {:?}, plan {:?}, validated apply {}, strict bootstrap {:?}, trusted bootstrap {:?}, first search {:?}",
            entity_count,
            build_elapsed,
            plan_elapsed,
            validated_apply_elapsed
                .map(|elapsed| format!("{elapsed:?}"))
                .unwrap_or_else(|| "skipped".to_string()),
            strict_import_elapsed,
            import_elapsed,
            search_elapsed,
        );
    }

    fn run_batched_append_workflow(entity_count: usize, batch_size: usize) {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:perf:workflow:append");
        let probe = usize::min(entity_count.saturating_sub(1), 123);

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Workflow Append Dataset",
                "Workflow append comparison",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                public_policy(),
            ),
        )
        .unwrap();

        let mut build_latencies = Vec::new();
        let mut apply_latencies = Vec::new();
        for start in (0..entity_count).step_by(batch_size) {
            let batch_count = usize::min(batch_size, entity_count - start);

            let build_start = Instant::now();
            let entities = benchmark_media_object_entities(
                start,
                batch_count,
                "workflow-append-keyword",
                "Workflow Append Entity",
                "workflow append record",
                "APPEND",
            );
            build_latencies.push(build_start.elapsed());

            let apply_start = Instant::now();
            node.append_new_root_data_entities(&writer, &graph, entities)
                .unwrap();
            apply_latencies.push(apply_start.elapsed());
        }

        let diagnostics_start = Instant::now();
        let diagnostics = node.graph_diagnostics(&graph).unwrap();
        let diagnostics_elapsed = diagnostics_start.elapsed();
        assert!(!diagnostics.has_orphans());

        let count_query = format!(
            "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
            graph.as_str()
        );
        let count_rows = solution_rows(node.query(&reader, &count_query).unwrap());
        assert_eq!(
            binding_i64(count_rows[0].get("count").unwrap()),
            entity_count as i64
        );

        let search_start = Instant::now();
        let hits = node
            .search(&reader, &format!("APPEND-{probe:06}"), 10)
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri == format!("./bulk/entity-{probe:06}.dat"))
        );

        println!(
            "base+append: {} entities, batch {}, build {}, apply {}, diagnostics read {:?}, first search {:?}",
            entity_count,
            batch_size,
            format_stats("build", &build_latencies),
            format_stats("apply", &apply_latencies),
            diagnostics_elapsed,
            search_elapsed,
        );
    }

    fn run_replace_workflow(entity_count: usize) {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:perf:workflow:replace");
        let jsonld = benchmark_rocrate_document(
            &graph,
            entity_count,
            "workflow-replace-keyword",
            "Workflow Replace Dataset",
        );

        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let replace_build_start = Instant::now();
        let updated_jsonld = updated_rocrate_document(&jsonld, graph.as_str(), entity_count);
        let replace_build_elapsed = replace_build_start.elapsed();

        let replace_start = Instant::now();
        node.apply_rocrate_document(&writer, graph.clone(), &updated_jsonld)
            .unwrap();
        let replace_elapsed = replace_start.elapsed();

        let search_start = Instant::now();
        let hits = node
            .search(&reader, "updated replacement marker", 10)
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(!hits.is_empty());

        println!(
            "full replace update: {} entities, updated document build {:?}, replace {:?}, first search {:?}",
            entity_count, replace_build_elapsed, replace_elapsed, search_elapsed,
        );
    }

    fn run_incremental_update_workflow(entity_count: usize) {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:perf:workflow:incremental-update");
        let probe = usize::min(entity_count.saturating_sub(1), 123);
        let jsonld = benchmark_rocrate_document(
            &graph,
            entity_count,
            "workflow-incremental-keyword",
            "Workflow Incremental Dataset",
        );

        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let update_start = Instant::now();
        node.update_property(
            &writer,
            &graph,
            &format!("./bulk/entity-{probe:06}.dat"),
            "schema:name",
            Some(&format!("Imported Entity {probe}")),
            "Incremental Update Marker",
        )
        .unwrap();
        let update_elapsed = update_start.elapsed();

        let search_start = Instant::now();
        let hits = node
            .search(&reader, "Incremental Update Marker", 10)
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(!hits.is_empty());

        println!(
            "incremental update: {} entities, single property update {:?}, first search {:?}",
            entity_count, update_elapsed, search_elapsed,
        );
    }

    fn run_append_like_replace_workflow(entity_count: usize) {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:perf:workflow:append-like-replace");
        let extra_count = usize::min(10_000, usize::max(1_000, entity_count / 10));
        let jsonld = benchmark_rocrate_document(
            &graph,
            entity_count,
            "workflow-append-like-keyword",
            "Workflow Append-Like Dataset",
        );

        node.bootstrap_rocrate_document(&writer, graph.clone(), &jsonld, public_policy())
            .unwrap();

        let replace_build_start = Instant::now();
        let updated_jsonld =
            append_entities_to_rocrate_document(&jsonld, graph.as_str(), entity_count, extra_count);
        let replace_build_elapsed = replace_build_start.elapsed();

        let replace_start = Instant::now();
        node.apply_rocrate_document(&writer, graph.clone(), &updated_jsonld)
            .unwrap();
        let replace_elapsed = replace_start.elapsed();

        let probe = entity_count + extra_count - 1;
        let search_start = Instant::now();
        let hits = node
            .search(&reader, &format!("DOC-{probe:06}"), 10)
            .unwrap();
        let search_elapsed = search_start.elapsed();
        assert!(!hits.is_empty());

        println!(
            "append-like replace: base {} entities, +{} entities, document build {:?}, replace {:?}, first search {:?}",
            entity_count, extra_count, replace_build_elapsed, replace_elapsed, search_elapsed,
        );
    }

    fn updated_rocrate_document(jsonld: &str, root_id: &str, entity_count: usize) -> String {
        let probe = usize::min(entity_count.saturating_sub(1), 123);
        let mut value: serde_json::Value = serde_json::from_str(jsonld).unwrap();
        let graph = value["@graph"].as_array_mut().unwrap();
        for entry in graph {
            if entry["@id"] == root_id {
                entry["description"] =
                    serde_json::Value::String("updated replacement marker".to_string());
            }
            if entry["@id"] == format!("./bulk/entity-{probe:06}.dat") {
                entry["name"] = serde_json::Value::String("Updated Replacement Entity".to_string());
            }
        }
        value.to_string()
    }

    fn append_entities_to_rocrate_document(
        jsonld: &str,
        root_id: &str,
        start: usize,
        count: usize,
    ) -> String {
        let mut value: serde_json::Value = serde_json::from_str(jsonld).unwrap();
        let graph = value["@graph"].as_array_mut().unwrap();
        let root_index = graph
            .iter()
            .position(|entry| entry["@id"] == root_id)
            .unwrap();

        for idx in start..(start + count) {
            graph[root_index]["hasPart"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "@id": format!("./bulk/entity-{idx:06}.dat")
                }));
            graph.push(serde_json::json!({
                "@id": format!("./bulk/entity-{idx:06}.dat"),
                "@type": "MediaObject",
                "name": format!("Imported Entity {idx}"),
                "description": format!("workflow-append-like-keyword imported record {idx}"),
                "keywords": "workflow-append-like-keyword",
                "identifier": format!("DOC-{idx:06}"),
            }));
        }

        value.to_string()
    }
}
