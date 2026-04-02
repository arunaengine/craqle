mod support;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use craqle::*;
    use oxrdf::{NamedNode, Term};
    use std::collections::BTreeSet;

    use crate::support::*;

    const DEFAULT_GRAPH_COUNT: usize = 128;
    const DEFAULT_ENTITIES_PER_GRAPH: usize = 512;
    const DEFAULT_CONTEXTUALS_PER_GRAPH: usize = 4;
    const DEFAULT_BATCH_SIZE: usize = 256;
    const DEFAULT_QUERY_SAMPLES: usize = 12;

    #[derive(Debug, Clone, Copy)]
    struct CrossGraphConfig {
        graph_count: usize,
        entities_per_graph: usize,
        contextuals_per_graph: usize,
        batch_size: usize,
        query_samples: usize,
    }

    impl CrossGraphConfig {
        fn from_env() -> Self {
            Self {
                graph_count: env_usize("CRAQLE_CROSS_GRAPH_COUNT", DEFAULT_GRAPH_COUNT),
                entities_per_graph: env_usize(
                    "CRAQLE_CROSS_GRAPH_ENTITIES_PER_GRAPH",
                    DEFAULT_ENTITIES_PER_GRAPH,
                ),
                contextuals_per_graph: env_usize(
                    "CRAQLE_CROSS_GRAPH_CONTEXTUALS_PER_GRAPH",
                    DEFAULT_CONTEXTUALS_PER_GRAPH,
                ),
                batch_size: env_usize("CRAQLE_CROSS_GRAPH_BATCH_SIZE", DEFAULT_BATCH_SIZE),
                query_samples: env_usize("CRAQLE_CROSS_GRAPH_QUERY_SAMPLES", DEFAULT_QUERY_SAMPLES),
            }
        }

        fn total_entities(self) -> usize {
            self.graph_count * self.entities_per_graph
        }
    }

    #[test]
    #[ignore = "release-only cross-graph graph-unbound sparql profile"]
    fn graph_unbound_sparql_patterns_across_many_graphs() {
        let config = CrossGraphConfig::from_env();
        assert!(config.graph_count > 0, "graph_count must be > 0");
        assert!(
            config.entities_per_graph > 0,
            "entities_per_graph must be > 0"
        );
        assert!(config.batch_size > 0, "batch_size must be > 0");
        let (_tmp, net) = setup_network(1);
        let node = net.peer(0);
        let reader = GrantAuthorizer::default();
        let shared_keyword = "cross-graph-common";
        let shared_subject = "urn:perf:cross-graph:shared-subject";

        let load_start = Instant::now();
        for graph_idx in 0..config.graph_count {
            let graph = GraphId::new(&format!("urn:perf:cross-graph:{graph_idx:04}"));
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    format!("Cross Graph Dataset {graph_idx}"),
                    "Graph-unbound SPARQL performance profile",
                    "2026-03-30",
                    "https://creativecommons.org/licenses/by/4.0/",
                    public_policy(),
                ),
            )
            .unwrap();

            let graph_index_predicate = NamedNode::new_unchecked("http://schema.org/position");
            node.add_contextual_entity_with_triples(
                &writer_auth(),
                &graph,
                shared_subject,
                "http://schema.org/Thing",
                &format!("Cross Graph Shared Subject {graph_idx}"),
                vec![(
                    graph_index_predicate.clone(),
                    Term::Literal(oxrdf::Literal::new_simple_literal(graph_idx.to_string())),
                )],
            )
            .unwrap();

            attach_contextual_entities(
                node,
                &writer_auth(),
                &graph,
                &format!("{graph_idx:04}"),
                config.contextuals_per_graph,
                "CrossGraph",
            );

            for start in (0..config.entities_per_graph).step_by(config.batch_size) {
                let batch_count = usize::min(config.batch_size, config.entities_per_graph - start);
                append_benchmark_media_objects(
                    node,
                    &writer_auth(),
                    &graph,
                    start,
                    batch_count,
                    shared_keyword,
                );
            }
        }
        let load_elapsed = load_start.elapsed();

        let mut subject_led_latencies = Vec::with_capacity(config.query_samples);
        let mut predicate_object_count_latencies = Vec::with_capacity(config.query_samples);
        let mut predicate_object_point_latencies = Vec::with_capacity(config.query_samples);

        let subject_led_query = r#"
        SELECT ?g ?name ?pos
        WHERE {
          GRAPH ?g {
            <urn:perf:cross-graph:shared-subject> schema:name ?name .
            <urn:perf:cross-graph:shared-subject> <http://schema.org/position> ?pos .
          }
        }
        ORDER BY ?g
        "#;

        let predicate_object_count_query = format!(
            r#"
        SELECT (COUNT(?s) AS ?count)
        WHERE {{
          GRAPH ?g {{
            ?s rdf:type schema:MediaObject .
            ?s schema:keywords "{shared_keyword}" .
          }}
        }}
        "#
        );

        let _ = solution_rows(node.query(&reader, subject_led_query).unwrap());
        let _ = solution_rows(node.query(&reader, &predicate_object_count_query).unwrap());
        let warm_entity_idx = sample_entity_index(0, config.entities_per_graph);
        let warm_point_query = format!(
            r#"
            SELECT ?g ?s ?name
            WHERE {{
              GRAPH ?g {{
                ?s schema:identifier "BENCH-{warm_entity_idx:06}" .
                ?s schema:name ?name .
              }}
            }}
            ORDER BY ?g
            "#
        );
        let _ = solution_rows(node.query(&reader, &warm_point_query).unwrap());

        for sample in 0..config.query_samples {
            let subject_led_start = Instant::now();
            let subject_led_rows = solution_rows(node.query(&reader, subject_led_query).unwrap());
            subject_led_latencies.push(subject_led_start.elapsed());
            assert_eq!(subject_led_rows.len(), config.graph_count);
            let mut seen_graphs = BTreeSet::new();
            let mut seen_positions = BTreeSet::new();
            for row in &subject_led_rows {
                let graph_id = row
                    .get("g")
                    .and_then(|term| term.to_named_node())
                    .map(|node| node.as_str().to_string())
                    .unwrap();
                seen_graphs.insert(graph_id);
                seen_positions.insert(binding_literal(row.get("pos").unwrap()));
            }
            assert_eq!(seen_graphs.len(), config.graph_count);
            assert_eq!(seen_positions.len(), config.graph_count);

            let count_start = Instant::now();
            let count_rows =
                solution_rows(node.query(&reader, &predicate_object_count_query).unwrap());
            predicate_object_count_latencies.push(count_start.elapsed());
            assert_eq!(
                binding_i64(count_rows[0].get("count").unwrap()),
                config.total_entities() as i64
            );

            let entity_idx = sample_entity_index(sample, config.entities_per_graph);
            let point_query = format!(
                r#"
            SELECT ?g ?s ?name
            WHERE {{
              GRAPH ?g {{
                ?s schema:identifier "BENCH-{entity_idx:06}" .
                ?s schema:name ?name .
              }}
            }}
            ORDER BY ?g
            "#
            );
            let point_start = Instant::now();
            let point_rows = solution_rows(node.query(&reader, &point_query).unwrap());
            predicate_object_point_latencies.push(point_start.elapsed());
            assert_eq!(point_rows.len(), config.graph_count);
            let mut point_graphs = BTreeSet::new();
            for row in &point_rows {
                point_graphs.insert(
                    row.get("g")
                        .and_then(|term| term.to_named_node())
                        .map(|node| node.as_str().to_string())
                        .unwrap(),
                );
            }
            assert_eq!(point_graphs.len(), config.graph_count);
        }

        println!(
            "cross-graph corpus: {} graphs x {} entities ({} total), {} contextual entities per graph, batch {}, loaded in {:?}",
            config.graph_count,
            config.entities_per_graph,
            config.total_entities(),
            config.contextuals_per_graph,
            config.batch_size,
            load_elapsed,
        );
        println!(
            "{}",
            format_stats(
                "graph-unbound subject-led root lookup",
                &subject_led_latencies
            )
        );
        println!(
            "{}",
            format_stats(
                "graph-unbound predicate/object mediaobject count",
                &predicate_object_count_latencies,
            )
        );
        println!(
            "{}",
            format_stats(
                "graph-unbound predicate/object identifier lookup",
                &predicate_object_point_latencies,
            )
        );
    }

    fn sample_entity_index(sample: usize, total: usize) -> usize {
        ((sample as u64 * 1_103_515_245 + 12_345) % total as u64) as usize
    }
}
