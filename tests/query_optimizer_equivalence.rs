mod support;

/// Result-equivalence harness for the craqle query-plan optimizer: every
/// shape in the perf matrix (plus OPTIONAL-unbound and typed-literal edge
/// cases) must produce identical result sets with the optimizer on and off,
/// with the lazy visibility predicate active.
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use craqle::*;

    use crate::support::*;

    const GRAPH_COUNT: usize = 400;
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    fn term_iri(iri: &str) -> EncodedTerm {
        EncodedTerm(format!("<{iri}>"))
    }

    fn term_str(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    fn graph_iri(idx: usize) -> String {
        format!("urn:eq:crate:{idx:04}")
    }

    fn seeded_corpus(node: &CraqleNode) -> Vec<GraphId> {
        let mut graphs = Vec::with_capacity(GRAPH_COUNT);
        for idx in 0..GRAPH_COUNT {
            let graph = GraphId::new(&graph_iri(idx));
            // The graph IRI doubles as crate root so the orphaned-entity
            // diagnostics keep all entities visible.
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
                insert(&root, RDF_TYPE, term_iri("http://schema.org/Dataset")),
                insert(
                    &root,
                    "http://schema.org/name",
                    term_str(&format!("Equivalence Dataset {idx:04}")),
                ),
            ];
            // Half the datasets have description + datePublished (OPTIONAL
            // bodies bind), half do not (OPTIONAL leaves vars unbound).
            if idx % 2 == 0 {
                changes.push(insert(
                    &root,
                    "http://schema.org/description",
                    term_str(&format!("Synthetic description {idx:04}")),
                ));
                changes.push(insert(
                    &root,
                    "http://schema.org/datePublished",
                    term_str("2026-01-01"),
                ));
            }
            // A third of the datasets have file parts (EXISTS / chain hits).
            if idx % 3 == 0 {
                let file = term_iri(&format!("{}/file-1", graph.as_str()));
                changes.push(insert(&file, RDF_TYPE, term_iri("http://schema.org/File")));
                changes.push(insert(
                    &file,
                    "http://schema.org/name",
                    term_str(&format!("file-{idx:04}.dat")),
                ));
                changes.push(insert(
                    &file,
                    "http://schema.org/contentSize",
                    term_str("1024"),
                ));
                changes.push(insert(&root, "http://schema.org/hasPart", file.clone()));
            }
            // Typed integer literal written with a non-canonical lexical form
            // on some graphs: value-equality folds must never fire on these.
            let version = if idx % 5 == 0 {
                "\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            } else {
                "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            };
            changes.push(insert(
                &root,
                "http://schema.org/version",
                EncodedTerm(version.to_string()),
            ));

            node.apply_changes_unchecked(&graph, changes).unwrap();
            graphs.push(graph);
        }
        node.ensure_query_indexes();
        graphs
    }

    /// Hides every 7th graph so the predicate path is exercised.
    fn visible(graph: &GraphId) -> bool {
        graph
            .as_str()
            .rsplit(':')
            .next()
            .and_then(|tail| tail.parse::<usize>().ok())
            .is_none_or(|idx| idx % 7 != 0)
    }

    fn canonical_rows(results: QueryResults) -> BTreeSet<String> {
        match results {
            QueryResults::Solutions(rows) => rows
                .into_iter()
                .map(|row| {
                    let mut entries: Vec<String> = row
                        .into_iter()
                        .map(|(var, term)| format!("{var}={}", term.0))
                        .collect();
                    entries.sort();
                    entries.join("|")
                })
                .collect(),
            QueryResults::Boolean(value) => BTreeSet::from([format!("bool={value}")]),
            QueryResults::Graph(triples) => triples
                .into_iter()
                .map(|(s, p, o)| format!("{} {} {}", s.0, p.0, o.0))
                .collect(),
        }
    }

    fn assert_equivalent(node: &CraqleNode, label: &str, sparql: &str) -> usize {
        let optimized = canonical_rows(
            node.query_graphs_with_planner(visible, sparql, true)
                .unwrap(),
        );
        let raw = canonical_rows(
            node.query_graphs_with_planner(visible, sparql, false)
                .unwrap(),
        );
        assert_eq!(
            optimized, raw,
            "{label}: optimizer changed the result set\nquery: {sparql}"
        );
        optimized.len()
    }

    fn matrix_shapes() -> Vec<(&'static str, String)> {
        let needle = "Equivalence Dataset 0123";
        vec![
            (
                "bgp_selective_last",
                format!(
                    "SELECT ?d WHERE {{ ?d a <http://schema.org/Dataset> . \
                     ?d <http://schema.org/name> \"{needle}\" }}"
                ),
            ),
            (
                "bgp_selective_first",
                format!(
                    "SELECT ?d WHERE {{ ?d <http://schema.org/name> \"{needle}\" . \
                     ?d a <http://schema.org/Dataset> }}"
                ),
            ),
            (
                "bgp_three_patterns",
                "SELECT ?d ?n ?v WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n ; <http://schema.org/version> ?v }"
                    .to_string(),
            ),
            (
                "optional_multi_pattern",
                "SELECT ?d ?n ?desc ?date WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n . OPTIONAL { ?d <http://schema.org/description> ?desc . \
                 ?d <http://schema.org/datePublished> ?date } }"
                    .to_string(),
            ),
            (
                "optional_unbound_then_filter",
                "SELECT ?d ?desc WHERE { ?d a <http://schema.org/Dataset> . \
                 OPTIONAL { ?d <http://schema.org/description> ?desc } \
                 FILTER(!BOUND(?desc)) }"
                    .to_string(),
            ),
            (
                "filter_eq_string",
                format!(
                    "SELECT ?d ?n WHERE {{ ?d <http://schema.org/name> ?n . \
                     FILTER(?n = \"{needle}\") }}"
                ),
            ),
            (
                "filter_eq_iri",
                "SELECT ?d ?f WHERE { ?d <http://schema.org/hasPart> ?f . \
                 FILTER(?f = <urn:eq:crate:0123/file-1>) }"
                    .to_string(),
            ),
            (
                "filter_eq_typed_numeric",
                "SELECT ?d WHERE { ?d <http://schema.org/version> ?v . FILTER(?v = 1) }"
                    .to_string(),
            ),
            (
                "filter_same_term_typed",
                "SELECT ?d WHERE { ?d <http://schema.org/version> ?v . \
                 FILTER(sameTerm(?v, \"1\"^^<http://www.w3.org/2001/XMLSchema#integer>)) }"
                    .to_string(),
            ),
            (
                "union_branches",
                "SELECT ?x ?n WHERE { { ?x a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } UNION { ?x a <http://schema.org/File> ; \
                 <http://schema.org/name> ?n } }"
                    .to_string(),
            ),
            (
                "nested_graph_pattern",
                "SELECT ?g ?d ?n WHERE { GRAPH ?g { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } }"
                    .to_string(),
            ),
            (
                "fixed_graph_pattern",
                "SELECT ?d ?n WHERE { GRAPH <urn:eq:crate:0123> { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } }"
                    .to_string(),
            ),
            (
                "filter_exists_two_patterns",
                "SELECT ?d ?n WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n . FILTER EXISTS { ?d <http://schema.org/hasPart> ?f . \
                 ?f a <http://schema.org/File> } }"
                    .to_string(),
            ),
            (
                "filter_not_exists_two_patterns",
                "SELECT ?d ?n WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n . FILTER NOT EXISTS { ?d <http://schema.org/hasPart> ?f . \
                 ?f a <http://schema.org/File> } }"
                    .to_string(),
            ),
            (
                "join_chain_dataset_file_name",
                "SELECT ?d ?fn WHERE { ?d a <http://schema.org/Dataset> . \
                 ?d <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> . \
                 ?f <http://schema.org/name> ?fn }"
                    .to_string(),
            ),
            (
                "join_chain_anchored_worst_order",
                "SELECT ?fn WHERE { ?f <http://schema.org/name> ?fn . \
                     ?f a <http://schema.org/File> . ?d <http://schema.org/hasPart> ?f . \
                     ?d a <http://schema.org/Dataset> . \
                     ?d <http://schema.org/name> \"Equivalence Dataset 0123\" }".to_string(),
            ),
            (
                "distinct_limit",
                "SELECT DISTINCT ?n WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } ORDER BY ?n LIMIT 25"
                    .to_string(),
            ),
            (
                "order_by_limit",
                "SELECT ?d ?n WHERE { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } ORDER BY ?n LIMIT 10"
                    .to_string(),
            ),
            (
                "union_under_limit",
                "SELECT ?x ?n WHERE { { ?x a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> ?n } UNION { ?x a <http://schema.org/File> ; \
                 <http://schema.org/name> ?n } } ORDER BY ?n LIMIT 30"
                    .to_string(),
            ),
            (
                "minus",
                "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> . \
                 MINUS { ?d <http://schema.org/description> ?desc . \
                 ?d <http://schema.org/datePublished> ?date } }"
                    .to_string(),
            ),
            (
                "blank_node_join",
                "SELECT ?n WHERE { _:b a <http://schema.org/Dataset> . \
                 _:b <http://schema.org/name> ?n . _:b <http://schema.org/hasPart> ?f }"
                    .to_string(),
            ),
            (
                "subquery_group",
                "SELECT ?d ?parts WHERE { { SELECT ?d (COUNT(?f) AS ?parts) WHERE { \
                 ?d <http://schema.org/hasPart> ?f } GROUP BY ?d } \
                 ?d <http://schema.org/name> ?n . FILTER(CONTAINS(?n, \"012\")) }"
                    .to_string(),
            ),
            (
                "ask_shape",
                "ASK { ?d a <http://schema.org/Dataset> ; \
                 <http://schema.org/name> \"Equivalence Dataset 0123\" }"
                    .to_string(),
            ),
            (
                "missing_term_short_circuit",
                "SELECT ?d WHERE { ?d a <http://schema.org/NoSuchType> . \
                 ?d <http://schema.org/name> ?n }"
                    .to_string(),
            ),
        ]
    }

    #[test]
    fn optimizer_preserves_result_sets_across_matrix_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        seeded_corpus(&node);

        let mut nonempty = 0usize;
        for (label, sparql) in matrix_shapes() {
            let rows = assert_equivalent(&node, label, &sparql);
            if rows > 0 {
                nonempty += 1;
            }
        }
        assert!(nonempty >= 18, "most shapes must return rows: {nonempty}");
    }

    #[test]
    fn optimizer_respects_visibility_predicate() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        seeded_corpus(&node);

        // Graph 0007 is hidden by the predicate (7 % 7 == 0 → wait, 7 % 7 == 0
        // hides idx 0, 7, 14, ...); ensure those rows never appear.
        let sparql = "SELECT ?d ?n WHERE { ?d a <http://schema.org/Dataset> ; \
                      <http://schema.org/name> ?n }";
        for optimize in [true, false] {
            let rows = solution_rows(
                node.query_graphs_with_planner(visible, sparql, optimize)
                    .unwrap(),
            );
            assert!(!rows.is_empty());
            for row in &rows {
                let name = &row.get("n").unwrap().0;
                let idx: usize = name
                    .trim_start_matches("\"Equivalence Dataset ")
                    .trim_end_matches('"')
                    .parse()
                    .unwrap();
                assert!(!idx.is_multiple_of(7), "hidden graph leaked: {name}");
            }
        }

        // Hidden quads must not influence EXISTS either.
        let exists = "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> . \
                      FILTER EXISTS { ?d <http://schema.org/hasPart> ?f } }";
        assert_equivalent(&node, "exists_under_visibility", exists);
    }

    #[test]
    fn typed_literal_folds_never_fire_on_non_canonical_spellings() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        seeded_corpus(&node);

        // `?v = 1` is value equality: it must match both "1"^^xsd:integer and
        // the non-canonical "01"^^xsd:integer spellings.
        let sparql = "SELECT ?d WHERE { ?d <http://schema.org/version> ?v . FILTER(?v = 1) }";
        let optimized = solution_rows(
            node.query_graphs_with_planner(visible, sparql, true)
                .unwrap(),
        );
        let raw = solution_rows(
            node.query_graphs_with_planner(visible, sparql, false)
                .unwrap(),
        );
        assert_eq!(optimized.len(), raw.len());
        // Every visible dataset has a version quad; idx % 5 == 0 graphs use
        // the "01" spelling and must still match.
        let visible_count = (0..GRAPH_COUNT).filter(|idx| idx % 7 != 0).count();
        assert_eq!(optimized.len(), visible_count);
    }
}
