//! Checks optimized queries against unoptimized result sets.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "../support.rs"]
mod support;

/// Compare optimizer results with lazy visibility, including unbound and typed terms.
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use craqle::*;

    use crate::support::*;

    const GRAPH_COUNT: usize = 400;
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    /// Above the explicit-dataset threshold, this uses union-view filtering.
    const UNION_VISIBLE_GRAPHS: usize = 40;

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

    fn canonical_rows(results: QueryResults) -> Vec<String> {
        let mut rows: Vec<String> = match results {
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
            QueryResults::Boolean(value) => vec![format!("bool={value}")],
            QueryResults::Graph(triples) => triples
                .into_iter()
                .map(|(s, p, o)| format!("{} {} {}", s.0, p.0, o.0))
                .collect(),
        };
        rows.sort();
        rows
    }

    fn planner_rows(node: &CraqleNode, sparql: &str, optimize: bool) -> QueryResults {
        let auth = |graph: &GraphId, _: &GraphPolicy, action: Action| {
            if visible(graph) {
                Ok(())
            } else {
                Err(AuthorizationError::PermissionDenied {
                    action,
                    graph: graph.as_str().to_owned(),
                })
            }
        };
        let prepared = node.prepare_query(sparql).unwrap();
        let mut options = QueryOptions::default();
        options.optimize = optimize;
        if !optimize {
            options.fast_paths = QueryFastPathMode::Disabled;
        }
        options.limits = QueryLimits::unbounded();
        node.execute_prepared(&auth, &prepared, &options)
            .unwrap()
            .results
    }

    fn all_rows(node: &CraqleNode, sparql: &str, optimize: bool) -> QueryResults {
        let prepared = node.prepare_query(sparql).unwrap();
        let mut options = QueryOptions::default();
        options.optimize = optimize;
        if !optimize {
            options.fast_paths = QueryFastPathMode::Disabled;
        }
        options.limits = QueryLimits::unbounded();
        node.execute_prepared(&AllowAllAuthorizer, &prepared, &options)
            .unwrap()
            .results
    }

    fn visible_rows(node: &CraqleNode, sparql: &str, allowed: &BTreeSet<String>) -> QueryResults {
        let auth = |graph: &GraphId, _: &GraphPolicy, action: Action| {
            if allowed.contains(graph.as_str()) {
                Ok(())
            } else {
                Err(AuthorizationError::PermissionDenied {
                    action,
                    graph: graph.as_str().to_owned(),
                })
            }
        };
        let prepared = node.prepare_query(sparql).unwrap();
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        node.execute_prepared(&auth, &prepared, &options)
            .unwrap()
            .results
    }

    fn listed_rows(node: &CraqleNode, graphs: &[GraphId], sparql: &str) -> QueryResults {
        let prepared = node.prepare_query(sparql).unwrap();
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        node.execute_prepared_in_graphs(&AllowAllAuthorizer, graphs, &prepared, &options)
            .unwrap()
            .results
    }

    fn assert_equivalent(node: &CraqleNode, label: &str, sparql: &str) -> usize {
        let optimized = canonical_rows(planner_rows(node, sparql, true));
        let raw = canonical_rows(planner_rows(node, sparql, false));
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
                "graph_only_join",
                "SELECT ?g ?d ?f WHERE { GRAPH ?g { ?d <http://schema.org/version> ?v . \
                 ?f a <http://schema.org/File> } }"
                    .to_string(),
            ),
            (
                "graph_subject_chain",
                "SELECT ?g ?n WHERE { GRAPH ?g { ?g <http://schema.org/name> ?n ; \
                 <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> } }"
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
    fn optimizer_preserves_results() {
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

    /// Patterns that share only the graph variable must still join within one graph.
    #[test]
    fn graph_joins_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(tmp.path()).unwrap();
        seeded_corpus(&node);
        for (label, sparql) in matrix_shapes()
            .into_iter()
            .filter(|(label, _)| label.starts_with("graph_"))
        {
            let raw = canonical_rows(all_rows(&node, &sparql, false));
            assert!(!raw.is_empty(), "{label}: shape must return rows");
            for mode in [JoinMode::ForceHash, JoinMode::ForceLateral] {
                let prepared = node.prepare_query(&sparql).unwrap();
                let mut options = QueryOptions::default();
                options.join_mode = mode;
                options.limits = QueryLimits::unbounded();
                let run = node
                    .execute_prepared(&AllowAllAuthorizer, &prepared, &options)
                    .unwrap();
                assert!(
                    !run.statistics.planned_joins.is_empty(),
                    "{label}: {mode:?}"
                );
                assert_eq!(canonical_rows(run.results), raw, "{label}: {mode:?}");
            }
        }
    }

    #[test]
    fn slice_demand_plans() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(tmp.path()).unwrap();
        let graph = GraphId::new("urn:test:slice-demand");
        let mut changes = Vec::new();
        let mut left = vec![Vec::new(); 129];
        let mut right = vec![Vec::new(); 129];
        for index in 0..256 {
            let key = if index < 128 {
                0
            } else {
                1 + (index - 128) % 128
            };
            let subject = format!("urn:test:slice-demand:left:{index:03}");
            left[key].push(subject.clone());
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: term_iri(&subject),
                predicate: term_iri("urn:test:slice-demand:left"),
                object: term_iri(&format!("urn:test:slice-demand:key:{key:03}")),
            });
        }
        for index in 0..512 {
            let key = if index < 256 {
                0
            } else {
                1 + (index - 256) % 128
            };
            let subject = format!("urn:test:slice-demand:right:{index:03}");
            right[key].push(subject.clone());
            changes.push(MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: term_iri(&subject),
                predicate: term_iri("urn:test:slice-demand:right"),
                object: term_iri(&format!("urn:test:slice-demand:key:{key:03}")),
            });
        }
        node.apply_changes_unchecked(&graph, changes).unwrap();
        let mut expected = BTreeSet::new();
        for (key, (left, right)) in left.iter().zip(&right).enumerate() {
            for left in left {
                for right in right {
                    expected.insert(format!(
                        "key=<urn:test:slice-demand:key:{key:03}>|left=<{left}>|right=<{right}>"
                    ));
                }
            }
        }
        let run = |suffix: &str, mode| {
            let query = node
                .prepare_query(&format!(
                    "SELECT ?left ?right ?key WHERE {{ \
                     ?left <urn:test:slice-demand:left> ?key . \
                     ?right <urn:test:slice-demand:right> ?key }} {suffix}"
                ))
                .unwrap();
            let mut options = QueryOptions::default();
            options.fast_paths = QueryFastPathMode::Disabled;
            options.join_mode = mode;
            options.collect_plan_statistics = false;
            node.execute_prepared_in_graphs(
                &AllowAllAuthorizer,
                std::slice::from_ref(&graph),
                &query,
                &options,
            )
            .unwrap()
        };
        let full = run("", JoinMode::Auto);
        assert_eq!(full.statistics.result_rows as usize, expected.len());
        assert_eq!(
            canonical_rows(full.results.clone()),
            expected.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            full.statistics.planned_joins[0].physical_operator,
            JoinKind::Hash
        );
        let assert_limited = |results: &QueryResults| {
            let QueryResults::Solutions(rows) = results else {
                panic!("expected solutions");
            };
            let actual = canonical_rows(results.clone());
            assert_eq!(rows.len(), 20);
            assert_eq!(actual.len(), 20);
            assert!(actual.iter().all(|row| expected.contains(row)));
        };
        let limited = run("LIMIT 20", JoinMode::Auto);
        assert_limited(&limited.results);
        assert_eq!(
            limited.statistics.planned_joins[0].physical_operator,
            JoinKind::IndexedLateral
        );
        let forced = run("LIMIT 20", JoinMode::ForceHash);
        assert_limited(&forced.results);
        assert_eq!(
            forced.statistics.planned_joins[0].physical_operator,
            JoinKind::Hash
        );
    }

    #[test]
    fn optimizer_respects_visibility() {
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
            let rows = solution_rows(planner_rows(&node, sparql, optimize));
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
    fn noncanonical_literals_unfolded() {
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
        let optimized = solution_rows(planner_rows(&node, sparql, true));
        let raw = solution_rows(planner_rows(&node, sparql, false));
        assert_eq!(optimized.len(), raw.len());
        // Every visible dataset has a version quad; idx % 5 == 0 graphs use
        // the "01" spelling and must still match.
        let visible_count = (0..GRAPH_COUNT).filter(|idx| idx % 7 != 0).count();
        assert_eq!(optimized.len(), visible_count);
    }

    // Bound-object patterns under a small visible set.

    fn bound_object_shapes() -> Vec<(&'static str, String)> {
        vec![
            (
                "bound_object_type",
                "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> }".to_string(),
            ),
            (
                "bound_object_only",
                "SELECT ?s ?p WHERE { ?s ?p <http://schema.org/File> }".to_string(),
            ),
            (
                "bound_object_graph_var",
                "SELECT ?g ?d WHERE { GRAPH ?g { ?d a <http://schema.org/Dataset> } }".to_string(),
            ),
            (
                "bound_object_typed_literal",
                "SELECT ?d WHERE { ?d <http://schema.org/version> \
                 \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> }"
                    .to_string(),
            ),
            (
                "bound_object_exact_part",
                "SELECT ?d WHERE { ?d <http://schema.org/hasPart> <urn:eq:crate:0000/file-1> }"
                    .to_string(),
            ),
            (
                "bound_object_join_chain",
                "SELECT ?d ?fn WHERE { ?d a <http://schema.org/Dataset> . \
                 ?d <http://schema.org/hasPart> ?f . ?f a <http://schema.org/File> . \
                 ?f <http://schema.org/name> ?fn }"
                    .to_string(),
            ),
            (
                "bound_object_ask",
                "ASK { ?s ?p <http://schema.org/File> }".to_string(),
            ),
        ]
    }

    /// Explicit lists and lazy predicates must agree for bound objects.
    #[test]
    fn patterns_respect_visibility() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let corpus = seeded_corpus(&node);

        let listed: Vec<GraphId> = corpus.iter().take(UNION_VISIBLE_GRAPHS).cloned().collect();
        let allowed: BTreeSet<String> = listed
            .iter()
            .map(|graph| graph.as_str().to_string())
            .collect();
        for (label, sparql) in bound_object_shapes() {
            let from_list = canonical_rows(listed_rows(&node, &listed, &sparql));
            let from_predicate = canonical_rows(visible_rows(&node, &sparql, &allowed));
            assert_eq!(
                from_list, from_predicate,
                "{label}: explicit visible list diverged from the visibility predicate\n\
                 query: {sparql}"
            );
            assert!(!from_list.is_empty(), "{label}: shape must return rows");

            // Planner-on vs planner-off identity over the same visible set.
            assert_equivalent(&node, label, &sparql);
        }

        // Soundness: no graph outside the visible set may surface.
        let graphs = solution_rows(
            node.query_in_graphs(
                &AllowAllAuthorizer,
                &listed,
                "SELECT ?g WHERE { GRAPH ?g { ?d a <http://schema.org/Dataset> } }",
            )
            .unwrap(),
        );
        assert_eq!(graphs.len(), UNION_VISIBLE_GRAPHS);
        for row in &graphs {
            let name = row.get("g").unwrap().0.trim_matches(['<', '>']).to_string();
            assert!(allowed.contains(&name), "invisible graph leaked: {name}");
        }
    }

    // FILTER effective-boolean-value matrix.

    /// `Error` is an invalid EBV removed by FILTER, unlike negatable `False`.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Ebv {
        True,
        False,
        Error,
    }

    const EBV_VALUE: &str = "urn:eq:ebv:value";
    const XSD: &str = "http://www.w3.org/2001/XMLSchema";

    /// Cached term decoding must preserve spareval's effective boolean value semantics.
    fn ebv_matrix() -> Vec<(&'static str, String, Ebv)> {
        let typed = |value: &str, datatype: &str| format!("\"{value}\"^^<{XSD}#{datatype}>");
        vec![
            ("bool-true", typed("true", "boolean"), Ebv::True),
            ("bool-false", typed("false", "boolean"), Ebv::False),
            ("str-empty", "\"\"".to_string(), Ebv::False),
            ("str-plain", "\"text\"".to_string(), Ebv::True),
            ("xsd-str-empty", typed("", "string"), Ebv::False),
            ("xsd-str-plain", typed("text", "string"), Ebv::True),
            ("int-zero", typed("0", "integer"), Ebv::False),
            ("int-positive", typed("5", "integer"), Ebv::True),
            ("int-negative", typed("-3", "integer"), Ebv::True),
            ("decimal-zero", typed("0.0", "decimal"), Ebv::False),
            ("decimal-positive", typed("1.5", "decimal"), Ebv::True),
            ("double-zero", typed("0.0E0", "double"), Ebv::False),
            ("double-positive", typed("1.0E0", "double"), Ebv::True),
            ("double-nan", typed("NaN", "double"), Ebv::False),
            ("float-zero", typed("0", "float"), Ebv::False),
            ("float-positive", typed("2.5", "float"), Ebv::True),
            ("lang-string", "\"text\"@en".to_string(), Ebv::Error),
            ("date", typed("2026-01-01", "date"), Ebv::Error),
            (
                "custom-typed",
                "\"abc\"^^<urn:eq:ebv:custom>".to_string(),
                Ebv::Error,
            ),
            ("iri", "<urn:eq:ebv:target>".to_string(), Ebv::Error),
            ("blank", "_:ebvBlank".to_string(), Ebv::Error),
        ]
    }

    fn ebv_entity(name: &str) -> String {
        format!("<urn:eq:ebv:e:{name}>")
    }

    fn seed_ebv_graph(node: &CraqleNode) {
        let graph = GraphId::new("urn:eq:ebv:graph");
        let changes = ebv_matrix()
            .into_iter()
            .map(|(name, literal, _)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm(ebv_entity(name)),
                predicate: term_iri(EBV_VALUE),
                object: EncodedTerm(literal),
            })
            .collect();
        node.apply_changes_unchecked(&graph, changes).unwrap();
        node.ensure_query_indexes();
    }

    /// Entities kept by `FILTER(<expression>)`, asserted identical with the
    /// planner on and off.
    fn ebv_entities(node: &CraqleNode, expression: &str) -> BTreeSet<String> {
        let sparql = format!("SELECT ?e WHERE {{ ?e <{EBV_VALUE}> ?v . FILTER({expression}) }}");
        let entities = |optimize: bool| -> BTreeSet<String> {
            solution_rows(all_rows(node, &sparql, optimize))
                .into_iter()
                .map(|row| row.get("e").expect("?e must be bound").0.clone())
                .collect()
        };
        let optimized = entities(true);
        assert_eq!(
            optimized,
            entities(false),
            "planner changed FILTER({expression})"
        );
        optimized
    }

    fn ebv_expecting(wanted: Ebv) -> BTreeSet<String> {
        ebv_matrix()
            .into_iter()
            .filter(|(_, _, ebv)| *ebv == wanted)
            .map(|(name, _, _)| ebv_entity(name))
            .collect()
    }

    #[test]
    fn hooks_preserve_booleans() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        seed_ebv_graph(&node);

        // FILTER(?v) keeps exactly the true-EBV rows; type errors drop out.
        assert_eq!(ebv_entities(&node, "?v"), ebv_expecting(Ebv::True));
        // `!` flips false to true but leaves type errors dropped, so this is
        // the only way to tell "EBV false" apart from "not an EBV type".
        assert_eq!(ebv_entities(&node, "!?v"), ebv_expecting(Ebv::False));
        // Boolean connectives must not turn a type error into a verdict.
        assert_eq!(ebv_entities(&node, "?v || ?v"), ebv_expecting(Ebv::True));
        assert_eq!(ebv_entities(&node, "?v && ?v"), ebv_expecting(Ebv::True));
        assert_eq!(
            ebv_entities(&node, "IF(?v, true, false)"),
            ebv_expecting(Ebv::True)
        );

        let errors = ebv_expecting(Ebv::Error);
        assert!(!errors.is_empty());
        assert!(ebv_entities(&node, "?v").is_disjoint(&errors));
        assert!(ebv_entities(&node, "!?v").is_disjoint(&errors));
    }

    #[test]
    fn hooks_preserve_classification() {
        let tmp = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        seed_ebv_graph(&node);

        let all: BTreeSet<String> = ebv_matrix()
            .into_iter()
            .map(|(name, _, _)| ebv_entity(name))
            .collect();

        // Externalization must reproduce the term kind, not just its EBV.
        assert_eq!(
            ebv_entities(&node, "isIRI(?v)"),
            BTreeSet::from([ebv_entity("iri")])
        );
        assert_eq!(
            ebv_entities(&node, "isBlank(?v)"),
            BTreeSet::from([ebv_entity("blank")])
        );
        let literals: BTreeSet<String> = all
            .difference(&BTreeSet::from([ebv_entity("iri"), ebv_entity("blank")]))
            .cloned()
            .collect();
        assert_eq!(ebv_entities(&node, "isLiteral(?v)"), literals);

        // Datatypes survive the round trip through the term cache.
        assert_eq!(
            ebv_entities(&node, &format!("datatype(?v) = <{XSD}#integer>")),
            BTreeSet::from([
                ebv_entity("int-zero"),
                ebv_entity("int-positive"),
                ebv_entity("int-negative"),
            ])
        );
        assert_eq!(
            ebv_entities(&node, "lang(?v) = \"en\""),
            BTreeSet::from([ebv_entity("lang-string")])
        );

        // STR() is defined for IRIs and literals but not for blank nodes,
        // where SPARQL raises a type error and the solution is eliminated.
        let stringable: BTreeSet<String> = all
            .difference(&BTreeSet::from([ebv_entity("blank")]))
            .cloned()
            .collect();
        assert_eq!(ebv_entities(&node, "STRLEN(STR(?v)) >= 0"), stringable);
    }
}
