//! Checks full-text service rewriting and authorized SPARQL hits.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

mod support;

/// Authorized SERVICE matches must fill the requested page under a real search index.
#[cfg(all(test, feature = "search"))]
mod tests {
    use std::collections::BTreeSet;

    use craqle::*;

    use crate::support::*;

    const SCHEMA_NAME: &str = "http://schema.org/name";
    const SCHEMA_DESCRIPTION: &str = "http://schema.org/description";
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const SCHEMA_DATASET: &str = "http://schema.org/Dataset";

    /// Every graph's crate matches the query, but only these are readable.
    const READABLE: usize = 50;
    const UNREADABLE: usize = 200;
    /// Matches the readable count, so a complete answer fills the limit exactly.
    const FTS_LIMIT: usize = 50;

    fn iri(value: &str) -> EncodedTerm {
        EncodedTerm(format!("<{value}>"))
    }

    fn text(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    fn readable_graph(idx: usize) -> GraphId {
        GraphId::new(&format!("urn:fts:readable:{idx:04}"))
    }

    fn unreadable_graph(idx: usize) -> GraphId {
        GraphId::new(&format!("urn:fts:unreadable:{idx:04}"))
    }

    /// Only the readable half is visible to the query.
    fn visible(graph: &GraphId) -> bool {
        graph.as_str().starts_with("urn:fts:readable:")
    }

    fn seed_matching_crate(node: &CraqleNode, graph: &GraphId) {
        let root = iri(graph.as_str());
        let insert = |predicate: &str, object: EncodedTerm| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: root.clone(),
            predicate: iri(predicate),
            object,
        };
        node.apply_changes_unchecked(
            graph,
            vec![
                insert(RDF_TYPE, iri(SCHEMA_DATASET)),
                insert(SCHEMA_NAME, text("Proteomics Atlas")),
                insert(
                    SCHEMA_DESCRIPTION,
                    text("Large-scale proteomics reference experiment"),
                ),
            ],
        )
        .unwrap();
    }

    fn seeded_node(tmp: &tempfile::TempDir) -> CraqleNode {
        let node = CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();

        // Equal-scoring readable and hidden documents exercise authorization before ranking.
        for idx in 0..UNREADABLE.max(READABLE) {
            if idx < UNREADABLE {
                seed_matching_crate(&node, &unreadable_graph(idx));
            }
            if idx < READABLE {
                seed_matching_crate(&node, &readable_graph(idx));
            }
        }

        node.flush_search_updates().unwrap();
        node.ensure_query_indexes();
        node
    }

    fn fts_graph_rows(node: &CraqleNode, sparql: &str) -> BTreeSet<String> {
        graph_rows_for(node, visible, sparql)
    }

    fn graph_rows_for<F>(node: &CraqleNode, visible: F, sparql: &str) -> BTreeSet<String>
    where
        F: Fn(&GraphId) -> bool + Send + Sync,
    {
        let auth = move |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
            if visible(graph) {
                Ok(())
            } else {
                Err(AuthorizationError::PermissionDenied {
                    action,
                    graph: graph.as_str().to_owned(),
                })
            }
        };
        let query = node.prepare_query(sparql).unwrap();
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        solution_rows(
            node.execute_prepared(&auth, &query, &options)
                .unwrap()
                .results,
        )
        .into_iter()
        .map(|row| row.get("g").expect("?g must be bound").0.clone())
        .collect()
    }

    fn bounded_graph_rows<F>(node: &CraqleNode, visible: F, sparql: &str) -> BTreeSet<String>
    where
        F: Fn(&GraphId) -> bool + Send + Sync,
    {
        solution_rows(query_with_visibility(node, visible, sparql).unwrap())
            .into_iter()
            .map(|row| row.get("g").expect("?g must be bound").0.clone())
            .collect()
    }

    #[test]
    fn service_returns_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let node = seeded_node(&tmp);

        let sparql = format!(
            r#"
            SELECT ?s ?g
            WHERE {{
                SERVICE <urn:craqle:fts> {{
                    ?s fts:query "proteomics" .
                    ?s fts:graph ?g .
                    ?s fts:limit {FTS_LIMIT} .
                }}
            }}
            "#
        );

        let graphs = fts_graph_rows(&node, &sparql);
        assert_eq!(
            graphs.len(),
            FTS_LIMIT,
            "FTS SERVICE under-returned authorized hits: {} of {FTS_LIMIT}",
            graphs.len()
        );

        // Soundness is not traded for completeness.
        assert!(
            graphs
                .iter()
                .all(|graph| graph.starts_with("<urn:fts:readable:")),
            "unreadable graph leaked into FTS results"
        );
    }

    /// Oversized SERVICE limits must return a typed error before collector allocation.
    #[test]
    fn service_clamps_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let node = seeded_node(&tmp);

        let sparql = format!(
            r#"
            SELECT ?s ?g
            WHERE {{
                SERVICE <urn:craqle:fts> {{
                    ?s fts:query "proteomics" .
                    ?s fts:graph ?g .
                    ?s fts:limit {} .
                }}
            }}
            "#,
            usize::MAX
        );

        let graphs = bounded_graph_rows(&node, visible, &sparql);
        assert_eq!(
            READABLE,
            graphs.len(),
            "a clamped limit must still answer completely"
        );
        assert!(graphs.len() <= craqle::MAX_SEARCH_LIMIT);
    }

    #[test]
    fn service_caps_results() {
        let tmp = tempfile::tempdir().unwrap();
        let node = seeded_node(&tmp);

        // Over-fetching must never inflate the answer beyond `fts:limit`.
        for limit in [1usize, 7, 25] {
            let sparql = format!(
                r#"
                SELECT ?s ?g
                WHERE {{
                    SERVICE <urn:craqle:fts> {{
                        ?s fts:query "proteomics" .
                        ?s fts:graph ?g .
                        ?s fts:limit {limit} .
                    }}
                }}
                "#
            );
            let graphs = fts_graph_rows(&node, &sparql);
            assert_eq!(graphs.len(), limit, "fts:limit {limit} not honoured");
            assert!(
                graphs
                    .iter()
                    .all(|graph| graph.starts_with("<urn:fts:readable:"))
            );
        }
    }

    /// An exhausted hidden corpus must finish, while sparse authorized matches remain reachable.
    #[test]
    fn service_terminates_unauthorized() {
        // The watchdog turns lost progress into an assertion rather than a hung suite.
        with_watchdog("service_terminates_unauthorized", || {
            let tmp = tempfile::tempdir().unwrap();
            let node = seeded_node(&tmp);
            let sparql = |limit: usize| {
                format!(
                    r#"
                    SELECT ?s ?g
                    WHERE {{
                        SERVICE <urn:craqle:fts> {{
                            ?s fts:query "proteomics" .
                            ?s fts:graph ?g .
                            ?s fts:limit {limit} .
                        }}
                    }}
                    "#
                )
            };

            let rows = bounded_graph_rows(&node, |_: &GraphId| false, &sparql(FTS_LIMIT));
            assert!(
                rows.is_empty(),
                "an unauthorized reader must see nothing: {rows:?}"
            );

            // ... and the loop escalates far enough to reach a lone authorized
            // graph buried among 250 equal-scoring documents.
            let single = sparql(1);
            for idx in 0..READABLE {
                let wanted = readable_graph(idx);
                let found = graph_rows_for(&node, |graph: &GraphId| graph == &wanted, &single);
                assert_eq!(
                    BTreeSet::from([format!("<{}>", wanted.as_str())]),
                    found,
                    "the only authorized graph was dropped before the limit was filled"
                );
            }
        });
    }

    #[test]
    fn service_honours_visibility() {
        let tmp = tempfile::tempdir().unwrap();
        let node = seeded_node(&tmp);

        let readable = readable_graph(0);
        let sparql = format!(
            r#"
            SELECT ?s
            WHERE {{
                SERVICE <urn:craqle:fts> {{
                    ?s fts:query "proteomics" .
                    ?s fts:graph <{}> .
                    ?s fts:limit 10 .
                }}
            }}
            "#,
            readable.as_str()
        );
        let auth = |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
            if visible(graph) {
                Ok(())
            } else {
                Err(AuthorizationError::PermissionDenied {
                    action,
                    graph: graph.as_str().to_owned(),
                })
            }
        };
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        let query = node.prepare_query(&sparql).unwrap();
        let rows = solution_rows(
            node.execute_prepared(&auth, &query, &options)
                .unwrap()
                .results,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("s").unwrap().0,
            format!("<{}>", readable.as_str())
        );

        let hidden = unreadable_graph(0);
        let hidden_sparql = format!(
            r#"
            SELECT ?s
            WHERE {{
                SERVICE <urn:craqle:fts> {{
                    ?s fts:query "proteomics" .
                    ?s fts:graph <{}> .
                    ?s fts:limit 10 .
                }}
            }}
            "#,
            hidden.as_str()
        );
        let hidden_query = node.prepare_query(&hidden_sparql).unwrap();
        let hidden_rows = solution_rows(
            node.execute_prepared(&auth, &hidden_query, &options)
                .unwrap()
                .results,
        );
        assert!(hidden_rows.is_empty());
    }
}
