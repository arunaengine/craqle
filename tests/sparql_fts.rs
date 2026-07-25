mod support;

/// Completeness of the `SERVICE <urn:craqle:fts>` clause (charter G8, finding K2).
///
/// Graph visibility is decided *after* tantivy has ranked its hits, so asking
/// the index for exactly `fts:limit` documents silently returns fewer
/// authorized rows than the caller requested whenever the top-ranked hits sit
/// in graphs the caller cannot read. Authorized results must never be omitted.
#[cfg(test)]
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

        // Interleave the two populations so relevance ranking cannot separate
        // them: with equal scores the index is free to return any mix, and a
        // no-over-fetch reader keeps only the readable minority of the top N.
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
        solution_rows(node.query_graphs_with(visible, sparql).unwrap())
            .into_iter()
            .map(|row| row.get("g").expect("?g must be bound").0.clone())
            .collect()
    }

    #[test]
    fn fts_service_returns_limit_when_authorized_matches_exist() {
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

    #[test]
    fn fts_service_still_stops_at_the_requested_limit() {
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

    #[test]
    fn fts_service_terminates_when_nothing_is_authorized() {
        let tmp = tempfile::tempdir().unwrap();
        let node = seeded_node(&tmp);

        // Escalation must stop once the index is exhausted rather than loop.
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
        let rows = solution_rows(
            node.query_graphs_with(|_: &GraphId| false, &sparql)
                .unwrap(),
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn fts_service_scoped_to_a_fixed_graph_still_honours_visibility() {
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
        let rows = solution_rows(node.query_graphs_with(visible, &sparql).unwrap());
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
        let hidden_rows = solution_rows(node.query_graphs_with(visible, &hidden_sparql).unwrap());
        assert!(hidden_rows.is_empty());
    }
}
