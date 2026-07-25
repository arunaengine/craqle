mod support;

/// Graph-existence semantics (charter G9, finding K5).
///
/// A named graph exists for SPARQL iff its metadata record exists **and** it is
/// visible to the caller — identically whether the visible set is small enough
/// for the explicit-dataset regime or large enough to force the union regime.
/// Empty graphs exist. Graphs whose entities are all orphan-hidden exist.
/// Deleted graphs do not, even though their IRI stays interned in the term
/// table.
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use craqle::*;

    use crate::support::*;

    /// Mirrors `sparql::EXPLICIT_DATASET_GRAPH_LIMIT`: at or below it the query
    /// runs against an explicit spareval dataset spec, above it against the
    /// union view. The two used to answer graph existence differently.
    const EXPLICIT_DATASET_GRAPH_LIMIT: usize = 32;
    const SMALL_VISIBLE: usize = 5;
    const LARGE_VISIBLE: usize = 40;
    /// `empty`, `deleted` and `orphaned` are visible on top of the populated ones.
    const FIXTURE_GRAPHS: usize = 3;

    // The two visible-set sizes really must straddle the regime boundary, or
    // this whole file would test one code path twice.
    const _: () = assert!(SMALL_VISIBLE <= EXPLICIT_DATASET_GRAPH_LIMIT);
    const _: () = assert!(LARGE_VISIBLE > EXPLICIT_DATASET_GRAPH_LIMIT);

    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const SCHEMA_NAME: &str = "http://schema.org/name";
    const SCHEMA_DATASET: &str = "http://schema.org/Dataset";
    const SCHEMA_MEDIA_OBJECT: &str = "http://schema.org/MediaObject";
    const SCHEMA_IS_BASED_ON: &str = "http://schema.org/isBasedOn";

    fn iri(value: &str) -> EncodedTerm {
        EncodedTerm(format!("<{value}>"))
    }

    fn text(value: &str) -> EncodedTerm {
        EncodedTerm(format!("\"{value}\""))
    }

    struct Triple<'a> {
        subject: &'a EncodedTerm,
        predicate: &'a str,
        object: EncodedTerm,
    }

    fn insert(graph: &GraphId, triple: Triple<'_>) -> MaterializedQuadChange {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: triple.subject.clone(),
            predicate: iri(triple.predicate),
            object: triple.object,
        }
    }

    fn delete(graph: &GraphId, triple: Triple<'_>) -> MaterializedQuadChange {
        MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: triple.subject.clone(),
            predicate: iri(triple.predicate),
            object: triple.object,
        }
    }

    /// The graph shapes graph existence has to distinguish.
    struct Corpus {
        /// Visible graphs holding live, visible quads.
        populated: Vec<GraphId>,
        /// Visible graph created and then emptied: metadata, no live quads.
        empty: GraphId,
        /// Visible graph created and then deleted; its IRI stays interned
        /// because another graph still references it as an object term.
        deleted: GraphId,
        /// Visible graph whose only entity is orphan-hidden, so it holds live
        /// quads of which none are visible.
        orphaned: GraphId,
        /// Exists and holds visible quads, but the caller cannot see it.
        hidden: GraphId,
    }

    impl Corpus {
        /// The caller's visible-graph list. `hidden` is deliberately absent.
        fn visible(&self) -> Vec<GraphId> {
            let mut graphs = self.populated.clone();
            graphs.push(self.empty.clone());
            graphs.push(self.deleted.clone());
            graphs.push(self.orphaned.clone());
            graphs
        }

        /// Visible graphs that still have a metadata record — exactly the set
        /// `GRAPH ?g {}` must enumerate.
        fn existing_visible(&self) -> BTreeSet<String> {
            self.populated
                .iter()
                .chain([&self.empty, &self.orphaned])
                .map(|graph| format!("<{}>", graph.as_str()))
                .collect()
        }
    }

    fn build_corpus(node: &CraqleNode, prefix: &str, populated: usize) -> Corpus {
        let graph_id = |name: &str| GraphId::new(&format!("urn:exists:{prefix}:{name}"));
        let deleted = graph_id("deleted");

        let mut graphs = Vec::with_capacity(populated);
        for idx in 0..populated {
            let graph = graph_id(&format!("data-{idx:03}"));
            let root = iri(graph.as_str());
            let mut changes = vec![
                insert(
                    &graph,
                    Triple {
                        subject: &root,
                        predicate: RDF_TYPE,
                        object: iri(SCHEMA_DATASET),
                    },
                ),
                insert(
                    &graph,
                    Triple {
                        subject: &root,
                        predicate: SCHEMA_NAME,
                        object: text(&format!("Dataset {idx:03}")),
                    },
                ),
            ];
            if idx == 0 {
                // Keeps the deleted graph's IRI interned independently of the
                // deleted graph itself, so this really tests "deleted graph
                // whose term survives".
                changes.push(insert(
                    &graph,
                    Triple {
                        subject: &root,
                        predicate: SCHEMA_IS_BASED_ON,
                        object: iri(deleted.as_str()),
                    },
                ));
            }
            node.apply_changes_unchecked(&graph, changes).unwrap();
            graphs.push(graph);
        }

        // Created, then emptied: the metadata record outlives the quads.
        let empty = graph_id("empty");
        let empty_root = iri(empty.as_str());
        let transient = Triple {
            subject: &empty_root,
            predicate: SCHEMA_NAME,
            object: text("Transient"),
        };
        node.apply_changes_unchecked(&empty, vec![insert(&empty, transient)])
            .unwrap();
        node.apply_changes_unchecked(
            &empty,
            vec![delete(
                &empty,
                Triple {
                    subject: &empty_root,
                    predicate: SCHEMA_NAME,
                    object: text("Transient"),
                },
            )],
        )
        .unwrap();
        assert!(
            node.contains_graph(&empty).unwrap(),
            "emptying a graph must not delete it"
        );

        // Created, then deleted: metadata gone, IRI still interned.
        let deleted_root = iri(deleted.as_str());
        node.apply_changes_unchecked(
            &deleted,
            vec![insert(
                &deleted,
                Triple {
                    subject: &deleted_root,
                    predicate: SCHEMA_NAME,
                    object: text("Doomed"),
                },
            )],
        )
        .unwrap();
        node.delete_graph_unchecked(&deleted).unwrap();
        assert!(!node.contains_graph(&deleted).unwrap());

        // Live quads, none of them visible: the single entity is unreachable
        // from the crate root, so orphan hiding suppresses every quad.
        let orphaned = graph_id("orphaned");
        let orphan_entity = iri(&format!("{}/file.txt", orphaned.as_str()));
        node.apply_changes_unchecked(
            &orphaned,
            vec![
                insert(
                    &orphaned,
                    Triple {
                        subject: &orphan_entity,
                        predicate: RDF_TYPE,
                        object: iri(SCHEMA_MEDIA_OBJECT),
                    },
                ),
                insert(
                    &orphaned,
                    Triple {
                        subject: &orphan_entity,
                        predicate: SCHEMA_NAME,
                        object: text("Unreachable File"),
                    },
                ),
            ],
        )
        .unwrap();
        node.rebuild_graph_diagnostics(&orphaned).unwrap();
        assert!(
            node.graph_diagnostics(&orphaned).unwrap().has_orphans(),
            "fixture must actually be orphan-hidden"
        );

        let hidden = graph_id("hidden");
        let hidden_root = iri(hidden.as_str());
        node.apply_changes_unchecked(
            &hidden,
            vec![insert(
                &hidden,
                Triple {
                    subject: &hidden_root,
                    predicate: SCHEMA_NAME,
                    object: text("Unauthorized"),
                },
            )],
        )
        .unwrap();

        Corpus {
            populated: graphs,
            empty,
            deleted,
            orphaned,
            hidden,
        }
    }

    /// Everything a caller can learn about graph existence for one corpus.
    #[derive(Debug, PartialEq, Eq)]
    struct ExistenceAnswers {
        /// `SELECT ?g WHERE { GRAPH ?g {} }`
        enumerated: BTreeSet<String>,
        /// `ASK { GRAPH <g> {} }` per labelled probe.
        asks: Vec<(&'static str, bool)>,
    }

    fn existence_answers(node: &CraqleNode, corpus: &Corpus) -> ExistenceAnswers {
        let visible = corpus.visible();

        let enumerated = solution_rows(
            node.query_graphs(&visible, "SELECT ?g WHERE { GRAPH ?g {} }")
                .unwrap(),
        )
        .into_iter()
        .map(|row| row.get("g").expect("?g must be bound").0.clone())
        .collect();

        let probes: [(&'static str, &str); 6] = [
            ("populated", corpus.populated[0].as_str()),
            ("empty", corpus.empty.as_str()),
            ("orphan-hidden", corpus.orphaned.as_str()),
            ("deleted", corpus.deleted.as_str()),
            ("hidden", corpus.hidden.as_str()),
            ("never-created", "urn:exists:never-created"),
        ];
        let asks = probes
            .into_iter()
            .map(|(label, graph_iri)| {
                let answer = node
                    .query_graphs(&visible, &format!("ASK {{ GRAPH <{graph_iri}> {{}} }}"))
                    .unwrap();
                (label, answer == QueryResults::Boolean(true))
            })
            .collect();

        ExistenceAnswers { enumerated, asks }
    }

    fn open_node(tmp: &tempfile::TempDir) -> CraqleNode {
        CraqleNode::open_with_options(
            tmp.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap()
    }

    #[test]
    fn graph_existence_is_identical_in_both_dataset_regimes() {
        let tmp = tempfile::tempdir().unwrap();
        let node = open_node(&tmp);

        let small = build_corpus(&node, "small", SMALL_VISIBLE - FIXTURE_GRAPHS);
        let large = build_corpus(&node, "large", LARGE_VISIBLE - FIXTURE_GRAPHS);
        node.ensure_query_indexes();

        assert_eq!(small.visible().len(), SMALL_VISIBLE);
        assert_eq!(large.visible().len(), LARGE_VISIBLE);

        let small_answers = existence_answers(&node, &small);
        let large_answers = existence_answers(&node, &large);

        // The regimes must not disagree about what a graph is.
        assert_eq!(
            small_answers.asks, large_answers.asks,
            "graph existence diverged between the explicit-dataset and union regimes"
        );

        // ... and the shared answer must be the charter's G9 semantics:
        // created-and-not-deleted and visible, regardless of content.
        let expected = vec![
            ("populated", true),
            ("empty", true),
            ("orphan-hidden", true),
            ("deleted", false),
            ("hidden", false),
            ("never-created", false),
        ];
        assert_eq!(small_answers.asks, expected);
        assert_eq!(large_answers.asks, expected);

        assert_eq!(small_answers.enumerated, small.existing_visible());
        assert_eq!(large_answers.enumerated, large.existing_visible());
    }

    #[test]
    fn existing_graphs_without_visible_quads_yield_no_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let node = open_node(&tmp);

        let small = build_corpus(&node, "small", SMALL_VISIBLE - FIXTURE_GRAPHS);
        let large = build_corpus(&node, "large", LARGE_VISIBLE - FIXTURE_GRAPHS);
        node.ensure_query_indexes();

        // Existence is about the metadata record, not about content: these
        // graphs exist (asserted above) yet contribute no solutions.
        for corpus in [&small, &large] {
            let visible = corpus.visible();
            for graph in [&corpus.empty, &corpus.orphaned] {
                let rows = solution_rows(
                    node.query_graphs(
                        &visible,
                        &format!(
                            "SELECT ?s ?p ?o WHERE {{ GRAPH <{}> {{ ?s ?p ?o }} }}",
                            graph.as_str()
                        ),
                    )
                    .unwrap(),
                );
                assert!(
                    rows.is_empty(),
                    "{} must contribute no visible quads",
                    graph.as_str()
                );
            }
        }
    }

    #[test]
    fn deleted_graph_is_absent_even_though_its_iri_is_still_referenced() {
        let tmp = tempfile::tempdir().unwrap();
        let node = open_node(&tmp);

        let small = build_corpus(&node, "small", SMALL_VISIBLE - FIXTURE_GRAPHS);
        let large = build_corpus(&node, "large", LARGE_VISIBLE - FIXTURE_GRAPHS);
        node.ensure_query_indexes();

        for corpus in [&small, &large] {
            // The IRI is still a live object term, so it is certainly still
            // interned — yet the graph itself is gone.
            let referenced = solution_rows(
                node.query_graphs(
                    &corpus.visible(),
                    &format!(
                        "SELECT ?s WHERE {{ ?s <{SCHEMA_IS_BASED_ON}> <{}> }}",
                        corpus.deleted.as_str()
                    ),
                )
                .unwrap(),
            );
            assert_eq!(referenced.len(), 1, "the deleted IRI must stay interned");

            let name = format!("<{}>", corpus.deleted.as_str());
            let answers = existence_answers(&node, corpus);
            assert!(!answers.enumerated.contains(&name));
        }
    }
}
