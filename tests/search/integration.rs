//! Checks search updates and SPARQL full-text integration.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "../support.rs"]
mod support;

/// Every test here asserts on real tantivy results, so the `search`-off stub —
/// which answers every query with an empty set — cannot satisfy any of them.
#[cfg(all(test, feature = "search"))]
mod tests {
    use craqle::*;

    use crate::support::*;

    #[test]
    fn created_crate_searchable() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Microbial Genomics Study",
                    "Analysis of microbial communities",
                    "2025-03-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.flush_search_updates().unwrap();

        let hits = net
            .peer(0)
            .search(
                &GrantAuthorizer::default(),
                SearchRequest {
                    query: "genomics",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(!hits.is_empty(), "should find 'genomics' in crate name");
    }

    #[test]
    fn reindex_keeps_results() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");

        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph,
                    "Proteomics Dataset",
                    "Search helper should commit indexed documents",
                    "2025-03-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();

        net.reindex_search().unwrap();
        let hits = net
            .peer(0)
            .search(
                &GrantAuthorizer::default(),
                SearchRequest {
                    query: "proteomics",
                    limit: 10,
                },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn concurrent_edits_searchable() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-search-edit");
        let writer = writer_auth();
        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Microbial Genomics Study",
                    "Microbial sequencing",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .update_property(
                &writer,
                &graph,
                graph.as_str(),
                "schema:name",
                None,
                "Microbial Proteomics Study",
            )
            .unwrap();
        net.peer(1)
            .add_data_entity_with_triples(
                &writer,
                &graph,
                "assembly.txt",
                "http://schema.org/MediaObject",
                "Assembly Notes",
                vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                    oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                        "metagenomic assembly",
                    )),
                )],
            )
            .unwrap();

        net.sync_until_converged(10).unwrap();

        let proteomics = reindex_and_search(&net, 0, "proteomics");
        let metagenomic = reindex_and_search(&net, 0, "metagenomic");
        let genomics = reindex_and_search(&net, 0, "genomics");

        assert!(proteomics.iter().any(|subject| subject == graph.as_str()));
        assert!(
            metagenomic
                .iter()
                .any(|subject| subject.contains("assembly.txt"))
        );
        assert!(genomics.is_empty());
    }

    #[test]
    fn sparql_uses_search() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-fts");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Integrated FTS Crate",
                    "SPARQL should see Tantivy hits",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.peer(0)
            .add_data_entity_with_triples(
                &writer,
                &graph,
                "data/proteomics-01.tsv",
                "http://schema.org/MediaObject",
                "Proteomics Table",
                vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                    oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                        "proteomics peptide quantification",
                    )),
                )],
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let query = format!(
            r#"
        SELECT ?s ?g ?score ?name
        WHERE {{
          SERVICE <urn:craqle:fts> {{
            ?s fts:query "proteomics" .
            ?s fts:score ?score .
            ?s fts:graph ?g .
            ?s fts:limit 10 .
          }}
          GRAPH ?g {{ ?s schema:name ?name }}
          FILTER(?g = <{}>)
        }}
        ORDER BY DESC(?score)
        "#,
            graph.as_str()
        );

        let prepared = net.peer(1).prepare_query(&query).unwrap();
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        let rows = solution_rows(
            net.peer(1)
                .execute_prepared(&GrantAuthorizer::default(), &prepared, &options)
                .unwrap()
                .results,
        );
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|row| {
            row.get("s")
                .is_some_and(|value| value.0.contains("proteomics-01.tsv"))
        }));
        assert!(rows[0].contains_key("score"));
    }

    #[test]
    fn index_persists_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:crate-fts-persist");

        {
            let node = CraqleNode::open(tmp.path().join("peer0")).unwrap();
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Persisted Search Crate",
                    "Committed Tantivy index should survive restart",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
            node.add_data_entity_with_triples(
                &writer_auth(),
                &graph,
                "data/proteomics-01.tsv",
                "http://schema.org/MediaObject",
                "Proteomics Table",
                vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                    oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                        "persisted proteomics peptide quantification",
                    )),
                )],
            )
            .unwrap();
            node.flush_search_updates().unwrap();

            let hits = node
                .search(
                    &GrantAuthorizer::default(),
                    SearchRequest {
                        query: "proteomics",
                        limit: 10,
                    },
                )
                .unwrap();
            assert!(
                hits.iter()
                    .any(|hit| hit.subject_iri.contains("proteomics-01.tsv"))
            );
        }

        let reopened = CraqleNode::open(tmp.path().join("peer0")).unwrap();
        reopened.flush_search_updates().unwrap();
        let hits = reopened
            .search(
                &GrantAuthorizer::default(),
                SearchRequest {
                    query: "proteomics",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri.contains("proteomics-01.tsv"))
        );
    }

    #[test]
    fn sync_updates_search() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:remote-batch-search");
        let writer = writer_auth();

        net.peer(0)
            .create_crate(
                &writer,
                CreateCrateRequest::new(
                    graph.clone(),
                    "Remote Batch Dataset",
                    "Receiver should update search directly",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    public_policy(),
                ),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .append_new_root_data_entities(
                &writer,
                &graph,
                benchmark_entities(EntityBatch {
                    start: 0,
                    count: 50,
                    keyword: "remote-batch-keyword",
                    name_prefix: "Remote Batch Entity",
                    description_label: "remote batch record",
                    identifier_prefix: "RBATCH",
                }),
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let hits = net
            .peer(1)
            .search(
                &GrantAuthorizer::default(),
                SearchRequest {
                    query: "RBATCH-000049",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.subject_iri.contains("entity-000049.dat"))
        );
    }

    #[test]
    fn delete_removes_results() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:delete-search");
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Deleted Search Dataset",
                "This should disappear from search",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();
        node.flush_search_updates().unwrap();
        assert!(
            !node
                .search(
                    &reader,
                    SearchRequest {
                        query: "deleted",
                        limit: 10
                    }
                )
                .unwrap()
                .is_empty()
        );

        node.delete_graph(&writer, &graph).unwrap();
        let recreate = node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Replacement Search Dataset",
                "Only replacement text should be searchable",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        );
        assert_eq!(recreate.unwrap_err().kind(), CraqleErrorKind::Conflict);
        node.flush_search_updates().unwrap();

        assert!(
            node.search(
                &reader,
                SearchRequest {
                    query: "deleted",
                    limit: 10
                }
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            node.search(
                &reader,
                SearchRequest {
                    query: "replacement",
                    limit: 10
                }
            )
            .unwrap()
            .is_empty()
        );
    }

    /// Unreadable high-scoring matches must not displace readable results from a full page.
    #[test]
    fn search_returns_limit() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();

        // Unreadable graphs, each scoring far above the readable ones so they
        // dominate the top of the ranking.
        for idx in 0..200 {
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    GraphId::new(&format!("urn:test:escalation:private-{idx:03}")),
                    format!("Private Escalation {idx}"),
                    "escalationneedle ".repeat(40),
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    GraphPolicy {
                        public: false,
                        permission_paths: vec!["/tests/private/escalation".to_string()],
                    },
                ),
            )
            .unwrap();
        }

        for idx in 0..50 {
            node.create_crate(
                &writer,
                CreateCrateRequest::new(
                    GraphId::new(&format!("urn:test:escalation:public-{idx:03}")),
                    format!("Public Escalation {idx}"),
                    "escalationneedle",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    GraphPolicy {
                        public: true,
                        permission_paths: vec!["/tests/public/escalation".to_string()],
                    },
                ),
            )
            .unwrap();
        }
        node.flush_search_updates().unwrap();

        for limit in [25, 50] {
            let hits = node
                .search(
                    &reader,
                    SearchRequest {
                        query: "escalationneedle",
                        limit,
                    },
                )
                .unwrap();

            assert_eq!(
                hits.len(),
                limit,
                "limit {limit} must be filled from the 50 readable graphs"
            );
            // Soundness: nothing from an unreadable graph may leak through.
            assert!(
                hits.iter()
                    .all(|hit| hit.graph_id.contains("escalation:public-")),
                "unreadable graph leaked into results"
            );
        }
    }

    /// A flush must cover its submission cutoff and finish while later writes continue.
    #[test]
    fn flush_survives_ingest() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();
        let reader = GrantAuthorizer::default();
        let graph = GraphId::new("urn:test:sustained-ingest");

        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Sustained Ingest Dataset",
                "Contains sustainedmarker before the flush",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();

        let stop = AtomicBool::new(false);
        // Stops the writer even if an assertion unwinds; without it a failure
        // leaves the writer looping forever inside `scope`'s join.
        struct StopOnDrop<'a>(&'a AtomicBool);
        impl Drop for StopOnDrop<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let written = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let _stopper = StopOnDrop(&stop);
            scope.spawn(|| {
                let writer = writer_auth();
                let mut idx = 0usize;
                while !stop.load(Ordering::SeqCst) {
                    node.add_data_entity_with_triples(
                        &writer,
                        &graph,
                        &format!("data/background-{idx:05}.dat"),
                        "http://schema.org/MediaObject",
                        &format!("Background Entity {idx}"),
                        vec![(
                            oxrdf::NamedNode::new_unchecked("http://schema.org/description"),
                            oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                                "background ingest record",
                            )),
                        )],
                    )
                    .unwrap();
                    idx += 1;
                    written.store(idx, Ordering::SeqCst);
                }
                idx
            });

            // Give the writer a head start so the queue is genuinely busy.
            std::thread::sleep(std::time::Duration::from_millis(250));

            // Pin the concurrent stream, not just the pre-loop marker: every
            // entity written before the flush must be searchable after it.
            let enqueued_before = written.load(Ordering::SeqCst);
            // A generous cap turns a livelocked flush into a failure instead of a hang.
            node.flush_search(&SearchFlushOptions {
                timeout: Some(std::time::Duration::from_secs(120)),
                ..SearchFlushOptions::default()
            })
            .unwrap();

            assert!(
                enqueued_before > 0,
                "the writer must have ingested something, or this proves nothing"
            );
            for idx in 0..enqueued_before {
                let hits = node
                    .search(
                        &reader,
                        SearchRequest {
                            query: &format!("\"Background Entity {idx}\""),
                            limit: 10,
                        },
                    )
                    .unwrap();
                assert!(
                    !hits.is_empty(),
                    "entity {idx} was enqueued before the flush but is not searchable after it"
                );
            }

            stop.store(true, Ordering::SeqCst);
        });
    }

    /// Each partition removes one reachability path; their merge must remove both.
    fn make_replicated_orphan(net: &mut CraqleCluster, graph: &GraphId) -> SnapshotQuadState {
        let has_part = EncodedTerm::from_named_node(&vocab::schema_has_part());
        let root = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(graph.as_str()));
        let edge = net
            .peer(0)
            .graph_snapshot(graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.subject == root && quad.predicate == has_part)
            .expect("root must link the child");
        let parent = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(format!(
            "{}#orphan-staging-parent",
            graph.as_str()
        )));
        let parent_name = EncodedTerm::from_term(&oxrdf::Term::Literal(
            oxrdf::Literal::new_simple_literal("Orphan staging parent"),
        ))
        .unwrap();

        net.peer(0)
            .apply_changes(
                &AllowAllAuthorizer,
                graph,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: parent.clone(),
                        predicate: EncodedTerm::from_named_node(&vocab::rdf_type()),
                        object: EncodedTerm::from_named_node(&vocab::schema_dataset()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: parent.clone(),
                        predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
                        object: parent_name.clone(),
                    },
                    MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: root.clone(),
                        predicate: has_part.clone(),
                        object: parent.clone(),
                    },
                    MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: parent.clone(),
                        predicate: has_part.clone(),
                        object: edge.object.clone(),
                    },
                ],
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.partition(0, 1);
        net.peer(0)
            .apply_changes(
                &AllowAllAuthorizer,
                graph,
                vec![MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: edge.subject.clone(),
                    predicate: edge.predicate.clone(),
                    object: edge.object.clone(),
                }],
            )
            .unwrap();
        net.peer(1)
            .apply_changes(
                &AllowAllAuthorizer,
                graph,
                vec![MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: parent.clone(),
                    predicate: has_part.clone(),
                    object: edge.object.clone(),
                }],
            )
            .unwrap();
        net.heal(0, 1);
        net.sync_until_converged(10).unwrap();

        net.peer(0)
            .apply_changes(
                &AllowAllAuthorizer,
                graph,
                vec![
                    MaterializedQuadChange::Delete {
                        graph: graph.clone(),
                        subject: root,
                        predicate: has_part,
                        object: parent.clone(),
                    },
                    MaterializedQuadChange::Delete {
                        graph: graph.clone(),
                        subject: parent.clone(),
                        predicate: EncodedTerm::from_named_node(&vocab::rdf_type()),
                        object: EncodedTerm::from_named_node(&vocab::schema_dataset()),
                    },
                    MaterializedQuadChange::Delete {
                        graph: graph.clone(),
                        subject: parent,
                        predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
                        object: parent_name,
                    },
                ],
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();
        edge
    }

    /// A reachability-only change must remove the indirectly orphaned entity from search.
    #[test]
    fn orphan_leaves_index() {
        let (_dir, mut net) = setup_network(2);
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:orphan-requeue");

        net.peer(0)
            .create_crate(
                &auth,
                CreateCrateRequest::new(
                    graph.clone(),
                    "requeue crate",
                    "description",
                    "2025-01-01",
                    None,
                    GraphPolicy {
                        public: true,
                        permission_paths: vec!["/t/x".to_string()],
                    },
                ),
            )
            .unwrap();
        net.peer(0)
            .append_new_root_data_entities(
                &auth,
                &graph,
                vec![NewDataEntity {
                    entity_id: "data/pufferfish.dat".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "pufferfish".to_string(),
                    additional_triples: Vec::new(),
                }],
            )
            .unwrap();
        net.peer(0).flush_search_updates().unwrap();

        assert_eq!(
            net.peer(0)
                .search(
                    &auth,
                    SearchRequest {
                        query: "pufferfish",
                        limit: 10
                    }
                )
                .unwrap()
                .len(),
            1,
            "the child must be searchable before it is orphaned"
        );

        make_replicated_orphan(&mut net, &graph);

        assert_eq!(
            net.peer(0)
                .graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .len(),
            1,
            "the child must now be recorded as orphaned"
        );
        assert_eq!(
            net.peer(0)
                .search(
                    &auth,
                    SearchRequest {
                        query: "pufferfish",
                        limit: 10
                    }
                )
                .unwrap()
                .len(),
            0,
            "an orphaned entity must not remain searchable"
        );
    }

    /// Deferred append diagnostics must restore a reattached entity without manual reindexing.
    #[test]
    fn append_restores_index() {
        let (_dir, mut net) = setup_network(2);
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:append-requeue");
        let entity = || NewDataEntity {
            entity_id: "data/nautilus.dat".to_string(),
            entity_type: "http://schema.org/MediaObject".to_string(),
            name: "nautilus".to_string(),
            additional_triples: Vec::new(),
        };

        net.peer(0)
            .create_crate(
                &auth,
                CreateCrateRequest::new(
                    graph.clone(),
                    "append crate",
                    "description",
                    "2025-01-01",
                    None,
                    GraphPolicy {
                        public: true,
                        permission_paths: vec!["/t/x".to_string()],
                    },
                ),
            )
            .unwrap();
        net.peer(0)
            .append_new_root_data_entities(&auth, &graph, vec![entity()])
            .unwrap();

        net.peer(0).flush_search_updates().unwrap();
        assert_eq!(
            1,
            net.peer(0)
                .search(
                    &auth,
                    SearchRequest {
                        query: "nautilus",
                        limit: 10,
                    },
                )
                .unwrap()
                .len(),
            "the appended entity starts out searchable"
        );

        make_replicated_orphan(&mut net, &graph);
        let node = net.peer(0);
        let searchable = || {
            node.flush_search_updates().unwrap();
            node.search(
                &auth,
                SearchRequest {
                    query: "nautilus",
                    limit: 10,
                },
            )
            .unwrap()
            .len()
        };
        assert_eq!(0, searchable(), "the orphan must leave the index");

        // Re-append the same id. This is the only write, and it must settle
        // the orphan record it invalidates.
        node.append_new_root_data_entities(&auth, &graph, vec![entity()])
            .unwrap();

        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "re-appending must clear the orphan record"
        );
        assert_eq!(1, searchable(), "the re-appended entity must be findable");
    }

    /// Adding a link to an untouched orphan must also restore that orphan's search entry.
    #[test]
    fn append_adopts_orphan() {
        let (_dir, mut net) = setup_network(2);
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:append-adopt");

        net.peer(0)
            .create_crate(
                &auth,
                CreateCrateRequest::new(
                    graph.clone(),
                    "adopt crate",
                    "description",
                    "2025-01-01",
                    None,
                    GraphPolicy {
                        public: true,
                        permission_paths: vec!["/t/x".to_string()],
                    },
                ),
            )
            .unwrap();
        net.peer(0)
            .append_new_root_data_entities(
                &auth,
                &graph,
                vec![NewDataEntity {
                    entity_id: "data/pangolin.dat".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "pangolin".to_string(),
                    additional_triples: Vec::new(),
                }],
            )
            .unwrap();
        net.peer(0).flush_search_updates().unwrap();
        assert_eq!(
            1,
            net.peer(0)
                .search(
                    &auth,
                    SearchRequest {
                        query: "pangolin",
                        limit: 10,
                    },
                )
                .unwrap()
                .len(),
            "the entity starts out searchable"
        );

        make_replicated_orphan(&mut net, &graph);
        let node = net.peer(0);
        let searchable = || {
            node.flush_search_updates().unwrap();
            node.search(
                &auth,
                SearchRequest {
                    query: "pangolin",
                    limit: 10,
                },
            )
            .unwrap()
            .len()
        };
        assert_eq!(0, searchable(), "the orphan must leave the index");

        // Adopt the orphan from a brand-new sibling. The orphan itself is
        // never written, so the write cannot enqueue it.
        node.append_new_root_data_entities(
            &auth,
            &graph,
            vec![NewDataEntity {
                entity_id: "data/folder".to_string(),
                entity_type: "http://schema.org/Dataset".to_string(),
                name: "folder".to_string(),
                additional_triples: vec![(
                    oxrdf::NamedNode::new_unchecked("http://schema.org/hasPart"),
                    oxrdf::Term::NamedNode(oxrdf::NamedNode::new_unchecked("./data/pangolin.dat")),
                )],
            }],
        )
        .unwrap();

        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "adoption must clear the orphan record"
        );
        assert_eq!(1, searchable(), "the adopted entity must be findable");
    }

    #[test]
    fn reattach_restores_index() {
        let (_dir, mut net) = setup_network(2);
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:orphan-requeue-back");

        net.peer(0)
            .create_crate(
                &auth,
                CreateCrateRequest::new(
                    graph.clone(),
                    "requeue crate",
                    "description",
                    "2025-01-01",
                    None,
                    GraphPolicy {
                        public: true,
                        permission_paths: vec!["/t/x".to_string()],
                    },
                ),
            )
            .unwrap();
        net.peer(0)
            .append_new_root_data_entities(
                &auth,
                &graph,
                vec![NewDataEntity {
                    entity_id: "data/coelacanth.dat".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "coelacanth".to_string(),
                    additional_triples: Vec::new(),
                }],
            )
            .unwrap();

        let searchable = |node: &CraqleNode| {
            node.search(
                &auth,
                SearchRequest {
                    query: "coelacanth",
                    limit: 10,
                },
            )
            .unwrap()
            .len()
        };
        let settle = |node: &CraqleNode| {
            node.rebuild_graph_diagnostics(&graph).unwrap();
            node.flush_search_updates().unwrap();
        };

        settle(net.peer(0));
        assert_eq!(
            1,
            searchable(net.peer(0)),
            "the child starts out searchable"
        );

        let edge = make_replicated_orphan(&mut net, &graph);
        let node = net.peer(0);
        let link = |graph: &GraphId| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };
        settle(node);
        assert_eq!(0, searchable(node), "the orphan must leave the index");

        node.apply_changes(&AllowAllAuthorizer, &graph, vec![link(&graph)])
            .unwrap();
        settle(node);
        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "re-attaching must clear the orphan record"
        );
        assert_eq!(
            1,
            searchable(node),
            "a re-attached entity must come back to search without being touched"
        );
    }

    fn axolotl_hits(node: &CraqleNode) -> usize {
        node.search(
            &GrantAuthorizer::default(),
            SearchRequest {
                query: "axolotl",
                limit: 10,
            },
        )
        .unwrap()
        .len()
    }

    /// An unrelated write must not advance the diagnostic baseline past an unqueued relink.
    fn interleave_relink(net: &mut CraqleCluster, graph: &GraphId) {
        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "interleave crate",
                    "description",
                    "2025-01-01",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
        net.peer(0)
            .append_new_root_data_entities(
                &writer_auth(),
                graph,
                vec![NewDataEntity {
                    entity_id: "data/axolotl.dat".to_string(),
                    entity_type: "http://schema.org/MediaObject".to_string(),
                    name: "axolotl".to_string(),
                    additional_triples: Vec::new(),
                }],
            )
            .unwrap();
        net.peer(0).flush_search_updates().unwrap();
        assert_eq!(
            1,
            axolotl_hits(net.peer(0)),
            "the child starts out searchable"
        );

        let edge = make_replicated_orphan(net, graph);
        let link = MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };

        let node = net.peer(0);
        assert_eq!(0, axolotl_hits(node), "the orphan must leave the index");

        // Re-link without rebuilding, then commit something unrelated.
        node.apply_changes(&AllowAllAuthorizer, graph, vec![link])
            .unwrap();
        node.apply_changes(
            &AllowAllAuthorizer,
            graph,
            vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: edge.subject,
                predicate: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                    "http://schema.org/description",
                )),
                object: EncodedTerm::from_term(&oxrdf::Term::Literal(
                    oxrdf::Literal::new_simple_literal("an unrelated note"),
                ))
                .unwrap(),
            }],
        )
        .unwrap();

        node.rebuild_graph_diagnostics(graph).unwrap();
        node.flush_search_updates().unwrap();
    }

    /// A settle between a deferred bulk write and its rebuild must not advance
    /// the re-queue baseline past the re-link it happens to observe (G7).
    #[test]
    fn interleaved_relink_searchable() {
        let (_dir, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:interleaved-relink");

        interleave_relink(&mut net, &graph);
        let node = net.peer(0);

        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "re-linking must clear the orphan record"
        );
        assert_eq!(
            1,
            axolotl_hits(node),
            "the re-linked entity must come back to search"
        );
    }

    /// Restart is not a repair path for this: the persisted record is already
    /// correct and freshly clock-tagged, so a reopen finds nothing to fix.
    #[test]
    fn interleaved_relink_restart() {
        let (dir, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:interleaved-relink-restart");

        interleave_relink(&mut net, &graph);
        drop(net);

        let reopened = CraqleNode::open(dir.path().join("peer_0")).unwrap();
        reopened.flush_search_updates().unwrap();
        assert_eq!(
            1,
            axolotl_hits(&reopened),
            "the re-linked entity must still be searchable after a restart"
        );
    }

    #[test]
    fn search_options_stop() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:search-options");
        net.peer(0)
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph,
                    "Stoppable Study",
                    "stopneedle study",
                    "2025-03-01",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
        net.flush_search_updates().unwrap();
        let node = net.peer(0);
        let request = || SearchRequest {
            query: "stopneedle",
            limit: 10,
        };
        let run = |options: &SearchOptions| {
            node.search_with_options(
                &GrantAuthorizer::default(),
                SearchRun {
                    request: request(),
                    options,
                },
            )
            .map_err(|error| error.kind())
        };
        let plain = node.search(&GrantAuthorizer::default(), request()).unwrap();
        assert!(!plain.is_empty());
        assert_eq!(plain.len(), run(&SearchOptions::default()).unwrap().len());

        let cancelled = SearchOptions::default();
        cancelled.cancellation.cancel();
        assert_eq!(run(&cancelled).unwrap_err(), CraqleErrorKind::Cancelled);

        let expired = SearchOptions {
            timeout: Some(std::time::Duration::ZERO),
            ..SearchOptions::default()
        };
        assert_eq!(run(&expired).unwrap_err(), CraqleErrorKind::QueryLimit);
    }

    /// Creates one searchable crate per flush, so each lands in its own index segment.
    fn budget_node(graphs: &[(&str, bool)]) -> (tempfile::TempDir, CraqleNode) {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();
        for (graph, public) in graphs {
            let policy = if *public {
                public_policy()
            } else {
                GraphPolicy {
                    public: false,
                    permission_paths: Vec::new(),
                }
            };
            node.create_crate(
                &AllowAllAuthorizer,
                CreateCrateRequest::new(
                    GraphId::new(graph),
                    "Budget Study",
                    "budgetneedle",
                    "2025-03-01",
                    None,
                    policy,
                ),
            )
            .unwrap();
            node.flush_search_updates().unwrap();
        }
        (directory, node)
    }

    const ENTRIES: [&str; 3] = ["search", "graphs", "resources"];

    /// A labelled fixture of `(graph, public)` crates with one query and limit.
    type BudgetCase<'a> = (&'a str, &'a [(&'a str, bool)], &'a str, usize);

    /// One search call: its entry point, request, and options.
    struct BudgetCall<'a> {
        entry: &'static str,
        query: &'a str,
        limit: usize,
        options: &'a SearchOptions,
    }

    /// Runs one plain, graph-scoped, or hydrated search and counts its hits.
    fn budget_run(
        node: &CraqleNode,
        auth: &dyn Authorizer,
        call: BudgetCall<'_>,
    ) -> std::result::Result<usize, CraqleErrorKind> {
        let graphs: Vec<GraphId> = ["urn:test:budget:a", "urn:test:budget:b"]
            .into_iter()
            .map(GraphId::new)
            .collect();
        let request = SearchRequest {
            query: call.query,
            limit: call.limit,
        };
        let options = call.options;
        let hits = match call.entry {
            "search" => node
                .search_with_options(auth, SearchRun { request, options })
                .map(|hits| hits.len()),
            "graphs" => node
                .search_graphs_with(
                    auth,
                    GraphSearchRun {
                        request: GraphSearchRequest {
                            graphs: &graphs,
                            query: call.query,
                            limit: call.limit,
                        },
                        options,
                    },
                )
                .map(|hits| hits.len()),
            "resources" => node
                .search_resources_with(auth, SearchRun { request, options })
                .map(|hits| hits.len()),
            other => panic!("unknown search entry {other}"),
        };
        hits.map_err(|error| error.kind())
    }

    #[test]
    fn ended_budget_fails() {
        let cancelled = SearchOptions::default();
        cancelled.cancellation.cancel();
        let expired = SearchOptions {
            timeout: Some(std::time::Duration::ZERO),
            ..SearchOptions::default()
        };
        let cases: [BudgetCase<'_>; 5] = [
            ("empty index", &[], "budgetneedle", 10),
            (
                "no match",
                &[("urn:test:budget:a", true)],
                "absentneedle",
                10,
            ),
            (
                "all denied",
                &[("urn:test:budget:a", false)],
                "budgetneedle",
                10,
            ),
            (
                "matches",
                &[("urn:test:budget:a", true), ("urn:test:budget:b", true)],
                "budgetneedle",
                10,
            ),
            (
                "zero limit",
                &[("urn:test:budget:a", true)],
                "budgetneedle",
                0,
            ),
        ];
        let auth = GrantAuthorizer::default();
        for (label, graphs, query, limit) in cases {
            let (_directory, node) = budget_node(graphs);
            let open = SearchOptions::default();
            let expected = if label == "matches" { 2 } else { 0 };
            for (options, expected) in [
                (&open, Ok(expected)),
                (&cancelled, Err(CraqleErrorKind::Cancelled)),
                (&expired, Err(CraqleErrorKind::QueryLimit)),
            ] {
                for entry in ENTRIES {
                    let call = BudgetCall {
                        entry,
                        query,
                        limit,
                        options,
                    };
                    let result = budget_run(&node, &auth, call);
                    assert_eq!(expected, result, "{label} {entry}");
                }
            }
        }
    }

    #[test]
    fn late_cancel_fails() {
        let (_directory, node) = budget_node(&[("urn:test:budget:a", true)]);
        // Call 1 is candidate authorization, call 2 the final recheck, call 3 hydration.
        for (entry, cancel_on) in [
            ("search", 2),
            ("graphs", 2),
            ("resources", 2),
            ("resources", 3),
        ] {
            let options = SearchOptions::default();
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let auth = |graph: &GraphId, policy: &GraphPolicy, action: Action| {
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == cancel_on {
                    options.cancellation.cancel();
                }
                GrantAuthorizer::default().authorize(graph, policy, action)
            };
            let call = BudgetCall {
                entry,
                query: "budgetneedle",
                limit: 10,
                options: &options,
            };
            let result = budget_run(&node, &auth, call);
            assert_eq!(
                CraqleErrorKind::Cancelled,
                result.unwrap_err(),
                "{entry} cancelled on call {cancel_on}"
            );
        }
    }
}
