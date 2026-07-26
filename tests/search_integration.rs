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
    fn test_search_after_concurrent_edits_scenario() {
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
    fn test_sparql_integrated_fts_uses_tantivy_hits() {
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

        let rows = solution_rows(
            net.peer(1)
                .query(&GrantAuthorizer::default(), &query)
                .unwrap(),
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
                benchmark_media_object_entities(
                    0,
                    50,
                    "remote-batch-keyword",
                    "Remote Batch Entity",
                    "remote batch record",
                    "RBATCH",
                ),
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
        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Replacement Search Dataset",
                "Only replacement text should be searchable",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();
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
            !node
                .search(
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

    /// G8 completeness: an authorized caller must never be shown a short page
    /// while matching, readable documents exist.
    ///
    /// Tantivy collects a global top-k by score, so unreadable graphs can fill
    /// the whole over-fetch window and starve the authorization filter. With a
    /// fixed `limit * 4` over-fetch this returned 21 hits for `limit = 25` and
    /// 41 for `limit = 50` (finding K2).
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

    /// G7: `flush_search_updates()` must terminate even while a writer keeps
    /// enqueueing, and everything enqueued *before* the call must be indexed
    /// when it returns.
    ///
    /// The unbounded drain re-read work enqueued between drain and
    /// acknowledgement, so the flush could spin forever (finding W15b).
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
            node.flush_search_updates().unwrap();

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

    /// An entity that becomes orphaned by a write that never touches it must leave
    /// the search index (G6, G7).
    ///
    /// Deleting `root hasPart child` orphans the child without naming it in the
    /// change set. The diagnostics settle then has to notice that the orphan set
    /// moved and re-queue the child for indexing — otherwise search keeps returning
    /// an entity that export and SPARQL now hide, and nothing repairs it until some
    /// unrelated write happens to dirty that subject.
    #[test]
    fn orphan_leaves_index() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:orphan-requeue");

        node.create_crate(
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
        node.append_new_root_data_entities(
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
        node.rebuild_graph_diagnostics(&graph).unwrap();
        node.flush_search_updates().unwrap();

        assert_eq!(
            node.search(
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

        // Cut the only edge to the child, naming just the edge — never the child.
        let has_part = "<http://schema.org/hasPart>";
        let edge = node
            .graph_snapshot(&graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.predicate.0 == has_part)
            .expect("root must link the child");
        node.apply_changes_bulk_unchecked(
            &graph,
            vec![MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: edge.subject,
                predicate: edge.predicate,
                object: edge.object,
            }],
        )
        .unwrap();
        node.rebuild_graph_diagnostics(&graph).unwrap();
        node.flush_search_updates().unwrap();

        assert_eq!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .len(),
            1,
            "the child must now be recorded as orphaned"
        );
        assert_eq!(
            node.search(
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

    /// The other direction: re-attaching an orphan must put it *back* in the
    /// search index (G6, G7).
    ///
    /// The re-queue diffs the orphan set with `symmetric_difference`, so both
    /// transitions have to be covered. With a one-sided `difference` the hiding
    /// direction still passes and this one does not: the child is un-orphaned
    /// everywhere except in search, where it stays invisible until some
    /// unrelated write happens to dirty it.
    /// Re-attaching through the public append API, with no manual rebuild.
    /// The append path defers diagnostics, so nothing else settles the record
    /// and the entity has to come back to search on the write alone (G7).
    #[test]
    fn append_restores_index() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:append-requeue");
        let entity = || NewDataEntity {
            entity_id: "data/nautilus.dat".to_string(),
            entity_type: "http://schema.org/MediaObject".to_string(),
            name: "nautilus".to_string(),
            additional_triples: Vec::new(),
        };

        node.create_crate(
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
        node.append_new_root_data_entities(&auth, &graph, vec![entity()])
            .unwrap();

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

        assert_eq!(1, searchable(), "the appended entity starts out searchable");

        let edge = node
            .graph_snapshot(&graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.predicate.0 == "<http://schema.org/hasPart>")
            .expect("root must link the child");
        node.apply_changes_bulk_unchecked(
            &graph,
            vec![MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: edge.subject.clone(),
                predicate: edge.predicate.clone(),
                object: edge.object.clone(),
            }],
        )
        .unwrap();
        node.rebuild_graph_diagnostics(&graph).unwrap();
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

    /// An append can adopt an entity it never touches: `additional_triples`
    /// carries a `hasPart` edge to an existing orphan. Nothing enqueues that
    /// entity, so only a settled orphan record can return it to search (G7).
    #[test]
    fn append_adopts_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:append-adopt");

        node.create_crate(
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
        node.append_new_root_data_entities(
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

        assert_eq!(1, searchable(), "the entity starts out searchable");

        let edge = node
            .graph_snapshot(&graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.predicate.0 == "<http://schema.org/hasPart>")
            .expect("root must link the child");
        node.apply_changes_bulk_unchecked(
            &graph,
            vec![MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: edge.subject.clone(),
                predicate: edge.predicate.clone(),
                object: edge.object.clone(),
            }],
        )
        .unwrap();
        node.rebuild_graph_diagnostics(&graph).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let auth =
            GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)]);
        let graph = GraphId::new("urn:test:orphan-requeue-back");

        node.create_crate(
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
        node.append_new_root_data_entities(
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

        settle(&node);
        assert_eq!(1, searchable(&node), "the child starts out searchable");

        // The only edge to the child. Cut it, then restore it — naming the edge
        // both times and the child neither time.
        let edge = node
            .graph_snapshot(&graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.predicate.0 == "<http://schema.org/hasPart>")
            .expect("root must link the child");
        let link = |graph: &GraphId| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };
        let unlink = |graph: &GraphId| MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };

        node.apply_changes_bulk_unchecked(&graph, vec![unlink(&graph)])
            .unwrap();
        settle(&node);
        assert_eq!(0, searchable(&node), "the orphan must leave the index");

        node.apply_changes_bulk_unchecked(&graph, vec![link(&graph)])
            .unwrap();
        settle(&node);
        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "re-attaching must clear the orphan record"
        );
        assert_eq!(
            1,
            searchable(&node),
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

    /// Orphan one data entity, re-link it with a deferred bulk write, then land
    /// an unrelated commit before the diagnostics rebuild.
    ///
    /// The unrelated write touches neither `rdf:type` nor `hasPart`, so the
    /// settle takes its re-stamp path — and re-stamping a read that already
    /// reflects the bulk write would move the re-queue baseline past the
    /// re-link without queueing it.
    fn interleave_relink(node: &CraqleNode, graph: &GraphId) {
        node.create_crate(
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
        node.append_new_root_data_entities(
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
        node.rebuild_graph_diagnostics(graph).unwrap();
        node.flush_search_updates().unwrap();
        assert_eq!(1, axolotl_hits(node), "the child starts out searchable");

        let edge = node
            .graph_snapshot(graph)
            .unwrap()
            .quads
            .into_iter()
            .find(|quad| quad.predicate.0 == "<http://schema.org/hasPart>")
            .expect("root must link the child");
        let unlink = MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };
        let link = MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: edge.subject.clone(),
            predicate: edge.predicate.clone(),
            object: edge.object.clone(),
        };

        node.apply_changes_bulk_unchecked(graph, vec![unlink])
            .unwrap();
        node.rebuild_graph_diagnostics(graph).unwrap();
        node.flush_search_updates().unwrap();
        assert_eq!(0, axolotl_hits(node), "the orphan must leave the index");

        // Re-link without rebuilding, then commit something unrelated.
        node.apply_changes_bulk_unchecked(graph, vec![link])
            .unwrap();
        node.apply_changes_unchecked(
            graph,
            vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: edge.subject,
                predicate: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                    "http://schema.org/description",
                )),
                object: EncodedTerm::from_term(&oxrdf::Term::Literal(
                    oxrdf::Literal::new_simple_literal("an unrelated note"),
                )),
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
        let dir = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:interleaved-relink");

        interleave_relink(&node, &graph);

        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "re-linking must clear the orphan record"
        );
        assert_eq!(
            1,
            axolotl_hits(&node),
            "the re-linked entity must come back to search"
        );
    }

    /// Restart is not a repair path for this: the persisted record is already
    /// correct and freshly clock-tagged, so a reopen finds nothing to fix.
    #[test]
    fn interleaved_relink_restart() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:interleaved-relink-restart");

        {
            let node = CraqleNode::open(dir.path()).unwrap();
            interleave_relink(&node, &graph);
        }

        let reopened = CraqleNode::open(dir.path()).unwrap();
        reopened.flush_search_updates().unwrap();
        assert_eq!(
            1,
            axolotl_hits(&reopened),
            "the re-linked entity must still be searchable after a restart"
        );
    }
}
