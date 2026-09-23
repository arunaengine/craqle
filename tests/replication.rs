//! Checks replica convergence, deletion, and rejected remote events.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

mod support;

#[cfg(test)]
mod tests {
    use craqle::*;
    use irokle::Event;
    use proptest::prelude::*;

    use crate::support::*;

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, irokle::Event)]
    #[irokle(type_id = "craqle.graph.v1")]
    struct PoisonEvent {
        junk: Vec<u64>,
    }

    #[test]
    fn deletion_replicates_peers() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-delete");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();
        assert!(net.peer(1).contains_graph(&graph).unwrap());

        net.peer(0).delete_graph_unchecked(&graph).unwrap();
        assert!(!net.peer(0).contains_graph(&graph).unwrap());
        net.sync_until_converged(10).unwrap();
        assert!(!net.peer(1).contains_graph(&graph).unwrap());
    }

    #[test]
    fn reused_ids_converge() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:reused-mutation-id");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();
        let topic = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
        net.partition(0, 1);
        let id = MutationId::new();
        let object = |index: usize| EncodedTerm(format!("\"peer {index}\""));
        for index in 0..2 {
            net.irokle(index)
                .open_topic::<CraqleGraphEvent>(topic)
                .unwrap()
                .publish(CraqleGraphEvent::Mutation {
                    id,
                    graph: graph.clone(),
                    changes: vec![MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: EncodedTerm("<urn:reused>".into()),
                        predicate: EncodedTerm("<urn:p>".into()),
                        object: object(index),
                    }],
                    render_hints: None,
                })
                .unwrap();
            // Each peer applies its own record first, so the two see opposite orders.
            net.peer(index).reconcile_irokle().unwrap();
        }
        net.heal(0, 1);
        net.sync_until_converged(10).unwrap();
        let snapshot = net.peer(0).graph_snapshot(&graph).unwrap();
        assert_eq!(snapshot, net.peer(1).graph_snapshot(&graph).unwrap());
        for index in 0..2 {
            assert!(
                snapshot
                    .quads
                    .iter()
                    .any(|quad| quad.object == object(index))
            );
            let rejected = net
                .peer(index)
                .list_rejected_replication_records(&AllowAllAuthorizer)
                .unwrap();
            assert!(rejected.is_empty());
        }
    }

    #[test]
    fn tagged_policy_convergence() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:tagged-policy-convergence");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        net.partition(0, 1);
        net.partition(0, 2);
        net.partition(1, 2);
        let policies = (0..3)
            .map(|index| GraphPolicy {
                public: index != 0,
                permission_paths: vec![format!("/tests/policy-{index}")],
            })
            .collect::<Vec<_>>();
        for (index, policy) in policies.iter().enumerate() {
            net.peer(index)
                .set_graph_policy(&writer_auth(), &graph, policy.clone())
                .unwrap();
        }
        let topic = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
        let expected = (0..3)
            .max_by_key(|index| {
                ActorId::from_bytes(
                    *irokle::actor_id_for(topic, net.irokle(*index).peer_id()).as_bytes(),
                )
            })
            .map(|index| policies[index].clone())
            .unwrap();

        net.heal(0, 1);
        net.heal(0, 2);
        net.heal(1, 2);
        net.sync_until_converged(10).unwrap();
        for index in 0..3 {
            assert_eq!(net.peer(index).graph_policy(&graph).unwrap(), expected);
        }
    }

    #[test]
    fn unauthorized_policy_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let net =
            CraqleCluster::new_with_options(2, tmp.path(), |_| CraqleOptions::default()).unwrap();
        let graph = GraphId::new("urn:test:unauthorized-remote-policy");
        create_test_crate(&net, 0, &graph);
        for _ in 0..3 {
            net.sync_round().unwrap();
        }

        let records = net
            .peer(1)
            .list_rejected_replication_records(&AllowAllAuthorizer)
            .unwrap();
        assert!(records.iter().any(|record| {
            record.graph.as_ref() == Some(&graph)
                && record.error_kind == CraqleErrorKind::Unauthorized
        }));
        assert_ne!(
            net.peer(1).graph_policy(&graph).unwrap(),
            net.peer(0).graph_policy(&graph).unwrap(),
            "default remote policy authority must deny the incoming policy"
        );
    }

    // Peers must adopt one shared genesis rather than mint competing graph topics.
    #[test]
    fn graph_genesis_converges() {
        use irokle::Storage as _;
        let (_tmp, net) = setup_network(3);
        let graph = GraphId::new("urn:test:graph-genesis-single-minter");

        let topic_id = net.peer(0).bind_or_derive_irokle_topic(&graph).unwrap();
        for idx in 0..net.peer_count() {
            assert_eq!(
                net.peer(idx).bind_or_derive_irokle_topic(&graph).unwrap(),
                topic_id
            );
            assert!(net.peer(idx).irokle_topic_id(&graph).unwrap().is_none());
            assert!(
                net.irokle(idx)
                    .storage()
                    .topic_state(&topic_id)
                    .unwrap()
                    .is_none()
            );
        }

        let members = (1..net.peer_count())
            .map(|idx| net.irokle(idx).peer_id())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            net.peer(0).mint_irokle_topic(&graph, members).unwrap(),
            topic_id
        );

        for idx in 1..net.peer_count() {
            assert_eq!(
                net.peer(idx).bind_or_derive_irokle_topic(&graph).unwrap(),
                topic_id
            );
            assert!(
                net.irokle(idx)
                    .storage()
                    .topic_state(&topic_id)
                    .unwrap()
                    .is_none()
            );
        }

        for _ in 0..5 {
            net.sync_round().unwrap();
        }

        let genesis = net
            .irokle(0)
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis;
        for idx in 0..net.peer_count() {
            let state = net
                .irokle(idx)
                .storage()
                .topic_state(&topic_id)
                .unwrap()
                .unwrap();
            assert_eq!(state.genesis, genesis);
            assert_eq!(
                net.peer(idx).bind_irokle_topic(&graph).unwrap(),
                Some(topic_id)
            );
        }
    }

    #[test]
    fn deletion_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let open = || {
            CraqleNode::open_with_options(
                dir.path(),
                CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
            )
            .unwrap()
        };
        let graph = GraphId::new("urn:test:crate-delete-reopen");

        let node = open();
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Doomed",
                "To be deleted",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();
        node.delete_graph_unchecked(&graph).unwrap();
        assert!(!node.contains_graph(&graph).unwrap());
        drop(node);

        let node = open();
        assert!(!node.contains_graph(&graph).unwrap());
    }

    #[test]
    fn poison_preserves_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:crate-poison");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Poisoned",
                "Has a bad record",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();

        let topic_id = node.irokle_topic_id(&graph).unwrap().unwrap();
        irokle
            .open_topic::<PoisonEvent>(topic_id)
            .unwrap()
            .publish(PoisonEvent { junk: vec![99; 99] })
            .unwrap();

        node.reconcile_irokle().unwrap();
        assert!(node.contains_graph(&graph).unwrap());
        let rejected = node
            .list_rejected_replication_records(&AllowAllAuthorizer)
            .unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(
            rejected[0].error_kind,
            CraqleErrorKind::CorruptAuthoritativeData
        );
        assert_eq!(
            rejected[0].reason,
            "malformed or poison graph-event payload"
        );
    }

    #[test]
    fn catchup_pages_bound() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:test:catchup-pages");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Paged",
                "Bounded replay",
                "2026-01-01",
                None,
                public_policy(),
            ),
        )
        .unwrap();
        node.reconcile_irokle().unwrap();

        let topic_id = node.irokle_topic_id(&graph).unwrap().unwrap();
        let topic = irokle.open_topic::<CraqleGraphEvent>(topic_id).unwrap();
        for _ in 0..1025 {
            topic
                .publish(CraqleGraphEvent::QuadChanges {
                    graph: graph.clone(),
                    changes: Vec::new(),
                })
                .unwrap();
        }

        assert!(node.reconcile_irokle().unwrap().contains(&graph));
        let before = irokle.storage().counters();
        assert!(node.reconcile_irokle().unwrap().is_empty());
        let after = irokle.storage().counters();
        assert_eq!(after.op_reads, before.op_reads);
    }

    #[test]
    fn lost_response_retries() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:mutation-receipt");
        create_test_crate(&net, 0, &graph);
        let before = net.peer(0).graph_snapshot(&graph).unwrap();
        let id = MutationId::new();
        let request = |admission_sequence| MutationRequest {
            id,
            admission_sequence,
            graph: graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: EncodedTerm("\"receipt-keyword\"".to_owned()),
            }],
        };

        let prepared = net
            .peer(0)
            .apply_mutation(&writer_auth(), request(None))
            .unwrap();
        assert_eq!(prepared.source, SourceOutcome::Prepared);
        let _ = net
            .peer(0)
            .apply_mutation(&writer_auth(), request(Some(prepared.admission_sequence)))
            .unwrap();
        let applied = net.peer(0).graph_snapshot(&graph).unwrap();
        let MutationStatus::Known(lost) = net
            .peer(0)
            .mutation_status(
                &writer_auth(),
                MutationLookup {
                    graph: graph.clone(),
                    id,
                    admission_sequence: Some(prepared.admission_sequence),
                },
            )
            .unwrap()
        else {
            panic!("lost response receipt must remain queryable");
        };
        let retried = net
            .peer(0)
            .apply_mutation(&writer_auth(), request(Some(prepared.admission_sequence)))
            .unwrap();
        let after_retry = net.peer(0).graph_snapshot(&graph).unwrap();
        assert_eq!(applied, after_retry, "retry must not add another dot");
        assert_ne!(before.clock, applied.clock);
        let keyword = applied
            .quads
            .iter()
            .find(|quad| quad.object.0 == "\"receipt-keyword\"")
            .expect("applied mutation must be visible");
        assert_eq!(keyword.dots.len(), 1);
        assert!(lost.event_id.is_some());
        assert_eq!(retried.event_id, lost.event_id);
        assert_eq!(retried.source_version, lost.source_version);
        let MutationStatus::Known(status) = net
            .peer(0)
            .mutation_status(
                &writer_auth(),
                MutationLookup {
                    graph,
                    id,
                    admission_sequence: Some(prepared.admission_sequence),
                },
            )
            .unwrap()
        else {
            panic!("retry receipt must remain queryable");
        };
        assert_eq!(status.event_id, retried.event_id);
        assert_eq!(status.source_version, retried.source_version);
    }

    #[test]
    fn receipt_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:receipt-reopen");
        let id = MutationId::new();
        let (receipt, sequence) = {
            let node = CraqleNode::open(directory.path()).unwrap();
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Receipt reopen",
                    "Persist mutation status",
                    "2026-09-20",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
            let request = |admission_sequence| MutationRequest {
                id,
                admission_sequence,
                graph: graph.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: literal_term("durable receipt"),
                }],
            };
            let prepared = node.apply_mutation(&writer_auth(), request(None)).unwrap();
            let sequence = Some(prepared.admission_sequence);
            let receipt = node
                .apply_mutation(&writer_auth(), request(sequence))
                .unwrap();
            (receipt, sequence)
        };

        let node = CraqleNode::open(directory.path()).unwrap();
        let MutationStatus::Known(reopened) = node
            .mutation_status(
                &writer_auth(),
                MutationLookup {
                    graph: graph.clone(),
                    id,
                    admission_sequence: sequence,
                },
            )
            .unwrap()
        else {
            panic!("persisted receipt must survive reopen");
        };
        assert_eq!(reopened.id, receipt.id);
        assert_eq!(reopened.admission_sequence, receipt.admission_sequence);
        assert_eq!(reopened.event_id, receipt.event_id);
        assert_eq!(reopened.source_version, receipt.source_version);
        assert_eq!(reopened.repair_graphs, vec![graph]);
        assert!(!matches!(
            reopened.persistence,
            PersistenceOutcome::Pending | PersistenceOutcome::Unknown
        ));
        assert!(
            reopened.repairs.diagnostics != RepairOutcome::NotRequired
                || reopened.repairs.search != RepairOutcome::NotRequired
                || reopened.repairs.query_view != RepairOutcome::NotRequired
        );
    }

    #[test]
    fn rejection_skips_receipt() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:mutation-rejected");
        create_test_crate(&net, 0, &graph);

        let denied_id = MutationId::new();
        let denied = MutationRequest {
            id: denied_id,
            admission_sequence: None,
            graph: graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: literal_term("denied"),
            }],
        };
        assert_eq!(
            net.peer(0)
                .apply_mutation(&DenyAllAuthorizer, denied)
                .unwrap_err()
                .kind(),
            CraqleErrorKind::Unauthorized
        );
        assert!(matches!(
            net.peer(0)
                .mutation_status(
                    &writer_auth(),
                    MutationLookup {
                        graph: graph.clone(),
                        id: denied_id,
                        admission_sequence: None,
                    },
                )
                .unwrap(),
            MutationStatus::Unknown
        ));

        let malformed_id = MutationId::new();
        let foreign = GraphId::new("urn:test:mutation-foreign");
        let malformed = MutationRequest {
            id: malformed_id,
            admission_sequence: None,
            graph: graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: foreign,
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: literal_term("malformed"),
            }],
        };
        assert_eq!(
            net.peer(0)
                .apply_mutation(&writer_auth(), malformed)
                .unwrap_err()
                .kind(),
            CraqleErrorKind::InvalidInput
        );
        assert!(matches!(
            net.peer(0)
                .mutation_status(
                    &writer_auth(),
                    MutationLookup {
                        graph,
                        id: malformed_id,
                        admission_sequence: None,
                    },
                )
                .unwrap(),
            MutationStatus::Unknown
        ));
    }

    /// A peer's undecodable event after the prepare frontier must not block the retry.
    #[test]
    fn peer_poison_retries() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:mutation-peer-poison");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();
        let request = |admission_sequence| MutationRequest {
            id: MutationId::new(),
            admission_sequence,
            graph: graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                object: literal_term("after poison"),
            }],
        };
        let mut retry = request(None);
        let prepared = net
            .peer(0)
            .apply_mutation(&writer_auth(), retry.clone())
            .unwrap();
        assert_eq!(prepared.source, SourceOutcome::Prepared);

        let topic = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
        net.irokle(1)
            .open_topic::<PoisonEvent>(topic)
            .unwrap()
            .publish(PoisonEvent { junk: vec![7; 7] })
            .unwrap();
        net.sync_until_converged(10).unwrap();

        retry.admission_sequence = Some(prepared.admission_sequence);
        let applied = net.peer(0).apply_mutation(&writer_auth(), retry).unwrap();
        assert_eq!(applied.source, SourceOutcome::Applied);
    }

    #[test]
    fn outbound_hides_repairs() {
        let graph = GraphId::new("urn:test:receipt-visible");
        let hidden = GraphId::new("urn:test:receipt-hidden");
        let receipt = MutationReceipt {
            id: MutationId::new(),
            admission_sequence: 5,
            graph: graph.clone(),
            request_digest: [1; 32],
            event_id: Some([2; 32]),
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: Some(8),
            repair_graphs: vec![graph.clone(), hidden.clone()],
            source: SourceOutcome::Applied,
            persistence: PersistenceOutcome::DataSynced,
            repairs: RepairState {
                diagnostics: RepairOutcome::Complete,
                shacl: RepairOutcome::Pending,
                search: RepairOutcome::Complete,
                query_view: RepairOutcome::Complete,
            },
            source_version: [3; 32],
            updated_unix_nanos: 13,
        };
        let merge_receipt = receipt.clone();
        let error: CraqleError = UpdateError::Accepted {
            receipt: Box::new(receipt),
            error_kind: CraqleErrorKind::Storage,
            reason: "injected post-commit failure".to_owned(),
        }
        .into();
        let CraqleError::Update(UpdateError::Accepted { receipt, .. }) = error else {
            panic!("accepted mutation error must retain its receipt");
        };

        assert_eq!(receipt.repair_graphs, vec![graph.clone()]);
        assert!(!format!("{receipt:?}").contains(hidden.as_str()));
        assert!(
            !serde_json::to_string(&receipt)
                .unwrap()
                .contains(hidden.as_str())
        );

        let error: CraqleError = MergeError::Accepted {
            receipt: Box::new(merge_receipt),
            error_kind: CraqleErrorKind::Storage,
            reason: "injected merge settlement failure".to_owned(),
        }
        .into();
        let CraqleError::Merge(MergeError::Accepted { receipt, .. }) = error else {
            panic!("accepted merge error must retain its receipt");
        };
        assert_eq!(receipt.repair_graphs, vec![graph]);
        assert!(!format!("{receipt:?}").contains(hidden.as_str()));
    }

    #[test]
    fn repair_dry_run() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:repair-dry-run");
        create_test_crate(&net, 0, &graph);
        let snapshot = net.peer(0).graph_snapshot(&graph).unwrap();
        let digest = *blake3::hash(&postcard::to_allocvec(&snapshot).unwrap()).as_bytes();
        let report = net
            .peer(0)
            .reconcile_graph(
                &AllowAllAuthorizer,
                ReconcileRequest {
                    id: MutationId::new(),
                    graph,
                    mode: RepairMode::DryRun,
                    source: ReconcileSource::HealthySnapshot {
                        source: "test fixture".to_owned(),
                        snapshot,
                        digest,
                    },
                },
            )
            .unwrap();
        assert_eq!(report.audit.result, RepairResult::Exact);
        assert!(report.diff.is_none());
    }

    #[test]
    fn repair_restores_source() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:repair-apply");
        let healthy = {
            let node = CraqleNode::open(directory.path()).unwrap();
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Healthy source",
                    "Repair fixture",
                    "2026-09-20",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
            let snapshot = node.graph_snapshot(&graph).unwrap();
            node.apply_changes_unchecked(
                &graph,
                vec![MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: literal_term("corrupt local source"),
                }],
            )
            .unwrap();
            let digest = *blake3::hash(&postcard::to_allocvec(&snapshot).unwrap()).as_bytes();
            let report = node
                .reconcile_graph(
                    &writer_auth(),
                    ReconcileRequest {
                        id: MutationId::new(),
                        graph: graph.clone(),
                        mode: RepairMode::Apply,
                        source: ReconcileSource::HealthySnapshot {
                            source: "verified fixture".to_owned(),
                            snapshot: snapshot.clone(),
                            digest,
                        },
                    },
                )
                .unwrap();
            assert_eq!(report.audit.result, RepairResult::Applied);
            assert!(report.audit.backup.is_some());
            assert_eq!(node.graph_snapshot(&graph).unwrap(), snapshot);
            snapshot
        };

        let reopened = CraqleNode::open(directory.path()).unwrap();
        assert_eq!(reopened.graph_snapshot(&graph).unwrap(), healthy);
    }

    /// A repair that keeps the clock must not leave diagnostics from the replaced quads.
    #[test]
    fn repair_refreshes_diagnostics() {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();
        let graph = GraphId::new("urn:test:repair-diagnostics");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Linked source",
                "Repair fixture",
                "2026-09-20",
                None,
                public_policy(),
            ),
        )
        .unwrap();
        let part =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:repair-part"));
        let link = EncodedTerm::from_named_node(&vocab::schema_has_part());
        node.apply_changes_unchecked(
            &graph,
            vec![
                MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: link.clone(),
                    object: part.clone(),
                },
                MaterializedQuadChange::Insert {
                    graph: graph.clone(),
                    subject: part.clone(),
                    predicate: EncodedTerm::from_named_node(&vocab::rdf_type()),
                    object: EncodedTerm::from_named_node(&vocab::schema_dataset()),
                },
            ],
        )
        .unwrap();
        assert!(
            node.graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty()
        );
        let mut healthy = node.graph_snapshot(&graph).unwrap();
        healthy
            .quads
            .retain(|quad| !(quad.predicate == link && quad.object == part));
        let digest = *blake3::hash(&postcard::to_allocvec(&healthy).unwrap()).as_bytes();

        let report = node
            .reconcile_graph(
                &writer_auth(),
                ReconcileRequest {
                    id: MutationId::new(),
                    graph: graph.clone(),
                    mode: RepairMode::Apply,
                    source: ReconcileSource::HealthySnapshot {
                        source: "same clock fixture".to_owned(),
                        snapshot: healthy.clone(),
                        digest,
                    },
                },
            )
            .unwrap();

        assert_eq!(report.audit.result, RepairResult::Applied);
        assert_eq!(node.graph_snapshot(&graph).unwrap().clock, healthy.clock);
        assert_eq!(
            node.graph_diagnostics(&graph).unwrap().orphaned_entities,
            vec!["urn:test:repair-part".to_owned()]
        );
    }

    #[test]
    fn repair_keeps_tombstone() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:repair-tombstone");
        let snapshot = {
            let node = CraqleNode::open(directory.path()).unwrap();
            node.create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "Deleted source",
                    "Tombstone fixture",
                    "2026-09-20",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
            let snapshot = node.graph_snapshot(&graph).unwrap();
            node.delete_graph_unchecked(&graph).unwrap();
            assert!(!node.contains_graph(&graph).unwrap());
            snapshot
        };

        let node = CraqleNode::open(directory.path()).unwrap();
        let digest = *blake3::hash(&postcard::to_allocvec(&snapshot).unwrap()).as_bytes();
        let request = |mode| ReconcileRequest {
            id: MutationId::new(),
            graph: graph.clone(),
            mode,
            source: ReconcileSource::HealthySnapshot {
                source: "stale healthy copy".to_owned(),
                snapshot: snapshot.clone(),
                digest,
            },
        };
        let dry_run = node
            .reconcile_graph(&writer_auth(), request(RepairMode::DryRun))
            .unwrap();
        assert_eq!(dry_run.audit.result, RepairResult::Tombstoned);
        assert!(dry_run.audit.after_digest.is_none());
        assert!(dry_run.audit.backup.is_none());

        let denied = node
            .reconcile_graph(&DenyAllAuthorizer, request(RepairMode::Apply))
            .unwrap_err();
        assert_eq!(denied.kind(), CraqleErrorKind::Unauthorized);

        let error = node
            .reconcile_graph(&writer_auth(), request(RepairMode::Apply))
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
        assert!(matches!(
            error,
            CraqleError::Merge(MergeError::InputRejected(reason))
                if reason == "live authoritative snapshot cannot replace a permanently deleted graph"
        ));
        assert!(!node.contains_graph(&graph).unwrap());
        drop(node);
        let reopened = CraqleNode::open(directory.path()).unwrap();
        assert!(!reopened.contains_graph(&graph).unwrap());
    }

    #[test]
    fn graph_injection_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let irokle = irokle::Irokle::builder().build().unwrap();
        let node = CraqleNode::open_with_options(
            dir.path(),
            CraqleOptions::new().with_irokle(irokle.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph_a = GraphId::new("urn:test:crate-bound");
        let graph_b = GraphId::new("urn:test:crate-injected");
        node.create_crate(
            &writer_auth(),
            CreateCrateRequest::new(
                graph_a.clone(),
                "Bound",
                "Legit graph",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            ),
        )
        .unwrap();

        let topic_id = node.irokle_topic_id(&graph_a).unwrap().unwrap();
        irokle
            .open_topic::<CraqleGraphEvent>(topic_id)
            .unwrap()
            .publish(CraqleGraphEvent::QuadChanges {
                graph: graph_b.clone(),
                changes: vec![MaterializedQuadChange::Insert {
                    graph: graph_b.clone(),
                    subject: EncodedTerm::from_named_node(&graph_b.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    object: literal_term("injected"),
                }],
            })
            .unwrap();

        node.reconcile_irokle().unwrap();
        assert!(!node.contains_graph(&graph_b).unwrap());
        assert!(node.contains_graph(&graph_a).unwrap());
    }

    #[test]
    fn durable_write_unforked() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-topic-fork");
        create_test_crate(&net, 0, &graph);
        let topic_a = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();

        // Deliver A's irokle ops to B without letting B's craqle layer apply
        // them, so B still has no graph->topic binding when it writes.
        let summary = net.irokle(1).sync_summary(topic_a).unwrap();
        let data = net
            .irokle(0)
            .plan_sync_data(net.irokle(1).peer_id(), &summary)
            .unwrap();
        let ack = net
            .irokle(1)
            .receive_sync_data_from(net.irokle(0).peer_id(), data)
            .unwrap();
        let _ = net.irokle(0).apply_sync_ack(&ack.0);
        assert!(net.peer(1).irokle_topic_id(&graph).unwrap().is_none());

        create_test_crate(&net, 1, &graph);
        let topic_b = net.peer(1).irokle_topic_id(&graph).unwrap().unwrap();
        assert_eq!(topic_a, topic_b, "both nodes must agree on the graph topic");

        net.sync_until_converged(10).unwrap();
        let state0 = graph_state(&net, 0, &graph);
        assert_eq!(state0, graph_state(&net, 1, &graph));
        assert!(!state0.is_empty());

        keyword_insert(net.peer(1), &graph, "post-fork-keyword");
        net.sync_until_converged(10).unwrap();
        assert!(
            graph_state(&net, 0, &graph)
                .iter()
                .any(|(_, _, object)| object.contains("post-fork-keyword")),
            "B-side change must replicate to A over the shared topic"
        );

        for peer in 0..2 {
            let topics: Vec<_> = net
                .irokle(peer)
                .list_topics()
                .unwrap()
                .into_iter()
                .filter(|topic| topic.event_type_id == CraqleGraphEvent::TYPE_ID)
                .collect();
            assert_eq!(topics.len(), 1, "no second topic may exist for the graph");
            assert_eq!(topics[0].topic_id, topic_a);
        }
    }

    #[test]
    fn actor_writes_deterministic() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let node_a = CraqleNode::open(dir_a.path()).unwrap();
        let node_b = CraqleNode::open(dir_b.path()).unwrap();
        let graph = GraphId::new("urn:test:crate-deterministic");
        let actor = ActorId::from_bytes([7u8; 32]);
        let request = || {
            CreateCrateRequest::new(
                graph.clone(),
                "Det",
                "Deterministic materialization",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public_policy(),
            )
        };

        for node in [&node_a, &node_b] {
            node.create_crate_with_durability_as(
                &writer_auth(),
                request(),
                CraqleRequestDurability::WalAlreadyDurable,
                Some(actor),
            )
            .unwrap();
        }

        let normalize = |snapshot: GraphReplicaSnapshot| {
            let mut quads = snapshot.quads;
            quads.sort_by(|a, b| {
                (&a.subject.0, &a.predicate.0, &a.object.0).cmp(&(
                    &b.subject.0,
                    &b.predicate.0,
                    &b.object.0,
                ))
            });
            (snapshot.clock, quads)
        };
        assert_eq!(
            normalize(node_a.graph_snapshot(&graph).unwrap()),
            normalize(node_b.graph_snapshot(&graph).unwrap()),
        );
    }

    #[test]
    fn single_peer_creation() {
        let (_tmp, net) = setup_network(1);
        let graph = GraphId::new("urn:test:crate1");
        create_test_crate(&net, 0, &graph);

        // Verify the crate was created
        assert!(net.peer(0).contains_graph(&graph).unwrap());
    }

    #[test]
    fn additions_converge() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create crate on peer 0, sync to peer 1
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Both peers add different entities (offline)
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/file_a.csv",
            "http://schema.org/MediaObject",
            "File A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/file_b.csv",
            "http://schema.org/MediaObject",
            "File B",
            vec![],
        )
        .unwrap();

        // Sync
        net.sync_until_converged(10).unwrap();

        // Both peers should have both entities
        let f0 = net.peer(0).vector_clock(&graph).unwrap();
        let f1 = net.peer(1).vector_clock(&graph).unwrap();
        assert_eq!(f0, f1, "vector clocks must match after convergence");
    }

    #[test]
    fn three_peers_converge() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate1");

        // Create on peer 0, sync to all
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        // Partition: isolate peer 2
        net.partition(0, 2);
        net.partition(1, 2);

        // Peer 0 and 1 make changes
        let mgr0 = manager(net.peer(0));
        mgr0.add_data_entity(
            &graph,
            "data/entity_a.csv",
            "http://schema.org/MediaObject",
            "Entity A",
            vec![],
        )
        .unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "data/entity_b.csv",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();

        // Peer 2 (isolated) makes a change
        let mgr2 = manager(net.peer(2));
        mgr2.add_data_entity(
            &graph,
            "data/entity_c.csv",
            "http://schema.org/MediaObject",
            "Entity C",
            vec![],
        )
        .unwrap();

        // Sync 0 and 1
        net.sync_until_converged(10).unwrap();

        // Heal partition
        net.heal(0, 2);
        net.heal(1, 2);

        // Full sync
        net.sync_until_converged(10).unwrap();

        // All three peers must converge
        let f0 = net.peer(0).vector_clock(&graph).unwrap();
        let f1 = net.peer(1).vector_clock(&graph).unwrap();
        let f2 = net.peer(2).vector_clock(&graph).unwrap();
        assert_eq!(f0, f1);
        assert_eq!(f1, f2);
    }

    #[test]
    fn batch_replay_idempotent() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        // Create and sync
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let f_before = net.peer(1).vector_clock(&graph).unwrap();

        // Sync again (duplicate delivery)
        net.sync_until_converged(10).unwrap();

        let f_after = net.peer(1).vector_clock(&graph).unwrap();
        assert_eq!(f_before, f_after, "duplicate sync must be idempotent");
    }

    #[test]
    fn concurrent_values_survive() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate1");

        let mgr = manager(net.peer(0));
        mgr.create_crate(
            graph.clone(),
            "Original",
            "Concurrent title updates",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        let update0 = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:name \"Original\" }} }} INSERT {{ GRAPH <{}> {{ ?root schema:name \"Peer 0 Title\" }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name \"Original\" . }} }}",
            graph.as_str(),
            graph.as_str(),
            graph.as_str()
        );
        let update1 = format!(
            "DELETE {{ GRAPH <{}> {{ ?root schema:name \"Original\" }} }} INSERT {{ GRAPH <{}> {{ ?root schema:name \"Peer 1 Title\" }} }} WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name \"Original\" . }} }}",
            graph.as_str(),
            graph.as_str(),
            graph.as_str()
        );
        let mut update_options = UpdateOptions::default();
        update_options.limits = UpdateLimits::unbounded();

        net.peer_mut(0)
            .apply_sparql_update_with_options(&writer_auth(), &update0, &update_options)
            .unwrap();
        net.peer_mut(1)
            .apply_sparql_update_with_options(&writer_auth(), &update1, &update_options)
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let query = format!(
            "SELECT ?name WHERE {{ GRAPH <{}> {{ ?root rdf:type schema:Dataset . ?root schema:name ?name . }} }}",
            graph.as_str()
        );
        let results = net
            .peer(0)
            .query(&GrantAuthorizer::default(), &query)
            .unwrap();
        let mut names: Vec<String> = solution_rows(results)
            .iter()
            .map(|binding| binding_literal(binding.get("name").unwrap()))
            .collect();
        names.sort();

        assert_eq!(names, vec!["Peer 0 Title", "Peer 1 Title"]);
    }

    #[test]
    fn metadata_edits_converge() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-metadata");

        let mgr0 = manager(net.peer(0));
        mgr0.create_crate(
            graph.clone(),
            "Original Dataset",
            "Original description",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
        )
        .unwrap();
        net.sync_until_converged(10).unwrap();

        mgr0.update_property(
            &graph,
            graph.as_str(),
            "schema:name",
            None,
            "Updated Dataset v2",
        )
        .unwrap();
        let mgr1 = manager(net.peer(1));
        mgr1.update_property(
            &graph,
            graph.as_str(),
            "schema:description",
            None,
            "Improved description with more detail",
        )
        .unwrap();

        net.sync_until_converged(10).unwrap();

        let exported = mgr1.export_jsonld(&graph).unwrap();
        assert!(exported.contains("Updated Dataset v2"));
        assert!(exported.contains("Improved description with more detail"));
        assert!(violation_messages(&net, 0, &graph).is_empty());
        assert!(violation_messages(&net, 1, &graph).is_empty());

        // Search needs a real tantivy index; the rest of the scenario does not.
        #[cfg(feature = "search")]
        {
            assert!(!reindex_and_search(&net, 0, "updated").is_empty());
            assert!(!reindex_and_search(&net, 1, "improved").is_empty());
        }
    }

    #[test]
    fn concurrent_entities_converge() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-entities");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        let mgr0 = manager(net.peer(0));
        let mgr1 = manager(net.peer(1));
        mgr0.add_data_entity(
            &graph,
            "results.csv",
            "http://schema.org/MediaObject",
            "Results CSV",
            vec![],
        )
        .unwrap();
        mgr1.add_data_entity(
            &graph,
            "analysis.py",
            "http://schema.org/MediaObject",
            "Analysis Script",
            vec![],
        )
        .unwrap();

        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(state.iter().any(|(s, _, _)| s.contains("results.csv")));
        assert!(state.iter().any(|(s, _, _)| s.contains("analysis.py")));
        assert!(
            state
                .iter()
                .any(|(_, p, o)| p.contains("hasPart") && o.contains("results.csv"))
        );
        assert!(
            state
                .iter()
                .any(|(_, p, o)| p.contains("hasPart") && o.contains("analysis.py"))
        );
        assert!(violation_messages(&net, 0, &graph).is_empty());
    }

    #[test]
    fn observed_removal_converges() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-observed-remove");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        keyword_insert(net.peer(0), &graph, "observed-keyword");
        net.sync_until_converged(10).unwrap();
        keyword_delete(net.peer(1), &graph, "observed-keyword");
        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(
            !state
                .iter()
                .any(|(_, _, object)| object.contains("observed-keyword"))
        );
        assert_eq!(state, graph_state(&net, 1, &graph));
    }

    #[test]
    fn concurrent_addition_wins() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-add-wins");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        keyword_insert(net.peer(0), &graph, "race-keyword");
        keyword_delete(net.peer(1), &graph, "race-keyword");
        net.sync_until_converged(10).unwrap();

        let state = graph_state(&net, 0, &graph);
        assert!(
            state
                .iter()
                .any(|(_, _, object)| object.contains("race-keyword"))
        );
        assert_eq!(state, graph_state(&net, 1, &graph));
    }

    #[test]
    fn reordered_events_converge() {
        let (_tmp, mut net) = setup_network(2);
        let graph = GraphId::new("urn:test:crate-out-of-order");
        create_test_crate(&net, 0, &graph);
        net.sync_until_converged(10).unwrap();

        net.peer_mut(0)
            .insert_quads(
                &AllowAllAuthorizer,
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-one"),
                )],
            )
            .unwrap();
        net.peer_mut(0)
            .insert_quads(
                &AllowAllAuthorizer,
                &graph,
                vec![(
                    EncodedTerm::from_named_node(&graph.0),
                    EncodedTerm::from_named_node(&vocab::schema_keywords()),
                    literal_term("kw-two"),
                )],
            )
            .unwrap();

        let topic_id = net.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
        let summary = net.irokle(1).sync_summary(topic_id).unwrap();
        let mut data = net
            .irokle(0)
            .plan_sync_data(net.irokle(1).peer_id(), &summary)
            .unwrap();
        assert_eq!(data.ops.len(), 2);
        data.ops.reverse();

        let ack = net
            .irokle(1)
            .receive_sync_data_from(net.irokle(0).peer_id(), data)
            .unwrap();
        let _ = net.irokle(0).apply_sync_ack(&ack.0);
        net.peer(1).reconcile_irokle().unwrap();

        assert_eq!(graph_state(&net, 0, &graph), graph_state(&net, 1, &graph));
    }

    #[test]
    fn partitioned_peers_converge() {
        let (_tmp, mut net) = setup_network(3);
        let graph = GraphId::new("urn:test:crate-partition");
        manager(net.peer(0))
            .create_crate(
                graph.clone(),
                "Partitioned Crate",
                "Original description",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
            )
            .unwrap();
        net.sync_until_converged(10).unwrap();

        net.partition(0, 2);
        net.partition(1, 2);

        manager(net.peer(0))
            .add_data_entity(
                &graph,
                "entity-a.txt",
                "http://schema.org/MediaObject",
                "Entity A",
                vec![],
            )
            .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr1 = manager(net.peer(1));
        mgr1.add_data_entity(
            &graph,
            "entity-b.txt",
            "http://schema.org/MediaObject",
            "Entity B",
            vec![],
        )
        .unwrap();
        net.sync_pair(0, 1).unwrap();

        let mgr2 = manager(net.peer(2));
        mgr2.add_data_entity(
            &graph,
            "entity-c.txt",
            "http://schema.org/MediaObject",
            "Entity C",
            vec![],
        )
        .unwrap();
        mgr2.update_property(
            &graph,
            graph.as_str(),
            "schema:description",
            None,
            "Updated by isolated peer",
        )
        .unwrap();

        net.heal(0, 2);
        net.heal(1, 2);
        net.sync_until_converged(20).unwrap();

        for peer in 0..3 {
            let exported = manager(net.peer(peer)).export_jsonld(&graph).unwrap();
            assert!(exported.contains("Entity A"));
            assert!(exported.contains("Entity B"));
            assert!(exported.contains("Entity C"));
            assert!(exported.contains("Updated by isolated peer"));
        }
    }

    #[derive(Debug, Clone)]
    enum RandomOp {
        Add { peer: usize, keyword: u8 },
        Remove { peer: usize, keyword: u8 },
        SyncAll,
    }

    fn random_op_strategy() -> impl Strategy<Value = RandomOp> {
        prop_oneof![
            (0usize..3, 0u8..6).prop_map(|(peer, keyword)| RandomOp::Add { peer, keyword }),
            (0usize..3, 0u8..6).prop_map(|(peer, keyword)| RandomOp::Remove { peer, keyword }),
            Just(RandomOp::SyncAll),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn generated_operations_converge(ops in prop::collection::vec(random_op_strategy(), 1..40)) {
            let (_tmp, net) = setup_network(3);
            let graph = GraphId::new("urn:test:crate-proptest");
            create_test_crate(&net, 0, &graph);
            net.sync_until_converged(10).unwrap();

            for op in ops {
                match op {
                    RandomOp::Add { peer, keyword } => {
                        keyword_insert(net.peer(peer), &graph, &format!("kw-{keyword}"));
                    }
                    RandomOp::Remove { peer, keyword } => {
                        keyword_delete(net.peer(peer), &graph, &format!("kw-{keyword}"));
                    }
                    RandomOp::SyncAll => net.sync_until_converged(20).unwrap(),
                }
            }

            net.sync_until_converged(100).unwrap();

            let state0 = graph_state(&net, 0, &graph);
            let clock0 = net.peer(0).vector_clock(&graph).unwrap();
            for peer in 1..3 {
                prop_assert_eq!(state0.clone(), graph_state(&net, peer, &graph));
                prop_assert_eq!(clock0.clone(), net.peer(peer).vector_clock(&graph).unwrap());
            }
        }
    }

    #[test]
    fn custom_context_replicates() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:ctx-replicate");
        let organism_iri = "https://w3id.org/aruna/profiles/proteomics#organism";
        let document = format!(
            r#"{{
                "@context": [
                    "https://w3id.org/ro/crate/1.2/context",
                    {{"organism": "{organism_iri}"}}
                ],
                "@graph": [
                    {{
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
                        "about": {{"@id": "{graph}"}}
                    }},
                    {{
                        "@id": "{graph}",
                        "@type": "Dataset",
                        "name": "Replicated Context Crate",
                        "description": "Custom context should replicate",
                        "datePublished": "2025-01-01",
                        "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}},
                        "organism": "Homo sapiens"
                    }}
                ]
            }}"#,
            graph = graph.as_str()
        );

        manager(net.peer(0))
            .import_jsonld(graph.clone(), &document)
            .unwrap();
        net.sync_until_converged(10).unwrap();

        let exported_a: serde_json::Value =
            serde_json::from_str(&manager(net.peer(0)).export_jsonld(&graph).unwrap()).unwrap();
        let exported_b: serde_json::Value =
            serde_json::from_str(&manager(net.peer(1)).export_jsonld(&graph).unwrap()).unwrap();

        // The receiving peer reproduces the exact custom context, not the default.
        assert!(
            exported_b["@context"].is_array(),
            "peer B should export the custom array context, got {}",
            exported_b["@context"]
        );
        assert_eq!(exported_a["@context"], exported_b["@context"]);

        // And peer B compacts the custom predicate using the replicated context.
        let root_b = exported_b["@graph"]
            .as_array()
            .expect("@graph array")
            .iter()
            .find(|entry| entry["@id"] == serde_json::json!(graph.as_str()))
            .expect("root entity present");
        assert_eq!(root_b["organism"], serde_json::json!("Homo sapiens"));
    }

    /// Keep RDF quads identical so only the custom-context register can conflict.
    fn context_crate(graph: &GraphId, context: &serde_json::Value) -> String {
        serde_json::json!({
            "@context": context,
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Concurrent Context Crate",
                    "description": "Same content, different context",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                    "hasPart": [{"@id": "./data/file1.txt"}]
                },
                {
                    "@id": "./data/file1.txt",
                    "@type": "File",
                    "name": "Measurement File"
                }
            ]
        })
        .to_string()
    }

    #[test]
    fn concurrent_contexts_converge() {
        let (_tmp, net) = setup_network(2);
        let graph = GraphId::new("urn:test:ctx-concurrent");
        let default_context = serde_json::json!("https://w3id.org/ro/crate/1.2/context");

        // The custom terms are unused, isolating context convergence from quad convergence.
        let context_a = serde_json::json!([
            "https://w3id.org/ro/crate/1.2/context",
            {"organism": "https://example.org/profiles/a#organism"}
        ]);
        let context_b = serde_json::json!([
            "https://w3id.org/ro/crate/1.2/context",
            {"assayType": "https://example.org/profiles/b#assayType"}
        ]);

        // Synchronize the single genesis before partitioned context writes.
        manager(net.peer(0))
            .import_jsonld(graph.clone(), &context_crate(&graph, &default_context))
            .unwrap();
        net.sync_until_converged(10).unwrap();

        // Equal counters force the actor identifier to resolve concurrent context tags.
        manager(net.peer(0))
            .import_jsonld(graph.clone(), &context_crate(&graph, &context_a))
            .unwrap();
        manager(net.peer(1))
            .import_jsonld(graph.clone(), &context_crate(&graph, &context_b))
            .unwrap();

        net.sync_until_converged(10).unwrap();

        let exported_a: serde_json::Value =
            serde_json::from_str(&manager(net.peer(0)).export_jsonld(&graph).unwrap()).unwrap();
        let exported_b: serde_json::Value =
            serde_json::from_str(&manager(net.peer(1)).export_jsonld(&graph).unwrap()).unwrap();

        // Convergence: both peers export the SAME context (not swapped, not each
        // keeping their own).
        assert_eq!(
            exported_a["@context"], exported_b["@context"],
            "peers must converge on one context, got A={} B={}",
            exported_a["@context"], exported_b["@context"]
        );

        // The winner is deterministic and independent of arrival order: both tags
        // have counter 1 (first write on each peer), so the larger actor id wins.
        let winner_context = if net.peer(0).actor() >= net.peer(1).actor() {
            &context_a
        } else {
            &context_b
        };
        assert_eq!(
            exported_a["@context"], *winner_context,
            "converged context must be the deterministic last-write-wins winner"
        );
        // And it is exactly one of the two imported contexts, never a merge.
        assert!(
            exported_a["@context"] == context_a || exported_a["@context"] == context_b,
            "converged context must be one of the two imported contexts"
        );
    }
}
