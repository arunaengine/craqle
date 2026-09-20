//! Exercises mixed maintenance, replication convergence, and durable reopen.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

mod support;

#[cfg(all(test, feature = "search"))]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread;

    use craqle::*;
    use irokle::Event;

    use crate::support::*;

    type WorkerResult = std::result::Result<(), CraqleErrorKind>;
    type TestResult<T> = std::result::Result<T, Box<CraqleError>>;

    fn node_options(irokle: irokle::Irokle, peers: BTreeSet<irokle::PeerId>) -> CraqleOptions {
        CraqleOptions::new()
            .with_search_storage(SearchStorage::Memory)
            .with_remote_policy_authorizer(Arc::new(|_: &GraphId, _: &ActorId, _: &GraphPolicy| {
                true
            }))
            .with_irokle(irokle, CraqleIrokleOptions::new().with_initial_peers(peers))
    }

    fn sync_nodes(
        source: &irokle::Irokle,
        target: &irokle::Irokle,
        receiver: &CraqleNode,
    ) -> std::result::Result<usize, irokle::Error> {
        let mut moved = 0usize;
        for topic in source.list_topics()? {
            if topic.event_type_id != CraqleGraphEvent::TYPE_ID {
                continue;
            }
            let summary = target.sync_summary(topic.topic_id)?;
            let data = source.plan_sync_data(target.peer_id(), &summary)?;
            moved += data.ops.len();
            if data.ops.is_empty() {
                continue;
            }
            let ack = target.receive_sync_data_from(source.peer_id(), data)?;
            let _ = source.apply_sync_ack(&ack.0);
            receiver
                .reconcile_irokle()
                .map_err(|error| irokle::Error::Storage(error.to_string()))?;
        }
        Ok(moved)
    }

    fn query_values(node: &CraqleNode, graph: &GraphId) -> TestResult<Vec<String>> {
        let query = format!(
            "SELECT ?value WHERE {{ GRAPH <{}> {{ <{}> <http://schema.org/keywords> ?value }} }} ORDER BY ?value",
            graph.as_str(),
            graph.as_str()
        );
        let mut options = QueryOptions::default();
        options.limits = QueryLimits::unbounded();
        let results = node
            .query_with_options(
                &writer_auth(),
                QueryRequest {
                    sparql: &query,
                    options: &options,
                },
            )?
            .results;
        Ok(solution_rows(results)
            .iter()
            .map(|row| binding_literal(row.get("value").unwrap()))
            .collect())
    }

    fn search_keys(node: &CraqleNode) -> Vec<(String, String)> {
        let mut hits = node
            .search(
                &writer_auth(),
                SearchRequest {
                    query: "mixed-token",
                    limit: 32,
                },
            )
            .unwrap()
            .into_iter()
            .map(|hit| (hit.graph_id, hit.subject_iri))
            .collect::<Vec<_>>();
        hits.sort();
        hits
    }

    fn accepted_worker(result: WorkerResult) {
        if let Err(kind) = result {
            assert!(matches!(
                kind,
                CraqleErrorKind::Conflict
                    | CraqleErrorKind::Storage
                    | CraqleErrorKind::DependencyUnavailable
                    | CraqleErrorKind::QueryLimit
            ));
        }
    }

    #[test]
    fn mixed_restart_converges() {
        let root = tempfile::tempdir().unwrap();
        let irokle_a = irokle::Irokle::builder().build().unwrap();
        let irokle_b = irokle::Irokle::builder().build().unwrap();
        let peers_a = BTreeSet::from([irokle_b.peer_id()]);
        let peers_b = BTreeSet::from([irokle_a.peer_id()]);
        let path_a = root.path().join("peer-a");
        let path_b = root.path().join("peer-b");
        let node_a = Arc::new(
            CraqleNode::open_with_options(&path_a, node_options(irokle_a.clone(), peers_a.clone()))
                .unwrap(),
        );
        let node_b = Arc::new(
            CraqleNode::open_with_options(&path_b, node_options(irokle_b.clone(), peers_b))
                .unwrap(),
        );
        let graph = GraphId::new("urn:test:system-recovery");
        node_a
            .create_crate(
                &writer_auth(),
                CreateCrateRequest::new(
                    graph.clone(),
                    "mixed-token-base",
                    "mixed maintenance fixture",
                    "2026-09-20",
                    None,
                    public_policy(),
                ),
            )
            .unwrap();
        sync_nodes(&irokle_a, &irokle_b, &node_b).unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let (done_tx, done_rx) = mpsc::channel::<WorkerResult>();
        let (receipt_tx, receipt_rx) = mpsc::channel();

        let writer_node = Arc::clone(&node_a);
        let writer_graph = graph.clone();
        let writer_barrier = Arc::clone(&barrier);
        let writer_done = done_tx.clone();
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            let outcome = (|| -> TestResult<Vec<MutationReceipt>> {
                let mut receipts = Vec::new();
                for index in 0..4u8 {
                    let id = MutationId([index + 1; 32]);
                    let request = |sequence| MutationRequest {
                        id,
                        admission_sequence: sequence,
                        graph: writer_graph.clone(),
                        changes: vec![MaterializedQuadChange::Insert {
                            graph: writer_graph.clone(),
                            subject: EncodedTerm::from_named_node(&writer_graph.0),
                            predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
                            object: EncodedTerm(format!("\"mixed-token-{index}\"")),
                        }],
                    };
                    let prepared = writer_node.apply_mutation(&writer_auth(), request(None))?;
                    let applied = writer_node.apply_mutation(
                        &writer_auth(),
                        request(Some(prepared.admission_sequence)),
                    )?;
                    receipts.push(applied);
                }
                Ok(receipts)
            })();
            match outcome {
                Ok(receipts) => {
                    receipt_tx.send(receipts).unwrap();
                    writer_done.send(Ok(())).unwrap();
                }
                Err(error) => {
                    receipt_tx.send(Vec::new()).unwrap();
                    writer_done.send(Err(error.kind())).unwrap();
                }
            }
        });

        let reader_node = Arc::clone(&node_a);
        let reader_graph = graph.clone();
        let reader_barrier = Arc::clone(&barrier);
        let reader_done = done_tx.clone();
        let reader = thread::spawn(move || {
            reader_barrier.wait();
            let outcome = (|| -> TestResult<()> {
                for _ in 0..8 {
                    let _ = reader_node.graph_snapshot(&reader_graph)?;
                    let _ = query_values(&reader_node, &reader_graph)?;
                }
                Ok(())
            })();
            reader_done
                .send(outcome.map_err(|error| error.kind()))
                .unwrap();
        });

        let maintenance_node = Arc::clone(&node_a);
        let maintenance_barrier = Arc::clone(&barrier);
        let maintenance_done = done_tx.clone();
        let maintenance = thread::spawn(move || {
            maintenance_barrier.wait();
            let outcome = (|| -> TestResult<()> {
                maintenance_node.rebuild_query_indexes()?;
                maintenance_node.flush_search(&SearchFlushOptions::default())?;
                Ok(())
            })();
            maintenance_done
                .send(outcome.map_err(|error| error.kind()))
                .unwrap();
        });

        drop(done_tx);
        barrier.wait();
        for _ in 0..3 {
            accepted_worker(done_rx.recv_timeout(WATCHDOG_TIMEOUT).unwrap());
        }
        writer.join().unwrap();
        reader.join().unwrap();
        maintenance.join().unwrap();
        let receipts = receipt_rx.recv_timeout(WATCHDOG_TIMEOUT).unwrap();
        assert_eq!(receipts.len(), 4);

        node_a.rebuild_query_indexes().unwrap();
        node_a.reindex_search().unwrap();
        let search_receipt = node_a.flush_search(&SearchFlushOptions::default()).unwrap();
        assert!(search_receipt.covered >= search_receipt.target);

        for _ in 0..10 {
            let moved = sync_nodes(&irokle_a, &irokle_b, &node_b).unwrap()
                + sync_nodes(&irokle_b, &irokle_a, &node_a).unwrap();
            if moved == 0
                && node_a.graph_snapshot(&graph).unwrap() == node_b.graph_snapshot(&graph).unwrap()
            {
                break;
            }
        }
        node_b.rebuild_query_indexes().unwrap();
        node_b.reindex_search().unwrap();
        node_b.flush_search(&SearchFlushOptions::default()).unwrap();

        let snapshot = node_a.graph_snapshot(&graph).unwrap();
        assert_eq!(snapshot, node_b.graph_snapshot(&graph).unwrap());
        let values = query_values(&node_a, &graph).unwrap();
        assert_eq!(
            values,
            vec![
                "mixed-token-0",
                "mixed-token-1",
                "mixed-token-2",
                "mixed-token-3",
            ]
        );
        assert_eq!(values, query_values(&node_b, &graph).unwrap());
        let search = search_keys(&node_a);
        assert!(!search.is_empty());
        assert_eq!(search, search_keys(&node_b));
        let query_status = node_a.query_index_status_fast().unwrap();
        let receipt_states = receipts
            .iter()
            .map(|receipt| {
                node_a
                    .mutation_status(
                        &writer_auth(),
                        MutationLookup {
                            graph: graph.clone(),
                            id: receipt.id,
                            admission_sequence: Some(receipt.admission_sequence),
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();

        drop(node_b);
        drop(node_a);
        let reopened =
            CraqleNode::open_with_options(&path_a, node_options(irokle_a, peers_a)).unwrap();
        reopened.reindex_search().unwrap();
        let reopened_search = reopened
            .flush_search(&SearchFlushOptions::default())
            .unwrap();
        assert!(reopened_search.covered >= reopened_search.target);
        assert_eq!(reopened.graph_snapshot(&graph).unwrap(), snapshot);
        assert_eq!(query_values(&reopened, &graph).unwrap(), values);
        assert_eq!(reopened.query_index_status_fast().unwrap(), query_status);
        assert_eq!(search_keys(&reopened), search);
        for (receipt, expected) in receipts.iter().zip(receipt_states) {
            assert_eq!(
                reopened
                    .mutation_status(
                        &writer_auth(),
                        MutationLookup {
                            graph: graph.clone(),
                            id: receipt.id,
                            admission_sequence: Some(receipt.admission_sequence),
                        },
                    )
                    .unwrap(),
                expected
            );
        }
    }
}
