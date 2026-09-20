//! Measures private store lock and mutation receipt contracts for B14 and B17.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Instant;

use serde_json::{Value, json};

use super::*;
use crate::sync::{
    MutationId, MutationReceipt, PersistenceOutcome, RepairOutcome, RepairState, SourceOutcome,
};

fn bench_case() -> Value {
    serde_json::from_str(&std::env::var("CRAQLE_BENCH_CASE").unwrap()).unwrap()
}

fn receipt(id: usize, terminal: bool) -> MutationReceipt {
    let mut mutation_id = [0u8; 32];
    mutation_id[..8].copy_from_slice(&(id as u64).to_be_bytes());
    let mut request_digest = mutation_id;
    request_digest[31] = request_digest[31].wrapping_add(1);
    let graph = GraphId::new("urn:bench:receipt");
    MutationReceipt {
        id: MutationId(mutation_id),
        admission_sequence: id as u64,
        graph: graph.clone(),
        request_digest,
        event_id: None,
        topic: None,
        publish_after: None,
        topic_epoch: None,
        topic_genesis: None,
        search_token: None,
        repair_graphs: vec![graph],
        source: if terminal {
            SourceOutcome::Applied
        } else {
            SourceOutcome::Prepared
        },
        persistence: if terminal {
            PersistenceOutcome::FullySynced
        } else {
            PersistenceOutcome::Pending
        },
        repairs: RepairState {
            diagnostics: if terminal {
                RepairOutcome::Complete
            } else {
                RepairOutcome::Pending
            },
            shacl: RepairOutcome::NotRequired,
            search: if terminal {
                RepairOutcome::Complete
            } else {
                RepairOutcome::Pending
            },
            query_view: if terminal {
                RepairOutcome::Complete
            } else {
                RepairOutcome::Pending
            },
        },
        source_version: mutation_id.map(|byte| byte.wrapping_add(2)),
        updated_unix_nanos: id as i64,
    }
}

fn stage_receipt(store: &GraphStore, value: &MutationReceipt) -> Option<MutationReceipt> {
    let mut batch = store.new_batch();
    let existing = store.stage_receipt(&mut batch, value).unwrap();
    if existing.is_none() {
        store.commit(batch).unwrap();
    }
    existing
}

fn lock_shard(store: &GraphStore, graph: &GraphId) -> usize {
    let hash = blake3::hash(graph.as_str().as_bytes());
    let value = u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()) as usize;
    value % store.write_locks.len()
}

fn colliding_graph(store: &GraphStore, first: &GraphId) -> GraphId {
    let target = lock_shard(store, first);
    (0usize..)
        .map(|index| GraphId::new(&format!("urn:bench:collision:{index}")))
        .find(|graph| graph != first && lock_shard(store, graph) == target)
        .unwrap()
}

fn distinct_graph(store: &GraphStore, first: &GraphId) -> GraphId {
    let target = lock_shard(store, first);
    (0usize..)
        .map(|index| GraphId::new(&format!("urn:bench:distinct:{index}")))
        .find(|graph| lock_shard(store, graph) != target)
        .unwrap()
}

#[test]
#[ignore = "release-only B14 private lock workload"]
fn catalog_b14() {
    let case = bench_case();
    let stores = case["stores"].as_u64().unwrap_or(1) as usize;
    let root = tempfile::tempdir().unwrap();
    let primary = Arc::new(GraphStore::open(root.path().join("primary")).unwrap());
    let graph = GraphId::new("urn:bench:shared-lock");
    let collision_mode = case["collision"].as_bool().unwrap_or(true);
    let collision = if collision_mode {
        colliding_graph(&primary, &graph)
    } else {
        distinct_graph(&primary, &graph)
    };
    let held = primary.graph_write_guard(&graph);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = Arc::clone(&primary);
    let collision_worker = collision.clone();
    let blocked = std::thread::spawn(move || {
        entered_tx.send(()).unwrap();
        let started = Instant::now();
        let _guard = worker.graph_write_guard(&collision_worker);
        done_tx.send(started.elapsed().as_nanos()).unwrap();
    });
    entered_rx.recv().unwrap();

    let started = Instant::now();
    let mut independent_ns = Vec::new();
    for index in 0..stores.max(1) {
        let store = GraphStore::open(root.path().join(format!("independent-{index}"))).unwrap();
        let acquired = Instant::now();
        let _guard = store.graph_write_guard(&graph);
        independent_ns.push(acquired.elapsed().as_nanos());
    }
    let blocked_ns = if collision_mode {
        assert!(
            done_rx.try_recv().is_err(),
            "a colliding shard acquired while its guard was held"
        );
        drop(held);
        done_rx.recv().unwrap()
    } else {
        let elapsed = done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        drop(held);
        elapsed
    };
    blocked.join().unwrap();

    #[cfg(feature = "shacl-core")]
    let shacl = {
        let mode = case["shacl"].as_str().unwrap_or("none");
        match mode {
            "advisory" => {
                let _binding = primary.binding_guard();
            }
            "enforcing" => {
                let _binding = primary.binding_guard();
                let _commit = primary.graph_commit_guard(&graph);
            }
            "none" => {}
            value => panic!("unknown SHACL mode {value}"),
        }
        primary.shacl_runtime_statistics()
    };
    #[cfg(feature = "shacl-core")]
    let lock_metrics = (
        shacl.graph_commit_lock_wait_ns,
        shacl.binding_lock_wait_ns,
        shacl.binding_lock_hold_ns,
    );
    #[cfg(not(feature = "shacl-core"))]
    let lock_metrics = (0, 0, 0);
    println!(
        "{}",
        json!({
            "benchmark_id": "B14",
            "status": "measured",
            "case": case,
            "result": {
                "completed": stores.max(1),
                "collision_shard": lock_shard(&primary, &collision),
                "blocked_ns": blocked_ns,
                "independent_ns": independent_ns,
                "work_ns": started.elapsed().as_nanos(),
                "lock_wait_ns": lock_metrics.0,
                "binding_wait_ns": lock_metrics.1,
                "binding_hold_ns": lock_metrics.2,
            }
        })
    );
}

#[test]
#[ignore = "release-only B17 private receipt workload"]
fn catalog_b17() {
    let case = bench_case();
    let mode = case["receipt"].as_str().unwrap_or("pending");
    let root = tempfile::tempdir().unwrap();
    let store = GraphStore::open(root.path()).unwrap();
    let before_work = store.receipt_work();
    let started = Instant::now();
    let first = receipt(1, mode == "expired");
    stage_receipt(&store, &first);
    let mut duplicates = 0usize;
    let mut expired = false;
    match mode {
        "retry" | "lost-reply" => {
            duplicates += usize::from(stage_receipt(&store, &first).is_some());
        }
        "diagnostics-failure" => {
            let mut failed = first.clone();
            failed.source = SourceOutcome::Applied;
            failed.repairs.diagnostics = RepairOutcome::Failed(crate::CraqleErrorKind::Storage);
            store.update_receipt(&failed).unwrap();
        }
        "expired" => {
            for id in 2..=RECEIPT_RETENTION + 2 {
                stage_receipt(&store, &receipt(id, true));
            }
            expired = store.mutation_receipt(&first.id).unwrap().is_none();
        }
        "pending" => {}
        value => panic!("unknown receipt mode {value}"),
    }
    store.persist_receipts().unwrap();
    let receipt_work = store.receipt_work();
    let elapsed = started.elapsed();
    drop(store);
    let reopened = GraphStore::open(root.path()).unwrap();
    let reopened_receipt = reopened.mutation_receipt(&first.id).unwrap();
    let present = reopened_receipt.is_some();
    if mode == "expired" {
        assert!(
            expired && !present,
            "expired receipt remained authoritative"
        );
    } else {
        assert!(present, "live receipt did not survive restart");
    }
    let failed_repair = reopened_receipt
        .as_ref()
        .is_some_and(|value| matches!(value.repairs.diagnostics, RepairOutcome::Failed(_)));
    let output_digest = blake3::hash(format!("{reopened_receipt:?}").as_bytes())
        .to_hex()
        .to_string();
    if mode == "diagnostics-failure" {
        assert!(failed_repair, "failed repair outcome was not durable");
    }
    println!(
        "{}",
        json!({
            "benchmark_id": "B17",
            "status": "measured",
            "case": case,
            "result": {
                "completed": 1,
                "duplicates_avoided": duplicates,
                "expired": expired,
                "present_after_restart": present,
                "failed_repair": failed_repair,
                "output_digest": output_digest,
                "receipt_lookups": receipt_work.lookups - before_work.lookups,
                "receipt_writes": receipt_work.writes - before_work.writes,
                "persist_calls": receipt_work.persists - before_work.persists,
                "work_ns": elapsed.as_nanos(),
            }
        })
    );
}
