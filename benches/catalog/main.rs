//! Runs one capacity-bounded B01-B20 workload case selected by the dispatcher.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[path = "case.rs"]
mod catalog_case;
#[path = "reads.rs"]
mod catalog_reads;

use craqle::{
    Action, ActorId, AllowAllAuthorizer, AuthorizationError, CraqleFjallPersistMode as PersistMode,
    CraqleNode, CraqleOptions, EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange,
    SearchRequest,
};
use serde_json::{Value, json};

use catalog_case::{
    CAP_ENV, CASE_ENV, Case, ID_ENV, case_seed, check_values, estimate_bytes, merge_actor,
    supported_axes, work_rows,
};
use catalog_reads::{run_cache, run_reads};

fn main() {
    let id = std::env::var(ID_ENV).expect("CRAQLE_BENCH_ID is required");
    let case = Case(serde_json::from_str(&std::env::var(CASE_ENV).unwrap()).unwrap());
    let byte_cap = std::env::var(CAP_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128 * 1024 * 1024usize);
    let unsupported = case.unsupported(supported_axes(&id));
    if let Err(reason) = check_values(&id, &case).and(if unsupported.is_empty() {
        Ok(())
    } else {
        Err(format!("axes not implemented: {}", unsupported.join(", ")))
    }) {
        println!(
            "{}",
            json!({
                "benchmark_id": id,
                "status": "unsupported",
                "reason": reason,
                "case": case.0,
            })
        );
        return;
    }
    let estimate = estimate_bytes(&case);
    if estimate > byte_cap {
        println!(
            "{}",
            json!({
                "benchmark_id": id,
                "status": "capacity_blocked",
                "estimated_bytes": estimate,
                "byte_cap": byte_cap,
                "case": case.0,
            })
        );
        return;
    }

    let started = Instant::now();
    let result = run_case(&id, &case);
    println!(
        "{}",
        json!({
            "benchmark_id": id,
            "status": "measured",
            "elapsed_ns": started.elapsed().as_nanos(),
            "estimated_bytes": estimate,
            "case": case.0,
            "result": result,
        })
    );
}

fn run_case(id: &str, case: &Case) -> Value {
    let directory = tempfile::tempdir().unwrap();
    match id {
        "B01" | "B04" => run_writes(directory.path(), case),
        "B02" | "B12" | "B13" => run_reads(directory.path(), case),
        "B15" => run_cache(directory.path(), case),
        "B03" | "B19" => run_rebuild(directory.path(), case),
        "B05" | "B06" => run_merge(directory.path(), case),
        "B07" | "B08" | "B09" | "B10" | "B11" => run_search(directory.path(), case),
        "B14" | "B16" => run_stores(directory.path(), case),
        "B17" => run_retries(directory.path(), case),
        "B18" => run_ingress(directory.path(), case),
        "B20" => run_mixed(directory.path(), case),
        _ => panic!("unknown benchmark id {id}"),
    }
}

fn persist_mode(case: &Case) -> PersistMode {
    match case.text("persist", "sync-all") {
        "buffer" => PersistMode::Buffer,
        "sync-data" => PersistMode::SyncData,
        "sync-all" => PersistMode::SyncAll,
        mode => panic!("unknown persist mode {mode}"),
    }
}

fn open_node(path: &Path, case: &Case) -> CraqleNode {
    open_actor(path, case, [0x43; 32])
}

fn open_actor(path: &Path, case: &Case, actor: [u8; 32]) -> CraqleNode {
    CraqleNode::open_with_options(
        path,
        CraqleOptions::new()
            .with_actor(ActorId::from_bytes(actor))
            .with_graph_store_persist_mode(persist_mode(case)),
    )
    .unwrap()
}

fn graph_id(index: usize) -> GraphId {
    GraphId::new(&format!("urn:bench:catalog:{index}"))
}

fn graph_policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/bench/**".to_string()],
    }
}

fn term(value: impl Into<String>) -> EncodedTerm {
    EncodedTerm(value.into())
}

fn change(graph: &GraphId, index: usize, salt: usize) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: term(format!("<urn:catalog:s:{salt}:{index}>")),
        predicate: term("<urn:catalog:p>"),
        object: term(format!("\"catalog token {salt} {index}\"")),
    }
}

fn sized_change(graph: &GraphId, index: usize, bytes: usize) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: term(format!("<urn:catalog:wide:{index}>")),
        predicate: term("<urn:catalog:p>"),
        object: term(format!("\"{}\"", "x".repeat(bytes))),
    }
}

fn deletion(graph: &GraphId, index: usize, salt: usize) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: term(format!("<urn:catalog:s:{salt}:{index}>")),
        predicate: term("<urn:catalog:p>"),
        object: term(format!("\"catalog token {salt} {index}\"")),
    }
}

fn prepare_node(node: &CraqleNode, case: &Case) -> usize {
    let graphs = case.usize("graphs", 1).max(1);
    let rows = work_rows(case);
    let seed = case_seed(case);
    for index in 0..graphs {
        node.set_graph_policy(&AllowAllAuthorizer, &graph_id(index), graph_policy())
            .unwrap();
    }
    for graph_index in 0..graphs {
        let indexes: Vec<_> = (graph_index..rows).step_by(graphs).collect();
        for chunk in indexes.chunks(1_000) {
            let graph = graph_id(graph_index);
            let changes = chunk
                .iter()
                .map(|index| change(&graph, *index, seed))
                .collect();
            node.apply_changes(&AllowAllAuthorizer, &graph, changes)
                .unwrap();
        }
    }
    rows
}

fn settle(node: &CraqleNode, case: &Case) {
    node.persist_fjall().unwrap();
    node.ensure_query_indexes();
    if case.bool("search", true) {
        node.flush_search_updates().unwrap();
    }
}

fn run_writes(path: &Path, case: &Case) -> Value {
    let node = Arc::new(open_node(path, case));
    let preloaded = prepare_node(&node, case);
    let writers = case.usize("writers", 1).max(1);
    let changed = case.usize("changed", 1_000).max(writers);
    let graphs = case.usize("graphs", 1).max(1);
    let related = case.bool("related", true);
    let write_graphs = if related { graphs } else { 1 };
    let graph_base = if related { 0 } else { graphs };
    if !related {
        node.set_graph_policy(&AllowAllAuthorizer, &graph_id(graph_base), graph_policy())
            .unwrap();
    }
    let seed = case_seed(case);
    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let node = Arc::clone(&node);
            std::thread::spawn(move || {
                let writer_started = Instant::now();
                for graph_index in 0..write_graphs {
                    let graph = graph_id(graph_base + graph_index);
                    let changes: Vec<_> = (writer..changed)
                        .step_by(writers)
                        .filter(|index| index % write_graphs == graph_index)
                        .map(|index| {
                            change(&graph, preloaded + index, seed.wrapping_add(writer + 1))
                        })
                        .collect();
                    if !changes.is_empty() {
                        node.apply_changes(&AllowAllAuthorizer, &graph, changes)
                            .unwrap();
                    }
                }
                writer_started.elapsed().as_nanos()
            })
        })
        .collect();
    let mut writer_ns: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    writer_ns.sort_unstable();
    settle(&node, case);
    if case.bool("replicated", false) {
        let replica = open_node(&path.join("replica"), case);
        for index in 0..graphs {
            let graph = graph_id(index);
            replica
                .set_graph_policy(&AllowAllAuthorizer, &graph, graph_policy())
                .unwrap();
            replica
                .install_graph_snapshot(&node.graph_snapshot(&graph).unwrap())
                .unwrap();
        }
        settle(&replica, case);
    }
    json!({
        "completed": changed,
        "writers": writers,
        "writer_ns": writer_ns,
        "replicated": case.bool("replicated", false),
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "work_ns": started.elapsed().as_nanos(),
    })
}

fn run_rebuild(path: &Path, case: &Case) -> Value {
    let node = Arc::new(open_node(path, case));
    let rows = prepare_node(&node, case);
    let rate = case.usize("rate", 0);
    let offered = usize::max(1, rows.saturating_mul(rate) / 100) * usize::from(rate > 0);
    let barrier = Arc::new(std::sync::Barrier::new(1 + usize::from(offered > 0)));
    // Completion time and size of each applied chunk, classified against the timer later.
    let completed = Arc::new(std::sync::Mutex::new(Vec::<(Instant, usize)>::new()));
    let rejected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = (offered > 0).then(|| {
        let node = Arc::clone(&node);
        let barrier = Arc::clone(&barrier);
        let completed = Arc::clone(&completed);
        let rejected = Arc::clone(&rejected);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            barrier.wait();
            for start in (0..offered).step_by(100) {
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                let end = usize::min(start + 100, offered);
                let changes = (start..end)
                    .map(|index| change(&graph_id(0), rows + index, 0x5a))
                    .collect();
                match node.apply_changes(&AllowAllAuthorizer, &graph_id(0), changes) {
                    Ok(_) => completed
                        .lock()
                        .unwrap()
                        .push((Instant::now(), end - start)),
                    Err(_) => {
                        rejected.fetch_add(end - start, std::sync::atomic::Ordering::AcqRel);
                    }
                }
            }
        })
    });
    if offered > 0 {
        barrier.wait();
    }
    let started = Instant::now();
    let status = node.rebuild_query_indexes().unwrap();
    let ended = Instant::now();
    let rebuild_ns = ended.duration_since(started).as_nanos();
    stop.store(true, std::sync::atomic::Ordering::Release);
    if let Some(writer) = writer {
        writer.join().unwrap();
    }
    let (mut before, mut during, mut after) = (0, 0, 0);
    for (at, rows) in completed.lock().unwrap().iter() {
        let slot = if *at < started {
            &mut before
        } else if *at <= ended {
            &mut during
        } else {
            &mut after
        };
        *slot += rows;
    }
    let rejected = rejected.load(std::sync::atomic::Ordering::Acquire);
    let drain_started = Instant::now();
    node.persist_fjall().unwrap();
    json!({
        "completed": rows,
        "effective": {
            "offered_rows": offered,
            "applied_before_rebuild": before,
            "applied_during_rebuild": during,
            "applied_after_rebuild": after,
            "rejected_rows": rejected,
            "not_attempted_rows": offered - before - during - after - rejected,
            "concurrent": during > 0,
        },
        "work_ns": rebuild_ns,
        "rebuild_ns": rebuild_ns,
        "drain_ns": drain_started.elapsed().as_nanos(),
        "source_rows": status.source_live_quads,
        "indexed_rows": status.indexed_quads,
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
    })
}

fn run_merge(path: &Path, case: &Case) -> Value {
    let left = open_node(&path.join("left"), case);
    let actors = case.usize("actors", 1);
    let right = open_actor(&path.join("right"), case, [0x44; 32]);
    let rows = prepare_node(&left, case);
    let graph = graph_id(0);
    right
        .set_graph_policy(&AllowAllAuthorizer, &graph, graph_policy())
        .unwrap();
    let overlap = rows.saturating_mul(case.usize("overlap", 0)) / 100;
    if overlap > 0 {
        let seed = case_seed(case);
        let changes = (0..overlap)
            .map(|index| change(&graph, index, seed))
            .collect();
        right
            .apply_changes(&AllowAllAuthorizer, &graph, changes)
            .unwrap();
    }
    if case.bool("deleted", false) && overlap > 0 {
        let seed = case_seed(case);
        let changes = (0..overlap / 2)
            .map(|index| deletion(&graph, index, seed))
            .collect();
        right
            .apply_changes(&AllowAllAuthorizer, &graph, changes)
            .unwrap();
    }
    // Each actor writes its own suffix rows in its own store, so the merged
    // graph really carries that many independent clock entries.
    let suffix = case.usize("suffix", 0);
    let mut actor_snapshots = Vec::with_capacity(actors);
    for actor in 0..actors {
        let peer = open_actor(
            &path.join(format!("actor-{actor}")),
            case,
            merge_actor(actor),
        );
        peer.set_graph_policy(&AllowAllAuthorizer, &graph, graph_policy())
            .unwrap();
        let changes = (0..suffix.max(1))
            .map(|index| change(&graph, rows + actor * suffix.max(1) + index, actor))
            .collect();
        peer.apply_changes(&AllowAllAuthorizer, &graph, changes)
            .unwrap();
        actor_snapshots.push(peer.graph_snapshot(&graph).unwrap());
    }
    let snapshot = left.graph_snapshot(&graph).unwrap();
    let started = Instant::now();
    let first = right.install_graph_snapshot(&snapshot).unwrap();
    let second = right.install_graph_snapshot(&snapshot).unwrap();
    for snapshot in &actor_snapshots {
        right.install_graph_snapshot(snapshot).unwrap();
    }
    right.persist_fjall().unwrap();
    // Includes the baseline and merging stores next to the requested actors.
    let clock_entries = right.vector_clock(&graph).unwrap().0.len();
    json!({
        "completed": rows,
        "overlap": overlap,
        "effective": {
            "requested_actors": actors,
            "clock_entries": clock_entries,
            "suffix_rows_per_actor": suffix.max(1),
        },
        "suffix": suffix,
        "work_ns": started.elapsed().as_nanos(),
        "first": format!("{first:?}"),
        "second": format!("{second:?}"),
        "fingerprint": format!("{:?}", right.graph_fingerprint(&graph).unwrap()),
    })
}

fn run_search(path: &Path, case: &Case) -> Value {
    let node = open_node(path, case);
    let rows = prepare_node(&node, case);
    let graphs = case.usize("graphs", 1).max(1);
    let subject_bytes = case.usize("subject_bytes", 0);
    if subject_bytes > 0 {
        node.apply_changes(
            &AllowAllAuthorizer,
            &graph_id(0),
            vec![sized_change(&graph_id(0), rows + 1, subject_bytes)],
        )
        .unwrap();
    }
    let readable = graphs.saturating_mul(case.usize("readable_per_mille", 1_000)) / 1_000;
    let allowed: Vec<String> = (0..readable)
        .map(|index| graph_id(index).to_string())
        .collect();
    let auth = move |graph: &GraphId, _policy: &GraphPolicy, action: Action| {
        if action == Action::Read && allowed.iter().any(|name| name == graph.as_str()) {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    };
    let reindex_started = Instant::now();
    node.reindex_search().unwrap();
    let reindex_ns = reindex_started.elapsed().as_nanos();
    let drain_started = Instant::now();
    node.flush_search_updates().unwrap();
    let drain_ns = drain_started.elapsed().as_nanos();
    let limit = case.usize("page", 10);
    let query_started = Instant::now();
    let hits = node
        .search(
            &auth,
            SearchRequest {
                query: "catalog",
                limit,
            },
        )
        .unwrap();
    let query_ns = query_started.elapsed().as_nanos();
    let hit_digest = blake3::hash(format!("{hits:?}").as_bytes())
        .to_hex()
        .to_string();
    json!({
        "completed": rows,
        "hits": hits.len(),
        "effective": {
            "graphs": graphs,
            "readable_graphs": readable,
            "readable_per_mille": readable.saturating_mul(1_000) / graphs,
            "page": limit,
        },
        "output_digest": hit_digest,
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "reindex_ns": reindex_ns,
        "drain_ns": drain_ns,
        "query_ns": query_ns,
        "work_ns": reindex_ns + drain_ns + query_ns,
    })
}

fn run_stores(path: &Path, case: &Case) -> Value {
    let stores = case.usize("stores", 1).max(1);
    let started = Instant::now();
    let mut fingerprints = Vec::new();
    for index in 0..stores {
        let node = open_node(&path.join(format!("store-{index}")), case);
        prepare_node(&node, case);
        settle(&node, case);
        fingerprints.push(format!(
            "{:?}",
            node.graph_fingerprint(&graph_id(0)).unwrap()
        ));
    }
    json!({"completed": stores, "fingerprints": fingerprints, "work_ns": started.elapsed().as_nanos()})
}

fn run_retries(path: &Path, case: &Case) -> Value {
    let node = open_node(path, case);
    node.set_graph_policy(&AllowAllAuthorizer, &graph_id(0), graph_policy())
        .unwrap();
    let changes = vec![change(&graph_id(0), 0, 17)];
    let started = Instant::now();
    node.apply_changes(&AllowAllAuthorizer, &graph_id(0), changes.clone())
        .unwrap();
    node.apply_changes(&AllowAllAuthorizer, &graph_id(0), changes)
        .unwrap();
    settle(&node, case);
    json!({
        "completed": 2,
        "work_ns": started.elapsed().as_nanos(),
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
    })
}

fn run_ingress(path: &Path, case: &Case) -> Value {
    let node = open_node(path, case);
    let graph = graph_id(0);
    let before = node.contains_graph(&graph).unwrap();
    let payload = if case.text("input", "valid") == "valid" {
        json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {"@id":"ro-crate-metadata.json","@type":"CreativeWork",
                 "conformsTo":{"@id":"https://w3id.org/ro/crate/1.2"},
                 "about":{"@id":graph.as_str()}},
                {"@id":graph.as_str(),"@type":"Dataset","name":"catalog",
                 "description":"catalog ingress","datePublished":"2026-09-20"}
            ]
        })
        .to_string()
    } else {
        format!(
            "{{\"@context\":[\"{}",
            "x".repeat(case.usize("bytes", 1_024))
        )
    };
    let started = Instant::now();
    let outcome = node.apply_rocrate_document_with_policy(
        &AllowAllAuthorizer,
        graph.clone(),
        &payload,
        graph_policy(),
    );
    let fingerprint = node
        .contains_graph(&graph)
        .unwrap()
        .then(|| format!("{:?}", node.graph_fingerprint(&graph).unwrap()));
    json!({
        "completed": usize::from(outcome.is_ok()),
        "rejected": usize::from(outcome.is_err()),
        "unchanged_on_reject": outcome.is_ok() || node.contains_graph(&graph).unwrap() == before,
        "fingerprint": fingerprint,
        "work_ns": started.elapsed().as_nanos(),
    })
}

fn run_mixed(path: &Path, case: &Case) -> Value {
    let node = open_node(path, case);
    let rows = prepare_node(&node, case);
    let started = Instant::now();
    let changed = usize::max(1, rows.saturating_mul(case.usize("rate", 50)) / 100);
    for start in (0..changed).step_by(1_000) {
        let end = usize::min(start + 1_000, changed);
        let changes = (start..end)
            .map(|index| change(&graph_id(0), rows + index, 20))
            .collect();
        node.apply_changes(&AllowAllAuthorizer, &graph_id(0), changes)
            .unwrap();
    }
    let query = node
        .query_with_statistics(&AllowAllAuthorizer, "ASK { ?s <urn:catalog:p> ?o }")
        .unwrap();
    let output_digest = blake3::hash(format!("{:?}", query.results).as_bytes())
        .to_hex()
        .to_string();
    node.rebuild_query_indexes().unwrap();
    node.flush_search_updates().unwrap();
    node.persist_fjall().unwrap();
    let fingerprint = node.graph_fingerprint(&graph_id(0)).unwrap();
    drop(node);
    let reopened = case.bool("restart", false).then(|| open_node(path, case));
    let restart_equal = reopened
        .as_ref()
        .is_none_or(|node| node.graph_fingerprint(&graph_id(0)).unwrap() == fingerprint);
    json!({
        "completed": rows + changed,
        "work_ns": started.elapsed().as_nanos(),
        "source_keys_read": query.statistics.source_keys_read,
        "qv_keys_read": query.statistics.qv_keys_read,
        "remaining_debt": 0,
        "restart_equal": restart_equal,
        "fingerprint": format!("{fingerprint:?}"),
        "output_digest": output_digest,
    })
}
