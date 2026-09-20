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
    ActorId, AllowAllAuthorizer, CraqleFjallPersistMode as PersistMode, CraqleNode, CraqleOptions,
    EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange, SearchRequest,
};
use serde_json::{Value, json};

use catalog_case::{CAP_ENV, CASE_ENV, Case, ID_ENV, case_seed, estimate_bytes, work_rows};
use catalog_reads::{run_cache, run_reads};

fn main() {
    let id = std::env::var(ID_ENV).expect("CRAQLE_BENCH_ID is required");
    let case = Case(serde_json::from_str(&std::env::var(CASE_ENV).unwrap()).unwrap());
    let byte_cap = std::env::var(CAP_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128 * 1024 * 1024usize);
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
    open_actor(path, case, 0x43)
}

fn open_actor(path: &Path, case: &Case, actor: u8) -> CraqleNode {
    CraqleNode::open_with_options(
        path,
        CraqleOptions::new()
            .with_actor(ActorId::from_bytes([actor; 32]))
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
    let node = open_node(path, case);
    let rows = prepare_node(&node, case);
    let rate = case.usize("rate", 0);
    if rate > 0 {
        let count = usize::max(1, rows.saturating_mul(rate) / 100);
        let changes = (0..count)
            .map(|index| change(&graph_id(0), rows + index, rate))
            .collect();
        node.apply_changes(&AllowAllAuthorizer, &graph_id(0), changes)
            .unwrap();
    }
    let started = Instant::now();
    let status = node.rebuild_query_indexes().unwrap();
    node.persist_fjall().unwrap();
    json!({
        "completed": rows,
        "work_ns": started.elapsed().as_nanos(),
        "source_rows": status.source_live_quads,
        "indexed_rows": status.indexed_quads,
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
    })
}

fn run_merge(path: &Path, case: &Case) -> Value {
    let left = open_node(&path.join("left"), case);
    let actor = case.usize("actors", 1).min(255) as u8;
    let right = open_actor(&path.join("right"), case, actor);
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
    let suffix = case.usize("suffix", 0);
    if suffix > 0 {
        let changes = (0..suffix)
            .map(|index| change(&graph, rows + index, actor as usize))
            .collect();
        right
            .apply_changes(&AllowAllAuthorizer, &graph, changes)
            .unwrap();
    }
    let snapshot = left.graph_snapshot(&graph).unwrap();
    let started = Instant::now();
    let first = right.install_graph_snapshot(&snapshot).unwrap();
    let second = right.install_graph_snapshot(&snapshot).unwrap();
    right.persist_fjall().unwrap();
    json!({
        "completed": rows,
        "overlap": overlap,
        "actors": actor,
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
    let subject_bytes = case.usize("subject_bytes", 0);
    if subject_bytes > 0 {
        node.apply_changes(
            &AllowAllAuthorizer,
            &graph_id(0),
            vec![sized_change(&graph_id(0), rows + 1, subject_bytes)],
        )
        .unwrap();
    }
    let started = Instant::now();
    node.reindex_search().unwrap();
    node.flush_search_updates().unwrap();
    let limit = case.usize("page", 10);
    let hits = node
        .search(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "catalog",
                limit,
            },
        )
        .unwrap();
    let hit_digest = blake3::hash(format!("{hits:?}").as_bytes())
        .to_hex()
        .to_string();
    json!({
        "completed": rows,
        "hits": hits.len(),
        "output_digest": hit_digest,
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "work_ns": started.elapsed().as_nanos(),
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
