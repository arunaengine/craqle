//! Measures private search failure and bounded queue contracts for B09 and B10.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::time::Instant;

use serde_json::{Value, json};

use super::*;

fn bench_case() -> Value {
    serde_json::from_str(&std::env::var("CRAQLE_BENCH_CASE").unwrap()).unwrap()
}

fn byte_cap() -> usize {
    std::env::var("CRAQLE_BENCH_BYTE_CAP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128 * 1024 * 1024)
}

fn writer_auth() -> crate::GrantAuthorizer {
    crate::GrantAuthorizer::new(vec![crate::PermissionGrant::new(
        "/bench/**",
        crate::PermissionLevel::Write,
    )])
}

fn crate_request(graph: &GraphId, label: &str) -> crate::CreateCrateRequest {
    crate::CreateCrateRequest::new(
        graph.clone(),
        "Search Bench",
        label,
        "2026-09-20",
        None,
        crate::core::GraphPolicy {
            public: true,
            permission_paths: vec!["/bench/search".to_string()],
        },
    )
}

fn reopen_store(path: &Path) -> Arc<GraphStore> {
    Arc::new(GraphStore::open(path.join("store")).unwrap())
}

fn graph_term(store: &GraphStore, graph: &GraphId) -> TermId {
    store
        .lookup_term(&EncodedTerm::from_named_node(&graph.0))
        .unwrap()
        .unwrap()
}

fn seed_graphs(path: &Path) -> (Arc<GraphStore>, Arc<SearchIndex>, GraphId, GraphId) {
    let healthy = GraphId::new("urn:bench:failure:healthy");
    let broken = GraphId::new("urn:bench:failure:broken");
    {
        let node = crate::CraqleNode::open(path).unwrap();
        for graph in [&healthy, &broken] {
            node.create_crate(&writer_auth(), crate_request(graph, "failure needle"))
                .unwrap();
        }
        node.flush_search_updates().unwrap();
    }
    let store = reopen_store(path);
    let search = Arc::new(SearchIndex::open_in_memory().unwrap());
    search.bind_store(&store).unwrap();
    let mut batch = store.new_batch();
    for graph in [&healthy, &broken] {
        store
            .enqueue_fts_reindex(&mut batch, graph_term(&store, graph))
            .unwrap();
    }
    store.commit(batch).unwrap();
    (store, search, healthy, broken)
}

#[test]
#[ignore = "release-only B09 private failure workload"]
fn catalog_b09() {
    let case = bench_case();
    let mode = case["failure"].as_str().unwrap_or("permanent");
    let root = tempfile::tempdir().unwrap();
    let (store, search, healthy, broken) = seed_graphs(root.path());
    let started = Instant::now();
    let mut failures = 0usize;
    let mut retries = 0u32;
    if mode == "shutdown" {
        let control = DrainControl::default();
        control.cancel();
        let result = search.drain_queues(
            &store,
            DrainRequest {
                bound: QueueBound {
                    chunk: 64,
                    max_token: None,
                },
                control,
            },
        );
        assert!(matches!(result, Err(SearchError::Cancelled)));
    } else if mode == "transient" {
        store.arm_commit_failure();
        let result = search.drain_queues(
            &store,
            DrainRequest {
                bound: QueueBound {
                    chunk: 64,
                    max_token: Some(store.current_dirty_token()),
                },
                control: DrainControl::default(),
            },
        );
        assert!(
            result.is_err(),
            "the injected global commit failure was not observed"
        );
        failures = 1;
        crate::flush_search_queue(&store, &search).unwrap();
    } else {
        *search
            .hooks
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(broken.as_str().to_string());
        search.set_retry_now(1);
        let attempts = match mode {
            "permanent" => MAX_RETRY_ATTEMPTS,
            "flush-flood" => 64,
            value => panic!("unknown failure mode {value}"),
        };
        for attempt in 0..attempts {
            search.set_retry_now(u64::from(attempt).saturating_mul(RETRY_MAX_MS + 1) + 1);
            let result = search
                .drain_queues(
                    &store,
                    DrainRequest {
                        bound: QueueBound {
                            chunk: if mode == "flush-flood" { 1 } else { 64 },
                            max_token: Some(store.current_dirty_token()),
                        },
                        control: DrainControl::default(),
                    },
                )
                .unwrap();
            failures += result.failures.len();
        }
        let id = graph_queue_id(QueueKind::Reindex, &broken);
        retries = store
            .fts_failure(&id)
            .unwrap()
            .map_or(0, |state| state.attempts);
        *search
            .hooks
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }
    let healthy_hits = search.search("failure", 10).unwrap();
    assert!(
        mode == "shutdown"
            || healthy_hits
                .iter()
                .any(|hit| hit.graph_id == healthy.as_str())
    );
    let output_digest = blake3::hash(format!("{healthy_hits:?}").as_bytes())
        .to_hex()
        .to_string();
    println!(
        "{}",
        json!({
            "benchmark_id": "B09",
            "status": "measured",
            "case": case,
            "result": {
                "completed": healthy_hits.len(),
                "output_digest": output_digest,
                "failures": failures,
                "retries": retries,
                "remaining_debt": store.drain_reindex_queue(usize::MAX).unwrap().len(),
                "work_ns": started.elapsed().as_nanos(),
            }
        })
    );
}

fn queue_graph(index: usize, bytes: usize) -> GraphId {
    GraphId::new(&format!("urn:bench:queue:{index}:{}", "x".repeat(bytes)))
}

fn enqueue_graph(store: &GraphStore, graph: &GraphId) {
    store.create_graph(graph).unwrap();
    let mut batch = store.new_batch();
    store
        .enqueue_fts_reindex(&mut batch, graph_term(store, graph))
        .unwrap();
    store.commit(batch).unwrap();
}

#[test]
#[ignore = "release-only B10 private queue workload"]
fn catalog_b10() {
    let case = bench_case();
    let future = case["future"].as_u64().unwrap_or(0) as usize;
    let eligible = case["eligible"].as_u64().unwrap_or(1) as usize;
    let subject_bytes = case["subject_bytes"].as_u64().unwrap_or(16) as usize;
    let estimated = future
        .saturating_add(eligible)
        .saturating_mul(subject_bytes.saturating_add(256));
    if estimated > byte_cap() {
        println!(
            "{}",
            json!({
                "benchmark_id": "B10",
                "status": "capacity_blocked",
                "case": case,
                "estimated_bytes": estimated,
                "byte_cap": byte_cap(),
            })
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let store = GraphStore::open(root.path()).unwrap();
    for index in 0..eligible {
        enqueue_graph(&store, &queue_graph(index, subject_bytes));
    }
    let target = store.current_dirty_token();
    for index in 0..future {
        enqueue_graph(&store, &queue_graph(eligible + index, subject_bytes));
    }

    let row_limit = eligible.saturating_add(1).max(1);
    let byte_limit = row_limit.saturating_mul(subject_bytes.saturating_add(128));
    let started = Instant::now();
    let page = store
        .scan_fts_reindexes(&QueueScan {
            max_token: Some(target),
            after: None,
            row_limit,
            byte_limit,
        })
        .unwrap();
    assert!(page.entries.len() <= eligible);
    assert!(page.rows <= row_limit);
    assert!(page.bytes <= byte_limit);
    let output_digest = blake3::hash(format!("{:?}", page.entries).as_bytes())
        .to_hex()
        .to_string();
    println!(
        "{}",
        json!({
            "benchmark_id": "B10",
            "status": "measured",
            "case": case,
            "result": {
                "completed": page.entries.len(),
                "output_digest": output_digest,
                "records_visited": page.rows,
                "prepared_bytes": page.bytes,
                "max_queue_bytes": byte_limit,
                "remaining_debt": usize::from(page.remaining),
                "future_rows": future,
                "work_ns": started.elapsed().as_nanos(),
            }
        })
    );
}

const PRUNE_VOCABULARY: [&str; 8] = [
    "common", "frequent", "usual", "middle", "sparse", "scarce", "rare", "unique",
];

/// Deterministic text whose term frequencies fall off across the vocabulary.
fn prune_text(index: usize) -> String {
    let mut state = (index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut words = Vec::new();
    for (rank, word) in PRUNE_VOCABULARY.iter().enumerate() {
        state ^= state >> 31;
        state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let repeats = (state % 4) as usize;
        if state.is_multiple_of(1u64 << (rank * 2)) {
            words.extend(std::iter::repeat_n(*word, repeats + 1));
        }
    }
    if index.is_multiple_of(11) {
        return "tie filler".to_owned();
    }
    words.join(" ")
}

fn prune_index(docs: usize, segments: usize) -> SearchIndex {
    let index = SearchIndex::open_in_memory().unwrap();
    // Background merges would race the explicit merge and change the measured layout.
    index
        .writer()
        .unwrap()
        .set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
    let chunk = docs.div_ceil(segments.max(1));
    for position in 0..docs {
        let graph = format!("urn:bench:prune:graph:{}", position % 64);
        let subject = format!("urn:bench:prune:subject:{position}");
        index
            .index_resource(&graph, &subject, Some(&prune_text(position)))
            .unwrap();
        if (position + 1) % chunk == 0 {
            index.commit().unwrap();
        }
    }
    index.commit().unwrap();
    if segments == 1 {
        let ids = index.index.searchable_segment_ids().unwrap();
        if ids.len() > 1 {
            index.writer().unwrap().merge(&ids).wait().unwrap();
        }
        index.reader.reload().unwrap();
        index.publish_searcher();
    }
    index
}

struct PruneRun<'a> {
    query: &'a str,
    limit: usize,
    sparse: bool,
    exhaustive: bool,
}

fn prune_search(index: &SearchIndex, run: &PruneRun<'_>) -> (u64, Vec<SearchHit>) {
    index
        .hooks
        .exhaustive
        .store(run.exhaustive, Ordering::SeqCst);
    let allows = |graph: &str| {
        let slot: usize = graph.rsplit(':').next().unwrap().parse().unwrap();
        Ok::<bool, SearchError>(!run.sparse || slot.is_multiple_of(3))
    };
    let check = || Ok::<(), SearchError>(());
    let started = Instant::now();
    let hits = index
        .collect_filtered(FilterQuery {
            candidates: None,
            query: run.query,
            limit: run.limit,
            subject: None,
            allows: &allows,
            check: &check,
        })
        .unwrap();
    (started.elapsed().as_nanos() as u64, hits)
}

/// Checks pruned hits against the exhaustive oracle by identity, eligibility, and score;
/// hits tied within the tolerance may reorder, and only the cutoff tie group may differ.
fn assert_same_hits(oracle: &[SearchHit], pruned: &[SearchHit], sparse: bool) {
    let tolerance = |score: f32| score.abs() * 1e-5 + 1e-6;
    assert_eq!(oracle.len(), pruned.len(), "result sizes differ");
    for hits in [oracle, pruned] {
        let mut seen = HashSet::new();
        for hit in hits {
            let position: usize = hit
                .subject_iri
                .strip_prefix("urn:bench:prune:subject:")
                .and_then(|position| position.parse().ok())
                .unwrap_or_else(|| panic!("{} is not a fixture subject", hit.subject_iri));
            let graph = format!("urn:bench:prune:graph:{}", position % 64);
            assert_eq!(graph, hit.graph_id, "wrong graph for {}", hit.subject_iri);
            assert!(
                !sparse || (position % 64).is_multiple_of(3),
                "ineligible {graph}"
            );
            assert!(
                seen.insert(&hit.subject_iri),
                "duplicate {}",
                hit.subject_iri
            );
        }
    }
    let mut group = 0;
    while group < oracle.len() {
        let score = oracle[group].score;
        let end = oracle[group..]
            .iter()
            .position(|hit| (hit.score - score).abs() > tolerance(score))
            .map_or(oracle.len(), |offset| group + offset);
        for hit in &pruned[group..end] {
            assert!((hit.score - score).abs() <= tolerance(score), "score moved");
        }
        if end < oracle.len() {
            let mut expected: Vec<_> = oracle[group..end]
                .iter()
                .map(|hit| &hit.subject_iri)
                .collect();
            let mut actual: Vec<_> = pruned[group..end]
                .iter()
                .map(|hit| &hit.subject_iri)
                .collect();
            expected.sort();
            actual.sort();
            assert_eq!(expected, actual, "tie group members differ");
        }
        group = end;
    }
}

fn median(samples: &mut [u64]) -> u64 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[test]
#[ignore = "release-only exhaustive and pruned text collection comparison"]
fn text_pruning() {
    let docs = std::env::var("CRAQLE_PRUNE_DOCS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let rounds = 15;
    for segments in [1, 8] {
        let index = prune_index(docs, segments);
        let layout = index.pin_view().searcher.segment_readers().len();
        for query in ["common", "common frequent", "rare", "unique", "tie"] {
            for (limit, sparse) in [(10, false), (100, false), (10, true), (100, true)] {
                let mut timings = [Vec::new(), Vec::new()];
                let mut hydrate = [0u64, 0u64];
                let mut pruned_segments = 0;
                for round in 0..=rounds {
                    let mut results = Vec::new();
                    for step in 0..2 {
                        let exhaustive = (round + step) % 2 == 0;
                        let before = index.hooks.pruned.load(Ordering::SeqCst);
                        let hydrated = index.hooks.hydrate_ns.load(Ordering::SeqCst);
                        let (elapsed, hits) = prune_search(
                            &index,
                            &PruneRun {
                                query,
                                limit,
                                sparse,
                                exhaustive,
                            },
                        );
                        if round > 0 {
                            let slot = usize::from(!exhaustive);
                            timings[slot].push(elapsed);
                            hydrate[slot] +=
                                index.hooks.hydrate_ns.load(Ordering::SeqCst) - hydrated;
                            pruned_segments += index.hooks.pruned.load(Ordering::SeqCst) - before;
                        }
                        results.push(hits);
                    }
                    let exhaustive = usize::from(round % 2 != 0);
                    assert_same_hits(&results[exhaustive], &results[1 - exhaustive], sparse);
                }
                println!(
                    "{}",
                    json!({
                        "benchmark_id": "text_pruning",
                        "docs": docs,
                        "segments": layout,
                        "requested_segments": segments,
                        "query": query,
                        "limit": limit,
                        "sparse": sparse,
                        "rounds": rounds,
                        "exhaustive_median_ns": median(&mut timings[0]),
                        "pruned_median_ns": median(&mut timings[1]),
                        "exhaustive_hydrate_ns": hydrate[0] / rounds as u64,
                        "pruned_hydrate_ns": hydrate[1] / rounds as u64,
                        "pruned_segments": pruned_segments,
                    })
                );
            }
        }
    }
}

#[test]
#[ignore = "release-only generation publication cost by graph count"]
fn generation_publication() {
    for graphs in [1_000usize, 10_000, 50_000] {
        let index = SearchIndex::open_in_memory().unwrap();
        let rows = (0..graphs)
            .map(|slot| GraphGeneration {
                graph: GraphId::new(&format!("urn:bench:publish:{slot:06}")),
                active: Some(DIRECT_GENERATION),
                covered: 0,
            })
            .collect();
        index.publish_generations(GenerationView::from_rows(index.index_id, rows));
        let switches = 200;
        let mut lock_ns = Vec::with_capacity(switches);
        let started = Instant::now();
        for switch in 0..switches {
            let graph = format!("urn:bench:publish:{:06}", switch * 7 % graphs);
            let call = Instant::now();
            index.set_generation(&graph, Some(GenerationId(2 + switch as u64)));
            lock_ns.push(call.elapsed().as_nanos() as u64);
        }
        let total = started.elapsed().as_nanos();
        lock_ns.sort_unstable();
        let per_switch = total / switches as u128;
        println!(
            "{}",
            json!({
                "benchmark_id": "generation_publication",
                "graphs": graphs,
                "switches": switches,
                "copied_entries_per_switch": graphs,
                "median_switch_ns": lock_ns[switches / 2],
                "p99_switch_ns": lock_ns[(switches - 1) * 99 / 100],
                "mean_switch_ns": per_switch,
                // A projection from per-switch timings, not a measured rebuild.
                "projected_rebuild_ns": per_switch * graphs as u128,
                "projection_only": true,
            })
        );
    }
}
