//! Exercises durable search recovery across process termination and damage repair.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use tempfile::tempdir;

use super::tests::{
    RAW_GRAPH, RawDoc, crate_request, drain_all, queued_reindex, raw_search, write_raw,
};
use super::*;
use crate::core::{GraphId, GraphPolicy};
use crate::search::queue::{CleanupScan, GenerationRequest};
use crate::store::GraphStore;
use crate::{AllowAllAuthorizer, CraqleNode, CreateCrateRequest, SearchRequest};

const CHILD_ENV: &str = "CRAQLE_SEARCH_CRASH_CHILD";
const ROOT_ENV: &str = "CRAQLE_SEARCH_CRASH_ROOT";
const GRAPH_ENV: &str = "CRAQLE_SEARCH_CRASH_GRAPH";
const CHECKPOINT: &str = "CRAQLE_SEARCH_CRASH_CHECKPOINT";
const TEST_NAME: &str = "search::recovery_tests::search_crash_cuts";
const WATCHDOG: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Cut {
    Page,
    Stage,
    Switch,
    Cleanup,
}

impl Cut {
    fn name(self) -> &'static str {
        match self {
            Self::Page => "page",
            Self::Stage => "stage",
            Self::Switch => "switch",
            Self::Cleanup => "cleanup",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "page" => Self::Page,
            "stage" => Self::Stage,
            "switch" => Self::Switch,
            "cleanup" => Self::Cleanup,
            _ => panic!("unknown search crash cut {value}"),
        }
    }
}

fn prepare_fixture(root: &Path, graph: &GraphId) {
    {
        let node = CraqleNode::open(root).expect("open fixture node");
        node.create_crate(
            &AllowAllAuthorizer,
            CreateCrateRequest::new(
                graph.clone(),
                "Search crash crate",
                "currentneedle",
                "2026-09-20",
                None,
                GraphPolicy::default(),
            ),
        )
        .expect("create fixture crate");
        node.flush_search_updates().expect("index fixture crate");
    }

    let store = GraphStore::open(root.join("store")).expect("open fixture store");
    let search = SearchIndex::open(root.join("search")).expect("open fixture search");
    search.bind_store(&store).expect("bind fixture search");
    search
        .index_resource(graph.as_str(), graph.as_str(), Some("legacyneedle"))
        .expect("seed stale search text");
    search.commit().expect("commit stale search text");

    let graph_term = EncodedTerm::from_named_node(&graph.0);
    let graph_id = store
        .lookup_term(&graph_term)
        .expect("lookup fixture graph")
        .expect("fixture graph is interned");
    let mut batch = store.new_batch();
    store
        .enqueue_fts_reindex(&mut batch, graph_id)
        .expect("queue fixture rebuild");
    store.commit(batch).expect("commit fixture rebuild debt");
    store.persist().expect("persist fixture rebuild debt");
}

fn run_child(root: &Path, graph: GraphId, cut: Cut) {
    let store = Arc::new(GraphStore::open(root.join("store")).expect("open child store"));
    let search = Arc::new(SearchIndex::open(root.join("search")).expect("open child search"));
    search.bind_store(&store).expect("bind child search");

    match cut {
        Cut::Page => search.arm_page_gate(),
        Cut::Stage => search.arm_stage_gate(),
        Cut::Switch => search.arm_switch_gate(),
        Cut::Cleanup => search.arm_cleanup_gate(),
    }

    let target = store.current_dirty_token();
    let worker = {
        let store = store.clone();
        let search = search.clone();
        std::thread::spawn(move || {
            loop {
                let progress = search
                    .drain_queues(
                        &store,
                        DrainRequest {
                            bound: QueueBound {
                                chunk: 8,
                                max_token: Some(target),
                            },
                            control: DrainControl::default(),
                        },
                    )
                    .expect("drain child search debt");
                assert!(progress.failures.is_empty(), "child search item failed");
                if !progress.remaining {
                    panic!("child drain completed before crash checkpoint");
                }
            }
        })
    };

    match cut {
        Cut::Page => search.await_page_gate(),
        Cut::Stage => search.await_stage_gate(),
        Cut::Switch => search.await_switch_gate(),
        Cut::Cleanup => search.await_cleanup_gate(),
    }
    eprintln!("{CHECKPOINT}:{}:{}", cut.name(), graph.as_str());
    let _ = worker.join();
    panic!("child search gate was unexpectedly released");
}

enum ChildEvent {
    Line(String),
    End,
}

fn crash_child(root: &Path, graph: &GraphId, cut: Cut) {
    let mut child = Command::new(std::env::current_exe().expect("resolve test executable"))
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_ENV, cut.name())
        .env(ROOT_ENV, root)
        .env(GRAPH_ENV, graph.as_str())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn search crash child");
    let stderr = child.stderr.take().expect("capture child stderr");
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            match line {
                Ok(line) => {
                    if sender.send(ChildEvent::Line(line)).is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = sender.send(ChildEvent::End);
    });

    let expected = format!("{CHECKPOINT}:{}:{}", cut.name(), graph.as_str());
    let mut output = Vec::new();
    loop {
        match receiver.recv_timeout(WATCHDOG) {
            Ok(ChildEvent::Line(line)) if line == expected => break,
            Ok(ChildEvent::Line(line)) => output.push(line),
            Ok(ChildEvent::End) => {
                let status = child.wait().expect("wait for exited crash child");
                panic!(
                    "search crash child exited at {status}: {}",
                    output.join("\n")
                );
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "search crash child missed {expected}: {}",
                    output.join("\n")
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("search crash child output disconnected");
            }
        }
    }
    child.kill().expect("kill search crash child");
    let status = child.wait().expect("reap search crash child");
    assert!(!status.success(), "crash child exited successfully");
    reader.join().expect("join child output reader");
}

fn verify_debt(root: &Path, graph: &GraphId, cut: Cut) {
    let store = GraphStore::open(root.join("store")).expect("open crashed store");
    assert!(
        store
            .contains_graph(graph)
            .expect("read crashed source graph")
    );
    let source = store.graph_snapshot(graph).expect("read crashed source");
    assert!(
        source
            .quads
            .iter()
            .any(|quad| quad.object.0.contains("currentneedle"))
    );
    let coverage = store
        .search_coverage()
        .expect("read crashed coverage")
        .expect("crashed coverage exists");
    assert!(coverage.covered < store.current_dirty_token());
    let rebuilds = store
        .drain_reindex_queue(2)
        .expect("read crashed rebuild debt");
    let stage = store
        .search_stage(&GenerationRequest {
            index_id: coverage.index_id,
            graph: graph.clone(),
        })
        .expect("read crashed stage");
    let cleanup = store
        .scan_search_cleanup(&CleanupScan {
            after: None,
            row_limit: 2,
            byte_limit: usize::MAX,
        })
        .expect("read crashed cleanup debt");
    match cut {
        Cut::Page | Cut::Stage => {
            assert_eq!(1, rebuilds.len());
            assert!(stage.is_some());
        }
        Cut::Switch => {
            assert_eq!(1, rebuilds.len());
            assert!(stage.is_none());
        }
        Cut::Cleanup => {
            assert!(rebuilds.is_empty());
            assert!(stage.is_none());
            assert!(!cleanup.entries.is_empty());
        }
    }
}

fn verify_reopen(root: &Path, graph: &GraphId) {
    let node = CraqleNode::open(root).expect("reopen crashed search node");
    node.flush_search_updates()
        .expect("repair crashed search debt");

    let current = node
        .search(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "currentneedle",
                limit: 10,
            },
        )
        .expect("search repaired text");
    assert!(
        current
            .iter()
            .any(|hit| hit.graph_id == graph.as_str() && hit.subject_iri == graph.as_str())
    );
    assert!(
        node.search(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "legacyneedle",
                limit: 10,
            },
        )
        .expect("search stale text")
        .is_empty()
    );

    let coverage = node
        .store
        .search_coverage()
        .expect("read repaired coverage")
        .expect("repaired coverage exists");
    assert_eq!(node.search.index_id(), coverage.index_id);
    assert_eq!(node.store.current_dirty_token(), coverage.covered);
    assert!(coverage.rebuild.is_none());
    assert!(
        node.store
            .drain_fts_queue(1)
            .expect("read subject debt")
            .is_empty()
    );
    assert!(
        node.store
            .drain_reindex_queue(1)
            .expect("read rebuild debt")
            .is_empty()
    );
    assert!(
        node.store
            .drain_delete_queue(1)
            .expect("read delete debt")
            .is_empty()
    );
    assert!(
        node.store
            .search_stage(&GenerationRequest {
                index_id: coverage.index_id,
                graph: graph.clone(),
            })
            .expect("read staged generation")
            .is_none()
    );
    let cleanup = node
        .store
        .scan_search_cleanup(&CleanupScan {
            after: None,
            row_limit: 1,
            byte_limit: usize::MAX,
        })
        .expect("read cleanup debt");
    assert!(cleanup.entries.is_empty() && !cleanup.remaining && cleanup.oversized.is_none());
}

#[test]
fn search_crash_cuts() {
    if let Ok(cut) = std::env::var(CHILD_ENV) {
        let root = std::env::var_os(ROOT_ENV).expect("child root is set");
        let graph = std::env::var(GRAPH_ENV).expect("child graph is set");
        run_child(Path::new(&root), GraphId::new(&graph), Cut::parse(&cut));
        return;
    }

    for cut in [Cut::Page, Cut::Stage, Cut::Switch, Cut::Cleanup] {
        let directory = tempfile::tempdir().expect("create search crash directory");
        let graph = GraphId::new(&format!("urn:test:search-crash-{}", cut.name()));
        prepare_fixture(directory.path(), &graph);
        crash_child(directory.path(), &graph, cut);
        verify_debt(directory.path(), &graph, cut);
        verify_reopen(directory.path(), &graph);
    }
}

/// A bound store and index with settled coverage and an empty raw graph.
fn settled(dir: &Path) -> (GraphStore, SearchIndex) {
    let store = GraphStore::open(dir).unwrap();
    let index = SearchIndex::open_in_memory().unwrap();
    index.bind_store(&store).unwrap();
    let graph = GraphId::new(RAW_GRAPH);
    store.create_graph(&graph).unwrap();
    store
        .set_graph_diagnostics(&graph, &crate::GraphDiagnostics::default())
        .unwrap();
    let target = drain_all(&index, &store);
    index.complete_coverage(&store, target).unwrap();
    assert!(store.search_coverage().unwrap().unwrap().rebuild.is_none());
    (store, index)
}

/// A settled fixture plus one matching document with neither a scope nor a generation key.
fn unaddressed(dir: &Path) -> (GraphStore, SearchIndex) {
    let (store, index) = settled(dir);
    write_raw(
        &index,
        RawDoc {
            scope: None,
            key: None,
            ..RawDoc::valid(&index)
        },
    );
    (store, index)
}

fn one_pass(store: &GraphStore) -> DrainRequest {
    DrainRequest {
        bound: QueueBound {
            chunk: 50,
            max_token: Some(store.current_dirty_token()),
        },
        control: DrainControl::default(),
    }
}

fn rebuild_bound(store: &GraphStore) -> bool {
    store.search_coverage().unwrap().unwrap().rebuild.is_some()
}

/// Drains the durable rebuild, runs its final sweep, and checks nothing is owed afterwards.
fn finish_repair(index: &SearchIndex, store: &GraphStore) {
    let target = drain_all(index, store);
    assert!(index.rebuild_owed.pending().is_none());
    index.complete_coverage(store, target).unwrap();
    assert!(!rebuild_bound(store));
    assert!(raw_search(index).unwrap().is_empty());
    assert!(!index.queue_repairs(store));
    assert_eq!(0, index.pin_view().searcher.num_docs());
}

#[test]
fn revision_failure_retained() {
    let dir = tempdir().unwrap();
    let (store, index) = unaddressed(dir.path());
    let error = raw_search(&index).unwrap_err();
    assert!(matches!(error, SearchError::Damaged { .. }), "{error:?}");
    let requested = index.rebuild_owed.pending();
    assert!(requested.is_some());

    index.hooks.revision_io.store(true, Ordering::SeqCst);
    let error = index.drain_queues(&store, one_pass(&store)).unwrap_err();
    assert!(matches!(error, SearchError::Io(_)), "{error:?}");
    assert_eq!(requested, index.rebuild_owed.pending());
    assert!(!rebuild_bound(&store));

    // The next pass adopts the retained request without another search.
    finish_repair(&index, &store);
}

#[test]
fn bind_failure_retained() {
    let dir = tempdir().unwrap();
    let (store, index) = unaddressed(dir.path());
    raw_search(&index).unwrap_err();
    let requested = index.rebuild_owed.pending();

    store.arm_commit_failure();
    let error = index.drain_queues(&store, one_pass(&store)).unwrap_err();
    assert!(matches!(error, SearchError::Store(_)), "{error:?}");
    assert_eq!(requested, index.rebuild_owed.pending());
    assert!(!rebuild_bound(&store));
    finish_repair(&index, &store);
}

#[test]
fn newer_request_survives() {
    let intent = RebuildIntent::default();
    intent.request();
    let seen = intent.pending().unwrap();
    // A report racing the durable bind must outlive the acceptance of the older one.
    intent.request();
    intent.accept(seen);
    assert_eq!(Some(seen + 1), intent.pending());
    intent.accept(seen + 1);
    assert_eq!(None, intent.pending());
}

#[test]
fn column_failures_classified() {
    let index = SearchIndex::open_in_memory().unwrap();
    index.set_generation(RAW_GRAPH, Some(DIRECT_GENERATION));
    write_raw(&index, RawDoc::valid(&index));
    let io = std::io::Error::other("injected column read");
    *index.hooks.column_failure.lock().unwrap() =
        Some(tantivy::TantivyError::IoError(Arc::new(io)));
    let error = raw_search(&index).unwrap_err();
    assert!(matches!(error, SearchError::Io(_)), "{error:?}");
    assert!(!error.to_string().contains("injected"), "{error}");
    assert!(index.rebuild_owed.pending().is_none());

    *index.hooks.column_failure.lock().unwrap() = Some(tantivy::TantivyError::InvalidArgument(
        "injected corrupt column".to_owned(),
    ));
    let error = raw_search(&index).unwrap_err();
    assert!(matches!(error, SearchError::Damaged { .. }), "{error:?}");
    assert!(index.rebuild_owed.pending().is_some());
    assert_eq!(1, raw_search(&index).unwrap().len());
}

fn report(index: &SearchIndex, view: &SearchView, generation: GenerationId) {
    let scope = generation_scope(index.index_id, RAW_GRAPH, generation);
    index.mark_damaged(view, Some(&scope));
}

fn pending_generation(index: &SearchIndex) -> Option<GenerationId> {
    index.damaged.lock().unwrap().graphs.get(RAW_GRAPH).copied()
}

/// Queues the pending graph repair and drains it until a new generation is published.
fn repair_cycle(index: &SearchIndex, store: &GraphStore) -> GenerationId {
    let before = index.active_generation(RAW_GRAPH);
    assert!(index.queue_repairs(store));
    drain_all(index, store);
    let after = index.active_generation(RAW_GRAPH).unwrap();
    assert_ne!(before, Some(after));
    after
}

#[test]
fn stale_reports_ignored() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let first = index.active_generation(RAW_GRAPH).unwrap();
    let first_view = index.pin_view();
    report(&index, &first_view, first);
    let second = repair_cycle(&index, &store);
    let second_view = index.pin_view();
    report(&index, &second_view, second);
    let third = repair_cycle(&index, &store);
    let third_view = index.pin_view();

    // Current first, then late reports from two older pinned views.
    report(&index, &third_view, third);
    report(&index, &first_view, first);
    report(&index, &second_view, second);
    assert_eq!(Some(third), pending_generation(&index));
    let fourth = repair_cycle(&index, &store);

    // Stale first, then current.
    report(&index, &second_view, second);
    assert_eq!(None, pending_generation(&index));
    report(&index, &index.pin_view(), fourth);
    assert_eq!(Some(fourth), pending_generation(&index));
    repair_cycle(&index, &store);

    for (view, generation) in [(&first_view, first), (&third_view, third)] {
        report(&index, view, generation);
    }
    assert_eq!(0, index.pending_repairs());
    assert!(!index.queue_repairs(&store));
    assert!(!queued_reindex(&store));
}

#[test]
fn publication_supersedes_report() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let reported = index.active_generation(RAW_GRAPH).unwrap();
    report(&index, &index.pin_view(), reported);
    // An unrelated reindex publishes a newer generation before the report is queued.
    store.ensure_reindex(&GraphId::new(RAW_GRAPH)).unwrap();
    drain_all(&index, &store);
    assert_ne!(Some(reported), index.active_generation(RAW_GRAPH));
    assert!(!index.queue_repairs(&store));
    assert_eq!(0, index.pending_repairs());
    assert!(!queued_reindex(&store));
}

#[test]
fn deleted_graph_ignored() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let reported = index.active_generation(RAW_GRAPH).unwrap();
    let view = index.pin_view();
    report(&index, &view, reported);
    store.delete_graph(&GraphId::new(RAW_GRAPH)).unwrap();
    drain_all(&index, &store);
    assert_eq!(None, index.active_generation(RAW_GRAPH));
    assert!(!index.queue_repairs(&store));
    report(&index, &view, reported);
    assert_eq!(0, index.pending_repairs());
}

#[test]
fn failed_enqueue_rereported() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let reported = index.active_generation(RAW_GRAPH).unwrap();
    index.set_retry_now(1_000);
    report(&index, &index.pin_view(), reported);
    store.arm_commit_failure();
    assert!(!index.queue_repairs(&store));
    report(&index, &index.pin_view(), reported);
    assert_eq!(Some(reported), pending_generation(&index));
    index.set_retry_now(1_000 + RETRY_MAX_MS);
    assert!(index.queue_repairs(&store));
    assert_eq!(0, index.pending_repairs());
    assert!(queued_reindex(&store));
}

#[test]
fn stale_overflow_ignored() {
    let index = SearchIndex::open_in_memory().unwrap();
    let view = index.pin_view();
    for graph in 0..DAMAGED_GRAPHS {
        let graph = format!("urn:test:capacity:{graph}");
        index.set_generation(&graph, Some(DIRECT_GENERATION));
        let scope = generation_scope(index.index_id, &graph, DIRECT_GENERATION);
        index.mark_damaged(&index.pin_view(), Some(&scope));
    }
    // A full set neither escalates nor evicts for a report the current view no longer shows.
    let stale = generation_scope(index.index_id, "urn:test:capacity:gone", GenerationId(9));
    index.mark_damaged(&view, Some(&stale));
    assert_eq!(DAMAGED_GRAPHS, index.pending_repairs());
    assert!(index.rebuild_owed.pending().is_none());
}

#[test]
fn concurrent_reports_keep() {
    let index = Arc::new(SearchIndex::open_in_memory().unwrap());
    index.set_generation(RAW_GRAPH, Some(DIRECT_GENERATION));
    let old = index.pin_view();
    let current = GenerationId(2);
    index.set_generation(RAW_GRAPH, Some(current));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|scope| {
        for reporter in 0..8 {
            let (index, old, barrier) = (Arc::clone(&index), Arc::clone(&old), barrier.clone());
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..200 {
                    if reporter % 2 == 0 {
                        report(&index, &old, DIRECT_GENERATION);
                    } else {
                        report(&index, &index.pin_view(), current);
                    }
                }
            });
        }
    });
    assert_eq!(Some(current), pending_generation(&index));
    assert_eq!(1, index.pending_repairs());
}

#[test]
fn sweep_removes_unaddressed() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let active = index.active_generation(RAW_GRAPH).unwrap();
    let malformed = RawDoc {
        scope: Some(vec![0; 8]),
        key: Some("not a generation key".to_owned()),
        ..RawDoc::valid(&index)
    };
    let orphan_scope = generation_scope(index.index_id, "urn:test:orphan", GenerationId(77));
    let orphan = RawDoc {
        scope: Some(orphan_scope),
        key: Some(generation_key(
            index.index_id,
            "urn:test:orphan",
            GenerationId(77),
        )),
        ..RawDoc::valid(&index)
    };
    let missing = RawDoc {
        scope: None,
        key: None,
        ..RawDoc::valid(&index)
    };
    for raw in [malformed, orphan, missing] {
        write_raw(&index, raw);
    }
    let before = index.pin_view();
    raw_search(&index).unwrap_err();
    let sweeps = index.sweeps.load(Ordering::SeqCst);
    finish_repair(&index, &store);
    assert_eq!(sweeps + 1, index.sweeps.load(Ordering::SeqCst));
    assert_ne!(Some(active), index.active_generation(RAW_GRAPH));

    // A reader pinned before the sweep cannot restart the finished repair.
    index.mark_damaged(&before, None);
    assert!(index.rebuild_owed.pending().is_none());
}

#[test]
fn overflow_repair_completes() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    for graph in 0..=DAMAGED_GRAPHS {
        let graph = format!("urn:test:overflow:{graph}");
        index.set_generation(&graph, Some(DIRECT_GENERATION));
        let scope = generation_scope(index.index_id, &graph, DIRECT_GENERATION);
        write_raw(
            &index,
            RawDoc {
                scope: Some(scope.clone()),
                stable: None,
                key: Some(generation_key(index.index_id, &graph, DIRECT_GENERATION)),
                ..RawDoc::valid(&index)
            },
        );
        index.mark_damaged(&index.pin_view(), Some(&scope));
    }
    assert!(index.rebuild_owed.pending().is_some());
    finish_repair(&index, &store);
    assert_eq!(0, index.pending_repairs());
}

#[test]
fn sweep_keeps_staged() {
    let dir = tempdir().unwrap();
    let (store, index) = settled(dir.path());
    let staged = GraphId::new("urn:test:staged");
    store.create_graph(&staged).unwrap();
    let job = store
        .begin_search_stage(&StageRequest {
            index_id: index.index_id,
            graph: staged.clone(),
            target: store.current_dirty_token(),
            session: index.session,
        })
        .unwrap();
    write_raw(
        &index,
        RawDoc {
            scope: Some(generation_scope(
                index.index_id,
                staged.as_str(),
                job.generation,
            )),
            key: Some(generation_key(
                index.index_id,
                staged.as_str(),
                job.generation,
            )),
            ..RawDoc::valid(&index)
        },
    );
    let active = index.pin_view().generations.as_ref().clone();
    index.sweep_unaddressed(&store, &active).unwrap();
    assert_eq!(1, index.pin_view().searcher.num_docs());
}

#[test]
fn finish_failure_resumes() {
    let dir = tempdir().unwrap();
    let (store, index) = unaddressed(dir.path());
    raw_search(&index).unwrap_err();
    let target = drain_all(&index, &store);
    // The sweep commits, then the durable rebuild completion fails once.
    store.arm_commit_failure();
    index.complete_coverage(&store, target).unwrap_err();
    assert!(rebuild_bound(&store));
    assert_eq!(0, index.pin_view().searcher.num_docs());
    index.complete_coverage(&store, target).unwrap();
    assert!(!rebuild_bound(&store));
    assert!(raw_search(&index).unwrap().is_empty());
}

/// Indexes one crate, then adds a matching document with no scope, key, or identity.
fn unaddressed_node(dir: &Path) -> (crate::CraqleNode, Vec<(String, String)>) {
    let graph = GraphId::new("urn:test:unaddressed");
    let node = crate::CraqleNode::open(dir).unwrap();
    node.create_crate(
        &crate::AllowAllAuthorizer,
        crate_request(&graph, "sweepneedle", true),
    )
    .unwrap();
    node.flush_search_updates().unwrap();
    let expected = sweep_search(&node).unwrap();
    assert!(!expected.is_empty());
    let mut document = TantivyDocument::default();
    document.add_text(node.search.f_all_text, "sweepneedle");
    node.search
        .writer()
        .unwrap()
        .add_document(document)
        .unwrap();
    node.search.write_epoch.fetch_add(1, Ordering::SeqCst);
    node.search.commit().unwrap();
    (node, expected)
}

fn sweep_search(node: &crate::CraqleNode) -> crate::Result<Vec<(String, String)>> {
    let hits = node.search(
        &crate::AllowAllAuthorizer,
        crate::SearchRequest {
            query: "sweepneedle",
            limit: 10,
        },
    )?;
    Ok(hits
        .into_iter()
        .map(|hit| (hit.graph_id, hit.subject_iri))
        .collect())
}

/// Waits, without searching, until the worker durably adopts the whole-rebuild request.
fn await_adopted(node: &crate::CraqleNode) {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    while node.search.rebuild_owed.pending().is_some() {
        assert!(
            std::time::Instant::now() < deadline,
            "rebuild never adopted"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn unaddressed_repair_completes() {
    let dir = tempdir().unwrap();
    let (node, expected) = unaddressed_node(dir.path());
    let sweeps = node.search.sweeps.load(Ordering::SeqCst);
    let error = sweep_search(&node).unwrap_err();
    assert_eq!(error.kind(), crate::CraqleErrorKind::CorruptDerivedData);
    await_adopted(&node);
    // A flush requested after adoption covers the rebuild and its final sweep.
    node.flush_search_updates().unwrap();
    assert_eq!(expected, sweep_search(&node).unwrap());
    assert_eq!(sweeps + 1, node.search.sweeps.load(Ordering::SeqCst));
    assert!(node.search.rebuild_owed.pending().is_none());
    assert!(!rebuild_bound(&node.store));
}

#[test]
fn adopted_sweep_reopens() {
    let dir = tempdir().unwrap();
    let (node, expected) = unaddressed_node(dir.path());
    sweep_search(&node).unwrap_err();
    await_adopted(&node);
    drop(node);

    let node = crate::CraqleNode::open(dir.path()).unwrap();
    node.flush_search_updates().unwrap();
    assert_eq!(expected, sweep_search(&node).unwrap());
    assert!(!rebuild_bound(&node.store));
}
