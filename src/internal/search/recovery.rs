//! Exercises durable search recovery across process termination.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;

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
