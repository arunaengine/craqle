//! Exercises query-view rebuild races and crash recovery.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, mpsc};
use std::time::Duration;

use super::*;
use crate::QueryIndexVerificationMode as VerificationMode;

const CHILD_MODE: &str = "CRAQLE_QV_CRASH_PHASE";
const CHILD_MARKER: &str = "CRAQLE_QV_CRASH_MARKER";
const CHILD_ROOT: &str = "CRAQLE_QV_CRASH_ROOT";
const CHILD_TEST: &str = "store::recovery_tests::crash_child";
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const TEST_GRAPH: &str = "urn:test:qv-recovery";

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}

struct PhaseGate {
    phase: RebuildPhase,
    state: Mutex<GateState>,
    changed: Condvar,
}

impl PhaseGate {
    fn new(phase: RebuildPhase) -> Self {
        Self {
            phase,
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
        }
    }

    fn run(&self, phase: RebuildPhase) {
        if phase != self.phase {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.entered {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, PROGRESS_TIMEOUT)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
            assert!(!timeout.timed_out(), "rebuild phase was not released");
        }
    }

    fn wait(&self) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, PROGRESS_TIMEOUT, |state| !state.entered)
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.entered && !timeout.timed_out(),
            "rebuild phase was not reached"
        );
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.released = true;
        self.changed.notify_all();
    }
}

struct CleanupGate {
    active: AtomicBool,
    gate: PhaseGate,
}

impl CleanupGate {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            gate: PhaseGate::new(RebuildPhase::CleanupPage),
        }
    }

    fn run(&self, phase: RebuildPhase) {
        if phase == RebuildPhase::AfterSwitch {
            self.active.store(true, Ordering::SeqCst);
        } else if self.active.load(Ordering::SeqCst) {
            self.gate.run(phase);
        }
    }
}

fn named(value: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(value))
}

fn encode_quad(store: &GraphStore, graph: &GraphId, suffix: &str) -> EncodedQuad {
    EncodedQuad {
        graph: store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap(),
        subject: store
            .resolve_term(&named(&format!("urn:subject:{suffix}")))
            .unwrap(),
        predicate: store.resolve_term(&named("urn:predicate")).unwrap(),
        object: store
            .resolve_term(&named(&format!("urn:object:{suffix}")))
            .unwrap(),
    }
}

fn try_commit(store: &GraphStore, graph: &GraphId, quad: EncodedQuad) -> Result<Dot> {
    let _commit = store.graph_commit_guard(graph);
    let actor = ActorId::random();
    let mut batch = store.new_batch();
    let counter = store.next_counter(
        &mut batch,
        CounterKey {
            graph_id: quad.graph,
            actor,
        },
    )?;
    let dot = Dot { actor, counter };
    store.insert_quad(&mut batch, QuadAdd { quad, dot })?;
    let mut clock = store.vector_clock_id(quad.graph)?;
    clock.advance(actor, counter);
    store.set_vector_clock(
        &mut batch,
        ClockUpdate {
            graph_id: quad.graph,
            clock: &clock,
        },
    )?;
    store.commit(batch)?;
    Ok(dot)
}

fn commit_quad(store: &GraphStore, graph: &GraphId, suffix: &str) {
    let quad = encode_quad(store, graph, suffix);
    try_commit(store, graph, quad).unwrap();
}

fn setup_store(path: &Path) -> Arc<GraphStore> {
    let store = Arc::new(GraphStore::open_with_mode(path, PersistMode::SyncAll).unwrap());
    let graph = GraphId::new(TEST_GRAPH);
    if !store.contains_graph(&graph).unwrap() {
        store.create_graph(&graph).unwrap();
        commit_quad(&store, &graph, "base");
        store.persist().unwrap();
    }
    store
}

fn set_state(store: &GraphStore, state: StoredIndexState) {
    let snapshot = store.db.snapshot();
    let mut header = match store.snapshot_index_header(&snapshot).unwrap() {
        IndexHeaderRead::Valid(header) => header,
        _ => panic!("fixture query header must be valid"),
    };
    header.state = state;
    let mut batch = store.buffered_batch();
    store.stage_index_header(&mut batch, &header);
    store.commit_fjall_batch(batch).unwrap();
}

fn quad_key(quad: &EncodedQuad) -> [u128; 4] {
    [
        quad.graph.0,
        quad.subject.0,
        quad.predicate.0,
        quad.object.0,
    ]
}

fn assert_exact(store: &GraphStore, graph: &GraphId, suffixes: &[&str]) {
    let mut expected: Vec<_> = suffixes
        .iter()
        .map(|suffix| encode_quad(store, graph, suffix))
        .collect();
    let mut source = store.quads_for_pattern(None, None, None, None).unwrap();
    expected.sort_by_key(quad_key);
    source.sort_by_key(quad_key);
    assert_eq!(
        expected, source,
        "authoritative source changed during recovery"
    );

    let report = store.verify_query_indexes(VerificationMode::Full).unwrap();
    assert!(report.valid, "query view differs from source: {report:?}");
    assert_eq!(expected.len() as u64, report.source_live_quads);
    assert_eq!(expected.len() as u64, report.indexed_quads);
    assert!(
        store
            .snapshot_admission(&store.db.snapshot())
            .unwrap()
            .trusted
    );
}

fn install_gate<'a>(store: &'a GraphStore, gate: Arc<PhaseGate>) -> RebuildHook<'a> {
    store.install_rebuild_hook(Arc::new(move |phase| gate.run(phase)))
}

fn switch_write(state: StoredIndexState) {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let graph = GraphId::new(TEST_GRAPH);
    set_state(&store, state);
    let gate = Arc::new(PhaseGate::new(RebuildPhase::BeforeSwitch));
    let _hook = install_gate(&store, gate.clone());

    let rebuild = {
        let store = store.clone();
        std::thread::spawn(move || store.rebuild_query_indexes())
    };
    gate.wait();
    commit_quad(&store, &graph, "during-switch");
    gate.release();
    rebuild.join().unwrap().unwrap();

    assert_exact(&store, &graph, &["base", "during-switch"]);
}

#[test]
fn failed_write_switch() {
    switch_write(StoredIndexState::Failed(
        "open-admission-failed".to_string(),
    ));
}

#[test]
fn building_write_switch() {
    switch_write(StoredIndexState::Building);
}

#[test]
fn retirement_with_write() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let graph = GraphId::new(TEST_GRAPH);
    let stop = Arc::new(AtomicBool::new(false));
    let scan_gate = Arc::new(PhaseGate::new(RebuildPhase::ScanPage));
    let scan_hook = install_gate(&store, scan_gate.clone());
    let rebuild = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || store.rebuild_query_with(&stop))
    };
    scan_gate.wait();
    stop.store(true, Ordering::SeqCst);
    scan_gate.release();
    assert!(matches!(
        rebuild.join().unwrap(),
        Err(StoreError::Cancelled)
    ));
    drop(scan_hook);

    let retire_gate = Arc::new(PhaseGate::new(RebuildPhase::Retire));
    let _retire_hook = install_gate(&store, retire_gate.clone());
    let repair = {
        let store = store.clone();
        std::thread::spawn(move || store.repair_query_indexes())
    };
    retire_gate.wait();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let writer = {
        let store = store.clone();
        let graph = graph.clone();
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            commit_quad(&store, &graph, "during-retire");
        })
    };
    started_rx.recv_timeout(PROGRESS_TIMEOUT).unwrap();
    retire_gate.release();
    repair.join().unwrap().unwrap();
    writer.join().unwrap();

    assert_exact(&store, &graph, &["base", "during-retire"]);
}

#[test]
fn cleanup_overlaps_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let graph = GraphId::new(TEST_GRAPH);
    let cleanup = Arc::new(CleanupGate::new());
    let hook_cleanup = cleanup.clone();
    let _hook = store.install_rebuild_hook(Arc::new(move |phase| hook_cleanup.run(phase)));

    let first = {
        let store = store.clone();
        std::thread::spawn(move || store.rebuild_query_indexes())
    };
    cleanup.gate.wait();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let second = {
        let store = store.clone();
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = store.rebuild_query_indexes();
            done_tx.send(result.is_ok()).unwrap();
            result
        })
    };
    started_rx.recv_timeout(PROGRESS_TIMEOUT).unwrap();
    assert!(
        done_rx.try_recv().is_err(),
        "second rebuild bypassed cleanup ownership"
    );
    cleanup.gate.release();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    assert!(done_rx.recv_timeout(PROGRESS_TIMEOUT).unwrap());

    assert_exact(&store, &graph, &["base"]);
}

#[test]
fn cap_drains_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup_store(dir.path());
    let graph = GraphId::new(TEST_GRAPH);
    let _limits = store.set_delta_limits(2, u64::MAX);
    let gate = Arc::new(PhaseGate::new(RebuildPhase::ScanPage));
    let _hook = install_gate(&store, gate.clone());
    let rebuild = {
        let store = store.clone();
        std::thread::spawn(move || store.rebuild_query_indexes())
    };
    gate.wait();
    commit_quad(&store, &graph, "delta-one");
    commit_quad(&store, &graph, "delta-two");
    let rejected = encode_quad(&store, &graph, "delta-three");
    assert!(matches!(
        try_commit(&store, &graph, rejected),
        Err(StoreError::QueryIndexCapacity)
    ));
    gate.release();
    rebuild.join().unwrap().unwrap();

    commit_quad(&store, &graph, "delta-three");
    assert_exact(
        &store,
        &graph,
        &["base", "delta-one", "delta-two", "delta-three"],
    );
}

fn parse_phase(value: &str) -> RebuildPhase {
    match value {
        "build-create" => RebuildPhase::BuildCreate,
        "scan-page" => RebuildPhase::ScanPage,
        "replay-page" => RebuildPhase::ReplayPage,
        "before-switch" => RebuildPhase::BeforeSwitch,
        "after-switch" => RebuildPhase::AfterSwitch,
        "cleanup-page" => RebuildPhase::CleanupPage,
        other => panic!("unknown crash phase {other}"),
    }
}

fn write_marker(path: &Path, value: &str) {
    let mut marker = std::fs::File::create(path).unwrap();
    marker.write_all(value.as_bytes()).unwrap();
    marker.sync_all().unwrap();
    std::fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

#[test]
fn crash_child() {
    let Some(mode) = std::env::var_os(CHILD_MODE) else {
        return;
    };
    let mode = mode.to_string_lossy().into_owned();
    let target = parse_phase(&mode);
    let root = std::path::PathBuf::from(std::env::var_os(CHILD_ROOT).unwrap());
    let marker = std::path::PathBuf::from(std::env::var_os(CHILD_MARKER).unwrap());
    let store = setup_store(&root);
    let graph = GraphId::new(TEST_GRAPH);
    let replay_seeded = Arc::new(AtomicBool::new(false));
    let cleanup_ready = Arc::new(AtomicBool::new(false));
    let hook_store = store.clone();
    let hook_graph = graph.clone();
    let hook_seeded = replay_seeded.clone();
    let hook_cleanup = cleanup_ready.clone();
    let hook_mode = mode.clone();
    let _hook = store.install_rebuild_hook(Arc::new(move |phase| {
        if target == RebuildPhase::ReplayPage
            && phase == RebuildPhase::ScanPage
            && !hook_seeded.swap(true, Ordering::SeqCst)
        {
            commit_quad(&hook_store, &hook_graph, "replayed");
        }
        if phase == RebuildPhase::AfterSwitch {
            hook_cleanup.store(true, Ordering::SeqCst);
        }
        let cleanup_target = target == RebuildPhase::CleanupPage;
        let should_crash =
            phase == target && (!cleanup_target || hook_cleanup.load(Ordering::SeqCst));
        if should_crash {
            hook_store.persist().unwrap();
            write_marker(&marker, &hook_mode);
            std::process::abort();
        }
    }));
    let _ = store.rebuild_query_indexes();
    panic!("child did not reach crash phase {mode}");
}

fn run_crash(mode: &str) {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new(TEST_GRAPH);
    let store_path = dir.path().join("store");
    let marker = dir.path().join("phase-reached");
    drop(setup_store(&store_path));
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(CHILD_TEST)
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_MARKER, &marker)
        .env(CHILD_ROOT, &store_path)
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "crash child unexpectedly succeeded at {mode}"
    );
    assert_eq!(std::fs::read_to_string(marker).unwrap(), mode);

    let reopened = GraphStore::open(&store_path).unwrap();
    reopened.repair_query_indexes().unwrap();
    if mode == "replay-page" {
        assert_exact(&reopened, &graph, &["base", "replayed"]);
    } else {
        assert_exact(&reopened, &graph, &["base"]);
    }
}

#[test]
fn crash_reopens_exact() {
    for mode in [
        "build-create",
        "scan-page",
        "replay-page",
        "before-switch",
        "after-switch",
        "cleanup-page",
    ] {
        run_crash(mode);
    }
}
