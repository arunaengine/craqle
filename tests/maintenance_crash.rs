//! Exercises deterministic subprocess crash checkpoints and restart inspection.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use craqle::{AllowAllAuthorizer, CraqleNode, CreateCrateRequest, GraphId, GraphPolicy};

const CHILD_MODE: &str = "CRAQLE_CRASH_MODE";
const CHILD_ROOT: &str = "CRAQLE_CRASH_ROOT";
const CHECKPOINT: &str = "CRAQLE_CHECKPOINT";

fn graph_id() -> GraphId {
    GraphId::new("urn:test:maintenance-crash")
}

fn policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/tests/**".to_string()],
    }
}

fn emit_checkpoint(name: &str) {
    println!("{CHECKPOINT} {name}");
    std::io::stdout().flush().unwrap();
}

fn child_main(mode: &str, root: &Path) {
    let graph = graph_id();
    let node = CraqleNode::open(root).unwrap();
    if mode == "before" {
        emit_checkpoint("before-source");
        std::thread::park();
    }
    if mode == "after" {
        node.create_crate(
            &AllowAllAuthorizer,
            CreateCrateRequest::new(
                graph,
                "crash fixture",
                "durable restart fixture",
                "2026-09-20",
                None,
                policy(),
            ),
        )
        .unwrap();
        emit_checkpoint("after-source");
        std::thread::park();
    }
    emit_checkpoint("stuck");
    std::thread::park();
}

fn spawn_child(mode: &str, root: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("crash_child")
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_ROOT, root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}

fn wait_checkpoint(child: &mut Child, expected: &str) {
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let _ = sender.send(line);
        }
    });
    let wanted = format!("{CHECKPOINT} {expected}");
    loop {
        match receiver.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(line)) if line == wanted => return,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                stop_child(child);
                panic!("failed to read child checkpoint: {error}");
            }
            Err(error) => {
                stop_child(child);
                panic!("child did not reach its checkpoint: {error}");
            }
        }
    }
}

fn stop_child(child: &mut Child) {
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(
        !status.success(),
        "crash child unexpectedly exited successfully"
    );
}

#[test]
fn crash_child() {
    let Some(mode) = std::env::var_os(CHILD_MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).unwrap());
    child_main(&mode.to_string_lossy(), &root);
}

#[test]
fn watchdog_stops_child() {
    let root = tempfile::tempdir().unwrap();
    let mut child = spawn_child("hang", root.path());
    wait_checkpoint(&mut child, "stuck");
    stop_child(&mut child);
}

#[test]
fn precommit_state_absent() {
    let root = tempfile::tempdir().unwrap();
    let mut child = spawn_child("before", root.path());
    wait_checkpoint(&mut child, "before-source");
    stop_child(&mut child);

    let node = CraqleNode::open(root.path()).unwrap();
    assert!(!node.contains_graph(&graph_id()).unwrap());
}

#[test]
fn persisted_state_reopens() {
    let root = tempfile::tempdir().unwrap();
    let mut child = spawn_child("after", root.path());
    wait_checkpoint(&mut child, "after-source");
    stop_child(&mut child);

    let node = CraqleNode::open(root.path()).unwrap();
    assert!(node.contains_graph(&graph_id()).unwrap());
    assert!(node.graph_fingerprint(&graph_id()).unwrap().0 > 0);
}
