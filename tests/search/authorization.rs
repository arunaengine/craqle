//! Authorization-aware paging over the full-text index.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#![cfg(feature = "search")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use craqle::{
    Action, AuthorizationError, CraqleNode, CreateCrateRequest, GrantAuthorizer, GraphId,
    GraphPolicy, PermissionGrant, PermissionLevel, SearchRequest,
};

const NEEDLE: &str = "authneedle";
const WAIT: Duration = Duration::from_secs(180);

struct GateRelease(Option<mpsc::Sender<()>>);

impl GateRelease {
    fn new(sender: mpsc::Sender<()>) -> Self {
        Self(Some(sender))
    }

    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

fn writer_auth() -> GrantAuthorizer {
    GrantAuthorizer::new(vec![PermissionGrant::new("/t/**", PermissionLevel::Write)])
}

fn policy(public: bool) -> GraphPolicy {
    GraphPolicy {
        public,
        permission_paths: vec!["/t/auth-fixture".to_string()],
    }
}

fn seed_crate(node: &CraqleNode, graph: &GraphId, description: &str, public: bool) {
    node.create_crate(
        &writer_auth(),
        CreateCrateRequest::new(
            graph.clone(),
            "Authorization Fixture",
            description,
            "2025-01-01",
            None,
            policy(public),
        ),
    )
    .unwrap();
}

fn hits(node: &CraqleNode, limit: usize) -> Vec<String> {
    node.search(
        &GrantAuthorizer::default(),
        SearchRequest {
            query: NEEDLE,
            limit,
        },
    )
    .unwrap()
    .into_iter()
    .map(|hit| format!("{} {}", hit.graph_id, hit.subject_iri))
    .collect()
}

/// A one-row page over a corpus that is almost entirely unreadable must still
/// answer with the one readable match.
#[test]
fn sparse_authorization_page() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();

    for index in 0..80 {
        seed_crate(
            &node,
            &GraphId::new(&format!("urn:test:auth-hidden-{index}")),
            NEEDLE,
            false,
        );
    }
    let visible = GraphId::new("urn:test:auth-visible");
    let padding = "filler ".repeat(200);
    seed_crate(&node, &visible, &format!("{NEEDLE} {padding}"), true);
    node.flush_search_updates().unwrap();

    let page = hits(&node, 1);
    assert_eq!(1, page.len(), "the readable match must fill the page");
    assert!(page[0].starts_with(visible.as_str()));
}

/// No readable graph is a complete empty answer, not a budget failure.
#[test]
fn no_readable_matches() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();

    for index in 0..8 {
        seed_crate(
            &node,
            &GraphId::new(&format!("urn:test:auth-closed-{index}")),
            NEEDLE,
            false,
        );
    }
    node.flush_search_updates().unwrap();

    assert!(hits(&node, 10).is_empty());
}

#[test]
fn request_pins_searcher() {
    let directory = tempfile::tempdir().unwrap();
    let node = Arc::new(CraqleNode::open(directory.path()).unwrap());
    let graph = GraphId::new("urn:test:auth-pinned");
    seed_crate(&node, &graph, NEEDLE, true);
    node.flush_search_updates().unwrap();
    let original = hits(&node, 10);
    assert_eq!(original.len(), 1);

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut release = GateRelease::new(release_tx);
    let (done_tx, done_rx) = mpsc::channel();
    let search_node = Arc::clone(&node);
    let handle = std::thread::spawn(move || {
        let first = AtomicBool::new(true);
        let release_rx = std::sync::Mutex::new(release_rx);
        let auth = move |_: &GraphId, _: &GraphPolicy, _: Action| {
            if first.swap(false, Ordering::SeqCst) {
                reached_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(WAIT)
                    .expect("authorization gate was not released");
            }
            Ok(())
        };
        let result = search_node.search(
            &auth,
            SearchRequest {
                query: NEEDLE,
                limit: 10,
            },
        );
        done_tx.send(result).unwrap();
    });

    reached_rx
        .recv_timeout(WAIT)
        .expect("search did not reach authorization");
    node.add_data_entity(
        &writer_auth(),
        &graph,
        "data/added.txt",
        "http://schema.org/MediaObject",
        NEEDLE,
    )
    .unwrap();
    node.flush_search_updates().unwrap();
    release.release();

    let in_flight = done_rx
        .recv_timeout(WAIT)
        .expect("in-flight search did not finish")
        .unwrap()
        .into_iter()
        .map(|hit| format!("{} {}", hit.graph_id, hit.subject_iri))
        .collect::<Vec<_>>();
    handle.join().unwrap();
    assert_eq!(in_flight, original);
    let next = hits(&node, 10);
    assert_eq!(next.len(), 2);
    assert!(next.iter().any(|hit| hit.contains("data/added.txt")));
}

#[test]
fn revocation_filters_response() {
    let directory = tempfile::tempdir().unwrap();
    let node = Arc::new(CraqleNode::open(directory.path()).unwrap());
    let graph = GraphId::new("urn:test:auth-revoked");
    seed_crate(&node, &graph, NEEDLE, true);
    node.flush_search_updates().unwrap();

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut release = GateRelease::new(release_tx);
    let (done_tx, done_rx) = mpsc::channel();
    let search_node = Arc::clone(&node);
    let handle = std::thread::spawn(move || {
        let first = AtomicBool::new(true);
        let release_rx = std::sync::Mutex::new(release_rx);
        let auth = move |graph: &GraphId, policy: &GraphPolicy, action: Action| {
            if first.swap(false, Ordering::SeqCst) {
                reached_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(WAIT)
                    .expect("authorization gate was not released");
            }
            if policy.public {
                Ok(())
            } else {
                Err(AuthorizationError::PermissionDenied {
                    action,
                    graph: graph.as_str().to_owned(),
                })
            }
        };
        let result = search_node.search(
            &auth,
            SearchRequest {
                query: NEEDLE,
                limit: 10,
            },
        );
        done_tx.send(result).unwrap();
    });

    reached_rx
        .recv_timeout(WAIT)
        .expect("search did not reach authorization");
    node.set_graph_policy(&writer_auth(), &graph, policy(false))
        .unwrap();
    release.release();

    let response = done_rx
        .recv_timeout(WAIT)
        .expect("revoked search did not finish")
        .unwrap();
    handle.join().unwrap();
    assert!(response.is_empty());
}

/// Equal scores must order by a stable application identity, so a rebuild that
/// moves documents between segments cannot reorder a page.
#[test]
fn tie_order_stable() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();

    for index in 0..24 {
        seed_crate(
            &node,
            &GraphId::new(&format!("urn:test:auth-tie-{index:02}")),
            NEEDLE,
            true,
        );
    }
    node.flush_search_updates().unwrap();

    let before = hits(&node, 17);
    assert_eq!(before.len(), 17);
    assert_eq!(hits(&node, 5), before[..5].to_vec());

    node.reindex_search().unwrap();
    node.flush_search_updates().unwrap();

    assert_eq!(
        before,
        hits(&node, 17),
        "equal-scoring hits reordered across a full rebuild"
    );
    assert_eq!(hits(&node, 5), before[..5].to_vec());
}
