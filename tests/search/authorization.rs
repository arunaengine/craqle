//! Authorization-aware paging over the full-text index.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#![cfg(feature = "search")]

use craqle::{
    CraqleNode, CreateCrateRequest, GrantAuthorizer, GraphId, GraphPolicy, PermissionGrant,
    PermissionLevel, SearchRequest,
};

const NEEDLE: &str = "authneedle";

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

/// Equal scores must order by a stable application identity, so a rebuild that
/// moves documents between segments cannot reorder a page.
#[test]
fn order_survives_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();

    for index in 0..12 {
        seed_crate(
            &node,
            &GraphId::new(&format!("urn:test:auth-tie-{index:02}")),
            NEEDLE,
            true,
        );
    }
    node.flush_search_updates().unwrap();

    let before = hits(&node, 12);
    assert_eq!(12, before.len());

    node.reindex_search().unwrap();
    node.flush_search_updates().unwrap();

    assert_eq!(
        before,
        hits(&node, 12),
        "equal-scoring hits reordered across a full rebuild"
    );
}
