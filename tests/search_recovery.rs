//! Search coverage across a change of build features.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT
//!
//! The three phases run as separate processes over one shared directory, so
//! they need separate `cargo test` invocations with different features. Each
//! phase does nothing unless `CRAQLE_SEARCH_PHASE` and `CRAQLE_SEARCH_DIR`
//! name it, which keeps an ordinary suite run independent of the others.

use craqle::{AllowAllAuthorizer, CraqleNode, GraphId, GraphPolicy};

const GRAPH: &str = "urn:test:feature-transition";

fn phase_dir(phase: &str) -> Option<std::path::PathBuf> {
    if std::env::var("CRAQLE_SEARCH_PHASE").ok()? != phase {
        return None;
    }
    Some(std::path::PathBuf::from(
        std::env::var("CRAQLE_SEARCH_DIR").ok()?,
    ))
}

fn crate_document(description: &str) -> String {
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": GRAPH}
            },
            {
                "@id": GRAPH,
                "@type": "Dataset",
                "name": "Feature Transition Crate",
                "description": description,
                "datePublished": "2025-01-01",
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
            }
        ]
    })
    .to_string()
}

fn apply(node: &CraqleNode, description: &str) {
    node.apply_rocrate_document_with_policy(
        &AllowAllAuthorizer,
        GraphId::new(GRAPH),
        &crate_document(description),
        GraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
    )
    .unwrap();
}

/// Phase one: an indexing build records the original text.
#[cfg(feature = "search")]
#[test]
fn phase_writes_index() {
    let Some(dir) = phase_dir("write") else {
        return;
    };
    let node = CraqleNode::open(&dir).unwrap();
    apply(&node, "oldneedle");
    node.flush_search_updates().unwrap();

    assert_eq!(1, matches(&node, "oldneedle"));
}

/// Phase two: a build with no index changes the source text. It may coalesce
/// what it owes, but it must not report that anything was indexed.
#[cfg(not(feature = "search"))]
#[test]
fn phase_mutates_source() {
    let Some(dir) = phase_dir("mutate") else {
        return;
    };
    let node = CraqleNode::open(&dir).unwrap();
    apply(&node, "newneedle");
    node.flush_search_updates().unwrap();
}

/// Phase three: an indexing build must repair the index it left behind rather
/// than certify it because the schema still matches.
#[cfg(feature = "search")]
#[test]
fn phase_repairs_index() {
    let Some(dir) = phase_dir("verify") else {
        return;
    };
    let node = CraqleNode::open(&dir).unwrap();
    node.flush_search_updates().unwrap();

    assert_eq!(
        1,
        matches(&node, "newneedle"),
        "the text written while search was disabled never reached the index"
    );
    assert_eq!(
        0,
        matches(&node, "oldneedle"),
        "the index still serves text the store no longer holds"
    );
}

#[cfg(feature = "search")]
fn matches(node: &CraqleNode, needle: &str) -> usize {
    node.search(
        &AllowAllAuthorizer,
        craqle::SearchRequest {
            query: needle,
            limit: 10,
        },
    )
    .unwrap()
    .len()
}
