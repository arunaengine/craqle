//! Exercises the shared process memory budget across many live default stores.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use craqle::CraqleNode;

#[test]
fn many_default_stores() {
    let directory = tempfile::tempdir().unwrap();
    let nodes = (0..40)
        .map(|index| CraqleNode::open(directory.path().join(index.to_string())).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 40);
}
