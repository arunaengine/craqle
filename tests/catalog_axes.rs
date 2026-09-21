//! Checks that catalog benchmark axes are implemented rather than silently ignored.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

// The shared benchmark module carries axis helpers this test does not call.
#[allow(dead_code)]
#[path = "../benches/catalog/case.rs"]
mod catalog_case;

use catalog_case::{Case, MAX_ACTORS, check_values, merge_actor, supported_axes};
use serde_json::json;

#[test]
fn unimplemented_axes_rejected() {
    let requested = Case(json!({"rows": 10_000, "state": "debt"}));
    assert_eq!(
        requested.unsupported(supported_axes("B02")),
        vec!["state".to_owned()],
        "an unimplemented axis must be reported, not measured as its default"
    );
    assert!(
        Case(json!({"rows": 10_000}))
            .unsupported(supported_axes("B02"))
            .is_empty()
    );
    assert_eq!(
        Case(json!({"cache": 4_000})).unsupported(supported_axes("B04")),
        vec!["cache".to_owned()],
        "the cache-size axis is not implemented by the write workload"
    );
    assert!(
        Case(json!({"readers": 4}))
            .unsupported(supported_axes("B15"))
            .is_empty()
    );
}

#[test]
fn unknown_values_rejected() {
    assert!(check_values("B12", &Case(json!({"query": "fts"}))).is_ok());
    assert!(check_values("B12", &Case(json!({"query": "regex"}))).is_err());
    assert!(
        check_values(
            "B13",
            &Case(json!({"selectivity": "high", "cache": "warm"}))
        )
        .is_ok()
    );
    assert!(check_values("B13", &Case(json!({"cache": "hot"}))).is_err());
    assert!(check_values("B18", &Case(json!({"input": "nested"}))).is_err());
    assert!(check_values("B01", &Case(json!({"persist": "none"}))).is_err());
    assert!(check_values("B02", &Case(json!({"query": "regex"}))).is_err());
    assert!(check_values("B12", &Case(json!({"cache": "hot"}))).is_err());
    assert!(check_values("B12", &Case(json!({"query": "absent"}))).is_ok());
}

#[test]
fn malformed_numbers_rejected() {
    for (label, case) in [
        ("negative", json!({"rows": -1})),
        ("fraction", json!({"rows": 1500.5})),
        ("text number", json!({"rows": "10000"})),
        ("below fixture", json!({"rows": 10})),
        ("zero readers", json!({"readers": 0})),
        ("percent", json!({"hit_percent": 101})),
        ("per mille", json!({"readable_per_mille": 1_001})),
        ("no actors", json!({"actors": 0})),
        ("too many actors", json!({"actors": MAX_ACTORS + 1})),
        ("text flag", json!({"search": "yes"})),
        ("numeric text", json!({"query": 3})),
        ("raised samples", json!({"readers": 8, "samples": 4})),
        ("raised changes", json!({"writers": 8, "changed": 4})),
    ] {
        assert!(check_values("B05", &Case(case)).is_err(), "{label}");
    }
    let accepted = json!({"rows": 10_000, "actors": MAX_ACTORS, "overlap": 100});
    assert!(check_values("B05", &Case(accepted)).is_ok());
}

#[test]
fn merge_actors_distinct() {
    let fixtures = [[0x43; 32], [0x44; 32]];
    let mut seen = std::collections::HashSet::new();
    // Covers 128, where single-byte IDs overflowed, and 195, where they wrapped onto 0x43.
    for index in 0..=MAX_ACTORS {
        let actor = merge_actor(index);
        assert!(!fixtures.contains(&actor), "actor {index}");
        assert!(seen.insert(actor), "actor {index} repeats an earlier ID");
    }
}
