//! Checks that catalog benchmark axes are implemented rather than silently ignored.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

// The shared benchmark module carries axis helpers this test does not call.
#[allow(dead_code)]
#[path = "../benches/catalog/case.rs"]
mod catalog_case;

use catalog_case::{Case, check_values, supported_axes};
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
}
