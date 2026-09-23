//! Checks that catalog benchmark axes are implemented rather than silently ignored.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

// The shared benchmark module carries axis helpers this test does not call.
#[allow(dead_code)]
#[path = "../benches/catalog/case.rs"]
mod catalog_case;

#[allow(dead_code)]
#[path = "../benches/catalog/oracle.rs"]
mod catalog_oracle;

use std::collections::HashMap;

use catalog_case::{Case, MAX_ACTORS, check_values, merge_actor, supported_axes};
use catalog_oracle::{Fixture, canonical_rows, check_hits, check_rows, expected};
use craqle::{EncodedTerm, QueryResults};
use serde_json::json;

const FIXTURE: Fixture = Fixture {
    seed: 7,
    rows: 150,
    graphs: 3,
};

fn solutions(rows: Vec<Vec<(&str, String)>>) -> QueryResults {
    QueryResults::Solutions(
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(name, term)| (name.to_owned(), EncodedTerm(term)))
                    .collect::<HashMap<_, _>>()
            })
            .collect(),
    )
}

fn subject_rows(indexes: impl IntoIterator<Item = usize>) -> QueryResults {
    solutions(
        indexes
            .into_iter()
            .map(|index| vec![("s", format!("<{}>", FIXTURE.subject(index)))])
            .collect(),
    )
}

/// Checks one engine output for a named read against the fixture's own answer.
fn verdict(query: &str, results: &QueryResults) -> Result<String, String> {
    let expected = expected(query, &FIXTURE)?;
    check_rows(&expected, canonical_rows(results, expected.variables)?)
}

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

#[test]
fn oracle_accepts_answers() {
    assert!(verdict("values", &subject_rows([0])).is_ok());
    assert!(verdict("join", &subject_rows((0..150).rev())).is_ok());
    assert!(verdict("absent", &subject_rows([])).is_ok());
    let count = "\"150\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned();
    assert!(verdict("group", &solutions(vec![vec![("count", count)]])).is_ok());
    // Two different valid selections under an unordered limit are both accepted.
    assert!(verdict("default", &subject_rows(0..100)).is_ok());
    assert!(verdict("default", &subject_rows(50..150)).is_ok());
    let mut sorted: Vec<usize> = (0..150).collect();
    sorted.sort_by_key(|index| FIXTURE.object(*index));
    assert!(verdict("sort", &subject_rows(sorted[..100].to_vec())).is_ok());
}

#[test]
fn oracle_rejects_corruption() {
    let integer = "http://www.w3.org/2001/XMLSchema#integer";
    let mut sorted: Vec<usize> = (0..150).collect();
    sorted.sort_by_key(|index| FIXTURE.object(*index));
    let mut swapped = sorted[..100].to_vec();
    swapped.swap(3, 4);
    let mut doubled: Vec<usize> = (0..150).collect();
    doubled[149] = 0;
    let cases = [
        (
            "wrong count",
            "group",
            solutions(vec![vec![("count", format!("\"149\"^^<{integer}>"))]]),
        ),
        (
            "untyped count",
            "group",
            solutions(vec![vec![("count", "\"150\"".to_owned())]]),
        ),
        ("repeated subject", "default", subject_rows([0; 100])),
        ("foreign subject", "default", subject_rows(100..200)),
        ("short limit", "default", subject_rows(0..99)),
        ("wrong order", "sort", subject_rows(swapped)),
        ("missing binding", "values", solutions(vec![vec![]])),
        ("substituted binding", "values", subject_rows([1])),
        ("duplicate row", "join", subject_rows(doubled)),
        (
            "extra variable",
            "values",
            solutions(vec![vec![
                ("s", format!("<{}>", FIXTURE.subject(0))),
                ("p", "<urn:catalog:p>".to_owned()),
            ]]),
        ),
        ("negative lookup found rows", "absent", subject_rows([0])),
    ];
    for (label, query, results) in cases {
        assert!(verdict(query, &results).is_err(), "{label} was accepted");
    }
}

#[test]
fn oracle_rejects_hits() {
    let expected = expected("fts", &FIXTURE).unwrap();
    let hit = |index: usize| (FIXTURE.graph(index), FIXTURE.subject(index));
    let valid: Vec<_> = (0..100).map(hit).collect();
    assert!(check_hits(&expected, &valid).is_ok());
    let mut wrong_graph = valid.clone();
    wrong_graph[5].0 = FIXTURE.graph(6);
    let mut duplicate = valid.clone();
    duplicate[5] = hit(4);
    let mut foreign = valid.clone();
    foreign[5].1 = "urn:catalog:s:8:5".to_owned();
    for (label, hits) in [
        ("wrong graph", wrong_graph),
        ("duplicate hit", duplicate),
        ("foreign subject", foreign),
        ("short page", valid[..99].to_vec()),
    ] {
        assert!(
            check_hits(&expected, &hits).is_err(),
            "{label} was accepted"
        );
    }
}
