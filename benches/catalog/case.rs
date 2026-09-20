//! Parses catalog axes and estimates bounded workload sizes.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use serde_json::Value;

pub const CASE_ENV: &str = "CRAQLE_BENCH_CASE";
pub const ID_ENV: &str = "CRAQLE_BENCH_ID";
pub const CAP_ENV: &str = "CRAQLE_BENCH_BYTE_CAP";

pub struct Case(pub Value);

impl Case {
    pub fn usize(&self, name: &str, default: usize) -> usize {
        self.0[name]
            .as_u64()
            .map_or(default, |value| value as usize)
    }

    pub fn bool(&self, name: &str, default: bool) -> bool {
        self.0[name].as_bool().unwrap_or(default)
    }

    pub fn text<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        self.0[name].as_str().unwrap_or(default)
    }
}

pub fn estimate_bytes(case: &Case) -> usize {
    let rows = work_rows(case);
    let changed = case.usize("changed", case.usize("jobs", case.usize("suffix", 100)));
    let subject = case.usize("subject_bytes", 64);
    rows.saturating_add(changed)
        .saturating_mul(192usize.saturating_add(subject))
        .saturating_add(case.usize("bytes", 0))
}

pub fn work_rows(case: &Case) -> usize {
    [
        "rows", "history", "hits", "future", "cache", "jobs", "eligible",
    ]
    .into_iter()
    .map(|name| case.usize(name, 0))
    .max()
    .unwrap_or(0)
    .max(1_000)
}

pub fn case_seed(case: &Case) -> usize {
    case.0
        .to_string()
        .bytes()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            hash.wrapping_mul(0x100000001b3).wrapping_add(byte as u64)
        }) as usize
}
