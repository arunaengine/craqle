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

    /// Axis names set by this case that the selected workload does not implement.
    pub fn unsupported(&self, supported: &[&str]) -> Vec<String> {
        self.0
            .as_object()
            .into_iter()
            .flat_map(|axes| axes.keys())
            .filter(|name| !supported.contains(&name.as_str()))
            .cloned()
            .collect()
    }

    /// A text axis limited to `allowed`, whose first value is the default.
    pub fn choice<'a>(&'a self, name: &str, allowed: &[&'a str]) -> Result<&'a str, String> {
        let value = self.text(name, allowed[0]);
        allowed
            .iter()
            .find(|candidate| **candidate == value)
            .copied()
            .ok_or_else(|| format!("{name}={value} is not implemented"))
    }
}

/// Axes each catalog workload really varies; other axes are rejected, not ignored.
pub fn supported_axes(id: &str) -> &'static [&'static str] {
    match id {
        "B01" => &["writers", "graphs", "replicated", "search", "persist"],
        "B04" => &["changed", "related", "writers", "graphs", "persist"],
        "B02" | "B12" | "B13" => &[
            "rows",
            "query",
            "selectivity",
            "cache",
            "readers",
            "persist",
        ],
        "B15" => &["readers", "hit_percent", "samples", "rows", "persist"],
        "B03" | "B19" => &["rate", "rows", "persist"],
        "B05" | "B06" => &[
            "rows", "history", "overlap", "actors", "deleted", "suffix", "persist",
        ],
        "B11" => &[
            "page",
            "readable_per_mille",
            "hits",
            "graphs",
            "subject_bytes",
            "persist",
        ],
        "B16" => &["stores", "search", "persist"],
        "B18" => &["input", "bytes", "persist"],
        "B20" => &["rate", "restart", "rows", "persist"],
        _ => &[],
    }
}

/// Rejects text axis values the selected workload would otherwise replace with a default.
pub fn check_values(id: &str, case: &Case) -> Result<(), String> {
    case.choice("persist", &["sync-all", "buffer", "sync-data"])?;
    match id {
        "B12" => case
            .choice(
                "query",
                &[
                    "default", "values", "join", "sort", "distinct", "group", "string", "path",
                    "fts",
                ],
            )
            .map(drop),
        "B13" => case
            .choice("selectivity", &["low", "high"])
            .and(case.choice("cache", &["cold", "warm"]))
            .map(drop),
        "B18" => case.choice("input", &["valid", "invalid"]).map(drop),
        _ => Ok(()),
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
