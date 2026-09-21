//! Runs deterministic read and cache workloads for the consistency catalog.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use craqle::{
    AllowAllAuthorizer, CraqleNode, PreparedQuery, QueryExecutionStatistics, QueryOptions,
    QueryResults, SearchRequest,
};
use serde_json::{Value, json};

use super::{Case, case_seed, graph_id, open_node, prepare_node, settle};

/// How a read's answer is compared across readers and against its expected size.
#[derive(Clone, Copy)]
enum RowCheck {
    /// Complete solutions compared as a multiset in a fixed variable order.
    Multiset,
    /// Rows whose order is part of the answer.
    Ordered,
    /// A limit without ordering: any subset of fixture subjects of the expected size.
    Limited,
}

/// One read case with the exact row count every correct answer has.
#[derive(Clone)]
struct ReadQuery {
    text: String,
    expected: usize,
    check: RowCheck,
    /// Fixture seed and subject count that every limited answer must come from.
    seed: usize,
    rows: usize,
}

/// Results of one measured read, validated after its timer stops.
enum ReadOutput {
    Rows(QueryResults),
    Hits(Vec<(String, String)>),
}

pub(super) fn run_reads(path: &Path, case: &Case) -> Value {
    let node = Arc::new(open_node(path, case));
    let prepare_started = Instant::now();
    let preloaded = prepare_node(&node, case);
    settle(&node, case);
    let prepare_ns = prepare_started.elapsed().as_nanos();
    let readers = case.usize("readers", 1).max(1);
    let warm = case.text("cache", "cold") == "warm";
    let query = read_query(case, preloaded);
    let prepared = (!query.text.starts_with("fts:"))
        .then(|| Arc::new(node.prepare_query(&query.text).unwrap()));
    let barrier = Arc::new(Barrier::new(readers));
    let origin = Instant::now();
    let handles: Vec<_> = (0..readers)
        .map(|_| {
            let node = Arc::clone(&node);
            let barrier = Arc::clone(&barrier);
            let query = query.clone();
            let prepared = prepared.clone();
            std::thread::spawn(move || {
                if warm {
                    black_box(run_query(&node, &query.text, prepared.as_deref()));
                }
                barrier.wait();
                // Results-only execution; validation and counters stay outside the interval.
                let started = Instant::now();
                let output = run_query(&node, &query.text, prepared.as_deref());
                let elapsed = started.elapsed().as_nanos();
                (started, elapsed, output)
            })
        })
        .collect();
    let mut latency_ns = Vec::new();
    let mut digests = Vec::new();
    let mut first_end: Option<u128> = None;
    let mut last_start: Option<u128> = None;
    for handle in handles {
        let (started, elapsed, output) = handle.join().unwrap();
        let start_offset = started.duration_since(origin).as_nanos();
        last_start = Some(last_start.map_or(start_offset, |value| value.max(start_offset)));
        let end_offset = start_offset + elapsed;
        first_end = Some(first_end.map_or(end_offset, |value| value.min(end_offset)));
        latency_ns.push(elapsed);
        digests.push(validate(&query, output));
    }
    assert!(
        digests.windows(2).all(|pair| pair[0] == pair[1]),
        "concurrent readers disagreed on the result"
    );
    let counters = prepared.as_deref().map(|prepared| {
        let mut options = QueryOptions::default();
        options.collect_costs = true;
        node.execute_prepared(&AllowAllAuthorizer, prepared, &options)
            .unwrap()
            .statistics
    });
    latency_ns.sort_unstable();
    json!({
        "completed": readers,
        "preloaded_rows": preloaded,
        "effective": {
            "readers": readers,
            "concurrent": readers > 1,
            "overlapped": readers == 1 || last_start < first_end,
            "query": case.text("query", case.text("selectivity", "default")),
            "warm_statistics": warm,
            "cold_storage_pages": false,
            "timed": "results-only",
        },
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "prepare_ns": prepare_ns,
        "work_ns": latency_ns.iter().sum::<u128>(),
        "latency_ns": latency_ns,
        "result_rows": query.expected,
        "counter_run": counters.map(read_counters),
        "output_digest": digests.first().cloned().unwrap_or_default(),
    })
}

fn read_counters(stats: QueryExecutionStatistics) -> Value {
    json!({
        "source_keys_read": stats.source_keys_read,
        "source_bytes_read": stats.source_bytes_read,
        "qv_keys_read": stats.qv_keys_read,
        "qv_bytes_read": stats.qv_bytes_read,
        "qv_fallback": stats.fallback_reason,
        "intermediate_rows": stats.intermediate_rows,
        "result_rows": stats.result_rows,
    })
}

/// One measured read: text search for the `fts` axis, otherwise a results-only query.
fn run_query(node: &CraqleNode, query: &str, prepared: Option<&PreparedQuery>) -> ReadOutput {
    let Some(prepared) = prepared else {
        let text = query
            .strip_prefix("fts:")
            .expect("unprepared reads are text searches");
        let hits = node
            .search(
                &AllowAllAuthorizer,
                SearchRequest {
                    query: text,
                    limit: 100,
                },
            )
            .unwrap();
        return ReadOutput::Hits(
            hits.into_iter()
                .map(|hit| (hit.graph_id, hit.subject_iri))
                .collect(),
        );
    };
    let mut options = QueryOptions::default();
    options.collect_plan_statistics = false;
    ReadOutput::Rows(
        node.execute_prepared(&AllowAllAuthorizer, prepared, &options)
            .unwrap()
            .results,
    )
}

/// Checks the exact row count and returns a digest that is equal for every correct answer.
fn validate(query: &ReadQuery, output: ReadOutput) -> String {
    let lines: Vec<String> = match output {
        ReadOutput::Hits(hits) => hits
            .into_iter()
            .map(|(graph, subject)| format!("{graph}\u{1f}{subject}"))
            .collect(),
        ReadOutput::Rows(QueryResults::Solutions(solutions)) => solutions
            .into_iter()
            .map(|row| {
                let mut cells: Vec<String> = row
                    .into_iter()
                    .map(|(name, term)| format!("{name}={}", term.0))
                    .collect();
                cells.sort();
                cells.join("\u{1f}")
            })
            .collect(),
        ReadOutput::Rows(other) => vec![format!("{other:?}")],
    };
    assert_eq!(
        query.expected,
        lines.len(),
        "{} returned the wrong number of rows",
        query.text
    );
    let mut lines = lines;
    match query.check {
        RowCheck::Ordered => {}
        RowCheck::Multiset => lines.sort(),
        RowCheck::Limited => {
            let prefix = format!("s=<urn:catalog:s:{}:", query.seed);
            for line in &lines {
                let index = line
                    .strip_prefix(&prefix)
                    .and_then(|rest| rest.strip_suffix('>'))
                    .and_then(|index| index.parse::<usize>().ok());
                assert!(
                    index.is_some_and(|index| index < query.rows),
                    "{line} is not a fixture subject"
                );
            }
            return format!("limited:{}", lines.len());
        }
    }
    blake3::hash(lines.join("\n").as_bytes())
        .to_hex()
        .to_string()
}

/// Builds each read from the fixture's own seed, with its exact expected row count.
fn read_query(case: &Case, rows: usize) -> ReadQuery {
    let seed = case_seed(case);
    let page = rows.min(100);
    let (text, expected, check) = match case.text("query", case.text("selectivity", "default")) {
        "fts" => ("fts:catalog".to_owned(), page, RowCheck::Ordered),
        "values" => (
            format!("SELECT ?s WHERE {{ VALUES ?s {{ <urn:catalog:s:{seed}:0> }} ?s ?p ?o }}"),
            1,
            RowCheck::Multiset,
        ),
        "join" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> ?o . ?s <urn:catalog:p> ?x }".to_owned(),
            rows,
            RowCheck::Multiset,
        ),
        "sort" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } ORDER BY ?o LIMIT 100".to_owned(),
            page,
            RowCheck::Ordered,
        ),
        "distinct" => (
            "SELECT DISTINCT ?o WHERE { ?s <urn:catalog:p> ?o }".to_owned(),
            rows,
            RowCheck::Multiset,
        ),
        "group" => (
            "SELECT (COUNT(?s) AS ?count) WHERE { ?s <urn:catalog:p> ?o }".to_owned(),
            1,
            RowCheck::Multiset,
        ),
        "string" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> ?o FILTER(CONTAINS(STR(?o), \"token\")) }"
                .to_owned(),
            rows,
            RowCheck::Multiset,
        ),
        "path" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p>+ ?o } LIMIT 100".to_owned(),
            page,
            RowCheck::Limited,
        ),
        "high" => (
            format!("SELECT ?s WHERE {{ ?s <urn:catalog:p> \"catalog token {seed} 0\" }}"),
            1,
            RowCheck::Multiset,
        ),
        // A lookup that must find nothing, kept apart from the positive cases.
        "absent" => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> \"catalog token absent\" }".to_owned(),
            0,
            RowCheck::Multiset,
        ),
        _ => (
            "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } LIMIT 100".to_owned(),
            page,
            RowCheck::Limited,
        ),
    };
    ReadQuery {
        text,
        expected,
        check,
        seed,
        rows,
    }
}

pub(super) fn run_cache(path: &Path, case: &Case) -> Value {
    let node = Arc::new(open_node(path, case));
    let rows = prepare_node(&node, case);
    settle(&node, case);
    let readers = case.usize("readers", 1).max(1);
    let samples = case.usize("samples", 256).max(readers);
    let hit_percent = case.usize("hit_percent", 100).min(100);
    let barrier = Arc::new(Barrier::new(readers));
    let seed = case_seed(case);
    let started = Instant::now();
    let handles: Vec<_> = (0..readers)
        .map(|reader| {
            let node = Arc::clone(&node);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut latencies = Vec::new();
                let mut output = 0u64;
                for ordinal in (reader..samples).step_by(readers) {
                    let subject = if ordinal % 100 < hit_percent {
                        format!("urn:catalog:s:{seed}:{}", ordinal % rows)
                    } else {
                        format!("urn:catalog:miss:{reader}:{ordinal}")
                    };
                    let query = format!("ASK {{ <{subject}> <urn:catalog:p> ?o }}");
                    let request = Instant::now();
                    let result = node.query(&AllowAllAuthorizer, &query).unwrap();
                    latencies.push(request.elapsed().as_nanos());
                    output = format!("{result:?}").bytes().fold(output, |digest, byte| {
                        digest
                            .wrapping_mul(1_099_511_628_211)
                            .wrapping_add(byte as u64)
                    });
                    black_box(result);
                }
                (latencies, output)
            })
        })
        .collect();
    let mut latency_ns = Vec::new();
    let mut per_reader = Vec::new();
    let mut output_digest = 0u64;
    for handle in handles {
        let (samples, output) = handle.join().unwrap();
        output_digest ^= output;
        per_reader.push(samples.len());
        latency_ns.extend(samples);
    }
    latency_ns.sort_unstable();
    json!({
        "completed": latency_ns.len(),
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "hit_percent": hit_percent,
        "latency_ns": latency_ns,
        "per_reader": per_reader,
        "output_digest": format!("{output_digest:016x}"),
        "work_ns": started.elapsed().as_nanos(),
    })
}
