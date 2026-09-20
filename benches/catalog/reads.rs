//! Runs deterministic read and cache workloads for the consistency catalog.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::hint::black_box;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use craqle::AllowAllAuthorizer;
use serde_json::{Value, json};

use super::{Case, case_seed, graph_id, open_node, prepare_node, settle};

pub(super) fn run_reads(path: &Path, case: &Case) -> Value {
    let node = open_node(path, case);
    let preloaded = prepare_node(&node, case);
    settle(&node, case);
    let readers = case.usize("readers", 1).max(1);
    let query = query_text(case);
    let iterations = readers + usize::from(case.text("cache", "cold") == "warm");
    let started = Instant::now();
    let mut stats = None;
    let mut output = blake3::Hasher::new();
    for _ in 0..iterations {
        let execution = node
            .query_with_statistics(&AllowAllAuthorizer, &query)
            .unwrap();
        output.update(format!("{:?}", execution.results).as_bytes());
        black_box(&execution.results);
        stats = Some(execution.statistics);
    }
    let stats = stats.unwrap();
    json!({
        "completed": iterations,
        "preloaded_rows": preloaded,
        "fingerprint": format!("{:?}", node.graph_fingerprint(&graph_id(0)).unwrap()),
        "work_ns": started.elapsed().as_nanos(),
        "source_keys_read": stats.source_keys_read,
        "source_bytes_read": stats.source_bytes_read,
        "qv_keys_read": stats.qv_keys_read,
        "qv_bytes_read": stats.qv_bytes_read,
        "qv_fallback": stats.fallback_reason,
        "intermediate_rows": stats.intermediate_rows,
        "result_rows": stats.result_rows,
        "output_digest": output.finalize().to_hex().to_string(),
    })
}

fn query_text(case: &Case) -> String {
    match case.text("query", case.text("selectivity", "default")) {
        "values" => "SELECT ?s WHERE { VALUES ?s { <urn:catalog:s:0:0> } ?s ?p ?o }".into(),
        "join" => "SELECT ?s WHERE { ?s <urn:catalog:p> ?o . ?s <urn:catalog:p> ?x }".into(),
        "sort" => "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } ORDER BY ?o LIMIT 100".into(),
        "distinct" => "SELECT DISTINCT ?o WHERE { ?s <urn:catalog:p> ?o }".into(),
        "group" => "SELECT (COUNT(?s) AS ?count) WHERE { ?s <urn:catalog:p> ?o }".into(),
        "string" => {
            "SELECT ?s WHERE { ?s <urn:catalog:p> ?o FILTER(CONTAINS(STR(?o), \"token\")) }".into()
        }
        "path" => "SELECT ?s WHERE { ?s <urn:catalog:p>+ ?o } LIMIT 100".into(),
        "high" => "SELECT ?s WHERE { ?s <urn:catalog:p> \"catalog token 0 0\" }".into(),
        _ => "SELECT ?s WHERE { ?s <urn:catalog:p> ?o } LIMIT 100".into(),
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
                    output = format!("{result:?}").bytes().fold(output, |digest, byte| {
                        digest
                            .wrapping_mul(1_099_511_628_211)
                            .wrapping_add(byte as u64)
                    });
                    black_box(result);
                    latencies.push(request.elapsed().as_nanos());
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
