//! Compares query result collection allocations and runtime.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use craqle::EncodedTerm;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::{json, to_string};

#[path = "support.rs"]
mod support;

use support::fixture::{binary_blake3, repository_commit};

struct CountingAllocator;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record_allocation(bytes: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

// `try_update` needs Rust 1.95; the deprecated name stays until the MSRV moves.
#[allow(deprecated)]
fn record_deallocation(bytes: usize) {
    let _ = LIVE_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(bytes))
    });
}

// SAFETY: operations delegate to `System` with the original pointer and layout.
// The atomics only observe sizes and never alter ownership or results.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegated with the caller-provided layout.
        let pointer = unsafe { System.alloc(layout) };
        if COUNTING.load(Ordering::Relaxed) && !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegated with the caller-provided layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if COUNTING.load(Ordering::Relaxed) && !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if COUNTING.load(Ordering::Relaxed) {
            record_deallocation(layout.size());
        }
        // SAFETY: delegated with the original pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: delegated with the original pointer and layout.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if COUNTING.load(Ordering::Relaxed) && !new_pointer.is_null() {
            record_deallocation(layout.size());
            record_allocation(new_size);
        }
        new_pointer
    }
}

#[derive(Debug)]
struct AllocationStatistics {
    allocations: usize,
    allocated_bytes: usize,
    peak_live_bytes: usize,
}

fn measure_allocations<T>(build: impl FnOnce() -> T) -> (T, AllocationStatistics) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    LIVE_BYTES.store(0, Ordering::Relaxed);
    PEAK_LIVE_BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Release);
    let output = build();
    COUNTING.store(false, Ordering::Release);
    let statistics = AllocationStatistics {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: PEAK_LIVE_BYTES.load(Ordering::Relaxed),
    };
    (output, statistics)
}

#[derive(Clone)]
struct PositionalResults {
    variables: Arc<[String]>,
    rows: Vec<Vec<Option<EncodedTerm>>>,
}

fn source_rows(count: usize) -> (Vec<String>, Vec<Vec<Option<EncodedTerm>>>) {
    let variables = vec!["s".to_owned(), "name".to_owned(), "date".to_owned()];
    let rows = (0..count)
        .map(|index| {
            vec![
                Some(EncodedTerm(format!("<urn:collection:subject:{index}>"))),
                Some(EncodedTerm(format!("\"name-{index}\""))),
                (index % 10 != 0).then(|| {
                    EncodedTerm(
                        "\"2026-08-20\"^^<http://www.w3.org/2001/XMLSchema#date>".to_owned(),
                    )
                }),
            ]
        })
        .collect();
    (variables, rows)
}

fn collect_current(
    variables: &[String],
    source: &[Vec<Option<EncodedTerm>>],
) -> Vec<HashMap<String, EncodedTerm>> {
    source
        .iter()
        .map(|values| {
            let mut row = HashMap::with_capacity(values.len());
            for (variable, value) in variables.iter().zip(values) {
                if let Some(value) = value {
                    row.insert(variable.clone(), value.clone());
                }
            }
            row
        })
        .collect()
}

fn collect_positional(
    variables: &[String],
    source: &[Vec<Option<EncodedTerm>>],
) -> PositionalResults {
    PositionalResults {
        variables: Arc::from(variables),
        rows: source.to_vec(),
    }
}

fn positional_to_compatibility(
    positional: &PositionalResults,
) -> Vec<HashMap<String, EncodedTerm>> {
    collect_current(&positional.variables, &positional.rows)
}

fn collect_positional_compat(
    variables: &[String],
    source: &[Vec<Option<EncodedTerm>>],
) -> Vec<HashMap<String, EncodedTerm>> {
    positional_to_compatibility(&collect_positional(variables, source))
}

fn push_value(hasher: &mut blake3::Hasher, variable: &str, value: Option<&EncodedTerm>) {
    hasher.update(&(variable.len() as u64).to_le_bytes());
    hasher.update(variable.as_bytes());
    if let Some(value) = value {
        hasher.update(&[1]);
        hasher.update(&(value.0.len() as u64).to_le_bytes());
        hasher.update(value.0.as_bytes());
    } else {
        hasher.update(&[0]);
    }
}

fn row_digest(variables: &[String], rows: &[Vec<Option<EncodedTerm>>]) -> String {
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        for (variable, value) in variables.iter().zip(row) {
            push_value(&mut hasher, variable, value.as_ref());
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn map_digest(variables: &[String], rows: &[HashMap<String, EncodedTerm>]) -> String {
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        for variable in variables {
            push_value(&mut hasher, variable, row.get(variable));
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn env_sample_size() -> usize {
    match std::env::var("CRAQLE_BENCH_SAMPLE_SIZE") {
        Ok(value) => {
            let sample_size = value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("CRAQLE_BENCH_SAMPLE_SIZE must be an integer"));
            assert!(
                sample_size >= 10,
                "CRAQLE_BENCH_SAMPLE_SIZE must be at least 10"
            );
            sample_size
        }
        Err(std::env::VarError::NotPresent) => 10,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("CRAQLE_BENCH_SAMPLE_SIZE must be valid UTF-8")
        }
    }
}

fn benchmark_result_collection(c: &mut Criterion) {
    let warm_up = std::env::var("CRAQLE_BENCH_WARMUP_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(2));
    let measurement = std::env::var("CRAQLE_BENCH_MEASUREMENT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(3));
    let sample_size = env_sample_size();
    let mut group = c.benchmark_group("query_result_collection");
    group.sample_size(sample_size);
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

    let row_counts = [10_usize, 1_000, 10_000, 100_000];
    println!(
        "{}",
        to_string(&json!({
            "record": "query_result_collection_provenance",
            "commit": repository_commit(),
            "binary_blake3": binary_blake3(),
            "row_counts": row_counts,
            "variables": ["s", "name", "date"],
            "transport": "in_process_collection",
            "timing_allocator": "global counting allocator installed; Criterion timings include its disabled atomic check per allocation",
            "allocation_boundary": "diagnostic interval around one collection call before Criterion timing",
            "public_api": "compatibility map conversion remains required and unchanged",
        }))
        .expect("serialize result-collection provenance")
    );

    for row_count in row_counts {
        let (variables, source) = source_rows(row_count);
        let positional = collect_positional(&variables, &source);

        let (current, current_allocations) =
            measure_allocations(|| collect_current(&variables, &source));
        let (positional_result, positional_allocations) =
            measure_allocations(|| collect_positional(&variables, &source));
        let (converted, converted_allocations) =
            measure_allocations(|| collect_positional_compat(&variables, &source));
        let (conversion_only, conversion_only_allocations) =
            measure_allocations(|| positional_to_compatibility(&positional));
        assert_eq!(current, converted);
        assert_eq!(current, conversion_only);
        assert_eq!(positional_result.rows.len(), row_count);
        let fixture_digest = row_digest(&variables, &source);
        let current_digest = map_digest(&variables, &current);
        assert_eq!(fixture_digest, current_digest);
        assert_eq!(
            current_digest,
            row_digest(&variables, &positional_result.rows)
        );
        assert_eq!(current_digest, map_digest(&variables, &converted));
        assert_eq!(current_digest, map_digest(&variables, &conversion_only));
        for (mode, statistics) in [
            ("current", current_allocations),
            ("positional", positional_allocations),
            ("positional_then_compatibility", converted_allocations),
            ("positional_conversion_only", conversion_only_allocations),
        ] {
            println!(
                "{}",
                to_string(&json!({
                    "record": "query_result_collection_allocation",
                    "rows": row_count,
                    "variables": variables.len(),
                    "mode": mode,
                    "fixture_digest": &fixture_digest,
                    "result_digest": &current_digest,
                    "exact_compatibility_equal": true,
                    "allocations": statistics.allocations,
                    "allocated_bytes": statistics.allocated_bytes,
                    "peak_live_bytes": statistics.peak_live_bytes,
                    "counted_as_timing": false,
                }))
                .expect("serialize result-collection allocation")
            );
        }

        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("current", row_count),
            &row_count,
            |b, _| b.iter(|| black_box(collect_current(&variables, &source))),
        );
        group.bench_with_input(
            BenchmarkId::new("positional", row_count),
            &row_count,
            |b, _| b.iter(|| black_box(collect_positional(&variables, &source))),
        );
        group.bench_with_input(
            BenchmarkId::new("positional_then_compatibility", row_count),
            &row_count,
            |b, _| {
                b.iter(|| black_box(collect_positional_compat(&variables, &source)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("positional_conversion_only", row_count),
            &row_count,
            |b, _| b.iter(|| black_box(positional_to_compatibility(&positional))),
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_result_collection);
criterion_main!(benches);
