use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use craqle::EncodedTerm;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

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

fn record_deallocation(bytes: usize) {
    let _ = LIVE_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(bytes))
    });
}

// SAFETY: every operation delegates to `System` with the original pointer and
// layout. The additional atomics only observe sizes and never alter allocation
// results or pointer ownership.
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
    let mut group = c.benchmark_group("query_result_collection");
    group.sample_size(10);
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

    for row_count in [10_usize, 1_000, 10_000, 100_000] {
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
        for (mode, statistics) in [
            ("current", current_allocations),
            ("positional", positional_allocations),
            ("positional_then_compatibility", converted_allocations),
            ("positional_conversion_only", conversion_only_allocations),
        ] {
            println!(
                "query_result_collection allocations: rows={row_count} mode={mode} allocations={} allocated_bytes={} peak_live_bytes={}",
                statistics.allocations, statistics.allocated_bytes, statistics.peak_live_bytes
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
