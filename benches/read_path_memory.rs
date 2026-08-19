//! Coarse process-memory baseline for completed public SPARQL query results.
//!
//! VmRSS/VmHWM describe the whole process, not a query-local allocator. The
//! current public API fully collects `QueryResults`, so this observes retained
//! completed results rather than first-row latency or an exact allocation peak.

use std::env;
use std::hint::black_box;

#[cfg(target_os = "linux")]
use std::fs;

use craqle::QueryResults;
use criterion::{Criterion, criterion_group, criterion_main};

#[path = "support/mod.rs"]
mod support;

use support::QUADS_1M;
use support::fixture::Fixture;

fn read_path_memory_benchmarks(c: &mut Criterion) {
    let before_fixture = process_memory();
    print_process_memory("before_fixture", before_fixture);

    let fixture = Fixture::from_environment();
    let after_fixture = process_memory();
    print_process_memory("after_fixture", after_fixture);

    // The shared fixture's untimed semantics settle and validate its standard
    // cases before this benchmark deliberately holds each returned result.
    let report = fixture.assert_semantics();
    fixture.print_report(&report);

    let broad_enabled = broad_scan_enabled(&fixture);
    let cases = memory_cases(&fixture, broad_enabled);
    if !broad_enabled {
        let reason = if fixture.config().corpus.quads >= QUADS_1M {
            "set CRAQLE_MEMORY_BROAD_SCAN=1 to enable it for a 1M+ corpus"
        } else {
            "CRAQLE_MEMORY_BROAD_SCAN=0 requested the opt-out"
        };
        println!("read_path_memory: broad_visible_union skipped ({reason})");
    }

    println!(
        "read_path_memory: VmHWM is process-wide cumulative; VmRSS deltas are coarse process \
         observations, not allocated bytes or exact per-query peaks. Capture Rust/profile/features \
         with rustc -Vv, cargo bench, and cargo tree -e features -p craqle."
    );
    for case in &cases {
        // Keep `result` live through the RSS sample: this is intentionally the
        // memory shape of the current fully collected public API.
        let result = black_box(case.run(&fixture));
        let summary = assert_untimed_case(case.kind, &result);
        let after_case = process_memory();
        print_case_sample(case.label, summary, after_case);
        black_box(&result);
    }

    let config = fixture.config();
    let mut group = c.benchmark_group("read_path_memory");
    group.sample_size(10);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    for case in &cases {
        group.bench_function(case.label, |b| {
            b.iter(|| black_box(case.run(&fixture)));
        });
    }
    group.finish();
}

struct MemoryCase {
    label: &'static str,
    kind: MemoryCaseKind,
    sparql: Option<String>,
}

#[derive(Clone, Copy)]
enum MemoryCaseKind {
    BoundAskHit,
    FixedPredicateObjectLimit,
    BroadVisibleUnion,
    DuplicateHeavyUnion,
}

impl MemoryCase {
    fn run(&self, fixture: &Fixture) -> QueryResults {
        match self.kind {
            MemoryCaseKind::BoundAskHit => fixture.run_hot_path(0),
            MemoryCaseKind::FixedPredicateObjectLimit => fixture.run_hot_path(2),
            MemoryCaseKind::BroadVisibleUnion => fixture.run_visible_query(
                self.sparql
                    .as_deref()
                    .expect("broad visible-union query is configured"),
                self.label,
            ),
            MemoryCaseKind::DuplicateHeavyUnion => fixture.run_all_graph_query(
                self.sparql
                    .as_deref()
                    .expect("duplicate-heavy union query is configured"),
                self.label,
            ),
        }
    }
}

fn memory_cases(fixture: &Fixture, broad_enabled: bool) -> Vec<MemoryCase> {
    let terms = fixture.query_terms();
    let duplicate_heavy_union = format!(
        "SELECT ?s WHERE {{ ?s {} {} }}",
        terms.common_predicate.0, terms.common_object.0
    );
    let mut cases = vec![
        MemoryCase {
            label: "bound_ask_hit",
            kind: MemoryCaseKind::BoundAskHit,
            sparql: None,
        },
        MemoryCase {
            label: "fixed_predicate_object_select_limit10",
            kind: MemoryCaseKind::FixedPredicateObjectLimit,
            sparql: None,
        },
    ];
    if broad_enabled {
        cases.push(MemoryCase {
            label: "broad_visible_union",
            kind: MemoryCaseKind::BroadVisibleUnion,
            sparql: Some("SELECT ?s ?p ?o WHERE { ?s ?p ?o }".to_string()),
        });
    }
    cases.push(MemoryCase {
        label: "duplicate_heavy_union",
        kind: MemoryCaseKind::DuplicateHeavyUnion,
        sparql: Some(duplicate_heavy_union),
    });
    cases
}

fn broad_scan_enabled(fixture: &Fixture) -> bool {
    match memory_broad_scan_setting() {
        Some(false) => false,
        Some(true) => true,
        None => fixture.config().corpus.quads < QUADS_1M,
    }
}

fn memory_broad_scan_setting() -> Option<bool> {
    match env::var("CRAQLE_MEMORY_BROAD_SCAN") {
        Ok(value) => match value.trim() {
            "1" => Some(true),
            "0" => Some(false),
            _ => panic!("CRAQLE_MEMORY_BROAD_SCAN must be 0 or 1"),
        },
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            panic!("CRAQLE_MEMORY_BROAD_SCAN must be valid UTF-8")
        }
    }
}

#[derive(Clone, Copy)]
struct ResultSummary {
    form: &'static str,
    rows: usize,
}

fn assert_untimed_case(kind: MemoryCaseKind, result: &QueryResults) -> ResultSummary {
    let summary = match result {
        QueryResults::Boolean(value) => ResultSummary {
            form: "boolean",
            rows: usize::from(*value),
        },
        QueryResults::Solutions(rows) => ResultSummary {
            form: "solutions",
            rows: rows.len(),
        },
        QueryResults::Graph(triples) => ResultSummary {
            form: "graph",
            rows: triples.len(),
        },
    };

    match kind {
        MemoryCaseKind::BoundAskHit => {
            assert!(
                matches!(result, QueryResults::Boolean(true)),
                "ASK must be true"
            );
        }
        MemoryCaseKind::FixedPredicateObjectLimit => {
            assert!(
                matches!(result, QueryResults::Solutions(rows) if rows.len() == 10),
                "fixed predicate-object SELECT must return exactly ten rows"
            );
        }
        MemoryCaseKind::BroadVisibleUnion => {
            assert!(
                matches!(result, QueryResults::Solutions(rows) if !rows.is_empty()),
                "enabled broad visible-union SELECT must return rows"
            );
        }
        MemoryCaseKind::DuplicateHeavyUnion => {
            assert!(
                matches!(result, QueryResults::Solutions(rows) if !rows.is_empty()),
                "duplicate-heavy union scan must return rows"
            );
        }
    }
    summary
}

#[derive(Clone, Copy)]
struct ProcessMemory {
    rss_bytes: Option<u64>,
    hwm_bytes: Option<u64>,
}

fn print_process_memory(phase: &str, sample: ProcessMemory) {
    match (sample.rss_bytes, sample.hwm_bytes) {
        (Some(rss), Some(hwm)) => {
            println!("read_path_memory rss: phase={phase} vmrss_bytes={rss} vmhwm_bytes={hwm}")
        }
        _ => println!(
            "read_path_memory rss: phase={phase} vmrss_bytes=unavailable vmhwm_bytes=unavailable"
        ),
    }
}

fn print_case_sample(label: &str, summary: ResultSummary, sample: ProcessMemory) {
    match (sample.rss_bytes, sample.hwm_bytes) {
        (Some(rss), Some(hwm)) => println!(
            "read_path_memory result: case={label} result_form={} result_rows={} \
             vmrss_bytes={rss} vmhwm_bytes={hwm}",
            summary.form, summary.rows
        ),
        _ => println!(
            "read_path_memory result: case={label} result_form={} result_rows={} \
             vmrss_bytes=unavailable vmhwm_bytes=unavailable",
            summary.form, summary.rows
        ),
    }
}

#[cfg(target_os = "linux")]
fn process_memory() -> ProcessMemory {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return ProcessMemory {
            rss_bytes: None,
            hwm_bytes: None,
        };
    };
    ProcessMemory {
        rss_bytes: status_bytes(&status, "VmRSS:"),
        hwm_bytes: status_bytes(&status, "VmHWM:"),
    }
}

#[cfg(target_os = "linux")]
fn status_bytes(status: &str, field: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn process_memory() -> ProcessMemory {
    ProcessMemory {
        rss_bytes: None,
        hwm_bytes: None,
    }
}

criterion_group!(benches, read_path_memory_benchmarks);
criterion_main!(benches);
