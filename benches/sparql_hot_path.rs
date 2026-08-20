use std::hint::black_box;

#[path = "support/allocation.rs"]
mod allocation;
#[path = "support/mod.rs"]
mod support;

use allocation::AllocationInterval;
use craqle::{QueryExecutionOptions, QueryFastPathMode};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use support::fixture::Fixture;

fn print_alloc(fixture: &Fixture, index: usize, mode: craqle::QueryReadMode) {
    let interval = AllocationInterval::begin();
    let (results, _) = fixture.run_hot_mode(index, mode);
    let sample = interval.finish();
    println!(
        "sparql_hot_path allocation: fixture_digest={} case={} mode={mode:?} \
         allocations={} allocated_bytes={} peak_live_delta_bytes={}",
        fixture.fixture_digest(),
        fixture.hot_path_label(index),
        sample.allocations,
        sample.allocated_bytes,
        sample.peak_live_delta_bytes,
    );
    std::hint::black_box(results);
}

fn sparql_hot_path_benchmarks(c: &mut Criterion) {
    let fixture = Fixture::from_environment();
    // This untimed sweep checks every result and deterministically warms the
    // completed-result path before Criterion begins collecting samples.
    let report = fixture.assert_semantics();
    fixture.print_report(&report);
    fixture.print_hot_work();
    for index in 0..fixture.hot_path_count() {
        let execution = fixture.measure_hot_path(index);
        let statistics = &execution.statistics;
        println!(
            "sparql_hot_path execution: fixture_digest={} case={} mode=Auto \
             logical_operator={:?} physical_operator={:?} fast_path={:?} plan_fingerprint={} access_path={:?} \
             estimated_rows={:?} actual_rows={:?} plan_candidates={} plan_output_rows={} \
             plan_elapsed_ns={} index_seeks={} qv_admission_checks={} qv_header_reads={} qv_counter_reads={} \
             qv_trusted={} fallback_reason={} source_keys={} source_bytes={} qv_keys={} \
             qv_bytes={} candidate_quads={} matching_quads={} graph_checks={} orphan_checks={} \
             duplicate_groups={} duplicate_copies_skipped={} term_decodes={} intermediate_rows={} \
             result_rows={} result_cells={} parse_ns={} rewrite_ns={} planning_ns={} execution_ns={} \
             collection_ns={} first_internal_ns={:?}",
            fixture.fixture_digest(),
            fixture.hot_path_label(index),
            statistics.plan.root.logical_operator,
            statistics.plan.root.physical_operator,
            statistics.fast_path,
            statistics.plan_fingerprint,
            statistics.selected_access_paths,
            statistics.plan.root.estimated_rows,
            statistics.plan.root.actual_rows,
            statistics.plan.root.candidate_rows,
            statistics.plan.root.output_rows,
            statistics.plan.root.elapsed_time.as_nanos(),
            statistics.index_seeks,
            statistics.qv_admission_checks,
            statistics.qv_header_reads,
            statistics.qv_counter_reads,
            statistics.qv_trusted,
            statistics.fallback_reason.as_deref().unwrap_or("none"),
            statistics.source_keys_read,
            statistics.source_bytes_read,
            statistics.qv_keys_read,
            statistics.qv_bytes_read,
            statistics.candidate_quads,
            statistics.matching_quads,
            statistics.graphs_considered,
            statistics.orphan_checks,
            statistics.duplicate_groups,
            statistics.duplicate_copies_skipped,
            statistics.terms_decoded,
            statistics.intermediate_rows,
            statistics.result_rows,
            statistics.result_cells,
            statistics.parse_time.as_nanos(),
            statistics.rewrite_time.as_nanos(),
            statistics.planning_time.as_nanos(),
            statistics.execution_time.as_nanos(),
            statistics.result_collection_time.as_nanos(),
            statistics
                .time_to_first_internal_result
                .map(|duration| duration.as_nanos()),
        );
    }
    for index in 0..fixture.hot_path_count() {
        print_alloc(&fixture, index, craqle::QueryReadMode::Auto);
    }

    let config = fixture.config();
    let mut group = c.benchmark_group("sparql_hot_path");
    group.sample_size(config.sample_size);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);

    for index in 0..fixture.hot_path_count() {
        let label = fixture.hot_path_label(index);
        group.bench_function(label, |b| {
            b.iter(|| black_box(fixture.run_hot_path(index)));
        });
    }
    group.finish();

    let mut fast_options = QueryExecutionOptions::default();
    fast_options.fast_paths = QueryFastPathMode::Auto;
    let mut generic_options = QueryExecutionOptions::default();
    generic_options.fast_paths = QueryFastPathMode::Disabled;
    let mut group = c.benchmark_group("sparql_fast_path_comparison");
    group.sample_size(config.sample_size);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    for index in 0..5 {
        let query = fixture.prepare_hot_path(index);
        let fast = fixture.run_hot_prepared(&query, &fast_options);
        let generic = fixture.run_hot_prepared(&query, &generic_options);
        assert_eq!(fast.results, generic.results);
        assert!(fast.statistics.fast_path.is_some());
        group.bench_function(format!("{}/fast", fixture.hot_path_label(index)), |b| {
            b.iter(|| black_box(fixture.run_hot_prepared(&query, &fast_options)));
        });
        group.bench_function(format!("{}/generic", fixture.hot_path_label(index)), |b| {
            b.iter(|| black_box(fixture.run_hot_prepared(&query, &generic_options)));
        });
    }
    group.finish();

    let prepared = fixture.prepare_hot_path(0);
    let options = QueryExecutionOptions::default();
    let parsed_result = fixture.measure_hot_path(0);
    let prepared_result = fixture.run_hot_prepared(&prepared, &options);
    assert_eq!(parsed_result.results, prepared_result.results);

    let config = fixture.config();
    let mut group = c.benchmark_group("sparql_repeated_execution");
    group.sample_size(config.sample_size);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);

    for executions in [1_u64, 10, 100, 10_000] {
        group.throughput(Throughput::Elements(executions));
        group.bench_with_input(
            BenchmarkId::new("parse_each", executions),
            &executions,
            |b, &executions| {
                b.iter(|| {
                    for _ in 0..executions {
                        black_box(fixture.measure_hot_path(0));
                    }
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("prepared", executions),
            &executions,
            |b, &executions| {
                b.iter(|| {
                    for _ in 0..executions {
                        black_box(fixture.run_hot_prepared(&prepared, &options));
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, sparql_hot_path_benchmarks);
criterion_main!(benches);
