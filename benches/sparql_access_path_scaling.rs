use std::hint::black_box;

#[path = "support/allocation.rs"]
mod allocation;
#[path = "support/mod.rs"]
mod support;

use allocation::{AllocationInterval, AllocationSample};
use craqle::{PreparedQuery, QueryExecution, QueryExecutionOptions, QueryReadMode};
use criterion::{Criterion, criterion_group, criterion_main};
use support::{QUADS_1M, fixture::Fixture};

struct ScalingCase {
    label: &'static str,
    query: PreparedQuery,
}

fn execute(fixture: &Fixture, query: &PreparedQuery, mode: QueryReadMode) -> QueryExecution {
    let mut options = QueryExecutionOptions::default();
    options.read_mode = mode;
    fixture.run_hot_prepared(query, &options)
}

fn sample_alloc(
    fixture: &Fixture,
    query: &PreparedQuery,
    mode: QueryReadMode,
) -> (QueryExecution, AllocationSample) {
    let interval = AllocationInterval::begin();
    let execution = execute(fixture, query, mode);
    let sample = interval.finish();
    (execution, sample)
}

fn print_work(
    fixture: &Fixture,
    label: &str,
    mode: QueryReadMode,
    execution: &QueryExecution,
    sample: AllocationSample,
) {
    let statistics = &execution.statistics;
    println!(
        "sparql_access_path_scaling fixture_digest={} case={label} mode={mode:?} \
         logical_operator={:?} physical_operator={:?} fast_path={:?} plan_fingerprint={} access_path={:?} \
         estimated_rows={:?} actual_rows={:?} plan_candidates={} plan_output_rows={} \
         plan_elapsed_ns={} index_seeks={} qv_admission_checks={} qv_header_reads={} qv_counter_reads={} \
         qv_trusted={} fallback_reason={} source_keys={} source_bytes={} qv_keys={} \
         qv_bytes={} candidate_quads={} matching_quads={} graph_checks={} orphan_checks={} \
         duplicate_groups={} duplicate_copies_skipped={} term_decodes={} intermediate_rows={} \
         result_rows={} result_cells={} parse_ns={} rewrite_ns={} planning_ns={} execution_ns={} \
         collection_ns={} first_internal_ns={:?} allocations={} allocated_bytes={} \
         peak_live_delta_bytes={}",
        fixture.fixture_digest(),
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
        sample.allocations,
        sample.allocated_bytes,
        sample.peak_live_delta_bytes,
    );
}

fn assert_modes(fixture: &Fixture, case: &ScalingCase) {
    let (automatic, automatic_alloc) = sample_alloc(fixture, &case.query, QueryReadMode::Auto);
    let (source, source_alloc) = sample_alloc(fixture, &case.query, QueryReadMode::ForceSource);
    let (qv, qv_alloc) = sample_alloc(fixture, &case.query, QueryReadMode::ForceQv);
    assert_eq!(automatic.results, source.results);
    assert_eq!(source.results, qv.results);
    assert!(automatic.statistics.qv_trusted);
    assert!(qv.statistics.qv_trusted);
    assert_eq!(automatic.statistics.source_keys_read, 0);
    assert_eq!(qv.statistics.source_keys_read, 0);
    assert!(qv.statistics.qv_keys_read > 0);
    if fixture.config().corpus.quads >= QUADS_1M {
        assert!(
            source.statistics.source_keys_read >= qv.statistics.qv_keys_read.saturating_mul(100),
            "{} source keys={} qv keys={}",
            case.label,
            source.statistics.source_keys_read,
            qv.statistics.qv_keys_read,
        );
    }
    for (mode, execution, sample) in [
        (QueryReadMode::Auto, automatic, automatic_alloc),
        (QueryReadMode::ForceSource, source, source_alloc),
        (QueryReadMode::ForceQv, qv, qv_alloc),
    ] {
        print_work(fixture, case.label, mode, &execution, sample);
    }
}

fn query_path_scaling(c: &mut Criterion) {
    let fixture = Fixture::from_environment();
    let pattern = fixture.late_rare_pattern();
    let cases = [
        ScalingCase {
            label: "named_predicate_object_ask",
            query: fixture.prepare_query(&format!("ASK {{ {pattern} }}")),
        },
        ScalingCase {
            label: "named_predicate_object_limit",
            query: fixture.prepare_query(&format!("SELECT ?s WHERE {{ {pattern} }} LIMIT 10")),
        },
    ];
    for case in &cases {
        assert_modes(&fixture, case);
    }

    let config = fixture.config();
    let mut group = c.benchmark_group("sparql_access_path_scaling");
    group.sample_size(config.sample_size);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    for case in &cases {
        for (label, mode) in [
            ("auto", QueryReadMode::Auto),
            ("source", QueryReadMode::ForceSource),
            ("qv", QueryReadMode::ForceQv),
        ] {
            group.bench_function(format!("{}/{label}", case.label), |b| {
                b.iter(|| black_box(execute(&fixture, &case.query, mode)))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, query_path_scaling);
criterion_main!(benches);
