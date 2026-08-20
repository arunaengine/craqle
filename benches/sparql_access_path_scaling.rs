use std::hint::black_box;

#[path = "support/mod.rs"]
mod support;

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

fn print_work(label: &str, mode: QueryReadMode, execution: &QueryExecution) {
    let statistics = &execution.statistics;
    println!(
        "sparql_access_path_scaling case={label} mode={mode:?} access_path={:?} \
         qv_trusted={} fallback_reason={} source_keys={} source_bytes={} qv_keys={} \
         qv_bytes={} candidate_quads={} matching_quads={} graph_checks={} orphan_checks={} \
         duplicate_groups={} duplicate_copies_skipped={} term_decodes={} result_rows={}",
        statistics.selected_access_paths,
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
        statistics.result_rows,
    );
}

fn assert_modes(fixture: &Fixture, case: &ScalingCase) {
    let automatic = execute(fixture, &case.query, QueryReadMode::Auto);
    let source = execute(fixture, &case.query, QueryReadMode::ForceSource);
    let qv = execute(fixture, &case.query, QueryReadMode::ForceQv);
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
    for (mode, execution) in [
        (QueryReadMode::Auto, automatic),
        (QueryReadMode::ForceSource, source),
        (QueryReadMode::ForceQv, qv),
    ] {
        print_work(case.label, mode, &execution);
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
    group.sample_size(10);
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
