use std::env;
use std::hint::black_box;
use std::process::Command;
use std::time::{Duration, Instant};

#[path = "support/mod.rs"]
mod support;

use craqle::{
    CompiledShaclSchema, EncodedTerm, GraphId, MaterializedQuadChange, ShaclCompileOptions,
    ShaclValidationOptions, ShaclValidationReport, ShaclValidationResult,
};
use criterion::{Criterion, criterion_group, criterion_main};
use support::fixture::Fixture;

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const SH_NODE: &str = "<http://www.w3.org/ns/shacl#NodeShape>";
const SH_TARGET: &str = "<http://www.w3.org/ns/shacl#targetNode>";
const SH_CLASS: &str = "<http://www.w3.org/ns/shacl#targetClass>";
const SH_SUBJECTS: &str = "<http://www.w3.org/ns/shacl#targetSubjectsOf>";
const SH_PROP: &str = "<http://www.w3.org/ns/shacl#property>";
const SH_PATH: &str = "<http://www.w3.org/ns/shacl#path>";
const SH_MIN: &str = "<http://www.w3.org/ns/shacl#minCount>";
const SH_INVERSE: &str = "<http://www.w3.org/ns/shacl#inversePath>";
const SH_ONE_MORE: &str = "<http://www.w3.org/ns/shacl#oneOrMorePath>";
const ONE: &str = "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>";
const TWO: &str = "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>";
const ADD_FOCUS: &str = "<urn:craqle:bench:delta:add-focus>";
const DEL_FOCUS: &str = "<urn:craqle:bench:delta:delete-focus>";
const TYPE_FOCUS: &str = "<urn:craqle:bench:delta:type-focus>";
const INVERSE_FOCUS: &str = "<urn:craqle:bench:delta:inverse-focus>";
const GLOBAL_FOCUS: &str = "<urn:craqle:bench:delta:global-focus>";
const VALUE: &str = "<urn:craqle:bench:delta:value>";
const BATCH: &str = "<urn:craqle:bench:delta:batch>";
const CLASS: &str = "<urn:craqle:bench:delta:class>";
const TYPE_VALUE: &str = "<urn:craqle:bench:delta:type-value>";
const INBOUND: &str = "<urn:craqle:bench:delta:inbound>";
const WALK: &str = "<urn:craqle:bench:delta:walk>";
const PRESENT: &str = "<urn:craqle:bench:delta:present>";
const UNRELATED: &str = "<urn:craqle:bench:delta:unrelated>";

struct DeltaCase {
    label: &'static str,
    changes: Vec<MaterializedQuadChange>,
    expected: Vec<ShaclValidationResult>,
}

struct BenchData {
    fixture: Fixture,
    data: GraphId,
    schema: CompiledShaclSchema,
    options: ShaclValidationOptions,
    base: Vec<ShaclValidationResult>,
    cases: Vec<DeltaCase>,
}

fn shacl_incremental(c: &mut Criterion) {
    let mut data = setup();
    smoke(&mut data);
    print_provenance(&data);

    let (duration, report) = run_full(&data);
    assert_eq!(report.results, data.base, "full benchmark report changed");
    print_case("full_native", 0, duration, &report, true);
    for case in &data.cases {
        let (duration, report) = run_delta(&data, &case.changes);
        assert_eq!(
            report.results, case.expected,
            "{} report changed",
            case.label
        );
        print_case(case.label, case.changes.len(), duration, &report, true);
    }

    let config = data.fixture.config();
    let mut group = c.benchmark_group("shacl_incremental");
    group.sample_size(10);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    group.bench_function("full_native", |b| {
        b.iter(|| {
            let report = data
                .fixture
                .node()
                .validate_shacl(&data.data, &data.schema, &data.options)
                .expect("run full native validation");
            assert_eq!(report.results, data.base, "full benchmark report changed");
            black_box(report)
        })
    });
    for case in &data.cases {
        group.bench_function(case.label, |b| {
            b.iter(|| {
                let report = data
                    .fixture
                    .node()
                    .validate_shacl_delta(&data.data, &data.schema, &case.changes, &data.options)
                    .expect("run incremental native validation");
                assert_eq!(
                    report.results, case.expected,
                    "{} report changed",
                    case.label
                );
                black_box(report)
            })
        });
    }
    group.finish();
}

fn setup() -> BenchData {
    let fixture = Fixture::from_environment();
    let data = fixture.query_terms().graph.clone();
    let shapes = GraphId::new("urn:craqle:bench:delta:shapes");
    fixture
        .node()
        .apply_changes_unchecked(&data, base_rows(&data))
        .expect("seed incremental benchmark data");
    fixture
        .node()
        .apply_changes_unchecked(&shapes, shape_rows(&shapes))
        .expect("seed incremental benchmark shapes");
    let schema = fixture
        .node()
        .compile_shacl(&shapes, &ShaclCompileOptions::default())
        .expect("compile incremental benchmark shapes");
    BenchData {
        fixture,
        data: data.clone(),
        schema,
        options: ShaclValidationOptions::default(),
        base: Vec::new(),
        cases: cases(&data),
    }
}

fn smoke(data: &mut BenchData) {
    let base = data
        .fixture
        .node()
        .validate_shacl(&data.data, &data.schema, &data.options)
        .expect("validate incremental baseline");
    data.base = base.results;
    for case in &mut data.cases {
        let delta = data
            .fixture
            .node()
            .validate_shacl_delta(&data.data, &data.schema, &case.changes, &data.options)
            .expect("validate incremental smoke delta");
        data.fixture
            .node()
            .apply_changes_unchecked(&data.data, case.changes.clone())
            .expect("apply incremental smoke delta");
        let full = data
            .fixture
            .node()
            .validate_shacl(&data.data, &data.schema, &data.options)
            .expect("validate incremental smoke full");
        assert_eq!(delta.results, full.results, "{} delta differs", case.label);
        case.expected = full.results;
        data.fixture
            .node()
            .apply_changes_unchecked(&data.data, reverse(&case.changes))
            .expect("restore incremental smoke data");
        let restored = data
            .fixture
            .node()
            .validate_shacl(&data.data, &data.schema, &data.options)
            .expect("validate restored incremental data");
        assert_eq!(
            restored.results, data.base,
            "{} did not restore",
            case.label
        );
    }
}

fn run_full(data: &BenchData) -> (Duration, ShaclValidationReport) {
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl(&data.data, &data.schema, &data.options)
        .expect("run full native validation");
    (start.elapsed(), report)
}

fn run_delta(
    data: &BenchData,
    changes: &[MaterializedQuadChange],
) -> (Duration, ShaclValidationReport) {
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl_delta(&data.data, &data.schema, changes, &data.options)
        .expect("run incremental native validation");
    (start.elapsed(), report)
}

fn base_rows(graph: &GraphId) -> Vec<MaterializedQuadChange> {
    vec![add(graph, DEL_FOCUS, VALUE, PRESENT)]
}

fn shape_rows(graph: &GraphId) -> Vec<MaterializedQuadChange> {
    let mut rows = Vec::new();
    let mut push = |subject, predicate, object| rows.push(add(graph, subject, predicate, object));

    push("_:add", RDF_TYPE, SH_NODE);
    push("_:add", SH_TARGET, ADD_FOCUS);
    push("_:add", SH_PROP, "_:add-prop");
    push("_:add-prop", SH_PATH, VALUE);
    push("_:add-prop", SH_MIN, ONE);

    push("_:delete", RDF_TYPE, SH_NODE);
    push("_:delete", SH_TARGET, DEL_FOCUS);
    push("_:delete", SH_PROP, "_:delete-prop");
    push("_:delete-prop", SH_PATH, VALUE);
    push("_:delete-prop", SH_MIN, ONE);

    push("_:batch", RDF_TYPE, SH_NODE);
    push("_:batch", SH_SUBJECTS, BATCH);
    push("_:batch", SH_PROP, "_:batch-prop");
    push("_:batch-prop", SH_PATH, BATCH);
    push("_:batch-prop", SH_MIN, TWO);

    push("_:type", RDF_TYPE, SH_NODE);
    push("_:type", SH_CLASS, CLASS);
    push("_:type", SH_PROP, "_:type-prop");
    push("_:type-prop", SH_PATH, TYPE_VALUE);
    push("_:type-prop", SH_MIN, ONE);

    push("_:inverse", RDF_TYPE, SH_NODE);
    push("_:inverse", SH_TARGET, INVERSE_FOCUS);
    push("_:inverse", SH_PROP, "_:inverse-prop");
    push("_:inverse-prop", SH_PATH, "_:inverse-path");
    push("_:inverse-path", SH_INVERSE, INBOUND);
    push("_:inverse-prop", SH_MIN, ONE);

    push("_:global", RDF_TYPE, SH_NODE);
    push("_:global", SH_TARGET, GLOBAL_FOCUS);
    push("_:global", SH_PROP, "_:global-prop");
    push("_:global-prop", SH_PATH, "_:global-path");
    push("_:global-path", SH_ONE_MORE, WALK);
    push("_:global-prop", SH_MIN, ONE);
    rows
}

fn cases(graph: &GraphId) -> Vec<DeltaCase> {
    vec![
        DeltaCase {
            label: "single_unrelated_insert",
            changes: vec![add(
                graph,
                "<urn:craqle:bench:delta:unrelated-subject>",
                UNRELATED,
                "<urn:craqle:bench:delta:unrelated-value>",
            )],
            expected: Vec::new(),
        },
        DeltaCase {
            label: "single_relevant_insert",
            changes: vec![add(
                graph,
                ADD_FOCUS,
                VALUE,
                "<urn:craqle:bench:delta:add-value>",
            )],
            expected: Vec::new(),
        },
        DeltaCase {
            label: "single_relevant_delete",
            changes: vec![del(graph, DEL_FOCUS, VALUE, PRESENT)],
            expected: Vec::new(),
        },
        DeltaCase {
            label: "batch_10_insert",
            changes: batch_rows(graph, 10),
            expected: Vec::new(),
        },
        DeltaCase {
            label: "batch_100_insert",
            changes: batch_rows(graph, 100),
            expected: Vec::new(),
        },
        DeltaCase {
            label: "batch_1000_insert",
            changes: batch_rows(graph, 1_000),
            expected: Vec::new(),
        },
        DeltaCase {
            label: "type_target_insert",
            changes: vec![add(graph, TYPE_FOCUS, RDF_TYPE, CLASS)],
            expected: Vec::new(),
        },
        DeltaCase {
            label: "inverse_path_insert",
            changes: vec![add(
                graph,
                "<urn:craqle:bench:delta:inverse-subject>",
                INBOUND,
                INVERSE_FOCUS,
            )],
            expected: Vec::new(),
        },
        DeltaCase {
            label: "global_path_insert",
            changes: vec![add(
                graph,
                GLOBAL_FOCUS,
                WALK,
                "<urn:craqle:bench:delta:global-value>",
            )],
            expected: Vec::new(),
        },
    ]
}

fn batch_rows(graph: &GraphId, count: usize) -> Vec<MaterializedQuadChange> {
    (0..count)
        .map(|index| {
            add(
                graph,
                &format!("<urn:craqle:bench:delta:batch-focus:{index}>"),
                BATCH,
                &format!("<urn:craqle:bench:delta:batch-value:{index}>"),
            )
        })
        .collect()
}

fn add(graph: &GraphId, subject: &str, predicate: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm(subject.to_owned()),
        predicate: EncodedTerm(predicate.to_owned()),
        object: EncodedTerm(object.to_owned()),
    }
}

fn del(graph: &GraphId, subject: &str, predicate: &str, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: EncodedTerm(subject.to_owned()),
        predicate: EncodedTerm(predicate.to_owned()),
        object: EncodedTerm(object.to_owned()),
    }
}

fn reverse(changes: &[MaterializedQuadChange]) -> Vec<MaterializedQuadChange> {
    changes
        .iter()
        .map(|change| match change {
            MaterializedQuadChange::Insert {
                graph,
                subject,
                predicate,
                object,
            } => MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            },
            MaterializedQuadChange::Delete {
                graph,
                subject,
                predicate,
                object,
            } => MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            },
        })
        .collect()
}

fn print_provenance(data: &BenchData) {
    let corpus = data.fixture.config().corpus;
    println!(
        "shacl_incremental provenance: commit={} package_version={} feature_shacl_core={} \
         corpus_version={} seed={:#x} quads={} graphs={} duplicate_percent={} \
         fixture_selectors=CRAQLE_BENCH_QUADS,CRAQLE_BENCH_GRAPHS,CRAQLE_BENCH_DUPLICATE_PERCENT",
        commit(),
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "shacl-core"),
        support::CORPUS_VERSION,
        corpus.seed,
        corpus.quads,
        corpus.graphs,
        corpus.duplicate_percent,
    );
}

fn print_case(
    label: &str,
    delta_size: usize,
    duration: Duration,
    report: &ShaclValidationReport,
    equal: bool,
) {
    let stats = &report.statistics;
    let read = &stats.read;
    println!(
        "shacl_incremental result: case={label} delta_size={delta_size} \
         affected_shapes={} focus_nodes={} source_keys={} qv_keys={} candidate_quads={} \
         constraints={} full_fallbacks={} validation_ns={} complete_report_equal={equal}",
        stats.shapes_executed,
        stats.focus_nodes,
        read.source_keys_read,
        read.qv_keys_read,
        read.candidate_quads,
        stats.constraints_evaluated,
        stats.full_graph_fallbacks,
        duration.as_nanos(),
    );
}

fn commit() -> String {
    env::var("CRAQLE_GIT_COMMIT")
        .ok()
        .or_else(|| option_env!("CRAQLE_GIT_COMMIT").map(str::to_owned))
        .or_else(|| {
            let output = Command::new("git")
                .args(["rev-parse", "--verify", "HEAD"])
                .output()
                .ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

criterion_group!(benches, shacl_incremental);
criterion_main!(benches);
