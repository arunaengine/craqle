use std::env;
use std::hint::black_box;
use std::process::Command;
use std::time::{Duration, Instant};

#[path = "support/mod.rs"]
mod support;

use craqle::{
    AllowAllAuthorizer, CompiledShaclSchema, EncodedTerm, GraphId, MaterializedQuadChange,
    ShaclBinding, ShaclBindingOptions, ShaclCompileOptions, ShaclExecutionMode,
    ShaclValidationOptions, ShaclValidationReport, ShaclValidationResult, ValidationPolicy,
};
use criterion::{Criterion, criterion_group, criterion_main};
use support::fixture::Fixture;

const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const SH_NODE: &str = "<http://www.w3.org/ns/shacl#NodeShape>";
const SH_PROPERTY_NODE: &str = "<http://www.w3.org/ns/shacl#PropertyShape>";
const SH_TARGET: &str = "<http://www.w3.org/ns/shacl#targetNode>";
const SH_CLASS: &str = "<http://www.w3.org/ns/shacl#targetClass>";
const SH_SUBJECTS: &str = "<http://www.w3.org/ns/shacl#targetSubjectsOf>";
const SH_PROP: &str = "<http://www.w3.org/ns/shacl#property>";
const SH_PATH: &str = "<http://www.w3.org/ns/shacl#path>";
const SH_MIN: &str = "<http://www.w3.org/ns/shacl#minCount>";
const SH_INVERSE: &str = "<http://www.w3.org/ns/shacl#inversePath>";
const SH_ONE_MORE: &str = "<http://www.w3.org/ns/shacl#oneOrMorePath>";
const SH_MESSAGE: &str = "<http://www.w3.org/ns/shacl#message>";
const OWL_IMPORTS: &str = "<http://www.w3.org/2002/07/owl#imports>";
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
const AUTH: AllowAllAuthorizer = AllowAllAuthorizer;

struct DeltaCase {
    label: &'static str,
    changes: Vec<MaterializedQuadChange>,
    expected: Vec<ShaclValidationResult>,
    conforms: bool,
}

struct MutationCase {
    label: &'static str,
    graph: GraphId,
    root: GraphId,
    change: MaterializedQuadChange,
    options: ShaclCompileOptions,
    expected: Vec<ShaclValidationResult>,
    conforms: bool,
}

struct BenchData {
    fixture: Fixture,
    data: GraphId,
    schema: CompiledShaclSchema,
    options: ShaclValidationOptions,
    base: Vec<ShaclValidationResult>,
    base_conforms: bool,
    cases: Vec<DeltaCase>,
    mutation_cases: Vec<MutationCase>,
}

fn shacl_incremental(c: &mut Criterion) {
    let mut data = setup();
    smoke(&mut data);
    print_provenance(&data);

    let (duration, report) = run_full(&data);
    let equal = report.results == data.base && report.conforms == data.base_conforms;
    assert!(equal, "full benchmark report changed");
    print_case(
        "validation",
        "full_native",
        data.options.execution_mode,
        0,
        duration,
        &report,
        equal,
    );
    for case in &data.cases {
        for mode in [
            ShaclExecutionMode::ForceDelta,
            ShaclExecutionMode::ForceFull,
            ShaclExecutionMode::Auto,
        ] {
            let (duration, report) = run_mode(&data, &case.changes, mode);
            let equal = report.results == case.expected && report.conforms == case.conforms;
            assert!(equal, "{} {mode:?} report changed", case.label);
            print_case(
                "validation",
                case.label,
                mode,
                case.changes.len(),
                duration,
                &report,
                equal,
            );
        }
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
                .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
                .expect("run full native validation");
            assert_eq!(report.results, data.base, "full benchmark report changed");
            assert_eq!(
                report.conforms, data.base_conforms,
                "full benchmark state changed"
            );
            black_box(report)
        })
    });
    for case in &data.cases {
        for (label, mode) in [
            ("force_delta", ShaclExecutionMode::ForceDelta),
            ("force_full", ShaclExecutionMode::ForceFull),
            ("auto", ShaclExecutionMode::Auto),
        ] {
            let options = mode_options(&data.options, mode);
            group.bench_function(format!("{label}/{}", case.label), |b| {
                b.iter(|| {
                    let report = data
                        .fixture
                        .node()
                        .validate_shacl_delta(
                            &AUTH,
                            &data.data,
                            &data.schema,
                            &case.changes,
                            &options,
                        )
                        .expect("run native validation");
                    assert_eq!(
                        report.results, case.expected,
                        "{} {label} report changed",
                        case.label
                    );
                    assert_eq!(
                        report.conforms, case.conforms,
                        "{} {label} state changed",
                        case.label
                    );
                    black_box(report)
                })
            });
        }
    }
    group.finish();

    for case in &data.mutation_cases {
        let (duration, report) = run_settle(&data, case);
        let equal = report.results == case.expected && report.conforms == case.conforms;
        assert_eq!(
            report.results, case.expected,
            "{} report changed",
            case.label
        );
        assert_eq!(
            report.conforms, case.conforms,
            "{} state changed",
            case.label
        );
        print_case(
            "mutation_recompile_full_settle",
            case.label,
            data.options.execution_mode,
            1,
            duration,
            &report,
            equal,
        );
    }
    let mut settle = c.benchmark_group("shacl_shape_settle");
    settle.sample_size(10);
    settle.warm_up_time(config.warm_up);
    settle.measurement_time(config.measurement);
    for case in &data.mutation_cases {
        settle.bench_function(case.label, |b| {
            b.iter(|| {
                let (_, report) = run_settle(&data, case);
                assert_eq!(
                    report.results, case.expected,
                    "{} report changed",
                    case.label
                );
                assert_eq!(
                    report.conforms, case.conforms,
                    "{} state changed",
                    case.label
                );
                black_box(report)
            })
        });
    }
    settle.finish();

    let (duration, report) = run_write(&data);
    assert!(report.conforms, "checked write did not conform");
    assert!(
        report.results.is_empty(),
        "checked write report was not empty"
    );
    print_case(
        "checked_write",
        "valid_enforce_write",
        ShaclExecutionMode::Auto,
        1,
        duration,
        &report,
        true,
    );
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
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .expect("compile incremental benchmark shapes");
    let imported_shapes = GraphId::new("urn:craqle:bench:delta:imported-shapes");
    let import_root = GraphId::new("urn:craqle:bench:delta:import-root");
    fixture
        .node()
        .apply_changes_unchecked(&imported_shapes, shape_rows(&imported_shapes))
        .expect("seed imported shapes benchmark");
    fixture
        .node()
        .apply_changes_unchecked(
            &import_root,
            vec![add(
                &import_root,
                "<urn:craqle:bench:delta:import-root>",
                OWL_IMPORTS,
                "<urn:craqle:bench:delta:imported-shapes>",
            )],
        )
        .expect("seed import root benchmark");
    fixture
        .node()
        .rebuild_query_indexes()
        .expect("build incremental benchmark counters");
    BenchData {
        fixture,
        data: data.clone(),
        schema,
        options: ShaclValidationOptions::default(),
        base: Vec::new(),
        base_conforms: false,
        cases: cases(&data),
        mutation_cases: vec![
            MutationCase {
                label: "shape_mutate_recompile_full_settle",
                graph: shapes.clone(),
                root: shapes.clone(),
                change: add(
                    &shapes,
                    "<urn:craqle:bench:delta:batch-shape>",
                    SH_MESSAGE,
                    "\"post-change\"",
                ),
                options: ShaclCompileOptions::default(),
                expected: Vec::new(),
                conforms: false,
            },
            MutationCase {
                label: "import_mutate_recompile_full_settle",
                graph: imported_shapes.clone(),
                root: import_root,
                change: add(
                    &imported_shapes,
                    "<urn:craqle:bench:delta:batch-shape>",
                    SH_MESSAGE,
                    "\"import-change\"",
                ),
                options: ShaclCompileOptions {
                    allow_local_imports: true,
                    ..ShaclCompileOptions::default()
                },
                expected: Vec::new(),
                conforms: false,
            },
        ],
    }
}

fn smoke(data: &mut BenchData) {
    let base = data
        .fixture
        .node()
        .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
        .expect("validate incremental baseline");
    data.base_conforms = base.conforms;
    data.base = base.results;
    for index in 0..data.mutation_cases.len() {
        let (_, report) = run_settle(data, &data.mutation_cases[index]);
        data.mutation_cases[index].conforms = report.conforms;
        data.mutation_cases[index].expected = report.results;
    }
    let shapes = data.mutation_cases[0].root.clone();
    data.schema = data
        .fixture
        .node()
        .compile_shacl(&AUTH, &shapes, &ShaclCompileOptions::default())
        .expect("compile restored benchmark shapes");
    for index in 0..data.cases.len() {
        let label = data.cases[index].label;
        let changes = data.cases[index].changes.clone();
        let (_, delta) = run_mode(data, &changes, ShaclExecutionMode::ForceDelta);
        let (_, candidate_full) = run_mode(data, &changes, ShaclExecutionMode::ForceFull);
        let (_, auto) = run_mode(data, &changes, ShaclExecutionMode::Auto);
        data.fixture
            .node()
            .apply_changes_unchecked(&data.data, changes.clone())
            .expect("apply incremental smoke delta");
        let full = data
            .fixture
            .node()
            .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
            .expect("validate incremental smoke full");
        assert_eq!(
            delta.results, candidate_full.results,
            "{label} forced modes differ"
        );
        assert_eq!(
            delta.conforms, candidate_full.conforms,
            "{label} forced modes differ"
        );
        assert_eq!(delta.results, auto.results, "{label} auto differs");
        assert_eq!(delta.conforms, auto.conforms, "{label} auto differs");
        assert_eq!(delta.results, full.results, "{label} delta differs");
        assert_eq!(delta.conforms, full.conforms, "{label} delta differs");
        data.cases[index].expected = full.results;
        data.cases[index].conforms = full.conforms;
        data.fixture
            .node()
            .apply_changes_unchecked(&data.data, reverse(&changes))
            .expect("restore incremental smoke data");
        let restored = data
            .fixture
            .node()
            .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
            .expect("validate restored incremental data");
        assert_eq!(restored.results, data.base, "{label} did not restore");
        assert_eq!(
            restored.conforms, data.base_conforms,
            "{label} state did not restore"
        );
    }
}

fn run_full(data: &BenchData) -> (Duration, ShaclValidationReport) {
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
        .expect("run full native validation");
    (start.elapsed(), report)
}

fn run_mode(
    data: &BenchData,
    changes: &[MaterializedQuadChange],
    execution_mode: ShaclExecutionMode,
) -> (Duration, ShaclValidationReport) {
    let options = mode_options(&data.options, execution_mode);
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl_delta(&AUTH, &data.data, &data.schema, changes, &options)
        .expect("run native validation");
    (start.elapsed(), report)
}

fn run_settle(data: &BenchData, case: &MutationCase) -> (Duration, ShaclValidationReport) {
    let start = Instant::now();
    data.fixture
        .node()
        .apply_changes_unchecked(&case.graph, vec![case.change.clone()])
        .expect("apply shapes mutation");
    let schema = data
        .fixture
        .node()
        .compile_shacl(&AUTH, &case.root, &case.options)
        .expect("compile mutated shapes");
    let report = data
        .fixture
        .node()
        .validate_shacl(&AUTH, &data.data, &schema, &data.options)
        .expect("settle mutated shapes");
    let duration = start.elapsed();
    data.fixture
        .node()
        .apply_changes_unchecked(&case.graph, reverse(std::slice::from_ref(&case.change)))
        .expect("restore shapes mutation");
    (duration, report)
}

fn run_write(data: &BenchData) -> (Duration, ShaclValidationReport) {
    let graph = GraphId::new("urn:craqle:bench:delta:checked-write-data");
    let shapes = GraphId::new("urn:craqle:bench:delta:checked-write-shapes");
    data.fixture
        .node()
        .apply_changes_unchecked(&shapes, checked_rows(&shapes))
        .expect("seed checked write shapes");
    data.fixture
        .node()
        .apply_changes_unchecked(
            &graph,
            vec![add(
                &graph,
                "<urn:craqle:bench:delta:checked-write-focus>",
                VALUE,
                PRESENT,
            )],
        )
        .expect("seed checked write data");
    data.fixture
        .node()
        .bind_shacl(
            &AUTH,
            &ShaclBinding {
                data_graph: graph.clone(),
                shapes_graph: shapes.clone(),
                policy: ValidationPolicy::Enforce,
                validation_options: ShaclBindingOptions::default(),
            },
        )
        .expect("bind checked write shapes");
    let start = Instant::now();
    data.fixture
        .node()
        .apply_changes(
            &graph,
            vec![add(
                &graph,
                "<urn:craqle:bench:delta:checked-write-focus>",
                VALUE,
                "<urn:craqle:bench:delta:checked-write-next>",
            )],
        )
        .expect("apply valid checked write");
    let duration = start.elapsed();
    let mut statuses = data
        .fixture
        .node()
        .shacl_binding_statuses(&AUTH, &graph)
        .expect("read checked write status");
    let report = statuses
        .pop()
        .and_then(|status| status.report)
        .expect("persisted checked write report");
    data.fixture
        .node()
        .unbind_shacl(&AUTH, &graph, &shapes)
        .expect("unbind checked write shapes");
    (duration, report)
}

fn mode_options(
    options: &ShaclValidationOptions,
    execution_mode: ShaclExecutionMode,
) -> ShaclValidationOptions {
    ShaclValidationOptions {
        execution_mode,
        ..options.clone()
    }
}

fn base_rows(graph: &GraphId) -> Vec<MaterializedQuadChange> {
    vec![add(graph, DEL_FOCUS, VALUE, PRESENT)]
}

fn shape_rows(graph: &GraphId) -> Vec<MaterializedQuadChange> {
    let mut rows = Vec::new();
    let mut push = |subject, predicate, object| rows.push(add(graph, subject, predicate, object));

    push("<urn:craqle:bench:delta:add-shape>", RDF_TYPE, SH_NODE);
    push("<urn:craqle:bench:delta:add-shape>", SH_TARGET, ADD_FOCUS);
    push(
        "<urn:craqle:bench:delta:add-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:add-property>",
    );
    push("<urn:craqle:bench:delta:add-property>", SH_PATH, VALUE);
    push("<urn:craqle:bench:delta:add-property>", SH_MIN, ONE);
    push(
        "<urn:craqle:bench:delta:add-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );

    push("<urn:craqle:bench:delta:delete-shape>", RDF_TYPE, SH_NODE);
    push(
        "<urn:craqle:bench:delta:delete-shape>",
        SH_TARGET,
        DEL_FOCUS,
    );
    push(
        "<urn:craqle:bench:delta:delete-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:delete-property>",
    );
    push("<urn:craqle:bench:delta:delete-property>", SH_PATH, VALUE);
    push("<urn:craqle:bench:delta:delete-property>", SH_MIN, ONE);
    push(
        "<urn:craqle:bench:delta:delete-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );

    push("<urn:craqle:bench:delta:batch-shape>", RDF_TYPE, SH_NODE);
    push("<urn:craqle:bench:delta:batch-shape>", SH_SUBJECTS, BATCH);
    push(
        "<urn:craqle:bench:delta:batch-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:batch-property>",
    );
    push("<urn:craqle:bench:delta:batch-property>", SH_PATH, BATCH);
    push("<urn:craqle:bench:delta:batch-property>", SH_MIN, TWO);
    push(
        "<urn:craqle:bench:delta:batch-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );

    push("<urn:craqle:bench:delta:type-shape>", RDF_TYPE, SH_NODE);
    push("<urn:craqle:bench:delta:type-shape>", SH_CLASS, CLASS);
    push(
        "<urn:craqle:bench:delta:type-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:type-property>",
    );
    push(
        "<urn:craqle:bench:delta:type-property>",
        SH_PATH,
        TYPE_VALUE,
    );
    push("<urn:craqle:bench:delta:type-property>", SH_MIN, ONE);
    push(
        "<urn:craqle:bench:delta:type-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );

    push("<urn:craqle:bench:delta:inverse-shape>", RDF_TYPE, SH_NODE);
    push(
        "<urn:craqle:bench:delta:inverse-shape>",
        SH_TARGET,
        INVERSE_FOCUS,
    );
    push(
        "<urn:craqle:bench:delta:inverse-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:inverse-property>",
    );
    push(
        "<urn:craqle:bench:delta:inverse-property>",
        SH_PATH,
        "_:inverse-path",
    );
    push("_:inverse-path", SH_INVERSE, INBOUND);
    push("<urn:craqle:bench:delta:inverse-property>", SH_MIN, ONE);
    push(
        "<urn:craqle:bench:delta:inverse-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );

    push("<urn:craqle:bench:delta:global-shape>", RDF_TYPE, SH_NODE);
    push(
        "<urn:craqle:bench:delta:global-shape>",
        SH_TARGET,
        GLOBAL_FOCUS,
    );
    push(
        "<urn:craqle:bench:delta:global-shape>",
        SH_PROP,
        "<urn:craqle:bench:delta:global-property>",
    );
    push(
        "<urn:craqle:bench:delta:global-property>",
        SH_PATH,
        "_:global-path",
    );
    push("_:global-path", SH_ONE_MORE, WALK);
    push("<urn:craqle:bench:delta:global-property>", SH_MIN, ONE);
    push(
        "<urn:craqle:bench:delta:global-property>",
        RDF_TYPE,
        SH_PROPERTY_NODE,
    );
    rows
}

fn checked_rows(graph: &GraphId) -> Vec<MaterializedQuadChange> {
    vec![
        add(
            graph,
            "<urn:craqle:bench:delta:checked-write-shape>",
            RDF_TYPE,
            SH_NODE,
        ),
        add(
            graph,
            "<urn:craqle:bench:delta:checked-write-shape>",
            SH_TARGET,
            "<urn:craqle:bench:delta:checked-write-focus>",
        ),
        add(
            graph,
            "<urn:craqle:bench:delta:checked-write-shape>",
            SH_PROP,
            "_:checked-write-property",
        ),
        add(graph, "_:checked-write-property", SH_PATH, VALUE),
        add(graph, "_:checked-write-property", SH_MIN, ONE),
    ]
}

fn cases(graph: &GraphId) -> Vec<DeltaCase> {
    vec![
        DeltaCase {
            label: "unrelated_change",
            changes: vec![add(
                graph,
                "<urn:craqle:bench:delta:unrelated-subject>",
                UNRELATED,
                "<urn:craqle:bench:delta:unrelated-value>",
            )],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "relevant_property_change",
            changes: vec![add(
                graph,
                ADD_FOCUS,
                VALUE,
                "<urn:craqle:bench:delta:add-value>",
            )],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "relevant_property_delete",
            changes: vec![del(graph, DEL_FOCUS, VALUE, PRESENT)],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_1",
            changes: batch_rows(graph, 1),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_2",
            changes: batch_rows(graph, 2),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_5",
            changes: batch_rows(graph, 5),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_10",
            changes: batch_rows(graph, 10),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_25",
            changes: batch_rows(graph, 25),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_50",
            changes: batch_rows(graph, 50),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_100",
            changes: batch_rows(graph, 100),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_250",
            changes: batch_rows(graph, 250),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_500",
            changes: batch_rows(graph, 500),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "batch_1000",
            changes: batch_rows(graph, 1_000),
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "rdf_type_change",
            changes: vec![add(graph, TYPE_FOCUS, RDF_TYPE, CLASS)],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "inverse_path_change",
            changes: vec![add(
                graph,
                "<urn:craqle:bench:delta:inverse-subject>",
                INBOUND,
                INVERSE_FOCUS,
            )],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "reachability_change",
            changes: vec![add(
                graph,
                GLOBAL_FOCUS,
                WALK,
                "<urn:craqle:bench:delta:global-value>",
            )],
            expected: Vec::new(),
            conforms: false,
        },
        DeltaCase {
            label: "mixed_batch",
            changes: vec![
                add(
                    graph,
                    ADD_FOCUS,
                    VALUE,
                    "<urn:craqle:bench:delta:mixed-value>",
                ),
                add(
                    graph,
                    "<urn:craqle:bench:delta:mixed-subject>",
                    UNRELATED,
                    "<urn:craqle:bench:delta:mixed-unrelated>",
                ),
                add(graph, TYPE_FOCUS, RDF_TYPE, CLASS),
                add(
                    graph,
                    "<urn:craqle:bench:delta:mixed-inverse>",
                    INBOUND,
                    INVERSE_FOCUS,
                ),
                add(
                    graph,
                    GLOBAL_FOCUS,
                    WALK,
                    "<urn:craqle:bench:delta:mixed-walk>",
                ),
            ],
            expected: Vec::new(),
            conforms: false,
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
    operation: &str,
    label: &str,
    requested_mode: ShaclExecutionMode,
    delta_size: usize,
    duration: Duration,
    report: &ShaclValidationReport,
    equal: bool,
) {
    let stats = &report.statistics;
    let read = &stats.read;
    assert_ne!(
        0, stats.estimated_full_work,
        "{operation} {label} reported free full validation"
    );
    let validation = (operation == "validation")
        .then(|| duration.as_nanos().to_string())
        .unwrap_or_else(|| "not_measured".to_owned());
    let checked_write = (operation == "checked_write")
        .then(|| duration.as_nanos().to_string())
        .unwrap_or_else(|| "not_measured".to_owned());
    let settle = (operation == "mutation_recompile_full_settle")
        .then(|| duration.as_nanos().to_string())
        .unwrap_or_else(|| "not_measured".to_owned());
    println!(
        "shacl_incremental result: operation={operation} case={label} requested_mode={requested_mode:?} \
         selected_mode={:?} delta_size={delta_size} estimated_delta_work={} \
         estimated_full_work={} estimated_affected_shapes={} estimated_focus_nodes={} \
         affected_shapes={} focus_nodes={} index_seeks={} qv_admission_checks={} qv_counter_reads={} \
         source_keys={} qv_keys={} \
         candidate_quads={} constraints={} full_fallbacks={} elapsed_ns={} validation_ns={validation} \
         total_checked_write_ns={checked_write} total_settle_ns={settle} complete_report_equal={equal}",
        stats.selected_mode,
        stats.estimated_delta_work,
        stats.estimated_full_work,
        stats.estimated_affected_shapes,
        stats.estimated_focus_nodes,
        stats.shapes_executed,
        stats.focus_nodes,
        read.index_seeks,
        read.qv_admission_checks,
        read.qv_counter_reads,
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
