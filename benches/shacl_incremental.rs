use std::env;
use std::hint::black_box;
use std::process::Command;
use std::time::{Duration, Instant};

#[path = "support/allocation.rs"]
mod allocation;
#[path = "support/mod.rs"]
mod support;

use support::BenchWriteExt as _;

use allocation::AllocationInterval;
use craqle::{
    AllowAllAuthorizer, CompiledShaclSchema, CreateCrateRequest, EncodedTerm, GraphId, GraphPolicy,
    MaterializedQuadChange, ShaclBinding, ShaclBindingOptions, ShaclCompileOptions,
    ShaclExecutionMode, ShaclValidationOptions, ShaclValidationReport, ShaclValidationResult,
    ShaclValidationState, ValidationPolicy,
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
const HAS_PART: &str = "<http://schema.org/hasPart>";
const MEDIA_OBJECT: &str = "<http://schema.org/MediaObject>";
const NAME: &str = "<http://schema.org/name>";
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

struct PolicySample {
    allocation: allocation::AllocationSample,
    validation_scope: &'static str,
    source_keys: u64,
    qv_keys: u64,
    candidate_quads: u64,
    constraints: u64,
}

fn shacl_incremental(c: &mut Criterion) {
    let mut data = setup();
    smoke(&mut data);
    print_provenance(&data);

    let (duration, report, allocations) = run_full(&data);
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
        allocations,
    );
    for case in &data.cases {
        for mode in [
            ShaclExecutionMode::ForceDelta,
            ShaclExecutionMode::ForceFull,
            ShaclExecutionMode::Auto,
        ] {
            let (duration, report, allocations) = run_mode(&data, &case.changes, mode);
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
                allocations,
            );
        }
    }

    let config = data.fixture.config();
    let mut group = c.benchmark_group("shacl_incremental");
    group.sample_size(config.sample_size);
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
        let (duration, report, allocations) = run_settle(&data, case);
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
            allocations,
        );
    }
    let mut settle = c.benchmark_group("shacl_shape_settle");
    settle.sample_size(config.sample_size);
    settle.warm_up_time(config.warm_up);
    settle.measurement_time(config.measurement);
    for case in &data.mutation_cases {
        settle.bench_function(case.label, |b| {
            b.iter(|| {
                let (_, report, _) = run_settle(&data, case);
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

    let (duration, report, allocations) = run_write(&data);
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
        allocations,
    );
    policy_bench(c, &data);
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
        let (_, report, _) = run_settle(data, &data.mutation_cases[index]);
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
        let (_, delta, _) = run_mode(data, &changes, ShaclExecutionMode::ForceDelta);
        let (_, candidate_full, _) = run_mode(data, &changes, ShaclExecutionMode::ForceFull);
        let (_, auto, _) = run_mode(data, &changes, ShaclExecutionMode::Auto);
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

fn run_full(
    data: &BenchData,
) -> (
    Duration,
    ShaclValidationReport,
    allocation::AllocationSample,
) {
    let interval = AllocationInterval::begin();
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl(&AUTH, &data.data, &data.schema, &data.options)
        .expect("run full native validation");
    let duration = start.elapsed();
    let allocations = interval.finish();
    (duration, report, allocations)
}

fn run_mode(
    data: &BenchData,
    changes: &[MaterializedQuadChange],
    execution_mode: ShaclExecutionMode,
) -> (
    Duration,
    ShaclValidationReport,
    allocation::AllocationSample,
) {
    let options = mode_options(&data.options, execution_mode);
    let interval = AllocationInterval::begin();
    let start = Instant::now();
    let report = data
        .fixture
        .node()
        .validate_shacl_delta(&AUTH, &data.data, &data.schema, changes, &options)
        .expect("run native validation");
    let duration = start.elapsed();
    let allocations = interval.finish();
    (duration, report, allocations)
}

fn run_settle(
    data: &BenchData,
    case: &MutationCase,
) -> (
    Duration,
    ShaclValidationReport,
    allocation::AllocationSample,
) {
    let interval = AllocationInterval::begin();
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
    let allocations = interval.finish();
    data.fixture
        .node()
        .apply_changes_unchecked(&case.graph, reverse(std::slice::from_ref(&case.change)))
        .expect("restore shapes mutation");
    (duration, report, allocations)
}

fn run_write(
    data: &BenchData,
) -> (
    Duration,
    ShaclValidationReport,
    allocation::AllocationSample,
) {
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
    let interval = AllocationInterval::begin();
    let start = Instant::now();
    data.fixture
        .node()
        .apply_changes(
            &AUTH,
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
    let allocations = interval.finish();
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
    (duration, report, allocations)
}

fn policy_bench(c: &mut Criterion, data: &BenchData) {
    let specs = [
        (
            "disabled_write",
            ValidationPolicy::Disabled,
            2usize,
            true,
            VALUE,
        ),
        (
            "unrelated_advisory",
            ValidationPolicy::Advisory,
            2usize,
            true,
            UNRELATED,
        ),
        (
            "relevant_advisory",
            ValidationPolicy::Advisory,
            2usize,
            true,
            VALUE,
        ),
        (
            "valid_enforce",
            ValidationPolicy::Enforce,
            1usize,
            true,
            VALUE,
        ),
        (
            "rejected_enforce",
            ValidationPolicy::Enforce,
            2usize,
            false,
            VALUE,
        ),
    ];
    let mut cases = Vec::with_capacity(specs.len());
    for (label, policy, minimum, accepts, predicate) in specs {
        let graph = GraphId::new(&format!("urn:craqle:bench:policy:{label}:data"));
        let shapes = GraphId::new(&format!("urn:craqle:bench:policy:{label}:shapes"));
        policy_setup(data, &graph, &shapes, label, predicate, minimum, policy);
        cases.push((label, graph, shapes, policy, accepts, predicate));
    }

    let config = data.fixture.config();
    let mut group = c.benchmark_group("shacl_write_policy");
    group.sample_size(config.sample_size);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    for (label, graph, shapes, policy, accepts, predicate) in &cases {
        let base_graph = data
            .fixture
            .node()
            .graph_snapshot(graph)
            .expect("read policy benchmark graph");
        let base_statuses = data
            .fixture
            .node()
            .shacl_binding_statuses(&AUTH, graph)
            .expect("read policy benchmark status");
        group.bench_function(*label, |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let changes = policy_changes(graph, label, 0, predicate);
                    let start = Instant::now();
                    let result = data
                        .fixture
                        .node()
                        .apply_changes(&AUTH, graph, changes.clone());
                    elapsed += start.elapsed();
                    assert_eq!(
                        result.is_ok(),
                        *accepts,
                        "unexpected {policy:?} write result"
                    );
                    if *accepts {
                        data.fixture
                            .node()
                            .apply_changes_unchecked(graph, reverse(&changes))
                            .expect("restore policy benchmark graph");
                        data.fixture
                            .node()
                            .unbind_shacl(&AUTH, graph, shapes)
                            .expect("unbind policy benchmark shapes");
                        policy_bind(data, graph, shapes, *policy);
                        data.fixture
                            .node()
                            .flush_search_updates()
                            .expect("settle policy benchmark search");
                    }
                    let restored = data
                        .fixture
                        .node()
                        .graph_snapshot(graph)
                        .expect("read restored policy graph");
                    assert_eq!(
                        restored.quads, base_graph.quads,
                        "policy benchmark graph was not restored"
                    );
                    if !*accepts {
                        assert_eq!(
                            restored, base_graph,
                            "rejected policy benchmark changed graph state"
                        );
                        assert_eq!(
                            data.fixture
                                .node()
                                .shacl_binding_statuses(&AUTH, graph)
                                .expect("read rejected policy status"),
                            base_statuses,
                            "rejected policy benchmark changed status"
                        );
                    }
                }
                elapsed
            })
        });
    }
    group.finish();

    for (label, graph, _shapes, policy, accepts, predicate) in cases {
        let sample = policy_sample(data, &graph, label, policy, accepts, predicate);
        let base_scope = if accepts {
            "semantic_quads_restored_search_settled_clock_advances"
        } else {
            "graph_clock_status_unchanged_on_reject"
        };
        println!(
            "shacl_incremental write_sample case={label} policy={policy:?} accepted={accepts} \
             base_scope={base_scope} validation_scope={} \
             source_keys={} qv_keys={} candidate_quads={} constraints={} \
             allocations={} allocated_bytes={} peak_live_delta_bytes={}",
            sample.validation_scope,
            sample.source_keys,
            sample.qv_keys,
            sample.candidate_quads,
            sample.constraints,
            sample.allocation.allocations,
            sample.allocation.allocated_bytes,
            sample.allocation.peak_live_delta_bytes,
        );
    }
}

fn policy_setup(
    data: &BenchData,
    graph: &GraphId,
    shapes: &GraphId,
    label: &str,
    predicate: &str,
    minimum: usize,
    policy: ValidationPolicy,
) {
    data.fixture
        .node()
        .create_crate(
            &AUTH,
            CreateCrateRequest::new(
                graph.clone(),
                "Policy benchmark",
                "Checked write policy benchmark graph.",
                "2026-08-20",
                None,
                GraphPolicy::default(),
            ),
        )
        .expect("create policy benchmark crate");
    let terms = policy_changes(graph, label, 0, predicate);
    data.fixture
        .node()
        .apply_changes_unchecked(graph, terms.clone())
        .expect("intern policy benchmark terms");
    data.fixture
        .node()
        .apply_changes_unchecked(graph, reverse(&terms))
        .expect("restore policy benchmark terms");
    data.fixture
        .node()
        .rebuild_graph_diagnostics(graph)
        .expect("settle policy benchmark crate");
    data.fixture
        .node()
        .flush_search_updates()
        .expect("settle policy benchmark search");
    data.fixture
        .node()
        .apply_changes_unchecked(shapes, policy_rows(shapes, minimum))
        .expect("seed policy benchmark shapes");
    policy_bind(data, graph, shapes, policy);
}

fn policy_bind(data: &BenchData, graph: &GraphId, shapes: &GraphId, policy: ValidationPolicy) {
    data.fixture
        .node()
        .bind_shacl(
            &AUTH,
            &ShaclBinding {
                data_graph: graph.clone(),
                shapes_graph: shapes.clone(),
                policy,
                validation_options: ShaclBindingOptions::default(),
            },
        )
        .expect("bind policy benchmark shapes");
}

fn policy_sample(
    data: &BenchData,
    graph: &GraphId,
    label: &str,
    policy: ValidationPolicy,
    accepts: bool,
    predicate: &str,
) -> PolicySample {
    let changes = policy_changes(graph, label, 0, predicate);
    let before_graph = data
        .fixture
        .node()
        .graph_snapshot(graph)
        .expect("read policy benchmark graph");
    let before_statuses = data
        .fixture
        .node()
        .shacl_binding_statuses(&AUTH, graph)
        .expect("read policy benchmark status");
    let interval = AllocationInterval::begin();
    let result = data
        .fixture
        .node()
        .apply_changes(&AUTH, graph, changes.clone());
    let allocation = interval.finish();
    assert_eq!(
        result.is_ok(),
        accepts,
        "unexpected {policy:?} sample result"
    );
    let statuses = data
        .fixture
        .node()
        .shacl_binding_statuses(&AUTH, graph)
        .expect("read policy benchmark status");
    assert!(!statuses.is_empty(), "policy benchmark status is missing");
    let status = &statuses[0];
    let (validation_scope, statistics) = match policy {
        ValidationPolicy::Disabled => {
            assert!(status.report.is_none(), "disabled write produced a report");
            ("skipped", None)
        }
        ValidationPolicy::Advisory if accepts && predicate == VALUE => {
            assert_eq!(status.state, ShaclValidationState::Invalid);
            let report = status.report.as_ref().expect("advisory report is missing");
            assert!(!report.conforms, "advisory report unexpectedly conforms");
            assert_eq!(report.results.len(), 1, "advisory report is incomplete");
            ("persisted_report", Some(&report.statistics))
        }
        ValidationPolicy::Advisory if accepts => {
            assert_eq!(status.state, ShaclValidationState::Valid);
            let report = status.report.as_ref().expect("advisory report is missing");
            assert!(report.conforms, "advisory report is invalid");
            assert!(report.results.is_empty(), "advisory report is not empty");
            ("persisted_report", Some(&report.statistics))
        }
        ValidationPolicy::Enforce if accepts => {
            assert_eq!(status.state, ShaclValidationState::Valid);
            let report = status.report.as_ref().expect("enforce report is missing");
            assert!(report.conforms, "enforce report is invalid");
            assert!(report.results.is_empty(), "enforce report is not empty");
            ("persisted_report", Some(&report.statistics))
        }
        ValidationPolicy::Enforce => {
            let after_graph = data
                .fixture
                .node()
                .graph_snapshot(graph)
                .expect("read rejected policy graph");
            assert_eq!(
                after_graph, before_graph,
                "rejected write changed graph state"
            );
            assert_eq!(statuses, before_statuses, "rejected write changed status");
            ("rejected_error_unavailable", None)
        }
        ValidationPolicy::Advisory => unreachable!(),
        _ => unreachable!(),
    };
    let read = statistics.map(|statistics| &statistics.read);
    PolicySample {
        allocation,
        validation_scope,
        source_keys: read.map_or(0, |read| read.source_keys_read),
        qv_keys: read.map_or(0, |read| read.qv_keys_read),
        candidate_quads: read.map_or(0, |read| read.candidate_quads),
        constraints: statistics.map_or(0, |statistics| statistics.constraints_evaluated),
    }
}

fn policy_rows(graph: &GraphId, minimum: usize) -> Vec<MaterializedQuadChange> {
    let shape = "<urn:craqle:bench:policy:shape>";
    let property = "<urn:craqle:bench:policy:property>";
    vec![
        add(graph, shape, RDF_TYPE, SH_NODE),
        add(graph, shape, SH_SUBJECTS, VALUE),
        add(graph, shape, SH_PROP, property),
        add(graph, property, SH_PATH, VALUE),
        add(
            graph,
            property,
            SH_MIN,
            &format!("\"{minimum}\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        ),
    ]
}

fn policy_changes(
    graph: &GraphId,
    label: &str,
    index: usize,
    predicate: &str,
) -> Vec<MaterializedQuadChange> {
    let root = format!("<{}>", graph.as_str());
    let focus = format!("<urn:craqle:bench:policy:{label}:focus:{index}>");
    let value = format!("<urn:craqle:bench:policy:{label}:value:{index}>");
    vec![
        add(graph, &root, HAS_PART, &focus),
        add(graph, &focus, RDF_TYPE, MEDIA_OBJECT),
        add(graph, &focus, NAME, &format!("\"policy-{label}-{index}\"")),
        add(graph, &focus, predicate, &value),
    ]
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
        "shacl_incremental provenance: commit={} fixture_digest={} package_version={} feature_shacl_core={} \
         corpus_version={} seed={:#x} quads={} graphs={} duplicate_percent={} \
         fixture_selectors=CRAQLE_BENCH_QUADS,CRAQLE_BENCH_GRAPHS,CRAQLE_BENCH_DUPLICATE_PERCENT",
        commit(),
        data.fixture.fixture_digest(),
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "shacl-core"),
        support::CORPUS_VERSION,
        corpus.seed,
        corpus.quads,
        corpus.graphs,
        corpus.duplicate_percent,
    );
}

#[allow(clippy::too_many_arguments)]
fn print_case(
    operation: &str,
    label: &str,
    requested_mode: ShaclExecutionMode,
    delta_size: usize,
    duration: Duration,
    report: &ShaclValidationReport,
    equal: bool,
    allocations: allocation::AllocationSample,
) {
    let stats = &report.statistics;
    let read = &stats.read;
    assert_ne!(
        0, stats.estimated_full_work,
        "{operation} {label} reported free full validation"
    );
    let validation = if operation == "validation" {
        duration.as_nanos().to_string()
    } else {
        "not_measured".to_owned()
    };
    let checked_write = if operation == "checked_write" {
        duration.as_nanos().to_string()
    } else {
        "not_measured".to_owned()
    };
    let settle = if operation == "mutation_recompile_full_settle" {
        duration.as_nanos().to_string()
    } else {
        "not_measured".to_owned()
    };
    println!(
        "shacl_incremental result: operation={operation} case={label} requested_mode={requested_mode:?} \
         selected_mode={:?} delta_size={delta_size} estimated_delta_work={} \
         estimated_full_work={} estimated_affected_shapes={} estimated_focus_nodes={} \
         affected_shapes={} focus_nodes={} index_seeks={} qv_admission_checks={} qv_counter_reads={} \
         source_keys={} qv_keys={} \
         candidate_quads={} constraints={} full_fallbacks={} elapsed_ns={} validation_ns={validation} \
         total_checked_write_ns={checked_write} total_settle_ns={settle} complete_report_equal={equal} \
         allocations={} allocated_bytes={} peak_live_delta_bytes={}",
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
        allocations.allocations,
        allocations.allocated_bytes,
        allocations.peak_live_delta_bytes,
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
