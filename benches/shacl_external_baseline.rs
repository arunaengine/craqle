//! External-copy SHACL baseline for the later Craqle-native validator.
//!
//! This deliberately exports the visible Craqle graph into Rudof's in-memory
//! Oxigraph graph. It is comparison-only: production validation must not take
//! this path. Rudof's Native API returns a completed report, so it cannot
//! expose a true time-to-first-violation measurement.

use std::collections::HashSet;
use std::hint::black_box;
use std::time::Instant;

#[cfg(target_os = "linux")]
use std::fs;

use craqle::QueryResults;
use criterion::{Criterion, criterion_group, criterion_main};
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};
use shacl::ir::IRSchema;
use shacl::rdf::ShaclParser;
use shacl::validator::ShaclValidationMode;
use shacl::validator::processor::{GraphValidation, ShaclProcessor};
use shacl::validator::report::ValidationReport;
use shacl::validator::store::Graph;

#[path = "support/mod.rs"]
mod support;

use support::QUADS_10M;
use support::fixture::Fixture;

const COPY_VISIBLE_QUERY: &str = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const FIXED_SHAPES: &str = r#"
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix sh: <http://www.w3.org/ns/shacl#> .

    <urn:craqle:bench:shacl:external-baseline-shape> a sh:NodeShape ;
        sh:targetSubjectsOf rdf:type ;
        sh:property [
            sh:path rdf:type ;
            sh:maxCount 0
        ] .
"#;

struct CopiedData {
    graph: OxigraphInMemory,
    exported_triples: usize,
    copied_triples: usize,
    serialized_bytes: usize,
}

fn shacl_external_baseline(c: &mut Criterion) {
    print_process_memory("before_fixture", process_memory());
    let fixture = Fixture::from_environment();
    print_process_memory("after_fixture", process_memory());
    let config = fixture.config();
    let metadata = config.corpus.metadata();
    if config.corpus.quads == QUADS_10M {
        println!(
            "shacl_external_baseline: 10M corpus explicitly selected with \
             CRAQLE_BENCH_QUADS; running the external-copy comparison"
        );
    }
    println!(
        "shacl_external_baseline corpus: version={} seed={:#x} quads={} graphs={} \
         duplicate_percent={} visible_graphs={} hidden_graphs={}",
        metadata.version,
        metadata.seed,
        metadata.quads,
        metadata.graphs,
        metadata.duplicate_percent,
        metadata.visible_graphs,
        metadata.hidden_graphs,
    );

    let shapes = FIXED_SHAPES;
    let total_start = Instant::now();

    let copy_start = Instant::now();
    let copied = copy_visible_data(&fixture);
    let copy_duration = copy_start.elapsed();
    print_process_memory("after_visible_data_export_and_copy", process_memory());
    let expected_violations = expected_violation_count(&copied.graph);

    let compile_start = Instant::now();
    let schema = compile_shapes(shapes);
    let compile_duration = compile_start.elapsed();
    print_process_memory("after_shapes_parse_and_ir_compile", process_memory());

    let validation_start = Instant::now();
    let report = validate_native(copied.graph.clone(), &schema);
    let validation_duration = validation_start.elapsed();
    assert_fixed_violations(&report, expected_violations);
    let total_duration = total_start.elapsed();
    print_process_memory("after_native_validation", process_memory());

    println!(
        "shacl_external_baseline phases: data_copy_export_ms={:.3} \
         shapes_parse_ir_compile_ms={:.3} native_validation_completion_ms={:.3} \
         total_ms={:.3} exported_triples={} copied_unique_triples={} \
         serialized_ntriples_bytes={} expected_violations={} violations={} conforms={}",
        duration_millis(copy_duration),
        duration_millis(compile_duration),
        duration_millis(validation_duration),
        duration_millis(total_duration),
        copied.exported_triples,
        copied.copied_triples,
        copied.serialized_bytes,
        expected_violations,
        report.results().len(),
        report.conforms(),
    );
    println!(
        "shacl_external_baseline first_violation: unavailable; Rudof Native returns only a \
         completed ValidationReport, so native_validation_completion_ms is an upper bound for \
         first-violation time, not a true measurement."
    );

    let mut group = c.benchmark_group("shacl_external_baseline");
    group.sample_size(10);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    group.bench_function("visible_data_export_and_rudof_copy", |b| {
        b.iter(|| black_box(copy_visible_data(&fixture)));
    });
    group.bench_function("shapes_parse_and_ir_compile", |b| {
        b.iter(|| black_box(compile_shapes(shapes)));
    });
    group.bench_function("retained_copy_native_validate", |b| {
        b.iter(|| black_box(validate_native(copied.graph.clone(), &schema)));
    });
    group.bench_function("full_visible_export_copy_parse_native_validate", |b| {
        b.iter(|| {
            let copied = copy_visible_data(&fixture);
            let schema = compile_shapes(shapes);
            black_box(validate_native(copied.graph, &schema))
        });
    });
    group.finish();
}

fn copy_visible_data(fixture: &Fixture) -> CopiedData {
    let QueryResults::Graph(triples) =
        fixture.run_visible_query(COPY_VISIBLE_QUERY, "external SHACL data export")
    else {
        panic!("external SHACL data export must return CONSTRUCT triples");
    };
    assert!(
        !triples.is_empty(),
        "external SHACL data export must include visible triples"
    );

    let exported_triples = triples.len();
    let mut ntriples = String::new();
    for (subject, predicate, object) in triples {
        ntriples.push_str(&subject.0);
        ntriples.push(' ');
        ntriples.push_str(&predicate.0);
        ntriples.push(' ');
        ntriples.push_str(&object.0);
        ntriples.push_str(" .\n");
    }
    let serialized_bytes = ntriples.len();
    let graph =
        OxigraphInMemory::from_str(&ntriples, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .expect("load exported visible data into Rudof OxigraphInMemory");
    let copied_triples = graph.len();
    assert!(
        copied_triples > 0 && copied_triples <= exported_triples,
        "Rudof copy must contain visible exported data and preserve graph de-duplication"
    );

    CopiedData {
        graph,
        exported_triples,
        copied_triples,
        serialized_bytes,
    }
}

fn compile_shapes(shapes: &str) -> IRSchema {
    let graph = OxigraphInMemory::from_str(shapes, &RDFFormat::Turtle, None, &ReaderMode::Strict)
        .expect("parse fixed SHACL shape graph");
    let mut parser = ShaclParser::new(graph);
    let ast = parser
        .parse()
        .expect("parse fixed SHACL shapes through Rudof");
    ast.try_into()
        .expect("compile fixed SHACL shapes into Rudof IR")
}

fn validate_native(data: OxigraphInMemory, schema: &IRSchema) -> ValidationReport {
    let mut validator: GraphValidation = Graph::from(data).into();
    validator
        .validate(schema, &ShaclValidationMode::Native)
        .expect("run Rudof Native SHACL validation")
}

fn expected_violation_count(data: &OxigraphInMemory) -> usize {
    let subjects: HashSet<_> = data
        .quads()
        .filter(|quad| quad.predicate.as_str() == RDF_TYPE_IRI)
        .map(|quad| quad.subject)
        .collect();
    let count = subjects.len();
    assert!(
        count > 0,
        "the fixed SHACL target must find at least one rdf:type subject"
    );
    count
}

fn assert_fixed_violations(report: &ValidationReport, expected: usize) {
    assert!(
        !report.conforms(),
        "the fixed max-count shape must not conform"
    );
    assert_eq!(
        report.results().len(),
        expected,
        "the fixed max-count shape must produce one violation per rdf:type subject"
    );
}

fn duration_millis(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[derive(Clone, Copy)]
struct ProcessMemory {
    rss_bytes: Option<u64>,
    hwm_bytes: Option<u64>,
}

fn print_process_memory(phase: &str, sample: ProcessMemory) {
    match (sample.rss_bytes, sample.hwm_bytes) {
        (Some(rss), Some(hwm)) => println!(
            "shacl_external_baseline rss: phase={phase} vmrss_bytes={rss} vmhwm_bytes={hwm}"
        ),
        _ => println!(
            "shacl_external_baseline rss: phase={phase} vmrss_bytes=unavailable vmhwm_bytes=unavailable"
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

criterion_group!(benches, shacl_external_baseline);
criterion_main!(benches);
