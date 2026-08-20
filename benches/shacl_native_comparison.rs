use std::collections::HashSet;
use std::hint::black_box;
use std::time::Instant;

#[path = "support/allocation.rs"]
mod allocation;
#[path = "support/mod.rs"]
mod support;

use allocation::AllocationInterval;
use craqle::{
    CompiledShaclSchema, EncodedTerm, GraphId, MaterializedQuadChange, QueryResults,
    ShaclCompileOptions, ShaclValidationOptions,
};
use criterion::{Criterion, criterion_group, criterion_main};
use rudof_rdf::rdf_core::RDFFormat;
use rudof_rdf::rdf_impl::{OxigraphInMemory, ReaderMode};
use shacl::ir::IRSchema;
use shacl::rdf::ShaclParser;
use shacl::validator::ShaclValidationMode;
use shacl::validator::processor::{GraphValidation, ShaclProcessor};
use shacl::validator::report::ValidationReport;
use shacl::validator::store::Graph;
use support::fixture::Fixture;

const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SHAPES: &str = r#"
<urn:craqle:bench:shacl:native-shape> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/shacl#NodeShape> .
<urn:craqle:bench:shacl:native-shape> <http://www.w3.org/ns/shacl#targetSubjectsOf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .
<urn:craqle:bench:shacl:native-shape> <http://www.w3.org/ns/shacl#property> _:property .
_:property <http://www.w3.org/ns/shacl#path> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .
_:property <http://www.w3.org/ns/shacl#maxCount> "0"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

struct CopiedData {
    graph: OxigraphInMemory,
    triples: usize,
    serialized_bytes: usize,
    data_hash: String,
}

fn shacl_native_comparison(c: &mut Criterion) {
    let fixture = Fixture::from_environment();
    let config = fixture.config();
    let data_graph = GraphId::new("urn:craqle:bench:shacl:paired-data-graph");
    let shapes_graph = GraphId::new("urn:craqle:bench:shacl:native-shapes-graph");
    let setup_copy = prepare_paired_data(&fixture, &data_graph);
    let setup_triples = setup_copy.triples;
    let setup_bytes = setup_copy.serialized_bytes;
    let setup_hash = setup_copy.data_hash.clone();
    let expected_violations = expected_violation_count(&setup_copy.graph);
    drop(setup_copy);
    insert_shapes(&fixture, &shapes_graph);

    let external_start = Instant::now();
    let external_allocations = AllocationInterval::begin();
    let copy_start = Instant::now();
    let copied = copy_named_data(&fixture, &data_graph);
    let copy_duration = copy_start.elapsed();
    let external_copy_triples = copied.triples;
    let external_copy_bytes = copied.serialized_bytes;
    let external_hash = copied.data_hash.clone();
    assert_eq!(external_hash, setup_hash);
    let validation_options = ShaclValidationOptions {
        max_results: expected_violations.saturating_add(1),
        ..ShaclValidationOptions::default()
    };
    let parse_compile_start = Instant::now();
    let rudof_schema = compile_rudof_shapes();
    let parse_compile_duration = parse_compile_start.elapsed();
    let validation_start = Instant::now();
    let rudof_report = validate_rudof(copied.graph, &rudof_schema);
    let external_validation_duration = validation_start.elapsed();
    let external_duration = external_start.elapsed();
    let external_allocations = external_allocations.finish();
    assert_report(&rudof_report, expected_violations);
    drop(rudof_report);
    drop(rudof_schema);

    let native_start = Instant::now();
    let native_allocations = AllocationInterval::begin();
    let native_schema = fixture
        .node()
        .compile_shacl(
            &craqle::AllowAllAuthorizer,
            &shapes_graph,
            &ShaclCompileOptions::default(),
        )
        .expect("compile native benchmark shapes");
    let native_report = fixture
        .node()
        .validate_shacl(
            &craqle::AllowAllAuthorizer,
            &data_graph,
            &native_schema,
            &validation_options,
        )
        .expect("run uncached native validation");
    let native_duration = native_start.elapsed();
    let native_allocations = native_allocations.finish();
    assert_eq!(native_report.results.len(), expected_violations);

    let cached_start = Instant::now();
    let cached_allocations = AllocationInterval::begin();
    let cached_report = fixture
        .node()
        .validate_shacl(
            &craqle::AllowAllAuthorizer,
            &data_graph,
            &native_schema,
            &validation_options,
        )
        .expect("run cached native validation");
    let cached_duration = cached_start.elapsed();
    let cached_allocations = cached_allocations.finish();
    assert_eq!(cached_report.results, native_report.results);
    assert!(cached_report.statistics.shape_compile_cache_hit);

    println!(
        "shacl_native_comparison corpus_quads={} graphs={} duplicate_percent={} data_graph={} \
         focus_nodes={} violations={} setup_copy_triples={} setup_copy_bytes={} \
         external_copy_triples={} external_copy_bytes={} data_hash={}",
        config.corpus.quads,
        config.corpus.graphs,
        config.corpus.duplicate_percent,
        data_graph,
        expected_violations,
        native_report.results.len(),
        setup_triples,
        setup_bytes,
        external_copy_triples,
        external_copy_bytes,
        setup_hash,
    );
    println!(
        "shacl_native_comparison external total_ns={} copy_ns={} parse_compile_ns={} \
         validation_ns={} allocations={} allocated_bytes={} peak_live_delta_bytes={} \
         data_copy_bytes={}",
        external_duration.as_nanos(),
        copy_duration.as_nanos(),
        parse_compile_duration.as_nanos(),
        external_validation_duration.as_nanos(),
        external_allocations.allocations,
        external_allocations.allocated_bytes,
        external_allocations.peak_live_delta_bytes,
        external_copy_bytes,
    );
    print_native(
        "fresh_shapes",
        native_duration,
        native_allocations,
        &native_schema,
        &native_report,
        true,
    );
    print_native(
        "cached_shapes",
        cached_duration,
        cached_allocations,
        &native_schema,
        &cached_report,
        false,
    );

    let mut group = c.benchmark_group("shacl_native_comparison");
    group.sample_size(10);
    group.warm_up_time(config.warm_up);
    group.measurement_time(config.measurement);
    group.bench_function("external_export_copy_parse_validate", |b| {
        b.iter(|| {
            let copied = copy_named_data(&fixture, &data_graph);
            let schema = compile_rudof_shapes();
            black_box(validate_rudof(copied.graph, &schema))
        })
    });
    group.bench_function("native_cached_shapes_validate", |b| {
        b.iter(|| {
            black_box(
                fixture
                    .node()
                    .validate_shacl(
                        &craqle::AllowAllAuthorizer,
                        &data_graph,
                        &native_schema,
                        &validation_options,
                    )
                    .expect("run cached native validation"),
            )
        })
    });
    group.bench_function("native_compile_cache_lookup_and_validate", |b| {
        b.iter(|| {
            let schema = fixture
                .node()
                .compile_shacl(
                    &craqle::AllowAllAuthorizer,
                    &shapes_graph,
                    &ShaclCompileOptions::default(),
                )
                .expect("look up cached native shapes");
            black_box(
                fixture
                    .node()
                    .validate_shacl(
                        &craqle::AllowAllAuthorizer,
                        &data_graph,
                        &schema,
                        &validation_options,
                    )
                    .expect("run native validation after compile-cache lookup"),
            )
        })
    });
    group.finish();
}

fn prepare_paired_data(fixture: &Fixture, graph: &GraphId) -> CopiedData {
    let QueryResults::Graph(triples) = fixture.run_visible_query(
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
        "paired SHACL data setup",
    ) else {
        panic!("paired SHACL data setup must return triples");
    };
    let serialized_bytes: usize = triples
        .iter()
        .map(|(subject, predicate, object)| {
            subject.0.len() + predicate.0.len() + object.0.len() + 5
        })
        .sum();
    let changes = triples
        .iter()
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            },
        )
        .collect();
    fixture
        .node()
        .apply_changes_bulk_unchecked(graph, changes)
        .expect("insert paired native data graph");
    fixture
        .node()
        .rebuild_graph_diagnostics(graph)
        .expect("rebuild paired native data diagnostics");
    fixture
        .node()
        .flush_search_updates()
        .expect("settle paired native data search work");
    fixture
        .node()
        .persist_fjall()
        .expect("persist paired native data graph");
    let graph_copy = copy_named_data(fixture, graph);
    assert_eq!(graph_copy.triples, triples.len());
    assert_eq!(graph_copy.serialized_bytes, serialized_bytes);
    graph_copy
}

fn insert_shapes(fixture: &Fixture, graph: &GraphId) {
    let parsed =
        OxigraphInMemory::from_str(SHAPES, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .expect("parse fixed native benchmark shapes");
    let changes = parsed
        .quads()
        .map(|quad| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object),
        })
        .collect();
    fixture
        .node()
        .apply_changes_unchecked(graph, changes)
        .expect("insert fixed native benchmark shapes");
}

fn copy_named_data(fixture: &Fixture, graph: &GraphId) -> CopiedData {
    let query = format!(
        "CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{}> {{ ?s ?p ?o }} }}",
        graph.as_str()
    );
    let QueryResults::Graph(triples) = fixture
        .node()
        .query_graphs(std::slice::from_ref(graph), &query)
        .expect("paired external SHACL data export query failed")
    else {
        panic!("paired external SHACL data export must return triples");
    };
    let mut ntriples = String::new();
    for (subject, predicate, object) in &triples {
        ntriples.push_str(&subject.0);
        ntriples.push(' ');
        ntriples.push_str(&predicate.0);
        ntriples.push(' ');
        ntriples.push_str(&object.0);
        ntriples.push_str(" .\n");
    }
    let serialized_bytes = ntriples.len();
    let data_hash = blake3::hash(ntriples.as_bytes()).to_hex().to_string();
    let graph =
        OxigraphInMemory::from_str(&ntriples, &RDFFormat::NTriples, None, &ReaderMode::Strict)
            .expect("load paired external data copy");
    CopiedData {
        graph,
        triples: triples.len(),
        serialized_bytes,
        data_hash,
    }
}

fn compile_rudof_shapes() -> IRSchema {
    let graph = OxigraphInMemory::from_str(SHAPES, &RDFFormat::NTriples, None, &ReaderMode::Strict)
        .expect("parse paired Rudof shapes");
    let mut parser = ShaclParser::new(graph);
    parser
        .parse()
        .expect("parse paired shapes through Rudof")
        .try_into()
        .expect("compile paired Rudof shape IR")
}

fn validate_rudof(data: OxigraphInMemory, schema: &IRSchema) -> ValidationReport {
    let mut validator: GraphValidation = Graph::from(data).into();
    validator
        .validate(schema, &ShaclValidationMode::Native)
        .expect("run paired Rudof validation")
}

fn expected_violation_count(data: &OxigraphInMemory) -> usize {
    data.quads()
        .filter(|quad| quad.predicate.as_str() == RDF_TYPE_IRI)
        .map(|quad| quad.subject)
        .collect::<HashSet<_>>()
        .len()
}

fn assert_report(report: &ValidationReport, expected: usize) {
    assert!(expected > 0);
    assert!(!report.conforms());
    assert_eq!(report.results().len(), expected);
}

fn print_native(
    label: &str,
    duration: std::time::Duration,
    allocations: allocation::AllocationSample,
    schema: &CompiledShaclSchema,
    report: &craqle::ShaclValidationReport,
    include_shape_compile: bool,
) {
    let read = &report.statistics.read;
    let (parse_time, compile_time) = if include_shape_compile {
        (
            schema.statistics().parse_time,
            schema.statistics().compile_time,
        )
    } else {
        Default::default()
    };
    let compile_time = compile_time + report.statistics.compile_time;
    println!(
        "shacl_native_comparison native mode={label} total_ns={} parse_ns={} compile_ns={} \
         validation_ns={} cache_hit={} shapes={} focus_nodes={} violations={} data_copy_bytes=0 \
         source_keys={} qv_keys={} candidate_quads={} terms_decoded={} allocations={} \
         allocated_bytes={} peak_live_delta_bytes={}",
        duration.as_nanos(),
        parse_time.as_nanos(),
        compile_time.as_nanos(),
        report.statistics.target_time.as_nanos()
            + report.statistics.constraint_time.as_nanos()
            + report.statistics.report_time.as_nanos(),
        report.statistics.shape_compile_cache_hit,
        report.statistics.shapes_executed,
        report.statistics.focus_nodes,
        report.statistics.violations,
        read.source_keys_read,
        read.qv_keys_read,
        read.candidate_quads,
        read.terms_decoded,
        allocations.allocations,
        allocations.allocated_bytes,
        allocations.peak_live_delta_bytes,
    );
}

criterion_group!(benches, shacl_native_comparison);
criterion_main!(benches);
