use std::time::Duration;

use aruna_core::{EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use aruna_rocrate::RoCrateManager;
use aruna_sync::SyncNetwork;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

const PAGE_SIZE: usize = 1_000;
const DEFAULT_ENTITY_COUNTS: &[usize] = &[10_000, 100_000];
const DEFAULT_BATCH_SIZE: usize = 2_000;

fn literal_term(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
}

fn bulk_media_object_changes(
    graph: &GraphId,
    start: usize,
    count: usize,
    keyword: &str,
) -> Vec<MaterializedQuadChange> {
    let mut changes = Vec::with_capacity(count * 6);
    for idx in start..start + count {
        let entity = format!("./bulk/entity-{idx:06}.dat");
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&vocab::root_entity()),
            predicate: EncodedTerm::from_named_node(&vocab::schema_has_part()),
            object: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
        });
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            predicate: EncodedTerm::from_named_node(&vocab::rdf_type()),
            object: EncodedTerm::from_named_node(&vocab::schema_media_object()),
        });
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
            object: literal_term(&format!("Proteomics sample {idx}")),
        });
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            predicate: EncodedTerm::from_named_node(&vocab::schema_description()),
            object: literal_term(&format!("{keyword} benchmark record {idx}")),
        });
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            predicate: EncodedTerm::from_named_node(&vocab::schema_keywords()),
            object: literal_term(keyword),
        });
        changes.push(MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(&entity)),
            predicate: EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                "http://schema.org/identifier",
            )),
            object: literal_term(&format!("BENCH-{idx:06}")),
        });
    }
    changes
}

fn bench_entity_counts() -> Vec<usize> {
    std::env::var("ARUNA_BENCH_ENTITY_COUNTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|entry| entry.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| DEFAULT_ENTITY_COUNTS.to_vec())
}

struct Fixture {
    _tmp: tempfile::TempDir,
    net: SyncNetwork,
    graph: GraphId,
    deep_cursor: Option<String>,
}

fn build_fixture(entity_count: usize) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let net = SyncNetwork::new(1, tmp.path()).unwrap();
    let graph = GraphId::new(&format!("urn:bench:crate-{entity_count}"));
    let mgr = RoCrateManager::new(net.peer(0).engine.clone());
    mgr.create_crate(
        graph.clone(),
        "Criterion Partial Loading",
        "Large crate partial loading benchmark",
        "2025-01-01",
        "https://creativecommons.org/licenses/by/4.0/",
    )
    .unwrap();

    for start in (0..entity_count).step_by(DEFAULT_BATCH_SIZE) {
        let batch_count = usize::min(DEFAULT_BATCH_SIZE, entity_count - start);
        net.peer(0)
            .engine
            .local_apply_changes_unchecked(
                &graph,
                bulk_media_object_changes(&graph, start, batch_count, "proteomics"),
            )
            .unwrap();
    }

    let deep_cursor = (entity_count > PAGE_SIZE * 2)
        .then(|| format!("./bulk/entity-{:06}.dat", entity_count / 2 - 1));

    Fixture {
        _tmp: tmp,
        net,
        graph,
        deep_cursor,
    }
}

fn partial_loading_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("partial_loading");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for entity_count in bench_entity_counts() {
        let fixture = build_fixture(entity_count);
        let mgr = RoCrateManager::new(fixture.net.peer(0).engine.clone());

        group.throughput(Throughput::Elements(entity_count as u64));
        group.bench_with_input(
            BenchmarkId::new("summary", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| black_box(mgr.export_jsonld_summary(graph).unwrap()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("offset_page_start", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| black_box(mgr.export_jsonld_page(graph, 0, PAGE_SIZE).unwrap()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cursor_page_start", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| {
                    black_box(
                        mgr.export_jsonld_page_after(graph, None, PAGE_SIZE)
                            .unwrap(),
                    )
                });
            },
        );

        if let Some(cursor) = fixture.deep_cursor.as_deref() {
            group.bench_with_input(
                BenchmarkId::new("cursor_page_deep", entity_count),
                &fixture.graph,
                |b, graph| {
                    b.iter(|| {
                        black_box(
                            mgr.export_jsonld_page_after(graph, Some(cursor), PAGE_SIZE)
                                .unwrap(),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, partial_loading_benchmarks);
criterion_main!(benches);
