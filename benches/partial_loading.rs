use std::hint::black_box;
use std::time::Duration;

#[path = "../tests/support/perf.rs"]
mod perf;
#[path = "../tests/support/sim.rs"]
mod sim;

use craqle::{CreateCrateRequest, GrantAuthorizer, GraphId};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use perf::{append_benchmark_media_objects, bench_auth, bench_policy, env_usize_list};
use sim::CraqleCluster;

const PAGE_SIZE: usize = 1_000;
const DEFAULT_ENTITY_COUNTS: &[usize] = &[10_000, 100_000];
const DEFAULT_BATCH_SIZE: usize = 2_000;

fn bench_entity_counts() -> Vec<usize> {
    env_usize_list("CRAQLE_BENCH_ENTITY_COUNTS", DEFAULT_ENTITY_COUNTS)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    cluster: CraqleCluster,
    graph: GraphId,
    deep_cursor: Option<String>,
}

fn build_fixture(entity_count: usize) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let cluster = CraqleCluster::new(1, tmp.path()).unwrap();
    let graph = GraphId::new(&format!("urn:bench:crate-{entity_count}"));
    cluster
        .peer(0)
        .create_crate(
            &bench_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                "Criterion Partial Loading",
                "Large crate partial loading benchmark",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                bench_policy(),
            ),
        )
        .unwrap();

    for start in (0..entity_count).step_by(DEFAULT_BATCH_SIZE) {
        let batch_count = usize::min(DEFAULT_BATCH_SIZE, entity_count - start);
        append_benchmark_media_objects(
            cluster.peer(0),
            &bench_auth(),
            &graph,
            start,
            batch_count,
            "proteomics",
        );
    }

    cluster.peer(0).rebuild_graph_diagnostics(&graph).unwrap();

    let deep_cursor = (entity_count > PAGE_SIZE * 2)
        .then(|| format!("./bulk/entity-{:06}.dat", entity_count / 2 - 1));

    Fixture {
        _tmp: tmp,
        cluster,
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
        let node = fixture.cluster.peer(0);
        let anonymous = GrantAuthorizer::default();

        group.throughput(Throughput::Elements(entity_count as u64));
        group.bench_with_input(
            BenchmarkId::new("summary", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| black_box(node.export_rocrate_summary(&anonymous, graph).unwrap()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("offset_page_start", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| {
                    black_box(
                        node.export_rocrate_page(&anonymous, graph, 0, PAGE_SIZE)
                            .unwrap(),
                    )
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cursor_page_start", entity_count),
            &fixture.graph,
            |b, graph| {
                b.iter(|| {
                    black_box(
                        node.export_rocrate_page_after(&anonymous, graph, None, PAGE_SIZE)
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
                            node.export_rocrate_page_after(
                                &anonymous,
                                graph,
                                Some(cursor),
                                PAGE_SIZE,
                            )
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
