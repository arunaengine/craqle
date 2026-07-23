use std::hint::black_box;
use std::time::Duration;

#[path = "../tests/support/perf.rs"]
mod perf;
#[path = "../tests/support/sim.rs"]
mod sim;

use craqle::{CreateCrateRequest, GrantAuthorizer, GraphId};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use perf::{
    append_benchmark_media_objects, attach_contextual_entities, bench_auth, bench_policy, env_usize,
};
use sim::CraqleCluster;

const PAGE_SIZE: usize = 1_000;
const DEFAULT_CRATE_COUNT: usize = 24;
const DEFAULT_ENTITIES_PER_CRATE: usize = 50_000;
const DEFAULT_CONTEXTUALS_PER_CRATE: usize = 6;
const DEFAULT_BATCH_SIZE: usize = 10_000;
#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    crate_count: usize,
    entities_per_crate: usize,
    contextuals_per_crate: usize,
    batch_size: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            crate_count: env_usize("CRAQLE_LARGE_BENCH_CRATE_COUNT", DEFAULT_CRATE_COUNT),
            entities_per_crate: env_usize(
                "CRAQLE_LARGE_BENCH_ENTITIES_PER_CRATE",
                DEFAULT_ENTITIES_PER_CRATE,
            ),
            contextuals_per_crate: env_usize(
                "CRAQLE_LARGE_BENCH_CONTEXTUALS_PER_CRATE",
                DEFAULT_CONTEXTUALS_PER_CRATE,
            ),
            batch_size: env_usize("CRAQLE_LARGE_BENCH_BATCH_SIZE", DEFAULT_BATCH_SIZE),
        }
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    cluster: CraqleCluster,
    graph: GraphId,
    deep_cursor: Option<String>,
    count_query: String,
    fts_query: String,
}

fn build_fixture(config: BenchConfig) -> Fixture {
    assert!(config.crate_count > 0, "crate_count must be > 0");
    assert!(
        config.entities_per_crate > 0,
        "entities_per_crate must be > 0"
    );
    assert!(config.batch_size > 0, "batch_size must be > 0");
    let tmp = tempfile::tempdir().unwrap();
    let cluster = CraqleCluster::new(1, tmp.path()).unwrap();
    let node = cluster.peer(0);
    let graph = GraphId::new("urn:bench:large-rocrate-00");

    for crate_idx in 0..config.crate_count {
        let graph = GraphId::new(&format!("urn:bench:large-rocrate-{crate_idx:02}"));
        node.create_crate(
            &bench_auth(),
            CreateCrateRequest::new(
                graph.clone(),
                format!("Large Bench Crate {crate_idx}"),
                "Multi-crate large RO-Crate benchmark",
                "2025-01-01",
                Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                bench_policy(),
            ),
        )
        .unwrap();

        attach_contextual_entities(
            node,
            &bench_auth(),
            &graph,
            &format!("{crate_idx:02}"),
            config.contextuals_per_crate,
            "Bench",
        );

        let crate_keyword = format!("crate-keyword-{crate_idx:02}");
        for start in (0..config.entities_per_crate).step_by(config.batch_size) {
            let batch_count = usize::min(config.batch_size, config.entities_per_crate - start);
            append_benchmark_media_objects(
                node,
                &bench_auth(),
                &graph,
                start,
                batch_count,
                &crate_keyword,
            );
        }

        node.rebuild_graph_diagnostics(&graph).unwrap();
    }

    cluster.reindex_search().unwrap();

    let deep_cursor = (config.entities_per_crate > PAGE_SIZE)
        .then(|| format!("./bulk/entity-{:06}.dat", config.entities_per_crate / 2 - 1));
    let count_query = format!(
        "SELECT (COUNT(?s) AS ?count) WHERE {{ GRAPH <{}> {{ ?s rdf:type schema:MediaObject }} }}",
        graph.as_str()
    );
    let fts_query = format!(
        r#"
    SELECT ?s ?score
    WHERE {{
      SERVICE <urn:craqle:fts> {{
        ?s fts:query "crate-keyword-00" .
        ?s fts:score ?score .
        ?s fts:graph <{}> .
        ?s fts:limit 10 .
      }}
    }}
    ORDER BY DESC(?score)
    "#,
        graph.as_str(),
    );

    Fixture {
        _tmp: tmp,
        cluster,
        graph,
        deep_cursor,
        count_query,
        fts_query,
    }
}

fn large_rocrate_latency_benchmarks(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let fixture = build_fixture(config);
    let node = fixture.cluster.peer(0);
    let reader = GrantAuthorizer::default();

    let mut group = c.benchmark_group("large_rocrate_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(config.entities_per_crate as u64));

    group.bench_with_input(
        BenchmarkId::new("summary", config.entities_per_crate),
        &fixture.graph,
        |b, graph| {
            b.iter(|| black_box(node.export_rocrate_summary(&reader, graph).unwrap()));
        },
    );

    group.bench_with_input(
        BenchmarkId::new("cursor_page_start", config.entities_per_crate),
        &fixture.graph,
        |b, graph| {
            b.iter(|| {
                black_box(
                    node.export_rocrate_page_after(&reader, graph, None, PAGE_SIZE)
                        .unwrap(),
                )
            });
        },
    );

    if let Some(cursor) = fixture.deep_cursor.as_deref() {
        group.bench_with_input(
            BenchmarkId::new("cursor_page_deep", config.entities_per_crate),
            &fixture.graph,
            |b, graph| {
                b.iter(|| {
                    black_box(
                        node.export_rocrate_page_after(&reader, graph, Some(cursor), PAGE_SIZE)
                            .unwrap(),
                    )
                });
            },
        );
    }

    group.bench_with_input(
        BenchmarkId::new("sparql_count", config.entities_per_crate),
        &fixture.count_query,
        |b, query| {
            b.iter(|| {
                black_box(node.query(&reader, query).unwrap());
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("fts_fixed_graph", config.entities_per_crate),
        &fixture.fts_query,
        |b, query| {
            b.iter(|| {
                black_box(node.query(&reader, query).unwrap());
            });
        },
    );

    group.finish();
}

criterion_group!(benches, large_rocrate_latency_benchmarks);
criterion_main!(benches);
