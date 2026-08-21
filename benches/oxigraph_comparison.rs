//! Focused same-corpus Craqle versus Oxigraph SPARQL comparison.
//!
//! Oxigraph 0.5.9 runs in memory, which favors Oxigraph relative to Craqle's
//! durable Fjall store. The comparison therefore covers warm, fully consumed
//! query execution only; it does not claim database-size or durable-load
//! parity. Ten million quads are deliberately rejected by this executable.

mod support;

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use craqle::{QueryExecutionOptions, QueryResults as CraqleResults};
use oxigraph::model::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults as OxigraphResults, SparqlEvaluator};
use oxigraph::store::Store;

use support::fixture::{BenchConfig, Fixture, graph_id, object_term, predicate_term, subject_term};
use support::{DEFAULT_SEED, DeterministicCorpus, QUADS_1M, QUADS_10K, QuadSpec};

const DEFAULT_GRAPHS: usize = 32;
const DEFAULT_SAMPLES: usize = 10;

#[derive(Debug, PartialEq, Eq)]
enum CanonicalResults {
    Boolean(bool),
    Solutions(Vec<Vec<(String, String)>>),
    Graph(Vec<(String, String, String)>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResultWork {
    rows: usize,
    bindings: usize,
}

fn main() {
    let quads = env_usize("CRAQLE_COMPARE_QUADS", QUADS_10K);
    assert!(
        matches!(quads, QUADS_10K | QUADS_1M),
        "CRAQLE_COMPARE_QUADS must be 10000 or 1000000; 10M is not authorized"
    );
    let graphs = env_usize("CRAQLE_COMPARE_GRAPHS", DEFAULT_GRAPHS);
    let samples = env_usize("CRAQLE_COMPARE_SAMPLES", DEFAULT_SAMPLES);
    assert!(samples >= 10, "CRAQLE_COMPARE_SAMPLES must be at least 10");

    let corpus = support::CorpusConfig::new(quads, graphs, 0, DEFAULT_SEED)
        .expect("comparison corpus must be a supported zero-duplicate configuration");
    let config = BenchConfig {
        corpus,
        warm_up: Duration::from_secs(1),
        measurement: Duration::from_secs(1),
        sample_size: samples,
        load_batch: support::fixture::LOAD_BATCH_SIZE,
    };

    let craqle_load_started = Instant::now();
    let fixture = Fixture::load(config);
    let craqle_load = craqle_load_started.elapsed();
    let semantic_report = fixture.assert_semantics();
    fixture.print_provenance("oxigraph_comparison");
    fixture.print_report(&semantic_report);

    let oxigraph_load_started = Instant::now();
    let oxigraph = load_oxigraph(corpus);
    let oxigraph_load = oxigraph_load_started.elapsed();
    println!(
        "oxigraph_comparison setup: craqle_load_ms={:.3} oxigraph_load_ms={:.3} \
         craqle_persistence=fjall_durable oxigraph_persistence=in_memory \
         oxigraph_version=0.5.9",
        millis(craqle_load),
        millis(oxigraph_load),
    );

    fixture.print_hot_work();
    for index in 0..fixture.hot_path_count() {
        compare_case(&fixture, &oxigraph, index, samples);
    }
}

fn load_oxigraph(corpus: support::CorpusConfig) -> Store {
    let store = Store::new().expect("create Oxigraph in-memory store");
    let mut loader = store.bulk_loader();
    loader
        .load_quads(
            DeterministicCorpus::new(corpus)
                .expect("validated comparison corpus")
                .iter()
                .map(oxigraph_quad),
        )
        .expect("load comparison corpus into Oxigraph");
    loader.commit().expect("commit Oxigraph bulk load");
    store
}

fn oxigraph_quad(record: QuadSpec) -> Quad {
    let Term::NamedNode(subject) = subject_term(record.subject)
        .to_term()
        .expect("benchmark subject must decode")
    else {
        panic!("benchmark subject must be an IRI")
    };
    let Term::NamedNode(predicate) = predicate_term(record.predicate)
        .to_term()
        .expect("benchmark predicate must decode")
    else {
        panic!("benchmark predicate must be an IRI")
    };
    let object = object_term(record.object)
        .to_term()
        .expect("benchmark object must decode");
    let graph = NamedNode::new(graph_id(record.graph as usize).as_str())
        .expect("benchmark graph must be an IRI");
    Quad::new(subject, predicate, object, graph)
}

fn compare_case(fixture: &Fixture, oxigraph: &Store, index: usize, samples: usize) {
    let label = fixture.hot_path_label(index);
    let query = fixture.hot_path_query(index);
    let craqle_prepared = fixture.prepare_hot_path(index);
    let oxigraph_prepared = prepare_oxigraph(query, fixture);

    let craqle_canonical = canonicalize_craqle(fixture.run_hot_path(index));
    let oxigraph_canonical = canonicalize_oxigraph(
        oxigraph_prepared
            .clone()
            .on_store(oxigraph)
            .execute()
            .unwrap_or_else(|error| panic!("{label}: Oxigraph parity query failed: {error}")),
    );
    let parity = if fixture.hot_path_is_unordered_limit(index) {
        assert_eq!(
            result_work_canonical(&craqle_canonical),
            result_work_canonical(&oxigraph_canonical),
            "{label}: unordered LIMIT result shape differs"
        );
        "valid_unordered_limit"
    } else {
        assert_eq!(
            craqle_canonical, oxigraph_canonical,
            "{label}: Craqle and Oxigraph results differ"
        );
        "exact_normalized"
    };
    let craqle_digest = canonical_digest(&craqle_canonical);
    let oxigraph_digest = canonical_digest(&oxigraph_canonical);

    let options = QueryExecutionOptions::default();
    black_box(fixture.run_hot_prepared(&craqle_prepared, &options).results);
    black_box(consume_oxigraph(
        oxigraph_prepared
            .clone()
            .on_store(oxigraph)
            .execute()
            .unwrap_or_else(|error| panic!("{label}: Oxigraph warm-up failed: {error}")),
    ));

    let mut craqle_times = Vec::with_capacity(samples);
    let mut oxigraph_times = Vec::with_capacity(samples);
    for sample in 0..samples {
        if sample.is_multiple_of(2) {
            craqle_times.push(time_craqle(fixture, &craqle_prepared, &options));
            oxigraph_times.push(time_oxigraph(oxigraph, &oxigraph_prepared, label));
        } else {
            oxigraph_times.push(time_oxigraph(oxigraph, &oxigraph_prepared, label));
            craqle_times.push(time_craqle(fixture, &craqle_prepared, &options));
        }
    }

    let craqle_p50 = nearest_rank(&craqle_times, 50);
    let oxigraph_p50 = nearest_rank(&oxigraph_times, 50);
    println!(
        "oxigraph_comparison result: case={label} samples={samples} parity={parity} \
         craqle_result_digest={craqle_digest} oxigraph_result_digest={oxigraph_digest} \
         craqle_p50_ns={} craqle_p95_nearest_rank_ns={} \
         oxigraph_p50_ns={} oxigraph_p95_nearest_rank_ns={} p50_ratio={:.3}",
        craqle_p50.as_nanos(),
        nearest_rank(&craqle_times, 95).as_nanos(),
        oxigraph_p50.as_nanos(),
        nearest_rank(&oxigraph_times, 95).as_nanos(),
        craqle_p50.as_secs_f64() / oxigraph_p50.as_secs_f64(),
    );
}

fn prepare_oxigraph(query: &str, fixture: &Fixture) -> PreparedSparqlQuery {
    let visible: Vec<_> = fixture
        .visible_graphs()
        .iter()
        .map(|graph| NamedNode::new(graph.as_str()).expect("benchmark graph must be an IRI"))
        .collect();
    let mut prepared = SparqlEvaluator::new()
        .parse_query(query)
        .expect("prepare Oxigraph comparison query");
    prepared
        .dataset_mut()
        .set_default_graph(visible.iter().cloned().map(GraphName::NamedNode).collect());
    prepared.dataset_mut().set_available_named_graphs(
        visible
            .into_iter()
            .map(NamedOrBlankNode::NamedNode)
            .collect(),
    );
    prepared
}

fn time_craqle(
    fixture: &Fixture,
    prepared: &craqle::PreparedQuery,
    options: &QueryExecutionOptions,
) -> Duration {
    let started = Instant::now();
    let execution = fixture.run_hot_prepared(prepared, options);
    let elapsed = started.elapsed();
    black_box(result_work_craqle(&execution.results));
    elapsed
}

fn time_oxigraph(store: &Store, prepared: &PreparedSparqlQuery, label: &str) -> Duration {
    let started = Instant::now();
    let results = prepared
        .clone()
        .on_store(store)
        .execute()
        .unwrap_or_else(|error| panic!("{label}: Oxigraph timed query failed: {error}"));
    let work = consume_oxigraph(results);
    let elapsed = started.elapsed();
    black_box(work);
    elapsed
}

fn result_work_craqle(results: &CraqleResults) -> ResultWork {
    match results {
        CraqleResults::Boolean(_) => ResultWork {
            rows: 1,
            bindings: 0,
        },
        CraqleResults::Solutions(rows) => ResultWork {
            rows: rows.len(),
            bindings: rows.iter().map(std::collections::HashMap::len).sum(),
        },
        CraqleResults::Graph(triples) => ResultWork {
            rows: triples.len(),
            bindings: triples.len() * 3,
        },
    }
}

fn result_work_canonical(results: &CanonicalResults) -> ResultWork {
    match results {
        CanonicalResults::Boolean(_) => ResultWork {
            rows: 1,
            bindings: 0,
        },
        CanonicalResults::Solutions(rows) => ResultWork {
            rows: rows.len(),
            bindings: rows.iter().map(Vec::len).sum(),
        },
        CanonicalResults::Graph(triples) => ResultWork {
            rows: triples.len(),
            bindings: triples.len() * 3,
        },
    }
}

fn consume_oxigraph(results: OxigraphResults<'_>) -> ResultWork {
    match results {
        OxigraphResults::Boolean(_) => ResultWork {
            rows: 1,
            bindings: 0,
        },
        OxigraphResults::Solutions(solutions) => {
            let mut work = ResultWork {
                rows: 0,
                bindings: 0,
            };
            for solution in solutions {
                let solution = solution.expect("read Oxigraph solution");
                work.rows += 1;
                work.bindings += solution.iter().count();
                black_box(solution);
            }
            work
        }
        OxigraphResults::Graph(triples) => {
            let mut rows = 0;
            for triple in triples {
                black_box(triple.expect("read Oxigraph graph result"));
                rows += 1;
            }
            ResultWork {
                rows,
                bindings: rows * 3,
            }
        }
    }
}

fn canonicalize_craqle(results: CraqleResults) -> CanonicalResults {
    match results {
        CraqleResults::Boolean(value) => CanonicalResults::Boolean(value),
        CraqleResults::Solutions(rows) => {
            let mut rows: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    let mut row: Vec<_> = row
                        .into_iter()
                        .map(|(variable, term)| (variable, term.0))
                        .collect();
                    row.sort();
                    row
                })
                .collect();
            rows.sort();
            CanonicalResults::Solutions(rows)
        }
        CraqleResults::Graph(triples) => {
            let mut triples: Vec<_> = triples
                .into_iter()
                .map(|(subject, predicate, object)| (subject.0, predicate.0, object.0))
                .collect();
            triples.sort();
            CanonicalResults::Graph(triples)
        }
    }
}

fn canonicalize_oxigraph(results: OxigraphResults<'_>) -> CanonicalResults {
    match results {
        OxigraphResults::Boolean(value) => CanonicalResults::Boolean(value),
        OxigraphResults::Solutions(solutions) => {
            let mut rows = Vec::new();
            for solution in solutions {
                let solution = solution.expect("read Oxigraph parity solution");
                let mut row: Vec<_> = solution
                    .iter()
                    .map(|(variable, term)| (variable.as_str().to_string(), term.to_string()))
                    .collect();
                row.sort();
                rows.push(row);
            }
            rows.sort();
            CanonicalResults::Solutions(rows)
        }
        OxigraphResults::Graph(results) => {
            let mut triples = Vec::new();
            for triple in results {
                let triple = triple.expect("read Oxigraph parity graph result");
                triples.push((
                    triple.subject.to_string(),
                    triple.predicate.to_string(),
                    triple.object.to_string(),
                ));
            }
            triples.sort();
            CanonicalResults::Graph(triples)
        }
    }
}

fn canonical_digest(results: &CanonicalResults) -> String {
    blake3::hash(format!("{results:?}").as_bytes())
        .to_hex()
        .to_string()
}

fn nearest_rank(samples: &[Duration], percentile: usize) -> Duration {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let rank = (samples.len() * percentile).div_ceil(100).max(1);
    samples[rank - 1]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn env_usize(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}
