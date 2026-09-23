//! Runs isolated local Fluree queries through its public reference APIs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::error::Error;
use std::time::Instant;

use fluree_db_api::{Bm25CreateConfig as IndexConfig, Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_query::bm25::{Analyzer, Bm25Scorer};
use serde_json::{Value, json};

const LEDGER: &str = "craqle/compare:main";
const EX: &str = "http://example.org/compare/";

struct Config {
    subjects: usize,
    multiplicity: usize,
    samples: usize,
    limit: usize,
    query: String,
    max_rows: usize,
}

fn env_usize(name: &str, fallback: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("invalid numeric environment {name}")),
        Err(std::env::VarError::NotPresent) => fallback,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("non-Unicode numeric environment {name}")
        }
    }
}

fn read_config() -> Config {
    Config {
        subjects: env_usize("FLUREE_SUBJECTS", 32),
        multiplicity: env_usize("FLUREE_MULTIPLICITY", 2),
        samples: env_usize("FLUREE_SAMPLES", 5),
        limit: env_usize("FLUREE_BM25_LIMIT", 10),
        query: std::env::var("FLUREE_BM25_QUERY").unwrap_or_else(|_| "common rare".to_string()),
        max_rows: env_usize("FLUREE_MAX_ROWS", 1_000_000),
    }
}

fn expected_count(config: &Config) -> usize {
    let product = config
        .multiplicity
        .checked_pow(4)
        .and_then(|product| config.subjects.checked_mul(product))
        .expect("star result cardinality overflow");
    assert!(product <= config.max_rows, "star result exceeds FLUREE_MAX_ROWS");
    product
}

fn expected_rows(config: &Config) -> Vec<Value> {
    let mut rows = Vec::with_capacity(expected_count(config));
    for subject in 0..config.subjects {
        for v0 in 0..config.multiplicity {
            for v1 in 0..config.multiplicity {
                for v2 in 0..config.multiplicity {
                    for v3 in 0..config.multiplicity {
                        rows.push(json!([
                            format!("ex:doc{subject:08}"),
                            format!("p0-v{v0}"),
                            format!("p1-v{v1}"),
                            format!("p2-v{v2}"),
                            format!("p3-v{v3}"),
                        ]));
                    }
                }
            }
        }
    }
    rows.sort_by_cached_key(|row| serde_json::to_string(row).expect("serialize expected row"));
    rows
}

fn fixture(config: &Config) -> Value {
    let graph: Vec<_> = (0..config.subjects)
        .map(|subject| {
            let values = |property| {
                (0..config.multiplicity)
                    .map(|value| format!("p{property}-v{value}"))
                    .collect::<Vec<_>>()
            };
            let mut text = format!("common subject{subject}");
            if subject % 7 == 0 {
                text.push_str(" rare");
            }
            if subject % 2 == 0 {
                text.push_str(" even");
            }
            json!({
                "@id": format!("ex:doc{subject:08}"),
                "@type": "ex:Doc",
                "ex:p0": values(0),
                "ex:p1": values(1),
                "ex:p2": values(2),
                "ex:p3": values(3),
                "ex:text": text,
            })
        })
        .collect();
    json!({"@context": {"ex": EX}, "@graph": graph})
}

fn sort_rows(value: Value) -> Vec<Value> {
    let mut rows = value
        .as_array()
        .expect("SELECT result must be a row array")
        .to_vec();
    rows.sort_by_cached_key(|row| serde_json::to_string(row).expect("serialize result row"));
    rows
}

fn nanos(started: Instant) -> u128 {
    started.elapsed().as_nanos()
}

fn emit(value: Value) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

async fn run_star(fluree: &Fluree, config: &Config) -> Result<(), Box<dyn Error + Send + Sync>> {
    let snapshot = fluree.graph(LEDGER).load().await?;
    let query = format!(
        "PREFIX ex: <{EX}> SELECT ?s ?v0 ?v1 ?v2 ?v3 WHERE {{ \
         ?s ex:p0 ?v0 ; ex:p1 ?v1 ; ex:p2 ?v2 ; ex:p3 ?v3 . }}"
    );

    let expected = expected_rows(config);
    let warmup = snapshot.query().sparql(&query).execute().await?;
    let warmup = warmup
        .to_jsonld_async(snapshot.db().as_graph_db_ref())
        .await?;
    assert_eq!(sort_rows(warmup), expected, "bare star semantic mismatch");

    let mut execute_ns = Vec::with_capacity(config.samples);
    let mut format_ns = Vec::with_capacity(config.samples);
    let mut outer_ns = Vec::with_capacity(config.samples);
    let mut final_rows = Vec::new();
    for _ in 0..config.samples {
        let outer = Instant::now();
        let started = Instant::now();
        let result = snapshot.query().sparql(&query).execute().await?;
        execute_ns.push(nanos(started));
        let started = Instant::now();
        let formatted = result
            .to_jsonld_async(snapshot.db().as_graph_db_ref())
            .await?;
        format_ns.push(nanos(started));
        outer_ns.push(nanos(outer));
        final_rows = sort_rows(formatted);
        assert_eq!(final_rows, expected, "formatted star bag mismatch");
    }

    emit(json!({
        "record": "fluree_star",
        "semantics": "SPARQL SELECT bag; four same-subject properties; duplicates preserved",
        "query": query,
        "subjects": config.subjects,
        "multiplicity": config.multiplicity,
        "expected_rows": expected.len(),
        "execute_ns": execute_ns,
        "format_ns": format_ns,
        "outer_ns": outer_ns,
        "canonical_multiset": final_rows,
    }))?;
    Ok(())
}

async fn run_bm25(fluree: &Fluree, config: &Config) -> Result<(), Box<dyn Error + Send + Sync>> {
    let index_query = json!({
        "@context": {"ex": EX},
        "where": [{"@id": "?x", "@type": "ex:Doc", "ex:text": "?text"}],
        "select": {"?x": ["@id", "ex:text"]},
    });
    let started = Instant::now();
    let created = fluree
        .create_full_text_index(IndexConfig::new("craqle-compare", LEDGER, index_query))
        .await?;
    let index_ns = nanos(started);
    let started = Instant::now();
    let index = fluree.load_bm25_index(&created.graph_source_id).await?;
    let load_ns = nanos(started);
    let index_id = created.index_id.as_ref().map(ToString::to_string);

    let started = Instant::now();
    let analyzer = Analyzer::english_default();
    let terms = analyzer.analyze_to_strings(&config.query);
    let analyze_ns = nanos(started);
    let term_refs: Vec<_> = terms.iter().map(String::as_str).collect();
    let _ = Bm25Scorer::new(&index, &term_refs).score_all();

    let mut full_ns = Vec::with_capacity(config.samples);
    let mut top_ns = Vec::with_capacity(config.samples);
    let mut complete = Vec::new();
    let mut top = Vec::new();
    for _ in 0..config.samples {
        let started = Instant::now();
        complete = Bm25Scorer::new(&index, &term_refs).score_all();
        full_ns.push(nanos(started));

        let started = Instant::now();
        top = Bm25Scorer::new(&index, &term_refs).top_k(config.limit);
        top_ns.push(nanos(started));
    }
    for (ranked, complete) in top.iter().zip(&complete) {
        assert_eq!(
            ranked.0, complete.0,
            "top-k identity differs from score_all"
        );
        assert_eq!(
            ranked.1.to_bits(),
            complete.1.to_bits(),
            "top-k score differs from score_all"
        );
    }

    let candidates: Vec<_> = complete
        .iter()
        .map(|(key, score)| {
            json!({
                "ledger": key.ledger_alias.as_ref(),
                "subject": key.subject_iri.as_ref(),
                "score": score,
                "score_bits": format!("{:016x}", score.to_bits()),
            })
        })
        .collect();
    let top_candidates: Vec<_> = top
        .iter()
        .map(|(key, score)| {
            json!({
                "ledger": key.ledger_alias.as_ref(),
                "subject": key.subject_iri.as_ref(),
                "score": score,
                "score_bits": format!("{:016x}", score.to_bits()),
            })
        })
        .collect();
    emit(json!({
        "record": "fluree_bm25",
        "semantics": {
            "analyzer": "Fluree English tokenizer, lowercase, stopwords, Snowball stemming",
            "score": "Fluree f64 BM25 with k1=1.2 and b=0.75",
            "authorization": "none; no pre-top-k candidate filter",
            "parity": "not claimed against Craqle Tantivy analysis or f32 field norms",
        },
        "documents": created.doc_count,
        "terms_indexed": created.term_count,
        "source_t": created.index_t,
        "index_id": index_id,
        "graph_source": created.graph_source_id,
        "query": config.query,
        "terms": terms,
        "limit": config.limit,
        "index_ns": index_ns,
        "load_ns": load_ns,
        "analyze_ns": analyze_ns,
        "score_all_ns": full_ns,
        "top_k_ns": top_ns,
        "complete_candidates": candidates,
        "top_candidates": top_candidates,
    }))?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = read_config();
    assert!(config.subjects > 0, "FLUREE_SUBJECTS must be positive");
    assert!(
        config.multiplicity > 0,
        "FLUREE_MULTIPLICITY must be positive"
    );
    assert!(config.samples > 0, "FLUREE_SAMPLES must be positive");
    assert!(config.limit > 0, "FLUREE_BM25_LIMIT must be positive");
    assert!(config.max_rows > 0, "FLUREE_MAX_ROWS must be positive");
    let _ = expected_count(&config);

    let started = Instant::now();
    let fluree = FlureeBuilder::memory().build_memory();
    let startup_ns = nanos(started);
    let started = Instant::now();
    let ledger = fluree.create_ledger(LEDGER).await?;
    let ledger = fluree.insert(ledger, &fixture(&config)).await?.ledger;
    let load_ns = nanos(started);
    drop(ledger);
    let started = Instant::now();
    fluree.reindex(LEDGER, ReindexOptions::default()).await?;
    let reindex_ns = nanos(started);

    emit(json!({
        "record": "fluree_setup",
        "reference": "82dbcec3e435d6ed1d45bc0ed929432323b6b201",
        "fluree_version": env!("CARGO_PKG_VERSION"),
        "storage": "in-memory",
        "network_in_timed_spans": false,
        "startup_ns": startup_ns,
        "fixture_load_ns": load_ns,
        "reindex_ns": reindex_ns,
        "subjects": config.subjects,
        "multiplicity": config.multiplicity,
    }))?;
    run_star(&fluree, &config).await?;
    run_bm25(&fluree, &config).await?;
    Ok(())
}
