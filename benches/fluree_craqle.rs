//! Mirrors the isolated Fluree property-star workload through local Craqle APIs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::time::Instant;

use craqle::{
    AllowAllAuthorizer, CraqleNode, EncodedTerm, GraphId, GraphPolicy, QueryOptions, QueryRequest,
    QueryResults,
};
use oxrdf::{Literal, NamedNode, Term};
use serde_json::{Value, json};

const EX: &str = "http://example.org/compare/";
const GRAPH: &str = "urn:craqle:fluree-compare";

struct Config {
    subjects: usize,
    multiplicity: usize,
    samples: usize,
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
        max_rows: env_usize("FLUREE_MAX_ROWS", 1_000_000),
    }
}

fn expected_count(config: &Config) -> usize {
    let rows = config
        .multiplicity
        .checked_pow(4)
        .and_then(|product| config.subjects.checked_mul(product))
        .expect("star result cardinality overflow");
    assert!(
        rows <= config.max_rows,
        "star result exceeds FLUREE_MAX_ROWS"
    );
    rows
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
    sort_rows(&mut rows);
    rows
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&NamedNode::new_unchecked(value))
}

fn literal(value: &str) -> EncodedTerm {
    EncodedTerm::from_literal(&Literal::new_simple_literal(value))
}

fn fixture(config: &Config) -> Vec<(EncodedTerm, EncodedTerm, EncodedTerm)> {
    let rdf_type = iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let doc_type = iri(&format!("{EX}Doc"));
    let text_predicate = iri(&format!("{EX}text"));
    let mut triples = Vec::with_capacity(config.subjects * (2 + config.multiplicity * 4));
    for subject in 0..config.subjects {
        let subject_term = iri(&format!("{EX}doc{subject:08}"));
        triples.push((subject_term.clone(), rdf_type.clone(), doc_type.clone()));
        for property in 0..4 {
            let predicate = iri(&format!("{EX}p{property}"));
            for value in 0..config.multiplicity {
                triples.push((
                    subject_term.clone(),
                    predicate.clone(),
                    literal(&format!("p{property}-v{value}")),
                ));
            }
        }
        let mut text = format!("common subject{subject}");
        if subject % 7 == 0 {
            text.push_str(" rare");
        }
        if subject % 2 == 0 {
            text.push_str(" even");
        }
        triples.push((subject_term, text_predicate.clone(), literal(&text)));
    }
    triples
}

fn query_text() -> String {
    format!(
        "PREFIX ex: <{EX}> SELECT ?s ?v0 ?v1 ?v2 ?v3 WHERE {{ \
         ?s ex:p0 ?v0 ; ex:p1 ?v1 ; ex:p2 ?v2 ; ex:p3 ?v3 . }}"
    )
}

fn json_value(term: &EncodedTerm) -> Value {
    match term.to_term().expect("query binding must decode") {
        Term::NamedNode(node) => node.as_str().strip_prefix(EX).map_or_else(
            || Value::String(node.as_str().to_string()),
            |suffix| Value::String(format!("ex:{suffix}")),
        ),
        Term::Literal(literal) => Value::String(literal.value().to_string()),
        Term::BlankNode(node) => Value::String(format!("_:{}", node.as_str())),
        Term::Triple(_) => panic!("RDF-star binding is outside this fixture"),
        #[allow(unreachable_patterns)]
        _ => panic!("unsupported query binding"),
    }
}

fn json_rows(results: QueryResults) -> Vec<Value> {
    let QueryResults::Solutions(rows) = results else {
        panic!("property-star query must return solutions");
    };
    rows.into_iter()
        .map(|row| {
            Value::Array(
                ["s", "v0", "v1", "v2", "v3"]
                    .iter()
                    .map(|name| json_value(row.get(*name).expect("projected binding missing")))
                    .collect(),
            )
        })
        .collect()
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_cached_key(|row| serde_json::to_string(row).expect("serialize result row"));
}

fn nanos(started: Instant) -> u128 {
    started.elapsed().as_nanos()
}

fn query_options(config: &Config) -> QueryOptions {
    let mut options = QueryOptions::default();
    options.limits.max_result_rows = config.max_rows;
    options.limits.max_result_cells = config
        .max_rows
        .checked_mul(5)
        .expect("result cell cap overflow");
    options.limits.max_intermediate_rows = config.max_rows;
    options.limits.deadline = None;
    options
}

fn main() {
    let config = read_config();
    assert!(config.subjects > 0, "FLUREE_SUBJECTS must be positive");
    assert!(
        config.multiplicity > 0,
        "FLUREE_MULTIPLICITY must be positive"
    );
    assert!(config.samples > 0, "FLUREE_SAMPLES must be positive");
    assert!(config.max_rows > 0, "FLUREE_MAX_ROWS must be positive");
    let _ = expected_count(&config);
    let root = tempfile::tempdir().expect("create Craqle fixture directory");
    let started = Instant::now();
    let node = CraqleNode::open(root.path()).expect("open local Craqle node");
    let startup_ns = nanos(started);
    let graph = GraphId::new(GRAPH);
    let started = Instant::now();
    node.set_graph_policy(
        &AllowAllAuthorizer,
        &graph,
        GraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
    )
    .expect("create fixture graph");
    node.insert_quads(&AllowAllAuthorizer, &graph, fixture(&config))
        .expect("insert fixture triples");
    let load_ns = nanos(started);
    let started = Instant::now();
    node.ensure_query_indexes();
    let index_ns = nanos(started);
    let query = query_text();
    let options = query_options(&config);
    let expected = expected_rows(&config);
    let mut warm = json_rows(
        node.query_with_options(
            &AllowAllAuthorizer,
            QueryRequest {
                sparql: &query,
                options: &options,
            },
        )
        .expect("warmup query")
        .results,
    );
    sort_rows(&mut warm);
    assert_eq!(expected, warm, "bare star semantic mismatch");

    let mut execute_ns = Vec::with_capacity(config.samples);
    let mut format_ns = Vec::with_capacity(config.samples);
    let mut outer_ns = Vec::with_capacity(config.samples);
    let mut final_rows = Vec::new();
    for _ in 0..config.samples {
        let outer = Instant::now();
        let started = Instant::now();
        let results = node
            .query_with_options(
                &AllowAllAuthorizer,
                QueryRequest {
                    sparql: &query,
                    options: &options,
                },
            )
            .expect("star query")
            .results;
        execute_ns.push(nanos(started));
        let started = Instant::now();
        let mut rows = json_rows(results);
        format_ns.push(nanos(started));
        outer_ns.push(nanos(outer));
        sort_rows(&mut rows);
        assert_eq!(expected, rows, "canonical star bag mismatch");
        final_rows = rows;
    }
    println!(
        "{}",
        json!({
            "record":"craqle_star",
            "semantics":"SPARQL SELECT bag; four same-subject properties; duplicates preserved",
            "query":query,
            "subjects":config.subjects,
            "multiplicity":config.multiplicity,
            "expected_rows":expected.len(),
            "execute_ns":execute_ns,
            "format_ns":format_ns,
            "outer_ns":outer_ns,
            "canonical_multiset":final_rows,
            "setup":{"storage":"local Fjall disk","startup_ns":startup_ns,"fixture_load_ns":load_ns,
                "query_index_prepare_ns":index_ns,
                "authorization":"AllowAllAuthorizer","network":false,"remote_replication":false,
                "dataset":"dataset-free query over Craqle visible named-graph union"},
            "differences":{"crdt":"local Craqle event and source commit path",
                "fluree_storage":"in-memory ledger with binary reindex","bm25_parity_claimed":false}
        })
    );
}
