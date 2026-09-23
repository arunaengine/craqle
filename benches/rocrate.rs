//! Runs the RO-Crate query catalog in-process against Craqle for the Virtuoso comparison.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, Authorizer, CraqleNode, GraphId, GraphPolicy,
    QueryFastPathMode as FastPathMode, QueryOptions, QueryResults, SearchFlushOptions,
    SearchRequest,
};
use serde_json::{Value, json};

const DESCRIPTOR: &str = "ro-crate-metadata.json";

fn main() {
    let fixture = PathBuf::from(env::var("CRAQLE_ROCRATE_FIXTURE").expect("fixture directory"));
    let samples: usize = env::var("CRAQLE_ROCRATE_SAMPLES").map_or(20, |value| {
        value.parse().expect("CRAQLE_ROCRATE_SAMPLES is a count")
    });
    let directory = tempfile::tempdir().expect("store directory");
    // A kept store is loaded once and then only read, so repeated profiles skip ingestion.
    let kept = env::var("CRAQLE_ROCRATE_STORE").ok().map(PathBuf::from);
    let path = kept.clone().unwrap_or_else(|| directory.path().to_owned());
    let loaded = path.join("rocrate-loaded");
    let reuse = kept.is_some() && loaded.exists();
    let node = CraqleNode::open(&path).expect("open node");
    let crates: Vec<Value> = read_json(&fixture.join("crates.json"));
    let cases: Vec<Value> = read_json(&fixture.join("cases.json"));
    let selected: Option<Vec<String>> = env::var("CRAQLE_ROCRATE_CASES")
        .ok()
        .map(|ids| ids.split(',').map(str::to_owned).collect());
    if reuse {
        for case in &cases {
            let id = case["id"].as_str().unwrap();
            if selected
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|wanted| wanted == id))
            {
                emit(run_case(&node, case, samples));
            }
        }
        return;
    }

    let started = Instant::now();
    for entry in &crates {
        let document = fs::read_to_string(fixture.join(entry["file"].as_str().unwrap())).unwrap();
        let graph = GraphId::new(entry["graph"].as_str().unwrap());
        node.apply_rocrate_document_with_policy(
            &AllowAllAuthorizer,
            graph,
            &document,
            GraphPolicy::default(),
        )
        .expect("ingest crate");
    }
    let ingest_ns = started.elapsed().as_nanos();
    let started = Instant::now();
    node.flush_search_updates().expect("settle search");
    let search_ns = started.elapsed().as_nanos();
    emit(json!({
        "record": "load",
        "crates": crates.len(),
        "ingest_ns": ingest_ns,
        "text_index_ns": search_ns,
        "durability": "SyncAll acknowledgement per crate",
    }));
    emit(equivalence(&node, &fixture, &crates));
    if kept.is_some() {
        fs::write(&loaded, b"").expect("mark kept store");
    }

    for case in &cases {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_case(&node, case, samples)
        }));
        match result {
            Ok(record) => emit(record),
            Err(_) => emit(json!({"record": "case", "case": case["id"], "status": "failed"})),
        }
    }
    // The update case changes data, so a kept store stays unchanged.
    if kept.is_none() {
        emit(update_case(&node, &crates));
    }
    emit(json!({"record": "footprint", "bytes": directory_bytes(directory.path())}));
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    serde_json::from_slice(&fs::read(path).expect("read fixture file")).expect("fixture JSON")
}

fn emit(record: Value) {
    println!("{record}");
}

/// Compares each graph's live quads with the generator's canonical quads.
fn equivalence(node: &CraqleNode, fixture: &Path, crates: &[Value]) -> Value {
    let mut expected: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for line in fs::read_to_string(fixture.join("dataset.nq"))
        .unwrap()
        .lines()
    {
        let body = line.strip_suffix(" .").expect("N-Quads line");
        let split = body.rfind(" <").expect("named graph");
        let graph = body[split + 2..body.len() - 1].to_owned();
        expected
            .entry(graph)
            .or_default()
            .insert(body[..split].to_owned());
    }
    let mut missing = 0usize;
    let mut extra = 0usize;
    let mut examples = Vec::new();
    for entry in crates {
        let graph = entry["graph"].as_str().unwrap();
        let snapshot = node.graph_snapshot(&GraphId::new(graph)).unwrap();
        // Craqle keeps the relative descriptor id; the shared dataset resolves it.
        let resolved = format!("<{graph}{DESCRIPTOR}>");
        let actual: BTreeSet<String> = snapshot
            .quads
            .iter()
            .filter(|quad| !quad.dots.is_empty())
            .map(|quad| {
                let subject = if quad.subject.0 == format!("<{DESCRIPTOR}>") {
                    resolved.clone()
                } else {
                    quad.subject.0.clone()
                };
                format!("{subject} {} {}", quad.predicate.0, quad.object.0)
            })
            .collect();
        let wanted = expected.remove(graph).unwrap_or_default();
        for quad in wanted.difference(&actual) {
            missing += 1;
            if examples.len() < 5 {
                examples.push(format!("missing {quad}"));
            }
        }
        for quad in actual.difference(&wanted) {
            extra += 1;
            if examples.len() < 5 {
                examples.push(format!("extra {quad}"));
            }
        }
    }
    json!({
        "record": "equivalence",
        "missing_quads": missing,
        "extra_quads": extra,
        "examples": examples,
        "descriptor_mapping": "<ro-crate-metadata.json> read as <graph>ro-crate-metadata.json",
    })
}

/// Grants read access only to the listed graphs.
struct Readable(HashSet<String>);

impl Authorizer for Readable {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: Action,
    ) -> Result<(), AuthorizationError> {
        if action == Action::Read && self.0.contains(graph.as_str()) {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    }
}

/// Validates once outside timing, then times complete results-only calls.
fn run_case(node: &CraqleNode, case: &Value, samples: usize) -> Value {
    let id = case["id"].as_str().unwrap();
    let kind = case["kind"].as_str().unwrap();
    let readable = case["readable"].as_array().map(|graphs| {
        Readable(
            graphs
                .iter()
                .map(|graph| graph.as_str().unwrap().to_owned())
                .collect(),
        )
    });
    let auth: &dyn Authorizer = match &readable {
        Some(readable) => readable,
        None => &AllowAllAuthorizer,
    };
    let (rows, samples_ns) = match kind {
        "text" => {
            let terms = case["terms"].as_str().unwrap();
            let limit = case["limit"]
                .as_u64()
                .map_or(10_000, |limit| limit as usize);
            let search = || {
                node.search(
                    auth,
                    SearchRequest {
                        query: terms,
                        limit,
                    },
                )
                .expect("search")
            };
            let rows = search()
                .into_iter()
                .map(|hit| json!([hit.graph_id, hit.subject_iri]))
                .collect();
            (rows, timed(samples, || search().len()))
        }
        _ => {
            let text = case["craqle_sparql"]
                .as_str()
                .or(case["sparql"].as_str())
                .unwrap();
            let variables: Vec<&str> = case["variables"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| name.as_str().unwrap())
                .collect();
            let prepared = node.prepare_query(text).expect("prepare");
            let graphs: Option<Vec<GraphId>> = case["graphs"].as_array().map(|graphs| {
                graphs
                    .iter()
                    .map(|graph| GraphId::new(graph.as_str().unwrap()))
                    .collect()
            });
            let mut options = QueryOptions::results_only();
            if env::var("CRAQLE_ROCRATE_FAST_PATHS").as_deref() == Ok("off") {
                options.fast_paths = FastPathMode::Disabled;
            }
            let execute = || {
                let run = match &graphs {
                    Some(graphs) => {
                        node.execute_prepared_in_graphs(auth, graphs, &prepared, &options)
                    }
                    None => node.execute_prepared(auth, &prepared, &options),
                };
                run.expect("query").results
            };
            let rows = canonical(&execute(), &variables);
            if env::var("CRAQLE_ROCRATE_COSTS").is_ok() {
                let mut costs = options.clone();
                costs.collect_costs = true;
                let run = match &graphs {
                    Some(graphs) => {
                        node.execute_prepared_in_graphs(auth, graphs, &prepared, &costs)
                    }
                    None => node.execute_prepared(auth, &prepared, &costs),
                };
                emit(json!({
                    "record": "costs",
                    "case": id,
                    "statistics": format!("{:?}", run.expect("query").statistics),
                }));
            }
            (rows, timed(samples, || solution_count(&execute())))
        }
    };
    json!({
        "record": "case",
        "case": id,
        "status": "measured",
        "rows": rows,
        "samples_ns": samples_ns,
    })
}

/// Times complete calls; the closure's row count keeps the result from being optimized away.
fn timed(samples: usize, mut call: impl FnMut() -> usize) -> Vec<u128> {
    std::hint::black_box(call());
    (0..samples)
        .map(|_| {
            let started = Instant::now();
            std::hint::black_box(call());
            started.elapsed().as_nanos()
        })
        .collect()
}

fn solution_count(results: &QueryResults) -> usize {
    match results {
        QueryResults::Solutions(rows) => rows.len(),
        _ => 1,
    }
}

/// Rows as `name=term` cells in projection order, keeping unbound variables and duplicates.
fn canonical(results: &QueryResults, variables: &[&str]) -> Vec<Value> {
    let QueryResults::Solutions(rows) = results else {
        return vec![json!(format!("{results:?}"))];
    };
    rows.iter()
        .map(|row| {
            let cells: Vec<String> = variables
                .iter()
                .map(|name| match row.get(*name) {
                    Some(term) => format!("{name}={}", term.0),
                    None => format!("{name}=UNBOUND"),
                })
                .collect();
            json!(cells.join(" "))
        })
        .collect()
}

/// Renames one crate and times acknowledgement, query visibility, and search visibility.
fn update_case(node: &CraqleNode, crates: &[Value]) -> Value {
    let entry = &crates[crates.len().min(8) - 1];
    let graph = GraphId::new(entry["graph"].as_str().unwrap());
    let fixture = PathBuf::from(env::var("CRAQLE_ROCRATE_FIXTURE").unwrap());
    let mut document: Value = read_json(&fixture.join(entry["file"].as_str().unwrap()));
    let root = document["@graph"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entity| entity["@id"] == "./")
        .unwrap();
    root["name"] = json!("Renamed zephyr crate");
    let started = Instant::now();
    node.apply_rocrate_document(&AllowAllAuthorizer, graph.clone(), &document.to_string())
        .expect("update crate");
    let acknowledged = started.elapsed().as_nanos();
    let query = format!(
        "SELECT ?name WHERE {{ GRAPH <{}> {{ <{0}> <http://schema.org/name> ?name }} }}",
        graph.as_str()
    );
    let rows = canonical(&node.query(&AllowAllAuthorizer, &query).unwrap(), &["name"]);
    let queried = started.elapsed().as_nanos();
    let receipt = node
        .flush_search(&SearchFlushOptions::default())
        .expect("search barrier");
    let flushed = started.elapsed().as_nanos();
    let hits = node
        .search(
            &AllowAllAuthorizer,
            SearchRequest {
                query: "zephyr",
                limit: 10,
            },
        )
        .unwrap();
    json!({
        "record": "update",
        "case": "RC14-rename",
        "graph": graph.as_str(),
        "acknowledged_ns": acknowledged,
        "query_visible_ns": queried,
        "search_visible_ns": flushed,
        "receipt_covered": receipt.covered,
        "rows": rows,
        "hits": hits.iter().map(|hit| json!([hit.graph_id, hit.subject_iri])).collect::<Vec<_>>(),
    })
}

fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| match entry.metadata() {
                    Ok(metadata) if metadata.is_dir() => directory_bytes(&entry.path()),
                    Ok(metadata) => metadata.len(),
                    Err(_) => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}
