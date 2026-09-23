//! Attributes wide-read cost layer by layer on a kept RO-Crate store.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use oxrdf::NamedNode;
use serde_json::json;

use crate::core::{EncodedTerm, GraphId};
use crate::query::context::{QueryCancellation, ReadContext};
use crate::query::cursor::RawIndexPattern;
use crate::rdf_read::{DenseScan, GraphSelector, QuadPattern, StoreReadView};
use crate::search::SearchIndex;
use crate::sparql::{QueryOptions, QueryRun, SparqlEngine};
use crate::sparql_fast_path::QueryFastPathMode as FastPathMode;
use crate::store::GraphStore;

const DEFAULT_PREDICATE: &str = "http://schema.org/license";

/// Median wall time of repeated complete runs, in microseconds.
fn median(samples: usize, mut run: impl FnMut() -> usize) -> (f64, usize) {
    let rows = run();
    let mut times: Vec<f64> = (0..samples)
        .map(|_| {
            let started = Instant::now();
            assert_eq!(std::hint::black_box(run()), rows);
            started.elapsed().as_secs_f64() * 1e6
        })
        .collect();
    times.sort_by(f64::total_cmp);
    (times[times.len() / 2], rows)
}

#[test]
#[ignore = "release-only read ladder over a kept store"]
fn read_ladder() {
    let root = PathBuf::from(std::env::var("CRAQLE_LADDER_STORE").expect("kept store"));
    let samples: usize = std::env::var("CRAQLE_LADDER_SAMPLES").map_or(30, |value| {
        value.parse().expect("CRAQLE_LADDER_SAMPLES is a count")
    });
    let predicate =
        std::env::var("CRAQLE_LADDER_PREDICATE").unwrap_or_else(|_| DEFAULT_PREDICATE.to_owned());
    let store = Arc::new(GraphStore::open(root.join("store")).unwrap());
    if std::env::var("CRAQLE_LADDER_FLUSH").is_ok() {
        store.flush_memtables().unwrap();
    }
    let term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(&predicate));
    let view = StoreReadView::new(&store);
    let context = ReadContext::new(QueryCancellation::new());
    let source = crate::rdf_read::RdfReadView::lookup_term(&view, &context, &term)
        .unwrap()
        .expect("predicate is stored");
    let dense = view
        .query_term_id(&context, source)
        .unwrap()
        .expect("dense id");
    let pattern = QuadPattern {
        predicate: Some(source),
        ..QuadPattern::default()
    };
    let mut rungs = Vec::new();

    rungs.push((
        "raw keys",
        median(samples, || {
            let view = StoreReadView::new(&store);
            let context = ReadContext::new(QueryCancellation::new());
            let mut cursor = view
                .raw_index_keys(&context, GraphSelector::Union, pattern)
                .unwrap()
                .unwrap();
            let mut keys = 0;
            while let Some(key) = cursor.next_key() {
                key.unwrap();
                keys += 1;
            }
            keys
        }),
    ));
    rungs.push((
        "decoded keys",
        median(samples, || {
            let view = StoreReadView::new(&store);
            let context = ReadContext::new(QueryCancellation::new());
            let mut cursor = view
                .raw_index_keys(&context, GraphSelector::Union, pattern)
                .unwrap()
                .unwrap();
            let mut keys = 0;
            let mut sum = 0_u64;
            while let Some(key) = cursor.next_key() {
                let key = key.unwrap();
                sum = sum.wrapping_add(key.graph().0 ^ key.subject().0 ^ key.object().0);
                keys += 1;
            }
            std::hint::black_box(sum);
            keys
        }),
    ));
    let graphs: Vec<_> = view
        .snapshot()
        .graph_term_iter(&store)
        .map(Result::unwrap)
        .collect();
    rungs.push((
        "graph metadata point reads",
        median(samples, || {
            let view = StoreReadView::new(&store);
            graphs
                .iter()
                .filter(|graph| view.contains_graph_id(**graph).unwrap())
                .count()
        }),
    ));
    rungs.push((
        "graph orphan state",
        median(samples, || {
            let view = StoreReadView::new(&store);
            let context = ReadContext::new(QueryCancellation::new());
            for graph in &graphs {
                view.orphaned_ids(&context, *graph).unwrap();
            }
            graphs.len()
        }),
    ));
    let dense_scan = || DenseScan {
        selector: GraphSelector::Union,
        shape: pattern,
        pattern: RawIndexPattern::from_terms([None, None, Some(dense), None]),
        source_hints: [None; 4],
        scope: 0,
        resolver: None,
        cache_entries: 4_096,
        cache_bytes: 1 << 20,
        graphs: None,
    };
    let visible = |context: &ReadContext<'_>, view: &StoreReadView<'_>| {
        let mut rows = 0;
        for quad in view.dense_keys(context, dense_scan()).unwrap().unwrap() {
            quad.unwrap();
            rows += 1;
        }
        rows
    };
    rungs.push((
        "visible tuples, first graph visit",
        median(samples, || {
            let view = StoreReadView::new(&store);
            let context = ReadContext::new(QueryCancellation::new());
            visible(&context, &view)
        }),
    ));
    rungs.push((
        "visible tuples, policy reads, first graph visit",
        median(samples, || {
            let view = StoreReadView::new(&store);
            let policy = |graph: &GraphId| {
                view.snapshot()
                    .graph_policy(&store, graph)
                    .unwrap()
                    .is_some()
            };
            let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &policy);
            visible(&context, &view)
        }),
    ));
    let warm_view = StoreReadView::new(&store);
    let warm_context = ReadContext::new(QueryCancellation::new());
    rungs.push((
        "visible tuples, warm graph state",
        median(samples, || visible(&warm_context, &warm_view)),
    ));

    // Probe calibration with warm graph state: one index range per fact.
    let facts: Vec<_> = view
        .dense_keys(&context, dense_scan())
        .unwrap()
        .unwrap()
        .map(|quad| quad.unwrap())
        .collect();
    let probe_view = StoreReadView::new(&store);
    let probe_context = ReadContext::new(QueryCancellation::new());
    let probe = |prefix: usize| {
        let (view, context) = (&probe_view, &probe_context);
        let mut resolver = None;
        let mut found = 0;
        for fact in &facts {
            let terms = [
                Some(fact.graph.query()),
                Some(fact.subject.query()),
                Some(fact.predicate.query()),
                Some(fact.object.query()),
            ];
            let mut bounded = [None; 4];
            bounded[..prefix].copy_from_slice(&terms[..prefix]);
            let scan = DenseScan {
                selector: GraphSelector::Named(fact.graph_source),
                shape: QuadPattern {
                    graph: Some(fact.graph_source),
                    subject: bounded[1].map(|_| crate::store::TermId(0)),
                    predicate: bounded[2].map(|_| crate::store::TermId(0)),
                    object: bounded[3].map(|_| crate::store::TermId(0)),
                },
                pattern: RawIndexPattern::from_terms(bounded),
                source_hints: [
                    Some((fact.graph.query(), fact.graph_source)),
                    None,
                    None,
                    None,
                ],
                resolver: resolver.clone(),
                ..dense_scan()
            };
            let mut cursor = view.dense_keys(context, scan).unwrap().unwrap();
            resolver.get_or_insert_with(|| cursor.resolver());
            found += usize::from(cursor.next().is_some());
        }
        found
    };
    probe(4);
    rungs.push((
        "point probes, warm graph state",
        median(samples, || probe(4)),
    ));
    rungs.push((
        "prefix probes, warm graph state",
        median(samples, || probe(3)),
    ));

    let engine = SparqlEngine::new(
        store.clone(),
        Arc::new(SearchIndex::open_in_memory().unwrap()),
    );
    let run = |sparql: &str, fast_paths: FastPathMode| {
        let mut options = QueryOptions::results_only();
        options.fast_paths = fast_paths;
        let (_, execution) = engine
            .query_with_options(
                QueryRun {
                    sparql,
                    options: &options,
                },
                &|snapshot, graph| snapshot.graph_policy(&store, graph).unwrap().is_some(),
            )
            .unwrap();
        match execution.results {
            crate::sparql::QueryResults::Solutions(rows) => rows.len(),
            _ => 1,
        }
    };
    let adapter = format!("SELECT ?s ?o ?g WHERE {{ GRAPH ?g {{ ?s <{predicate}> ?o }} }}");
    rungs.push((
        "generic adapter tuples",
        median(samples, || run(&adapter, FastPathMode::Disabled)),
    ));
    let facet = format!(
        "SELECT ?value (COUNT(DISTINCT ?g) AS ?crates) WHERE {{ GRAPH ?g {{ \
         ?d <http://schema.org/about> ?g . ?g <{predicate}> ?value }} }} \
         GROUP BY ?value ORDER BY ?value"
    );
    rungs.push((
        "facet query, generic",
        median(samples, || run(&facet, FastPathMode::Disabled)),
    ));
    rungs.push((
        "facet query, native",
        median(samples, || run(&facet, FastPathMode::Auto)),
    ));
    for (rung, (micros, rows)) in rungs {
        println!(
            "{}",
            json!({"record": "ladder", "predicate": predicate, "rung": rung, "median_us": micros, "rows": rows})
        );
    }
}

#[test]
#[ignore = "release-only native plan trace over a kept store"]
fn native_plan() {
    let root = PathBuf::from(std::env::var("CRAQLE_LADDER_STORE").expect("kept store"));
    let sparql = std::env::var("CRAQLE_LADDER_QUERY").expect("query text");
    let samples: usize = std::env::var("CRAQLE_LADDER_SAMPLES").map_or(30, |value| {
        value.parse().expect("CRAQLE_LADDER_SAMPLES is a count")
    });
    let store = GraphStore::open(root.join("store")).unwrap();
    let query = spargebra::SparqlParser::new().parse_query(&sparql).unwrap();
    let plan = crate::graph_distinct::analyze(&query).expect("native shape");
    let mut stats = None;
    let (micros, rows) = median(samples, || {
        let view = StoreReadView::new(&store);
        let policy = |graph: &GraphId| {
            view.snapshot()
                .graph_policy(&store, graph)
                .unwrap()
                .is_some()
        };
        let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &policy);
        let budget = crate::sparql::QueryBudget::new(
            crate::query::budget::BudgetShape::default(),
            crate::sparql::QueryLimits::default(),
            crate::query::deadline::RequestClock::start(
                None,
                QueryCancellation::new(),
                Instant::now(),
            ),
        )
        .unwrap();
        let relation = crate::graph_distinct::execute(
            &plan,
            crate::graph_join::JoinInput {
                view: &view,
                context: &context,
                budget: &budget,
            },
        )
        .unwrap()
        .expect("native relation");
        let rows = match &relation.values {
            spargebra::algebra::GraphPattern::Values { bindings, .. } => bindings.len(),
            _ => 0,
        };
        stats = Some(relation.stats);
        rows
    });
    println!(
        "{}",
        json!({"record": "native", "median_us": micros, "rows": rows, "stats": format!("{:?}", stats.unwrap())})
    );
}
