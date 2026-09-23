//! Small, deterministic semantic baseline for the public SPARQL API.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT
//! Compare optimizer-on/off solutions as multisets, independent of execution timing.
#[path = "../support.rs"]
mod support;

use crate::support::TestWriteExt as _;

use oxrdf::Term;

use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CraqleNode, CraqleOptions, EncodedTerm,
    GraphId, GraphPolicy, MaterializedQuadChange, QueryLimits, QueryOptions, QueryResults,
    SearchStorage,
};
use support::query_with_visibility;

const PRIMARY_GRAPH: &str = "urn:baseline:primary";
const DUPLICATE_GRAPH: &str = "urn:baseline:duplicate";
const HIDDEN_GRAPH: &str = "urn:baseline:hidden";
const ORPHAN_GRAPH: &str = "urn:baseline:orphan";

const COMMON: &str = "urn:baseline:common";
const KNOWN: &str = "urn:baseline:known";
const KNOWN_OBJECT: &str = "urn:baseline:known-object";
const RARE: &str = "urn:baseline:rare";
const NAME: &str = "http://schema.org/name";
const SHARED: &str = "urn:baseline:shared";
const HIDDEN: &str = "urn:baseline:hidden-property";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const DATASET: &str = "http://schema.org/Dataset";
const MEDIA_OBJECT: &str = "http://schema.org/MediaObject";
const NEEDLE_SUBJECT: &str = "urn:baseline:needle";
const DUPLICATE_SUBJECT: &str = "urn:baseline:duplicate-subject";
const ORPHAN_SUBJECT: &str = "urn:baseline:stray";

struct Fixture {
    _directory: tempfile::TempDir,
    node: CraqleNode,
    hidden: GraphId,
    orphan: GraphId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CanonicalResults {
    Boolean(bool),
    Solutions(Vec<Vec<(String, EncodedTerm)>>),
    Graph(Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>),
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn literal(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
}

fn common_subject(index: usize) -> String {
    format!("urn:baseline:common:{index:02}")
}

fn common_value(index: usize) -> String {
    format!("common-{index:02}")
}

fn insert(
    graph: &GraphId,
    subject: EncodedTerm,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject,
        predicate: iri(predicate),
        object,
    }
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        directory.path(),
        CraqleOptions::new().with_search_storage(SearchStorage::Memory),
    )
    .unwrap();
    let primary = GraphId::new(PRIMARY_GRAPH);
    let duplicate = GraphId::new(DUPLICATE_GRAPH);
    let hidden = GraphId::new(HIDDEN_GRAPH);
    let orphan = GraphId::new(ORPHAN_GRAPH);

    let mut primary_changes = Vec::new();
    for index in 0..10 {
        let subject = iri(&common_subject(index));
        primary_changes.push(insert(
            &primary,
            subject.clone(),
            COMMON,
            literal(&common_value(index)),
        ));
        primary_changes.push(insert(&primary, subject, KNOWN, iri(KNOWN_OBJECT)));
    }
    primary_changes.extend([
        insert(
            &primary,
            iri(NEEDLE_SUBJECT),
            COMMON,
            literal("common-needle"),
        ),
        insert(&primary, iri(NEEDLE_SUBJECT), KNOWN, iri(KNOWN_OBJECT)),
        insert(&primary, iri(NEEDLE_SUBJECT), RARE, literal("needle")),
        insert(&primary, iri(NEEDLE_SUBJECT), NAME, literal("Needle")),
        insert(&primary, iri(DUPLICATE_SUBJECT), SHARED, literal("same")),
    ]);
    node.apply_changes_unchecked(&primary, primary_changes)
        .unwrap();

    node.apply_changes_unchecked(
        &duplicate,
        vec![insert(
            &duplicate,
            iri(DUPLICATE_SUBJECT),
            SHARED,
            literal("same"),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &hidden,
        vec![insert(
            &hidden,
            iri("urn:baseline:hidden-subject"),
            HIDDEN,
            literal("hidden"),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &orphan,
        vec![
            insert(&orphan, iri(ORPHAN_GRAPH), RDF_TYPE, iri(DATASET)),
            insert(&orphan, iri(ORPHAN_SUBJECT), RDF_TYPE, iri(MEDIA_OBJECT)),
            insert(&orphan, iri(ORPHAN_SUBJECT), NAME, literal("stray")),
        ],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&orphan).unwrap();
    node.ensure_query_indexes();

    Fixture {
        _directory: directory,
        node,
        hidden,
        orphan,
    }
}

fn canonicalize(results: QueryResults, ordered: bool) -> CanonicalResults {
    match results {
        QueryResults::Boolean(value) => CanonicalResults::Boolean(value),
        QueryResults::Solutions(rows) => {
            let mut rows: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    let mut row: Vec<_> = row.into_iter().collect();
                    row.sort();
                    row
                })
                .collect();
            if !ordered {
                rows.sort();
            }
            CanonicalResults::Solutions(rows)
        }
        QueryResults::Graph(mut triples) => {
            triples.sort();
            CanonicalResults::Graph(triples)
        }
    }
}

fn planner_result<F>(node: &CraqleNode, visible: F, sparql: &str) -> CanonicalResults
where
    F: Fn(&GraphId) -> bool + Sync,
{
    let prepared = node.prepare_query(sparql).unwrap();
    let auth = |graph: &GraphId, _: &GraphPolicy, action: Action| {
        if visible(graph) {
            Ok(())
        } else {
            Err(AuthorizationError::PermissionDenied {
                action,
                graph: graph.as_str().to_owned(),
            })
        }
    };
    let execute = |optimize| {
        let mut options = QueryOptions::default();
        options.optimize = optimize;
        if !optimize {
            options.fast_paths = craqle::QueryFastPathMode::Disabled;
        }
        options.limits = QueryLimits::unbounded();
        canonicalize(
            node.execute_prepared(&auth, &prepared, &options)
                .unwrap()
                .results,
            false,
        )
    };
    let optimized = execute(true);
    let unoptimized = execute(false);
    assert_eq!(
        optimized, unoptimized,
        "optimizer changed SPARQL semantics\nquery: {sparql}"
    );
    optimized
}

fn solution_rows(results: CanonicalResults) -> Vec<Vec<(String, EncodedTerm)>> {
    match results {
        CanonicalResults::Solutions(rows) => rows,
        other => panic!("expected solution rows, got {other:?}"),
    }
}

fn binding<'a>(row: &'a [(String, EncodedTerm)], variable: &str) -> &'a EncodedTerm {
    row.iter()
        .find_map(|(name, value)| (name == variable).then_some(value))
        .unwrap_or_else(|| panic!("missing ?{variable} binding"))
}

fn literal_value(term: &EncodedTerm) -> String {
    match term.to_term() {
        Some(Term::Literal(value)) => value.value().to_owned(),
        other => panic!("expected a literal term, got {other:?}"),
    }
}

fn expected_rows(mut rows: Vec<Vec<(String, EncodedTerm)>>) -> Vec<Vec<(String, EncodedTerm)>> {
    for row in &mut rows {
        row.sort();
    }
    rows.sort();
    rows
}

#[test]
fn baseline_plans_equivalent() {
    let fixture = fixture();

    let ask_hit = format!("ASK WHERE {{ <{NEEDLE_SUBJECT}> <{KNOWN}> <{KNOWN_OBJECT}> }}");
    assert_eq!(
        planner_result(&fixture.node, |_| true, &ask_hit),
        CanonicalResults::Boolean(true)
    );

    let ask_miss = format!("ASK WHERE {{ <urn:baseline:missing> <{KNOWN}> <{KNOWN_OBJECT}> }}");
    assert_eq!(
        planner_result(&fixture.node, |_| true, &ask_miss),
        CanonicalResults::Boolean(false)
    );

    let limit = format!("SELECT ?s WHERE {{ ?s <{KNOWN}> <{KNOWN_OBJECT}> }} LIMIT 10");
    let limit_rows = solution_rows(planner_result(&fixture.node, |_| true, &limit));
    assert_eq!(limit_rows.len(), 10);
    let allowed_limit_rows = expected_rows(
        (0..10)
            .map(|index| vec![("s".to_string(), iri(&common_subject(index)))])
            .chain(std::iter::once(vec![(
                "s".to_string(),
                iri(NEEDLE_SUBJECT),
            )]))
            .collect(),
    );
    assert!(
        limit_rows
            .iter()
            .all(|row| allowed_limit_rows.contains(row))
    );
    let mut distinct_limit_rows = limit_rows.clone();
    distinct_limit_rows.dedup();
    assert_eq!(distinct_limit_rows.len(), 10);

    let count = format!("SELECT (COUNT(*) AS ?count) WHERE {{ ?s <{COMMON}> ?value }}");
    let count_rows = solution_rows(planner_result(&fixture.node, |_| true, &count));
    assert_eq!(count_rows.len(), 1);
    assert_eq!(literal_value(binding(&count_rows[0], "count")), "11");

    let property_star = format!(
        "SELECT ?s ?name ?rare WHERE {{ ?s <{NAME}> ?name ; <{RARE}> ?rare ; <{COMMON}> ?value }}"
    );
    assert_eq!(
        solution_rows(planner_result(&fixture.node, |_| true, &property_star,)),
        expected_rows(vec![vec![
            ("s".to_string(), iri(NEEDLE_SUBJECT)),
            ("name".to_string(), literal("Needle")),
            ("rare".to_string(), literal("needle")),
        ]])
    );

    let rare_to_common =
        format!("SELECT ?s ?value WHERE {{ ?s <{RARE}> \"needle\" . ?s <{COMMON}> ?value }}");
    let common_to_rare =
        format!("SELECT ?s ?value WHERE {{ ?s <{COMMON}> ?value . ?s <{RARE}> \"needle\" }}");
    let expected_join = expected_rows(vec![vec![
        ("s".to_string(), iri(NEEDLE_SUBJECT)),
        ("value".to_string(), literal("common-needle")),
    ]]);
    assert_eq!(
        solution_rows(planner_result(&fixture.node, |_| true, &rare_to_common,)),
        expected_join
    );
    assert_eq!(
        solution_rows(planner_result(&fixture.node, |_| true, &common_to_rare,)),
        expected_join
    );
}

#[test]
fn plans_preserve_multiplicity() {
    let fixture = fixture();
    let default_query = format!("SELECT ?s WHERE {{ ?s <{SHARED}> \"same\" }}");
    let named_query = format!("SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s <{SHARED}> \"same\" }} }}");
    let named_rows = expected_rows(vec![
        vec![
            ("g".to_string(), iri(PRIMARY_GRAPH)),
            ("s".to_string(), iri(DUPLICATE_SUBJECT)),
        ],
        vec![
            ("g".to_string(), iri(DUPLICATE_GRAPH)),
            ("s".to_string(), iri(DUPLICATE_SUBJECT)),
        ],
    ]);

    let _ = planner_result(&fixture.node, |_| true, &default_query);
    assert_eq!(
        solution_rows(planner_result(&fixture.node, |_| true, &named_query,)),
        named_rows,
        "non-DISTINCT SELECT must preserve one row from each named graph"
    );
}

#[test]
fn union_deduplicates_triples() {
    let fixture = fixture();
    let query = format!("SELECT ?s WHERE {{ ?s <{SHARED}> \"same\" }}");
    let expected = expected_rows(vec![vec![("s".to_string(), iri(DUPLICATE_SUBJECT))]]);

    assert_eq!(
        solution_rows(canonicalize(
            query_with_visibility(&fixture.node, |_| true, &query).unwrap(),
            false,
        )),
        expected,
        "the union default graph must contain one copy of an identical triple"
    );
}

#[test]
fn queries_hide_orphans() {
    let fixture = fixture();
    let hidden_query = format!("SELECT ?s WHERE {{ ?s <{HIDDEN}> \"hidden\" }}");
    assert_eq!(
        solution_rows(planner_result(&fixture.node, |_| true, &hidden_query,)),
        expected_rows(vec![vec![(
            "s".to_string(),
            iri("urn:baseline:hidden-subject"),
        )]])
    );
    let hidden = fixture.hidden.clone();
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            move |graph| graph != &hidden,
            &hidden_query,
        )),
        Vec::<Vec<(String, EncodedTerm)>>::new()
    );

    assert_eq!(
        fixture
            .node
            .graph_diagnostics(&fixture.orphan)
            .unwrap()
            .orphaned_entities,
        vec![ORPHAN_SUBJECT.to_string()],
        "the public checked-write fixture must construct a recorded orphan"
    );
    let orphan_query = format!("SELECT ?s WHERE {{ ?s <{NAME}> \"stray\" }}");
    assert_eq!(
        solution_rows(canonicalize(
            fixture
                .node
                .query_in_graphs(
                    &AllowAllAuthorizer,
                    std::slice::from_ref(&fixture.orphan),
                    &orphan_query,
                )
                .unwrap(),
            false,
        )),
        Vec::<Vec<(String, EncodedTerm)>>::new(),
        "an explicit graph list must not expose a recorded orphan"
    );
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            |graph| graph == &fixture.orphan,
            &orphan_query,
        )),
        Vec::<Vec<(String, EncodedTerm)>>::new()
    );
}

fn external_result(store: &oxigraph::store::Store, query: &str) -> CanonicalResults {
    use oxigraph::sparql::{QueryResults as ExternalResults, SparqlEvaluator};
    let result = SparqlEvaluator::new()
        .parse_query(query)
        .unwrap()
        .on_store(store)
        .execute()
        .unwrap();
    match result {
        ExternalResults::Boolean(value) => CanonicalResults::Boolean(value),
        ExternalResults::Solutions(solutions) => {
            let mut rows: Vec<Vec<(String, EncodedTerm)>> = solutions
                .map(|row| {
                    let row = row.unwrap();
                    let mut cells: Vec<_> = row
                        .iter()
                        .map(|(name, term)| {
                            (name.as_str().to_owned(), EncodedTerm(term.to_string()))
                        })
                        .collect();
                    cells.sort();
                    cells
                })
                .collect();
            if !query.contains("ORDER BY") {
                rows.sort();
            }
            CanonicalResults::Solutions(rows)
        }
        ExternalResults::Graph(triples) => {
            let mut triples: Vec<_> = triples
                .map(|triple| {
                    let triple = triple.unwrap();
                    (
                        EncodedTerm(triple.subject.to_string()),
                        EncodedTerm(triple.predicate.to_string()),
                        EncodedTerm(triple.object.to_string()),
                    )
                })
                .collect();
            triples.sort();
            CanonicalResults::Graph(triples)
        }
    }
}

#[test]
fn independent_queries_match() {
    use oxigraph::model::{GraphName, Literal, NamedNode, Quad, Term};
    let named = |value: String| NamedNode::new(value).unwrap();
    for seed in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open_with_options(
            directory.path(),
            CraqleOptions::new().with_search_storage(SearchStorage::Memory),
        )
        .unwrap();
        let reference = oxigraph::store::Store::new().unwrap();
        for graph_index in 0..3 {
            let graph = GraphId::new(&format!("urn:oracle:g{graph_index}"));
            let mut changes = Vec::new();
            for index in 0..12 {
                let subject = named(format!("urn:oracle:s{}", index % 8));
                let mut triples = vec![
                    (
                        "urn:oracle:p",
                        Term::Literal(Literal::from((index + seed + graph_index) % 5)),
                    ),
                    (
                        "urn:oracle:r",
                        Term::NamedNode(named(format!("urn:oracle:s{}", (index + 1) % 8))),
                    ),
                ];
                if (index + seed + graph_index) % 3 == 0 {
                    triples.push((
                        "urn:oracle:q",
                        Term::Literal(Literal::new_simple_literal("shared")),
                    ));
                }
                for (predicate, object) in triples {
                    let predicate = named(predicate.to_owned());
                    changes.push(MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: EncodedTerm(subject.to_string()),
                        predicate: EncodedTerm(predicate.to_string()),
                        object: EncodedTerm(object.to_string()),
                    });
                    // Materialize a set-valued default graph independently of the query adapter.
                    for scope in [
                        GraphName::DefaultGraph,
                        GraphName::NamedNode(named(graph.as_str().to_owned())),
                    ] {
                        reference
                            .insert(&Quad::new(
                                subject.clone(),
                                predicate.clone(),
                                object.clone(),
                                scope,
                            ))
                            .unwrap();
                    }
                }
            }
            node.apply_changes_unchecked(&graph, changes).unwrap();
        }
        for query in [
            "SELECT ?s ?v WHERE { ?s <urn:oracle:p> ?v }",
            "SELECT ?s WHERE { ?s <urn:oracle:p> ?v }",
            "SELECT DISTINCT ?s WHERE { ?s <urn:oracle:p> ?v }",
            "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:oracle:p> ?v }",
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s <urn:oracle:p> ?v } }",
            "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s <urn:oracle:p> 2 ; <urn:oracle:q> ?q } }",
            "SELECT ?v (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s <urn:oracle:p> ?v } } GROUP BY ?v ORDER BY ?v",
            "SELECT ?s ?v ?q WHERE { ?s <urn:oracle:p> ?v OPTIONAL { ?s <urn:oracle:q> ?q } }",
            "SELECT ?s ?v WHERE { ?s <urn:oracle:p> ?v FILTER(?v >= 2) }",
            "SELECT ?s WHERE { ?s <urn:oracle:p> ?v FILTER EXISTS { ?s <urn:oracle:q> ?q } }",
            "SELECT ?s WHERE { ?s <urn:oracle:p> ?v FILTER NOT EXISTS { ?s <urn:oracle:q> ?q } }",
            "SELECT ?s WHERE { { ?s <urn:oracle:p> 1 } UNION { ?s <urn:oracle:p> 2 } }",
            "SELECT ?s WHERE { ?s <urn:oracle:p> ?v MINUS { ?s <urn:oracle:q> ?q } }",
            "SELECT ?s ?v WHERE { VALUES ?v { 1 2 } ?s <urn:oracle:p> ?v }",
            "SELECT ?s ?next WHERE { ?s <urn:oracle:p> ?v BIND((?v + 1) AS ?next) }",
            "SELECT ?s ?o WHERE { ?s <urn:oracle:r>+ ?o } ORDER BY ?s ?o LIMIT 20",
            "SELECT ?s ?v WHERE { ?s <urn:oracle:p> ?v } ORDER BY ?s ?v LIMIT 5 OFFSET 2",
            "ASK { ?s <urn:oracle:p> 2 }",
            "ASK { ?s <urn:oracle:missing> ?o }",
        ] {
            let expected = external_result(&reference, query);
            let prepared = node.prepare_query(query).unwrap();
            for fast in [
                craqle::QueryFastPathMode::Auto,
                craqle::QueryFastPathMode::Disabled,
            ] {
                let mut options = QueryOptions::results_only();
                options.fast_paths = fast;
                let actual = node
                    .execute_prepared(&AllowAllAuthorizer, &prepared, &options)
                    .unwrap();
                assert_eq!(
                    canonicalize(actual.results, query.contains("ORDER BY")),
                    expected,
                    "seed {seed}, {fast:?}: {query}"
                );
            }
        }
    }
}
