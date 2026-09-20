//! Black-box query contracts for graph routing and duplicate handling.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT
//! Public triples lack graph provenance; internal tests cover CONSTRUCT and DESCRIBE.
#[path = "../support.rs"]
mod support;

use crate::support::TestWriteExt as _;
use craqle::{
    AllowAllAuthorizer, Authorizer, CraqleNode, EncodedTerm, GrantAuthorizer, GraphId, GraphPolicy,
    MaterializedQuadChange, QueryLimits, QueryOptions, QueryResults,
};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_DATASET: &str = "http://schema.org/Dataset";
const SCHEMA_MEDIA_OBJECT: &str = "http://schema.org/MediaObject";
const SCHEMA_HAS_PART: &str = "http://schema.org/hasPart";
const TEST_PREDICATE: &str = "urn:pr3:query-semantics:predicate";
const SHARED_SUBJECT: &str = "urn:pr3:query-semantics:shared";
const SHARED_VALUE: &str = "same";
const UNIQUE_SUBJECT: &str = "urn:pr3:query-semantics:unique";
const UNIQUE_VALUE: &str = "other";
const HIDDEN_GRAPH: &str = "urn:pr3:query-semantics:hidden";
const ORPHAN_GRAPH: &str = "urn:pr3:query-semantics:orphan";
const ORPHAN_SUBJECT: &str = "urn:pr3:query-semantics:orphan-subject";

struct Fixture {
    _directory: tempfile::TempDir,
    node: CraqleNode,
    visible: Vec<GraphId>,
    hidden: GraphId,
    orphan: GraphId,
    reader: GrantAuthorizer,
}

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn literal(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
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

fn public_policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/datasets/public/query-semantics/**".to_string()],
    }
}

fn hidden_policy() -> GraphPolicy {
    GraphPolicy {
        public: false,
        permission_paths: vec!["/datasets/private/query-semantics/**".to_string()],
    }
}

fn write_visible_graph(node: &CraqleNode, graph: &GraphId, unique: bool) {
    let root = iri(graph.as_str());
    let shared = iri(SHARED_SUBJECT);
    let mut changes = vec![
        insert(graph, root.clone(), RDF_TYPE, iri(SCHEMA_DATASET)),
        insert(graph, root.clone(), SCHEMA_HAS_PART, shared.clone()),
        insert(graph, shared, RDF_TYPE, iri(SCHEMA_MEDIA_OBJECT)),
        insert(
            graph,
            iri(SHARED_SUBJECT),
            TEST_PREDICATE,
            literal(SHARED_VALUE),
        ),
    ];
    if unique {
        let unique = iri(UNIQUE_SUBJECT);
        changes.extend([
            insert(graph, root.clone(), SCHEMA_HAS_PART, unique.clone()),
            insert(graph, unique.clone(), RDF_TYPE, iri(SCHEMA_MEDIA_OBJECT)),
            insert(graph, unique, TEST_PREDICATE, literal(UNIQUE_VALUE)),
        ]);
    }
    node.apply_changes_unchecked(graph, changes).unwrap();
}

fn fixture(visible_count: usize) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    let visible: Vec<_> = (0..visible_count)
        .map(|index| GraphId::new(&format!("urn:pr3:query-semantics:visible-{index:02}")))
        .collect();
    for (index, graph) in visible.iter().enumerate() {
        node.import_graph_policy(graph, public_policy()).unwrap();
        write_visible_graph(&node, graph, index == 0);
    }

    let hidden = GraphId::new(HIDDEN_GRAPH);
    node.import_graph_policy(&hidden, hidden_policy()).unwrap();
    write_visible_graph(&node, &hidden, false);

    let orphan = GraphId::new(ORPHAN_GRAPH);
    node.import_graph_policy(&orphan, public_policy()).unwrap();
    node.apply_changes_unchecked(
        &orphan,
        vec![
            insert(
                &orphan,
                iri(ORPHAN_SUBJECT),
                RDF_TYPE,
                iri(SCHEMA_MEDIA_OBJECT),
            ),
            insert(
                &orphan,
                iri(ORPHAN_SUBJECT),
                TEST_PREDICATE,
                literal(SHARED_VALUE),
            ),
        ],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&orphan).unwrap();
    assert!(node.graph_diagnostics(&orphan).unwrap().has_orphans());
    node.ensure_query_indexes();

    Fixture {
        _directory: directory,
        node,
        visible,
        hidden,
        orphan,
        reader: GrantAuthorizer::default(),
    }
}

fn canonical_rows(results: QueryResults) -> Vec<Vec<(String, EncodedTerm)>> {
    let QueryResults::Solutions(rows) = results else {
        panic!("expected solution rows, got {results:?}");
    };
    let mut rows: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let mut row: Vec<_> = row.into_iter().collect();
            row.sort();
            row
        })
        .collect();
    rows.sort();
    rows
}

fn query_unbounded(
    node: &CraqleNode,
    auth: &dyn Authorizer,
    sparql: &str,
) -> craqle::Result<QueryResults> {
    let prepared = node.prepare_query(sparql)?;
    let mut options = QueryOptions::default();
    options.limits = QueryLimits::unbounded();
    node.execute_prepared(auth, &prepared, &options)
        .map(|execution| execution.results)
}

fn expected_shared_rows() -> Vec<Vec<(String, EncodedTerm)>> {
    vec![vec![("s".to_string(), iri(SHARED_SUBJECT))]]
}

fn canonical_expected(
    mut rows: Vec<Vec<(String, EncodedTerm)>>,
) -> Vec<Vec<(String, EncodedTerm)>> {
    for row in &mut rows {
        row.sort();
    }
    rows.sort();
    rows
}

fn expected_named_rows(graphs: &[GraphId]) -> Vec<Vec<(String, EncodedTerm)>> {
    let mut rows: Vec<_> = graphs
        .iter()
        .map(|graph| {
            vec![
                ("g".to_string(), iri(graph.as_str())),
                ("s".to_string(), iri(SHARED_SUBJECT)),
            ]
        })
        .collect();
    rows.sort();
    rows
}

fn expected_all_rows() -> Vec<Vec<(String, EncodedTerm)>> {
    canonical_expected(vec![
        vec![
            ("s".to_string(), iri(SHARED_SUBJECT)),
            ("o".to_string(), literal(SHARED_VALUE)),
        ],
        vec![
            ("s".to_string(), iri(UNIQUE_SUBJECT)),
            ("o".to_string(), literal(UNIQUE_VALUE)),
        ],
    ])
}

fn shared_query() -> String {
    format!("SELECT ?s WHERE {{ ?s <{TEST_PREDICATE}> \"{SHARED_VALUE}\" }}")
}

fn named_shared_query() -> String {
    format!("SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s <{TEST_PREDICATE}> \"{SHARED_VALUE}\" }} }}")
}

#[test]
fn graph_variables_preserve() {
    let fixture = fixture(2);
    let rows = canonical_rows(
        fixture
            .node
            .query(&fixture.reader, &named_shared_query())
            .unwrap(),
    );

    assert_eq!(rows, expected_named_rows(&fixture.visible));
}

#[test]
fn mixed_patterns_preserve() {
    let fixture = fixture(2);
    let query = format!(
        "SELECT ?g ?s WHERE {{ ?s <{TEST_PREDICATE}> \"{SHARED_VALUE}\" . \
         GRAPH ?g {{ ?s <{TEST_PREDICATE}> \"{SHARED_VALUE}\" }} }}"
    );
    let rows = canonical_rows(query_unbounded(&fixture.node, &fixture.reader, &query).unwrap());

    assert_eq!(rows, expected_named_rows(&fixture.visible));
    assert!(rows.iter().all(|row| {
        row.iter()
            .find_map(|(name, value)| (name == "g").then_some(value))
            .is_some_and(|value| value != &iri("urn:craqle:default"))
    }));
}

#[test]
fn scans_filter_visibility() {
    let fixture = fixture(2);
    let default_rows = canonical_rows(
        fixture
            .node
            .query(&fixture.reader, &shared_query())
            .unwrap(),
    );
    assert_eq!(default_rows, expected_shared_rows());

    let named_rows = canonical_rows(
        fixture
            .node
            .query(&fixture.reader, &named_shared_query())
            .unwrap(),
    );
    assert_eq!(named_rows, expected_named_rows(&fixture.visible));
    assert!(named_rows.iter().all(|row| {
        let graph = row
            .iter()
            .find_map(|(name, value)| (name == "g").then_some(value))
            .expect("named query must bind ?g");
        graph != &iri(fixture.hidden.as_str()) && graph != &iri(fixture.orphan.as_str())
    }));
}

#[test]
fn scope_boundary_stable() {
    let fixture = fixture(33);
    let default_query = shared_query();
    let named_query = named_shared_query();

    for count in [1, 2, 32, 33] {
        let graphs = &fixture.visible[..count];
        assert_eq!(
            canonical_rows(
                fixture
                    .node
                    .query_in_graphs(&AllowAllAuthorizer, graphs, &default_query)
                    .unwrap(),
            ),
            expected_shared_rows(),
            "default union scope of {count} graphs"
        );
        assert_eq!(
            canonical_rows(
                fixture
                    .node
                    .query_in_graphs(&AllowAllAuthorizer, graphs, &named_query)
                    .unwrap(),
            ),
            expected_named_rows(graphs),
            "named graph scope of {count} graphs"
        );
    }
}

#[test]
fn bounded_queries_deduplicate() {
    let fixture = fixture(2);
    let hit = format!("ASK {{ <{SHARED_SUBJECT}> <{TEST_PREDICATE}> \"{SHARED_VALUE}\" }}");
    let miss = format!(
        "ASK {{ <urn:pr3:query-semantics:missing> <{TEST_PREDICATE}> \"{SHARED_VALUE}\" }}"
    );
    assert_eq!(
        fixture.node.query(&fixture.reader, &hit).unwrap(),
        QueryResults::Boolean(true)
    );
    assert_eq!(
        fixture.node.query(&fixture.reader, &miss).unwrap(),
        QueryResults::Boolean(false)
    );

    let limited = format!("SELECT ?s ?o WHERE {{ ?s <{TEST_PREDICATE}> ?o }} ORDER BY ?o LIMIT 1");
    let ten = format!("SELECT ?s ?o WHERE {{ ?s <{TEST_PREDICATE}> ?o }} ORDER BY ?o LIMIT 10");
    assert_eq!(
        canonical_rows(query_unbounded(&fixture.node, &fixture.reader, &limited).unwrap()),
        canonical_expected(vec![vec![
            ("s".to_string(), iri(UNIQUE_SUBJECT)),
            ("o".to_string(), literal(UNIQUE_VALUE)),
        ]])
    );
    assert_eq!(
        canonical_rows(query_unbounded(&fixture.node, &fixture.reader, &ten).unwrap()),
        expected_all_rows()
    );
}
