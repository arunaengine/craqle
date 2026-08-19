//! Small, deterministic semantic baseline for the public SPARQL API.
//!
//! This deliberately checks results, not query timings.  Each baseline query
//! runs with the craqle optimizer both enabled and disabled; solution rows are
//! compared as multisets because SPARQL does not prescribe an order here.

use oxrdf::Term;

use craqle::{
    CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange, QueryResults,
    SearchStorage,
};

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

fn canonicalize(results: QueryResults) -> CanonicalResults {
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
            rows.sort();
            CanonicalResults::Solutions(rows)
        }
        QueryResults::Graph(mut triples) => {
            triples.sort();
            CanonicalResults::Graph(triples)
        }
    }
}

fn planner_result<F>(node: &CraqleNode, label: &str, visible: F, sparql: &str) -> CanonicalResults
where
    F: Fn(&GraphId) -> bool,
{
    let optimized = canonicalize(
        node.query_graphs_with_planner(&visible, sparql, true)
            .unwrap(),
    );
    let unoptimized = canonicalize(
        node.query_graphs_with_planner(&visible, sparql, false)
            .unwrap(),
    );
    assert_eq!(
        optimized, unoptimized,
        "{label}: optimizer changed SPARQL semantics\nquery: {sparql}"
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
fn baseline_queries_are_exact_and_planner_invariant() {
    let fixture = fixture();

    let ask_hit = format!("ASK WHERE {{ <{NEEDLE_SUBJECT}> <{KNOWN}> <{KNOWN_OBJECT}> }}");
    assert_eq!(
        planner_result(&fixture.node, "bound ASK hit", |_| true, &ask_hit),
        CanonicalResults::Boolean(true)
    );

    let ask_miss = format!("ASK WHERE {{ <urn:baseline:missing> <{KNOWN}> <{KNOWN_OBJECT}> }}");
    assert_eq!(
        planner_result(&fixture.node, "bound ASK miss", |_| true, &ask_miss),
        CanonicalResults::Boolean(false)
    );

    let limit = format!("SELECT ?s WHERE {{ ?s <{KNOWN}> <{KNOWN_OBJECT}> }} LIMIT 10");
    let limit_rows = solution_rows(planner_result(
        &fixture.node,
        "SELECT LIMIT 10",
        |_| true,
        &limit,
    ));
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
    let count_rows = solution_rows(planner_result(
        &fixture.node,
        "exact COUNT(*)",
        |_| true,
        &count,
    ));
    assert_eq!(count_rows.len(), 1);
    assert_eq!(literal_value(binding(&count_rows[0], "count")), "11");

    let property_star = format!(
        "SELECT ?s ?name ?rare WHERE {{ ?s <{NAME}> ?name ; <{RARE}> ?rare ; <{COMMON}> ?value }}"
    );
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "same-subject property star",
            |_| true,
            &property_star,
        )),
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
        solution_rows(planner_result(
            &fixture.node,
            "rare-to-common join",
            |_| true,
            &rare_to_common,
        )),
        expected_join
    );
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "common-to-rare join",
            |_| true,
            &common_to_rare,
        )),
        expected_join
    );
}

#[test]
fn duplicate_graph_results_are_planner_invariant_and_named_rows_keep_multiplicity() {
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

    let _ = planner_result(
        &fixture.node,
        "union default duplicate behavior",
        |_| true,
        &default_query,
    );
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "named graph duplicate multiplicity",
            |_| true,
            &named_query,
        )),
        named_rows,
        "non-DISTINCT SELECT must preserve one row from each named graph"
    );
}

#[test]
fn union_default_graph_deduplicates_identical_triples() {
    let fixture = fixture();
    let query = format!("SELECT ?s WHERE {{ ?s <{SHARED}> \"same\" }}");
    let expected = expected_rows(vec![vec![("s".to_string(), iri(DUPLICATE_SUBJECT))]]);

    assert_eq!(
        solution_rows(canonicalize(
            fixture.node.query_graphs_with(|_| true, &query).unwrap(),
        )),
        expected,
        "the union default graph must contain one copy of an identical triple"
    );
}

#[test]
fn hidden_graphs_and_recorded_orphans_are_not_query_visible() {
    let fixture = fixture();
    let hidden_query = format!("SELECT ?s WHERE {{ ?s <{HIDDEN}> \"hidden\" }}");
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "visible graph baseline",
            |_| true,
            &hidden_query,
        )),
        expected_rows(vec![vec![(
            "s".to_string(),
            iri("urn:baseline:hidden-subject"),
        )]])
    );
    let hidden = fixture.hidden.clone();
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "hidden graph visibility",
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
        "the public unchecked-write fixture must construct a recorded orphan"
    );
    let orphan_query = format!("SELECT ?s WHERE {{ ?s <{NAME}> \"stray\" }}");
    assert_eq!(
        solution_rows(canonicalize(
            fixture
                .node
                .query_graphs(std::slice::from_ref(&fixture.orphan), &orphan_query)
                .unwrap(),
        )),
        Vec::<Vec<(String, EncodedTerm)>>::new(),
        "an explicit graph list must not expose a recorded orphan"
    );
    assert_eq!(
        solution_rows(planner_result(
            &fixture.node,
            "orphan filtering",
            |graph| graph == &fixture.orphan,
            &orphan_query,
        )),
        Vec::<Vec<(String, EncodedTerm)>>::new()
    );
}
