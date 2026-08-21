use craqle::{
    AllowAllAuthorizer, CraqleErrorKind, CraqleNode, DenyAllAuthorizer, EncodedTerm, GraphId,
    JoinKind, JoinMode, MaterializedQuadChange, QueryExecution, QueryExecutionOptions,
    QueryFastPathKind, QueryFastPathMode, QueryResults,
};

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn literal(value: &str) -> EncodedTerm {
    EncodedTerm(format!("\"{value}\""))
}

fn insert(
    graph: &GraphId,
    subject: &str,
    predicate: &str,
    object: EncodedTerm,
) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: iri(subject),
        predicate: iri(predicate),
        object,
    }
}

fn run(
    node: &CraqleNode,
    graphs: &[GraphId],
    query: &str,
    fast_paths: QueryFastPathMode,
) -> QueryExecution {
    let query = node.prepare_query(query).unwrap();
    let mut options = QueryExecutionOptions::default();
    options.fast_paths = fast_paths;
    node.execute_prepared_in_graphs(&AllowAllAuthorizer, graphs, &query, &options)
        .unwrap()
}

fn canonical(results: &QueryResults) -> Vec<Vec<(String, EncodedTerm)>> {
    let QueryResults::Solutions(rows) = results else {
        panic!("expected solutions, got {results:?}");
    };
    let mut rows: Vec<_> = rows
        .iter()
        .map(|row| {
            let mut row: Vec<_> = row
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            row.sort();
            row
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn fast_paths_match_generic() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let primary = GraphId::new("urn:test:fast:primary");
    let duplicate = GraphId::new("urn:test:fast:duplicate");
    let orphan = GraphId::new("urn:test:fast:orphan");
    let mut state = 0x4352_4151_4c45_4650_u64;
    let mut changes = Vec::new();
    for index in 0..64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let subject = format!("urn:test:fast:s:{index:03}");
        let object = format!("urn:test:fast:o:{}", state % 8);
        changes.push(insert(&primary, &subject, "urn:test:fast:p", iri(&object)));
        if index < 16 {
            changes.push(insert(
                &primary,
                &subject,
                "urn:test:fast:name",
                literal(&format!("name-{index}")),
            ));
            changes.push(insert(
                &primary,
                &subject,
                "urn:test:fast:date",
                literal(&format!("date-{index}")),
            ));
        }
    }
    changes.push(insert(
        &primary,
        "urn:test:fast:shared",
        "urn:test:fast:p",
        iri("urn:test:fast:o:shared"),
    ));
    changes.push(insert(
        &primary,
        "urn:test:fast:shared",
        "urn:test:fast:q",
        iri("urn:test:fast:o:other"),
    ));
    changes.push(insert(
        &primary,
        "urn:test:fast:shared",
        "urn:test:fast:r",
        iri("urn:test:fast:o:other"),
    ));
    node.apply_changes_unchecked(&primary, changes).unwrap();
    node.apply_changes_unchecked(
        &duplicate,
        vec![insert(
            &duplicate,
            "urn:test:fast:shared",
            "urn:test:fast:p",
            iri("urn:test:fast:o:shared"),
        )],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &orphan,
        vec![
            insert(
                &orphan,
                orphan.as_str(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                iri("http://schema.org/Dataset"),
            ),
            insert(
                &orphan,
                "urn:test:fast:stray",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                iri("http://schema.org/MediaObject"),
            ),
            insert(
                &orphan,
                "urn:test:fast:stray",
                "urn:test:fast:p",
                iri("urn:test:fast:o:orphan"),
            ),
        ],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&orphan).unwrap();
    node.ensure_query_indexes();
    let graphs = vec![primary.clone(), duplicate.clone(), orphan.clone()];

    let cases = [
        (
            "ASK { <urn:test:fast:shared> <urn:test:fast:p> <urn:test:fast:o:shared> }",
            QueryFastPathKind::Ask,
        ),
        (
            "SELECT ?s WHERE { ?s <urn:test:fast:p> ?o } LIMIT 10",
            QueryFastPathKind::SelectLimit,
        ),
        (
            "SELECT ?s WHERE { ?s <urn:test:fast:p> <urn:test:fast:o:shared> }",
            QueryFastPathKind::Projection,
        ),
        (
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:test:fast:p> ?o }",
            QueryFastPathKind::UnionCount,
        ),
        (
            "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:primary> { ?s <urn:test:fast:p> ?o } }",
            QueryFastPathKind::NamedCount,
        ),
        (
            "SELECT (COUNT(*) AS ?count) WHERE { <urn:test:fast:shared> ?p <urn:test:fast:o:shared> }",
            QueryFastPathKind::UnionCount,
        ),
        (
            "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s <urn:test:fast:p> <urn:test:fast:o:shared> }",
            QueryFastPathKind::CountDistinctSubject,
        ),
        (
            "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ?p <urn:test:fast:o:shared> }",
            QueryFastPathKind::CountDistinctSubject,
        ),
        (
            "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { <urn:test:fast:shared> <urn:test:fast:p> ?o }",
            QueryFastPathKind::CountDistinctObject,
        ),
        (
            "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s <urn:test:fast:p> ?o }",
            QueryFastPathKind::CountDistinctObject,
        ),
        (
            "SELECT ?s ?name ?date WHERE { ?s <urn:test:fast:p> ?o ; <urn:test:fast:name> ?name ; <urn:test:fast:date> ?date }",
            QueryFastPathKind::PropertyStar,
        ),
    ];
    for (query, expected_kind) in cases {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.statistics.fast_path, Some(expected_kind), "{query}");
        assert_eq!(generic.statistics.fast_path, None, "{query}");
        match (&fast.results, &generic.results) {
            (QueryResults::Boolean(left), QueryResults::Boolean(right)) => {
                assert_eq!(left, right, "{query}")
            }
            (QueryResults::Solutions(_), QueryResults::Solutions(_)) => {
                assert_eq!(
                    canonical(&fast.results),
                    canonical(&generic.results),
                    "{query}"
                )
            }
            _ => panic!("result form changed for {query}"),
        }
    }

    let orphan_ask = "ASK { <urn:test:fast:stray> <urn:test:fast:p> <urn:test:fast:o:orphan> }";
    let fast = run(&node, &graphs, orphan_ask, QueryFastPathMode::Auto);
    let generic = run(&node, &graphs, orphan_ask, QueryFastPathMode::Disabled);
    assert_eq!(fast.results, QueryResults::Boolean(false));
    assert_eq!(fast.results, generic.results);

    let duplicate_count = run(
        &node,
        &graphs,
        "SELECT (COUNT(*) AS ?count) WHERE { <urn:test:fast:shared> <urn:test:fast:p> <urn:test:fast:o:shared> }",
        QueryFastPathMode::Auto,
    );
    let generic_duplicate_count = run(
        &node,
        &graphs,
        "SELECT (COUNT(*) AS ?count) WHERE { <urn:test:fast:shared> <urn:test:fast:p> <urn:test:fast:o:shared> }",
        QueryFastPathMode::Disabled,
    );
    assert_eq!(duplicate_count.results, generic_duplicate_count.results);
    assert_eq!(duplicate_count.statistics.encoded_quad_constructions, 0);
    assert_eq!(duplicate_count.statistics.authoritative_terms_decoded, 0);
    assert_eq!(duplicate_count.statistics.result_terms_decoded, 0);
    assert_eq!(duplicate_count.statistics.terms_decoded, 0);

    let subject_only = run(
        &node,
        &graphs,
        "SELECT ?s WHERE { ?s <urn:test:fast:p> ?o } LIMIT 10",
        QueryFastPathMode::Auto,
    );
    assert_eq!(
        subject_only.statistics.authoritative_terms_decoded,
        subject_only.statistics.result_cells
    );
    assert_eq!(
        subject_only.statistics.result_terms_decoded,
        subject_only.statistics.result_cells
    );
}

#[test]
fn fast_paths_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:private");
    node.apply_changes_unchecked(
        &graph,
        vec![insert(
            &graph,
            "urn:test:fast:s",
            "urn:test:fast:p",
            iri("urn:test:fast:o"),
        )],
    )
    .unwrap();
    let prepared = node
        .prepare_query("ASK { <urn:test:fast:s> <urn:test:fast:p> <urn:test:fast:o> }")
        .unwrap();
    let denied = node
        .execute_prepared(
            &DenyAllAuthorizer,
            &prepared,
            &QueryExecutionOptions::default(),
        )
        .unwrap();
    assert_eq!(denied.results, QueryResults::Boolean(false));
    assert_eq!(denied.statistics.fast_path, Some(QueryFastPathKind::Ask));

    let star = node
        .prepare_query("SELECT ?s WHERE { ?s <urn:test:fast:p> ?o ; <urn:test:fast:q> ?q }")
        .unwrap();
    let mut property_options = QueryExecutionOptions::default();
    property_options.join_mode = craqle::JoinMode::ForcePropertyStar;
    node.execute_prepared_in_graphs(
        &AllowAllAuthorizer,
        std::slice::from_ref(&graph),
        &star,
        &property_options,
    )
    .unwrap();
    let single = node
        .prepare_query("SELECT ?s WHERE { ?s <urn:test:fast:p> ?o } LIMIT 1")
        .unwrap();
    assert!(
        node.execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &single,
            &property_options,
        )
        .is_err()
    );

    let mut values = Vec::new();
    for index in 0..64 {
        values.push(insert(
            &graph,
            "urn:test:fast:bounded-star",
            "urn:test:fast:name",
            literal(&format!("name-{index}")),
        ));
    }
    values.push(insert(
        &graph,
        "urn:test:fast:bounded-star",
        "urn:test:fast:date",
        literal("date"),
    ));
    node.apply_changes_unchecked(&graph, values).unwrap();
    let bounded = run(
        &node,
        std::slice::from_ref(&graph),
        "SELECT ?name ?date WHERE { <urn:test:fast:bounded-star> <urn:test:fast:name> ?name ; <urn:test:fast:date> ?date } LIMIT 2",
        QueryFastPathMode::Auto,
    );
    assert_eq!(
        bounded.statistics.fast_path,
        Some(QueryFastPathKind::PropertyStar)
    );
    assert_eq!(bounded.statistics.result_rows, 2);
    assert!(bounded.statistics.candidate_quads <= 4);
}

#[test]
fn fixed_predicate_triangle_ask_uses_bounded_query_ids() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:triangle");
    let duplicate = GraphId::new("urn:test:fast:triangle:duplicate");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:a", "urn:edge", iri("urn:b")),
            insert(&graph, "urn:b", "urn:edge", iri("urn:c")),
            insert(&graph, "urn:c", "urn:edge", iri("urn:a")),
            insert(&graph, "urn:x", "urn:edge", iri("urn:y")),
            insert(&graph, "urn:a", "urn:other", iri("urn:b")),
            insert(&graph, "urn:b", "urn:other", iri("urn:c")),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &duplicate,
        vec![insert(&duplicate, "urn:a", "urn:edge", iri("urn:b"))],
    )
    .unwrap();
    node.ensure_query_indexes();
    let graphs = vec![graph.clone(), duplicate.clone()];

    for query in [
        "ASK { ?a <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:edge> ?a }",
        "ASK { ?c <urn:edge> ?a . ?a <urn:edge> ?b . ?b <urn:edge> ?c }",
        "ASK { GRAPH <urn:test:fast:triangle> { \
         ?a <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:edge> ?a } }",
        "ASK { ?a <urn:other> ?b . ?b <urn:other> ?c . ?c <urn:other> ?a }",
    ] {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::Ask),
            "{query}"
        );
        assert_eq!(fast.statistics.encoded_quad_constructions, 0, "{query}");
        assert_eq!(fast.statistics.authoritative_terms_decoded, 0, "{query}");
    }

    let hidden = run(
        &node,
        std::slice::from_ref(&duplicate),
        "ASK { ?a <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:edge> ?a }",
        QueryFastPathMode::Auto,
    );
    assert_eq!(hidden.results, QueryResults::Boolean(false));

    for query in [
        "ASK { ?a <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:other> ?a }",
        "ASK { <urn:a> <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:edge> <urn:a> }",
        "ASK { ?a <urn:edge> ?b . ?b <urn:edge> ?c . \
         ?c <urn:edge> ?d . ?d <urn:edge> ?a }",
    ] {
        let auto = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(auto.results, generic.results, "{query}");
        assert_eq!(auto.statistics.fast_path, None, "{query}");
    }

    let prepared = node
        .prepare_query("ASK { ?a <urn:edge> ?b . ?b <urn:edge> ?c . ?c <urn:edge> ?a }")
        .unwrap();
    let mut limited = QueryExecutionOptions::default();
    limited.limits.max_hash_entries = 1;
    let error = node
        .execute_prepared_in_graphs(&AllowAllAuthorizer, &graphs, &prepared, &limited)
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::QueryLimit);
}

#[test]
fn count_fast_path_matches_every_triple_binding_shape() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:count-bindings");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:s", "urn:p", iri("urn:o")),
            insert(&graph, "urn:s", "urn:q", iri("urn:x")),
            insert(&graph, "urn:t", "urn:p", iri("urn:o")),
        ],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();

    for triple in [
        "?s ?p ?o",
        "<urn:s> ?p ?o",
        "?s <urn:p> ?o",
        "?s ?p <urn:o>",
        "<urn:s> <urn:p> ?o",
        "<urn:s> ?p <urn:o>",
        "?s <urn:p> <urn:o>",
        "<urn:s> <urn:p> <urn:o>",
    ] {
        for body in [
            format!("{{ {triple} }}"),
            format!("{{ GRAPH <{}> {{ {triple} }} }}", graph.as_str()),
        ] {
            for aggregate in ["COUNT(*)", "COUNT(?s)"] {
                if aggregate == "COUNT(?s)" && !triple.contains("?s") {
                    continue;
                }
                let query = format!("SELECT ({aggregate} AS ?count) WHERE {body}");
                let fast = run(
                    &node,
                    std::slice::from_ref(&graph),
                    &query,
                    QueryFastPathMode::Auto,
                );
                let generic = run(
                    &node,
                    std::slice::from_ref(&graph),
                    &query,
                    QueryFastPathMode::Disabled,
                );
                assert_eq!(fast.results, generic.results, "{query}");
                assert!(fast.statistics.fast_path.is_some(), "{query}");
                assert_eq!(fast.statistics.encoded_quad_constructions, 0, "{query}");
                assert_eq!(fast.statistics.authoritative_terms_decoded, 0, "{query}");
            }
        }
    }
}

#[test]
fn duplicate_free_union_count_uses_exact_graph_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let first = GraphId::new("urn:test:fast:union-meta:first");
    let second = GraphId::new("urn:test:fast:union-meta:second");
    node.apply_changes_unchecked(
        &first,
        vec![insert(&first, "urn:s:first", "urn:p", iri("urn:o:first"))],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &second,
        vec![insert(
            &second,
            "urn:s:second",
            "urn:p",
            iri("urn:o:second"),
        )],
    )
    .unwrap();
    let graphs = vec![first.clone(), second.clone()];
    let query = "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?o }";

    let exact = run(&node, &graphs, query, QueryFastPathMode::Auto);
    let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
    assert_eq!(exact.results, generic.results);
    assert_eq!(exact.statistics.qv_keys_read, 0);
    assert_eq!(exact.statistics.encoded_quad_constructions, 0);

    node.apply_changes_unchecked(
        &second,
        vec![insert(&second, "urn:s:first", "urn:p", iri("urn:o:first"))],
    )
    .unwrap();
    let grouped = run(&node, &graphs, query, QueryFastPathMode::Auto);
    let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
    assert_eq!(grouped.results, generic.results);
    assert!(grouped.statistics.qv_keys_read > 0);
}

#[test]
fn subject_star_count_preserves_multiplicity() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:subject-star");
    let mut changes = Vec::new();
    for object in ["urn:o:a", "urn:o:b"] {
        changes.push(insert(&graph, "urn:s:1", "urn:p:1", iri(object)));
    }
    for object in ["urn:o:c", "urn:o:d", "urn:o:e"] {
        changes.push(insert(&graph, "urn:s:1", "urn:p:2", iri(object)));
    }
    changes.push(insert(&graph, "urn:s:1", "urn:p:3", iri("urn:o:f")));
    changes.push(insert(&graph, "urn:s:2", "urn:p:1", iri("urn:o:g")));
    changes.push(insert(&graph, "urn:s:2", "urn:p:2", iri("urn:o:h")));
    for object in ["urn:o:i", "urn:o:j"] {
        changes.push(insert(&graph, "urn:s:2", "urn:p:3", iri(object)));
    }
    node.apply_changes_unchecked(&graph, changes).unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p:1> ?a ; <urn:p:2> ?b ; <urn:p:3> ?c }",
        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:subject-star> { \
         ?s <urn:p:1> ?a ; <urn:p:2> ?b ; <urn:p:3> ?c } }",
    ] {
        let fast = run(
            &node,
            std::slice::from_ref(&graph),
            query,
            QueryFastPathMode::Auto,
        );
        let generic = run(
            &node,
            std::slice::from_ref(&graph),
            query,
            QueryFastPathMode::Disabled,
        );
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::SubjectStarCount)
        );
        assert_eq!(fast.statistics.encoded_quad_constructions, 0);
        assert_eq!(fast.statistics.authoritative_terms_decoded, 0);
    }
}

#[test]
fn optional_subject_star_count_preserves_left_join_multiplicity() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:optional-star");
    let duplicate = GraphId::new("urn:test:fast:optional-star-duplicate");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:s:1", "urn:p", iri("urn:left:1")),
            insert(&graph, "urn:s:1", "urn:p", iri("urn:left:2")),
            insert(&graph, "urn:s:1", "urn:q", iri("urn:right:1")),
            insert(&graph, "urn:s:1", "urn:q", iri("urn:right:2")),
            insert(&graph, "urn:s:1", "urn:q", iri("urn:right:3")),
            insert(&graph, "urn:s:1", "urn:m", iri("urn:mandatory:1")),
            insert(&graph, "urn:s:1", "urn:r", iri("urn:extra:1")),
            insert(&graph, "urn:s:1", "urn:r", iri("urn:extra:2")),
            insert(&graph, "urn:s:2", "urn:p", iri("urn:left:3")),
            insert(&graph, "urn:s:2", "urn:m", iri("urn:mandatory:2")),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &duplicate,
        vec![insert(&duplicate, "urn:s:1", "urn:p", iri("urn:left:1"))],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();
    let graphs = vec![graph.clone(), duplicate];

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?left OPTIONAL { ?s <urn:q> ?right } }",
        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:optional-star> { \
         ?s <urn:p> ?left OPTIONAL { ?s <urn:q> ?right } } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?left OPTIONAL { ?s <urn:missing> ?right } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?left ; <urn:m> ?mandatory \
         OPTIONAL { ?s <urn:q> ?right ; <urn:r> ?extra } }",
    ] {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::SubjectStarCount),
            "{query}"
        );
        assert_eq!(fast.statistics.encoded_quad_constructions, 0, "{query}");
        assert_eq!(fast.statistics.authoritative_terms_decoded, 0, "{query}");
    }

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?value OPTIONAL { ?s <urn:q> ?value } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?left OPTIONAL { ?s <urn:q> ?right \
         FILTER(?right = <urn:right:1>) } }",
    ] {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(fast.statistics.fast_path, None, "{query}");
    }
}

#[test]
fn subject_set_counts_cover_exists_not_exists_and_minus() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:subject-set");
    let duplicate = GraphId::new("urn:test:fast:subject-set-duplicate");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:s:1", "urn:p", iri("urn:outer:1")),
            insert(&graph, "urn:s:1", "urn:p", iri("urn:outer:2")),
            insert(&graph, "urn:s:1", "urn:m", iri("urn:mandatory:1")),
            insert(&graph, "urn:s:1", "urn:q", iri("urn:inner:1")),
            insert(&graph, "urn:s:1", "urn:q", iri("urn:inner:2")),
            insert(&graph, "urn:s:1", "urn:r", iri("urn:required:1")),
            insert(&graph, "urn:s:2", "urn:p", iri("urn:outer:3")),
            insert(&graph, "urn:s:2", "urn:m", iri("urn:mandatory:2")),
            insert(&graph, "urn:s:3", "urn:p", iri("urn:outer:4")),
            insert(&graph, "urn:s:3", "urn:q", iri("urn:inner:3")),
        ],
    )
    .unwrap();
    node.apply_changes_unchecked(
        &duplicate,
        vec![insert(&duplicate, "urn:s:1", "urn:p", iri("urn:outer:1"))],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();
    let graphs = vec![graph.clone(), duplicate];

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer FILTER EXISTS { ?s <urn:q> ?inner } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer FILTER NOT EXISTS { ?s <urn:q> ?inner } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer MINUS { ?s <urn:q> ?inner } }",
        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:subject-set> { \
         ?s <urn:p> ?outer FILTER EXISTS { ?s <urn:q> ?inner } } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer ; <urn:m> ?mandatory \
         FILTER EXISTS { ?s <urn:q> ?inner ; <urn:r> ?required } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer FILTER NOT EXISTS { ?s <urn:missing> ?inner } }",
    ] {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::HashJoinCount),
            "{query}"
        );
        assert_eq!(fast.statistics.encoded_quad_constructions, 0, "{query}");
        assert_eq!(fast.statistics.authoritative_terms_decoded, 0, "{query}");
    }

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?value FILTER EXISTS { ?s <urn:q> ?value } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         <urn:s:1> <urn:p> ?outer MINUS { <urn:s:1> <urn:q> ?inner } }",
        "SELECT (COUNT(*) AS ?count) WHERE { \
         ?s <urn:p> ?outer FILTER EXISTS { ?s <urn:q> ?inner \
         FILTER(?inner = <urn:inner:1>) } }",
    ] {
        let fast = run(&node, &graphs, query, QueryFastPathMode::Auto);
        let generic = run(&node, &graphs, query, QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(fast.statistics.fast_path, None, "{query}");
    }

    let prepared = node
        .prepare_query(
            "SELECT (COUNT(*) AS ?count) WHERE { \
             ?s <urn:p> ?outer FILTER EXISTS { ?s <urn:q> ?inner } }",
        )
        .unwrap();
    let mut limited = QueryExecutionOptions::default();
    limited.limits.max_hash_entries = 1;
    let error = node
        .execute_prepared_in_graphs(&AllowAllAuthorizer, &graphs, &prepared, &limited)
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::QueryLimit);
}

#[test]
fn linear_chain_count_uses_explicit_cross_domains() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:linear-chain");
    node.apply_changes_unchecked(
        &graph,
        vec![
            insert(&graph, "urn:a", "urn:p:1", iri("urn:k:1")),
            insert(&graph, "urn:b", "urn:p:1", iri("urn:k:2")),
            insert(&graph, "urn:k:1", "urn:q:1", iri("urn:x")),
            insert(&graph, "urn:k:1", "urn:q:1", iri("urn:y")),
            insert(&graph, "urn:k:2", "urn:q:1", iri("urn:z")),
            insert(&graph, "urn:c", "urn:p:2", iri("urn:m:1")),
            insert(&graph, "urn:d", "urn:p:2", iri("urn:m:1")),
            insert(&graph, "urn:e", "urn:p:2", iri("urn:m:2")),
            insert(&graph, "urn:m:1", "urn:q:2", iri("urn:u")),
            insert(&graph, "urn:m:2", "urn:q:2", iri("urn:v")),
        ],
    )
    .unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();

    for body in [
        "?s <urn:p:1> ?key . ?key <urn:q:1> ?value",
        "?s <urn:p:2> ?key . ?key <urn:q:2> ?value",
    ] {
        for query in [
            format!("SELECT (COUNT(*) AS ?count) WHERE {{ {body} }}"),
            format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ GRAPH <{}> {{ {body} }} }}",
                graph.as_str()
            ),
        ] {
            let prepared = node.prepare_query(&query).unwrap();
            let execute = |fast_paths| {
                let mut options = QueryExecutionOptions::default();
                options.fast_paths = fast_paths;
                options.join_mode = JoinMode::ForceHash;
                node.execute_prepared_in_graphs(
                    &AllowAllAuthorizer,
                    std::slice::from_ref(&graph),
                    &prepared,
                    &options,
                )
                .unwrap()
            };
            let fast = execute(QueryFastPathMode::Auto);
            let generic = execute(QueryFastPathMode::Disabled);
            assert_eq!(fast.results, generic.results, "{query}");
            assert_eq!(
                fast.statistics.fast_path,
                Some(QueryFastPathKind::HashJoinCount)
            );
            assert_eq!(fast.statistics.encoded_quad_constructions, 0);
            assert_eq!(fast.statistics.authoritative_terms_decoded, 0);
        }
    }
}

#[test]
fn hash_count_multiplicity() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:hash");
    let mut changes = Vec::with_capacity(1_024);
    for index in 0..512 {
        let subject = format!("urn:test:fast:hash:s:{}", index % 8);
        changes.push(insert(
            &graph,
            &subject,
            "urn:test:fast:left",
            iri(&format!("urn:test:fast:join-value:{index}")),
        ));
        changes.push(insert(
            &graph,
            &subject,
            "urn:test:fast:right",
            iri(&format!("urn:test:fast:join-value:{index}")),
        ));
    }
    node.apply_changes_unchecked(&graph, changes).unwrap();
    node.rebuild_graph_diagnostics(&graph).unwrap();
    node.ensure_query_indexes();
    let query = node
        .prepare_query(
            "SELECT (COUNT(*) AS ?count) WHERE { \
             ?s <urn:test:fast:left> ?left . \
             ?s <urn:test:fast:right> ?right }",
        )
        .unwrap();

    let execute = |join_mode| {
        let mut options = QueryExecutionOptions::default();
        options.join_mode = join_mode;
        node.execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &query,
            &options,
        )
        .unwrap()
    };
    let lateral = execute(JoinMode::ForceLateral);
    let hash = execute(JoinMode::ForceHash);
    let automatic = execute(JoinMode::Auto);

    assert_eq!(lateral.results, hash.results);
    assert_eq!(hash.results, automatic.results);
    assert_eq!(lateral.statistics.fast_path, None);
    assert_eq!(
        hash.statistics.fast_path,
        Some(QueryFastPathKind::HashJoinCount)
    );
    assert_eq!(
        automatic.statistics.fast_path,
        Some(QueryFastPathKind::HashJoinCount)
    );
    assert_eq!(
        lateral.statistics.planned_joins[0].physical_operator,
        JoinKind::IndexedLateral
    );
    assert_eq!(
        hash.statistics.planned_joins[0].physical_operator,
        JoinKind::Hash
    );
    assert_eq!(
        automatic.statistics.planned_joins[0].physical_operator,
        JoinKind::Hash
    );

    for query in [
        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:hash> { \
         ?s <urn:test:fast:left> ?left . ?s <urn:test:fast:right> ?right } }",
        "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:hash> { \
         ?left <urn:test:fast:left> ?key . ?right <urn:test:fast:right> ?key } }",
    ] {
        let fast = run(
            &node,
            std::slice::from_ref(&graph),
            query,
            QueryFastPathMode::Auto,
        );
        let generic = run(
            &node,
            std::slice::from_ref(&graph),
            query,
            QueryFastPathMode::Disabled,
        );
        assert_eq!(fast.results, generic.results, "{query}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::HashJoinCount)
        );
        assert_eq!(fast.statistics.encoded_quad_constructions, 0);
        assert_eq!(fast.statistics.authoritative_terms_decoded, 0);
        assert_eq!(fast.statistics.result_terms_decoded, 0);
        assert_eq!(
            fast.statistics.key_fields_extracted,
            fast.statistics.qv_keys_read
        );
    }

    let mut limited = QueryExecutionOptions::default();
    limited.join_mode = JoinMode::ForceHash;
    limited.limits.max_hash_entries = 1;
    let error = node
        .execute_prepared_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &query,
            &limited,
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::QueryLimit);
}

#[test]
fn randomized_paths_match() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fast:randomized");
    let mut state = 0x5348_4143_4c46_4153_u64;
    let mut changes = Vec::new();
    for seed in 0..4 {
        for index in 0..96 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let subject = format!("urn:test:fast:random:{seed}:s:{index}");
            let object = format!("urn:test:fast:random:{seed}:o:{}", state % 13);
            for predicate in ["p", "left", "right"] {
                changes.push(insert(
                    &graph,
                    &subject,
                    &format!("urn:test:fast:random:{seed}:{predicate}"),
                    iri(&object),
                ));
            }
            if state & 1 == 0 {
                changes.push(insert(
                    &graph,
                    &subject,
                    &format!("urn:test:fast:random:{seed}:name"),
                    literal(&format!("name-{index}")),
                ));
                changes.push(insert(
                    &graph,
                    &subject,
                    &format!("urn:test:fast:random:{seed}:date"),
                    literal(&format!("date-{index}")),
                ));
            }
        }
    }
    node.apply_changes_unchecked(&graph, changes).unwrap();
    node.ensure_query_indexes();

    for seed in 0..4 {
        let queries = [
            format!(
                "ASK {{ <urn:test:fast:random:{seed}:s:17> <urn:test:fast:random:{seed}:p> ?o }}"
            ),
            format!("SELECT ?s WHERE {{ ?s <urn:test:fast:random:{seed}:p> ?o }} LIMIT 10"),
            format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ ?s <urn:test:fast:random:{seed}:p> ?o }}"
            ),
            format!(
                "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE {{ ?s <urn:test:fast:random:{seed}:p> <urn:test:fast:random:{seed}:o:3> }}"
            ),
            format!(
                "SELECT ?s ?name ?date WHERE {{ ?s <urn:test:fast:random:{seed}:name> ?name ; <urn:test:fast:random:{seed}:date> ?date }}"
            ),
        ];
        for query in queries {
            let fast = run(
                &node,
                std::slice::from_ref(&graph),
                &query,
                QueryFastPathMode::Auto,
            );
            let generic = run(
                &node,
                std::slice::from_ref(&graph),
                &query,
                QueryFastPathMode::Disabled,
            );
            assert_eq!(fast.results, generic.results, "seed {seed}: {query}");
        }

        let hash_query = node
            .prepare_query(&format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ \
                 ?s <urn:test:fast:random:{seed}:left> ?key . \
                 ?s <urn:test:fast:random:{seed}:right> ?key }}"
            ))
            .unwrap();
        let execute_hash = |fast_paths| {
            let mut options = QueryExecutionOptions::default();
            options.fast_paths = fast_paths;
            options.join_mode = JoinMode::ForceHash;
            node.execute_prepared_in_graphs(
                &AllowAllAuthorizer,
                std::slice::from_ref(&graph),
                &hash_query,
                &options,
            )
            .unwrap()
        };
        let fast = execute_hash(QueryFastPathMode::Auto);
        let generic = execute_hash(QueryFastPathMode::Disabled);
        assert_eq!(fast.results, generic.results, "hash seed {seed}");
        assert_eq!(
            fast.statistics.fast_path,
            Some(QueryFastPathKind::HashJoinCount)
        );
    }
}
