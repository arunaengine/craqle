use craqle::{
    CraqleNode, DenyAllAuthorizer, EncodedTerm, GraphId, JoinKind, JoinMode,
    MaterializedQuadChange, QueryExecution, QueryExecutionOptions, QueryFastPathKind,
    QueryFastPathMode, QueryResults,
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
    node.execute_prepared_graphs(graphs, &query, &options)
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
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:test:fast:p> ?o }",
            QueryFastPathKind::UnionCount,
        ),
        (
            "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <urn:test:fast:primary> { ?s <urn:test:fast:p> ?o } }",
            QueryFastPathKind::NamedCount,
        ),
        (
            "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s <urn:test:fast:p> <urn:test:fast:o:shared> }",
            QueryFastPathKind::CountDistinctSubject,
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
    node.execute_prepared_graphs(std::slice::from_ref(&graph), &star, &property_options)
        .unwrap();
    let single = node
        .prepare_query("SELECT ?s WHERE { ?s <urn:test:fast:p> ?o } LIMIT 1")
        .unwrap();
    assert!(
        node.execute_prepared_graphs(std::slice::from_ref(&graph), &single, &property_options)
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
            iri(&format!("urn:test:fast:left-value:{index}")),
        ));
        changes.push(insert(
            &graph,
            &subject,
            "urn:test:fast:right",
            iri(&format!("urn:test:fast:right-value:{index}")),
        ));
    }
    node.apply_changes_unchecked(&graph, changes).unwrap();
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
        node.execute_prepared_graphs(std::slice::from_ref(&graph), &query, &options)
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
            node.execute_prepared_graphs(std::slice::from_ref(&graph), &hash_query, &options)
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
