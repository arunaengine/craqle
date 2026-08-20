use craqle::{
    AllowAllAuthorizer, CraqleError, CraqleNode, CreateCrateOptions, CreateCrateRequest,
    EncodedTerm, GraphId, GraphPolicy, MaterializedQuadChange, RoCrateError, RoCrateVersion, vocab,
};

const DCTERMS_CONFORMS_TO: &str = "<http://purl.org/dc/terms/conformsTo>";

fn crate_request(graph: GraphId) -> CreateCrateRequest {
    CreateCrateRequest::new(
        graph,
        "Rules version coverage",
        "Rules use each stored RO-Crate profile.",
        "2026-08-20",
        None,
        GraphPolicy::default(),
    )
}

fn drop_name(graph: &GraphId) -> MaterializedQuadChange {
    MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject: EncodedTerm::from_named_node(&graph.0),
        predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
        object: EncodedTerm("\"Rules version coverage\"".to_string()),
    }
}

fn version_term(version: RoCrateVersion) -> EncodedTerm {
    let specification = match version {
        RoCrateVersion::V1_1 => "https://w3id.org/ro/crate/1.1",
        RoCrateVersion::V1_2 => "https://w3id.org/ro/crate/1.2",
        RoCrateVersion::V1_3 => "https://w3id.org/ro/crate/1.3",
        _ => unreachable!("unsupported version in test"),
    };
    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(specification))
}

fn marker_change(
    graph: &GraphId,
    version: RoCrateVersion,
    inserted: bool,
) -> MaterializedQuadChange {
    let graph = graph.clone();
    let subject = EncodedTerm::from_named_node(&vocab::metadata_descriptor());
    let predicate = EncodedTerm(DCTERMS_CONFORMS_TO.to_string());
    let object = version_term(version);
    if inserted {
        MaterializedQuadChange::Insert {
            graph,
            subject,
            predicate,
            object,
        }
    } else {
        MaterializedQuadChange::Delete {
            graph,
            subject,
            predicate,
            object,
        }
    }
}

#[test]
fn rules_match_versions() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let mut expected = None;

    for version in [
        RoCrateVersion::V1_1,
        RoCrateVersion::V1_2,
        RoCrateVersion::V1_3,
    ] {
        let graph = GraphId::new(&format!("urn:test:rules-version:{version:?}"));
        node.create_crate_with_options(
            &AllowAllAuthorizer,
            crate_request(graph.clone()),
            CreateCrateOptions {
                version,
                license: None,
            },
        )
        .unwrap();
        assert_eq!(node.crate_version(&graph).unwrap(), version);

        node.apply_changes_unchecked(&graph, vec![drop_name(&graph)])
            .unwrap();
        let codes: Vec<_> = node
            .graph_violations(&graph)
            .unwrap()
            .into_iter()
            .map(|violation| violation.code)
            .collect();
        assert_eq!(codes, ["missing_required_property"]);
        if let Some(expected) = &expected {
            assert_eq!(&codes, expected);
        } else {
            expected = Some(codes);
        }
    }
}

#[test]
fn version_marker_cases() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    for (index, (from, to)) in [
        (RoCrateVersion::V1_2, RoCrateVersion::V1_1),
        (RoCrateVersion::V1_1, RoCrateVersion::V1_2),
        (RoCrateVersion::V1_1, RoCrateVersion::V1_3),
    ]
    .into_iter()
    .enumerate()
    {
        let graph = GraphId::new(&format!("urn:test:rules-marker-transition:{index}"));
        node.create_crate_with_options(
            &AllowAllAuthorizer,
            crate_request(graph.clone()),
            CreateCrateOptions {
                version: from,
                license: None,
            },
        )
        .unwrap();

        node.apply_changes(
            &graph,
            vec![
                marker_change(&graph, from, false),
                marker_change(&graph, to, true),
            ],
        )
        .unwrap();

        assert_eq!(node.crate_version(&graph).unwrap(), to);
        assert!(node.graph_violations(&graph).unwrap().is_empty());
    }
}

#[test]
fn version_fixture_results() {
    for (version, root, fixture) in [
        (
            RoCrateVersion::V1_1,
            "urn:fixture:rocrate:valid-1.1:root",
            include_str!("fixtures/rocrate/valid-1.1.json"),
        ),
        (
            RoCrateVersion::V1_2,
            "urn:fixture:rocrate:valid-1.2:root",
            include_str!("fixtures/rocrate/valid-1.2.json"),
        ),
        (
            RoCrateVersion::V1_3,
            "urn:fixture:rocrate:valid-1.3:root",
            include_str!("fixtures/rocrate/valid-1.3.json"),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();
        let graph = GraphId::new(root);
        node.apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            graph.clone(),
            fixture,
            GraphPolicy::default(),
        )
        .unwrap();
        assert_eq!(node.crate_version(&graph).unwrap(), version);
        assert!(node.graph_violations(&graph).unwrap().is_empty());

        let change = match version {
            RoCrateVersion::V1_1 => {
                let name = node
                    .graph_snapshot(&graph)
                    .unwrap()
                    .quads
                    .into_iter()
                    .find(|quad| {
                        quad.subject == EncodedTerm::from_named_node(&graph.0)
                            && quad.predicate == EncodedTerm::from_named_node(&vocab::schema_name())
                    })
                    .expect("fixture root name")
                    .object;
                MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: EncodedTerm::from_named_node(&graph.0),
                    predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
                    object: name,
                }
            }
            RoCrateVersion::V1_2 => MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm("<urn:test:fixture-untyped>".to_string()),
                predicate: EncodedTerm::from_named_node(&vocab::schema_name()),
                object: EncodedTerm("\"untyped fixture\"".to_string()),
            },
            RoCrateVersion::V1_3 => MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: EncodedTerm::from_named_node(&graph.0),
                predicate: EncodedTerm::from_named_node(&vocab::schema_date_published()),
                object: EncodedTerm("\"2026-08-20\"".to_string()),
            },
            _ => unreachable!("unsupported version in test"),
        };
        node.apply_changes_unchecked(&graph, vec![change]).unwrap();

        let violations = node.graph_violations(&graph).unwrap();
        assert_eq!(violations.len(), 1, "unexpected violations for {version:?}");
        match version {
            RoCrateVersion::V1_1 => {
                assert_eq!(violations[0].code, "missing_required_property");
                assert_eq!(violations[0].entity_id.as_deref(), Some(root));
                assert!(violations[0].message.contains("schema:name"));
            }
            RoCrateVersion::V1_2 => {
                assert_eq!(violations[0].code, "entity_missing_type");
                assert_eq!(
                    violations[0].entity_id.as_deref(),
                    Some("urn:test:fixture-untyped")
                );
                assert!(violations[0].message.contains("missing rdf:type"));
            }
            RoCrateVersion::V1_3 => {
                assert_eq!(violations[0].code, "invalid_date_published_cardinality");
                assert!(violations[0].message.contains("found 2"));
            }
            _ => unreachable!("unsupported version in test"),
        }
    }
}

#[test]
fn import_version_rejection() {
    for route in ["default", "checked"] {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();

        let conflict_graph = GraphId::new(&format!("urn:test:rules-route-conflict:{route}"));
        let conflict = match route {
            "default" => node
                .apply_rocrate_document_with_policy(
                    &AllowAllAuthorizer,
                    conflict_graph,
                    include_str!("fixtures/rocrate/context-spec-mismatch.json"),
                    GraphPolicy::default(),
                )
                .unwrap_err(),
            "checked" => node
                .apply_rocrate_document_checked_with_policy(
                    &AllowAllAuthorizer,
                    conflict_graph,
                    include_str!("fixtures/rocrate/context-spec-mismatch.json"),
                    GraphPolicy::default(),
                )
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert!(matches!(
            conflict,
            CraqleError::RoCrate(RoCrateError::VersionMismatch { .. })
        ));

        let unknown_graph = GraphId::new(&format!("urn:test:rules-route-unknown:{route}"));
        let unknown = match route {
            "default" => node
                .apply_rocrate_document_with_policy(
                    &AllowAllAuthorizer,
                    unknown_graph,
                    include_str!("fixtures/rocrate/unknown-future-specification.json"),
                    GraphPolicy::default(),
                )
                .unwrap_err(),
            "checked" => node
                .apply_rocrate_document_checked_with_policy(
                    &AllowAllAuthorizer,
                    unknown_graph,
                    include_str!("fixtures/rocrate/unknown-future-specification.json"),
                    GraphPolicy::default(),
                )
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert!(matches!(
            unknown,
            CraqleError::RoCrate(RoCrateError::UnknownVersion(_))
        ));
    }
}
