use craqle::{
    AllowAllAuthorizer, CraqleError, CraqleNode, CraqleRequestDurability, CreateCrateOptions,
    CreateCrateRequest, DenyAllAuthorizer, GraphId, GraphPolicy, RoCrateError, RoCrateVersion,
    canonicalize_jsonld,
};
use serde_json::{Value, json};

fn policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/tests/rocrate-versions".to_string()],
    }
}

fn context_url(version: RoCrateVersion) -> &'static str {
    match version {
        RoCrateVersion::V1_1 => "https://w3id.org/ro/crate/1.1/context",
        RoCrateVersion::V1_2 => "https://w3id.org/ro/crate/1.2/context",
        RoCrateVersion::V1_3 => "https://w3id.org/ro/crate/1.3/context",
        _ => unreachable!("unsupported version in test"),
    }
}

fn specification_url(version: RoCrateVersion) -> &'static str {
    match version {
        RoCrateVersion::V1_1 => "https://w3id.org/ro/crate/1.1",
        RoCrateVersion::V1_2 => "https://w3id.org/ro/crate/1.2",
        RoCrateVersion::V1_3 => "https://w3id.org/ro/crate/1.3",
        _ => unreachable!("unsupported version in test"),
    }
}

fn request(graph: GraphId, license: Option<&str>) -> CreateCrateRequest {
    CreateCrateRequest::new(
        graph,
        "Versioned Dataset",
        "RO-Crate version coverage",
        "2026-08-19",
        license.map(str::to_string),
        policy(),
    )
}

fn exported(node: &CraqleNode, graph: &GraphId) -> Value {
    serde_json::from_str(&node.export_rocrate(&AllowAllAuthorizer, graph).unwrap()).unwrap()
}

fn metadata(document: &Value) -> &Value {
    document["@graph"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entity| entity["@id"] == "ro-crate-metadata.json")
        .unwrap()
}

fn crate_document(
    graph: &GraphId,
    context: Option<Value>,
    metadata_version: Option<&str>,
    root_version: Option<&str>,
) -> String {
    let mut descriptor = json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "about": {"@id": graph.as_str()}
    });
    if let Some(version) = metadata_version {
        descriptor["conformsTo"] = json!({"@id": version});
    }
    let mut root = json!({
        "@id": graph.as_str(),
        "@type": "Dataset",
        "name": "Version evidence crate",
        "description": "Tests context and specification evidence.",
        "datePublished": "2026-08-19"
    });
    if let Some(version) = root_version {
        root["conformsTo"] = json!({"@id": version});
    }
    let mut document = json!({"@graph": [descriptor, root]});
    if let Some(context) = context {
        document["@context"] = context;
    }
    document.to_string()
}

fn assert_unknown_version(error: CraqleError, expected: &str) {
    match error {
        CraqleError::RoCrate(RoCrateError::UnknownVersion(found)) => {
            assert_eq!(found, expected);
        }
        other => panic!("expected unknown RO-Crate version `{expected}`, got {other:?}"),
    }
}

fn assert_unknown_on_every_import_route(jsonld: &str, expected: &str) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    for route in ["default", "existing", "checked", "explicit-durability"] {
        let graph = GraphId::new(&format!("urn:test:version-error:{route}"));
        let error = match route {
            "default" => node
                .apply_rocrate_document_with_policy(&AllowAllAuthorizer, graph, jsonld, policy())
                .unwrap_err(),
            "existing" => {
                node.create_crate(&AllowAllAuthorizer, request(graph.clone(), None))
                    .unwrap();
                node.apply_rocrate_document(&AllowAllAuthorizer, graph, jsonld)
                    .unwrap_err()
            }
            "checked" => node
                .apply_rocrate_document_checked_with_policy(
                    &AllowAllAuthorizer,
                    graph,
                    jsonld,
                    policy(),
                )
                .unwrap_err(),
            "explicit-durability" => node
                .apply_rocrate_document_with_policy_and_durability(
                    &AllowAllAuthorizer,
                    graph,
                    jsonld,
                    policy(),
                    CraqleRequestDurability::Durable,
                )
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert_unknown_version(error, expected);
    }
}

#[test]
fn supported_versions_round_trip_canonical_rdf_and_context() {
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
        let source = CraqleNode::open(directory.path().join("source")).unwrap();
        let replica = CraqleNode::open(directory.path().join("replica")).unwrap();
        let graph = GraphId::new(root);

        source
            .apply_rocrate_document_checked_with_policy(
                &AllowAllAuthorizer,
                graph.clone(),
                fixture,
                policy(),
            )
            .unwrap();
        assert_eq!(source.crate_version(&graph).unwrap(), version);

        let first = source.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
        let first_value: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(
            first_value["@context"],
            json!(context_url(version)),
            "export must retain the stored bare version context"
        );
        assert_eq!(
            canonicalize_jsonld(fixture).unwrap().nquads,
            canonicalize_jsonld(&first).unwrap().nquads,
            "import-export must preserve canonical RDF for {version:?}"
        );

        replica
            .apply_rocrate_document_checked_with_policy(
                &AllowAllAuthorizer,
                graph.clone(),
                &first,
                policy(),
            )
            .unwrap();
        assert_eq!(replica.crate_version(&graph).unwrap(), version);
        let second = replica.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
        assert_eq!(
            canonicalize_jsonld(&first).unwrap().nquads,
            canonicalize_jsonld(&second).unwrap().nquads
        );
    }
}

#[test]
fn creation_defaults_to_1_3_and_options_select_version_and_license() {
    assert_eq!(RoCrateVersion::default(), RoCrateVersion::V1_3);
    assert_eq!(
        context_url(RoCrateVersion::V1_3),
        "https://w3id.org/ro/crate/1.3/context"
    );
    assert_eq!(
        specification_url(RoCrateVersion::V1_3),
        "https://w3id.org/ro/crate/1.3"
    );
    assert_eq!(CreateCrateOptions::default().version, RoCrateVersion::V1_3);

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    let default_graph = GraphId::new("urn:test:version-create-default");
    node.create_crate(&AllowAllAuthorizer, request(default_graph.clone(), None))
        .unwrap();
    assert_eq!(
        node.crate_version(&default_graph).unwrap(),
        RoCrateVersion::V1_3
    );
    assert_eq!(
        exported(&node, &default_graph)["@context"],
        json!(context_url(RoCrateVersion::V1_3))
    );

    let retained_license_graph = GraphId::new("urn:test:version-create-1-1");
    node.create_crate_with_options(
        &AllowAllAuthorizer,
        request(
            retained_license_graph.clone(),
            Some("https://example.org/licenses/request"),
        ),
        CreateCrateOptions {
            version: RoCrateVersion::V1_1,
            license: None,
        },
    )
    .unwrap();
    assert_eq!(
        node.crate_version(&retained_license_graph).unwrap(),
        RoCrateVersion::V1_1
    );
    let retained = canonicalize_jsonld(
        &node
            .export_rocrate(&AllowAllAuthorizer, &retained_license_graph)
            .unwrap(),
    )
    .unwrap()
    .nquads;
    assert!(retained.contains("<https://example.org/licenses/request>"));

    let override_license_graph = GraphId::new("urn:test:version-create-1-2");
    node.create_crate_with_options(
        &AllowAllAuthorizer,
        request(
            override_license_graph.clone(),
            Some("https://example.org/licenses/request"),
        ),
        CreateCrateOptions {
            version: RoCrateVersion::V1_2,
            license: Some("https://example.org/licenses/override".to_string()),
        },
    )
    .unwrap();
    assert_eq!(
        node.crate_version(&override_license_graph).unwrap(),
        RoCrateVersion::V1_2
    );
    let overridden = canonicalize_jsonld(
        &node
            .export_rocrate(&AllowAllAuthorizer, &override_license_graph)
            .unwrap(),
    )
    .unwrap()
    .nquads;
    assert!(overridden.contains("<https://example.org/licenses/override>"));
    assert!(!overridden.contains("<https://example.org/licenses/request>"));

    let denied_graph = GraphId::new("urn:test:version-create-denied");
    assert!(matches!(
        node.create_crate_with_options(
            &DenyAllAuthorizer,
            request(denied_graph, None),
            CreateCrateOptions::default(),
        ),
        Err(CraqleError::Authorization(_))
    ));
}

#[test]
fn v1_3_contexts_and_bioschemas_terms_are_offline_and_faithful() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();

    let bioschemas = GraphId::new("urn:test:version-bioschemas");
    node.apply_rocrate_document_checked_with_policy(
        &AllowAllAuthorizer,
        bioschemas.clone(),
        include_str!("fixtures/rocrate/bioschemas-1.3.json"),
        policy(),
    )
    .unwrap();
    let canonical = canonicalize_jsonld(
        &node
            .export_rocrate(&AllowAllAuthorizer, &bioschemas)
            .unwrap(),
    )
    .unwrap()
    .nquads;
    for term in [
        "https://bioschemas.org/terms/ComputationalWorkflow",
        "https://bioschemas.org/terms/FormalParameter",
        "https://bioschemas.org/terms/input",
        "https://bioschemas.org/terms/output",
    ] {
        assert!(canonical.contains(term), "1.3 context must expand `{term}`");
    }

    for (graph, fixture) in [
        (
            GraphId::new("urn:test:version-custom-object"),
            include_str!("fixtures/rocrate/custom-context-1.3.json"),
        ),
        (
            GraphId::new("urn:test:version-custom-array"),
            include_str!("fixtures/rocrate/context-array-1.3.json"),
        ),
    ] {
        let submitted: Value = serde_json::from_str(fixture).unwrap();
        node.apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            graph.clone(),
            fixture,
            policy(),
        )
        .unwrap();
        assert_eq!(exported(&node, &graph)["@context"], submitted["@context"]);
    }

    let mapped_iri = GraphId::new("urn:test:version-custom-mapped-iri");
    let mapped_document = json!({
        "@context": {
            "@vocab": "http://schema.org/",
            "conformsTo": "http://purl.org/dc/terms/conformsTo",
            "looksLikeContext": "https://w3id.org/ro/crate/9.9/context"
        },
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
                "about": {"@id": mapped_iri.as_str()}
            },
            {
                "@id": mapped_iri.as_str(),
                "@type": "Dataset",
                "name": "Mapped IRI",
                "description": "An ordinary context mapping is not version evidence.",
                "datePublished": "2026-08-19",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3/profile"}
            }
        ]
    })
    .to_string();
    node.apply_rocrate_document_checked_with_policy(
        &AllowAllAuthorizer,
        mapped_iri.clone(),
        &mapped_document,
        policy(),
    )
    .unwrap();
    assert_eq!(
        node.crate_version(&mapped_iri).unwrap(),
        RoCrateVersion::V1_3
    );
}

#[test]
fn context_only_and_specification_only_versions_survive_round_trips() {
    let directory = tempfile::tempdir().unwrap();
    let source = CraqleNode::open(directory.path().join("source")).unwrap();
    let replica = CraqleNode::open(directory.path().join("replica")).unwrap();

    let context_only_graph = GraphId::new("urn:test:version-context-only");
    let context_only = crate_document(
        &context_only_graph,
        Some(json!(context_url(RoCrateVersion::V1_2))),
        None,
        None,
    );
    source
        .apply_rocrate_document_with_policy(
            &AllowAllAuthorizer,
            context_only_graph.clone(),
            &context_only,
            policy(),
        )
        .unwrap();
    assert_eq!(
        source.crate_version(&context_only_graph).unwrap(),
        RoCrateVersion::V1_2
    );
    let context_only_export = source
        .export_rocrate(&AllowAllAuthorizer, &context_only_graph)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&context_only_export).unwrap()["@context"],
        json!(context_url(RoCrateVersion::V1_2))
    );
    assert_eq!(
        canonicalize_jsonld(&context_only).unwrap().nquads,
        canonicalize_jsonld(&context_only_export).unwrap().nquads,
        "retaining context-only evidence must not synthesize RDF"
    );
    assert!(
        metadata(&serde_json::from_str(&context_only_export).unwrap())
            .get("conformsTo")
            .is_none()
    );
    replica
        .apply_rocrate_document_with_policy(
            &AllowAllAuthorizer,
            context_only_graph.clone(),
            &context_only_export,
            policy(),
        )
        .unwrap();
    assert_eq!(
        replica.crate_version(&context_only_graph).unwrap(),
        RoCrateVersion::V1_2
    );
    assert_eq!(
        canonicalize_jsonld(&context_only_export).unwrap().nquads,
        canonicalize_jsonld(
            &replica
                .export_rocrate(&AllowAllAuthorizer, &context_only_graph)
                .unwrap()
        )
        .unwrap()
        .nquads
    );

    let specification_only_graph = GraphId::new("urn:test:version-specification-only");
    let specification_only_context = json!({
        "@vocab": "http://schema.org/",
        "conformsTo": "http://purl.org/dc/terms/conformsTo"
    });
    let specification_only = crate_document(
        &specification_only_graph,
        Some(specification_only_context.clone()),
        None,
        Some(specification_url(RoCrateVersion::V1_1)),
    );
    source
        .apply_rocrate_document_with_policy(
            &AllowAllAuthorizer,
            specification_only_graph.clone(),
            &specification_only,
            policy(),
        )
        .unwrap();
    assert_eq!(
        source.crate_version(&specification_only_graph).unwrap(),
        RoCrateVersion::V1_1
    );
    let specification_only_export = source
        .export_rocrate(&AllowAllAuthorizer, &specification_only_graph)
        .unwrap();
    assert_eq!(
        canonicalize_jsonld(&specification_only).unwrap().nquads,
        canonicalize_jsonld(&specification_only_export)
            .unwrap()
            .nquads,
        "retaining specification-only evidence must not synthesize RDF"
    );
    let specification_only_value: Value = serde_json::from_str(&specification_only_export).unwrap();
    assert_eq!(
        specification_only_value["@context"],
        specification_only_context
    );
    assert!(
        metadata(&specification_only_value)
            .get("conformsTo")
            .is_none()
    );
    replica
        .apply_rocrate_document_with_policy(
            &AllowAllAuthorizer,
            specification_only_graph.clone(),
            &specification_only_export,
            policy(),
        )
        .unwrap();
    assert_eq!(
        replica.crate_version(&specification_only_graph).unwrap(),
        RoCrateVersion::V1_1
    );
    assert_eq!(
        canonicalize_jsonld(&specification_only_export)
            .unwrap()
            .nquads,
        canonicalize_jsonld(
            &replica
                .export_rocrate(&AllowAllAuthorizer, &specification_only_graph)
                .unwrap()
        )
        .unwrap()
        .nquads
    );
}

#[test]
fn entity_edits_keep_the_stored_version_marker() {
    for (version, fixture) in [
        (
            RoCrateVersion::V1_1,
            include_str!("fixtures/rocrate/valid-1.1.json"),
        ),
        (
            RoCrateVersion::V1_2,
            include_str!("fixtures/rocrate/valid-1.2.json"),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let node = CraqleNode::open(directory.path()).unwrap();
        let graph = GraphId::new(&format!("urn:test:version-edit:{version:?}"));
        node.apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            graph.clone(),
            fixture,
            policy(),
        )
        .unwrap();
        node.add_data_entity_with_triples(
            &AllowAllAuthorizer,
            &graph,
            "./ordinary-edit.txt",
            "http://schema.org/MediaObject",
            "Ordinary edit",
            Vec::new(),
        )
        .unwrap();
        assert_eq!(node.crate_version(&graph).unwrap(), version);
        assert_eq!(
            exported(&node, &graph)["@context"],
            json!(context_url(version))
        );
    }
}

#[test]
fn version_errors_are_shared_by_every_import_route() {
    assert_unknown_on_every_import_route(
        include_str!("fixtures/rocrate/unknown-future-context.json"),
        "https://w3id.org/ro/crate/9.9/context",
    );
    assert_unknown_on_every_import_route(
        include_str!("fixtures/rocrate/unknown-future-specification.json"),
        "https://w3id.org/ro/crate/9.9",
    );

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:version-mismatch");
    assert!(matches!(
        node.apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            graph,
            include_str!("fixtures/rocrate/context-spec-mismatch.json"),
            policy(),
        ),
        Err(CraqleError::RoCrate(RoCrateError::VersionMismatch { .. }))
    ));
}
