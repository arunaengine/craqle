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

    for route in [
        "default",
        "existing",
        "checked",
        "prevalidated",
        "bootstrap",
    ] {
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
            "prevalidated" => node
                .apply_rocrate_document_prevalidated_with_policy_and_durability_as(
                    &AllowAllAuthorizer,
                    graph,
                    jsonld,
                    policy(),
                    CraqleRequestDurability::Durable,
                    None,
                )
                .unwrap_err(),
            "bootstrap" => node
                .bootstrap_rocrate_document(&AllowAllAuthorizer, graph, jsonld, policy())
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
