#![cfg(feature = "shacl-core")]

use craqle::{
    AllowAllAuthorizer, CraqleError, CraqleErrorKind, CraqleNode, DenyAllAuthorizer, EncodedTerm,
    GraphId, GraphPolicy, MaterializedQuadChange, PrepareRoCrateOptions, PreparedCommitMode,
    PreparedGraphBase, RoCratePolicyOptions, RoCrateVersion, ShaclCompileOptions, ShaclError,
};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SH: &str = "http://www.w3.org/ns/shacl#";
const SCHEMA: &str = "http://schema.org/";

fn iri(value: &str) -> EncodedTerm {
    EncodedTerm(format!("<{value}>"))
}

fn add(
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

fn insert(node: &CraqleNode, graph: &GraphId, changes: Vec<MaterializedQuadChange>) {
    node.apply_changes_unchecked(graph, changes).unwrap();
}

fn install_identifier_shape(node: &CraqleNode, graph: &GraphId) {
    let shape = iri("urn:test:rocrate-policy:dataset-shape");
    let property = EncodedTerm("_:identifier-property".to_owned());
    insert(
        node,
        graph,
        vec![
            add(
                graph,
                shape.clone(),
                RDF_TYPE,
                iri(&format!("{SH}NodeShape")),
            ),
            add(
                graph,
                shape.clone(),
                &format!("{SH}targetClass"),
                iri(&format!("{SCHEMA}Dataset")),
            ),
            add(graph, shape, &format!("{SH}property"), property.clone()),
            add(
                graph,
                property.clone(),
                &format!("{SH}path"),
                iri(&format!("{SCHEMA}identifier")),
            ),
            add(
                graph,
                property,
                &format!("{SH}minCount"),
                EncodedTerm("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned()),
            ),
        ],
    );
}

fn context(version: RoCrateVersion) -> (&'static str, &'static str) {
    match version {
        RoCrateVersion::V1_1 => (
            "https://w3id.org/ro/crate/1.1/context",
            "https://w3id.org/ro/crate/1.1",
        ),
        RoCrateVersion::V1_2 => (
            "https://w3id.org/ro/crate/1.2/context",
            "https://w3id.org/ro/crate/1.2",
        ),
        RoCrateVersion::V1_3 => (
            "https://w3id.org/ro/crate/1.3/context",
            "https://w3id.org/ro/crate/1.3",
        ),
        _ => unreachable!("test covers every supported RO-Crate version"),
    }
}

fn document(
    graph: &GraphId,
    version: RoCrateVersion,
    identifier: bool,
    name: &str,
    extras: Vec<serde_json::Value>,
) -> String {
    let (context, specification) = context(version);
    let mut root = serde_json::json!({
        "@id": graph.as_str(),
        "@type": "Dataset",
        "name": name,
        "description": "Raw policy candidate",
        "datePublished": "2026-08-21",
        "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
    });
    if identifier {
        root.as_object_mut()
            .unwrap()
            .insert("identifier".to_owned(), serde_json::json!("dataset-1"));
    }
    let mut graph_entries = vec![
        serde_json::json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": specification},
            "about": {"@id": graph.as_str()}
        }),
        root,
    ];
    graph_entries.extend(extras);
    serde_json::json!({"@context": context, "@graph": graph_entries}).to_string()
}

fn compile_policy(
    node: &CraqleNode,
    shapes: &GraphId,
    version: RoCrateVersion,
    allow_local_imports: bool,
) -> craqle::CompiledRoCratePolicy {
    node.compile_rocrate_policy(
        &AllowAllAuthorizer,
        shapes,
        &ShaclCompileOptions {
            rocrate_version: version,
            allow_local_imports,
        },
    )
    .unwrap()
}

#[test]
fn valid_new_document_is_prepared_evaluated_and_committed_after_one_parse() {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(database.path()).unwrap();
    let data = GraphId::new("urn:test:rocrate-policy:new");
    let shapes = GraphId::new("urn:test:rocrate-policy:shapes");
    install_identifier_shape(&node, &shapes);
    let policy = compile_policy(&node, &shapes, RoCrateVersion::V1_2, false);
    let jsonld = document(&data, RoCrateVersion::V1_2, true, "Dataset", vec![]);
    let prepared = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &jsonld,
            &PrepareRoCrateOptions {
                new_graph_policy: GraphPolicy {
                    public: true,
                    permission_paths: vec![],
                },
                ..PrepareRoCrateOptions::default()
            },
        )
        .unwrap();

    assert_eq!(prepared.base, PreparedGraphBase::New);
    assert_eq!(prepared.statistics.parse_count, 1);
    assert!(!node.contains_graph(&data).unwrap());
    let report = node
        .evaluate_rocrate_policy(
            &AllowAllAuthorizer,
            &prepared,
            &policy,
            &RoCratePolicyOptions::default(),
        )
        .unwrap();
    assert!(report.conforms);
    assert_eq!(report.statistics.parse_count, 1);
    assert!(report.rocrate_violations.is_empty());
    assert!(!node.contains_graph(&data).unwrap());

    let outcome = node
        .commit_prepared_rocrate_document(
            &AllowAllAuthorizer,
            prepared,
            Some(&policy),
            PreparedCommitMode::Enforce,
        )
        .unwrap();
    assert!(!outcome.batch.ops.is_empty());
    assert!(outcome.policy_report.unwrap().conforms);
    assert!(node.contains_graph(&data).unwrap());
}

#[test]
fn enforce_rejects_invalid_shacl_without_side_effects_and_advisory_commits() {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(database.path()).unwrap();
    let data = GraphId::new("urn:test:rocrate-policy:advisory");
    let shapes = GraphId::new("urn:test:rocrate-policy:advisory-shapes");
    install_identifier_shape(&node, &shapes);
    let policy = compile_policy(&node, &shapes, RoCrateVersion::V1_2, false);
    let prepared = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &document(&data, RoCrateVersion::V1_2, false, "Dataset", vec![]),
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    assert!(prepared.structural_findings().is_empty());

    let error = node
        .commit_prepared_rocrate_document(
            &AllowAllAuthorizer,
            prepared.clone(),
            Some(&policy),
            PreparedCommitMode::Enforce,
        )
        .unwrap_err();
    assert!(matches!(error, CraqleError::RoCratePolicyRejected(_)));
    assert!(!node.contains_graph(&data).unwrap());

    let advisory = node
        .commit_prepared_rocrate_document(
            &AllowAllAuthorizer,
            prepared,
            Some(&policy),
            PreparedCommitMode::Advisory,
        )
        .unwrap();
    assert!(!advisory.policy_report.unwrap().conforms);
    assert!(node.contains_graph(&data).unwrap());
}

#[test]
fn existing_unchanged_replacement_and_stale_data_base_are_fenced() {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(database.path()).unwrap();
    let data = GraphId::new("urn:test:rocrate-policy:existing");
    let original = document(&data, RoCrateVersion::V1_2, true, "Original", vec![]);
    let prepared = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &original,
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    node.commit_prepared_rocrate_document(
        &AllowAllAuthorizer,
        prepared,
        None,
        PreparedCommitMode::StructuralOnly,
    )
    .unwrap();

    let unchanged = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &original,
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    assert!(matches!(unchanged.base, PreparedGraphBase::Existing { .. }));
    assert_eq!(unchanged.change_count(), 0);
    node.commit_prepared_rocrate_document(
        &AllowAllAuthorizer,
        unchanged,
        None,
        PreparedCommitMode::StructuralOnly,
    )
    .unwrap();

    let replacement = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &document(&data, RoCrateVersion::V1_2, true, "Replacement", vec![]),
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    node.commit_prepared_rocrate_document(
        &AllowAllAuthorizer,
        replacement,
        None,
        PreparedCommitMode::StructuralOnly,
    )
    .unwrap();
    assert!(
        node.export_rocrate(&AllowAllAuthorizer, &data)
            .unwrap()
            .contains("Replacement")
    );

    let stale = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &document(&data, RoCrateVersion::V1_2, true, "Stale", vec![]),
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    insert(
        &node,
        &data,
        vec![add(
            &data,
            iri(data.as_str()),
            "urn:test:rocrate-policy:changed",
            EncodedTerm("\"after preparation\"".to_owned()),
        )],
    );
    let error = node
        .commit_prepared_rocrate_document(
            &AllowAllAuthorizer,
            stale,
            None,
            PreparedCommitMode::StructuralOnly,
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::StalePreparedState);
    assert!(
        !node
            .export_rocrate(&AllowAllAuthorizer, &data)
            .unwrap()
            .contains("Stale")
    );
}

#[test]
fn root_and_imported_shape_changes_invalidate_compiled_policy() {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(database.path()).unwrap();
    let data = GraphId::new("urn:test:rocrate-policy:shape-fence");
    let root = GraphId::new("urn:test:rocrate-policy:root-shapes");
    let imported = GraphId::new("urn:test:rocrate-policy:imported-shapes");
    install_identifier_shape(&node, &imported);
    insert(
        &node,
        &root,
        vec![add(
            &root,
            iri("urn:test:rocrate-policy:ontology"),
            OWL_IMPORTS,
            iri(imported.as_str()),
        )],
    );
    let prepared = node
        .prepare_rocrate_document(
            &AllowAllAuthorizer,
            &data,
            &document(&data, RoCrateVersion::V1_2, true, "Dataset", vec![]),
            &PrepareRoCrateOptions::default(),
        )
        .unwrap();
    let imported_policy = compile_policy(&node, &root, RoCrateVersion::V1_2, true);
    node.evaluate_rocrate_policy(
        &AllowAllAuthorizer,
        &prepared,
        &imported_policy,
        &RoCratePolicyOptions::default(),
    )
    .unwrap();
    insert(
        &node,
        &imported,
        vec![add(
            &imported,
            iri("urn:test:rocrate-policy:second-shape"),
            RDF_TYPE,
            iri(&format!("{SH}NodeShape")),
        )],
    );
    let error = node
        .commit_prepared_rocrate_document(
            &AllowAllAuthorizer,
            prepared.clone(),
            Some(&imported_policy),
            PreparedCommitMode::Enforce,
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::StalePreparedState);
    assert!(error.to_string().contains(imported.as_str()));
    assert!(!node.contains_graph(&data).unwrap());

    let root_policy = compile_policy(&node, &root, RoCrateVersion::V1_2, true);
    insert(
        &node,
        &root,
        vec![add(
            &root,
            iri("urn:test:rocrate-policy:ontology"),
            "urn:test:rocrate-policy:changed",
            EncodedTerm("\"changed\"".to_owned()),
        )],
    );
    let error = node
        .evaluate_rocrate_policy(
            &AllowAllAuthorizer,
            &prepared,
            &root_policy,
            &RoCratePolicyOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::StalePreparedState);
    assert!(error.to_string().contains(root.as_str()));
}

#[test]
fn versions_jsonld_terms_authorization_and_unsupported_shapes_fail_or_report_exactly() {
    let database = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(database.path()).unwrap();
    let shapes = GraphId::new("urn:test:rocrate-policy:versions-shapes");
    install_identifier_shape(&node, &shapes);

    for version in [
        RoCrateVersion::V1_1,
        RoCrateVersion::V1_2,
        RoCrateVersion::V1_3,
    ] {
        let data = GraphId::new(&format!("urn:test:rocrate-policy:{version:?}"));
        let extras = (version == RoCrateVersion::V1_3)
            .then(|| {
                vec![
                    serde_json::json!({
                        "@id": "_:contact",
                        "@type": "Person",
                        "name": {"@value": "Kontakt", "@language": "de"}
                    }),
                    serde_json::json!({
                        "@id": "_:contact",
                        "@type": "Person",
                        "identifier": "contact-1"
                    }),
                ]
            })
            .unwrap_or_default();
        let prepared = node
            .prepare_rocrate_document(
                &AllowAllAuthorizer,
                &data,
                &document(&data, version, true, "Dataset", extras),
                &PrepareRoCrateOptions::default(),
            )
            .unwrap();
        assert_eq!(prepared.detected_version, version);
        let policy = compile_policy(&node, &shapes, version, false);
        assert!(
            node.evaluate_rocrate_policy(
                &AllowAllAuthorizer,
                &prepared,
                &policy,
                &RoCratePolicyOptions::default(),
            )
            .unwrap()
            .conforms
        );
    }

    let denied = GraphId::new("urn:test:rocrate-policy:denied");
    assert!(
        node.prepare_rocrate_document(
            &DenyAllAuthorizer,
            &denied,
            &document(&denied, RoCrateVersion::V1_2, true, "Dataset", vec![]),
            &PrepareRoCrateOptions::default(),
        )
        .is_err()
    );
    assert!(
        node.compile_rocrate_policy(&DenyAllAuthorizer, &shapes, &ShaclCompileOptions::default(),)
            .is_err()
    );
    assert!(
        node.prepare_rocrate_document(
            &AllowAllAuthorizer,
            &denied,
            "not json",
            &PrepareRoCrateOptions::default(),
        )
        .is_err()
    );

    let unsupported = GraphId::new("urn:test:rocrate-policy:unsupported-shapes");
    insert(
        &node,
        &unsupported,
        vec![add(
            &unsupported,
            iri("urn:test:rocrate-policy:component"),
            RDF_TYPE,
            iri(&format!("{SH}ConstraintComponent")),
        )],
    );
    let error = node
        .compile_rocrate_policy(
            &AllowAllAuthorizer,
            &unsupported,
            &ShaclCompileOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        CraqleError::Shacl(ShaclError::UnsupportedComponent { .. })
    ));
}
