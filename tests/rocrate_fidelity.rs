mod support;

use craqle::{
    CanonicalJsonLd, CraqleError, CraqleNode, CreateCrateRequest, GraphId, RoCrateError,
    UpdateError, canonicalize_jsonld,
};
use proptest::prelude::*;
use serde_json::{Value, json};
use support::{public_policy, writer_auth};

fn crate_document(context: Value, version: Option<&str>, license: Option<Value>) -> String {
    let mut metadata = json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "about": {"@id": "./"}
    });
    if let Some(version) = version {
        metadata["conformsTo"] = json!({"@id": version});
    }

    let mut root = json!({
        "@id": "./",
        "@type": "Dataset",
        "name": "Fidelity crate",
        "description": "JSON-LD projection fidelity",
        "datePublished": "2026-07-23"
    });
    if let Some(license) = license {
        root["license"] = license;
    }

    json!({
        "@context": context,
        "@graph": [metadata, root]
    })
    .to_string()
}

fn checked_document(jsonld: &str) {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    node.validate_rocrate_document_checked_with_policy(
        &writer_auth(),
        GraphId::new("urn:test:fidelity"),
        jsonld,
        public_policy(),
    )
    .unwrap();
}

fn roundtrip_document(jsonld: &str) -> String {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:fidelity-roundtrip");
    node.apply_rocrate_document_checked_with_policy(
        &writer_auth(),
        graph.clone(),
        jsonld,
        public_policy(),
    )
    .unwrap();
    let exported = node.export_rocrate(&writer_auth(), &graph).unwrap();
    canonicalize_jsonld(&exported).unwrap().nquads
}

#[test]
fn accepts_both_contexts() {
    for version in ["1.1", "1.2"] {
        let context = format!("https://w3id.org/ro/crate/{version}/context");
        let specification = format!("https://w3id.org/ro/crate/{version}");
        checked_document(&crate_document(json!(context), Some(&specification), None));
    }

    let mut aliased: Value = serde_json::from_str(&crate_document(
        json!([
            "https://w3id.org/ro/crate/1.2/context",
            {"title": "http://schema.org/name"}
        ]),
        Some("https://w3id.org/ro/crate/1.2"),
        None,
    ))
    .unwrap();
    let root = aliased["@graph"][1].as_object_mut().unwrap();
    root.remove("name");
    root.insert("title".to_string(), json!("Aliased title"));
    checked_document(&aliased.to_string());

    let mut root_version: Value = serde_json::from_str(&crate_document(
        json!("https://w3id.org/ro/crate/1.2/context"),
        None,
        None,
    ))
    .unwrap();
    root_version["@graph"][1]["conformsTo"] = json!([{"@id": "https://w3id.org/ro/crate/1.1"}]);
    checked_document(&root_version.to_string());
}

#[test]
fn license_shapes_project() {
    let context = json!([
        "https://w3id.org/ro/crate/1.2/context",
        {
            "cc": "https://creativecommons.org/licenses/",
            "ex": "https://example.org/vocab/"
        }
    ]);
    let fixtures = [
        (None, Vec::<&str>::new()),
        (
            Some(json!("MIT")),
            vec!["<http://schema.org/license> \"MIT\""],
        ),
        (
            Some(json!({"@id": "https://spdx.org/licenses/Apache-2.0"})),
            vec!["<http://schema.org/license> <https://spdx.org/licenses/Apache-2.0>"],
        ),
        (
            Some(json!({"@id": "cc:by"})),
            vec!["<http://schema.org/license> <https://creativecommons.org/licenses/by>"],
        ),
        (
            Some(json!({
                "@type": "CreativeWork",
                "name": "Custom license",
                "ex:code": "custom-α"
            })),
            vec![
                "<http://schema.org/license> _:b",
                "<https://example.org/vocab/code> \"custom-α\"",
            ],
        ),
        (
            Some(json!([
                "Unicode 许可",
                {"@id": "https://example.org/licenses/custom"},
                {"@value": "frei", "@language": "de"},
                {
                    "@value": "L-42",
                    "@type": "https://example.org/vocab/licenseCode"
                }
            ])),
            vec![
                "<http://schema.org/license> \"Unicode 许可\"",
                "<http://schema.org/license> <https://example.org/licenses/custom>",
                "\"frei\"@de",
                "\"L-42\"^^<https://example.org/vocab/licenseCode>",
            ],
        ),
    ];

    for (license, expected) in fixtures {
        let document = crate_document(
            context.clone(),
            Some("https://w3id.org/ro/crate/1.2"),
            license,
        );
        checked_document(&document);
        let CanonicalJsonLd { nquads, .. } = canonicalize_jsonld(&document).unwrap();
        let projected = roundtrip_document(&document);
        if expected.is_empty() {
            assert!(!projected.contains("<http://schema.org/license>"));
        }
        for expected in expected {
            assert!(
                nquads.contains(expected),
                "missing `{expected}` in:\n{nquads}"
            );
            assert!(
                projected.contains(expected),
                "round trip lost `{expected}` in:\n{projected}"
            );
        }
    }
}

#[test]
fn violations_have_pointers() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let document = crate_document(
        json!("https://w3id.org/ro/crate/1.2/context"),
        Some("https://w3id.org/ro/crate/9.9"),
        None,
    );
    let error = node
        .validate_rocrate_document_checked_with_policy(
            &writer_auth(),
            GraphId::new("urn:test:version"),
            &document,
            public_policy(),
        )
        .unwrap_err();

    let CraqleError::RoCrate(RoCrateError::Update(UpdateError::ValidationFailed(violations))) =
        error
    else {
        panic!("expected structured validation error, got {error:?}");
    };
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].code, "unsupported_crate_version");
    assert_eq!(violations[0].pointer, "/@graph/0/conformsTo");
    assert_eq!(
        violations[0].entity_id.as_deref(),
        Some("ro-crate-metadata.json")
    );
}

#[test]
fn unknown_context_fails() {
    let document = crate_document(
        json!([
            "https://w3id.org/ro/crate/1.2/context",
            "https://example.invalid/essential-context"
        ]),
        Some("https://w3id.org/ro/crate/1.2"),
        None,
    );
    assert!(matches!(
        canonicalize_jsonld(&document),
        Err(RoCrateError::JsonLd(_))
    ));

    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    assert!(matches!(
        node.validate_rocrate_document_checked_with_policy(
            &writer_auth(),
            GraphId::new("urn:test:context"),
            &document,
            public_policy(),
        ),
        Err(CraqleError::RoCrate(RoCrateError::JsonLd(_)))
    ));
}

#[test]
fn scaffold_license_optional() {
    let directory = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(directory.path()).unwrap();
    let graph = GraphId::new("urn:test:no-license");
    node.create_crate(
        &writer_auth(),
        CreateCrateRequest::new(
            graph.clone(),
            "No license",
            "License is optional metadata",
            "2026-07-23",
            None,
            public_policy(),
        ),
    )
    .unwrap();

    assert!(node.graph_violations(&graph).unwrap().is_empty());
    let exported: Value =
        serde_json::from_str(&node.export_rocrate(&writer_auth(), &graph).unwrap()).unwrap();
    let root = exported["@graph"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entity| entity["@id"] == graph.as_str())
        .unwrap();
    assert!(root.get("license").is_none());
}

proptest! {
    #[test]
    fn digest_is_stable(value in ".{0,64}") {
        let document = crate_document(
            json!("https://w3id.org/ro/crate/1.2/context"),
            Some("https://w3id.org/ro/crate/1.2"),
            Some(json!({
                "@type": "CreativeWork",
                "name": value
            })),
        );
        let pretty = serde_json::to_string_pretty(
            &serde_json::from_str::<Value>(&document).unwrap()
        ).unwrap();
        let first = canonicalize_jsonld(&document).unwrap();
        let second = canonicalize_jsonld(&pretty).unwrap();
        prop_assert_eq!(first, second);
    }
}
