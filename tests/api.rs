mod support;

use craqle::*;

use support::{CraqleCluster, QueryOptions};

fn writer_auth() -> GrantAuthorizer {
    GrantAuthorizer::new(vec![PermissionGrant::new(
        "/datasets/**",
        PermissionLevel::Write,
    )])
}

fn reader_auth() -> GrantAuthorizer {
    GrantAuthorizer::new(vec![PermissionGrant::new(
        "/datasets/public/**",
        PermissionLevel::Read,
    )])
}

#[test]
fn public_graphs_are_visible_without_grants() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();
    let graph = GraphId::new("urn:test:public");

    node.create_crate(
        &writer_auth(),
        CreateCrateRequest::new(
            graph.clone(),
            "Public Dataset",
            "Visible to everyone",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: true,
                permission_paths: vec!["/datasets/public/demo".to_string()],
            },
        ),
    )
    .unwrap();

    let anonymous = GrantAuthorizer::default();
    let rows = match node
        .query(&anonymous, "SELECT ?name WHERE { ?s schema:name ?name }")
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };

    assert!(!rows.is_empty());
    assert!(node.export_rocrate(&anonymous, &graph).is_ok());
}

#[test]
fn read_requires_matching_path_while_write_implies_read() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();
    let graph = GraphId::new("urn:test:private");
    let writer = writer_auth();

    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            graph.clone(),
            "Private Dataset",
            "Only path-matched users can see this",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: false,
                permission_paths: vec!["/datasets/private/project-a".to_string()],
            },
        ),
    )
    .unwrap();

    let no_access = GrantAuthorizer::new(vec![PermissionGrant::new(
        "/datasets/public/**",
        PermissionLevel::Read,
    )]);
    let rows = match node
        .query(&no_access, "SELECT ?name WHERE { ?s schema:name ?name }")
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };
    assert!(rows.is_empty());
    assert!(matches!(
        node.export_rocrate(&no_access, &graph),
        Err(CraqleError::Authorization(_))
    ));

    let writer_rows = match node
        .query(&writer, "SELECT ?name WHERE { ?s schema:name ?name }")
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };
    assert!(!writer_rows.is_empty());
}

#[test]
fn write_access_is_required_for_updates() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();
    let graph = GraphId::new("urn:test:update");
    let writer = writer_auth();
    let reader = reader_auth();

    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            graph.clone(),
            "Protected Dataset",
