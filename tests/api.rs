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
            "Only writers may mutate this crate",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: false,
                permission_paths: vec!["/datasets/public/demo".to_string()],
            },
        ),
    )
    .unwrap();

    let err = node
        .apply_sparql_update(
            &reader,
            "INSERT DATA { GRAPH <urn:test:update> { <urn:test:item> schema:name \"forbidden\" } }",
        )
        .unwrap_err();
    assert!(matches!(err, CraqleError::Authorization(_)));

    node.apply_sparql_update(
        &writer,
        "INSERT { GRAPH <urn:test:update> { ?root schema:hasPart <urn:test:item> . <urn:test:item> rdf:type schema:MediaObject . <urn:test:item> schema:name \"allowed\" } } WHERE { GRAPH <urn:test:update> { ?root rdf:type schema:Dataset . ?root schema:datePublished ?date . } }",
    )
    .unwrap();
}

#[test]
fn explicit_batch_and_snapshot_sync_replicate_graphs_and_policy() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let node_a = CraqleNode::open(dir_a.path()).unwrap();
    let node_b = CraqleNode::open(dir_b.path()).unwrap();
    let graph = GraphId::new("urn:test:sync");
    let auth = writer_auth();

    node_a
        .create_crate(
            &auth,
            CreateCrateRequest::new(
                graph.clone(),
                "Synced Dataset",
                "Replicated through sync messages",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: false,
                    permission_paths: vec!["/datasets/private/project-a".to_string()],
                },
            ),
        )
        .unwrap();

    let policy = node_a.graph_policy(&graph).unwrap();
    let snapshot = node_a.graph_snapshot(&graph).unwrap();
    node_b.import_graph_snapshot(&snapshot, policy).unwrap();

    node_a
        .add_data_entity(
            &auth,
            &graph,
            "data/synced.txt",
            "http://schema.org/MediaObject",
            "Synced File",
        )
        .unwrap();
    let batches = node_a
        .catchup_batches(&graph, &node_b.vector_clock(&graph).unwrap())
        .unwrap();
    node_b.apply_remote_batches(batches).unwrap();
    node_b
        .import_graph_policy(&graph, node_a.graph_policy(&graph).unwrap())
        .unwrap();

    assert_eq!(
        node_b.graph_policy(&graph).unwrap().permission_paths,
        vec!["/datasets/private/project-a".to_string()]
    );
    let rows = match node_b
        .query(
            &auth,
            "SELECT ?name WHERE { GRAPH <urn:test:sync> { ?s schema:name ?name } }",
        )
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .any(|row| row.values().any(|value| value.0.contains("Synced File")))
    );
}

#[test]
fn search_filters_private_graphs_by_policy() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();
    let writer = writer_auth();

    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            GraphId::new("urn:test:public-search"),
            "Public Proteomics",
            "Visible search document",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: true,
                permission_paths: vec!["/datasets/public/demo".to_string()],
            },
        ),
    )
    .unwrap();
    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            GraphId::new("urn:test:private-search"),
            "Private Proteomics",
            "Hidden search document",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: false,
                permission_paths: vec!["/datasets/private/project-a".to_string()],
            },
        ),
    )
    .unwrap();

    let anonymous_hits = node
        .search(&GrantAuthorizer::default(), "proteomics", 10)
        .unwrap();
    assert_eq!(anonymous_hits.len(), 1);
    assert_eq!(anonymous_hits[0].subject_iri, "./");

    let writer_hits = node.search(&writer, "proteomics", 10).unwrap();
    assert_eq!(writer_hits.len(), 2);
}

#[test]
fn search_hits_can_be_hydrated_from_rdf() {
    let dir = tempfile::tempdir().unwrap();
    let node = CraqleNode::open(dir.path()).unwrap();
    let writer = writer_auth();
    let reader = GrantAuthorizer::default();
    let graph = GraphId::new("urn:test:hydrate-search");

    node.create_crate(
        &writer,
        CreateCrateRequest::new(
            graph.clone(),
            "Hydrated Search Dataset",
            "Search results can be hydrated from RDF",
            "2025-01-01",
            "https://creativecommons.org/licenses/by/4.0/",
            GraphPolicy {
                public: true,
                permission_paths: vec!["/datasets/public/hydrate-search".to_string()],
            },
        ),
    )
    .unwrap();

    let hits = node.search(&reader, "hydrated", 10).unwrap();
    assert_eq!(hits.len(), 1);

    let hydrated = node.hydrate_search_hits(&reader, &hits).unwrap();
    assert_eq!(hydrated.len(), 1);
    assert!(
        hydrated[0]
            .properties
            .iter()
            .any(|(predicate, object)| predicate
                == &EncodedTerm::from_named_node(&vocab::schema_name())
                && object.0.contains("Hydrated Search Dataset"))
    );

    let hydrated_search = node.search_resources(&reader, "hydrated", 10).unwrap();
    assert_eq!(hydrated_search.len(), 1);
}

#[test]
fn cluster_sync_converges_through_public_api() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = CraqleCluster::new(2, dir.path()).unwrap();
    let graph = GraphId::new("urn:test:cluster");
    let writer = writer_auth();
    let anonymous = GrantAuthorizer::default();

    cluster
        .peer(0)
        .create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                "Cluster Dataset",
                "Synced via simulation helper",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/datasets/public/cluster".to_string()],
                },
            ),
        )
        .unwrap();
    cluster
        .peer(0)
        .add_data_entity(
            &writer,
            &graph,
            "data/cluster.txt",
            "http://schema.org/MediaObject",
            "Cluster File",
        )
        .unwrap();

    cluster.sync_until_converged(10).unwrap();

    let exported = cluster.peer(1).export_rocrate(&anonymous, &graph).unwrap();
    assert!(exported.contains("Cluster File"));

    cluster.reindex_search().unwrap();
    let hits = cluster.peer(1).search(&anonymous, "cluster", 10).unwrap();
    assert!(!hits.is_empty());

    cluster.partition(0, 1);
    cluster.heal(0, 1);
}

#[test]
fn cluster_query_options_can_fan_out_across_peers() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = CraqleCluster::new(2, dir.path()).unwrap();
    let writer = writer_auth();
    let anonymous = GrantAuthorizer::default();

    cluster
        .peer(0)
        .create_crate(
            &writer,
            CreateCrateRequest::new(
                GraphId::new("urn:test:peer0"),
                "Peer Zero Dataset",
                "Lives only on peer zero",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/datasets/public/peer0".to_string()],
                },
            ),
        )
        .unwrap();
    cluster
        .peer(1)
        .create_crate(
            &writer,
            CreateCrateRequest::new(
                GraphId::new("urn:test:peer1"),
                "Peer One Dataset",
                "Lives only on peer one",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/datasets/public/peer1".to_string()],
                },
            ),
        )
        .unwrap();

    let local_rows = match cluster
        .query_from_peer(
            0,
            &anonymous,
            "SELECT ?name WHERE { ?s schema:name ?name }",
            QueryOptions { local_only: true },
        )
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };
    assert_eq!(local_rows.len(), 1);

    let federated_rows = match cluster
        .query_from_peer(
            0,
            &anonymous,
            "SELECT ?name WHERE { ?s schema:name ?name }",
            QueryOptions { local_only: false },
        )
        .unwrap()
    {
        QueryResults::Solutions(rows) => rows,
        other => panic!("expected solutions, got {other:?}"),
    };
    assert!(federated_rows.len() > local_rows.len());

    let hits = cluster
        .search_from_peer(
            0,
            &anonymous,
            "peer",
            10,
            QueryOptions { local_only: false },
        )
        .unwrap();
    assert!(hits.iter().any(|hit| hit.graph_id == "urn:test:peer1"));
}

#[test]
fn federated_queries_do_not_leak_remote_private_graphs() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = CraqleCluster::new(2, dir.path()).unwrap();
    let writer = writer_auth();
    let anonymous = GrantAuthorizer::default();

    cluster
        .peer(0)
        .create_crate(
            &writer,
            CreateCrateRequest::new(
                GraphId::new("urn:test:federated-public"),
                "Public Federated Dataset",
                "Visible everywhere",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/datasets/public/federated".to_string()],
                },
            ),
        )
        .unwrap();
    cluster
        .peer(1)
        .create_crate(
            &writer,
            CreateCrateRequest::new(
                GraphId::new("urn:test:federated-private"),
                "Private Federated Dataset",
                "Must not leak through remote fanout",
                "2025-01-01",
                "https://creativecommons.org/licenses/by/4.0/",
                GraphPolicy {
                    public: false,
                    permission_paths: vec!["/datasets/private/federated".to_string()],
                },
            ),
        )
