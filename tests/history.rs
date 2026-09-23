mod support;

use craqle::{
    AllowAllAuthorizer, CraqleIrokleOptions, CraqleNode, CraqleOptions, CreateCrateRequest,
    DenyAllAuthorizer, GraphHistory, GraphId, GraphPolicy, SearchStorage,
};
use irokle::{Irokle, MemoryStorage};

struct Fixture {
    directory: tempfile::TempDir,
    node: CraqleNode,
    native: Irokle<MemoryStorage>,
    graph: GraphId,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = Irokle::builder()
            .with_signer(irokle::Ed25519Signer::from_bytes(&[71; 32]))
            .build()
            .unwrap();
        let node = CraqleNode::open_with_options(
            directory.path().join("live"),
            CraqleOptions::new()
                .with_search_storage(SearchStorage::Memory)
                .with_irokle(native.clone(), CraqleIrokleOptions::new()),
        )
        .unwrap();
        let graph = GraphId::new("urn:history:crate");
        node.create_crate(
            &AllowAllAuthorizer,
            CreateCrateRequest::new(
                graph.clone(),
                "Original",
                "Native history",
                "2026-09-23",
                Some("https://example.org/license".into()),
                GraphPolicy {
                    public: true,
                    permission_paths: vec!["/history".into()],
                },
            ),
        )
        .unwrap();
        Self {
            directory,
            node,
            native,
            graph,
        }
    }

    fn request(&self, name: &str) -> GraphHistory {
        let topic = self.node.irokle_topic_id(&self.graph).unwrap().unwrap();
        GraphHistory {
            graph: self.graph.clone(),
            heads: self
                .native
                .raw_topic(topic)
                .unwrap()
                .heads()
                .unwrap()
                .into_iter()
                .collect(),
            directory: self.directory.path().join(name),
            max_operations: 100,
            max_bytes: 1024 * 1024,
        }
    }
}

#[test]
fn replays_old_heads() {
    let fixture = Fixture::new();
    let request = fixture.request("past");
    let original = fixture
        .node
        .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
        .unwrap();
    let original_state = fixture.node.graph_snapshot(&fixture.graph).unwrap();
    let original_policy = fixture.node.graph_policy(&fixture.graph).unwrap();
    let modified = original.replace("Original", "Changed");
    fixture
        .node
        .apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            fixture.graph.clone(),
            &modified,
            original_policy.clone(),
        )
        .unwrap();
    let live_state = fixture.node.graph_snapshot(&fixture.graph).unwrap();
    let live_heads = fixture.request("unused").heads;
    let view = fixture
        .node
        .project_history(&AllowAllAuthorizer, &request)
        .unwrap();
    assert_eq!(
        view.node.graph_snapshot(&fixture.graph).unwrap(),
        original_state
    );
    assert_eq!(
        view.node.graph_policy(&fixture.graph).unwrap(),
        original_policy
    );
    assert_eq!(
        view.node
            .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
            .unwrap(),
        original
    );
    assert_eq!(
        fixture.node.graph_snapshot(&fixture.graph).unwrap(),
        live_state
    );
    assert_eq!(fixture.request("unused").heads, live_heads);
    assert!(
        request
            .heads
            .iter()
            .all(|id| view.operations.iter().any(|op| op.id == *id))
    );
    assert!(view.node.irokle_topic_id(&fixture.graph).unwrap().is_none());
}

#[test]
fn refuses_unsafe_requests() {
    let fixture = Fixture::new();
    let mut request = fixture.request("denied");
    assert!(
        fixture
            .node
            .project_history(&DenyAllAuthorizer, &request)
            .is_err()
    );
    assert!(!request.directory.exists());
    request.max_operations = 1;
    assert!(
        fixture
            .node
            .project_history(&AllowAllAuthorizer, &request)
            .is_err()
    );
    assert!(!request.directory.exists());
    request.max_operations = 100;
    request.max_bytes = 1;
    assert!(
        fixture
            .node
            .project_history(&AllowAllAuthorizer, &request)
            .is_err()
    );
    assert!(!request.directory.exists());
    request.max_bytes = 1024 * 1024;
    request.heads = vec![irokle::OpId::from_bytes([0xff; 32])];
    assert!(
        fixture
            .node
            .project_history(&AllowAllAuthorizer, &request)
            .is_err()
    );
    assert!(!request.directory.exists());
    let mut request = fixture.request("existing");
    request.directory = fixture.directory.path().join("live");
    let before = fixture.node.graph_snapshot(&fixture.graph).unwrap();
    assert!(
        fixture
            .node
            .project_history(&AllowAllAuthorizer, &request)
            .is_err()
    );
    assert_eq!(fixture.node.graph_snapshot(&fixture.graph).unwrap(), before);
}

#[test]
fn replays_deleted_graph() {
    let fixture = Fixture::new();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let mut request = fixture.request("deleted");
    fixture
        .node
        .delete_graph(&AllowAllAuthorizer, &fixture.graph)
        .unwrap();
    request.heads = fixture
        .native
        .raw_topic(topic)
        .unwrap()
        .heads()
        .unwrap()
        .into_iter()
        .collect();
    let view = fixture
        .node
        .project_history(&AllowAllAuthorizer, &request)
        .unwrap();
    assert!(!view.node.contains_graph(&fixture.graph).unwrap());
}

#[test]
fn preserves_concurrent_heads() {
    let directory = tempfile::tempdir().unwrap();
    let cluster = support::CraqleCluster::new(2, directory.path()).unwrap();
    let graph = GraphId::new("urn:history:concurrent");
    support::create_test_crate(&cluster, 0, &graph);
    cluster.sync_until_converged(10).unwrap();
    let topic = cluster.peer(0).irokle_topic_id(&graph).unwrap().unwrap();
    let mut requests = Vec::new();
    let mut states = Vec::new();
    for index in 0..2 {
        let node = cluster.peer(index);
        let text = node.export_rocrate(&AllowAllAuthorizer, &graph).unwrap();
        let mut doc: serde_json::Value = serde_json::from_str(&text).unwrap();
        let root = doc["@graph"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|value| value["@type"] == "Dataset")
            .unwrap();
        root["name"] = serde_json::json!(format!("Branch {index}"));
        node.apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            graph.clone(),
            &doc.to_string(),
            node.graph_policy(&graph).unwrap(),
        )
        .unwrap();
        states.push(node.graph_snapshot(&graph).unwrap());
        requests.push(GraphHistory {
            graph: graph.clone(),
            heads: cluster
                .irokle(index)
                .raw_topic(topic)
                .unwrap()
                .heads()
                .unwrap()
                .into_iter()
                .collect(),
            directory: directory.path().join(format!("history-{index}")),
            max_operations: 100,
            max_bytes: 1024 * 1024,
        });
    }
    cluster.sync_until_converged(10).unwrap();
    let live = cluster.peer(0).graph_snapshot(&graph).unwrap();
    for (request, expected) in requests.iter().zip(states) {
        let view = cluster
            .peer(0)
            .project_history(&AllowAllAuthorizer, request)
            .unwrap();
        assert_eq!(view.node.graph_snapshot(&graph).unwrap(), expected);
        assert_eq!(cluster.peer(0).graph_snapshot(&graph).unwrap(), live);
    }
}
