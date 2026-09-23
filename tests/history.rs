mod support;

use std::collections::BTreeSet;

use craqle::{
    AllowAllAuthorizer, CraqleIrokleOptions, CraqleNode, CraqleOptions, CreateCrateRequest,
    DenyAllAuthorizer, GraphHistory, GraphId, GraphPolicy, HistoryCompare, HistoryLog,
    MaterializedQuadChange, SearchStorage,
};
use irokle::{Irokle, MemoryStorage, OpId};

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

impl Fixture {
    fn rename(&self, name: &str) {
        let policy = self.node.graph_policy(&self.graph).unwrap();
        let text = self
            .node
            .export_rocrate(&AllowAllAuthorizer, &self.graph)
            .unwrap();
        let mut doc: serde_json::Value = serde_json::from_str(&text).unwrap();
        let root = doc["@graph"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|value| value["@type"] == "Dataset")
            .unwrap();
        root["name"] = serde_json::json!(name);
        self.node
            .apply_rocrate_document_checked_with_policy(
                &AllowAllAuthorizer,
                self.graph.clone(),
                &doc.to_string(),
                policy,
            )
            .unwrap();
    }

    fn heads(&self) -> Vec<OpId> {
        self.node
            .graph_heads(&AllowAllAuthorizer, &self.graph)
            .unwrap()
    }
}

fn names(changes: &[MaterializedQuadChange]) -> (Vec<String>, Vec<String>) {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for change in changes {
        match change {
            MaterializedQuadChange::Insert { object, .. } => added.push(object.0.clone()),
            MaterializedQuadChange::Delete { object, .. } => removed.push(object.0.clone()),
        }
    }
    (added, removed)
}

#[test]
fn pages_log_newest_first() {
    let fixture = Fixture::new();
    fixture.rename("Second");
    fixture.rename("Third");
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let all = fixture.native.raw_topic(topic).unwrap().history().unwrap();
    assert_eq!(fixture.heads(), fixture.request("unused").heads);
    let mut request = HistoryLog {
        graph: fixture.graph.clone(),
        heads: fixture.heads(),
        limit: 1,
        max_bytes: 1024 * 1024,
    };
    let mut walked = Vec::new();
    while !request.heads.is_empty() {
        let page = fixture
            .node
            .history_log(&AllowAllAuthorizer, &request)
            .unwrap();
        assert_eq!(page.operations.len(), 1);
        walked.extend(page.operations);
        request.heads = page.next;
    }
    let ids = walked.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), walked.len());
    assert_eq!(ids, all.iter().map(|op| op.id).collect());
    assert_eq!(vec![walked[0].id], fixture.heads());
    for (index, op) in walked.iter().enumerate() {
        assert!(
            op.parents
                .iter()
                .all(|parent| { walked[index + 1..].iter().any(|older| older.id == *parent) })
        );
    }
    assert!(
        walked
            .windows(2)
            .all(|pair| pair[0].generation >= pair[1].generation)
    );
    let third = &walked[0];
    assert_eq!(third.actor, walked[1].actor);
    assert_eq!(third.sequence, walked[1].sequence + 1);
    assert!(third.parents.contains(&walked[1].id));
    let (added, removed) = names(third.changes());
    assert_eq!(added, vec!["\"Third\"".to_owned()]);
    assert_eq!(removed, vec!["\"Second\"".to_owned()]);
    assert!(walked.last().unwrap().parents.is_empty());
    assert!(walked.last().unwrap().changes().is_empty());
}

#[test]
fn compares_two_states() {
    let fixture = Fixture::new();
    let before = fixture.heads();
    fixture.rename("Changed");
    let request = HistoryCompare {
        graph: fixture.graph.clone(),
        from: before.clone(),
        to: fixture.heads(),
        max_operations: 100,
        max_bytes: 1024 * 1024,
    };
    let changes = fixture
        .node
        .compare_history(&AllowAllAuthorizer, &request)
        .unwrap();
    assert_eq!(
        names(&changes),
        (
            vec!["\"Changed\"".to_owned()],
            vec!["\"Original\"".to_owned()]
        )
    );
    let same = HistoryCompare {
        to: before,
        ..request
    };
    assert!(
        fixture
            .node
            .compare_history(&AllowAllAuthorizer, &same)
            .unwrap()
            .is_empty()
    );
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
