mod support;

use std::collections::BTreeSet;

use craqle::{
    Action, AllowAllAuthorizer, AuthorizationError, CommitInfo, CraqleErrorKind, CraqleGraphEvent,
    CraqleIrokleOptions, CraqleNode, CraqleOptions, CraqleRequestDurability, CreateCrateRequest,
    DenyAllAuthorizer, EncodedTerm, GraphHistory, GraphId, GraphPolicy, HistoryCompare, HistoryLog,
    HistoryPoint, HistoryRestore, MaterializedQuadChange, MutationCommit, MutationId,
    MutationRequest, RoCrateWrite, SearchStorage,
};
use irokle::{Irokle, MemoryStorage, OpId};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, irokle::Event)]
#[irokle(type_id = "craqle.graph.v1")]
struct PoisonEvent {
    junk: Vec<u64>,
}

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
        self.node
            .apply_rocrate_document_checked_with_policy(
                &AllowAllAuthorizer,
                self.graph.clone(),
                &self.renamed(name),
                policy,
            )
            .unwrap();
    }

    fn renamed(&self, name: &str) -> String {
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
        doc.to_string()
    }

    fn rocrate_write<'a>(&self, jsonld: &'a str, commit: CommitInfo) -> RoCrateWrite<'a> {
        RoCrateWrite {
            graph: self.graph.clone(),
            jsonld,
            policy: self.node.graph_policy(&self.graph).unwrap(),
            durability: CraqleRequestDurability::Durable,
            actor: None,
            commit: Some(commit),
        }
    }

    fn insert(&self, object: &str) -> MaterializedQuadChange {
        MaterializedQuadChange::Insert {
            graph: self.graph.clone(),
            subject: EncodedTerm("<urn:commit>".into()),
            predicate: EncodedTerm("<urn:p>".into()),
            object: EncodedTerm(format!("\"{object}\"")),
        }
    }

    fn heads(&self) -> Vec<OpId> {
        self.node
            .graph_heads(&AllowAllAuthorizer, &self.graph)
            .unwrap()
    }

    fn content(&self) -> BTreeSet<(EncodedTerm, EncodedTerm, EncodedTerm)> {
        let snapshot = self.node.graph_snapshot(&self.graph).unwrap();
        snapshot
            .quads
            .into_iter()
            .map(|quad| (quad.subject, quad.predicate, quad.object))
            .collect()
    }

    fn restore(&self, heads: &[OpId], expected: Option<Vec<OpId>>) -> HistoryRestore {
        HistoryRestore {
            graph: self.graph.clone(),
            heads: heads.to_vec(),
            expected,
            id: None,
            max_operations: 100,
            max_bytes: 1024 * 1024,
            commit: None,
        }
    }
}

fn commit(message: &str) -> CommitInfo {
    CommitInfo {
        message: message.to_owned(),
        author_name: "Ada Lovelace".to_owned(),
        author_email: "ada@example.org".to_owned(),
        author_time_ms: 1_790_000_000_000,
        author_tz_offset_minutes: 120,
        sources: Vec::new(),
    }
}

fn point(graph: &str, heads: usize) -> HistoryPoint {
    HistoryPoint {
        graph: GraphId::new(graph),
        heads: (0..heads)
            .map(|head| OpId::from_bytes([head as u8; 32]))
            .collect(),
    }
}

/// Commit infos that break exactly one bound each, for commits to `graph`.
fn invalid_commits(graph: &GraphId) -> Vec<CommitInfo> {
    let base = commit("invalid");
    let mut long = base.clone();
    long.message = "m".repeat(CommitInfo::MAX_MESSAGE_BYTES + 1);
    let mut wide = base.clone();
    wide.author_email = "e".repeat(CommitInfo::MAX_AUTHOR_BYTES + 1);
    let mut broken = base.clone();
    broken.author_name = "Ada\nCommitter: Eve".to_owned();
    let mut zone = base.clone();
    zone.author_tz_offset_minutes = -1081;
    let with_sources = |sources: Vec<HistoryPoint>| CommitInfo {
        sources,
        ..base.clone()
    };
    let many = (0..=CommitInfo::MAX_SOURCES)
        .map(|index| point(&format!("urn:source:{index}"), 1))
        .collect();
    vec![
        long,
        wide,
        broken,
        zone,
        with_sources(many),
        with_sources(vec![point("urn:source:empty", 0)]),
        with_sources(vec![point(
            "urn:source:wide",
            CommitInfo::MAX_SOURCE_HEADS + 1,
        )]),
        with_sources(vec![
            point("urn:source:twice", 1),
            point("urn:source:twice", 2),
        ]),
        with_sources(vec![point(graph.as_str(), 1)]),
        with_sources(vec![HistoryPoint {
            graph: GraphId::new("urn:source:repeat"),
            heads: vec![OpId::from_bytes([7; 32]); 2],
        }]),
    ]
}

/// The newest operation of a graph with a single head.
fn newest(node: &CraqleNode, graph: &GraphId) -> craqle::HistoryOperation {
    let log = HistoryLog {
        graph: graph.clone(),
        heads: node.graph_heads(&AllowAllAuthorizer, graph).unwrap(),
        limit: 1,
        max_bytes: 1024 * 1024,
    };
    let mut page = node.history_log(&AllowAllAuthorizer, &log).unwrap();
    page.operations.remove(0)
}

fn read_only(
    _: &GraphId,
    _: &GraphPolicy,
    action: Action,
) -> std::result::Result<(), AuthorizationError> {
    match action {
        Action::Read => Ok(()),
        Action::Write => Err(AuthorizationError::PermissionDenied {
            action,
            graph: String::new(),
        }),
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
        names(&changes.changes),
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
            .changes
            .is_empty()
    );
}

#[test]
fn restores_as_new_operation() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let original_content = fixture.content();
    let exported = fixture
        .node
        .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
        .unwrap();
    fixture.rename("Changed");
    let changed = fixture.heads();
    let restored = fixture
        .node
        .restore_history(&AllowAllAuthorizer, &fixture.restore(&original, None))
        .unwrap()
        .unwrap();
    assert!(restored.extends(&changed));
    let restored = restored.operation;
    assert_eq!(fixture.heads(), vec![restored]);
    assert_eq!(fixture.content(), original_content);
    assert_eq!(
        fixture
            .node
            .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
            .unwrap(),
        exported
    );
    let page = fixture
        .node
        .history_log(
            &AllowAllAuthorizer,
            &HistoryLog {
                graph: fixture.graph.clone(),
                heads: vec![restored],
                limit: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
    assert_eq!(page.operations[0].parents, changed);
    assert_eq!(page.operations[1].id, changed[0]);
    let (added, removed) = names(page.operations[0].changes());
    assert_eq!(added, vec!["\"Original\"".to_owned()]);
    assert_eq!(removed, vec!["\"Changed\"".to_owned()]);
    let old = fixture.request("old");
    let view = fixture
        .node
        .project_history(
            &AllowAllAuthorizer,
            &GraphHistory {
                heads: changed.clone(),
                ..old
            },
        )
        .unwrap();
    assert!(
        view.node
            .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
            .unwrap()
            .contains("Changed")
    );
    assert_eq!(
        fixture
            .node
            .restore_history(&AllowAllAuthorizer, &fixture.restore(&original, None))
            .unwrap(),
        None
    );
    assert_eq!(fixture.heads(), vec![restored]);
}

#[test]
fn fences_current_heads() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    fixture.rename("Changed");
    let content = fixture.content();
    let stale = fixture
        .node
        .restore_history(
            &AllowAllAuthorizer,
            &fixture.restore(&original, Some(original.clone())),
        )
        .unwrap_err();
    assert_eq!(stale.kind(), CraqleErrorKind::Conflict);
    assert_eq!(fixture.content(), content);
    let current = fixture.heads();
    let restored = fixture
        .node
        .restore_history(
            &AllowAllAuthorizer,
            &fixture.restore(&original, Some(current.clone())),
        )
        .unwrap()
        .unwrap();
    assert!(restored.extends(&current));
    assert_eq!(fixture.heads(), vec![restored.operation]);
}

#[test]
fn repeats_restore_by_id() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    fixture.rename("Changed");
    let mut request = fixture.restore(&original, None);
    request.id = Some(craqle::MutationId::new());
    let first = fixture
        .node
        .restore_history(&AllowAllAuthorizer, &request)
        .unwrap()
        .unwrap();
    fixture.rename("Again");
    let heads = fixture.heads();
    let second = fixture
        .node
        .restore_history(&AllowAllAuthorizer, &request)
        .unwrap()
        .unwrap();
    assert_eq!(second, first);
    assert_eq!(Some(second.id), request.id);
    assert_eq!(fixture.heads(), heads);
}

#[test]
fn restores_over_unapplied_records() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let original_content = fixture.content();
    for name in ["Two", "Three", "Four", "Five"] {
        fixture.rename(name);
    }
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::QuadChanges {
            graph: fixture.graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: fixture.graph.clone(),
                subject: EncodedTerm("<urn:late>".into()),
                predicate: EncodedTerm("<urn:p>".into()),
                object: EncodedTerm("\"late\"".into()),
            }],
        })
        .unwrap();
    let mut walk = HistoryLog {
        graph: fixture.graph.clone(),
        heads: original.clone(),
        limit: 100,
        max_bytes: 1024 * 1024,
    };
    let past = fixture
        .node
        .history_log(&AllowAllAuthorizer, &walk)
        .unwrap();
    assert!(past.next.is_empty());
    let mut request = fixture.restore(&original, None);
    request.max_operations = past.operations.len();
    fixture
        .node
        .restore_history(&AllowAllAuthorizer, &request)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.content(), original_content);
    fixture.node.reconcile_irokle().unwrap();
    assert_eq!(fixture.content(), original_content);
    walk.heads = fixture.heads();
    walk.limit = past.operations.len();
    assert_eq!(
        fixture
            .node
            .history_log(&AllowAllAuthorizer, &walk)
            .unwrap()
            .operations
            .len(),
        past.operations.len()
    );
}

#[test]
fn rejects_unauthorized_history() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    fixture.rename("Changed");
    let heads = fixture.heads();
    let content = fixture.content();
    let log = HistoryLog {
        graph: fixture.graph.clone(),
        heads: heads.clone(),
        limit: 10,
        max_bytes: 1024 * 1024,
    };
    let compare = HistoryCompare {
        graph: fixture.graph.clone(),
        from: original.clone(),
        to: heads.clone(),
        max_operations: 100,
        max_bytes: 1024 * 1024,
    };
    let denied = [
        fixture
            .node
            .graph_heads(&DenyAllAuthorizer, &fixture.graph)
            .map(drop),
        fixture.node.history_log(&DenyAllAuthorizer, &log).map(drop),
        fixture
            .node
            .compare_history(&DenyAllAuthorizer, &compare)
            .map(drop),
        fixture
            .node
            .restore_history(&DenyAllAuthorizer, &fixture.restore(&original, None))
            .map(drop),
        fixture
            .node
            .restore_history(&read_only, &fixture.restore(&original, None))
            .map(drop),
    ];
    for result in denied {
        assert_eq!(result.unwrap_err().kind(), CraqleErrorKind::Unauthorized);
    }
    assert!(fixture.node.history_log(&read_only, &log).is_ok());
    assert_eq!(fixture.heads(), heads);
    assert_eq!(fixture.content(), content);
}

#[test]
fn fails_when_bounds_exceeded() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    fixture.rename("Changed");
    let heads = fixture.heads();
    let content = fixture.content();
    let log = HistoryLog {
        graph: fixture.graph.clone(),
        heads: heads.clone(),
        limit: 10,
        max_bytes: 1,
    };
    let kind = |result: craqle::Result<()>| result.unwrap_err().kind();
    assert_eq!(
        kind(
            fixture
                .node
                .history_log(&AllowAllAuthorizer, &log)
                .map(drop)
        ),
        CraqleErrorKind::ResourceLimit
    );
    let empty = HistoryLog {
        limit: 0,
        max_bytes: 1024,
        ..log
    };
    assert_eq!(
        kind(
            fixture
                .node
                .history_log(&AllowAllAuthorizer, &empty)
                .map(drop)
        ),
        CraqleErrorKind::InvalidInput
    );
    let compare = HistoryCompare {
        graph: fixture.graph.clone(),
        from: original.clone(),
        to: heads.clone(),
        max_operations: 1,
        max_bytes: 1024 * 1024,
    };
    assert_eq!(
        kind(
            fixture
                .node
                .compare_history(&AllowAllAuthorizer, &compare)
                .map(drop)
        ),
        CraqleErrorKind::ResourceLimit
    );
    let mut restore = fixture.restore(&original, None);
    restore.max_operations = 1;
    assert_eq!(
        kind(
            fixture
                .node
                .restore_history(&AllowAllAuthorizer, &restore)
                .map(drop)
        ),
        CraqleErrorKind::ResourceLimit
    );
    restore.max_operations = 100;
    restore.heads = vec![OpId::from_bytes([0xff; 32])];
    assert_eq!(
        kind(
            fixture
                .node
                .restore_history(&AllowAllAuthorizer, &restore)
                .map(drop)
        ),
        CraqleErrorKind::InvalidInput
    );
    assert_eq!(fixture.heads(), heads);
    assert_eq!(fixture.content(), content);
}

#[test]
fn skips_rejected_records() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    fixture
        .native
        .open_topic::<PoisonEvent>(topic)
        .unwrap()
        .publish(PoisonEvent { junk: vec![7; 9] })
        .unwrap();
    let other = GraphId::new("urn:history:other");
    fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::QuadChanges {
            graph: other.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: other,
                subject: EncodedTerm("<urn:hidden>".into()),
                predicate: EncodedTerm("<urn:p>".into()),
                object: EncodedTerm("\"secret\"".into()),
            }],
        })
        .unwrap();
    fixture.node.reconcile_irokle().unwrap();
    let heads = fixture.heads();
    let page = fixture
        .node
        .history_log(
            &AllowAllAuthorizer,
            &HistoryLog {
                graph: fixture.graph.clone(),
                heads: heads.clone(),
                limit: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
    assert!(
        page.operations
            .iter()
            .all(|op| op.rejected && op.event.is_none())
    );
    let compare = HistoryCompare {
        graph: fixture.graph.clone(),
        from: original,
        to: heads.clone(),
        max_operations: 100,
        max_bytes: 1024 * 1024,
    };
    assert!(
        fixture
            .node
            .compare_history(&AllowAllAuthorizer, &compare)
            .unwrap()
            .changes
            .is_empty()
    );
    let view = fixture
        .node
        .project_history(
            &AllowAllAuthorizer,
            &GraphHistory {
                heads,
                ..fixture.request("poisoned")
            },
        )
        .unwrap();
    assert_eq!(
        view.node.graph_snapshot(&fixture.graph).unwrap(),
        fixture.node.graph_snapshot(&fixture.graph).unwrap()
    );
}

#[test]
fn restores_context_and_license() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let exported = fixture
        .node
        .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
        .unwrap();
    let mut doc: serde_json::Value = serde_json::from_str(&exported).unwrap();
    doc["@context"] = serde_json::json!([
        doc["@context"].clone(),
        {"extra": "https://example.org/extra"}
    ]);
    let root = doc["@graph"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|value| value["@type"] == "Dataset")
        .unwrap();
    root["license"] = serde_json::json!("https://example.org/other");
    fixture
        .node
        .apply_rocrate_document_checked_with_policy(
            &AllowAllAuthorizer,
            fixture.graph.clone(),
            &doc.to_string(),
            fixture.node.graph_policy(&fixture.graph).unwrap(),
        )
        .unwrap();
    let changed = fixture
        .node
        .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
        .unwrap();
    assert_ne!(changed, exported);
    let diff = fixture
        .node
        .compare_history(
            &AllowAllAuthorizer,
            &HistoryCompare {
                graph: fixture.graph.clone(),
                from: fixture.heads(),
                to: original.clone(),
                max_operations: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
    assert!(diff.hints.is_some());
    fixture
        .node
        .restore_history(&AllowAllAuthorizer, &fixture.restore(&original, None))
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .node
            .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
            .unwrap(),
        exported
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
    for (request, expected) in requests.iter().zip(&states) {
        let view = cluster
            .peer(0)
            .project_history(&AllowAllAuthorizer, request)
            .unwrap();
        assert_eq!(&view.node.graph_snapshot(&graph).unwrap(), expected);
        assert_eq!(cluster.peer(0).graph_snapshot(&graph).unwrap(), live);
    }
    let node = cluster.peer(0);
    let heads = node.graph_heads(&AllowAllAuthorizer, &graph).unwrap();
    assert_eq!(heads.len(), 2);
    let mut log = HistoryLog {
        graph: graph.clone(),
        heads: heads.clone(),
        limit: 1,
        max_bytes: 1024 * 1024,
    };
    let narrow = node.history_log(&AllowAllAuthorizer, &log).unwrap_err();
    assert_eq!(narrow.kind(), CraqleErrorKind::ResourceLimit);
    log.limit = 2;
    let mut walked = BTreeSet::new();
    while !log.heads.is_empty() {
        let page = node.history_log(&AllowAllAuthorizer, &log).unwrap();
        assert!(page.operations.into_iter().all(|op| walked.insert(op.id)));
        log.heads = page.next;
    }
    let all = cluster
        .irokle(0)
        .raw_topic(topic)
        .unwrap()
        .history()
        .unwrap();
    assert_eq!(walked, all.iter().map(|op| op.id).collect());
    let diff = node
        .compare_history(
            &AllowAllAuthorizer,
            &HistoryCompare {
                graph: graph.clone(),
                from: requests[0].heads.clone(),
                to: heads.clone(),
                max_operations: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
    let (added, removed) = names(&diff.changes);
    assert!(added.contains(&"\"Branch 1\"".to_owned()));
    assert!(removed.is_empty());
    let restored = node
        .restore_history(
            &AllowAllAuthorizer,
            &HistoryRestore {
                graph: graph.clone(),
                heads: requests[0].heads.clone(),
                expected: Some(heads.clone()),
                id: None,
                max_operations: 100,
                max_bytes: 1024 * 1024,
                commit: None,
            },
        )
        .unwrap()
        .unwrap();
    assert!(restored.extends(&heads));
    let quads = |snapshot: &craqle::GraphReplicaSnapshot| {
        snapshot
            .quads
            .iter()
            .map(|quad| {
                (
                    quad.subject.clone(),
                    quad.predicate.clone(),
                    quad.object.clone(),
                )
            })
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        quads(&node.graph_snapshot(&graph).unwrap()),
        quads(&states[0])
    );
    cluster.sync_until_converged(10).unwrap();
    assert_eq!(
        cluster.peer(1).graph_snapshot(&graph).unwrap(),
        node.graph_snapshot(&graph).unwrap()
    );
}

#[test]
fn syncs_commit_messages() {
    let directory = tempfile::tempdir().unwrap();
    let cluster = support::CraqleCluster::new(2, directory.path()).unwrap();
    let graph = GraphId::new("urn:history:commits");
    support::create_test_crate(&cluster, 0, &graph);
    cluster.sync_until_converged(10).unwrap();
    let node = cluster.peer(0);
    let change = MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm::from_named_node(&graph.0),
        predicate: EncodedTerm::from_named_node(&craqle::vocab::schema_keywords()),
        object: EncodedTerm("\"synced\"".into()),
    };
    let id = MutationId::new();
    let request = |admission_sequence| MutationCommit {
        request: MutationRequest {
            id,
            admission_sequence,
            graph: graph.clone(),
            changes: vec![change.clone()],
        },
        commit: CommitInfo {
            author_tz_offset_minutes: -330,
            sources: vec![point("urn:history:fork", 2), point("urn:history:merged", 1)],
            ..commit("Add a synced keyword")
        },
    };
    let ticket = node
        .apply_mutation_with(&AllowAllAuthorizer, request(None))
        .unwrap();
    node.apply_mutation_with(
        &AllowAllAuthorizer,
        request(Some(ticket.admission_sequence)),
    )
    .unwrap();
    cluster.sync_until_converged(10).unwrap();
    let remote = newest(cluster.peer(1), &graph);
    assert_eq!(remote.commit(), Some(&request(None).commit));
    assert!(!remote.rejected);
    assert_eq!(remote.changes(), std::slice::from_ref(&change));
    assert_eq!(remote, newest(node, &graph));
    let content = cluster.peer(1).graph_snapshot(&graph).unwrap();
    assert_eq!(content, node.graph_snapshot(&graph).unwrap());
    let topic = node.irokle_topic_id(&graph).unwrap().unwrap();
    let rejections = cluster.peer(1).replication_rejection_count();
    cluster
        .irokle(0)
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::CommittedMutation {
            id: MutationId::new(),
            graph: graph.clone(),
            changes: vec![change],
            render_hints: None,
            commit: Box::new(invalid_commits(&graph).pop().unwrap()),
        })
        .unwrap();
    cluster.sync_until_converged(10).unwrap();
    let replica = cluster.peer(1);
    assert_eq!(replica.replication_rejection_count(), rejections + 1);
    let rejected = newest(replica, &graph);
    assert!(rejected.rejected && rejected.event.is_none());
    assert_eq!(replica.graph_snapshot(&graph).unwrap(), content);
}

#[test]
fn records_rocrate_commits() {
    let fixture = Fixture::new();
    let document = fixture.renamed("Committed");
    fixture
        .node
        .apply_rocrate_with(
            &AllowAllAuthorizer,
            fixture.rocrate_write(&document, commit("Rename the crate")),
        )
        .unwrap();
    let operation = newest(&fixture.node, &fixture.graph);
    assert_eq!(operation.commit(), Some(&commit("Rename the crate")));
    assert!(matches!(
        operation.event,
        Some(CraqleGraphEvent::CommittedMutation {
            render_hints: Some(_),
            ..
        })
    ));
    assert!(
        fixture
            .node
            .export_rocrate(&AllowAllAuthorizer, &fixture.graph)
            .unwrap()
            .contains("Committed")
    );
    fixture.rename("Plain");
    let plain = newest(&fixture.node, &fixture.graph);
    assert!(matches!(
        plain.event,
        Some(CraqleGraphEvent::Mutation { .. })
    ));
    assert_eq!(plain.commit(), None);
}

#[test]
fn records_restore_commits() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let content = fixture.content();
    fixture.rename("Changed");
    let request = HistoryRestore {
        commit: Some(commit("Revert the rename")),
        ..fixture.restore(&original, None)
    };
    let restored = fixture
        .node
        .restore_history(&AllowAllAuthorizer, &request)
        .unwrap()
        .unwrap();
    let operation = newest(&fixture.node, &fixture.graph);
    assert_eq!(operation.id, restored.operation);
    assert_eq!(operation.commit(), Some(&commit("Revert the rename")));
    assert_eq!(fixture.content(), content);
}

#[test]
fn rejects_invalid_commits() {
    let fixture = Fixture::new();
    let heads = fixture.heads();
    let content = fixture.content();
    let document = fixture.renamed("Rejected");
    for invalid in invalid_commits(&fixture.graph) {
        let error = fixture
            .node
            .apply_rocrate_with(
                &AllowAllAuthorizer,
                fixture.rocrate_write(&document, invalid.clone()),
            )
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
        let mutation = MutationCommit {
            request: MutationRequest {
                id: MutationId::new(),
                admission_sequence: None,
                graph: fixture.graph.clone(),
                changes: vec![fixture.insert("rejected")],
            },
            commit: invalid.clone(),
        };
        let error = fixture
            .node
            .apply_mutation_with(&AllowAllAuthorizer, mutation)
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
        let restore = HistoryRestore {
            commit: Some(invalid),
            ..fixture.restore(&heads, None)
        };
        let error = fixture
            .node
            .restore_history(&AllowAllAuthorizer, &restore)
            .unwrap_err();
        assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
    }
    // A write that publishes no event has nowhere to keep the commit.
    let local = RoCrateWrite {
        durability: CraqleRequestDurability::WalAlreadyDurable,
        ..fixture.rocrate_write(&document, commit("Local"))
    };
    let error = fixture
        .node
        .apply_rocrate_with(&AllowAllAuthorizer, local)
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
    assert_eq!(fixture.heads(), heads);
    assert_eq!(fixture.content(), content);
}

#[test]
fn rejects_peer_commits() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let content = fixture.content();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let native = fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap();
    let invalid = invalid_commits(&fixture.graph);
    let rejections = fixture.node.replication_rejection_count();
    for commit in &invalid {
        native
            .publish(CraqleGraphEvent::CommittedMutation {
                id: MutationId::new(),
                graph: fixture.graph.clone(),
                changes: vec![fixture.insert("invalid")],
                render_hints: None,
                commit: Box::new(commit.clone()),
            })
            .unwrap();
        let unapplied = newest(&fixture.node, &fixture.graph);
        assert!(unapplied.rejected && unapplied.event.is_none());
    }
    fixture.node.reconcile_irokle().unwrap();
    assert_eq!(
        fixture.node.replication_rejection_count(),
        rejections + invalid.len() as u64
    );
    assert_eq!(fixture.content(), content);
    let heads = fixture.heads();
    let compare = HistoryCompare {
        graph: fixture.graph.clone(),
        from: original,
        to: heads.clone(),
        max_operations: 100,
        max_bytes: 1024 * 1024,
    };
    let diff = fixture
        .node
        .compare_history(&AllowAllAuthorizer, &compare)
        .unwrap();
    assert!(diff.changes.is_empty());
    native
        .publish(CraqleGraphEvent::CommittedMutation {
            id: MutationId::new(),
            graph: fixture.graph.clone(),
            changes: vec![fixture.insert("valid")],
            render_hints: None,
            commit: Box::new(commit("Peer change")),
        })
        .unwrap();
    fixture.node.reconcile_irokle().unwrap();
    let valid = newest(&fixture.node, &fixture.graph);
    assert_eq!(valid.commit(), Some(&commit("Peer change")));
    assert_eq!(valid.parents, heads);
    assert!(fixture.content().contains(&(
        EncodedTerm("<urn:commit>".into()),
        EncodedTerm("<urn:p>".into()),
        EncodedTerm("\"valid\"".into()),
    )));
    let view = fixture
        .node
        .project_history(&AllowAllAuthorizer, &fixture.request("committed"))
        .unwrap();
    assert_eq!(
        view.node.graph_snapshot(&fixture.graph).unwrap(),
        fixture.node.graph_snapshot(&fixture.graph).unwrap()
    );
}

#[test]
fn replays_plain_mutations() {
    let id = [1; 32];
    // Encoding of a `Mutation` event before `CommittedMutation` was appended.
    let mut stored = vec![4];
    stored.extend(id);
    stored.push(5);
    stored.extend(b"urn:g");
    stored.extend([0, 0]);
    let decoded = postcard::from_bytes::<CraqleGraphEvent>(&stored).unwrap();
    assert_eq!(
        decoded,
        CraqleGraphEvent::Mutation {
            id: MutationId(id),
            graph: GraphId::new("urn:g"),
            changes: Vec::new(),
            render_hints: None,
        }
    );
    let committed = CraqleGraphEvent::CommittedMutation {
        id: MutationId(id),
        graph: GraphId::new("urn:g"),
        changes: Vec::new(),
        render_hints: None,
        commit: Box::new(commit("New")),
    };
    let encoded = postcard::to_allocvec(&committed).unwrap();
    assert_eq!((encoded[0], &encoded[1..stored.len()]), (5, &stored[1..]));

    let fixture = Fixture::new();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let before = fixture.heads();
    fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::Mutation {
            id: MutationId::new(),
            graph: fixture.graph.clone(),
            changes: vec![fixture.insert("plain")],
            render_hints: None,
        })
        .unwrap();
    fixture.node.reconcile_irokle().unwrap();
    let plain = newest(&fixture.node, &fixture.graph);
    assert!(!plain.rejected);
    assert_eq!(plain.commit(), None);
    assert_eq!(plain.changes(), [fixture.insert("plain")]);
    let compare = HistoryCompare {
        graph: fixture.graph.clone(),
        from: before,
        to: fixture.heads(),
        max_operations: 100,
        max_bytes: 1024 * 1024,
    };
    let diff = fixture
        .node
        .compare_history(&AllowAllAuthorizer, &compare)
        .unwrap();
    assert_eq!(diff.changes, [fixture.insert("plain")]);
}

#[test]
fn restore_rechecks_policy() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    fixture.rename("Changed");
    let content = fixture.content();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let actor = irokle::actor_id_for(topic, fixture.native.peer_id());
    // Written by the node's own peer, so reconcile applies it without remote policy checks.
    fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::Policy {
            graph: fixture.graph.clone(),
            tagged: craqle::TaggedGraphPolicy {
                policy: GraphPolicy {
                    public: true,
                    permission_paths: vec!["/revoked".into()],
                },
                tag: craqle::PolicyTag {
                    counter: 100,
                    actor: craqle::ActorId::from_bytes(*actor.as_bytes()),
                },
            },
        })
        .unwrap();
    let auth = |graph: &GraphId, policy: &GraphPolicy, action: Action| {
        if action == Action::Read
            || policy
                .permission_paths
                .iter()
                .any(|path| path == "/history")
        {
            return Ok(());
        }
        Err(AuthorizationError::PermissionDenied {
            graph: graph.to_string(),
            action,
        })
    };
    let error = fixture
        .node
        .restore_history(&auth, &fixture.restore(&original, None))
        .unwrap_err();
    assert_eq!(error.kind(), CraqleErrorKind::Unauthorized);
    let policy = fixture.node.graph_policy(&fixture.graph).unwrap();
    assert_eq!(policy.permission_paths, ["/revoked"]);
    assert_eq!(fixture.content(), content);
}

#[test]
fn failed_restore_retries() {
    let fixture = Fixture::new();
    let original = fixture.heads();
    let content = fixture.content();
    fixture.rename("Changed");
    let admin = Irokle::builder()
        .with_storage(fixture.native.storage().clone())
        .with_signer(irokle::Ed25519Signer::from_bytes(&[77; 32]))
        .build()
        .unwrap();
    fixture
        .node
        .add_irokle_peer(&fixture.graph, admin.peer_id())
        .unwrap();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let control = admin.open_topic::<CraqleGraphEvent>(topic).unwrap();
    control.remove_peer(fixture.native.peer_id()).unwrap();
    let request = HistoryRestore {
        id: Some(MutationId::new()),
        ..fixture.restore(&original, None)
    };
    // Publishing fails while the node is not a topic member, leaving a prepared receipt.
    assert!(
        fixture
            .node
            .restore_history(&AllowAllAuthorizer, &request)
            .is_err()
    );
    control.add_peer(fixture.native.peer_id()).unwrap();
    let retry = fixture
        .node
        .restore_history(&AllowAllAuthorizer, &request)
        .unwrap();
    assert_eq!(retry.map(|restored| restored.id), request.id);
    assert_eq!(fixture.content(), content);
}

#[test]
fn rejects_reused_ids() {
    let fixture = Fixture::new();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    let native = fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap();
    let publish = |id: MutationId, label: &str| {
        native
            .publish(CraqleGraphEvent::Mutation {
                id,
                graph: fixture.graph.clone(),
                changes: vec![fixture.insert(label)],
                render_hints: None,
            })
            .unwrap()
    };
    let id = MutationId::new();
    publish(id, "first");
    fixture.node.reconcile_irokle().unwrap();
    let rejections = fixture.node.replication_rejection_count();
    publish(id, "conflicting");
    publish(MutationId::new(), "later");
    fixture.node.reconcile_irokle().unwrap();
    assert_eq!(fixture.node.replication_rejection_count(), rejections + 1);
    let content = fixture.content();
    let object = |label: &str| {
        content
            .iter()
            .any(|(_, _, object)| object.0 == format!("\"{label}\""))
    };
    assert!(object("first") && object("later") && !object("conflicting"));
}

#[test]
fn hides_store_rejections() {
    let fixture = Fixture::new();
    let topic = fixture
        .node
        .irokle_topic_id(&fixture.graph)
        .unwrap()
        .unwrap();
    // Decodes and targets this graph, but the store rejects a literal subject.
    fixture
        .native
        .open_topic::<CraqleGraphEvent>(topic)
        .unwrap()
        .publish(CraqleGraphEvent::QuadChanges {
            graph: fixture.graph.clone(),
            changes: vec![MaterializedQuadChange::Insert {
                graph: fixture.graph.clone(),
                subject: EncodedTerm("\"illegal subject\"".into()),
                predicate: EncodedTerm("<urn:p>".into()),
                object: EncodedTerm("\"unapplied\"".into()),
            }],
        })
        .unwrap();
    fixture.node.reconcile_irokle().unwrap();
    let log = HistoryLog {
        graph: fixture.graph.clone(),
        heads: fixture.heads(),
        limit: 1,
        max_bytes: 1024 * 1024,
    };
    let page = fixture.node.history_log(&AllowAllAuthorizer, &log).unwrap();
    let rejected = &page.operations[0];
    assert!(rejected.rejected && rejected.event.is_none());
    assert!(rejected.changes().is_empty());
}
