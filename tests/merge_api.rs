//! Checks public batch merge and snapshot installation semantics.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use chrono::Utc;
use craqle::*;

fn policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["/tests/merge".to_string()],
    }
}

fn actor(seed: u8) -> ActorId {
    ActorId::from_bytes([seed; 32])
}

fn node(root: &std::path::Path, name: &str) -> CraqleNode {
    CraqleNode::open_with_actor(root.join(name), actor(name.as_bytes()[0])).unwrap()
}

fn doc(graph: &GraphId, file_count: usize) -> String {
    let root = graph.as_str();
    let mut entries = vec![
        serde_json::json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
            "about": {"@id": root}
        }),
        serde_json::json!({
            "@id": root,
            "@type": "Dataset",
            "name": "Merge Dataset",
            "description": "Merge API test",
            "datePublished": "2026-06-10",
            "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
            "hasPart": (0..file_count)
                .map(|idx| serde_json::json!({"@id": format!("./data/file-{idx}.raw")}))
                .collect::<Vec<_>>()
        }),
    ];
    for idx in 0..file_count {
        entries.push(serde_json::json!({
            "@id": format!("./data/file-{idx}.raw"),
            "@type": "File",
            "name": format!("file-{idx}.raw")
        }));
    }
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": entries
    })
    .to_string()
}

fn apply_doc(node: &CraqleNode, graph: &GraphId, jsonld: &str) -> Batch {
    node.apply_rocrate_document_checked_with_policy(
        &AllowAllAuthorizer,
        graph.clone(),
        jsonld,
        policy(),
    )
    .unwrap()
}

fn keyword_change(graph: &GraphId, keyword: &str, insert: bool) -> MaterializedQuadChange {
    let subject = EncodedTerm::from_named_node(&graph.0);
    let predicate = EncodedTerm::from_named_node(&vocab::schema_keywords());
    let object = EncodedTerm(format!("\"{keyword}\""));
    if insert {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject,
            predicate,
            object,
        }
    } else {
        MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject,
            predicate,
            object,
        }
    }
}

fn objects_for(node: &CraqleNode, graph: &GraphId, predicate: &str) -> Vec<String> {
    node.graph_snapshot(graph)
        .unwrap()
        .quads
        .into_iter()
        .filter(|quad| quad.predicate.0 == format!("<{predicate}>"))
        .map(|quad| quad.object.0)
        .collect()
}

#[test]
fn merge_batch_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let author = node(tmp.path(), "author");
    let holder = node(tmp.path(), "holder");
    let graph = GraphId::new("https://w3id.org/aruna/merge-batch");
    let batch = apply_doc(&author, &graph, &doc(&graph, 2));

    let merged = holder.merge_batch(&batch).unwrap();
    assert!(merged.applied);
    assert_eq!(
        author.graph_snapshot(&graph).unwrap(),
        holder.graph_snapshot(&graph).unwrap()
    );
    assert!(!holder.merge_batch(&batch).unwrap().applied);
    assert_eq!(
        author.graph_snapshot(&graph).unwrap(),
        holder.graph_snapshot(&graph).unwrap()
    );
}

#[test]
fn concurrent_batches_converge() {
    // Both replicas merge both batches; add-wins, and the remove only takes
    // the dots it witnessed.
    let tmp = tempfile::tempdir().unwrap();
    let left = node(tmp.path(), "left");
    let right = node(tmp.path(), "right");
    let graph = GraphId::new("https://w3id.org/aruna/merge-concurrent");
    apply_doc(&left, &graph, &doc(&graph, 1));
    left.apply_changes(
        &AllowAllAuthorizer,
        &graph,
        vec![keyword_change(&graph, "x", true)],
    )
    .unwrap();
    assert!(
        right
            .install_graph_snapshot(&left.graph_snapshot(&graph).unwrap())
            .unwrap()
            .applied
    );

    let left_batch = Batch::from_changes(
        graph.clone(),
        actor(200),
        1,
        left.vector_clock(&graph).unwrap(),
        vec![
            keyword_change(&graph, "x", false),
            keyword_change(&graph, "y", true),
        ],
        Utc::now(),
    )
    .unwrap();
    let right_batch = Batch::from_changes(
        graph.clone(),
        actor(201),
        1,
        right.vector_clock(&graph).unwrap(),
        vec![keyword_change(&graph, "z", true)],
        Utc::now(),
    )
    .unwrap();

    for batch in [&left_batch, &right_batch] {
        assert!(left.merge_batch(batch).unwrap().applied);
    }
    for batch in [&right_batch, &left_batch] {
        assert!(right.merge_batch(batch).unwrap().applied);
    }

    let keywords = objects_for(&left, &graph, vocab::schema_keywords().as_str());
    assert_eq!(
        vec!["\"y\"".to_string(), "\"z\"".to_string()],
        {
            let mut sorted = keywords.clone();
            sorted.sort();
            sorted
        },
        "expected the concurrent adds and not the removed quad"
    );
    assert_eq!(
        left.graph_snapshot(&graph).unwrap(),
        right.graph_snapshot(&graph).unwrap()
    );
}

#[test]
fn install_then_remove() {
    // Installing twice changes nothing, and a batch witnessing the installed
    // dots still removes them.
    let tmp = tempfile::tempdir().unwrap();
    let author = node(tmp.path(), "author");
    let replica = node(tmp.path(), "replica");
    let graph = GraphId::new("https://w3id.org/aruna/merge-snapshot");
    apply_doc(&author, &graph, &doc(&graph, 1));
    author
        .apply_changes(
            &AllowAllAuthorizer,
            &graph,
            vec![keyword_change(&graph, "x", true)],
        )
        .unwrap();
    let snapshot = author.graph_snapshot(&graph).unwrap();

    assert!(replica.install_graph_snapshot(&snapshot).unwrap().applied);
    assert!(!replica.install_graph_snapshot(&snapshot).unwrap().applied);
    assert_eq!(snapshot, replica.graph_snapshot(&graph).unwrap());

    let removal = Batch::from_changes(
        graph.clone(),
        actor(202),
        1,
        snapshot.clock.clone(),
        vec![keyword_change(&graph, "x", false)],
        Utc::now(),
    )
    .unwrap();
    assert!(replica.merge_batch(&removal).unwrap().applied);
    assert!(objects_for(&replica, &graph, vocab::schema_keywords().as_str()).is_empty());
}

#[test]
fn plan_matches_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let planner = node(tmp.path(), "planner");
    let graph = GraphId::new("https://w3id.org/aruna/merge-plan");
    apply_doc(&planner, &graph, &doc(&graph, 1));
    let before = planner.graph_snapshot(&graph).unwrap();

    let jsonld = doc(&graph, 3);
    let planned = planner
        .plan_rocrate_document_checked(&AllowAllAuthorizer, &graph, &jsonld)
        .unwrap();
    assert!(!planned.is_empty());
    assert_eq!(before, planner.graph_snapshot(&graph).unwrap());

    let batch = apply_doc(&planner, &graph, &jsonld);
    let applied = batch
        .ops
        .iter()
        .map(|op| match op {
            QuadOp::Add {
                subject,
                predicate,
                object,
                ..
            } => MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            },
            QuadOp::Remove {
                subject,
                predicate,
                object,
                ..
            } => MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
            },
        })
        .collect::<Vec<_>>();
    assert_eq!(planned, applied);
}

#[test]
fn plan_patch_merges() {
    let tmp = tempfile::tempdir().unwrap();
    let planner = node(tmp.path(), "planner");
    let graph = GraphId::new("https://w3id.org/aruna/merge-plan-patch");
    apply_doc(&planner, &graph, &doc(&graph, 1));
    let before = planner.graph_snapshot(&graph).unwrap();

    let request = PatchEntityRequest {
        entity: CreateEntityRequest {
            graph: graph.clone(),
            entity_id: "./data/file-0.raw".to_string(),
            entity_type: "File".to_string(),
            name: "renamed.raw".to_string(),
            additional_triples: Vec::new(),
        },
        replaced_predicates: vec![vocab::schema_name()],
    };
    let planned = planner
        .plan_patch_data(&AllowAllAuthorizer, &request)
        .unwrap();
    assert!(!planned.is_empty());
    assert_eq!(before, planner.graph_snapshot(&graph).unwrap());

    let holder = node(tmp.path(), "holder");
    assert!(holder.install_graph_snapshot(&before).unwrap().applied);
    let batch = Batch::from_changes(
        graph.clone(),
        actor(203),
        1,
        planner.vector_clock(&graph).unwrap(),
        planned,
        Utc::now(),
    )
    .unwrap();
    assert!(planner.merge_batch(&batch).unwrap().applied);
    assert!(holder.merge_batch(&batch).unwrap().applied);
    assert_eq!(
        planner.graph_snapshot(&graph).unwrap(),
        holder.graph_snapshot(&graph).unwrap()
    );
    assert!(
        objects_for(&planner, &graph, vocab::schema_name().as_str())
            .contains(&"\"renamed.raw\"".to_string())
    );
}

#[test]
fn plan_remove_merges() {
    let tmp = tempfile::tempdir().unwrap();
    let planner = node(tmp.path(), "planner");
    let graph = GraphId::new("https://w3id.org/aruna/merge-plan-remove");
    apply_doc(&planner, &graph, &doc(&graph, 2));
    let before = planner.graph_snapshot(&graph).unwrap();
    let remove = |entity_id: &str| RemoveEntityRequest {
        graph: graph.clone(),
        entity_id: entity_id.to_string(),
    };
    for refused in [
        graph.as_str(),
        "ro-crate-metadata.json",
        "./data/missing.raw",
    ] {
        let error = planner.plan_remove_data(&AllowAllAuthorizer, &remove(refused));
        assert!(error.is_err(), "{refused}");
    }

    let planned = planner
        .plan_remove_data(&AllowAllAuthorizer, &remove("./data/file-0.raw"))
        .unwrap();
    assert_eq!(before, planner.graph_snapshot(&graph).unwrap());
    let holder = node(tmp.path(), "holder");
    assert!(holder.install_graph_snapshot(&before).unwrap().applied);
    let batch = Batch::from_changes(
        graph.clone(),
        actor(205),
        1,
        planner.vector_clock(&graph).unwrap(),
        planned,
        Utc::now(),
    )
    .unwrap();
    assert!(planner.merge_batch(&batch).unwrap().applied);
    assert!(holder.merge_batch(&batch).unwrap().applied);
    assert_eq!(
        planner.graph_snapshot(&graph).unwrap(),
        holder.graph_snapshot(&graph).unwrap()
    );
    // The removed file and its link are gone; the other file, its link and the root stay.
    let parts = objects_for(&planner, &graph, vocab::schema_has_part().as_str());
    assert_eq!(parts.len(), 1);
    assert!(parts[0].contains("file-1.raw"), "{parts:?}");
    let names = objects_for(&planner, &graph, vocab::schema_name().as_str());
    assert!(names.contains(&"\"file-1.raw\"".to_string()));
    assert!(!names.contains(&"\"file-0.raw\"".to_string()));
    assert!(names.contains(&"\"Merge Dataset\"".to_string()));
    let again = planner.plan_remove_data(&AllowAllAuthorizer, &remove("./data/file-0.raw"));
    assert!(again.is_err());
}

#[test]
fn rejects_foreign_graph() {
    let graph = GraphId::new("https://w3id.org/aruna/merge-builder");
    let other = GraphId::new("https://w3id.org/aruna/merge-builder-other");
    let error = Batch::from_changes(
        graph.clone(),
        actor(204),
        1,
        VectorClock::new(),
        vec![keyword_change(&other, "x", true)],
        Utc::now(),
    )
    .unwrap_err();
    assert_eq!(graph, error.expected);
    assert_eq!(other, error.actual);
}

#[test]
fn stale_snapshot_ignored() {
    // A device seeded from a current holder must not get a removed quad back
    // from a lagging holder's snapshot.
    let tmp = tempfile::tempdir().unwrap();
    let alpha = node(tmp.path(), "alpha");
    let beta = node(tmp.path(), "beta");
    let device = node(tmp.path(), "device");
    let graph = GraphId::new("https://w3id.org/aruna/merge-stale");
    apply_doc(&alpha, &graph, &doc(&graph, 1));
    alpha
        .apply_changes(
            &AllowAllAuthorizer,
            &graph,
            vec![keyword_change(&graph, "x", true)],
        )
        .unwrap();
    assert!(
        beta.install_graph_snapshot(&alpha.graph_snapshot(&graph).unwrap())
            .unwrap()
            .applied
    );

    let removal = Batch::from_changes(
        graph.clone(),
        actor(210),
        1,
        alpha.vector_clock(&graph).unwrap(),
        vec![keyword_change(&graph, "x", false)],
        Utc::now(),
    )
    .unwrap();
    assert!(alpha.merge_batch(&removal).unwrap().applied);
    assert!(objects_for(&alpha, &graph, vocab::schema_keywords().as_str()).is_empty());

    let current = alpha.graph_snapshot(&graph).unwrap();
    assert!(device.install_graph_snapshot(&current).unwrap().applied);
    let stale = beta.graph_snapshot(&graph).unwrap();
    assert!(
        stale.quads.iter().any(|quad| quad.object.0 == "\"x\""),
        "the lagging holder must still carry the removed quad"
    );
    assert!(!device.install_graph_snapshot(&stale).unwrap().applied);
    assert_eq!(current, device.graph_snapshot(&graph).unwrap());

    assert!(beta.merge_batch(&removal).unwrap().applied);
    assert_eq!(current, beta.graph_snapshot(&graph).unwrap());
}

/// Literal aliases from peers that predate canonical storage are one value in any merge order.
#[test]
fn replicated_aliases_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let author = node(tmp.path(), "author");
    let replica = node(tmp.path(), "replica");
    let graph = GraphId::new("https://w3id.org/aruna/merge-alias");
    let setup = apply_doc(&author, &graph, &doc(&graph, 1));
    let base = author.vector_clock(&graph).unwrap();
    let local = author
        .apply_changes(
            &AllowAllAuthorizer,
            &graph,
            vec![keyword_change(&graph, "x", true)],
        )
        .unwrap();
    let subject = EncodedTerm::from_named_node(&graph.0);
    let predicate = EncodedTerm::from_named_node(&vocab::schema_keywords());
    let object = EncodedTerm("\"x\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string());
    let add = MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: subject.clone(),
        predicate: predicate.clone(),
        object: object.clone(),
    };
    let legacy = Batch::from_changes(graph.clone(), actor(220), 1, base, vec![add], Utc::now());
    let legacy = legacy.unwrap();

    assert!(author.merge_batch(&legacy).unwrap().applied);
    for batch in [&setup, &legacy, &local] {
        assert!(replica.merge_batch(batch).unwrap().applied);
    }
    let keywords = vocab::schema_keywords();
    for holder in [&author, &replica] {
        assert_eq!(objects_for(holder, &graph, keywords.as_str()), ["\"x\""]);
    }
    assert_eq!(
        author.graph_snapshot(&graph).unwrap(),
        replica.graph_snapshot(&graph).unwrap()
    );
    // An older holder's snapshot keeps the two spellings apart.
    let mut split = author.graph_snapshot(&graph).unwrap();
    let index = split.quads.iter().position(|quad| quad.object.0 == "\"x\"");
    let aliased = &mut split.quads[index.unwrap()];
    let mut raw = aliased.clone();
    raw.object = object.clone();
    raw.dots = vec![aliased.dots.pop().unwrap()];
    split.quads.push(raw);
    assert!(!replica.install_graph_snapshot(&split).unwrap().applied);
    assert_eq!(
        author.graph_snapshot(&graph).unwrap(),
        replica.graph_snapshot(&graph).unwrap()
    );

    let delete = MaterializedQuadChange::Delete {
        graph: graph.clone(),
        subject,
        predicate,
        object,
    };
    let clock = author.vector_clock(&graph).unwrap();
    let removal = Batch::from_changes(
        graph.clone(),
        actor(220),
        2,
        clock,
        vec![delete],
        Utc::now(),
    );
    let removal = removal.unwrap();
    for holder in [&author, &replica] {
        assert!(holder.merge_batch(&removal).unwrap().applied);
        assert!(objects_for(holder, &graph, keywords.as_str()).is_empty());
    }
}

/// A batch of `changes` planned on `node` by a fresh `actor`.
fn planned_batch(
    node: &CraqleNode,
    graph: &GraphId,
    (seed, changes): (u8, Vec<MaterializedQuadChange>),
) -> Batch {
    let clock = node.vector_clock(graph).unwrap();
    Batch::from_changes(graph.clone(), actor(seed), 1, clock, changes, Utc::now()).unwrap()
}

/// The quads of `graph` about `subject`, and whether the root links it.
fn entity_state(
    node: &CraqleNode,
    graph: &GraphId,
    subject: &str,
) -> (Vec<(String, String)>, bool) {
    let quads = node.graph_snapshot(graph).unwrap().quads;
    let own = (quads.iter())
        .filter(|quad| quad.subject.0.contains(subject))
        .map(|quad| (quad.predicate.0.clone(), quad.object.0.clone()))
        .collect();
    let linked = quads.iter().any(|quad| {
        quad.predicate.0 == format!("<{}>", vocab::schema_has_part().as_str())
            && quad.object.0.contains(subject)
    });
    (own, linked)
}

#[test]
fn removal_meets_patch() {
    // A file renamed on one replica while another removes it keeps its type and root link with
    // the new name, whichever batch merges first: the concurrent write wins.
    let tmp = tempfile::tempdir().unwrap();
    let graph = GraphId::new("https://w3id.org/aruna/merge-remove-patch");
    let origin = node(tmp.path(), "origin");
    apply_doc(&origin, &graph, &doc(&graph, 2));
    let base = origin.graph_snapshot(&graph).unwrap();
    let replica = |name: &str| {
        let replica = node(tmp.path(), name);
        assert!(replica.install_graph_snapshot(&base).unwrap().applied);
        replica
    };
    let (remover, patcher) = (replica("remover"), replica("patcher"));
    let removal = RemoveEntityRequest {
        graph: graph.clone(),
        entity_id: "./data/file-0.raw".to_string(),
    };
    let removal = remover
        .plan_remove_data(&AllowAllAuthorizer, &removal)
        .unwrap();
    let removal = planned_batch(&remover, &graph, (210, removal));
    let patch = PatchEntityRequest {
        entity: CreateEntityRequest {
            graph: graph.clone(),
            entity_id: "./data/file-0.raw".to_string(),
            entity_type: "File".to_string(),
            name: "renamed.raw".to_string(),
            additional_triples: Vec::new(),
        },
        replaced_predicates: vec![vocab::schema_name()],
    };
    let patch = patcher
        .plan_patch_data(&AllowAllAuthorizer, &patch)
        .unwrap();
    let patch = planned_batch(&patcher, &graph, (211, patch));
    let (first, second) = (replica("first"), replica("second"));
    for batch in [&removal, &patch] {
        assert!(first.merge_batch(batch).unwrap().applied);
    }
    for batch in [&patch, &removal] {
        assert!(second.merge_batch(batch).unwrap().applied);
    }
    assert_eq!(
        first.graph_snapshot(&graph).unwrap(),
        second.graph_snapshot(&graph).unwrap()
    );
    let (own, linked) = entity_state(&first, &graph, "file-0.raw");
    assert!(linked, "{own:?}");
    let kind = format!("<{}>", vocab::rdf_type().as_str());
    assert!(
        own.iter().any(|(predicate, _)| *predicate == kind),
        "{own:?}"
    );
    let name = format!("<{}>", vocab::schema_name().as_str());
    let names: Vec<&String> = (own.iter())
        .filter(|(predicate, _)| *predicate == name)
        .map(|(_, object)| object)
        .collect();
    assert_eq!(names, ["\"renamed.raw\""]);
}

#[test]
fn removal_needs_file() {
    // Linked contextual entities are refused without a change; a removed file's incoming
    // references from other subjects stay.
    let tmp = tempfile::tempdir().unwrap();
    let planner = node(tmp.path(), "planner");
    let graph = GraphId::new("https://w3id.org/aruna/merge-remove-file");
    let root = graph.as_str();
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
             "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}, "about": {"@id": root}},
            {"@id": root, "@type": "Dataset", "name": "Files", "description": "d",
             "datePublished": "2026-06-10",
             "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
             "hasPart": [{"@id": "./a.raw"}, {"@id": "#alice"}, {"@id": "#notes"},
                         {"@id": "#output"}]},
            {"@id": "./a.raw", "@type": "File", "name": "a.raw", "author": {"@id": "#alice"}},
            {"@id": "#output", "@type": "File", "name": "Output"},
            {"@id": "#alice", "@type": "Person", "name": "Alice"},
            {"@id": "#notes", "@type": "CreativeWork", "name": "Notes", "about": {"@id": "./a.raw"}}
        ]
    });
    apply_doc(&planner, &graph, &jsonld.to_string());
    let before = planner.graph_snapshot(&graph).unwrap();
    let remove = |entity_id: &str| RemoveEntityRequest {
        graph: graph.clone(),
        entity_id: entity_id.to_string(),
    };
    for contextual in ["#alice", "#notes", "#output"] {
        let refused = planner.plan_remove_data(&AllowAllAuthorizer, &remove(contextual));
        assert!(refused.is_err(), "{contextual}");
    }
    assert_eq!(before, planner.graph_snapshot(&graph).unwrap());
    let planned = planner
        .plan_remove_data(&AllowAllAuthorizer, &remove("./a.raw"))
        .unwrap();
    let batch = planned_batch(&planner, &graph, (212, planned));
    assert!(planner.merge_batch(&batch).unwrap().applied);
    let about = objects_for(&planner, &graph, vocab::schema_about().as_str());
    assert!(
        about.iter().any(|object| object.contains("a.raw")),
        "{about:?}"
    );
    let (own, linked) = entity_state(&planner, &graph, "a.raw");
    assert!(!linked);
    assert!(own.is_empty(), "{own:?}");
}
