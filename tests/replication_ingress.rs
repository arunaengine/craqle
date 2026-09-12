//! Remote input must be rejected before it can change any durable state.

use chrono::Utc;
use craqle::*;
use std::path::Path;

const SUBJECT: &str = "<urn:test:ingress:s>";
const PREDICATE: &str = "<urn:test:ingress:p>";
/// Mirrors the per-term cap the validators enforce.
const TERM_BYTES: usize = 4 * 1024 * 1024;

fn actor(id: u8) -> ActorId {
    ActorId::from_bytes([id; 32])
}

fn open(root: &Path, name: &str, id: u8) -> CraqleNode {
    CraqleNode::open_with_actor(root.join(name), actor(id)).unwrap()
}

fn dot(entry: (u8, u64)) -> Dot {
    Dot {
        actor: actor(entry.0),
        counter: entry.1,
    }
}

fn insert(graph: &GraphId, object: &str) -> MaterializedQuadChange {
    MaterializedQuadChange::Insert {
        graph: graph.clone(),
        subject: EncodedTerm(SUBJECT.to_owned()),
        predicate: EncodedTerm(PREDICATE.to_owned()),
        object: EncodedTerm(object.to_owned()),
    }
}

fn valid(graph: &GraphId) -> Batch {
    Batch::from_changes(
        graph.clone(),
        actor(1),
        1,
        VectorClock::new(),
        [insert(graph, "\"x\"")],
        Utc::now(),
    )
    .unwrap()
}

fn quad(object: &str, dots: Vec<Dot>) -> SnapshotQuadState {
    SnapshotQuadState {
        subject: EncodedTerm(SUBJECT.to_owned()),
        predicate: EncodedTerm(PREDICATE.to_owned()),
        object: EncodedTerm(object.to_owned()),
        dots,
    }
}

#[derive(Clone, Copy, Debug)]
enum Place {
    Subject,
    Predicate,
    Object,
}

/// Replace one position of the batch's single add.
fn poison(incoming: &mut Batch, place: Place, term: &str) {
    let QuadOp::Add {
        subject,
        predicate,
        object,
        ..
    } = &mut incoming.ops[0]
    else {
        panic!("expected an add op");
    };
    let slot = match place {
        Place::Subject => subject,
        Place::Predicate => predicate,
        Place::Object => object,
    };
    *slot = EncodedTerm(term.to_owned());
}

/// Everything a rejection must leave untouched: quad rows with their dots, the
/// graph clock, and whether the graph exists at all. A graph this node does
/// not hold fingerprints the empty hash, an existing empty one zeroes.
fn durable(
    node: &CraqleNode,
    graph: &GraphId,
) -> (
    GraphReplicaSnapshot,
    VectorClock,
    (u64, [u8; 32], [u8; 32]),
) {
    (
        node.graph_snapshot(graph).unwrap(),
        node.vector_clock(graph).unwrap(),
        node.graph_fingerprint(graph).unwrap(),
    )
}

fn rejects(node: &CraqleNode, incoming: &Batch) -> CraqleError {
    let before = durable(node, &incoming.graph);
    let error = node
        .merge_batch(incoming)
        .expect_err("the batch must be rejected");
    assert_eq!(
        before,
        durable(node, &incoming.graph),
        "a rejected batch must change nothing"
    );
    assert!(
        matches!(error, CraqleError::Merge(MergeError::InputRejected(_))),
        "expected an input rejection, got {error}"
    );
    assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
    error
}

fn refuses(node: &CraqleNode, incoming: &GraphReplicaSnapshot) -> CraqleError {
    let before = durable(node, &incoming.graph);
    let error = node
        .install_graph_snapshot(incoming)
        .expect_err("the snapshot must be rejected");
    assert_eq!(
        before,
        durable(node, &incoming.graph),
        "a rejected snapshot must change nothing"
    );
    assert!(
        matches!(error, CraqleError::Merge(MergeError::InputRejected(_))),
        "expected an input rejection, got {error}"
    );
    assert_eq!(error.kind(), CraqleErrorKind::InvalidInput);
    error
}

#[test]
fn rejects_literal_predicate() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "predicate", 30);
    let graph = GraphId::new("urn:test:ingress:literal-predicate");
    let mut incoming = valid(&graph);
    poison(&mut incoming, Place::Predicate, "\"not-an-iri\"");
    rejects(&node, &incoming);
}

#[test]
fn rejects_literal_subject() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "subject", 31);
    let graph = GraphId::new("urn:test:ingress:literal-subject");
    let mut incoming = valid(&graph);
    poison(&mut incoming, Place::Subject, "\"not-a-node\"");
    rejects(&node, &incoming);
}

#[test]
fn rejects_relative_iri() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "relative", 32);
    let graph = GraphId::new("urn:test:ingress:relative-iri");
    for place in [Place::Subject, Place::Predicate, Place::Object] {
        let mut incoming = valid(&graph);
        poison(&mut incoming, place, "<not-absolute>");
        rejects(&node, &incoming);
    }
}

#[test]
fn rejects_partial_literal() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "partial", 33);
    let graph = GraphId::new("urn:test:ingress:partial-literal");
    let mut incoming = valid(&graph);
    poison(&mut incoming, Place::Object, "\"unterminated");
    rejects(&node, &incoming);
}

#[test]
fn rejects_trailing_data() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "trailing", 34);
    let graph = GraphId::new("urn:test:ingress:trailing-data");
    for term in ["\"x\" and more", "\"x\"^^<urn:test:d> junk", "<urn:a> <urn:b>"] {
        let mut incoming = valid(&graph);
        poison(&mut incoming, Place::Object, term);
        rejects(&node, &incoming);
    }
}

#[test]
fn rejects_malformed_terms() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "malformed", 35);
    let graph = GraphId::new("urn:test:ingress:malformed");
    let malformed = [
        "",
        "<",
        "<>",
        "<urn:test:bad space>",
        "\"",
        "\"x\"@",
        "\"x\"^^<>",
        "\"x\"^^<not-absolute>",
        "\"x\"@not a tag",
        "_:",
        "_:has space",
        "bare",
        "<<urn:a>>",
    ];
    for term in malformed {
        let mut incoming = valid(&graph);
        poison(&mut incoming, Place::Object, term);
        let error = rejects(&node, &incoming);
        assert!(
            !format!("{error}").is_empty(),
            "rejection of `{term}` must be reported"
        );
    }
}

#[test]
fn rejects_star_term() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "star", 36);
    let graph = GraphId::new("urn:test:ingress:star");
    let mut incoming = valid(&graph);
    poison(
        &mut incoming,
        Place::Subject,
        "<<<urn:a> <urn:b> <urn:c>>>",
    );
    rejects(&node, &incoming);
}

#[test]
fn accepts_blank_nodes() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "blank", 37);
    let graph = GraphId::new("urn:test:ingress:blank");
    let mut incoming = valid(&graph);
    poison(&mut incoming, Place::Subject, "_:b0");
    poison(&mut incoming, Place::Object, "_:b1");
    assert!(node.merge_batch(&incoming).unwrap().applied);

    let quads = node.graph_snapshot(&graph).unwrap().quads;
    assert_eq!(quads.len(), 1);
    assert_eq!(quads[0].subject.0, "_:b0");
    assert_eq!(quads[0].object.0, "_:b1");
    assert_eq!(quads[0].dots, vec![dot((1, 1))]);
}

#[test]
fn rejects_dot_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "mismatch", 38);
    let graph = GraphId::new("urn:test:ingress:dot-mismatch");
    for wrong in [dot((1, 2)), dot((2, 1))] {
        let mut incoming = valid(&graph);
        let QuadOp::Add { dot: slot, .. } = &mut incoming.ops[0] else {
            panic!("expected an add op");
        };
        *slot = wrong;
        rejects(&node, &incoming);
    }
}

#[test]
fn rejects_unwitnessed_remove() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "witnessed", 39);
    let graph = GraphId::new("urn:test:ingress:unwitnessed");
    let mut incoming = Batch::from_changes(
        graph.clone(),
        actor(1),
        1,
        VectorClock::new(),
        [MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: EncodedTerm(SUBJECT.to_owned()),
            predicate: EncodedTerm(PREDICATE.to_owned()),
            object: EncodedTerm("\"x\"".to_owned()),
        }],
        Utc::now(),
    )
    .unwrap();
    let QuadOp::Remove { witnessed, .. } = &mut incoming.ops[0] else {
        panic!("expected a remove op");
    };
    // The remove claims to have observed state the batch itself does not
    // declare, so it could drop dots the author never saw.
    witnessed.advance(actor(7), 9);
    rejects(&node, &incoming);
}

#[test]
fn rejects_invalid_graph() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "graph", 40);
    let graph = GraphId::new("not an iri");
    rejects(&node, &valid(&graph));
}

#[test]
fn rejects_wide_clock() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "wide", 41);
    let graph = GraphId::new("urn:test:ingress:wide-clock");
    let mut incoming = valid(&graph);
    for index in 0..65_537u32 {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&index.to_be_bytes());
        incoming.base_clock.advance(ActorId::from_bytes(bytes), 1);
    }
    rejects(&node, &incoming);
}

#[test]
fn rejects_large_envelope() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "large", 42);
    let graph = GraphId::new("urn:test:ingress:large-envelope");
    let filler = format!("\"{}\"", "a".repeat(TERM_BYTES - 2));
    let changes = (0..17).map(|_| insert(&graph, &filler)).collect::<Vec<_>>();
    let incoming = Batch::from_changes(
        graph.clone(),
        actor(1),
        1,
        VectorClock::new(),
        changes,
        Utc::now(),
    )
    .unwrap();
    rejects(&node, &incoming);
}

#[test]
fn rejects_uncovered_dot() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "uncovered", 43);
    let graph = GraphId::new("urn:test:ingress:uncovered-dot");
    let incoming = GraphReplicaSnapshot {
        graph: graph.clone(),
        clock: VectorClock::new(),
        quads: vec![quad("\"x\"", vec![dot((1, 1))])],
    };
    refuses(&node, &incoming);
}

#[test]
fn rejects_duplicate_quads() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "duplicate", 44);
    let graph = GraphId::new("urn:test:ingress:duplicate-quads");
    let mut clock = VectorClock::new();
    clock.advance(actor(1), 2);
    let incoming = GraphReplicaSnapshot {
        graph: graph.clone(),
        clock,
        quads: vec![
            quad("\"x\"", vec![dot((1, 1))]),
            quad("\"x\"", vec![dot((1, 2))]),
        ],
    };
    refuses(&node, &incoming);
}

#[test]
fn rejects_duplicate_dots() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "repeated", 45);
    let graph = GraphId::new("urn:test:ingress:duplicate-dots");
    let mut clock = VectorClock::new();
    clock.advance(actor(1), 1);
    let incoming = GraphReplicaSnapshot {
        graph: graph.clone(),
        clock,
        quads: vec![quad("\"x\"", vec![dot((1, 1)), dot((1, 1))])],
    };
    refuses(&node, &incoming);
}

#[test]
fn rejects_empty_dots() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "liveness", 46);
    let graph = GraphId::new("urn:test:ingress:empty-dots");
    let incoming = GraphReplicaSnapshot {
        graph: graph.clone(),
        clock: VectorClock::new(),
        quads: vec![quad("\"x\"", Vec::new())],
    };
    refuses(&node, &incoming);
}

#[test]
fn rejects_many_dots() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "dots", 47);
    let graph = GraphId::new("urn:test:ingress:many-dots");
    let count = 1_048_577u64;
    let mut clock = VectorClock::new();
    clock.advance(actor(1), count);
    let dots = (1..=count).map(|counter| dot((1, counter))).collect();
    let incoming = GraphReplicaSnapshot {
        graph: graph.clone(),
        clock,
        quads: vec![quad("\"x\"", dots)],
    };
    refuses(&node, &incoming);
}
