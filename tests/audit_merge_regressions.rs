//! Snapshot-join and external-batch causality contracts.

use chrono::Utc;
use craqle::*;
use std::collections::BTreeMap;
use std::path::Path;

const SUBJECT: &str = "<urn:test:merge:s>";
const PREDICATE: &str = "<urn:test:merge:p>";

fn actor(id: u8) -> ActorId {
    ActorId::from_bytes([id; 32])
}

fn open(root: &Path, name: &str, id: u8) -> CraqleNode {
    CraqleNode::open_with_actor(root.join(name), actor(id)).unwrap()
}

fn clock(entries: &[(u8, u64)]) -> VectorClock {
    let mut value = VectorClock::new();
    for &(id, counter) in entries {
        value.advance(actor(id), counter);
    }
    value
}

fn dot(entry: (u8, u64)) -> Dot {
    Dot {
        actor: actor(entry.0),
        counter: entry.1,
    }
}

fn change(graph: &GraphId, object: &str, insert: bool) -> MaterializedQuadChange {
    let subject = EncodedTerm(SUBJECT.to_owned());
    let predicate = EncodedTerm(PREDICATE.to_owned());
    let object = EncodedTerm(format!("\"{object}\""));
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

/// One external batch: its graph, its event identity, its causal base and the
/// single change it carries.
struct Event<'a> {
    graph: &'a GraphId,
    dot: Dot,
    base: VectorClock,
    object: &'a str,
    insert: bool,
}

fn batch(event: Event<'_>) -> Batch {
    Batch::from_changes(
        event.graph.clone(),
        event.dot.actor,
        event.dot.counter,
        event.base,
        [change(event.graph, event.object, event.insert)],
        Utc::now(),
    )
    .unwrap()
}

fn quad(object: &str, dots: &[(u8, u64)]) -> SnapshotQuadState {
    SnapshotQuadState {
        subject: EncodedTerm(SUBJECT.to_owned()),
        predicate: EncodedTerm(PREDICATE.to_owned()),
        object: EncodedTerm(format!("\"{object}\"")),
        dots: dots.iter().copied().map(dot).collect(),
    }
}

/// One replica state without a graph name: its causal context and live quads.
struct State {
    clock: VectorClock,
    quads: Vec<SnapshotQuadState>,
}

fn state(clock: VectorClock, quads: Vec<SnapshotQuadState>) -> State {
    State { clock, quads }
}

fn snap(graph: &GraphId, state: &State) -> GraphReplicaSnapshot {
    GraphReplicaSnapshot {
        graph: graph.clone(),
        clock: state.clock.clone(),
        quads: state.quads.clone(),
    }
}

fn empty(graph: &GraphId) -> GraphReplicaSnapshot {
    GraphReplicaSnapshot {
        graph: graph.clone(),
        clock: VectorClock::new(),
        quads: Vec::new(),
    }
}

/// Snapshot content without the graph name, so results held under different
/// test graphs compare directly.
fn content(snapshot: &GraphReplicaSnapshot) -> (VectorClock, Vec<SnapshotQuadState>) {
    (snapshot.clock.clone(), snapshot.quads.clone())
}

fn normalize(snapshot: &mut GraphReplicaSnapshot) {
    for quad in &mut snapshot.quads {
        quad.dots
            .sort_unstable_by_key(|dot| (dot.actor, dot.counter));
        quad.dots.dedup();
    }
    snapshot.quads.retain(|quad| !quad.dots.is_empty());
    snapshot.quads.sort_unstable_by(|left, right| {
        (&left.subject, &left.predicate, &left.object).cmp(&(
            &right.subject,
            &right.predicate,
            &right.object,
        ))
    });
}

type QuadKey = (EncodedTerm, EncodedTerm, EncodedTerm);

fn index(snapshot: &GraphReplicaSnapshot) -> BTreeMap<QuadKey, Vec<Dot>> {
    snapshot
        .quads
        .iter()
        .map(|quad| {
            (
                (
                    quad.subject.clone(),
                    quad.predicate.clone(),
                    quad.object.clone(),
                ),
                quad.dots.clone(),
            )
        })
        .collect()
}

/// Independent model of the OR-Set snapshot join, kept separate from the
/// engine so the engine can be checked against it.
fn oracle(left: &GraphReplicaSnapshot, right: &GraphReplicaSnapshot) -> GraphReplicaSnapshot {
    let (here, there) = (index(left), index(right));
    let mut keys = here.keys().chain(there.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let mut quads = Vec::new();
    for key in keys {
        let local = here.get(&key).cloned().unwrap_or_default();
        let remote = there.get(&key).cloned().unwrap_or_default();
        let mut dots = local
            .iter()
            .copied()
            .filter(|dot| remote.contains(dot) || !right.clock.contains(dot))
            .collect::<Vec<_>>();
        dots.extend(
            remote
                .iter()
                .copied()
                .filter(|dot| !local.contains(dot) && !left.clock.contains(dot)),
        );
        quads.push(SnapshotQuadState {
            subject: key.0,
            predicate: key.1,
            object: key.2,
            dots,
        });
    }
    let mut clock = left.clock.clone();
    clock.merge(&right.clock);
    let mut joined = GraphReplicaSnapshot {
        graph: left.graph.clone(),
        clock,
        quads,
    };
    normalize(&mut joined);
    joined
}

fn objects(node: &CraqleNode, graph: &GraphId) -> Vec<String> {
    let mut result = node
        .graph_snapshot(graph)
        .unwrap()
        .quads
        .into_iter()
        .filter(|quad| quad.predicate.0 == PREDICATE)
        .map(|quad| quad.object.0)
        .collect::<Vec<_>>();
    result.sort();
    result
}

/// Everything an ignored or rejected merge must leave untouched: quad rows
/// with their dots, the graph clock, and whether the graph exists at all. A
/// missing graph fingerprints the empty hash, an existing empty one zeroes.
fn durable(node: &CraqleNode, graph: &GraphId) -> (GraphReplicaSnapshot, VectorClock, u64) {
    (
        node.graph_snapshot(graph).unwrap(),
        node.vector_clock(graph).unwrap(),
        node.graph_fingerprint(graph).unwrap().0,
    )
}

/// Four causally consistent replica states over two quads and three actors.
fn family() -> Vec<State> {
    vec![
        state(clock(&[(1, 1)]), vec![quad("x", &[(1, 1)])]),
        state(clock(&[(1, 2)]), Vec::new()),
        state(clock(&[(1, 1), (2, 1)]), vec![quad("x", &[(1, 1), (2, 1)])]),
        state(clock(&[(3, 1)]), vec![quad("y", &[(3, 1)])]),
    ]
}

#[test]
fn join_order_agrees() {
    let temp = tempfile::tempdir().unwrap();
    let left = open(temp.path(), "left", 10);
    let right = open(temp.path(), "right", 11);
    let graph = GraphId::new("urn:test:merge:join-order");
    let old = snap(&graph, &state(clock(&[(1, 1)]), vec![quad("x", &[(1, 1)])]));
    let new = snap(&graph, &state(clock(&[(1, 2)]), Vec::new()));

    left.install_graph_snapshot(&old).unwrap();
    left.install_graph_snapshot(&new).unwrap();
    right.install_graph_snapshot(&new).unwrap();
    right.install_graph_snapshot(&old).unwrap();

    let expected = oracle(&oracle(&empty(&graph), &old), &new);
    assert!(expected.quads.is_empty(), "the model must drop the old dot");
    assert_eq!(left.graph_snapshot(&graph).unwrap(), expected);
    assert_eq!(right.graph_snapshot(&graph).unwrap(), expected);
}

#[test]
fn join_keeps_concurrent() {
    let temp = tempfile::tempdir().unwrap();
    let forward = open(temp.path(), "forward", 12);
    let backward = open(temp.path(), "backward", 13);
    let graph = GraphId::new("urn:test:merge:join-concurrent");
    let concurrent = snap(
        &graph,
        &state(clock(&[(1, 1), (2, 1)]), vec![quad("x", &[(1, 1), (2, 1)])]),
    );
    let removed = snap(&graph, &state(clock(&[(1, 2)]), Vec::new()));

    forward.install_graph_snapshot(&concurrent).unwrap();
    forward.install_graph_snapshot(&removed).unwrap();
    backward.install_graph_snapshot(&removed).unwrap();
    backward.install_graph_snapshot(&concurrent).unwrap();

    let mut expected = snap(
        &graph,
        &state(clock(&[(1, 2), (2, 1)]), vec![quad("x", &[(2, 1)])]),
    );
    normalize(&mut expected);
    assert_eq!(forward.graph_snapshot(&graph).unwrap(), expected);
    assert_eq!(backward.graph_snapshot(&graph).unwrap(), expected);
}

#[test]
fn join_covers_union() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "union", 14);
    let graph = GraphId::new("urn:test:merge:join-union");
    let local = snap(
        &graph,
        &state(
            clock(&[(1, 2), (3, 1)]),
            vec![
                quad("a", &[(1, 1)]),
                quad("b", &[(1, 2)]),
                quad("c", &[(3, 1)]),
            ],
        ),
    );
    let remote = snap(
        &graph,
        &state(
            clock(&[(1, 2), (2, 1)]),
            vec![quad("a", &[(1, 1)]), quad("d", &[(2, 1)])],
        ),
    );

    node.install_graph_snapshot(&local).unwrap();
    node.install_graph_snapshot(&remote).unwrap();

    let mut expected = snap(
        &graph,
        &state(
            clock(&[(1, 2), (2, 1), (3, 1)]),
            vec![
                quad("a", &[(1, 1)]),
                quad("c", &[(3, 1)]),
                quad("d", &[(2, 1)]),
            ],
        ),
    );
    normalize(&mut expected);
    assert_eq!(node.graph_snapshot(&graph).unwrap(), expected);
    assert_eq!(
        node.graph_snapshot(&graph).unwrap(),
        oracle(&local, &remote)
    );
}

#[test]
fn model_laws_hold() {
    let graph = GraphId::new("urn:test:merge:model");
    let states = family()
        .iter()
        .map(|state| snap(&graph, state))
        .collect::<Vec<_>>();
    for left in &states {
        assert_eq!(content(&oracle(left, left)), content(left));
        for right in &states {
            assert_eq!(
                content(&oracle(left, right)),
                content(&oracle(right, left)),
                "the model join must commute"
            );
            for third in &states {
                assert_eq!(
                    content(&oracle(&oracle(left, right), third)),
                    content(&oracle(left, &oracle(right, third))),
                    "the model join must associate"
                );
            }
        }
    }
}

#[test]
fn join_laws_hold() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "laws", 15);
    let states = family();

    for (position, incoming) in states.iter().enumerate() {
        let graph = GraphId::new(&format!("urn:test:merge:law-same-{position}"));
        let incoming = snap(&graph, incoming);
        assert!(node.install_graph_snapshot(&incoming).unwrap().applied);
        let settled = durable(&node, &graph);
        assert!(!node.install_graph_snapshot(&incoming).unwrap().applied);
        assert_eq!(settled, durable(&node, &graph));
        assert_eq!(settled.0, oracle(&empty(&graph), &incoming));
    }

    let mut by_set: BTreeMap<Vec<usize>, (VectorClock, Vec<SnapshotQuadState>)> = BTreeMap::new();
    for first in 0..states.len() {
        for second in 0..states.len() {
            for third in 0..states.len() {
                if first == second || second == third || first == third {
                    continue;
                }
                let order = [first, second, third];
                let graph = GraphId::new(&format!("urn:test:merge:law-{first}-{second}-{third}"));
                let mut expected = empty(&graph);
                for step in order {
                    let incoming = snap(&graph, &states[step]);
                    node.install_graph_snapshot(&incoming).unwrap();
                    expected = oracle(&expected, &incoming);
                }
                let observed = node.graph_snapshot(&graph).unwrap();
                assert_eq!(
                    observed, expected,
                    "engine must match the model for {order:?}"
                );

                let mut key = order.to_vec();
                key.sort_unstable();
                match by_set.get(&key) {
                    Some(previous) => assert_eq!(
                        *previous,
                        content(&observed),
                        "join result must not depend on arrival order for {key:?}"
                    ),
                    None => {
                        by_set.insert(key, content(&observed));
                    }
                }
            }
        }
    }
}

#[test]
fn join_matches_query() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "query", 16);
    let graph = GraphId::new("urn:test:merge:join-query");
    let seeded = snap(
        &graph,
        &state(
            clock(&[(1, 1), (2, 1)]),
            vec![quad("x", &[(1, 1)]), quad("y", &[(2, 1)])],
        ),
    );
    let removed = snap(
        &graph,
        &state(clock(&[(1, 2), (2, 1)]), vec![quad("y", &[(2, 1)])]),
    );
    node.install_graph_snapshot(&seeded).unwrap();
    node.install_graph_snapshot(&removed).unwrap();

    let results = node
        .query_in_graphs(
            &AllowAllAuthorizer,
            std::slice::from_ref(&graph),
            &format!("SELECT ?o WHERE {{ {SUBJECT} {PREDICATE} ?o }}"),
        )
        .unwrap();
    let QueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    let mut visible = rows
        .iter()
        .map(|row| row.get("o").unwrap().0.clone())
        .collect::<Vec<_>>();
    visible.sort();
    assert_eq!(visible, objects(&node, &graph));
    assert_eq!(visible, vec!["\"y\"".to_owned()]);
}

#[test]
fn join_reload_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("reload");
    let graph = GraphId::new("urn:test:merge:join-reload");
    let seeded = snap(&graph, &state(clock(&[(1, 1)]), vec![quad("x", &[(1, 1)])]));
    let removed = snap(&graph, &state(clock(&[(1, 2)]), Vec::new()));
    let settled = {
        let node = CraqleNode::open_with_actor(&root, actor(17)).unwrap();
        node.install_graph_snapshot(&seeded).unwrap();
        node.install_graph_snapshot(&removed).unwrap();
        durable(&node, &graph)
    };

    let node = CraqleNode::open_with_actor(&root, actor(17)).unwrap();
    assert_eq!(settled, durable(&node, &graph));
    for incoming in [&seeded, &removed] {
        assert!(!node.install_graph_snapshot(incoming).unwrap().applied);
        assert_eq!(settled, durable(&node, &graph));
    }
}

#[test]
fn join_keeps_tombstone() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "tombstone", 18);
    let graph = GraphId::new("urn:test:merge:join-tombstone");
    let seeded = snap(&graph, &state(clock(&[(1, 1)]), vec![quad("x", &[(1, 1)])]));
    assert!(node.install_graph_snapshot(&seeded).unwrap().applied);
    node.delete_graph(&AllowAllAuthorizer, &graph).unwrap();
    let deleted = durable(&node, &graph);

    assert!(!node.install_graph_snapshot(&seeded).unwrap().applied);
    assert_eq!(deleted, durable(&node, &graph));
    assert!(objects(&node, &graph).is_empty());
}

#[test]
fn batch_gap_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "gap", 19);
    let graph = GraphId::new("urn:test:merge:batch-gap");
    let first = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "x",
        insert: true,
    });
    let second = batch(Event {
        graph: &graph,
        dot: dot((1, 2)),
        base: clock(&[(1, 1)]),
        object: "y",
        insert: true,
    });

    let before = durable(&node, &graph);
    let error = node.merge_batch(&second).unwrap_err();
    assert!(
        matches!(&error, CraqleError::Merge(MergeError::MissingDependencies(missing))
            if missing == &vec![dot((1, 1))]),
        "expected a named missing dependency, got {error}"
    );
    assert_eq!(before, durable(&node, &graph));

    assert!(node.merge_batch(&first).unwrap().applied);
    assert!(node.merge_batch(&second).unwrap().applied);
    assert_eq!(objects(&node, &graph), vec!["\"x\"", "\"y\""]);
}

#[test]
fn remove_waits_add() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "remove", 20);
    let graph = GraphId::new("urn:test:merge:causal-remove");
    let add = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "x",
        insert: true,
    });
    let remove = batch(Event {
        graph: &graph,
        dot: dot((2, 1)),
        base: clock(&[(1, 1)]),
        object: "x",
        insert: false,
    });

    let before = durable(&node, &graph);
    assert!(node.merge_batch(&remove).is_err());
    assert_eq!(before, durable(&node, &graph));

    assert!(node.merge_batch(&add).unwrap().applied);
    assert!(node.merge_batch(&remove).unwrap().applied);
    assert!(objects(&node, &graph).is_empty());
}

#[test]
fn noncontiguous_counters_apply() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "control", 21);
    let graph = GraphId::new("urn:test:merge:noncontiguous");
    let first = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "x",
        insert: true,
    });
    // Counter two belongs to a control record that carries no quad change, so
    // the next quad batch must not be treated as a gap.
    let third = batch(Event {
        graph: &graph,
        dot: dot((1, 3)),
        base: clock(&[(1, 1)]),
        object: "y",
        insert: true,
    });
    assert!(node.merge_batch(&first).unwrap().applied);
    assert!(node.merge_batch(&third).unwrap().applied);
    assert_eq!(objects(&node, &graph), vec!["\"x\"", "\"y\""]);
}

#[test]
fn permanent_gap_explicit() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "permanent", 22);
    let graph = GraphId::new("urn:test:merge:permanent-gap");
    let orphan = batch(Event {
        graph: &graph,
        dot: dot((1, 4)),
        base: clock(&[(9, 7)]),
        object: "orphan",
        insert: true,
    });
    let ready = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "ready",
        insert: true,
    });

    for _ in 0..3 {
        let before = durable(&node, &graph);
        assert!(node.merge_batch(&orphan).is_err());
        assert_eq!(before, durable(&node, &graph));
    }
    assert!(node.merge_batch(&ready).unwrap().applied);
    assert_eq!(objects(&node, &graph), vec!["\"ready\""]);
    assert!(node.merge_batch(&orphan).is_err());
    assert_eq!(objects(&node, &graph), vec!["\"ready\""]);
}

#[test]
fn gap_survives_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("reopen");
    let graph = GraphId::new("urn:test:merge:gap-reopen");
    let first = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "x",
        insert: true,
    });
    let second = batch(Event {
        graph: &graph,
        dot: dot((1, 2)),
        base: clock(&[(1, 1)]),
        object: "y",
        insert: true,
    });
    {
        let node = CraqleNode::open_with_actor(&root, actor(23)).unwrap();
        assert!(node.merge_batch(&second).is_err());
    }

    let node = CraqleNode::open_with_actor(&root, actor(23)).unwrap();
    assert!(node.merge_batch(&second).is_err());
    assert!(objects(&node, &graph).is_empty());
    assert!(node.merge_batch(&first).unwrap().applied);
    assert!(node.merge_batch(&second).unwrap().applied);
    assert_eq!(objects(&node, &graph), vec!["\"x\"", "\"y\""]);
}

#[test]
fn reused_id_differs() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "reused", 24);
    let graph = GraphId::new("urn:test:merge:reused-id");
    let genuine = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[]),
        object: "x",
        insert: true,
    });
    // Same event identity, a payload that depends on state this node has never
    // seen: that is a different event, not a redelivery.
    let impostor = batch(Event {
        graph: &graph,
        dot: dot((1, 1)),
        base: clock(&[(5, 3)]),
        object: "y",
        insert: true,
    });
    assert!(node.merge_batch(&genuine).unwrap().applied);
    assert!(!node.merge_batch(&genuine).unwrap().applied);

    let before = durable(&node, &graph);
    assert!(node.merge_batch(&impostor).is_err());
    assert_eq!(before, durable(&node, &graph));
    assert_eq!(objects(&node, &graph), vec!["\"x\""]);
}

#[test]
fn shuffled_history_converges() {
    let temp = tempfile::tempdir().unwrap();
    let node = open(temp.path(), "shuffled", 25);
    let orders: [[usize; 5]; 4] = [
        [0, 1, 2, 3, 4],
        [4, 3, 2, 1, 0],
        [2, 0, 4, 1, 3],
        [1, 3, 0, 4, 2],
    ];
    let mut settled: Option<(VectorClock, Vec<SnapshotQuadState>)> = None;
    for (round, order) in orders.iter().enumerate() {
        let graph = GraphId::new(&format!("urn:test:merge:shuffled-{round}"));
        let history = [
            batch(Event {
                graph: &graph,
                dot: dot((1, 1)),
                base: clock(&[]),
                object: "a",
                insert: true,
            }),
            batch(Event {
                graph: &graph,
                dot: dot((1, 2)),
                base: clock(&[(1, 1)]),
                object: "b",
                insert: true,
            }),
            batch(Event {
                graph: &graph,
                dot: dot((2, 1)),
                base: clock(&[(1, 1)]),
                object: "a",
                insert: false,
            }),
            batch(Event {
                graph: &graph,
                dot: dot((2, 2)),
                base: clock(&[(1, 2), (2, 1)]),
                object: "c",
                insert: true,
            }),
            batch(Event {
                graph: &graph,
                dot: dot((3, 1)),
                base: clock(&[]),
                object: "d",
                insert: true,
            }),
        ];

        // Duplicates are delivered too, and a rejected batch is retried until
        // no pass makes progress: a bounded number of passes, never a sleep.
        let mut queue = order
            .iter()
            .flat_map(|&step| [step, step])
            .collect::<Vec<_>>();
        for _ in 0..=queue.len() {
            if queue.is_empty() {
                break;
            }
            let mut blocked = Vec::new();
            let mut progressed = false;
            for step in queue {
                match node.merge_batch(&history[step]) {
                    Ok(_) => progressed = true,
                    Err(_) => blocked.push(step),
                }
            }
            queue = blocked;
            if !progressed {
                break;
            }
        }
        assert!(queue.is_empty(), "every batch must become applicable");
        assert_eq!(objects(&node, &graph), vec!["\"b\"", "\"c\"", "\"d\""]);

        let observed = content(&node.graph_snapshot(&graph).unwrap());
        match &settled {
            Some(previous) => assert_eq!(*previous, observed, "order {order:?} diverged"),
            None => settled = Some(observed),
        }
    }
}
