//! WS0 restart-recovery guarantees, exercised end to end through the public
//! `CraqleNode` API.
//!
//! The store-level proofs (clock-tag mismatch detection, FTS queue token
//! ordering, the legacy-clock migration fallback) live in
//! `src/internal/store.rs`'s unit tests, because `craqle::store` is a private
//! module and integration tests cannot reach `GraphStore` or any of the
//! `pub(crate)` WS0 API. What is testable from out here is the observable
//! contract those mechanisms exist to uphold: after a reopen the node reports
//! exactly the state it committed, and nothing is served stale.

use craqle::{
    CraqleNode, EncodedTerm, GrantAuthorizer, GraphDiagnostics, GraphId, GraphPolicy,
    MaterializedQuadChange, PermissionGrant, PermissionLevel, SearchRequest, vocab,
};

fn writer_auth() -> GrantAuthorizer {
    GrantAuthorizer::new(vec![PermissionGrant {
        pattern: "*".to_string(),
        level: PermissionLevel::Write,
    }])
}

fn public_policy() -> GraphPolicy {
    GraphPolicy {
        public: true,
        permission_paths: vec!["*".to_string()],
    }
}

fn named(iri: &str) -> EncodedTerm {
    EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(iri))
}

/// The graph IRI is the crate root; a typed data entity that no `hasPart`
/// chain reaches from it is an orphan.
fn orphan_triples(entity: &str) -> Vec<(EncodedTerm, EncodedTerm, EncodedTerm)> {
    vec![(
        named(entity),
        EncodedTerm::from_named_node(&vocab::rdf_type()),
        EncodedTerm::from_named_node(&vocab::schema_media_object()),
    )]
}

fn inserts(
    graph: &GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
) -> Vec<MaterializedQuadChange> {
    triples
        .into_iter()
        .map(
            |(subject, predicate, object)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject,
                predicate,
                object,
            },
        )
        .collect()
}

/// Write triples verbatim, skipping structural validation. These tests need to
/// commit states that validation would reject (orphans) or that a partially
/// built crate would show, which is exactly the state recovery must handle.
fn write_unchecked(
    node: &CraqleNode,
    graph: &GraphId,
    triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
) {
    node.apply_changes_unchecked(graph, inserts(graph, triples))
        .unwrap();
}

fn orphans(diagnostics: &GraphDiagnostics) -> Vec<String> {
    let mut entities = diagnostics.orphaned_entities.clone();
    entities.sort();
    entities
}

fn open_node(dir: &std::path::Path, graph: &GraphId) -> CraqleNode {
    let node = CraqleNode::open(dir).unwrap();
    if !node.contains_graph(graph).unwrap() {
        node.import_graph_policy(graph, public_policy()).unwrap();
    }
    node
}

/// Diagnostics are durable: a reopened node reports the same orphan set it
/// reported before shutdown, without being told to rebuild.
#[test]
fn diagnostics_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:restart:diagnostics-persist");

    {
        let node = open_node(dir.path(), &graph);
        write_unchecked(&node, &graph, orphan_triples("urn:orphan:a"));
        assert_eq!(
            vec!["urn:orphan:a".to_string()],
            orphans(&node.graph_diagnostics(&graph).unwrap())
        );
    }

    let node = open_node(dir.path(), &graph);
    assert_eq!(
        vec!["urn:orphan:a".to_string()],
        orphans(&node.graph_diagnostics(&graph).unwrap()),
        "a reopened node must report the persisted orphan set"
    );
}

/// The bulk write path deliberately defers the diagnostics refresh, which is
/// exactly the state a crash between the quad commit and the diagnostics write
/// leaves behind. Reopening must repair it, not serve the pre-bulk set.
#[test]
fn diagnostics_repaired_promptly_after_simulated_crash() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:restart:diagnostics-crash");

    {
        let node = open_node(dir.path(), &graph);
        write_unchecked(&node, &graph, orphan_triples("urn:orphan:known"));
        assert_eq!(
            vec!["urn:orphan:known".to_string()],
            orphans(&node.graph_diagnostics(&graph).unwrap())
        );

        // Committed durably, diagnostics never refreshed.
        node.apply_changes_bulk_unchecked(
            &graph,
            inserts(&graph, orphan_triples("urn:orphan:crashed")),
        )
        .unwrap();
    }

    let node = open_node(dir.path(), &graph);
    assert_eq!(
        vec![
            "urn:orphan:crashed".to_string(),
            "urn:orphan:known".to_string()
        ],
        orphans(&node.graph_diagnostics(&graph).unwrap()),
        "the reopened node must recompute diagnostics that no longer match the graph clock"
    );

    // And the repair is durable, so the *next* reopen agrees too.
    drop(node);
    let reopened = open_node(dir.path(), &graph);
    assert_eq!(
        vec![
            "urn:orphan:crashed".to_string(),
            "urn:orphan:known".to_string()
        ],
        orphans(&reopened.graph_diagnostics(&graph).unwrap())
    );
}

/// G10: after a bulk ingest under the WS0-T7 fjall configuration, recovery
/// reproduces exactly the committed state — same quad count, same content
/// fingerprint, same dot sets.
#[test]
fn reopen_full_fingerprint_equality() {
    const ENTITIES: usize = 1_500;

    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:restart:fingerprint");

    let (fingerprint, clock, mut snapshot) = {
        let node = open_node(dir.path(), &graph);
        let name = EncodedTerm::from_named_node(&vocab::schema_name());
        let quads = (0..ENTITIES)
            .map(|index| {
                (
                    named(&format!("urn:bulk:entity-{index}")),
                    name.clone(),
                    EncodedTerm(format!("\"bulk entity {index}\"")),
                )
            })
            .collect();
        write_unchecked(&node, &graph, quads);

        (
            node.graph_fingerprint(&graph).unwrap(),
            node.vector_clock(&graph).unwrap(),
            node.graph_snapshot(&graph).unwrap(),
        )
    };
    assert_eq!(ENTITIES as u64, fingerprint.0);

    let node = open_node(dir.path(), &graph);
    let mut reopened = node.graph_snapshot(&graph).unwrap();
    assert_eq!(fingerprint, node.graph_fingerprint(&graph).unwrap());
    assert_eq!(clock, node.vector_clock(&graph).unwrap());

    // Snapshot order is index order, which is not part of the contract.
    let sort_key = |state: &craqle::SnapshotQuadState| {
        (
            state.subject.0.clone(),
            state.predicate.0.clone(),
            state.object.0.clone(),
        )
    };
    snapshot.quads.sort_by_key(sort_key);
    reopened.quads.sort_by_key(sort_key);
    assert_eq!(snapshot, reopened);
}

/// Register row 13: the clock lives under its own key and is deleted with the
/// graph, so recreating a graph starts from a fresh clock instead of inheriting
/// counters that would suppress replays of the new graph's first batches (G2).
#[test]
fn deleted_graph_clock_not_resurrected() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:restart:clock-resurrection");

    {
        let node = open_node(dir.path(), &graph);
        write_unchecked(&node, &graph, orphan_triples("urn:orphan:a"));
        assert!(!node.vector_clock(&graph).unwrap().0.is_empty());

        node.delete_graph_unchecked(&graph).unwrap();
        assert!(
            node.vector_clock(&graph).unwrap().0.is_empty(),
            "deleting a graph must delete its clock"
        );
    }

    let node = open_node(dir.path(), &graph);
    assert!(
        node.vector_clock(&graph).unwrap().0.is_empty(),
        "a graph recreated after deletion must not inherit the deleted clock"
    );
    assert!(orphans(&node.graph_diagnostics(&graph).unwrap()).is_empty());
}

/// G7 across a restart: work queued before shutdown and work queued after it
/// must both reach the search index. This is the observable consequence of the
/// FTS queue tokens resuming past every live token (K4) — with the counter
/// restarting at 1, post-restart entries can be acknowledged away by a
/// pre-restart token and their subjects never get indexed.
#[test]
fn fts_updates_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphId::new("urn:test:restart:fts-tokens");
    let auth = writer_auth();
    let name = EncodedTerm::from_named_node(&vocab::schema_name());

    {
        let node = open_node(dir.path(), &graph);
        write_unchecked(
            &node,
            &graph,
            vec![(
                named("urn:entity:before"),
                name.clone(),
                EncodedTerm("\"quokka before restart\"".to_string()),
            )],
        );
        node.flush_search_updates().unwrap();
        assert_eq!(
            1,
            node.search(
                &auth,
                SearchRequest {
                    query: "quokka",
                    limit: 10
                }
            )
            .unwrap()
            .len()
        );
    }

    let node = open_node(dir.path(), &graph);
    node.reindex_search().unwrap();
    write_unchecked(
        &node,
        &graph,
        vec![(
            named("urn:entity:after"),
            name,
            EncodedTerm("\"quokka after restart\"".to_string()),
        )],
    );
    node.flush_search_updates().unwrap();

    let mut hits: Vec<String> = node
        .search(
            &auth,
            SearchRequest {
                query: "quokka",
                limit: 10,
            },
        )
        .unwrap()
        .into_iter()
        .map(|hit| hit.subject_iri)
        .collect();
    hits.sort();
    assert_eq!(
        vec![
            "urn:entity:after".to_string(),
            "urn:entity:before".to_string()
        ],
        hits,
        "a subject queued after the restart must not be dropped by a pre-restart token"
    );
}
