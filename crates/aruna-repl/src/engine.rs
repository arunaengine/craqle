use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use aruna_core::*;
use aruna_rdf_store::GraphStore;
use aruna_shacl::{self, GraphSnapshot, Guard};
use aruna_sparql::SparqlEngine;
use chrono::Utc;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("sparql: {0}")]
    Sparql(#[from] aruna_sparql::SparqlError),
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<CrateViolation>),
    #[error("invalid change set: {0}")]
    InvalidChangeSet(String),
    #[error("store: {0}")]
    Store(#[from] aruna_rdf_store::StoreError),
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store: {0}")]
    Store(#[from] aruna_rdf_store::StoreError),
}

#[derive(Debug)]
pub struct MergeResult {
    pub applied: bool,
    pub violations: Vec<CrateViolation>,
}

/// The replication engine: local writes, CRDT merge, catch-up.
pub struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    guards: Vec<Box<dyn Guard>>,
    actor: ActorId,
    /// Buffer for out-of-order batches: (graph, actor) -> counter -> batch
    gap_buffer: std::sync::Mutex<BTreeMap<(String, ActorId), BTreeMap<u64, Batch>>>,
}

impl ReplicationEngine {
    pub fn new(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        guards: Vec<Box<dyn Guard>>,
        actor: ActorId,
    ) -> Self {
        Self {
            store,
            sparql,
            guards,
            actor,
            gap_buffer: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    pub fn actor(&self) -> ActorId {
        self.actor
    }

    pub fn store(&self) -> &Arc<GraphStore> {
        &self.store
    }

    pub fn sparql(&self) -> &Arc<SparqlEngine> {
        &self.sparql
    }

    // ── Local Write Pipeline ────────────────────────────────────────────

    /// Execute a SPARQL Update locally with full validation.
    /// Returns `None` if the update produced no changes.
    pub fn local_update(&self, sparql_update: &str) -> Result<Option<Batch>, UpdateError> {
        // 1. Evaluate SPARQL UPDATE → materialized delta
        let changes = self.sparql.evaluate_update(sparql_update)?;

        if changes.is_empty() {
            return Ok(None);
        }

        // Determine the target graph from the first change
        let graph = match &changes[0] {
            MaterializedQuadChange::Insert { graph, .. }
            | MaterializedQuadChange::Delete { graph, .. } => graph.clone(),
        };

        // 2. Build snapshot + validate with SHACL guards
        let snapshot = GraphSnapshot::from_store(&self.store, &graph)
            .map_err(|e| UpdateError::Store(e.into()))?;

        aruna_shacl::pre_execution_validate(&self.guards, &snapshot, &changes)
            .map_err(UpdateError::ValidationFailed)?;

        // 3. Commit the changes
        self.commit_changes(&graph, &changes).map(Some)
    }

    /// Insert raw quads (bypasses SPARQL, still validates).
    pub fn local_insert_quads(
        &self,
        graph: &GraphId,
        quads: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
    ) -> Result<Batch, UpdateError> {
        let changes: Vec<MaterializedQuadChange> = quads
            .into_iter()
            .map(|(s, p, o)| MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: s,
                predicate: p,
                object: o,
            })
            .collect();

        self.local_apply_changes(graph, changes)
    }

    /// Apply a pre-materialized change set locally with full validation.
    pub fn local_apply_changes(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return Ok(Batch {
                graph: graph.clone(),
                actor: self.actor,
                counter: 0,
                base_frontier: self.store.get_frontier(graph)?,
                ops: vec![],
                timestamp: Utc::now(),
            });
        }

        let snapshot = GraphSnapshot::from_store(&self.store, graph)
            .map_err(|e| UpdateError::Store(e.into()))?;

        aruna_shacl::pre_execution_validate(&self.guards, &snapshot, &changes)
            .map_err(UpdateError::ValidationFailed)?;

        self.commit_changes(graph, &changes)
    }

    /// Apply a pre-materialized change set locally without full graph validation.
    ///
    /// Intended for trusted higher-level RO-Crate operations that maintain
    /// structural invariants incrementally.
    pub fn local_apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return Ok(Batch {
                graph: graph.clone(),
                actor: self.actor,
                counter: 0,
                base_frontier: self.store.get_frontier(graph)?,
                ops: vec![],
                timestamp: Utc::now(),
            });
        }

        self.commit_changes(graph, &changes)
    }

    fn ensure_change_set_targets(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        for change in changes {
            let change_graph = match change {
                MaterializedQuadChange::Insert { graph, .. }
                | MaterializedQuadChange::Delete { graph, .. } => graph,
            };
            if change_graph != graph {
                return Err(UpdateError::InvalidChangeSet(format!(
                    "all changes must target `{}` but found `{}`",
                    graph.as_str(),
                    change_graph.as_str()
                )));
            }
        }
        Ok(())
    }

    /// Internal: assign dots, write to store, build replication batch.
    fn commit_changes(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<Batch, UpdateError> {
        let mut batch = self.store.new_batch();

        // Ensure graph exists
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Get current frontier and next counter
        let mut frontier = self.store.get_frontier(graph)?;
        let counter = self.store.next_counter(&mut batch, graph, &self.actor)?;
        let dot = Dot {
            actor: self.actor,
            counter,
        };
        let base_frontier = frontier.clone();
        let g = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;

        // Build quad operations
        let mut ops = Vec::new();
        let mut affected_subjects = std::collections::HashSet::new();
        let mut count_deltas = HashMap::new();

        for change in changes {
            match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let s = self.store.resolve_term(subject)?;
                    let p = self.store.resolve_term(predicate)?;
                    let o = self.store.resolve_term(object)?;

                    if self.store.insert_quad(&mut batch, g, s, p, o, &dot)? {
                        *count_deltas.entry((g, s, p)).or_insert(0i64) += 1;
                    }

                    affected_subjects.insert((subject.clone(), s));

                    ops.push(QuadOp::Add {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: object.clone(),
                        dot,
                    });
                }
                MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let s = self.store.resolve_term(subject)?;
                    let p = self.store.resolve_term(predicate)?;
                    let o = self.store.resolve_term(object)?;

                    if self.store.remove_quad(&mut batch, g, s, p, o, &frontier)? {
                        *count_deltas.entry((g, s, p)).or_insert(0i64) -= 1;
                    }

                    affected_subjects.insert((subject.clone(), s));

                    ops.push(QuadOp::Remove {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: object.clone(),
                        witnessed: frontier.clone(),
                    });
                }
            }
        }

        self.store
            .apply_subject_predicate_count_deltas(&mut batch, &count_deltas)?;

        // Update frontier
        frontier.advance(self.actor, counter);
        self.store.set_frontier(&mut batch, graph, &frontier)?;

        // Build replication batch
        let repl_batch = Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter,
            base_frontier,
            ops,
            timestamp: Utc::now(),
        };

        // Append to batch log
        self.store.append_batch_log(&mut batch, &repl_batch)?;

        // Enqueue FTS updates
        for (_, tid) in &affected_subjects {
            self.store.enqueue_fts(&mut batch, graph, *tid)?;
        }

        // Atomic commit
        self.store.commit(batch)?;

        Ok(repl_batch)
    }

    // ── Incoming Batch Application (CRDT Merge) ─────────────────────────

    /// Apply a remote batch using OR-Set CRDT semantics.
    pub fn apply_remote_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        // Ensure graph exists
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        let mut frontier = self.store.get_frontier(graph)?;

        // Idempotence check: already seen?
        if frontier.contains(&Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        }) {
            return Ok(MergeResult {
                applied: false,
                violations: vec![],
            });
        }

        // Gap check: is this the next expected counter?
        let expected = frontier.0.get(&incoming.actor).map(|c| c + 1).unwrap_or(1);

        if incoming.counter > expected {
            // Buffer for later
            let mut buffer = self.gap_buffer.lock().unwrap();
            buffer
                .entry((graph.as_str().to_string(), incoming.actor))
                .or_default()
                .insert(incoming.counter, incoming);
            return Ok(MergeResult {
                applied: false,
                violations: vec![],
            });
        }

        // Apply this batch
        self.apply_single_batch(&incoming, &mut frontier)?;

        // Try to drain buffered batches for this actor
        loop {
            let next_expected = frontier.0.get(&incoming.actor).map(|c| c + 1).unwrap_or(1);

            let buffered = {
                let mut buffer = self.gap_buffer.lock().unwrap();
                let key = (graph.as_str().to_string(), incoming.actor);
                buffer.get_mut(&key).and_then(|m| m.remove(&next_expected))
            };

            match buffered {
                Some(b) => self.apply_single_batch(&b, &mut frontier)?,
                None => break,
            }
        }

        // Post-merge validation (non-blocking)
        let snapshot = GraphSnapshot::from_store(&self.store, graph)
            .map_err(|e| MergeError::Store(e.into()))?;
        let violations = aruna_shacl::post_merge_check(&snapshot);

        Ok(MergeResult {
            applied: true,
            violations,
        })
    }

    fn apply_single_batch(
        &self,
        incoming: &Batch,
        frontier: &mut Frontier,
    ) -> Result<(), MergeError> {
        let graph = &incoming.graph;
        let mut batch = self.store.new_batch();
        let mut affected_subjects = std::collections::HashSet::new();
        let mut count_deltas = HashMap::new();

        let g = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;

        for op in &incoming.ops {
            match op {
                QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => {
                    let s = self.store.resolve_term(subject)?;
                    let p = self.store.resolve_term(predicate)?;
                    let o = self.store.resolve_term(object)?;
                    if self.store.insert_quad(&mut batch, g, s, p, o, dot)? {
                        *count_deltas.entry((g, s, p)).or_insert(0i64) += 1;
                    }
                    affected_subjects.insert(s);
                }
                QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => {
                    let s = self.store.resolve_term(subject)?;
                    let p = self.store.resolve_term(predicate)?;
                    let o = self.store.resolve_term(object)?;
                    if self.store.remove_quad(&mut batch, g, s, p, o, witnessed)? {
                        *count_deltas.entry((g, s, p)).or_insert(0i64) -= 1;
                    }
                    affected_subjects.insert(s);
                }
            }
        }

        self.store
            .apply_subject_predicate_count_deltas(&mut batch, &count_deltas)?;

        // Update frontier
        frontier.advance(incoming.actor, incoming.counter);
        self.store.set_frontier(&mut batch, graph, frontier)?;

        // Append to batch log (for further replication)
        self.store.append_batch_log(&mut batch, incoming)?;

        for tid in affected_subjects {
            self.store.enqueue_fts(&mut batch, graph, tid)?;
        }

        // Commit
        self.store.commit(batch)?;
        Ok(())
    }

    // ── Catch-up ────────────────────────────────────────────────────────

    /// Get batches that a remote peer needs (beyond their frontier).
    pub fn batches_for_catchup(
        &self,
        graph: &GraphId,
        remote_frontier: &Frontier,
    ) -> Result<Vec<Batch>, MergeError> {
        Ok(self.store.batches_beyond_frontier(graph, remote_frontier)?)
    }
}
