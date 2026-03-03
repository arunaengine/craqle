use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::*;
use crate::rules::{GraphSnapshot, Rule};
use crate::sparql::SparqlEngine;
use crate::store::GraphStore;
use chrono::Utc;
use oxrdf::NamedNode;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("sparql: {0}")]
    Sparql(#[from] crate::sparql::SparqlError),
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<CrateViolation>),
    #[error("invalid change set: {0}")]
    InvalidChangeSet(String),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("input rejected: {0}")]
    InputRejected(String),
}

#[derive(Debug)]
pub struct MergeResult {
    pub applied: bool,
}

/// The replication engine: local writes, CRDT merge, catch-up.
pub struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    rules: Vec<Box<dyn Rule>>,
    actor: ActorId,
    gap_buffer: std::sync::Mutex<HashMap<GraphId, Vec<Batch>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsRefresh {
    Immediate,
    Deferred,
}

const MAX_BUFFERED_REMOTE_BATCHES_PER_GRAPH: usize = 10_000;

impl ReplicationEngine {
    pub fn new(store: Arc<GraphStore>, sparql: Arc<SparqlEngine>, actor: ActorId) -> Self {
        Self {
            store,
            sparql,
            rules: crate::rules::default_rules(),
            actor,
            gap_buffer: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn store(&self) -> &Arc<GraphStore> {
        &self.store
    }

    /// Execute a SPARQL Update locally with full validation.
    /// Returns `None` if the update produced no changes.
    pub fn local_update(&self, sparql_update: &str) -> Result<Option<Batch>, UpdateError> {
        let changes = self.sparql.evaluate_update(sparql_update)?;

        if changes.is_empty() {
            return Ok(None);
        }

        let graph = match &changes[0] {
            MaterializedQuadChange::Insert { graph, .. }
            | MaterializedQuadChange::Delete { graph, .. } => graph.clone(),
        };
        self.ensure_change_set_targets(&graph, &changes)?;

        match crate::rules::validate_change_set(&self.rules, &self.store, &graph, &changes) {
            Ok(()) => {}
            Err(crate::rules::RuleEvaluationError::Store(error)) => {
                return Err(UpdateError::Store(error));
            }
            Err(crate::rules::RuleEvaluationError::Violations(violations)) => {
                return Err(UpdateError::ValidationFailed(violations));
            }
        }

        self.commit_changes(&graph, changes).map(Some)
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
                base_clock: self.store.get_vector_clock(graph)?,
                ops: vec![],
                timestamp: Utc::now(),
            });
        }

        match crate::rules::validate_change_set(&self.rules, &self.store, graph, &changes) {
            Ok(()) => {}
            Err(crate::rules::RuleEvaluationError::Store(error)) => {
                return Err(UpdateError::Store(error));
            }
            Err(crate::rules::RuleEvaluationError::Violations(violations)) => {
                return Err(UpdateError::ValidationFailed(violations));
            }
        }

        self.commit_changes(graph, changes)
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
                base_clock: self.store.get_vector_clock(graph)?,
                ops: vec![],
                timestamp: Utc::now(),
            });
        }

        self.commit_changes_with_mode(graph, changes, DiagnosticsRefresh::Immediate, false)
    }

    /// Apply a trusted bulk change set locally and defer graph-diagnostics
    /// recomputation until the caller explicitly rebuilds diagnostics.
    pub fn local_apply_changes_bulk_unchecked(
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
                base_clock: self.store.get_vector_clock(graph)?,
                ops: vec![],
                timestamp: Utc::now(),
            });
        }

        self.commit_changes_with_mode(graph, changes, DiagnosticsRefresh::Deferred, false)
    }

    pub fn rebuild_graph_diagnostics(&self, graph: &GraphId) -> Result<(), UpdateError> {
        let snapshot = GraphSnapshot::from_store(&self.store, graph).map_err(UpdateError::Store)?;
        self.refresh_graph_diagnostics(graph, &snapshot)
            .map_err(UpdateError::Store)
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
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.commit_changes_with_mode(graph, changes, DiagnosticsRefresh::Immediate, true)
    }

    fn commit_changes_with_mode(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        diagnostics_refresh: DiagnosticsRefresh,
        validated_orphan_free: bool,
    ) -> Result<Batch, UpdateError> {
        let mut batch = self.store.new_batch();
        let can_preserve_clean_diagnostics = diagnostics_refresh == DiagnosticsRefresh::Immediate
            && validated_orphan_free
            && !self.store.graph_diagnostics(graph)?.has_orphans();

        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        let mut vector_clock = self.store.get_vector_clock(graph)?;
        let counter = self.store.next_counter(&mut batch, graph, &self.actor)?;
        let dot = Dot {
            actor: self.actor,
            counter,
        };
        let base_clock = vector_clock.clone();
        let g = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;

        let mut ops = Vec::with_capacity(changes.len());
        let mut stored_ops = Vec::with_capacity(changes.len());
        let mut affected_subjects = std::collections::HashSet::new();
        let mut term_cache = HashMap::new();

        self.store.seed_term_cache(
            &mut batch,
            &mut term_cache,
            changes.iter().flat_map(|change| match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                }
                | MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => [subject, predicate, object],
            }),
        )?;

        for change in changes {
            match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let s =
                        self.store
                            .resolve_term_cached(&mut batch, &mut term_cache, &subject)?;
                    let p =
                        self.store
                            .resolve_term_cached(&mut batch, &mut term_cache, &predicate)?;
                    let o = self
                        .store
                        .resolve_term_cached(&mut batch, &mut term_cache, &object)?;

                    self.store.insert_quad(&mut batch, g, s, p, o, &dot)?;

                    affected_subjects.insert(s);

                    ops.push(QuadOp::Add {
                        subject,
                        predicate,
                        object,
                        dot,
                    });
                    stored_ops.push(crate::store::StoredQuadOp::Add {
                        subject: s,
                        predicate: p,
                        object: o,
                        dot,
                    });
                }
                MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let s =
                        self.store
                            .resolve_term_cached(&mut batch, &mut term_cache, &subject)?;
                    let p =
                        self.store
                            .resolve_term_cached(&mut batch, &mut term_cache, &predicate)?;
                    let o = self
                        .store
                        .resolve_term_cached(&mut batch, &mut term_cache, &object)?;

                    self.store
                        .remove_quad(&mut batch, g, s, p, o, &vector_clock)?;
