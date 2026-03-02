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

