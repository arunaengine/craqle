use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

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
    #[error("sync: {0}")]
    Sync(#[from] crate::sync::CraqleSyncError),
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
    sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
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
        Self::new_with_sync(store, sparql, actor, None)
    }

    pub fn new_with_sync(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    ) -> Self {
        Self {
            store,
            sparql,
            rules: crate::rules::default_rules(),
            actor,
            sync,
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
        let total_started = Instant::now();
        let change_count = changes.len() as u64;
        let result = (|| {
            crate::trace_latency_step(
                "craqle.replication.local_apply_changes",
                "ensure_change_set_targets",
                graph,
                || self.ensure_change_set_targets(graph, &changes),
            )?;

            if changes.is_empty() {
                let base_clock = crate::trace_latency_step(
                    "craqle.replication.local_apply_changes",
                    "get_vector_clock_empty_change_set",
                    graph,
                    || self.store.get_vector_clock(graph),
                )?;
                return Ok(Batch {
                    graph: graph.clone(),
                    actor: self.actor,
                    counter: 0,
                    base_clock,
                    ops: vec![],
                    timestamp: Utc::now(),
                });
            }

            crate::trace_latency_step(
                "craqle.replication.local_apply_changes",
                "validate_change_set",
                graph,
                || match crate::rules::validate_change_set(
                    &self.rules,
                    &self.store,
                    graph,
                    &changes,
                ) {
                    Ok(()) => Ok(()),
                    Err(crate::rules::RuleEvaluationError::Store(error)) => {
                        Err(UpdateError::Store(error))
                    }
                    Err(crate::rules::RuleEvaluationError::Violations(violations)) => {
                        Err(UpdateError::ValidationFailed(violations))
                    }
                },
            )?;

            crate::trace_latency_step(
                "craqle.replication.local_apply_changes",
                "commit_changes",
                graph,
                || self.commit_changes(graph, changes),
            )
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        let batch_ops = result
            .as_ref()
            .map(|batch| batch.ops.len() as u64)
            .unwrap_or(0);
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.replication.local_apply_changes",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            change_count = change_count,
            batch_ops = batch_ops,
        );
        result
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
        let total_started = Instant::now();
        let change_count = changes.len() as u64;
        if let Some(sync) = &self.sync {
            let result = (|| {
                let can_preserve_clean_diagnostics = if diagnostics_refresh
                    == DiagnosticsRefresh::Immediate
                    && validated_orphan_free
                {
                    !crate::trace_latency_step(
                        "craqle.replication.commit_changes_with_mode",
                        "graph_diagnostics",
                        graph,
                        || self.store.graph_diagnostics(graph),
                    )?
                    .has_orphans()
                } else {
                    false
                };
                crate::trace_latency_step(
                    "craqle.replication.commit_changes_with_mode",
                    "publish_and_apply_changes",
                    graph,
                    || {
                        self.publish_and_apply_changes(
                            sync,
                            graph,
                            changes,
                            diagnostics_refresh,
                            can_preserve_clean_diagnostics,
                        )
                    },
                )
            })();

            let elapsed = total_started.elapsed();
            let result_status = if result.is_ok() { "ok" } else { "error" };
            let batch_ops = result
                .as_ref()
                .map(|batch| batch.ops.len() as u64)
                .unwrap_or(0);
            tracing::debug!(
                event = "craqle.latency.total",
                operation = "craqle.replication.commit_changes_with_mode",
                graph = %graph.as_str(),
                duration_ms = elapsed.as_millis() as u64,
                duration_us = elapsed.as_micros() as u64,
                result = result_status,
                sync_enabled = true,
                change_count = change_count,
                batch_ops = batch_ops,
            );
            return result;
        }

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

                    affected_subjects.insert(s);

                    ops.push(QuadOp::Remove {
                        subject,
                        predicate,
                        object,
                        witnessed: vector_clock.clone(),
                    });
                    stored_ops.push(crate::store::StoredQuadOp::Remove {
                        subject: s,
                        predicate: p,
                        object: o,
                        witnessed: vector_clock.clone(),
                    });
                }
            }
        }

        vector_clock.advance(self.actor, counter);
        self.store
            .set_vector_clock(&mut batch, graph, &vector_clock)?;

        let repl_batch = Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter,
            base_clock,
            ops,
            timestamp: Utc::now(),
        };

        self.store.append_compact_batch_log(
            &mut batch,
            graph,
            &crate::store::StoredBatch {
                actor: repl_batch.actor,
                counter: repl_batch.counter,
                base_clock: repl_batch.base_clock.clone(),
                ops: stored_ops,
                timestamp: repl_batch.timestamp,
            },
        )?;

        match diagnostics_refresh {
            DiagnosticsRefresh::Immediate => {
                self.store
                    .enqueue_fts_subjects(&mut batch, graph, &affected_subjects)?;
            }
            DiagnosticsRefresh::Deferred => {
                self.store.enqueue_fts_reindex(&mut batch, graph)?;
            }
        }

        self.store.commit(batch)?;
        if diagnostics_refresh == DiagnosticsRefresh::Immediate {
            if can_preserve_clean_diagnostics {
                self.store
                    .set_graph_diagnostics(graph, &crate::core::GraphDiagnostics::default())
                    .map_err(UpdateError::Store)?;
            } else {
                let snapshot =
                    GraphSnapshot::from_store(&self.store, graph).map_err(UpdateError::Store)?;
                self.refresh_graph_diagnostics(graph, &snapshot)
                    .map_err(UpdateError::Store)?;
            }
        }

        Ok(repl_batch)
    }

    fn publish_and_apply_changes(
        &self,
        sync: &Arc<dyn crate::sync::CraqleGraphSync>,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        diagnostics_refresh: DiagnosticsRefresh,
        can_preserve_clean_diagnostics: bool,
    ) -> Result<Batch, UpdateError> {
        let total_started = Instant::now();
        let change_count = changes.len() as u64;
        let result = (|| {
            let record = crate::trace_latency_step(
                "craqle.replication.publish_and_apply_changes",
                "irokle_publish_changes",
                graph,
                || sync.publish_changes(&self.store, graph, changes),
            )?;
            let batch = crate::trace_latency_step(
                "craqle.replication.publish_and_apply_changes",
                "batch_from_irokle_record",
                graph,
                || crate::sync::batch_from_irokle_record(&record),
            )?;
            let Some(batch) = batch else {
                return Err(UpdateError::InvalidChangeSet(
                    "irokle changes publish did not return a quad-change record".to_string(),
                ));
            };

            crate::trace_latency_step(
                "craqle.replication.publish_and_apply_changes",
                "apply_irokle_batch_with_mode",
                graph,
                || {
                    self.apply_irokle_batch_with_mode(
                        batch.clone(),
                        diagnostics_refresh,
                        can_preserve_clean_diagnostics,
                    )
                    .map_err(update_error_from_merge)
                },
            )?;
            Ok(batch)
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        let batch_ops = result
            .as_ref()
            .map(|batch| batch.ops.len() as u64)
            .unwrap_or(0);
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.replication.publish_and_apply_changes",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            change_count = change_count,
            batch_ops = batch_ops,
        );
        result
    }

    /// Apply a remote batch using OR-Set CRDT semantics.
    pub fn apply_remote_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        let mut touched_graphs = HashSet::new();
        self.apply_remote_batch_internal(incoming, true, &mut touched_graphs)
    }

    pub fn apply_remote_batches(
        &self,
        incoming: Vec<Batch>,
    ) -> Result<Vec<MergeResult>, MergeError> {
        let mut touched_graphs = HashSet::new();
        let mut results = Vec::with_capacity(incoming.len());
        for batch in incoming {
            results.push(self.apply_remote_batch_internal(batch, false, &mut touched_graphs)?);
        }

        for graph in touched_graphs {
            self.finalize_remote_graph(&graph)?;
        }

        Ok(results)
    }

    /// Apply a causally ordered batch produced from an Irokle graph event.
    ///
    /// Irokle actor sequences include genesis and topic-control operations, so
    /// they are not contiguous over Craqle domain events. The Irokle DAG already
    /// enforces causal delivery; this path intentionally bypasses Craqle's old
    /// vector-clock gap buffering while preserving OR-Set add/remove semantics.
    pub fn apply_irokle_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        self.apply_irokle_batch_with_mode(incoming, DiagnosticsRefresh::Immediate, false)
    }

    fn apply_irokle_batch_with_mode(
        &self,
        incoming: Batch,
        diagnostics_refresh: DiagnosticsRefresh,
        can_preserve_clean_diagnostics: bool,
    ) -> Result<MergeResult, MergeError> {
        let graph_id = incoming.graph.clone();
        let op_count = incoming.ops.len() as u64;
        let total_started = Instant::now();
        let result = (|| {
            let graph = &incoming.graph;

            if !crate::trace_latency_step(
                "craqle.replication.apply_irokle_batch_with_mode",
                "contains_graph",
                graph,
                || self.store.contains_graph(graph),
            )? {
                crate::trace_latency_step(
                    "craqle.replication.apply_irokle_batch_with_mode",
                    "create_graph",
                    graph,
                    || self.store.create_graph(graph),
                )?;
            }

            let mut vector_clock = crate::trace_latency_step(
                "craqle.replication.apply_irokle_batch_with_mode",
                "get_vector_clock",
                graph,
                || self.store.get_vector_clock(graph),
            )?;
            let started = Instant::now();
            let already_applied = vector_clock.contains(&Dot {
                actor: incoming.actor,
                counter: incoming.counter,
            });
            crate::record_latency_step(
                "craqle.replication.apply_irokle_batch_with_mode",
                "contains_dot",
                graph,
                started,
                true,
            );
            if already_applied {
                return Ok(MergeResult { applied: false });
            }

            crate::trace_latency_step(
                "craqle.replication.apply_irokle_batch_with_mode",
                "apply_single_batch_with_mode",
                graph,
                || {
                    self.apply_single_batch_with_mode(
                        &incoming,
                        &mut vector_clock,
                        diagnostics_refresh,
                    )
                },
            )?;
            match diagnostics_refresh {
                DiagnosticsRefresh::Immediate => {
                    if can_preserve_clean_diagnostics {
                        crate::trace_latency_step(
                            "craqle.replication.apply_irokle_batch_with_mode",
                            "set_clean_graph_diagnostics",
                            graph,
                            || {
                                self.store.set_graph_diagnostics(
                                    graph,
                                    &crate::core::GraphDiagnostics::default(),
                                )
                            },
                        )?;
                    } else {
                        crate::trace_latency_step(
                            "craqle.replication.apply_irokle_batch_with_mode",
                            "finalize_remote_graph",
                            graph,
                            || self.finalize_remote_graph(graph),
                        )?;
                    }
                }
                DiagnosticsRefresh::Deferred => {}
            }
            Ok(MergeResult { applied: true })
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        let applied = result
            .as_ref()
            .map(|result| result.applied)
            .unwrap_or(false);
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.replication.apply_irokle_batch_with_mode",
            graph = %graph_id.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            applied = applied,
            op_count = op_count,
        );
        result
    }

    pub fn apply_irokle_record(
        &self,
        record: &irokle::reducer::EventRecord<crate::sync::CraqleGraphEvent>,
    ) -> Result<Option<MergeResult>, MergeError> {
        let batch = crate::sync::batch_from_irokle_record(record)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        batch
            .map(|batch| self.apply_irokle_batch(batch))
            .transpose()
    }

    fn apply_remote_batch_internal(
        &self,
        incoming: Batch,
        finalize_graph: bool,
        touched_graphs: &mut HashSet<GraphId>,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        let mut vector_clock = self.store.get_vector_clock(graph)?;

        if vector_clock.contains(&Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        }) {
            return Ok(MergeResult { applied: false });
        }

        if !self.batch_is_ready(&vector_clock, &incoming) {
            self.buffer_remote_batch(incoming)?;
            return Ok(MergeResult { applied: false });
        }

        self.apply_single_batch(&incoming, &mut vector_clock)?;
        touched_graphs.insert(graph.clone());
        self.apply_ready_buffered_batches(graph, &mut vector_clock, touched_graphs)?;

        if finalize_graph {
            self.finalize_remote_graph(graph)?;
        }

        Ok(MergeResult { applied: true })
    }

    fn finalize_remote_graph(&self, graph: &GraphId) -> Result<(), MergeError> {
        let snapshot = GraphSnapshot::from_store(&self.store, graph).map_err(MergeError::Store)?;
        self.refresh_graph_diagnostics(graph, &snapshot)
            .map_err(MergeError::Store)
    }

    fn apply_single_batch(
        &self,
        incoming: &Batch,
        vector_clock: &mut VectorClock,
    ) -> Result<(), MergeError> {
        self.apply_single_batch_with_mode(incoming, vector_clock, DiagnosticsRefresh::Immediate)
    }

    fn apply_single_batch_with_mode(
        &self,
        incoming: &Batch,
        vector_clock: &mut VectorClock,
        diagnostics_refresh: DiagnosticsRefresh,
    ) -> Result<(), MergeError> {
        let graph = &incoming.graph;
        let total_started = Instant::now();
        let op_count = incoming.ops.len() as u64;
        let started = Instant::now();
        let mut batch = self.store.new_batch();
        crate::record_latency_step(
            "craqle.replication.apply_single_batch_with_mode",
            "new_batch",
            graph,
            started,
            true,
        );
        let mut affected_subjects = std::collections::HashSet::new();
        let mut term_cache = HashMap::new();
        let mut stored_ops = Vec::with_capacity(incoming.ops.len());

        let result = (|| {
            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "seed_term_cache",
                graph,
                || {
                    self.store.seed_term_cache(
                        &mut batch,
                        &mut term_cache,
                        incoming.ops.iter().flat_map(|op| match op {
                            QuadOp::Add {
                                subject,
                                predicate,
                                object,
                                ..
                            }
                            | QuadOp::Remove {
                                subject,
                                predicate,
                                object,
                                ..
                            } => [subject, predicate, object],
                        }),
                    )
                },
            )?;

            let g = crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "resolve_graph_term",
                graph,
                || {
                    self.store
                        .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                },
            )?;

            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "apply_ops",
                graph,
                || {
                    for op in &incoming.ops {
                        match op {
                            QuadOp::Add {
                                subject,
                                predicate,
                                object,
                                dot,
                            } => {
                                let s = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    subject,
                                )?;
                                let p = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    predicate,
                                )?;
                                let o = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    object,
                                )?;
                                self.store.insert_quad(&mut batch, g, s, p, o, dot)?;
                                affected_subjects.insert(s);
                                stored_ops.push(crate::store::StoredQuadOp::Add {
                                    subject: s,
                                    predicate: p,
                                    object: o,
                                    dot: *dot,
                                });
                            }
                            QuadOp::Remove {
                                subject,
                                predicate,
                                object,
                                witnessed,
                            } => {
                                let s = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    subject,
                                )?;
                                let p = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    predicate,
                                )?;
                                let o = self.store.resolve_term_cached(
                                    &mut batch,
                                    &mut term_cache,
                                    object,
                                )?;
                                self.store.remove_quad(&mut batch, g, s, p, o, witnessed)?;
                                affected_subjects.insert(s);
                                stored_ops.push(crate::store::StoredQuadOp::Remove {
                                    subject: s,
                                    predicate: p,
                                    object: o,
                                    witnessed: witnessed.clone(),
                                });
                            }
                        }
                    }
                    Ok::<(), MergeError>(())
                },
            )?;

            vector_clock.advance(incoming.actor, incoming.counter);
            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "set_vector_clock",
                graph,
                || self.store.set_vector_clock(&mut batch, graph, vector_clock),
            )?;

            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "append_compact_batch_log",
                graph,
                || {
                    self.store.append_compact_batch_log(
                        &mut batch,
                        graph,
                        &crate::store::StoredBatch {
                            actor: incoming.actor,
                            counter: incoming.counter,
                            base_clock: incoming.base_clock.clone(),
                            ops: stored_ops,
                            timestamp: incoming.timestamp,
                        },
                    )
                },
            )?;

            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "enqueue_fts",
                graph,
                || match diagnostics_refresh {
                    DiagnosticsRefresh::Immediate => {
                        self.store
                            .enqueue_fts_subjects(&mut batch, graph, &affected_subjects)
                    }
                    DiagnosticsRefresh::Deferred => {
                        self.store.enqueue_fts_reindex(&mut batch, graph)
                    }
                },
            )?;

            crate::trace_latency_step(
                "craqle.replication.apply_single_batch_with_mode",
                "commit_batch",
                graph,
                || self.store.commit(batch),
            )?;
            Ok(())
        })();

        let elapsed = total_started.elapsed();
        let result_status = if result.is_ok() { "ok" } else { "error" };
        tracing::debug!(
            event = "craqle.latency.total",
            operation = "craqle.replication.apply_single_batch_with_mode",
            graph = %graph.as_str(),
            duration_ms = elapsed.as_millis() as u64,
            duration_us = elapsed.as_micros() as u64,
            result = result_status,
            op_count = op_count,
        );
        result
    }

    fn batch_is_ready(&self, vector_clock: &VectorClock, incoming: &Batch) -> bool {
        let expected = vector_clock
            .0
            .get(&incoming.actor)
            .map(|counter| counter + 1)
            .unwrap_or(1);
        incoming.counter == expected
            && incoming
                .base_clock
                .0
                .iter()
                .all(|(actor, counter)| vector_clock.0.get(actor).copied().unwrap_or(0) >= *counter)
    }

    fn buffer_remote_batch(&self, incoming: Batch) -> Result<(), MergeError> {
        let mut buffer = self.gap_buffer.lock().unwrap();
        let graph_buffer = buffer.entry(incoming.graph.clone()).or_default();
        if graph_buffer
            .iter()
            .any(|batch| batch.actor == incoming.actor && batch.counter == incoming.counter)
        {
            return Ok(());
        }
        if graph_buffer.len() >= MAX_BUFFERED_REMOTE_BATCHES_PER_GRAPH {
            return Err(MergeError::InputRejected(format!(
                "gap buffer on graph `{}` exceeded {} pending batches",
                incoming.graph.as_str(),
                MAX_BUFFERED_REMOTE_BATCHES_PER_GRAPH
            )));
        }
        graph_buffer.push(incoming);
        Ok(())
    }

    fn apply_ready_buffered_batches(
        &self,
        graph: &GraphId,
        vector_clock: &mut VectorClock,
        touched_graphs: &mut HashSet<GraphId>,
    ) -> Result<(), MergeError> {
        loop {
            let next = {
                let mut buffer = self.gap_buffer.lock().unwrap();
                let Some(graph_buffer) = buffer.get_mut(graph) else {
                    return Ok(());
                };
                let Some((index, _)) = graph_buffer
                    .iter()
                    .enumerate()
                    .filter(|(_, batch)| self.batch_is_ready(vector_clock, batch))
                    .min_by_key(|(_, batch)| (batch.actor, batch.counter))
                else {
                    return Ok(());
                };
                let batch = graph_buffer.swap_remove(index);
                if graph_buffer.is_empty() {
                    buffer.remove(graph);
                }
                batch
            };

            self.apply_single_batch(&next, vector_clock)?;
            touched_graphs.insert(graph.clone());
        }
    }

    fn refresh_graph_diagnostics(
        &self,
        graph: &GraphId,
        snapshot: &GraphSnapshot,
    ) -> crate::store::Result<()> {
        let previous = self.store.graph_diagnostics(graph)?;
        let current = GraphDiagnostics::from_orphaned_entities(
            crate::rules::orphaned_data_entities(snapshot)
                .into_iter()
                .map(|term| encoded_identifier_value(&term))
                .collect(),
        );

        if previous == current {
            return Ok(());
        }

        self.store.set_graph_diagnostics(graph, &current)?;
        self.enqueue_orphan_fts_updates(graph, &previous, &current)
    }

    fn enqueue_orphan_fts_updates(
        &self,
        graph: &GraphId,
        previous: &GraphDiagnostics,
        current: &GraphDiagnostics,
    ) -> crate::store::Result<()> {
        let previous: std::collections::HashSet<&String> =
            previous.orphaned_entities.iter().collect();
        let current: std::collections::HashSet<&String> =
            current.orphaned_entities.iter().collect();
        let mut batch = self.store.new_batch();
        let mut dirty = false;

        for entity_id in previous.symmetric_difference(&current) {
            let subject =
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(entity_id.as_str()));
            let Some(subject_tid) = self.store.lookup_term(&subject)? else {
                continue;
            };
            self.store.enqueue_fts(&mut batch, graph, subject_tid)?;
            dirty = true;
        }

        if dirty {
            self.store.commit(batch)?;
        }

        Ok(())
    }

    /// Get batches that a remote peer needs beyond their current vector clock.
    pub fn batches_for_catchup(
        &self,
        graph: &GraphId,
        remote_clock: &VectorClock,
    ) -> Result<Vec<Batch>, MergeError> {
        Ok(self
            .store
            .batches_beyond_vector_clock(graph, remote_clock)?)
    }
}

fn encoded_identifier_value(term: &EncodedTerm) -> String {
    term.to_named_node()
        .map(|node| node.as_str().to_string())
        .unwrap_or_else(|| term.0.clone())
}

fn update_error_from_merge(error: MergeError) -> UpdateError {
    match error {
        MergeError::Store(error) => UpdateError::Store(error),
        MergeError::InputRejected(message) => UpdateError::InvalidChangeSet(message),
    }
}
