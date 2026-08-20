use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use crate::core::*;
use crate::rules::{ChangeSet, DeltaSummary, Rule};
use crate::sparql::SparqlEngine;
use crate::store::{
    BatchTermCtx, ClockUpdate, CounterKey, EncodedQuad, FtsEnqueue, FtsSubject, GraphStore,
    QuadAdd, QuadRemove, TermId,
};
use chrono::Utc;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("sparql: {0}")]
    Sparql(#[from] crate::sparql::SparqlError),
    #[error("validation failed: {0:?}")]
    ValidationFailed(Vec<CrateViolation>),
    #[cfg(feature = "shacl-core")]
    #[error("shacl: {0}")]
    Shacl(#[from] crate::ShaclError),
    #[cfg(feature = "shacl-core")]
    #[error("SHACL validation failed for {} schema(s)", .0.len())]
    ShaclValidationFailed(Vec<crate::ShaclValidationReport>),
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
pub(crate) struct MergeResult {
    pub applied: bool,
}

/// Number of shards backing [`GRAPH_WRITE_LOCKS`].
const GRAPH_WRITE_LOCK_SHARDS: usize = 32;

/// Makes publish order the apply order for one graph, and the `@context` tag
/// mint atomic.
///
/// Not `graph_commit_guard`: both uses must span a call that takes that guard
/// internally (`set_graph_context`, and `ensure_graph_topic` on a first
/// publish), and `std::sync::Mutex` is not reentrant. Process-wide because one
/// store is shared by several engines.
///
/// Lock order: **graph write lock ▸ graph commit guard**, never the reverse.
static GRAPH_WRITE_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| {
    (0..GRAPH_WRITE_LOCK_SHARDS)
        .map(|_| Mutex::new(()))
        .collect()
});

fn graph_write_lock(graph: &GraphId) -> &'static Mutex<()> {
    let hash = blake3::hash(graph.as_str().as_bytes());
    let shard = u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()) as usize;
    &GRAPH_WRITE_LOCKS[shard % GRAPH_WRITE_LOCK_SHARDS]
}

/// Acquire a graph's engine-level write lock; see [`GRAPH_WRITE_LOCKS`] for
/// what it orders and for the lock order it belongs to.
pub(crate) fn graph_write_guard(graph: &GraphId) -> MutexGuard<'static, ()> {
    graph_write_lock(graph)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The replication engine: local writes and CRDT merge of Irokle records.
pub(crate) struct ReplicationEngine {
    store: Arc<GraphStore>,
    sparql: Arc<SparqlEngine>,
    rules: Vec<Box<dyn Rule>>,
    actor: ActorId,
    sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    #[cfg(feature = "shacl-core")]
    shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    /// Set by a test to fail the next replicated apply with a store error,
    /// standing in for a transient fjall failure. Per-engine rather than global
    /// so concurrent tests cannot arm each other's nodes.
    #[cfg(test)]
    armed_apply_failure: std::sync::atomic::AtomicBool,
}

/// How a write should leave the graph's persisted diagnostics record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnosticsPlan {
    /// Refresh the record as part of this commit, under its guard.
    Immediate,
    /// Bulk import: the caller rebuilds diagnostics once at the end.
    Deferred,
}

impl DiagnosticsPlan {
    /// Run `capture` only when this write refreshes diagnostics immediately.
    fn pending_diagnostics<F>(&self, capture: F) -> crate::store::Result<Option<PendingDiagnostics>>
    where
        F: FnOnce() -> crate::store::Result<PendingDiagnostics>,
    {
        match self {
            Self::Immediate => capture().map(Some),
            Self::Deferred => Ok(None),
        }
    }
}

/// A local write, ready to be committed to one graph.
struct LocalCommit<'a> {
    graph: &'a GraphId,
    changes: Vec<MaterializedQuadChange>,
    plan: DiagnosticsPlan,
    validate_rules: bool,
}

#[cfg(feature = "shacl-core")]
struct ShaclEvaluation {
    binding: crate::ShaclBinding,
    schema: Option<crate::CompiledShaclSchema>,
    result: std::result::Result<crate::ShaclValidationReport, String>,
    data_version: Option<[u8; 32]>,
    shapes_version: [u8; 32],
    schema_fingerprint: [u8; 32],
    shape_versions: Vec<(GraphId, [u8; 32])>,
}

impl ReplicationEngine {
    #[cfg(any(not(feature = "shacl-core"), test))]
    pub(crate) fn new(store: Arc<GraphStore>, sparql: Arc<SparqlEngine>, actor: ActorId) -> Self {
        #[cfg(feature = "shacl-core")]
        {
            let shacl = Arc::new(crate::shacl_impl::ShaclCompiler::new(store.clone()));
            Self::new_sync_shacl(store, sparql, actor, None, shacl)
        }
        #[cfg(not(feature = "shacl-core"))]
        {
            Self::new_with_sync(store, sparql, actor, None)
        }
    }

    #[cfg(any(not(feature = "shacl-core"), test))]
    pub(crate) fn new_with_sync(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    ) -> Self {
        #[cfg(feature = "shacl-core")]
        {
            let shacl = Arc::new(crate::shacl_impl::ShaclCompiler::new(store.clone()));
            Self::new_sync_shacl(store, sparql, actor, sync, shacl)
        }
        #[cfg(not(feature = "shacl-core"))]
        {
            Self {
                store,
                sparql,
                rules: crate::rules::default_rules(),
                actor,
                sync,
                #[cfg(test)]
                armed_apply_failure: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn new_with_shacl(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        actor: ActorId,
        shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    ) -> Self {
        Self::new_sync_shacl(store, sparql, actor, None, shacl)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn new_sync_shacl(
        store: Arc<GraphStore>,
        sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
        shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    ) -> Self {
        Self {
            store,
            sparql,
            rules: crate::rules::default_rules(),
            actor,
            sync,
            shacl,
            #[cfg(test)]
            armed_apply_failure: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn store(&self) -> &Arc<GraphStore> {
        &self.store
    }

    /// Make the next replicated apply fail with a store error. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_apply_failure(&self) {
        self.armed_apply_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Consumes a pending injected failure, reporting whether one was armed.
    #[cfg(test)]
    pub(crate) fn take_apply_failure(&self) -> bool {
        self.armed_apply_failure
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// Persist a graph's render hints (last-write-wins) and replicate them when
    /// sync is configured, minting one tag for both the local write and the event.
    ///
    /// Publish-first, and load-bearing (G4): storing locally first would leave a
    /// failed publish looking like success, so the retry would trip
    /// `store_import_context`'s unchanged-state short-circuit and peers would
    /// never receive the update.
    pub(crate) fn set_graph_context(
        &self,
        graph: &GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
    ) -> Result<(), UpdateError> {
        // Bind the topic before taking any lock. `ensure_graph_topic` reaches
        // `GraphStore::set_irokle_topic_id`, which takes the graph commit guard
        // itself; doing it under a lock we also hold would deadlock (addendum
        // A1). It is idempotent and, after the first call, a memo hit.
        if let Some(sync) = &self.sync {
            sync.ensure_graph_topic(&self.store, graph)?;
        }

        // Guards the context-tag mint through to the store write; see
        // GRAPH_WRITE_LOCKS.
        let _write_guard = graph_write_guard(graph);

        let tag = ContextTag::next_local(self.store.graph_context_tag(graph)?, self.actor);
        if let Some(sync) = &self.sync {
            sync.publish_context(
                &self.store,
                graph,
                context.clone(),
                license.clone(),
                license_digest,
                tag,
            )?;
        }
        self.store.set_graph_context(
            graph,
            context.as_deref(),
            license.as_deref(),
            license_digest,
            tag,
        )?;
        Ok(())
    }

    /// Execute a SPARQL Update locally with full validation.
    /// Returns `None` if the update produced no changes.
    pub(crate) fn local_update(&self, sparql_update: &str) -> Result<Option<Batch>, UpdateError> {
        let changes = self.sparql.evaluate_update(sparql_update)?;

        if changes.is_empty() {
            return Ok(None);
        }

        let graph = match &changes[0] {
            MaterializedQuadChange::Insert { graph, .. }
            | MaterializedQuadChange::Delete { graph, .. } => graph.clone(),
        };
        self.ensure_change_set_targets(&graph, &changes)?;

        self.commit_changes(&graph, changes).map(Some)
    }

    /// Insert raw quads (bypasses SPARQL, still validates).
    pub(crate) fn local_insert_quads(
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
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %graph.as_str(), change_count = changes.len()))]
    pub(crate) fn local_apply_changes(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes(graph, changes)
    }

    /// Apply a pre-materialized change set locally without full graph validation.
    ///
    /// Intended for trusted higher-level RO-Crate operations that maintain
    /// structural invariants incrementally.
    pub(crate) fn local_apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Immediate,
            validate_rules: false,
        })
    }

    /// Apply a trusted bulk change set locally and defer graph-diagnostics
    /// recomputation until the caller explicitly rebuilds diagnostics.
    pub(crate) fn local_apply_changes_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Deferred,
            validate_rules: false,
        })
    }

    pub(crate) fn local_apply_bulk(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;
        if changes.is_empty() {
            return self.empty_batch(graph);
        }
        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Deferred,
            validate_rules: true,
        })
    }

    pub(crate) fn rebuild_graph_diagnostics(&self, graph: &GraphId) -> Result<(), UpdateError> {
        // Guards the recompute→persist cycle so the record cannot be tagged with
        // a clock newer than the state it describes.
        let _commit_guard = self.store.graph_commit_guard(graph);
        self.recompute_graph_diagnostics(graph)
            .map_err(UpdateError::Store)
    }

    fn empty_batch(&self, graph: &GraphId) -> Result<Batch, UpdateError> {
        Ok(Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter: 0,
            base_clock: self.store.get_vector_clock(graph)?,
            ops: vec![],
            timestamp: Utc::now(),
        })
    }

    fn validate(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        let change_set = ChangeSet {
            store: &self.store,
            graph,
            delta: changes,
        };
        match crate::rules::validate_change_set(&self.rules, change_set) {
            Ok(()) => Ok(()),
            Err(crate::rules::RuleEvaluationError::Store(error)) => Err(UpdateError::Store(error)),
            Err(crate::rules::RuleEvaluationError::Violations(violations)) => {
                Err(UpdateError::ValidationFailed(violations))
            }
        }
    }

    #[cfg(feature = "shacl-core")]
    fn evaluate_enforce(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<Vec<ShaclEvaluation>, UpdateError> {
        let statuses = self.store.shacl_binding_statuses(graph)?;
        let data_version = self.store.graph_version_digest(graph)?;
        let mut evaluations = Vec::new();
        let mut violations = Vec::new();
        for status in statuses {
            if status.binding.policy != crate::ValidationPolicy::Enforce {
                continue;
            }
            let shapes_version = self
                .store
                .graph_version_digest(&status.binding.shapes_graph)
                .unwrap_or([0; 32]);
            let evaluated = self.evaluate_binding(status, data_version, shapes_version, changes);
            match evaluated {
                Ok((binding, schema, report)) => {
                    if !report.conforms {
                        violations.push(report.clone());
                    }
                    let schema_fingerprint = schema.plan_fingerprint();
                    let shape_versions = schema.shape_versions().to_vec();
                    evaluations.push(ShaclEvaluation {
                        binding,
                        schema: Some(schema),
                        result: Ok(report),
                        data_version: None,
                        shapes_version,
                        schema_fingerprint,
                        shape_versions,
                    });
                }
                Err(error) => return Err(map_update_error(error)),
            }
        }
        if !violations.is_empty() {
            return Err(UpdateError::ShaclValidationFailed(violations));
        }
        Ok(evaluations)
    }

    #[cfg(feature = "shacl-core")]
    fn evaluate_binding(
        &self,
        status: crate::ShaclBindingStatus,
        data_version: [u8; 32],
        shapes_version: [u8; 32],
        changes: &[MaterializedQuadChange],
    ) -> crate::Result<(
        crate::ShaclBinding,
        crate::CompiledShaclSchema,
        crate::ShaclValidationReport,
    )> {
        let binding = status.binding;
        let schema = self.shacl.compile(
            &binding.shapes_graph,
            &binding.validation_options.compile_options(),
        )?;
        if !changes.is_empty()
            && schema
                .shape_versions()
                .iter()
                .any(|(dependency, _)| dependency == &binding.data_graph)
        {
            return Err(crate::ShaclError::ShapesGraphMutationUnsupported {
                graph: binding.data_graph.to_string(),
            }
            .into());
        }
        let options = binding.validation_options.validation_options();
        if status.data_version == data_version
            && status.shapes_version == shapes_version
            && status.schema_fingerprint == schema.plan_fingerprint()
            && let Some(report) = status.report
        {
            self.shacl.seed_validation_report(
                &binding.data_graph,
                &schema,
                data_version,
                &options,
                report,
            );
        }
        let report = self
            .shacl
            .validate_delta(&binding.data_graph, &schema, changes, &options)?;
        if !self.shacl.versions_are_current(schema.shape_versions())? {
            return Err(crate::ShaclError::SchemaChangedDuringValidation {
                graph: binding.shapes_graph.to_string(),
            }
            .into());
        }
        Ok((binding, schema, report))
    }

    #[cfg(feature = "shacl-core")]
    fn stamp_evaluations(
        &self,
        graph: &GraphId,
        evaluations: &mut [ShaclEvaluation],
    ) -> crate::store::Result<[u8; 32]> {
        let data_version = self.store.graph_version_digest(graph)?;
        for evaluation in evaluations {
            evaluation.data_version = Some(data_version);
        }
        Ok(data_version)
    }

    #[cfg(feature = "shacl-core")]
    fn persist_shacl_evaluations(
        &self,
        graph: &GraphId,
        evaluations: Vec<ShaclEvaluation>,
    ) -> crate::store::Result<()> {
        if evaluations.is_empty() {
            return Ok(());
        }
        let _binding_guard = self.store.binding_guard();
        let data_version = self.store.graph_version_digest(graph)?;
        let current = self.store.shacl_binding_statuses(graph)?;
        let mut batch = self.store.new_batch();
        for evaluation in evaluations {
            if !current
                .iter()
                .any(|status| status.binding == evaluation.binding)
            {
                continue;
            }
            let shapes_version = self
                .store
                .graph_version_digest(&evaluation.binding.shapes_graph)
                .unwrap_or([0; 32]);
            let mut status = crate::ShaclBindingStatus {
                binding: evaluation.binding.clone(),
                state: crate::ShaclValidationState::Pending,
                report: None,
                error: None,
                data_version,
                shapes_version,
                schema_fingerprint: evaluation.schema_fingerprint,
                shape_versions: evaluation.shape_versions,
            };
            if evaluation.data_version == Some(data_version)
                && shapes_version == evaluation.shapes_version
            {
                match evaluation.result {
                    Ok(report)
                        if self
                            .shacl
                            .versions_are_current(&status.shape_versions)
                            .unwrap_or(false) =>
                    {
                        status.state = if report.conforms {
                            crate::ShaclValidationState::Valid
                        } else {
                            crate::ShaclValidationState::Invalid
                        };
                        if let Some(schema) = &evaluation.schema {
                            let options =
                                evaluation.binding.validation_options.validation_options();
                            let _ = self.shacl.cache_current_report(
                                graph,
                                schema,
                                &options,
                                report.clone(),
                            );
                        }
                        status.report = Some(report);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        status.state = crate::ShaclValidationState::Failed;
                        status.error = Some(error);
                    }
                }
            }
            self.store.stage_binding_status(&mut batch, &status)?;
        }
        self.store.commit(batch)
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
        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Immediate,
            validate_rules: true,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %commit.graph.as_str(), change_count = commit.changes.len(), sync_enabled = self.sync.is_some()))]
    fn commit_changes_with_plan(&self, commit: LocalCommit<'_>) -> Result<Batch, UpdateError> {
        let LocalCommit {
            graph,
            changes,
            plan,
            validate_rules,
        } = commit;

        if let Some(sync) = &self.sync {
            // Topic binding may take the graph commit guard itself, so finish
            // that idempotent step before acquiring either write guard.
            sync.ensure_graph_topic(&self.store, graph)?;
            // Orders this graph's publish against its own apply; see
            // GRAPH_WRITE_LOCKS. Taken before the publish and held across it.
            let _write_guard = graph_write_guard(graph);

            // A deleted graph stays deleted. Publishing alone would resurrect
            // it: binding the topic writes the graph's metadata record back.
            // Every tombstone writer takes the lock held here, so a delete
            // cannot land between this check and the publish.
            if self.store.graph_tombstoned(graph)? {
                return self.empty_batch(graph);
            }

            let _commit_guard = self.store.graph_commit_guard(graph);
            #[cfg(feature = "shacl-core")]
            let binding_guard = self.store.binding_guard();
            if validate_rules {
                self.validate(graph, &changes)?;
            }
            #[cfg(feature = "shacl-core")]
            let mut shacl_evaluations = if validate_rules {
                self.evaluate_enforce(graph, &changes)?
            } else {
                Vec::new()
            };

            // Publish-first (G4): no source state changes until the event is
            // durable in the topic. A failed publish therefore leaves the
            // validated candidate unapplied.
            let record = sync.publish_changes(&self.store, graph, changes)?;
            let Some(batch) = crate::sync::batch_from_owned(record)? else {
                return Err(UpdateError::InvalidChangeSet(
                    "irokle changes publish did not return a quad-change record".to_string(),
                ));
            };
            let _merged = self
                .apply_irokle_guarded(&batch, plan)
                .map_err(update_error_from_merge)?;
            #[cfg(feature = "shacl-core")]
            {
                if _merged.applied {
                    self.stamp_evaluations(graph, &mut shacl_evaluations)?;
                }
                drop(_commit_guard);
                drop(binding_guard);
                drop(_write_guard);
                if _merged.applied {
                    let _ = self.persist_shacl_evaluations(graph, shacl_evaluations);
                }
            }
            return Ok(batch);
        }

        // `create_graph` is self-guarding, so the graph must exist *before* the
        // commit guard is taken; the guard is not reentrant. (The sync branch
        // leaves this to the apply, which does the same thing before its guard.)
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Guards the whole read→write cycle of this graph's CRDT state: the
        // diagnostics read, the clock read, the counter mint, every quad op, the
        // clock write, the FTS enqueue, the commit and the diagnostics refresh
        // (G1, G2, G5, G6).
        let _commit_guard = self.store.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let binding_guard = self.store.binding_guard();

        if validate_rules {
            self.validate(graph, &changes)?;
        }
        #[cfg(feature = "shacl-core")]
        let mut shacl_evaluations = if validate_rules {
            self.evaluate_enforce(graph, &changes)?
        } else {
            Vec::new()
        };

        // Captured before the write, under the guard, so it describes exactly
        // the state this commit starts from. Skipped entirely when the caller
        // defers the refresh — reading it can itself force a recompute.
        let pending = plan.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_delta(graph, &changes),
            })
        })?;

        let mut batch = self.store.new_batch();
        let mut vector_clock = self.store.get_vector_clock(graph)?;
        let graph_id = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;
        let counter = self.store.next_counter(
            &mut batch,
            CounterKey {
                graph_id,
                actor: self.actor,
            },
        )?;
        let dot = Dot {
            actor: self.actor,
            counter,
        };
        let base_clock = vector_clock.clone();
        vector_clock.advance(self.actor, counter);

        let mut ops = Vec::with_capacity(changes.len());
        let mut affected_subjects = HashSet::new();
        let mut term_cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut term_cache,
        };

        self.store.seed_term_cache(
            &mut cx,
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
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject: &subject,
                            predicate: &predicate,
                            object: &object,
                        },
                    )?;
                    self.store.insert_quad(cx.batch, QuadAdd { quad, dot })?;
                    affected_subjects.insert(quad.subject);
                    ops.push(QuadOp::Add {
                        subject,
                        predicate,
                        object,
                        dot,
                    });
                }
                MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject: &subject,
                            predicate: &predicate,
                            object: &object,
                        },
                    )?;
                    self.store.remove_quad(
                        cx.batch,
                        QuadRemove {
                            quad,
                            witnessed: &vector_clock,
                        },
                    )?;
                    affected_subjects.insert(quad.subject);
                    ops.push(QuadOp::Remove {
                        subject,
                        predicate,
                        object,
                        witnessed: vector_clock.clone(),
                    });
                }
            }
        }

        self.store.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: &vector_clock,
            },
        )?;
        self.store.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &affected_subjects,
            },
        )?;
        #[cfg(feature = "shacl-core")]
        self.store.stage_pending_bindings(&mut batch, graph)?;
        self.store.commit(batch)?;
        #[cfg(feature = "shacl-core")]
        self.stamp_evaluations(graph, &mut shacl_evaluations)?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)
                .map_err(UpdateError::Store)?;
        }
        #[cfg(feature = "shacl-core")]
        {
            drop(_commit_guard);
            drop(binding_guard);
            let _ = self.persist_shacl_evaluations(graph, shacl_evaluations);
        }

        Ok(Batch {
            graph: graph.clone(),
            actor: self.actor,
            counter,
            base_clock,
            ops,
            timestamp: Utc::now(),
        })
    }

    fn resolve_quad(
        &self,
        cx: &mut BatchTermCtx<'_>,
        terms: QuadTerms<'_>,
    ) -> crate::store::Result<EncodedQuad> {
        Ok(EncodedQuad {
            graph: terms.graph_id,
            subject: self.store.resolve_term_cached(cx, terms.subject)?,
            predicate: self.store.resolve_term_cached(cx, terms.predicate)?,
            object: self.store.resolve_term_cached(cx, terms.object)?,
        })
    }

    /// Apply a causally ordered batch produced from an Irokle graph event.
    /// **Call with the graph's write lock held.**
    ///
    /// Irokle actor sequences include genesis and topic-control operations, so
    /// they are not contiguous over Craqle domain events. The Irokle DAG already
    /// enforces causal delivery; this path intentionally bypasses Craqle's old
    /// vector-clock gap buffering while preserving OR-Set add/remove semantics.
    pub(crate) fn apply_irokle_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        self.apply_irokle_batch_with_plan(&incoming, DiagnosticsPlan::Immediate)
    }

    /// **Call with the graph's write lock held.** Every caller does, and so
    /// does every writer of a graph tombstone, which is what makes the check
    /// below atomic against a concurrent delete.
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %incoming.graph.as_str(), op_count = incoming.ops.len()))]
    fn apply_irokle_batch_with_plan(
        &self,
        incoming: &Batch,
        plan: DiagnosticsPlan,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        // A deleted graph stays deleted. This is also the *local* write's apply
        // path, which never passes through `CraqleNode::apply_irokle_record`,
        // so without the check a write racing a delete re-creates the graph the
        // delete just tombstoned — and the tombstone then drops every later
        // replicated record for it, so replication can never repair the
        // divergence.
        if self.store.graph_tombstoned(graph)? {
            return Ok(MergeResult { applied: false });
        }

        // Self-guarding, so it must run before the commit guard is taken.
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Guards the dedup gate through to the diagnostics refresh: the clock
        // read that decides "already applied" must be the same clock the apply
        // then advances, or a concurrent commit can make one batch apply twice
        // or the clock lose an entry (G1, G2).
        let _commit_guard = self.store.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.store.binding_guard();

        self.apply_irokle_guarded(incoming, plan)
    }

    fn apply_irokle_guarded(
        &self,
        incoming: &Batch,
        plan: DiagnosticsPlan,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        let mut vector_clock = self.store.get_vector_clock(graph)?;
        if vector_clock.contains(&Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        }) {
            return Ok(MergeResult { applied: false });
        }

        let pending = plan.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_ops(graph, &incoming.ops),
            })
        })?;

        self.apply_single_batch(incoming, &mut vector_clock)?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)?;
        }
        Ok(MergeResult { applied: true })
    }

    pub(crate) fn apply_irokle_record(
        &self,
        record: &irokle::reducer::EventRecord<crate::sync::CraqleGraphEvent>,
    ) -> Result<Option<MergeResult>, MergeError> {
        #[cfg(test)]
        if self.take_apply_failure() {
            return Err(MergeError::Store(crate::store::StoreError::Fjall(
                fjall::Error::Io(std::io::Error::other("injected apply failure")),
            )));
        }
        let batch = crate::sync::batch_from_record(record)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        batch
            .map(|batch| self.apply_irokle_batch(batch))
            .transpose()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %incoming.graph.as_str(), op_count = incoming.ops.len()))]
    fn apply_single_batch(
        &self,
        incoming: &Batch,
        vector_clock: &mut VectorClock,
    ) -> Result<(), MergeError> {
        let graph = &incoming.graph;
        let mut batch = self.store.new_batch();
        let mut affected_subjects = HashSet::new();
        let mut term_cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut term_cache,
        };

        self.store.seed_term_cache(
            &mut cx,
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
        )?;

        let graph_id = self
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
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject,
                            predicate,
                            object,
                        },
                    )?;
                    self.store
                        .insert_quad(cx.batch, QuadAdd { quad, dot: *dot })?;
                    affected_subjects.insert(quad.subject);
                }
                QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => {
                    let quad = self.resolve_quad(
                        &mut cx,
                        QuadTerms {
                            graph_id,
                            subject,
                            predicate,
                            object,
                        },
                    )?;
                    self.store
                        .remove_quad(cx.batch, QuadRemove { quad, witnessed })?;
                    affected_subjects.insert(quad.subject);
                }
            }
        }

        vector_clock.advance(incoming.actor, incoming.counter);
        self.store.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: vector_clock,
            },
        )?;
        self.store.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &affected_subjects,
            },
        )?;
        #[cfg(feature = "shacl-core")]
        self.store.stage_pending_bindings(&mut batch, graph)?;
        self.store.commit(batch)?;
        Ok(())
    }

    /// Bring the persisted diagnostics record back in step with the state the
    /// commit just produced. **Call with the graph commit guard held.**
    ///
    /// Re-stamps the previous verdict when the write cannot have moved the orphan
    /// set; otherwise recomputes.
    ///
    /// Validation success is not an orphan verdict; only the dependency summary
    /// proves when the previous set remains exact.
    fn settle_diagnostics(
        &self,
        graph: &GraphId,
        pending: &PendingDiagnostics,
    ) -> crate::store::Result<()> {
        // Case 1. `orphaned_data_entities` reads exactly two triple shapes:
        // `?s rdf:type schema:Dataset|schema:MediaObject` (which entities count
        // as data entities) and `?s schema:hasPart ?o` (which adds to that set
        // and forms every edge of the reachability graph). Nothing else in the
        // graph can affect it. `touches_reachability` is set by exactly those
        // two shapes, so a write that leaves it clear provably leaves the orphan
        // set identical — the previous verdict is still exact and only needs
        // re-stamping so its clock tag matches the new state (G6).
        if !pending.summary.touches_reachability() {
            return self.publish_graph_diagnostics(graph, &pending.previous);
        }

        // Case 2. `pending.previous` is not the post-write set, so recompute.
        self.recompute_graph_diagnostics(graph)
    }

    /// Recompute the orphan set from post-write state and publish it (G6, G7).
    fn recompute_graph_diagnostics(&self, graph: &GraphId) -> crate::store::Result<()> {
        // The commit already made the stored record's clock tag stale, so this
        // read recomputes. It does not persist: this is the record's writer.
        let current = self.store.graph_diagnostics(graph)?;
        self.publish_graph_diagnostics(graph, &current)
    }

    /// Persist `current` as the graph's orphan record and re-queue for search
    /// every entity whose orphan status differs from the last persisted set.
    ///
    /// The baseline is the *persisted* record, never a caller's pre-write read:
    /// a deferred bulk write that has committed but not yet rebuilt leaves that
    /// read already reflecting flips the index has never seen, and persisting it
    /// would strand them (G7).
    ///
    /// Re-queue first, record second: they are separate commits, and a crash
    /// between them must leave the older baseline so the next rebuild re-queues.
    fn publish_graph_diagnostics(
        &self,
        graph: &GraphId,
        current: &GraphDiagnostics,
    ) -> crate::store::Result<()> {
        let baseline = self.store.last_persisted_diagnostics(graph)?;
        if baseline != *current {
            self.queue_orphan_updates(
                graph,
                OrphanChange {
                    previous: baseline,
                    current: current.clone(),
                },
            )?;
        }
        self.store.set_graph_diagnostics(graph, current)
    }

    fn queue_orphan_updates(
        &self,
        graph: &GraphId,
        change: OrphanChange,
    ) -> crate::store::Result<()> {
        let previous: HashSet<&String> = change.previous.orphaned_entities.iter().collect();
        let current: HashSet<&String> = change.current.orphaned_entities.iter().collect();
        let Some(graph_id) = self
            .store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))?
        else {
            return Ok(());
        };
        let mut batch = self.store.new_batch();
        let mut dirty = false;

        for entity_id in previous.symmetric_difference(&current) {
            // `from_subject_id`, not `from_named_node`: a blank node is stored
            // as `_:b0`, and the IRI `<_:b0>` would miss the lookup.
            let subject = EncodedTerm::from_subject_id(entity_id.as_str());
            let Some(subject_tid) = self.store.lookup_term(&subject)? else {
                // A literal cannot be re-encoded as a subject, so its search
                // document stays stale until something else dirties it.
                tracing::warn!(
                    entity = entity_id.as_str(),
                    "orphan re-queue skipped an entity it could not look up"
                );
                continue;
            };
            self.store.enqueue_fts(
                &mut batch,
                FtsSubject {
                    graph_id,
                    subject: subject_tid,
                },
            )?;
            dirty = true;
        }

        if dirty {
            self.store.commit(batch)?;
        }

        Ok(())
    }
}

/// The four term ids of one quad, before interning.
struct QuadTerms<'a> {
    graph_id: TermId,
    subject: &'a EncodedTerm,
    predicate: &'a EncodedTerm,
    object: &'a EncodedTerm,
}

/// Diagnostics inputs captured before a write, under the commit guard.
struct PendingDiagnostics {
    /// The record as it stood before the commit.
    previous: GraphDiagnostics,
    /// What the write touches, in the terms the orphan set depends on.
    summary: DeltaSummary,
}

/// The orphan set before and after a recompute.
struct OrphanChange {
    previous: GraphDiagnostics,
    current: GraphDiagnostics,
}

#[cfg(feature = "shacl-core")]
fn map_update_error(error: crate::CraqleError) -> UpdateError {
    match error {
        crate::CraqleError::Store(error) => UpdateError::Store(error),
        crate::CraqleError::Shacl(error) => UpdateError::Shacl(error),
        error => UpdateError::InvalidChangeSet(error.to_string()),
    }
}

fn update_error_from_merge(error: MergeError) -> UpdateError {
    match error {
        MergeError::Store(error) => UpdateError::Store(error),
        MergeError::InputRejected(message) => UpdateError::InvalidChangeSet(message),
    }
}
