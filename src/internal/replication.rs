use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
#[cfg(feature = "shacl-core")]
use std::time::{Duration, Instant};

use crate::core::*;
#[cfg(feature = "shacl-core")]
use crate::rdf_read::StoreReadView;
use crate::rules::{ChangeSet, DeltaSummary, Rule};
use crate::sparql::SparqlEngine;
#[cfg(feature = "shacl-core")]
use crate::store::BindingGuard;
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
    #[cfg(feature = "shacl-core")]
    #[error("prepared state is stale: {fence}")]
    StalePreparedState { fence: String },
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

impl UpdateError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Sparql(error) => error.kind(),
            Self::ValidationFailed(_) | Self::InvalidChangeSet(_) => {
                crate::CraqleErrorKind::InvalidInput
            }
            #[cfg(feature = "shacl-core")]
            Self::Shacl(error) => error.kind(),
            #[cfg(feature = "shacl-core")]
            Self::ShaclValidationFailed(_) => crate::CraqleErrorKind::InvalidInput,
            #[cfg(feature = "shacl-core")]
            Self::StalePreparedState { .. } => crate::CraqleErrorKind::StalePreparedState,
            Self::Store(error) => error.kind(),
            Self::Sync(error) => error.kind(),
        }
    }
}

impl MergeError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Store(error) => error.kind(),
            Self::InputRejected(_) => crate::CraqleErrorKind::InvalidInput,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MergeResult {
    pub applied: bool,
}

/// Number of shards backing [`GRAPH_WRITE_LOCKS`].
const GRAPH_WRITE_LOCK_SHARDS: usize = 32;
#[cfg(feature = "shacl-core")]
const SHACL_WRITE_RETRIES: usize = 3;

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
    /// Test-only failure after the source commit and before SHACL settlement.
    #[cfg(all(test, feature = "shacl-core"))]
    armed_settle_failure_after: std::sync::atomic::AtomicUsize,
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
    #[cfg(feature = "shacl-core")]
    prepared_fence: Option<PreparedCommitFence<'a>>,
}

#[cfg(feature = "shacl-core")]
struct PreparedCommitFence<'a> {
    data_version: Option<[u8; 32]>,
    shape_versions: &'a [(GraphId, [u8; 32])],
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
    refresh_dependencies: bool,
}

#[cfg(feature = "shacl-core")]
struct BindingWork<'store> {
    base: StoreReadView<'store>,
    data_version: [u8; 32],
    statuses: Vec<crate::ShaclBindingStatus>,
}

#[cfg(feature = "shacl-core")]
struct PreparedShaclWrite<'store> {
    data_version: [u8; 32],
    statuses: Vec<crate::ShaclBindingStatus>,
    advisory_work: Option<BindingWork<'store>>,
    enforce_evaluations: Vec<ShaclEvaluation>,
}

#[cfg(feature = "shacl-core")]
struct SettlementTimer<'store> {
    store: &'store GraphStore,
    started: Instant,
}

#[cfg(feature = "shacl-core")]
impl Drop for SettlementTimer<'_> {
    fn drop(&mut self) {
        self.store.record_settlement(self.started.elapsed());
    }
}

impl ReplicationEngine {
    #[cfg(any(not(feature = "shacl-core"), test))]
    pub(crate) fn new(store: Arc<GraphStore>, _sparql: Arc<SparqlEngine>, actor: ActorId) -> Self {
        #[cfg(feature = "shacl-core")]
        {
            let shacl = Arc::new(crate::shacl_impl::ShaclCompiler::new(store.clone()));
            Self::new_sync_shacl(store, _sparql, actor, None, shacl)
        }
        #[cfg(not(feature = "shacl-core"))]
        {
            Self::new_with_sync(store, _sparql, actor, None)
        }
    }

    #[cfg(any(not(feature = "shacl-core"), test))]
    pub(crate) fn new_with_sync(
        store: Arc<GraphStore>,
        _sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    ) -> Self {
        #[cfg(feature = "shacl-core")]
        {
            let shacl = Arc::new(crate::shacl_impl::ShaclCompiler::new(store.clone()));
            Self::new_sync_shacl(store, _sparql, actor, sync, shacl)
        }
        #[cfg(not(feature = "shacl-core"))]
        {
            Self {
                store,
                rules: crate::rules::default_rules(),
                actor,
                sync,
                #[cfg(test)]
                armed_apply_failure: std::sync::atomic::AtomicBool::new(false),
                #[cfg(all(test, feature = "shacl-core"))]
                armed_settle_failure_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
            }
        }
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn new_with_shacl(
        store: Arc<GraphStore>,
        _sparql: Arc<SparqlEngine>,
        actor: ActorId,
        shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    ) -> Self {
        Self::new_sync_shacl(store, _sparql, actor, None, shacl)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn new_sync_shacl(
        store: Arc<GraphStore>,
        _sparql: Arc<SparqlEngine>,
        actor: ActorId,
        sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
        shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    ) -> Self {
        Self {
            store,
            rules: crate::rules::default_rules(),
            actor,
            sync,
            shacl,
            #[cfg(test)]
            armed_apply_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            armed_settle_failure_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
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

    /// Make the next SHACL settlement fail after the source commit. Test-only.
    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn arm_settle_failure(&self) {
        self.arm_settle_failure_after(0);
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn arm_settle_failure_after(&self, successful_settlements: usize) {
        self.armed_settle_failure_after
            .store(successful_settlements, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(all(test, feature = "shacl-core"))]
    fn take_settle_failure(&self) -> bool {
        loop {
            let remaining = self
                .armed_settle_failure_after
                .load(std::sync::atomic::Ordering::SeqCst);
            if remaining == usize::MAX {
                return false;
            }
            let next = if remaining == 0 {
                usize::MAX
            } else {
                remaining - 1
            };
            if self
                .armed_settle_failure_after
                .compare_exchange(
                    remaining,
                    next,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                return remaining == 0;
            }
        }
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
            #[cfg(feature = "shacl-core")]
            prepared_fence: None,
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
            #[cfg(feature = "shacl-core")]
            prepared_fence: None,
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
            #[cfg(feature = "shacl-core")]
            prepared_fence: None,
        })
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn local_apply_bulk_prepared(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        data_version: Option<[u8; 32]>,
        shape_versions: &[(GraphId, [u8; 32])],
    ) -> Result<Batch, UpdateError> {
        self.ensure_change_set_targets(graph, &changes)?;
        let fence = PreparedCommitFence {
            data_version,
            shape_versions,
        };
        if changes.is_empty() {
            let _write_guard = graph_write_guard(graph);
            let _commit_guard = self.store.graph_commit_guard(graph);
            self.ensure_prepared_data_current(graph, &fence)?;
            let _binding_guard = self.store.binding_guard();
            self.ensure_prepared_shapes_current(&fence)?;
            return self.empty_batch(graph);
        }
        self.commit_changes_with_plan(LocalCommit {
            graph,
            changes,
            plan: DiagnosticsPlan::Deferred,
            validate_rules: true,
            prepared_fence: Some(fence),
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
        changes: &[MaterializedQuadChange],
        work: BindingWork<'_>,
    ) -> Result<Vec<ShaclEvaluation>, UpdateError> {
        let mut evaluations = Vec::new();
        let mut violations = Vec::new();
        for status in work.statuses {
            let shapes_version = self
                .store
                .graph_version_digest(&status.binding.shapes_graph)?;
            let evaluated =
                self.evaluate_binding(status, work.data_version, shapes_version, changes);
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
                        refresh_dependencies: false,
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
    fn binding_work(
        &self,
        graph: &GraphId,
        policies: &[crate::ValidationPolicy],
    ) -> crate::store::Result<BindingWork<'_>> {
        let data_version = self.store.graph_version_digest(graph)?;
        let statuses = self
            .store
            .shacl_binding_statuses(graph)?
            .into_iter()
            .filter(|status| policies.contains(&status.binding.policy))
            .collect();
        Ok(BindingWork {
            base: StoreReadView::new(&self.store),
            data_version,
            statuses,
        })
    }

    #[cfg(feature = "shacl-core")]
    fn prepare_shacl_write(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        validate_rules: bool,
    ) -> Result<PreparedShaclWrite<'_>, UpdateError> {
        let (data_version, statuses) = {
            let _binding_guard = self.store.binding_guard();
            (
                self.store.graph_version_digest(graph)?,
                self.store.shacl_binding_statuses(graph)?,
            )
        };
        let base = StoreReadView::new(&self.store);
        let advisory_statuses = statuses
            .iter()
            .filter(|status| status.binding.policy == crate::ValidationPolicy::Advisory)
            .cloned()
            .collect::<Vec<_>>();
        let advisory_work = (!advisory_statuses.is_empty()).then(|| BindingWork {
            base: base.clone(),
            data_version,
            statuses: advisory_statuses,
        });
        let enforce_evaluations = if validate_rules {
            let enforce_work = BindingWork {
                base,
                data_version,
                statuses: statuses
                    .iter()
                    .filter(|status| status.binding.policy == crate::ValidationPolicy::Enforce)
                    .cloned()
                    .collect(),
            };
            self.evaluate_enforce(changes, enforce_work)?
        } else {
            Vec::new()
        };
        Ok(PreparedShaclWrite {
            data_version,
            statuses,
            advisory_work,
            enforce_evaluations,
        })
    }

    #[cfg(feature = "shacl-core")]
    fn prepared_shacl_write_is_current(
        &self,
        graph: &GraphId,
        prepared: &PreparedShaclWrite<'_>,
    ) -> crate::store::Result<bool> {
        if self.store.graph_version_digest(graph)? != prepared.data_version
            || self.store.shacl_binding_statuses(graph)? != prepared.statuses
        {
            return Ok(false);
        }
        for evaluation in &prepared.enforce_evaluations {
            let Some(schema) = &evaluation.schema else {
                return Ok(false);
            };
            if schema.plan_fingerprint() != evaluation.schema_fingerprint
                || self
                    .store
                    .graph_version_digest(&evaluation.binding.shapes_graph)?
                    != evaluation.shapes_version
                || !self
                    .shacl
                    .versions_are_current(&evaluation.shape_versions)
                    .map_err(map_store_error)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(feature = "shacl-core")]
    fn prepare_shacl_write_for_commit(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        validate_rules: bool,
    ) -> Result<(BindingGuard<'_>, PreparedShaclWrite<'_>), UpdateError> {
        for _ in 0..SHACL_WRITE_RETRIES {
            let prepared = self.prepare_shacl_write(graph, changes, validate_rules)?;
            let binding_guard = self.store.binding_guard();
            if self.prepared_shacl_write_is_current(graph, &prepared)? {
                return Ok((binding_guard, prepared));
            }
        }
        Err(crate::ShaclError::SchemaChangedDuringValidation {
            graph: graph.to_string(),
        }
        .into())
    }

    #[cfg(feature = "shacl-core")]
    fn evaluate_bindings(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        data_version: [u8; 32],
        work: BindingWork<'_>,
    ) -> crate::store::Result<Vec<ShaclEvaluation>> {
        let mut evaluations = Vec::new();
        for status in work.statuses {
            let previous_schema = status.schema_fingerprint;
            let previous_versions = status.shape_versions.clone();
            let binding = status.binding.clone();
            let shapes_graph = binding.shapes_graph.to_string();
            let shapes_version = self.store.graph_version_digest(&binding.shapes_graph)?;
            let result = self
                .shacl
                .compile(
                    &binding.shapes_graph,
                    &binding.validation_options.compile_options(),
                )
                .and_then(|schema| {
                    let base_report = (status.data_version == work.data_version
                        && status.shapes_version == shapes_version
                        && status.schema_fingerprint == schema.plan_fingerprint())
                    .then_some(status.report)
                    .flatten();
                    self.shacl
                        .validate_delta_from(
                            work.base.clone(),
                            graph,
                            &schema,
                            changes,
                            &binding.validation_options.validation_options(),
                            base_report,
                        )
                        .map(|report| (schema, report))
                });
            match result {
                Ok((schema, report))
                    if self
                        .shacl
                        .versions_are_current(schema.shape_versions())
                        .map_err(map_store_error)? =>
                {
                    let schema_fingerprint = schema.plan_fingerprint();
                    let shape_versions = schema.shape_versions().to_vec();
                    evaluations.push(ShaclEvaluation {
                        binding,
                        schema: Some(schema),
                        result: Ok(report),
                        data_version: Some(data_version),
                        shapes_version,
                        schema_fingerprint,
                        shape_versions,
                        refresh_dependencies: false,
                    });
                }
                Ok(_) => evaluations.push(ShaclEvaluation {
                    binding,
                    schema: None,
                    result: Err(crate::ShaclError::SchemaChangedDuringValidation {
                        graph: shapes_graph,
                    }
                    .to_string()),
                    data_version: Some(data_version),
                    shapes_version,
                    schema_fingerprint: previous_schema,
                    shape_versions: previous_versions.clone(),
                    refresh_dependencies: false,
                }),
                Err(crate::CraqleError::Store(error)) => return Err(error),
                Err(error) => {
                    let refresh_dependencies = stable_error(&error);
                    let shape_versions = if refresh_dependencies {
                        self.error_versions(&binding, &previous_versions)?
                    } else {
                        previous_versions
                    };
                    evaluations.push(ShaclEvaluation {
                        binding,
                        schema: None,
                        result: Err(error.to_string()),
                        data_version: Some(data_version),
                        shapes_version,
                        schema_fingerprint: previous_schema,
                        shape_versions,
                        refresh_dependencies,
                    });
                }
            }
        }
        Ok(evaluations)
    }

    #[cfg(feature = "shacl-core")]
    fn evaluate_current_bindings(
        &self,
        graph: &GraphId,
    ) -> crate::store::Result<Vec<ShaclEvaluation>> {
        let statuses = {
            let _binding_guard = self.store.binding_guard();
            self.store.shacl_binding_statuses(graph)?
        };
        let data_version = Some(self.store.graph_version_digest(graph)?);
        let mut evaluations = Vec::new();
        for status in statuses {
            if status.state != crate::ShaclValidationState::Pending {
                continue;
            }
            let previous_schema = status.schema_fingerprint;
            let previous_versions = status.shape_versions;
            let binding = status.binding;
            let shapes_graph = binding.shapes_graph.to_string();
            if binding.policy == crate::ValidationPolicy::Disabled {
                continue;
            }
            let shapes_version = self.store.graph_version_digest(&binding.shapes_graph)?;
            let result = self
                .shacl
                .compile(
                    &binding.shapes_graph,
                    &binding.validation_options.compile_options(),
                )
                .and_then(|schema| {
                    self.shacl
                        .validate(
                            graph,
                            &schema,
                            &binding.validation_options.validation_options(),
                            false,
                        )
                        .map(|report| (schema, report))
                });
            match result {
                Ok((schema, report))
                    if self
                        .shacl
                        .versions_are_current(schema.shape_versions())
                        .map_err(map_store_error)? =>
                {
                    let schema_fingerprint = schema.plan_fingerprint();
                    let shape_versions = schema.shape_versions().to_vec();
                    evaluations.push(ShaclEvaluation {
                        binding,
                        schema: Some(schema),
                        result: Ok(report),
                        data_version,
                        shapes_version,
                        schema_fingerprint,
                        shape_versions,
                        refresh_dependencies: false,
                    });
                }
                Ok(_) => evaluations.push(ShaclEvaluation {
                    binding,
                    schema: None,
                    result: Err(crate::ShaclError::SchemaChangedDuringValidation {
                        graph: shapes_graph,
                    }
                    .to_string()),
                    data_version,
                    shapes_version,
                    schema_fingerprint: previous_schema,
                    shape_versions: previous_versions.clone(),
                    refresh_dependencies: false,
                }),
                Err(crate::CraqleError::Store(error)) => return Err(error),
                Err(error) => {
                    let refresh_dependencies = stable_error(&error);
                    let shape_versions = if refresh_dependencies {
                        self.error_versions(&binding, &previous_versions)?
                    } else {
                        previous_versions
                    };
                    evaluations.push(ShaclEvaluation {
                        binding,
                        schema: None,
                        result: Err(error.to_string()),
                        data_version,
                        shapes_version,
                        schema_fingerprint: previous_schema,
                        shape_versions,
                        refresh_dependencies,
                    });
                }
            }
        }
        Ok(evaluations)
    }

    #[cfg(feature = "shacl-core")]
    fn error_versions(
        &self,
        binding: &crate::ShaclBinding,
        previous: &[(GraphId, [u8; 32])],
    ) -> crate::store::Result<Vec<(GraphId, [u8; 32])>> {
        let imports = EncodedTerm("<http://www.w3.org/2002/07/owl#imports>".to_owned());
        let mut graphs: Vec<GraphId> = previous.iter().map(|(graph, _)| graph.clone()).collect();
        if !graphs.iter().any(|graph| graph == &binding.shapes_graph) {
            graphs.push(binding.shapes_graph.clone());
        }
        let mut next = vec![binding.shapes_graph.clone()];
        let mut visited = Vec::new();
        while let Some(graph) = next.pop() {
            if visited.iter().any(|known| known == &graph) {
                continue;
            }
            visited.push(graph.clone());
            if !self.store.contains_graph(&graph)? {
                continue;
            }
            for quad in self.store.graph_snapshot(&graph)?.quads {
                if quad.predicate != imports {
                    continue;
                }
                let Some(import) = quad.object.to_named_node().map(GraphId) else {
                    continue;
                };
                if !graphs.iter().any(|known| known == &import) {
                    graphs.push(import.clone());
                }
                next.push(import);
            }
        }
        let mut versions = graphs
            .into_iter()
            .map(|graph| {
                self.store
                    .graph_version_digest(&graph)
                    .map(|version| (graph, version))
            })
            .collect::<crate::store::Result<Vec<_>>>()?;
        versions.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        Ok(versions)
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
    ) -> crate::store::Result<usize> {
        let _settlement_timer = SettlementTimer {
            store: &self.store,
            started: Instant::now(),
        };
        #[cfg(test)]
        if self.take_settle_failure() {
            return Err(crate::store::StoreError::Fjall(fjall::Error::Io(
                std::io::Error::other("injected settlement failure"),
            )));
        }
        let evaluations = evaluations
            .into_iter()
            .map(|evaluation| {
                let refreshed_versions = evaluation
                    .refresh_dependencies
                    .then(|| self.error_versions(&evaluation.binding, &evaluation.shape_versions))
                    .transpose()?;
                Ok((evaluation, refreshed_versions))
            })
            .collect::<crate::store::Result<Vec<_>>>()?;
        let _binding_guard = self.store.binding_guard();
        let data_version = self.store.graph_version_digest(graph)?;
        let mut current = self.store.shacl_binding_statuses(graph)?;
        let mut batch = self.store.new_batch();
        let mut reports_produced = 0usize;
        for (evaluation, refreshed_versions) in evaluations {
            let Some(existing) = current
                .iter_mut()
                .find(|status| status.binding == evaluation.binding)
            else {
                continue;
            };
            let shapes_version = self
                .store
                .graph_version_digest(&evaluation.binding.shapes_graph)?;
            // Pending is staged before its source mutation releases the binding guard.
            // An older evaluation must not overwrite its versions or report.
            let versions_current = self
                .shacl
                .versions_are_current(&evaluation.shape_versions)
                .map_err(map_store_error)?;
            if existing.state != crate::ShaclValidationState::Pending
                || evaluation.data_version != Some(data_version)
                || shapes_version != evaluation.shapes_version
            {
                continue;
            }
            if !versions_current {
                if let Some(shape_versions) = refreshed_versions {
                    let status = crate::ShaclBindingStatus {
                        binding: evaluation.binding.clone(),
                        state: crate::ShaclValidationState::Pending,
                        report: None,
                        error: None,
                        data_version,
                        shapes_version,
                        schema_fingerprint: existing.schema_fingerprint,
                        compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                        shape_versions,
                    };
                    *existing = status.clone();
                    self.store.stage_binding_pending(&mut batch, &status)?;
                }
                continue;
            }
            let mut status = crate::ShaclBindingStatus {
                binding: evaluation.binding.clone(),
                state: crate::ShaclValidationState::Pending,
                report: None,
                error: None,
                data_version,
                shapes_version,
                schema_fingerprint: evaluation.schema_fingerprint,
                compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                shape_versions: evaluation.shape_versions,
            };
            match evaluation.result {
                Ok(report) => {
                    reports_produced += 1;
                    status.state = if report.conforms {
                        crate::ShaclValidationState::Valid
                    } else {
                        crate::ShaclValidationState::Invalid
                    };
                    if let Some(schema) = &evaluation.schema {
                        let options = evaluation.binding.validation_options.validation_options();
                        let _ = self.shacl.cache_current_report(
                            graph,
                            schema,
                            &options,
                            report.clone(),
                        );
                    }
                    status.report = Some(report);
                }
                Err(error) => {
                    status.state = crate::ShaclValidationState::Failed;
                    status.error = Some(error);
                }
            }
            *existing = status.clone();
            self.store.stage_binding_status(&mut batch, &status)?;
        }
        if !current.iter().any(|status| {
            status.binding.policy != crate::ValidationPolicy::Disabled
                && status.state == crate::ShaclValidationState::Pending
        }) {
            self.store.stage_shacl_settled(&mut batch, graph)?;
        }
        self.store.commit(batch)?;
        Ok(reports_produced)
    }

    #[cfg(feature = "shacl-core")]
    fn settle_current(&self, graph: &GraphId) -> crate::store::Result<usize> {
        let evaluations = self.evaluate_current_bindings(graph)?;
        self.persist_shacl_evaluations(graph, evaluations)
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn replay_pending_bindings(&self) -> crate::store::Result<()> {
        let outcome = self.replay_pending_bindings_bounded(usize::MAX, None)?;
        if let Some(failure) = outcome.failures.first() {
            return Err(crate::store::StoreError::InvalidEncoding {
                context: "SHACL pending replay",
                message: failure.error.clone(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn replay_pending_bindings_bounded(
        &self,
        max_graphs: usize,
        max_elapsed: Option<Duration>,
    ) -> crate::store::Result<crate::PendingReplayOutcome> {
        let started = Instant::now();
        let deadline = max_elapsed.and_then(|elapsed| started.checked_add(elapsed));
        let scan = self
            .store
            .pending_shacl_queue_bounded(max_graphs, deadline)?;
        let mut outcome = crate::PendingReplayOutcome {
            budget_exhausted: scan.budget_exhausted,
            ..crate::PendingReplayOutcome::default()
        };
        outcome.statistics.pending_queue_entries_scanned = scan.entries_scanned;
        for graph in scan.graphs {
            if deadline.is_some_and(|limit| Instant::now() >= limit) {
                outcome.budget_exhausted = true;
                break;
            }
            match self.settle_current(&graph) {
                Ok(reports) => {
                    outcome.statistics.reports_produced += reports as u64;
                    if !self.store.shacl_graph_is_pending(&graph)? {
                        outcome.statistics.graphs_settled += 1;
                    }
                }
                Err(error) => outcome
                    .failures
                    .push(self.report_settlement_failure(&graph, &error)),
            }
        }
        outcome.statistics.elapsed = started.elapsed();
        Ok(outcome)
    }

    #[cfg(feature = "shacl-core")]
    fn settle_shacl_graphs(&self, graphs: &[GraphId], skip: Option<&GraphId>) {
        for graph in graphs {
            if skip == Some(graph) {
                continue;
            }
            if let Err(error) = self.settle_current(graph) {
                self.report_settlement_failure(graph, &error);
            }
        }
    }

    #[cfg(feature = "shacl-core")]
    fn settle_bindings(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        data_version: [u8; 32],
        work: BindingWork<'_>,
    ) -> crate::store::Result<usize> {
        let evaluations = self.evaluate_bindings(graph, changes, data_version, work)?;
        self.persist_shacl_evaluations(graph, evaluations)
    }

    #[cfg(feature = "shacl-core")]
    fn report_settlement_failure(
        &self,
        graph: &GraphId,
        error: &crate::store::StoreError,
    ) -> crate::PendingReplayFailure {
        self.store.record_settlement_failure();
        let statuses = {
            let _binding_guard = self.store.binding_guard();
            self.store.shacl_binding_statuses(graph)
        };
        let statuses = match statuses {
            Ok(statuses) => statuses
                .into_iter()
                .filter(|status| status.state == crate::ShaclValidationState::Pending)
                .collect::<Vec<_>>(),
            Err(status_error) => {
                tracing::error!(
                    graph = %graph.as_str(),
                    error = %error,
                    status_error = %status_error,
                    "SHACL settlement failed and pending status lookup also failed"
                );
                Vec::new()
            }
        };
        for status in &statuses {
            tracing::error!(
                graph = %graph.as_str(),
                binding = %status.binding.shapes_graph.as_str(),
                data_version = ?status.data_version,
                error = %error,
                "SHACL settlement failed; binding remains pending"
            );
        }
        if statuses.is_empty() {
            tracing::error!(
                graph = %graph.as_str(),
                error = %error,
                "SHACL settlement failed; graph remains queued"
            );
        }
        crate::PendingReplayFailure {
            graph: graph.clone(),
            bindings: statuses
                .iter()
                .map(|status| status.binding.clone())
                .collect(),
            data_version: statuses.first().map(|status| status.data_version),
            error: error.to_string(),
        }
    }

    #[cfg(feature = "shacl-core")]
    fn persist_shacl_evaluations_post_commit(
        &self,
        graph: &GraphId,
        evaluations: Vec<ShaclEvaluation>,
    ) -> bool {
        if evaluations.is_empty() {
            return true;
        }
        if let Err(error) = self.persist_shacl_evaluations(graph, evaluations) {
            self.report_settlement_failure(graph, &error);
            return false;
        }
        true
    }

    #[cfg(feature = "shacl-core")]
    fn settle_bindings_post_commit(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        data_version: [u8; 32],
        work: BindingWork<'_>,
    ) -> bool {
        if let Err(error) = self.settle_bindings(graph, changes, data_version, work) {
            self.report_settlement_failure(graph, &error);
            return false;
        }
        true
    }

    #[cfg(feature = "shacl-core")]
    fn settle_current_post_commit(&self, graph: &GraphId) -> bool {
        if let Err(error) = self.settle_current(graph) {
            self.report_settlement_failure(graph, &error);
            return false;
        }
        true
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

    #[cfg(feature = "shacl-core")]
    fn ensure_prepared_data_current(
        &self,
        graph: &GraphId,
        fence: &PreparedCommitFence<'_>,
    ) -> Result<(), UpdateError> {
        let current = self.store.contains_graph(graph)?;
        let matches = match fence.data_version {
            None => !current,
            Some(expected) => current && self.store.graph_version_digest(graph)? == expected,
        };
        if matches {
            Ok(())
        } else {
            Err(UpdateError::StalePreparedState {
                fence: "data graph version".to_owned(),
            })
        }
    }

    #[cfg(feature = "shacl-core")]
    fn ensure_prepared_shapes_current(
        &self,
        fence: &PreparedCommitFence<'_>,
    ) -> Result<(), UpdateError> {
        for (graph, expected) in fence.shape_versions {
            if !self.store.contains_graph(graph)?
                || self.store.graph_version_digest(graph)? != *expected
            {
                return Err(UpdateError::StalePreparedState {
                    fence: format!("shapes graph `{}` version", graph.as_str()),
                });
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
            #[cfg(feature = "shacl-core")]
            prepared_fence: None,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %commit.graph.as_str(), change_count = commit.changes.len(), sync_enabled = self.sync.is_some()))]
    fn commit_changes_with_plan(&self, commit: LocalCommit<'_>) -> Result<Batch, UpdateError> {
        let LocalCommit {
            graph,
            changes,
            plan,
            validate_rules,
            #[cfg(feature = "shacl-core")]
            prepared_fence,
        } = commit;

        if let Some(sync) = &self.sync {
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

            // Validation and publication are serialized with every local CRDT
            // mutation of this graph.
            let _commit_guard = self.store.graph_commit_guard(graph);
            #[cfg(feature = "shacl-core")]
            if let Some(fence) = prepared_fence.as_ref() {
                self.ensure_prepared_data_current(graph, fence)?;
            }
            if validate_rules {
                self.validate(graph, &changes)?;
            }
            #[cfg(feature = "shacl-core")]
            let (binding_guard, prepared_shacl) =
                self.prepare_shacl_write_for_commit(graph, &changes, validate_rules)?;
            #[cfg(feature = "shacl-core")]
            if let Some(fence) = prepared_fence.as_ref() {
                self.ensure_prepared_shapes_current(fence)?;
            }
            #[cfg(feature = "shacl-core")]
            let pending_graphs = self.store.affected_shacl_graphs(graph)?;
            #[cfg(feature = "shacl-core")]
            let advisory_work = prepared_shacl.advisory_work;
            #[cfg(feature = "shacl-core")]
            let advisory_changes = advisory_work.as_ref().map(|_| changes.clone());
            #[cfg(feature = "shacl-core")]
            let mut shacl_evaluations = prepared_shacl.enforce_evaluations;

            sync.ensure_topic_guarded(&self.store, graph)?;

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
                let data_version = _merged
                    .applied
                    .then(|| self.stamp_evaluations(graph, &mut shacl_evaluations))
                    .transpose()?;
                drop(_commit_guard);
                drop(binding_guard);
                drop(_write_guard);
                if _merged.applied {
                    let mut source_settled =
                        self.persist_shacl_evaluations_post_commit(graph, shacl_evaluations);
                    if let (Some(work), Some(changes), Some(data_version)) =
                        (advisory_work, advisory_changes, data_version)
                    {
                        source_settled &=
                            self.settle_bindings_post_commit(graph, &changes, data_version, work);
                    }
                    self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
                }
            }
            return Ok(batch);
        }

        // Guards the whole read→write cycle of this graph's CRDT state: the
        // diagnostics read, the clock read, the counter mint, every quad op, the
        // clock write, the FTS enqueue, the commit and the diagnostics refresh
        // (G1, G2, G5, G6).
        let _commit_guard = self.store.graph_commit_guard(graph);

        #[cfg(feature = "shacl-core")]
        if let Some(fence) = prepared_fence.as_ref() {
            self.ensure_prepared_data_current(graph, fence)?;
        }

        if validate_rules {
            self.validate(graph, &changes)?;
        }
        #[cfg(feature = "shacl-core")]
        let (binding_guard, prepared_shacl) =
            self.prepare_shacl_write_for_commit(graph, &changes, validate_rules)?;
        #[cfg(feature = "shacl-core")]
        if let Some(fence) = prepared_fence.as_ref() {
            self.ensure_prepared_shapes_current(fence)?;
        }
        #[cfg(feature = "shacl-core")]
        let pending_graphs = self.store.affected_shacl_graphs(graph)?;
        #[cfg(feature = "shacl-core")]
        let advisory_work = prepared_shacl.advisory_work;
        #[cfg(feature = "shacl-core")]
        let advisory_changes = advisory_work.as_ref().map(|_| changes.clone());
        #[cfg(feature = "shacl-core")]
        let mut shacl_evaluations = prepared_shacl.enforce_evaluations;

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
        let graph_id = self.store.stage_graph(&mut batch, graph)?;
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
        self.store
            .stage_pending_bindings(&mut batch, graph, clock_digest(&vector_clock)?)?;
        self.store.commit(batch)?;
        #[cfg(feature = "shacl-core")]
        let data_version = self.stamp_evaluations(graph, &mut shacl_evaluations)?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)
                .map_err(UpdateError::Store)?;
        }
        #[cfg(feature = "shacl-core")]
        {
            drop(_commit_guard);
            drop(binding_guard);
            let mut source_settled =
                self.persist_shacl_evaluations_post_commit(graph, shacl_evaluations);
            if let (Some(work), Some(changes)) = (advisory_work, advisory_changes) {
                source_settled &=
                    self.settle_bindings_post_commit(graph, &changes, data_version, work);
            }
            self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
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
        #[cfg(feature = "shacl-core")]
        let (merged, binding_work, binding_changes, data_version, pending_graphs) = {
            let _commit_guard = self.store.graph_commit_guard(graph);
            let _binding_guard = self.store.binding_guard();
            let pending_graphs = self.store.affected_shacl_graphs(graph)?;
            let binding_work = {
                let work = self.binding_work(
                    graph,
                    &[
                        crate::ValidationPolicy::Advisory,
                        crate::ValidationPolicy::Enforce,
                    ],
                )?;
                (!work.statuses.is_empty()).then_some(work)
            };
            let binding_changes = binding_work
                .as_ref()
                .map(|_| batch_changes(self.store.as_ref(), incoming))
                .transpose()?;
            let merged = self.apply_irokle_guarded(incoming, plan)?;
            let data_version = merged
                .applied
                .then(|| self.store.graph_version_digest(graph))
                .transpose()?;
            (
                merged,
                binding_work,
                binding_changes,
                data_version,
                pending_graphs,
            )
        };
        #[cfg(not(feature = "shacl-core"))]
        let merged = {
            let _commit_guard = self.store.graph_commit_guard(graph);
            self.apply_irokle_guarded(incoming, plan)?
        };
        #[cfg(feature = "shacl-core")]
        let source_settled = if merged.applied {
            if let (Some(work), Some(changes), Some(data_version)) =
                (binding_work, binding_changes, data_version)
            {
                self.settle_bindings_post_commit(graph, &changes, data_version, work)
            } else {
                true
            }
        } else {
            self.settle_current_post_commit(graph)
        };
        #[cfg(feature = "shacl-core")]
        self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
        Ok(merged)
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
        self.store
            .stage_pending_bindings(&mut batch, graph, clock_digest(vector_clock)?)?;
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
struct BatchQuad {
    subject: EncodedTerm,
    predicate: EncodedTerm,
    object: EncodedTerm,
    was_live: bool,
    dots: Vec<Dot>,
}

#[cfg(feature = "shacl-core")]
fn batch_changes(
    store: &GraphStore,
    batch: &Batch,
) -> crate::store::Result<Vec<MaterializedQuadChange>> {
    let mut indexes = HashMap::new();
    let mut quads = Vec::new();

    for op in &batch.ops {
        let (subject, predicate, object) = match op {
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
            } => (subject, predicate, object),
        };
        let key = (subject.clone(), predicate.clone(), object.clone());
        let index = if let Some(index) = indexes.get(&key) {
            *index
        } else {
            let dots = store.quad_dots(&batch.graph, subject, predicate, object)?;
            let index = quads.len();
            quads.push(BatchQuad {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
                was_live: !dots.is_empty(),
                dots,
            });
            indexes.insert(key, index);
            index
        };
        let quad = &mut quads[index];
        match op {
            QuadOp::Add { dot, .. } => {
                if !quad.dots.contains(dot) {
                    quad.dots.push(*dot);
                }
            }
            QuadOp::Remove { witnessed, .. } => {
                quad.dots.retain(|dot| !witnessed.contains(dot));
            }
        }
    }

    Ok(quads
        .into_iter()
        .filter_map(|quad| match (quad.was_live, quad.dots.is_empty()) {
            (false, false) => Some(MaterializedQuadChange::Insert {
                graph: batch.graph.clone(),
                subject: quad.subject,
                predicate: quad.predicate,
                object: quad.object,
            }),
            (true, true) => Some(MaterializedQuadChange::Delete {
                graph: batch.graph.clone(),
                subject: quad.subject,
                predicate: quad.predicate,
                object: quad.object,
            }),
            _ => None,
        })
        .collect())
}

#[cfg(feature = "shacl-core")]
fn map_store_error(error: crate::CraqleError) -> crate::store::StoreError {
    match error {
        crate::CraqleError::Store(error) => error,
        error => crate::store::StoreError::InvalidEncoding {
            context: "SHACL dependency version",
            message: error.to_string(),
        },
    }
}

#[cfg(feature = "shacl-core")]
fn clock_digest(clock: &VectorClock) -> crate::store::Result<[u8; 32]> {
    Ok(*blake3::hash(&postcard::to_allocvec(clock)?).as_bytes())
}

#[cfg(feature = "shacl-core")]
fn stable_error(error: &crate::CraqleError) -> bool {
    matches!(
        error,
        crate::CraqleError::Shacl(error)
            if !matches!(
                error,
                crate::ShaclError::DataGraphNotFound { .. }
                    | crate::ShaclError::SchemaChangedDuringValidation { .. }
                    | crate::ShaclError::ShapesGraphNotFound { .. }
                    | crate::ShaclError::ValidationCancelled
            )
    )
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

#[cfg(all(test, feature = "shacl-core"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::search::SearchIndex;
    use crate::sync::{CraqleGraphSync, CraqleIrokleOptions, IrokleGraphSync};
    use crate::{ShaclBinding, ShaclBindingOptions, ValidationPolicy};

    fn engine_at(dir: &std::path::Path) -> (Arc<GraphStore>, ReplicationEngine) {
        let store = Arc::new(GraphStore::open(dir).unwrap());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search));
        let engine = ReplicationEngine::new(store.clone(), sparql, ActorId::random());
        (store, engine)
    }

    fn pending_engine(
        dir: &std::path::Path,
        policy: ValidationPolicy,
    ) -> (Arc<GraphStore>, ReplicationEngine, GraphId, ShaclBinding) {
        let (store, engine) = engine_at(dir);
        let data = GraphId::new("urn:test:pending-data");
        let shapes = GraphId::new("urn:test:pending-shapes");
        let focus = EncodedTerm("<urn:test:pending-focus>".to_owned());
        engine
            .local_apply_changes_unchecked(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: focus.clone(),
                    predicate: EncodedTerm("<urn:test:pending-value>".to_owned()),
                    object: EncodedTerm("<urn:test:pending-object>".to_owned()),
                }],
            )
            .unwrap();
        engine
            .local_apply_changes_unchecked(
                &shapes,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: EncodedTerm("<urn:test:pending-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm("<http://www.w3.org/ns/shacl#NodeShape>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: EncodedTerm("<urn:test:pending-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#targetNode>".to_owned(),
                        ),
                        object: focus,
                    },
                ],
            )
            .unwrap();
        let binding = ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy,
            validation_options: ShaclBindingOptions::default(),
        };
        let shapes_version = store.graph_version_digest(&shapes).unwrap();
        let status = crate::ShaclBindingStatus {
            binding: binding.clone(),
            state: crate::ShaclValidationState::Pending,
            report: None,
            error: None,
            data_version: store.graph_version_digest(&data).unwrap(),
            shapes_version,
            schema_fingerprint: [0; 32],
            compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
            shape_versions: vec![(shapes, shapes_version)],
        };
        let mut batch = store.new_batch();
        store.stage_binding_status(&mut batch, &status).unwrap();
        store.commit(batch).unwrap();
        let mut batch = store.new_batch();
        store
            .stage_pending_bindings(
                &mut batch,
                &data,
                store.graph_version_digest(&data).unwrap(),
            )
            .unwrap();
        store.commit(batch).unwrap();
        (store, engine, data, binding)
    }

    fn report(conforms: bool) -> crate::ShaclValidationReport {
        crate::ShaclValidationReport {
            conforms,
            results: Vec::new(),
            statistics: crate::ShaclValidationStatistics::default(),
        }
    }

    fn queued_graphs(store: &GraphStore, count: usize) -> Vec<GraphId> {
        let mut graphs = Vec::new();
        for index in 0..count {
            let data = GraphId::new(&format!("urn:test:queued-data:{index}"));
            let shapes = GraphId::new(&format!("urn:test:queued-shapes:{index}"));
            store.create_graph(&data).unwrap();
            store.create_graph(&shapes).unwrap();
            let shapes_version = store.graph_version_digest(&shapes).unwrap();
            let status = crate::ShaclBindingStatus {
                binding: ShaclBinding {
                    data_graph: data.clone(),
                    shapes_graph: shapes.clone(),
                    policy: ValidationPolicy::Advisory,
                    validation_options: ShaclBindingOptions::default(),
                },
                state: crate::ShaclValidationState::Pending,
                report: None,
                error: None,
                data_version: store.graph_version_digest(&data).unwrap(),
                shapes_version,
                schema_fingerprint: [0; 32],
                compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                shape_versions: vec![(shapes, shapes_version)],
            };
            let mut batch = store.new_batch();
            store.stage_binding_pending(&mut batch, &status).unwrap();
            store.commit(batch).unwrap();
            graphs.push(data);
        }
        graphs
    }

    #[test]
    fn bounded_queue_replay_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, engine) = engine_at(dir.path());
            queued_graphs(&store, 5);
            let outcome = engine.replay_pending_bindings_bounded(2, None).unwrap();
            assert_eq!(outcome.statistics.pending_queue_entries_scanned, 2);
            assert_eq!(outcome.statistics.graphs_settled, 2);
            assert_eq!(outcome.statistics.reports_produced, 2);
            assert!(outcome.budget_exhausted);
            assert_eq!(store.pending_shacl_count().unwrap(), 3);
            store.persist().unwrap();
        }

        let (store, engine) = engine_at(dir.path());
        let outcome = engine
            .replay_pending_bindings_bounded(usize::MAX, None)
            .unwrap();
        assert_eq!(outcome.statistics.graphs_settled, 3);
        assert_eq!(outcome.statistics.reports_produced, 3);
        assert!(!outcome.budget_exhausted);
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn replay_continues_after_one_graph_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        queued_graphs(&store, 3);
        engine.arm_settle_failure_after(1);

        let outcome = engine
            .replay_pending_bindings_bounded(usize::MAX, None)
            .unwrap();
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.statistics.graphs_settled, 2);
        assert_eq!(outcome.statistics.reports_produced, 2);
        assert_eq!(store.pending_shacl_count().unwrap(), 1);
        assert_eq!(store.shacl_runtime_statistics().settlement_failures, 1);
        assert_eq!(
            store
                .shacl_binding_statuses(&outcome.failures[0].graph)
                .unwrap()[0]
                .state,
            crate::ShaclValidationState::Pending
        );

        let retry = engine
            .replay_pending_bindings_bounded(usize::MAX, None)
            .unwrap();
        assert_eq!(retry.statistics.graphs_settled, 1);
        assert_eq!(retry.statistics.reports_produced, 1);
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn empty_queue_replay_does_no_graph_work() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let outcome = engine
            .replay_pending_bindings_bounded(usize::MAX, None)
            .unwrap();
        assert_eq!(outcome.statistics.pending_queue_entries_scanned, 0);
        assert_eq!(outcome.statistics.graphs_settled, 0);
        assert_eq!(outcome.statistics.reports_produced, 0);
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn queue_replays() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, engine, _data, _binding) =
                pending_engine(dir.path(), ValidationPolicy::Advisory);
            store.persist().unwrap();
            drop(engine);
            drop(store);
        }
        let (store, engine) = engine_at(dir.path());
        engine.replay_pending_bindings().unwrap();
        let status = store
            .shacl_binding_statuses(&GraphId::new("urn:test:pending-data"))
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Valid);
        assert!(status.report.unwrap().conforms);
        assert!(store.pending_shacl_graphs().unwrap().is_empty());
    }

    #[test]
    fn settle_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, data, _binding) =
            pending_engine(dir.path(), ValidationPolicy::Advisory);
        let before = store.graph_snapshot(&data).unwrap();

        engine.arm_settle_failure();
        assert!(engine.replay_pending_bindings().is_err());
        assert_eq!(store.graph_snapshot(&data).unwrap(), before);
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(store.pending_shacl_graphs().unwrap(), vec![data.clone()]);

        store.arm_commit_failure();
        assert!(engine.replay_pending_bindings().is_err());
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(store.pending_shacl_graphs().unwrap(), vec![data.clone()]);

        engine.replay_pending_bindings().unwrap();
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Valid
        );
    }

    #[test]
    fn committed_local_settlement_failures_stay_pending_across_restart() {
        for policy in [ValidationPolicy::Enforce, ValidationPolicy::Advisory] {
            let dir = tempfile::tempdir().unwrap();
            let (data, snapshot) = {
                let (store, engine, data, _binding) = pending_engine(dir.path(), policy);
                engine.replay_pending_bindings().unwrap();
                engine.arm_settle_failure();
                let batch = engine
                    .local_apply_changes(
                        &data,
                        vec![MaterializedQuadChange::Insert {
                            graph: data.clone(),
                            subject: EncodedTerm("<urn:test:pending-focus>".to_owned()),
                            predicate: EncodedTerm("<urn:test:post-commit-predicate>".to_owned()),
                            object: EncodedTerm("<urn:test:post-commit-object>".to_owned()),
                        }],
                    )
                    .unwrap();
                assert_eq!(batch.graph, data);
                let snapshot = store.graph_snapshot(&data).unwrap();
                assert!(snapshot.quads.iter().any(|quad| {
                    quad.predicate == EncodedTerm("<urn:test:post-commit-predicate>".to_owned())
                }));
                assert_eq!(
                    store.shacl_binding_statuses(&data).unwrap()[0].state,
                    crate::ShaclValidationState::Pending
                );
                assert_eq!(store.pending_shacl_count().unwrap(), 1);
                assert_eq!(store.shacl_runtime_statistics().settlement_failures, 1);
                store.persist().unwrap();
                (data, snapshot)
            };

            let (store, engine) = engine_at(dir.path());
            let replay = engine
                .replay_pending_bindings_bounded(usize::MAX, None)
                .unwrap();
            assert_eq!(replay.statistics.graphs_settled, 1);
            assert_eq!(store.graph_snapshot(&data).unwrap(), snapshot);
            assert_eq!(
                store.shacl_binding_statuses(&data).unwrap()[0].state,
                crate::ShaclValidationState::Valid
            );
            assert_eq!(store.pending_shacl_count().unwrap(), 0);
        }
    }

    #[test]
    fn affected_graph_settlement_failure_does_not_reject_shape_write() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, data, binding) = pending_engine(dir.path(), ValidationPolicy::Advisory);
        engine.replay_pending_bindings().unwrap();
        engine.arm_settle_failure();
        let shapes = binding.shapes_graph;
        let batch = engine
            .local_apply_changes_unchecked(
                &shapes,
                vec![MaterializedQuadChange::Insert {
                    graph: shapes.clone(),
                    subject: EncodedTerm("<urn:test:changed-shape>".to_owned()),
                    predicate: EncodedTerm("<urn:test:shape-metadata>".to_owned()),
                    object: EncodedTerm("<urn:test:shape-value>".to_owned()),
                }],
            )
            .unwrap();
        assert_eq!(batch.graph, shapes);
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(store.pending_shacl_count().unwrap(), 1);
        assert_eq!(store.shacl_runtime_statistics().settlement_failures, 1);
        store.persist().unwrap();
        drop(engine);
        drop(store);

        let (store, engine) = engine_at(dir.path());
        engine.replay_pending_bindings().unwrap();
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn open_replays() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let data = {
            let (store, engine, data, _binding) =
                pending_engine(&store_path, ValidationPolicy::Advisory);
            store.persist().unwrap();
            drop(engine);
            drop(store);
            data
        };

        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let status = node
            .shacl_binding_statuses(&crate::AllowAllAuthorizer, &data)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Valid);
        assert!(status.report.unwrap().conforms);
        assert!(node.store.pending_shacl_graphs().unwrap().is_empty());
        assert_eq!(
            node.startup_pending_replay()
                .statistics
                .binding_records_scanned,
            1
        );
        assert_eq!(node.startup_pending_replay().statistics.graphs_settled, 1);
        assert_eq!(node.startup_pending_replay().statistics.reports_produced, 1);
    }

    #[test]
    fn open_can_defer_and_resume_pending_queue() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let data = {
            let (store, engine, data, _binding) =
                pending_engine(&store_path, ValidationPolicy::Advisory);
            store.persist().unwrap();
            drop(engine);
            drop(store);
            data
        };
        let node = crate::CraqleNode::open_with_options(
            dir.path(),
            crate::CraqleOptions::new()
                .with_pending_replay_policy(crate::PendingReplayPolicy::Defer),
        )
        .unwrap();
        assert_eq!(node.pending_shacl_queue_status().unwrap().pending_count, 1);
        assert_eq!(
            node.shacl_binding_statuses(&crate::AllowAllAuthorizer, &data)
                .unwrap()[0]
                .state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(
            node.startup_pending_replay()
                .statistics
                .binding_records_scanned,
            1
        );
        assert_eq!(node.startup_pending_replay().statistics.graphs_settled, 0);
        let replay = node
            .replay_pending_shacl(1, Duration::from_secs(1))
            .unwrap();
        assert_eq!(replay.statistics.graphs_settled, 1);
        assert_eq!(node.pending_shacl_queue_status().unwrap().pending_count, 0);
    }

    #[test]
    fn empty_open_has_zero_pending_startup_work() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let startup = node.startup_pending_replay();
        assert_eq!(startup.statistics.binding_records_scanned, 0);
        assert_eq!(startup.statistics.pending_queue_entries_scanned, 0);
        assert_eq!(startup.statistics.graphs_settled, 0);
        assert_eq!(startup.statistics.reports_produced, 0);
    }

    #[test]
    fn healthy_reopen_does_not_scan_binding_records() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        {
            let (store, engine, _data, _binding) =
                pending_engine(&store_path, ValidationPolicy::Advisory);
            store.persist().unwrap();
            drop(engine);
            drop(store);
        }
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            assert_eq!(
                node.startup_pending_replay()
                    .statistics
                    .binding_records_scanned,
                1
            );
        }

        let reopened = crate::CraqleNode::open(dir.path()).unwrap();
        assert_eq!(
            reopened
                .startup_pending_replay()
                .statistics
                .binding_records_scanned,
            0
        );
        assert_eq!(
            reopened
                .startup_pending_replay()
                .statistics
                .pending_queue_entries_scanned,
            0
        );
    }

    #[test]
    fn remote_retry() {
        let sender_dir = tempfile::tempdir().unwrap();
        let receiver_dir = tempfile::tempdir().unwrap();
        let (_sender_store, sender) = engine_at(sender_dir.path());
        let (store, receiver, data, _binding) =
            pending_engine(receiver_dir.path(), ValidationPolicy::Advisory);
        receiver.replay_pending_bindings().unwrap();
        let shapes = GraphId::new("urn:test:pending-shapes");
        let batch = sender
            .local_apply_changes_unchecked(
                &shapes,
                vec![MaterializedQuadChange::Insert {
                    graph: shapes.clone(),
                    subject: EncodedTerm("<urn:test:remote-subject>".to_owned()),
                    predicate: EncodedTerm("<urn:test:remote-predicate>".to_owned()),
                    object: EncodedTerm("<urn:test:remote-object>".to_owned()),
                }],
            )
            .unwrap();

        receiver.arm_settle_failure();
        receiver.apply_irokle_batch(batch.clone()).unwrap();
        let source = store.graph_snapshot(&shapes).unwrap();
        assert!(
            source
                .quads
                .iter()
                .any(|quad| quad.subject == EncodedTerm("<urn:test:remote-subject>".to_owned()))
        );
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(store.pending_shacl_graphs().unwrap(), vec![data.clone()]);
        assert_eq!(store.shacl_runtime_statistics().settlement_failures, 1);

        receiver.apply_irokle_batch(batch).unwrap();
        assert_eq!(store.graph_snapshot(&shapes).unwrap(), source);
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Valid
        );
        assert!(store.pending_shacl_graphs().unwrap().is_empty());
    }

    #[test]
    fn remote_reopens() {
        let sender_dir = tempfile::tempdir().unwrap();
        let receiver_dir = tempfile::tempdir().unwrap();
        let (_sender_store, sender) = engine_at(sender_dir.path());
        let store_path = receiver_dir.path().join("store");
        let (source, data, shapes) = {
            let (store, receiver, data, _binding) =
                pending_engine(&store_path, ValidationPolicy::Advisory);
            receiver.replay_pending_bindings().unwrap();
            let shapes = GraphId::new("urn:test:pending-shapes");
            let batch = sender
                .local_apply_changes_unchecked(
                    &shapes,
                    vec![
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:pending-shape>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/ns/shacl#property>".to_owned(),
                            ),
                            object: EncodedTerm("<urn:test:pending-property>".to_owned()),
                        },
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:pending-property>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                            ),
                            object: EncodedTerm(
                                "<http://www.w3.org/ns/shacl#PropertyShape>".to_owned(),
                            ),
                        },
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:pending-property>".to_owned()),
                            predicate: EncodedTerm("<http://www.w3.org/ns/shacl#path>".to_owned()),
                            object: EncodedTerm("<urn:test:pending-value>".to_owned()),
                        },
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:pending-property>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/ns/shacl#maxCount>".to_owned(),
                            ),
                            object: EncodedTerm(
                                "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned(),
                            ),
                        },
                    ],
                )
                .unwrap();

            receiver.arm_settle_failure();
            receiver.apply_irokle_batch(batch).unwrap();
            let source = store.graph_snapshot(&shapes).unwrap();
            assert_eq!(
                store.shacl_binding_statuses(&data).unwrap()[0].state,
                crate::ShaclValidationState::Pending
            );
            assert_eq!(store.pending_shacl_graphs().unwrap(), vec![data.clone()]);
            store.persist().unwrap();
            drop(receiver);
            drop(store);
            (source, data, shapes)
        };

        let node = crate::CraqleNode::open(receiver_dir.path()).unwrap();
        assert_eq!(node.graph_snapshot(&shapes).unwrap(), source);
        let status = node
            .shacl_binding_statuses(&crate::AllowAllAuthorizer, &data)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Invalid);
        assert_eq!(
            status.data_version,
            node.store.graph_version_digest(&data).unwrap()
        );
        assert_eq!(
            status.shapes_version,
            node.store.graph_version_digest(&shapes).unwrap()
        );
        let report = status.report.unwrap();
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        assert!(node.store.pending_shacl_graphs().unwrap().is_empty());
    }

    #[test]
    fn disabled_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, data, _binding) =
            pending_engine(dir.path(), ValidationPolicy::Disabled);
        let before = store.graph_version_digest(&data).unwrap();

        engine
            .local_apply_changes_unchecked(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: EncodedTerm("<urn:test:disabled-subject>".to_owned()),
                    predicate: EncodedTerm("<urn:test:disabled-predicate>".to_owned()),
                    object: EncodedTerm("<urn:test:disabled-object>".to_owned()),
                }],
            )
            .unwrap();

        let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Pending);
        assert!(status.report.is_none());
        assert_ne!(status.data_version, before);
        assert_eq!(
            status.data_version,
            store.graph_version_digest(&data).unwrap()
        );
    }

    #[test]
    fn commit_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, data, _binding) = pending_engine(dir.path(), ValidationPolicy::Enforce);
        engine.replay_pending_bindings().unwrap();
        let before = (
            store.graph_snapshot(&data).unwrap(),
            store.get_vector_clock(&data).unwrap(),
            store.query_index_status_fast().unwrap(),
            store.shacl_binding_statuses(&data).unwrap(),
        );

        store.arm_commit_failure();
        let error = engine
            .local_apply_changes(
                &data,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: data.clone(),
                        subject: EncodedTerm("<urn:test:commit-subject>".to_owned()),
                        predicate: EncodedTerm("<urn:test:commit-predicate>".to_owned()),
                        object: EncodedTerm("<urn:test:commit-object>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: data.clone(),
                        subject: EncodedTerm("<urn:test:commit-subject>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm("<urn:test:commit-class>".to_owned()),
                    },
                ],
            )
            .unwrap_err();
        assert!(matches!(error, UpdateError::Store(_)), "{error:?}");

        assert_eq!(store.graph_snapshot(&data).unwrap(), before.0);
        assert_eq!(store.get_vector_clock(&data).unwrap(), before.1);
        assert_eq!(store.query_index_status_fast().unwrap(), before.2);
        assert_eq!(store.shacl_binding_statuses(&data).unwrap(), before.3);
    }

    #[test]
    fn sync_reject() {
        let dir = tempfile::tempdir().unwrap();
        let (store, setup, data, binding) = pending_engine(dir.path(), ValidationPolicy::Enforce);
        let shapes = binding.shapes_graph.clone();
        let shape = EncodedTerm("<urn:test:pending-shape>".to_owned());
        let property = EncodedTerm("<urn:test:pending-property>".to_owned());
        setup
            .local_apply_changes_unchecked(
                &shapes,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: shape.clone(),
                        predicate: EncodedTerm("<http://www.w3.org/ns/shacl#property>".to_owned()),
                        object: property.clone(),
                    },
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: property.clone(),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#PropertyShape>".to_owned(),
                        ),
                    },
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: property.clone(),
                        predicate: EncodedTerm("<http://www.w3.org/ns/shacl#path>".to_owned()),
                        object: EncodedTerm("<urn:test:pending-value>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: property,
                        predicate: EncodedTerm("<http://www.w3.org/ns/shacl#maxCount>".to_owned()),
                        object: EncodedTerm(
                            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned(),
                        ),
                    },
                ],
            )
            .unwrap();
        setup
            .local_apply_changes_unchecked(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: EncodedTerm("<urn:test:pending-focus>".to_owned()),
                    predicate: EncodedTerm(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                    ),
                    object: EncodedTerm("<urn:test:pending-class>".to_owned()),
                }],
            )
            .unwrap();
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Valid
        );
        store.clear_fts_queue().unwrap();

        let irokle = irokle::Irokle::builder().build().unwrap();
        let sync: Arc<dyn CraqleGraphSync> =
            Arc::new(IrokleGraphSync::new(irokle, CraqleIrokleOptions::new()));
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search));
        let engine = ReplicationEngine::new_sync_shacl(
            store.clone(),
            sparql,
            ActorId::random(),
            Some(sync.clone()),
            Arc::new(crate::shacl_impl::ShaclCompiler::new(store.clone())),
        );
        let before = (
            store.graph_snapshot(&data).unwrap(),
            store.get_vector_clock(&data).unwrap(),
            store.query_index_status_fast().unwrap(),
            store.shacl_binding_statuses(&data).unwrap(),
        );

        assert!(matches!(
            engine.local_apply_changes(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: EncodedTerm("<urn:test:pending-focus>".to_owned()),
                    predicate: EncodedTerm("<urn:test:pending-value>".to_owned()),
                    object: EncodedTerm("<urn:test:pending-second>".to_owned()),
                }],
            ),
            Err(UpdateError::ShaclValidationFailed(_))
        ));

        assert_eq!(store.graph_snapshot(&data).unwrap(), before.0);
        assert_eq!(store.get_vector_clock(&data).unwrap(), before.1);
        assert_eq!(store.query_index_status_fast().unwrap(), before.2);
        assert_eq!(store.shacl_binding_statuses(&data).unwrap(), before.3);
        assert!(store.drain_fts_queue(usize::MAX).unwrap().is_empty());
        assert!(sync.graph_topic_id(&store, &data).unwrap().is_none());
        assert!(
            !sync
                .craqle_topic_ids()
                .unwrap()
                .contains(&crate::sync::graph_topic_id(&data))
        );
    }

    #[test]
    fn import_arrives() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let data = GraphId::new("urn:test:arrive-data");
        let root = GraphId::new("urn:test:arrive-root");
        let imported = GraphId::new("urn:test:arrive-import");
        let focus = EncodedTerm("<urn:test:arrive-focus>".to_owned());
        engine
            .local_apply_changes_unchecked(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: focus.clone(),
                    predicate: EncodedTerm("<urn:test:arrive-value>".to_owned()),
                    object: EncodedTerm("<urn:test:arrive-object>".to_owned()),
                }],
            )
            .unwrap();
        engine
            .local_apply_changes_unchecked(
                &root,
                vec![MaterializedQuadChange::Insert {
                    graph: root.clone(),
                    subject: EncodedTerm("<urn:test:arrive-ontology>".to_owned()),
                    predicate: EncodedTerm("<http://www.w3.org/2002/07/owl#imports>".to_owned()),
                    object: EncodedTerm(format!("<{}>", imported.as_str())),
                }],
            )
            .unwrap();
        let root_version = store.graph_version_digest(&root).unwrap();
        let binding = ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: root.clone(),
            policy: ValidationPolicy::Advisory,
            validation_options: ShaclBindingOptions {
                allow_local_imports: true,
                ..ShaclBindingOptions::default()
            },
        };
        let mut batch = store.new_batch();
        store
            .stage_binding_status(
                &mut batch,
                &crate::ShaclBindingStatus {
                    binding,
                    state: crate::ShaclValidationState::Pending,
                    report: None,
                    error: None,
                    data_version: store.graph_version_digest(&data).unwrap(),
                    shapes_version: root_version,
                    schema_fingerprint: [0; 32],
                    compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                    shape_versions: vec![(root, root_version)],
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        let mut batch = store.new_batch();
        store
            .stage_pending_bindings(
                &mut batch,
                &data,
                store.graph_version_digest(&data).unwrap(),
            )
            .unwrap();
        store.commit(batch).unwrap();

        engine.replay_pending_bindings().unwrap();
        assert_eq!(
            store.shacl_binding_statuses(&data).unwrap()[0].state,
            crate::ShaclValidationState::Pending
        );
        assert_eq!(store.pending_shacl_graphs().unwrap(), vec![data.clone()]);

        engine
            .local_apply_changes_unchecked(
                &imported,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: imported.clone(),
                        subject: EncodedTerm("<urn:test:arrive-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm("<http://www.w3.org/ns/shacl#NodeShape>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: imported.clone(),
                        subject: EncodedTerm("<urn:test:arrive-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#targetNode>".to_owned(),
                        ),
                        object: focus,
                    },
                ],
            )
            .unwrap();

        let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Valid);
        assert!(status.report.unwrap().conforms);
        assert!(store.pending_shacl_graphs().unwrap().is_empty());
        assert_eq!(store.affected_shacl_graphs(&imported).unwrap(), vec![data]);
    }

    #[test]
    fn stale_deps() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let data = GraphId::new("urn:test:stale-deps-data");
        let root = GraphId::new("urn:test:stale-deps-root");
        let first = GraphId::new("urn:test:stale-deps-first");
        let nested = GraphId::new("urn:test:stale-deps-nested");
        let second = GraphId::new("urn:test:stale-deps-second");
        let focus = EncodedTerm("<urn:test:stale-deps-focus>".to_owned());
        let imports = EncodedTerm("<http://www.w3.org/2002/07/owl#imports>".to_owned());
        engine
            .local_apply_changes_unchecked(
                &data,
                vec![MaterializedQuadChange::Insert {
                    graph: data.clone(),
                    subject: focus.clone(),
                    predicate: EncodedTerm("<urn:test:stale-deps-value>".to_owned()),
                    object: EncodedTerm("<urn:test:stale-deps-object>".to_owned()),
                }],
            )
            .unwrap();
        for (graph, import) in [(&root, &first), (&first, &nested)] {
            engine
                .local_apply_changes_unchecked(
                    graph,
                    vec![MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: EncodedTerm("<urn:test:stale-deps-ontology>".to_owned()),
                        predicate: imports.clone(),
                        object: EncodedTerm(format!("<{}>", import.as_str())),
                    }],
                )
                .unwrap();
        }
        let old_nested = store.graph_version_digest(&nested).unwrap();
        engine
            .local_apply_changes_unchecked(
                &nested,
                vec![MaterializedQuadChange::Insert {
                    graph: nested.clone(),
                    subject: EncodedTerm("<urn:test:stale-deps-ontology>".to_owned()),
                    predicate: imports,
                    object: EncodedTerm(format!("<{}>", second.as_str())),
                }],
            )
            .unwrap();

        let data_version = store.graph_version_digest(&data).unwrap();
        let root_version = store.graph_version_digest(&root).unwrap();
        let first_version = store.graph_version_digest(&first).unwrap();
        let nested_version = store.graph_version_digest(&nested).unwrap();
        let second_version = store.graph_version_digest(&second).unwrap();
        let binding = ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: root.clone(),
            policy: ValidationPolicy::Advisory,
            validation_options: ShaclBindingOptions {
                allow_local_imports: true,
                ..ShaclBindingOptions::default()
            },
        };
        let current_versions = vec![
            (root.clone(), root_version),
            (first.clone(), first_version),
            (nested.clone(), nested_version),
            (second.clone(), second_version),
        ];
        let mut batch = store.new_batch();
        store
            .stage_binding_status(
                &mut batch,
                &crate::ShaclBindingStatus {
                    binding: binding.clone(),
                    state: crate::ShaclValidationState::Pending,
                    report: None,
                    error: None,
                    data_version,
                    shapes_version: root_version,
                    schema_fingerprint: [9; 32],
                    compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                    shape_versions: current_versions,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();

        engine
            .persist_shacl_evaluations(
                &data,
                vec![ShaclEvaluation {
                    binding,
                    schema: None,
                    result: Err("missing nested import".to_owned()),
                    data_version: Some(data_version),
                    shapes_version: root_version,
                    schema_fingerprint: [1; 32],
                    shape_versions: vec![
                        (root.clone(), root_version),
                        (first.clone(), first_version),
                        (nested.clone(), old_nested),
                    ],
                    refresh_dependencies: true,
                }],
            )
            .unwrap();

        let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Pending);
        assert_eq!(status.schema_fingerprint, [9; 32]);
        assert!(
            status
                .shape_versions
                .iter()
                .any(|(graph, _)| graph == &second)
        );
        assert_eq!(
            store.affected_shacl_graphs(&second).unwrap(),
            vec![data.clone()]
        );

        engine
            .local_apply_changes_unchecked(
                &second,
                vec![
                    MaterializedQuadChange::Insert {
                        graph: second.clone(),
                        subject: EncodedTerm("<urn:test:stale-deps-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm("<http://www.w3.org/ns/shacl#NodeShape>".to_owned()),
                    },
                    MaterializedQuadChange::Insert {
                        graph: second.clone(),
                        subject: EncodedTerm("<urn:test:stale-deps-shape>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#targetNode>".to_owned(),
                        ),
                        object: focus,
                    },
                ],
            )
            .unwrap();
        let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Valid);
        assert!(status.report.unwrap().conforms);
    }

    #[test]
    fn error_deps() {
        for imported in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (store, engine) = engine_at(dir.path());
            let data = GraphId::new("urn:test:error-data");
            let root = GraphId::new("urn:test:error-root");
            let import = GraphId::new("urn:test:error-import");
            let shapes = if imported { &import } else { &root };
            let focus = EncodedTerm("<urn:test:error-focus>".to_owned());
            engine
                .local_apply_changes_unchecked(
                    &data,
                    vec![MaterializedQuadChange::Insert {
                        graph: data.clone(),
                        subject: focus.clone(),
                        predicate: EncodedTerm("<urn:test:error-value>".to_owned()),
                        object: EncodedTerm("<urn:test:error-object>".to_owned()),
                    }],
                )
                .unwrap();
            if imported {
                engine
                    .local_apply_changes_unchecked(
                        &root,
                        vec![MaterializedQuadChange::Insert {
                            graph: root.clone(),
                            subject: EncodedTerm("<urn:test:error-ontology>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/2002/07/owl#imports>".to_owned(),
                            ),
                            object: EncodedTerm(format!("<{}>", import.as_str())),
                        }],
                    )
                    .unwrap();
            }
            engine
                .local_apply_changes_unchecked(
                    shapes,
                    vec![
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:error-shape>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                            ),
                            object: EncodedTerm(
                                "<http://www.w3.org/ns/shacl#NodeShape>".to_owned(),
                            ),
                        },
                        MaterializedQuadChange::Insert {
                            graph: shapes.clone(),
                            subject: EncodedTerm("<urn:test:error-shape>".to_owned()),
                            predicate: EncodedTerm(
                                "<http://www.w3.org/ns/shacl#targetNode>".to_owned(),
                            ),
                            object: focus,
                        },
                    ],
                )
                .unwrap();
            let root_version = store.graph_version_digest(&root).unwrap();
            let mut shape_versions = vec![(root.clone(), root_version)];
            if imported {
                shape_versions.push((import.clone(), store.graph_version_digest(&import).unwrap()));
            }
            let binding = ShaclBinding {
                data_graph: data.clone(),
                shapes_graph: root.clone(),
                policy: ValidationPolicy::Advisory,
                validation_options: ShaclBindingOptions {
                    allow_local_imports: imported,
                    ..ShaclBindingOptions::default()
                },
            };
            let mut batch = store.new_batch();
            store
                .stage_binding_status(
                    &mut batch,
                    &crate::ShaclBindingStatus {
                        binding,
                        state: crate::ShaclValidationState::Pending,
                        report: None,
                        error: None,
                        data_version: store.graph_version_digest(&data).unwrap(),
                        shapes_version: root_version,
                        schema_fingerprint: [0; 32],
                        compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                        shape_versions,
                    },
                )
                .unwrap();
            store.commit(batch).unwrap();
            let mut batch = store.new_batch();
            store
                .stage_pending_bindings(
                    &mut batch,
                    &data,
                    store.graph_version_digest(&data).unwrap(),
                )
                .unwrap();
            store.commit(batch).unwrap();
            engine.replay_pending_bindings().unwrap();

            engine
                .local_apply_changes_unchecked(
                    shapes,
                    vec![MaterializedQuadChange::Insert {
                        graph: shapes.clone(),
                        subject: EncodedTerm("<urn:test:error-property>".to_owned()),
                        predicate: EncodedTerm(
                            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_owned(),
                        ),
                        object: EncodedTerm(
                            "<http://www.w3.org/ns/shacl#PropertyShape>".to_owned(),
                        ),
                    }],
                )
                .unwrap();

            let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
            assert_eq!(status.state, crate::ShaclValidationState::Failed);
            assert!(status.error.unwrap().contains("ill-formed SHACL"));
            assert!(
                status
                    .shape_versions
                    .iter()
                    .any(|(graph, _)| graph == &root)
            );
            if imported {
                assert!(
                    status
                        .shape_versions
                        .iter()
                        .any(|(graph, _)| graph == &import)
                );
            }
            assert_eq!(store.affected_shacl_graphs(shapes).unwrap(), vec![data]);
        }
    }

    #[test]
    fn stale_eval() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let data = GraphId::new("urn:test:stale-data");
        let shapes = GraphId::new("urn:test:stale-shapes");
        let imported = GraphId::new("urn:test:stale-import");
        for graph in [&data, &shapes, &imported] {
            store.create_graph(graph).unwrap();
        }
        let data_version = store.graph_version_digest(&data).unwrap();
        let shapes_version = store.graph_version_digest(&shapes).unwrap();
        let imported_version = store.graph_version_digest(&imported).unwrap();
        let binding = ShaclBinding {
            data_graph: data.clone(),
            shapes_graph: shapes.clone(),
            policy: ValidationPolicy::Advisory,
            validation_options: ShaclBindingOptions::default(),
        };
        let old_versions = vec![(shapes.clone(), shapes_version)];
        let mut batch = store.new_batch();
        store
            .stage_binding_status(
                &mut batch,
                &crate::ShaclBindingStatus {
                    binding: binding.clone(),
                    state: crate::ShaclValidationState::Pending,
                    report: None,
                    error: None,
                    data_version,
                    shapes_version,
                    schema_fingerprint: [1; 32],
                    compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                    shape_versions: old_versions.clone(),
                },
            )
            .unwrap();
        store.commit(batch).unwrap();

        let new_versions = vec![
            (shapes.clone(), shapes_version),
            (imported.clone(), imported_version),
        ];
        engine
            .persist_shacl_evaluations(
                &data,
                vec![ShaclEvaluation {
                    binding: binding.clone(),
                    schema: None,
                    result: Ok(report(false)),
                    data_version: Some(data_version),
                    shapes_version,
                    schema_fingerprint: [2; 32],
                    shape_versions: new_versions.clone(),
                    refresh_dependencies: false,
                }],
            )
            .unwrap();
        engine
            .persist_shacl_evaluations(
                &data,
                vec![ShaclEvaluation {
                    binding,
                    schema: None,
                    result: Ok(report(true)),
                    data_version: Some(data_version),
                    shapes_version,
                    schema_fingerprint: [1; 32],
                    shape_versions: old_versions,
                    refresh_dependencies: false,
                }],
            )
            .unwrap();

        let status = store.shacl_binding_statuses(&data).unwrap().pop().unwrap();
        assert_eq!(status.state, crate::ShaclValidationState::Invalid);
        assert!(!status.report.unwrap().conforms);
        assert_eq!(status.schema_fingerprint, [2; 32]);
        assert_eq!(status.shape_versions, new_versions);
    }
}
