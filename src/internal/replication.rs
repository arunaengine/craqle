//! Publishes graph events and merges observed-remove replica state.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
#[cfg(feature = "shacl-core")]
use std::time::{Duration, Instant};

use crate::core::{
    ActorId, Batch, ContextTag, CrateRenderHints, CrateViolation, Dot, EncodedTerm, EventId,
    GraphDiagnostics, GraphId, GraphReplicaSnapshot, GraphTombstone, MaterializedQuadChange,
    QuadOp, SnapshotQuadState, TaggedGraphPolicy, TaggedRenderHints,
    UnsupportedRdfStarTerm as RdfStarError, VectorClock,
};
#[cfg(feature = "shacl-core")]
use crate::rdf_read::StoreReadView;
use crate::rules::{ChangeSet, DeltaSummary, Rule};
use crate::sparql::SparqlEngine;
#[cfg(feature = "shacl-core")]
use crate::store::BindingGuard;
use crate::store::{
    BatchReceiptLink, BatchTermCtx, ClockUpdate, CounterKey, EncodedQuad, FtsEnqueue, FtsSubject,
    GraphStore, PolicyReceipt, QuadAdd, QuadRemove, SnapshotLimits, TermId,
};
use crate::sync::CraqleGraphEvent;
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
    #[error(transparent)]
    UnsupportedRdfStarTerm(#[from] RdfStarError),
    #[error("prepared state is stale: {fence}")]
    StalePreparedState { fence: String },
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("sync: {0}")]
    Sync(#[from] crate::sync::CraqleSyncError),
    #[error("graph `{}` was permanently deleted by event {}", .tombstone.graph, .tombstone.delete_event)]
    GraphDeleted { tombstone: GraphTombstone },
    #[error("mutation {:?} has a durable outcome but did not finish: {reason}", .receipt.id)]
    Accepted {
        receipt: Box<crate::sync::MutationReceipt>,
        error_kind: crate::CraqleErrorKind,
        reason: String,
    },
    #[error("mutation receipt expired")]
    ReceiptExpired,
    #[error("mutation admission ticket is unknown")]
    ReceiptUnknown,
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("input rejected: {0}")]
    InputRejected(String),
    /// Events the batch declares as its causal base that this replica has not
    /// applied. The transport must fetch them and retry.
    #[error("missing causal dependencies: {}", render_dots(.0))]
    MissingDependencies(Vec<Dot>),
    #[error("mutation {:?} was accepted but follow-up work failed: {reason}", .receipt.id)]
    Accepted {
        receipt: Box<crate::sync::MutationReceipt>,
        error_kind: crate::CraqleErrorKind,
        reason: String,
    },
}

fn render_dots(dots: &[Dot]) -> String {
    dots.iter()
        .map(|dot| format!("{}:{}", dot.actor, dot.counter))
        .collect::<Vec<_>>()
        .join(", ")
}

fn mutation_digest(
    graph: &GraphId,
    changes: &[MaterializedQuadChange],
    hints: Option<&TaggedRenderHints>,
) -> Result<[u8; 32], UpdateError> {
    let bytes =
        postcard::to_allocvec(&(graph, changes, hints)).map_err(crate::store::StoreError::from)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn check_commit(commit: Option<&crate::CommitInfo>, graph: &GraphId) -> Result<(), UpdateError> {
    match commit.map(|commit| commit.check(graph)) {
        Some(Err(reason)) => Err(UpdateError::InvalidChangeSet(reason.to_owned())),
        Some(Ok(())) | None => Ok(()),
    }
}

/// Writes literal aliases in canonical form; deletes also remove an alias stored before canonicalization.
pub(crate) fn canonical_changes(
    changes: Vec<MaterializedQuadChange>,
) -> Vec<MaterializedQuadChange> {
    let mut canonical = Vec::with_capacity(changes.len());
    for change in changes {
        let (MaterializedQuadChange::Insert {
            graph,
            subject,
            predicate,
            object,
        }
        | MaterializedQuadChange::Delete {
            graph,
            subject,
            predicate,
            object,
        }) = &change;
        let raw = [subject, predicate, object];
        let terms = raw.map(|term| term.canonical().unwrap_or_else(|| term.clone()));
        if terms.iter().zip(raw).all(|(term, raw)| term == raw) {
            canonical.push(change);
            continue;
        }
        let graph = graph.clone();
        let [subject, predicate, object] = terms;
        match change {
            MaterializedQuadChange::Insert { .. } => {
                canonical.push(MaterializedQuadChange::Insert {
                    graph,
                    subject,
                    predicate,
                    object,
                });
            }
            MaterializedQuadChange::Delete { .. } => {
                canonical.push(MaterializedQuadChange::Delete {
                    graph,
                    subject,
                    predicate,
                    object,
                });
                canonical.push(change);
            }
        }
    }
    canonical
}

fn new_mutation() -> crate::sync::MutationId {
    crate::sync::MutationId::new()
}

fn batch_digest(batch: &Batch) -> Result<[u8; 32], MergeError> {
    let bytes = postcard::to_allocvec(batch)
        .map_err(crate::store::StoreError::from)
        .map_err(MergeError::Store)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn encoded_digest(value: &impl serde::Serialize) -> crate::store::Result<[u8; 32]> {
    Ok(*blake3::hash(&postcard::to_allocvec(value)?).as_bytes())
}

fn settled_repairs() -> crate::sync::RepairState {
    crate::sync::RepairState {
        diagnostics: crate::sync::RepairOutcome::NotRequired,
        shacl: crate::sync::RepairOutcome::NotRequired,
        search: crate::sync::RepairOutcome::NotRequired,
        query_view: crate::sync::RepairOutcome::NotRequired,
    }
}

fn delete_repairs() -> crate::sync::RepairState {
    crate::sync::RepairState {
        diagnostics: crate::sync::RepairOutcome::NotRequired,
        #[cfg(feature = "shacl-core")]
        shacl: crate::sync::RepairOutcome::Pending,
        #[cfg(not(feature = "shacl-core"))]
        shacl: crate::sync::RepairOutcome::NotRequired,
        search: crate::sync::RepairOutcome::Pending,
        query_view: crate::sync::RepairOutcome::Pending,
    }
}

fn source_receipt(
    graph: &GraphId,
    plan: ApplyPlan<'_>,
    clock: &VectorClock,
) -> Result<crate::sync::MutationReceipt, MergeError> {
    #[cfg(feature = "shacl-core")]
    let shacl = crate::sync::RepairOutcome::Pending;
    #[cfg(not(feature = "shacl-core"))]
    let shacl = crate::sync::RepairOutcome::NotRequired;
    Ok(crate::sync::MutationReceipt {
        id: plan.id,
        admission_sequence: 0,
        graph: graph.clone(),
        request_digest: plan.request_digest,
        event_id: plan.event_id,
        topic: plan.topic,
        publish_after: plan.publish_after.cloned(),
        topic_epoch: plan.topic_epoch,
        topic_genesis: plan.topic_genesis,
        search_token: None,
        repair_graphs: vec![graph.clone()],
        source: crate::sync::SourceOutcome::Applied,
        persistence: crate::sync::PersistenceOutcome::Pending,
        repairs: crate::sync::RepairState {
            diagnostics: crate::sync::RepairOutcome::Pending,
            shacl,
            search: crate::sync::RepairOutcome::Pending,
            query_view: crate::sync::RepairOutcome::Pending,
        },
        source_version: clock_digest(clock)?,
        updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
    })
}

impl UpdateError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Sparql(error) => error.kind(),
            Self::ValidationFailed(_) | Self::InvalidChangeSet(_) => {
                crate::CraqleErrorKind::InvalidInput
            }
            Self::UnsupportedRdfStarTerm(_) => crate::CraqleErrorKind::Unsupported,
            #[cfg(feature = "shacl-core")]
            Self::Shacl(error) => error.kind(),
            #[cfg(feature = "shacl-core")]
            Self::ShaclValidationFailed(_) => crate::CraqleErrorKind::InvalidInput,
            Self::StalePreparedState { .. } => crate::CraqleErrorKind::StalePreparedState,
            Self::Store(error) => error.kind(),
            Self::Sync(error) => error.kind(),
            Self::GraphDeleted { .. } => crate::CraqleErrorKind::Conflict,
            Self::Accepted { error_kind, .. } => *error_kind,
            Self::ReceiptExpired | Self::ReceiptUnknown => crate::CraqleErrorKind::Conflict,
        }
    }
}

impl MergeError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Store(error) => error.kind(),
            Self::InputRejected(_) => crate::CraqleErrorKind::InvalidInput,
            Self::MissingDependencies(_) => crate::CraqleErrorKind::Conflict,
            Self::Accepted { error_kind, .. } => *error_kind,
        }
    }
}

/// Outcome of merging replicated state into local state.
#[derive(Debug)]
pub struct MergeResult {
    /// Whether the merge changed authoritative graph state.
    pub applied: bool,
}

pub(crate) struct MergeReceipt {
    pub result: MergeResult,
    pub receipt: Option<crate::sync::MutationReceipt>,
}

pub(crate) struct PolicyMutation<'a> {
    pub graph: &'a GraphId,
    pub tagged: TaggedGraphPolicy,
    pub publish: bool,
    #[cfg(test)]
    pub before_apply: Option<fn()>,
}

#[cfg(feature = "shacl-core")]
const SHACL_WRITE_RETRIES: usize = 3;

/// The replication engine: local writes and CRDT merge of Irokle records.
pub(crate) struct ReplicationEngine {
    store: Arc<GraphStore>,
    rules: Vec<Box<dyn Rule>>,
    actor: ActorId,
    sync: Option<Arc<dyn crate::sync::CraqleGraphSync>>,
    #[cfg(feature = "shacl-core")]
    shacl: Arc<crate::shacl_impl::ShaclCompiler>,
    /// Per-engine injection prevents concurrent tests from faulting other nodes.
    #[cfg(test)]
    armed_apply_failure: std::sync::atomic::AtomicBool,
    /// Test-only failure after the source commit and before SHACL settlement.
    #[cfg(all(test, feature = "shacl-core"))]
    settle_failure_after: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckMode {
    Enabled,
    Bypassed,
}

impl CheckMode {
    fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// How a write should leave the graph's persisted diagnostics record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticsMode {
    /// Refresh the record as part of this commit, under its guard.
    Immediate,
    /// Bulk import: the caller rebuilds diagnostics once at the end.
    Deferred,
}

impl DiagnosticsMode {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteChecks {
    pub structural_rules: CheckMode,
    pub shacl_enforcement: CheckMode,
    pub diagnostics: DiagnosticsMode,
}

impl WriteChecks {
    fn normal(diagnostics: DiagnosticsMode) -> Self {
        Self {
            structural_rules: CheckMode::Enabled,
            shacl_enforcement: CheckMode::Enabled,
            diagnostics,
        }
    }

    fn bypassing_structural_rules(diagnostics: DiagnosticsMode) -> Self {
        Self {
            structural_rules: CheckMode::Bypassed,
            shacl_enforcement: CheckMode::Enabled,
            diagnostics,
        }
    }
}

/// A local write, ready to be committed to one graph.
struct LocalCommit<'a> {
    id: Option<crate::sync::MutationId>,
    write_locked: bool,
    graph: &'a GraphId,
    changes: Vec<MaterializedQuadChange>,
    checks: WriteChecks,
    prepared_fence: Option<PreparedCommitFence<'a>>,
    render_hints: Option<CrateRenderHints>,
    commit: Option<crate::CommitInfo>,
}

/// What a local write publishes next to its quad changes.
pub(crate) struct EventExtras {
    pub(crate) render_hints: Option<CrateRenderHints>,
    pub(crate) commit: Option<crate::CommitInfo>,
}

/// A strict RO-Crate change set and the versions it was prepared against.
pub(crate) struct PreparedWrite<'a> {
    pub(crate) graph: &'a GraphId,
    pub(crate) changes: Vec<MaterializedQuadChange>,
    pub(crate) data_version: Option<[u8; 32]>,
    pub(crate) shape_versions: &'a [(GraphId, [u8; 32])],
    pub(crate) extras: EventExtras,
}

struct PreparedCommitFence<'a> {
    data_version: Option<[u8; 32]>,
    shape_versions: &'a [(GraphId, [u8; 32])],
}

#[derive(Clone, Copy)]
struct ApplyPlan<'a> {
    hints: Option<&'a TaggedRenderHints>,
    diagnostics: DiagnosticsMode,
    id: crate::sync::MutationId,
    event_id: Option<[u8; 32]>,
    request_digest: [u8; 32],
    topic: Option<irokle::TopicId>,
    publish_after: Option<&'a irokle::ActorClock>,
    topic_epoch: Option<u64>,
    topic_genesis: Option<irokle::OpId>,
}

struct ApplyBatch<'a> {
    incoming: &'a Batch,
    clock: &'a mut VectorClock,
    plan: ApplyPlan<'a>,
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
                settle_failure_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
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
            settle_failure_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
        }
    }

    pub(crate) fn store(&self) -> &Arc<GraphStore> {
        &self.store
    }

    fn refresh_state(
        &self,
        receipt: &mut crate::sync::MutationReceipt,
    ) -> crate::store::Result<bool> {
        if receipt.source == crate::sync::SourceOutcome::Prepared {
            return Ok(false);
        }
        let previous = receipt.repairs;
        if matches!(
            receipt.repairs.query_view,
            crate::sync::RepairOutcome::Pending | crate::sync::RepairOutcome::Failed(_)
        ) && self.store.query_view_covered(receipt)?
        {
            receipt.repairs.query_view = crate::sync::RepairOutcome::Complete;
        }
        if matches!(
            receipt.repairs.search,
            crate::sync::RepairOutcome::Pending | crate::sync::RepairOutcome::Failed(_)
        ) && self.store.search_covered(receipt)?
        {
            receipt.repairs.search = crate::sync::RepairOutcome::Complete;
        }
        if matches!(
            receipt.repairs.diagnostics,
            crate::sync::RepairOutcome::Pending | crate::sync::RepairOutcome::Failed(_)
        ) {
            let _write = self.store.graph_write_guard(&receipt.graph);
            let _commit = self.store.graph_commit_guard(&receipt.graph);
            if self.recompute_graph_diagnostics(&receipt.graph).is_ok() {
                receipt.repairs.diagnostics = crate::sync::RepairOutcome::Complete;
            }
        }
        #[cfg(feature = "shacl-core")]
        if matches!(
            receipt.repairs.shacl,
            crate::sync::RepairOutcome::Pending | crate::sync::RepairOutcome::Failed(_)
        ) {
            let mut settled = true;
            for graph in &receipt.repair_graphs {
                settled &= !self.store.shacl_graph_pending(graph)?;
            }
            if settled {
                receipt.repairs.shacl = crate::sync::RepairOutcome::Complete;
            }
        }
        Ok(receipt.repairs != previous)
    }

    pub(crate) fn mark_persisted(
        &self,
        id: &crate::sync::MutationId,
    ) -> crate::store::Result<Option<crate::sync::MutationReceipt>> {
        let Some(mut receipt) = self.store.mutation_receipt(id)? else {
            return Ok(None);
        };
        if receipt.source == crate::sync::SourceOutcome::Prepared {
            return Ok(Some(receipt));
        }
        let repair_changed = self.refresh_state(&mut receipt)?;
        let persistence = self.store.persistence_outcome();
        let persistence_changed = receipt.persistence != persistence;
        if !repair_changed && !persistence_changed {
            return Ok(Some(receipt));
        }
        receipt.persistence = persistence;
        receipt.updated_unix_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
        let receipt = self.store.update_receipt(&receipt)?;
        self.store.persist_receipts()?;
        Ok(Some(receipt))
    }

    pub(crate) fn mutation_status(
        &self,
        lookup: &crate::sync::MutationLookup,
    ) -> crate::store::Result<crate::sync::MutationStatus> {
        match self.store.receipt_status(lookup)? {
            crate::sync::MutationStatus::Known(mut receipt) => {
                if self.refresh_state(&mut receipt)? {
                    receipt.updated_unix_nanos =
                        Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
                    receipt = Box::new(self.store.update_receipt(&receipt)?);
                    self.store.persist_receipts()?;
                }
                Ok(crate::sync::MutationStatus::Known(receipt))
            }
            status => Ok(status),
        }
    }

    pub(crate) fn receipt_for_batch(
        &self,
        batch: &Batch,
    ) -> crate::store::Result<crate::sync::MutationStatus> {
        self.store.receipt_for_batch(batch)
    }

    fn accepted_store(
        &self,
        id: crate::sync::MutationId,
        error: crate::store::StoreError,
    ) -> UpdateError {
        match self.store.mutation_receipt(&id) {
            Ok(Some(receipt)) => UpdateError::Accepted {
                receipt: Box::new(receipt),
                error_kind: error.kind(),
                reason: error.to_string(),
            },
            _ => UpdateError::Store(error),
        }
    }

    fn accepted_merge(&self, id: crate::sync::MutationId, error: MergeError) -> UpdateError {
        match self.store.mutation_receipt(&id) {
            Ok(Some(receipt)) => UpdateError::Accepted {
                receipt: Box::new(receipt),
                error_kind: error.kind(),
                reason: error.to_string(),
            },
            _ => merge_update_error(error),
        }
    }

    fn accepted_sync(
        &self,
        id: crate::sync::MutationId,
        error: crate::sync::CraqleSyncError,
    ) -> UpdateError {
        match self.store.mutation_receipt(&id) {
            Ok(Some(receipt)) => UpdateError::Accepted {
                receipt: Box::new(receipt),
                error_kind: error.kind(),
                reason: error.to_string(),
            },
            _ => UpdateError::Sync(error),
        }
    }

    fn accepted_outcome(receipt: crate::sync::MutationReceipt, error: UpdateError) -> UpdateError {
        UpdateError::Accepted {
            error_kind: error.kind(),
            reason: error.to_string(),
            receipt: Box::new(receipt),
        }
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
        self.arm_settle_after(0);
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn arm_settle_after(&self, successful_settlements: usize) {
        self.settle_failure_after
            .store(successful_settlements, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(all(test, feature = "shacl-core"))]
    fn take_settle_failure(&self) -> bool {
        loop {
            let remaining = self
                .settle_failure_after
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
                .settle_failure_after
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

    /// Test helper for exercising concurrent render-hint tag minting through
    /// the same atomic graph-mutation path used by imports.
    #[cfg(test)]
    pub(crate) fn set_graph_context(
        &self,
        graph: &GraphId,
        context: Option<String>,
        license: Option<String>,
        license_digest: Option<[u8; 32]>,
    ) -> Result<(), UpdateError> {
        self.apply_bulk_hints(
            graph,
            Vec::new(),
            CrateRenderHints {
                context,
                license,
                license_digest,
            },
        )
        .map(|_| ())
    }

    fn changed_render_hints(
        &self,
        graph: &GraphId,
        desired: Option<CrateRenderHints>,
    ) -> Result<Option<TaggedRenderHints>, UpdateError> {
        let Some(desired) = desired else {
            return Ok(None);
        };
        let desired_license = desired.license.clone().zip(desired.license_digest);
        if self.store.graph_context(graph)? == desired.context
            && self.store.graph_license(graph)? == desired_license
        {
            return Ok(None);
        }
        Ok(Some(TaggedRenderHints {
            tag: ContextTag::next_local(self.store.graph_context_tag(graph)?, self.actor),
            hints: desired,
        }))
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
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_changes(graph, changes)
    }

    /// [`Self::local_apply_changes`] for a caller that already holds the graph write lock.
    pub(crate) fn apply_changes_locked(
        &self,
        request: crate::sync::MutationRequest,
        extras: EventExtras,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(request.changes);
        self.ensure_change_targets(&request.graph, &changes)?;
        self.commit_with_plan(LocalCommit {
            id: Some(request.id),
            write_locked: true,
            graph: &request.graph,
            changes,
            checks: WriteChecks::normal(DiagnosticsMode::Immediate),
            prepared_fence: None,
            render_hints: extras.render_hints,
            commit: extras.commit,
        })
    }

    pub(crate) fn apply_mutation(
        &self,
        mut request: crate::sync::MutationRequest,
        commit: Option<crate::CommitInfo>,
    ) -> Result<crate::sync::MutationReceipt, UpdateError> {
        check_commit(commit.as_ref(), &request.graph)?;
        request.changes = canonical_changes(request.changes);
        self.ensure_change_targets(&request.graph, &request.changes)?;
        if request.changes.is_empty() {
            return Err(UpdateError::InvalidChangeSet(
                "mutation request must contain at least one change".to_owned(),
            ));
        }
        let digest = mutation_digest(&request.graph, &request.changes, None)?;
        let retry_guard;
        if let Some(receipt) = self.store.mutation_receipt(&request.id)? {
            if receipt.graph != request.graph || receipt.request_digest != digest {
                return Err(UpdateError::InvalidChangeSet(
                    "mutation id is already bound to a different request".to_owned(),
                ));
            }
            match request.admission_sequence {
                None => return Ok(receipt),
                Some(sequence) if sequence != receipt.admission_sequence => {
                    return Err(UpdateError::ReceiptExpired);
                }
                Some(_) => {}
            }
            if receipt.source != crate::sync::SourceOutcome::Prepared {
                return Ok(receipt);
            }
            retry_guard = Some(self.store.graph_write_guard(&request.graph));
            if let Some(sync) = &self.sync
                && let Some(record) = sync
                    .find_mutation(&receipt)
                    .map_err(|error| self.accepted_sync(request.id, error))?
            {
                self.apply_irokle_record(&record)
                    .map_err(|error| self.accepted_merge(request.id, error))?;
                return self.store.mutation_receipt(&request.id)?.ok_or_else(|| {
                    UpdateError::InvalidChangeSet("mutation receipt was not stored".into())
                });
            }
        } else if request.admission_sequence.is_none() {
            return self.prepare_mutation(&request);
        } else {
            return match self.store.receipt_status(&crate::sync::MutationLookup {
                graph: request.graph,
                id: request.id,
                admission_sequence: request.admission_sequence,
            })? {
                crate::sync::MutationStatus::Expired => Err(UpdateError::ReceiptExpired),
                crate::sync::MutationStatus::Known(receipt) => Ok(*receipt),
                crate::sync::MutationStatus::Unknown => Err(UpdateError::ReceiptUnknown),
            };
        }
        self.commit_with_plan(LocalCommit {
            id: Some(request.id),
            write_locked: retry_guard.is_some(),
            graph: &request.graph,
            changes: request.changes,
            checks: WriteChecks::normal(DiagnosticsMode::Immediate),
            prepared_fence: None,
            render_hints: None,
            commit,
        })?;
        drop(retry_guard);
        self.store
            .mutation_receipt(&request.id)?
            .ok_or_else(|| UpdateError::InvalidChangeSet("mutation receipt was not stored".into()))
    }

    fn prepare_mutation(
        &self,
        request: &crate::sync::MutationRequest,
    ) -> Result<crate::sync::MutationReceipt, UpdateError> {
        let _write_guard = self.store.graph_write_guard(&request.graph);
        if let Some(tombstone) = self.store.graph_tombstone(&request.graph)? {
            return Err(UpdateError::GraphDeleted { tombstone });
        }
        let _commit_guard = self.store.graph_commit_guard(&request.graph);
        self.validate(&request.graph, &request.changes)?;
        #[cfg(feature = "shacl-core")]
        let (_binding_guard, _) =
            self.prepare_shacl_commit(&request.graph, &request.changes, true)?;
        let (topic, frontier) = if let Some(sync) = &self.sync {
            let topic = sync.ensure_topic_guarded(&self.store, &request.graph)?;
            let frontier = sync.topic_frontier(topic)?;
            (Some(topic), Some(frontier))
        } else {
            (None, None)
        };
        #[cfg(feature = "shacl-core")]
        drop(_binding_guard);
        let receipt = crate::sync::MutationReceipt {
            id: request.id,
            admission_sequence: 0,
            graph: request.graph.clone(),
            request_digest: mutation_digest(&request.graph, &request.changes, None)?,
            event_id: None,
            topic,
            publish_after: frontier.as_ref().map(|frontier| frontier.clock.clone()),
            topic_epoch: frontier.as_ref().map(|frontier| frontier.epoch),
            topic_genesis: frontier.as_ref().map(|frontier| frontier.genesis),
            search_token: None,
            repair_graphs: vec![request.graph.clone()],
            source: crate::sync::SourceOutcome::Prepared,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: crate::sync::RepairState {
                diagnostics: crate::sync::RepairOutcome::Pending,
                #[cfg(feature = "shacl-core")]
                shacl: crate::sync::RepairOutcome::Pending,
                #[cfg(not(feature = "shacl-core"))]
                shacl: crate::sync::RepairOutcome::NotRequired,
                search: crate::sync::RepairOutcome::Pending,
                query_view: crate::sync::RepairOutcome::Pending,
            },
            source_version: self.store.graph_version_digest(&request.graph)?,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        let _receipt_guard = self.store.receipt_guard(&request.id);
        let mut batch = self.store.new_batch();
        if let Some(existing) = self.store.stage_receipt(&mut batch, &receipt)? {
            return Ok(existing);
        }
        self.store.commit(batch)?;
        self.store.persist_receipts()?;
        self.store
            .mutation_receipt(&request.id)?
            .ok_or_else(|| UpdateError::InvalidChangeSet("mutation receipt was not stored".into()))
    }

    pub(crate) fn set_policy(
        &self,
        request: PolicyMutation<'_>,
    ) -> Result<Option<crate::sync::MutationReceipt>, UpdateError> {
        let _write = self.store.graph_write_guard(request.graph);
        if let Some(tombstone) = self.store.graph_tombstone(request.graph)? {
            return Err(UpdateError::GraphDeleted { tombstone });
        }
        let current = self.store.graph_tagged_policy(request.graph)?;
        if self.store.contains_graph(request.graph)? && current == request.tagged {
            return Ok(None);
        }
        if request.publish
            && let Some(sync) = &self.sync
        {
            let topic = sync.ensure_graph_topic(&self.store, request.graph)?;
            let frontier = sync.topic_frontier(topic)?;
            let record = sync.publish_policy(&self.store, request.graph, request.tagged)?;
            #[cfg(test)]
            if let Some(before_apply) = request.before_apply {
                before_apply();
            }
            let CraqleGraphEvent::Policy { tagged, .. } = &record.event else {
                return Err(UpdateError::InvalidChangeSet(
                    "policy publish returned a different event".to_owned(),
                ));
            };
            let id = crate::sync::MutationId::from_op(record.meta.op_id);
            let digest = encoded_digest(tagged)?;
            let applied = crate::sync::MutationReceipt {
                id,
                admission_sequence: 0,
                graph: request.graph.clone(),
                request_digest: digest,
                event_id: Some(*record.meta.op_id.as_bytes()),
                topic: Some(topic),
                publish_after: Some(frontier.clock),
                topic_epoch: Some(frontier.epoch),
                topic_genesis: Some(frontier.genesis),
                search_token: None,
                repair_graphs: Vec::new(),
                source: crate::sync::SourceOutcome::Applied,
                persistence: crate::sync::PersistenceOutcome::Pending,
                repairs: settled_repairs(),
                source_version: digest,
                updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
            };
            return self
                .store
                .set_policy_receipt(
                    request.graph,
                    PolicyReceipt {
                        tagged,
                        receipt: &applied,
                    },
                )
                .map(Some)
                .map_err(|error| Self::accepted_outcome(applied, UpdateError::Store(error)));
        }

        let id = new_mutation();
        let digest = encoded_digest(&request.tagged)?;
        let receipt = crate::sync::MutationReceipt {
            id,
            admission_sequence: 0,
            graph: request.graph.clone(),
            request_digest: digest,
            event_id: None,
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs: Vec::new(),
            source: crate::sync::SourceOutcome::Applied,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: settled_repairs(),
            source_version: digest,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        self.store
            .set_policy_receipt(
                request.graph,
                PolicyReceipt {
                    tagged: &request.tagged,
                    receipt: &receipt,
                },
            )
            .map(Some)
            .map_err(UpdateError::Store)
    }

    pub(crate) fn apply_policy_record(
        &self,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<Option<crate::sync::MutationReceipt>, MergeError> {
        let CraqleGraphEvent::Policy { graph, tagged } = &record.event else {
            return Err(MergeError::InputRejected(
                "expected a policy replication record".to_owned(),
            ));
        };
        let current = self.store.graph_tagged_policy(graph)?;
        if tagged.tag <= current.tag {
            return Ok(None);
        }
        let digest = encoded_digest(tagged)?;
        let receipt = crate::sync::MutationReceipt {
            id: crate::sync::MutationId::from_op(record.meta.op_id),
            admission_sequence: 0,
            graph: graph.clone(),
            request_digest: digest,
            event_id: Some(*record.meta.op_id.as_bytes()),
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs: Vec::new(),
            source: crate::sync::SourceOutcome::Applied,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: settled_repairs(),
            source_version: digest,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        self.store
            .set_policy_receipt(
                graph,
                PolicyReceipt {
                    tagged,
                    receipt: &receipt,
                },
            )
            .map(Some)
            .map_err(MergeError::Store)
    }

    pub(crate) fn delete_graph(
        &self,
        graph: &GraphId,
        publish: bool,
    ) -> Result<Option<crate::sync::MutationReceipt>, UpdateError> {
        let _write = self.store.graph_write_guard(graph);
        if self.store.graph_tombstoned(graph)? && !self.store.contains_graph(graph)? {
            return Ok(None);
        }
        let mut delete_clock = self.store.get_vector_clock(graph)?;
        let delete_counter = delete_clock
            .0
            .get(&self.actor)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        delete_clock.advance(self.actor, delete_counter);
        let tombstone = GraphTombstone {
            graph: graph.clone(),
            delete_event: EventId::graph_delete(graph, self.actor, &delete_clock),
            delete_actor: self.actor,
            delete_clock,
        };
        let digest = encoded_digest(&tombstone)?;
        #[cfg(feature = "shacl-core")]
        let repair_graphs = self.store.affected_shacl_graphs(graph)?;
        #[cfg(not(feature = "shacl-core"))]
        let repair_graphs = Vec::new();

        if publish
            && let Some(sync) = &self.sync
            && let Some(topic) = sync.graph_topic_id(&self.store, graph)?
        {
            let frontier = sync.topic_frontier(topic)?;
            let record = sync.publish_delete(&self.store, tombstone)?;
            let CraqleGraphEvent::GraphDeleted { tombstone } = &record.event else {
                return Err(UpdateError::InvalidChangeSet(
                    "delete publish returned a different event".to_owned(),
                ));
            };
            let id = crate::sync::MutationId::from_op(record.meta.op_id);
            let applied = crate::sync::MutationReceipt {
                id,
                admission_sequence: 0,
                graph: graph.clone(),
                request_digest: digest,
                event_id: Some(*record.meta.op_id.as_bytes()),
                topic: Some(topic),
                publish_after: Some(frontier.clock),
                topic_epoch: Some(frontier.epoch),
                topic_genesis: Some(frontier.genesis),
                search_token: None,
                repair_graphs: repair_graphs.clone(),
                source: crate::sync::SourceOutcome::Applied,
                persistence: crate::sync::PersistenceOutcome::Pending,
                repairs: delete_repairs(),
                source_version: digest,
                updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
            };
            return self
                .store
                .delete_with_receipt(tombstone, &applied)
                .map(Some)
                .map_err(|error| Self::accepted_outcome(applied, UpdateError::Store(error)));
        }

        let receipt = crate::sync::MutationReceipt {
            id: new_mutation(),
            admission_sequence: 0,
            graph: graph.clone(),
            request_digest: digest,
            event_id: None,
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs,
            source: crate::sync::SourceOutcome::Applied,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: delete_repairs(),
            source_version: digest,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        self.store
            .delete_with_receipt(&tombstone, &receipt)
            .map(Some)
            .map_err(UpdateError::Store)
    }

    pub(crate) fn apply_delete_record(
        &self,
        record: &irokle::reducer::EventRecord<CraqleGraphEvent>,
    ) -> Result<Option<crate::sync::MutationReceipt>, MergeError> {
        let CraqleGraphEvent::GraphDeleted { tombstone } = &record.event else {
            return Err(MergeError::InputRejected(
                "expected a graph deletion record".to_owned(),
            ));
        };
        if self.store.graph_tombstoned(&tombstone.graph)?
            && !self.store.contains_graph(&tombstone.graph)?
        {
            return Ok(None);
        }
        let digest = encoded_digest(tombstone)?;
        #[cfg(feature = "shacl-core")]
        let repair_graphs = self.store.affected_shacl_graphs(&tombstone.graph)?;
        #[cfg(not(feature = "shacl-core"))]
        let repair_graphs = Vec::new();
        let receipt = crate::sync::MutationReceipt {
            id: crate::sync::MutationId::from_op(record.meta.op_id),
            admission_sequence: 0,
            graph: tombstone.graph.clone(),
            request_digest: digest,
            event_id: Some(*record.meta.op_id.as_bytes()),
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs,
            source: crate::sync::SourceOutcome::Applied,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: delete_repairs(),
            source_version: digest,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        self.store
            .delete_with_receipt(tombstone, &receipt)
            .map(Some)
            .map_err(MergeError::Store)
    }

    pub(crate) fn apply_changes_hints(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        render_hints: CrateRenderHints,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;
        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::normal(DiagnosticsMode::Immediate),
            prepared_fence: None,
            render_hints: Some(render_hints),
            commit: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn apply_changes_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::bypassing_structural_rules(DiagnosticsMode::Immediate),
            prepared_fence: None,
            render_hints: None,
            commit: None,
        })
    }

    /// Apply a trusted bulk change set locally and defer graph-diagnostics
    /// recomputation until the caller explicitly rebuilds diagnostics.
    #[cfg(test)]
    pub(crate) fn apply_bulk_unchecked(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;

        if changes.is_empty() {
            return self.empty_batch(graph);
        }

        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::bypassing_structural_rules(DiagnosticsMode::Deferred),
            prepared_fence: None,
            render_hints: None,
            commit: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn apply_bulk_hints(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
        render_hints: CrateRenderHints,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;
        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::bypassing_structural_rules(DiagnosticsMode::Deferred),
            prepared_fence: None,
            render_hints: Some(render_hints),
            commit: None,
        })
    }

    pub(crate) fn local_apply_bulk(
        &self,
        graph: &GraphId,
        changes: Vec<MaterializedQuadChange>,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(changes);
        self.ensure_change_targets(graph, &changes)?;
        if changes.is_empty() {
            return self.empty_batch(graph);
        }
        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::normal(DiagnosticsMode::Deferred),
            prepared_fence: None,
            render_hints: None,
            commit: None,
        })
    }

    pub(crate) fn apply_bulk_prepared(
        &self,
        write: PreparedWrite<'_>,
    ) -> Result<Batch, UpdateError> {
        let changes = canonical_changes(write.changes);
        self.ensure_change_targets(write.graph, &changes)?;
        let fence = PreparedCommitFence {
            data_version: write.data_version,
            shape_versions: write.shape_versions,
        };
        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph: write.graph,
            changes,
            // Preparation already evaluated structural rules over this exact encoded candidate.
            checks: WriteChecks::bypassing_structural_rules(DiagnosticsMode::Deferred),
            prepared_fence: Some(fence),
            render_hints: write.extras.render_hints,
            commit: write.extras.commit,
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

    /// Validate without writing; later concurrent writes can invalidate this verdict.
    pub(crate) fn check_planned_changes(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        let changes = canonical_changes(changes.to_vec());
        self.ensure_change_targets(graph, &changes)?;
        self.validate(graph, &changes)
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
                    if !report.accepted_by_write_policy {
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
        policies: &[crate::ShaclWritePolicy],
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
        enforce_shacl: bool,
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
            .filter(|status| status.binding.policy == crate::ShaclWritePolicy::Advisory)
            .cloned()
            .collect::<Vec<_>>();
        let advisory_work = (!advisory_statuses.is_empty()).then(|| BindingWork {
            base: base.clone(),
            data_version,
            statuses: advisory_statuses,
        });
        let enforce_evaluations = if enforce_shacl {
            let enforce_work = BindingWork {
                base,
                data_version,
                statuses: statuses
                    .iter()
                    .filter(|status| status.binding.policy == crate::ShaclWritePolicy::Enforce)
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
    fn prepared_shacl_current(
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
    fn prepare_shacl_commit(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
        enforce_shacl: bool,
    ) -> Result<(BindingGuard<'_>, PreparedShaclWrite<'_>), UpdateError> {
        for _ in 0..SHACL_WRITE_RETRIES {
            let prepared = self.prepare_shacl_write(graph, changes, enforce_shacl)?;
            let binding_guard = self.store.binding_guard();
            if self.prepared_shacl_current(graph, &prepared)? {
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
            if binding.policy == crate::ShaclWritePolicy::Disabled {
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
            status.binding.policy != crate::ShaclWritePolicy::Disabled
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
        let outcome = self.replay_bindings_bounded(usize::MAX, None)?;
        if let Some(failure) = outcome.failures.first() {
            return Err(crate::store::StoreError::InvalidEncoding {
                context: "SHACL pending replay",
                message: failure.error.clone(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn replay_bindings_bounded(
        &self,
        max_graphs: usize,
        max_elapsed: Option<Duration>,
    ) -> crate::store::Result<crate::PendingReplayOutcome> {
        let started = Instant::now();
        let deadline = max_elapsed.and_then(|elapsed| started.checked_add(elapsed));
        let scan = self.store.bounded_shacl_queue(max_graphs, deadline)?;
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
                    if !self.store.shacl_graph_pending(&graph)? {
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
    fn persist_shacl_post(&self, graph: &GraphId, evaluations: Vec<ShaclEvaluation>) -> bool {
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
    fn settle_bindings_post(
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
    fn settle_current_post(&self, graph: &GraphId) -> bool {
        if let Err(error) = self.settle_current(graph) {
            self.report_settlement_failure(graph, &error);
            return false;
        }
        true
    }

    fn ensure_change_targets(
        &self,
        graph: &GraphId,
        changes: &[MaterializedQuadChange],
    ) -> Result<(), UpdateError> {
        for change in changes {
            let (change_graph, subject, predicate, object) = match change {
                MaterializedQuadChange::Insert {
                    graph,
                    subject,
                    predicate,
                    object,
                }
                | MaterializedQuadChange::Delete {
                    graph,
                    subject,
                    predicate,
                    object,
                } => (graph, subject, predicate, object),
            };
            if change_graph != graph {
                return Err(UpdateError::InvalidChangeSet(format!(
                    "all changes must target `{}` but found `{}`",
                    graph.as_str(),
                    change_graph.as_str()
                )));
            }
            if let Some(term) = [subject, predicate, object]
                .into_iter()
                .find(|term| term.is_rdf_star())
            {
                return Err(RdfStarError {
                    term: term.0.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn ensure_data_current(
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

    fn ensure_shapes_current(&self, fence: &PreparedCommitFence<'_>) -> Result<(), UpdateError> {
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
        self.commit_with_plan(LocalCommit {
            id: None,
            write_locked: false,
            graph,
            changes,
            checks: WriteChecks::normal(DiagnosticsMode::Immediate),
            prepared_fence: None,
            render_hints: None,
            commit: None,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %commit.graph.as_str(), change_count = commit.changes.len(), sync_enabled = self.sync.is_some()))]
    fn commit_with_plan(&self, commit: LocalCommit<'_>) -> Result<Batch, UpdateError> {
        let LocalCommit {
            id,
            write_locked,
            graph,
            changes,
            checks,
            prepared_fence,
            render_hints,
            commit,
        } = commit;
        check_commit(commit.as_ref(), graph)?;
        if self.sync.is_none() && commit.is_some() {
            return Err(UpdateError::InvalidChangeSet(
                "commit info needs a write that publishes an Irokle event".to_owned(),
            ));
        }
        let mutation_id = id.unwrap_or_else(new_mutation);

        // Serializes the permanent tombstone check with every local write and
        // graph delete, regardless of whether replication is configured.
        let _write_guard = (!write_locked).then(|| self.store.graph_write_guard(graph));
        if let Some(tombstone) = self.store.graph_tombstone(graph)? {
            return Err(UpdateError::GraphDeleted { tombstone });
        }

        if let Some(sync) = &self.sync {
            // The store-local graph guard serializes validation, publication, and apply.
            let _commit_guard = self.store.graph_commit_guard(graph);
            if let Some(fence) = prepared_fence.as_ref() {
                self.ensure_data_current(graph, fence)?;
            }
            let render_hints = self.changed_render_hints(graph, render_hints)?;
            if changes.is_empty() && render_hints.is_none() {
                return self.empty_batch(graph);
            }
            if checks.structural_rules.enabled() {
                self.validate(graph, &changes)?;
            }
            #[cfg(feature = "shacl-core")]
            let (binding_guard, prepared_shacl) =
                self.prepare_shacl_commit(graph, &changes, checks.shacl_enforcement.enabled())?;
            if let Some(fence) = prepared_fence.as_ref() {
                self.ensure_shapes_current(fence)?;
            }
            #[cfg(feature = "shacl-core")]
            let pending_graphs = self.store.affected_shacl_graphs(graph)?;
            #[cfg(feature = "shacl-core")]
            let advisory_work = prepared_shacl.advisory_work;
            #[cfg(feature = "shacl-core")]
            let advisory_changes = advisory_work.as_ref().map(|_| changes.clone());
            #[cfg(feature = "shacl-core")]
            let mut shacl_evaluations = prepared_shacl.enforce_evaluations;

            let topic = sync
                .ensure_topic_guarded(&self.store, graph)
                .map_err(|error| self.accepted_sync(mutation_id, error))?;
            let frontier = sync
                .topic_frontier(topic)
                .map_err(|error| self.accepted_sync(mutation_id, error))?;
            let request_digest = mutation_digest(graph, &changes, render_hints.as_ref())?;
            let existing_receipt = self.store.mutation_receipt(&mutation_id)?;
            if existing_receipt.as_ref().is_some_and(|receipt| {
                receipt.graph != *graph || receipt.request_digest != request_digest
            }) {
                return Err(UpdateError::InvalidChangeSet(
                    "mutation id is already bound to a different request".to_owned(),
                ));
            }
            let prepared_receipt =
                existing_receipt
                    .clone()
                    .unwrap_or(crate::sync::MutationReceipt {
                        id: mutation_id,
                        admission_sequence: 0,
                        graph: graph.clone(),
                        request_digest,
                        event_id: None,
                        topic: Some(topic),
                        publish_after: Some(frontier.clock.clone()),
                        topic_epoch: Some(frontier.epoch),
                        topic_genesis: Some(frontier.genesis),
                        search_token: None,
                        repair_graphs: vec![graph.clone()],
                        source: crate::sync::SourceOutcome::Prepared,
                        persistence: crate::sync::PersistenceOutcome::Pending,
                        repairs: crate::sync::RepairState {
                            diagnostics: crate::sync::RepairOutcome::Pending,
                            #[cfg(feature = "shacl-core")]
                            shacl: crate::sync::RepairOutcome::Pending,
                            #[cfg(not(feature = "shacl-core"))]
                            shacl: crate::sync::RepairOutcome::NotRequired,
                            search: crate::sync::RepairOutcome::Pending,
                            query_view: crate::sync::RepairOutcome::Pending,
                        },
                        source_version: self.store.graph_version_digest(graph)?,
                        updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
                    });
            if existing_receipt.is_none() {
                let _receipt_guard = self.store.receipt_guard(&mutation_id);
                let mut receipt_batch = self.store.new_batch();
                if self
                    .store
                    .stage_receipt(&mut receipt_batch, &prepared_receipt)?
                    .is_none()
                {
                    self.store.commit(receipt_batch)?;
                }
            }

            // Publish-first ordering leaves a failed publication unapplied locally.
            let record = sync
                .publish_mutation(
                    &self.store,
                    crate::sync::OutgoingMutation {
                        id: mutation_id,
                        graph: graph.clone(),
                        changes,
                        render_hints,
                        commit,
                    },
                )
                .map_err(|error| self.accepted_sync(mutation_id, error))?;
            let Some(mutation) = crate::sync::batch_from_owned(record)
                .map_err(|error| self.accepted_sync(mutation_id, error))?
            else {
                return Err(UpdateError::InvalidChangeSet(
                    "irokle changes publish did not return a quad-change record".to_string(),
                ));
            };
            let batch = mutation.batch;
            let bound_receipt = self
                .store
                .bind_receipt_event(&mutation_id, *mutation.event_id.as_bytes())
                .map_err(|error| self.accepted_store(mutation_id, error))?;
            let apply = ApplyPlan {
                hints: mutation.render_hints.as_ref(),
                diagnostics: checks.diagnostics,
                id: mutation.mutation_id,
                event_id: bound_receipt.event_id,
                request_digest: mutation.request_digest,
                topic: bound_receipt.topic,
                publish_after: bound_receipt.publish_after.as_ref(),
                topic_epoch: bound_receipt.topic_epoch,
                topic_genesis: bound_receipt.topic_genesis,
            };
            let _merged = self
                .apply_irokle_guarded(&batch, apply)
                .map_err(|error| self.accepted_merge(mutation_id, error))?;
            #[cfg(feature = "shacl-core")]
            {
                let data_version = _merged
                    .applied
                    .then(|| self.stamp_evaluations(graph, &mut shacl_evaluations))
                    .transpose()
                    .map_err(|error| self.accepted_store(mutation_id, error))?;
                drop(_commit_guard);
                drop(binding_guard);
                drop(_write_guard);
                if _merged.applied {
                    let mut source_settled = self.persist_shacl_post(graph, shacl_evaluations);
                    if let (Some(work), Some(changes), Some(data_version)) =
                        (advisory_work, advisory_changes, data_version)
                    {
                        source_settled &=
                            self.settle_bindings_post(graph, &changes, data_version, work);
                    }
                    self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
                    self.complete_shacl(mutation_id, source_settled)
                        .map_err(|error| self.accepted_store(mutation_id, error))?;
                }
            }
            return Ok(batch);
        }

        // Guard the complete CRDT read, mutation, commit, and diagnostics cycle.
        let _commit_guard = self.store.graph_commit_guard(graph);

        if let Some(fence) = prepared_fence.as_ref() {
            self.ensure_data_current(graph, fence)?;
        }
        let render_hints = self.changed_render_hints(graph, render_hints)?;
        if changes.is_empty() && render_hints.is_none() {
            return self.empty_batch(graph);
        }
        let request_digest = mutation_digest(graph, &changes, render_hints.as_ref())?;

        if checks.structural_rules.enabled() {
            self.validate(graph, &changes)?;
        }
        #[cfg(feature = "shacl-core")]
        let (binding_guard, prepared_shacl) =
            self.prepare_shacl_commit(graph, &changes, checks.shacl_enforcement.enabled())?;
        if let Some(fence) = prepared_fence.as_ref() {
            self.ensure_shapes_current(fence)?;
        }
        #[cfg(feature = "shacl-core")]
        let pending_graphs = self.store.affected_shacl_graphs(graph)?;
        #[cfg(feature = "shacl-core")]
        let advisory_work = prepared_shacl.advisory_work;
        #[cfg(feature = "shacl-core")]
        let advisory_changes = advisory_work.as_ref().map(|_| changes.clone());
        #[cfg(feature = "shacl-core")]
        let mut shacl_evaluations = prepared_shacl.enforce_evaluations;

        // Capture diagnostics under the guard unless the caller defers their potentially costly refresh.
        let pending = checks.diagnostics.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_delta(graph, &changes),
            })
        })?;

        let mut batch = self.store.new_batch();
        let _receipt_guard = self.store.receipt_guard(&mutation_id);
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
        if let Some(render_hints) = &render_hints {
            self.store
                .stage_graph_context(&mut batch, graph_id, render_hints)?;
        }
        #[cfg(feature = "shacl-core")]
        self.store
            .stage_pending_bindings(&mut batch, graph, clock_digest(&vector_clock)?)?;
        let receipt = source_receipt(
            graph,
            ApplyPlan {
                hints: render_hints.as_ref(),
                diagnostics: checks.diagnostics,
                id: mutation_id,
                event_id: None,
                request_digest,
                topic: None,
                publish_after: None,
                topic_epoch: None,
                topic_genesis: None,
            },
            &vector_clock,
        )
        .map_err(merge_update_error)?;
        if self.store.mutation_receipt(&mutation_id)?.is_some() {
            self.store.stage_receipt_update(&mut batch, &receipt)?;
        } else if self.store.stage_receipt(&mut batch, &receipt)?.is_some() {
            return Err(UpdateError::InvalidChangeSet(
                "mutation receipt changed".to_owned(),
            ));
        }
        self.store.stage_batch_receipt(
            &mut batch,
            &BatchReceiptLink {
                graph: graph_id,
                actor: self.actor,
                counter,
                id: mutation_id,
            },
        )?;
        self.store.commit(batch)?;
        drop(_receipt_guard);
        #[cfg(feature = "shacl-core")]
        let data_version = self
            .stamp_evaluations(graph, &mut shacl_evaluations)
            .map_err(|error| self.accepted_store(mutation_id, error))?;

        drop(_commit_guard);
        #[cfg(feature = "shacl-core")]
        drop(binding_guard);
        drop(_write_guard);

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)
                .map_err(|error| self.accepted_store(mutation_id, error))?;
            self.complete_diagnostics(mutation_id)
                .map_err(|error| self.accepted_store(mutation_id, error))?;
        }
        #[cfg(feature = "shacl-core")]
        {
            let mut source_settled = self.persist_shacl_post(graph, shacl_evaluations);
            if let (Some(work), Some(changes)) = (advisory_work, advisory_changes) {
                source_settled &= self.settle_bindings_post(graph, &changes, data_version, work);
            }
            self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
            self.complete_shacl(mutation_id, source_settled)
                .map_err(|error| self.accepted_store(mutation_id, error))?;
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
        // Replicated literal aliases keep their signed spelling but share the canonical quad.
        let resolve = |cx: &mut BatchTermCtx<'_>, term: &EncodedTerm| match term.canonical() {
            Some(canonical) => self.store.resolve_term_cached(cx, &canonical),
            None => self.store.resolve_term_cached(cx, term),
        };
        Ok(EncodedQuad {
            graph: terms.graph_id,
            subject: resolve(cx, terms.subject)?,
            predicate: resolve(cx, terms.predicate)?,
            object: resolve(cx, terms.object)?,
        })
    }

    /// Apply an Irokle-ordered event under the graph write lock without requiring domain counters to be contiguous.
    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn apply_irokle_batch(&self, incoming: Batch) -> Result<MergeResult, MergeError> {
        let digest = batch_digest(&incoming)?;
        self.apply_irokle_plan(
            &incoming,
            ApplyPlan {
                hints: None,
                diagnostics: DiagnosticsMode::Immediate,
                id: crate::sync::MutationId(digest),
                event_id: None,
                request_digest: digest,
                topic: None,
                publish_after: None,
                topic_epoch: None,
                topic_genesis: None,
            },
        )
    }

    /// Validate and merge an externally transported batch under the graph write lock.
    /// Missing causal dependencies leave source state unchanged for transport retry.
    pub(crate) fn merge_batch(&self, incoming: &Batch) -> Result<MergeResult, MergeError> {
        crate::sync::check_batch(incoming)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        self.check_causal_base(incoming)?;
        let digest = batch_digest(incoming)?;
        self.apply_irokle_plan(
            incoming,
            ApplyPlan {
                hints: None,
                diagnostics: DiagnosticsMode::Immediate,
                id: crate::sync::MutationId(digest),
                event_id: None,
                request_digest: digest,
                topic: None,
                publish_after: None,
                topic_epoch: None,
                topic_genesis: None,
            },
        )
    }

    /// Reject a causally unready external batch while the graph write lock keeps the verdict valid.
    fn check_causal_base(&self, incoming: &Batch) -> Result<(), MergeError> {
        let identity = Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        };
        // Counter zero marks a batch that carries no event of its own.
        if incoming.counter > 0 && incoming.base_clock.contains(&identity) {
            return Err(MergeError::InputRejected(format!(
                "batch event {}:{} declares itself as its own dependency",
                identity.actor, identity.counter
            )));
        }
        let applied = self.store.get_vector_clock(&incoming.graph)?;
        let missing = crate::sync::missing_dots(&incoming.base_clock, &applied);
        if missing.is_empty() {
            Ok(())
        } else {
            Err(MergeError::MissingDependencies(missing))
        }
    }

    /// Call with the graph write lock held to make tombstone checks atomic with delete.
    #[tracing::instrument(level = "debug", skip_all, fields(graph = %incoming.graph.as_str(), op_count = incoming.ops.len()))]
    fn apply_irokle_plan(
        &self,
        incoming: &Batch,
        plan: ApplyPlan<'_>,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        // Reject both local and replicated writes after a permanent tombstone.
        if self.store.graph_tombstoned(graph)? {
            return Ok(MergeResult { applied: false });
        }

        // Self-guarding, so it must run before the commit guard is taken.
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        // Keep dedup, clock advancement, and diagnostics under one commit guard.
        #[cfg(feature = "shacl-core")]
        let (merged, binding_work, binding_changes, data_version, pending_graphs) = {
            let _commit_guard = self.store.graph_commit_guard(graph);
            let _binding_guard = self.store.binding_guard();
            let pending_graphs = self.store.affected_shacl_graphs(graph)?;
            let binding_work = {
                let work = self.binding_work(
                    graph,
                    &[
                        crate::ShaclWritePolicy::Advisory,
                        crate::ShaclWritePolicy::Enforce,
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
                self.settle_bindings_post(graph, &changes, data_version, work)
            } else {
                true
            }
        } else {
            self.settle_current_post(graph)
        };
        #[cfg(feature = "shacl-core")]
        self.settle_shacl_graphs(&pending_graphs, (!source_settled).then_some(graph));
        #[cfg(feature = "shacl-core")]
        if merged.applied {
            self.complete_shacl(plan.id, source_settled)?;
        }
        Ok(merged)
    }

    fn apply_irokle_guarded(
        &self,
        incoming: &Batch,
        plan: ApplyPlan<'_>,
    ) -> Result<MergeResult, MergeError> {
        let graph = &incoming.graph;

        let mut vector_clock = self.store.get_vector_clock(graph)?;
        if vector_clock.contains(&Dot {
            actor: incoming.actor,
            counter: incoming.counter,
        }) {
            let receipt = crate::sync::MutationReceipt {
                id: plan.id,
                admission_sequence: 0,
                graph: graph.clone(),
                request_digest: plan.request_digest,
                event_id: plan.event_id,
                topic: plan.topic,
                publish_after: plan.publish_after.cloned(),
                topic_epoch: plan.topic_epoch,
                topic_genesis: plan.topic_genesis,
                search_token: None,
                repair_graphs: vec![graph.clone()],
                source: crate::sync::SourceOutcome::Duplicate,
                persistence: crate::sync::PersistenceOutcome::Pending,
                repairs: settled_repairs(),
                source_version: clock_digest(&vector_clock)?,
                updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
            };
            let _receipt_guard = self.store.receipt_guard(&plan.id);
            let mut batch = self.store.new_batch();
            if self.store.stage_receipt(&mut batch, &receipt)?.is_none() {
                self.store.commit(batch)?;
            }
            return Ok(MergeResult { applied: false });
        }

        let pending = plan.diagnostics.pending_diagnostics(|| {
            Ok(PendingDiagnostics {
                previous: self.store.graph_diagnostics(graph)?,
                summary: crate::rules::summarize_ops(graph, &incoming.ops),
            })
        })?;

        self.apply_single_batch(ApplyBatch {
            incoming,
            clock: &mut vector_clock,
            plan,
        })?;

        if let Some(pending) = &pending {
            self.settle_diagnostics(graph, pending)?;
            self.complete_diagnostics(plan.id)?;
        }
        Ok(MergeResult { applied: true })
    }

    fn complete_diagnostics(&self, id: crate::sync::MutationId) -> crate::store::Result<()> {
        let Some(mut receipt) = self.store.mutation_receipt(&id)? else {
            return Err(crate::store::StoreError::InvalidEncoding {
                context: "mutation receipt",
                message: "accepted source has no receipt".to_owned(),
            });
        };
        receipt.repairs.diagnostics = crate::sync::RepairOutcome::Complete;
        receipt.updated_unix_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
        self.store.update_receipt(&receipt)?;
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn complete_shacl(
        &self,
        id: crate::sync::MutationId,
        settled: bool,
    ) -> crate::store::Result<()> {
        let Some(mut receipt) = self.store.mutation_receipt(&id)? else {
            return Err(crate::store::StoreError::InvalidEncoding {
                context: "mutation receipt",
                message: "accepted source has no receipt".to_owned(),
            });
        };
        receipt.repairs.shacl = if settled {
            crate::sync::RepairOutcome::Complete
        } else {
            crate::sync::RepairOutcome::Failed(crate::CraqleErrorKind::Storage)
        };
        receipt.updated_unix_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
        self.store.update_receipt(&receipt)?;
        Ok(())
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
        let mutation = crate::sync::batch_from_record(record)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        mutation
            .map(|mutation| {
                let mut prior = self.store.mutation_receipt(&mutation.mutation_id)?;
                // A record that reuses a mutation id bound to other content can never apply.
                if prior.as_ref().is_some_and(|receipt| {
                    receipt.graph != mutation.batch.graph
                        || match receipt.event_id {
                            Some(event) => event != *mutation.event_id.as_bytes(),
                            None => receipt.request_digest != mutation.request_digest,
                        }
                }) {
                    return Err(MergeError::InputRejected(
                        "mutation id is already bound to another record".to_owned(),
                    ));
                }
                if prior.as_ref().is_some_and(|receipt| {
                    receipt.source == crate::sync::SourceOutcome::Prepared
                        && receipt.event_id.is_none()
                }) {
                    prior = Some(self.store.bind_receipt_event(
                        &mutation.mutation_id,
                        *mutation.event_id.as_bytes(),
                    )?);
                }
                self.apply_irokle_plan(
                    &mutation.batch,
                    ApplyPlan {
                        hints: mutation.render_hints.as_ref(),
                        diagnostics: DiagnosticsMode::Immediate,
                        id: mutation.mutation_id,
                        event_id: prior
                            .as_ref()
                            .and_then(|receipt| receipt.event_id)
                            .or(Some(*mutation.event_id.as_bytes())),
                        request_digest: mutation.request_digest,
                        topic: prior.as_ref().and_then(|receipt| receipt.topic),
                        publish_after: prior
                            .as_ref()
                            .and_then(|receipt| receipt.publish_after.as_ref()),
                        topic_epoch: prior.as_ref().and_then(|receipt| receipt.topic_epoch),
                        topic_genesis: prior.as_ref().and_then(|receipt| receipt.topic_genesis),
                    },
                )
            })
            .transpose()
    }

    pub(crate) fn install_with_receipt(
        &self,
        snapshot: &GraphReplicaSnapshot,
    ) -> Result<MergeReceipt, MergeError> {
        let graph = &snapshot.graph;
        crate::sync::check_snapshot(snapshot)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        if self.store.graph_tombstoned(graph)? {
            return Ok(MergeReceipt {
                result: MergeResult { applied: false },
                receipt: None,
            });
        }
        // Self-guarding, so it must run before the commit guard is taken.
        if !self.store.contains_graph(graph)? {
            self.store.create_graph(graph)?;
        }

        #[cfg(feature = "shacl-core")]
        let repair_graphs = self.store.affected_shacl_graphs(graph)?;
        #[cfg(not(feature = "shacl-core"))]
        let repair_graphs = Vec::new();
        let id = new_mutation();
        let request_digest = snapshot_digest(snapshot)?;
        let source_receipt = crate::sync::MutationReceipt {
            id,
            admission_sequence: 0,
            graph: graph.clone(),
            request_digest,
            event_id: None,
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs: repair_graphs.clone(),
            source: crate::sync::SourceOutcome::Applied,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: crate::sync::RepairState {
                diagnostics: crate::sync::RepairOutcome::Pending,
                #[cfg(feature = "shacl-core")]
                shacl: crate::sync::RepairOutcome::Pending,
                #[cfg(not(feature = "shacl-core"))]
                shacl: crate::sync::RepairOutcome::NotRequired,
                search: crate::sync::RepairOutcome::Pending,
                query_view: crate::sync::RepairOutcome::Pending,
            },
            source_version: clock_digest(&snapshot.clock)?,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        let applied = {
            let _commit_guard = self.store.graph_commit_guard(graph);
            self.join_snapshot(snapshot, Some(&source_receipt))?
        };

        #[cfg(feature = "shacl-core")]
        if applied {
            let settled = self.settle_current_post(graph);
            self.settle_shacl_graphs(&repair_graphs, (!settled).then_some(graph));
            if let Err(error) = self.complete_shacl(id, settled) {
                let Some(receipt) = self.store.mutation_receipt(&id)? else {
                    return Err(MergeError::Store(error));
                };
                return Err(MergeError::Accepted {
                    receipt: Box::new(receipt),
                    error_kind: error.kind(),
                    reason: error.to_string(),
                });
            }
        }
        let receipt = if applied {
            self.store.mutation_receipt(&id)?
        } else {
            None
        };
        Ok(MergeReceipt {
            result: MergeResult { applied },
            receipt,
        })
    }

    pub(crate) fn preview_repair(
        &self,
        request: &crate::sync::RepairRequest,
    ) -> Result<crate::sync::RepairReport, MergeError> {
        crate::sync::check_snapshot(&request.authoritative)
            .map_err(|error| MergeError::InputRejected(error.to_string()))?;
        let graph = &request.authoritative.graph;
        let local = self.store.graph_snapshot_bounded(
            graph,
            SnapshotLimits {
                max_rows: 1_048_576,
                max_bytes: 64 * 1024 * 1024,
            },
        )?;
        let before_digest = snapshot_digest(&local)?;
        let authority_digest = snapshot_digest(&request.authoritative)?;
        if let crate::sync::RepairAuthority::HealthySnapshot { digest, .. } = &request.authority
            && *digest != authority_digest
        {
            return Err(MergeError::InputRejected(
                "healthy snapshot digest does not match its authoritative state".to_owned(),
            ));
        }
        let authoritative = canonical_snapshot(&request.authoritative);
        let tombstoned = self.store.graph_tombstoned(graph)?;
        let result = if tombstoned {
            crate::sync::RepairResult::Tombstoned
        } else if local == *authoritative {
            crate::sync::RepairResult::Exact
        } else if request.mode == crate::sync::RepairMode::Apply
            && request
                .backup
                .as_ref()
                .is_none_or(|backup| backup.source_revision != before_digest)
        {
            crate::sync::RepairResult::BackupRequired
        } else {
            crate::sync::RepairResult::Differs
        };
        let diff = (local != *authoritative).then(|| crate::sync::RepairDiff {
            unresolved: crate::sync::missing_dots(&authoritative.clock, &local.clock),
            local,
            authoritative: authoritative.into_owned(),
        });
        Ok(crate::sync::RepairReport {
            audit: crate::sync::RepairAudit {
                id: request.id,
                graph: graph.clone(),
                mode: request.mode,
                authority: request.authority.clone(),
                before_digest,
                after_digest: None,
                backup: request.backup.clone(),
                result,
                updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
            },
            diff,
        })
    }

    pub(crate) fn reconcile_snapshot(
        &self,
        request: &crate::sync::RepairRequest,
    ) -> Result<crate::sync::RepairReport, MergeError> {
        let mut report = self.preview_repair(request)?;
        if request.mode == crate::sync::RepairMode::Apply
            && report.audit.result == crate::sync::RepairResult::Tombstoned
        {
            return Err(MergeError::InputRejected(
                "live authoritative snapshot cannot replace a permanently deleted graph".to_owned(),
            ));
        }
        if request.mode == crate::sync::RepairMode::Apply
            && matches!(
                report.audit.result,
                crate::sync::RepairResult::Differs | crate::sync::RepairResult::BackupRequired
            )
        {
            report.audit.backup = Some(self.store.backup_snapshot(&report.audit.graph)?);
            report.audit.result = crate::sync::RepairResult::Applied;
            let authoritative = canonical_snapshot(&request.authoritative);
            report.audit.after_digest = Some(snapshot_digest(&authoritative)?);
            self.store.replace_snapshot(&authoritative, &report.audit)?;
            let _write = self.store.graph_write_guard(&report.audit.graph);
            let _commit = self.store.graph_commit_guard(&report.audit.graph);
            self.recompute_graph_diagnostics(&report.audit.graph)?;
            return Ok(report);
        }
        self.record_repair(&report.audit)?;
        Ok(report)
    }

    pub(crate) fn record_repair(
        &self,
        audit: &crate::sync::RepairAudit,
    ) -> crate::store::Result<()> {
        let mut batch = self.store.new_batch();
        self.store.stage_repair_audit(&mut batch, audit)?;
        self.store.commit(batch)
    }

    pub(crate) fn record_history(
        &self,
        failure: crate::sync::HistoryFailure,
        result: crate::sync::RepairResult,
    ) -> Result<crate::sync::RepairReport, MergeError> {
        if !matches!(
            result,
            crate::sync::RepairResult::HistoryMissing | crate::sync::RepairResult::Tombstoned
        ) {
            return Err(MergeError::InputRejected(
                "history audit result must be missing or tombstoned".to_owned(),
            ));
        }
        let local = self.store.graph_snapshot_bounded(
            &failure.graph,
            SnapshotLimits {
                max_rows: 1_048_576,
                max_bytes: 64 * 1024 * 1024,
            },
        )?;
        let audit = crate::sync::RepairAudit {
            id: failure.id,
            graph: failure.graph,
            mode: failure.mode,
            authority: failure.authority,
            before_digest: snapshot_digest(&local)?,
            after_digest: None,
            backup: None,
            result,
            updated_unix_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
        };
        self.record_repair(&audit)?;
        Ok(crate::sync::RepairReport { audit, diff: None })
    }

    /// Write the OR-Set join of local state and `snapshot`, reporting whether
    /// anything changed. **Call with the graph commit guard held.**
    fn join_snapshot(
        &self,
        snapshot: &GraphReplicaSnapshot,
        receipt: Option<&crate::sync::MutationReceipt>,
    ) -> Result<bool, MergeError> {
        let graph = &snapshot.graph;
        let snapshot = &*canonical_snapshot(snapshot);
        // An absent dot covered by the other pre-merge clock is an observed removal.
        let local = self.store.graph_snapshot(graph)?;
        let mut clock = local.clock.clone();
        clock.merge(&snapshot.clock);
        let mut changed = clock != local.clock;

        let mut batch = self.store.new_batch();
        let mut affected_subjects = HashSet::new();
        let mut term_cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut term_cache,
        };
        self.store.seed_term_cache(
            &mut cx,
            local
                .quads
                .iter()
                .chain(&snapshot.quads)
                .flat_map(|quad| [&quad.subject, &quad.predicate, &quad.object]),
        )?;
        let graph_id = self
            .store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))?;

        for (terms, (here, there)) in join_union(&local, snapshot) {
            let joined = join_dots(here, there, (&local.clock, &snapshot.clock));
            let dropped = here
                .iter()
                .copied()
                .filter(|dot| !joined.contains(dot))
                .collect::<Vec<_>>();
            let added = joined
                .iter()
                .copied()
                .filter(|dot| !here.contains(dot))
                .collect::<Vec<_>>();
            if dropped.is_empty() && added.is_empty() {
                continue;
            }
            changed = true;
            let quad = self.resolve_quad(
                &mut cx,
                QuadTerms {
                    graph_id,
                    subject: terms.0,
                    predicate: terms.1,
                    object: terms.2,
                },
            )?;
            // Adds first: a quad the join keeps must not look removed in
            // between, so the derived state sees one liveness transition.
            for dot in &added {
                self.store
                    .insert_quad(cx.batch, QuadAdd { quad, dot: *dot })?;
            }
            if !dropped.is_empty() {
                let mut witnessed = VectorClock::new();
                for dot in &dropped {
                    witnessed.advance(dot.actor, dot.counter);
                }
                self.store.remove_quad(
                    cx.batch,
                    QuadRemove {
                        quad,
                        witnessed: &witnessed,
                    },
                )?;
                // A witnessed clock covers whole counter ranges, so restore any
                // kept dot it took with it.
                for dot in joined.iter().filter(|dot| witnessed.contains(dot)) {
                    self.store
                        .insert_quad(cx.batch, QuadAdd { quad, dot: *dot })?;
                }
            }
            affected_subjects.insert(quad.subject);
        }

        // Nothing to commit: the staged term interning is content-addressed, so
        // discarding the batch loses no state.
        if !changed {
            return Ok(false);
        }

        self.store.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: &clock,
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
            .stage_pending_bindings(&mut batch, graph, clock_digest(&clock)?)?;
        let mut staged_receipt = receipt.cloned();
        if affected_subjects.is_empty()
            && let Some(receipt) = &mut staged_receipt
        {
            receipt.repairs.search = crate::sync::RepairOutcome::NotRequired;
        }
        let _receipt_guard = staged_receipt
            .as_ref()
            .map(|receipt| self.store.receipt_guard(&receipt.id));
        if let Some(receipt) = &staged_receipt
            && self.store.stage_receipt(&mut batch, receipt)?.is_some()
        {
            return Err(MergeError::Store(crate::store::StoreError::ReceiptConflict));
        }
        self.store.commit(batch)?;
        drop(_receipt_guard);
        if let Err(error) = self.recompute_graph_diagnostics(graph) {
            if let Some(receipt) = staged_receipt
                .as_ref()
                .and_then(|receipt| self.store.mutation_receipt(&receipt.id).ok().flatten())
            {
                return Err(MergeError::Accepted {
                    receipt: Box::new(receipt),
                    error_kind: error.kind(),
                    reason: error.to_string(),
                });
            }
            return Err(MergeError::Store(error));
        }
        if let Some(receipt) = &staged_receipt {
            self.complete_diagnostics(receipt.id)?;
        }
        Ok(true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(graph = %apply.incoming.graph.as_str(), op_count = apply.incoming.ops.len()))]
    fn apply_single_batch(&self, apply: ApplyBatch<'_>) -> Result<(), MergeError> {
        let ApplyBatch {
            incoming,
            clock: vector_clock,
            plan,
        } = apply;
        let graph = &incoming.graph;
        let _receipt_guard = self.store.receipt_guard(&plan.id);
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
        if let Some(render_hints) = plan.hints {
            self.store
                .stage_graph_context(&mut batch, graph_id, render_hints)?;
        }
        #[cfg(feature = "shacl-core")]
        self.store
            .stage_pending_bindings(&mut batch, graph, clock_digest(vector_clock)?)?;
        let receipt = source_receipt(graph, plan, vector_clock)?;
        if self.store.mutation_receipt(&plan.id)?.is_some() {
            self.store.stage_receipt_update(&mut batch, &receipt)?;
        } else if self.store.stage_receipt(&mut batch, &receipt)?.is_some() {
            return Err(MergeError::Store(
                crate::store::StoreError::InvalidEncoding {
                    context: "mutation receipt",
                    message: "receipt exists without applied source clock".to_owned(),
                },
            ));
        }
        self.store.stage_batch_receipt(
            &mut batch,
            &BatchReceiptLink {
                graph: graph_id,
                actor: incoming.actor,
                counter: incoming.counter,
                id: plan.id,
            },
        )?;
        self.store.commit(batch)?;
        Ok(())
    }

    /// Settle diagnostics under the graph commit guard, restamping only when reachability is unchanged.
    fn settle_diagnostics(
        &self,
        graph: &GraphId,
        pending: &PendingDiagnostics,
    ) -> crate::store::Result<()> {
        // Only data-entity type and `hasPart` shapes can change the orphan set.
        if !pending.summary.touches_reachability() {
            return self.publish_graph_diagnostics(graph, &pending.previous);
        }

        // Case 2. `pending.previous` is not the post-write set, so recompute.
        self.recompute_graph_diagnostics(graph)
    }

    /// Recompute and publish the post-write orphan set.
    fn recompute_graph_diagnostics(&self, graph: &GraphId) -> crate::store::Result<()> {
        // The commit already made the stored record's clock tag stale, so this
        // read recomputes. It does not persist: this is the record's writer.
        let current = self.store.graph_diagnostics(graph)?;
        self.publish_graph_diagnostics(graph, &current)
    }

    /// Re-queue orphan changes against the persisted baseline before publishing current diagnostics.
    /// A crash between those commits retains the older baseline for safe replay.
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
        let [subject, predicate, object] = [subject, predicate, object]
            .map(|term| term.canonical().unwrap_or_else(|| term.clone()));
        let key = (subject.clone(), predicate.clone(), object.clone());
        let index = if let Some(index) = indexes.get(&key) {
            *index
        } else {
            let dots = store.quad_dots(&batch.graph, &subject, &predicate, &object)?;
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

type TermTriple<'a> = (&'a EncodedTerm, &'a EncodedTerm, &'a EncodedTerm);

/// Merges literal aliases into their canonical quads, like applying the same events would.
fn canonical_snapshot(snapshot: &GraphReplicaSnapshot) -> Cow<'_, GraphReplicaSnapshot> {
    fn terms(quad: &SnapshotQuadState) -> [&EncodedTerm; 3] {
        [&quad.subject, &quad.predicate, &quad.object]
    }
    let aliased =
        |quad: &SnapshotQuadState| terms(quad).iter().any(|term| term.canonical().is_some());
    if !snapshot.quads.iter().any(aliased) {
        return Cow::Borrowed(snapshot);
    }
    let mut quads: BTreeMap<[EncodedTerm; 3], Vec<Dot>> = BTreeMap::new();
    for quad in &snapshot.quads {
        let key = terms(quad).map(|term| term.canonical().unwrap_or_else(|| term.clone()));
        quads.entry(key).or_default().extend(&quad.dots);
    }
    let quads = quads
        .into_iter()
        .map(|([subject, predicate, object], mut dots)| {
            dots.sort_unstable_by_key(|dot| (dot.actor, dot.counter));
            dots.dedup();
            SnapshotQuadState {
                subject,
                predicate,
                object,
                dots,
            }
        })
        .collect();
    Cow::Owned(GraphReplicaSnapshot {
        graph: snapshot.graph.clone(),
        clock: snapshot.clock.clone(),
        quads,
    })
}

/// Include unilateral quads so the opposite clock can prove an observed removal.
fn join_union<'a>(
    local: &'a GraphReplicaSnapshot,
    remote: &'a GraphReplicaSnapshot,
) -> BTreeMap<TermTriple<'a>, (&'a [Dot], &'a [Dot])> {
    let mut rows: BTreeMap<TermTriple<'a>, (&'a [Dot], &'a [Dot])> = BTreeMap::new();
    for quad in &local.quads {
        rows.insert(
            (&quad.subject, &quad.predicate, &quad.object),
            (&quad.dots, &[]),
        );
    }
    for quad in &remote.quads {
        rows.entry((&quad.subject, &quad.predicate, &quad.object))
            .or_insert((&[], &[]))
            .1 = &quad.dots;
    }
    rows
}

/// Dots the OR-Set join keeps for one quad: the dots both sides hold, plus
/// each side's dots the other side's pre-merge context does not cover.
fn join_dots(local: &[Dot], remote: &[Dot], contexts: (&VectorClock, &VectorClock)) -> Vec<Dot> {
    let (here, there) = contexts;
    let mut joined = local
        .iter()
        .copied()
        .filter(|dot| remote.contains(dot) || !there.contains(dot))
        .collect::<Vec<_>>();
    joined.extend(
        remote
            .iter()
            .copied()
            .filter(|dot| !local.contains(dot) && !here.contains(dot)),
    );
    joined
}

fn snapshot_digest(snapshot: &GraphReplicaSnapshot) -> Result<[u8; 32], MergeError> {
    let bytes = postcard::to_allocvec(snapshot)
        .map_err(crate::store::StoreError::from)
        .map_err(MergeError::Store)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn merge_update_error(error: MergeError) -> UpdateError {
    match error {
        MergeError::Store(error) => UpdateError::Store(error),
        MergeError::InputRejected(message) => UpdateError::InvalidChangeSet(message),
        MergeError::MissingDependencies(_) => UpdateError::InvalidChangeSet(error.to_string()),
        MergeError::Accepted {
            receipt,
            error_kind,
            reason,
        } => UpdateError::Accepted {
            receipt,
            error_kind,
            reason,
        },
    }
}

#[cfg(all(test, feature = "shacl-core"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::search::SearchIndex;
    use crate::sync::{CraqleGraphSync, CraqleIrokleOptions, IrokleGraphSync};
    use crate::{ShaclBinding, ShaclBindingOptions, ShaclWritePolicy};

    fn engine_at(dir: &std::path::Path) -> (Arc<GraphStore>, ReplicationEngine) {
        let store = Arc::new(GraphStore::open(dir).unwrap());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        let sparql = Arc::new(SparqlEngine::new(store.clone(), search));
        let engine = ReplicationEngine::new(store.clone(), sparql, ActorId::random());
        (store, engine)
    }

    fn settled_receipt(
        store: &GraphStore,
        graph: &GraphId,
        persistence: crate::sync::PersistenceOutcome,
    ) -> crate::sync::MutationReceipt {
        crate::sync::MutationReceipt {
            id: crate::sync::MutationId::new(),
            admission_sequence: 0,
            graph: graph.clone(),
            request_digest: [7; 32],
            event_id: None,
            topic: None,
            publish_after: None,
            topic_epoch: None,
            topic_genesis: None,
            search_token: None,
            repair_graphs: Vec::new(),
            source: crate::sync::SourceOutcome::Applied,
            persistence,
            repairs: settled_repairs(),
            source_version: store.graph_version_digest(graph).unwrap(),
            updated_unix_nanos: 1,
        }
    }

    fn store_receipt(store: &GraphStore, receipt: &crate::sync::MutationReceipt) {
        let _guard = store.receipt_guard(&receipt.id);
        let mut batch = store.new_batch();
        assert!(store.stage_receipt(&mut batch, receipt).unwrap().is_none());
        store.commit(batch).unwrap();
        store.persist_receipts().unwrap();
    }

    #[test]
    fn status_skips_writes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let graph = GraphId::new("urn:test:receipt-status-work");
        store.create_graph(&graph).unwrap();
        let receipt = settled_receipt(&store, &graph, store.persistence_outcome());
        store_receipt(&store, &receipt);
        let stored = store.mutation_receipt(&receipt.id).unwrap().unwrap();
        let lookup = crate::sync::MutationLookup {
            graph,
            id: stored.id,
            admission_sequence: Some(stored.admission_sequence),
        };
        let before = store.receipt_work();

        assert!(matches!(
            engine.mutation_status(&lookup).unwrap(),
            crate::sync::MutationStatus::Known(_)
        ));
        assert!(matches!(
            engine.mutation_status(&lookup).unwrap(),
            crate::sync::MutationStatus::Known(_)
        ));

        let after = store.receipt_work();
        assert_eq!(after.writes, before.writes);
        assert_eq!(after.persists, before.persists);
    }

    #[test]
    fn persist_writes_once() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let graph = GraphId::new("urn:test:receipt-persist-work");
        store.create_graph(&graph).unwrap();
        let receipt = settled_receipt(&store, &graph, crate::sync::PersistenceOutcome::Pending);
        store_receipt(&store, &receipt);
        let before = store.receipt_work();

        engine.mark_persisted(&receipt.id).unwrap().unwrap();

        let after = store.receipt_work();
        assert_eq!(after.writes - before.writes, 1);
        assert_eq!(after.persists - before.persists, 1);
    }

    #[test]
    fn store_locks_isolate() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let store_a = Arc::new(GraphStore::open(dir_a.path()).unwrap());
        let store_b = Arc::new(GraphStore::open(dir_b.path()).unwrap());
        let graph = GraphId::new("urn:test:store-lock-isolation");
        let held = store_a.graph_write_guard(&graph);
        let (tx, rx) = std::sync::mpsc::channel();
        let other_graph = graph.clone();
        std::thread::spawn(move || {
            let _guard = store_b.graph_write_guard(&other_graph);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(180))
            .expect("an unrelated store was blocked by the same graph IRI");
        drop(held);
    }

    fn pending_engine(
        dir: &std::path::Path,
        policy: ShaclWritePolicy,
    ) -> (Arc<GraphStore>, ReplicationEngine, GraphId, ShaclBinding) {
        let (store, engine) = engine_at(dir);
        let data = GraphId::new("urn:test:pending-data");
        let shapes = GraphId::new("urn:test:pending-shapes");
        let focus = EncodedTerm("<urn:test:pending-focus>".to_owned());
        engine
            .apply_changes_unchecked(
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
            .apply_changes_unchecked(
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
        let results = (!conforms)
            .then(|| crate::ShaclValidationResult {
                focus_node: crate::EncodedTerm("<urn:test:focus>".to_owned()),
                value: None,
                result_path: None,
                source_shape: crate::EncodedTerm("<urn:test:shape>".to_owned()),
                source_constraint_component: "urn:test:constraint".to_owned(),
                severity: crate::EncodedTerm("<http://www.w3.org/ns/shacl#Violation>".to_owned()),
                messages: Vec::new(),
            })
            .into_iter()
            .collect();
        crate::ShaclValidationReport {
            conforms,
            accepted_by_write_policy: conforms,
            results,
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
                    policy: ShaclWritePolicy::Advisory,
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
    fn replay_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, engine) = engine_at(dir.path());
            queued_graphs(&store, 5);
            let outcome = engine.replay_bindings_bounded(2, None).unwrap();
            assert_eq!(outcome.statistics.pending_queue_entries_scanned, 2);
            assert_eq!(outcome.statistics.graphs_settled, 2);
            assert_eq!(outcome.statistics.reports_produced, 2);
            assert!(outcome.budget_exhausted);
            assert_eq!(store.pending_shacl_count().unwrap(), 3);
            store.persist().unwrap();
        }

        let (store, engine) = engine_at(dir.path());
        let outcome = engine.replay_bindings_bounded(usize::MAX, None).unwrap();
        assert_eq!(outcome.statistics.graphs_settled, 3);
        assert_eq!(outcome.statistics.reports_produced, 3);
        assert!(!outcome.budget_exhausted);
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn replay_isolates_failures() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        queued_graphs(&store, 3);
        engine.arm_settle_after(1);

        let outcome = engine.replay_bindings_bounded(usize::MAX, None).unwrap();
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

        let retry = engine.replay_bindings_bounded(usize::MAX, None).unwrap();
        assert_eq!(retry.statistics.graphs_settled, 1);
        assert_eq!(retry.statistics.reports_produced, 1);
        assert_eq!(store.pending_shacl_count().unwrap(), 0);
    }

    #[test]
    fn empty_replay_idle() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine) = engine_at(dir.path());
        let outcome = engine.replay_bindings_bounded(usize::MAX, None).unwrap();
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
                pending_engine(dir.path(), ShaclWritePolicy::Advisory);
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
            pending_engine(dir.path(), ShaclWritePolicy::Advisory);
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
    fn settlement_debt_survives() {
        for policy in [ShaclWritePolicy::Enforce, ShaclWritePolicy::Advisory] {
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
            let replay = engine.replay_bindings_bounded(usize::MAX, None).unwrap();
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
    fn settlement_preserves_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, engine, data, binding) = pending_engine(dir.path(), ShaclWritePolicy::Advisory);
        engine.replay_pending_bindings().unwrap();
        engine.arm_settle_failure();
        let shapes = binding.shapes_graph;
        let batch = engine
            .apply_changes_unchecked(
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
                pending_engine(&store_path, ShaclWritePolicy::Advisory);
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
    fn open_defers_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        let data = {
            let (store, engine, data, _binding) =
                pending_engine(&store_path, ShaclWritePolicy::Advisory);
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
    fn empty_startup_idle() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let startup = node.startup_pending_replay();
        assert_eq!(startup.statistics.binding_records_scanned, 0);
        assert_eq!(startup.statistics.pending_queue_entries_scanned, 0);
        assert_eq!(startup.statistics.graphs_settled, 0);
        assert_eq!(startup.statistics.reports_produced, 0);
    }

    #[test]
    fn healthy_open_lazy() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        {
            let (store, engine, _data, _binding) =
                pending_engine(&store_path, ShaclWritePolicy::Advisory);
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
            pending_engine(receiver_dir.path(), ShaclWritePolicy::Advisory);
        receiver.replay_pending_bindings().unwrap();
        let shapes = GraphId::new("urn:test:pending-shapes");
        let batch = sender
            .apply_changes_unchecked(
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
                pending_engine(&store_path, ShaclWritePolicy::Advisory);
            receiver.replay_pending_bindings().unwrap();
            let shapes = GraphId::new("urn:test:pending-shapes");
            let batch = sender
                .apply_changes_unchecked(
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
            pending_engine(dir.path(), ShaclWritePolicy::Disabled);
        let before = store.graph_version_digest(&data).unwrap();

        engine
            .apply_changes_unchecked(
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
        let (store, engine, data, _binding) = pending_engine(dir.path(), ShaclWritePolicy::Enforce);
        engine.replay_pending_bindings().unwrap();
        let before = (
            store.graph_snapshot(&data).unwrap(),
            store.get_vector_clock(&data).unwrap(),
            store.index_status_fast().unwrap(),
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
        assert_eq!(store.index_status_fast().unwrap(), before.2);
        assert_eq!(store.shacl_binding_statuses(&data).unwrap(), before.3);
    }

    #[test]
    fn sync_reject() {
        let dir = tempfile::tempdir().unwrap();
        let (store, setup, data, binding) = pending_engine(dir.path(), ShaclWritePolicy::Enforce);
        let shapes = binding.shapes_graph.clone();
        let shape = EncodedTerm("<urn:test:pending-shape>".to_owned());
        let property = EncodedTerm("<urn:test:pending-property>".to_owned());
        setup
            .apply_changes_unchecked(
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
            .apply_changes_unchecked(
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
            store.index_status_fast().unwrap(),
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
        assert_eq!(store.index_status_fast().unwrap(), before.2);
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
            .apply_changes_unchecked(
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
            .apply_changes_unchecked(
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
            policy: ShaclWritePolicy::Advisory,
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
            .apply_changes_unchecked(
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
            .apply_changes_unchecked(
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
                .apply_changes_unchecked(
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
            .apply_changes_unchecked(
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
            policy: ShaclWritePolicy::Advisory,
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
            .apply_changes_unchecked(
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
                .apply_changes_unchecked(
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
                    .apply_changes_unchecked(
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
                .apply_changes_unchecked(
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
                policy: ShaclWritePolicy::Advisory,
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
                .apply_changes_unchecked(
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
            policy: ShaclWritePolicy::Advisory,
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
