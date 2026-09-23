//! Reads, compares, replays and restores a graph's signed causal history.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::path::PathBuf;

use crate::core::{CrateRenderHints, TaggedRenderHints};
use crate::replication::{EventExtras, UpdateError};
use crate::sync::GraphHints;
use crate::{
    Action, AuthorizationError, Authorizer, CommitInfo, CraqleErrorKind, CraqleGraphEvent,
    CraqleNode, CraqleOptions, CraqleSyncError, Dot, EncodedTerm, GraphId, GraphPolicy,
    MaterializedQuadChange, MemoryBudget, MutationId, MutationRequest, QuadOp, Result,
    SearchStorage,
};
use irokle::OpId;
use irokle::reducer::EventRecord;

/// Explicit heads and resource bounds for an isolated historical projection.
pub struct GraphHistory {
    pub graph: GraphId,
    pub heads: Vec<OpId>,
    pub directory: PathBuf,
    pub max_operations: usize,
    pub max_bytes: usize,
}

/// A disposable graph view and the signed operations from which it was built.
pub struct HistoryProjection {
    pub node: CraqleNode,
    pub operations: Vec<irokle::Op>,
}

/// One page of a graph's history, walked newest first from `heads`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryLog {
    pub graph: GraphId,
    /// Start points; pass [`HistoryPage::next`] to read the following page.
    pub heads: Vec<OpId>,
    pub limit: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryPage {
    pub operations: Vec<HistoryOperation>,
    /// Heads of the next page; empty once the walk reached the topic genesis.
    pub next: Vec<OpId>,
}

/// One signed operation. Irokle operations carry no wall-clock timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryOperation {
    pub id: OpId,
    pub parents: Vec<OpId>,
    pub author: irokle::PeerId,
    pub actor: irokle::ActorId,
    pub sequence: u64,
    pub generation: u64,
    /// `None` for topic control operations and for rejected records.
    pub event: Option<CraqleGraphEvent>,
    /// The record could not be decoded, targets another graph, or the store rejected it.
    /// Rejected records are skipped when history is replayed.
    pub rejected: bool,
}

impl HistoryOperation {
    /// Quad inserts and deletes recorded by this operation.
    pub fn changes(&self) -> &[MaterializedQuadChange] {
        match &self.event {
            Some(
                CraqleGraphEvent::QuadChanges { changes, .. }
                | CraqleGraphEvent::RoCrateMutation { changes, .. }
                | CraqleGraphEvent::Mutation { changes, .. }
                | CraqleGraphEvent::CommittedMutation { changes, .. },
            ) => changes,
            Some(CraqleGraphEvent::Policy { .. } | CraqleGraphEvent::GraphDeleted { .. })
            | None => &[],
        }
    }

    /// The commit metadata signed with this operation, if any.
    pub fn commit(&self) -> Option<&CommitInfo> {
        self.event.as_ref().and_then(CraqleGraphEvent::commit)
    }
}

/// Selects the graph content at `from` and at `to`; bounds apply to each side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryCompare {
    pub graph: GraphId,
    pub from: Vec<OpId>,
    pub to: Vec<OpId>,
    pub max_operations: usize,
    pub max_bytes: usize,
}

/// What turns the graph at `from` into the graph at `to`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryDiff {
    pub changes: Vec<MaterializedQuadChange>,
    /// The RO-Crate context and license at `to`, when they differ from `from`.
    pub hints: Option<GraphHints>,
}

/// Writes the graph content and RO-Crate hints at `heads` back as one new local mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRestore {
    pub graph: GraphId,
    pub heads: Vec<OpId>,
    /// Best effort, not compare-and-swap: the restore fails unchanged unless the current heads
    /// equal these, but Irokle can still admit a remote operation before the new one is signed.
    pub expected: Option<Vec<OpId>>,
    /// Repeating a restore with the same id returns its first result instead of writing again.
    pub id: Option<MutationId>,
    pub max_operations: usize,
    pub max_bytes: usize,
    /// Signed with the new operation, like a revert message.
    pub commit: Option<CommitInfo>,
}

/// The operation a restore wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRestored {
    pub id: MutationId,
    pub operation: OpId,
    pub parents: Vec<OpId>,
    /// The writing actor's previous operation, which Irokle always adds as a parent.
    pub previous: Option<OpId>,
}

impl HistoryRestored {
    /// Whether the parents are exactly `heads` plus `previous`, so no other operation joined.
    pub fn extends(&self, heads: &[OpId]) -> bool {
        let expected = heads.iter().chain(&self.previous).collect::<BTreeSet<_>>();
        self.parents.iter().collect::<BTreeSet<_>>() == expected
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HistoryError {
    #[error("history request needs heads and nonzero bounds")]
    InvalidRequest,
    #[error("history operation limit exceeded")]
    OperationLimit,
    #[error("history byte limit exceeded")]
    ByteLimit,
    #[error("history operation {0} is unavailable for this graph")]
    Unavailable(OpId),
    #[error("graph history heads changed")]
    HeadsChanged,
    #[error("history heads describe a deleted graph")]
    GraphDeleted,
    #[error("graph history heads are not yet applied to the store")]
    NotApplied,
}

impl HistoryError {
    pub(crate) fn kind(&self) -> CraqleErrorKind {
        match self {
            Self::InvalidRequest | Self::Unavailable(_) => CraqleErrorKind::InvalidInput,
            Self::OperationLimit | Self::ByteLimit => CraqleErrorKind::ResourceLimit,
            Self::HeadsChanged | Self::GraphDeleted | Self::NotApplied => CraqleErrorKind::Conflict,
        }
    }
}

pub(crate) struct TopicHistory {
    pub graph: GraphId,
    pub topic: irokle::TopicId,
    pub heads: Vec<OpId>,
    pub limit: usize,
    pub max_bytes: usize,
}

pub(crate) struct HistoryEntry {
    pub op: irokle::Op,
    pub record: Option<EventRecord<CraqleGraphEvent>>,
    pub rejected: bool,
}

type Quads = BTreeMap<(EncodedTerm, EncodedTerm, EncodedTerm), Vec<Dot>>;

/// Graph content replayed from history, with its last-writer-wins RO-Crate hints.
struct Content {
    quads: Quads,
    hints: CrateRenderHints,
}

impl CraqleNode {
    /// Current causal heads of a graph's history; requires graph READ permission.
    pub fn graph_heads(&self, auth: &dyn Authorizer, graph: &GraphId) -> Result<Vec<OpId>> {
        auth.authorize(graph, &self.history_policy(graph)?, Action::Read)?;
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        Ok(sync
            .topic_heads(self.history_topic(graph)?)?
            .into_iter()
            .collect())
    }

    /// Reads at most `limit` operations newest first; requires graph READ permission.
    /// More heads than `limit` or more than `max_bytes` fail the page instead of shortening it.
    pub fn history_log(&self, auth: &dyn Authorizer, request: &HistoryLog) -> Result<HistoryPage> {
        auth.authorize(
            &request.graph,
            &self.history_policy(&request.graph)?,
            Action::Read,
        )?;
        if request.limit == 0 || request.max_bytes == 0 {
            return Err(HistoryError::InvalidRequest.into());
        }
        let (entries, next) = self.history_page(&TopicHistory {
            graph: request.graph.clone(),
            topic: self.history_topic(&request.graph)?,
            heads: request.heads.clone(),
            limit: request.limit,
            max_bytes: request.max_bytes,
        })?;
        let operations = entries
            .into_iter()
            .map(|entry| {
                let body = entry.op.signed.body;
                HistoryOperation {
                    id: entry.op.id,
                    parents: body.deps.into_iter().collect(),
                    author: body.author,
                    actor: body.actor_id,
                    sequence: body.actor_seq,
                    generation: body.generation,
                    event: entry
                        .record
                        .filter(|_| !entry.rejected)
                        .map(|record| record.event),
                    rejected: entry.rejected,
                }
            })
            .collect();
        Ok(HistoryPage { operations, next })
    }

    /// Returns the quad and hint changes that turn the content at `from` into the one at `to`.
    /// Requires graph READ permission; a side that exceeds its bounds fails the call.
    pub fn compare_history(
        &self,
        auth: &dyn Authorizer,
        request: &HistoryCompare,
    ) -> Result<HistoryDiff> {
        auth.authorize(
            &request.graph,
            &self.history_policy(&request.graph)?,
            Action::Read,
        )?;
        let topic = self.history_topic(&request.graph)?;
        let side = |heads: &[OpId]| {
            self.history_content(&TopicHistory {
                graph: request.graph.clone(),
                topic,
                heads: heads.to_vec(),
                limit: request.max_operations,
                max_bytes: request.max_bytes,
            })
        };
        let from = side(&request.from)?;
        let to = side(&request.to)?;
        Ok(HistoryDiff {
            changes: content_changes(&request.graph, &from.quads, &to.quads),
            hints: (from.hints != to.hints).then_some(GraphHints {
                context: to.hints.context,
                license: to.hints.license,
                license_digest: to.hints.license_digest,
            }),
        })
    }

    /// Requires graph READ and WRITE permission. Writes the content at `heads` as one new
    /// validated local mutation, or returns `None` when nothing changes.
    pub fn restore_history(
        &self,
        auth: &dyn Authorizer,
        request: &HistoryRestore,
    ) -> Result<Option<HistoryRestored>> {
        let policy = self.history_policy(&request.graph)?;
        auth.authorize(&request.graph, &policy, Action::Read)?;
        auth.authorize(&request.graph, &policy, Action::Write)?;
        if let Some(Err(reason)) = request
            .commit
            .as_ref()
            .map(|commit| commit.check(&request.graph))
        {
            return Err(UpdateError::InvalidChangeSet(reason.to_owned()).into());
        }
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic = self.history_topic(&request.graph)?;
        if let Some(id) = request.id
            && let Some(receipt) = self.store.mutation_receipt(&id)?
        {
            if receipt.graph != request.graph {
                return Err(HistoryError::InvalidRequest.into());
            }
            return self.restored(topic, Some(receipt));
        }
        let target = self.history_content(&TopicHistory {
            graph: request.graph.clone(),
            topic,
            heads: request.heads.clone(),
            limit: request.max_operations,
            max_bytes: request.max_bytes,
        })?;
        let reconcile_guard = self
            .reconcile_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pass = self.reconcile_topic(sync, topic)?;
        if !pass.applied.is_empty() {
            self.persist_fjall()?;
        }
        if let Some(error) = pass.stalled {
            return Err(error);
        }
        let write_guard = self.store.graph_write_guard(&request.graph);
        // Reconcile may have applied a policy that removes the caller's access.
        let policy = self.history_policy(&request.graph)?;
        auth.authorize(&request.graph, &policy, Action::Read)?;
        auth.authorize(&request.graph, &policy, Action::Write)?;
        let heads = sync.topic_heads(topic)?.into_iter().collect::<Vec<_>>();
        if let Some(expected) = &request.expected
            && expected.iter().collect::<BTreeSet<_>>() != heads.iter().collect()
        {
            return Err(HistoryError::HeadsChanged.into());
        }
        // The diff must see every record the new operation's deletes will cover.
        self.ensure_applied(topic, &heads)?;
        let current = self
            .store
            .graph_snapshot(&request.graph)?
            .quads
            .into_iter()
            .map(|quad| ((quad.subject, quad.predicate, quad.object), quad.dots))
            .collect();
        let changes = content_changes(&request.graph, &current, &target.quads);
        let hints_changed = self.store.graph_context(&request.graph)? != target.hints.context
            || self.store.graph_license(&request.graph)?
                != target
                    .hints
                    .license
                    .clone()
                    .zip(target.hints.license_digest);
        if changes.is_empty() && !hints_changed {
            return Ok(None);
        }
        let id = request.id.unwrap_or_default();
        let batch = self.replication.apply_changes_locked(
            MutationRequest {
                id,
                admission_sequence: None,
                graph: request.graph.clone(),
                changes,
            },
            EventExtras {
                render_hints: Some(target.hints),
                commit: request.commit.clone(),
            },
        )?;
        drop(write_guard);
        drop(reconcile_guard);
        self.finish_batch(&request.graph, batch)?;
        self.restored(topic, self.store.mutation_receipt(&id)?)
    }

    fn restored(
        &self,
        topic: irokle::TopicId,
        receipt: Option<crate::MutationReceipt>,
    ) -> Result<Option<HistoryRestored>> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let Some((id, event)) = receipt.and_then(|receipt| Some((receipt.id, receipt.event_id?)))
        else {
            return Ok(None);
        };
        let operation = OpId::from_bytes(event);
        let body = sync
            .history_entry(topic, operation)?
            .ok_or(HistoryError::Unavailable(operation))?
            .op
            .signed
            .body;
        Ok(Some(HistoryRestored {
            id,
            operation,
            parents: body.deps.into_iter().collect(),
            previous: body.actor_prev,
        }))
    }

    /// Fails unless reconciliation has consumed every head, and so their causal past.
    fn ensure_applied(&self, topic: irokle::TopicId, heads: &[OpId]) -> Result<()> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let clock = match self.store.applied_topic_clock(topic.as_bytes())? {
            Some(bytes) => crate::sync::applied_clock(topic, &bytes)?,
            None => irokle::ActorClock::default(),
        };
        for head in heads {
            let entry = sync
                .history_entry(topic, *head)?
                .ok_or(HistoryError::Unavailable(*head))?;
            let body = &entry.op.signed.body;
            if clock.get(&body.actor_id) < body.actor_seq {
                return Err(HistoryError::NotApplied.into());
            }
        }
        Ok(())
    }

    /// Requires current graph READ permission and a destination that does not exist.
    /// Never publishes; a failure removes the destination it created and leaves no partial result.
    pub fn project_history(
        &self,
        auth: &dyn Authorizer,
        request: &GraphHistory,
    ) -> Result<HistoryProjection> {
        auth.authorize(
            &request.graph,
            &self.history_policy(&request.graph)?,
            Action::Read,
        )?;
        let entries = self.history_entries(&TopicHistory {
            graph: request.graph.clone(),
            topic: self.history_topic(&request.graph)?,
            heads: request.heads.clone(),
            limit: request.max_operations,
            max_bytes: request.max_bytes,
        })?;
        std::fs::create_dir(&request.directory)?;
        self.fill_projection(request, entries).inspect_err(|_| {
            if let Err(error) = std::fs::remove_dir_all(&request.directory) {
                tracing::warn!(%error, "could not remove a failed history projection");
            }
        })
    }

    fn fill_projection(
        &self,
        request: &GraphHistory,
        entries: Vec<HistoryEntry>,
    ) -> Result<HistoryProjection> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic = self.history_topic(&request.graph)?;
        let options = CraqleOptions::new()
            .with_actor(self.actor)
            .with_remote_policy_authorizer(self.remote_policy_authorizer.clone())
            .with_search_storage(SearchStorage::Memory)
            .with_memory_budget(projection_budget());
        let node = CraqleNode::open_with_options(&request.directory, options)?;
        let mut operations = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(record) = entry.record.filter(|_| !entry.rejected) {
                let _guard = node.store.graph_write_guard(record.event.graph());
                match node.apply_record_locked(&record, sync.is_local_record(topic, &record)) {
                    Err(error) if !error.rejects_record() => return Err(error),
                    Ok(_) | Err(_) => {}
                }
            }
            operations.push(entry.op);
        }
        node.persist_fjall()?;
        Ok(HistoryProjection { node, operations })
    }

    /// A deleted graph keeps the policy it had when it was deleted.
    fn history_policy(&self, graph: &GraphId) -> Result<GraphPolicy> {
        if !self.store.graph_tombstoned(graph)? {
            return Ok(self.store.graph_policy(graph)?);
        }
        Ok(self.store.deleted_graph_policy(graph)?.ok_or_else(|| {
            AuthorizationError::PermissionDenied {
                action: Action::Read,
                graph: graph.to_string(),
            }
        })?)
    }

    fn history_topic(&self, graph: &GraphId) -> Result<irokle::TopicId> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        Ok(sync
            .graph_topic_id(&self.store, graph)?
            .unwrap_or_else(|| crate::sync::graph_topic_id(graph)))
    }

    /// Walks newest first by generation, so no page repeats an operation of an earlier page.
    fn history_page(&self, query: &TopicHistory) -> Result<(Vec<HistoryEntry>, Vec<OpId>)> {
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let generation = |id: OpId| -> Result<(u64, OpId)> {
            let known = sync.history_generation(query.topic, id)?;
            Ok((known.ok_or(HistoryError::Unavailable(id))?, id))
        };
        let heads = query.heads.iter().collect::<BTreeSet<_>>();
        if heads.len() > query.limit {
            return Err(HistoryError::OperationLimit.into());
        }
        let mut pending = heads
            .into_iter()
            .map(|id| generation(*id))
            .collect::<Result<BinaryHeap<_>>>()?;
        let mut seen = BTreeSet::new();
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        while entries.len() < query.limit
            && let Some((level, id)) = pending.pop()
        {
            if !seen.insert(id) {
                continue;
            }
            let mut entry = sync
                .history_entry(query.topic, id)?
                .ok_or(HistoryError::Unavailable(id))?;
            if entry
                .record
                .as_ref()
                .is_some_and(|record| record.event.graph() != &query.graph)
            {
                entry.record = None;
                entry.rejected = true;
            }
            entry.rejected |= self
                .store
                .replication_rejection(&query.topic, &id)?
                .is_some();
            let body = &entry.op.signed.body;
            if body.generation != level {
                return Err(CraqleSyncError::InvalidEvent(
                    "history operation generation differs from its position".into(),
                )
                .into());
            }
            let encoded = postcard::to_allocvec(&entry.op)
                .map_err(|error| CraqleSyncError::InvalidEvent(error.to_string()))?;
            bytes = bytes.saturating_add(encoded.len());
            if bytes > query.max_bytes {
                return Err(HistoryError::ByteLimit.into());
            }
            for parent in body.deps.iter().filter(|parent| !seen.contains(*parent)) {
                let parent = generation(*parent)?;
                if parent.0 >= level {
                    return Err(CraqleSyncError::InvalidEvent(
                        "history parent is not older than its child".into(),
                    )
                    .into());
                }
                pending.push(parent);
            }
            entries.push(entry);
        }
        let next = pending
            .into_iter()
            .map(|(_, id)| id)
            .filter(|id| !seen.contains(id))
            .collect::<BTreeSet<_>>();
        Ok((entries, next.into_iter().collect()))
    }

    /// The complete causal past of the heads, oldest first, or an error.
    fn history_entries(&self, query: &TopicHistory) -> Result<Vec<HistoryEntry>> {
        if query.heads.is_empty() || query.limit == 0 || query.max_bytes == 0 {
            return Err(HistoryError::InvalidRequest.into());
        }
        let (mut entries, next) = self.history_page(query)?;
        if !next.is_empty() {
            return Err(HistoryError::OperationLimit.into());
        }
        entries.reverse();
        Ok(entries)
    }

    /// Replays the observed-remove quad set of the heads in memory.
    fn history_content(&self, query: &TopicHistory) -> Result<Content> {
        let mut quads = Quads::new();
        let mut hints: Option<TaggedRenderHints> = None;
        for entry in self.history_entries(query)? {
            let Some(record) = entry.record.filter(|_| !entry.rejected) else {
                continue;
            };
            if matches!(record.event, CraqleGraphEvent::GraphDeleted { .. }) {
                return Err(HistoryError::GraphDeleted.into());
            }
            let mutation = match crate::sync::batch_from_record(&record) {
                Ok(Some(mutation)) => mutation,
                Err(error) if !error.rejects_record() => return Err(error.into()),
                Ok(None) | Err(_) => continue,
            };
            if let Some(next) = mutation.render_hints
                && hints.as_ref().is_none_or(|current| next.tag > current.tag)
            {
                hints = Some(next);
            }
            for op in mutation.batch.ops {
                match op {
                    QuadOp::Add {
                        subject,
                        predicate,
                        object,
                        dot,
                    } => {
                        let dots = quads.entry(canonical_key(subject, predicate, object));
                        let dots = dots.or_default();
                        if !dots.contains(&dot) {
                            dots.push(dot);
                        }
                    }
                    QuadOp::Remove {
                        subject,
                        predicate,
                        object,
                        witnessed,
                    } => {
                        let key = canonical_key(subject, predicate, object);
                        if let Some(dots) = quads.get_mut(&key) {
                            dots.retain(|dot| !witnessed.contains(dot));
                            if dots.is_empty() {
                                quads.remove(&key);
                            }
                        }
                    }
                }
            }
        }
        let hints = hints.map_or(
            CrateRenderHints {
                context: None,
                license: None,
                license_digest: None,
            },
            |tagged| tagged.hints,
        );
        Ok(Content { quads, hints })
    }
}

/// Replays literal aliases onto the canonical quad, as replica apply does.
fn canonical_key(
    subject: EncodedTerm,
    predicate: EncodedTerm,
    object: EncodedTerm,
) -> (EncodedTerm, EncodedTerm, EncodedTerm) {
    let [subject, predicate, object] =
        [subject, predicate, object].map(|term| term.canonical().unwrap_or(term));
    (subject, predicate, object)
}

fn content_changes(graph: &GraphId, from: &Quads, to: &Quads) -> Vec<MaterializedQuadChange> {
    let removed = from.keys().filter(|key| !to.contains_key(*key)).map(|key| {
        MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: key.0.clone(),
            predicate: key.1.clone(),
            object: key.2.clone(),
        }
    });
    let added = to.keys().filter(|key| !from.contains_key(*key)).map(|key| {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: key.0.clone(),
            predicate: key.1.clone(),
            object: key.2.clone(),
        }
    });
    removed.chain(added).collect()
}

/// The smallest store reservation the memory budget accepts, so projections stay cheap.
fn projection_budget() -> MemoryBudget {
    let floor = MemoryBudget::new(0, 0);
    let bytes = floor
        .storage_bytes()
        .saturating_add(floor.application_bytes())
        .saturating_add(floor.search_writer_bytes())
        .saturating_add(floor.prepared_work_bytes());
    MemoryBudget::new(MemoryBudget::default().process_bytes(), bytes)
}
