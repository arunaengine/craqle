//! Reads, compares, replays and restores a graph's signed causal history.

use std::collections::{BTreeSet, BinaryHeap};
use std::path::PathBuf;

use crate::{
    Action, AuthorizationError, Authorizer, CraqleErrorKind, CraqleGraphEvent, CraqleNode,
    CraqleOptions, CraqleSyncError, GraphId, GraphPolicy, MemoryBudget, Result, SearchStorage,
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
    #[error("history contains an operation for another graph")]
    ForeignGraph,
    #[error("graph history heads changed")]
    HeadsChanged,
    #[error("history heads describe a deleted graph")]
    GraphDeleted,
}

impl HistoryError {
    pub(crate) fn kind(&self) -> CraqleErrorKind {
        match self {
            Self::InvalidRequest | Self::Unavailable(_) => CraqleErrorKind::InvalidInput,
            Self::OperationLimit | Self::ByteLimit => CraqleErrorKind::ResourceLimit,
            Self::ForeignGraph => CraqleErrorKind::CorruptAuthoritativeData,
            Self::HeadsChanged | Self::GraphDeleted => CraqleErrorKind::Conflict,
        }
    }
}

pub(crate) struct TopicHistory {
    pub topic: irokle::TopicId,
    pub heads: Vec<OpId>,
    pub limit: usize,
    pub max_bytes: usize,
}

pub(crate) struct HistoryEntry {
    pub op: irokle::Op,
    pub record: Option<EventRecord<CraqleGraphEvent>>,
}

impl CraqleNode {
    /// Requires current graph READ permission and a destination that does not exist.
    /// Never publishes; incomplete history or exhausted bounds fail without a partial result.
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
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic = self.history_topic(&request.graph)?;
        let entries = self.history_entries(
            &request.graph,
            &TopicHistory {
                topic,
                heads: request.heads.clone(),
                limit: request.max_operations,
                max_bytes: request.max_bytes,
            },
        )?;
        std::fs::create_dir(&request.directory)?;
        let options = CraqleOptions::new()
            .with_actor(self.actor)
            .with_remote_policy_authorizer(self.remote_policy_authorizer.clone())
            .with_search_storage(SearchStorage::Memory)
            .with_memory_budget(projection_budget());
        let node = CraqleNode::open_with_options(&request.directory, options)?;
        let mut operations = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(record) = entry.record {
                let _guard = node.store.graph_write_guard(record.event.graph());
                node.apply_record_locked(&record, sync.is_local_record(topic, &record))?;
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
        let mut pending = query
            .heads
            .iter()
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
            let entry = sync
                .history_entry(query.topic, id)?
                .ok_or(HistoryError::Unavailable(id))?;
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
    fn history_entries(&self, graph: &GraphId, query: &TopicHistory) -> Result<Vec<HistoryEntry>> {
        if query.heads.is_empty() || query.limit == 0 || query.max_bytes == 0 {
            return Err(HistoryError::InvalidRequest.into());
        }
        let (mut entries, next) = self.history_page(query)?;
        if !next.is_empty() {
            return Err(HistoryError::OperationLimit.into());
        }
        if entries.iter().any(|entry| {
            entry
                .record
                .as_ref()
                .is_some_and(|record| record.event.graph() != graph)
        }) {
            return Err(HistoryError::ForeignGraph.into());
        }
        entries.reverse();
        Ok(entries)
    }
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
