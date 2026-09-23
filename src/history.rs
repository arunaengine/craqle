//! Replays a bounded causal history into a separate, unpublished graph store.

use std::collections::HashSet;
use std::path::PathBuf;

use crate::{
    Action, Authorizer, CraqleGraphEvent, CraqleNode, CraqleOptions, CraqleSyncError, GraphId,
    Result, SearchStorage,
};
use irokle::reducer::EventRecord;

/// Explicit heads and resource bounds for an isolated historical projection.
pub struct GraphHistory {
    pub graph: GraphId,
    pub heads: Vec<irokle::OpId>,
    pub directory: PathBuf,
    pub max_operations: usize,
    pub max_bytes: usize,
}

/// A disposable graph view and the signed operations from which it was built.
pub struct HistoryProjection {
    pub node: CraqleNode,
    pub operations: Vec<irokle::Op>,
}

pub(crate) struct TopicHistory {
    pub topic: irokle::TopicId,
    pub heads: Vec<irokle::OpId>,
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
        self.ensure_graph_action(&request.graph, auth, Action::Read)?;
        if request.heads.is_empty() || request.max_operations == 0 || request.max_bytes == 0 {
            return Err(CraqleSyncError::InvalidEvent(
                "explicit heads and nonzero bounds required".into(),
            )
            .into());
        }
        let sync = self.sync.as_ref().ok_or(CraqleSyncError::NotConfigured)?;
        let topic = sync
            .graph_topic_id(&self.store, &request.graph)?
            .unwrap_or_else(|| crate::sync::graph_topic_id(&request.graph));
        let entries = sync.selected_history(&TopicHistory {
            topic,
            heads: request.heads.clone(),
            limit: request.max_operations,
            max_bytes: request.max_bytes,
        })?;
        let mut seen = HashSet::new();
        for entry in &entries {
            let body = &entry.op.signed.body;
            if body
                .deps
                .iter()
                .chain(body.actor_prev.iter())
                .any(|id| !seen.contains(id))
                || entry
                    .record
                    .as_ref()
                    .is_some_and(|record| record.event.graph() != &request.graph)
            {
                return Err(CraqleSyncError::InvalidEvent(
                    "incomplete or foreign graph history".into(),
                )
                .into());
            }
            seen.insert(entry.op.id);
        }
        if request.heads.iter().any(|head| !seen.contains(head)) {
            return Err(CraqleSyncError::InvalidEvent("history head is unavailable".into()).into());
        }
        std::fs::create_dir(&request.directory)?;
        let options = CraqleOptions::new()
            .with_actor(self.actor)
            .with_remote_policy_authorizer(self.remote_policy_authorizer.clone())
            .with_search_storage(SearchStorage::Memory);
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
}
