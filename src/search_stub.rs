use std::path::Path;

use crate::core::GraphId;
use crate::store::GraphStore;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("search support is disabled; enable the `search` feature")]
    Disabled,
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

pub type Result<T> = std::result::Result<T, SearchError>;

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub graph_id: String,
    pub subject_iri: String,
    pub score: f32,
}

/// Mirrors `search::GraphSetQuery`.
pub struct GraphSetQuery<'a> {
    pub graphs: &'a [GraphId],
    pub query: &'a str,
    pub limit: usize,
}

/// Mirrors `search::QueueBound`.
pub struct QueueBound {
    pub chunk: usize,
    pub max_token: Option<u64>,
}

#[derive(Debug, Default)]
pub struct SearchIndex;

impl SearchIndex {
    pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self)
    }

    /// Test-only parity with the real index; the stub never indexes, so there
    /// is no drain cycle to make panic.
    #[cfg(test)]
    pub(crate) fn arm_drain_panic(&self) {}

    #[cfg(test)]
    pub(crate) fn take_armed_drain_panic(&self) -> bool {
        false
    }

    pub fn open_in_memory() -> Result<Self> {
        Ok(Self)
    }

    pub fn needs_rebuild(&self) -> bool {
        false
    }

    pub fn index_resource(
        &self,
        _graph_id: &str,
        _subject_iri: &str,
        _all_text: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    pub fn search(&self, _query: &str, _limit: usize) -> Result<Vec<SearchHit>> {
        Ok(Vec::new())
    }

    pub fn search_in_graph(
        &self,
        _graph_id: &str,
        _query: &str,
        _limit: usize,
    ) -> Result<Vec<SearchHit>> {
        Ok(Vec::new())
    }

    pub fn search_in_graphs(&self, _req: GraphSetQuery<'_>) -> Result<Vec<SearchHit>> {
        Ok(Vec::new())
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    /// Drain and acknowledge without indexing. The token bound is honoured so
    /// the caller's flush contract behaves the same with the feature off.
    pub fn process_queued_updates(&self, store: &GraphStore, bound: QueueBound) -> Result<usize> {
        let queued_deletes =
            retain_upto(store.drain_fts_delete_queue(bound.chunk)?, &bound, |e| e.1);
        if !queued_deletes.is_empty() {
            store.acknowledge_fts_queues_for_deleted_graphs(&queued_deletes)?;
            store.acknowledge_fts_delete_queue(&queued_deletes)?;
            return Ok(queued_deletes.len());
        }

        let queued_reindexes =
            retain_upto(store.drain_fts_reindex_queue(bound.chunk)?, &bound, |e| e.1);
        if !queued_reindexes.is_empty() {
            store.acknowledge_fts_subjects_for_reindexed_graphs(&queued_reindexes)?;
            store.acknowledge_fts_reindex_queue(&queued_reindexes)?;
            return Ok(queued_reindexes.len());
        }

        let queued = retain_upto(store.drain_fts_queue(bound.chunk)?, &bound, |e| e.2);
        store.acknowledge_fts_queue(&queued)?;
        Ok(queued.len())
    }

    pub fn reindex_from_store(&self, _store: &GraphStore, _graph: &GraphId) -> Result<usize> {
        Ok(0)
    }
}

fn retain_upto<T>(entries: Vec<T>, bound: &QueueBound, token: impl Fn(&T) -> u64) -> Vec<T> {
    let Some(max_token) = bound.max_token else {
        return entries;
    };
    entries
        .into_iter()
        .filter(|entry| token(entry) <= max_token)
        .collect()
}
