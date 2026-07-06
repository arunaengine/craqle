use std::path::Path;

use crate::core::{EncodedTerm, GraphId};
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

#[derive(Debug, Default)]
pub struct SearchIndex;

impl SearchIndex {
    pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self)
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

    pub fn delete_resource(&self, _graph_id: &str, _subject_iri: &str) -> Result<()> {
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

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn process_queued_updates(&self, store: &GraphStore, limit: usize) -> Result<usize> {
        let queued_deletes = store.drain_fts_delete_queue(limit)?;
        if !queued_deletes.is_empty() {
            store.acknowledge_fts_queues_for_deleted_graphs(&queued_deletes)?;
            store.acknowledge_fts_delete_queue(&queued_deletes)?;
            return Ok(queued_deletes.len());
        }

        let queued_reindexes = store.drain_fts_reindex_queue(limit)?;
        if !queued_reindexes.is_empty() {
            store.acknowledge_fts_subjects_for_reindexed_graphs(&queued_reindexes)?;
            store.acknowledge_fts_reindex_queue(&queued_reindexes)?;
            return Ok(queued_reindexes.len());
        }

        let queued = store.drain_fts_queue(limit)?;
        store.acknowledge_fts_queue(&queued)?;
        Ok(queued.len())
    }

    pub fn sync_subject_from_store(
        &self,
        _store: &GraphStore,
        _graph: &GraphId,
        _subject: &EncodedTerm,
    ) -> Result<()> {
        Ok(())
    }

    pub fn reindex_from_store(&self, _store: &GraphStore, _graph: &GraphId) -> Result<usize> {
        Ok(0)
    }
}
