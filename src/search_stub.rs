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
