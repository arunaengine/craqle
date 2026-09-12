use std::path::Path;

use crate::core::GraphId;
pub(crate) use crate::search_queue::QueueBound;
use crate::store::GraphStore;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("search support is disabled; enable the `search` feature")]
    Disabled,
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

impl SearchError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Disabled => crate::CraqleErrorKind::Unsupported,
            Self::Store(error) => error.kind(),
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, SearchError>;

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

#[derive(Debug, Default)]
pub struct SearchIndex;

impl SearchIndex {
    pub fn ensure_available(&self) -> Result<()> {
        Err(SearchError::Disabled)
    }

    pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self)
    }

    /// Test-only parity with the real index; the stub never indexes, so there
    /// is no drain cycle to make panic.
    /// Parity with the real index; the stub has no drain to make panic.
    #[cfg(test)]
    #[allow(dead_code)]
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
        Err(SearchError::Disabled)
    }

    pub fn search_in_graph(
        &self,
        _graph_id: &str,
        _query: &str,
        _limit: usize,
    ) -> Result<Vec<SearchHit>> {
        Err(SearchError::Disabled)
    }

    pub fn search_in_graphs(&self, _req: GraphSetQuery<'_>) -> Result<Vec<SearchHit>> {
        Err(SearchError::Disabled)
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    /// Retain every queued update without indexing it.
    ///
    /// This build has no index, so it cannot cover any queue entry. Draining
    /// and acknowledging them anyway destroyed the only durable record of what
    /// a later search-enabled build still owed: that build finds a
    /// schema-compatible index and an empty queue, certifies the index it
    /// wrote before the feature was turned off, and serves text the store no
    /// longer holds. The entries coalesce per `(graph, subject)`, so the
    /// retained debt is bounded by the corpus rather than by the write count.
    pub fn process_queued_updates(&self, _store: &GraphStore, _bound: QueueBound) -> Result<usize> {
        Ok(0)
    }

    pub fn reindex_from_store(&self, _store: &GraphStore, _graph: &GraphId) -> Result<usize> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    /// A build without an index must not acknowledge queue rows it never
    /// indexed: those rows are the only record of what a later search-enabled
    /// build still owes.
    #[test]
    fn drain_keeps_debt() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        node.create_crate(
            &crate::AllowAllAuthorizer,
            crate::CreateCrateRequest::new(
                crate::core::GraphId::new("urn:test:disabled-debt"),
                "Disabled Debt Crate",
                "needlebody",
                "2025-01-01",
                None,
                crate::core::GraphPolicy::default(),
            ),
        )
        .unwrap();
        node.flush_search_updates().unwrap();

        let owed = node.store.drain_fts_queue(usize::MAX).unwrap();
        assert!(
            !owed.is_empty(),
            "the disabled build erased the search debt it could not index"
        );
    }
}
