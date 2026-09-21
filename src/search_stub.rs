//! Preserves search repair debt when the search feature is disabled.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[path = "internal/search/queue.rs"]
pub(crate) mod queue;

use std::path::Path;

use crate::core::GraphId;
pub(crate) use crate::search::queue::{DrainRequest, QueueBound};
use crate::store::GraphStore;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("search support is disabled; enable the `search` feature")]
    Disabled,
    #[error("search maintenance cancelled")]
    Cancelled,
    #[error("search request timeout expired")]
    Deadline,
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

impl SearchError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Disabled => crate::CraqleErrorKind::Unsupported,
            Self::Cancelled => crate::CraqleErrorKind::Cancelled,
            Self::Deadline => crate::CraqleErrorKind::QueryLimit,
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

pub(crate) struct AuthorizedQuery<'a> {
    pub query: &'a str,
    pub limit: usize,
    pub subject: Option<&'a str>,
    pub allows: &'a dyn Fn(&str) -> crate::Result<bool>,
}

pub(crate) struct FilterQuery<'a, E> {
    pub query: &'a str,
    pub limit: usize,
    pub subject: Option<&'a str>,
    pub allows: &'a dyn Fn(&str) -> std::result::Result<bool, E>,
    pub check: &'a dyn Fn() -> std::result::Result<(), E>,
}

pub(crate) fn stable_hit_key(graph_id: &str, subject_iri: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(graph_id.len() as u64).to_be_bytes());
    hasher.update(graph_id.as_bytes());
    hasher.update(&(subject_iri.len() as u64).to_be_bytes());
    hasher.update(subject_iri.as_bytes());
    *hasher.finalize().as_bytes()
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

    pub fn open_with_budget(
        _path: impl AsRef<Path>,
        _budget: crate::memory::MemoryBudget,
    ) -> Result<Self> {
        Ok(Self)
    }

    /// Parity with the real index; the stub has no drain to make panic.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn arm_drain_panic(&self) {}

    #[cfg(test)]
    pub(crate) fn take_drain_panic(&self) -> bool {
        false
    }

    pub fn open_in_memory() -> Result<Self> {
        Ok(Self)
    }

    pub fn memory_with_budget(_budget: crate::memory::MemoryBudget) -> Result<Self> {
        Ok(Self)
    }

    pub fn needs_rebuild(&self) -> bool {
        false
    }

    pub(crate) fn query_bytes(&self) -> usize {
        0
    }

    pub(crate) fn check_query_bytes(&self, _bytes: usize) -> crate::Result<()> {
        Err(SearchError::Disabled.into())
    }

    pub(crate) fn queue_repairs(&self, _store: &GraphStore) -> bool {
        false
    }

    pub(crate) fn retry_wait(&self, _retry_at_ms: u64) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    pub(crate) fn coverage_target(&self, store: &GraphStore) -> Result<Option<u64>> {
        Ok(Some(store.require_search_rebuild()?))
    }

    pub(crate) fn bind_store(&self, store: &GraphStore) -> Result<Option<u64>> {
        self.coverage_target(store)
    }

    pub(crate) fn complete_coverage(&self, _store: &GraphStore, _target: u64) -> Result<()> {
        Err(SearchError::Disabled)
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

    pub(crate) fn search_checked(
        &self,
        req: AuthorizedQuery<'_>,
        check: &dyn Fn() -> crate::Result<()>,
    ) -> crate::Result<Vec<SearchHit>> {
        let _ = (req.query, req.limit, req.subject, req.allows, check);
        Err(SearchError::Disabled.into())
    }

    pub(crate) fn collect_filtered<E>(
        &self,
        req: FilterQuery<'_, E>,
    ) -> std::result::Result<Vec<SearchHit>, E>
    where
        E: From<SearchError>,
    {
        let _ = (req.query, req.limit, req.subject, req.allows, req.check);
        Err(E::from(SearchError::Disabled))
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

    /// Retain coalesced queue debt for a later search-enabled build.
    pub fn process_queued_updates(&self, store: &GraphStore, bound: QueueBound) -> Result<usize> {
        Ok(self
            .drain_queues(
                store,
                DrainRequest {
                    bound,
                    control: crate::search::queue::DrainControl::default(),
                },
            )?
            .covered)
    }

    pub fn drain_queues(
        &self,
        _store: &GraphStore,
        _request: DrainRequest,
    ) -> Result<crate::search::queue::DrainProgress> {
        Ok(crate::search::queue::DrainProgress::default())
    }

    pub fn reindex_from_store(&self, _store: &GraphStore, _graph: &GraphId) -> Result<usize> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    /// A search-disabled build must retain every unindexed queue row.
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
        assert_eq!(
            node.flush_search_updates().unwrap_err().kind(),
            crate::CraqleErrorKind::Unsupported
        );

        let owed = node.store.drain_fts_queue(usize::MAX).unwrap();
        assert!(
            !owed.is_empty(),
            "the disabled build erased the search debt it could not index"
        );
    }
}
