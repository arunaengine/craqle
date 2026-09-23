//! Bounded draining of the durable FTS queues.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::core::{EncodedTerm, GraphId};
use crate::store::{EncodedQuad, TermId};

pub(crate) const SEARCH_META_FORMAT: u16 = 3;

#[derive(
    Clone, Copy, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, serde::Deserialize, serde::Serialize,
)]
pub(crate) struct GenerationId(pub(crate) u64);

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct GraphGeneration {
    pub(crate) graph: GraphId,
    pub(crate) active: Option<GenerationId>,
    pub(crate) covered: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestScan {
    pub(crate) index_id: [u8; 16],
    pub(crate) after: Option<TermId>,
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestRow {
    pub(crate) graph: GraphId,
    pub(crate) active: Option<GenerationId>,
    pub(crate) wrong_index: bool,
    pub(crate) live: bool,
    pub(crate) source_owed: Option<u64>,
    pub(crate) delete_owed: Option<u64>,
    pub(crate) stage_target: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestPage {
    pub(crate) entries: Vec<ManifestRow>,
    pub(crate) next: Option<TermId>,
    pub(crate) remaining: bool,
    pub(crate) bytes: usize,
    pub(crate) oversized: Option<OversizedSource>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct StageJob {
    pub(crate) graph: GraphId,
    pub(crate) target: u64,
    pub(crate) generation: GenerationId,
    #[serde(with = "crate::core::quad_cursor")]
    pub(crate) cursor: Option<[u8; 64]>,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) complete: bool,
    pub(crate) session: [u8; 16],
}

#[derive(Clone, Debug)]
pub(crate) struct StageRequest {
    pub(crate) index_id: [u8; 16],
    pub(crate) graph: GraphId,
    pub(crate) target: u64,
    pub(crate) session: [u8; 16],
}

#[derive(Clone, Debug)]
pub(crate) struct GenerationRequest {
    pub(crate) index_id: [u8; 16],
    pub(crate) graph: GraphId,
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteGeneration {
    pub(crate) index_id: [u8; 16],
    pub(crate) graph: GraphId,
    pub(crate) covered: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct CleanupJob {
    pub(crate) graph: GraphId,
    pub(crate) generation: GenerationId,
}

#[derive(Clone, Debug)]
pub(crate) struct CleanupScan {
    pub(crate) after: Option<GenerationId>,
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OversizedCleanup {
    pub(crate) generation: GenerationId,
    pub(crate) bytes: usize,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CleanupPage {
    pub(crate) entries: Vec<CleanupJob>,
    pub(crate) next: Option<GenerationId>,
    pub(crate) remaining: bool,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) oversized: Option<OversizedCleanup>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct GenerationSwitch {
    pub(crate) graph: GraphId,
    pub(crate) previous: Option<GenerationId>,
    pub(crate) active: Option<GenerationId>,
    pub(crate) covered: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) enum QueueKind {
    Delete,
    Reindex,
    Subject,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct QueueId {
    pub(crate) kind: QueueKind,
    pub(crate) graph: GraphId,
    pub(crate) subject: Option<TermId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct QueueCursor {
    pub(crate) token: u64,
    pub(crate) kind: QueueKind,
    pub(crate) graph: TermId,
    pub(crate) subject: Option<TermId>,
}

#[derive(Clone, Debug)]
pub(crate) struct QueueScan {
    pub(crate) max_token: Option<u64>,
    pub(crate) after: Option<QueueCursor>,
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Debug)]
pub(crate) struct QueuePage<T> {
    pub(crate) entries: Vec<T>,
    pub(crate) next: Option<QueueCursor>,
    pub(crate) remaining: bool,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) oversized: Option<OversizedEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OversizedEntry {
    pub(crate) id: QueueId,
    pub(crate) owed_from: u64,
    pub(crate) target: u64,
    pub(crate) bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct RetryState {
    pub(crate) id: QueueId,
    pub(crate) owed_from: u64,
    pub(crate) target: u64,
    pub(crate) attempts: u32,
    pub(crate) retry_at_ms: u64,
    pub(crate) code: String,
    pub(crate) error_kind: crate::CraqleErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(crate) struct SearchCoverage {
    pub(crate) format: u16,
    pub(crate) index_id: [u8; 16],
    pub(crate) index_revision: u64,
    pub(crate) covered: u64,
    pub(crate) rebuild: Option<u64>,
    pub(crate) manifest_count: u64,
    pub(crate) manifest_hash: [u8; 32],
    pub(crate) manifest_epoch: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RebuildRequest {
    pub(crate) index_id: [u8; 16],
    pub(crate) index_revision: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RebuildScan {
    pub(crate) index_id: [u8; 16],
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct RebuildPage {
    pub(crate) remaining: bool,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) target: u64,
    pub(crate) oversized: Option<OversizedSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ManifestDigest {
    pub(crate) count: u64,
    pub(crate) hash: [u8; 32],
    pub(crate) epoch: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct StageFailure {
    pub(crate) index_id: [u8; 16],
    pub(crate) state: RetryState,
    pub(crate) job: Option<StageJob>,
}

#[derive(Clone, Debug)]
pub(crate) struct GraphScan {
    pub(crate) graph: TermId,
    pub(crate) after: Option<[u8; 64]>,
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Debug)]
pub(crate) struct QuadPage {
    pub(crate) entries: Vec<EncodedQuad>,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) oversized: Option<OversizedSource>,
}

#[derive(Clone, Debug)]
pub(crate) struct SubjectScan {
    pub(crate) graph: TermId,
    pub(crate) subject: TermId,
    pub(crate) after: Option<(TermId, TermId)>,
    pub(crate) row_limit: usize,
    pub(crate) byte_limit: usize,
}

#[derive(Debug)]
pub(crate) struct SubjectPage {
    pub(crate) entries: Vec<(EncodedTerm, EncodedTerm)>,
    pub(crate) next: Option<(TermId, TermId)>,
    pub(crate) remaining: bool,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) oversized: Option<OversizedSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OversizedSource {
    pub(crate) bytes: usize,
    pub(crate) limit: usize,
}

/// Bounds one drain pass over the durable FTS queues.
pub(crate) struct QueueBound {
    /// Maximum eligible entries one drain may hand back.
    pub(crate) chunk: usize,
    /// Highest eligible dirty token, or `None` for all queued work.
    pub(crate) max_token: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DrainControl {
    cancelled: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl DrainControl {
    pub(crate) fn with_stop(stop: Arc<AtomicBool>) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            stop,
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst) || self.stop.load(Ordering::SeqCst)
    }
}

pub(crate) struct DrainRequest {
    pub(crate) bound: QueueBound,
    pub(crate) control: DrainControl,
}

/// Oldest owed and latest coalesced dirty tokens for one queue entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirtyTokens {
    pub(crate) oldest: u64,
    pub(crate) latest: u64,
}

/// What one drain pass covered and what it still owes.
#[derive(Debug, Default)]
pub(crate) struct DrainProgress {
    /// Queue entries committed to the index and then acknowledged.
    pub(crate) covered: usize,
    /// Eligible work remains under the same bound and needs another pass.
    pub(crate) remaining: bool,
    /// Entries this pass could not index. They stay queued.
    pub(crate) failures: Vec<DrainFailure>,
    /// Token a recovery raised the flush target to, if one was scheduled.
    pub(crate) recovery: Option<u64>,
    /// Durable queue rows examined in this slice.
    pub(crate) rows_read: usize,
    /// Encoded queue bytes retained in this slice.
    pub(crate) queue_bytes: usize,
    /// Cleanup entries that exceeded this slice's byte budget.
    pub(crate) cleanup_failures: Vec<CleanupFailure>,
}

/// One queue entry that failed, with what is known about why.
#[derive(Debug, Clone)]
pub(crate) struct DrainFailure {
    pub(crate) kind: QueueKind,
    pub(crate) error_kind: crate::CraqleErrorKind,
    pub(crate) graph: GraphId,
    pub(crate) owed_from: u64,
    pub(crate) target: u64,
    pub(crate) attempts: u32,
    pub(crate) retry_at_ms: u64,
    pub(crate) code: String,
    pub(crate) diagnostic: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CleanupFailure {
    pub(crate) generation: GenerationId,
    pub(crate) bytes: usize,
    pub(crate) limit: usize,
}

/// A queued subject: one dirty entry in the per-`(graph, subject)` queue.
#[derive(Clone, Debug)]
pub(crate) struct DirtySubject {
    pub(crate) graph: GraphId,
    pub(crate) subject: TermId,
    pub(crate) tokens: DirtyTokens,
}

/// A queued whole-graph entry: a reindex or a search-delete.
#[derive(Clone, Debug)]
pub(crate) struct DirtyGraph {
    pub(crate) graph: GraphId,
    pub(crate) tokens: DirtyTokens,
}
