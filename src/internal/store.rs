//! Persists graph state, query indexes, and durable maintenance queues.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::{Cell, OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard as ReadGuard,
    RwLockWriteGuard as WriteGuard, TryLockError,
};
use std::time::Duration;
#[cfg(feature = "shacl-core")]
use std::time::Instant;

use crate::cache::BoundedCache;
#[cfg(test)]
use crate::cache::CacheStatistics;
use crate::core::{
    ActorId, Batch, ContextTag, Dot, EncodedTerm, GraphDiagnostics, GraphId, GraphPolicy,
    GraphReplicaSnapshot, GraphTombstone, PolicyTag, SnapshotQuadState, TaggedGraphPolicy,
    TaggedRenderHints, VectorClock,
};
#[cfg(test)]
use crate::core::{CrateRenderHints as RenderHints, EventId};
use crate::memory::{MemoryBudget, MemoryLease};
use crate::qv_gate::QvCommitGate;
use crate::search::queue::{
    CleanupJob, CleanupPage, CleanupScan, DeleteGeneration, DirtyGraph, DirtySubject, DirtyTokens,
    GenerationId, GenerationRequest, GenerationSwitch, GraphGeneration, GraphScan, ManifestDigest,
    ManifestPage, ManifestRow, ManifestScan, OversizedCleanup, OversizedEntry, OversizedSource,
    QuadPage, QueueCursor, QueueId, QueueKind, QueuePage, QueueScan, RebuildPage, RebuildRequest,
    RebuildScan, RetryState, SEARCH_META_FORMAT, SearchCoverage, StageFailure, StageJob,
    StageRequest, SubjectPage, SubjectScan,
};
use crate::sync::{
    BackupProof, MutationId, MutationLookup, MutationReceipt, MutationStatus, RepairAudit,
    RepairMode, RepairOutcome, RepairResult, SourceOutcome,
};
use crate::{
    CraqleErrorKind, DISK_FORMAT_VERSION, DiskFormatVersion, QueryIndexState, QueryIndexStatus,
    QueryIndexVerification, QueryIndexVerificationMode as IndexVerifyMode,
};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, Snapshot,
    compaction::Leveled, config::CompressionPolicy,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("query cancelled")]
    Cancelled,
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("memory budget: {0}")]
    Memory(#[from] crate::memory::MemoryBudgetError),
    #[error("term not found: {0:032x}")]
    TermNotFound(u128),
    #[error("term hash collision for `{attempted}` against existing `{existing}`")]
    TermCollision { attempted: String, existing: String },
    #[error("graph not found: {0}")]
    GraphNotFound(String),
    #[error("invalid stored encoding for {context}: {message}")]
    InvalidEncoding {
        context: &'static str,
        message: String,
    },
    #[error("query index verification failed: {0}")]
    IndexVerificationFailed(&'static str),
    #[error("invalid query-index encoding for {context}: {message}")]
    InvalidIndexEncoding {
        context: &'static str,
        message: String,
    },
    #[error("query index unavailable: {0}")]
    QueryIndexUnavailable(&'static str),
    #[error("query-index maintenance is busy; the write was not applied")]
    QueryIndexBusy,
    #[error("query-view delta capacity is exhausted; the write was not applied")]
    QueryIndexCapacity,
    #[error("{resource} limit {limit} exceeded by {actual}")]
    LimitExceeded {
        resource: &'static str,
        limit: u64,
        actual: u64,
    },
    #[error("invalid search-derived state: {0}")]
    InvalidSearchState(&'static str),
    #[error("unsupported search metadata format {found}; this release supports {supported}")]
    UnsupportedSearchFormat { found: u16, supported: u16 },
    #[error("unsupported query-view format {found}; this release supports {supported}")]
    UnsupportedIndexFormat { found: u32, supported: u32 },
    #[error("mutation identity was reused with different request content")]
    ReceiptConflict,
    #[error("authoritative disk-format marker is missing from a non-empty store")]
    MissingAuthoritativeFormat,
    #[error("invalid authoritative disk-format marker")]
    InvalidAuthoritativeFormat,
    #[error(
        "unsupported authoritative disk format {found_major}.{found_minor}; this release supports {supported_major}.{supported_minor}"
    )]
    UnsupportedAuthoritativeFormat {
        found_major: u16,
        found_minor: u16,
        supported_major: u16,
        supported_minor: u16,
    },
    #[error("replication cursor changed before repair")]
    CursorCompareFailed,
}

impl StoreError {
    pub(crate) fn kind(&self) -> CraqleErrorKind {
        match self {
            Self::Cancelled => CraqleErrorKind::Cancelled,
            Self::TermCollision { .. }
            | Self::CursorCompareFailed
            | Self::QueryIndexBusy
            | Self::QueryIndexCapacity
            | Self::ReceiptConflict => CraqleErrorKind::Conflict,
            Self::GraphNotFound(_) | Self::LimitExceeded { .. } => CraqleErrorKind::InvalidInput,
            Self::IndexVerificationFailed(_)
            | Self::InvalidIndexEncoding { .. }
            | Self::QueryIndexUnavailable(_)
            | Self::InvalidSearchState(_) => CraqleErrorKind::CorruptDerivedData,
            Self::TermNotFound(_)
            | Self::InvalidEncoding { .. }
            | Self::MissingAuthoritativeFormat
            | Self::InvalidAuthoritativeFormat => CraqleErrorKind::CorruptAuthoritativeData,
            Self::UnsupportedAuthoritativeFormat { .. }
            | Self::UnsupportedSearchFormat { .. }
            | Self::UnsupportedIndexFormat { .. } => CraqleErrorKind::Unsupported,
            Self::Fjall(_) | Self::Postcard(_) | Self::Memory(_) => CraqleErrorKind::Storage,
        }
    }

    /// Whether the bytes offered are what failed, rather than the storage layer
    /// underneath them. A retry can only ever reproduce these.
    pub fn rejects_record(&self) -> bool {
        matches!(
            self,
            Self::TermCollision { .. } | Self::InvalidEncoding { .. }
        )
    }
}

pub(crate) type Result<T> = std::result::Result<T, StoreError>;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TermId(pub u128);

impl TermId {
    pub fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }
}

/// Dense identifier used only by rebuildable query-derived state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct QueryTermId(pub(crate) u64);

impl QueryTermId {
    fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EncodedQuad {
    pub graph: TermId,
    pub subject: TermId,
    pub predicate: TermId,
    pub object: TermId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryQuad {
    pub(crate) graph: QueryTermId,
    pub(crate) subject: QueryTermId,
    pub(crate) predicate: QueryTermId,
    pub(crate) object: QueryTermId,
}

const DOT_ENCODING_TAG: u8 = b'D';
const GRAPH_META_PREFIX: u8 = b'M';
const GRAPH_DIRTY_PREFIX: u8 = b'D';
const GRAPH_REINDEX_PREFIX: u8 = b'R';
const GRAPH_DELETE_PREFIX: u8 = b'X';
const SEARCH_ORDER_PREFIX: u8 = b'O';
const SEARCH_FAILURE_PREFIX: u8 = b'F';
const SEARCH_GENERATION_PREFIX: u8 = b'G';
const SEARCH_STAGE_PREFIX: u8 = b'S';
const SEARCH_CLEANUP_PREFIX: u8 = b'L';
const SEARCH_NEXT_KEY: &[u8] = b"N";
const SEARCH_COVERAGE_KEY: &[u8] = b"C";
const SEARCH_HEAD_KEY: &[u8] = b"H";
const SEARCH_SCHEMA_KEY: &[u8] = b"V";
const SEARCH_MANIFEST_KEY: &[u8] = b"M";
const SEARCH_REBUILD_KEY: &[u8] = b"E";
const SEARCH_COVERAGE_MAGIC: [u8; 2] = *b"SC";
const SEARCH_GENERATION_MAGIC: [u8; 2] = *b"SG";
const SEARCH_STAGE_MAGIC: [u8; 2] = *b"SS";
const SEARCH_ORDER_KEY: &[u8] = b"Q";
const SEARCH_ORDER_FORMAT: u16 = 1;
const RECEIPT_RETENTION: usize = 4_096;
const RECEIPT_EXPIRED_KEY: &[u8] = b"\0receipt-expired";
const BATCH_RECEIPT_PREFIX: [u8; 2] = [0, b'B'];
const BATCH_REVERSE_PREFIX: [u8; 2] = [0, b'R'];
const BATCH_ORDER_PREFIX: [u8; 2] = [0, b'M'];
const LOG_HEAD_PREFIX: u8 = b'H';
const LOG_BATCH_PREFIX: u8 = b'B';
const TOPIC_CLOCK_PREFIX: u8 = b'C';
const TOPIC_BINDING_PREFIX: u8 = b'T';
const GRAPH_TOMBSTONE_PREFIX: u8 = b'Z';
const DELETED_POLICY_PREFIX: u8 = b'Y';
const REPLICATION_REJECTION_PREFIX: u8 = b'J';
const CURSOR_AUDIT_PREFIX: u8 = b'A';
/// Per-graph vector clock, split out of the graph meta record so a
/// commit writes only the clock and never rewrites policy/context/topic bytes.
const GRAPH_CLOCK_PREFIX: u8 = b'K';
/// Persisted, clock-tagged graph diagnostics.
const GRAPH_DIAGNOSTICS_PREFIX: u8 = b'O';
const DISK_FORMAT_KEY: &[u8] = b"\0craqle-authoritative-format";
const LITERAL_ALIAS_KEY: &[u8] = b"\0craqle-literal-aliases";
#[cfg(feature = "shacl-core")]
const SHACL_BINDING_PREFIX: u8 = b'S';
#[cfg(feature = "shacl-core")]
const SHACL_REVERSE_PREFIX: u8 = b's';
/// Queued active SHACL data graphs awaiting settlement.
#[cfg(feature = "shacl-core")]
const SHACL_PENDING_PREFIX: u8 = b'V';
#[cfg(feature = "shacl-core")]
const SHACL_QUEUE_KEY: &[u8] = b"vshacl-pending-queue";
#[cfg(feature = "shacl-core")]
const SHACL_QUEUE_VERSION: u8 = 1;
const TERM_LOCK_SHARDS: usize = 64;
const COMMIT_LOCK_SHARDS: usize = 64;
const GRAPH_LOCK_SHARDS: usize = 32;
const INDEX_EPOCH_SHARDS: usize = 256;
/// Maximum wait for query-view maintenance ownership before reporting busy.
const QV_COMMIT_WAIT: Duration = Duration::from_secs(120);
const TERM_CACHE_CAP: usize = 1_000_000;
/// Graph records one read snapshot may hold after a range prefetch.
const PREFETCH_RECORD_BYTES: usize = 16 * 1_048_576;
const SOURCE_CACHE_CAP: usize = 1_000_000;
const TERM_CACHE_BYTES: usize = 128 * 1_048_576;
const SUBJECT_CACHE_CAP: usize = 65_536;
const SUBJECT_CACHE_BYTES: usize = 64 * 1_048_576;
const ORDER_CACHE_CAP: usize = 4_096;
const ORDER_CACHE_BYTES: usize = 64 * 1_048_576;
const PLANNER_CACHE_CAP: usize = 4_096;
const PLANNER_SAMPLE_ROWS: usize = 4_096;
const PLANNER_CACHE_BYTES: usize = 1_048_576;
const FTS_REINDEX_THRESHOLD: usize = 10_000;
/// Used only when no memory ceiling can be established.
#[cfg(test)]
const DEFAULT_DB_BYTES: u64 = 1_024 * 1_024 * 1_024;
#[cfg(test)]
const MAX_DB_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
/// Smallest useful block cache. Applied instead of the former 1 GiB floor, which
/// exceeded the whole budget of a small container.
#[cfg(test)]
const MIN_DB_BYTES: u64 = 16 * 1_048_576;
/// Share of the process budget for the storage block cache, and again for all
/// application caches together.
#[cfg(test)]
const CACHE_BUDGET_SHARE: u64 = 8;
/// Control groups report an unlimited controller as a very large number rather
/// than as `max`, so anything at or above this is treated as no ceiling.
#[cfg(test)]
const CGROUP_UNLIMITED_BYTES: u64 = 1 << 62;
/// Floor for one application cache, so a tight budget still caches something.
const MIN_APP_BYTES: usize = 262_144;
/// Memtable ceiling that makes append-heavy keyspaces flush and reclaim journals.
const WRITE_MEMTABLE_BYTES: u64 = 64 * 1_024 * 1_024;
/// Smaller point-read memtables prevent quiet keyspaces from pinning journals.
const READ_MEMTABLE_BYTES: u64 = 32 * 1_024 * 1_024;
/// Retained journal ceiling that bounds crash-recovery replay to one rotation.
const MAX_JOURNALING_BYTES: u64 = 64 * 1_024 * 1_024;
const WRITE_TABLE_BYTES: u64 = 256 * 1_024 * 1_024;
const WRITE_LEVEL_LIMIT: u8 = 12;
const WRITE_LEVEL_RATIO: f32 = 20.0;

fn encode_disk_format(version: DiskFormatVersion) -> [u8; 4] {
    let mut bytes = [0u8; 4];
    bytes[..2].copy_from_slice(&version.major.to_be_bytes());
    bytes[2..].copy_from_slice(&version.minor.to_be_bytes());
    bytes
}

fn decode_disk_format(bytes: &[u8]) -> Result<DiskFormatVersion> {
    if bytes.len() != 4 {
        return Err(StoreError::InvalidAuthoritativeFormat);
    }
    Ok(DiskFormatVersion {
        major: u16::from_be_bytes(bytes[..2].try_into().unwrap()),
        minor: u16::from_be_bytes(bytes[2..].try_into().unwrap()),
    })
}

/// Source terms of dense IDs by query-ID generation, shared by all readers of one store.
pub(crate) type SourceCache = Arc<RwLock<BoundedCache<(u64, QueryTermId), TermId>>>;

/// Per-store cache ceilings derived from the admitted process memory budget.
#[derive(Clone, Copy)]
struct CacheBudget {
    database: u64,
    terms: usize,
    subjects: usize,
    objects: usize,
    planner: usize,
    shacl: usize,
}

struct OpenMemory {
    budget: MemoryBudget,
    lease: MemoryLease,
}

impl CacheBudget {
    #[cfg(test)]
    fn from_limit(limit: Option<u64>) -> Self {
        let Some(limit) = limit else {
            return Self {
                database: DEFAULT_DB_BYTES,
                terms: TERM_CACHE_BYTES,
                subjects: SUBJECT_CACHE_BYTES,
                objects: ORDER_CACHE_BYTES,
                planner: PLANNER_CACHE_BYTES,
                shacl: 0,
            };
        };
        let allowed = limit / CACHE_BUDGET_SHARE;
        let total =
            (TERM_CACHE_BYTES + SUBJECT_CACHE_BYTES + ORDER_CACHE_BYTES + PLANNER_CACHE_BYTES)
                as u64;
        Self {
            database: (limit / CACHE_BUDGET_SHARE).clamp(MIN_DB_BYTES, MAX_DB_BYTES),
            terms: scaled_ceiling(TERM_CACHE_BYTES, allowed, total),
            subjects: scaled_ceiling(SUBJECT_CACHE_BYTES, allowed, total),
            objects: scaled_ceiling(ORDER_CACHE_BYTES, allowed, total),
            planner: scaled_ceiling(PLANNER_CACHE_BYTES, allowed, total),
            shacl: 0,
        }
    }

    fn from_budget(budget: MemoryBudget) -> Self {
        let total =
            (TERM_CACHE_BYTES + SUBJECT_CACHE_BYTES + ORDER_CACHE_BYTES + PLANNER_CACHE_BYTES)
                as u64;
        let allowed = budget.application_bytes();
        let shacl = if cfg!(feature = "shacl-core") {
            usize::try_from(allowed / 4).unwrap_or(usize::MAX)
        } else {
            0
        };
        let cache_allowed = allowed.saturating_sub(shacl as u64);
        Self {
            database: budget.storage_bytes(),
            terms: scaled_ceiling(TERM_CACHE_BYTES, cache_allowed, total),
            subjects: scaled_ceiling(SUBJECT_CACHE_BYTES, cache_allowed, total),
            objects: scaled_ceiling(ORDER_CACHE_BYTES, cache_allowed, total),
            planner: scaled_ceiling(PLANNER_CACHE_BYTES, cache_allowed, total),
            shacl,
        }
    }
}

fn scaled_ceiling(bytes: usize, allowed: u64, total: u64) -> usize {
    if allowed >= total || total == 0 {
        return bytes;
    }
    let scaled = (bytes as u64).saturating_mul(allowed) / total;
    usize::try_from(scaled).unwrap_or(bytes).max(MIN_APP_BYTES)
}

#[cfg(test)]
fn meminfo_available(path: &Path) -> Option<u64> {
    let meminfo = std::fs::read_to_string(path).ok()?;
    meminfo.lines().find_map(|line| {
        let value = line.strip_prefix("MemAvailable:")?.trim();
        let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
        kib.checked_mul(1024)
    })
}

/// Returns the smallest applicable cgroup v1 or v2 memory ceiling.
#[cfg(test)]
fn cgroup_memory_limit(mapping: &Path, root: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(mapping).ok()?;
    let mut limit: Option<u64> = None;
    for line in text.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let found = if hierarchy == "0" && controllers.is_empty() {
            smallest_limit(root, Path::new(path), "memory.max")
        } else if controllers.split(',').any(|name| name == "memory") {
            smallest_limit(
                &root.join("memory"),
                Path::new(path),
                "memory.limit_in_bytes",
            )
        } else {
            continue;
        };
        if let Some(value) = found {
            limit = Some(limit.map_or(value, |current: u64| current.min(value)));
        }
    }
    limit
}

/// Reads `file` in `root` and in each directory along `relative`, keeping the
/// smallest ceiling found, because an ancestor's limit also binds this process.
#[cfg(test)]
fn smallest_limit(root: &Path, relative: &Path, file: &str) -> Option<u64> {
    let mut limit = numeric_limit(&root.join(file));
    let mut directory = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        directory.push(name);
        if let Some(value) = numeric_limit(&directory.join(file)) {
            limit = Some(limit.map_or(value, |found: u64| found.min(value)));
        }
    }
    limit
}

/// `max`, an absent file, a denied read, and a malformed value all mean "no
/// ceiling here". Version 1 reports an unlimited controller as a huge sentinel.
#[cfg(test)]
fn numeric_limit(path: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    let value = text.trim().parse::<u64>().ok()?;
    (value < CGROUP_UNLIMITED_BYTES).then_some(value)
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct StoredGraphMeta {
    policy: GraphPolicy,
    #[serde(default)]
    policy_tag: PolicyTag,
    /// Legacy clock retained only as the fallback before the separate clock key exists.
    clock: VectorClock,
    #[serde(default)]
    irokle_topic: Option<[u8; 32]>,
    /// Verbatim imported RO-Crate context, or `None` for the default context.
    #[serde(default)]
    rocrate_context: Option<String>,
    /// Verbatim root license JSON retained for export fidelity.
    #[serde(default)]
    rocrate_license: Option<String>,
    #[serde(default)]
    rocrate_license_digest: Option<[u8; 32]>,
    /// Last-write-wins ordering tag for the stored RO-Crate render hints.
    /// See [`ContextTag`].
    #[serde(default)]
    context_tag: ContextTag,
}

#[derive(Debug, Clone)]
enum QuadMutation {
    Insert(EncodedQuad),
    Remove(EncodedQuad),
}

/// Quad key bytes: `graph || subject || predicate || object`, 4 × 16 bytes.
type QuadKey = [u8; 64];
type QueryQuadKey = [u8; 32];

const QV_SCHEMA_VERSION: u32 = 5;
/// Version 2 headers predate atomic source-plus-query-view publication, so a
/// `Ready` state from one is not evidence of coverage until it is verified.
const QV_LEGACY_VERSION: u32 = 2;
const QV_ATOMIC_VERSION: u32 = 3;
const QV_HEADER_KEY: [u8; 1] = *b"H";
const PRIMARY_TERM_MAP: &str = "qv2_term_to_query";
const PRIMARY_QUERY_MAP: &str = "qv2_query_to_term";
const SECONDARY_TERM_MAP: &str = "qv3_term_to_query";
const SECONDARY_QUERY_MAP: &str = "qv3_query_to_term";
const QV_TOTAL_KEY: [u8; 1] = *b"T";
const QV_HEADER_MAGIC: [u8; 4] = *b"QVI2";
const QV_HEADER_LEN: usize = 71;
const QV_LEGACY_LEN: usize = 70;
const QV_FAILURE_BYTES: usize = 256;
const QV_BUILD_ROWS: usize = 1_024;
const QV_SAMPLE_ROWS: u64 = 128;
const QV_PROBLEM_LIMIT: usize = 32;

const QV_GRAPH_TAG: u8 = b'G';
const QV_PREDICATE_TAG: u8 = b'P';
const QV_VERSION_TAG: u8 = b'V';
const QV_GP_TAG: u8 = b'A';
const QV_PO_TAG: u8 = b'O';
const QV_GPO_TAG: u8 = b'X';
const QV_UNION_TAG: u8 = b'U';
const QV_DEBT_TAG: u8 = b'W';
const QV_BUILD_KEY: &[u8] = b"B";
const QV_CLEANUP_KEY: &[u8] = b"C";
const QV_DELTA_TAG: u8 = b'D';
const QV_DELTA_ROWS: u64 = 65_536;
const QV_DELTA_BYTES: u64 = 64 * 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
enum StoredIndexState {
    Building,
    Ready,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexHeader {
    active_slot: u8,
    state: StoredIndexState,
    source_epoch: u64,
    index_epoch: u64,
    source_live_quads: u64,
    indexed_quads: u64,
    last_build_sequence: u64,
    query_id_generation: u64,
    next_query_id: u64,
}

impl IndexHeader {
    fn empty_ready() -> Self {
        Self {
            active_slot: 0,
            state: StoredIndexState::Ready,
            source_epoch: 0,
            index_epoch: 0,
            source_live_quads: 0,
            indexed_quads: 0,
            last_build_sequence: 0,
            query_id_generation: 1,
            next_query_id: 0,
        }
    }

    fn failed_from(previous: Option<&Self>, reason: &'static str) -> Self {
        let mut header = previous.cloned().unwrap_or_else(Self::empty_ready);
        header.state = StoredIndexState::Failed(reason.to_owned());
        header
    }

    fn state(&self) -> QueryIndexState {
        match &self.state {
            StoredIndexState::Building => QueryIndexState::Building,
            StoredIndexState::Ready => QueryIndexState::Ready,
            StoredIndexState::Failed(reason) => QueryIndexState::Failed(reason.clone()),
        }
    }

    fn ready_is_coherent(&self) -> bool {
        matches!(self.state, StoredIndexState::Ready)
            && self.source_epoch == self.index_epoch
            && self.source_live_quads == self.indexed_quads
            && self.query_id_generation != 0
    }

    fn fits_snapshot(&self, snapshot_sequence: u64) -> bool {
        self.source_epoch <= snapshot_sequence
            && self.index_epoch <= snapshot_sequence
            && self.last_build_sequence <= snapshot_sequence
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexSlot {
    Primary,
    Secondary,
}

impl IndexSlot {
    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Primary),
            1 => Some(Self::Secondary),
            _ => None,
        }
    }

    fn encode(self) -> u8 {
        match self {
            Self::Primary => 0,
            Self::Secondary => 1,
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Primary => Self::Secondary,
            Self::Secondary => Self::Primary,
        }
    }
}

#[derive(Clone, Copy)]
struct IndexSpaces<'a> {
    gspo: &'a Keyspace,
    gpos: &'a Keyspace,
    spog: &'a Keyspace,
    posg: &'a Keyspace,
    ospg: &'a Keyspace,
    gosp: &'a Keyspace,
    term_to_query: &'a Keyspace,
    query_to_term: &'a Keyspace,
    meta: &'a Keyspace,
}

struct TermResolver<'a> {
    snapshot: &'a Snapshot,
    spaces: IndexSpaces<'a>,
    allow_allocate: bool,
    resolved: HashMap<TermId, QueryTermId>,
    mappings: Vec<(TermId, QueryTermId)>,
    next_query_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexCounterKey {
    Total,
    UnionDuplicateFree,
    Graph(QueryTermId),
    Predicate(QueryTermId),
    GraphPredicate(QueryTermId, QueryTermId),
    PredicateObject(QueryTermId, QueryTermId),
    GraphPredicateObject(QueryTermId, QueryTermId, QueryTermId),
}

impl IndexCounterKey {
    fn bytes(self) -> Vec<u8> {
        let mut key = match self {
            Self::Total => return QV_TOTAL_KEY.to_vec(),
            Self::UnionDuplicateFree => return vec![QV_UNION_TAG],
            Self::Graph(_) | Self::Predicate(_) => vec![0; 9],
            Self::GraphPredicate(_, _) | Self::PredicateObject(_, _) => vec![0; 17],
            Self::GraphPredicateObject(_, _, _) => vec![0; 25],
        };
        match self {
            Self::Graph(graph) => {
                key[0] = QV_GRAPH_TAG;
                key[1..9].copy_from_slice(&graph.to_be_bytes());
            }
            Self::Predicate(predicate) => {
                key[0] = QV_PREDICATE_TAG;
                key[1..9].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::GraphPredicate(graph, predicate) => {
                key[0] = QV_GP_TAG;
                key[1..9].copy_from_slice(&graph.to_be_bytes());
                key[9..17].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::PredicateObject(predicate, object) => {
                key[0] = QV_PO_TAG;
                key[1..9].copy_from_slice(&predicate.to_be_bytes());
                key[9..17].copy_from_slice(&object.to_be_bytes());
            }
            Self::GraphPredicateObject(graph, predicate, object) => {
                key[0] = QV_GPO_TAG;
                key[1..9].copy_from_slice(&graph.to_be_bytes());
                key[9..17].copy_from_slice(&predicate.to_be_bytes());
                key[17..25].copy_from_slice(&object.to_be_bytes());
            }
            Self::Total => unreachable!("total counter returned before allocating a key"),
            Self::UnionDuplicateFree => {
                unreachable!("union proof returned before allocating a key")
            }
        }
        key
    }
}

enum IndexHeaderRead {
    Absent,
    Valid(IndexHeader),
    Legacy(IndexHeader),
    Malformed,
}

enum CounterKeyRead {
    Header,
    Counter(IndexCounterKey),
    Revision,
    ProjectionDebt,
    Control,
    UnknownTag,
    InvalidLength,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct NetQuadTransition {
    quad: EncodedQuad,
    was_live: bool,
    is_live: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum QueryBuildPhase {
    Clear,
    Scan,
    Replay,
    Verify,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RebuildPhase {
    BuildCreate,
    ScanPage,
    ReplayPage,
    BeforeSwitch,
    AfterSwitch,
    CleanupPage,
    Retire,
}

#[cfg(test)]
pub(crate) type RebuildCallback = Arc<dyn Fn(RebuildPhase) + Send + Sync>;

#[cfg(test)]
pub(crate) struct RebuildHook<'a> {
    store: &'a GraphStore,
}

#[cfg(test)]
impl Drop for RebuildHook<'_> {
    fn drop(&mut self) {
        *self
            .store
            .rebuild_hook
            .write()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
pub(crate) struct DeltaLimitGuard<'a> {
    store: &'a GraphStore,
    rows: u64,
    bytes: u64,
}

#[cfg(test)]
impl Drop for DeltaLimitGuard<'_> {
    fn drop(&mut self) {
        self.store
            .delta_row_limit
            .store(self.rows, Ordering::SeqCst);
        self.store
            .delta_byte_limit
            .store(self.bytes, Ordering::SeqCst);
    }
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct QueryBuildRecord {
    format: u32,
    slot: u8,
    source_sequence: u64,
    source_epoch: u64,
    control_digest: [u8; 32],
    trusted_active: bool,
    #[serde(with = "crate::core::quad_cursor")]
    scan_cursor: Option<QuadKey>,
    replay_cursor: u64,
    next_delta: u64,
    delta_rows: u64,
    delta_bytes: u64,
    phase: QueryBuildPhase,
    target: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct QueryDeltaRecord {
    transitions: Vec<NetQuadTransition>,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct QueryCleanupRecord {
    format: u32,
    slot: u8,
}

struct QueryClear<'a> {
    keyspace: &'a Keyspace,
    stop: Option<&'a AtomicBool>,
}

struct QueryBuildCtx<'a> {
    snapshot: &'a Snapshot,
    spaces: IndexSpaces<'a>,
    build: &'a mut QueryBuildRecord,
    stop: &'a AtomicBool,
}

#[derive(Clone)]
pub(crate) struct BatchReceiptLink {
    pub(crate) graph: TermId,
    pub(crate) actor: ActorId,
    pub(crate) counter: u64,
    pub(crate) id: MutationId,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ReceiptWork {
    pub(crate) lookups: u64,
    pub(crate) writes: u64,
    pub(crate) persists: u64,
}

struct DeleteGraph<'a> {
    graph: &'a GraphId,
    tombstone: Option<&'a GraphTombstone>,
    receipt: Option<&'a MutationReceipt>,
}

pub(crate) struct PolicyReceipt<'a> {
    pub(crate) tagged: &'a TaggedGraphPolicy,
    pub(crate) receipt: &'a MutationReceipt,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredBatchReceipt {
    id: MutationId,
    sequence: u64,
}

struct IndexCounterUpdate {
    key: IndexCounterKey,
    value: Option<u64>,
}

struct IndexUpdatePlan {
    slot: IndexSlot,
    transitions: Vec<(QueryQuad, bool)>,
    mappings: Vec<(TermId, QueryTermId)>,
    counters: Vec<IndexCounterUpdate>,
    revisions: Vec<(QueryTermId, u64)>,
    header: Option<IndexHeader>,
}

enum IndexCounterRead {
    Missing,
    Value(u64),
    Malformed,
}

/// A durable FTS queue key, minus the dirty token it is stamped with.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum FtsQueueKey {
    Subject { graph: TermId, subject: TermId },
    Reindex(TermId),
    Delete(TermId),
}

impl FtsQueueKey {
    fn bytes(self) -> Vec<u8> {
        match self {
            Self::Subject { graph, subject } => graph_dirty_key(graph, subject).to_vec(),
            Self::Reindex(graph) => graph_reindex_key(graph).to_vec(),
            Self::Delete(graph) => graph_delete_key(graph).to_vec(),
        }
    }

    fn cursor(self, token: u64) -> QueueCursor {
        match self {
            Self::Subject { graph, subject } => QueueCursor {
                token,
                kind: QueueKind::Subject,
                graph,
                subject: Some(subject),
            },
            Self::Reindex(graph) => QueueCursor {
                token,
                kind: QueueKind::Reindex,
                graph,
                subject: None,
            },
            Self::Delete(graph) => QueueCursor {
                token,
                kind: QueueKind::Delete,
                graph,
                subject: None,
            },
        }
    }
}

/// Deduplicated FTS debt keys retained in enqueue order for token acknowledgements.
#[derive(Default)]
struct PendingFts {
    order: Vec<FtsQueueKey>,
    seen: HashSet<FtsQueueKey>,
}

impl PendingFts {
    fn push(&mut self, key: FtsQueueKey) {
        if self.seen.insert(key) {
            self.order.push(key);
        }
    }
}

/// One queue entry an indexing pass covered up to `covered`.
struct AckedEntry {
    key: Vec<u8>,
    covered: u64,
}

struct OrderedQueuePage {
    rows: Vec<(QueueCursor, DirtyTokens)>,
    next: Option<QueueCursor>,
    remaining: bool,
    visited: usize,
    bytes: usize,
    oversized: Option<(QueueCursor, u64, usize)>,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
enum SearchOrderStage {
    Clear,
    Delete,
    Reindex,
    Subject,
    Done,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SearchOrderMigration {
    format: u16,
    stage: SearchOrderStage,
    after: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredGeneration {
    format: u16,
    index_id: [u8; 16],
    generation: GraphGeneration,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredStage {
    format: u16,
    index_id: [u8; 16],
    job: StageJob,
}

#[derive(serde::Deserialize)]
struct LegacySearchCoverage {
    format: u16,
    index_id: [u8; 16],
    covered: u64,
    rebuild: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredManifest {
    format: u16,
    index_id: [u8; 16],
    count: u64,
    hash: [u8; 32],
    epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SearchRebuildScan {
    format: u16,
    index_id: [u8; 16],
    target: u64,
    after: Option<TermId>,
    done: bool,
}

struct ManifestChange<'a> {
    index_id: [u8; 16],
    graph: &'a GraphId,
    previous: Option<GenerationId>,
    active: Option<GenerationId>,
}

/// A batch's durable half: the staged fjall writes plus the FTS queue keys
/// whose tokens are minted when it publishes.
struct DurableCommit {
    batch: fjall::OwnedWriteBatch,
    pending_fts: PendingFts,
    pending_receipts: Vec<MutationReceipt>,
    receipt_trim: ReceiptTrim,
}

/// Receipt count changes and trim positions a batch applies once it commits.
#[derive(Default)]
struct ReceiptTrim {
    receipts: TrimChange,
    batches: TrimChange,
}

#[derive(Default)]
struct TrimChange {
    added: usize,
    removed: usize,
    floor: Option<Vec<u8>>,
}

/// Committed entries of one retained kind; races only lower the count, so a
/// trim never evicts a retained entry. The floor skips earlier trims' tombstones.
#[derive(Default)]
struct TrimCount {
    live: AtomicUsize,
    floor: Mutex<Option<Vec<u8>>>,
}

impl TrimCount {
    fn excess(&self, change: &TrimChange) -> usize {
        (self.live.load(Ordering::SeqCst) + change.added + 1)
            .saturating_sub(change.removed)
            .saturating_sub(RECEIPT_RETENTION)
            .min(QV_BUILD_ROWS)
    }

    fn start(&self, change: &TrimChange) -> Option<Vec<u8>> {
        change.floor.clone().or_else(|| {
            self.floor
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        })
    }

    fn settle(&self, change: TrimChange) {
        self.live.fetch_add(change.added, Ordering::SeqCst);
        let _ = self
            .live
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |live| {
                Some(live.saturating_sub(change.removed))
            });
        if let Some(floor) = change.floor {
            let mut current = self.floor.lock().unwrap_or_else(PoisonError::into_inner);
            if current.as_ref().is_none_or(|current| *current < floor) {
                *current = Some(floor);
            }
        }
    }
}

pub struct WriteBatch {
    inner: fjall::OwnedWriteBatch,
    /// Batch-local dot states; `None` means the quad was written dead.
    pending_quad_states: HashMap<QuadKey, Option<Vec<Dot>>>,
    pending_terms: HashMap<TermId, String>,
    publish: PendingPublish,
    /// Queue keys staged and tokenized under the queue lock at commit.
    pending_fts: PendingFts,
    pending_receipts: Vec<MutationReceipt>,
    receipt_trim: ReceiptTrim,
}

impl WriteBatch {
    fn new(inner: fjall::OwnedWriteBatch) -> Self {
        Self {
            inner,
            pending_quad_states: HashMap::new(),
            pending_terms: HashMap::new(),
            publish: PendingPublish::default(),
            pending_fts: PendingFts::default(),
            pending_receipts: Vec::new(),
            receipt_trim: ReceiptTrim::default(),
        }
    }

    pub fn insert<K, V>(&mut self, keyspace: &Keyspace, key: K, value: V)
    where
        K: Into<fjall::UserKey>,
        V: Into<fjall::UserValue>,
    {
        self.inner.insert(keyspace, key, value);
    }

    pub fn remove<K>(&mut self, keyspace: &Keyspace, key: K)
    where
        K: Into<fjall::UserKey>,
    {
        self.inner.remove(keyspace, key);
    }
}

/// Bounded cache state published after a durable graph commit.
struct IndexState {
    #[allow(clippy::type_complexity)]
    quad_subjects: BoundedCache<(TermId, TermId, u64), Arc<Vec<(TermId, TermId)>>>,
    object_order: ObjectOrderCache,
    planner_distinct: BoundedCache<PlannerCacheKey, PlannerEstimate>,
    epochs: [u64; INDEX_EPOCH_SHARDS],
}

impl IndexState {
    fn with_budget(budget: &CacheBudget) -> Self {
        Self {
            quad_subjects: BoundedCache::new(SUBJECT_CACHE_CAP, budget.subjects),
            object_order: ObjectOrderCache::with_budget(budget),
            planner_distinct: BoundedCache::new(PLANNER_CACHE_CAP, budget.planner),
            epochs: [0; INDEX_EPOCH_SHARDS],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DistinctDomain {
    Subject,
    Object,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum StatRevision {
    Epoch(u64),
    Predicate(u64),
}

type PlannerCacheKey = (u64, StatRevision, Option<QueryTermId>, DistinctDomain);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum PlannerStat {
    Graph(TermId),
    GraphPredicate(TermId, TermId),
    GraphPredicateObject(TermId, TermId, TermId),
    PredicateObject(TermId, TermId),
    Predicate(TermId),
    PredicateSubjects(TermId),
    PredicateObjects(TermId),
    Object(TermId),
    Subject(TermId),
    DistinctSubjects,
    DistinctObjects,
    Total,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlannerEstimate {
    Exact(usize),
    LowerBound(usize),
    Unknown,
}

impl PlannerEstimate {
    pub(crate) fn row_upper(self) -> usize {
        match self {
            Self::Exact(value) => value,
            Self::LowerBound(_) | Self::Unknown => usize::MAX,
        }
    }

    pub(crate) fn distinct_lower(self) -> usize {
        match self {
            Self::Exact(value) | Self::LowerBound(value) => value,
            Self::Unknown => 1,
        }
    }
}

#[derive(Clone, Copy)]
struct DistinctStat {
    predicate: Option<TermId>,
    domain: DistinctDomain,
}

type ObjectOrderKey = (TermId, TermId, TermId);
type ObjectOrderValues = Arc<Vec<TermId>>;

/// Objects in decoded-term order, fenced by the graph shard epoch.
struct ObjectOrderCache {
    entries: BoundedCache<(ObjectOrderKey, u64), ObjectOrderValues>,
}

impl ObjectOrderCache {
    fn with_budget(budget: &CacheBudget) -> Self {
        Self {
            entries: BoundedCache::new(ORDER_CACHE_CAP, budget.objects),
        }
    }
}

impl ObjectOrderCache {
    fn get(&mut self, key: &ObjectOrderKey, generation: u64) -> Option<ObjectOrderValues> {
        self.entries.get_cloned(&(*key, generation))
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.entries.clear();
    }

    /// Drop every entry belonging to `graph`, e.g. when the graph is deleted.
    fn drop_graph(&mut self, graph: TermId) {
        self.entries
            .remove_where(|((cached, _, _), _)| *cached == graph);
    }

    fn install(&mut self, entry: OrderEntry, generation: u64) {
        let bytes = entry
            .objects
            .len()
            .saturating_mul(std::mem::size_of::<TermId>());
        self.entries
            .insert((entry.key, generation), entry.objects, bytes);
    }

    #[cfg(test)]
    fn statistics(&self) -> CacheStatistics {
        self.entries.statistics()
    }
}

/// One `(graph, subject, predicate)` ordering, decoded and sorted.
struct OrderEntry {
    key: ObjectOrderKey,
    objects: ObjectOrderValues,
}

impl IndexState {
    fn graph_epoch(&self, graph: TermId) -> u64 {
        self.epochs[(graph.0 as usize) % INDEX_EPOCH_SHARDS]
    }

    /// Bumps changed graph shards so older cache entries become unreachable.
    fn publish(&mut self, publish: &PendingPublish) {
        let mut changed_graphs = HashSet::new();
        for mutation in &publish.quad_mutations {
            let quad = match mutation {
                QuadMutation::Insert(quad) | QuadMutation::Remove(quad) => *quad,
            };
            changed_graphs.insert(quad.graph);
        }
        for graph in changed_graphs {
            let epoch = &mut self.epochs[(graph.0 as usize) % INDEX_EPOCH_SHARDS];
            *epoch = epoch.wrapping_add(1);
        }
    }
}

/// The in-memory half of a commit, staged alongside the durable batch and
/// published once that batch lands.
#[derive(Default)]
struct PendingPublish {
    quad_mutations: Vec<QuadMutation>,
    /// Clock writes keep clock-only batches on the post-commit publication path.
    clocks: HashMap<TermId, Option<VectorClock>>,
}

impl PendingPublish {
    fn is_empty(&self) -> bool {
        self.quad_mutations.is_empty() && self.clocks.is_empty()
    }
}

/// Persisted graph diagnostics tagged with their source vector clock.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StoredDiagnostics {
    diagnostics: GraphDiagnostics,
    at_clock: VectorClock,
}

/// Interned vocabulary ids used by orphan detection; `None` means absent.
struct OrphanVocab {
    rdf_type: Option<TermId>,
    /// `schema:Dataset` and `schema:MediaObject` are the two types that make a
    /// non-root subject a data entity.
    data_types: [Option<TermId>; 2],
    has_part: Option<TermId>,
}

pub struct GraphStore {
    _memory_lease: MemoryLease,
    #[cfg(feature = "shacl-core")]
    shacl_cache_bytes: usize,
    db: Database,
    persist_mode: PersistMode,
    terms: Keyspace,
    quads: Keyspace,
    graphs: Keyspace,
    log: Keyspace,
    qv2_gspo: Keyspace,
    qv2_gpos: Keyspace,
    qv2_spog: Keyspace,
    qv2_posg: Keyspace,
    qv2_ospg: Keyspace,
    qv2_gosp: Keyspace,
    primary_term_map: Keyspace,
    primary_query_map: Keyspace,
    qv2_meta: Keyspace,
    qv3_gspo: Keyspace,
    qv3_gpos: Keyspace,
    qv3_spog: Keyspace,
    qv3_posg: Keyspace,
    qv3_ospg: Keyspace,
    qv3_gosp: Keyspace,
    secondary_term_map: Keyspace,
    secondary_query_map: Keyspace,
    qv3_meta: Keyspace,
    search_queue: Keyspace,
    search_meta: Keyspace,
    receipts: Keyspace,
    receipt_order: Keyspace,
    repair_audits: Keyspace,
    repair_backups: Keyspace,
    /// Guards first-write-wins term interning, sharded by term id.
    term_locks: Vec<Mutex<()>>,
    /// Guards whole read→write→commit cycles of one graph's CRDT state; see
    /// [`GraphStore::graph_commit_guard`].
    commit_locks: Vec<Mutex<()>>,
    /// Orders publication and local apply for one graph within this store.
    write_locks: Vec<Mutex<()>>,
    receipt_locks: Vec<Mutex<()>>,
    receipt_next: AtomicU64,
    receipt_count: TrimCount,
    batch_receipt_count: TrimCount,
    #[cfg(test)]
    receipt_lookups: AtomicU64,
    #[cfg(test)]
    receipt_writes: AtomicU64,
    #[cfg(test)]
    receipt_persists: AtomicU64,
    /// Commits share this guard; rebuilds exclude them before owning the qv gate.
    projection_lock: RwLock<()>,
    qv_maintenance: Mutex<()>,
    /// Serializes QV counters; contenders persist debt that closes admission.
    qv_gate: QvCommitGate,
    qv_commit_wait: Duration,
    qv_debt_next: AtomicU64,
    #[cfg(feature = "shacl-core")]
    binding_lock: Mutex<()>,
    #[cfg(feature = "shacl-core")]
    binding_wait_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    binding_hold_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    graph_lock_wait: AtomicU64,
    #[cfg(feature = "shacl-core")]
    validation_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    settlement_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    settlement_failures: AtomicU64,
    #[cfg(feature = "shacl-core")]
    status_bindings_read: AtomicU64,
    #[cfg(feature = "shacl-core")]
    status_version_checks: AtomicU64,
    #[cfg(feature = "shacl-core")]
    status_shape_compilations: AtomicU64,
    #[cfg(feature = "shacl-core")]
    status_shape_scans: AtomicU64,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_stall: Mutex<Duration>,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_active: std::sync::atomic::AtomicUsize,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_max_active: std::sync::atomic::AtomicUsize,
    indexes: RwLock<IndexState>,
    /// Global term-id → term cache. Term ids are content hashes, so entries do
    /// not need invalidation; capacity and bytes are bounded independently.
    term_decode_cache: RwLock<BoundedCache<TermId, Arc<EncodedTerm>>>,
    /// Source terms of dense IDs; a mapping never changes within one query-ID generation.
    source_cache: SourceCache,
    /// Highest query-ID generation seen by this process, so a rebuild never reuses one.
    generation_floor: AtomicU64,
    /// Counts for the shared-lock term hit path, which cannot update the cache.
    term_cache_hits: AtomicU64,
    term_cache_misses: AtomicU64,
    /// Set by a test to stall between the durable commit and the index apply,
    /// widening a window that is otherwise microseconds wide.
    #[cfg(test)]
    commit_stall: Mutex<Option<std::time::Duration>>,
    /// True while a test-only commit is stalled before cache publication.
    #[cfg(test)]
    commit_stalled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    commit_stall_active: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    peak_commit_stalls: std::sync::atomic::AtomicUsize,
    /// Makes the next durable batch fail immediately before fjall commits.
    #[cfg(test)]
    commit_failure: std::sync::atomic::AtomicBool,
    /// Makes every snapshot policy read of this graph fail with an I/O error.
    #[cfg(test)]
    policy_failure: Mutex<Option<GraphId>>,
    /// Set by a test to stall inside a held [`GraphStore::fts_queue_guard`],
    /// between an acknowledgement's token read and its commit.
    #[cfg(test)]
    fts_ack_stall: Mutex<Option<std::time::Duration>>,
    /// Set by a test to stall a rebuild between its durable scan and the
    /// install; `rebuild_stalled` publishes that the window has been entered.
    #[cfg(test)]
    rebuild_stall: Mutex<Option<std::time::Duration>>,
    #[cfg(test)]
    rebuild_stalled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    rebuild_hook: RwLock<Option<RebuildCallback>>,
    #[cfg(test)]
    delta_row_limit: AtomicU64,
    #[cfg(test)]
    delta_byte_limit: AtomicU64,
    /// Set by a test to stall a graph delete between its queue scan and the
    /// commit; `delete_stalled` publishes that the window has been entered.
    #[cfg(test)]
    delete_stall: Mutex<Option<std::time::Duration>>,
    #[cfg(test)]
    delete_stalled: std::sync::atomic::AtomicBool,
    /// Serializes FTS queue mutations and is innermost in the store lock order.
    fts_queue_lock: Mutex<()>,
    dirty_counter: AtomicU64,
    /// No queued search work has a lower token; skips acknowledged queue tombstones.
    queue_floor: AtomicU64,
    dirty_committed: AtomicU64,
    /// Number of graph diagnostics recomputations by this store instance.
    diagnostics_computed: AtomicU64,
    /// Metadata point reads performed by the O(1) qv2 admission gate.
    #[cfg(test)]
    index_admission_probes: AtomicU64,
    #[cfg(test)]
    index_verification_runs: AtomicU64,
    /// Explicit persists so far. Tests use it to pin a durability call that
    /// leaves no other trace inside one process.
    #[cfg(test)]
    persists: AtomicU64,
}

// Mutation parameter structs.

/// Serializes one graph's read-write cycle before term shard locks.
pub(crate) struct GraphCommitGuard<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);

/// Reentrant on its thread, so an entry point can hold the lock across authorization and commit.
pub(crate) struct GraphWriteGuard<'a> {
    guard: Option<MutexGuard<'a, ()>>,
    lock: usize,
}

thread_local! {
    /// The write lock shards this thread holds, by address.
    static HELD_WRITE_LOCKS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

impl Drop for GraphWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            HELD_WRITE_LOCKS.with(|held| held.borrow_mut().retain(|lock| *lock != self.lock));
            drop(guard);
        }
    }
}

pub(crate) struct ReceiptGuard<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);

#[cfg(feature = "shacl-core")]
pub(crate) struct BindingGuard<'a> {
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
    hold_started: Instant,
    hold_ns: &'a AtomicU64,
}

#[cfg(feature = "shacl-core")]
impl Drop for BindingGuard<'_> {
    fn drop(&mut self) {
        self.hold_ns
            .fetch_add(elapsed_ns(self.hold_started.elapsed()), Ordering::Relaxed);
    }
}

#[cfg(feature = "shacl-core")]
pub(crate) struct QueueRepairStats {
    pub(crate) binding_records_scanned: u64,
    pub(crate) pending_entries_scanned: u64,
}

#[cfg(feature = "shacl-core")]
pub(crate) struct PendingQueueScan {
    pub(crate) graphs: Vec<GraphId>,
    pub(crate) entries_scanned: u64,
    pub(crate) budget_exhausted: bool,
}

#[cfg(feature = "shacl-core")]
fn elapsed_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(all(test, feature = "shacl-core"))]
pub(crate) struct ValidationProbe<'a> {
    active: &'a std::sync::atomic::AtomicUsize,
}

#[cfg(all(test, feature = "shacl-core"))]
impl Drop for ValidationProbe<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An OR-Set add contributes exactly one unique dot to the quad's dot set.
pub struct QuadAdd {
    pub quad: EncodedQuad,
    pub dot: Dot,
}

/// An OR-Set remove: deletes exactly the dots contained in the witnessed clock
/// and can never kill a dot it did not witness.
pub struct QuadRemove<'a> {
    pub quad: EncodedQuad,
    pub witnessed: &'a VectorClock,
}

pub struct ClockUpdate<'a> {
    pub graph_id: TermId,
    pub clock: &'a VectorClock,
}

pub struct CounterKey {
    pub graph_id: TermId,
    pub actor: ActorId,
}

/// Batch-scoped term interning context: the write batch the terms are staged
/// into plus the caller's term → id memo.
pub struct BatchTermCtx<'a> {
    pub batch: &'a mut WriteBatch,
    pub cache: &'a mut HashMap<String, TermId>,
}

pub struct FtsSubject {
    pub graph_id: TermId,
    pub subject: TermId,
}

pub struct FtsEnqueue<'a> {
    pub graph_id: TermId,
    pub subjects: &'a HashSet<TermId>,
}

pub struct GraphSubjectPredicate<'a> {
    pub graph: &'a GraphId,
    pub subject: &'a EncodedTerm,
    pub predicate: &'a EncodedTerm,
}

pub enum PageCursor<'a> {
    Offset(usize),
    After(Option<&'a EncodedTerm>),
}

pub struct PageRequest<'a> {
    pub cursor: PageCursor<'a>,
    pub limit: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotLimits {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

fn encode_dirty_tokens(tokens: DirtyTokens) -> [u8; 16] {
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&tokens.oldest.to_be_bytes());
    value[8..].copy_from_slice(&tokens.latest.to_be_bytes());
    value
}

/// Decodes current two-token debt and the legacy single-token representation.
fn decode_dirty_tokens(bytes: &[u8], context: &'static str) -> Result<DirtyTokens> {
    if bytes.len() == 8 {
        let token = decode_u64(bytes, context)?;
        return Ok(DirtyTokens {
            oldest: token,
            latest: token,
        });
    }
    if bytes.len() != 16 {
        return Err(StoreError::InvalidEncoding {
            context,
            message: format!("expected 8 or 16 bytes, found {}", bytes.len()),
        });
    }
    Ok(DirtyTokens {
        oldest: decode_u64(&bytes[..8], context)?,
        latest: decode_u64(&bytes[8..], context)?,
    })
}

fn decode_u64(bytes: &[u8], context: &'static str) -> Result<u64> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| StoreError::InvalidEncoding {
        context,
        message: format!("expected 8 bytes, found {}", bytes.len()),
    })?;
    Ok(u64::from_be_bytes(raw))
}

fn queue_kind_tag(kind: QueueKind) -> u8 {
    match kind {
        QueueKind::Delete => 0,
        QueueKind::Reindex => 1,
        QueueKind::Subject => 2,
    }
}

fn decode_queue_kind(tag: u8) -> Result<QueueKind> {
    match tag {
        0 => Ok(QueueKind::Delete),
        1 => Ok(QueueKind::Reindex),
        2 => Ok(QueueKind::Subject),
        _ => Err(StoreError::InvalidSearchState("queue-kind-invalid")),
    }
}

fn search_order_key(cursor: QueueCursor) -> [u8; 42] {
    let mut key = [0u8; 42];
    key[0] = SEARCH_ORDER_PREFIX;
    key[1..9].copy_from_slice(&cursor.token.to_be_bytes());
    key[9] = queue_kind_tag(cursor.kind);
    key[10..26].copy_from_slice(&cursor.graph.to_be_bytes());
    if let Some(subject) = cursor.subject {
        key[26..42].copy_from_slice(&subject.to_be_bytes());
    }
    key
}

fn decode_search_order(bytes: &[u8]) -> Result<QueueCursor> {
    if bytes.len() != 42 || bytes[0] != SEARCH_ORDER_PREFIX {
        return Err(StoreError::InvalidSearchState("queue-order-key-invalid"));
    }
    let kind = decode_queue_kind(bytes[9])?;
    let subject = TermId::from_be_bytes(bytes[26..42].try_into().unwrap());
    Ok(QueueCursor {
        token: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        kind,
        graph: TermId::from_be_bytes(bytes[10..26].try_into().unwrap()),
        subject: matches!(kind, QueueKind::Subject).then_some(subject),
    })
}

fn search_failure_key(cursor: QueueCursor) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[0] = SEARCH_FAILURE_PREFIX;
    key[1] = queue_kind_tag(cursor.kind);
    key[2..18].copy_from_slice(&cursor.graph.to_be_bytes());
    if let Some(subject) = cursor.subject {
        key[18..34].copy_from_slice(&subject.to_be_bytes());
    }
    key
}

fn search_generation_key(graph: &GraphId) -> [u8; 17] {
    let term = hash_term(&EncodedTerm::from_named_node(&graph.0));
    let mut key = [0u8; 17];
    key[0] = SEARCH_GENERATION_PREFIX;
    key[1..].copy_from_slice(&term.to_be_bytes());
    key
}

fn search_stage_key(graph: &GraphId) -> [u8; 17] {
    let mut key = search_generation_key(graph);
    key[0] = SEARCH_STAGE_PREFIX;
    key
}

fn search_cleanup_key(generation: GenerationId) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = SEARCH_CLEANUP_PREFIX;
    key[1..].copy_from_slice(&generation.0.to_be_bytes());
    key
}

fn encode_search_coverage(coverage: &SearchCoverage) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SEARCH_COVERAGE_MAGIC);
    bytes.extend_from_slice(&coverage.format.to_be_bytes());
    bytes.extend_from_slice(&postcard::to_allocvec(coverage)?);
    Ok(bytes)
}

fn encode_search_generation(stored: &StoredGeneration) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SEARCH_GENERATION_MAGIC);
    bytes.extend_from_slice(&stored.format.to_be_bytes());
    bytes.extend_from_slice(&stored.index_id);
    bytes.extend_from_slice(&postcard::to_allocvec(&stored.generation)?);
    Ok(bytes)
}

fn encode_search_stage(stored: &StoredStage) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SEARCH_STAGE_MAGIC);
    bytes.extend_from_slice(&stored.format.to_be_bytes());
    bytes.extend_from_slice(&stored.index_id);
    bytes.extend_from_slice(&postcard::to_allocvec(&stored.job)?);
    Ok(bytes)
}

fn decode_search_generation(bytes: &[u8]) -> Result<StoredGeneration> {
    if !bytes.starts_with(&SEARCH_GENERATION_MAGIC) {
        return Ok(postcard::from_bytes(bytes)?);
    }
    if bytes.len() < 20 {
        return Err(StoreError::InvalidSearchState(
            "search-generation-header-invalid",
        ));
    }
    let format = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
    if format > SEARCH_META_FORMAT {
        return Err(StoreError::UnsupportedSearchFormat {
            found: format,
            supported: SEARCH_META_FORMAT,
        });
    }
    Ok(StoredGeneration {
        format,
        index_id: bytes[4..20].try_into().unwrap(),
        generation: postcard::from_bytes(&bytes[20..])?,
    })
}

fn decode_search_stage(bytes: &[u8]) -> Result<StoredStage> {
    if !bytes.starts_with(&SEARCH_STAGE_MAGIC) {
        return Ok(postcard::from_bytes(bytes)?);
    }
    if bytes.len() < 20 {
        return Err(StoreError::InvalidSearchState(
            "search-stage-header-invalid",
        ));
    }
    let format = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
    if format > SEARCH_META_FORMAT {
        return Err(StoreError::UnsupportedSearchFormat {
            found: format,
            supported: SEARCH_META_FORMAT,
        });
    }
    Ok(StoredStage {
        format,
        index_id: bytes[4..20].try_into().unwrap(),
        job: postcard::from_bytes(&bytes[20..])?,
    })
}

fn receipt_order_key(receipt: &MutationReceipt) -> [u8; 40] {
    let mut key = [0u8; 40];
    let ordered_time = (receipt.updated_unix_nanos as u64) ^ (1 << 63);
    key[..8].copy_from_slice(&ordered_time.to_be_bytes());
    key[8..].copy_from_slice(&receipt.id.0);
    key
}

fn batch_receipt_key(link: &BatchReceiptLink) -> [u8; 58] {
    let mut key = [0u8; 58];
    key[..2].copy_from_slice(&BATCH_RECEIPT_PREFIX);
    key[2..18].copy_from_slice(&link.graph.to_be_bytes());
    key[18..50].copy_from_slice(link.actor.as_bytes());
    key[50..].copy_from_slice(&link.counter.to_be_bytes());
    key
}

fn batch_reverse_key(id: &MutationId) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&BATCH_REVERSE_PREFIX);
    key[2..].copy_from_slice(&id.0);
    key
}

fn batch_order_key(sequence: u64) -> [u8; 10] {
    let mut key = [0u8; 10];
    key[..2].copy_from_slice(&BATCH_ORDER_PREFIX);
    key[2..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn encode_dots(dots: &[Dot]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + dots.len() * 40);
    bytes.push(DOT_ENCODING_TAG);
    for dot in dots {
        bytes.extend_from_slice(dot.actor.as_bytes());
        bytes.extend_from_slice(&dot.counter.to_be_bytes());
    }
    bytes
}

fn decode_dots(bytes: &[u8]) -> Result<Vec<Dot>> {
    if bytes.first().copied() != Some(DOT_ENCODING_TAG) {
        return Ok(postcard::from_bytes(bytes)?);
    }
    if !(bytes.len() - 1).is_multiple_of(40) {
        return Err(StoreError::InvalidEncoding {
            context: "quad dots",
            message: format!("invalid dot payload length {}", bytes.len()),
        });
    }

    let mut dots = Vec::with_capacity((bytes.len() - 1) / 40);
    for chunk in bytes[1..].as_chunks::<40>().0 {
        dots.push(Dot {
            actor: ActorId::from_bytes(chunk[..32].try_into().unwrap()),
            counter: u64::from_be_bytes(chunk[32..40].try_into().unwrap()),
        });
    }
    Ok(dots)
}

/// Both dot encodings represent an empty set in at most one byte.
fn dots_empty(bytes: &[u8]) -> bool {
    bytes.len() <= 1
}

fn normalize_dots(dots: &mut Vec<Dot>) {
    dots.sort_unstable_by(|left, right| {
        (left.actor, left.counter).cmp(&(right.actor, right.counter))
    });
    dots.dedup();
}

pub(crate) fn hash_term(term: &EncodedTerm) -> TermId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"craqle-term/v1\0");
    hasher.update(term.0.as_bytes());
    let hash = hasher.finalize();
    TermId(u128::from_be_bytes(
        hash.as_bytes()[..16].try_into().unwrap(),
    ))
}

fn graph_meta_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_META_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_dirty_key(graph: TermId, subject: TermId) -> [u8; 33] {
    let mut key = [0u8; 33];
    key[0] = GRAPH_DIRTY_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..33].copy_from_slice(&subject.to_be_bytes());
    key
}

fn graph_dirty_scope(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_DIRTY_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_dirty_prefix() -> [u8; 1] {
    [GRAPH_DIRTY_PREFIX]
}

fn graph_reindex_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_REINDEX_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_reindex_prefix() -> [u8; 1] {
    [GRAPH_REINDEX_PREFIX]
}

fn graph_delete_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_DELETE_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_delete_prefix() -> [u8; 1] {
    [GRAPH_DELETE_PREFIX]
}

fn graph_meta_prefix() -> [u8; 1] {
    [GRAPH_META_PREFIX]
}

fn graph_clock_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_CLOCK_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_diagnostics_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_DIAGNOSTICS_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn shacl_binding_key(data_graph: TermId, shapes_graph: TermId) -> [u8; 33] {
    let mut key = [0u8; 33];
    key[0] = SHACL_BINDING_PREFIX;
    key[1..17].copy_from_slice(&data_graph.to_be_bytes());
    key[17..33].copy_from_slice(&shapes_graph.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn shacl_binding_prefix(data_graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = SHACL_BINDING_PREFIX;
    key[1..17].copy_from_slice(&data_graph.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn binding_reverse_key(dependency: TermId, data_graph: TermId, shapes_graph: TermId) -> [u8; 49] {
    let mut key = [0u8; 49];
    key[0] = SHACL_REVERSE_PREFIX;
    key[1..17].copy_from_slice(&dependency.to_be_bytes());
    key[17..33].copy_from_slice(&data_graph.to_be_bytes());
    key[33..49].copy_from_slice(&shapes_graph.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn binding_reverse_prefix(dependency: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = SHACL_REVERSE_PREFIX;
    key[1..17].copy_from_slice(&dependency.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn shacl_pending_key(data_graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = SHACL_PENDING_PREFIX;
    key[1..17].copy_from_slice(&data_graph.to_be_bytes());
    key
}

#[cfg(feature = "shacl-core")]
fn shacl_pending_prefix() -> [u8; 1] {
    [SHACL_PENDING_PREFIX]
}

fn topic_clock_key(topic_id: &[u8; 32]) -> [u8; 33] {
    let mut key = [0u8; 33];
    key[0] = TOPIC_CLOCK_PREFIX;
    key[1..33].copy_from_slice(topic_id);
    key
}

fn topic_binding_key(topic_id: &[u8; 32]) -> [u8; 33] {
    let mut key = [0u8; 33];
    key[0] = TOPIC_BINDING_PREFIX;
    key[1..33].copy_from_slice(topic_id);
    key
}

fn graph_tombstone_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_TOMBSTONE_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn deleted_policy_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = DELETED_POLICY_PREFIX;
    key[1..].copy_from_slice(&graph.to_be_bytes());
    key
}

fn replication_rejection_key(topic: &irokle::TopicId, record: &irokle::OpId) -> [u8; 65] {
    let mut key = [0u8; 65];
    key[0] = REPLICATION_REJECTION_PREFIX;
    key[1..33].copy_from_slice(topic.as_bytes());
    key[33..65].copy_from_slice(record.as_bytes());
    key
}

fn replication_rejection_prefix() -> [u8; 1] {
    [REPLICATION_REJECTION_PREFIX]
}

fn cursor_audit_key(audit: &crate::sync::TopicCursorRepairAudit) -> [u8; 97] {
    let mut key = [0u8; 97];
    key[0] = CURSOR_AUDIT_PREFIX;
    key[1..33].copy_from_slice(audit.topic.as_bytes());
    key[33..65].copy_from_slice(&audit.old_cursor_digest);
    key[65..97].copy_from_slice(&audit.replacement_cursor_digest);
    key
}

fn log_head_key(graph: TermId, actor: &ActorId) -> [u8; 49] {
    let mut key = [0u8; 49];
    key[0] = LOG_HEAD_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..49].copy_from_slice(actor.as_bytes());
    key
}

// Prefix of legacy batch-log entries; only used to prune them on graph delete.
fn log_batch_prefix(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = LOG_BATCH_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn log_head_prefix(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = LOG_HEAD_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

/// Confirms an interned term, decoding only to report a hash collision.
fn confirm_stored_term(stored: &[u8], term: &EncodedTerm) -> Result<()> {
    if stored == term.0.as_bytes() {
        return Ok(());
    }
    Err(StoreError::TermCollision {
        attempted: term.0.clone(),
        existing: decode_term_text(stored)?,
    })
}

fn decode_term_text(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| StoreError::InvalidEncoding {
        context: "terms",
        message: error.to_string(),
    })
}

fn decode_term_id(bytes: &[u8], context: &'static str) -> Result<TermId> {
    let raw: [u8; 16] = bytes.try_into().map_err(|_| StoreError::InvalidEncoding {
        context,
        message: format!("expected 16 bytes, found {}", bytes.len()),
    })?;
    Ok(TermId::from_be_bytes(raw))
}

fn decode_index_count(bytes: &[u8]) -> Option<u64> {
    let raw: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

fn projection_debt_key(debt: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = QV_DEBT_TAG;
    key[1..9].copy_from_slice(&debt.to_be_bytes());
    key
}

fn query_delta_key(delta: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = QV_DELTA_TAG;
    key[1..].copy_from_slice(&delta.to_be_bytes());
    key
}

fn valid_index_failure(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= QV_FAILURE_BYTES
        && code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn encode_index_header(header: &IndexHeader) -> Vec<u8> {
    let (state_tag, failure) = match &header.state {
        StoredIndexState::Building => (1, ""),
        StoredIndexState::Ready => (2, ""),
        StoredIndexState::Failed(reason) => (3, reason.as_str()),
    };
    let failure = if valid_index_failure(failure) || (failure.is_empty() && state_tag != 3) {
        failure
    } else {
        "metadata-malformed"
    };
    let mut bytes = Vec::with_capacity(QV_HEADER_LEN + failure.len());
    bytes.extend_from_slice(&QV_HEADER_MAGIC);
    bytes.extend_from_slice(&QV_SCHEMA_VERSION.to_be_bytes());
    bytes.push(state_tag);
    bytes.extend_from_slice(&[0, 0, 0]);
    bytes.push(header.active_slot);
    bytes.extend_from_slice(&header.source_epoch.to_be_bytes());
    bytes.extend_from_slice(&header.index_epoch.to_be_bytes());
    bytes.extend_from_slice(&header.source_live_quads.to_be_bytes());
    bytes.extend_from_slice(&header.indexed_quads.to_be_bytes());
    bytes.extend_from_slice(&header.last_build_sequence.to_be_bytes());
    bytes.extend_from_slice(&header.query_id_generation.to_be_bytes());
    bytes.extend_from_slice(&header.next_query_id.to_be_bytes());
    bytes.extend_from_slice(&(failure.len() as u16).to_be_bytes());
    bytes.extend_from_slice(failure.as_bytes());
    bytes
}

fn decode_index_header(bytes: &[u8]) -> IndexHeaderRead {
    if bytes.len() < 12
        || bytes[0..4] != QV_HEADER_MAGIC
        || bytes[8] == 0
        || bytes[9..12] != [0, 0, 0]
    {
        return IndexHeaderRead::Malformed;
    }
    let schema_version = u32::from_be_bytes(
        bytes[4..8]
            .try_into()
            .expect("fixed query-index header slice"),
    );
    if schema_version != QV_SCHEMA_VERSION
        && schema_version != QV_ATOMIC_VERSION
        && schema_version != QV_LEGACY_VERSION
    {
        return IndexHeaderRead::Malformed;
    }
    let legacy = schema_version != QV_SCHEMA_VERSION;
    let (base_len, offset, active_slot) = if legacy {
        (QV_LEGACY_LEN, 12, 0)
    } else {
        if bytes.len() < QV_HEADER_LEN || IndexSlot::decode(bytes[12]).is_none() {
            return IndexHeaderRead::Malformed;
        }
        (QV_HEADER_LEN, 13, bytes[12])
    };
    if bytes.len() < base_len {
        return IndexHeaderRead::Malformed;
    }
    let Some(source_epoch) = decode_index_count(&bytes[offset..offset + 8]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(index_epoch) = decode_index_count(&bytes[offset + 8..offset + 16]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(source_live_quads) = decode_index_count(&bytes[offset + 16..offset + 24]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(indexed_quads) = decode_index_count(&bytes[offset + 24..offset + 32]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(last_build_sequence) = decode_index_count(&bytes[offset + 32..offset + 40]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(query_id_generation) = decode_index_count(&bytes[offset + 40..offset + 48]) else {
        return IndexHeaderRead::Malformed;
    };
    let Some(next_query_id) = decode_index_count(&bytes[offset + 48..offset + 56]) else {
        return IndexHeaderRead::Malformed;
    };
    let failure_len = u16::from_be_bytes(
        bytes[offset + 56..offset + 58]
            .try_into()
            .expect("fixed query-index header slice"),
    ) as usize;
    if failure_len > QV_FAILURE_BYTES || bytes.len() != base_len + failure_len {
        return IndexHeaderRead::Malformed;
    }
    let failure = std::str::from_utf8(&bytes[base_len..]).ok();
    let state = match (bytes[8], failure) {
        (1, Some("")) => StoredIndexState::Building,
        (2, Some("")) => StoredIndexState::Ready,
        (3, Some(reason)) if valid_index_failure(reason) => {
            StoredIndexState::Failed(reason.to_owned())
        }
        _ => return IndexHeaderRead::Malformed,
    };
    let header = IndexHeader {
        active_slot,
        state,
        source_epoch,
        index_epoch,
        source_live_quads,
        indexed_quads,
        last_build_sequence,
        query_id_generation,
        next_query_id,
    };
    if legacy {
        IndexHeaderRead::Legacy(header)
    } else {
        IndexHeaderRead::Valid(header)
    }
}

fn index_term_at(bytes: &[u8], offset: usize) -> QueryTermId {
    QueryTermId::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed query-index term slice"),
    )
}

fn predicate_revision_key(predicate: QueryTermId) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = QV_VERSION_TAG;
    key[1..].copy_from_slice(&predicate.to_be_bytes());
    key
}

fn decode_counter_key(bytes: &[u8]) -> CounterKeyRead {
    match bytes.first().copied() {
        Some(b'H') if bytes.len() == 1 => CounterKeyRead::Header,
        Some(b'H') => CounterKeyRead::InvalidLength,
        Some(b'T') if bytes.len() == 1 => CounterKeyRead::Counter(IndexCounterKey::Total),
        Some(b'T') => CounterKeyRead::InvalidLength,
        Some(QV_UNION_TAG) if bytes.len() == 1 => {
            CounterKeyRead::Counter(IndexCounterKey::UnionDuplicateFree)
        }
        Some(QV_UNION_TAG) => CounterKeyRead::InvalidLength,
        Some(QV_GRAPH_TAG) if bytes.len() == 9 => {
            CounterKeyRead::Counter(IndexCounterKey::Graph(index_term_at(bytes, 1)))
        }
        Some(QV_PREDICATE_TAG) if bytes.len() == 9 => {
            CounterKeyRead::Counter(IndexCounterKey::Predicate(index_term_at(bytes, 1)))
        }
        Some(QV_VERSION_TAG) if bytes.len() == 9 => CounterKeyRead::Revision,
        Some(QV_GP_TAG) if bytes.len() == 17 => CounterKeyRead::Counter(
            IndexCounterKey::GraphPredicate(index_term_at(bytes, 1), index_term_at(bytes, 9)),
        ),
        Some(QV_PO_TAG) if bytes.len() == 17 => CounterKeyRead::Counter(
            IndexCounterKey::PredicateObject(index_term_at(bytes, 1), index_term_at(bytes, 9)),
        ),
        Some(QV_GPO_TAG) if bytes.len() == 25 => {
            CounterKeyRead::Counter(IndexCounterKey::GraphPredicateObject(
                index_term_at(bytes, 1),
                index_term_at(bytes, 9),
                index_term_at(bytes, 17),
            ))
        }
        Some(
            QV_GRAPH_TAG | QV_PREDICATE_TAG | QV_VERSION_TAG | QV_GP_TAG | QV_PO_TAG | QV_GPO_TAG,
        ) => CounterKeyRead::InvalidLength,
        Some(QV_DEBT_TAG) if bytes.len() == 9 => CounterKeyRead::ProjectionDebt,
        Some(QV_DEBT_TAG) => CounterKeyRead::InvalidLength,
        Some(b'B' | b'C') if bytes.len() == 1 => CounterKeyRead::Control,
        Some(QV_DELTA_TAG) if bytes.len() == 9 => CounterKeyRead::Control,
        Some(_) => CounterKeyRead::UnknownTag,
        None => CounterKeyRead::InvalidLength,
    }
}

fn query_index_key(parts: [QueryTermId; 4]) -> QueryQuadKey {
    let mut key = [0u8; 32];
    for (index, term) in parts.into_iter().enumerate() {
        key[index * 8..(index + 1) * 8].copy_from_slice(&term.to_be_bytes());
    }
    key
}

fn query_index_prefix(parts: &[QueryTermId]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(parts.len() * 8);
    for term in parts {
        prefix.extend_from_slice(&term.to_be_bytes());
    }
    prefix
}

fn gspo_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.subject, quad.predicate, quad.object])
}

fn gpos_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.predicate, quad.object, quad.subject])
}

fn spog_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.subject, quad.predicate, quad.object, quad.graph])
}

fn posg_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.predicate, quad.object, quad.subject, quad.graph])
}

fn ospg_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.object, quad.subject, quad.predicate, quad.graph])
}

fn gosp_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.object, quad.subject, quad.predicate])
}

fn decode_gspo_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: index_term_at(bytes, 0),
        subject: index_term_at(bytes, 8),
        predicate: index_term_at(bytes, 16),
        object: index_term_at(bytes, 24),
    })
}

fn decode_gpos_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: index_term_at(bytes, 0),
        predicate: index_term_at(bytes, 8),
        object: index_term_at(bytes, 16),
        subject: index_term_at(bytes, 24),
    })
}

fn decode_spog_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        subject: index_term_at(bytes, 0),
        predicate: index_term_at(bytes, 8),
        object: index_term_at(bytes, 16),
        graph: index_term_at(bytes, 24),
    })
}

fn decode_posg_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        predicate: index_term_at(bytes, 0),
        object: index_term_at(bytes, 8),
        subject: index_term_at(bytes, 16),
        graph: index_term_at(bytes, 24),
    })
}

fn decode_ospg_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        object: index_term_at(bytes, 0),
        subject: index_term_at(bytes, 8),
        predicate: index_term_at(bytes, 16),
        graph: index_term_at(bytes, 24),
    })
}

fn decode_gosp_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: index_term_at(bytes, 0),
        object: index_term_at(bytes, 8),
        subject: index_term_at(bytes, 16),
        predicate: index_term_at(bytes, 24),
    })
}

fn source_term_at(bytes: &[u8], offset: usize) -> TermId {
    TermId::from_be_bytes(
        bytes[offset..offset + 16]
            .try_into()
            .expect("fixed source term slice"),
    )
}

fn decode_query_id(bytes: &[u8], context: &'static str) -> Result<QueryTermId> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StoreError::InvalidIndexEncoding {
            context,
            message: format!("expected 8 bytes, found {}", bytes.len()),
        })?;
    Ok(QueryTermId::from_be_bytes(raw))
}

fn decode_source_id(bytes: &[u8], context: &'static str) -> Result<TermId> {
    let raw: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StoreError::InvalidIndexEncoding {
            context,
            message: format!("expected 16 bytes, found {}", bytes.len()),
        })?;
    Ok(TermId::from_be_bytes(raw))
}

fn decode_source_quad(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        graph: source_term_at(bytes, 0),
        subject: source_term_at(bytes, 16),
        predicate: source_term_at(bytes, 32),
        object: source_term_at(bytes, 48),
    })
}

fn coalesced_transitions(mutations: &[QuadMutation]) -> Vec<NetQuadTransition> {
    let mut transitions = BTreeMap::<QuadKey, NetQuadTransition>::new();
    for mutation in mutations {
        let (quad, is_live) = match mutation {
            QuadMutation::Insert(quad) => (*quad, true),
            QuadMutation::Remove(quad) => (*quad, false),
        };
        let key = GraphStore::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
        if let Some(existing) = transitions.get_mut(&key) {
            existing.is_live = is_live;
        } else {
            transitions.insert(
                key,
                NetQuadTransition {
                    quad,
                    was_live: !is_live,
                    is_live,
                },
            );
        }
    }
    transitions
        .into_values()
        .filter(|transition| transition.was_live != transition.is_live)
        .collect()
}

fn live_counter_keys(quad: QueryQuad) -> [IndexCounterKey; 6] {
    [
        IndexCounterKey::Total,
        IndexCounterKey::Graph(quad.graph),
        IndexCounterKey::Predicate(quad.predicate),
        IndexCounterKey::GraphPredicate(quad.graph, quad.predicate),
        IndexCounterKey::PredicateObject(quad.predicate, quad.object),
        IndexCounterKey::GraphPredicateObject(quad.graph, quad.predicate, quad.object),
    ]
}

struct IndexVerifyBuilder {
    report: QueryIndexVerification,
}

struct CounterCheck<'a> {
    snapshot: &'a Snapshot,
    spaces: IndexSpaces<'a>,
    key: IndexCounterKey,
    expected: u64,
    problems: (&'static str, &'static str),
}

#[derive(Clone, Copy)]
enum IndexVerifyState {
    Ready,
    BuildingCandidate,
}

#[derive(Clone, Copy)]
enum IndexKeyOrder {
    Gspo,
    Gpos,
    Spog,
    Posg,
    Ospg,
    Gosp,
}

/// The physical order selected for one trusted qv2 range. This remains
/// crate-private so query readers never learn Fjall keyspace details.
#[derive(Clone, Copy)]
pub(crate) enum IndexCursorOrder {
    Gspo,
    Gpos,
    Spog,
    Posg,
    Ospg,
    Gosp,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryIndexAdmission {
    pub(crate) trusted: bool,
    pub(crate) query_id_generation: Option<u64>,
    pub(crate) query_id_limit: Option<u64>,
    pub(crate) fallback_reason: Option<&'static str>,
    pub(crate) header_reads: u64,
    pub(crate) counter_reads: u64,
    pub(crate) debt_reads: u64,
}

/// One immutable, publication-coherent durable read view.
#[derive(Clone)]
pub(crate) struct StoreReadSnapshot {
    snapshot: Snapshot,
    /// The active index slot is fixed for this snapshot's sequence, so the
    /// header is decoded at most once per snapshot.
    active_slot: OnceCell<Option<IndexSlot>>,
    /// Graph metadata, clock and diagnostics records by key, read at most once per snapshot.
    graph_records: RefCell<HashMap<[u8; 17], Option<fjall::Slice>>>,
    /// Set once `graph_records` holds every graph record, so a missing key means absent.
    graph_records_complete: Cell<bool>,
    /// Graph names decoded for this snapshot, so policy reads skip a dictionary read.
    graph_names: RefCell<HashMap<String, TermId>>,
}

impl From<&Snapshot> for StoreReadSnapshot {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            snapshot: snapshot.clone(),
            active_slot: OnceCell::new(),
            graph_records: RefCell::default(),
            graph_records_complete: Cell::new(false),
            graph_names: RefCell::default(),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum QvStat {
    Graph(TermId),
    Total,
    UnionUnique,
    Predicate(TermId),
    PredicateObject(TermId, TermId),
    GraphPredicate(TermId, TermId),
    GraphPredicateObject(TermId, TermId, TermId),
}

pub(crate) struct QvRead<'a> {
    pub(crate) stat: QvStat,
    pub(crate) costs: &'a crate::query::context::QueryCost,
}

#[derive(Clone)]
pub(crate) struct SearchSnapshot {
    snapshot: Snapshot,
    terms: Keyspace,
    quads: Keyspace,
    graphs: Keyspace,
}

#[derive(Clone)]
pub(crate) struct SearchManifestSnapshot {
    snapshot: Snapshot,
    terms: Keyspace,
    graphs: Keyspace,
    search_meta: Keyspace,
    search_queue: Keyspace,
}

impl SearchManifestSnapshot {
    pub(crate) fn digest(&self, index_id: [u8; 16]) -> Result<ManifestDigest> {
        let manifest = self
            .snapshot
            .get(&self.search_meta, SEARCH_MANIFEST_KEY)?
            .and_then(|value| postcard::from_bytes::<StoredManifest>(value.as_ref()).ok());
        Ok(manifest
            .filter(|manifest| {
                manifest.format == SEARCH_META_FORMAT && manifest.index_id == index_id
            })
            .map(|manifest| ManifestDigest {
                count: manifest.count,
                hash: manifest.hash,
                epoch: manifest.epoch,
            })
            .unwrap_or(ManifestDigest {
                count: 0,
                hash: [0; 32],
                epoch: 0,
            }))
    }

    pub(crate) fn oldest_owed(&self) -> Result<Option<u64>> {
        match self
            .snapshot
            .prefix(&self.search_queue, [SEARCH_ORDER_PREFIX])
            .next()
        {
            Some(guard) => {
                let (key, _) = guard.into_inner()?;
                Ok(Some(decode_search_order(key.as_ref())?.token))
            }
            None => Ok(None),
        }
    }
}

impl SearchSnapshot {
    pub(crate) fn scan_graph(&self, scan: &GraphScan) -> Result<QuadPage> {
        scan_graph_snapshot(&self.snapshot, &self.quads, scan)
    }

    pub(crate) fn subject_present(&self, graph: TermId, subject: TermId) -> Result<bool> {
        let mut prefix = [0u8; 32];
        prefix[..16].copy_from_slice(&graph.to_be_bytes());
        prefix[16..].copy_from_slice(&subject.to_be_bytes());
        for guard in self.snapshot.prefix(&self.quads, prefix) {
            let (_, value) = guard.into_inner()?;
            if !dots_empty(value.as_ref()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn scan_subject(
        &self,
        scan: &SubjectScan,
        predicates: &[TermId; 8],
    ) -> Result<SubjectPage> {
        let mut prefix = [0u8; 32];
        prefix[..16].copy_from_slice(&scan.graph.to_be_bytes());
        prefix[16..].copy_from_slice(&scan.subject.to_be_bytes());
        let mut entries = Vec::new();
        let mut next = scan.after;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut remaining = false;
        let mut oversized = None;
        let candidates = predicates
            .iter()
            .filter(|predicate| scan.after.is_none_or(|(after, _)| **predicate >= after))
            .flat_map(|predicate| {
                let mut scoped = [0u8; 48];
                scoped[..32].copy_from_slice(&prefix);
                scoped[32..].copy_from_slice(&predicate.to_be_bytes());
                let after = scan
                    .after
                    .filter(|(after, _)| after == predicate)
                    .map(|(_, object)| [&scoped[..], &object.to_be_bytes()].concat());
                let range = resume_range(&scoped, after.as_deref());
                self.snapshot.range(&self.quads, range)
            });
        for guard in candidates {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = GraphStore::decode_quad_key(key.as_ref())?;
            let cursor = (quad.predicate, quad.object);
            if rows == scan.row_limit {
                remaining = true;
                break;
            }
            let predicate_bytes =
                self.snapshot
                    .size_of(&self.terms, quad.predicate.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(quad.predicate.0))? as usize;
            let object_bytes =
                self.snapshot
                    .size_of(&self.terms, quad.object.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(quad.object.0))? as usize;
            let encoded = predicate_bytes.saturating_add(object_bytes);
            if bytes.saturating_add(encoded) > scan.byte_limit {
                if encoded > scan.byte_limit {
                    oversized = Some(OversizedSource {
                        bytes: encoded,
                        limit: scan.byte_limit,
                    });
                }
                remaining = true;
                break;
            }
            let predicate = snapshot_term(&self.snapshot, &self.terms, quad.predicate)?;
            let object = snapshot_term(&self.snapshot, &self.terms, quad.object)?;
            entries.push((predicate, object));
            next = Some(cursor);
            rows += 1;
            bytes = bytes.saturating_add(encoded);
        }
        Ok(SubjectPage {
            entries,
            next,
            remaining,
            rows,
            bytes,
            oversized,
        })
    }

    pub(crate) fn orphaned_ids(&self, graph: TermId, byte_limit: usize) -> Result<HashSet<TermId>> {
        let Some(meta) = self.snapshot.get(&self.graphs, graph_meta_key(graph))? else {
            return Ok(HashSet::new());
        };
        let clock = match self.snapshot.get(&self.graphs, graph_clock_key(graph))? {
            Some(value) => postcard::from_bytes(value.as_ref())?,
            None => postcard::from_bytes::<StoredGraphMeta>(meta.as_ref())?.clock,
        };
        let value = self
            .snapshot
            .get(&self.graphs, graph_diagnostics_key(graph))?
            .ok_or(StoreError::InvalidSearchState("search-diagnostics-missing"))?;
        if value.len() > byte_limit {
            return Err(StoreError::LimitExceeded {
                resource: "search diagnostics bytes",
                limit: u64::try_from(byte_limit).unwrap_or(u64::MAX),
                actual: u64::try_from(value.len()).unwrap_or(u64::MAX),
            });
        }
        let record: StoredDiagnostics = postcard::from_bytes(value.as_ref())?;
        if record.at_clock != clock {
            return Err(StoreError::InvalidSearchState("search-diagnostics-stale"));
        }
        let row_limit = byte_limit / 64;
        if record.diagnostics.orphaned_entities.len() > row_limit {
            return Err(StoreError::LimitExceeded {
                resource: "search diagnostics rows",
                limit: u64::try_from(row_limit).unwrap_or(u64::MAX),
                actual: u64::try_from(record.diagnostics.orphaned_entities.len())
                    .unwrap_or(u64::MAX),
            });
        }
        let mut orphaned = HashSet::new();
        let mut bytes = value.len();
        for entity in record.diagnostics.orphaned_entities {
            bytes = bytes.saturating_add(entity.len()).saturating_add(16);
            if bytes > byte_limit {
                return Err(StoreError::LimitExceeded {
                    resource: "search diagnostics bytes",
                    limit: u64::try_from(byte_limit).unwrap_or(u64::MAX),
                    actual: u64::try_from(bytes).unwrap_or(u64::MAX),
                });
            }
            let term = EncodedTerm::from_subject_id(&entity);
            let id = hash_term(&term);
            if self
                .snapshot
                .get(&self.terms, id.to_be_bytes())?
                .is_some_and(|stored| stored.as_ref() == term.0.as_bytes())
            {
                orphaned.insert(id);
            }
        }
        Ok(orphaned)
    }
}

fn snapshot_term(snapshot: &Snapshot, terms: &Keyspace, id: TermId) -> Result<EncodedTerm> {
    let value = snapshot
        .get(terms, id.to_be_bytes())?
        .ok_or(StoreError::TermNotFound(id.0))?;
    Ok(EncodedTerm(decode_term_text(value.as_ref())?))
}

/// Keys under `prefix`, starting strictly after `after` when resuming a page.
fn resume_range(prefix: &[u8], after: Option<&[u8]>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let lower = after.map_or_else(|| Included(prefix.to_vec()), |key| Excluded(key.to_vec()));
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return (lower, Excluded(end));
        }
    }
    (lower, Unbounded)
}

fn scan_graph_snapshot(
    snapshot: &Snapshot,
    quads: &Keyspace,
    scan: &GraphScan,
) -> Result<QuadPage> {
    let mut entries = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let mut oversized = None;
    let after = scan.after.as_ref().map(|after| &after[..]);
    for guard in snapshot.range(quads, resume_range(&scan.graph.to_be_bytes(), after)) {
        let (key, value) = guard.into_inner()?;
        let raw: [u8; 64] = key
            .as_ref()
            .try_into()
            .map_err(|_| StoreError::InvalidSearchState("graph-scan-key-invalid"))?;
        if dots_empty(value.as_ref()) {
            continue;
        }
        if rows == scan.row_limit {
            break;
        }
        let encoded = key.len().saturating_add(value.len());
        if bytes.saturating_add(encoded) > scan.byte_limit {
            if encoded > scan.byte_limit {
                oversized = Some(OversizedSource {
                    bytes: encoded,
                    limit: scan.byte_limit,
                });
            }
            break;
        }
        entries.push(GraphStore::decode_quad_key(&raw)?);
        rows += 1;
        bytes = bytes.saturating_add(encoded);
    }
    Ok(QuadPage {
        entries,
        rows,
        bytes,
        oversized,
    })
}

impl IndexVerifyBuilder {
    fn new(full: bool) -> Self {
        Self {
            report: QueryIndexVerification {
                full,
                valid: true,
                source_live_quads: 0,
                indexed_quads: 0,
                checked_source_rows: 0,
                checked_index_rows: 0,
                problems: Vec::new(),
            },
        }
    }

    fn problem(&mut self, problem: &'static str) {
        self.report.valid = false;
        if self.report.problems.len() < QV_PROBLEM_LIMIT
            && !self
                .report
                .problems
                .iter()
                .any(|current| current == problem)
        {
            self.report.problems.push(problem.to_owned());
        }
    }

    fn finish(self) -> QueryIndexVerification {
        self.report
    }
}

impl StoreReadSnapshot {
    #[must_use]
    pub(crate) fn sequence(&self) -> u64 {
        self.snapshot.seqno()
    }

    #[must_use]
    pub(crate) fn snapshot_ref(&self) -> &Snapshot {
        &self.snapshot
    }

    pub(crate) fn raw_quad_cursor(
        &self,
        store: &GraphStore,
        pattern: crate::rdf_read::QuadPattern,
    ) -> crate::query::cursor::RawQuadCursor {
        store
            .derived_raw_cursor(self.sequence(), pattern)
            .unwrap_or_else(|| {
                crate::query::cursor::RawQuadCursor::new(
                    self.snapshot.clone(),
                    &store.quads,
                    pattern,
                )
            })
    }

    pub(crate) fn source_quad_cursor(
        &self,
        store: &GraphStore,
        pattern: crate::rdf_read::QuadPattern,
    ) -> crate::query::cursor::RawQuadCursor {
        crate::query::cursor::RawQuadCursor::new(self.snapshot.clone(), &store.quads, pattern)
    }

    pub(crate) fn raw_quad_point(
        &self,
        store: &GraphStore,
        quad: EncodedQuad,
    ) -> Result<Option<crate::query::cursor::RawQuadCandidate>> {
        crate::query::cursor::point_candidate(&self.snapshot, &store.quads, quad)
    }

    pub(crate) fn query_index_cursor(
        &self,
        store: &GraphStore,
        scan: &crate::query::cursor::IndexScan<'_>,
    ) -> Result<crate::query::cursor::RawQuadCursor> {
        let Some((keyspace, query_to_term, prefix)) = store.query_index_range(self, scan)? else {
            return Ok(crate::query::cursor::RawQuadCursor::empty());
        };
        Ok(crate::query::cursor::RawQuadCursor::query_index(
            self.snapshot.clone(),
            crate::query::cursor::QueryIndexScan {
                keyspace,
                query_to_term,
                order: scan.order,
                prefix,
            },
        ))
    }

    pub(crate) fn index_key_cursor(
        &self,
        store: &GraphStore,
        scan: &crate::query::cursor::IndexScan<'_>,
    ) -> Result<Option<crate::query::cursor::RawIndexCursor>> {
        let resolve = |term: Option<TermId>| -> Result<Option<Option<QueryTermId>>> {
            match term {
                Some(term) => {
                    let term = store.snapshot_query_id(self, term)?;
                    scan.costs.forward_mapping(
                        16 + term.map_or(0, |_| std::mem::size_of::<u64>() as u64),
                    );
                    Ok(term.map(Some))
                }
                None => Ok(Some(None)),
            }
        };
        let Some(graph) = resolve(scan.pattern.graph)? else {
            return Ok(None);
        };
        let Some(subject) = resolve(scan.pattern.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = resolve(scan.pattern.predicate)? else {
            return Ok(None);
        };
        let Some(object) = resolve(scan.pattern.object)? else {
            return Ok(None);
        };
        let Some((keyspace, query_to_term, prefix)) = store.query_index_range(self, scan)? else {
            return Ok(None);
        };
        let filter = crate::query::cursor::RawIndexPattern::new(graph, subject, predicate, object)
            .without_prefix(scan.order, prefix.len() / 8);
        Ok(Some(
            crate::query::cursor::RawIndexCursor::new(
                self.snapshot.clone(),
                crate::query::cursor::RawIndexScan {
                    keyspace,
                    query_to_term,
                    order: scan.order,
                    prefix,
                    pattern: filter,
                    query_id_limit: scan
                        .query_id_limit
                        .expect("index-key scan requires a dense-ID upper bound"),
                },
            )
            .track_costs(scan.costs.clone()),
        ))
    }

    pub(crate) fn query_index_admission(&self, store: &GraphStore) -> Result<QueryIndexAdmission> {
        store.snapshot_admission(self.snapshot_ref())
    }

    pub(crate) fn query_term_id(
        &self,
        store: &GraphStore,
        term: TermId,
    ) -> Result<(Option<QueryTermId>, Option<u64>)> {
        let Some(spaces) = store.active_query_spaces(self)? else {
            return Ok((None, None));
        };
        let value = self
            .snapshot
            .get(spaces.term_to_query, term.to_be_bytes())?;
        let bytes = 16 + value.as_ref().map_or(0, |value| value.len() as u64);
        let term = value
            .map(|value| decode_query_id(value.as_ref(), "term-to-query mapping"))
            .transpose()?;
        Ok((term, Some(bytes)))
    }

    #[cfg(test)]
    pub(crate) fn qv_total_count(&self, store: &GraphStore) -> Result<Option<u64>> {
        self.qv_count(store, IndexCounterKey::Total, false)
    }

    pub(crate) fn qv_stat(&self, store: &GraphStore, read: &QvRead<'_>) -> Result<Option<u64>> {
        let map = |term: TermId| -> Result<Option<QueryTermId>> {
            read.costs.planner_points(1);
            let Some(spaces) = store.active_query_spaces(self)? else {
                return Ok(None);
            };
            let value = self
                .snapshot
                .get(spaces.term_to_query, term.to_be_bytes())?;
            read.costs
                .forward_mapping(16 + value.as_ref().map_or(0, |value| value.len() as u64));
            value
                .map(|value| decode_query_id(value.as_ref(), "term-to-query mapping"))
                .transpose()
        };
        let (key, zero_missing) = match read.stat {
            QvStat::Graph(graph) => {
                let Some(graph) = map(graph)? else {
                    return Ok(Some(0));
                };
                (IndexCounterKey::Graph(graph), false)
            }
            QvStat::Total => (IndexCounterKey::Total, false),
            QvStat::UnionUnique => (IndexCounterKey::UnionDuplicateFree, false),
            QvStat::Predicate(predicate) => {
                let Some(predicate) = map(predicate)? else {
                    return Ok(Some(0));
                };
                (IndexCounterKey::Predicate(predicate), true)
            }
            QvStat::PredicateObject(predicate, object) => {
                let Some(predicate) = map(predicate)? else {
                    return Ok(Some(0));
                };
                let Some(object) = map(object)? else {
                    return Ok(Some(0));
                };
                (IndexCounterKey::PredicateObject(predicate, object), true)
            }
            QvStat::GraphPredicate(graph, predicate) => {
                let Some(graph) = map(graph)? else {
                    return Ok(Some(0));
                };
                let Some(predicate) = map(predicate)? else {
                    return Ok(Some(0));
                };
                (IndexCounterKey::GraphPredicate(graph, predicate), true)
            }
            QvStat::GraphPredicateObject(graph, predicate, object) => {
                let Some(graph) = map(graph)? else {
                    return Ok(Some(0));
                };
                let Some(predicate) = map(predicate)? else {
                    return Ok(Some(0));
                };
                let Some(object) = map(object)? else {
                    return Ok(Some(0));
                };
                (
                    IndexCounterKey::GraphPredicateObject(graph, predicate, object),
                    true,
                )
            }
        };
        read.costs.planner_points(2);
        match store.snapshot_counter(self.snapshot_ref(), key)? {
            IndexCounterRead::Value(count) => Ok(Some(count)),
            IndexCounterRead::Missing if zero_missing => Ok(Some(0)),
            IndexCounterRead::Missing | IndexCounterRead::Malformed => Ok(None),
        }
    }

    #[cfg(test)]
    fn qv_count(
        &self,
        store: &GraphStore,
        key: IndexCounterKey,
        zero_missing: bool,
    ) -> Result<Option<u64>> {
        match store.snapshot_counter(self.snapshot_ref(), key)? {
            IndexCounterRead::Value(count) => Ok(Some(count)),
            IndexCounterRead::Missing if zero_missing => Ok(Some(0)),
            IndexCounterRead::Missing | IndexCounterRead::Malformed => Ok(None),
        }
    }

    pub(crate) fn contains_graph_id(&self, store: &GraphStore, graph: TermId) -> Result<bool> {
        Ok(self.graph_meta(store, graph)?.is_some())
    }

    fn graph_meta(&self, store: &GraphStore, graph: TermId) -> Result<Option<fjall::Slice>> {
        self.graph_record(store, graph_meta_key(graph))
    }

    fn graph_record(&self, store: &GraphStore, key: [u8; 17]) -> Result<Option<fjall::Slice>> {
        if let Some(record) = self.graph_records.borrow().get(&key) {
            return Ok(record.clone());
        }
        if self.graph_records_complete.get() {
            return Ok(None);
        }
        let record = self.snapshot.get(&store.graphs, key)?;
        self.graph_records.borrow_mut().insert(key, record.clone());
        Ok(record)
    }

    /// Reads all graph metadata, clock and diagnostics records with three range scans. Keeps
    /// nothing and returns `false` past `limit` graphs or the byte cap, bounding the work.
    pub(crate) fn prefetch_graph_records(&self, store: &GraphStore, limit: usize) -> Result<bool> {
        if self.graph_records_complete.get() {
            return Ok(true);
        }
        let mut bytes = 0_usize;
        let mut records = Vec::new();
        for prefix in [
            GRAPH_META_PREFIX,
            GRAPH_CLOCK_PREFIX,
            GRAPH_DIAGNOSTICS_PREFIX,
        ] {
            let mut graphs = 0_usize;
            for guard in self.snapshot.prefix(&store.graphs, [prefix]) {
                let (key, value) = guard.into_inner()?;
                let Ok(key) = <[u8; 17]>::try_from(key.as_ref()) else {
                    continue;
                };
                graphs += 1;
                bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                if graphs > limit || bytes > PREFETCH_RECORD_BYTES {
                    return Ok(false);
                }
                records.push((key, value));
            }
        }
        let mut memo = self.graph_records.borrow_mut();
        for (key, value) in records {
            memo.insert(key, Some(value));
        }
        self.graph_records_complete.set(true);
        Ok(true)
    }

    fn vector_clock(&self, store: &GraphStore, graph: TermId) -> Result<VectorClock> {
        if let Some(bytes) = self.graph_record(store, graph_clock_key(graph))? {
            return Ok(postcard::from_bytes(bytes.as_ref())?);
        }
        Ok(self
            .graph_meta(store, graph)?
            .map(|bytes| postcard::from_bytes::<StoredGraphMeta>(bytes.as_ref()))
            .transpose()?
            .unwrap_or_default()
            .clock)
    }

    /// Records a graph name decoded from this snapshot's dictionary.
    pub(crate) fn remember_graph_name(&self, graph: &GraphId, term: TermId) {
        self.graph_names
            .borrow_mut()
            .insert(graph.as_str().to_owned(), term);
    }

    pub(crate) fn graph_version(&self, store: &GraphStore, graph: TermId) -> Result<[u8; 32]> {
        let clock = store.snapshot_vector_clock(&self.snapshot, graph)?;
        Ok(*blake3::hash(&postcard::to_allocvec(&clock)?).as_bytes())
    }

    pub(crate) fn graph_term_iter<'a>(
        &'a self,
        store: &'a GraphStore,
    ) -> impl Iterator<Item = Result<TermId>> + 'a {
        self.snapshot
            .prefix(&store.graphs, graph_meta_prefix())
            .filter_map(|guard| match guard.into_inner() {
                Ok((key, _)) if key.len() == 17 => {
                    Some(decode_term_id(&key[1..17], "graph meta key"))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error.into())),
            })
    }

    pub(crate) fn lookup_term(
        &self,
        store: &GraphStore,
        term: &EncodedTerm,
    ) -> Result<Option<TermId>> {
        let id = hash_term(term);
        let Some(existing) = self.snapshot.get(&store.terms, id.to_be_bytes())? else {
            return Ok(None);
        };
        if existing.as_ref() == term.0.as_bytes() {
            return Ok(Some(id));
        }
        Err(StoreError::TermCollision {
            attempted: term.0.clone(),
            existing: decode_term_text(existing.as_ref())?,
        })
    }

    pub(crate) fn graph_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> Result<Option<GraphPolicy>> {
        #[cfg(test)]
        if store
            .policy_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            == Some(graph)
        {
            return Err(StoreError::Fjall(fjall::Error::Io(std::io::Error::other(
                "injected policy read failure",
            ))));
        }
        let known = self.graph_names.borrow().get(graph.as_str()).copied();
        let graph = match known {
            Some(graph) => graph,
            None => match self.lookup_term(store, &EncodedTerm::from_named_node(&graph.0))? {
                Some(graph) => graph,
                None => return Ok(None),
            },
        };
        let Some(bytes) = self.graph_meta(store, graph)? else {
            return Ok(None);
        };
        Ok(Some(
            postcard::from_bytes::<StoredGraphMeta>(bytes.as_ref())?.policy,
        ))
    }

    /// Returns orphan ids derived from this exact snapshot without persisting reads.
    pub(crate) fn orphaned_entity_ids(
        &self,
        store: &GraphStore,
        context: &crate::query::context::ReadContext<'_>,
        graph: TermId,
    ) -> Result<HashSet<TermId>> {
        if !self.contains_graph_id(store, graph)? {
            return Ok(HashSet::new());
        }
        let clock = self.vector_clock(store, graph)?;
        let stored = self
            .graph_record(store, graph_diagnostics_key(graph))?
            .map(|bytes| postcard::from_bytes::<StoredDiagnostics>(bytes.as_ref()))
            .transpose()?;
        if let Some(record) = stored
            && record.at_clock == clock
        {
            let mut orphaned = HashSet::with_capacity(record.diagnostics.orphaned_entities.len());
            context.check_cancelled()?;
            for (index, entity) in record.diagnostics.orphaned_entities.into_iter().enumerate() {
                if index != 0 && index % 1_024 == 0 {
                    context.check_cancelled()?;
                }
                if let Some(term) =
                    self.lookup_term(store, &EncodedTerm::from_subject_id(&entity))?
                {
                    orphaned.insert(term);
                }
            }
            return Ok(orphaned);
        }

        store.diagnostics_computed.fetch_add(1, Ordering::Relaxed);
        let id = |named_node: oxrdf::NamedNode| {
            self.lookup_term(store, &EncodedTerm::from_named_node(&named_node))
        };
        let vocab = OrphanVocab {
            rdf_type: id(crate::core::vocab::rdf_type())?,
            data_types: [
                id(crate::core::vocab::schema_dataset())?,
                id(crate::core::vocab::schema_media_object())?,
            ],
            has_part: id(crate::core::vocab::schema_has_part())?,
        };
        store.snapshot_orphan_ids(&self.snapshot, context, graph, &vocab)
    }
}

impl GraphStore {
    fn query_spaces(&self, slot: IndexSlot) -> IndexSpaces<'_> {
        match slot {
            IndexSlot::Primary => IndexSpaces {
                gspo: &self.qv2_gspo,
                gpos: &self.qv2_gpos,
                spog: &self.qv2_spog,
                posg: &self.qv2_posg,
                ospg: &self.qv2_ospg,
                gosp: &self.qv2_gosp,
                term_to_query: &self.primary_term_map,
                query_to_term: &self.primary_query_map,
                meta: &self.qv2_meta,
            },
            IndexSlot::Secondary => IndexSpaces {
                gspo: &self.qv3_gspo,
                gpos: &self.qv3_gpos,
                spog: &self.qv3_spog,
                posg: &self.qv3_posg,
                ospg: &self.qv3_ospg,
                gosp: &self.qv3_gosp,
                term_to_query: &self.secondary_term_map,
                query_to_term: &self.secondary_query_map,
                meta: &self.qv3_meta,
            },
        }
    }

    fn active_query_spaces(&self, view: &StoreReadSnapshot) -> Result<Option<IndexSpaces<'_>>> {
        let slot = match view.active_slot.get() {
            Some(slot) => *slot,
            None => {
                let slot = match self.snapshot_index_header(&view.snapshot)? {
                    IndexHeaderRead::Valid(header) => IndexSlot::decode(header.active_slot),
                    IndexHeaderRead::Legacy(_) => Some(IndexSlot::Primary),
                    IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
                };
                *view.active_slot.get_or_init(|| slot)
            }
        };
        Ok(slot.map(|slot| self.query_spaces(slot)))
    }

    fn term_lock_index(&self, id: TermId) -> usize {
        (id.0 as usize) % self.term_locks.len()
    }

    // Locking.

    /// Serializes the complete read-write-commit cycle for one graph.
    pub(crate) fn graph_commit_guard(&self, graph: &GraphId) -> GraphCommitGuard<'_> {
        self.graph_id_guard(hash_term(&EncodedTerm::from_named_node(&graph.0)))
    }

    /// Id-keyed twin of [`GraphStore::graph_commit_guard`]. The shard is chosen
    /// from the graph term id, so both entry points map to the same lock.
    pub(crate) fn graph_id_guard(&self, graph_id: TermId) -> GraphCommitGuard<'_> {
        let shard = (graph_id.0 as usize) % self.commit_locks.len();
        #[cfg(feature = "shacl-core")]
        let wait_started = Instant::now();
        let guard = self.commit_locks[shard]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(feature = "shacl-core")]
        self.graph_lock_wait
            .fetch_add(elapsed_ns(wait_started.elapsed()), Ordering::Relaxed);
        GraphCommitGuard(guard)
    }

    /// Order publication and local apply without coupling independent stores.
    pub(crate) fn graph_write_guard(&self, graph: &GraphId) -> GraphWriteGuard<'_> {
        let hash = blake3::hash(graph.as_str().as_bytes());
        let shard = u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap()) as usize;
        let mutex = &self.write_locks[shard % self.write_locks.len()];
        let lock = std::ptr::from_ref(mutex) as usize;
        if HELD_WRITE_LOCKS.with(|held| held.borrow().contains(&lock)) {
            return GraphWriteGuard { guard: None, lock };
        }
        let guard = mutex.lock().unwrap_or_else(PoisonError::into_inner);
        HELD_WRITE_LOCKS.with(|held| held.borrow_mut().push(lock));
        GraphWriteGuard {
            guard: Some(guard),
            lock,
        }
    }

    /// Hold through receipt staging and source commit for one stable identity.
    pub(crate) fn receipt_guard(&self, id: &MutationId) -> ReceiptGuard<'_> {
        let shard = u64::from_be_bytes(id.0[..8].try_into().unwrap()) as usize;
        ReceiptGuard(
            self.receipt_locks[shard % self.receipt_locks.len()]
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn binding_guard(&self) -> BindingGuard<'_> {
        let wait_started = Instant::now();
        let guard = self
            .binding_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.binding_wait_ns
            .fetch_add(elapsed_ns(wait_started.elapsed()), Ordering::Relaxed);
        BindingGuard {
            guard,
            hold_started: Instant::now(),
            hold_ns: &self.binding_hold_ns,
        }
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn record_validation(&self, elapsed: Duration) {
        self.validation_ns
            .fetch_add(elapsed_ns(elapsed), Ordering::Relaxed);
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn record_settlement(&self, elapsed: Duration) {
        self.settlement_ns
            .fetch_add(elapsed_ns(elapsed), Ordering::Relaxed);
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn record_settlement_failure(&self) {
        self.settlement_failures.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn record_status_read(&self, bindings: u64, version_checks: u64) {
        self.status_bindings_read
            .fetch_add(bindings, Ordering::Relaxed);
        self.status_version_checks
            .fetch_add(version_checks, Ordering::Relaxed);
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn shacl_runtime_statistics(&self) -> crate::ShaclRuntimeStatistics {
        crate::ShaclRuntimeStatistics {
            binding_lock_wait_ns: self.binding_wait_ns.load(Ordering::Relaxed),
            binding_lock_hold_ns: self.binding_hold_ns.load(Ordering::Relaxed),
            graph_commit_lock_wait_ns: self.graph_lock_wait.load(Ordering::Relaxed),
            validation_ns: self.validation_ns.load(Ordering::Relaxed),
            settlement_ns: self.settlement_ns.load(Ordering::Relaxed),
            settlement_failures: self.settlement_failures.load(Ordering::Relaxed),
            status_bindings_read: self.status_bindings_read.load(Ordering::Relaxed),
            status_version_checks: self.status_version_checks.load(Ordering::Relaxed),
            status_shape_compilations: self.status_shape_compilations.load(Ordering::Relaxed),
            status_full_shape_scans: self.status_shape_scans.load(Ordering::Relaxed),
        }
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn validation_probe(&self) -> ValidationProbe<'_> {
        let active = self.validation_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.validation_max_active
            .fetch_max(active, Ordering::SeqCst);
        let stall = *self
            .validation_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !stall.is_zero() {
            std::thread::sleep(stall);
        }
        ValidationProbe {
            active: &self.validation_active,
        }
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn set_validation_stall(&self, stall: Duration) {
        *self
            .validation_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = stall;
        self.validation_max_active.store(0, Ordering::SeqCst);
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn validation_max_active(&self) -> usize {
        self.validation_max_active.load(Ordering::SeqCst)
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn validation_active(&self) -> usize {
        self.validation_active.load(Ordering::SeqCst)
    }

    fn indexes_read(&self) -> ReadGuard<'_, IndexState> {
        self.indexes.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn indexes_write(&self) -> WriteGuard<'_, IndexState> {
        self.indexes.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn snapshot_index_header(&self, snapshot: &Snapshot) -> Result<IndexHeaderRead> {
        let Some(bytes) = snapshot.get(&self.qv2_meta, QV_HEADER_KEY)? else {
            return Ok(IndexHeaderRead::Absent);
        };
        if bytes.len() >= 8 && bytes.as_ref()[..4] == QV_HEADER_MAGIC {
            let version = u32::from_be_bytes(bytes.as_ref()[4..8].try_into().unwrap());
            if version > QV_SCHEMA_VERSION {
                return Err(StoreError::UnsupportedIndexFormat {
                    found: version,
                    supported: QV_SCHEMA_VERSION,
                });
            }
        }
        let header = decode_index_header(bytes.as_ref());
        if let IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) = &header {
            self.generation_floor
                .fetch_max(header.query_id_generation, Ordering::SeqCst);
        }
        Ok(header)
    }

    /// Shared source mappings for dense cursors, keyed by query-ID generation.
    pub(crate) fn source_cache(&self) -> SourceCache {
        Arc::clone(&self.source_cache)
    }

    /// A generation above `previous` and every generation this process has seen. Shared
    /// source mappings stay exact even when a lost header restarts the stored count.
    fn next_generation(&self, previous: u64) -> u64 {
        let mut generation = previous.checked_add(1).unwrap_or(1);
        loop {
            let seen = self
                .generation_floor
                .fetch_max(generation, Ordering::SeqCst);
            if seen < generation {
                return generation;
            }
            generation = seen.checked_add(1).unwrap_or(1);
        }
    }

    fn query_header_digest(&self, snapshot: &Snapshot) -> Result<[u8; 32]> {
        let mut hash = blake3::Hasher::new();
        match snapshot.get(&self.qv2_meta, QV_HEADER_KEY)? {
            Some(value) => {
                hash.update(&[1]);
                hash.update(value.as_ref());
            }
            None => {
                hash.update(&[0]);
            }
        }
        Ok(*hash.finalize().as_bytes())
    }

    fn stage_index_header(&self, batch: &mut fjall::OwnedWriteBatch, header: &IndexHeader) {
        batch.insert(&self.qv2_meta, QV_HEADER_KEY, encode_index_header(header));
    }

    fn stage_index_failure(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        previous: Option<&IndexHeader>,
        reason: &'static str,
    ) {
        self.stage_index_header(batch, &IndexHeader::failed_from(previous, reason));
    }

    fn snapshot_query_id(
        &self,
        view: &StoreReadSnapshot,
        term: TermId,
    ) -> Result<Option<QueryTermId>> {
        let Some(spaces) = self.active_query_spaces(view)? else {
            return Ok(None);
        };
        view.snapshot
            .get(spaces.term_to_query, term.to_be_bytes())?
            .map(|value| decode_query_id(value.as_ref(), "term-to-query mapping"))
            .transpose()
    }

    #[cfg(test)]
    fn snapshot_query_quad(
        &self,
        view: &StoreReadSnapshot,
        quad: EncodedQuad,
    ) -> Result<Option<QueryQuad>> {
        let Some(spaces) = self.active_query_spaces(view)? else {
            return Ok(None);
        };
        self.spaces_query_quad(&view.snapshot, spaces, quad)
    }

    fn spaces_query_quad(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        quad: EncodedQuad,
    ) -> Result<Option<QueryQuad>> {
        let decode = |term: TermId| {
            snapshot
                .get(spaces.term_to_query, term.to_be_bytes())?
                .map(|value| decode_query_id(value.as_ref(), "term-to-query mapping"))
                .transpose()
        };
        let Some(graph) = decode(quad.graph)? else {
            return Ok(None);
        };
        let Some(subject) = decode(quad.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = decode(quad.predicate)? else {
            return Ok(None);
        };
        let Some(object) = decode(quad.object)? else {
            return Ok(None);
        };
        Ok(Some(QueryQuad {
            graph,
            subject,
            predicate,
            object,
        }))
    }

    fn spaces_source_quad(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        quad: QueryQuad,
    ) -> Result<Option<EncodedQuad>> {
        let decode = |term: QueryTermId| {
            snapshot
                .get(spaces.query_to_term, term.to_be_bytes())?
                .map(|value| decode_source_id(value.as_ref(), "query-to-term mapping"))
                .transpose()
        };
        let Some(graph) = decode(quad.graph)? else {
            return Ok(None);
        };
        let Some(subject) = decode(quad.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = decode(quad.predicate)? else {
            return Ok(None);
        };
        let Some(object) = decode(quad.object)? else {
            return Ok(None);
        };
        Ok(Some(EncodedQuad {
            graph,
            subject,
            predicate,
            object,
        }))
    }

    fn count_live_rows(&self, snapshot: &Snapshot) -> Result<u64> {
        let mut rows = 0u64;
        for guard in snapshot.iter(&self.quads) {
            let (_, value) = guard.into_inner()?;
            if !dots_empty(value.as_ref()) {
                rows = rows
                    .checked_add(1)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "source-row-count-overflow",
                    ))?;
            }
        }
        Ok(rows)
    }

    fn summarize_qv_rows(&self, snapshot: &Snapshot, keyspace: &Keyspace) -> Result<(u64, bool)> {
        let mut rows = 0u64;
        let mut well_formed = true;
        for guard in snapshot.iter(keyspace) {
            let (key, value) = guard.into_inner()?;
            rows = rows
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "index-row-count-overflow",
                ))?;
            well_formed &= key.as_ref().len() == 32 && value.as_ref().is_empty();
        }
        Ok((rows, well_formed))
    }

    fn index_spaces_empty(&self, snapshot: &Snapshot) -> Result<bool> {
        for slot in [IndexSlot::Primary, IndexSlot::Secondary] {
            let spaces = self.query_spaces(slot);
            for keyspace in [
                spaces.gspo,
                spaces.gpos,
                spaces.spog,
                spaces.posg,
                spaces.ospg,
                spaces.gosp,
                spaces.term_to_query,
                spaces.query_to_term,
                spaces.meta,
            ] {
                if let Some(guard) = snapshot.iter(keyspace).next() {
                    let _ = guard.into_inner()?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Captures the durable source/qv authority. Cache publication may follow
    /// it; generation checks keep stale cache entries off this read path.
    pub(crate) fn read_snapshot(&self) -> StoreReadSnapshot {
        StoreReadSnapshot::from(&self.db.snapshot())
    }

    pub(crate) fn search_snapshot(&self) -> SearchSnapshot {
        SearchSnapshot {
            snapshot: self.db.snapshot(),
            terms: self.terms.clone(),
            quads: self.quads.clone(),
            graphs: self.graphs.clone(),
        }
    }

    pub(crate) fn search_manifest_snapshot(&self) -> Result<SearchManifestSnapshot> {
        let _queue = self.fts_queue_guard();
        Ok(SearchManifestSnapshot {
            snapshot: self.db.snapshot(),
            terms: self.terms.clone(),
            graphs: self.graphs.clone(),
            search_meta: self.search_meta.clone(),
            search_queue: self.search_queue.clone(),
        })
    }

    fn query_index_snapshot(&self) -> Snapshot {
        self.read_snapshot().snapshot
    }

    /// Tests QV eligibility solely from one execution snapshot.
    fn snapshot_admission(&self, snapshot: &Snapshot) -> Result<QueryIndexAdmission> {
        #[cfg(test)]
        self.index_admission_probes.fetch_add(1, Ordering::Relaxed);
        if self.projection_debt_present(snapshot)? {
            return Ok(QueryIndexAdmission {
                trusted: false,
                query_id_generation: None,
                query_id_limit: None,
                fallback_reason: Some("source-commit-projection-pending"),
                header_reads: 0,
                counter_reads: 0,
                debt_reads: 1,
            });
        }
        let header = match self.snapshot_index_header(snapshot)? {
            IndexHeaderRead::Absent => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    query_id_generation: None,
                    query_id_limit: None,
                    fallback_reason: Some("metadata-missing"),
                    header_reads: 1,
                    counter_reads: 0,
                    debt_reads: 1,
                });
            }
            IndexHeaderRead::Malformed => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    query_id_generation: None,
                    query_id_limit: None,
                    fallback_reason: Some("metadata-malformed"),
                    header_reads: 1,
                    counter_reads: 0,
                    debt_reads: 1,
                });
            }
            IndexHeaderRead::Legacy(_) => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    query_id_generation: None,
                    query_id_limit: None,
                    fallback_reason: Some("legacy-coverage-unverified"),
                    header_reads: 1,
                    counter_reads: 0,
                    debt_reads: 1,
                });
            }
            IndexHeaderRead::Valid(header) => header,
        };
        let mut admission = self.header_admission(snapshot, &header)?;
        admission.debt_reads += 1;
        Ok(admission)
    }

    fn header_admission(
        &self,
        snapshot: &Snapshot,
        header: &IndexHeader,
    ) -> Result<QueryIndexAdmission> {
        let fallback_reason = match &header.state {
            StoredIndexState::Building => Some("index-building"),
            StoredIndexState::Failed(_) => Some("index-failed"),
            StoredIndexState::Ready if !header.ready_is_coherent() => Some("metadata-incoherent"),
            StoredIndexState::Ready if !header.fits_snapshot(snapshot.seqno()) => {
                Some("metadata-ahead-of-snapshot")
            }
            StoredIndexState::Ready => None,
        };
        if let Some(fallback_reason) = fallback_reason {
            return Ok(QueryIndexAdmission {
                trusted: false,
                query_id_generation: None,
                query_id_limit: None,
                fallback_reason: Some(fallback_reason),
                header_reads: 1,
                counter_reads: 0,
                debt_reads: 0,
            });
        }
        #[cfg(test)]
        self.index_admission_probes.fetch_add(1, Ordering::Relaxed);
        let (trusted, fallback_reason) =
            match self.snapshot_counter(snapshot, IndexCounterKey::Total)? {
                IndexCounterRead::Value(total) if total == header.indexed_quads => (true, None),
                IndexCounterRead::Value(_) => (false, Some("total-counter-mismatch")),
                IndexCounterRead::Missing => (false, Some("total-counter-missing")),
                IndexCounterRead::Malformed => (false, Some("total-counter-malformed")),
            };
        Ok(QueryIndexAdmission {
            trusted,
            query_id_generation: trusted.then_some(header.query_id_generation),
            query_id_limit: trusted.then_some(header.next_query_id),
            fallback_reason,
            header_reads: 1,
            counter_reads: 1,
            debt_reads: 0,
        })
    }

    fn query_index_range(
        &self,
        view: &StoreReadSnapshot,
        scan: &crate::query::cursor::IndexScan<'_>,
    ) -> Result<Option<(&Keyspace, &Keyspace, Vec<u8>)>> {
        scan.costs.planner_points(1);
        let Some(spaces) = self.active_query_spaces(view)? else {
            return Ok(None);
        };
        let terms = match scan.order {
            IndexCursorOrder::Gspo => match (
                scan.pattern.graph,
                scan.pattern.subject,
                scan.pattern.predicate,
                scan.pattern.object,
            ) {
                (Some(graph), Some(subject), Some(predicate), Some(object)) => {
                    vec![graph, subject, predicate, object]
                }
                (Some(graph), Some(subject), Some(predicate), None) => {
                    vec![graph, subject, predicate]
                }
                (Some(graph), Some(subject), None, _) => vec![graph, subject],
                (Some(graph), None, _, _) => vec![graph],
                (None, _, _, _) => Vec::new(),
            },
            IndexCursorOrder::Gpos => match (
                scan.pattern.graph,
                scan.pattern.predicate,
                scan.pattern.object,
            ) {
                (Some(graph), Some(predicate), Some(object)) => vec![graph, predicate, object],
                (Some(graph), Some(predicate), None) => vec![graph, predicate],
                (Some(graph), None, _) => vec![graph],
                (None, _, _) => Vec::new(),
            },
            IndexCursorOrder::Spog => match (
                scan.pattern.subject,
                scan.pattern.predicate,
                scan.pattern.object,
            ) {
                (Some(subject), Some(predicate), Some(object)) => {
                    vec![subject, predicate, object]
                }
                (Some(subject), Some(predicate), None) => vec![subject, predicate],
                (Some(subject), None, _) => vec![subject],
                (None, _, _) => Vec::new(),
            },
            IndexCursorOrder::Posg => match (scan.pattern.predicate, scan.pattern.object) {
                (Some(predicate), Some(object)) => vec![predicate, object],
                (Some(predicate), None) => vec![predicate],
                (None, _) => Vec::new(),
            },
            IndexCursorOrder::Ospg => match (
                scan.pattern.object,
                scan.pattern.subject,
                scan.pattern.predicate,
            ) {
                (Some(object), Some(subject), Some(predicate)) => {
                    vec![object, subject, predicate]
                }
                (Some(object), Some(subject), None) => vec![object, subject],
                (Some(object), None, _) => vec![object],
                (None, _, _) => Vec::new(),
            },
            IndexCursorOrder::Gosp => match (
                scan.pattern.graph,
                scan.pattern.object,
                scan.pattern.subject,
                scan.pattern.predicate,
            ) {
                (Some(graph), Some(object), Some(subject), Some(predicate)) => {
                    vec![graph, object, subject, predicate]
                }
                (Some(graph), Some(object), Some(subject), None) => {
                    vec![graph, object, subject]
                }
                (Some(graph), Some(object), None, _) => vec![graph, object],
                (Some(graph), None, _, _) => vec![graph],
                (None, _, _, _) => Vec::new(),
            },
        };
        let keyspace = match scan.order {
            IndexCursorOrder::Gspo => spaces.gspo,
            IndexCursorOrder::Gpos => spaces.gpos,
            IndexCursorOrder::Spog => spaces.spog,
            IndexCursorOrder::Posg => spaces.posg,
            IndexCursorOrder::Ospg => spaces.ospg,
            IndexCursorOrder::Gosp => spaces.gosp,
        };
        let mut query_terms = Vec::with_capacity(terms.len());
        for term in terms {
            scan.costs.planner_points(1);
            let value = view
                .snapshot
                .get(spaces.term_to_query, term.to_be_bytes())?;
            scan.costs
                .forward_mapping(16 + value.as_ref().map_or(0, |value| value.len() as u64));
            let Some(value) = value else {
                return Ok(None);
            };
            query_terms.push(decode_query_id(value.as_ref(), "term-to-query mapping")?);
        }
        Ok(Some((
            keyspace,
            spaces.query_to_term,
            query_index_prefix(&query_terms),
        )))
    }

    pub(crate) fn query_index_status(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let snapshot_sequence = snapshot.seqno();
        let header = self.snapshot_index_header(&snapshot)?;
        let slot = match &header {
            IndexHeaderRead::Valid(header) => IndexSlot::decode(header.active_slot)
                .ok_or(StoreError::IndexVerificationFailed("active-slot-invalid"))?,
            IndexHeaderRead::Legacy(_) | IndexHeaderRead::Absent | IndexHeaderRead::Malformed => {
                IndexSlot::Primary
            }
        };
        let spaces = self.query_spaces(slot);
        let source_live_quads = self.count_live_rows(&snapshot)?;
        let (indexed_quads, gpos_well_formed) = self.summarize_qv_rows(&snapshot, spaces.gpos)?;
        let (gspo_quads, gspo_well_formed) = self.summarize_qv_rows(&snapshot, spaces.gspo)?;
        let (spog_quads, spog_well_formed) = self.summarize_qv_rows(&snapshot, spaces.spog)?;
        let (posg_quads, posg_well_formed) = self.summarize_qv_rows(&snapshot, spaces.posg)?;
        let (ospg_quads, ospg_well_formed) = self.summarize_qv_rows(&snapshot, spaces.ospg)?;
        let (gosp_quads, gosp_well_formed) = self.summarize_qv_rows(&snapshot, spaces.gosp)?;
        let (state, last_build_sequence, query_id_generation, query_term_ids) = match header {
            IndexHeaderRead::Absent => (QueryIndexState::Missing, 0, 0, 0),
            IndexHeaderRead::Malformed => (
                QueryIndexState::Failed("metadata-malformed".to_owned()),
                0,
                0,
                0,
            ),
            IndexHeaderRead::Legacy(header) => (
                QueryIndexState::Failed("legacy-coverage-unverified".to_owned()),
                header.last_build_sequence,
                header.query_id_generation,
                header.next_query_id,
            ),
            IndexHeaderRead::Valid(header) => {
                let total_matches_header = matches!(
                    self.snapshot_counter(&snapshot, IndexCounterKey::Total)?,
                    IndexCounterRead::Value(total) if total == header.indexed_quads
                );
                let ready_matches_snapshot = header.ready_is_coherent()
                    && header.fits_snapshot(snapshot_sequence)
                    && header.source_live_quads == source_live_quads
                    && header.indexed_quads == indexed_quads
                    && indexed_quads == gspo_quads
                    && indexed_quads == spog_quads
                    && indexed_quads == posg_quads
                    && indexed_quads == ospg_quads
                    && indexed_quads == gosp_quads
                    && gspo_well_formed
                    && gpos_well_formed
                    && spog_well_formed
                    && posg_well_formed
                    && ospg_well_formed
                    && gosp_well_formed
                    && total_matches_header;
                if matches!(header.state, StoredIndexState::Ready) && !ready_matches_snapshot {
                    (
                        QueryIndexState::Failed("ready-status-mismatch".to_owned()),
                        header.last_build_sequence,
                        header.query_id_generation,
                        header.next_query_id,
                    )
                } else {
                    (
                        header.state(),
                        header.last_build_sequence,
                        header.query_id_generation,
                        header.next_query_id,
                    )
                }
            }
        };
        Ok(QueryIndexStatus {
            schema_version: QV_SCHEMA_VERSION,
            state,
            query_id_generation,
            query_term_ids,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        })
    }

    pub(crate) fn index_status_fast(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let (
            state,
            query_id_generation,
            query_term_ids,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        ) = match self.snapshot_index_header(&snapshot)? {
            IndexHeaderRead::Absent => (QueryIndexState::Missing, 0, 0, 0, 0, 0),
            IndexHeaderRead::Malformed => (
                QueryIndexState::Failed("metadata-malformed".to_owned()),
                0,
                0,
                0,
                0,
                0,
            ),
            IndexHeaderRead::Legacy(header) => (
                QueryIndexState::Failed("legacy-coverage-unverified".to_owned()),
                header.query_id_generation,
                header.next_query_id,
                header.source_live_quads,
                header.indexed_quads,
                header.last_build_sequence,
            ),
            IndexHeaderRead::Valid(header) => {
                #[cfg(test)]
                self.index_admission_probes.fetch_add(1, Ordering::Relaxed);
                let admission = self.header_admission(&snapshot, &header)?;
                let state = if matches!(header.state, StoredIndexState::Ready) && !admission.trusted
                {
                    QueryIndexState::Failed(
                        admission
                            .fallback_reason
                            .unwrap_or("ready-metadata-mismatch")
                            .to_owned(),
                    )
                } else {
                    header.state()
                };
                (
                    state,
                    header.query_id_generation,
                    header.next_query_id,
                    header.source_live_quads,
                    header.indexed_quads,
                    header.last_build_sequence,
                )
            }
        };
        Ok(QueryIndexStatus {
            schema_version: QV_SCHEMA_VERSION,
            state,
            query_id_generation,
            query_term_ids,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        })
    }

    pub(crate) fn verify_query_indexes(
        &self,
        mode: impl Into<IndexVerifyMode>,
    ) -> Result<QueryIndexVerification> {
        let snapshot = self.query_index_snapshot();
        let mode = mode.into();
        self.verify_index_snapshot(
            &snapshot,
            matches!(mode, IndexVerifyMode::Full),
            IndexVerifyState::Ready,
            None,
            None,
        )
    }

    fn initialize_indexes(&self) -> Result<()> {
        let view = self.read_snapshot();
        let snapshot = view.snapshot_ref();
        if self.fail_unrepaired_debt(snapshot)? {
            return Ok(());
        }
        match self.snapshot_index_header(snapshot)? {
            IndexHeaderRead::Absent => {
                let source_live_quads = self.count_live_rows(snapshot)?;
                if source_live_quads != 0 {
                    return Ok(());
                }
                if !self.index_spaces_empty(snapshot)? {
                    let mut batch = self.buffered_batch();
                    self.stage_index_failure(
                        &mut batch,
                        None,
                        "metadata-missing-with-derived-residue",
                    );
                    return self.commit_fjall_batch(batch);
                }
                let mut batch = self.buffered_batch();
                self.stage_index_header(&mut batch, &IndexHeader::empty_ready());
                batch.insert(&self.qv2_meta, QV_TOTAL_KEY, 0u64.to_be_bytes());
                batch.insert(
                    &self.qv2_meta,
                    IndexCounterKey::UnionDuplicateFree.bytes(),
                    1u64.to_be_bytes(),
                );
                self.commit_fjall_batch(batch)
            }
            IndexHeaderRead::Malformed => {
                let mut batch = self.buffered_batch();
                self.stage_index_failure(&mut batch, None, "metadata-malformed");
                self.commit_fjall_batch(batch)
            }
            IndexHeaderRead::Legacy(header) => self.certify_legacy_header(snapshot, header),
            IndexHeaderRead::Valid(header) => {
                if !matches!(header.state, StoredIndexState::Ready) {
                    return Ok(());
                }
                let admission = self.snapshot_admission(snapshot)?;
                if admission.trusted {
                    return Ok(());
                }
                let mut batch = self.buffered_batch();
                self.stage_index_failure(&mut batch, Some(&header), "open-admission-failed");
                self.commit_fjall_batch(batch)
            }
        }
    }

    /// Fully verifies a legacy header before rewriting it in the current format.
    fn certify_legacy_header(&self, snapshot: &Snapshot, header: IndexHeader) -> Result<()> {
        if !matches!(header.state, StoredIndexState::Ready) {
            let mut batch = self.buffered_batch();
            self.stage_index_header(&mut batch, &header);
            return self.commit_fjall_batch(batch);
        }
        let report =
            self.verify_index_snapshot(snapshot, true, IndexVerifyState::Ready, None, None)?;
        let mut batch = self.buffered_batch();
        if report.valid {
            self.stage_index_header(&mut batch, &header);
        } else {
            self.stage_index_failure(&mut batch, Some(&header), "legacy-coverage-unverified");
        }
        self.commit_fjall_batch(batch)
    }

    fn index_row_sampled(full: bool, checked: u64) -> bool {
        full || checked < QV_SAMPLE_ROWS
    }

    fn qv_row_empty(
        &self,
        snapshot: &Snapshot,
        keyspace: &Keyspace,
        key: QueryQuadKey,
    ) -> Result<bool> {
        Ok(snapshot
            .get(keyspace, key)?
            .is_some_and(|value| value.as_ref().is_empty()))
    }

    fn verify_source_rows(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        full: bool,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        for guard in snapshot.iter(&self.quads) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            report.report.source_live_quads =
                report.report.source_live_quads.checked_add(1).ok_or(
                    StoreError::IndexVerificationFailed("source-row-count-overflow"),
                )?;
            let Some(quad) = decode_source_quad(key.as_ref()) else {
                report.problem("source-key-length");
                continue;
            };
            if !Self::index_row_sampled(full, report.report.checked_source_rows) {
                continue;
            }
            report.report.checked_source_rows =
                report.report.checked_source_rows.checked_add(1).ok_or(
                    StoreError::IndexVerificationFailed("source-check-count-overflow"),
                )?;
            let Some(quad) = self.spaces_query_quad(snapshot, spaces, quad)? else {
                report.problem("source-query-id-mapping-missing");
                continue;
            };
            if !self.qv_row_empty(snapshot, spaces.gspo, gspo_key(quad))? {
                report.problem("source-gspo-missing-or-nonempty");
            }
            if !self.qv_row_empty(snapshot, spaces.gpos, gpos_key(quad))? {
                report.problem("source-gpos-missing-or-nonempty");
            }
            if !self.qv_row_empty(snapshot, spaces.spog, spog_key(quad))? {
                report.problem("source-spog-missing-or-nonempty");
            }
            if !self.qv_row_empty(snapshot, spaces.posg, posg_key(quad))? {
                report.problem("source-posg-missing-or-nonempty");
            }
            if !self.qv_row_empty(snapshot, spaces.ospg, ospg_key(quad))? {
                report.problem("source-ospg-missing-or-nonempty");
            }
            if !self.qv_row_empty(snapshot, spaces.gosp, gosp_key(quad))? {
                report.problem("source-gosp-missing-or-nonempty");
            }
        }
        Ok(())
    }

    fn verify_qv_rows(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        order: IndexKeyOrder,
        full: bool,
        report: &mut IndexVerifyBuilder,
    ) -> Result<u64> {
        let (keyspace, key_problem, value_problem, source_problem) = match order {
            IndexKeyOrder::Gspo => (
                spaces.gspo,
                "qv-gspo-key-length",
                "qv-gspo-value-nonempty",
                "qv-gspo-source-missing",
            ),
            IndexKeyOrder::Gpos => (
                spaces.gpos,
                "qv-gpos-key-length",
                "qv-gpos-value-nonempty",
                "qv-gpos-source-missing",
            ),
            IndexKeyOrder::Spog => (
                spaces.spog,
                "qv-spog-key-length",
                "qv-spog-value-nonempty",
                "qv-spog-source-missing",
            ),
            IndexKeyOrder::Posg => (
                spaces.posg,
                "qv-posg-key-length",
                "qv-posg-value-nonempty",
                "qv-posg-source-missing",
            ),
            IndexKeyOrder::Ospg => (
                spaces.ospg,
                "qv-ospg-key-length",
                "qv-ospg-value-nonempty",
                "qv-ospg-source-missing",
            ),
            IndexKeyOrder::Gosp => (
                spaces.gosp,
                "qv-gosp-key-length",
                "qv-gosp-value-nonempty",
                "qv-gosp-source-missing",
            ),
        };
        let mut rows = 0u64;
        let mut checked_space = 0u64;
        for guard in snapshot.iter(keyspace) {
            let (key, value) = guard.into_inner()?;
            rows = rows
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "index-row-count-overflow",
                ))?;
            if !Self::index_row_sampled(full, checked_space) {
                continue;
            }
            checked_space =
                checked_space
                    .checked_add(1)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "index-check-count-overflow",
                    ))?;
            report.report.checked_index_rows =
                report.report.checked_index_rows.checked_add(1).ok_or(
                    StoreError::IndexVerificationFailed("index-check-count-overflow"),
                )?;
            if !value.as_ref().is_empty() {
                report.problem(value_problem);
            }
            let quad = match order {
                IndexKeyOrder::Gspo => decode_gspo_key(key.as_ref()),
                IndexKeyOrder::Gpos => decode_gpos_key(key.as_ref()),
                IndexKeyOrder::Spog => decode_spog_key(key.as_ref()),
                IndexKeyOrder::Posg => decode_posg_key(key.as_ref()),
                IndexKeyOrder::Ospg => decode_ospg_key(key.as_ref()),
                IndexKeyOrder::Gosp => decode_gosp_key(key.as_ref()),
            };
            let Some(quad) = quad else {
                report.problem(key_problem);
                continue;
            };
            let Some(quad) = self.spaces_source_quad(snapshot, spaces, quad)? else {
                report.problem("qv-query-id-mapping-missing");
                continue;
            };
            let source_key = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
            let source_is_live = snapshot
                .get(&self.quads, source_key)?
                .is_some_and(|source| !dots_empty(source.as_ref()));
            if !source_is_live {
                report.problem(source_problem);
            }
        }
        Ok(rows)
    }

    fn verify_index_counter(
        &self,
        check: CounterCheck<'_>,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        match check.snapshot.get(check.spaces.meta, check.key.bytes())? {
            None => report.problem(check.problems.0),
            Some(value) => match decode_index_count(value.as_ref()) {
                Some(actual) if actual == check.expected => {}
                _ => report.problem(check.problems.1),
            },
        }
        Ok(())
    }

    fn verify_gpos_group(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        dimension: usize,
        terms: [QueryTermId; 3],
        expected: u64,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let (key, missing_problem, mismatch_problem) = match dimension {
            1 => (
                IndexCounterKey::Graph(terms[0]),
                "counter-g-missing",
                "counter-g-mismatch",
            ),
            2 => (
                IndexCounterKey::GraphPredicate(terms[0], terms[1]),
                "counter-gp-missing",
                "counter-gp-mismatch",
            ),
            3 => (
                IndexCounterKey::GraphPredicateObject(terms[0], terms[1], terms[2]),
                "counter-gpo-missing",
                "counter-gpo-mismatch",
            ),
            _ => unreachable!("only GPOS counter dimensions are used"),
        };
        self.verify_index_counter(
            CounterCheck {
                snapshot,
                spaces,
                key,
                expected,
                problems: (missing_problem, mismatch_problem),
            },
            report,
        )
    }

    fn verify_gpos_dimension(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        dimension: usize,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let mut current = None::<[QueryTermId; 3]>;
        let mut count = 0u64;
        for guard in snapshot.iter(spaces.gpos) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_gpos_key(key.as_ref()) else {
                continue;
            };
            let terms = [quad.graph, quad.predicate, quad.object];
            if let Some(previous) = current
                && previous[..dimension] != terms[..dimension]
            {
                self.verify_gpos_group(snapshot, spaces, dimension, previous, count, report)?;
                count = 0;
            }
            current = Some(terms);
            count = count
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "counter-count-overflow",
                ))?;
        }
        if let Some(previous) = current {
            self.verify_gpos_group(snapshot, spaces, dimension, previous, count, report)?;
        }
        Ok(())
    }

    fn verify_posg_group(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        dimension: usize,
        terms: [QueryTermId; 2],
        expected: u64,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let (key, missing_problem, mismatch_problem) = match dimension {
            1 => (
                IndexCounterKey::Predicate(terms[0]),
                "counter-p-missing",
                "counter-p-mismatch",
            ),
            2 => (
                IndexCounterKey::PredicateObject(terms[0], terms[1]),
                "counter-po-missing",
                "counter-po-mismatch",
            ),
            _ => unreachable!("only POSG counter dimensions are used"),
        };
        self.verify_index_counter(
            CounterCheck {
                snapshot,
                spaces,
                key,
                expected,
                problems: (missing_problem, mismatch_problem),
            },
            report,
        )
    }

    fn verify_posg_dimension(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        dimension: usize,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let mut current = None::<[QueryTermId; 2]>;
        let mut count = 0u64;
        for guard in snapshot.iter(spaces.posg) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_posg_key(key.as_ref()) else {
                continue;
            };
            let terms = [quad.predicate, quad.object];
            if let Some(previous) = current
                && previous[..dimension] != terms[..dimension]
            {
                self.verify_posg_group(snapshot, spaces, dimension, previous, count, report)?;
                count = 0;
            }
            current = Some(terms);
            count = count
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "counter-count-overflow",
                ))?;
        }
        if let Some(previous) = current {
            self.verify_posg_group(snapshot, spaces, dimension, previous, count, report)?;
        }
        Ok(())
    }

    fn counter_has_rows(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        key: IndexCounterKey,
    ) -> Result<bool> {
        let mut rows = match key {
            IndexCounterKey::Graph(graph) => {
                snapshot.prefix(spaces.gpos, query_index_prefix(&[graph]))
            }
            IndexCounterKey::Predicate(predicate) => {
                snapshot.prefix(spaces.posg, query_index_prefix(&[predicate]))
            }
            IndexCounterKey::GraphPredicate(graph, predicate) => {
                snapshot.prefix(spaces.gpos, query_index_prefix(&[graph, predicate]))
            }
            IndexCounterKey::PredicateObject(predicate, object) => {
                snapshot.prefix(spaces.posg, query_index_prefix(&[predicate, object]))
            }
            IndexCounterKey::GraphPredicateObject(graph, predicate, object) => {
                snapshot.prefix(spaces.gpos, query_index_prefix(&[graph, predicate, object]))
            }
            IndexCounterKey::Total | IndexCounterKey::UnionDuplicateFree => {
                return Ok(true);
            }
        };
        match rows.next() {
            Some(guard) => {
                let _ = guard.into_inner()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn verify_index_meta(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        header: Option<&IndexHeader>,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let mut headers = 0u64;
        let mut totals = 0u64;
        let mut union_proofs = 0u64;
        for guard in snapshot.iter(spaces.meta) {
            let (key, value) = guard.into_inner()?;
            match decode_counter_key(key.as_ref()) {
                CounterKeyRead::Header => {
                    headers = headers
                        .checked_add(1)
                        .ok_or(StoreError::IndexVerificationFailed(
                            "metadata-count-overflow",
                        ))?;
                    if !matches!(
                        decode_index_header(value.as_ref()),
                        IndexHeaderRead::Valid(_) | IndexHeaderRead::Legacy(_)
                    ) {
                        report.problem("meta-header-malformed");
                    }
                }
                CounterKeyRead::Counter(counter) => {
                    let Some(value) = decode_index_count(value.as_ref()) else {
                        report.problem("meta-counter-value-length");
                        continue;
                    };
                    match counter {
                        IndexCounterKey::Total => {
                            totals = totals.checked_add(1).ok_or(
                                StoreError::IndexVerificationFailed("metadata-count-overflow"),
                            )?;
                            match header {
                                Some(header) if value == header.indexed_quads => {}
                                Some(_) => report.problem("meta-total-mismatch"),
                                None => report.problem("meta-total-without-header"),
                            }
                        }
                        IndexCounterKey::UnionDuplicateFree => {
                            union_proofs = union_proofs.checked_add(1).ok_or(
                                StoreError::IndexVerificationFailed("metadata-count-overflow"),
                            )?;
                            if value > 1 {
                                report.problem("union-proof-value-invalid");
                            } else if value == 1 && !self.index_union_unique(snapshot, spaces)? {
                                report.problem("union-proof-mismatch");
                            }
                        }
                        IndexCounterKey::Predicate(predicate) => {
                            if value == 0 {
                                report.problem("meta-counter-zero");
                            }
                            if !self.counter_has_rows(snapshot, spaces, counter)? {
                                report.problem("meta-counter-orphan");
                            }
                            match snapshot.get(spaces.meta, predicate_revision_key(predicate))? {
                                Some(revision)
                                    if decode_index_count(revision.as_ref())
                                        .is_some_and(|v| v > 0) => {}
                                _ => report.problem("meta-revision-missing"),
                            }
                        }
                        _ => {
                            if value == 0 {
                                report.problem("meta-counter-zero");
                            }
                            if !self.counter_has_rows(snapshot, spaces, counter)? {
                                report.problem("meta-counter-orphan");
                            }
                        }
                    }
                }
                CounterKeyRead::Revision => match decode_index_count(value.as_ref()) {
                    Some(value) if value > 0 => {}
                    _ => report.problem("meta-revision-value"),
                },
                CounterKeyRead::ProjectionDebt => report.problem("qv-projection-debt"),
                CounterKeyRead::Control => {}
                CounterKeyRead::UnknownTag => report.problem("meta-unknown-tag"),
                CounterKeyRead::InvalidLength => report.problem("meta-counter-key-length"),
            }
        }
        if headers > 1 {
            report.problem("meta-header-count");
        }
        if totals != 1 {
            report.problem("meta-total-count");
        }
        if union_proofs != 1 {
            report.problem("union-proof-count");
        }
        Ok(())
    }

    fn verify_id_mappings(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        header: Option<&IndexHeader>,
        report: &mut IndexVerifyBuilder,
    ) -> Result<()> {
        let mut forward_count = 0u64;
        for guard in snapshot.iter(spaces.term_to_query) {
            let (key, value) = guard.into_inner()?;
            let Ok(term_bytes) = <[u8; 16]>::try_from(key.as_ref()) else {
                report.problem("term-to-query-key-length");
                continue;
            };
            let Ok(query_bytes) = <[u8; 8]>::try_from(value.as_ref()) else {
                report.problem("term-to-query-value-length");
                continue;
            };
            let term = TermId::from_be_bytes(term_bytes);
            let query = QueryTermId::from_be_bytes(query_bytes);
            forward_count =
                forward_count
                    .checked_add(1)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "query-id-mapping-count-overflow",
                    ))?;
            match snapshot.get(spaces.query_to_term, query.to_be_bytes())? {
                Some(reverse) if reverse.as_ref() == term.to_be_bytes() => {}
                _ => report.problem("term-to-query-reverse-mismatch"),
            }
            if header.is_some_and(|header| query.0 >= header.next_query_id) {
                report.problem("query-id-outside-header-range");
            }
        }

        let mut reverse_count = 0u64;
        for guard in snapshot.iter(spaces.query_to_term) {
            let (key, value) = guard.into_inner()?;
            let Ok(query_bytes) = <[u8; 8]>::try_from(key.as_ref()) else {
                report.problem("query-to-term-key-length");
                continue;
            };
            let Ok(term_bytes) = <[u8; 16]>::try_from(value.as_ref()) else {
                report.problem("query-to-term-value-length");
                continue;
            };
            let query = QueryTermId::from_be_bytes(query_bytes);
            let term = TermId::from_be_bytes(term_bytes);
            reverse_count =
                reverse_count
                    .checked_add(1)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "query-id-mapping-count-overflow",
                    ))?;
            match snapshot.get(spaces.term_to_query, term.to_be_bytes())? {
                Some(forward) if forward.as_ref() == query.to_be_bytes() => {}
                _ => report.problem("query-to-term-forward-mismatch"),
            }
        }
        if forward_count != reverse_count {
            report.problem("query-id-mapping-total-mismatch");
        }
        if header.is_some_and(|header| header.next_query_id != forward_count) {
            report.problem("query-id-header-total-mismatch");
        }
        Ok(())
    }

    fn verify_index_snapshot(
        &self,
        snapshot: &Snapshot,
        full: bool,
        expected_state: IndexVerifyState,
        slot: Option<IndexSlot>,
        header_override: Option<&IndexHeader>,
    ) -> Result<QueryIndexVerification> {
        #[cfg(test)]
        self.index_verification_runs.fetch_add(1, Ordering::Relaxed);
        let header_read = self.snapshot_index_header(snapshot)?;
        let snapshot_sequence = snapshot.seqno();
        let decoded_header = match &header_read {
            IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => Some(header),
            IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
        };
        let header = header_override.or(decoded_header);
        let selected =
            slot.or_else(|| header.and_then(|header| IndexSlot::decode(header.active_slot)));
        let spaces = self.query_spaces(selected.unwrap_or(IndexSlot::Primary));
        let mut report = IndexVerifyBuilder::new(full);
        self.verify_source_rows(snapshot, spaces, full, &mut report)?;
        let gspo_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Gspo, full, &mut report)?;
        let gpos_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Gpos, full, &mut report)?;
        let spog_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Spog, full, &mut report)?;
        let posg_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Posg, full, &mut report)?;
        let ospg_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Ospg, full, &mut report)?;
        let gosp_rows =
            self.verify_qv_rows(snapshot, spaces, IndexKeyOrder::Gosp, full, &mut report)?;
        report.report.indexed_quads = gpos_rows;
        if gpos_rows != gspo_rows
            || gpos_rows != spog_rows
            || gpos_rows != posg_rows
            || gpos_rows != ospg_rows
            || gpos_rows != gosp_rows
        {
            report.problem("qv-row-total-mismatch");
        }

        match header {
            None => match header_read {
                IndexHeaderRead::Absent => report.problem("meta-header-missing"),
                IndexHeaderRead::Malformed => report.problem("meta-header-malformed"),
                IndexHeaderRead::Valid(_) | IndexHeaderRead::Legacy(_) => {
                    unreachable!("decoded header was retained")
                }
            },
            Some(header) => {
                let expected_state_matches = match expected_state {
                    IndexVerifyState::Ready => {
                        matches!(header.state, StoredIndexState::Ready)
                    }
                    IndexVerifyState::BuildingCandidate => {
                        matches!(header.state, StoredIndexState::Building)
                    }
                };
                if !expected_state_matches {
                    report.problem("meta-state-mismatch");
                }
                if header.source_epoch != header.index_epoch {
                    report.problem("meta-epoch-mismatch");
                }
                if header.source_epoch > snapshot_sequence || header.index_epoch > snapshot_sequence
                {
                    report.problem("meta-epoch-ahead-of-snapshot");
                }
                if header.last_build_sequence > snapshot_sequence {
                    report.problem("meta-build-sequence-ahead-of-snapshot");
                }
                if header.source_live_quads != report.report.source_live_quads {
                    report.problem("meta-source-total-mismatch");
                }
                if header.indexed_quads != report.report.indexed_quads {
                    report.problem("meta-index-total-mismatch");
                }
                if header.source_live_quads != header.indexed_quads {
                    report.problem("meta-header-total-mismatch");
                }
            }
        }

        if full {
            self.verify_gpos_dimension(snapshot, spaces, 1, &mut report)?;
            self.verify_gpos_dimension(snapshot, spaces, 2, &mut report)?;
            self.verify_gpos_dimension(snapshot, spaces, 3, &mut report)?;
            self.verify_posg_dimension(snapshot, spaces, 1, &mut report)?;
            self.verify_posg_dimension(snapshot, spaces, 2, &mut report)?;
            self.verify_index_meta(snapshot, spaces, header, &mut report)?;
            self.verify_id_mappings(snapshot, spaces, header, &mut report)?;
        }
        Ok(report.finish())
    }

    fn pending_batch_term<'a>(&self, batch: Option<&'a WriteBatch>, id: TermId) -> Option<&'a str> {
        batch
            .and_then(|batch| batch.pending_terms.get(&id))
            .map(String::as_str)
    }

    fn encode_term_internal(
        &self,
        batch: Option<&mut WriteBatch>,
        term: &EncodedTerm,
    ) -> Result<TermId> {
        let id = hash_term(term);
        let key = id.to_be_bytes();

        if let Some(existing) = self.pending_batch_term(batch.as_deref(), id) {
            if existing == term.0 {
                return Ok(id);
            }
            return Err(StoreError::TermCollision {
                attempted: term.0.clone(),
                existing: existing.to_string(),
            });
        }

        if let Some(existing) = self.terms.get(key)? {
            confirm_stored_term(existing.as_ref(), term)?;
            return Ok(id);
        }

        // Guards first-write-wins interning for this term id's shard.
        let _term_shard = self.term_locks[self.term_lock_index(id)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = self.pending_batch_term(batch.as_deref(), id) {
            if existing == term.0 {
                return Ok(id);
            }
            return Err(StoreError::TermCollision {
                attempted: term.0.clone(),
                existing: existing.to_string(),
            });
        }

        if let Some(existing) = self.terms.get(key)? {
            confirm_stored_term(existing.as_ref(), term)?;
            return Ok(id);
        }

        if let Some(batch) = batch {
            batch.insert(&self.terms, key, term.0.as_bytes());
            batch.pending_terms.insert(id, term.0.clone());
        } else {
            let mut batch = self.buffered_batch();
            batch.insert(&self.terms, key, term.0.as_bytes());
            batch.commit()?;
        }
        Ok(id)
    }

    fn read_graph_meta(&self, graph: TermId) -> Result<Option<StoredGraphMeta>> {
        self.graphs
            .get(graph_meta_key(graph))?
            .map(|bytes| postcard::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(Into::into)
    }

    fn read_quad_dots(&self, key: &[u8]) -> Result<Vec<Dot>> {
        match self.quads.get(key)? {
            Some(bytes) => decode_dots(bytes.as_ref()),
            None => Ok(Vec::new()),
        }
    }

    fn current_quad_dots(&self, batch: &WriteBatch, key: &QuadKey) -> Result<Vec<Dot>> {
        if let Some(state) = batch.pending_quad_states.get(key) {
            return Ok(state.clone().unwrap_or_default());
        }
        self.read_quad_dots(key)
    }

    fn write_quad_state(
        &self,
        batch: &mut WriteBatch,
        quad: EncodedQuad,
        mut dots: Vec<Dot>,
    ) -> Result<bool> {
        normalize_dots(&mut dots);
        let key = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
        let previous = self.current_quad_dots(batch, &key)?;
        let was_live = !previous.is_empty();
        let is_live = !dots.is_empty();

        if is_live {
            // Encode first so the dot vector can be moved into the pending map
            // instead of cloned.
            batch.insert(&self.quads, key, encode_dots(&dots));
            batch.pending_quad_states.insert(key, Some(dots));
        } else {
            batch.remove(&self.quads, key);
            batch.pending_quad_states.insert(key, None);
        }

        match (was_live, is_live) {
            (false, true) => {
                batch
                    .publish
                    .quad_mutations
                    .push(QuadMutation::Insert(quad));
                Ok(true)
            }
            (true, false) => {
                batch
                    .publish
                    .quad_mutations
                    .push(QuadMutation::Remove(quad));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Drop rebuildable cache state. Durable source/qv keyspaces are the read
    /// authority, so rebuilding does not require a corpus-sized mirror.
    #[cfg(test)]
    fn rebuild_indexes(&self) -> Result<()> {
        #[cfg(test)]
        self.stall_in_rebuild();
        let mut indexes = self.indexes_write();
        indexes.quad_subjects.clear();
        indexes.object_order.clear();
        self.term_decode_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Ok(())
    }

    fn clear_query_space(&self, clear: QueryClear<'_>) -> Result<()> {
        let snapshot = self.db.snapshot();
        let mut batch = self.buffered_batch();
        let mut pending = 0usize;
        for guard in snapshot.iter(clear.keyspace) {
            let (key, _) = guard.into_inner()?;
            batch.remove(clear.keyspace, key);
            pending += 1;
            if pending == QV_BUILD_ROWS {
                self.commit_fjall_batch(batch)?;
                #[cfg(test)]
                self.run_rebuild_hook(RebuildPhase::CleanupPage);
                if clear.stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
                    return Err(StoreError::Cancelled);
                }
                batch = self.buffered_batch();
                pending = 0;
            }
        }
        if pending != 0 {
            self.commit_fjall_batch(batch)?;
            #[cfg(test)]
            self.run_rebuild_hook(RebuildPhase::CleanupPage);
        }
        Ok(())
    }

    fn clear_query_meta(&self, slot: IndexSlot, stop: Option<&AtomicBool>) -> Result<()> {
        let spaces = self.query_spaces(slot);
        let snapshot = self.db.snapshot();
        let mut batch = self.buffered_batch();
        let mut pending = 0usize;
        for guard in snapshot.iter(spaces.meta) {
            let (key, _) = guard.into_inner()?;
            let remove = match slot {
                IndexSlot::Secondary => true,
                IndexSlot::Primary => matches!(
                    decode_counter_key(key.as_ref()),
                    CounterKeyRead::Counter(_)
                        | CounterKeyRead::Revision
                        | CounterKeyRead::UnknownTag
                        | CounterKeyRead::InvalidLength
                ),
            };
            if !remove {
                continue;
            }
            batch.remove(spaces.meta, key);
            pending += 1;
            if pending == QV_BUILD_ROWS {
                self.commit_fjall_batch(batch)?;
                if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
                    return Err(StoreError::Cancelled);
                }
                batch = self.buffered_batch();
                pending = 0;
            }
        }
        if pending != 0 {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    fn clear_query_slot(&self, slot: IndexSlot, stop: Option<&AtomicBool>) -> Result<()> {
        let spaces = self.query_spaces(slot);
        for keyspace in [
            spaces.gspo,
            spaces.gpos,
            spaces.spog,
            spaces.posg,
            spaces.ospg,
            spaces.gosp,
            spaces.term_to_query,
            spaces.query_to_term,
        ] {
            self.clear_query_space(QueryClear { keyspace, stop })?;
        }
        self.clear_query_meta(slot, stop)
    }

    fn rebuild_query_id(
        &self,
        spaces: IndexSpaces<'_>,
        batch: &mut fjall::OwnedWriteBatch,
        allocated: &mut HashMap<TermId, QueryTermId>,
        next_query_id: &mut u64,
        term: TermId,
    ) -> Result<QueryTermId> {
        if let Some(query) = allocated.get(&term) {
            return Ok(*query);
        }
        if let Some(value) = spaces.term_to_query.get(term.to_be_bytes())? {
            let query = decode_query_id(value.as_ref(), "term-to-query mapping")?;
            let Some(reverse) = spaces.query_to_term.get(query.to_be_bytes())? else {
                return Err(StoreError::IndexVerificationFailed(
                    "rebuild-query-id-reverse-missing",
                ));
            };
            if reverse.as_ref() != term.to_be_bytes() {
                return Err(StoreError::IndexVerificationFailed(
                    "rebuild-query-id-reverse-mismatch",
                ));
            }
            allocated.insert(term, query);
            return Ok(query);
        }

        let query = QueryTermId(*next_query_id);
        *next_query_id =
            next_query_id
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "query-id-space-exhausted",
                ))?;
        if spaces.query_to_term.get(query.to_be_bytes())?.is_some() {
            return Err(StoreError::IndexVerificationFailed(
                "rebuild-query-id-already-used",
            ));
        }
        batch.insert(
            spaces.term_to_query,
            term.to_be_bytes(),
            query.to_be_bytes(),
        );
        batch.insert(
            spaces.query_to_term,
            query.to_be_bytes(),
            term.to_be_bytes(),
        );
        allocated.insert(term, query);
        Ok(query)
    }

    fn build_index_chunk(
        &self,
        spaces: IndexSpaces<'_>,
        quads: &[EncodedQuad],
        next_query_id: &mut u64,
    ) -> Result<()> {
        let mut increments = BTreeMap::<Vec<u8>, (IndexCounterKey, u64)>::new();
        let mut allocated = HashMap::new();
        let mut revisions = HashSet::new();
        let mut batch = self.buffered_batch();
        for quad in quads {
            let quad = QueryQuad {
                graph: self.rebuild_query_id(
                    spaces,
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.graph,
                )?,
                subject: self.rebuild_query_id(
                    spaces,
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.subject,
                )?,
                predicate: self.rebuild_query_id(
                    spaces,
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.predicate,
                )?,
                object: self.rebuild_query_id(
                    spaces,
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.object,
                )?,
            };
            for (keyspace, key) in [
                (spaces.gspo, gspo_key(quad)),
                (spaces.gpos, gpos_key(quad)),
                (spaces.spog, spog_key(quad)),
                (spaces.posg, posg_key(quad)),
                (spaces.ospg, ospg_key(quad)),
                (spaces.gosp, gosp_key(quad)),
            ] {
                batch.insert(keyspace, key, Vec::<u8>::new());
            }
            for counter in live_counter_keys(quad) {
                let entry = increments.entry(counter.bytes()).or_insert((counter, 0));
                entry.1 = entry
                    .1
                    .checked_add(1)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "rebuild-counter-overflow",
                    ))?;
            }
            revisions.insert(quad.predicate);
        }
        for (_, (counter, increment)) in increments {
            let current = match spaces.meta.get(counter.bytes())? {
                None => 0,
                Some(value) => decode_index_count(value.as_ref()).ok_or(
                    StoreError::IndexVerificationFailed("rebuild-counter-malformed"),
                )?,
            };
            let next =
                current
                    .checked_add(increment)
                    .ok_or(StoreError::IndexVerificationFailed(
                        "rebuild-counter-overflow",
                    ))?;
            batch.insert(spaces.meta, counter.bytes(), next.to_be_bytes());
        }
        for predicate in revisions {
            let key = predicate_revision_key(predicate);
            let current = match spaces.meta.get(key)? {
                None => 0,
                Some(value) => decode_index_count(value.as_ref()).ok_or(
                    StoreError::IndexVerificationFailed("rebuild-revision-malformed"),
                )?,
            };
            let next = current
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "rebuild-revision-overflow",
                ))?;
            batch.insert(spaces.meta, key, next.to_be_bytes());
        }
        self.commit_fjall_batch(batch)
    }

    fn build_query_rows(&self, ctx: QueryBuildCtx<'_>) -> Result<(u64, u64)> {
        let mut rows = 0u64;
        let mut next_query_id = 0u64;
        let mut chunk = Vec::with_capacity(QV_BUILD_ROWS);
        for guard in ctx.snapshot.iter(&self.quads) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = decode_source_quad(key.as_ref()).ok_or(
                StoreError::IndexVerificationFailed("rebuild-source-key-malformed"),
            )?;
            rows = rows
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed(
                    "rebuild-source-count-overflow",
                ))?;
            chunk.push(quad);
            ctx.build.scan_cursor = Some(key.as_ref().try_into().map_err(|_| {
                StoreError::IndexVerificationFailed("rebuild-source-key-malformed")
            })?);
            if chunk.len() == QV_BUILD_ROWS {
                self.build_index_chunk(ctx.spaces, &chunk, &mut next_query_id)?;
                self.update_build_cursor(ctx.build.scan_cursor)?;
                #[cfg(test)]
                self.run_rebuild_hook(RebuildPhase::ScanPage);
                if ctx.stop.load(Ordering::Relaxed) {
                    return Err(StoreError::Cancelled);
                }
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            self.build_index_chunk(ctx.spaces, &chunk, &mut next_query_id)?;
            self.update_build_cursor(ctx.build.scan_cursor)?;
            #[cfg(test)]
            self.run_rebuild_hook(RebuildPhase::ScanPage);
            if ctx.stop.load(Ordering::Relaxed) {
                return Err(StoreError::Cancelled);
            }
        }
        Ok((rows, next_query_id))
    }

    fn index_union_unique(&self, snapshot: &Snapshot, spaces: IndexSpaces<'_>) -> Result<bool> {
        let mut previous = None;
        for guard in snapshot.iter(spaces.spog) {
            let (key, _) = guard.into_inner()?;
            let quad = decode_spog_key(key.as_ref()).ok_or(StoreError::IndexVerificationFailed(
                "union-proof-row-malformed",
            ))?;
            let current = (quad.subject, quad.predicate, quad.object);
            if previous == Some(current) {
                return Ok(false);
            }
            previous = Some(current);
        }
        Ok(true)
    }

    fn clear_query_deltas(&self, stop: Option<&AtomicBool>) -> Result<()> {
        loop {
            let snapshot = self.db.snapshot();
            let mut batch = self.buffered_batch();
            let mut removed = 0usize;
            for guard in snapshot.prefix(&self.qv2_meta, [QV_DELTA_TAG]) {
                let (key, _) = guard.into_inner()?;
                batch.remove(&self.qv2_meta, key);
                removed += 1;
                if removed == QV_BUILD_ROWS {
                    break;
                }
            }
            if removed == 0 {
                return Ok(());
            }
            self.commit_fjall_batch(batch)?;
            if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
                return Err(StoreError::Cancelled);
            }
        }
    }

    fn recover_query_build(&self, stop: Option<&AtomicBool>) -> Result<()> {
        let snapshot = self.db.snapshot();
        let active = match self.snapshot_index_header(&snapshot)? {
            IndexHeaderRead::Valid(header) => IndexSlot::decode(header.active_slot),
            IndexHeaderRead::Legacy(_) => Some(IndexSlot::Primary),
            IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
        };
        if let Some(build) = self.query_build(&snapshot)? {
            let slot = IndexSlot::decode(build.slot)
                .ok_or(StoreError::IndexVerificationFailed("build-slot-invalid"))?;
            if Some(slot) == active {
                return Err(StoreError::IndexVerificationFailed("build-target-active"));
            }
            self.clear_query_slot(slot, stop)?;
            {
                let _projection = self
                    .projection_lock
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                let owner = self
                    .qv_gate
                    .acquire_timeout(self.qv_commit_wait)
                    .ok_or(StoreError::QueryIndexBusy)?;
                let current = self.query_build(&self.db.snapshot())?.ok_or(
                    StoreError::QueryIndexUnavailable("query-index build record missing"),
                )?;
                if current.format != build.format
                    || current.slot != build.slot
                    || current.source_sequence != build.source_sequence
                    || current.control_digest != build.control_digest
                {
                    return Err(StoreError::QueryIndexUnavailable(
                        "query-index build changed during recovery",
                    ));
                }
                #[cfg(test)]
                self.run_rebuild_hook(RebuildPhase::Retire);
                let mut batch = self.buffered_batch();
                batch.remove(&self.qv2_meta, QV_BUILD_KEY);
                self.commit_fjall_batch(batch)?;
                owner.finish();
            }
            self.clear_query_deltas(stop)?;
        }
        let snapshot = self.db.snapshot();
        let cleanup = snapshot
            .get(&self.qv2_meta, QV_CLEANUP_KEY)?
            .map(|value| postcard::from_bytes::<QueryCleanupRecord>(value.as_ref()))
            .transpose()?;
        if let Some(cleanup) = cleanup {
            if cleanup.format > QV_SCHEMA_VERSION {
                return Err(StoreError::UnsupportedIndexFormat {
                    found: cleanup.format,
                    supported: QV_SCHEMA_VERSION,
                });
            }
            if cleanup.format != QV_SCHEMA_VERSION {
                return Ok(());
            }
            let slot = IndexSlot::decode(cleanup.slot)
                .ok_or(StoreError::IndexVerificationFailed("cleanup-slot-invalid"))?;
            let current = match self.snapshot_index_header(&self.db.snapshot())? {
                IndexHeaderRead::Valid(header) => IndexSlot::decode(header.active_slot),
                IndexHeaderRead::Legacy(_) => Some(IndexSlot::Primary),
                IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
            };
            if Some(slot) != current {
                self.clear_query_slot(slot, stop)?;
                self.clear_query_deltas(stop)?;
                let mut batch = self.buffered_batch();
                batch.remove(&self.qv2_meta, QV_CLEANUP_KEY);
                self.commit_fjall_batch(batch)?;
            } else {
                return Err(StoreError::IndexVerificationFailed("cleanup-target-active"));
            }
        }
        Ok(())
    }

    fn replay_query_deltas(
        &self,
        build: &mut QueryBuildRecord,
        candidate: &mut IndexHeader,
        target: u64,
        stop: &AtomicBool,
    ) -> Result<()> {
        while build.replay_cursor < target {
            if stop.load(Ordering::Relaxed) {
                return Err(StoreError::Cancelled);
            }
            let owner = self
                .qv_gate
                .acquire_timeout(self.qv_commit_wait)
                .ok_or(StoreError::QueryIndexBusy)?;
            let result = self.replay_query_delta(build, candidate, target);
            owner.finish();
            result?;
        }
        Ok(())
    }

    fn replay_query_delta(
        &self,
        build: &mut QueryBuildRecord,
        candidate: &mut IndexHeader,
        target: u64,
    ) -> Result<()> {
        let snapshot = self.db.snapshot();
        let current = self
            .query_build(&snapshot)?
            .ok_or(StoreError::QueryIndexUnavailable(
                "query-index build record missing",
            ))?;
        if current.replay_cursor != build.replay_cursor {
            return Err(StoreError::QueryIndexUnavailable(
                "query-index replay cursor changed",
            ));
        }
        *build = current;
        if build.replay_cursor < target {
            let delta_id = build
                .replay_cursor
                .checked_add(1)
                .ok_or(StoreError::IndexVerificationFailed("delta-cursor-overflow"))?;
            let value = snapshot
                .get(&self.qv2_meta, query_delta_key(delta_id))?
                .ok_or(StoreError::IndexVerificationFailed("delta-row-missing"))?;
            let delta: QueryDeltaRecord = postcard::from_bytes(value.as_ref())?;
            let mut plan = self
                .plan_index_update(&snapshot, candidate, delta.transitions)?
                .ok_or(StoreError::IndexVerificationFailed("delta-replay-failed"))?;
            let next = plan.header.take();
            let mut batch = self.buffered_batch();
            self.stage_index_plan(&mut batch, plan);
            build.replay_cursor = delta_id;
            build.phase = QueryBuildPhase::Replay;
            build.target = Some(target);
            batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(build)?);
            self.commit_fjall_batch(batch)?;
            #[cfg(test)]
            self.run_rebuild_hook(RebuildPhase::ReplayPage);
            if let Some(mut next) = next {
                next.state = StoredIndexState::Building;
                *candidate = next;
            }
        }
        Ok(())
    }

    fn replay_owned_deltas(
        &self,
        build: &mut QueryBuildRecord,
        candidate: &mut IndexHeader,
        target: u64,
        stop: &AtomicBool,
    ) -> Result<()> {
        while build.replay_cursor < target {
            if stop.load(Ordering::Relaxed) {
                return Err(StoreError::Cancelled);
            }
            self.replay_query_delta(build, candidate, target)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn repair_query_indexes(&self) -> Result<()> {
        self.repair_query_with(&AtomicBool::new(false))
    }

    pub(crate) fn repair_query_with(&self, stop: &AtomicBool) -> Result<()> {
        let _maintenance = self
            .qv_maintenance
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.recover_query_build(Some(stop))?;
        {
            // Commits hold this lock until their debt is settled, so debt seen here is stranded.
            let _projection = self
                .projection_lock
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            self.fail_unrepaired_debt(&self.db.snapshot())?;
        }
        if self
            .snapshot_admission(self.read_snapshot().snapshot_ref())?
            .trusted
        {
            return Ok(());
        }
        match self.rebuild_query_inner(stop) {
            Err(StoreError::QueryIndexUnavailable(_)) => Ok(()),
            result => result,
        }
    }

    pub(crate) fn rebuild_query_indexes(&self) -> Result<()> {
        self.rebuild_query_with(&AtomicBool::new(false))
    }

    pub(crate) fn rebuild_query_with(&self, stop: &AtomicBool) -> Result<()> {
        let _maintenance = self
            .qv_maintenance
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.rebuild_query_inner(stop)
    }

    fn rebuild_query_inner(&self, stop: &AtomicBool) -> Result<()> {
        if stop.load(Ordering::Relaxed) {
            return Err(StoreError::Cancelled);
        }
        let (active, inactive) = {
            let _projection = match self.projection_lock.try_write() {
                Ok(guard) => guard,
                Err(TryLockError::Poisoned(error)) => error.into_inner(),
                Err(TryLockError::WouldBlock) => {
                    return Err(StoreError::QueryIndexUnavailable(
                        "query-index rebuild overlaps a graph commit",
                    ));
                }
            };
            let owner = self
                .qv_gate
                .try_acquire()
                .ok_or(StoreError::QueryIndexUnavailable(
                    "query-index rebuild overlaps a graph commit",
                ))?;
            let snapshot = self.db.snapshot();
            if self.projection_debt_present(&snapshot)? || self.query_build(&snapshot)?.is_some() {
                return Err(StoreError::QueryIndexUnavailable(
                    "query-index capture is not ready",
                ));
            }
            let active = match self.snapshot_index_header(&snapshot)? {
                IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => {
                    IndexSlot::decode(header.active_slot)
                        .ok_or(StoreError::IndexVerificationFailed("active-slot-invalid"))?
                }
                IndexHeaderRead::Absent | IndexHeaderRead::Malformed => IndexSlot::Primary,
            };
            let inactive = active.other();
            let build = QueryBuildRecord {
                format: QV_SCHEMA_VERSION,
                slot: inactive.encode(),
                source_sequence: 0,
                source_epoch: 0,
                control_digest: self.query_header_digest(&snapshot)?,
                trusted_active: false,
                scan_cursor: None,
                replay_cursor: 0,
                next_delta: 1,
                delta_rows: 0,
                delta_bytes: 0,
                phase: QueryBuildPhase::Clear,
                target: None,
            };
            let mut batch = self.buffered_batch();
            batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
            self.commit_fjall_batch(batch)?;
            #[cfg(test)]
            self.run_rebuild_hook(RebuildPhase::BuildCreate);
            owner.finish();
            (active, inactive)
        };

        self.clear_query_slot(inactive, Some(stop))?;

        let (previous, source_snapshot, mut build) = {
            let _projection = match self.projection_lock.try_write() {
                Ok(guard) => guard,
                Err(TryLockError::Poisoned(error)) => error.into_inner(),
                Err(TryLockError::WouldBlock) => {
                    return Err(StoreError::QueryIndexUnavailable(
                        "query-index rebuild overlaps a graph commit",
                    ));
                }
            };
            let owner = self
                .qv_gate
                .try_acquire()
                .ok_or(StoreError::QueryIndexUnavailable(
                    "query-index rebuild overlaps a graph commit",
                ))?;
            let capture = self.db.snapshot();
            let current = self
                .query_build(&capture)?
                .ok_or(StoreError::QueryIndexUnavailable(
                    "query-index build record missing",
                ))?;
            if current.slot != inactive.encode()
                || !matches!(current.phase, QueryBuildPhase::Clear)
                || self.projection_debt_present(&capture)?
            {
                return Err(StoreError::QueryIndexUnavailable(
                    "query-index capture changed",
                ));
            }
            let header_read = self.snapshot_index_header(&capture)?;
            let previous = match &header_read {
                IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => header.clone(),
                IndexHeaderRead::Absent | IndexHeaderRead::Malformed => IndexHeader::empty_ready(),
            };
            if IndexSlot::decode(previous.active_slot) != Some(active) {
                return Err(StoreError::QueryIndexUnavailable(
                    "query-index active slot changed",
                ));
            }
            let mut build = current;
            build.control_digest = self.query_header_digest(&capture)?;
            build.trusted_active = matches!(header_read, IndexHeaderRead::Valid(_))
                && self
                    .snapshot_admission(self.read_snapshot().snapshot_ref())?
                    .trusted;
            build.phase = QueryBuildPhase::Scan;
            let mut batch = self.buffered_batch();
            batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
            self.commit_fjall_batch(batch)?;
            let source_snapshot = self.db.snapshot();
            build.source_sequence = source_snapshot.seqno();
            build.source_epoch = if build.trusted_active {
                previous.source_epoch
            } else {
                source_snapshot.seqno()
            };
            let mut batch = self.buffered_batch();
            batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
            self.commit_fjall_batch(batch)?;
            owner.finish();
            (previous, source_snapshot, build)
        };

        let result =
            (|| -> Result<()> {
                let spaces = self.query_spaces(inactive);
                let (source_live_quads, next_query_id) = self.build_query_rows(QueryBuildCtx {
                    snapshot: &source_snapshot,
                    spaces,
                    build: &mut build,
                    stop,
                })?;
                let union_duplicate_free = self.index_union_unique(&self.db.snapshot(), spaces)?;
                let query_id_generation = self.next_generation(previous.query_id_generation);
                let mut candidate = IndexHeader {
                    active_slot: inactive.encode(),
                    state: StoredIndexState::Building,
                    source_epoch: build.source_epoch,
                    index_epoch: build.source_epoch,
                    source_live_quads,
                    indexed_quads: source_live_quads,
                    last_build_sequence: build.source_sequence,
                    query_id_generation,
                    next_query_id,
                };
                let mut batch = self.buffered_batch();
                batch.insert(spaces.meta, QV_TOTAL_KEY, source_live_quads.to_be_bytes());
                batch.insert(
                    spaces.meta,
                    IndexCounterKey::UnionDuplicateFree.bytes(),
                    u64::from(union_duplicate_free).to_be_bytes(),
                );
                self.commit_fjall_batch(batch)?;

                let latest = self.query_build(&self.db.snapshot())?.ok_or(
                    StoreError::QueryIndexUnavailable("query-index build record missing"),
                )?;
                build = latest;
                let target = build.next_delta.saturating_sub(1);
                self.replay_query_deltas(&mut build, &mut candidate, target, stop)?;

                let owner = self
                    .qv_gate
                    .acquire_timeout(self.qv_commit_wait)
                    .ok_or(StoreError::QueryIndexBusy)?;
                build = self.query_build(&self.db.snapshot())?.ok_or(
                    StoreError::QueryIndexUnavailable("query-index build record missing"),
                )?;
                let verify_target = build.next_delta.saturating_sub(1);
                self.replay_owned_deltas(&mut build, &mut candidate, verify_target, stop)?;
                build.phase = QueryBuildPhase::Verify;
                build.target = Some(verify_target);
                let mut state = self.buffered_batch();
                state.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
                self.commit_fjall_batch(state)?;
                let verification_snapshot = self.db.snapshot();
                owner.finish();
                let report = self.verify_index_snapshot(
                    &verification_snapshot,
                    true,
                    IndexVerifyState::BuildingCandidate,
                    Some(inactive),
                    Some(&candidate),
                )?;
                if !report.valid {
                    return Err(StoreError::IndexVerificationFailed(
                        "rebuild-verification-failed",
                    ));
                }
                #[cfg(test)]
                self.run_rebuild_hook(RebuildPhase::BeforeSwitch);
                let owner = self
                    .qv_gate
                    .acquire_timeout(self.qv_commit_wait)
                    .ok_or(StoreError::QueryIndexBusy)?;
                build = self.query_build(&self.db.snapshot())?.ok_or(
                    StoreError::QueryIndexUnavailable("query-index build record missing"),
                )?;
                let final_target = build.next_delta.saturating_sub(1);
                self.replay_owned_deltas(&mut build, &mut candidate, final_target, stop)?;
                let final_snapshot = self.db.snapshot();
                let active_matches = if build.trusted_active {
                    match self.snapshot_index_header(&final_snapshot)? {
                        IndexHeaderRead::Valid(current) => {
                            IndexSlot::decode(current.active_slot) == Some(active)
                                && candidate.source_epoch == current.source_epoch
                                && candidate.source_live_quads == current.source_live_quads
                        }
                        IndexHeaderRead::Absent
                        | IndexHeaderRead::Legacy(_)
                        | IndexHeaderRead::Malformed => false,
                    }
                } else {
                    self.query_header_digest(&final_snapshot)? == build.control_digest
                };
                if !active_matches
                    || build.slot != inactive.encode()
                    || build.replay_cursor != final_target
                    || candidate.indexed_quads != candidate.source_live_quads
                {
                    return Err(StoreError::IndexVerificationFailed(
                        "final-coverage-mismatch",
                    ));
                }
                let mut ready = candidate;
                ready.state = StoredIndexState::Ready;
                let mut batch = self.buffered_batch();
                self.stage_index_header(&mut batch, &ready);
                batch.remove(&self.qv2_meta, QV_BUILD_KEY);
                batch.insert(
                    &self.qv2_meta,
                    QV_CLEANUP_KEY,
                    postcard::to_allocvec(&QueryCleanupRecord {
                        format: QV_SCHEMA_VERSION,
                        slot: active.encode(),
                    })?,
                );
                let published = self.commit_fjall_batch(batch);
                #[cfg(test)]
                if published.is_ok() {
                    self.run_rebuild_hook(RebuildPhase::AfterSwitch);
                }
                owner.finish();
                published?;
                self.recover_query_build(Some(stop))
            })();
        if result.is_err() {
            tracing::warn!("query-index rebuild left its active generation unchanged");
        }
        result
    }

    /// Commit a batch and publish its bounded in-memory cache state.
    fn apply_commit(&self, commit: DurableCommit, publish: PendingPublish) -> Result<()> {
        if publish.is_empty() {
            return self.commit_durable(commit);
        }

        self.commit_with_index(commit, &publish)
    }

    /// Stall inside the publish window. Test-only.
    #[cfg(test)]
    fn stall_after_commit(&self) {
        let stall = *self
            .commit_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(delay) = stall {
            let active = self.commit_stall_active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_commit_stalls.fetch_max(active, Ordering::SeqCst);
            self.commit_stalled.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
            self.commit_stall_active.fetch_sub(1, Ordering::SeqCst);
            self.commit_stalled.store(false, Ordering::SeqCst);
        }
    }

    /// Make every later commit stall between the durable write and the index
    /// apply. Test-only.
    #[cfg(test)]
    pub(crate) fn set_commit_stall(&self, delay: std::time::Duration) {
        self.commit_stalled.store(false, Ordering::SeqCst);
        *self
            .commit_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
        self.commit_stall_active.store(0, Ordering::SeqCst);
        self.peak_commit_stalls.store(0, Ordering::SeqCst);
    }

    /// Whether a commit is inside its post-durable publication stall.
    #[cfg(test)]
    pub(crate) fn commit_stalled(&self) -> bool {
        self.commit_stalled.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn peak_commit_stalls(&self) -> usize {
        self.peak_commit_stalls.load(Ordering::SeqCst)
    }

    /// Writes every memtable to tables, for read-layout experiments. Test-only.
    #[cfg(test)]
    pub(crate) fn flush_memtables(&self) -> Result<()> {
        for name in self.db.list_keyspace_names() {
            let keyspace = self.db.keyspace(&name, KeyspaceCreateOptions::default)?;
            keyspace.rotate_memtable_and_wait()?;
        }
        Ok(())
    }

    /// Make exactly the next durable batch commit fail. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_commit_failure(&self) {
        self.commit_failure.store(true, Ordering::SeqCst);
    }

    /// Make snapshot policy reads of one graph fail until cleared. Test-only.
    #[cfg(test)]
    pub(crate) fn fail_policy_reads(&self, graph: Option<GraphId>) {
        *self
            .policy_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = graph;
    }

    #[cfg(test)]
    fn take_commit_failure(&self) -> bool {
        self.commit_failure.swap(false, Ordering::SeqCst)
    }

    /// Stall a rebuild between its scan and its install. Test-only.
    #[cfg(test)]
    fn stall_in_rebuild(&self) {
        let stall = *self
            .rebuild_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(delay) = stall {
            self.rebuild_stalled.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
        }
    }

    /// Make the next rebuild pause between its scan and its install. Test-only.
    #[cfg(test)]
    pub(crate) fn set_rebuild_stall(&self, delay: std::time::Duration) {
        *self
            .rebuild_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    /// Whether a rebuild is inside its stall. Test-only.
    #[cfg(test)]
    pub(crate) fn rebuild_stalled(&self) -> bool {
        self.rebuild_stalled.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn install_rebuild_hook(&self, hook: RebuildCallback) -> RebuildHook<'_> {
        *self
            .rebuild_hook
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(hook);
        RebuildHook { store: self }
    }

    #[cfg(test)]
    fn run_rebuild_hook(&self, phase: RebuildPhase) {
        let hook = self
            .rebuild_hook
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(phase);
        }
    }

    fn delta_limits(&self) -> (u64, u64) {
        #[cfg(test)]
        {
            (
                self.delta_row_limit.load(Ordering::SeqCst),
                self.delta_byte_limit.load(Ordering::SeqCst),
            )
        }
        #[cfg(not(test))]
        {
            (QV_DELTA_ROWS, QV_DELTA_BYTES)
        }
    }

    #[cfg(test)]
    pub(crate) fn set_delta_limits(&self, rows: u64, bytes: u64) -> DeltaLimitGuard<'_> {
        DeltaLimitGuard {
            store: self,
            rows: self.delta_row_limit.swap(rows, Ordering::SeqCst),
            bytes: self.delta_byte_limit.swap(bytes, Ordering::SeqCst),
        }
    }

    /// Stall a graph delete between its queue scan and its commit. Test-only.
    #[cfg(test)]
    fn stall_in_delete(&self) {
        let stall = *self
            .delete_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(delay) = stall {
            self.delete_stalled.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
        }
    }

    /// Make the next graph delete pause before it commits. Test-only.
    #[cfg(test)]
    pub(crate) fn set_delete_stall(&self, delay: std::time::Duration) {
        *self
            .delete_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    /// Whether a graph delete is inside its stall. Test-only.
    #[cfg(test)]
    pub(crate) fn delete_stalled(&self) -> bool {
        self.delete_stalled.load(Ordering::SeqCst)
    }

    /// Stall between an acknowledgement's token read and its commit. Test-only.
    #[cfg(test)]
    fn stall_search_ack(&self) {
        let stall = *self
            .fts_ack_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(delay) = stall {
            std::thread::sleep(delay);
        }
    }

    /// Widen the acknowledgement's check-and-remove window. Test-only.
    #[cfg(test)]
    pub(crate) fn set_ack_stall(&self, delay: std::time::Duration) {
        *self
            .fts_ack_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    fn snapshot_counter(
        &self,
        snapshot: &Snapshot,
        key: IndexCounterKey,
    ) -> Result<IndexCounterRead> {
        let Some(spaces) = self.active_query_spaces(&snapshot.into())? else {
            return Ok(IndexCounterRead::Missing);
        };
        Self::query_index_counter(snapshot, spaces, key)
    }

    fn query_index_counter(
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        key: IndexCounterKey,
    ) -> Result<IndexCounterRead> {
        Ok(match snapshot.get(spaces.meta, key.bytes())? {
            None => IndexCounterRead::Missing,
            Some(value) => match decode_index_count(value.as_ref()) {
                Some(value) => IndexCounterRead::Value(value),
                None => IndexCounterRead::Malformed,
            },
        })
    }

    fn query_revision(
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        predicate: QueryTermId,
    ) -> Result<IndexCounterRead> {
        Ok(
            match snapshot.get(spaces.meta, predicate_revision_key(predicate))? {
                None => IndexCounterRead::Missing,
                Some(value) => match decode_index_count(value.as_ref()) {
                    Some(value) => IndexCounterRead::Value(value),
                    None => IndexCounterRead::Malformed,
                },
            },
        )
    }

    fn adjusted_counter(current: u64, delta: i128) -> Option<u64> {
        if delta >= 0 {
            current.checked_add(u64::try_from(delta).ok()?)
        } else {
            current.checked_sub(u64::try_from(delta.checked_neg()?).ok()?)
        }
    }

    fn resolve_query_term(
        &self,
        term: TermId,
        resolver: &mut TermResolver<'_>,
    ) -> Result<Option<QueryTermId>> {
        if let Some(query) = resolver.resolved.get(&term) {
            return Ok(Some(*query));
        }
        if let Some(value) = resolver
            .snapshot
            .get(resolver.spaces.term_to_query, term.to_be_bytes())?
        {
            let Ok(raw) = <[u8; 8]>::try_from(value.as_ref()) else {
                return Ok(None);
            };
            let query = QueryTermId::from_be_bytes(raw);
            if query.0 >= resolver.next_query_id {
                return Ok(None);
            }
            let Some(reverse) = resolver
                .snapshot
                .get(resolver.spaces.query_to_term, query.to_be_bytes())?
            else {
                return Ok(None);
            };
            if reverse.as_ref() != term.to_be_bytes() {
                return Ok(None);
            }
            resolver.resolved.insert(term, query);
            return Ok(Some(query));
        }
        if !resolver.allow_allocate {
            return Ok(None);
        }
        let query = QueryTermId(resolver.next_query_id);
        let Some(next) = resolver.next_query_id.checked_add(1) else {
            return Ok(None);
        };
        if resolver
            .snapshot
            .get(resolver.spaces.query_to_term, query.to_be_bytes())?
            .is_some()
        {
            return Ok(None);
        }
        resolver.next_query_id = next;
        resolver.resolved.insert(term, query);
        resolver.mappings.push((term, query));
        Ok(Some(query))
    }

    fn index_spo_exists(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        quad: QueryQuad,
    ) -> Result<bool> {
        let key = spog_key(quad);
        let mut prefix = [0u8; 24];
        prefix.copy_from_slice(&key[..24]);
        match snapshot.prefix(spaces.spog, prefix).next() {
            Some(guard) => {
                let _ = guard.into_inner()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn insertions_keep_union(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        transitions: &[(QueryQuad, bool)],
        new_terms: &HashSet<QueryTermId>,
    ) -> Result<bool> {
        let mut inserted = BTreeSet::new();
        for (quad, is_live) in transitions {
            if !is_live {
                continue;
            }
            let spo = (quad.subject, quad.predicate, quad.object);
            if !inserted.insert(spo) {
                return Ok(false);
            }
            if new_terms.contains(&quad.subject)
                || new_terms.contains(&quad.predicate)
                || new_terms.contains(&quad.object)
            {
                continue;
            }
            if self.index_spo_exists(snapshot, spaces, *quad)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn transition_keeps_union(
        &self,
        snapshot: &Snapshot,
        spaces: IndexSpaces<'_>,
        transition: (QueryQuad, bool),
        mappings: &[(TermId, QueryTermId)],
    ) -> Result<bool> {
        let (quad, is_live) = transition;
        if !is_live {
            return Ok(true);
        }
        if mappings.iter().any(|(_, query)| {
            *query == quad.subject || *query == quad.predicate || *query == quad.object
        }) {
            return Ok(true);
        }
        Ok(!self.index_spo_exists(snapshot, spaces, quad)?)
    }

    fn plan_index_update(
        &self,
        snapshot: &Snapshot,
        header: &IndexHeader,
        transitions: Vec<NetQuadTransition>,
    ) -> Result<Option<IndexUpdatePlan>> {
        let Some(slot) = IndexSlot::decode(header.active_slot) else {
            return Ok(None);
        };
        let spaces = self.query_spaces(slot);
        let total = match Self::query_index_counter(snapshot, spaces, IndexCounterKey::Total)? {
            IndexCounterRead::Value(total) if total == header.indexed_quads => total,
            IndexCounterRead::Missing
            | IndexCounterRead::Malformed
            | IndexCounterRead::Value(_) => return Ok(None),
        };

        let mut resolver = TermResolver {
            snapshot,
            spaces,
            allow_allocate: false,
            resolved: HashMap::new(),
            mappings: Vec::new(),
            next_query_id: header.next_query_id,
        };
        let mut query_transitions = Vec::with_capacity(transitions.len());
        for transition in transitions {
            resolver.allow_allocate = transition.is_live;
            let Some(graph) = self.resolve_query_term(transition.quad.graph, &mut resolver)? else {
                return Ok(None);
            };
            let Some(subject) = self.resolve_query_term(transition.quad.subject, &mut resolver)?
            else {
                return Ok(None);
            };
            let Some(predicate) =
                self.resolve_query_term(transition.quad.predicate, &mut resolver)?
            else {
                return Ok(None);
            };
            let Some(object) = self.resolve_query_term(transition.quad.object, &mut resolver)?
            else {
                return Ok(None);
            };
            let quad = QueryQuad {
                graph,
                subject,
                predicate,
                object,
            };
            let mut already_desired = true;
            let mut prior_state_all = true;
            for (keyspace, key) in [
                (spaces.gspo, gspo_key(quad)),
                (spaces.gpos, gpos_key(quad)),
                (spaces.spog, spog_key(quad)),
                (spaces.posg, posg_key(quad)),
                (spaces.ospg, ospg_key(quad)),
                (spaces.gosp, gosp_key(quad)),
            ] {
                let current = snapshot.get(keyspace, key)?;
                let present = match current {
                    None => false,
                    Some(value) if value.as_ref().is_empty() => true,
                    Some(_) => return Ok(None),
                };
                already_desired &= present == transition.is_live;
                prior_state_all &= present != transition.is_live;
            }
            if already_desired {
                continue;
            }
            if !prior_state_all {
                return Ok(None);
            }
            query_transitions.push((quad, transition.is_live));
        }
        let mappings = resolver.mappings;
        let next_query_id = resolver.next_query_id;

        if query_transitions.is_empty() {
            return Ok(Some(IndexUpdatePlan {
                slot,
                transitions: query_transitions,
                mappings,
                counters: Vec::new(),
                revisions: Vec::new(),
                header: None,
            }));
        }

        let union_duplicate_free =
            match Self::query_index_counter(snapshot, spaces, IndexCounterKey::UnionDuplicateFree)?
            {
                IndexCounterRead::Value(0) => false,
                IndexCounterRead::Value(1) => true,
                IndexCounterRead::Missing
                | IndexCounterRead::Malformed
                | IndexCounterRead::Value(_) => return Ok(None),
            };
        let union_uniqueness_preserved = !union_duplicate_free
            || if let [transition] = query_transitions.as_slice() {
                self.transition_keeps_union(snapshot, spaces, *transition, &mappings)?
            } else {
                let new_terms = mappings.iter().map(|(_, query)| *query).collect();
                self.insertions_keep_union(snapshot, spaces, &query_transitions, &new_terms)?
            };

        let mut deltas = BTreeMap::<Vec<u8>, (IndexCounterKey, i128)>::new();
        for (quad, is_live) in &query_transitions {
            let delta = if *is_live { 1 } else { -1 };
            for counter in live_counter_keys(*quad) {
                let entry = deltas.entry(counter.bytes()).or_insert((counter, 0));
                let Some(next) = entry.1.checked_add(delta) else {
                    return Ok(None);
                };
                entry.1 = next;
            }
        }

        let mut counters = Vec::with_capacity(deltas.len() + 1);
        for (counter, delta) in deltas.values() {
            let has_rows = !matches!(counter, IndexCounterKey::Total)
                && self.counter_has_rows(snapshot, spaces, *counter)?;
            let current = match Self::query_index_counter(snapshot, spaces, *counter)? {
                IndexCounterRead::Missing if !matches!(counter, IndexCounterKey::Total) => {
                    if has_rows {
                        return Ok(None);
                    }
                    0
                }
                IndexCounterRead::Value(value)
                    if matches!(counter, IndexCounterKey::Total) || (value != 0 && has_rows) =>
                {
                    value
                }
                IndexCounterRead::Missing
                | IndexCounterRead::Malformed
                | IndexCounterRead::Value(_) => return Ok(None),
            };
            let Some(next) = Self::adjusted_counter(current, *delta) else {
                return Ok(None);
            };
            counters.push(IndexCounterUpdate {
                key: *counter,
                value: if matches!(counter, IndexCounterKey::Total) || next != 0 {
                    Some(next)
                } else {
                    None
                },
            });
        }
        let predicates: HashSet<_> = query_transitions
            .iter()
            .map(|(quad, _)| quad.predicate)
            .collect();
        let mut revisions = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let current = match Self::query_revision(snapshot, spaces, predicate)? {
                IndexCounterRead::Missing => 0,
                IndexCounterRead::Value(value) => value,
                IndexCounterRead::Malformed => return Ok(None),
            };
            let Some(next) = current.checked_add(1) else {
                return Ok(None);
            };
            revisions.push((predicate, next));
        }
        if union_duplicate_free && !union_uniqueness_preserved {
            counters.push(IndexCounterUpdate {
                key: IndexCounterKey::UnionDuplicateFree,
                value: Some(0),
            });
        }

        let total_delta = deltas
            .get(QV_TOTAL_KEY.as_slice())
            .map(|(_, delta)| *delta)
            .unwrap_or(0);
        let Some(source_live_quads) = Self::adjusted_counter(header.source_live_quads, total_delta)
        else {
            return Ok(None);
        };
        let Some(indexed_quads) = Self::adjusted_counter(header.indexed_quads, total_delta) else {
            return Ok(None);
        };
        let Some(source_epoch) = header.source_epoch.checked_add(1) else {
            return Ok(None);
        };

        let Some(updated_total) = counters
            .iter()
            .find(|update| matches!(update.key, IndexCounterKey::Total))
            .and_then(|update| update.value)
        else {
            return Ok(None);
        };
        if total != header.indexed_quads
            || updated_total != indexed_quads
            || source_live_quads != indexed_quads
        {
            return Ok(None);
        }
        Ok(Some(IndexUpdatePlan {
            slot,
            transitions: query_transitions,
            mappings,
            counters,
            revisions,
            header: Some(IndexHeader {
                active_slot: header.active_slot,
                state: StoredIndexState::Ready,
                source_epoch,
                index_epoch: source_epoch,
                source_live_quads,
                indexed_quads,
                last_build_sequence: header.last_build_sequence,
                query_id_generation: header.query_id_generation,
                next_query_id,
            }),
        }))
    }

    fn stage_index_plan(&self, batch: &mut fjall::OwnedWriteBatch, plan: IndexUpdatePlan) {
        let spaces = self.query_spaces(plan.slot);
        for (term, query) in plan.mappings {
            batch.insert(
                spaces.term_to_query,
                term.to_be_bytes(),
                query.to_be_bytes(),
            );
            batch.insert(
                spaces.query_to_term,
                query.to_be_bytes(),
                term.to_be_bytes(),
            );
        }
        for (quad, is_live) in plan.transitions {
            let keys = [
                (spaces.gspo, gspo_key(quad)),
                (spaces.gpos, gpos_key(quad)),
                (spaces.spog, spog_key(quad)),
                (spaces.posg, posg_key(quad)),
                (spaces.ospg, ospg_key(quad)),
                (spaces.gosp, gosp_key(quad)),
            ];
            for (keyspace, key) in keys {
                if is_live {
                    batch.insert(keyspace, key, Vec::<u8>::new());
                } else {
                    batch.remove(keyspace, key);
                }
            }
        }
        for update in plan.counters {
            match update.value {
                Some(value) => batch.insert(spaces.meta, update.key.bytes(), value.to_be_bytes()),
                None => batch.remove(spaces.meta, update.key.bytes()),
            }
        }
        for (predicate, revision) in plan.revisions {
            batch.insert(
                spaces.meta,
                predicate_revision_key(predicate),
                revision.to_be_bytes(),
            );
        }
        if let Some(header) = plan.header {
            self.stage_index_header(batch, &header);
        }
    }

    fn stage_index_update(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        publish: &PendingPublish,
    ) -> Result<()> {
        let snapshot = self.db.snapshot();
        let transitions = coalesced_transitions(&publish.quad_mutations);
        let build_active = self.query_build(&snapshot)?.is_some();
        match self.snapshot_index_header(&snapshot)? {
            IndexHeaderRead::Absent | IndexHeaderRead::Legacy(_) => {}
            IndexHeaderRead::Malformed if !build_active => {
                self.stage_index_failure(batch, None, "metadata-malformed");
            }
            IndexHeaderRead::Malformed => {}
            IndexHeaderRead::Valid(header) => match header.state {
                StoredIndexState::Building | StoredIndexState::Failed(_) => {}
                StoredIndexState::Ready => {
                    if !header.ready_is_coherent() || !header.fits_snapshot(snapshot.seqno()) {
                        self.stage_index_failure(
                            batch,
                            Some(&header),
                            "ready-metadata-inconsistent",
                        );
                    } else {
                        match self.plan_index_update(&snapshot, &header, transitions.clone())? {
                            Some(plan) => self.stage_index_plan(batch, plan),
                            None => self.stage_index_failure(
                                batch,
                                Some(&header),
                                "maintenance-anomaly",
                            ),
                        }
                    }
                }
            },
        }
        self.stage_query_delta(batch, &snapshot, transitions)
    }

    fn query_build(&self, snapshot: &Snapshot) -> Result<Option<QueryBuildRecord>> {
        let build: Option<QueryBuildRecord> = snapshot
            .get(&self.qv2_meta, QV_BUILD_KEY)?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(StoreError::from))
            .transpose()?;
        if let Some(build) = &build
            && build.format > QV_SCHEMA_VERSION
        {
            return Err(StoreError::UnsupportedIndexFormat {
                found: build.format,
                supported: QV_SCHEMA_VERSION,
            });
        }
        Ok(build.filter(|build| build.format == QV_SCHEMA_VERSION))
    }

    fn update_build_cursor(&self, cursor: Option<QuadKey>) -> Result<()> {
        let owner = self
            .qv_gate
            .acquire_timeout(self.qv_commit_wait)
            .ok_or(StoreError::QueryIndexBusy)?;
        let snapshot = self.db.snapshot();
        let mut build = self
            .query_build(&snapshot)?
            .ok_or(StoreError::QueryIndexUnavailable(
                "query-index build record missing",
            ))?;
        build.scan_cursor = cursor;
        let mut batch = self.buffered_batch();
        batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
        let result = self.commit_fjall_batch(batch);
        owner.finish();
        result
    }

    fn stage_query_delta(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        snapshot: &Snapshot,
        transitions: Vec<NetQuadTransition>,
    ) -> Result<()> {
        let Some(mut build) = self.query_build(snapshot)? else {
            return Ok(());
        };
        if matches!(build.phase, QueryBuildPhase::Clear) {
            return Ok(());
        }
        if transitions.is_empty() {
            return Ok(());
        }
        let delta = QueryDeltaRecord { transitions };
        let encoded = postcard::to_allocvec(&delta)?;
        let rows = u64::try_from(delta.transitions.len()).unwrap_or(u64::MAX);
        let bytes = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        let next_rows = build.delta_rows.saturating_add(rows);
        let next_bytes = build.delta_bytes.saturating_add(bytes);
        let (row_limit, byte_limit) = self.delta_limits();
        if next_rows > row_limit || next_bytes > byte_limit {
            // A disposable build must not block source writes; maintenance clears it and rebuilds.
            let cleanup = QueryCleanupRecord {
                format: QV_SCHEMA_VERSION,
                slot: build.slot,
            };
            batch.remove(&self.qv2_meta, QV_BUILD_KEY);
            batch.insert(
                &self.qv2_meta,
                QV_CLEANUP_KEY,
                postcard::to_allocvec(&cleanup)?,
            );
            return Ok(());
        }
        let delta_id = build.next_delta;
        build.next_delta = build
            .next_delta
            .checked_add(1)
            .ok_or(StoreError::QueryIndexCapacity)?;
        build.delta_rows = next_rows;
        build.delta_bytes = next_bytes;
        batch.insert(&self.qv2_meta, query_delta_key(delta_id), encoded);
        batch.insert(&self.qv2_meta, QV_BUILD_KEY, postcard::to_allocvec(&build)?);
        Ok(())
    }

    /// Stages durable debt that keeps uncovered source rows out of QV admission.
    fn stage_projection_debt(&self, batch: &mut fjall::OwnedWriteBatch) -> u64 {
        let debt = self.qv_debt_next.fetch_add(1, Ordering::AcqRel);
        batch.insert(&self.qv2_meta, projection_debt_key(debt), [0u8; 0]);
        debt
    }

    /// Covers one source-only commit and clears its projection debt in a single
    /// batch. Any failure commits nothing, so the debt keeps admission closed.
    fn repair_projection_debt(&self, publish: &PendingPublish, debt: u64) -> Result<()> {
        let owner = self
            .qv_gate
            .acquire_timeout(self.qv_commit_wait)
            .ok_or(StoreError::QueryIndexBusy)?;
        let mut batch = self.buffered_batch();
        self.stage_index_update(&mut batch, publish)?;
        batch.remove(&self.qv2_meta, projection_debt_key(debt));
        let result = self.commit_fjall_batch(batch);
        owner.finish();
        result
    }

    /// Retires a delta that can no longer be replayed and closes QV admission.
    fn fail_projection_debt(&self, debt: u64, reason: &'static str) -> Result<()> {
        let snapshot = self.db.snapshot();
        let previous = match self.snapshot_index_header(&snapshot)? {
            IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => Some(header),
            IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        batch.remove(&self.qv2_meta, projection_debt_key(debt));
        self.stage_index_failure(&mut batch, previous.as_ref(), reason);
        self.commit_fjall_batch(batch)
    }

    /// True when this snapshot records source rows whose query-view maintenance
    /// has not been committed. One seek; the prefix is empty in steady state.
    fn projection_debt_present(&self, snapshot: &Snapshot) -> Result<bool> {
        match snapshot.prefix(&self.qv2_meta, [QV_DEBT_TAG]).next() {
            Some(guard) => {
                let _ = guard.into_inner()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Converts unrecoverable projection debt into a rebuild-required failure.
    fn fail_unrepaired_debt(&self, snapshot: &Snapshot) -> Result<bool> {
        let mut debts = Vec::new();
        for guard in snapshot.prefix(&self.qv2_meta, [QV_DEBT_TAG]) {
            let (key, _) = guard.into_inner()?;
            debts.push(key);
        }
        if debts.is_empty() {
            return Ok(false);
        }
        let previous = match self.snapshot_index_header(snapshot)? {
            IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => Some(header),
            IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        for debt in debts {
            batch.remove(&self.qv2_meta, debt);
        }
        self.stage_index_failure(&mut batch, previous.as_ref(), "projection-debt-unrepaired");
        self.commit_fjall_batch(batch)?;
        Ok(true)
    }

    /// Commits source with QV rows or durable debt, then publishes cache state.
    fn commit_with_index(&self, mut commit: DurableCommit, publish: &PendingPublish) -> Result<()> {
        let _projection = self
            .projection_lock
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let mut owner = self.qv_gate.try_acquire();
        if owner.is_none() && self.query_build(&self.db.snapshot())?.is_some() {
            owner = self.qv_gate.acquire_timeout(self.qv_commit_wait);
            if owner.is_none() {
                return Err(StoreError::QueryIndexBusy);
            }
        }
        let debt = match &owner {
            Some(_) => {
                self.stage_index_update(&mut commit.batch, publish)?;
                None
            }
            None => Some(self.stage_projection_debt(&mut commit.batch)),
        };
        let committed = self.commit_durable(commit);
        let published = if committed.is_ok() {
            #[cfg(test)]
            self.stall_after_commit();
            self.indexes_write().publish(publish);
            true
        } else {
            false
        };
        drop(owner);
        committed?;
        debug_assert!(published, "successful durable commit publishes cache state");
        if let Some(debt) = debt
            && let Err(error) = self.repair_projection_debt(publish, debt)
        {
            tracing::warn!(
                error = %error,
                "query-view catch-up failed after a durable source commit"
            );
            let _ = self.fail_projection_debt(debt, "concurrent-catch-up-failed");
        }
        Ok(())
    }

    pub fn ensure_derived_indexes(&self) {
        // qv and source keyspaces are the read authority; there is no required
        // corpus-wide in-memory mirror to warm.
    }

    /// Diagnostics recomputations performed by this store instance.
    #[cfg(test)]
    pub(crate) fn diagnostics_compute_count(&self) -> u64 {
        self.diagnostics_computed.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn cache_statistics(&self) -> [CacheStatistics; 3] {
        let indexes = self.indexes_read();
        let quad = indexes.quad_subjects.statistics();
        let object = indexes.object_order.statistics();
        drop(indexes);
        let mut terms = self
            .term_decode_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .statistics();
        terms.hits = terms
            .hits
            .saturating_add(self.term_cache_hits.load(Ordering::Relaxed));
        terms.misses = terms
            .misses
            .saturating_add(self.term_cache_misses.load(Ordering::Relaxed));
        [quad, object, terms]
    }

    #[cfg(test)]
    pub(crate) fn admission_probe_count(&self) -> u64 {
        self.index_admission_probes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn index_verify_count(&self) -> u64 {
        self.index_verification_runs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn fail_test_indexes(&self) {
        let snapshot = self.db.snapshot();
        let previous = match self.snapshot_index_header(&snapshot).unwrap() {
            IndexHeaderRead::Valid(header) | IndexHeaderRead::Legacy(header) => Some(header),
            IndexHeaderRead::Absent | IndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        self.stage_index_failure(&mut batch, previous.as_ref(), "test-failure");
        self.commit_fjall_batch(batch).unwrap();
    }

    #[cfg(test)]
    pub(crate) fn set_test_index(&self, state: QueryIndexState) {
        let snapshot = self.db.snapshot();
        let IndexHeaderRead::Valid(mut header) = self.snapshot_index_header(&snapshot).unwrap()
        else {
            panic!("query-index header must be present before degrading it");
        };
        let mut batch = self.buffered_batch();
        match state {
            QueryIndexState::Missing => batch.remove(&self.qv2_meta, QV_HEADER_KEY),
            QueryIndexState::Building => {
                header.state = StoredIndexState::Building;
                self.stage_index_header(&mut batch, &header);
            }
            QueryIndexState::Failed(reason) => {
                header.state = StoredIndexState::Failed(reason);
                self.stage_index_header(&mut batch, &header);
            }
            QueryIndexState::Ready => panic!("test helper only degrades query indexes"),
        }
        self.commit_fjall_batch(batch).unwrap();
    }

    /// Resolves the vocabulary ids used by orphan detection.
    fn orphan_vocab(&self) -> Result<OrphanVocab> {
        let id = |named_node: oxrdf::NamedNode| {
            self.lookup_term(&EncodedTerm::from_named_node(&named_node))
        };
        Ok(OrphanVocab {
            rdf_type: id(crate::core::vocab::rdf_type())?,
            data_types: [
                id(crate::core::vocab::schema_dataset())?,
                id(crate::core::vocab::schema_media_object())?,
            ],
            has_part: id(crate::core::vocab::schema_has_part())?,
        })
    }

    /// Computes orphaned entity ids directly from durable term ids.
    fn orphaned_entity_ids(
        &self,
        graph_id: TermId,
        vocab: &OrphanVocab,
    ) -> Result<HashSet<TermId>> {
        let mut data_entities: HashSet<TermId> = HashSet::new();
        let mut adjacency: HashMap<TermId, Vec<TermId>> = HashMap::new();
        self.visit_stored_quads(graph_id, |quad, _| {
            if vocab.has_part == Some(quad.predicate) {
                adjacency.entry(quad.subject).or_default().push(quad.object);
                if quad.subject != graph_id {
                    data_entities.insert(quad.subject);
                }
                if quad.object != graph_id {
                    data_entities.insert(quad.object);
                }
            }
            if vocab.rdf_type == Some(quad.predicate)
                && quad.subject != graph_id
                && vocab.data_types.contains(&Some(quad.object))
            {
                data_entities.insert(quad.subject);
            }
            Ok(())
        })?;

        if data_entities.is_empty() {
            return Ok(HashSet::new());
        }

        let mut reachable: HashSet<TermId> = HashSet::from([graph_id]);
        let mut queue: VecDeque<TermId> = VecDeque::from([graph_id]);
        while let Some(current) = queue.pop_front() {
            for &neighbor in adjacency.get(&current).into_iter().flatten() {
                if reachable.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }

        data_entities.retain(|entity| !reachable.contains(entity));
        Ok(data_entities)
    }

    /// Computes orphan ids from one snapshot without persisting read results.
    fn snapshot_orphan_ids(
        &self,
        snapshot: &Snapshot,
        context: &crate::query::context::ReadContext<'_>,
        graph_id: TermId,
        vocab: &OrphanVocab,
    ) -> Result<HashSet<TermId>> {
        let mut data_entities = HashSet::new();
        let mut adjacency = HashMap::<TermId, Vec<TermId>>::new();
        let mut work_since_check = 0usize;
        context.check_cancelled()?;
        context.increment_index_seeks();
        for guard in snapshot.prefix(&self.quads, graph_id.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            context.increment_candidate_quads();
            context.record_source_read((key.len() + value.len()) as u64);
            work_since_check += 1;
            if work_since_check == 1_024 {
                work_since_check = 0;
                context.check_cancelled()?;
            }
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            context.record_key_fields(4);
            context.increment_quad_builds();
            if vocab.has_part == Some(quad.predicate) {
                adjacency.entry(quad.subject).or_default().push(quad.object);
                if quad.subject != graph_id {
                    data_entities.insert(quad.subject);
                }
                if quad.object != graph_id {
                    data_entities.insert(quad.object);
                }
            }
            if vocab.rdf_type == Some(quad.predicate)
                && quad.subject != graph_id
                && vocab.data_types.contains(&Some(quad.object))
            {
                data_entities.insert(quad.subject);
            }
        }

        if data_entities.is_empty() {
            return Ok(HashSet::new());
        }
        let mut reachable = HashSet::from([graph_id]);
        let mut queue = VecDeque::from([graph_id]);
        while let Some(current) = queue.pop_front() {
            for &neighbor in adjacency.get(&current).into_iter().flatten() {
                work_since_check += 1;
                if work_since_check == 1_024 {
                    work_since_check = 0;
                    context.check_cancelled()?;
                }
                if reachable.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
        data_entities.retain(|entity| !reachable.contains(entity));
        Ok(data_entities)
    }

    fn snapshot_stored_diagnostics(
        &self,
        snapshot: &Snapshot,
        graph_id: TermId,
    ) -> Result<Option<StoredDiagnostics>> {
        snapshot
            .get(&self.graphs, graph_diagnostics_key(graph_id))?
            .map(|bytes| postcard::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(Into::into)
    }

    fn snapshot_vector_clock(&self, snapshot: &Snapshot, graph_id: TermId) -> Result<VectorClock> {
        if let Some(bytes) = snapshot.get(&self.graphs, graph_clock_key(graph_id))? {
            return Ok(postcard::from_bytes(bytes.as_ref())?);
        }
        Ok(snapshot
            .get(&self.graphs, graph_meta_key(graph_id))?
            .map(|bytes| postcard::from_bytes::<StoredGraphMeta>(bytes.as_ref()))
            .transpose()?
            .unwrap_or_default()
            .clock)
    }

    fn compute_graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        self.diagnostics_computed.fetch_add(1, Ordering::Relaxed);
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphDiagnostics::default());
        };
        let orphans = self.orphaned_entity_ids(graph_id, &self.orphan_vocab()?)?;
        // Only the orphans are decoded; the common case is none at all.
        let mut entities = Vec::with_capacity(orphans.len());
        for orphan in orphans {
            let term = self.decode_term_arc(orphan)?;
            entities.push(
                term.to_named_node()
                    .map(|named_node| named_node.as_str().to_string())
                    .unwrap_or_else(|| term.0.clone()),
            );
        }
        Ok(GraphDiagnostics::from_orphaned_entities(entities))
    }

    /// Repairs missing or stale persisted diagnostics before the store opens.
    fn repair_diagnostics(&self) -> Result<()> {
        for graph_id in self.graph_term_ids()? {
            let clock = self.snapshot_vector_clock(&self.db.snapshot(), graph_id)?;
            let stored = self.read_stored_diagnostics(graph_id)?;
            if stored
                .as_ref()
                .is_some_and(|record| record.at_clock == clock)
            {
                continue;
            }

            let previous = stored.map(|record| record.diagnostics).unwrap_or_default();
            let repaired = self.compute_tagged_diagnostics(graph_id)?;
            // Re-queue before recording so a crash retains a repairable baseline.
            self.requeue_orphan_changes(graph_id, (&previous, &repaired.diagnostics))?;
            self.store_diagnostics_record(graph_id, repaired)?;
        }
        Ok(())
    }

    /// Merges literal aliases that earlier versions stored under their raw spelling.
    fn repair_literal_aliases(&self) -> Result<()> {
        if self.graphs.contains_key(LITERAL_ALIAS_KEY)? {
            return Ok(());
        }
        let mut aliases = HashMap::new();
        for guard in self.db.snapshot().iter(&self.terms) {
            let (key, value) = guard.into_inner()?;
            if value.first() != Some(&b'"') {
                continue;
            }
            if let Some(canonical) = EncodedTerm(decode_term_text(value.as_ref())?).canonical() {
                let alias = decode_term_id(key.as_ref(), "term key")?;
                aliases.insert(alias, self.encode_term(&canonical)?);
            }
        }
        if !aliases.is_empty() {
            self.merge_aliases(&aliases)?;
        }
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, LITERAL_ALIAS_KEY, []);
        self.commit_fjall_batch(batch)
    }

    /// Moves the dots of every quad that uses an alias term onto its canonical quad.
    fn merge_aliases(&self, aliases: &HashMap<TermId, TermId>) -> Result<()> {
        let canonical = |id: TermId| aliases.get(&id).copied().unwrap_or(id);
        let mut batch = self.new_batch();
        let mut subjects: HashMap<TermId, HashSet<TermId>> = HashMap::new();
        let mut staged = 0;
        for guard in self.db.snapshot().iter(&self.quads) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 64 || dots_empty(value.as_ref()) {
                continue;
            }
            let raw = Self::decode_quad_key(key.as_ref())?;
            let quad = EncodedQuad {
                subject: canonical(raw.subject),
                predicate: canonical(raw.predicate),
                object: canonical(raw.object),
                ..raw
            };
            if quad == raw {
                continue;
            }
            let target = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
            let mut dots = self.current_quad_dots(&batch, &target)?;
            dots.extend(decode_dots(value.as_ref())?);
            self.write_quad_state(&mut batch, quad, dots)?;
            self.write_quad_state(&mut batch, raw, Vec::new())?;
            subjects.entry(quad.graph).or_default().insert(quad.subject);
            staged += 1;
            if staged == QV_BUILD_ROWS {
                let full = std::mem::replace(&mut batch, self.new_batch());
                self.commit_aliases(full, std::mem::take(&mut subjects))?;
                staged = 0;
            }
        }
        self.commit_aliases(batch, subjects)
    }

    /// Commits merged aliases with the search and SHACL work their graphs now owe.
    fn commit_aliases(
        &self,
        mut batch: WriteBatch,
        subjects: HashMap<TermId, HashSet<TermId>>,
    ) -> Result<()> {
        if subjects.is_empty() {
            return Ok(());
        }
        for (graph_id, subjects) in &subjects {
            let graph_id = *graph_id;
            self.enqueue_fts_subjects(&mut batch, FtsEnqueue { graph_id, subjects })?;
            #[cfg(feature = "shacl-core")]
            if let Some(graph) = self.decode_term(graph_id)?.to_named_node() {
                let graph = GraphId(graph);
                let version = self.graph_version_digest(&graph)?;
                self.stage_pending_bindings(&mut batch, &graph, version)?;
            }
        }
        self.commit(batch)
    }

    /// Re-queues entities whose repaired orphan visibility changed.
    fn requeue_orphan_changes(
        &self,
        graph_id: TermId,
        change: (&GraphDiagnostics, &GraphDiagnostics),
    ) -> Result<()> {
        let (previous, current) = change;
        if previous == current {
            return Ok(());
        }

        let before: HashSet<&String> = previous.orphaned_entities.iter().collect();
        let after: HashSet<&String> = current.orphaned_entities.iter().collect();
        let mut subjects = HashSet::new();
        for entity in before.symmetric_difference(&after) {
            // `from_subject_id`, not `from_named_node`: a blank node is stored
            // as `_:b0`, and the IRI `<_:b0>` would miss the lookup.
            let term = EncodedTerm::from_subject_id(entity.as_str());
            match self.lookup_term(&term)? {
                Some(subject) => {
                    subjects.insert(subject);
                }
                // A literal cannot be re-encoded as a subject, so its search
                // document stays stale until something else dirties it.
                None => tracing::warn!(
                    entity = entity.as_str(),
                    "orphan re-queue skipped an entity it could not look up"
                ),
            }
        }
        if subjects.is_empty() {
            return Ok(());
        }

        let mut batch = self.new_batch();
        self.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &subjects,
            },
        )?;
        self.commit(batch)
    }

    fn graph_id_for(&self, graph: &GraphId) -> Result<Option<TermId>> {
        self.lookup_term(&EncodedTerm::from_named_node(&graph.0))
    }

    fn graph_scan(
        &self,
        graph: TermId,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        let mut quads = Vec::new();
        for guard in self.quads.prefix(graph.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            if predicate.is_some_and(|expected| expected != quad.predicate)
                || object.is_some_and(|expected| expected != quad.object)
            {
                continue;
            }
            quads.push(quad);
        }
        Ok(quads)
    }

    fn source_pattern_count(
        &self,
        pattern: crate::rdf_read::QuadPattern,
        costs: &crate::query::context::QueryCost,
    ) -> PlannerEstimate {
        let snapshot = self.read_snapshot();
        let mut cursor = snapshot.raw_quad_cursor(self, pattern);
        let mut count = 0usize;
        let mut visited = 0usize;
        while let Some(candidate) = cursor.next_candidate() {
            costs.planner_entries(1);
            if visited == PLANNER_SAMPLE_ROWS {
                return PlannerEstimate::Unknown;
            }
            visited += 1;
            let Ok(candidate) = candidate else {
                return PlannerEstimate::Unknown;
            };
            if candidate.live && pattern.matches(candidate.quad) {
                count = count.saturating_add(1);
            }
        }
        PlannerEstimate::Exact(count)
    }

    fn index_pattern_count(
        &self,
        scan: &crate::query::cursor::IndexScan<'_>,
    ) -> Option<PlannerEstimate> {
        let snapshot = self.read_snapshot();
        let admission = snapshot.query_index_admission(self).ok()?;
        scan.costs.planner_points(
            admission
                .debt_reads
                .saturating_add(admission.header_reads)
                .saturating_add(admission.counter_reads.saturating_mul(2)),
        );
        if !admission.trusted {
            return None;
        }
        let mut cursor = snapshot
            .query_index_cursor(self, scan)
            .ok()?
            .track_costs(scan.costs.clone());
        let mut count = 0usize;
        let mut visited = 0usize;
        while let Some(candidate) = cursor.next_candidate() {
            scan.costs.planner_entries(1);
            if visited == PLANNER_SAMPLE_ROWS {
                return Some(PlannerEstimate::Unknown);
            }
            visited += 1;
            let candidate = candidate.ok()?;
            if candidate.live && scan.pattern.matches(candidate.quad) {
                count = count.saturating_add(1);
            }
        }
        Some(PlannerEstimate::Exact(count))
    }

    fn index_distinct_count(
        &self,
        stat: DistinctStat,
        costs: &crate::query::context::QueryCost,
    ) -> Option<PlannerEstimate> {
        let view = self.read_snapshot();
        let snapshot = view.snapshot_ref();
        let admission = self.snapshot_admission(snapshot).ok()?;
        costs.planner_points(
            admission
                .debt_reads
                .saturating_add(admission.header_reads)
                .saturating_add(admission.counter_reads.saturating_mul(2)),
        );
        if !admission.trusted {
            return None;
        }
        costs.planner_points(1);
        let IndexHeaderRead::Valid(header) = self.snapshot_index_header(snapshot).ok()? else {
            return None;
        };
        let slot = IndexSlot::decode(header.active_slot)?;
        let spaces = self.query_spaces(slot);
        let predicate = match stat.predicate {
            Some(predicate) => {
                costs.planner_points(1);
                let value = snapshot
                    .get(spaces.term_to_query, predicate.to_be_bytes())
                    .ok()?;
                costs.forward_mapping(16 + value.as_ref().map_or(0, |value| value.len() as u64));
                match value {
                    Some(value) => {
                        Some(decode_query_id(value.as_ref(), "term-to-query mapping").ok()?)
                    }
                    None => return Some(PlannerEstimate::Exact(0)),
                }
            }
            None => None,
        };
        let revision = match predicate {
            Some(predicate) => {
                costs.planner_points(1);
                match Self::query_revision(snapshot, spaces, predicate).ok()? {
                    IndexCounterRead::Value(version) => StatRevision::Predicate(version),
                    IndexCounterRead::Missing => StatRevision::Epoch(header.source_epoch),
                    IndexCounterRead::Malformed => return None,
                }
            }
            None => StatRevision::Epoch(header.source_epoch),
        };
        let cache_key = (header.query_id_generation, revision, predicate, stat.domain);
        if let Some(estimate) = self.indexes_write().planner_distinct.get_cloned(&cache_key) {
            costs.planner_cache(true);
            return Some(estimate);
        }
        costs.planner_cache(false);

        // Capped distinct samples are lower bounds, which biases join output upward.
        let estimate = match (predicate, stat.domain) {
            (Some(predicate), DistinctDomain::Subject) => {
                let mut subjects = HashSet::new();
                let mut truncated = false;
                for (visited, guard) in snapshot
                    .prefix(spaces.posg, predicate.to_be_bytes())
                    .enumerate()
                {
                    costs.planner_entries(1);
                    if visited == PLANNER_SAMPLE_ROWS {
                        truncated = true;
                        break;
                    }
                    let (key, _) = guard.into_inner().ok()?;
                    subjects.insert(decode_posg_key(key.as_ref())?.subject);
                }
                if truncated {
                    PlannerEstimate::LowerBound(subjects.len())
                } else {
                    PlannerEstimate::Exact(subjects.len())
                }
            }
            (Some(predicate), DistinctDomain::Object) => {
                let mut objects = HashSet::new();
                let mut truncated = false;
                for (visited, guard) in snapshot
                    .prefix(spaces.posg, predicate.to_be_bytes())
                    .enumerate()
                {
                    costs.planner_entries(1);
                    if visited == PLANNER_SAMPLE_ROWS {
                        truncated = true;
                        break;
                    }
                    let (key, _) = guard.into_inner().ok()?;
                    objects.insert(decode_posg_key(key.as_ref())?.object);
                }
                if truncated {
                    PlannerEstimate::LowerBound(objects.len())
                } else {
                    PlannerEstimate::Exact(objects.len())
                }
            }
            (None, DistinctDomain::Subject) => {
                let mut subjects = HashSet::new();
                let mut truncated = false;
                for (visited, guard) in snapshot.iter(spaces.spog).enumerate() {
                    costs.planner_entries(1);
                    if visited == PLANNER_SAMPLE_ROWS {
                        truncated = true;
                        break;
                    }
                    let (key, _) = guard.into_inner().ok()?;
                    subjects.insert(decode_spog_key(key.as_ref())?.subject);
                }
                if truncated {
                    PlannerEstimate::LowerBound(subjects.len())
                } else {
                    PlannerEstimate::Exact(subjects.len())
                }
            }
            (None, DistinctDomain::Object) => {
                let mut objects = HashSet::new();
                let mut truncated = false;
                for (visited, guard) in snapshot.iter(spaces.ospg).enumerate() {
                    costs.planner_entries(1);
                    if visited == PLANNER_SAMPLE_ROWS {
                        truncated = true;
                        break;
                    }
                    let (key, _) = guard.into_inner().ok()?;
                    objects.insert(decode_ospg_key(key.as_ref())?.object);
                }
                if truncated {
                    PlannerEstimate::LowerBound(objects.len())
                } else {
                    PlannerEstimate::Exact(objects.len())
                }
            }
        };
        self.indexes_write().planner_distinct.insert(
            cache_key,
            estimate,
            std::mem::size_of_val(&estimate),
        );
        Some(estimate)
    }

    pub(crate) fn planner_estimate(
        &self,
        stat: PlannerStat,
        costs: &crate::query::context::QueryCost,
    ) -> PlannerEstimate {
        let pattern = |subject, predicate, object| crate::rdf_read::QuadPattern {
            subject,
            predicate,
            object,
            ..crate::rdf_read::QuadPattern::default()
        };
        let counter = |stat: QvStat, pattern: crate::rdf_read::QuadPattern| {
            self.read_snapshot()
                .qv_stat(self, &QvRead { stat, costs })
                .ok()
                .flatten()
                .and_then(|count| usize::try_from(count).ok())
                .map(PlannerEstimate::Exact)
                .unwrap_or_else(|| self.source_pattern_count(pattern, costs))
        };
        match stat {
            PlannerStat::Graph(graph) => counter(
                QvStat::Graph(graph),
                crate::rdf_read::QuadPattern {
                    graph: Some(graph),
                    ..pattern(None, None, None)
                },
            ),
            PlannerStat::GraphPredicate(graph, predicate) => counter(
                QvStat::GraphPredicate(graph, predicate),
                crate::rdf_read::QuadPattern {
                    graph: Some(graph),
                    ..pattern(None, Some(predicate), None)
                },
            ),
            PlannerStat::GraphPredicateObject(graph, predicate, object) => counter(
                QvStat::GraphPredicateObject(graph, predicate, object),
                crate::rdf_read::QuadPattern {
                    graph: Some(graph),
                    ..pattern(None, Some(predicate), Some(object))
                },
            ),
            PlannerStat::PredicateObject(predicate, object) => {
                let snapshot = self.read_snapshot();
                snapshot
                    .qv_stat(
                        self,
                        &QvRead {
                            stat: QvStat::PredicateObject(predicate, object),
                            costs,
                        },
                    )
                    .ok()
                    .flatten()
                    .and_then(|count| usize::try_from(count).ok())
                    .map(PlannerEstimate::Exact)
                    .unwrap_or_else(|| {
                        self.source_pattern_count(
                            pattern(None, Some(predicate), Some(object)),
                            costs,
                        )
                    })
            }
            PlannerStat::Predicate(predicate) => {
                let snapshot = self.read_snapshot();
                snapshot
                    .qv_stat(
                        self,
                        &QvRead {
                            stat: QvStat::Predicate(predicate),
                            costs,
                        },
                    )
                    .ok()
                    .flatten()
                    .and_then(|count| usize::try_from(count).ok())
                    .map(PlannerEstimate::Exact)
                    .unwrap_or_else(|| {
                        self.source_pattern_count(pattern(None, Some(predicate), None), costs)
                    })
            }
            PlannerStat::PredicateSubjects(predicate) => self
                .index_distinct_count(
                    DistinctStat {
                        predicate: Some(predicate),
                        domain: DistinctDomain::Subject,
                    },
                    costs,
                )
                .unwrap_or(PlannerEstimate::Unknown),
            PlannerStat::PredicateObjects(predicate) => self
                .index_distinct_count(
                    DistinctStat {
                        predicate: Some(predicate),
                        domain: DistinctDomain::Object,
                    },
                    costs,
                )
                .unwrap_or(PlannerEstimate::Unknown),
            PlannerStat::Object(object) => self
                .index_pattern_count(&crate::query::cursor::IndexScan {
                    order: IndexCursorOrder::Ospg,
                    pattern: pattern(None, None, Some(object)),
                    query_id_limit: None,
                    costs,
                })
                .unwrap_or_else(|| {
                    self.source_pattern_count(pattern(None, None, Some(object)), costs)
                }),
            PlannerStat::Subject(subject) => self
                .index_pattern_count(&crate::query::cursor::IndexScan {
                    order: IndexCursorOrder::Spog,
                    pattern: pattern(Some(subject), None, None),
                    query_id_limit: None,
                    costs,
                })
                .unwrap_or_else(|| {
                    self.source_pattern_count(pattern(Some(subject), None, None), costs)
                }),
            PlannerStat::DistinctSubjects => self
                .index_distinct_count(
                    DistinctStat {
                        predicate: None,
                        domain: DistinctDomain::Subject,
                    },
                    costs,
                )
                .unwrap_or(PlannerEstimate::Unknown),
            PlannerStat::DistinctObjects => self
                .index_distinct_count(
                    DistinctStat {
                        predicate: None,
                        domain: DistinctDomain::Object,
                    },
                    costs,
                )
                .unwrap_or(PlannerEstimate::Unknown),
            PlannerStat::Total => {
                let snapshot = self.read_snapshot();
                snapshot
                    .qv_stat(
                        self,
                        &QvRead {
                            stat: QvStat::Total,
                            costs,
                        },
                    )
                    .ok()
                    .flatten()
                    .and_then(|count| usize::try_from(count).ok())
                    .map(PlannerEstimate::Exact)
                    .unwrap_or_else(|| self.source_pattern_count(pattern(None, None, None), costs))
            }
        }
    }

    pub(crate) fn planner_stat(
        &self,
        stat: PlannerStat,
        costs: &crate::query::context::QueryCost,
    ) -> usize {
        self.planner_estimate(stat, costs).row_upper()
    }

    #[cfg(test)]
    pub(crate) fn predicate_subject_count(&self, predicate: TermId) -> usize {
        self.planner_stat(
            PlannerStat::PredicateSubjects(predicate),
            &crate::query::context::QueryCost::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn predicate_object_count(&self, predicate: TermId) -> usize {
        self.planner_stat(
            PlannerStat::PredicateObjects(predicate),
            &crate::query::context::QueryCost::default(),
        )
    }

    pub(crate) fn decode_quad_key(bytes: &[u8]) -> Result<EncodedQuad> {
        if bytes.len() != 64 {
            return Err(StoreError::InvalidEncoding {
                context: "quad key",
                message: format!("expected 64 bytes, found {}", bytes.len()),
            });
        }
        Ok(EncodedQuad {
            graph: decode_term_id(&bytes[0..16], "quad graph")?,
            subject: decode_term_id(&bytes[16..32], "quad subject")?,
            predicate: decode_term_id(&bytes[32..48], "quad predicate")?,
            object: decode_term_id(&bytes[48..64], "quad object")?,
        })
    }

    pub(crate) fn decode_query_key(order: IndexCursorOrder, bytes: &[u8]) -> Result<QueryQuad> {
        let quad = match order {
            IndexCursorOrder::Gspo => decode_gspo_key(bytes),
            IndexCursorOrder::Gpos => decode_gpos_key(bytes),
            IndexCursorOrder::Spog => decode_spog_key(bytes),
            IndexCursorOrder::Posg => decode_posg_key(bytes),
            IndexCursorOrder::Ospg => decode_ospg_key(bytes),
            IndexCursorOrder::Gosp => decode_gosp_key(bytes),
        };
        quad.ok_or_else(|| StoreError::InvalidIndexEncoding {
            context: "qv2 query index key",
            message: format!("expected 32 bytes, found {}", bytes.len()),
        })
    }

    pub(crate) fn decode_query_term(
        snapshot: &Snapshot,
        query_to_term: &Keyspace,
        term: QueryTermId,
    ) -> Result<(TermId, u64)> {
        let value = snapshot.get(query_to_term, term.to_be_bytes())?.ok_or(
            StoreError::IndexVerificationFailed("query-to-term-mapping-missing"),
        )?;
        Ok((
            decode_source_id(value.as_ref(), "query-to-term mapping")?,
            u64::try_from(8usize.saturating_add(value.len())).unwrap_or(u64::MAX),
        ))
    }

    pub(crate) fn quad_key(
        graph: TermId,
        subject: TermId,
        predicate: TermId,
        object: TermId,
    ) -> [u8; 64] {
        let mut key = [0u8; 64];
        key[0..16].copy_from_slice(&graph.to_be_bytes());
        key[16..32].copy_from_slice(&subject.to_be_bytes());
        key[32..48].copy_from_slice(&predicate.to_be_bytes());
        key[48..64].copy_from_slice(&object.to_be_bytes());
        key
    }

    pub(crate) fn quad_is_live(bytes: &[u8]) -> bool {
        !dots_empty(bytes)
    }

    fn count_object_ids(&self, graph: TermId, subject: TermId, predicate: TermId) -> Result<usize> {
        Ok(self
            .subject_entries((graph, subject), None)?
            .into_iter()
            .filter(|(candidate_predicate, _)| *candidate_predicate == predicate)
            .count())
    }

    /// Returns decoded-term order, installing only under the captured graph epoch.
    fn ordered_objects(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
    ) -> Result<Arc<Vec<TermId>>> {
        let key = (graph, subject, predicate);
        let generation = {
            let mut indexes = self.indexes_write();
            let generation = indexes.graph_epoch(graph);
            if let Some(cached) = indexes.object_order.get(&key, generation) {
                return Ok(cached);
            }
            generation
        };
        let object_ids = self
            .subject_entries((graph, subject), None)?
            .into_iter()
            .filter_map(|(candidate_predicate, object)| {
                (candidate_predicate == predicate).then_some(object)
            })
            .collect::<Vec<_>>();

        let mut ordered = object_ids
            .into_iter()
            .map(|object| Ok((self.decode_term(object)?.0, object)))
            .collect::<Result<Vec<_>>>()?;
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        let objects = Arc::new(
            ordered
                .into_iter()
                .map(|(_, object)| object)
                .collect::<Vec<_>>(),
        );
        let mut indexes = self.indexes_write();
        if indexes.graph_epoch(graph) == generation {
            indexes.object_order.install(
                OrderEntry {
                    key,
                    objects: Arc::clone(&objects),
                },
                generation,
            );
        }
        Ok(objects)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_budget(path, PersistMode::Buffer, MemoryBudget::default())
    }

    pub fn open_with_mode(path: impl AsRef<Path>, persist_mode: PersistMode) -> Result<Self> {
        Self::open_with_budget(path, persist_mode, MemoryBudget::default())
    }

    pub(crate) fn open_with_budget(
        path: impl AsRef<Path>,
        persist_mode: PersistMode,
        memory: MemoryBudget,
    ) -> Result<Self> {
        let memory_lease = memory.reserve()?;
        let memory = memory_lease.budget();
        let worker_threads = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .min(32);
        // Per-keyspace memtables and the journal ceiling jointly bound replay.
        let budget = CacheBudget::from_budget(memory);
        let db = Database::builder(path.as_ref())
            .manual_journal_persist(true)
            .cache_size(budget.database)
            .journal_compression(CompressionType::None)
            .max_journaling_size(MAX_JOURNALING_BYTES)
            .worker_threads(worker_threads)
            .open()?;
        Self::with_memory(
            db,
            persist_mode,
            OpenMemory {
                budget: memory,
                lease: memory_lease,
            },
        )
    }

    /// Reports the budget this store actually reserved.
    pub(crate) fn memory_budget(&self) -> MemoryBudget {
        self._memory_lease.budget()
    }

    pub fn from_database(db: Database) -> Result<Self> {
        Self::with_persist_mode(db, PersistMode::Buffer)
    }

    /// Build a store on an already-open database with an explicit durability
    /// mode; [`GraphStore::open_with_mode`] opens the database first.
    pub fn with_persist_mode(db: Database, persist_mode: PersistMode) -> Result<Self> {
        let lease = MemoryBudget::default().reserve()?;
        let budget = lease.budget();
        Self::with_memory(db, persist_mode, OpenMemory { budget, lease })
    }

    fn with_memory(db: Database, persist_mode: PersistMode, memory: OpenMemory) -> Result<Self> {
        let budget = CacheBudget::from_budget(memory.budget);
        let point_read_heavy = || {
            KeyspaceCreateOptions::default()
                .expect_point_read_hits(true)
                .max_memtable_size(READ_MEMTABLE_BYTES)
        };
        let write_heavy = || {
            KeyspaceCreateOptions::default()
                .data_block_compression_policy(CompressionPolicy::disabled())
                .index_block_compression_policy(CompressionPolicy::disabled())
                .compaction_strategy(Arc::new(
                    Leveled::default()
                        .with_l0_threshold(WRITE_LEVEL_LIMIT)
                        .with_table_target_size(WRITE_TABLE_BYTES)
                        .with_level_ratio_policy(vec![WRITE_LEVEL_RATIO]),
                ))
                .max_memtable_size(WRITE_MEMTABLE_BYTES)
        };

        let store = Self {
            _memory_lease: memory.lease,
            #[cfg(feature = "shacl-core")]
            shacl_cache_bytes: budget.shacl,
            terms: db.keyspace("terms", point_read_heavy)?,
            quads: db.keyspace("quads", write_heavy)?,
            graphs: db.keyspace("graphs", point_read_heavy)?,
            log: db.keyspace("log", write_heavy)?,
            qv2_gspo: db.keyspace("qv2_gspo", write_heavy)?,
            qv2_gpos: db.keyspace("qv2_gpos", write_heavy)?,
            qv2_spog: db.keyspace("qv2_spog", write_heavy)?,
            qv2_posg: db.keyspace("qv2_posg", write_heavy)?,
            qv2_ospg: db.keyspace("qv2_ospg", write_heavy)?,
            qv2_gosp: db.keyspace("qv2_gosp", write_heavy)?,
            primary_term_map: db.keyspace(PRIMARY_TERM_MAP, point_read_heavy)?,
            primary_query_map: db.keyspace(PRIMARY_QUERY_MAP, point_read_heavy)?,
            qv2_meta: db.keyspace("qv2_meta", point_read_heavy)?,
            qv3_gspo: db.keyspace("qv3_gspo", write_heavy)?,
            qv3_gpos: db.keyspace("qv3_gpos", write_heavy)?,
            qv3_spog: db.keyspace("qv3_spog", write_heavy)?,
            qv3_posg: db.keyspace("qv3_posg", write_heavy)?,
            qv3_ospg: db.keyspace("qv3_ospg", write_heavy)?,
            qv3_gosp: db.keyspace("qv3_gosp", write_heavy)?,
            secondary_term_map: db.keyspace(SECONDARY_TERM_MAP, point_read_heavy)?,
            secondary_query_map: db.keyspace(SECONDARY_QUERY_MAP, point_read_heavy)?,
            qv3_meta: db.keyspace("qv3_meta", point_read_heavy)?,
            search_queue: db.keyspace("search_queue_v1", write_heavy)?,
            search_meta: db.keyspace("search_meta_v1", point_read_heavy)?,
            receipts: db.keyspace("mutation_receipts_v1", point_read_heavy)?,
            receipt_order: db.keyspace("mutation_receipt_order_v1", point_read_heavy)?,
            repair_audits: db.keyspace("repair_audits_v1", point_read_heavy)?,
            repair_backups: db.keyspace("repair_backups_v1", write_heavy)?,
            db,
            persist_mode,
            term_locks: (0..TERM_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            commit_locks: (0..COMMIT_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            write_locks: (0..GRAPH_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            receipt_locks: (0..COMMIT_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            receipt_next: AtomicU64::new(1),
            receipt_count: TrimCount::default(),
            batch_receipt_count: TrimCount::default(),
            #[cfg(test)]
            receipt_lookups: AtomicU64::new(0),
            #[cfg(test)]
            receipt_writes: AtomicU64::new(0),
            #[cfg(test)]
            receipt_persists: AtomicU64::new(0),
            projection_lock: RwLock::new(()),
            qv_maintenance: Mutex::new(()),
            qv_gate: QvCommitGate::new(),
            qv_commit_wait: QV_COMMIT_WAIT,
            qv_debt_next: AtomicU64::new(1),
            #[cfg(feature = "shacl-core")]
            binding_lock: Mutex::new(()),
            #[cfg(feature = "shacl-core")]
            binding_wait_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            binding_hold_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            graph_lock_wait: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            validation_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            settlement_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            settlement_failures: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            status_bindings_read: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            status_version_checks: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            status_shape_compilations: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            status_shape_scans: AtomicU64::new(0),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_stall: Mutex::new(Duration::ZERO),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_active: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_max_active: std::sync::atomic::AtomicUsize::new(0),
            indexes: RwLock::new(IndexState::with_budget(&budget)),
            term_decode_cache: RwLock::new(BoundedCache::new(TERM_CACHE_CAP, budget.terms)),
            source_cache: Arc::new(RwLock::new(BoundedCache::new(
                SOURCE_CACHE_CAP,
                budget.terms / 8,
            ))),
            generation_floor: AtomicU64::new(0),
            term_cache_hits: AtomicU64::new(0),
            term_cache_misses: AtomicU64::new(0),
            #[cfg(test)]
            commit_stall: Mutex::new(None),
            #[cfg(test)]
            commit_stalled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            commit_stall_active: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            peak_commit_stalls: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            commit_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            policy_failure: Mutex::new(None),
            #[cfg(test)]
            fts_ack_stall: Mutex::new(None),
            #[cfg(test)]
            rebuild_stall: Mutex::new(None),
            #[cfg(test)]
            rebuild_stalled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            rebuild_hook: RwLock::new(None),
            #[cfg(test)]
            delta_row_limit: AtomicU64::new(QV_DELTA_ROWS),
            #[cfg(test)]
            delta_byte_limit: AtomicU64::new(QV_DELTA_BYTES),
            #[cfg(test)]
            delete_stall: Mutex::new(None),
            #[cfg(test)]
            delete_stalled: std::sync::atomic::AtomicBool::new(false),
            fts_queue_lock: Mutex::new(()),
            dirty_counter: AtomicU64::new(1),
            queue_floor: AtomicU64::new(0),
            dirty_committed: AtomicU64::new(0),
            diagnostics_computed: AtomicU64::new(0),
            #[cfg(test)]
            index_admission_probes: AtomicU64::new(0),
            #[cfg(test)]
            index_verification_runs: AtomicU64::new(0),
            #[cfg(test)]
            persists: AtomicU64::new(0),
        };

        store.ensure_disk_format()?;
        store.recover_query_build(None)?;
        store.initialize_indexes()?;
        store.migrate_search_meta()?;
        store.restore_receipt_next()?;
        store.restore_dirty_counter()?;
        store.rebuild_search_order()?;
        store.repair_literal_aliases()?;
        store.repair_diagnostics()?;
        Ok(store)
    }

    fn ensure_disk_format(&self) -> Result<()> {
        match self.graphs.get(DISK_FORMAT_KEY)? {
            Some(bytes) => {
                let found = decode_disk_format(bytes.as_ref())?;
                if found.major != DISK_FORMAT_VERSION.major
                    || found.minor > DISK_FORMAT_VERSION.minor
                {
                    return Err(StoreError::UnsupportedAuthoritativeFormat {
                        found_major: found.major,
                        found_minor: found.minor,
                        supported_major: DISK_FORMAT_VERSION.major,
                        supported_minor: DISK_FORMAT_VERSION.minor,
                    });
                }
                Ok(())
            }
            None if self.authority_spaces_empty()? => {
                let mut batch = self.buffered_batch();
                batch.insert(
                    &self.graphs,
                    DISK_FORMAT_KEY,
                    encode_disk_format(DISK_FORMAT_VERSION),
                );
                batch.commit()?;
                self.db.persist(self.persist_mode)?;
                Ok(())
            }
            None => Err(StoreError::MissingAuthoritativeFormat),
        }
    }

    fn authority_spaces_empty(&self) -> Result<bool> {
        for keyspace in [&self.terms, &self.quads, &self.log] {
            if keyspace.iter().next().is_some() {
                return Ok(false);
            }
        }
        for guard in self.graphs.iter() {
            let (key, _) = guard.into_inner()?;
            if key.as_ref() != DISK_FORMAT_KEY && key.as_ref() != LITERAL_ALIAS_KEY {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Restores the monotonic committed FTS token head across restarts.
    fn restore_dirty_counter(&self) -> Result<()> {
        let stored_head = self
            .search_meta
            .get(SEARCH_HEAD_KEY)?
            .map(|value| decode_u64(value.as_ref(), "search token head"))
            .transpose()?;
        if let Some(highest) = stored_head {
            self.dirty_counter
                .store(highest.saturating_add(1), Ordering::SeqCst);
            self.dirty_committed.store(highest, Ordering::SeqCst);
            return Ok(());
        }
        let mut highest = stored_head.unwrap_or(0);
        for prefix in [
            graph_dirty_prefix(),
            graph_reindex_prefix(),
            graph_delete_prefix(),
        ] {
            for guard in self.graphs.prefix(prefix) {
                let (_, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "fts queue tokens")?;
                highest = highest.max(tokens.latest);
            }
        }
        if let Some(coverage) = self.search_coverage()? {
            highest = highest.max(coverage.covered);
            if let Some(rebuild) = coverage.rebuild {
                highest = highest.max(rebuild);
            }
        }
        for guard in self.receipts.iter() {
            let (key, value) = guard.into_inner()?;
            if key.len() != 32 {
                continue;
            }
            let receipt: MutationReceipt = postcard::from_bytes(value.as_ref())?;
            if let Some(token) = receipt.search_token {
                highest = highest.max(token);
            }
        }
        if stored_head != Some(highest) {
            let mut batch = self.buffered_batch();
            batch.insert(&self.search_meta, SEARCH_HEAD_KEY, highest.to_be_bytes());
            self.commit_fjall_batch(batch)?;
        }
        self.dirty_counter
            .store(highest.saturating_add(1), Ordering::SeqCst);
        self.dirty_committed.store(highest, Ordering::SeqCst);
        Ok(())
    }

    fn migrate_search_meta(&self) -> Result<()> {
        if let Some(value) = self.search_meta.get(SEARCH_SCHEMA_KEY)? {
            let found = u16::from_be_bytes(
                value
                    .as_ref()
                    .try_into()
                    .map_err(|_| StoreError::InvalidSearchState("search-schema-marker-invalid"))?,
            );
            if found > SEARCH_META_FORMAT {
                return Err(StoreError::UnsupportedSearchFormat {
                    found,
                    supported: SEARCH_META_FORMAT,
                });
            }
            if found == SEARCH_META_FORMAT {
                return Ok(());
            }
        }
        loop {
            let mut batch = self.buffered_batch();
            let mut removed = 0usize;
            for guard in self.search_meta.prefix([SEARCH_FAILURE_PREFIX]) {
                let (key, _) = guard.into_inner()?;
                batch.remove(&self.search_meta, key);
                removed += 1;
                if removed == QV_BUILD_ROWS {
                    break;
                }
            }
            if removed == 0 {
                batch.insert(
                    &self.search_meta,
                    SEARCH_SCHEMA_KEY,
                    SEARCH_META_FORMAT.to_be_bytes(),
                );
                self.commit_fjall_batch(batch)?;
                return Ok(());
            }
            self.commit_fjall_batch(batch)?;
        }
    }

    /// Restores the admission counter and orders receipts that older versions left untracked.
    fn restore_receipt_next(&self) -> Result<()> {
        let mut highest = self
            .receipt_order
            .get(RECEIPT_EXPIRED_KEY)?
            .and_then(|value| decode_index_count(value.as_ref()))
            .unwrap_or(0);
        let mut batch = self.buffered_batch();
        let mut staged = 0usize;
        let mut live = 0usize;
        for guard in self.receipts.iter() {
            let (key, value) = guard.into_inner()?;
            if key.len() != 32 {
                continue;
            }
            live += 1;
            let receipt: MutationReceipt = postcard::from_bytes(value.as_ref())?;
            highest = highest.max(receipt.admission_sequence);
            let order = receipt_order_key(&receipt);
            if !self.receipt_order.contains_key(order)? {
                batch.insert(&self.receipt_order, order, receipt.id.0);
                staged += 1;
            }
            if staged == QV_BUILD_ROWS {
                self.commit_fjall_batch(std::mem::replace(&mut batch, self.buffered_batch()))?;
                staged = 0;
            }
        }
        if staged > 0 {
            self.commit_fjall_batch(batch)?;
        }
        self.receipt_next
            .store(highest.saturating_add(1).max(1), Ordering::SeqCst);
        self.receipt_count.live.store(live, Ordering::SeqCst);
        let mut batches = 0usize;
        for guard in self.receipt_order.prefix(BATCH_ORDER_PREFIX) {
            guard.into_inner()?;
            batches += 1;
        }
        self.batch_receipt_count
            .live
            .store(batches, Ordering::SeqCst);
        Ok(())
    }

    /// Rebuild the disposable token ordering from durable queue identities.
    fn rebuild_search_order(&self) -> Result<()> {
        let mut migration = match self.search_meta.get(SEARCH_ORDER_KEY)? {
            Some(value) => postcard::from_bytes::<SearchOrderMigration>(value.as_ref())?,
            None => SearchOrderMigration {
                format: SEARCH_ORDER_FORMAT,
                stage: SearchOrderStage::Clear,
                after: Vec::new(),
            },
        };
        if migration.format > SEARCH_ORDER_FORMAT {
            return Err(StoreError::UnsupportedSearchFormat {
                found: migration.format,
                supported: SEARCH_ORDER_FORMAT,
            });
        }
        while !matches!(migration.stage, SearchOrderStage::Done) {
            let mut batch = self.buffered_batch();
            let mut visited = 0usize;
            let mut last = None;
            match migration.stage {
                SearchOrderStage::Clear => {
                    let after = (!migration.after.is_empty()).then_some(&migration.after[..]);
                    for guard in self.search_queue.range(resume_range(&[], after)) {
                        let (key, _) = guard.into_inner()?;
                        last = Some(key.to_vec());
                        batch.remove(&self.search_queue, key);
                        visited += 1;
                        if visited == QV_BUILD_ROWS {
                            break;
                        }
                    }
                }
                SearchOrderStage::Delete
                | SearchOrderStage::Reindex
                | SearchOrderStage::Subject => {
                    let (kind, prefix) = match migration.stage {
                        SearchOrderStage::Delete => (QueueKind::Delete, graph_delete_prefix()),
                        SearchOrderStage::Reindex => (QueueKind::Reindex, graph_reindex_prefix()),
                        SearchOrderStage::Subject => (QueueKind::Subject, graph_dirty_prefix()),
                        SearchOrderStage::Clear | SearchOrderStage::Done => unreachable!(),
                    };
                    let after = (!migration.after.is_empty()).then_some(&migration.after[..]);
                    for guard in self.graphs.range(resume_range(&prefix, after)) {
                        let (key, value) = guard.into_inner()?;
                        let cursor = self.queue_cursor(kind, key.as_ref(), value.as_ref())?;
                        batch.insert(&self.search_queue, search_order_key(cursor), key.as_ref());
                        last = Some(key.to_vec());
                        visited += 1;
                        if visited == QV_BUILD_ROWS {
                            break;
                        }
                    }
                }
                SearchOrderStage::Done => unreachable!(),
            }
            if let Some(last) = last {
                migration.after = last;
            }
            if visited < QV_BUILD_ROWS {
                migration.stage = match migration.stage {
                    SearchOrderStage::Clear => SearchOrderStage::Delete,
                    SearchOrderStage::Delete => SearchOrderStage::Reindex,
                    SearchOrderStage::Reindex => SearchOrderStage::Subject,
                    SearchOrderStage::Subject => SearchOrderStage::Done,
                    SearchOrderStage::Done => unreachable!(),
                };
                migration.after.clear();
            }
            batch.insert(
                &self.search_meta,
                SEARCH_ORDER_KEY,
                postcard::to_allocvec(&migration)?,
            );
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    fn queue_cursor(&self, kind: QueueKind, key: &[u8], value: &[u8]) -> Result<QueueCursor> {
        let tokens = decode_dirty_tokens(value, "fts queue tokens")?;
        let expected = if matches!(kind, QueueKind::Subject) {
            33
        } else {
            17
        };
        if key.len() != expected {
            return Err(StoreError::InvalidSearchState("queue-identity-key-invalid"));
        }
        Ok(QueueCursor {
            token: tokens.oldest,
            kind,
            graph: decode_term_id(&key[1..17], "fts queue graph")?,
            subject: matches!(kind, QueueKind::Subject)
                .then(|| decode_term_id(&key[17..33], "fts queue subject"))
                .transpose()?,
        })
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn persist_mode(&self) -> PersistMode {
        self.persist_mode
    }

    pub(crate) fn persistence_outcome(&self) -> crate::sync::PersistenceOutcome {
        match self.persist_mode {
            PersistMode::Buffer => crate::sync::PersistenceOutcome::Buffered,
            PersistMode::SyncData => crate::sync::PersistenceOutcome::DataSynced,
            PersistMode::SyncAll => crate::sync::PersistenceOutcome::FullySynced,
        }
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn shacl_cache_bytes(&self) -> usize {
        self.shacl_cache_bytes
    }

    #[cfg(test)]
    pub(crate) fn receipt_work(&self) -> ReceiptWork {
        ReceiptWork {
            lookups: self.receipt_lookups.load(Ordering::Relaxed),
            writes: self.receipt_writes.load(Ordering::Relaxed),
            persists: self.receipt_persists.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn persist_receipts(&self) -> Result<()> {
        self.db.persist(self.persist_mode)?;
        #[cfg(test)]
        self.receipt_persists.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flushes every keyspace before compaction, including stores with old ceilings.
    pub fn manual_compact(&self) -> Result<()> {
        self.db.persist(self.persist_mode)?;
        for keyspace in [
            &self.terms,
            &self.quads,
            &self.graphs,
            &self.log,
            &self.qv2_gspo,
            &self.qv2_gpos,
            &self.qv2_spog,
            &self.qv2_posg,
            &self.qv2_ospg,
            &self.qv2_gosp,
            &self.primary_term_map,
            &self.primary_query_map,
            &self.qv2_meta,
            &self.qv3_gspo,
            &self.qv3_gpos,
            &self.qv3_spog,
            &self.qv3_posg,
            &self.qv3_ospg,
            &self.qv3_gosp,
            &self.secondary_term_map,
            &self.secondary_query_map,
            &self.qv3_meta,
            &self.search_queue,
            &self.search_meta,
            &self.receipts,
            &self.receipt_order,
            &self.repair_audits,
            &self.repair_backups,
        ] {
            keyspace.rotate_memtable_and_wait()?;
            keyspace.major_compact()?;
        }
        self.db.persist(self.persist_mode)?;
        Ok(())
    }

    pub fn encode_term(&self, term: &EncodedTerm) -> Result<TermId> {
        self.encode_term_internal(None, term)
    }

    /// Intern `term` into `cx.batch`, memoized in `cx.cache`.
    pub fn resolve_term_cached(
        &self,
        cx: &mut BatchTermCtx<'_>,
        term: &EncodedTerm,
    ) -> Result<TermId> {
        if let Some(&id) = cx.cache.get(term.0.as_str()) {
            return Ok(id);
        }
        let id = self.encode_term_internal(Some(cx.batch), term)?;
        cx.cache.insert(term.0.clone(), id);
        Ok(id)
    }

    /// Intern every term that is not memoized yet, in one pass.
    pub fn seed_term_cache<'t>(
        &self,
        cx: &mut BatchTermCtx<'_>,
        terms: impl IntoIterator<Item = &'t EncodedTerm>,
    ) -> Result<()> {
        for term in terms {
            self.resolve_term_cached(cx, term)?;
        }
        Ok(())
    }

    fn read_term(&self, id: TermId) -> Result<EncodedTerm> {
        match self.terms.get(id.to_be_bytes())? {
            Some(bytes) => Ok(EncodedTerm(decode_term_text(bytes.as_ref())?)),
            None => Err(StoreError::TermNotFound(id.0)),
        }
    }

    /// Decodes an immutable term through the bounded global cache.
    pub(crate) fn decode_term_arc(&self, id: TermId) -> Result<Arc<EncodedTerm>> {
        if let Some(term) = self
            .term_decode_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .peek(&id)
            .cloned()
        {
            self.term_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(term);
        }
        self.term_cache_misses.fetch_add(1, Ordering::Relaxed);

        let term = Arc::new(self.read_term(id)?);
        let mut cache = self
            .term_decode_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        cache.insert(
            id,
            Arc::clone(&term),
            term.0.len().saturating_add(std::mem::size_of::<TermId>()),
        );
        Ok(term)
    }

    pub fn decode_term(&self, id: TermId) -> Result<EncodedTerm> {
        Ok(self.decode_term_arc(id)?.as_ref().clone())
    }

    pub fn lookup_term(&self, term: &EncodedTerm) -> Result<Option<TermId>> {
        let id = hash_term(term);
        let Some(existing) = self.terms.get(id.to_be_bytes())? else {
            return Ok(None);
        };
        // Compare the raw bytes; only decode when they differ, i.e. on the
        // (astronomically unlikely) hash collision path that needs the message.
        if existing.as_ref() == term.0.as_bytes() {
            return Ok(Some(id));
        }
        Err(StoreError::TermCollision {
            attempted: term.0.clone(),
            existing: decode_term_text(existing.as_ref())?,
        })
    }

    pub fn resolve_term(&self, term: &EncodedTerm) -> Result<TermId> {
        self.encode_term(term)
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn create_graph(&self, graph: &GraphId) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        if self.read_graph_meta(graph_id)?.is_none() {
            batch.insert(
                &self.graphs,
                graph_meta_key(graph_id),
                postcard::to_allocvec(&StoredGraphMeta::default())?,
            );
            self.commit(batch)?;
        }
        Ok(())
    }

    /// Stage graph creation into a caller-held graph commit batch.
    pub(crate) fn stage_graph(&self, batch: &mut WriteBatch, graph: &GraphId) -> Result<TermId> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        if self.read_graph_meta(graph_id)?.is_none() {
            batch.insert(
                &self.graphs,
                graph_meta_key(graph_id),
                postcard::to_allocvec(&StoredGraphMeta::default())?,
            );
        }
        Ok(graph_id)
    }

    pub fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(false);
        };
        self.contains_graph_id(graph_id)
    }

    /// O(1) existence probe that never decodes the metadata record.
    pub(crate) fn contains_graph_id(&self, graph_id: TermId) -> Result<bool> {
        Ok(self.graphs.contains_key(graph_meta_key(graph_id))?)
    }

    pub(crate) fn graph_version_digest(&self, graph: &GraphId) -> Result<[u8; 32]> {
        let graph = hash_term(&EncodedTerm::from_named_node(&graph.0));
        self.read_snapshot().graph_version(self, graph)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn shacl_binding_statuses(
        &self,
        data_graph: &GraphId,
    ) -> Result<Vec<crate::shacl::ShaclBindingStatus>> {
        let Some(data_graph_id) = self.graph_id_for(data_graph)? else {
            return Ok(Vec::new());
        };
        let mut statuses = Vec::new();
        for guard in self.graphs.prefix(shacl_binding_prefix(data_graph_id)) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 33 {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL binding key",
                    message: format!("expected 33 bytes, found {}", key.len()),
                });
            }
            statuses.push(postcard::from_bytes(value.as_ref())?);
        }
        statuses.sort_by(|left: &crate::shacl::ShaclBindingStatus, right| {
            left.binding
                .shapes_graph
                .as_str()
                .cmp(right.binding.shapes_graph.as_str())
        });
        Ok(statuses)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn shacl_repair_needed(&self) -> Result<bool> {
        Ok(self
            .graphs
            .get(SHACL_QUEUE_KEY)?
            .is_none_or(|value| value.as_ref() != [SHACL_QUEUE_VERSION]))
    }

    /// Rebuild the durable pending queue from all binding records.
    #[cfg(feature = "shacl-core")]
    pub(crate) fn repair_shacl_queue(&self) -> Result<QueueRepairStats> {
        let mut batch = self.new_batch();
        let mut pending_entries_scanned = 0u64;
        for guard in self.graphs.prefix(shacl_pending_prefix()) {
            let (key, _) = guard.into_inner()?;
            pending_entries_scanned += 1;
            batch.remove(&self.graphs, key);
        }

        let mut binding_records_scanned = 0u64;
        for guard in self.graphs.prefix([SHACL_BINDING_PREFIX]) {
            let (key, value) = guard.into_inner()?;
            binding_records_scanned += 1;
            if key.len() != 33 {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL binding key",
                    message: format!("expected 33 bytes, found {}", key.len()),
                });
            }
            let status: crate::shacl::ShaclBindingStatus = postcard::from_bytes(value.as_ref())?;
            if status.binding.policy == crate::shacl::ShaclWritePolicy::Disabled
                || status.state != crate::shacl::ShaclValidationState::Pending
            {
                continue;
            }
            let Some(data_graph) = self.graph_id_for(&status.binding.data_graph)? else {
                return Err(StoreError::GraphNotFound(
                    status.binding.data_graph.to_string(),
                ));
            };
            batch.insert(&self.graphs, shacl_pending_key(data_graph), []);
        }
        batch.insert(&self.graphs, SHACL_QUEUE_KEY, [SHACL_QUEUE_VERSION]);
        self.commit(batch)?;
        Ok(QueueRepairStats {
            binding_records_scanned,
            pending_entries_scanned,
        })
    }

    /// Scan only the durable pending queue, optionally stopping at a replay budget.
    #[cfg(feature = "shacl-core")]
    pub(crate) fn bounded_shacl_queue(
        &self,
        max_graphs: usize,
        deadline: Option<Instant>,
    ) -> Result<PendingQueueScan> {
        let mut graphs = Vec::new();
        let mut terms = HashMap::new();
        let mut entries_scanned = 0u64;
        for guard in self.graphs.prefix(shacl_pending_prefix()) {
            if graphs.len() >= max_graphs || deadline.is_some_and(|limit| Instant::now() >= limit) {
                return Ok(PendingQueueScan {
                    graphs,
                    entries_scanned,
                    budget_exhausted: true,
                });
            }
            let (key, value) = guard.into_inner()?;
            entries_scanned += 1;
            if key.len() != 17 || !value.is_empty() {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL pending graph key",
                    message: format!(
                        "expected 17-byte empty entry, found {} bytes and {} value bytes",
                        key.len(),
                        value.len()
                    ),
                });
            }
            let graph = decode_term_id(&key[1..17], "SHACL pending data graph")?;
            let graph = self
                .decode_term_cached(&mut terms, graph)?
                .to_named_node()
                .map(GraphId)
                .ok_or_else(|| StoreError::InvalidEncoding {
                    context: "SHACL pending data graph",
                    message: "data graph is not a named node".to_owned(),
                })?;
            graphs.push(graph);
        }
        graphs.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(PendingQueueScan {
            graphs,
            entries_scanned,
            budget_exhausted: false,
        })
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn pending_shacl_queue(&self) -> Result<Vec<GraphId>> {
        Ok(self.bounded_shacl_queue(usize::MAX, None)?.graphs)
    }

    #[cfg(all(test, feature = "shacl-core"))]
    pub(crate) fn pending_shacl_graphs(&self) -> Result<Vec<GraphId>> {
        self.pending_shacl_queue()
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn pending_shacl_count(&self) -> Result<u64> {
        let mut count = 0u64;
        for guard in self.graphs.prefix(shacl_pending_prefix()) {
            let _ = guard.into_inner()?;
            count += 1;
        }
        Ok(count)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn shacl_graph_pending(&self, graph: &GraphId) -> Result<bool> {
        let graph = hash_term(&EncodedTerm::from_named_node(&graph.0));
        Ok(self.graphs.contains_key(shacl_pending_key(graph))?)
    }

    /// Active data graphs whose bindings depend on `changed_graph`.
    #[cfg(feature = "shacl-core")]
    pub(crate) fn affected_shacl_graphs(&self, changed_graph: &GraphId) -> Result<Vec<GraphId>> {
        let Some(changed_graph) = self.graph_id_for(changed_graph)? else {
            return Ok(Vec::new());
        };
        let mut graphs = Vec::new();
        for key in self.binding_keys(changed_graph)? {
            let Some(value) = self.graphs.get(key)? else {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL reverse binding key",
                    message: "binding target is missing".to_owned(),
                });
            };
            let status: crate::shacl::ShaclBindingStatus = postcard::from_bytes(value.as_ref())?;
            if status.binding.policy != crate::shacl::ShaclWritePolicy::Disabled {
                graphs.push(status.binding.data_graph);
            }
        }
        graphs.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        graphs.dedup();
        Ok(graphs)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn stage_binding_status(
        &self,
        batch: &mut WriteBatch,
        status: &crate::shacl::ShaclBindingStatus,
    ) -> Result<()> {
        let Some(data_graph) = self.graph_id_for(&status.binding.data_graph)? else {
            return Err(StoreError::GraphNotFound(
                status.binding.data_graph.to_string(),
            ));
        };
        let Some(shapes_graph) = self.graph_id_for(&status.binding.shapes_graph)? else {
            return Err(StoreError::GraphNotFound(
                status.binding.shapes_graph.to_string(),
            ));
        };
        let mut dependencies = Vec::with_capacity(status.shape_versions.len());
        for (dependency, _) in &status.shape_versions {
            let Some(dependency) = self.graph_id_for(dependency)? else {
                return Err(StoreError::GraphNotFound(dependency.to_string()));
            };
            dependencies.push(dependency);
        }
        let binding_key = shacl_binding_key(data_graph, shapes_graph);
        if let Some(value) = self.graphs.get(binding_key)? {
            let previous: crate::shacl::ShaclBindingStatus = postcard::from_bytes(value.as_ref())?;
            for (dependency, _) in previous.shape_versions {
                if let Some(dependency) = self.graph_id_for(&dependency)? {
                    batch.remove(
                        &self.graphs,
                        binding_reverse_key(dependency, data_graph, shapes_graph),
                    );
                }
            }
        }
        batch.insert(&self.graphs, binding_key, postcard::to_allocvec(status)?);
        for dependency in dependencies {
            batch.insert(
                &self.graphs,
                binding_reverse_key(dependency, data_graph, shapes_graph),
                [],
            );
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn stage_binding_remove(
        &self,
        batch: &mut WriteBatch,
        data_graph: &GraphId,
        shapes_graph: &GraphId,
    ) -> Result<()> {
        let Some(data_graph) = self.graph_id_for(data_graph)? else {
            return Ok(());
        };
        let Some(shapes_graph) = self.graph_id_for(shapes_graph)? else {
            return Ok(());
        };
        let binding_key = shacl_binding_key(data_graph, shapes_graph);
        if let Some(value) = self.graphs.get(binding_key)? {
            let status: crate::shacl::ShaclBindingStatus = postcard::from_bytes(value.as_ref())?;
            for (dependency, _) in status.shape_versions {
                if let Some(dependency) = self.graph_id_for(&dependency)? {
                    batch.remove(
                        &self.graphs,
                        binding_reverse_key(dependency, data_graph, shapes_graph),
                    );
                }
            }
        }
        batch.remove(&self.graphs, binding_key);
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn stage_binding_pending(
        &self,
        batch: &mut WriteBatch,
        status: &crate::shacl::ShaclBindingStatus,
    ) -> Result<()> {
        let Some(data_graph) = self.graph_id_for(&status.binding.data_graph)? else {
            return Err(StoreError::GraphNotFound(
                status.binding.data_graph.to_string(),
            ));
        };
        batch.insert(&self.graphs, shacl_pending_key(data_graph), []);
        self.stage_binding_status(batch, status)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn stage_shacl_settled(
        &self,
        batch: &mut WriteBatch,
        data_graph: &GraphId,
    ) -> Result<()> {
        let Some(data_graph) = self.graph_id_for(data_graph)? else {
            return Ok(());
        };
        batch.remove(&self.graphs, shacl_pending_key(data_graph));
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn stage_pending_bindings(
        &self,
        batch: &mut WriteBatch,
        changed_graph: &GraphId,
        data_version: [u8; 32],
    ) -> Result<()> {
        let Some(changed_graph_id) = self.graph_id_for(changed_graph)? else {
            return Ok(());
        };
        for key in self.binding_keys(changed_graph_id)? {
            let Some(value) = self.graphs.get(key)? else {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL reverse binding key",
                    message: "binding target is missing".to_owned(),
                });
            };
            let mut status: crate::shacl::ShaclBindingStatus =
                postcard::from_bytes(value.as_ref())?;
            if status.binding.data_graph == *changed_graph {
                status.data_version = data_version;
            }
            if status.binding.policy == crate::shacl::ShaclWritePolicy::Disabled {
                if status.binding.data_graph == *changed_graph {
                    self.stage_binding_status(batch, &status)?;
                }
                continue;
            }
            status.state = crate::shacl::ShaclValidationState::Pending;
            status.report = None;
            status.error = None;
            self.stage_binding_pending(batch, &status)?;
        }
        Ok(())
    }

    #[cfg(feature = "shacl-core")]
    fn binding_keys(&self, changed_graph: TermId) -> Result<BTreeSet<[u8; 33]>> {
        let mut binding_keys = BTreeSet::new();
        for guard in self.graphs.prefix(shacl_binding_prefix(changed_graph)) {
            let (key, _) = guard.into_inner()?;
            let key: [u8; 33] =
                key.as_ref()
                    .try_into()
                    .map_err(|_| StoreError::InvalidEncoding {
                        context: "SHACL binding key",
                        message: format!("expected 33 bytes, found {}", key.len()),
                    })?;
            binding_keys.insert(key);
        }
        for guard in self.graphs.prefix(binding_reverse_prefix(changed_graph)) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 49 || !value.is_empty() {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL reverse binding key",
                    message: format!("expected 49-byte empty entry, found {} bytes", key.len()),
                });
            }
            let data_graph = decode_term_id(&key[17..33], "SHACL reverse data graph")?;
            let shapes_graph = decode_term_id(&key[33..49], "SHACL reverse shapes graph")?;
            binding_keys.insert(shacl_binding_key(data_graph, shapes_graph));
        }
        Ok(binding_keys)
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn delete_graph(&self, graph: &GraphId) -> Result<()> {
        self.delete_graph_inner(DeleteGraph {
            graph,
            tombstone: None,
            receipt: None,
        })?;
        Ok(())
    }

    /// Delete a graph and persist its tombstone in the same durable batch.
    #[cfg(test)]
    pub(crate) fn delete_graph_tombstoned(&self, tombstone: &GraphTombstone) -> Result<()> {
        self.delete_graph_inner(DeleteGraph {
            graph: &tombstone.graph,
            tombstone: Some(tombstone),
            receipt: None,
        })?;
        Ok(())
    }

    pub(crate) fn delete_with_receipt(
        &self,
        tombstone: &GraphTombstone,
        receipt: &MutationReceipt,
    ) -> Result<MutationReceipt> {
        self.delete_graph_inner(DeleteGraph {
            graph: &tombstone.graph,
            tombstone: Some(tombstone),
            receipt: Some(receipt),
        })?
        .ok_or(StoreError::ReceiptConflict)
    }

    fn delete_graph_inner(&self, request: DeleteGraph<'_>) -> Result<Option<MutationReceipt>> {
        let DeleteGraph {
            graph,
            tombstone,
            receipt,
        } = request;
        let _commit_guard = self.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.binding_guard();
        let mut batch = self.new_batch();
        let graph_id = match self.graph_id_for(graph)? {
            Some(graph_id) => graph_id,
            None if tombstone.is_some() => self
                .encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?,
            None => return Ok(None),
        };
        let deleted_policy = self
            .read_graph_meta(graph_id)?
            .unwrap_or_default()
            .policy
            .normalized();
        if let Some(tombstone) = tombstone {
            let retained = match self.graphs.get(graph_tombstone_key(graph_id))? {
                Some(existing) => {
                    let existing: GraphTombstone = postcard::from_bytes(existing.as_ref())?;
                    if existing.delete_event >= tombstone.delete_event {
                        existing
                    } else {
                        tombstone.clone()
                    }
                }
                None => tombstone.clone(),
            };
            batch.insert(
                &self.graphs,
                graph_tombstone_key(graph_id),
                postcard::to_allocvec(&retained)?,
            );
            batch.insert(
                &self.graphs,
                deleted_policy_key(graph_id),
                postcard::to_allocvec(&deleted_policy)?,
            );
        }
        self.visit_graph_quads::<StoreError, _>(graph_id, |quad| {
            self.write_quad_state(&mut batch, quad, Vec::new())?;
            Ok(())
        })?;

        batch.remove(&self.graphs, graph_meta_key(graph_id));
        // The clock and the diagnostics record live under their own keys: a
        // recreated graph must start fresh, not inherit the deleted one's.
        batch.remove(&self.graphs, graph_clock_key(graph_id));
        batch.publish.clocks.insert(graph_id, None);
        batch.remove(&self.graphs, graph_diagnostics_key(graph_id));
        #[cfg(feature = "shacl-core")]
        batch.remove(&self.graphs, shacl_pending_key(graph_id));
        #[cfg(feature = "shacl-core")]
        for key in self.binding_keys(graph_id)? {
            let Some(value) = self.graphs.get(key)? else {
                return Err(StoreError::InvalidEncoding {
                    context: "SHACL reverse binding key",
                    message: "binding target is missing".to_owned(),
                });
            };
            let mut status: crate::shacl::ShaclBindingStatus =
                postcard::from_bytes(value.as_ref())?;
            if status.binding.data_graph == *graph {
                self.stage_binding_remove(
                    &mut batch,
                    &status.binding.data_graph,
                    &status.binding.shapes_graph,
                )?;
            } else if status.binding.policy != crate::shacl::ShaclWritePolicy::Disabled {
                status.state = crate::shacl::ShaclValidationState::Pending;
                status.report = None;
                status.error = None;
                self.stage_binding_pending(&mut batch, &status)?;
            }
        }

        batch.pending_fts.push(FtsQueueKey::Delete(graph_id));

        let _receipt_guard = receipt.map(|receipt| self.receipt_guard(&receipt.id));
        // A receipt already stored under a reused id is kept; the delete still applies.
        if let Some(receipt) = receipt
            && self.mutation_receipt(&receipt.id)?.is_none()
        {
            self.stage_receipt(&mut batch, receipt)?;
        }

        #[cfg(test)]
        self.stall_in_delete();

        for guard in self.log.prefix(log_head_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.log, key);
        }
        for guard in self.log.prefix(log_batch_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.log, key);
        }

        self.commit(batch)?;
        drop(_receipt_guard);
        let mut indexes = self.indexes_write();
        indexes
            .quad_subjects
            .remove_where(|(graph, _, _)| *graph == graph_id);
        indexes.object_order.drop_graph(graph_id);
        let epoch = &mut indexes.epochs[(graph_id.0 as usize) % INDEX_EPOCH_SHARDS];
        *epoch = epoch.wrapping_add(1);
        receipt
            .map(|receipt| {
                self.mutation_receipt(&receipt.id)?
                    .ok_or(StoreError::ReceiptConflict)
            })
            .transpose()
    }

    pub fn graph_is_empty(&self, graph: &GraphId) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(true);
        };
        Ok(self.graph_subject_count(graph_id)? == 0)
    }

    pub(crate) fn graph_subject_count(&self, graph_id: TermId) -> Result<usize> {
        let mut previous = None;
        let mut count = 0usize;
        for guard in self.quads.prefix(graph_id.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let subject = Self::decode_quad_key(key.as_ref())?.subject;
            if previous != Some(subject) {
                previous = Some(subject);
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub fn contains_subject(&self, graph: &GraphId, subject: &EncodedTerm) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(false);
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(false);
        };

        let mut prefix = [0u8; 32];
        prefix[..16].copy_from_slice(&graph_id.to_be_bytes());
        prefix[16..].copy_from_slice(&subject_id.to_be_bytes());
        for guard in self.quads.prefix(prefix) {
            let (_, value) = guard.into_inner()?;
            if !dots_empty(value.as_ref()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn graphs(&self) -> Result<Vec<GraphId>> {
        let mut graphs = Vec::new();
        for graph_id in self.graph_term_ids()? {
            let term = self.decode_term(graph_id)?;
            if let Some(named_node) = term.to_named_node() {
                graphs.push(GraphId(named_node));
            }
        }
        Ok(graphs)
    }

    /// Term ids of all graphs with stored metadata, without decoding the
    /// graph IRIs (the meta key embeds the term id).
    pub fn graph_term_ids(&self) -> Result<Vec<TermId>> {
        self.graph_term_iter().collect()
    }

    /// Streams graph term ids lazily so short-circuiting avoids a full scan.
    pub fn graph_term_iter(&self) -> impl Iterator<Item = Result<TermId>> {
        self.graphs
            .prefix(graph_meta_prefix())
            .filter_map(|guard| match guard.into_inner() {
                Ok((key, _)) => {
                    if key.len() != 17 {
                        return None;
                    }
                    Some(decode_term_id(&key[1..17], "graph meta key"))
                }
                Err(error) => Some(Err(error.into())),
            })
    }

    // Persisted, clock-tagged diagnostics.

    fn read_stored_diagnostics(&self, graph_id: TermId) -> Result<Option<StoredDiagnostics>> {
        self.graphs
            .get(graph_diagnostics_key(graph_id))?
            .map(|bytes| postcard::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(Into::into)
    }

    /// Returns the persisted orphan baseline for search diffs without freshness checks.
    pub(crate) fn last_persisted_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphDiagnostics::default());
        };
        Ok(self
            .read_stored_diagnostics(graph_id)?
            .map(|record| record.diagnostics)
            .unwrap_or_default())
    }

    fn store_diagnostics_record(
        &self,
        graph_id: TermId,
        record: StoredDiagnostics,
    ) -> Result<GraphDiagnostics> {
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.graphs,
            graph_diagnostics_key(graph_id),
            postcard::to_allocvec(&record)?,
        );
        batch.commit()?;
        Ok(record.diagnostics)
    }

    /// Computes diagnostics after reading the clock, making concurrent tags only stale.
    fn compute_tagged_diagnostics(&self, graph_id: TermId) -> Result<StoredDiagnostics> {
        let at_clock = self.vector_clock_id(graph_id)?;
        let graph = self.graph_name(graph_id)?;
        Ok(StoredDiagnostics {
            diagnostics: self.compute_graph_diagnostics(&graph)?,
            at_clock,
        })
    }

    fn graph_name(&self, graph_id: TermId) -> Result<GraphId> {
        let term = self.decode_term_arc(graph_id)?;
        term.to_named_node()
            .map(GraphId)
            .ok_or_else(|| StoreError::InvalidEncoding {
                context: "graph term",
                message: term.0.clone(),
            })
    }

    /// Persists clock-tagged diagnostics while the caller holds the graph guard.
    pub fn set_graph_diagnostics(
        &self,
        graph: &GraphId,
        diagnostics: &GraphDiagnostics,
    ) -> Result<()> {
        let graph_id = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let record = StoredDiagnostics {
            diagnostics: diagnostics.clone(),
            at_clock: self.vector_clock_id(graph_id)?,
        };
        self.store_diagnostics_record(graph_id, record)?;
        Ok(())
    }

    pub fn graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphDiagnostics::default());
        };
        self.graph_diagnostics_id(graph_id)
    }

    /// Reads clock-tagged diagnostics by term id without persisting recomputations.
    pub fn graph_diagnostics_id(&self, graph_id: TermId) -> Result<GraphDiagnostics> {
        let snapshot = self.read_snapshot();
        let clock = self.snapshot_vector_clock(&snapshot.snapshot, graph_id)?;

        if let Some(record) = self.snapshot_stored_diagnostics(&snapshot.snapshot, graph_id)?
            && record.at_clock == clock
        {
            return Ok(record.diagnostics);
        }

        if !snapshot.contains_graph_id(self, graph_id)? {
            return Ok(GraphDiagnostics::default());
        }

        self.snapshot_diagnostics(&snapshot, graph_id)
    }

    fn snapshot_diagnostics(
        &self,
        snapshot: &StoreReadSnapshot,
        graph_id: TermId,
    ) -> Result<GraphDiagnostics> {
        let context = crate::query::context::ReadContext::default();
        let orphans = snapshot.orphaned_entity_ids(self, &context, graph_id)?;
        let mut entities = Vec::with_capacity(orphans.len());
        for orphan in orphans {
            let term = self.decode_term_arc(orphan)?;
            entities.push(
                term.to_named_node()
                    .map(|named_node| named_node.as_str().to_string())
                    .unwrap_or_else(|| term.0.clone()),
            );
        }
        Ok(GraphDiagnostics::from_orphaned_entities(entities))
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn set_tagged_policy(&self, graph: &GraphId, tagged: &TaggedGraphPolicy) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.binding_guard();
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        meta.policy = tagged.policy.clone().normalized();
        meta.policy_tag = tagged.tag;
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        self.commit(batch)
    }

    pub(crate) fn set_policy_receipt(
        &self,
        graph: &GraphId,
        update: PolicyReceipt<'_>,
    ) -> Result<MutationReceipt> {
        let _commit_guard = self.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.binding_guard();
        let _receipt_guard = self.receipt_guard(&update.receipt.id);
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        meta.policy = update.tagged.policy.clone().normalized();
        meta.policy_tag = update.tagged.tag;
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        // A receipt already stored under a reused id is kept; the policy still applies.
        if self.mutation_receipt(&update.receipt.id)?.is_none() {
            self.stage_receipt(&mut batch, update.receipt)?;
        }
        self.commit(batch)?;
        drop(_receipt_guard);
        self.mutation_receipt(&update.receipt.id)?
            .ok_or(StoreError::ReceiptConflict)
    }

    #[cfg(test)]
    pub fn set_graph_policy(&self, graph: &GraphId, policy: &GraphPolicy) -> Result<()> {
        let current = self.graph_tagged_policy(graph)?;
        self.set_tagged_policy(
            graph,
            &TaggedGraphPolicy {
                policy: policy.clone(),
                tag: current.tag,
            },
        )
    }

    pub fn graph_policy(&self, graph: &GraphId) -> Result<GraphPolicy> {
        Ok(self.graph_tagged_policy(graph)?.policy)
    }

    pub(crate) fn deleted_graph_policy(&self, graph: &GraphId) -> Result<Option<GraphPolicy>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        self.graphs
            .get(deleted_policy_key(graph_id))?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(StoreError::from))
            .transpose()
    }

    pub fn graph_tagged_policy(&self, graph: &GraphId) -> Result<TaggedGraphPolicy> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(TaggedGraphPolicy {
                policy: GraphPolicy::default(),
                tag: PolicyTag::default(),
            });
        };
        let meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        Ok(TaggedGraphPolicy {
            policy: meta.policy,
            tag: meta.policy_tag,
        })
    }

    pub fn irokle_topic_id(&self, graph: &GraphId) -> Result<Option<[u8; 32]>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        Ok(self
            .read_graph_meta(graph_id)?
            .unwrap_or_default()
            .irokle_topic)
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn set_topic_id(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        self.set_topic_guarded(graph, topic_id)
    }

    /// Caller holds this graph's [`GraphCommitGuard`].
    pub(crate) fn set_topic_guarded(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        meta.irokle_topic = Some(topic_id);
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        batch.insert(
            &self.graphs,
            topic_binding_key(&topic_id),
            graph.as_str().as_bytes(),
        );
        self.commit(batch)
    }

    /// Read the stored raw RO-Crate `@context` JSON for a graph, if any.
    pub fn graph_context(&self, graph: &GraphId) -> Result<Option<String>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        Ok(self
            .read_graph_meta(graph_id)?
            .unwrap_or_default()
            .rocrate_context)
    }

    /// Read the raw root `license` JSON and the graph digest it describes.
    pub fn graph_license(&self, graph: &GraphId) -> Result<Option<(String, [u8; 32])>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        let meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        Ok(meta.rocrate_license.zip(meta.rocrate_license_digest))
    }

    /// Read the last-write-wins ordering tag for a graph's stored `@context`.
    /// Returns [`ContextTag::GENESIS`] when the graph has no explicit context.
    pub fn graph_context_tag(&self, graph: &GraphId) -> Result<ContextTag> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(ContextTag::GENESIS);
        };
        Ok(self
            .read_graph_meta(graph_id)?
            .unwrap_or_default()
            .context_tag)
    }

    /// Persists raw RO-Crate render hints under its own graph guard.
    #[cfg(test)]
    pub fn set_graph_context(
        &self,
        graph: &GraphId,
        context: Option<&str>,
        license: Option<&str>,
        license_digest: Option<[u8; 32]>,
        tag: ContextTag,
    ) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        self.stage_graph_context(
            &mut batch,
            graph_id,
            &TaggedRenderHints {
                hints: RenderHints {
                    context: context.map(str::to_owned),
                    license: license.map(str::to_owned),
                    license_digest,
                },
                tag,
            },
        )?;
        self.commit(batch)
    }

    /// Stage render hints in the same durable batch as their graph mutation.
    /// Caller holds the graph commit guard.
    pub(crate) fn stage_graph_context(
        &self,
        batch: &mut WriteBatch,
        graph_id: TermId,
        tagged: &TaggedRenderHints,
    ) -> Result<bool> {
        let mut meta = self.read_graph_meta(graph_id)?.unwrap_or_default();
        if tagged.tag <= meta.context_tag {
            return Ok(false);
        }
        meta.rocrate_context = tagged.hints.context.clone();
        meta.rocrate_license = tagged.hints.license.clone();
        meta.rocrate_license_digest = tagged.hints.license_digest;
        meta.context_tag = tagged.tag;
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        Ok(true)
    }

    pub fn topic_graph_binding(&self, topic_id: &[u8; 32]) -> Result<Option<String>> {
        self.graphs
            .get(topic_binding_key(topic_id))?
            .map(|bytes| {
                String::from_utf8(bytes.to_vec()).map_err(|error| StoreError::InvalidEncoding {
                    context: "topic binding",
                    message: error.to_string(),
                })
            })
            .transpose()
    }

    pub fn applied_topic_clock(&self, topic_id: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .graphs
            .get(topic_clock_key(topic_id))?
            .map(|bytes| bytes.to_vec()))
    }

    pub fn set_topic_clock(&self, topic_id: &[u8; 32], clock: &[u8]) -> Result<()> {
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, topic_clock_key(topic_id), clock);
        batch.commit()?;
        Ok(())
    }

    pub fn record_replication_rejection(
        &self,
        mut record: crate::sync::RejectedReplicationRecord,
        cursor: Option<&[u8]>,
    ) -> Result<crate::sync::RejectedReplicationRecord> {
        let key = replication_rejection_key(&record.topic, &record.record_id);
        if let Some(existing) = self.graphs.get(key)? {
            let existing: crate::sync::RejectedReplicationRecord =
                postcard::from_bytes(existing.as_ref())?;
            record.seen_count = existing.seen_count.saturating_add(1);
            record.acknowledged = existing.acknowledged;
        } else {
            record.seen_count = 1;
        }
        let mut batch = self.new_batch();
        batch.insert(&self.graphs, key, postcard::to_allocvec(&record)?);
        if let Some(cursor) = cursor {
            batch.insert(
                &self.graphs,
                topic_clock_key(record.topic.as_bytes()),
                cursor,
            );
        }
        self.commit(batch)?;
        Ok(record)
    }

    pub fn replication_rejections(&self) -> Result<Vec<crate::sync::RejectedReplicationRecord>> {
        self.graphs
            .prefix(replication_rejection_prefix())
            .map(|guard| {
                let (_, value) = guard.into_inner()?;
                postcard::from_bytes(value.as_ref()).map_err(Into::into)
            })
            .collect()
    }

    pub fn replication_rejection(
        &self,
        topic: &irokle::TopicId,
        record: &irokle::OpId,
    ) -> Result<Option<crate::sync::RejectedReplicationRecord>> {
        self.graphs
            .get(replication_rejection_key(topic, record))?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(Into::into))
            .transpose()
    }

    pub fn acknowledge_replication_rejection(
        &self,
        topic: &irokle::TopicId,
        record_id: &irokle::OpId,
    ) -> Result<bool> {
        let key = replication_rejection_key(topic, record_id);
        let Some(value) = self.graphs.get(key)? else {
            return Ok(false);
        };
        let mut record: crate::sync::RejectedReplicationRecord =
            postcard::from_bytes(value.as_ref())?;
        record.acknowledged = true;
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, key, postcard::to_allocvec(&record)?);
        batch.commit()?;
        Ok(true)
    }

    pub fn delete_replication_rejection(
        &self,
        topic: &irokle::TopicId,
        record_id: &irokle::OpId,
    ) -> Result<bool> {
        let key = replication_rejection_key(topic, record_id);
        if self.graphs.get(key)?.is_none() {
            return Ok(false);
        }
        let mut batch = self.buffered_batch();
        batch.remove(&self.graphs, key);
        batch.commit()?;
        Ok(true)
    }

    pub fn repair_topic_cursor(
        &self,
        topic: irokle::TopicId,
        expected_old_digest: [u8; 32],
        replacement: &[u8],
        repair_time_ns: i64,
    ) -> Result<crate::sync::TopicCursorRepairAudit> {
        let key = topic_clock_key(topic.as_bytes());
        let old = self.graphs.get(key)?;
        let old_digest =
            crate::sync::topic_cursor_digest(old.as_ref().map_or(&[][..], |value| value.as_ref()));
        if old_digest != expected_old_digest {
            return Err(StoreError::CursorCompareFailed);
        }
        let audit = crate::sync::TopicCursorRepairAudit {
            topic,
            old_cursor_digest: old_digest,
            replacement_cursor_digest: crate::sync::topic_cursor_digest(replacement),
            repaired_at_unix_nanos: repair_time_ns,
        };
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, key, replacement);
        batch.insert(
            &self.graphs,
            cursor_audit_key(&audit),
            postcard::to_allocvec(&audit)?,
        );
        batch.commit()?;
        Ok(audit)
    }

    pub fn graph_tombstoned(&self, graph: &GraphId) -> Result<bool> {
        Ok(self.graph_tombstone(graph)?.is_some())
    }

    pub fn graph_tombstone(&self, graph: &GraphId) -> Result<Option<GraphTombstone>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        self.graphs
            .get(graph_tombstone_key(graph_id))?
            .map(|bytes| postcard::from_bytes(bytes.as_ref()).map_err(Into::into))
            .transpose()
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    #[cfg(test)]
    pub fn set_graph_tombstone(&self, graph: &GraphId) -> Result<()> {
        let actor = ActorId::from_bytes([0; 32]);
        let clock = VectorClock::default();
        let tombstone = GraphTombstone {
            graph: graph.clone(),
            delete_event: EventId::graph_delete(graph, actor, &clock),
            delete_actor: actor,
            delete_clock: clock,
        };
        let _commit_guard = self.graph_commit_guard(graph);
        let graph_id = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.graphs,
            graph_tombstone_key(graph_id),
            postcard::to_allocvec(&tombstone)?,
        );
        batch.commit()?;
        Ok(())
    }

    pub fn graph_snapshot(&self, graph: &GraphId) -> Result<GraphReplicaSnapshot> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphReplicaSnapshot {
                graph: graph.clone(),
                clock: VectorClock::new(),
                quads: Vec::new(),
            });
        };
        let snapshot = self.db.snapshot();
        let vector_clock = self.snapshot_vector_clock(&snapshot, graph_id)?;

        let mut quads = Vec::new();
        for guard in snapshot.prefix(&self.quads, graph_id.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            let mut dots = decode_dots(value.as_ref())?;
            // Value sorting makes equal replica state produce equal snapshots.
            dots.sort_unstable_by_key(|dot| (dot.actor, dot.counter));
            quads.push(SnapshotQuadState {
                subject: self.decode_term_arc(quad.subject)?.as_ref().clone(),
                predicate: self.decode_term_arc(quad.predicate)?.as_ref().clone(),
                object: self.decode_term_arc(quad.object)?.as_ref().clone(),
                dots,
            });
        }
        quads.sort_unstable_by(|left, right| {
            (&left.subject, &left.predicate, &left.object).cmp(&(
                &right.subject,
                &right.predicate,
                &right.object,
            ))
        });

        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            clock: vector_clock,
            quads,
        })
    }

    pub(crate) fn graph_snapshot_bounded(
        &self,
        graph: &GraphId,
        limits: SnapshotLimits,
    ) -> Result<GraphReplicaSnapshot> {
        const MAX_ROWS: u64 = 1_048_576;
        const MAX_BYTES: u64 = 64 * 1_048_576;

        let row_limit = limits.max_rows.min(MAX_ROWS);
        let byte_limit = limits.max_bytes.min(MAX_BYTES);
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphReplicaSnapshot {
                graph: graph.clone(),
                clock: VectorClock::new(),
                quads: Vec::new(),
            });
        };
        let snapshot = self.db.snapshot();
        let clock = self.snapshot_vector_clock(&snapshot, graph_id)?;
        let mut rows = 0u64;
        let mut bytes = 0u64;
        let mut quads = Vec::new();
        for guard in snapshot.prefix(&self.quads, graph_id.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let next_rows = rows.saturating_add(1);
            if next_rows > row_limit {
                return Err(StoreError::LimitExceeded {
                    resource: "graph snapshot rows",
                    limit: row_limit,
                    actual: next_rows,
                });
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            let subject_bytes =
                self.terms
                    .size_of(quad.subject.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(quad.subject.0))? as usize;
            let predicate_bytes =
                self.terms
                    .size_of(quad.predicate.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(quad.predicate.0))? as usize;
            let object_bytes =
                self.terms
                    .size_of(quad.object.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(quad.object.0))? as usize;
            let row_bytes = u64::try_from(
                subject_bytes
                    .saturating_add(predicate_bytes)
                    .saturating_add(object_bytes)
                    .saturating_add(value.len()),
            )
            .unwrap_or(u64::MAX);
            let next_bytes = bytes.saturating_add(row_bytes);
            if next_bytes > byte_limit {
                return Err(StoreError::LimitExceeded {
                    resource: "graph snapshot bytes",
                    limit: byte_limit,
                    actual: next_bytes,
                });
            }
            let subject = self.decode_term_arc(quad.subject)?.as_ref().clone();
            let predicate = self.decode_term_arc(quad.predicate)?.as_ref().clone();
            let object = self.decode_term_arc(quad.object)?.as_ref().clone();
            let mut dots = decode_dots(value.as_ref())?;
            dots.sort_unstable_by_key(|dot| (dot.actor, dot.counter));
            quads.push(SnapshotQuadState {
                subject,
                predicate,
                object,
                dots,
            });
            rows = next_rows;
            bytes = next_bytes;
        }
        quads.sort_unstable_by(|left, right| {
            (&left.subject, &left.predicate, &left.object).cmp(&(
                &right.subject,
                &right.predicate,
                &right.object,
            ))
        });
        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            clock,
            quads,
        })
    }

    /// Persist an exact, digest-addressed graph backup before authoritative repair.
    pub(crate) fn backup_snapshot(&self, graph: &GraphId) -> Result<BackupProof> {
        let _write = self.graph_write_guard(graph);
        let _commit = self.graph_commit_guard(graph);
        let snapshot = self.graph_snapshot_bounded(
            graph,
            SnapshotLimits {
                max_rows: 1_048_576,
                max_bytes: 64 * 1_048_576,
            },
        )?;
        let bytes = postcard::to_allocvec(&snapshot)?;
        let digest = *blake3::hash(&bytes).as_bytes();
        let mut batch = self.buffered_batch();
        batch.insert(&self.repair_backups, digest, bytes);
        self.commit_fjall_batch(batch)?;
        self.db.persist(self.persist_mode)?;
        let location = digest
            .iter()
            .fold(String::from("craqle:backup:"), |mut value, byte| {
                use std::fmt::Write;
                let _ = write!(value, "{byte:02x}");
                value
            });
        Ok(BackupProof {
            location,
            archive_digest: digest,
            source_revision: digest,
        })
    }

    /// Replace one live graph from authorized healthy state after exact backup.
    pub(crate) fn replace_snapshot(
        &self,
        snapshot: &GraphReplicaSnapshot,
        audit: &RepairAudit,
    ) -> Result<()> {
        let _write = self.graph_write_guard(&snapshot.graph);
        let _commit = self.graph_commit_guard(&snapshot.graph);
        if self.graph_tombstoned(&snapshot.graph)? {
            return Err(StoreError::ReceiptConflict);
        }
        let missing = !self.contains_graph(&snapshot.graph)?;
        let before = self.graph_snapshot_bounded(
            &snapshot.graph,
            SnapshotLimits {
                max_rows: 1_048_576,
                max_bytes: 64 * 1_048_576,
            },
        )?;
        let before_digest = *blake3::hash(&postcard::to_allocvec(&before)?).as_bytes();
        let after_digest = *blake3::hash(&postcard::to_allocvec(snapshot)?).as_bytes();
        let backup = audit.backup.as_ref().ok_or(StoreError::ReceiptConflict)?;
        let backup_bytes = self
            .repair_backups
            .get(backup.archive_digest)?
            .ok_or(StoreError::ReceiptConflict)?;
        let stored_digest = *blake3::hash(backup_bytes.as_ref()).as_bytes();
        let backup_snapshot: GraphReplicaSnapshot = postcard::from_bytes(backup_bytes.as_ref())?;
        let expected_location =
            backup
                .archive_digest
                .iter()
                .fold(String::from("craqle:backup:"), |mut value, byte| {
                    use std::fmt::Write;
                    let _ = write!(value, "{byte:02x}");
                    value
                });
        if audit.graph != snapshot.graph
            || audit.mode != RepairMode::Apply
            || audit.result != RepairResult::Applied
            || audit.before_digest != before_digest
            || audit.after_digest != Some(after_digest)
            || backup.source_revision != before_digest
            || backup.archive_digest != stored_digest
            || backup_snapshot != before
            || backup.location != expected_location
        {
            return Err(StoreError::ReceiptConflict);
        }
        let mut batch = self.new_batch();
        let graph_id = match self.graph_id_for(&snapshot.graph)? {
            Some(graph_id) if !missing => graph_id,
            Some(_) | None
                if missing && before.quads.is_empty() && before.clock == VectorClock::new() =>
            {
                self.stage_graph(&mut batch, &snapshot.graph)?
            }
            Some(_) | None => {
                return Err(StoreError::GraphNotFound(snapshot.graph.to_string()));
            }
        };
        let mut subjects = HashSet::new();
        self.visit_graph_quads::<StoreError, _>(graph_id, |quad| {
            subjects.insert(quad.subject);
            self.write_quad_state(&mut batch, quad, Vec::new())?;
            Ok(())
        })?;
        for state in &snapshot.quads {
            let quad = EncodedQuad {
                graph: graph_id,
                subject: self.encode_term_internal(Some(&mut batch), &state.subject)?,
                predicate: self.encode_term_internal(Some(&mut batch), &state.predicate)?,
                object: self.encode_term_internal(Some(&mut batch), &state.object)?,
            };
            subjects.insert(quad.subject);
            self.write_quad_state(&mut batch, quad, state.dots.clone())?;
        }
        self.set_vector_clock(
            &mut batch,
            ClockUpdate {
                graph_id,
                clock: &snapshot.clock,
            },
        )?;
        // An unchanged clock would keep trusting results derived from the replaced quads.
        batch.remove(&self.graphs, graph_diagnostics_key(graph_id));
        #[cfg(feature = "shacl-core")]
        self.stage_pending_bindings(
            &mut batch,
            &snapshot.graph,
            *blake3::hash(&postcard::to_allocvec(&snapshot.clock)?).as_bytes(),
        )?;
        self.enqueue_fts_subjects(
            &mut batch,
            FtsEnqueue {
                graph_id,
                subjects: &subjects,
            },
        )?;
        self.stage_repair_audit(&mut batch, audit)?;
        self.commit(batch)
    }

    pub fn graph_fingerprint(&self, graph: &GraphId) -> Result<(u64, [u8; 32], [u8; 32])> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            let empty = *blake3::hash(&[]).as_bytes();
            return Ok((0, empty, empty));
        };

        let mut count = 0u64;
        let mut xor = [0u8; 32];
        let mut sum = [0u8; 32];
        let snapshot = self.db.snapshot();
        for guard in snapshot.prefix(&self.quads, graph_id.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if dots_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(self.decode_term_arc(quad.subject)?.0.as_bytes());
            hasher.update(&[0]);
            hasher.update(self.decode_term_arc(quad.predicate)?.0.as_bytes());
            hasher.update(&[0]);
            hasher.update(self.decode_term_arc(quad.object)?.0.as_bytes());
            let quad_hash = hasher.finalize();
            for (index, byte) in quad_hash.as_bytes().iter().enumerate() {
                xor[index] ^= byte;
                sum[index] = sum[index].wrapping_add(*byte);
            }
            count += 1;
        }
        Ok((count, xor, sum))
    }

    pub fn subject_triple_count(&self, graph: TermId, subject: TermId) -> Result<usize> {
        Ok(self.subject_entries((graph, subject), None)?.len())
    }

    /// Stages one OR-Set add while the caller holds the graph guard.
    pub fn insert_quad(&self, batch: &mut WriteBatch, add: QuadAdd) -> Result<bool> {
        let QuadAdd { quad, dot } = add;
        let key = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
        let mut dots = self.current_quad_dots(batch, &key)?;
        if dots.contains(&dot) {
            return Ok(false);
        }
        dots.push(dot);
        self.write_quad_state(batch, quad, dots)
    }

    /// Removes only witnessed dots while the caller holds the graph guard.
    pub fn remove_quad(&self, batch: &mut WriteBatch, removal: QuadRemove<'_>) -> Result<bool> {
        let QuadRemove { quad, witnessed } = removal;
        let key = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
        let mut dots = self.current_quad_dots(batch, &key)?;
        let before = dots.len();
        dots.retain(|dot| !witnessed.contains(dot));
        if before == dots.len() {
            return Ok(false);
        }
        self.write_quad_state(batch, quad, dots)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn quad_dots(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
    ) -> Result<Vec<Dot>> {
        let Some(graph) = self.graph_id_for(graph)? else {
            return Ok(Vec::new());
        };
        let Some(subject) = self.lookup_term(subject)? else {
            return Ok(Vec::new());
        };
        let Some(predicate) = self.lookup_term(predicate)? else {
            return Ok(Vec::new());
        };
        let Some(object) = self.lookup_term(object)? else {
            return Ok(Vec::new());
        };
        self.read_quad_dots(&Self::quad_key(graph, subject, predicate, object))
    }

    /// Is this exact durable quad live? Uncommitted batch state is invisible.
    pub fn contains_quad(&self, quad: EncodedQuad) -> Result<bool> {
        Ok(self
            .quads
            .get(Self::quad_key(
                quad.graph,
                quad.subject,
                quad.predicate,
                quad.object,
            ))?
            .is_some_and(|value| !dots_empty(value.as_ref())))
    }

    /// Returns the highest committed FTS token under the queue lock.
    pub(crate) fn current_dirty_token(&self) -> u64 {
        let _queue = self.fts_queue_guard();
        self.locked_dirty_token()
    }

    fn locked_dirty_token(&self) -> u64 {
        self.dirty_committed.load(Ordering::SeqCst)
    }

    pub fn quads_for_pattern(
        &self,
        graph: Option<TermId>,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        let pattern = crate::rdf_read::QuadPattern {
            graph,
            subject,
            predicate,
            object,
        };
        let snapshot = self.read_snapshot();
        let mut cursor = snapshot.raw_quad_cursor(self, pattern);
        let mut quads = Vec::new();
        while let Some(candidate) = cursor.next_candidate() {
            let candidate = candidate?;
            if candidate.live && pattern.matches(candidate.quad) {
                quads.push(candidate.quad);
            }
        }
        Ok(quads)
    }

    /// Returns a derived range only when it matches the requested snapshot.
    fn derived_raw_cursor(
        &self,
        _snapshot_seqno: u64,
        _pattern: crate::rdf_read::QuadPattern,
    ) -> Option<crate::query::cursor::RawQuadCursor> {
        None
    }

    pub fn visit_graph_quads<E, F>(&self, graph: TermId, mut visit: F) -> std::result::Result<(), E>
    where
        E: From<StoreError>,
        F: FnMut(EncodedQuad) -> std::result::Result<(), E>,
    {
        for quad in self.graph_scan(graph, None, None)? {
            visit(quad)?;
        }
        Ok(())
    }

    /// Streams committed graph quads with their raw dot-set bytes.
    pub(crate) fn visit_stored_quads<F>(&self, graph: TermId, mut visit: F) -> Result<()>
    where
        F: FnMut(EncodedQuad, &[u8]) -> Result<()>,
    {
        for guard in self.quads.prefix(graph.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 64 {
                continue;
            }
            let dots = value.as_ref();
            if dots_empty(dots) {
                continue;
            }
            visit(Self::decode_quad_key(key.as_ref())?, dots)?;
        }
        Ok(())
    }

    pub fn get_vector_clock(&self, graph: &GraphId) -> Result<VectorClock> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(VectorClock::new());
        };
        self.vector_clock_id(graph_id)
    }

    /// Reads one graph clock from a coherent Fjall snapshot.
    pub(crate) fn vector_clock_id(&self, graph_id: TermId) -> Result<VectorClock> {
        self.snapshot_vector_clock(&self.db.snapshot(), graph_id)
    }

    /// Stages only the graph clock while the caller holds the graph guard.
    pub fn set_vector_clock(&self, batch: &mut WriteBatch, update: ClockUpdate<'_>) -> Result<()> {
        batch.insert(
            &self.graphs,
            graph_clock_key(update.graph_id),
            postcard::to_allocvec(update.clock)?,
        );
        batch
            .publish
            .clocks
            .insert(update.graph_id, Some(update.clock.clone()));
        Ok(())
    }

    /// Stages the next actor counter while the caller holds the graph guard.
    pub fn next_counter(&self, batch: &mut WriteBatch, key: CounterKey) -> Result<u64> {
        let head = log_head_key(key.graph_id, &key.actor);
        let counter = match self.log.get(head)? {
            Some(value) => decode_u64(value.as_ref(), "log head")? + 1,
            None => 1,
        };
        batch.insert(&self.log, head, counter.to_be_bytes());
        Ok(counter)
    }

    pub(crate) fn decode_term_cached(
        &self,
        cache: &mut HashMap<TermId, EncodedTerm>,
        id: TermId,
    ) -> Result<EncodedTerm> {
        if let Some(term) = cache.get(&id) {
            return Ok(term.clone());
        }
        let term = self.decode_term(id)?;
        cache.insert(id, term.clone());
        Ok(term)
    }

    /// Queues one subject in the source batch; commit later mints its token.
    pub fn enqueue_fts(&self, batch: &mut WriteBatch, key: FtsSubject) -> Result<()> {
        batch.pending_fts.push(FtsQueueKey::Subject {
            graph: key.graph_id,
            subject: key.subject,
        });
        Ok(())
    }

    /// Queues subjects or a cheaper whole-graph reindex.
    pub fn enqueue_fts_subjects(&self, batch: &mut WriteBatch, req: FtsEnqueue<'_>) -> Result<()> {
        if req.subjects.is_empty() {
            return Ok(());
        }
        if self.reindex_is_cheaper(req.graph_id, req.subjects.len())? {
            return self.enqueue_fts_reindex(batch, req.graph_id);
        }
        for subject in req.subjects {
            self.enqueue_fts(
                batch,
                FtsSubject {
                    graph_id: req.graph_id,
                    subject: *subject,
                },
            )?;
        }
        Ok(())
    }

    /// Chooses a full graph scan only for a large, dense subject set.
    fn reindex_is_cheaper(&self, graph_id: TermId, subjects: usize) -> Result<bool> {
        Ok(subjects >= FTS_REINDEX_THRESHOLD
            && subjects * 2 >= self.graph_subject_count(graph_id)?)
    }

    pub fn enqueue_fts_reindex(&self, batch: &mut WriteBatch, graph_id: TermId) -> Result<()> {
        batch.pending_fts.push(FtsQueueKey::Reindex(graph_id));
        Ok(())
    }

    pub(crate) fn ensure_reindex(&self, graph: &GraphId) -> Result<(u64, bool)> {
        let graph_id = self
            .graph_id_for(graph)?
            .ok_or_else(|| StoreError::GraphNotFound(graph.to_string()))?;
        let _queue = self.fts_queue_guard();
        let key = graph_reindex_key(graph_id);
        if let Some(value) = self.graphs.get(key)? {
            let tokens = decode_dirty_tokens(value.as_ref(), "graph reindex tokens")?;
            return Ok((tokens.latest, false));
        }
        let mut batch = self.buffered_batch();
        let token = self.stage_fts_entry(&mut batch, FtsQueueKey::Reindex(graph_id))?;
        batch.insert(&self.search_meta, SEARCH_HEAD_KEY, token.to_be_bytes());
        self.commit_fjall_batch(batch)?;
        self.dirty_committed.fetch_max(token, Ordering::SeqCst);
        Ok((token, true))
    }

    #[cfg(test)]
    pub(crate) fn drain_fts_queue(&self, limit: usize) -> Result<Vec<DirtySubject>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_dirty_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 33 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph dirty graph")?;
            let subject_id = decode_term_id(&key[17..33], "graph dirty subject")?;
            let tokens = decode_dirty_tokens(value.as_ref(), "graph dirty tokens")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push(DirtySubject {
                    graph,
                    subject: subject_id,
                    tokens,
                });
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn drain_reindex_queue(&self, limit: usize) -> Result<Vec<DirtyGraph>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_reindex_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph reindex graph")?;
            let tokens = decode_dirty_tokens(value.as_ref(), "graph reindex tokens")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push(DirtyGraph { graph, tokens });
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn drain_delete_queue(&self, limit: usize) -> Result<Vec<DirtyGraph>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_delete_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph search delete graph")?;
            let tokens = decode_dirty_tokens(value.as_ref(), "graph search delete tokens")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push(DirtyGraph { graph, tokens });
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    fn ordered_queue(&self, kind: QueueKind, scan: &QueueScan) -> Result<OrderedQueuePage> {
        let after = scan.after.map(search_order_key);
        let mut page = OrderedQueuePage {
            rows: Vec::new(),
            next: scan.after,
            remaining: false,
            visited: 0,
            bytes: 0,
            oversized: None,
        };
        let range = resume_range(&[SEARCH_ORDER_PREFIX], after.as_ref().map(|key| &key[..]));
        for guard in self.search_queue.range(range) {
            let (key, identity) = guard.into_inner()?;
            let cursor = decode_search_order(key.as_ref())?;
            if scan.max_token.is_some_and(|bound| cursor.token > bound) {
                break;
            }
            if page.visited == scan.row_limit {
                page.remaining = true;
                break;
            }
            page.visited += 1;
            if cursor.kind != kind {
                page.next = Some(cursor);
                continue;
            }
            let current = self
                .graphs
                .get(identity.as_ref())?
                .ok_or(StoreError::InvalidSearchState("queue-order-orphan"))?;
            let tokens = decode_dirty_tokens(current.as_ref(), "fts queue tokens")?;
            if tokens.oldest != cursor.token {
                return Err(StoreError::InvalidSearchState("queue-order-token-mismatch"));
            }
            let graph_bytes =
                self.terms
                    .size_of(cursor.graph.to_be_bytes())?
                    .ok_or(StoreError::TermNotFound(cursor.graph.0))? as usize;
            let encoded = key
                .len()
                .saturating_add(identity.len())
                .saturating_add(current.len())
                .saturating_add(graph_bytes);
            if page.bytes.saturating_add(encoded) > scan.byte_limit {
                if encoded > scan.byte_limit {
                    page.oversized = Some((cursor, tokens.latest, encoded));
                    page.next = Some(cursor);
                }
                page.remaining = true;
                break;
            }
            page.bytes = page.bytes.saturating_add(encoded);
            page.rows.push((cursor, tokens));
            page.next = Some(cursor);
        }
        Ok(page)
    }

    fn queue_id(&self, cursor: QueueCursor) -> Result<QueueId> {
        let graph = self
            .decode_term(cursor.graph)?
            .to_named_node()
            .map(GraphId)
            .ok_or(StoreError::InvalidSearchState("queue-graph-not-named"))?;
        Ok(QueueId {
            kind: cursor.kind,
            graph,
            subject: cursor.subject,
        })
    }

    pub(crate) fn scan_fts_subjects(&self, scan: &QueueScan) -> Result<QueuePage<DirtySubject>> {
        let page = self.ordered_queue(QueueKind::Subject, scan)?;
        let mut entries = Vec::with_capacity(page.rows.len());
        for (cursor, tokens) in page.rows {
            let id = self.queue_id(cursor)?;
            entries.push(DirtySubject {
                graph: id.graph,
                subject: cursor.subject.ok_or(StoreError::InvalidSearchState(
                    "subject-queue-subject-missing",
                ))?,
                tokens,
            });
        }
        Ok(QueuePage {
            entries,
            next: page.next,
            remaining: page.remaining,
            rows: page.visited,
            bytes: page.bytes,
            oversized: page
                .oversized
                .map(|(cursor, target, bytes)| {
                    Ok::<OversizedEntry, StoreError>(OversizedEntry {
                        id: self.queue_id(cursor)?,
                        owed_from: cursor.token,
                        target,
                        bytes,
                    })
                })
                .transpose()?,
        })
    }

    pub(crate) fn scan_fts_reindexes(&self, scan: &QueueScan) -> Result<QueuePage<DirtyGraph>> {
        self.scan_fts_graphs(QueueKind::Reindex, scan)
    }

    pub(crate) fn scan_fts_deletes(&self, scan: &QueueScan) -> Result<QueuePage<DirtyGraph>> {
        self.scan_fts_graphs(QueueKind::Delete, scan)
    }

    fn scan_fts_graphs(&self, kind: QueueKind, scan: &QueueScan) -> Result<QueuePage<DirtyGraph>> {
        let page = self.ordered_queue(kind, scan)?;
        let mut entries = Vec::with_capacity(page.rows.len());
        for (cursor, tokens) in page.rows {
            entries.push(DirtyGraph {
                graph: self.queue_id(cursor)?.graph,
                tokens,
            });
        }
        Ok(QueuePage {
            entries,
            next: page.next,
            remaining: page.remaining,
            rows: page.visited,
            bytes: page.bytes,
            oversized: page
                .oversized
                .map(|(cursor, target, bytes)| {
                    Ok::<OversizedEntry, StoreError>(OversizedEntry {
                        id: self.queue_id(cursor)?,
                        owed_from: cursor.token,
                        target,
                        bytes,
                    })
                })
                .transpose()?,
        })
    }

    fn queue_id_cursor(&self, id: &QueueId) -> Result<Option<QueueCursor>> {
        let Some(graph) = self.graph_id_for(&id.graph)? else {
            return Ok(None);
        };
        Ok(Some(QueueCursor {
            token: 0,
            kind: id.kind,
            graph,
            subject: id.subject,
        }))
    }

    pub(crate) fn fts_failure(&self, id: &QueueId) -> Result<Option<RetryState>> {
        let Some(cursor) = self.queue_id_cursor(id)? else {
            return Ok(None);
        };
        self.search_meta
            .get(search_failure_key(cursor))?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(StoreError::from))
            .transpose()
    }

    pub(crate) fn set_fts_failure(&self, state: &RetryState) -> Result<()> {
        let cursor = self
            .queue_id_cursor(&state.id)?
            .ok_or(StoreError::InvalidSearchState(
                "failure-queue-identity-missing",
            ))?;
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_failure_key(cursor),
            postcard::to_allocvec(state)?,
        );
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn clear_fts_failure(&self, id: &QueueId) -> Result<()> {
        let Some(cursor) = self.queue_id_cursor(id)? else {
            return Ok(());
        };
        let mut batch = self.buffered_batch();
        batch.remove(&self.search_meta, search_failure_key(cursor));
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn quarantine_search_failure(&self, failure: &StageFailure) -> Result<()> {
        if !matches!(failure.state.id.kind, QueueKind::Reindex)
            || failure.state.id.subject.is_some()
        {
            return Err(StoreError::InvalidSearchState("search-stage-failure-kind"));
        }
        let _queue = self.fts_queue_guard();
        let graph_id = self
            .graph_id_for(&failure.state.id.graph)?
            .ok_or(StoreError::InvalidSearchState("search-stage-graph-missing"))?;
        let current =
            self.graphs
                .get(graph_reindex_key(graph_id))?
                .ok_or(StoreError::InvalidSearchState(
                    "search-reindex-debt-missing",
                ))?;
        let tokens = decode_dirty_tokens(current.as_ref(), "graph reindex tokens")?;
        if failure.state.owed_from != tokens.oldest || failure.state.target != tokens.latest {
            return Err(StoreError::InvalidSearchState(
                "search-stage-failure-target",
            ));
        }
        let cursor = QueueCursor {
            token: tokens.oldest,
            kind: QueueKind::Reindex,
            graph: graph_id,
            subject: None,
        };
        if let Some(value) = self.search_meta.get(search_failure_key(cursor))? {
            let existing: RetryState = postcard::from_bytes(value.as_ref())?;
            if existing.id != failure.state.id
                || existing.owed_from != failure.state.owed_from
                || existing.target != failure.state.target
                || existing.attempts > failure.state.attempts
            {
                return Err(StoreError::InvalidSearchState(
                    "search-stage-failure-conflict",
                ));
            }
        }
        let stage = self.stored_search_stage(&failure.state.id.graph)?;
        match (&stage, &failure.job) {
            (Some(stored), Some(expected))
                if stored.index_id == failure.index_id && stored.job == *expected => {}
            (None, None) => {}
            (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => {
                return Err(StoreError::InvalidSearchState(
                    "search-stage-failure-conflict",
                ));
            }
        }
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_failure_key(cursor),
            postcard::to_allocvec(&failure.state)?,
        );
        if let Some(stage) = stage {
            batch.remove(&self.search_meta, search_stage_key(&failure.state.id.graph));
            self.queue_search_cleanup(
                &mut batch,
                &CleanupJob {
                    graph: failure.state.id.graph.clone(),
                    generation: stage.job.generation,
                },
            )?;
        }
        self.commit_fjall_batch(batch)
    }

    fn next_search_generation(&self, batch: &mut fjall::OwnedWriteBatch) -> Result<GenerationId> {
        let next = self
            .search_meta
            .get(SEARCH_NEXT_KEY)?
            .map(|value| decode_u64(value.as_ref(), "search generation counter"))
            .transpose()?
            .unwrap_or(1)
            .max(1);
        let following = next.checked_add(1).ok_or(StoreError::InvalidSearchState(
            "search-generation-exhausted",
        ))?;
        batch.insert(&self.search_meta, SEARCH_NEXT_KEY, following.to_be_bytes());
        Ok(GenerationId(next))
    }

    fn generation_hash(graph: &GraphId, generation: GenerationId) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(&(graph.as_str().len() as u64).to_be_bytes());
        hash.update(graph.as_str().as_bytes());
        hash.update(&generation.0.to_be_bytes());
        *hash.finalize().as_bytes()
    }

    fn stored_manifest(&self) -> Result<Option<StoredManifest>> {
        Ok(self
            .search_meta
            .get(SEARCH_MANIFEST_KEY)?
            .and_then(|value| postcard::from_bytes(value.as_ref()).ok()))
    }

    fn stage_manifest(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        change: ManifestChange<'_>,
    ) -> Result<()> {
        let stored = self
            .search_meta
            .get(SEARCH_MANIFEST_KEY)?
            .and_then(|value| postcard::from_bytes::<StoredManifest>(value.as_ref()).ok())
            .filter(|manifest| {
                manifest.format == SEARCH_META_FORMAT && manifest.index_id == change.index_id
            });
        let had_manifest = stored.is_some();
        let mut manifest = stored.unwrap_or(StoredManifest {
            format: SEARCH_META_FORMAT,
            index_id: change.index_id,
            count: 0,
            hash: [0; 32],
            epoch: 0,
        });
        if let Some(previous) = change
            .previous
            .filter(|_| had_manifest && manifest.count != 0)
        {
            let row = Self::generation_hash(change.graph, previous);
            for (current, removed) in manifest.hash.iter_mut().zip(row) {
                *current ^= removed;
            }
            manifest.count =
                manifest
                    .count
                    .checked_sub(1)
                    .ok_or(StoreError::InvalidSearchState(
                        "search-manifest-count-underflow",
                    ))?;
        }
        if let Some(active) = change.active {
            let row = Self::generation_hash(change.graph, active);
            for (current, added) in manifest.hash.iter_mut().zip(row) {
                *current ^= added;
            }
            manifest.count =
                manifest
                    .count
                    .checked_add(1)
                    .ok_or(StoreError::InvalidSearchState(
                        "search-manifest-count-overflow",
                    ))?;
        }
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or(StoreError::InvalidSearchState(
                "search-manifest-epoch-exhausted",
            ))?;
        batch.insert(
            &self.search_meta,
            SEARCH_MANIFEST_KEY,
            postcard::to_allocvec(&manifest)?,
        );
        Ok(())
    }

    pub(crate) fn search_manifest_digest(&self, index_id: [u8; 16]) -> Result<ManifestDigest> {
        Ok(self
            .stored_manifest()?
            .filter(|manifest| {
                manifest.format == SEARCH_META_FORMAT && manifest.index_id == index_id
            })
            .map(|manifest| ManifestDigest {
                count: manifest.count,
                hash: manifest.hash,
                epoch: manifest.epoch,
            })
            .unwrap_or(ManifestDigest {
                count: 0,
                hash: [0; 32],
                epoch: 0,
            }))
    }

    fn queue_search_cleanup(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        job: &CleanupJob,
    ) -> Result<()> {
        batch.insert(
            &self.search_meta,
            search_cleanup_key(job.generation),
            postcard::to_allocvec(job)?,
        );
        Ok(())
    }

    fn stored_generation(&self, graph: &GraphId) -> Result<Option<StoredGeneration>> {
        let Some(value) = self.search_meta.get(search_generation_key(graph))? else {
            return Ok(None);
        };
        let stored = decode_search_generation(value.as_ref())?;
        let stored = Some(stored);
        if let Some(stored) = &stored
            && stored.format > SEARCH_META_FORMAT
        {
            return Err(StoreError::UnsupportedSearchFormat {
                found: stored.format,
                supported: SEARCH_META_FORMAT,
            });
        }
        Ok(stored)
    }

    fn stored_search_stage(&self, graph: &GraphId) -> Result<Option<StoredStage>> {
        let Some(value) = self.search_meta.get(search_stage_key(graph))? else {
            return Ok(None);
        };
        let stored = decode_search_stage(value.as_ref())?;
        let stored = Some(stored);
        if let Some(stored) = &stored
            && stored.format > SEARCH_META_FORMAT
        {
            return Err(StoreError::UnsupportedSearchFormat {
                found: stored.format,
                supported: SEARCH_META_FORMAT,
            });
        }
        Ok(stored)
    }

    pub(crate) fn scan_search_manifest(
        &self,
        view: &SearchManifestSnapshot,
        scan: &ManifestScan,
    ) -> Result<ManifestPage> {
        let mut candidates = BTreeSet::new();
        let candidate_limit = scan.row_limit.saturating_add(1);
        let graph_lower = scan.after.map_or_else(
            || Included(vec![GRAPH_META_PREFIX]),
            |graph| Excluded(graph_meta_key(graph).to_vec()),
        );
        let graph_upper = Excluded(vec![GRAPH_META_PREFIX.saturating_add(1)]);
        for guard in view
            .snapshot
            .range(&view.graphs, (graph_lower, graph_upper))
        {
            let (key, _) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph = TermId::from_be_bytes(key.as_ref()[1..].try_into().unwrap());
            candidates.insert(graph);
            if candidates.len() == candidate_limit {
                break;
            }
        }
        let mut generation_rows = 0usize;
        let generation_lower = scan.after.map_or_else(
            || Included(vec![SEARCH_GENERATION_PREFIX]),
            |graph| {
                let mut key = vec![SEARCH_GENERATION_PREFIX];
                key.extend_from_slice(&graph.to_be_bytes());
                Excluded(key)
            },
        );
        let generation_upper = Excluded(vec![SEARCH_GENERATION_PREFIX.saturating_add(1)]);
        for guard in view
            .snapshot
            .range(&view.search_meta, (generation_lower, generation_upper))
        {
            let (key, _) = guard.into_inner()?;
            if key.len() != 17 {
                return Err(StoreError::InvalidSearchState(
                    "search-generation-key-invalid",
                ));
            }
            let graph = TermId::from_be_bytes(key.as_ref()[1..].try_into().unwrap());
            candidates.insert(graph);
            generation_rows += 1;
            if generation_rows == candidate_limit {
                break;
            }
        }

        let mut entries = Vec::new();
        let mut next = scan.after;
        let mut bytes = 0usize;
        let mut remaining = false;
        let mut oversized = None;
        for (rows, graph) in candidates.into_iter().enumerate() {
            if rows == scan.row_limit {
                remaining = true;
                break;
            }
            let term_bytes = view
                .snapshot
                .size_of(&view.terms, graph.to_be_bytes())?
                .ok_or(StoreError::TermNotFound(graph.0))? as usize;
            let raw_bytes = term_bytes.saturating_add(16);
            if bytes.saturating_add(raw_bytes) > scan.byte_limit {
                if raw_bytes > scan.byte_limit {
                    next = Some(graph);
                    oversized = Some(OversizedSource {
                        bytes: raw_bytes,
                        limit: scan.byte_limit,
                    });
                }
                remaining = true;
                break;
            }
            let term = view
                .snapshot
                .get(&view.terms, graph.to_be_bytes())?
                .ok_or(StoreError::TermNotFound(graph.0))?;
            let encoded = EncodedTerm(decode_term_text(term.as_ref())?);
            let graph_id = encoded
                .to_named_node()
                .map(GraphId)
                .ok_or(StoreError::InvalidSearchState("manifest-graph-not-named"))?;
            let generation = view
                .snapshot
                .get(&view.search_meta, search_generation_key(&graph_id))?
                .map(|value| decode_search_generation(value.as_ref()))
                .transpose()?;
            let stage = view
                .snapshot
                .get(&view.search_meta, search_stage_key(&graph_id))?
                .map(|value| decode_search_stage(value.as_ref()))
                .transpose()?;
            let source_owed = view
                .snapshot
                .get(&view.graphs, graph_reindex_key(graph))?
                .map(|value| decode_dirty_tokens(value.as_ref(), "graph reindex tokens"))
                .transpose()?
                .map(|tokens| tokens.oldest);
            let delete_owed = view
                .snapshot
                .get(&view.graphs, graph_delete_key(graph))?
                .map(|value| decode_dirty_tokens(value.as_ref(), "graph delete tokens"))
                .transpose()?
                .map(|tokens| tokens.oldest);
            let active = generation
                .as_ref()
                .and_then(|stored| stored.generation.active);
            let wrong_index = generation.as_ref().is_some_and(|stored| {
                stored.format != SEARCH_META_FORMAT || stored.index_id != scan.index_id
            }) || stage.as_ref().is_some_and(|stored| {
                stored.format != SEARCH_META_FORMAT || stored.index_id != scan.index_id
            });
            entries.push(ManifestRow {
                graph: graph_id,
                active,
                wrong_index,
                live: view
                    .snapshot
                    .get(&view.graphs, graph_meta_key(graph))?
                    .is_some(),
                source_owed,
                delete_owed,
                stage_target: stage.map(|stored| stored.job.target),
            });
            bytes = bytes.saturating_add(raw_bytes);
            next = Some(graph);
        }
        Ok(ManifestPage {
            entries,
            next,
            remaining,
            bytes,
            oversized,
        })
    }

    pub(crate) fn search_generation(&self, req: &GenerationRequest) -> Result<GraphGeneration> {
        Ok(self
            .stored_generation(&req.graph)?
            .filter(|stored| stored.format == SEARCH_META_FORMAT && stored.index_id == req.index_id)
            .map(|stored| stored.generation)
            .unwrap_or(GraphGeneration {
                graph: req.graph.clone(),
                active: None,
                covered: 0,
            }))
    }

    pub(crate) fn ensure_search_generation(
        &self,
        req: &GenerationRequest,
    ) -> Result<GenerationSwitch> {
        let _queue = self.fts_queue_guard();
        if !self.contains_graph(&req.graph)? || self.graph_tombstoned(&req.graph)? {
            return Err(StoreError::GraphNotFound(req.graph.to_string()));
        }
        if let Some(stored) = self.stored_generation(&req.graph)?
            && stored.format == SEARCH_META_FORMAT
            && stored.index_id == req.index_id
            && let Some(active) = stored.generation.active
        {
            return Ok(GenerationSwitch {
                graph: req.graph.clone(),
                previous: Some(active),
                active: Some(active),
                covered: stored.generation.covered,
            });
        }
        let mut batch = self.buffered_batch();
        let current = self.stored_generation(&req.graph)?;
        let previous = current.as_ref().and_then(|stored| stored.generation.active);
        if let Some(generation) = previous {
            self.queue_search_cleanup(
                &mut batch,
                &CleanupJob {
                    graph: req.graph.clone(),
                    generation,
                },
            )?;
        }
        let active = self.next_search_generation(&mut batch)?;
        let generation = GraphGeneration {
            graph: req.graph.clone(),
            active: Some(active),
            covered: 0,
        };
        batch.insert(
            &self.search_meta,
            search_generation_key(&req.graph),
            encode_search_generation(&StoredGeneration {
                format: SEARCH_META_FORMAT,
                index_id: req.index_id,
                generation: generation.clone(),
            })?,
        );
        self.stage_manifest(
            &mut batch,
            ManifestChange {
                index_id: req.index_id,
                graph: &req.graph,
                previous: current
                    .filter(|stored| stored.index_id == req.index_id)
                    .and_then(|stored| stored.generation.active),
                active: Some(active),
            },
        )?;
        self.commit_fjall_batch(batch)?;
        Ok(GenerationSwitch {
            graph: req.graph.clone(),
            previous,
            active: Some(active),
            covered: 0,
        })
    }

    /// Graph generations of every in-flight search stage of one index.
    pub(crate) fn staged_search_generations(
        &self,
        index_id: [u8; 16],
    ) -> Result<Vec<(GraphId, GenerationId)>> {
        let mut staged = Vec::new();
        for guard in self.search_meta.prefix([SEARCH_STAGE_PREFIX]) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let stored = decode_search_stage(value.as_ref())?;
            if stored.format == SEARCH_META_FORMAT && stored.index_id == index_id {
                staged.push((stored.job.graph, stored.job.generation));
            }
        }
        Ok(staged)
    }

    pub(crate) fn search_stage(&self, req: &GenerationRequest) -> Result<Option<StageJob>> {
        Ok(self
            .stored_search_stage(&req.graph)?
            .filter(|stored| stored.format == SEARCH_META_FORMAT && stored.index_id == req.index_id)
            .map(|stored| stored.job))
    }

    pub(crate) fn begin_search_stage(&self, req: &StageRequest) -> Result<StageJob> {
        let _queue = self.fts_queue_guard();
        if self.graph_tombstoned(&req.graph)? {
            return Err(StoreError::GraphNotFound(req.graph.to_string()));
        }
        if let Some(stored) = self.stored_search_stage(&req.graph)?
            && stored.format == SEARCH_META_FORMAT
            && stored.index_id == req.index_id
            && stored.job.target == req.target
            && (stored.job.complete || stored.job.session == req.session)
        {
            return Ok(stored.job);
        }
        let mut batch = self.buffered_batch();
        if let Some(stored) = self.stored_search_stage(&req.graph)? {
            self.queue_search_cleanup(
                &mut batch,
                &CleanupJob {
                    graph: req.graph.clone(),
                    generation: stored.job.generation,
                },
            )?;
        }
        let generation = self.next_search_generation(&mut batch)?;
        let job = StageJob {
            graph: req.graph.clone(),
            target: req.target,
            generation,
            cursor: None,
            rows: 0,
            bytes: 0,
            complete: false,
            session: req.session,
        };
        batch.insert(
            &self.search_meta,
            search_stage_key(&req.graph),
            encode_search_stage(&StoredStage {
                format: SEARCH_META_FORMAT,
                index_id: req.index_id,
                job: job.clone(),
            })?,
        );
        self.commit_fjall_batch(batch)?;
        Ok(job)
    }

    pub(crate) fn advance_search_stage(&self, job: &StageJob) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let mut stored = self
            .stored_search_stage(&job.graph)?
            .ok_or(StoreError::InvalidSearchState("search-stage-missing"))?;
        if stored.job.generation != job.generation
            || stored.job.session != job.session
            || stored.job.target != job.target
            || stored.job.complete
            || job.rows < stored.job.rows
            || job.bytes < stored.job.bytes
            || stored
                .job
                .cursor
                .zip(job.cursor)
                .is_some_and(|(current, next)| next < current)
        {
            return Err(StoreError::InvalidSearchState(
                "search-stage-compare-failed",
            ));
        }
        stored.job = job.clone();
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_stage_key(&job.graph),
            encode_search_stage(&stored)?,
        );
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn finish_search_stage(&self, job: &StageJob) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let mut stored = self
            .stored_search_stage(&job.graph)?
            .ok_or(StoreError::InvalidSearchState("search-stage-missing"))?;
        if stored.job.generation != job.generation
            || stored.job.session != job.session
            || stored.job.target != job.target
            || stored.job.cursor != job.cursor
            || stored.job.rows != job.rows
            || stored.job.bytes != job.bytes
        {
            return Err(StoreError::InvalidSearchState(
                "search-stage-compare-failed",
            ));
        }
        stored.job.complete = true;
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_stage_key(&job.graph),
            encode_search_stage(&stored)?,
        );
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn switch_search_stage(&self, job: &StageJob) -> Result<GenerationSwitch> {
        let _queue = self.fts_queue_guard();
        let stored = self
            .stored_search_stage(&job.graph)?
            .ok_or(StoreError::InvalidSearchState("search-stage-missing"))?;
        if stored.job != *job || !stored.job.complete {
            return Err(StoreError::InvalidSearchState("search-stage-incomplete"));
        }
        if self.graph_tombstoned(&job.graph)? || !self.contains_graph(&job.graph)? {
            return Err(StoreError::InvalidSearchState("search-stage-tombstoned"));
        }
        let graph_id = self
            .graph_id_for(&job.graph)?
            .ok_or(StoreError::InvalidSearchState("search-stage-graph-missing"))?;
        let debt =
            self.graphs
                .get(graph_reindex_key(graph_id))?
                .ok_or(StoreError::InvalidSearchState(
                    "search-reindex-debt-missing",
                ))?;
        let tokens = decode_dirty_tokens(debt.as_ref(), "graph reindex tokens")?;
        if job.target < tokens.oldest {
            return Err(StoreError::InvalidSearchState(
                "search-stage-target-mismatch",
            ));
        }
        let current = self.stored_generation(&job.graph)?;
        let previous = current
            .as_ref()
            .filter(|current| current.index_id == stored.index_id)
            .and_then(|current| current.generation.active);
        let generation = GraphGeneration {
            graph: job.graph.clone(),
            active: Some(job.generation),
            covered: job.target,
        };
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_generation_key(&job.graph),
            encode_search_generation(&StoredGeneration {
                format: SEARCH_META_FORMAT,
                index_id: stored.index_id,
                generation: generation.clone(),
            })?,
        );
        batch.remove(&self.search_meta, search_stage_key(&job.graph));
        self.stage_manifest(
            &mut batch,
            ManifestChange {
                index_id: stored.index_id,
                graph: &job.graph,
                previous,
                active: Some(job.generation),
            },
        )?;
        if let Some(previous) = previous.filter(|previous| *previous != job.generation) {
            self.queue_search_cleanup(
                &mut batch,
                &CleanupJob {
                    graph: job.graph.clone(),
                    generation: previous,
                },
            )?;
        }
        self.commit_fjall_batch(batch)?;
        Ok(GenerationSwitch {
            graph: job.graph.clone(),
            previous,
            active: Some(job.generation),
            covered: job.target,
        })
    }

    pub(crate) fn delete_search_graph(&self, req: &DeleteGeneration) -> Result<GenerationSwitch> {
        let _queue = self.fts_queue_guard();
        let current = self.stored_generation(&req.graph)?;
        let previous = current
            .as_ref()
            .filter(|stored| stored.index_id == req.index_id)
            .and_then(|stored| stored.generation.active);
        let stage = self
            .stored_search_stage(&req.graph)?
            .filter(|stored| stored.index_id == req.index_id);
        let generation = GraphGeneration {
            graph: req.graph.clone(),
            active: None,
            covered: req.covered,
        };
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            search_generation_key(&req.graph),
            encode_search_generation(&StoredGeneration {
                format: SEARCH_META_FORMAT,
                index_id: req.index_id,
                generation,
            })?,
        );
        batch.remove(&self.search_meta, search_stage_key(&req.graph));
        self.stage_manifest(
            &mut batch,
            ManifestChange {
                index_id: req.index_id,
                graph: &req.graph,
                previous,
                active: None,
            },
        )?;
        for generation in previous
            .into_iter()
            .chain(stage.as_ref().map(|stored| stored.job.generation))
        {
            self.queue_search_cleanup(
                &mut batch,
                &CleanupJob {
                    graph: req.graph.clone(),
                    generation,
                },
            )?;
        }
        self.commit_fjall_batch(batch)?;
        Ok(GenerationSwitch {
            graph: req.graph.clone(),
            previous,
            active: None,
            covered: req.covered,
        })
    }

    pub(crate) fn scan_search_cleanup(&self, scan: &CleanupScan) -> Result<CleanupPage> {
        let mut entries = Vec::with_capacity(scan.row_limit);
        let mut next = scan.after;
        let mut remaining = false;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut oversized = None;
        let after = scan
            .after
            .map(|after| [&[SEARCH_CLEANUP_PREFIX][..], &after.0.to_be_bytes()].concat());
        let range = resume_range(&[SEARCH_CLEANUP_PREFIX], after.as_deref());
        for guard in self.search_meta.range(range) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 9 {
                return Err(StoreError::InvalidSearchState("search-cleanup-key-invalid"));
            }
            let generation =
                GenerationId(u64::from_be_bytes(key.as_ref()[1..].try_into().unwrap()));
            if rows == scan.row_limit {
                remaining = true;
                break;
            }
            let raw_bytes = key.len().saturating_add(value.len());
            if raw_bytes > scan.byte_limit {
                rows += 1;
                next = Some(generation);
                remaining = true;
                oversized = Some(OversizedCleanup {
                    generation,
                    bytes: raw_bytes,
                    limit: scan.byte_limit,
                });
                break;
            }
            let job: CleanupJob = postcard::from_bytes(value.as_ref())?;
            let encoded = raw_bytes.saturating_add(job.graph.as_str().len());
            if bytes.saturating_add(encoded) > scan.byte_limit {
                if encoded > scan.byte_limit {
                    rows += 1;
                    next = Some(generation);
                    oversized = Some(OversizedCleanup {
                        generation,
                        bytes: encoded,
                        limit: scan.byte_limit,
                    });
                }
                remaining = true;
                break;
            }
            entries.push(job);
            rows += 1;
            bytes = bytes.saturating_add(encoded);
            next = Some(generation);
        }
        Ok(CleanupPage {
            entries,
            next,
            remaining,
            rows,
            bytes,
            oversized,
        })
    }

    pub(crate) fn ack_search_cleanup(&self, jobs: &[CleanupJob]) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        for job in jobs {
            let active = self
                .stored_generation(&job.graph)?
                .and_then(|stored| stored.generation.active);
            let staged = self
                .stored_search_stage(&job.graph)?
                .map(|stored| stored.job.generation);
            if active == Some(job.generation) || staged == Some(job.generation) {
                return Err(StoreError::InvalidSearchState(
                    "search-cleanup-generation-live",
                ));
            }
            let key = search_cleanup_key(job.generation);
            if let Some(value) = self.search_meta.get(key)? {
                let current: CleanupJob = postcard::from_bytes(value.as_ref())?;
                if current == *job {
                    batch.remove(&self.search_meta, key);
                }
            }
        }
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn search_coverage(&self) -> Result<Option<SearchCoverage>> {
        let Some(value) = self.search_meta.get(SEARCH_COVERAGE_KEY)? else {
            return Ok(None);
        };
        let bytes = value.as_ref();
        if !bytes.starts_with(&SEARCH_COVERAGE_MAGIC) {
            let legacy: LegacySearchCoverage = postcard::from_bytes(bytes)?;
            if legacy.format > SEARCH_META_FORMAT {
                return Err(StoreError::UnsupportedSearchFormat {
                    found: legacy.format,
                    supported: SEARCH_META_FORMAT,
                });
            }
            let _ = (legacy.index_id, legacy.covered, legacy.rebuild);
            return Ok(None);
        }
        if bytes.len() < 4 {
            return Err(StoreError::InvalidSearchState(
                "search-coverage-header-invalid",
            ));
        }
        let format = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
        if format > SEARCH_META_FORMAT {
            return Err(StoreError::UnsupportedSearchFormat {
                found: format,
                supported: SEARCH_META_FORMAT,
            });
        }
        let coverage: SearchCoverage = postcard::from_bytes(&bytes[4..])?;
        if coverage.format > SEARCH_META_FORMAT {
            return Err(StoreError::UnsupportedSearchFormat {
                found: coverage.format,
                supported: SEARCH_META_FORMAT,
            });
        }
        if coverage.format != SEARCH_META_FORMAT {
            return Ok(None);
        }
        Ok(Some(coverage))
    }

    #[cfg(any(not(feature = "search"), test))]
    pub(crate) fn require_search_rebuild(&self) -> Result<u64> {
        let _queue = self.fts_queue_guard();
        let mut coverage = self.search_coverage()?.unwrap_or(SearchCoverage {
            format: SEARCH_META_FORMAT,
            index_id: [0; 16],
            index_revision: 0,
            covered: 0,
            rebuild: None,
            manifest_count: 0,
            manifest_hash: [0; 32],
            manifest_epoch: 0,
        });
        if let Some(rebuild) = coverage.rebuild {
            return Ok(rebuild);
        }
        let rebuild = self.locked_dirty_token();
        coverage.rebuild = Some(rebuild);
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            SEARCH_COVERAGE_KEY,
            encode_search_coverage(&coverage)?,
        );
        self.commit_fjall_batch(batch)?;
        Ok(rebuild)
    }

    pub(crate) fn bind_search_rebuild(&self, req: &RebuildRequest) -> Result<u64> {
        let _queue = self.fts_queue_guard();
        if let Some(coverage) = self.search_coverage()?
            && coverage.index_id == req.index_id
            && req.index_revision >= coverage.index_revision
            && let Some(rebuild) = coverage.rebuild
        {
            if self.search_meta.get(SEARCH_REBUILD_KEY)?.is_none() {
                let mut batch = self.buffered_batch();
                batch.insert(
                    &self.search_meta,
                    SEARCH_REBUILD_KEY,
                    postcard::to_allocvec(&SearchRebuildScan {
                        format: SEARCH_META_FORMAT,
                        index_id: req.index_id,
                        target: rebuild,
                        after: None,
                        done: false,
                    })?,
                );
                self.commit_fjall_batch(batch)?;
            }
            return Ok(rebuild);
        }
        let rebuild = self.locked_dirty_token();
        let manifest = self.search_manifest_digest(req.index_id)?;
        let coverage = SearchCoverage {
            format: SEARCH_META_FORMAT,
            index_id: req.index_id,
            index_revision: req.index_revision,
            covered: 0,
            rebuild: Some(rebuild),
            manifest_count: manifest.count,
            manifest_hash: manifest.hash,
            manifest_epoch: manifest.epoch,
        };
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            SEARCH_COVERAGE_KEY,
            encode_search_coverage(&coverage)?,
        );
        batch.insert(
            &self.search_meta,
            SEARCH_REBUILD_KEY,
            postcard::to_allocvec(&SearchRebuildScan {
                format: SEARCH_META_FORMAT,
                index_id: req.index_id,
                target: rebuild,
                after: None,
                done: false,
            })?,
        );
        self.commit_fjall_batch(batch)?;
        Ok(rebuild)
    }

    pub(crate) fn queue_search_rebuild(&self, scan: &RebuildScan) -> Result<RebuildPage> {
        let _queue = self.fts_queue_guard();
        let coverage = self
            .search_coverage()?
            .ok_or(StoreError::InvalidSearchState("search-coverage-missing"))?;
        let rebuild = coverage.rebuild.ok_or(StoreError::InvalidSearchState(
            "search-rebuild-not-required",
        ))?;
        if coverage.index_id != scan.index_id {
            return Err(StoreError::InvalidSearchState(
                "search-rebuild-index-mismatch",
            ));
        }
        let mut state = self
            .search_meta
            .get(SEARCH_REBUILD_KEY)?
            .map(|value| postcard::from_bytes::<SearchRebuildScan>(value.as_ref()))
            .transpose()?
            .ok_or(StoreError::InvalidSearchState(
                "search-rebuild-cursor-missing",
            ))?;
        if state.format != SEARCH_META_FORMAT
            || state.index_id != scan.index_id
            || state.target != rebuild
        {
            return Err(StoreError::InvalidSearchState(
                "search-rebuild-cursor-mismatch",
            ));
        }
        if state.done {
            return Ok(RebuildPage {
                remaining: false,
                rows: 0,
                bytes: 0,
                target: self.locked_dirty_token(),
                oversized: None,
            });
        }
        let lower = state.after.map_or_else(
            || Included(vec![GRAPH_META_PREFIX]),
            |graph| Excluded(graph_meta_key(graph).to_vec()),
        );
        let upper = Excluded(vec![GRAPH_META_PREFIX.saturating_add(1)]);
        let iterator = self.graphs.range((lower, upper));
        let mut batch = self.buffered_batch();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut highest = self.locked_dirty_token();
        let mut remaining = false;
        let mut oversized = None;
        for guard in iterator {
            let (key, value) = guard.into_inner()?;
            if rows == scan.row_limit {
                remaining = true;
                break;
            }
            let graph = decode_term_id(&key.as_ref()[1..], "graph meta key")?;
            let term_bytes = self
                .terms
                .size_of(graph.to_be_bytes())?
                .ok_or(StoreError::TermNotFound(graph.0))? as usize;
            let encoded = key
                .len()
                .saturating_add(value.len())
                .saturating_add(term_bytes);
            if bytes.saturating_add(encoded) > scan.byte_limit {
                if encoded > scan.byte_limit {
                    highest =
                        highest.max(self.stage_fts_entry(&mut batch, FtsQueueKey::Reindex(graph))?);
                    state.after = Some(graph);
                    rows += 1;
                    oversized = Some(OversizedSource {
                        bytes: encoded,
                        limit: scan.byte_limit,
                    });
                }
                remaining = true;
                break;
            }
            highest = highest.max(self.stage_fts_entry(&mut batch, FtsQueueKey::Reindex(graph))?);
            state.after = Some(graph);
            rows += 1;
            bytes = bytes.saturating_add(encoded);
        }
        if !remaining {
            state.done = true;
        }
        batch.insert(
            &self.search_meta,
            SEARCH_REBUILD_KEY,
            postcard::to_allocvec(&state)?,
        );
        batch.insert(&self.search_meta, SEARCH_HEAD_KEY, highest.to_be_bytes());
        self.commit_fjall_batch(batch)?;
        self.dirty_committed.fetch_max(highest, Ordering::SeqCst);
        Ok(RebuildPage {
            remaining,
            rows,
            bytes,
            target: highest,
            oversized,
        })
    }

    pub(crate) fn finish_search_rebuild(&self, coverage: &SearchCoverage) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let current = self
            .search_coverage()?
            .ok_or(StoreError::InvalidSearchState("search-coverage-missing"))?;
        if current.rebuild != coverage.rebuild
            || current.index_id != coverage.index_id
            || coverage.format != SEARCH_META_FORMAT
            || !coverage
                .rebuild
                .is_some_and(|rebuild| coverage.covered >= rebuild)
            || coverage.covered < current.covered
        {
            return Err(StoreError::InvalidSearchState(
                "search-rebuild-target-mismatch",
            ));
        }
        let enumeration = self
            .search_meta
            .get(SEARCH_REBUILD_KEY)?
            .map(|value| postcard::from_bytes::<SearchRebuildScan>(value.as_ref()))
            .transpose()?
            .ok_or(StoreError::InvalidSearchState(
                "search-rebuild-cursor-missing",
            ))?;
        if enumeration.format != SEARCH_META_FORMAT
            || enumeration.index_id != coverage.index_id
            || Some(enumeration.target) != coverage.rebuild
            || !enumeration.done
        {
            return Err(StoreError::InvalidSearchState("search-rebuild-incomplete"));
        }
        let manifest = self.search_manifest_digest(coverage.index_id)?;
        if manifest.epoch != coverage.manifest_epoch {
            return Err(StoreError::InvalidSearchState("search-manifest-changed"));
        }
        if let Some(head) = self.queue_head()?
            && head.token <= coverage.covered
        {
            return Err(StoreError::InvalidSearchState(
                "search-coverage-debt-remains",
            ));
        }
        let settled = SearchCoverage {
            rebuild: None,
            ..*coverage
        };
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            SEARCH_MANIFEST_KEY,
            postcard::to_allocvec(&StoredManifest {
                format: SEARCH_META_FORMAT,
                index_id: coverage.index_id,
                count: coverage.manifest_count,
                hash: coverage.manifest_hash,
                epoch: coverage.manifest_epoch,
            })?,
        );
        batch.insert(
            &self.search_meta,
            SEARCH_COVERAGE_KEY,
            encode_search_coverage(&settled)?,
        );
        batch.remove(&self.search_meta, SEARCH_REBUILD_KEY);
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn advance_search_coverage(&self, coverage: &SearchCoverage) -> Result<()> {
        let _queue = self.fts_queue_guard();
        if coverage.format != SEARCH_META_FORMAT {
            return Err(StoreError::UnsupportedSearchFormat {
                found: coverage.format,
                supported: SEARCH_META_FORMAT,
            });
        }
        let manifest = self.search_manifest_digest(coverage.index_id)?;
        if manifest.epoch != coverage.manifest_epoch
            || manifest.count != coverage.manifest_count
            || manifest.hash != coverage.manifest_hash
        {
            return Err(StoreError::InvalidSearchState("search-manifest-changed"));
        }
        if let Some(head) = self.queue_head()?
            && head.token <= coverage.covered
        {
            return Err(StoreError::InvalidSearchState(
                "search-coverage-debt-remains",
            ));
        }
        if let Some(current) = self.search_coverage()?
            && (current.rebuild.is_some()
                || (current.index_id != [0; 16]
                    && current.index_id != coverage.index_id
                    && current.rebuild != Some(coverage.covered))
                || coverage.covered < current.covered)
        {
            return Err(StoreError::InvalidSearchState("search-coverage-regression"));
        }
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.search_meta,
            SEARCH_COVERAGE_KEY,
            encode_search_coverage(coverage)?,
        );
        self.commit_fjall_batch(batch)
    }

    /// Acknowledges covered subjects while preserving later redirty events.
    pub(crate) fn acknowledge_fts_queue(&self, queued: &[DirtySubject]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for entry in queued {
            let Some(graph_id) = self.graph_id_for(&entry.graph)? else {
                continue;
            };
            dirty |= self.settle_fts_entry(
                &mut batch,
                AckedEntry {
                    key: graph_dirty_key(graph_id, entry.subject).to_vec(),
                    covered: entry.tokens.latest,
                },
            )?;
        }
        #[cfg(test)]
        self.stall_search_ack();
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub(crate) fn acknowledge_reindex(&self, queued: &[DirtyGraph]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for entry in queued {
            let Some(graph_id) = self.graph_id_for(&entry.graph)? else {
                continue;
            };
            dirty |= self.settle_fts_entry(
                &mut batch,
                AckedEntry {
                    key: graph_reindex_key(graph_id).to_vec(),
                    covered: entry.tokens.latest,
                },
            )?;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub(crate) fn acknowledge_deletes(&self, queued: &[DirtyGraph]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for entry in queued {
            let Some(graph_id) = self.graph_id_for(&entry.graph)? else {
                continue;
            };
            dirty |= self.settle_fts_entry(
                &mut batch,
                AckedEntry {
                    key: graph_delete_key(graph_id).to_vec(),
                    covered: entry.tokens.latest,
                },
            )?;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub(crate) fn acknowledge_reindexed(&self, queued: &[DirtyGraph]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for entry in queued {
            let Some(graph_id) = self.graph_id_for(&entry.graph)? else {
                continue;
            };
            for guard in self.graphs.prefix(graph_dirty_scope(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "graph dirty tokens")?;
                // `latest`: a subject dirtied past the reindex token is not
                // covered by the scan that token bounded.
                if tokens.latest <= entry.tokens.latest {
                    let cursor =
                        self.queue_cursor(QueueKind::Subject, key.as_ref(), value.as_ref())?;
                    batch.remove(&self.search_queue, search_order_key(cursor));
                    batch.remove(&self.search_meta, search_failure_key(cursor));
                    batch.remove(&self.graphs, key);
                    dirty = true;
                }
            }
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub(crate) fn acknowledge_deleted(&self, queued: &[DirtyGraph]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for entry in queued {
            let Some(graph_id) = self.graph_id_for(&entry.graph)? else {
                continue;
            };
            let delete_token = entry.tokens.latest;
            for guard in self.graphs.prefix(graph_dirty_scope(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "graph dirty tokens")?;
                if tokens.latest <= delete_token {
                    let cursor =
                        self.queue_cursor(QueueKind::Subject, key.as_ref(), value.as_ref())?;
                    batch.remove(&self.search_queue, search_order_key(cursor));
                    batch.remove(&self.search_meta, search_failure_key(cursor));
                    batch.remove(&self.graphs, key);
                    dirty = true;
                }
            }

            let reindex_key = graph_reindex_key(graph_id);
            if let Some(current) = self.graphs.get(reindex_key)? {
                let tokens = decode_dirty_tokens(current.as_ref(), "graph reindex tokens")?;
                if tokens.latest <= delete_token {
                    let cursor =
                        self.queue_cursor(QueueKind::Reindex, &reindex_key, current.as_ref())?;
                    batch.remove(&self.search_queue, search_order_key(cursor));
                    batch.remove(&self.search_meta, search_failure_key(cursor));
                    batch.remove(&self.graphs, reindex_key);
                    dirty = true;
                }
            }
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    /// Retires graph queue entries only through the scan's pinned token.
    pub fn clear_graph_queue(&self, graph: &GraphId, upto: u64) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for guard in self.graphs.prefix(graph_dirty_scope(graph_id)) {
            let (key, _) = guard.into_inner()?;
            keys.push(key.to_vec());
        }
        keys.push(graph_reindex_key(graph_id).to_vec());
        keys.push(graph_delete_key(graph_id).to_vec());

        let mut dirty = false;
        for key in keys {
            dirty |= self.settle_fts_entry(&mut batch, AckedEntry { key, covered: upto })?;
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub fn clear_fts_queue(&self) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for guard in self.graphs.prefix(graph_dirty_prefix()) {
            let (key, value) = guard.into_inner()?;
            let cursor = self.queue_cursor(QueueKind::Subject, key.as_ref(), value.as_ref())?;
            batch.remove(&self.search_queue, search_order_key(cursor));
            batch.remove(&self.search_meta, search_failure_key(cursor));
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        for guard in self.graphs.prefix(graph_reindex_prefix()) {
            let (key, value) = guard.into_inner()?;
            let cursor = self.queue_cursor(QueueKind::Reindex, key.as_ref(), value.as_ref())?;
            batch.remove(&self.search_queue, search_order_key(cursor));
            batch.remove(&self.search_meta, search_failure_key(cursor));
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        for guard in self.graphs.prefix(graph_delete_prefix()) {
            let (key, value) = guard.into_inner()?;
            let cursor = self.queue_cursor(QueueKind::Delete, key.as_ref(), value.as_ref())?;
            batch.remove(&self.search_queue, search_order_key(cursor));
            batch.remove(&self.search_meta, search_failure_key(cursor));
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    /// Makes room for one more receipt by removing the oldest past retention, sparing
    /// receipts staged in this batch. It scans forward from the last committed trim.
    fn trim_receipts(&self, batch: &mut WriteBatch) -> Result<()> {
        let excess = self.receipt_count.excess(&batch.receipt_trim.receipts);
        if excess == 0 {
            return Ok(());
        }
        let start = self
            .receipt_count
            .start(&batch.receipt_trim.receipts)
            .map_or(Bound::Unbounded, Bound::Excluded);
        let mut expired = None;
        let mut removed = 0usize;
        for guard in self
            .receipt_order
            .range::<Vec<u8>, _>((start, Bound::Unbounded))
        {
            let (key, id) = guard.into_inner()?;
            if key.len() != 40 {
                continue;
            }
            let staged = batch
                .pending_receipts
                .iter()
                .any(|receipt| receipt.id.0[..] == id[..]);
            if staged {
                continue;
            }
            if let Some(value) = self.receipts.get(&id)? {
                let receipt: MutationReceipt = postcard::from_bytes(value.as_ref())?;
                expired = expired.max(Some(receipt.admission_sequence));
            }
            batch.inner.remove(&self.receipt_order, key.clone());
            batch.inner.remove(&self.receipts, id);
            batch.receipt_trim.receipts.floor = Some(key.to_vec());
            removed += 1;
            if removed == excess {
                break;
            }
        }
        batch.receipt_trim.receipts.removed += removed;
        if let Some(expired) = expired {
            let stored = self
                .receipt_order
                .get(RECEIPT_EXPIRED_KEY)?
                .and_then(|value| decode_index_count(value.as_ref()))
                .unwrap_or(0);
            batch.inner.insert(
                &self.receipt_order,
                RECEIPT_EXPIRED_KEY,
                stored.max(expired).to_be_bytes(),
            );
        }
        Ok(())
    }

    /// Stage first acceptance evidence in the caller's source batch.
    pub(crate) fn stage_receipt(
        &self,
        batch: &mut WriteBatch,
        receipt: &MutationReceipt,
    ) -> Result<Option<MutationReceipt>> {
        if let Some(existing) = self.mutation_receipt(&receipt.id)? {
            if existing.graph != receipt.graph
                || existing.request_digest != receipt.request_digest
                || existing.event_id != receipt.event_id
                || existing.topic != receipt.topic
                || existing.topic_epoch != receipt.topic_epoch
                || existing.topic_genesis != receipt.topic_genesis
                || existing.publish_after != receipt.publish_after
                || existing.repair_graphs != receipt.repair_graphs
            {
                return Err(StoreError::ReceiptConflict);
            }
            return Ok(Some(existing));
        }
        let mut stored = receipt.clone();
        if stored.admission_sequence == 0 {
            stored.admission_sequence = self.receipt_next.fetch_add(1, Ordering::SeqCst);
        }
        #[cfg(test)]
        self.receipt_writes.fetch_add(1, Ordering::Relaxed);
        self.trim_receipts(batch)?;
        batch.receipt_trim.receipts.added += 1;
        batch
            .inner
            .insert(&self.receipts, stored.id.0, postcard::to_allocvec(&stored)?);
        batch
            .inner
            .insert(&self.receipt_order, receipt_order_key(&stored), stored.id.0);
        batch.pending_receipts.push(stored);
        Ok(None)
    }

    pub(crate) fn mutation_receipt(&self, id: &MutationId) -> Result<Option<MutationReceipt>> {
        #[cfg(test)]
        self.receipt_lookups.fetch_add(1, Ordering::Relaxed);
        self.receipts
            .get(id.0)?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(StoreError::from))
            .transpose()
    }

    pub(crate) fn bind_receipt_event(
        &self,
        id: &MutationId,
        event_id: [u8; 32],
    ) -> Result<MutationReceipt> {
        let _guard = self.receipt_guard(id);
        let mut receipt = self
            .mutation_receipt(id)?
            .ok_or(StoreError::ReceiptConflict)?;
        if receipt.source != SourceOutcome::Prepared
            || receipt.event_id.is_some_and(|current| current != event_id)
        {
            return Err(StoreError::ReceiptConflict);
        }
        receipt.event_id = Some(event_id);
        #[cfg(test)]
        self.receipt_writes.fetch_add(1, Ordering::Relaxed);
        let mut batch = self.buffered_batch();
        batch.insert(
            &self.receipts,
            receipt.id.0,
            postcard::to_allocvec(&receipt)?,
        );
        self.commit_fjall_batch(batch)?;
        Ok(receipt)
    }

    pub(crate) fn receipt_status(&self, lookup: &MutationLookup) -> Result<MutationStatus> {
        if let Some(receipt) = self.mutation_receipt(&lookup.id)? {
            if receipt.graph != lookup.graph
                || lookup
                    .admission_sequence
                    .is_some_and(|sequence| sequence != receipt.admission_sequence)
            {
                return Ok(MutationStatus::Unknown);
            }
            return Ok(MutationStatus::Known(Box::new(receipt)));
        }
        let expired = self
            .receipt_order
            .get(RECEIPT_EXPIRED_KEY)?
            .and_then(|value| decode_index_count(value.as_ref()))
            .unwrap_or(0);
        Ok(match lookup.admission_sequence {
            Some(sequence) if sequence <= expired => MutationStatus::Expired,
            Some(_) | None => MutationStatus::Unknown,
        })
    }

    /// Makes room for one more batch receipt by removing the oldest past retention.
    fn trim_batch_receipts(&self, batch: &mut WriteBatch) -> Result<()> {
        let change = &batch.receipt_trim.batches;
        let excess = self.batch_receipt_count.excess(change);
        if excess == 0 {
            return Ok(());
        }
        let start = self.batch_receipt_count.start(change).map_or(
            Bound::Included(BATCH_ORDER_PREFIX.to_vec()),
            Bound::Excluded,
        );
        let mut removed = 0usize;
        for guard in self
            .receipt_order
            .range::<Vec<u8>, _>((start, Bound::Unbounded))
        {
            let (order, mapping_key) = guard.into_inner()?;
            if !order.starts_with(&BATCH_ORDER_PREFIX) {
                break;
            }
            if let Some(value) = self.receipts.get(&mapping_key)? {
                let mapping: StoredBatchReceipt = postcard::from_bytes(value.as_ref())?;
                batch
                    .inner
                    .remove(&self.receipts, batch_reverse_key(&mapping.id));
            }
            batch.inner.remove(&self.receipt_order, order.clone());
            batch.inner.remove(&self.receipts, mapping_key);
            batch.receipt_trim.batches.floor = Some(order.to_vec());
            removed += 1;
            if removed == excess {
                break;
            }
        }
        batch.receipt_trim.batches.removed += removed;
        Ok(())
    }

    pub(crate) fn stage_batch_receipt(
        &self,
        batch: &mut WriteBatch,
        link: &BatchReceiptLink,
    ) -> Result<()> {
        let receipt = batch
            .pending_receipts
            .iter()
            .find(|receipt| receipt.id == link.id)
            .cloned()
            .or_else(|| self.mutation_receipt(&link.id).ok().flatten())
            .ok_or(StoreError::ReceiptConflict)?;
        let mapping = StoredBatchReceipt {
            id: link.id,
            sequence: receipt.admission_sequence,
        };
        let key = batch_receipt_key(link);
        if let Some(value) = self.receipts.get(key)? {
            let existing: StoredBatchReceipt = postcard::from_bytes(value.as_ref())?;
            if existing.id != mapping.id || existing.sequence != mapping.sequence {
                return Err(StoreError::ReceiptConflict);
            }
            return Ok(());
        }
        if let Some(reverse) = self.receipts.get(batch_reverse_key(&link.id))?
            && reverse.as_ref() != key
        {
            return Err(StoreError::ReceiptConflict);
        }
        self.trim_batch_receipts(batch)?;
        batch.receipt_trim.batches.added += 1;
        batch
            .inner
            .insert(&self.receipts, key, postcard::to_allocvec(&mapping)?);
        batch
            .inner
            .insert(&self.receipts, batch_reverse_key(&link.id), key);
        batch
            .inner
            .insert(&self.receipt_order, batch_order_key(mapping.sequence), key);
        Ok(())
    }

    pub(crate) fn receipt_for_batch(&self, batch: &Batch) -> Result<MutationStatus> {
        let graph = hash_term(&EncodedTerm::from_named_node(&batch.graph.0));
        let link = BatchReceiptLink {
            graph,
            actor: batch.actor,
            counter: batch.counter,
            id: MutationId([0; 32]),
        };
        let Some(value) = self.receipts.get(batch_receipt_key(&link))? else {
            return Ok(MutationStatus::Unknown);
        };
        let mapping: StoredBatchReceipt = postcard::from_bytes(value.as_ref())?;
        if let Some(receipt) = self.mutation_receipt(&mapping.id)? {
            return Ok(MutationStatus::Known(Box::new(receipt)));
        }
        let expired = self
            .receipt_order
            .get(RECEIPT_EXPIRED_KEY)?
            .and_then(|value| decode_index_count(value.as_ref()))
            .unwrap_or(0);
        Ok(if mapping.sequence <= expired {
            MutationStatus::Expired
        } else {
            MutationStatus::Unknown
        })
    }

    pub(crate) fn query_view_covered(&self, receipt: &MutationReceipt) -> Result<bool> {
        if !matches!(
            receipt.source,
            SourceOutcome::Applied | SourceOutcome::Duplicate
        ) {
            return Ok(false);
        }
        Ok(self
            .snapshot_admission(self.read_snapshot().snapshot_ref())?
            .trusted)
    }

    pub(crate) fn search_covered(&self, receipt: &MutationReceipt) -> Result<bool> {
        let Some(target) = receipt.search_token else {
            return Ok(matches!(receipt.repairs.search, RepairOutcome::NotRequired));
        };
        Ok(self
            .search_coverage()?
            .is_some_and(|coverage| coverage.rebuild.is_none() && coverage.covered >= target))
    }

    /// Stage the Prepared to Applied transition in the source mutation batch.
    pub(crate) fn stage_receipt_update(
        &self,
        batch: &mut WriteBatch,
        receipt: &MutationReceipt,
    ) -> Result<()> {
        let previous = self
            .mutation_receipt(&receipt.id)?
            .ok_or(StoreError::ReceiptConflict)?;
        if previous.graph != receipt.graph
            || previous.request_digest != receipt.request_digest
            || previous.event_id != receipt.event_id
            || previous.topic != receipt.topic
            || previous.topic_epoch != receipt.topic_epoch
            || previous.topic_genesis != receipt.topic_genesis
            || previous.publish_after != receipt.publish_after
            || previous.repair_graphs != receipt.repair_graphs
            || previous.source != SourceOutcome::Prepared
        {
            return Err(StoreError::ReceiptConflict);
        }
        let mut stored = receipt.clone();
        if stored.admission_sequence == 0 {
            stored.admission_sequence = previous.admission_sequence;
        }
        if stored.admission_sequence != previous.admission_sequence {
            return Err(StoreError::ReceiptConflict);
        }
        #[cfg(test)]
        self.receipt_writes.fetch_add(1, Ordering::Relaxed);
        if receipt_order_key(&previous) != receipt_order_key(&stored) {
            batch
                .inner
                .remove(&self.receipt_order, receipt_order_key(&previous));
        }
        batch
            .inner
            .insert(&self.receipts, stored.id.0, postcard::to_allocvec(&stored)?);
        batch
            .inner
            .insert(&self.receipt_order, receipt_order_key(&stored), stored.id.0);
        batch.pending_receipts.push(stored);
        Ok(())
    }

    /// Compare-fenced update for post-commit persistence and repair settlement.
    pub(crate) fn update_receipt(&self, receipt: &MutationReceipt) -> Result<MutationReceipt> {
        let _guard = self.receipt_guard(&receipt.id);
        let previous = self
            .mutation_receipt(&receipt.id)?
            .ok_or(StoreError::ReceiptConflict)?;
        let mut stored = receipt.clone();
        if stored.admission_sequence == 0 {
            stored.admission_sequence = previous.admission_sequence;
        }
        if previous.graph != stored.graph
            || previous.request_digest != receipt.request_digest
            || previous.event_id != receipt.event_id
            || previous.topic != receipt.topic
            || previous.topic_epoch != receipt.topic_epoch
            || previous.topic_genesis != receipt.topic_genesis
            || previous.publish_after != receipt.publish_after
            || previous.repair_graphs != receipt.repair_graphs
            || previous.source_version != receipt.source_version
            || previous.admission_sequence != stored.admission_sequence
        {
            return Err(StoreError::ReceiptConflict);
        }
        let mut batch = self.buffered_batch();
        if receipt_order_key(&previous) != receipt_order_key(&stored) {
            batch.remove(&self.receipt_order, receipt_order_key(&previous));
        }
        batch.insert(&self.receipts, stored.id.0, postcard::to_allocvec(&stored)?);
        batch.insert(&self.receipt_order, receipt_order_key(&stored), stored.id.0);
        #[cfg(test)]
        self.receipt_writes.fetch_add(1, Ordering::Relaxed);
        self.commit_fjall_batch(batch)?;
        Ok(stored)
    }

    pub(crate) fn stage_repair_audit(
        &self,
        batch: &mut WriteBatch,
        audit: &RepairAudit,
    ) -> Result<()> {
        if let Some(existing) = self.repair_audit(&audit.id)?
            && existing != *audit
        {
            return Err(StoreError::ReceiptConflict);
        }
        batch.inner.insert(
            &self.repair_audits,
            audit.id.0,
            postcard::to_allocvec(audit)?,
        );
        Ok(())
    }

    pub(crate) fn repair_audit(&self, id: &MutationId) -> Result<Option<RepairAudit>> {
        self.repair_audits
            .get(id.0)?
            .map(|value| postcard::from_bytes(value.as_ref()).map_err(StoreError::from))
            .transpose()
    }

    pub fn new_batch(&self) -> WriteBatch {
        WriteBatch::new(self.buffered_batch())
    }

    fn buffered_batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(Some(PersistMode::Buffer))
    }

    pub fn persist(&self) -> Result<()> {
        self.db.persist(self.persist_mode)?;
        #[cfg(test)]
        self.persists.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Explicit persists run so far. Test-only.
    #[cfg(test)]
    pub(crate) fn persists(&self) -> u64 {
        self.persists.load(Ordering::Relaxed)
    }

    fn commit_fjall_batch(&self, batch: fjall::OwnedWriteBatch) -> Result<()> {
        #[cfg(test)]
        if self.take_commit_failure() {
            return Err(StoreError::Fjall(fjall::Error::Io(std::io::Error::other(
                "injected commit failure",
            ))));
        }
        batch.commit()?;
        Ok(())
    }

    /// Retires covered queue debt while preserving later tokens under the queue lock.
    fn settle_fts_entry(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        entry: AckedEntry,
    ) -> Result<bool> {
        let Some(current) = self.graphs.get(&entry.key)? else {
            return Ok(false);
        };
        let stored = decode_dirty_tokens(current.as_ref(), "fts queue tokens")?;
        let kind = match entry.key.first().copied() {
            Some(GRAPH_DIRTY_PREFIX) => QueueKind::Subject,
            Some(GRAPH_REINDEX_PREFIX) => QueueKind::Reindex,
            Some(GRAPH_DELETE_PREFIX) => QueueKind::Delete,
            _ => {
                return Err(StoreError::InvalidSearchState(
                    "queue-identity-prefix-invalid",
                ));
            }
        };
        let old_cursor = self.queue_cursor(kind, &entry.key, current.as_ref())?;
        batch.remove(&self.search_queue, search_order_key(old_cursor));
        if stored.latest <= entry.covered {
            batch.remove(&self.graphs, entry.key.clone());
            batch.remove(&self.search_meta, search_failure_key(old_cursor));
        } else {
            let narrowed = DirtyTokens {
                oldest: entry.covered + 1,
                latest: stored.latest,
            };
            batch.insert(
                &self.graphs,
                entry.key.clone(),
                encode_dirty_tokens(narrowed),
            );
            batch.insert(
                &self.search_queue,
                search_order_key(QueueCursor {
                    token: narrowed.oldest,
                    ..old_cursor
                }),
                entry.key,
            );
            if let Some(failure) = self.search_meta.get(search_failure_key(old_cursor))?
                && postcard::from_bytes::<RetryState>(failure.as_ref())
                    .is_ok_and(|state| state.target != narrowed.latest)
            {
                batch.remove(&self.search_meta, search_failure_key(old_cursor));
            }
        }
        Ok(true)
    }

    /// Oldest queued search work. Callers hold the queue lock, under which tokens are
    /// minted in increasing order, so no work can appear below a head once seen.
    fn queue_head(&self) -> Result<Option<QueueCursor>> {
        let floor = self.queue_floor.load(Ordering::SeqCst);
        let mut start = [0u8; 9];
        start[0] = SEARCH_ORDER_PREFIX;
        start[1..].copy_from_slice(&floor.to_be_bytes());
        let head = match self
            .search_queue
            .range(start.to_vec()..vec![SEARCH_ORDER_PREFIX + 1])
            .next()
        {
            Some(guard) => Some(decode_search_order(guard.into_inner()?.0.as_ref())?),
            None => None,
        };
        let next = head.map_or_else(
            || self.dirty_counter.load(Ordering::SeqCst),
            |head| head.token,
        );
        self.queue_floor.fetch_max(next, Ordering::SeqCst);
        Ok(head)
    }

    /// Take the FTS queue lock, recovering from poison: the state it guards
    /// lives in fjall, not behind the mutex.
    fn fts_queue_guard(&self) -> MutexGuard<'_, ()> {
        self.fts_queue_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Coalesces a newly tokenized entry while the caller holds the queue lock.
    fn stage_fts_entry(&self, batch: &mut fjall::OwnedWriteBatch, key: FtsQueueKey) -> Result<u64> {
        let token = self
            .dirty_counter
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map_err(|_| StoreError::InvalidSearchState("search-token-exhausted"))?;
        let bytes = key.bytes();
        let tokens = match self.graphs.get(&bytes)? {
            Some(current) => {
                let stored = decode_dirty_tokens(current.as_ref(), "fts queue tokens")?;
                batch.remove(
                    &self.search_queue,
                    search_order_key(key.cursor(stored.oldest)),
                );
                DirtyTokens {
                    oldest: stored.oldest.min(token),
                    latest: stored.latest.max(token),
                }
            }
            None => DirtyTokens {
                oldest: token,
                latest: token,
            },
        };
        batch.insert(&self.graphs, bytes, encode_dirty_tokens(tokens));
        batch.insert(
            &self.search_queue,
            search_order_key(key.cursor(tokens.oldest)),
            key.bytes(),
        );
        Ok(token)
    }

    /// Removes older graph work atomically before publishing its delete token.
    fn stage_delete_queue(&self, batch: &mut fjall::OwnedWriteBatch, graph: TermId) -> Result<()> {
        for guard in self.graphs.prefix(graph_dirty_scope(graph)) {
            let (key, value) = guard.into_inner()?;
            let cursor = self.queue_cursor(QueueKind::Subject, key.as_ref(), value.as_ref())?;
            batch.remove(&self.search_queue, search_order_key(cursor));
            batch.remove(&self.search_meta, search_failure_key(cursor));
            batch.remove(&self.graphs, key);
        }
        let key = graph_reindex_key(graph);
        if let Some(value) = self.graphs.get(key)? {
            let cursor = self.queue_cursor(QueueKind::Reindex, &key, value.as_ref())?;
            batch.remove(&self.search_queue, search_order_key(cursor));
            batch.remove(&self.search_meta, search_failure_key(cursor));
            batch.remove(&self.graphs, key);
        }
        Ok(())
    }

    /// Publish a batch together with the queue entries it owes.
    fn commit_durable(&self, commit: DurableCommit) -> Result<()> {
        let DurableCommit {
            mut batch,
            pending_fts,
            mut pending_receipts,
            receipt_trim,
        } = commit;
        let _queue = self.fts_queue_guard();
        let mut search_token = None;
        for key in pending_fts.order {
            if let FtsQueueKey::Delete(graph) = key {
                self.stage_delete_queue(&mut batch, graph)?;
            }
            let token = self.stage_fts_entry(&mut batch, key)?;
            search_token = Some(search_token.map_or(token, |current: u64| current.max(token)));
        }
        if let Some(search_token) = search_token {
            for receipt in &mut pending_receipts {
                receipt.search_token = Some(search_token);
                batch.insert(
                    &self.receipts,
                    receipt.id.0,
                    postcard::to_allocvec(receipt)?,
                );
            }
            batch.insert(
                &self.search_meta,
                SEARCH_HEAD_KEY,
                search_token.to_be_bytes(),
            );
        }
        let result = self.commit_fjall_batch(batch);
        if result.is_ok() {
            self.receipt_count.settle(receipt_trim.receipts);
            self.batch_receipt_count.settle(receipt_trim.batches);
            if let Some(token) = search_token {
                self.dirty_committed.fetch_max(token, Ordering::SeqCst);
            }
        }
        result
    }

    pub fn commit(&self, batch: WriteBatch) -> Result<()> {
        let WriteBatch {
            inner,
            pending_quad_states: _,
            pending_terms: _,
            publish,
            pending_fts,
            pending_receipts,
            receipt_trim,
        } = batch;
        let commit = DurableCommit {
            batch: inner,
            pending_fts,
            pending_receipts,
            receipt_trim,
        };
        self.apply_commit(commit, publish)
    }

    /// Copy a subject's live `(predicate, object)` ids from the bounded cache
    /// or the durable GSPO prefix when the cache generation is stale/missing.
    fn subject_entries(
        &self,
        key: (TermId, TermId),
        excluded: Option<TermId>,
    ) -> Result<Vec<(TermId, TermId)>> {
        let (graph, subject) = key;
        let generation = self.indexes_read().graph_epoch(graph);
        let cache_key = (graph, subject, generation);
        let entries =
            if let Some(entries) = self.indexes_write().quad_subjects.get_cloned(&cache_key) {
                entries
            } else {
                let mut prefix = [0u8; 32];
                prefix[..16].copy_from_slice(&graph.to_be_bytes());
                prefix[16..].copy_from_slice(&subject.to_be_bytes());
                let mut entries = Vec::new();
                for guard in self.quads.prefix(prefix) {
                    let (quad_key, value) = guard.into_inner()?;
                    if dots_empty(value.as_ref()) {
                        continue;
                    }
                    let quad = Self::decode_quad_key(quad_key.as_ref())?;
                    entries.push((quad.predicate, quad.object));
                }
                let entries = Arc::new(entries);
                let mut indexes = self.indexes_write();
                if indexes.graph_epoch(graph) == generation {
                    indexes.quad_subjects.insert(
                        cache_key,
                        Arc::clone(&entries),
                        entries
                            .len()
                            .saturating_mul(2 * std::mem::size_of::<TermId>()),
                    );
                }
                entries
            };
        Ok(entries
            .iter()
            .copied()
            .filter(|(predicate, _)| Some(*predicate) != excluded)
            .collect())
    }

    fn decode_entries(
        &self,
        entries: Vec<(TermId, TermId)>,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        entries
            .into_iter()
            .map(|(predicate, object)| {
                Ok((
                    self.decode_term_arc(predicate)?.as_ref().clone(),
                    self.decode_term_arc(object)?.as_ref().clone(),
                ))
            })
            .collect()
    }

    pub fn triples_for_subject(
        &self,
        graph: TermId,
        subject: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.decode_entries(self.subject_entries((graph, subject), None)?)
    }

    pub fn triples_excluding_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        excluded_predicate: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.decode_entries(self.subject_entries((graph, subject), Some(excluded_predicate))?)
    }

    pub fn count_matching_objects(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
    ) -> Result<usize> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(0);
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(0);
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok(0);
        };
        self.count_object_ids(graph_id, subject_id, predicate_id)
    }

    /// Returns total objects and one decoded-term-ordered page.
    pub fn objects_page(
        &self,
        key: GraphSubjectPredicate<'_>,
        page: PageRequest<'_>,
    ) -> Result<(usize, Vec<EncodedTerm>)> {
        if page.limit == 0 {
            return Ok((0, Vec::new()));
        }

        let Some(graph_id) = self.graph_id_for(key.graph)? else {
            return Ok((0, Vec::new()));
        };
        let Some(subject_id) = self.lookup_term(key.subject)? else {
            return Ok((0, Vec::new()));
        };
        let Some(predicate_id) = self.lookup_term(key.predicate)? else {
            return Ok((0, Vec::new()));
        };

        let object_ids = self.ordered_objects(graph_id, subject_id, predicate_id)?;
        let total = object_ids.len();

        let start = match page.cursor {
            PageCursor::Offset(offset) => offset,
            // An unknown or dropped cursor term restarts from the beginning,
            // which is what the previous `_after` entry point did.
            PageCursor::After(None) => 0,
            PageCursor::After(Some(after)) => match self.lookup_term(after)? {
                Some(after_id) => object_ids
                    .iter()
                    .position(|object| *object == after_id)
                    .map(|index| index + 1)
                    .unwrap_or(0),
                None => 0,
            },
        };

        let objects = object_ids
            .iter()
            .skip(start)
            .take(page.limit)
            .map(|object| Ok(self.decode_term_arc(*object)?.as_ref().clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok((total, objects))
    }

    /// Test-only hook that corrupts one bounded subject-cache entry without
    /// touching durable source state.
    #[cfg(test)]
    fn corrupt_test_index(&self, quad: EncodedQuad) {
        let mut entries = self
            .subject_entries((quad.graph, quad.subject), None)
            .unwrap();
        entries.retain(|entry| *entry != (quad.predicate, quad.object));
        let mut indexes = self.indexes_write();
        let generation = indexes.graph_epoch(quad.graph);
        let entries = Arc::new(entries);
        indexes.quad_subjects.insert(
            (quad.graph, quad.subject, generation),
            Arc::clone(&entries),
            entries
                .len()
                .saturating_mul(2 * std::mem::size_of::<TermId>()),
        );
    }

    #[cfg(test)]
    fn index_contains(&self, quad: EncodedQuad) -> bool {
        self.subject_entries((quad.graph, quad.subject), None)
            .unwrap()
            .contains(&(quad.predicate, quad.object))
    }
}

#[cfg(test)]
#[path = "store_bench.rs"]
mod store_bench;

#[cfg(test)]
#[path = "store_recovery.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::context::{QueryCost, QueryReadMode, ReadContext};
    use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
    use std::os::unix::fs::PermissionsExt;

    fn setup_store() -> (tempfile::TempDir, GraphStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        (dir, store)
    }

    /// Resumed pages seek past the cursor and stop at the prefix end, even for 0xff bytes.
    #[test]
    fn resume_range_bounds() {
        assert_eq!(
            resume_range(&[3, 0xff], Some(&[3, 0xff, 7])),
            (Excluded(vec![3, 0xff, 7]), Excluded(vec![4]))
        );
        assert_eq!(
            resume_range(&[0xff], None),
            (Included(vec![0xff]), Unbounded)
        );
        assert_eq!(resume_range(&[], None), (Included(Vec::new()), Unbounded));
    }

    fn pending_receipt(graph: &GraphId, nanos: usize) -> MutationReceipt {
        MutationReceipt {
            id: MutationId::new(),
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
            source: SourceOutcome::Prepared,
            persistence: crate::sync::PersistenceOutcome::Pending,
            repairs: crate::sync::RepairState {
                diagnostics: RepairOutcome::Pending,
                shacl: RepairOutcome::Pending,
                search: RepairOutcome::Pending,
                query_view: RepairOutcome::Pending,
            },
            source_version: [0; 32],
            updated_unix_nanos: i64::try_from(nanos).unwrap(),
        }
    }

    fn commit_receipt(store: &GraphStore, graph: &GraphId, nanos: usize) -> MutationReceipt {
        let receipt = pending_receipt(graph, nanos);
        let mut batch = store.new_batch();
        assert!(store.stage_receipt(&mut batch, &receipt).unwrap().is_none());
        store.commit(batch).unwrap();
        store.mutation_receipt(&receipt.id).unwrap().unwrap()
    }

    /// A trim in a batch that never commits changes neither the count nor the trim
    /// position, and a reopened store still retains exactly the newest receipts.
    #[test]
    fn dropped_trim_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:receipt-dropped");
        let store = GraphStore::open(dir.path()).unwrap();
        let first = commit_receipt(&store, &graph, 0);
        let second = commit_receipt(&store, &graph, 1);
        let third = commit_receipt(&store, &graph, 2);
        for nanos in 3..RECEIPT_RETENTION {
            commit_receipt(&store, &graph, nanos);
        }
        let mut dropped = store.new_batch();
        store
            .stage_receipt(&mut dropped, &pending_receipt(&graph, RECEIPT_RETENTION))
            .unwrap();
        drop(dropped);
        assert!(store.mutation_receipt(&first.id).unwrap().is_some());

        commit_receipt(&store, &graph, RECEIPT_RETENTION + 1);
        assert!(store.mutation_receipt(&first.id).unwrap().is_none());
        assert!(store.mutation_receipt(&second.id).unwrap().is_some());
        drop(store);

        let store = GraphStore::open(dir.path()).unwrap();
        commit_receipt(&store, &graph, RECEIPT_RETENTION + 2);
        assert!(store.mutation_receipt(&second.id).unwrap().is_none());
        assert!(store.mutation_receipt(&third.id).unwrap().is_some());
    }

    /// Receipts that never settle still leave retention; reopen orders untracked ones again.
    #[test]
    fn pending_receipts_expire() {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:receipt-retention");
        let stage = |nanos: usize| commit_receipt(&store, &graph, nanos);
        let first = stage(0);
        let second = stage(1);
        for nanos in 2..=RECEIPT_RETENTION {
            stage(nanos);
        }
        let lookup = |receipt: &MutationReceipt| MutationLookup {
            graph: graph.clone(),
            id: receipt.id,
            admission_sequence: Some(receipt.admission_sequence),
        };
        assert!(store.mutation_receipt(&first.id).unwrap().is_none());
        assert!(matches!(
            store.receipt_status(&lookup(&first)).unwrap(),
            MutationStatus::Expired
        ));
        assert!(matches!(
            store.receipt_status(&lookup(&second)).unwrap(),
            MutationStatus::Known(_)
        ));

        let order = receipt_order_key(&second);
        store.receipt_order.remove(order).unwrap();
        drop(store);
        let reopened = GraphStore::open(dir.path()).unwrap();
        assert!(reopened.receipt_order.contains_key(order).unwrap());
    }

    /// Remembered graph records belong to one snapshot; later writes stay invisible to it.
    #[test]
    fn snapshot_records_isolated() {
        let (_dir, store) = setup_store();
        let kept = GraphId::new("urn:test:memo-kept");
        let deleted = GraphId::new("urn:test:memo-deleted");
        store.create_graph(&kept).unwrap();
        store.create_graph(&deleted).unwrap();
        let before = store.read_snapshot();
        let id = |graph: &GraphId| {
            before
                .lookup_term(&store, &EncodedTerm::from_named_node(&graph.0))
                .unwrap()
                .unwrap()
        };
        let original = before.graph_policy(&store, &kept).unwrap().unwrap();
        assert!(before.contains_graph_id(&store, id(&deleted)).unwrap());
        let changed = GraphPolicy {
            public: !original.public,
            ..original.clone()
        };
        store.set_graph_policy(&kept, &changed).unwrap();
        store.delete_graph(&deleted).unwrap();

        assert_eq!(before.graph_policy(&store, &kept).unwrap(), Some(original));
        assert!(before.contains_graph_id(&store, id(&deleted)).unwrap());
        let after = store.read_snapshot();
        assert_eq!(after.graph_policy(&store, &kept).unwrap(), Some(changed));
        assert!(!after.contains_graph_id(&store, id(&deleted)).unwrap());
    }

    /// Range-read graph records answer exactly like point reads; a refused prefetch keeps
    /// nothing, and later graphs stay invisible to the older snapshot.
    #[test]
    fn prefetched_records_exact() {
        let (_dir, store) = setup_store();
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let media = "http://schema.org/MediaObject";
        let graphs: Vec<GraphId> = (0..3)
            .map(|index| GraphId::new(&format!("urn:test:prefetch-{index}")))
            .collect();
        for graph in &graphs {
            store.create_graph(graph).unwrap();
        }
        for graph in &graphs[..2] {
            let quad = encode_quad(&store, graph, ("urn:e:loose", rdf_type, media));
            commit_add(&store, graph, quad);
        }
        let diagnostics = store.compute_graph_diagnostics(&graphs[0]).unwrap();
        store
            .set_graph_diagnostics(&graphs[0], &diagnostics)
            .unwrap();
        let point = store.read_snapshot();
        let ranged = store.read_snapshot();
        assert!(!ranged.prefetch_graph_records(&store, 2).unwrap());
        assert!(ranged.prefetch_graph_records(&store, 3).unwrap());
        let late = GraphId::new("urn:test:prefetch-late");
        store.create_graph(&late).unwrap();

        let context = ReadContext::new(crate::query::context::QueryCancellation::new());
        for graph in graphs.iter().chain([&late]) {
            let id = store
                .lookup_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap()
                .unwrap();
            assert_eq!(
                point.contains_graph_id(&store, id).unwrap(),
                ranged.contains_graph_id(&store, id).unwrap()
            );
            assert_eq!(
                point.graph_policy(&store, graph).unwrap(),
                ranged.graph_policy(&store, graph).unwrap()
            );
            let orphans = point.orphaned_entity_ids(&store, &context, id).unwrap();
            assert_eq!(
                orphans,
                ranged.orphaned_entity_ids(&store, &context, id).unwrap()
            );
            assert_eq!(orphans.is_empty(), !graphs[..2].contains(graph));
        }
        assert!(!ranged.contains_graph_id(&store, TermId(u128::MAX)).unwrap());
    }

    fn seed_graph_record(path: &Path, key: &[u8], value: &[u8]) {
        let db = Database::builder(path).open().unwrap();
        let graphs = db
            .keyspace("graphs", KeyspaceCreateOptions::default)
            .unwrap();
        let mut batch = db.batch();
        batch.insert(&graphs, key, value);
        batch.commit().unwrap();
        db.persist(PersistMode::SyncAll).unwrap();
    }

    #[test]
    fn keyspace_names_stable() {
        assert_eq!(PRIMARY_TERM_MAP, "qv2_term_to_query");
        assert_eq!(PRIMARY_QUERY_MAP, "qv2_query_to_term");
        assert_eq!(SECONDARY_TERM_MAP, "qv3_term_to_query");
        assert_eq!(SECONDARY_QUERY_MAP, "qv3_query_to_term");
    }

    #[cfg(feature = "search")]
    #[test]
    fn rebuild_target_stable() {
        let (_dir, store) = setup_store();
        let first = store.require_search_rebuild().unwrap();
        assert_eq!(store.require_search_rebuild().unwrap(), first);
    }

    #[test]
    fn format_marker_reopens() {
        let dir = tempfile::tempdir().unwrap();
        GraphStore::open(dir.path()).unwrap().persist().unwrap();
        GraphStore::open(dir.path()).unwrap();
    }

    #[test]
    fn future_format_rejected() {
        let dir = tempfile::tempdir().unwrap();
        seed_graph_record(
            dir.path(),
            DISK_FORMAT_KEY,
            &encode_disk_format(DiskFormatVersion::new(DISK_FORMAT_VERSION.major + 1, 0)),
        );

        assert!(matches!(
            GraphStore::open(dir.path()),
            Err(StoreError::UnsupportedAuthoritativeFormat { .. })
        ));
    }

    #[test]
    fn malformed_format_rejected() {
        let dir = tempfile::tempdir().unwrap();
        seed_graph_record(dir.path(), DISK_FORMAT_KEY, &[1, 2, 3]);

        assert!(matches!(
            GraphStore::open(dir.path()),
            Err(StoreError::InvalidAuthoritativeFormat)
        ));
    }

    #[test]
    fn unmarked_source_rejected() {
        let dir = tempfile::tempdir().unwrap();
        seed_graph_record(dir.path(), b"Mlegacy", b"value");

        assert!(matches!(
            GraphStore::open(dir.path()),
            Err(StoreError::MissingAuthoritativeFormat)
        ));
    }

    #[cfg(feature = "shacl-core")]
    fn stage_binding_records(store: &GraphStore, start: usize, end: usize) {
        let data = GraphId::new("urn:test:queue-scale:data");
        if start == 0 {
            store.create_graph(&data).unwrap();
        }
        let mut graph_batch = store.new_batch();
        for index in start..end {
            store
                .stage_graph(
                    &mut graph_batch,
                    &GraphId::new(&format!("urn:test:queue-scale:shapes:{index}")),
                )
                .unwrap();
        }
        store.commit(graph_batch).unwrap();

        let data_version = store.graph_version_digest(&data).unwrap();
        let mut binding_batch = store.new_batch();
        for index in start..end {
            let shapes = GraphId::new(&format!("urn:test:queue-scale:shapes:{index}"));
            let shapes_version = store.graph_version_digest(&shapes).unwrap();
            store
                .stage_binding_status(
                    &mut binding_batch,
                    &crate::ShaclBindingStatus {
                        binding: crate::ShaclBinding {
                            data_graph: data.clone(),
                            shapes_graph: shapes.clone(),
                            policy: crate::ShaclWritePolicy::Advisory,
                            validation_options: crate::ShaclBindingOptions::default(),
                        },
                        state: crate::ShaclValidationState::Valid,
                        report: Some(crate::ShaclValidationReport {
                            conforms: true,
                            accepted_by_write_policy: true,
                            results: Vec::new(),
                            statistics: crate::ShaclValidationStatistics::default(),
                        }),
                        error: None,
                        data_version,
                        shapes_version,
                        schema_fingerprint: [index as u8; 32],
                        compiler_model_version: crate::SHACL_COMPILER_MODEL_VERSION,
                        shape_versions: vec![(shapes, shapes_version)],
                    },
                )
                .unwrap();
        }
        store.commit(binding_batch).unwrap();
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn pending_scan_bounded() {
        let (_dir, store) = setup_store();
        let mut previous = 0;
        for count in [0, 100, 1_000, 10_000] {
            stage_binding_records(&store, previous, count);
            let started = Instant::now();
            let scan = store.bounded_shacl_queue(usize::MAX, None).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(scan.entries_scanned, 0, "binding count {count}");
            assert!(scan.graphs.is_empty(), "binding count {count}");
            assert!(!scan.budget_exhausted, "binding count {count}");
            eprintln!("binding_records={count} pending_queue_scan={elapsed:?}");
            previous = count;
        }

        let data = GraphId::new("urn:test:queue-scale:data");
        let shapes = GraphId::new("urn:test:queue-scale:shapes:9999");
        let mut status = store
            .shacl_binding_statuses(&data)
            .unwrap()
            .into_iter()
            .find(|status| status.binding.shapes_graph == shapes)
            .unwrap();
        status.state = crate::ShaclValidationState::Pending;
        status.report = None;
        let mut batch = store.new_batch();
        store.stage_binding_pending(&mut batch, &status).unwrap();
        store.commit(batch).unwrap();
        let scan = store.bounded_shacl_queue(usize::MAX, None).unwrap();
        assert_eq!(scan.entries_scanned, 1);
        assert_eq!(scan.graphs, vec![data]);
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn repair_restores_queue() {
        let (_dir, store) = setup_store();
        stage_binding_records(&store, 0, 100);
        let data = GraphId::new("urn:test:queue-scale:data");
        let mut status = store
            .shacl_binding_statuses(&data)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        status.state = crate::ShaclValidationState::Pending;
        status.report = None;
        let mut batch = store.new_batch();
        store.stage_binding_status(&mut batch, &status).unwrap();
        batch.insert(&store.graphs, [SHACL_PENDING_PREFIX, 0], [1]);
        store.commit(batch).unwrap();

        assert!(store.pending_shacl_queue().is_err());
        assert!(store.shacl_repair_needed().unwrap());
        let repair = store.repair_shacl_queue().unwrap();
        assert_eq!(repair.binding_records_scanned, 100);
        assert_eq!(repair.pending_entries_scanned, 1);
        assert_eq!(store.pending_shacl_queue().unwrap(), vec![data.clone()]);
        assert!(!store.shacl_repair_needed().unwrap());

        let mut batch = store.new_batch();
        let data_id = store.graph_id_for(&data).unwrap().unwrap();
        batch.remove(&store.graphs, shacl_pending_key(data_id));
        store.commit(batch).unwrap();
        assert!(store.pending_shacl_queue().unwrap().is_empty());
        let repair = store.repair_shacl_queue().unwrap();
        assert_eq!(repair.binding_records_scanned, 100);
        assert_eq!(store.pending_shacl_queue().unwrap(), vec![data]);
    }

    #[test]
    fn open_preserves_durability() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:persist-mode:sync-all");

        {
            let store = GraphStore::open_with_mode(dir.path(), PersistMode::SyncAll).unwrap();
            assert_eq!(PersistMode::SyncAll, store.persist_mode());
            store.create_graph(&graph).unwrap();
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_eq!(PersistMode::Buffer, reopened.persist_mode());
        assert!(reopened.contains_graph(&graph).unwrap());
    }

    #[test]
    fn default_context_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:context-defaults-reopen");

        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            // Never set any context: the persisted metadata carries the default
            // (no context, genesis tag) for the current on-disk shape.
            store.persist().unwrap();
        }

        // Genuinely reopen from disk rather than reusing the in-memory instance.
        let reopened = GraphStore::open(dir.path()).unwrap();
        assert!(reopened.contains_graph(&graph).unwrap());
        assert_eq!(reopened.graph_context(&graph).unwrap(), None);
        assert_eq!(
            reopened.graph_context_tag(&graph).unwrap(),
            ContextTag::GENESIS
        );
    }

    fn named(iri: &str) -> EncodedTerm {
        EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(iri))
    }

    /// Resolve a quad's four terms, interning any that are new.
    fn encode_quad(store: &GraphStore, graph: &GraphId, triple: (&str, &str, &str)) -> EncodedQuad {
        EncodedQuad {
            graph: store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap(),
            subject: store.resolve_term(&named(triple.0)).unwrap(),
            predicate: store.resolve_term(&named(triple.1)).unwrap(),
            object: store.resolve_term(&named(triple.2)).unwrap(),
        }
    }

    /// Commit one add the way a real writer does: under the graph commit guard,
    /// minting the counter and advancing the clock in the same batch.
    fn commit_add(store: &GraphStore, graph: &GraphId, quad: EncodedQuad) -> Dot {
        let _commit_guard = store.graph_commit_guard(graph);
        let actor = ActorId::random();
        let mut batch = store.new_batch();
        let counter = store
            .next_counter(
                &mut batch,
                CounterKey {
                    graph_id: quad.graph,
                    actor,
                },
            )
            .unwrap();
        let dot = Dot { actor, counter };
        store
            .insert_quad(&mut batch, QuadAdd { quad, dot })
            .unwrap();
        let mut clock = store.vector_clock_id(quad.graph).unwrap();
        clock.advance(actor, counter);
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id: quad.graph,
                    clock: &clock,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        dot
    }

    fn commit_adds(store: &GraphStore, graph: &GraphId, quads: &[EncodedQuad]) {
        let _commit_guard = store.graph_commit_guard(graph);
        let actor = ActorId::random();
        let mut batch = store.new_batch();
        let mut clock = store.vector_clock_id(quads[0].graph).unwrap();
        for quad in quads {
            let counter = store
                .next_counter(
                    &mut batch,
                    CounterKey {
                        graph_id: quad.graph,
                        actor,
                    },
                )
                .unwrap();
            store
                .insert_quad(
                    &mut batch,
                    QuadAdd {
                        quad: *quad,
                        dot: Dot { actor, counter },
                    },
                )
                .unwrap();
            clock.advance(actor, counter);
        }
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id: quads[0].graph,
                    clock: &clock,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
    }

    fn commit_remove(
        store: &GraphStore,
        graph: &GraphId,
        quad: EncodedQuad,
        witnessed: &VectorClock,
    ) {
        let _commit_guard = store.graph_commit_guard(graph);
        let mut batch = store.new_batch();
        store
            .remove_quad(&mut batch, QuadRemove { quad, witnessed })
            .unwrap();
        store.commit(batch).unwrap();
    }

    /// Builds one add exactly as a writer does, but hands back the parts so a
    /// test can commit source rows without their query-view maintenance.
    fn source_only_batch(
        store: &GraphStore,
        quad: EncodedQuad,
    ) -> (fjall::OwnedWriteBatch, PendingPublish, PendingFts) {
        let actor = ActorId::random();
        let dot = Dot { actor, counter: 1 };
        let mut batch = store.new_batch();
        store
            .insert_quad(&mut batch, QuadAdd { quad, dot })
            .unwrap();
        let mut clock = store.vector_clock_id(quad.graph).unwrap();
        clock.advance(actor, 1);
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id: quad.graph,
                    clock: &clock,
                },
            )
            .unwrap();
        let WriteBatch {
            inner,
            pending_quad_states: _,
            pending_terms: _,
            publish,
            pending_fts,
            pending_receipts: _,
            receipt_trim: _,
        } = batch;
        (inner, publish, pending_fts)
    }

    #[test]
    fn rebuild_switches_slots() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-dual-slot");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let old = store.read_snapshot();

        store.rebuild_query_indexes().unwrap();
        let first = match store.snapshot_index_header(&store.db.snapshot()).unwrap() {
            IndexHeaderRead::Valid(header) => header,
            _ => panic!("rebuilt header is valid"),
        };
        assert_eq!(first.active_slot, IndexSlot::Secondary.encode());
        assert!(old.query_index_admission(&store).unwrap().trusted);
        assert_eq!(old.qv_total_count(&store).unwrap(), Some(1));

        store.rebuild_query_indexes().unwrap();
        let second = match store.snapshot_index_header(&store.db.snapshot()).unwrap() {
            IndexHeaderRead::Valid(header) => header,
            _ => panic!("rebuilt header is valid"),
        };
        assert_eq!(second.active_slot, IndexSlot::Primary.encode());
        assert!(old.query_index_admission(&store).unwrap().trusted);
        assert_eq!(old.qv_total_count(&store).unwrap(), Some(1));
    }

    #[test]
    fn malformed_header_rebuilds() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-malformed-rebuild");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let mut batch = store.buffered_batch();
        batch.insert(&store.qv2_meta, QV_HEADER_KEY, [0]);
        store.commit_fjall_batch(batch).unwrap();

        store.rebuild_query_indexes().unwrap();

        assert_index_ready(&store, 1);
        assert!(
            store
                .snapshot_admission(&store.db.snapshot())
                .unwrap()
                .trusted
        );
    }

    #[test]
    fn rebuild_clears_residue() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-meta-residue");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let mut batch = store.buffered_batch();
        batch.insert(&store.qv3_meta, b"?", [1]);
        store.commit_fjall_batch(batch).unwrap();
        store.rebuild_query_indexes().unwrap();
        assert!(store.qv3_meta.get(b"?").unwrap().is_none());

        let mut batch = store.buffered_batch();
        batch.insert(&store.qv2_meta, b"?", [1]);
        store.commit_fjall_batch(batch).unwrap();
        store.rebuild_query_indexes().unwrap();
        assert!(store.qv2_meta.get(b"?").unwrap().is_none());
        assert_index_ready(&store, 1);
    }

    #[test]
    fn incomplete_build_recovers() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv-build-recovery");
        let active_slot;
        {
            let store = GraphStore::open(directory.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
            commit_add(&store, &graph, quad);
            let header = match store.snapshot_index_header(&store.db.snapshot()).unwrap() {
                IndexHeaderRead::Valid(header) => header,
                _ => panic!("header is valid"),
            };
            active_slot = header.active_slot;
            let build = QueryBuildRecord {
                format: QV_SCHEMA_VERSION,
                slot: IndexSlot::decode(active_slot).unwrap().other().encode(),
                source_sequence: store.db.snapshot().seqno(),
                source_epoch: header.source_epoch,
                control_digest: [0; 32],
                trusted_active: false,
                scan_cursor: None,
                replay_cursor: 0,
                next_delta: 1,
                delta_rows: 0,
                delta_bytes: 0,
                phase: QueryBuildPhase::Scan,
                target: None,
            };
            let mut batch = store.buffered_batch();
            batch.insert(
                &store.qv2_meta,
                QV_BUILD_KEY,
                postcard::to_allocvec(&build).unwrap(),
            );
            store.commit_fjall_batch(batch).unwrap();
            store.persist().unwrap();
        }
        let reopened = GraphStore::open(directory.path()).unwrap();
        assert!(
            reopened
                .query_build(&reopened.db.snapshot())
                .unwrap()
                .is_none()
        );
        let header = match reopened
            .snapshot_index_header(&reopened.db.snapshot())
            .unwrap()
        {
            IndexHeaderRead::Valid(header) => header,
            _ => panic!("active header survives recovery"),
        };
        assert_eq!(header.active_slot, active_slot);
        assert!(
            reopened
                .snapshot_admission(&reopened.db.snapshot())
                .unwrap()
                .trusted
        );
    }

    /// Debt left by a failed catch-up is turned into a rebuild by runtime maintenance.
    #[test]
    fn stranded_debt_heals() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-stranded-debt");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let mut batch = store.buffered_batch();
        store.stage_projection_debt(&mut batch);
        store.commit_fjall_batch(batch).unwrap();
        assert!(
            !store
                .snapshot_admission(&store.db.snapshot())
                .unwrap()
                .trusted
        );

        store.repair_query_indexes().unwrap();

        let snapshot = store.db.snapshot();
        assert!(!store.projection_debt_present(&snapshot).unwrap());
        assert!(store.snapshot_admission(&snapshot).unwrap().trusted);
        assert!(store.index_contains(quad));
    }

    /// A full build delta log abandons the build instead of failing the source commit.
    #[test]
    fn delta_cap_abandons() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-delta-cap");
        store.create_graph(&graph).unwrap();
        let header = match store.snapshot_index_header(&store.db.snapshot()).unwrap() {
            IndexHeaderRead::Valid(header) => header,
            _ => panic!("header is valid"),
        };
        let build = QueryBuildRecord {
            format: QV_SCHEMA_VERSION,
            slot: IndexSlot::decode(header.active_slot)
                .unwrap()
                .other()
                .encode(),
            source_sequence: store.db.snapshot().seqno(),
            source_epoch: header.source_epoch,
            control_digest: [0; 32],
            trusted_active: false,
            scan_cursor: None,
            replay_cursor: 0,
            next_delta: 1,
            delta_rows: QV_DELTA_ROWS,
            delta_bytes: 0,
            phase: QueryBuildPhase::Scan,
            target: None,
        };
        let mut state = store.buffered_batch();
        state.insert(
            &store.qv2_meta,
            QV_BUILD_KEY,
            postcard::to_allocvec(&build).unwrap(),
        );
        store.commit_fjall_batch(state).unwrap();

        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        let mut batch = store.new_batch();
        store
            .insert_quad(
                &mut batch,
                QuadAdd {
                    quad,
                    dot: Dot {
                        actor: ActorId::random(),
                        counter: 1,
                    },
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        assert!(store.contains_quad(quad).unwrap());
        assert!(store.query_build(&store.db.snapshot()).unwrap().is_none());
        assert!(store.qv2_meta.contains_key(QV_CLEANUP_KEY).unwrap());

        store.repair_query_indexes().unwrap();
        assert!(!store.qv2_meta.contains_key(QV_CLEANUP_KEY).unwrap());
        store.rebuild_query_indexes().unwrap();
        assert!(store.query_build(&store.db.snapshot()).unwrap().is_none());
        assert!(
            store
                .snapshot_admission(&store.db.snapshot())
                .unwrap()
                .trusted
        );
        assert!(store.index_contains(quad));
    }

    #[test]
    fn search_generations_unique() {
        let (_dir, store) = setup_store();
        let first = GraphId::new("urn:test:search-generation-one");
        let second = GraphId::new("urn:test:search-generation-two");
        store.create_graph(&first).unwrap();
        store.create_graph(&second).unwrap();
        let index_id = [7; 16];
        let first = store
            .ensure_search_generation(&GenerationRequest {
                index_id,
                graph: first,
            })
            .unwrap()
            .active
            .unwrap();
        let second = store
            .ensure_search_generation(&GenerationRequest {
                index_id,
                graph: second,
            })
            .unwrap()
            .active
            .unwrap();
        assert_ne!(first.0, 0);
        assert_ne!(second.0, 0);
        assert_ne!(first, second);
    }

    #[test]
    fn stage_sessions_recover() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:search-stage-session");
        store.create_graph(&graph).unwrap();
        let index_id = [9; 16];
        let first = store
            .begin_search_stage(&StageRequest {
                index_id,
                graph: graph.clone(),
                target: 4,
                session: [1; 16],
            })
            .unwrap();
        let second = store
            .begin_search_stage(&StageRequest {
                index_id,
                graph: graph.clone(),
                target: 4,
                session: [2; 16],
            })
            .unwrap();
        assert_ne!(first.generation, second.generation);
        assert_eq!(
            store
                .scan_search_cleanup(&CleanupScan {
                    after: None,
                    row_limit: 8,
                    byte_limit: 1_048_576,
                })
                .unwrap()
                .entries
                .len(),
            1
        );

        let mut complete = second.clone();
        complete.complete = true;
        store.finish_search_stage(&complete).unwrap();
        let reused = store
            .begin_search_stage(&StageRequest {
                index_id,
                graph,
                target: 4,
                session: [3; 16],
            })
            .unwrap();
        assert_eq!(reused.generation, second.generation);
        assert!(reused.complete);
    }

    /// A read view captured during a source-only gap keeps its own rejection.
    #[test]
    fn uncovered_snapshot_rejected() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-coverage-gap");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o"));
        commit_add(&store, &graph, first);
        assert_index_ready(&store, 1);

        // Hold maintenance so the next commit takes the contender path.
        let held = store.qv_gate.try_acquire().expect("gate starts free");
        let second = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o"));
        let (mut inner, publish, pending_fts) = source_only_batch(&store, second);
        let debt = store.stage_projection_debt(&mut inner);
        store
            .commit_durable(DurableCommit {
                batch: inner,
                pending_fts,
                pending_receipts: Vec::new(),
                receipt_trim: ReceiptTrim::default(),
            })
            .unwrap();
        store.indexes_write().publish(&publish);

        // The gap: source holds two rows, the query view holds one.
        let captured = store.read_snapshot();
        let admission = store.snapshot_admission(captured.snapshot_ref()).unwrap();
        assert!(!admission.trusted);
        assert_eq!(
            admission.fallback_reason,
            Some("source-commit-projection-pending")
        );

        held.finish();
        store.repair_projection_debt(&publish, debt).unwrap();

        let admission = store.snapshot_admission(captured.snapshot_ref()).unwrap();
        assert!(
            !admission.trusted,
            "an old view must keep its own answer after the repair"
        );
        assert_index_ready(&store, 2);
    }

    /// Reopened debt stays inadmissible until the maintenance worker rebuilds it.
    #[test]
    fn unrepaired_debt_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv-persisted-gap");
        {
            let store = GraphStore::open(directory.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let first = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o"));
            commit_add(&store, &graph, first);
            let _held = store.qv_gate.try_acquire().expect("gate starts free");

            let second = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o"));
            let (mut inner, publish, pending_fts) = source_only_batch(&store, second);
            store.stage_projection_debt(&mut inner);
            store
                .commit_durable(DurableCommit {
                    batch: inner,
                    pending_fts,
                    pending_receipts: Vec::new(),
                    receipt_trim: ReceiptTrim::default(),
                })
                .unwrap();
            store.indexes_write().publish(&publish);
            store.persist().unwrap();
            // Stop here: the maintenance for this commit never runs.
        }

        let reopened = Arc::new(GraphStore::open(directory.path()).unwrap());
        let captured = reopened.read_snapshot();
        let admission = reopened
            .snapshot_admission(captured.snapshot_ref())
            .unwrap();
        assert!(
            !admission.trusted,
            "an uncovered query view must not be admitted after reopen"
        );
        assert_eq!(
            reopened.query_index_status().unwrap().state,
            QueryIndexState::Failed("projection-debt-unrepaired".to_owned())
        );
        maintenance_round(reopened.clone());
        assert_index_ready(&reopened, 2);
        assert!(
            !reopened
                .snapshot_admission(captured.snapshot_ref())
                .unwrap()
                .trusted
        );
    }

    /// A finished rebuild leaves no owner behind, so later writes still proceed.
    #[test]
    fn rebuild_releases_owner() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-rebuild-owner");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        store.rebuild_query_indexes().unwrap();
        assert_eq!(store.qv_gate.owner_count(), 0);
        let second = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o"));
        commit_add(&store, &graph, second);
        assert_index_ready(&store, 2);
    }

    #[test]
    fn repair_failed_projection() {
        let (_dir, mut store) = setup_store();
        store.qv_commit_wait = Duration::ZERO;
        let store = Arc::new(store);
        let graph = GraphId::new("urn:test:projection-repair");
        store.create_graph(&graph).unwrap();
        let held = store.qv_gate.try_acquire().unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let captured = store.read_snapshot();
        assert!(
            !store
                .snapshot_admission(captured.snapshot_ref())
                .unwrap()
                .trusted
        );
        drop(held);

        store.repair_diagnostics().unwrap();
        maintenance_round(store.clone());
        assert_index_ready(&store, 1);
        assert!(
            !store
                .snapshot_admission(captured.snapshot_ref())
                .unwrap()
                .trusted
        );
    }

    fn maintenance_round(store: Arc<GraphStore>) {
        let target = store.current_dirty_token();
        let search = Arc::new(crate::SearchIndex::open_in_memory().unwrap());
        #[cfg(feature = "search")]
        search.bind_store(&store).unwrap();
        let worker = crate::SearchUpdateWorker::start(Arc::clone(&store), search);
        let (sender, receiver) = std::sync::mpsc::channel();
        worker
            .sender
            .send(crate::SearchWorkerMessage::flush_reply(sender, target))
            .unwrap();
        let result = receiver.recv_timeout(Duration::from_secs(180)).unwrap();
        #[cfg(feature = "search")]
        result.unwrap();
        #[cfg(not(feature = "search"))]
        assert!(matches!(
            result,
            Err(error) if error.kind == crate::CraqleErrorKind::Unsupported
        ));
    }

    /// A failed commit releases maintenance ownership, so a healthy write that
    /// follows it can own the gate instead of waiting for an absent owner.
    #[test]
    fn failure_releases_ownership() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-failed-commit");
        store.create_graph(&graph).unwrap();
        let rejected = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o"));
        let mut batch = store.new_batch();
        let actor = ActorId::random();
        store
            .insert_quad(
                &mut batch,
                QuadAdd {
                    quad: rejected,
                    dot: Dot { actor, counter: 1 },
                },
            )
            .unwrap();
        let subjects = HashSet::from([rejected.subject]);
        store
            .enqueue_fts_subjects(
                &mut batch,
                FtsEnqueue {
                    graph_id: rejected.graph,
                    subjects: &subjects,
                },
            )
            .unwrap();
        let committed = store.current_dirty_token();
        store.arm_commit_failure();
        assert!(store.commit(batch).is_err());
        assert_eq!(store.current_dirty_token(), committed);
        assert_eq!(store.qv_gate.owner_count(), 0);

        let accepted = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o"));
        commit_add(&store, &graph, accepted);
        assert_index_ready(&store, 1);
    }

    /// Unrelated warmed entries do not add cache work to a write.
    #[test]
    fn writes_skip_scans() {
        let (_dir, store) = setup_store();
        let warm = GraphId::new("urn:test:cache-warm");
        let changed = GraphId::new("urn:test:cache-changed");
        store.create_graph(&warm).unwrap();
        store.create_graph(&changed).unwrap();

        let mut warmed_quads = Vec::new();
        for index in 0..256 {
            let quad = encode_quad(&store, &warm, (&format!("urn:s:{index}"), "urn:p", "urn:o"));
            commit_add(&store, &warm, quad);
            warmed_quads.push(quad);
        }
        for quad in &warmed_quads {
            store.triples_for_subject(quad.graph, quad.subject).unwrap();
            store
                .ordered_objects(quad.graph, quad.subject, quad.predicate)
                .unwrap();
        }
        let warmed = store.cache_statistics();
        assert!(warmed[0].entries > 0, "subject cache must be warm");
        assert!(warmed[1].entries > 0, "object-order cache must be warm");

        let before = store.cache_statistics();
        for index in 0..64 {
            let quad = encode_quad(
                &store,
                &changed,
                (&format!("urn:c:{index}"), "urn:p", "urn:o"),
            );
            commit_add(&store, &changed, quad);
        }
        let after = store.cache_statistics();

        assert_eq!(
            after[0].inspections, before[0].inspections,
            "publication inspected warmed subject-cache keys"
        );
        assert_eq!(
            after[0].compactions, before[0].compactions,
            "publication rebuilt the subject-cache recency order"
        );
        assert_eq!(
            after[1].inspections, before[1].inspections,
            "publication inspected warmed object-order keys"
        );
        assert_eq!(
            after[1].compactions, before[1].compactions,
            "publication rebuilt the object-order recency order"
        );

        let first_warm = warmed_quads[0];
        assert_eq!(
            1,
            store
                .triples_for_subject(first_warm.graph, first_warm.subject)
                .unwrap()
                .len()
        );
        let first_changed = encode_quad(&store, &changed, ("urn:c:0", "urn:p", "urn:o"));
        assert_eq!(
            1,
            store
                .triples_for_subject(first_changed.graph, first_changed.subject)
                .unwrap()
                .len()
        );
    }

    /// A removal that matches nothing must not rebuild the recency order.
    #[test]
    fn empty_removal_uncompacted() {
        let mut cache: BoundedCache<u64, u64> = BoundedCache::new(64, 4_096);
        for key in 0..16u64 {
            cache.insert(key, key, 8);
        }
        let before = cache.statistics().compactions;
        cache.remove_where(|key| *key > 1_000);
        assert_eq!(cache.statistics().compactions, before);
        assert_eq!(cache.statistics().entries, 16);
        cache.remove_where(|key| *key == 0);
        assert_eq!(cache.statistics().compactions, before + 1);
        assert_eq!(cache.statistics().entries, 15);
    }

    fn write_limit(path: &Path, contents: &str) {
        std::fs::create_dir_all(path.parent().expect("limit file has a parent")).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn unified_limits_bind() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cgroup");
        let mapping = dir.path().join("self-cgroup");
        std::fs::write(&mapping, "0::/service.slice/craqle.service\n").unwrap();
        write_limit(&root.join("memory.max"), "max\n");
        write_limit(&root.join("service.slice/memory.max"), "536870912\n");
        write_limit(
            &root.join("service.slice/craqle.service/memory.max"),
            "268435456\n",
        );
        assert_eq!(
            cgroup_memory_limit(&mapping, &root),
            Some(268_435_456),
            "the closest numeric ceiling applies"
        );
    }

    #[test]
    fn ancestor_limits_bind() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cgroup");
        let mapping = dir.path().join("self-cgroup");
        std::fs::write(&mapping, "0::/outer/inner\n").unwrap();
        write_limit(&root.join("outer/memory.max"), "134217728\n");
        write_limit(&root.join("outer/inner/memory.max"), "1073741824\n");
        assert_eq!(
            cgroup_memory_limit(&mapping, &root),
            Some(134_217_728),
            "an ancestor ceiling also binds this process"
        );
    }

    #[test]
    fn legacy_limits_bind() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cgroup");
        let mapping = dir.path().join("self-cgroup");
        std::fs::write(
            &mapping,
            "7:cpu,cpuacct:/user.slice\n6:memory:/user.slice/craqle\n",
        )
        .unwrap();
        write_limit(
            &root.join("memory/user.slice/craqle/memory.limit_in_bytes"),
            "402653184\n",
        );
        assert_eq!(cgroup_memory_limit(&mapping, &root), Some(402_653_184));
    }

    #[test]
    fn unreadable_limits_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cgroup");
        let mapping = dir.path().join("self-cgroup");
        std::fs::write(&mapping, "0::/only\n").unwrap();

        // Missing file.
        assert_eq!(cgroup_memory_limit(&mapping, &root), None);

        // Malformed value.
        write_limit(&root.join("only/memory.max"), "not-a-number\n");
        assert_eq!(cgroup_memory_limit(&mapping, &root), None);

        // Explicitly unlimited, both spellings.
        std::fs::write(root.join("only/memory.max"), "max\n").unwrap();
        assert_eq!(cgroup_memory_limit(&mapping, &root), None);
        std::fs::write(root.join("only/memory.max"), "9223372036854771712\n").unwrap();
        assert_eq!(cgroup_memory_limit(&mapping, &root), None);

        // Absent mapping file.
        assert_eq!(cgroup_memory_limit(&dir.path().join("absent"), &root), None);

        // Denied read.
        let denied = root.join("only/memory.max");
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).unwrap();
        let observed = cgroup_memory_limit(&mapping, &root);
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(observed, None, "a denied read contributes no ceiling");
    }

    #[test]
    fn small_budget_unfloored() {
        let tight = CacheBudget::from_limit(Some(256 * 1_048_576));
        assert!(
            tight.database < DEFAULT_DB_BYTES,
            "a 256 MiB process must not be given a 1 GiB block cache"
        );
        assert!(tight.database >= MIN_DB_BYTES);
        assert!(tight.terms < TERM_CACHE_BYTES);
        assert!(tight.subjects < SUBJECT_CACHE_BYTES);
        assert!(tight.objects < ORDER_CACHE_BYTES);
        assert!(tight.terms >= MIN_APP_BYTES);

        let application = tight.terms + tight.subjects + tight.objects + tight.planner;
        assert!(
            application <= 256 * 1_048_576,
            "application caches must stay inside the process budget"
        );
    }

    #[test]
    fn large_budget_defaults() {
        let roomy = CacheBudget::from_limit(Some(64 * 1_024 * 1_048_576));
        assert_eq!(roomy.database, MAX_DB_BYTES);
        assert_eq!(roomy.terms, TERM_CACHE_BYTES);
        assert_eq!(roomy.subjects, SUBJECT_CACHE_BYTES);
        assert_eq!(roomy.objects, ORDER_CACHE_BYTES);
        assert_eq!(roomy.planner, PLANNER_CACHE_BYTES);

        // No ceiling readable keeps the historical sizes rather than guessing.
        let unknown = CacheBudget::from_limit(None);
        assert_eq!(unknown.database, DEFAULT_DB_BYTES);
        assert_eq!(unknown.terms, TERM_CACHE_BYTES);
    }

    #[test]
    fn parses_available_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meminfo");
        std::fs::write(
            &path,
            "MemTotal:       65536000 kB\nMemAvailable:    1048576 kB\n",
        )
        .unwrap();
        assert_eq!(meminfo_available(&path), Some(1_073_741_824));
        std::fs::write(&path, "MemTotal: 1 kB\n").unwrap();
        assert_eq!(meminfo_available(&path), None);
        assert_eq!(meminfo_available(&dir.path().join("absent")), None);
    }

    /// Reports whether a mutation publishes debt while QV maintenance is owned.
    fn debt_seen_during(store: &GraphStore, mutate: impl FnOnce() + Send) -> bool {
        let held = store.qv_gate.try_acquire().expect("gate starts free");
        let mut observed = false;
        std::thread::scope(|scope| {
            let worker = scope.spawn(mutate);
            while store.qv_gate.waiting() == 0 && !worker.is_finished() {
                std::thread::yield_now();
            }
            observed = store
                .projection_debt_present(&store.read_snapshot().snapshot)
                .unwrap();
            held.finish();
            worker.join().expect("mutation finished");
        });
        observed
    }

    /// Real remove and delete paths publish QV rows or durable debt.
    #[test]
    fn mutations_record_debt() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-entry-points");
        store.create_graph(&graph).unwrap();
        let kept = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o"));
        let doomed = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o"));
        commit_add(&store, &graph, kept);
        let dot = commit_add(&store, &graph, doomed);
        assert_index_ready(&store, 2);

        let mut witnessed = VectorClock::new();
        witnessed.advance(dot.actor, dot.counter);
        assert!(
            debt_seen_during(&store, || {
                commit_remove(&store, &graph, doomed, &witnessed);
            }),
            "a removal that skipped maintenance must publish projection debt"
        );
        assert_index_ready(&store, 1);

        let actor = ActorId::random();
        let clock = store.get_vector_clock(&graph).unwrap();
        let tombstone = GraphTombstone {
            graph: graph.clone(),
            delete_event: EventId::graph_delete(&graph, actor, &clock),
            delete_actor: actor,
            delete_clock: clock,
        };
        assert!(
            debt_seen_during(&store, || {
                store.delete_graph_tombstoned(&tombstone).unwrap();
            }),
            "a graph deletion that skipped maintenance must publish projection debt"
        );
        assert_index_ready(&store, 0);
    }

    #[test]
    fn planner_distinct_counts() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:planner-distinct");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o:1"));
        let second = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o:2"));
        let third = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o:1"));
        commit_add(&store, &graph, first);
        commit_add(&store, &graph, second);
        commit_add(&store, &graph, third);

        assert_eq!(store.predicate_subject_count(first.predicate), 2);
        assert_eq!(store.predicate_object_count(first.predicate), 2);

        let clock = store.get_vector_clock(&graph).unwrap();
        commit_remove(&store, &graph, third, &clock);
        assert_eq!(store.predicate_subject_count(first.predicate), 1);
        assert_eq!(store.predicate_object_count(first.predicate), 2);

        let clock = store.get_vector_clock(&graph).unwrap();
        commit_remove(&store, &graph, first, &clock);
        assert_eq!(store.predicate_subject_count(first.predicate), 1);
        assert_eq!(store.predicate_object_count(first.predicate), 1);
    }

    #[test]
    fn predicate_cache_scopes() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:planner-cache-scope");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o:1"));
        let second = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o:2"));
        commit_add(&store, &graph, first);
        assert_index_ready(&store, 1);
        let measure = || {
            let costs = QueryCost::planner(true);
            let count = store.planner_stat(PlannerStat::PredicateSubjects(first.predicate), &costs);
            (count, costs.snapshot())
        };

        let (count, cold) = measure();
        assert_eq!(count, 1);
        assert!(cold.planner_index_entries > 0);
        let (_, warm) = measure();
        assert_eq!(warm.planner_index_entries, 0);

        let unrelated = encode_quad(&store, &graph, ("urn:s:3", "urn:q", "urn:o:3"));
        commit_add(&store, &graph, unrelated);
        let (_, unrelated_write) = measure();
        assert_eq!(unrelated_write.planner_index_entries, 0);

        commit_add(&store, &graph, first);
        let (_, duplicate_write) = measure();
        assert_eq!(duplicate_write.planner_index_entries, 0);

        let clock = store.get_vector_clock(&graph).unwrap();
        commit_remove(&store, &graph, first, &clock);
        let (count, relevant_delete) = measure();
        assert_eq!(count, 0);
        assert!(relevant_delete.planner_cache_misses > 0);
        assert_index_ready(&store, 1);

        commit_adds(&store, &graph, &[first, second]);
        let (count, reinserted) = measure();
        assert_eq!(count, 2);
        assert!(reinserted.planner_index_entries > 0);
        assert_index_ready(&store, 3);

        store.rebuild_query_indexes().unwrap();
        let (count, rebuilt) = measure();
        assert_eq!(count, 2);
        assert!(rebuilt.planner_index_entries > 0);
    }

    #[test]
    fn fallback_stays_unknown() {
        let (_dir, store) = setup_store();
        let mut batch = store.buffered_batch();
        let dot = Dot {
            actor: ActorId::from_bytes([7; 32]),
            counter: 1,
        };
        for index in 0..=PLANNER_SAMPLE_ROWS {
            let quad = EncodedQuad {
                graph: TermId(1),
                subject: TermId(index as u128),
                predicate: TermId(2),
                object: TermId(3),
            };
            batch.insert(
                &store.quads,
                GraphStore::quad_key(quad.graph, quad.subject, quad.predicate, quad.object),
                encode_dots(&[dot]),
            );
        }
        store.stage_index_failure(&mut batch, Some(&test_index_header(&store)), "test-failure");
        store.commit_fjall_batch(batch).unwrap();

        let costs = QueryCost::planner(true);
        let estimate = store.planner_estimate(PlannerStat::Subject(TermId(u128::MAX)), &costs);
        assert_eq!(estimate, PlannerEstimate::Unknown);
        assert_eq!(
            costs.snapshot().planner_index_entries,
            PLANNER_SAMPLE_ROWS as u64 + 1
        );
    }

    fn test_index_header(store: &GraphStore) -> IndexHeader {
        let snapshot = store.db.snapshot();
        match store.snapshot_index_header(&snapshot).unwrap() {
            IndexHeaderRead::Valid(header) => header,
            IndexHeaderRead::Absent | IndexHeaderRead::Legacy(_) | IndexHeaderRead::Malformed => {
                panic!("query-index header must be present and valid")
            }
        }
    }

    fn test_index_counter(store: &GraphStore, key: IndexCounterKey) -> Option<u64> {
        let snapshot = store.read_snapshot();
        let spaces = store
            .active_query_spaces(&snapshot)
            .unwrap()
            .expect("query-index header must select an active slot");
        snapshot
            .snapshot_ref()
            .get(spaces.meta, key.bytes())
            .unwrap()
            .map(|value| decode_index_count(value.as_ref()).unwrap())
    }

    fn test_query_id(store: &GraphStore, term: TermId) -> QueryTermId {
        let snapshot = store.read_snapshot();
        store
            .snapshot_query_id(&snapshot, term)
            .unwrap()
            .expect("live query-index term must have a dense id")
    }

    fn test_query_quad(store: &GraphStore, quad: EncodedQuad) -> QueryQuad {
        let snapshot = store.read_snapshot();
        store
            .snapshot_query_quad(&snapshot, quad)
            .unwrap()
            .expect("live query-index quad must have dense ids")
    }

    fn assert_index_ready(store: &GraphStore, source_rows: u64) {
        let status = store.query_index_status().unwrap();
        assert_eq!(status.state, QueryIndexState::Ready);
        assert_eq!(status.source_live_quads, source_rows);
        assert_eq!(status.indexed_quads, source_rows);
        assert!(store.verify_query_indexes(true).unwrap().valid);
    }

    fn assert_index_problem(report: &QueryIndexVerification, problem: &str) {
        assert!(
            report.problems.iter().any(|current| current == problem),
            "expected query-index problem {problem}, got {:?}",
            report.problems
        );
    }

    fn stage_test_header(store: &GraphStore, header: &IndexHeader) {
        let mut batch = store.buffered_batch();
        store.stage_index_header(&mut batch, header);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn stage_test_value(
        store: &GraphStore,
        keyspace: &Keyspace,
        key: impl Into<fjall::UserKey>,
        value: impl Into<fjall::UserValue>,
    ) {
        let mut batch = store.buffered_batch();
        batch.insert(keyspace, key, value);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn remove_test_key(store: &GraphStore, keyspace: &Keyspace, key: impl Into<fjall::UserKey>) {
        let mut batch = store.buffered_batch();
        batch.remove(keyspace, key);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn read_test_rows(
        store: &GraphStore,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Vec<EncodedQuad> {
        let view = StoreReadView::new(store);
        let context = ReadContext::default();
        let mut rows = view
            .scan(&context, selector, pattern)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        rows.sort_by_key(|quad| (quad.graph, quad.subject, quad.predicate, quad.object));
        rows
    }

    fn insert_quad(
        store: &GraphStore,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        object: &EncodedTerm,
        dot: Dot,
    ) {
        if !store.contains_graph(graph).unwrap() {
            store.create_graph(graph).unwrap();
        }
        let mut batch = store.new_batch();
        let quad = EncodedQuad {
            graph: store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap(),
            subject: store.resolve_term(subject).unwrap(),
            predicate: store.resolve_term(predicate).unwrap(),
            object: store.resolve_term(object).unwrap(),
        };
        store
            .insert_quad(&mut batch, QuadAdd { quad, dot })
            .unwrap();
        store.commit(batch).unwrap();
    }

    #[test]
    fn term_identifiers_roundtrip() {
        let (_dir, store) = setup_store();
        let term = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:term"));
        let id = store.encode_term(&term).unwrap();
        assert_eq!(Some(id), store.lookup_term(&term).unwrap());
        assert_eq!(term, store.decode_term(id).unwrap());
    }

    #[test]
    fn index_readiness_initializes() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:old-source");
        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_index_ready(&store, 0);

            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            assert_index_ready(&store, 1);

            remove_test_key(&store, &store.qv2_meta, QV_HEADER_KEY);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        let status = reopened.query_index_status().unwrap();
        assert_eq!(status.state, QueryIndexState::Missing);
        assert_eq!(status.source_live_quads, 1);
        assert_eq!(status.indexed_quads, 1);
        assert_eq!(
            reopened
                .quads_for_pattern(None, None, None, None)
                .unwrap()
                .len(),
            1,
            "Missing must leave canonical fallback reads available"
        );
    }

    #[test]
    fn prior_header_rebuilds() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:prior-query-format");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        store.rebuild_query_indexes().unwrap();
        let header = test_index_header(&store);
        assert_eq!(header.active_slot, IndexSlot::Secondary.encode());
        let mut bytes = encode_index_header(&header);
        bytes[4..8].copy_from_slice(&4u32.to_be_bytes());
        stage_test_value(&store, &store.qv2_meta, QV_HEADER_KEY, bytes);

        store.rebuild_query_indexes().unwrap();
        assert_index_ready(&store, 1);
        assert_eq!(
            store.quads_for_pattern(None, None, None, None).unwrap(),
            vec![quad]
        );
    }

    #[test]
    fn compact_indexes_smaller() {
        const ROWS: u64 = 10_000;
        let zero = QueryTermId(0);
        assert_eq!(query_index_key([zero; 4]).len(), 32);

        let (_dir, store) = setup_store();
        let index_options = || {
            KeyspaceCreateOptions::default()
                .data_block_compression_policy(CompressionPolicy::disabled())
                .index_block_compression_policy(CompressionPolicy::disabled())
        };
        let v2: Vec<_> = (0..6)
            .map(|index| {
                store
                    .db
                    .keyspace(&format!("test_qv2_{index}"), index_options)
                    .unwrap()
            })
            .collect();
        let u128_layout: Vec<_> = (0..6)
            .map(|index| {
                store
                    .db
                    .keyspace(&format!("test_qv1_{index}"), index_options)
                    .unwrap()
            })
            .collect();
        let term_to_query = store
            .db
            .keyspace("test_qv2_term_to_query", KeyspaceCreateOptions::default)
            .unwrap();
        let query_to_term = store
            .db
            .keyspace("test_qv2_query_to_term", KeyspaceCreateOptions::default)
            .unwrap();

        let graph = hash_term(&named("urn:test:qv2:size:graph"));
        let query_graph = QueryTermId(0);
        let mut batch = store.buffered_batch();
        batch.insert(
            &term_to_query,
            graph.to_be_bytes(),
            query_graph.to_be_bytes(),
        );
        batch.insert(
            &query_to_term,
            query_graph.to_be_bytes(),
            graph.to_be_bytes(),
        );
        for row in 0..ROWS {
            let subject = hash_term(&named(&format!("urn:test:qv2:size:s:{row}")));
            let predicate_index = row % 16;
            let predicate = hash_term(&named(&format!("urn:test:qv2:size:p:{predicate_index}")));
            let object_index = row % 1_000;
            let object = hash_term(&named(&format!("urn:test:qv2:size:o:{object_index}")));
            let query_subject = QueryTermId(row + 1);
            let query_predicate = QueryTermId(ROWS + 1 + predicate_index);
            let query_object = QueryTermId(ROWS + 17 + object_index);
            let query_quad = QueryQuad {
                graph: query_graph,
                subject: query_subject,
                predicate: query_predicate,
                object: query_object,
            };
            let v2_keys = [
                gspo_key(query_quad),
                gpos_key(query_quad),
                spog_key(query_quad),
                posg_key(query_quad),
                ospg_key(query_quad),
                gosp_key(query_quad),
            ];
            let u128_keys = [
                GraphStore::quad_key(graph, subject, predicate, object),
                GraphStore::quad_key(graph, predicate, object, subject),
                GraphStore::quad_key(subject, predicate, object, graph),
                GraphStore::quad_key(predicate, object, subject, graph),
                GraphStore::quad_key(object, subject, predicate, graph),
                GraphStore::quad_key(graph, object, subject, predicate),
            ];
            for ((keyspace, v2_key), (u128_keyspace, u128_key)) in v2
                .iter()
                .zip(v2_keys)
                .zip(u128_layout.iter().zip(u128_keys))
            {
                batch.insert(keyspace, v2_key, []);
                batch.insert(u128_keyspace, u128_key, []);
            }
            batch.insert(
                &term_to_query,
                subject.to_be_bytes(),
                query_subject.to_be_bytes(),
            );
            batch.insert(
                &query_to_term,
                query_subject.to_be_bytes(),
                subject.to_be_bytes(),
            );
            if row < 16 {
                batch.insert(
                    &term_to_query,
                    predicate.to_be_bytes(),
                    query_predicate.to_be_bytes(),
                );
                batch.insert(
                    &query_to_term,
                    query_predicate.to_be_bytes(),
                    predicate.to_be_bytes(),
                );
            }
            if row < 1_000 {
                batch.insert(
                    &term_to_query,
                    object.to_be_bytes(),
                    query_object.to_be_bytes(),
                );
                batch.insert(
                    &query_to_term,
                    query_object.to_be_bytes(),
                    object.to_be_bytes(),
                );
            }
        }
        batch.commit().unwrap();
        for keyspace in v2
            .iter()
            .chain(u128_layout.iter())
            .chain([&term_to_query, &query_to_term])
        {
            keyspace.rotate_memtable_and_wait().unwrap();
        }

        let v2_bytes = v2.iter().map(Keyspace::disk_space).sum::<u64>()
            + term_to_query.disk_space()
            + query_to_term.disk_space();
        let u128_bytes = u128_layout.iter().map(Keyspace::disk_space).sum::<u64>();
        eprintln!("query_index_rows={ROWS} qv2_bytes={v2_bytes} u128_bytes={u128_bytes}");
        assert!(
            v2_bytes * 100 <= u128_bytes * 70,
            "qv2 must use at least 30% less disk: qv2={v2_bytes}, u128={u128_bytes}"
        );
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn qv_counts_sparse() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:counts");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let missing = store.resolve_term(&named("urn:test:missing")).unwrap();
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        assert_eq!(Some(1), view.qv_g_count(&context, quad.graph).unwrap());
        assert_eq!(
            Some(1),
            view.qv_gp_count(&context, quad.graph, quad.predicate)
                .unwrap()
        );
        assert_eq!(
            Some(0),
            view.qv_gp_count(&context, quad.graph, missing).unwrap()
        );
        assert_eq!(
            Some(0),
            view.qv_gpo_count(&context, quad.graph, quad.predicate, missing)
                .unwrap()
        );
        let stats = context.snapshot();
        assert_eq!(1, stats.qv_admission_checks);
        assert_eq!(5, stats.qv_counter_reads);

        stage_test_value(
            &store,
            &store.qv2_meta,
            IndexCounterKey::GraphPredicate(
                test_query_id(&store, quad.graph),
                test_query_id(&store, quad.predicate),
            )
            .bytes(),
            [0_u8],
        );
        let view = StoreReadView::new(&store);
        assert_eq!(
            None,
            view.qv_gp_count(&ReadContext::default(), quad.graph, quad.predicate)
                .unwrap()
        );

        let mut failed = test_index_header(&store);
        failed.state = StoredIndexState::Failed("test-failed".to_owned());
        stage_test_header(&store, &failed);
        let context = ReadContext::default();
        assert_eq!(
            None,
            StoreReadView::new(&store)
                .qv_g_count(&context, quad.graph)
                .unwrap()
        );
        assert!(!context.snapshot().qv_trusted);
    }

    #[test]
    fn untrusted_metadata_fallback() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:fallback-binding-shapes");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(
            &store,
            &graph,
            ("urn:test:s1", "urn:test:p1", "urn:test:o1"),
        );
        let second = encode_quad(
            &store,
            &graph,
            ("urn:test:s1", "urn:test:p1", "urn:test:o2"),
        );
        let third = encode_quad(
            &store,
            &graph,
            ("urn:test:s2", "urn:test:p2", "urn:test:o1"),
        );
        commit_add(&store, &graph, first);
        commit_add(&store, &graph, second);
        commit_add(&store, &graph, third);
        settle_diagnostics(&store, &graph);

        let patterns: Vec<_> = (0..8)
            .map(|bindings| QuadPattern {
                subject: (bindings & 1 != 0).then_some(first.subject),
                predicate: (bindings & 2 != 0).then_some(first.predicate),
                object: (bindings & 4 != 0).then_some(first.object),
                ..QuadPattern::default()
            })
            .collect();
        let trusted: Vec<_> = patterns
            .iter()
            .copied()
            .map(|pattern| read_test_rows(&store, GraphSelector::Named(first.graph), pattern))
            .collect();
        let ready = test_index_header(&store);

        remove_test_key(&store, &store.qv2_meta, QV_HEADER_KEY);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_test_rows(&store, GraphSelector::Named(first.graph), *shape),
                "Missing changed binding shape {shape:?}"
            );
        }
        stage_test_header(&store, &ready);

        let mut building = ready.clone();
        building.state = StoredIndexState::Building;
        stage_test_header(&store, &building);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_test_rows(&store, GraphSelector::Named(first.graph), *shape),
                "Building changed binding shape {shape:?}"
            );
        }
        stage_test_header(&store, &ready);

        let mut failed = ready.clone();
        failed.state = StoredIndexState::Failed("test-failed".to_owned());
        stage_test_header(&store, &failed);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_test_rows(&store, GraphSelector::Named(first.graph), *shape),
                "Failed changed binding shape {shape:?}"
            );
        }
        stage_test_header(&store, &ready);
    }

    #[test]
    fn union_untrusted_fails() {
        let (_dir, store) = setup_store();
        let first_graph = GraphId::new("urn:test:qv:default-fallback:first");
        let second_graph = GraphId::new("urn:test:qv:default-fallback:second");
        store.create_graph(&first_graph).unwrap();
        store.create_graph(&second_graph).unwrap();
        let shared = encode_quad(
            &store,
            &first_graph,
            (
                "urn:test:shared:s",
                "urn:test:shared:p",
                "urn:test:shared:o",
            ),
        );
        let duplicate = encode_quad(
            &store,
            &second_graph,
            (
                "urn:test:shared:s",
                "urn:test:shared:p",
                "urn:test:shared:o",
            ),
        );
        let unique = encode_quad(
            &store,
            &first_graph,
            (
                "urn:test:unique:s",
                "urn:test:unique:p",
                "urn:test:unique:o",
            ),
        );
        commit_add(&store, &first_graph, shared);
        commit_add(&store, &second_graph, duplicate);
        commit_add(&store, &first_graph, unique);
        settle_diagnostics(&store, &first_graph);
        settle_diagnostics(&store, &second_graph);

        let patterns: Vec<_> = (0..8)
            .map(|bindings| QuadPattern {
                subject: (bindings & 1 != 0).then_some(shared.subject),
                predicate: (bindings & 2 != 0).then_some(shared.predicate),
                object: (bindings & 4 != 0).then_some(shared.object),
                ..QuadPattern::default()
            })
            .collect();
        for pattern in &patterns {
            let view = StoreReadView::new(&store);
            let context = ReadContext::default();
            assert!(
                view.scan(&context, GraphSelector::DefaultUnion, *pattern)
                    .unwrap()
                    .collect::<Result<Vec<_>>>()
                    .is_ok()
            );
        }
        let ready = test_index_header(&store);

        let assert_unavailable = |label: &str| {
            for pattern in &patterns {
                let view = StoreReadView::new(&store);
                let context = ReadContext::default();
                assert!(
                    matches!(
                        view.scan(&context, GraphSelector::DefaultUnion, *pattern),
                        Err(StoreError::QueryIndexUnavailable(_))
                    ),
                    "{label} must reject unbounded default-union fallback for {pattern:?}"
                );
                let statistics = context.snapshot();
                assert_eq!(0, statistics.source_keys_read);
                assert_eq!(0, statistics.qv_keys_read);
                assert_eq!(0, statistics.candidate_quads);
            }
        };

        remove_test_key(&store, &store.qv2_meta, QV_HEADER_KEY);
        assert_unavailable("Missing");
        stage_test_header(&store, &ready);

        let mut building = ready.clone();
        building.state = StoredIndexState::Building;
        stage_test_header(&store, &building);
        assert_unavailable("Building");
        stage_test_header(&store, &ready);

        let mut failed = ready.clone();
        failed.state = StoredIndexState::Failed("test-failed".to_owned());
        stage_test_header(&store, &failed);
        assert_unavailable("Failed");
        stage_test_header(&store, &ready);
    }

    #[test]
    fn invalid_headers_fallback() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:fallback-header");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        settle_diagnostics(&store, &graph);
        let pattern = QuadPattern {
            predicate: Some(quad.predicate),
            object: Some(quad.object),
            ..QuadPattern::default()
        };
        let expected = read_test_rows(&store, GraphSelector::Named(quad.graph), pattern);
        let ready = test_index_header(&store);

        stage_test_value(&store, &store.qv2_meta, QV_HEADER_KEY, [0_u8]);
        assert_eq!(
            expected,
            read_test_rows(&store, GraphSelector::Named(quad.graph), pattern)
        );
        stage_test_header(&store, &ready);

        let mut stale = ready.clone();
        stale.index_epoch = stale.index_epoch.saturating_add(1);
        stage_test_header(&store, &stale);
        assert_eq!(
            expected,
            read_test_rows(&store, GraphSelector::Named(quad.graph), pattern)
        );
    }

    #[test]
    fn corrupt_rows_fail() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:terminal-corruption");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p1", "urn:test:o1"));
        let second = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p2", "urn:test:o2"));
        commit_add(&store, &graph, first);
        commit_add(&store, &graph, second);
        settle_diagnostics(&store, &graph);

        let (first, corrupt) = if gspo_key(test_query_quad(&store, first))
            < gspo_key(test_query_quad(&store, second))
        {
            (first, second)
        } else {
            (second, first)
        };
        stage_test_value(
            &store,
            &store.qv2_gspo,
            gspo_key(test_query_quad(&store, corrupt)),
            [1_u8],
        );

        let view = StoreReadView::with_read_mode(&store, QueryReadMode::ForceQv);
        let context = ReadContext::default();
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern {
                    subject: Some(first.subject),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(matches!(cursor.next(), Some(Ok(quad)) if quad == first));
        assert!(matches!(
            cursor.next(),
            Some(Err(StoreError::InvalidIndexEncoding { .. }))
        ));
        assert!(
            cursor.next().is_none(),
            "a qv error must finish this cursor"
        );
    }

    #[test]
    fn index_mutations_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:restart");
        let (quad, query_quad, dot) = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            let dot = commit_add(&store, &graph, quad);
            assert_index_ready(&store, 1);
            let query_quad = test_query_quad(&store, quad);
            let header = test_index_header(&store);
            assert_eq!(header.next_query_id, 4);
            store.persist().unwrap();
            (quad, query_quad, dot)
        };

        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_index_ready(&store, 1);
            assert_eq!(test_query_quad(&store, quad), query_quad);
            let mut witnessed = VectorClock::new();
            witnessed.advance(dot.actor, dot.counter);
            commit_remove(&store, &graph, quad, &witnessed);
            assert_index_ready(&store, 0);
            assert_eq!(test_query_quad(&store, quad), query_quad);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_index_ready(&reopened, 0);
        assert_eq!(test_query_quad(&reopened, quad), query_quad);
    }

    #[test]
    fn index_coalesces_dots() {
        let (_dir, store) = setup_store();
        let store = Arc::new(store);
        let graph = GraphId::new("urn:test:qv:dots");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));

        let first = {
            let store = store.clone();
            let graph = graph.clone();
            std::thread::spawn(move || commit_add(&store, &graph, quad))
        };
        let second = {
            let store = store.clone();
            let graph = graph.clone();
            std::thread::spawn(move || commit_add(&store, &graph, quad))
        };
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_index_ready(&store, 1);

        let mut first_only = VectorClock::new();
        first_only.advance(first.actor, first.counter);
        commit_remove(&store, &graph, quad, &first_only);
        assert_index_ready(&store, 1);

        let mut all_dots = VectorClock::new();
        all_dots.advance(first.actor, first.counter);
        all_dots.advance(second.actor, second.counter);
        commit_remove(&store, &graph, quad, &all_dots);
        assert_index_ready(&store, 0);
    }

    #[test]
    fn concurrent_counters_exact() {
        let (_dir, store) = setup_store();
        let graph_one = GraphId::new("urn:test:qv:cross-graph:one");
        let graph_two = (0u64..)
            .map(|index| {
                let graph = format!("urn:test:qv:cross-graph:{index}");
                GraphId::new(&graph)
            })
            .find(|graph| {
                (hash_term(&EncodedTerm::from_named_node(&graph_one.0)).0 as usize)
                    % COMMIT_LOCK_SHARDS
                    != (hash_term(&EncodedTerm::from_named_node(&graph.0)).0 as usize)
                        % COMMIT_LOCK_SHARDS
            })
            .unwrap();
        store.create_graph(&graph_one).unwrap();
        store.create_graph(&graph_two).unwrap();
        let first = encode_quad(
            &store,
            &graph_one,
            ("urn:test:s1", "urn:test:p", "urn:test:o"),
        );
        let second = encode_quad(
            &store,
            &graph_two,
            ("urn:test:s2", "urn:test:p", "urn:test:o"),
        );
        let store = Arc::new(store);
        let start = Arc::new(std::sync::Barrier::new(3));

        let first_writer = {
            let store = store.clone();
            let graph = graph_one.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                commit_add(&store, &graph, first)
            })
        };
        let second_writer = {
            let store = store.clone();
            let graph = graph_two.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                commit_add(&store, &graph, second)
            })
        };
        start.wait();
        first_writer.join().unwrap();
        second_writer.join().unwrap();

        assert_index_ready(&store, 2);
        let first_query = test_query_quad(&store, first);
        let second_query = test_query_quad(&store, second);
        assert_eq!(test_index_counter(&store, IndexCounterKey::Total), Some(2));
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::Graph(first_query.graph)),
            Some(1)
        );
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::Graph(second_query.graph)),
            Some(1)
        );
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::Predicate(first_query.predicate),),
            Some(2)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::PredicateObject(first_query.predicate, first_query.object)
            ),
            Some(2)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::GraphPredicate(first_query.graph, first_query.predicate)
            ),
            Some(1)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::GraphPredicate(second_query.graph, second_query.predicate)
            ),
            Some(1)
        );
    }

    #[test]
    fn index_coalesces_crossings() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:net-transition");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        let first = Dot {
            actor: ActorId::random(),
            counter: 1,
        };
        let second = Dot {
            actor: ActorId::random(),
            counter: 1,
        };
        {
            let _guard = store.graph_commit_guard(&graph);
            let mut batch = store.new_batch();
            store
                .insert_quad(&mut batch, QuadAdd { quad, dot: first })
                .unwrap();
            let mut first_only = VectorClock::new();
            first_only.advance(first.actor, first.counter);
            store
                .remove_quad(
                    &mut batch,
                    QuadRemove {
                        quad,
                        witnessed: &first_only,
                    },
                )
                .unwrap();
            store
                .insert_quad(&mut batch, QuadAdd { quad, dot: second })
                .unwrap();
            store.commit(batch).unwrap();
        }
        let header = test_index_header(&store);
        assert_eq!(header.source_epoch, 1);
        assert_index_ready(&store, 1);

        {
            let _guard = store.graph_commit_guard(&graph);
            let mut batch = store.new_batch();
            let mut second_only = VectorClock::new();
            second_only.advance(second.actor, second.counter);
            store
                .remove_quad(
                    &mut batch,
                    QuadRemove {
                        quad,
                        witnessed: &second_only,
                    },
                )
                .unwrap();
            let third = Dot {
                actor: ActorId::random(),
                counter: 1,
            };
            store
                .insert_quad(&mut batch, QuadAdd { quad, dot: third })
                .unwrap();
            store.commit(batch).unwrap();
        }
        assert_eq!(test_index_header(&store).source_epoch, 1);
        assert_index_ready(&store, 1);
    }

    #[test]
    fn duplicate_proof_rebuilds() {
        let (_dir, store) = setup_store();
        let first_graph = GraphId::new("urn:test:qv:union-proof:first");
        let second_graph = GraphId::new("urn:test:qv:union-proof:second");
        store.create_graph(&first_graph).unwrap();
        store.create_graph(&second_graph).unwrap();
        let first = encode_quad(
            &store,
            &first_graph,
            ("urn:test:s", "urn:test:p", "urn:test:o"),
        );
        let second = encode_quad(
            &store,
            &second_graph,
            ("urn:test:s", "urn:test:p", "urn:test:o"),
        );

        assert_eq!(
            test_index_counter(&store, IndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        commit_add(&store, &first_graph, first);
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        commit_add(&store, &second_graph, second);
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::UnionDuplicateFree),
            Some(0)
        );

        let clock = store.get_vector_clock(&second_graph).unwrap();
        commit_remove(&store, &second_graph, second, &clock);
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::UnionDuplicateFree),
            Some(0)
        );
        store.rebuild_query_indexes().unwrap();
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        assert_index_ready(&store, 1);
    }

    #[test]
    fn dimension_counters_exact() {
        let (_dir, store) = setup_store();
        let graph_one = GraphId::new("urn:test:qv:counters:one");
        let graph_two = GraphId::new("urn:test:qv:counters:two");
        store.create_graph(&graph_one).unwrap();
        store.create_graph(&graph_two).unwrap();
        let one = encode_quad(
            &store,
            &graph_one,
            ("urn:test:s1", "urn:test:p1", "urn:test:o1"),
        );
        let two = encode_quad(
            &store,
            &graph_one,
            ("urn:test:s2", "urn:test:p1", "urn:test:o1"),
        );
        let three = encode_quad(
            &store,
            &graph_one,
            ("urn:test:s3", "urn:test:p2", "urn:test:o1"),
        );
        let four = encode_quad(
            &store,
            &graph_two,
            ("urn:test:s4", "urn:test:p1", "urn:test:o2"),
        );
        commit_add(&store, &graph_one, one);
        commit_add(&store, &graph_one, two);
        let three_dot = commit_add(&store, &graph_one, three);
        commit_add(&store, &graph_two, four);
        assert_index_ready(&store, 4);
        let one_query = test_query_quad(&store, one);
        let three_query = test_query_quad(&store, three);

        assert_eq!(test_index_counter(&store, IndexCounterKey::Total), Some(4));
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::Graph(one_query.graph)),
            Some(3)
        );
        assert_eq!(
            test_index_counter(&store, IndexCounterKey::Predicate(one_query.predicate),),
            Some(3)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::GraphPredicate(one_query.graph, one_query.predicate)
            ),
            Some(2)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::PredicateObject(one_query.predicate, one_query.object)
            ),
            Some(2)
        );
        assert_eq!(
            test_index_counter(
                &store,
                IndexCounterKey::GraphPredicateObject(
                    one_query.graph,
                    one_query.predicate,
                    one_query.object,
                )
            ),
            Some(2)
        );
        let mut witnessed = VectorClock::new();
        witnessed.advance(three_dot.actor, three_dot.counter);
        commit_remove(&store, &graph_one, three, &witnessed);
        assert_index_ready(&store, 3);
        for key in [
            IndexCounterKey::Predicate(three_query.predicate),
            IndexCounterKey::GraphPredicate(three_query.graph, three_query.predicate),
            IndexCounterKey::PredicateObject(three_query.predicate, three_query.object),
            IndexCounterKey::GraphPredicateObject(
                three_query.graph,
                three_query.predicate,
                three_query.object,
            ),
        ] {
            assert_eq!(test_index_counter(&store, key), None);
        }
    }

    #[test]
    fn final_removal_ready() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:last-row");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        let dot = commit_add(&store, &graph, quad);
        let query_quad = test_query_quad(&store, quad);

        let mut witnessed = VectorClock::new();
        witnessed.advance(dot.actor, dot.counter);
        commit_remove(&store, &graph, quad, &witnessed);

        assert_index_ready(&store, 0);
        let header = test_index_header(&store);
        assert_eq!(header.source_live_quads, 0);
        assert_eq!(header.indexed_quads, 0);
        assert_eq!(test_index_counter(&store, IndexCounterKey::Total), Some(0));
        for key in [
            IndexCounterKey::Graph(query_quad.graph),
            IndexCounterKey::Predicate(query_quad.predicate),
            IndexCounterKey::GraphPredicate(query_quad.graph, query_quad.predicate),
            IndexCounterKey::PredicateObject(query_quad.predicate, query_quad.object),
            IndexCounterKey::GraphPredicateObject(
                query_quad.graph,
                query_quad.predicate,
                query_quad.object,
            ),
        ] {
            assert_eq!(test_index_counter(&store, key), None);
        }
        let snapshot = store.db.snapshot();
        for keyspace in [
            &store.qv2_gspo,
            &store.qv2_gpos,
            &store.qv2_spog,
            &store.qv2_posg,
            &store.qv2_ospg,
            &store.qv2_gosp,
        ] {
            assert!(snapshot.iter(keyspace).next().is_none());
        }
    }

    #[test]
    fn index_keys_ordered() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:keys");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let snapshot = store.db.snapshot();
        let query_quad = test_query_quad(&store, quad);
        for (keyspace, key) in [
            (&store.qv2_gspo, gspo_key(query_quad)),
            (&store.qv2_gpos, gpos_key(query_quad)),
            (&store.qv2_spog, spog_key(query_quad)),
            (&store.qv2_posg, posg_key(query_quad)),
            (&store.qv2_ospg, ospg_key(query_quad)),
            (&store.qv2_gosp, gosp_key(query_quad)),
        ] {
            let value = snapshot.get(keyspace, key).unwrap().unwrap();
            assert!(value.as_ref().is_empty());
            let (stored_key, stored_value) = snapshot
                .iter(keyspace)
                .next()
                .unwrap()
                .into_inner()
                .unwrap();
            assert_eq!(stored_key.as_ref().len(), 32);
            assert!(stored_value.as_ref().is_empty());
        }
    }

    #[test]
    fn mismatched_index_rejected() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:status-qv-rows");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);

        remove_test_key(
            &store,
            &store.qv2_spog,
            spog_key(test_query_quad(&store, quad)),
        );
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_test_value(
            &store,
            &store.qv2_spog,
            spog_key(test_query_quad(&store, quad)),
            Vec::<u8>::new(),
        );
        stage_test_value(
            &store,
            &store.qv2_posg,
            posg_key(test_query_quad(&store, quad)),
            vec![1],
        );
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_test_value(
            &store,
            &store.qv2_posg,
            posg_key(test_query_quad(&store, quad)),
            Vec::<u8>::new(),
        );
        remove_test_key(
            &store,
            &store.qv2_spog,
            spog_key(test_query_quad(&store, quad)),
        );
        stage_test_value(&store, &store.qv2_spog, vec![0; 31], Vec::<u8>::new());
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        remove_test_key(&store, &store.qv2_spog, vec![0; 31]);
        stage_test_value(
            &store,
            &store.qv2_spog,
            spog_key(test_query_quad(&store, quad)),
            Vec::<u8>::new(),
        );
        stage_test_value(&store, &store.qv2_meta, QV_TOTAL_KEY, 2u64.to_be_bytes());
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );
    }

    #[test]
    fn index_status_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:fast-status-reopen");
        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_eq!(0, reopened.index_verify_count());
        let probes_before = reopened.admission_probe_count();
        let status = reopened.index_status_fast().unwrap();
        assert_eq!(QueryIndexState::Ready, status.state);
        assert_eq!(1, status.source_live_quads);
        assert_eq!(1, status.indexed_quads);
        assert_eq!(
            2,
            reopened.admission_probe_count() - probes_before,
            "fast status reads only the header and total counter"
        );
        assert_eq!(0, reopened.index_verify_count());

        let sampled = reopened
            .verify_query_indexes(IndexVerifyMode::Sample)
            .unwrap();
        assert!(sampled.valid);
        assert!(!sampled.full);
        assert_eq!(1, reopened.index_verify_count());
    }

    #[test]
    fn generations_never_reused() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:generation-floor");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        store.rebuild_query_indexes().unwrap();
        store.rebuild_query_indexes().unwrap();
        let used = test_index_header(&store).query_id_generation;
        assert!(
            used > 2,
            "the stored count must exceed a fresh header's first rebuild"
        );

        remove_test_key(&store, &store.qv2_meta, QV_HEADER_KEY);
        store.rebuild_query_indexes().unwrap();
        assert!(test_index_header(&store).query_id_generation > used);
        assert_index_ready(&store, 1);
    }

    #[test]
    fn reopened_rebuild_advances() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:rebuild-restart");
        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            remove_test_key(&store, &store.qv2_meta, QV_HEADER_KEY);
            store.persist().unwrap();
        }

        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_eq!(
                store.query_index_status().unwrap().state,
                QueryIndexState::Missing
            );
            let before_rebuild_sequence = store.db.snapshot().seqno();
            store.rebuild_query_indexes().unwrap();
            let first = test_index_header(&store);
            assert!(matches!(first.state, StoredIndexState::Ready));
            assert!(first.last_build_sequence >= before_rebuild_sequence);
            assert!(first.source_epoch >= before_rebuild_sequence);
            assert_eq!(first.source_live_quads, 1);
            assert_eq!(first.indexed_quads, 1);
            assert_index_ready(&store, 1);

            store.rebuild_query_indexes().unwrap();
            let second = test_index_header(&store);
            assert!(second.last_build_sequence > first.last_build_sequence);
            assert_eq!(second.source_epoch, first.source_epoch);
            assert!(second.query_id_generation > first.query_id_generation);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_index_ready(&reopened, 1);
    }

    #[test]
    fn interrupted_build_unpublished() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:interrupted-rebuild");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            let mut header = test_index_header(&store);
            header.state = StoredIndexState::Building;
            stage_test_header(&store, &header);
            remove_test_key(
                &store,
                &store.qv2_posg,
                posg_key(test_query_quad(&store, quad)),
            );
            store.persist().unwrap();
            quad
        };

        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_eq!(
                store.query_index_status().unwrap().state,
                QueryIndexState::Building
            );
            assert_eq!(
                store.quads_for_pattern(None, None, None, None).unwrap(),
                vec![quad],
                "Building must retain canonical fallback reads"
            );
            store.rebuild_query_indexes().unwrap();
            assert_index_ready(&store, 1);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_index_ready(&reopened, 1);
    }

    #[test]
    fn index_verification_deterministic() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:verify-sample");
        store.create_graph(&graph).unwrap();
        let rows = QV_SAMPLE_ROWS + 1;
        {
            let _guard = store.graph_commit_guard(&graph);
            let mut batch = store.new_batch();
            for index in 0..rows {
                let subject = format!("urn:test:qv:sample:{index}");
                let quad = encode_quad(&store, &graph, (&subject, "urn:test:p", "urn:test:o"));
                store
                    .insert_quad(
                        &mut batch,
                        QuadAdd {
                            quad,
                            dot: Dot {
                                actor: ActorId::random(),
                                counter: 1,
                            },
                        },
                    )
                    .unwrap();
            }
            store.commit(batch).unwrap();
        }

        let sample = store.verify_query_indexes(false).unwrap();
        assert!(sample.valid);
        assert!(!sample.full);
        assert_eq!(sample.source_live_quads, rows);
        assert_eq!(sample.indexed_quads, rows);
        assert_eq!(sample.checked_source_rows, QV_SAMPLE_ROWS);
        assert_eq!(sample.checked_index_rows, QV_SAMPLE_ROWS * 6);

        let full = store.verify_query_indexes(true).unwrap();
        assert!(full.valid);
        assert!(full.full);
        assert_eq!(full.source_live_quads, rows);
        assert_eq!(full.indexed_quads, rows);
        assert_eq!(full.checked_source_rows, rows);
        assert_eq!(full.checked_index_rows, rows * 6);
    }

    #[test]
    fn verification_detects_corruption() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:verification-corruption");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let query_quad = test_query_quad(&store, quad);
        let extra = QueryQuad {
            subject: QueryTermId(test_index_header(&store).next_query_id),
            ..query_quad
        };

        stage_test_value(&store, &store.qv2_gpos, gpos_key(extra), Vec::<u8>::new());
        stage_test_value(&store, &store.qv2_gpos, gpos_key(query_quad), vec![1]);
        remove_test_key(&store, &store.qv2_spog, spog_key(query_quad));
        stage_test_value(&store, &store.qv2_posg, vec![0; 31], Vec::<u8>::new());
        stage_test_value(&store, &store.qv2_meta, QV_TOTAL_KEY, vec![0; 7]);
        stage_test_value(&store, &store.qv2_meta, vec![b'Z'], 0u64.to_be_bytes());
        stage_test_value(&store, &store.qv2_meta, vec![b'G', 0], 0u64.to_be_bytes());
        let orphan_graph = QueryTermId(test_index_header(&store).next_query_id);
        stage_test_value(
            &store,
            &store.qv2_meta,
            IndexCounterKey::Graph(orphan_graph).bytes(),
            1u64.to_be_bytes(),
        );

        let report = store.verify_query_indexes(true).unwrap();
        assert!(!report.valid);
        for problem in [
            "source-gpos-missing-or-nonempty",
            "source-spog-missing-or-nonempty",
            "qv-gpos-value-nonempty",
            "qv-query-id-mapping-missing",
            "qv-posg-key-length",
            "meta-counter-value-length",
            "meta-unknown-tag",
            "meta-counter-key-length",
            "meta-counter-orphan",
        ] {
            assert_index_problem(&report, problem);
        }
    }

    #[test]
    fn anomalies_preserve_source() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-anomaly");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, first);
        remove_test_key(
            &store,
            &store.qv2_meta,
            IndexCounterKey::Predicate(test_query_id(&store, first.predicate)).bytes(),
        );

        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, second);

        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("maintenance-anomaly".to_owned())
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, None)
                .unwrap()
                .len(),
            2,
            "the canonical source commit must survive a derived-index anomaly"
        );
        let snapshot = store.db.snapshot();
        assert_eq!(snapshot.iter(&store.qv2_gpos).count(), 1);
    }

    #[test]
    fn ahead_header_rejected() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-ahead-header");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, first);
        let mut header = test_index_header(&store);
        let ahead = store.db.snapshot().seqno().checked_add(100).unwrap();
        header.source_epoch = ahead;
        header.index_epoch = ahead;
        stage_test_header(&store, &header);

        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, second);

        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-metadata-inconsistent".to_owned())
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, None)
                .unwrap()
                .len(),
            2,
            "the canonical source write must survive an ahead metadata hint"
        );
        let snapshot = store.db.snapshot();
        assert_eq!(snapshot.iter(&store.qv2_gpos).count(), 1);
    }

    #[test]
    fn orphan_counter_rejected() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-orphan-counter");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p1", "urn:test:o"));
        commit_add(&store, &graph, first);
        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p2", "urn:test:o"));
        let orphan_predicate = test_index_header(&store)
            .next_query_id
            .checked_add(1)
            .unwrap();
        stage_test_value(
            &store,
            &store.qv2_meta,
            IndexCounterKey::Predicate(QueryTermId(orphan_predicate)).bytes(),
            1u64.to_be_bytes(),
        );

        commit_add(&store, &graph, second);

        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("maintenance-anomaly".to_owned())
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, None)
                .unwrap()
                .len(),
            2,
            "the canonical source write must survive an orphan counter"
        );
        let snapshot = store.db.snapshot();
        assert_eq!(snapshot.iter(&store.qv2_gpos).count(), 1);
    }

    #[test]
    fn malformed_metadata_untrusted() {
        let metadata_dir = tempfile::tempdir().unwrap();
        let metadata_graph = GraphId::new("urn:test:qv:malformed-metadata");
        let metadata_quad = {
            let store = GraphStore::open(metadata_dir.path()).unwrap();
            store.create_graph(&metadata_graph).unwrap();
            let quad = encode_quad(
                &store,
                &metadata_graph,
                ("urn:test:s", "urn:test:p", "urn:test:o"),
            );
            commit_add(&store, &metadata_graph, quad);
            stage_test_value(&store, &store.qv2_meta, QV_HEADER_KEY, vec![0]);
            store.persist().unwrap();
            quad
        };
        let metadata_reopened = GraphStore::open(metadata_dir.path()).unwrap();
        assert_eq!(
            metadata_reopened.query_index_status().unwrap().state,
            QueryIndexState::Failed("metadata-malformed".to_owned())
        );
        assert_eq!(
            metadata_reopened
                .quads_for_pattern(None, None, None, None)
                .unwrap(),
            vec![metadata_quad]
        );
        drop(metadata_reopened);

        let counter_dir = tempfile::tempdir().unwrap();
        let counter_graph = GraphId::new("urn:test:qv:malformed-counter");
        let counter_quad = {
            let store = GraphStore::open(counter_dir.path()).unwrap();
            store.create_graph(&counter_graph).unwrap();
            let quad = encode_quad(
                &store,
                &counter_graph,
                ("urn:test:s", "urn:test:p", "urn:test:o"),
            );
            commit_add(&store, &counter_graph, quad);
            stage_test_value(&store, &store.qv2_meta, QV_TOTAL_KEY, vec![0; 7]);
            store.persist().unwrap();
            quad
        };
        let counter_reopened = GraphStore::open(counter_dir.path()).unwrap();
        assert_eq!(
            counter_reopened.query_index_status().unwrap().state,
            QueryIndexState::Failed("open-admission-failed".to_owned())
        );
        assert_eq!(
            counter_reopened
                .quads_for_pattern(None, None, None, None)
                .unwrap(),
            vec![counter_quad]
        );
    }

    #[test]
    fn epoch_mismatch_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:epoch-mismatch");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            let mut header = test_index_header(&store);
            header.index_epoch = header.index_epoch.checked_add(1).unwrap();
            stage_test_header(&store, &header);
            store.persist().unwrap();
            quad
        };

        let store = GraphStore::open(dir.path()).unwrap();
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("open-admission-failed".to_owned())
        );
        let source_before_rebuild = store.quads_for_pattern(None, None, None, None).unwrap();
        assert_eq!(source_before_rebuild, vec![quad]);
        assert!(!store.verify_query_indexes(true).unwrap().valid);

        store.rebuild_query_indexes().unwrap();
        assert_index_ready(&store, 1);
        assert_eq!(
            store.quads_for_pattern(None, None, None, None).unwrap(),
            source_before_rebuild,
            "rebuild must never mutate canonical source rows"
        );
    }

    #[test]
    fn rebuild_discards_hints() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:ahead-hints");
        let first = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, first);
            let mut header = test_index_header(&store);
            header.source_epoch = u64::MAX;
            header.index_epoch = u64::MAX;
            header.last_build_sequence = u64::MAX;
            stage_test_header(&store, &header);
            store.persist().unwrap();
            first
        };

        let store = GraphStore::open(dir.path()).unwrap();
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("open-admission-failed".to_owned())
        );
        let failed_report = store.verify_query_indexes(true).unwrap();
        assert!(!failed_report.valid);
        assert_index_problem(&failed_report, "meta-epoch-ahead-of-snapshot");
        assert_index_problem(&failed_report, "meta-build-sequence-ahead-of-snapshot");
        assert_eq!(
            store.quads_for_pattern(None, None, None, None).unwrap(),
            vec![first]
        );

        store.rebuild_query_indexes().unwrap();
        assert_index_ready(&store, 1);
        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, second);
        assert_index_ready(&store, 2);
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, None)
                .unwrap()
                .len(),
            2,
            "rebuild and the later live write must leave canonical source rows intact"
        );
    }

    #[test]
    fn compaction_preserves_keyspaces() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:manual-compact");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            store.manual_compact().unwrap();
            let snapshot = store.db.snapshot();
            let query_quad = test_query_quad(&store, quad);
            for (keyspace, key) in [
                (&store.qv2_gspo, gspo_key(query_quad)),
                (&store.qv2_gpos, gpos_key(query_quad)),
                (&store.qv2_spog, spog_key(query_quad)),
                (&store.qv2_posg, posg_key(query_quad)),
                (&store.qv2_ospg, ospg_key(query_quad)),
                (&store.qv2_gosp, gosp_key(query_quad)),
            ] {
                assert!(
                    snapshot
                        .get(keyspace, key)
                        .unwrap()
                        .unwrap()
                        .as_ref()
                        .is_empty()
                );
            }
            assert!(
                snapshot
                    .get(&store.qv2_meta, QV_HEADER_KEY)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                snapshot
                    .get(&store.qv2_meta, QV_TOTAL_KEY)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &1u64.to_be_bytes()
            );
            store.persist().unwrap();
            quad
        };

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_index_ready(&reopened, 1);
        assert_eq!(
            reopened.quads_for_pattern(None, None, None, None).unwrap(),
            vec![quad]
        );
    }

    #[test]
    fn queries_read_source() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        let subject = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:s"));
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p"));
        let object = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:o"));

        insert_quad(
            &store,
            &graph,
            &subject,
            &predicate,
            &object,
            Dot {
                actor: ActorId::random(),
                counter: 1,
            },
        );

        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();
        let subject_id = store.lookup_term(&subject).unwrap().unwrap();
        let quads = store
            .quads_for_pattern(Some(graph_id), Some(subject_id), None, None)
            .unwrap();
        assert_eq!(1, quads.len());
        assert_eq!(
            quads[0].object,
            store.lookup_term(&object).unwrap().unwrap()
        );
    }

    #[test]
    fn patterns_track_commits() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        let subject = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:s"));
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p"));
        let object = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:o"));

        // The compatibility entry point must not change durable reads.
        store.ensure_derived_indexes();

        let actor = ActorId::random();
        insert_quad(
            &store,
            &graph,
            &subject,
            &predicate,
            &object,
            Dot { actor, counter: 1 },
        );

        let subject_id = store.lookup_term(&subject).unwrap().unwrap();
        let object_id = store.lookup_term(&object).unwrap().unwrap();
        let predicate_id = store.lookup_term(&predicate).unwrap().unwrap();
        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();

        let quads = store
            .quads_for_pattern(None, Some(subject_id), None, None)
            .unwrap();
        assert_eq!(1, quads.len());
        assert_eq!(quads[0].object, object_id);
        let quads = store
            .quads_for_pattern(None, None, None, Some(object_id))
            .unwrap();
        assert_eq!(1, quads.len());
        let quads = store
            .quads_for_pattern(None, None, Some(predicate_id), Some(object_id))
            .unwrap();
        assert_eq!(1, quads.len());

        let mut witnessed = VectorClock::new();
        witnessed.advance(actor, 1);
        let mut batch = store.new_batch();
        store
            .remove_quad(
                &mut batch,
                QuadRemove {
                    quad: EncodedQuad {
                        graph: graph_id,
                        subject: subject_id,
                        predicate: predicate_id,
                        object: object_id,
                    },
                    witnessed: &witnessed,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();

        assert!(
            store
                .quads_for_pattern(None, Some(subject_id), None, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .quads_for_pattern(None, None, None, Some(object_id))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn dirty_subjects_deduplicate() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();
        let subject = store
            .resolve_term(&EncodedTerm::from_named_node(
                &oxrdf::NamedNode::new_unchecked("urn:test:subject"),
            ))
            .unwrap();

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.commit(batch).unwrap();

        let queued = store.drain_fts_queue(10).unwrap();
        assert_eq!(1, queued.len());
        store.acknowledge_fts_queue(&queued).unwrap();
        assert!(store.drain_fts_queue(10).unwrap().is_empty());
    }

    /// Redirtying preserves the oldest token promised to an earlier flush.
    #[test]
    fn enqueue_keeps_oldest() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject = store.resolve_term(&named("urn:test:subject")).unwrap();
        let other = store.resolve_term(&named("urn:test:other")).unwrap();

        let enqueue = |subject| {
            let mut batch = store.new_batch();
            store
                .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
                .unwrap();
            store.commit(batch).unwrap();
        };

        enqueue(subject);
        // What a flush starting right here would pin.
        let bound = QueueScan {
            max_token: Some(store.current_dirty_token()),
            after: None,
            row_limit: 10,
            byte_limit: 1_048_576,
        };
        // Carry the counter past the bound, then dirty the subject again.
        enqueue(other);
        enqueue(subject);

        let drained = store.scan_fts_subjects(&bound).unwrap();

        assert!(drained.entries.iter().any(|entry| entry.subject == subject));
    }

    #[test]
    fn token_head_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:token-head");
        let committed;
        {
            let store = GraphStore::open(directory.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let graph_id = store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap();
            let subject = store.resolve_term(&named("urn:test:subject")).unwrap();
            let mut batch = store.new_batch();
            store
                .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
                .unwrap();
            store.commit(batch).unwrap();
            committed = store.current_dirty_token();
            store.clear_graph_queue(&graph, committed).unwrap();
            store.persist().unwrap();
        }
        let reopened = GraphStore::open(directory.path()).unwrap();
        assert_eq!(reopened.current_dirty_token(), committed);
    }

    /// An enqueue concurrent with acknowledgement must retain its newer debt.
    #[test]
    fn acknowledgement_preserves_enqueue() {
        let (_dir, store) = setup_store();
        let store = Arc::new(store);
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject = store.resolve_term(&named("urn:test:subject")).unwrap();

        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.commit(batch).unwrap();
        let queued = store.drain_fts_queue(10).unwrap();
        assert_eq!(1, queued.len());

        store.set_ack_stall(std::time::Duration::from_millis(300));
        let acking = {
            let store = store.clone();
            std::thread::spawn(move || store.acknowledge_fts_queue(&queued).unwrap())
        };

        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.commit(batch).unwrap();
        acking.join().unwrap();

        assert_eq!(1, store.drain_fts_queue(10).unwrap().len());
    }

    #[test]
    fn reindex_queue_roundtrips() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let mut batch = store.new_batch();
        store.enqueue_fts_reindex(&mut batch, graph_id).unwrap();
        store.commit(batch).unwrap();

        let queued = store.drain_reindex_queue(10).unwrap();
        assert_eq!(1, queued.len());
        assert_eq!(queued[0].graph, graph);
        store.acknowledge_reindex(&queued).unwrap();
        assert!(store.drain_reindex_queue(10).unwrap().is_empty());
    }

    // Reindex collapse relative to graph size.

    /// Seeds distinct subjects and returns the graph and subject ids.
    fn seed_subjects(store: &GraphStore, graph: &GraphId, count: usize) -> (TermId, Vec<TermId>) {
        let mut batch = store.new_batch();
        let mut cache = HashMap::new();
        let mut cx = BatchTermCtx {
            batch: &mut batch,
            cache: &mut cache,
        };
        let mut resolve = |term| store.resolve_term_cached(&mut cx, &term).unwrap();

        let graph_id = resolve(EncodedTerm::from_named_node(&graph.0));
        let predicate = resolve(named("urn:test:w14:p"));
        let object = resolve(named("urn:test:w14:o"));
        let subjects: Vec<TermId> = (0..count)
            .map(|i| resolve(named(&format!("urn:test:w14:s{i}"))))
            .collect();

        let actor = ActorId::random();
        for (i, subject) in subjects.iter().enumerate() {
            store
                .insert_quad(
                    &mut batch,
                    QuadAdd {
                        quad: EncodedQuad {
                            graph: graph_id,
                            subject: *subject,
                            predicate,
                            object,
                        },
                        dot: Dot {
                            actor,
                            counter: i as u64 + 1,
                        },
                    },
                )
                .unwrap();
        }
        store.commit(batch).unwrap();

        assert_eq!(count, store.graph_subject_count(graph_id).unwrap());
        (graph_id, subjects)
    }

    /// Enqueue `subjects` for `graph_id` and report `(subject entries, reindex
    /// entries)` the enqueue produced.
    fn enqueue_and_count(
        store: &GraphStore,
        graph_id: TermId,
        subjects: &[TermId],
    ) -> (usize, usize) {
        let subjects: HashSet<TermId> = subjects.iter().copied().collect();
        let mut batch = store.new_batch();
        store
            .enqueue_fts_subjects(
                &mut batch,
                FtsEnqueue {
                    graph_id,
                    subjects: &subjects,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();

        let per_subject = store.drain_fts_queue(usize::MAX).unwrap().len();
        let reindexes = store.drain_reindex_queue(usize::MAX).unwrap().len();
        (per_subject, reindexes)
    }

    /// A large batch covering half the graph collapses to one rescan.
    #[test]
    fn enqueue_collapses_batch() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:half");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) = seed_subjects(&store, &graph, FTS_REINDEX_THRESHOLD * 2);
        let batch = &subjects[..FTS_REINDEX_THRESHOLD];

        assert_eq!((0, 1), enqueue_and_count(&store, graph_id, batch));
    }

    /// A batch below half the graph remains per-subject.
    #[test]
    fn enqueue_below_ratio() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:dwarfed");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) = seed_subjects(&store, &graph, FTS_REINDEX_THRESHOLD * 2 + 1);
        let batch = &subjects[..FTS_REINDEX_THRESHOLD];

        // Every affected subject is queued: the relative rule may only move
        // work off the rescan branch, never drop it.
        assert_eq!(
            (FTS_REINDEX_THRESHOLD, 0),
            enqueue_and_count(&store, graph_id, batch)
        );
    }

    /// Small whole-graph batches remain per-subject.
    #[test]
    fn enqueue_below_threshold() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:small");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) = seed_subjects(&store, &graph, 100);

        assert_eq!((100, 0), enqueue_and_count(&store, graph_id, &subjects));
    }

    // Commit guards and cache publication.

    /// Concurrent adds to one quad retain distinct dots.
    #[test]
    fn commits_keep_dots() {
        const WRITERS: usize = 8;
        const ADDS_PER_WRITER: usize = 25;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(GraphStore::open(dir.path()).unwrap());
        let graph = GraphId::new("urn:test:parallel-commits");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));

        std::thread::scope(|scope| {
            for _ in 0..WRITERS {
                let store = Arc::clone(&store);
                let graph = graph.clone();
                scope.spawn(move || {
                    for _ in 0..ADDS_PER_WRITER {
                        commit_add(&store, &graph, quad);
                    }
                });
            }
        });

        let snapshot = store.graph_snapshot(&graph).unwrap();
        assert_eq!(1, snapshot.quads.len(), "all writers target the same quad");

        let dots = &snapshot.quads[0].dots;
        assert_eq!(
            WRITERS * ADDS_PER_WRITER,
            dots.len(),
            "every add must contribute its own dot; a shorter set means adds were lost"
        );
        let unique: HashSet<(ActorId, u64)> =
            dots.iter().map(|dot| (dot.actor, dot.counter)).collect();
        assert_eq!(dots.len(), unique.len(), "two adds shared a dot");

        // Every minted counter is reflected in the graph clock.
        let clock = store.get_vector_clock(&graph).unwrap();
        let clocked: u64 = clock.0.values().sum();
        assert_eq!(dots.len() as u64, clocked);
    }

    #[test]
    fn graph_commits_overlap() {
        let (_dir, store) = setup_store();
        let first = GraphId::new("urn:test:independent-commit:first");
        let second = GraphId::new("urn:test:independent-commit:second");
        store.create_graph(&first).unwrap();
        store.create_graph(&second).unwrap();
        let first_quad = encode_quad(&store, &first, ("urn:s:1", "urn:p", "urn:o"));
        let second_quad = encode_quad(&store, &second, ("urn:s:2", "urn:p", "urn:o"));
        store.set_commit_stall(std::time::Duration::from_millis(200));

        std::thread::scope(|scope| {
            scope.spawn(|| {
                commit_add(&store, &first, first_quad);
            });
            scope.spawn(|| {
                commit_add(&store, &second, second_quad);
            });
        });

        assert!(
            store.peak_commit_stalls() >= 2,
            "independent durable commits were serialized by one cache lock"
        );
        assert_index_ready(&store, 2);
    }

    #[test]
    fn failure_preserves_caches() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:failed-commit-cache");
        store.create_graph(&graph).unwrap();
        let seeded = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:seeded"));
        let rejected = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:rejected"));
        commit_add(&store, &graph, seeded);
        assert!(store.index_contains(seeded));
        let generation = store.indexes_read().graph_epoch(seeded.graph);

        let _commit_guard = store.graph_commit_guard(&graph);
        let mut batch = store.new_batch();
        store
            .insert_quad(
                &mut batch,
                QuadAdd {
                    quad: rejected,
                    dot: Dot {
                        actor: ActorId::random(),
                        counter: 1,
                    },
                },
            )
            .unwrap();
        store.arm_commit_failure();
        assert!(store.commit(batch).is_err());

        assert_eq!(generation, store.indexes_read().graph_epoch(seeded.graph));
        assert!(store.index_contains(seeded));
        assert!(!store.contains_quad(rejected).unwrap());
    }

    #[test]
    fn restart_rebuilds_caches() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:durable-before-cache");
        let quad;
        {
            let store = GraphStore::open(directory.path()).unwrap();
            store.create_graph(&graph).unwrap();
            quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
            let actor = ActorId::random();
            let dot = Dot { actor, counter: 1 };
            let mut batch = store.new_batch();
            store
                .insert_quad(&mut batch, QuadAdd { quad, dot })
                .unwrap();
            let mut clock = VectorClock::new();
            clock.advance(actor, 1);
            store
                .set_vector_clock(
                    &mut batch,
                    ClockUpdate {
                        graph_id: quad.graph,
                        clock: &clock,
                    },
                )
                .unwrap();
            let WriteBatch {
                inner,
                pending_quad_states: _,
                pending_terms: _,
                publish,
                pending_fts,
                pending_receipts: _,
                receipt_trim: _,
            } = batch;
            let mut durable = DurableCommit {
                batch: inner,
                pending_fts,
                pending_receipts: Vec::new(),
                receipt_trim: ReceiptTrim::default(),
            };
            let owner = store.qv_gate.try_acquire().unwrap();
            store
                .stage_index_update(&mut durable.batch, &publish)
                .unwrap();
            store.commit_durable(durable).unwrap();
            owner.finish();
            store.persist().unwrap();
            // Deliberately omit `indexes.publish(&publish)`: this is the crash
            // window after durable commit and before cache publication.
        }

        let reopened = GraphStore::open(directory.path()).unwrap();
        assert!(reopened.contains_quad(quad).unwrap());
        assert_eq!(
            1,
            reopened
                .subject_triple_count(quad.graph, quad.subject)
                .unwrap()
        );
    }

    #[test]
    fn cache_statistics_bounded() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:bounded-cache-stats");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);

        store.triples_for_subject(quad.graph, quad.subject).unwrap();
        store.triples_for_subject(quad.graph, quad.subject).unwrap();
        let subject = named("urn:s");
        let predicate = named("urn:p");
        for _ in 0..2 {
            store
                .objects_page(
                    GraphSubjectPredicate {
                        graph: &graph,
                        subject: &subject,
                        predicate: &predicate,
                    },
                    PageRequest {
                        cursor: PageCursor::Offset(0),
                        limit: 1,
                    },
                )
                .unwrap();
        }
        store.decode_term_arc(quad.object).unwrap();
        store.decode_term_arc(quad.object).unwrap();

        for statistics in store.cache_statistics() {
            assert!(statistics.entries > 0);
            assert!(statistics.bytes > 0);
            assert!(statistics.hits > 0);
            assert!(statistics.misses > 0);
            assert_eq!(0, statistics.evictions);
        }
    }

    /// Concurrent self-guarding calls make progress even on one lock shard.
    #[test]
    fn guards_never_deadlock() {
        const THREADS: usize = 8;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(GraphStore::open(dir.path()).unwrap());
        let (tx, rx) = std::sync::mpsc::channel();

        let handles: Vec<_> = (0..THREADS)
            .map(|index| {
                let store = Arc::clone(&store);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    // Two threads per graph, so the same shard is contended.
                    let graph = GraphId::new(&format!("urn:test:self-guard:{}", index % 4));
                    for round in 0..20u64 {
                        store.create_graph(&graph).unwrap();
                        store
                            .set_graph_policy(&graph, &GraphPolicy::default())
                            .unwrap();
                        store
                            .set_topic_id(&graph, [(index * 32 + round as usize) as u8; 32])
                            .unwrap();
                        store
                            .set_graph_context(
                                &graph,
                                Some("{}"),
                                None,
                                None,
                                ContextTag {
                                    counter: round + 1,
                                    actor: ActorId::random(),
                                },
                            )
                            .unwrap();
                        store.set_graph_tombstone(&graph).unwrap();
                        store.delete_graph(&graph).unwrap();
                    }
                    tx.send(index).unwrap();
                })
            })
            .collect();
        drop(tx);

        for _ in 0..THREADS {
            rx.recv_timeout(std::time::Duration::from_secs(120))
                .expect("self-guarding store functions deadlocked");
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    /// A graph generation change makes every older subject cache entry stale,
    /// including unrelated entries that were locally corrupted.
    #[test]
    fn generation_invalidates_cache() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:index-anomaly");
        store.create_graph(&graph).unwrap();

        let removed = encode_quad(&store, &graph, ("urn:s1", "urn:p", "urn:o1"));
        let collateral = encode_quad(&store, &graph, ("urn:s2", "urn:p", "urn:o2"));
        let removed_dot = commit_add(&store, &graph, removed);
        commit_add(&store, &graph, collateral);
        assert!(store.index_contains(removed));
        assert!(store.index_contains(collateral));

        // Simulate drift in two subject-cache entries while durable source
        // still holds both quads.
        store.corrupt_test_index(removed);
        store.corrupt_test_index(collateral);
        assert!(!store.index_contains(removed));
        assert!(!store.index_contains(collateral));

        // Retracting one quad advances the graph generation.
        let mut witnessed = VectorClock::new();
        witnessed.advance(removed_dot.actor, removed_dot.counter);
        let _commit_guard = store.graph_commit_guard(&graph);
        let mut batch = store.new_batch();
        assert!(
            store
                .remove_quad(
                    &mut batch,
                    QuadRemove {
                        quad: removed,
                        witnessed: &witnessed,
                    },
                )
                .unwrap()
        );
        store.commit(batch).unwrap();

        assert!(
            store.index_contains(collateral),
            "the new generation must reload unrelated cached subjects"
        );
        assert!(
            !store.index_contains(removed),
            "the new generation must read the durable removal"
        );
        assert!(!store.contains_quad(removed).unwrap());
        assert!(store.contains_quad(collateral).unwrap());
    }

    // FTS queue tokens across restarts.

    /// A pre-restart reindex cannot acknowledge post-restart subject debt.
    #[test]
    fn tokens_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:fts-token-restart");

        let reindex_token = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let graph_id = store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap();

            let mut batch = store.new_batch();
            for name in ["urn:pre1", "urn:pre2", "urn:pre3"] {
                let subject = store.resolve_term(&named(name)).unwrap();
                store
                    .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
                    .unwrap();
            }
            store.enqueue_fts_reindex(&mut batch, graph_id).unwrap();
            store.commit(batch).unwrap();
            store.persist().unwrap();

            let queued = store.drain_reindex_queue(10).unwrap();
            assert_eq!(1, queued.len());
            // The pre-restart subject entries are legitimately covered.
            store.acknowledge_reindexed(&queued).unwrap();
            assert!(store.drain_fts_queue(10).unwrap().is_empty());
            queued[0].tokens.latest
        };

        let store = GraphStore::open(dir.path()).unwrap();
        assert_eq!(store.current_dirty_token(), reindex_token);

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject = store.resolve_term(&named("urn:post-restart")).unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.commit(batch).unwrap();
        assert!(store.current_dirty_token() > reindex_token);

        let reindex_queued = store.drain_reindex_queue(10).unwrap();
        assert_eq!(1, reindex_queued.len());
        assert_eq!(graph, reindex_queued[0].graph);
        assert_eq!(reindex_token, reindex_queued[0].tokens.latest);
        store.acknowledge_reindexed(&reindex_queued).unwrap();

        let remaining = store.drain_fts_queue(10).unwrap();
        assert_eq!(
            1,
            remaining.len(),
            "a subject queued after the reindex must survive its acknowledgement"
        );
        assert_eq!(subject, remaining[0].subject);
    }

    // Vector-clock key split.

    /// Open migrates clocks still embedded in legacy graph metadata.
    #[test]
    fn clock_split_migration() {
        let (dir, store) = setup_store();
        let graph = GraphId::new("urn:test:clock-migration");
        store.create_graph(&graph).unwrap();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();

        // A store written before the split: the clock lives inside the meta
        // record and there is no 'K' key.
        let legacy_actor = ActorId::random();
        let mut legacy_clock = VectorClock::new();
        legacy_clock.advance(legacy_actor, 7);
        let mut meta = store.read_graph_meta(graph_id).unwrap().unwrap();
        meta.clock = legacy_clock.clone();
        let mut batch = store.new_batch();
        batch.insert(
            &store.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta).unwrap(),
        );
        store.commit(batch).unwrap();
        assert!(
            store
                .graphs
                .get(graph_clock_key(graph_id))
                .unwrap()
                .is_none()
        );
        drop(store);

        let store = GraphStore::open(dir.path()).unwrap();
        assert_eq!(legacy_clock, store.get_vector_clock(&graph).unwrap());

        // The first clock write creates 'K', which wins from then on.
        let mut fresh = legacy_clock.clone();
        fresh.advance(legacy_actor, 9);
        let mut batch = store.new_batch();
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id,
                    clock: &fresh,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();

        assert_eq!(fresh, store.get_vector_clock(&graph).unwrap());
        // The clock write must not have touched the metadata record.
        let meta_after = store.read_graph_meta(graph_id).unwrap().unwrap();
        assert_eq!(legacy_clock, meta_after.clock);
    }

    #[test]
    fn deleted_clock_resets() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:clock-resurrection");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        assert!(!store.get_vector_clock(&graph).unwrap().0.is_empty());

        store.delete_graph(&graph).unwrap();
        assert_eq!(VectorClock::new(), store.get_vector_clock(&graph).unwrap());

        store.create_graph(&graph).unwrap();
        assert_eq!(
            VectorClock::new(),
            store.get_vector_clock(&graph).unwrap(),
            "a recreated graph must not inherit the deleted graph's clock"
        );
        assert!(
            store
                .graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty()
        );
    }

    // Persisted, clock-tagged diagnostics.

    /// Persists diagnostics under the same guard used by writers.
    fn settle_diagnostics(store: &GraphStore, graph: &GraphId) {
        let _commit_guard = store.graph_commit_guard(graph);
        let diagnostics = store.compute_graph_diagnostics(graph).unwrap();
        store.set_graph_diagnostics(graph, &diagnostics).unwrap();
    }

    /// Readers cannot combine a post-commit clock with pre-commit graph state.
    #[test]
    fn commit_publishes_atomically() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:commit-atomicity");
        store.create_graph(&graph).unwrap();
        commit_orphan(&store, &graph, "urn:test:first");
        settle_diagnostics(&store, &graph);

        let before = store.get_vector_clock(&graph).unwrap();
        store.set_commit_stall(std::time::Duration::from_millis(300));

        std::thread::scope(|scope| {
            scope.spawn(|| commit_orphan(&store, &graph, "urn:test:second"));

            // Spin on the clock, which the same batch published, until the
            // durable half of that commit lands.
            while store.get_vector_clock(&graph).unwrap() == before {
                std::hint::spin_loop();
            }
            assert_eq!(
                2,
                store
                    .graph_diagnostics(&graph)
                    .unwrap()
                    .orphaned_entities
                    .len(),
                "a reader past the new clock must see the index that clock describes"
            );
        });
    }

    /// Spin until a stalling thread reports it is inside its window, failing
    /// rather than hanging if that thread died before it got there.
    fn spin_until(entered: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !entered() {
            assert!(
                std::time::Instant::now() < deadline,
                "the stall window was never entered"
            );
            std::hint::spin_loop();
            std::thread::yield_now();
        }
    }

    /// The graph's quads as the index sees them and as the store holds them.
    fn index_and_store(
        store: &GraphStore,
        graph_id: TermId,
    ) -> (Vec<EncodedQuad>, Vec<EncodedQuad>) {
        let mut indexed = Vec::new();
        store
            .visit_graph_quads::<StoreError, _>(graph_id, |quad| {
                indexed.push(quad);
                Ok(())
            })
            .unwrap();
        let mut stored = Vec::new();
        store
            .visit_stored_quads(graph_id, |quad, _| {
                stored.push(quad);
                Ok(())
            })
            .unwrap();
        let key = |quad: &EncodedQuad| (quad.subject, quad.predicate, quad.object);
        indexed.sort_by_key(key);
        stored.sort_by_key(key);
        (indexed, stored)
    }

    /// Cache rebuilds preserve commits landing during their scan.
    #[test]
    fn rebuild_keeps_commits() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:rebuild-race");
        store.create_graph(&graph).unwrap();
        let seeded = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:seeded"));
        commit_add(&store, &graph, seeded);
        let raced = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:raced"));

        store.set_rebuild_stall(std::time::Duration::from_millis(300));
        std::thread::scope(|scope| {
            scope.spawn(|| store.rebuild_indexes().unwrap());
            spin_until(|| store.rebuild_stalled());
            commit_add(&store, &graph, raced);
        });

        let (indexed, stored) = index_and_store(&store, raced.graph);
        assert!(
            indexed.contains(&raced),
            "a commit that landed during a rebuild must survive its install"
        );
        assert_eq!(stored, indexed, "the index must describe the stored quads");
    }

    /// Object-order fills cannot overwrite a concurrent invalidation.
    #[test]
    fn paging_sees_appends() {
        const SEEDED: usize = 400;
        const APPENDED: usize = 150;

        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:order-cache");
        store.create_graph(&graph).unwrap();
        let has_part = crate::core::vocab::schema_has_part();
        let root = EncodedTerm::from_named_node(&graph.0);
        let predicate = EncodedTerm::from_named_node(&has_part);
        let append = |index: usize| {
            let child = format!("urn:test:child-{index:04}");
            let quad = encode_quad(&store, &graph, (graph.as_str(), has_part.as_str(), &child));
            commit_add(&store, &graph, quad);
        };
        for index in 0..SEEDED {
            append(index);
        }

        let total_objects = || {
            store
                .count_matching_objects(&graph, &root, &predicate)
                .unwrap()
        };
        let first_page = || {
            store
                .objects_page(
                    GraphSubjectPredicate {
                        graph: &graph,
                        subject: &root,
                        predicate: &predicate,
                    },
                    PageRequest {
                        cursor: PageCursor::Offset(0),
                        limit: 8,
                    },
                )
                .unwrap()
                .0
        };

        let done = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..3 {
                scope.spawn(|| {
                    while !done.load(Ordering::Relaxed) {
                        // Exercise cache repopulation while writes publish;
                        // exact post-write freshness is asserted below.
                        let _ = first_page();
                    }
                });
            }
            for index in SEEDED..SEEDED + APPENDED {
                append(index);
            }
            done.store(true, Ordering::Relaxed);
        });

        assert_eq!(SEEDED + APPENDED, total_objects());
        assert_eq!(
            SEEDED + APPENDED,
            first_page(),
            "an ordering cached over a newer one is served until the next write"
        );
    }

    fn commit_orphan(store: &GraphStore, graph: &GraphId, entity: &str) {
        let quad = encode_quad(
            store,
            graph,
            (
                entity,
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://schema.org/MediaObject",
            ),
        );
        commit_add(store, graph, quad);
    }

    /// Id-based orphan detection matches the rules implementation across graph shapes.
    #[test]
    fn orphan_ids_match() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:orphan-parity");
        store.create_graph(&graph).unwrap();

        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let has_part = "http://schema.org/hasPart";
        let media = "http://schema.org/MediaObject";
        let dataset = "http://schema.org/Dataset";
        let person = "http://schema.org/Person";
        let root = graph.as_str().to_string();

        for triple in [
            // Reachable in one hop.
            (root.as_str(), has_part, "urn:e:reachable"),
            ("urn:e:reachable", rdf_type, media),
            // Reachable through a two-hop chain.
            ("urn:e:reachable", has_part, "urn:e:grandchild"),
            ("urn:e:grandchild", rdf_type, media),
            // Typed data entity nothing points at.
            ("urn:e:orphan", rdf_type, media),
            // Orphan that is a Dataset rather than a MediaObject.
            ("urn:e:orphan-dataset", rdf_type, dataset),
            // A `hasPart` cycle with no path from the root: both ends orphaned,
            // and both count as data entities purely from the edge.
            ("urn:e:cycle-a", has_part, "urn:e:cycle-b"),
            ("urn:e:cycle-b", has_part, "urn:e:cycle-a"),
            // Only ever a `hasPart` object, never typed, and unreachable.
            ("urn:e:orphan-parent", has_part, "urn:e:untyped-child"),
            // Typed as something that is not a data entity: never an orphan.
            ("urn:e:person", rdf_type, person),
            // The root's own type must not make the root an orphan.
            (root.as_str(), rdf_type, dataset),
        ] {
            let quad = encode_quad(&store, &graph, triple);
            commit_add(&store, &graph, quad);
        }

        let snapshot = crate::rules::GraphSnapshot::from_store(&store, &graph).unwrap();
        let expected = GraphDiagnostics::from_orphaned_entities(
            crate::rules::orphaned_data_entities(&snapshot)
                .into_iter()
                .map(|term| {
                    term.to_named_node()
                        .map(|named_node| named_node.as_str().to_string())
                        .unwrap_or(term.0)
                })
                .collect(),
        );

        assert!(
            expected.has_orphans(),
            "the fixture must actually produce orphans, or this proves nothing"
        );
        assert_eq!(
            expected,
            store.compute_graph_diagnostics(&graph).unwrap(),
            "the id-based orphan pass disagrees with rules::orphaned_data_entities"
        );
    }

    /// A graph with no orphans at all must also agree, and must not invent one
    /// for the root.
    #[test]
    fn orphan_ids_empty() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:orphan-parity-clean");
        store.create_graph(&graph).unwrap();

        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let has_part = "http://schema.org/hasPart";
        let media = "http://schema.org/MediaObject";
        let root = graph.as_str().to_string();

        for triple in [
            (root.as_str(), rdf_type, "http://schema.org/Dataset"),
            (root.as_str(), has_part, "urn:e:one"),
            ("urn:e:one", rdf_type, media),
            ("urn:e:one", has_part, "urn:e:two"),
            ("urn:e:two", rdf_type, media),
        ] {
            let quad = encode_quad(&store, &graph, triple);
            commit_add(&store, &graph, quad);
        }

        let snapshot = crate::rules::GraphSnapshot::from_store(&store, &graph).unwrap();
        assert!(crate::rules::orphaned_data_entities(&snapshot).is_empty());
        assert!(
            !store
                .compute_graph_diagnostics(&graph)
                .unwrap()
                .has_orphans()
        );
    }

    #[test]
    fn diagnostics_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:diagnostics-reopen");

        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            commit_orphan(&store, &graph, "urn:orphan:a");
            settle_diagnostics(&store, &graph);
            assert_eq!(
                vec!["urn:orphan:a".to_string()],
                store.graph_diagnostics(&graph).unwrap().orphaned_entities
            );
            store.persist().unwrap();
        }

        let store = GraphStore::open(dir.path()).unwrap();
        assert_eq!(
            0,
            store.diagnostics_compute_count(),
            "opening with a matching clock tag must reuse the persisted record"
        );
        assert_eq!(
            vec!["urn:orphan:a".to_string()],
            store.graph_diagnostics(&graph).unwrap().orphaned_entities
        );
        assert_eq!(
            0,
            store.diagnostics_compute_count(),
            "reads with a matching tag must not recompute either"
        );
    }

    /// Open and reads repair diagnostics left stale by an interrupted writer.
    #[test]
    fn crash_repairs_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:diagnostics-crash");

        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            commit_orphan(&store, &graph, "urn:orphan:known");
            assert_eq!(
                vec!["urn:orphan:known".to_string()],
                store.graph_diagnostics(&graph).unwrap().orphaned_entities
            );

            // A raw commit: quads and clock advance durably, diagnostics never
            // refreshed. The persisted record now describes an older state.
            let quad = encode_quad(
                &store,
                &graph,
                (
                    "urn:orphan:crashed",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://schema.org/MediaObject",
                ),
            );
            let actor = ActorId::random();
            let mut batch = store.new_batch();
            let counter = store
                .next_counter(
                    &mut batch,
                    CounterKey {
                        graph_id: quad.graph,
                        actor,
                    },
                )
                .unwrap();
            store
                .insert_quad(
                    &mut batch,
                    QuadAdd {
                        quad,
                        dot: Dot { actor, counter },
                    },
                )
                .unwrap();
            let mut clock = store.vector_clock_id(quad.graph).unwrap();
            clock.advance(actor, counter);
            store
                .set_vector_clock(
                    &mut batch,
                    ClockUpdate {
                        graph_id: quad.graph,
                        clock: &clock,
                    },
                )
                .unwrap();
            store.commit(batch).unwrap();
            store.persist().unwrap();
        }

        let expected = vec![
            "urn:orphan:crashed".to_string(),
            "urn:orphan:known".to_string(),
        ];

        {
            let store = GraphStore::open(dir.path()).unwrap();
            // Open repaired it: exactly one graph had a stale tag.
            assert_eq!(
                1,
                store.diagnostics_compute_count(),
                "open must detect the stale clock tag and recompute"
            );
            assert_eq!(
                expected,
                store.graph_diagnostics(&graph).unwrap().orphaned_entities
            );
            assert_eq!(
                1,
                store.diagnostics_compute_count(),
                "the read is served from the record open repaired"
            );

            // A mismatched clock forces read-time recomputation.
            commit_orphan(&store, &graph, "urn:orphan:later");
            assert_eq!(
                vec![
                    "urn:orphan:crashed".to_string(),
                    "urn:orphan:known".to_string(),
                    "urn:orphan:later".to_string(),
                ],
                store.graph_diagnostics(&graph).unwrap().orphaned_entities
            );
            assert_eq!(2, store.diagnostics_compute_count());

            // Only a committing writer advances the stored search baseline.
            settle_diagnostics(&store, &graph);
            store.persist().unwrap();
        }

        // And the writer's repair is durable: the next open has nothing to fix.
        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_eq!(
            0,
            reopened.diagnostics_compute_count(),
            "the previous run must have persisted a correctly tagged record"
        );
    }

    /// Reads preserve the persisted diagnostics baseline used for search diffs.
    #[test]
    fn read_preserves_baseline() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:diagnostics-read-only");
        store.create_graph(&graph).unwrap();
        commit_orphan(&store, &graph, "urn:orphan:baseline");
        settle_diagnostics(&store, &graph);

        // A commit the writer never settled, so the stored record is stale.
        commit_orphan(&store, &graph, "urn:orphan:unsettled");

        assert_eq!(
            vec![
                "urn:orphan:baseline".to_string(),
                "urn:orphan:unsettled".to_string(),
            ],
            store.graph_diagnostics(&graph).unwrap().orphaned_entities,
            "a read must serve the set the current state implies"
        );
        assert_eq!(
            vec!["urn:orphan:baseline".to_string()],
            store
                .last_persisted_diagnostics(&graph)
                .unwrap()
                .orphaned_entities,
            "the read must leave the baseline for the writer that owns it"
        );
    }

    /// Orphan visibility repairs re-queue affected search subjects.
    #[test]
    fn open_requeues_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:diagnostics-requeue");

        let subject_id = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            commit_orphan(&store, &graph, "urn:orphan:known");
            settle_diagnostics(&store, &graph);
            assert_eq!(
                vec!["urn:orphan:known".to_string()],
                store.graph_diagnostics(&graph).unwrap().orphaned_entities
            );
            store.clear_fts_queue().unwrap();

            // A commit that never refreshed diagnostics.
            let quad = encode_quad(
                &store,
                &graph,
                (
                    "urn:orphan:appeared",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "http://schema.org/MediaObject",
                ),
            );
            commit_add(&store, &graph, quad);
            store.persist().unwrap();
            quad.subject
        };

        let store = GraphStore::open(dir.path()).unwrap();
        let queued: Vec<TermId> = store
            .drain_fts_queue(10)
            .unwrap()
            .into_iter()
            .map(|entry| entry.subject)
            .collect();
        assert_eq!(
            vec![subject_id],
            queued,
            "the newly orphaned entity must be re-queued for search at open"
        );
    }

    /// Open re-queues an entity adopted after the stored baseline.
    #[test]
    fn open_requeues_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:diagnostics-adopt");

        let subject_id = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            commit_orphan(&store, &graph, "urn:orphan:adopted");
            settle_diagnostics(&store, &graph);
            store.clear_fts_queue().unwrap();

            // The root adopts it, and nothing refreshes diagnostics.
            let quad = encode_quad(
                &store,
                &graph,
                (
                    graph.as_str(),
                    "http://schema.org/hasPart",
                    "urn:orphan:adopted",
                ),
            );
            commit_add(&store, &graph, quad);
            store.persist().unwrap();
            quad.object
        };

        let store = GraphStore::open(dir.path()).unwrap();
        assert!(
            store
                .graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .is_empty(),
            "open must repair the record the adoption invalidated"
        );
        let queued: Vec<TermId> = store
            .drain_fts_queue(10)
            .unwrap()
            .into_iter()
            .map(|entry| entry.subject)
            .collect();
        assert_eq!(
            vec![subject_id],
            queued,
            "the adopted entity must be re-queued for search at open"
        );
    }

    // Durability under the Fjall configuration.

    #[test]
    fn reopen_fingerprint_matches() {
        const ENTITIES: usize = 2_000;

        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:reopen-fingerprint");

        let (fingerprint, snapshot) = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let graph_id = store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap();
            let actor = ActorId::random();

            let _commit_guard = store.graph_commit_guard(&graph);
            let mut batch = store.new_batch();
            let mut clock = store.vector_clock_id(graph_id).unwrap();
            for index in 0..ENTITIES {
                let counter = store
                    .next_counter(&mut batch, CounterKey { graph_id, actor })
                    .unwrap();
                let quad = EncodedQuad {
                    graph: graph_id,
                    subject: store
                        .resolve_term(&named(&format!("urn:bulk:s{index}")))
                        .unwrap(),
                    predicate: store
                        .resolve_term(&named("http://schema.org/name"))
                        .unwrap(),
                    object: store
                        .resolve_term(&EncodedTerm(format!("\"entity {index}\"")))
                        .unwrap(),
                };
                store
                    .insert_quad(
                        &mut batch,
                        QuadAdd {
                            quad,
                            dot: Dot { actor, counter },
                        },
                    )
                    .unwrap();
                clock.advance(actor, counter);
            }
            store
                .set_vector_clock(
                    &mut batch,
                    ClockUpdate {
                        graph_id,
                        clock: &clock,
                    },
                )
                .unwrap();
            store.commit(batch).unwrap();
            store.persist().unwrap();

            let mut snapshot = store.graph_snapshot(&graph).unwrap();
            snapshot.quads.sort_by(|left, right| {
                (&left.subject, &left.predicate, &left.object).cmp(&(
                    &right.subject,
                    &right.predicate,
                    &right.object,
                ))
            });
            (store.graph_fingerprint(&graph).unwrap(), snapshot)
        };

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_eq!(fingerprint, reopened.graph_fingerprint(&graph).unwrap());
        assert_eq!(ENTITIES as u64, fingerprint.0);

        let mut reopened_snapshot = reopened.graph_snapshot(&graph).unwrap();
        reopened_snapshot.quads.sort_by(|left, right| {
            (&left.subject, &left.predicate, &left.object).cmp(&(
                &right.subject,
                &right.predicate,
                &right.object,
            ))
        });
        assert_eq!(snapshot, reopened_snapshot);
    }

    #[test]
    fn clear_removes_queues() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();
        let subject = store
            .resolve_term(&EncodedTerm::from_named_node(
                &oxrdf::NamedNode::new_unchecked("urn:test:subject"),
            ))
            .unwrap();

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.enqueue_fts_reindex(&mut batch, graph_id).unwrap();
        store.commit(batch).unwrap();

        store
            .clear_graph_queue(&graph, store.current_dirty_token())
            .unwrap();
        assert!(store.drain_fts_queue(10).unwrap().is_empty());
        assert!(store.drain_reindex_queue(10).unwrap().is_empty());
    }

    /// Graph deletion removes queue entries that raced before its commit.
    #[test]
    fn delete_sweeps_queue() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:delete-queue-race");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:o"));
        commit_add(&store, &graph, quad);
        let subject = store.resolve_term(&named("urn:test:raced")).unwrap();

        store.set_delete_stall(std::time::Duration::from_millis(300));
        std::thread::scope(|scope| {
            scope.spawn(|| store.delete_graph(&graph).unwrap());
            spin_until(|| store.delete_stalled());
            let mut batch = store.new_batch();
            store
                .enqueue_fts(
                    &mut batch,
                    FtsSubject {
                        graph_id: quad.graph,
                        subject,
                    },
                )
                .unwrap();
            store.commit(batch).unwrap();
        });

        assert!(
            store.drain_fts_queue(10).unwrap().is_empty(),
            "a subject queued during the delete must not outlive the graph"
        );
        assert_eq!(
            1,
            store.drain_delete_queue(10).unwrap().len(),
            "the delete's own queue entry must survive the sweep"
        );
    }

    fn table_bytes(root: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let path = entry.path();
                let meta = entry.metadata().unwrap();
                total += if meta.is_dir() {
                    table_bytes(&path)
                } else if path.parent().is_some_and(|p| p.ends_with("tables")) {
                    meta.len()
                } else {
                    0
                };
            }
        }
        total
    }

    /// Manual compaction flushes memtables before reclaiming journals.
    #[test]
    fn compact_flushes_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:manual-compact");
        store.create_graph(&graph).unwrap();

        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p"));
        for index in 0..2_000u64 {
            let subject = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(format!(
                "urn:test:s{index}"
            )));
            insert_quad(
                &store,
                &graph,
                &subject,
                &predicate,
                &EncodedTerm(format!("\"value {index}\"")),
                Dot {
                    actor: ActorId::random(),
                    counter: index + 1,
                },
            );
        }
        store.persist().unwrap();

        let before = table_bytes(dir.path());
        store.manual_compact().unwrap();
        let after = table_bytes(dir.path());

        assert!(
            after > before,
            "manual_compact must land pending writes in tables, but bytes went {before} -> {after}"
        );
    }

    /// Aliases stored raw by earlier versions merge on open; a delete of either spelling removes them.
    #[test]
    fn legacy_aliases_merge() {
        let dir = tempfile::tempdir().unwrap();
        let auth = crate::AllowAllAuthorizer;
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:alias:p"));
        let plain = EncodedTerm("\"x\"".to_owned());
        let typed = EncodedTerm("\"x\"^^<http://www.w3.org/2001/XMLSchema#string>".to_owned());
        let graphs = [
            GraphId::new("urn:test:alias:typed"),
            GraphId::new("urn:test:alias:plain"),
        ];
        let subject = |graph: &GraphId| EncodedTerm::from_named_node(&graph.0);
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            for graph in &graphs {
                let quads = vec![(subject(graph), predicate.clone(), plain.clone())];
                node.insert_quads(&auth, graph, quads).unwrap();
                let store = &node.store;
                let _commit = store.graph_commit_guard(graph);
                let mut batch = store.new_batch();
                let graph_id = store.resolve_term(&subject(graph)).unwrap();
                let actor = ActorId::random();
                let counter = store
                    .next_counter(&mut batch, CounterKey { graph_id, actor })
                    .unwrap();
                let quad = EncodedQuad {
                    graph: graph_id,
                    subject: graph_id,
                    predicate: store.resolve_term(&predicate).unwrap(),
                    object: store.resolve_term(&typed).unwrap(),
                };
                let dot = Dot { actor, counter };
                store
                    .insert_quad(&mut batch, QuadAdd { quad, dot })
                    .unwrap();
                let mut clock = store.vector_clock_id(graph_id).unwrap();
                clock.advance(actor, counter);
                let clock = ClockUpdate {
                    graph_id,
                    clock: &clock,
                };
                store.set_vector_clock(&mut batch, clock).unwrap();
                // Earlier versions never wrote the repair marker.
                batch.remove(&store.graphs, LITERAL_ALIAS_KEY);
                store.commit(batch).unwrap();
            }
            node.persist_fjall().unwrap();
        }

        let node = crate::CraqleNode::open(dir.path()).unwrap();
        for (graph, spelling) in graphs.iter().zip([&typed, &plain]) {
            let objects = |node: &crate::CraqleNode| {
                let snapshot = node.graph_snapshot(graph).unwrap();
                let quads = snapshot.quads.into_iter();
                quads
                    .map(|quad| (quad.object, quad.dots.len()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(objects(&node), [(plain.clone(), 2)]);
            let sparql = format!(
                "SELECT ?o WHERE {{ GRAPH <{}> {{ ?s ?p ?o }} }}",
                graph.as_str()
            );
            let rows = match node.query(&auth, &sparql).unwrap() {
                crate::QueryResults::Solutions(rows) => rows,
                other => panic!("expected solutions, got {other:?}"),
            };
            assert_eq!(rows.len(), 1, "{rows:?}");
            let delete = crate::MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: subject(graph),
                predicate: predicate.clone(),
                object: spelling.clone(),
            };
            node.apply_changes(&auth, graph, vec![delete]).unwrap();
            assert!(objects(&node).is_empty(), "{spelling:?}");
        }
    }
}
