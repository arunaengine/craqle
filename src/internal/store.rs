use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "shacl-core")]
use std::time::{Duration, Instant};

use crate::cache::BoundedCache;
#[cfg(test)]
use crate::cache::CacheStatistics;
use crate::core::*;
use crate::search_queue::{DirtyGraph, DirtySubject, DirtyTokens};
use crate::{
    CraqleErrorKind, DISK_FORMAT_VERSION, DiskFormatVersion, QueryIndexState, QueryIndexStatus,
    QueryIndexVerification, QueryIndexVerificationMode,
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
    QueryIndexVerificationFailed(&'static str),
    #[error("invalid query-index encoding for {context}: {message}")]
    InvalidQueryIndexEncoding {
        context: &'static str,
        message: String,
    },
    #[error("query index unavailable: {0}")]
    QueryIndexUnavailable(&'static str),
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
            Self::TermCollision { .. } | Self::CursorCompareFailed => CraqleErrorKind::Conflict,
            Self::GraphNotFound(_) => CraqleErrorKind::InvalidInput,
            Self::QueryIndexVerificationFailed(_)
            | Self::InvalidQueryIndexEncoding { .. }
            | Self::QueryIndexUnavailable(_) => CraqleErrorKind::CorruptDerivedData,
            Self::TermNotFound(_)
            | Self::InvalidEncoding { .. }
            | Self::MissingAuthoritativeFormat
            | Self::InvalidAuthoritativeFormat => CraqleErrorKind::CorruptAuthoritativeData,
            Self::UnsupportedAuthoritativeFormat { .. } => CraqleErrorKind::Unsupported,
            Self::Fjall(_) | Self::Postcard(_) => CraqleErrorKind::Storage,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
const GRAPH_SEARCH_DELETE_PREFIX: u8 = b'X';
const LOG_HEAD_PREFIX: u8 = b'H';
const LOG_BATCH_PREFIX: u8 = b'B';
const TOPIC_CLOCK_PREFIX: u8 = b'C';
const TOPIC_BINDING_PREFIX: u8 = b'T';
const GRAPH_TOMBSTONE_PREFIX: u8 = b'Z';
const REPLICATION_REJECTION_PREFIX: u8 = b'J';
const CURSOR_REPAIR_AUDIT_PREFIX: u8 = b'A';
/// Per-graph vector clock, split out of the graph meta record so a
/// commit writes only the clock and never rewrites policy/context/topic bytes.
const GRAPH_CLOCK_PREFIX: u8 = b'K';
/// Persisted, clock-tagged graph diagnostics.
const GRAPH_DIAGNOSTICS_PREFIX: u8 = b'O';
const DISK_FORMAT_KEY: &[u8] = b"\0craqle-authoritative-format";
#[cfg(feature = "shacl-core")]
const SHACL_BINDING_PREFIX: u8 = b'S';
#[cfg(feature = "shacl-core")]
const SHACL_REVERSE_PREFIX: u8 = b's';
/// Queued active SHACL data graphs awaiting settlement.
#[cfg(feature = "shacl-core")]
const SHACL_PENDING_PREFIX: u8 = b'V';
#[cfg(feature = "shacl-core")]
const SHACL_PENDING_QUEUE_SCHEMA_KEY: &[u8] = b"vshacl-pending-queue";
#[cfg(feature = "shacl-core")]
const SHACL_PENDING_QUEUE_SCHEMA_VERSION: u8 = 1;
const TERM_LOCK_SHARDS: usize = 64;
const COMMIT_LOCK_SHARDS: usize = 64;
const QV_COMMIT_ACTIVE: u64 = 1;
const QV_COMMIT_DIRTY: u64 = 2;
const TERM_DECODE_CACHE_CAP: usize = 1_000_000;
const TERM_DECODE_CACHE_BYTES: usize = 128 * 1_048_576;
const QUAD_SUBJECT_CACHE_CAP: usize = 65_536;
const QUAD_SUBJECT_CACHE_BYTES: usize = 64 * 1_048_576;
const OBJECT_ORDER_CACHE_CAP: usize = 4_096;
const OBJECT_ORDER_CACHE_BYTES: usize = 64 * 1_048_576;
const PLANNER_DISTINCT_CACHE_CAP: usize = 4_096;
const PLANNER_DISTINCT_CACHE_BYTES: usize = 1_048_576;
const FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD: usize = 10_000;
const DEFAULT_DB_CACHE_BYTES: u64 = 1_024 * 1_024 * 1_024;
const MAX_DB_CACHE_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
/// Memtable ceiling for the append-heavy keyspaces (`quads`, `log`).
///
/// This is the *only* knob that makes fjall 3.1.6 flush at all, and therefore
/// the only knob that lets a journal be reclaimed: journal rotation and the
/// `max_journaling_size` eviction both live inside the flush worker's message
/// handler, which is reached solely from `check_memtable_rotate`, i.e. from a
/// memtable exceeding this value. The previous 1 GiB never filled — the `quads`
/// keyspace is ~84 MB at 40,000 graphs — so nothing ever flushed, the store held
/// zero SSTables and the single journal grew with total write churn, making cold
/// start O(bytes ever written) rather than O(data) (findings C1/C2).
const WRITE_HEAVY_MEMTABLE_BYTES: u64 = 64 * 1_024 * 1_024;
/// Memtable ceiling for the point-read keyspaces (`terms`, `graphs`).
///
/// Deliberately smaller than [`WRITE_HEAVY_MEMTABLE_BYTES`]: a journal file is
/// only deleted once *every* keyspace holding a watermark in it has flushed, so
/// a lightly written keyspace sitting on a large memtable pins journals that the
/// busy keyspaces have long since flushed past.
const POINT_READ_MEMTABLE_BYTES: u64 = 32 * 1_024 * 1_024;
/// Ceiling on retained journal bytes, and hence on crash-recovery replay.
///
/// fjall rotates a journal file at a hard-coded ~61 MiB and only then checks
/// this budget, so this is the tightest value that still bounds anything: the
/// check fires on essentially every rotation and leaves just the active file
/// behind. Replay is therefore bounded by one journal file (~2 s at the ~31
/// ms/MiB this store replays at) instead of growing until the old 16 GiB
/// ceiling forced a rotation.
const MAX_JOURNALING_BYTES: u64 = 64 * 1_024 * 1_024;
const WRITE_HEAVY_TABLE_TARGET_BYTES: u64 = 256 * 1_024 * 1_024;
const WRITE_HEAVY_L0_THRESHOLD: u8 = 12;
const WRITE_HEAVY_LEVEL_RATIO: f32 = 20.0;

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

fn recommended_db_cache_bytes() -> u64 {
    let available = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|meminfo| {
            meminfo.lines().find_map(|line| {
                let value = line.strip_prefix("MemAvailable:")?.trim();
                let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
                Some(kib * 1024)
            })
        })
        .unwrap_or(DEFAULT_DB_CACHE_BYTES);

    (available / 8).clamp(DEFAULT_DB_CACHE_BYTES, MAX_DB_CACHE_BYTES)
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct StoredGraphMeta {
    policy: GraphPolicy,
    #[serde(default)]
    policy_tag: PolicyTag,
    /// Legacy home of the per-graph vector clock. The clock now lives
    /// under its own `'K' || graph_id` key and this field is only read as a
    /// one-time migration fallback for stores written before the split; it is
    /// ignored as soon as the `'K'` key exists. Never written by
    /// [`GraphStore::set_vector_clock`] any more.
    clock: VectorClock,
    #[serde(default)]
    irokle_topic: Option<[u8; 32]>,
    /// Raw RO-Crate `@context` JSON submitted on import, stored verbatim so it
    /// can be spliced back into exported documents. `None` means the graph uses
    /// the bare default RO-Crate context.
    #[serde(default)]
    rocrate_context: Option<String>,
    /// Raw root `license` JSON submitted on import, retained so exports can
    /// preserve its JSON-LD surface shape while the graph remains unchanged.
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

const QUERY_INDEX_SCHEMA_VERSION: u32 = 2;
const QUERY_INDEX_HEADER_KEY: [u8; 1] = *b"H";
const QUERY_INDEX_TOTAL_KEY: [u8; 1] = *b"T";
const QUERY_INDEX_HEADER_MAGIC: [u8; 4] = *b"QVI2";
const QUERY_INDEX_HEADER_BASE_LEN: usize = 70;
const QUERY_INDEX_FAILURE_MAX_BYTES: usize = 256;
const QUERY_INDEX_BUILD_CHUNK_ROWS: usize = 1_024;
const QUERY_INDEX_SAMPLE_ROWS: u64 = 128;
const QUERY_INDEX_PROBLEM_LIMIT: usize = 32;

const QUERY_INDEX_GRAPH_COUNT_TAG: u8 = b'G';
const QUERY_INDEX_PREDICATE_COUNT_TAG: u8 = b'P';
const QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG: u8 = b'A';
const QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG: u8 = b'O';
const QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG: u8 = b'X';
const QUERY_INDEX_UNION_DUPLICATE_FREE_TAG: u8 = b'U';

#[derive(Debug, Clone, PartialEq, Eq)]
enum StoredQueryIndexState {
    Building,
    Ready,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryIndexHeader {
    state: StoredQueryIndexState,
    source_epoch: u64,
    index_epoch: u64,
    source_live_quads: u64,
    indexed_quads: u64,
    last_build_sequence: u64,
    query_id_generation: u64,
    next_query_id: u64,
}

impl QueryIndexHeader {
    fn empty_ready() -> Self {
        Self {
            state: StoredQueryIndexState::Ready,
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
        header.state = StoredQueryIndexState::Failed(reason.to_owned());
        header
    }

    fn state(&self) -> QueryIndexState {
        match &self.state {
            StoredQueryIndexState::Building => QueryIndexState::Building,
            StoredQueryIndexState::Ready => QueryIndexState::Ready,
            StoredQueryIndexState::Failed(reason) => QueryIndexState::Failed(reason.clone()),
        }
    }

    fn ready_is_coherent(&self) -> bool {
        matches!(self.state, StoredQueryIndexState::Ready)
            && self.source_epoch == self.index_epoch
            && self.source_live_quads == self.indexed_quads
            && self.query_id_generation != 0
    }

    fn is_not_ahead_of_snapshot(&self, snapshot_sequence: u64) -> bool {
        self.source_epoch <= snapshot_sequence
            && self.index_epoch <= snapshot_sequence
            && self.last_build_sequence <= snapshot_sequence
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryIndexCounterKey {
    Total,
    UnionDuplicateFree,
    Graph(QueryTermId),
    Predicate(QueryTermId),
    GraphPredicate(QueryTermId, QueryTermId),
    PredicateObject(QueryTermId, QueryTermId),
    GraphPredicateObject(QueryTermId, QueryTermId, QueryTermId),
}

impl QueryIndexCounterKey {
    fn bytes(self) -> Vec<u8> {
        let mut key = match self {
            Self::Total => return QUERY_INDEX_TOTAL_KEY.to_vec(),
            Self::UnionDuplicateFree => return vec![QUERY_INDEX_UNION_DUPLICATE_FREE_TAG],
            Self::Graph(_) | Self::Predicate(_) => vec![0; 9],
            Self::GraphPredicate(_, _) | Self::PredicateObject(_, _) => vec![0; 17],
            Self::GraphPredicateObject(_, _, _) => vec![0; 25],
        };
        match self {
            Self::Graph(graph) => {
                key[0] = QUERY_INDEX_GRAPH_COUNT_TAG;
                key[1..9].copy_from_slice(&graph.to_be_bytes());
            }
            Self::Predicate(predicate) => {
                key[0] = QUERY_INDEX_PREDICATE_COUNT_TAG;
                key[1..9].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::GraphPredicate(graph, predicate) => {
                key[0] = QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG;
                key[1..9].copy_from_slice(&graph.to_be_bytes());
                key[9..17].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::PredicateObject(predicate, object) => {
                key[0] = QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG;
                key[1..9].copy_from_slice(&predicate.to_be_bytes());
                key[9..17].copy_from_slice(&object.to_be_bytes());
            }
            Self::GraphPredicateObject(graph, predicate, object) => {
                key[0] = QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG;
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

enum QueryIndexHeaderRead {
    Absent,
    Valid(QueryIndexHeader),
    Malformed,
}

enum QueryIndexCounterKeyRead {
    Header,
    Counter(QueryIndexCounterKey),
    UnknownTag,
    InvalidLength,
}

#[derive(Clone, Copy)]
struct NetQuadTransition {
    quad: EncodedQuad,
    was_live: bool,
    is_live: bool,
}

struct QueryIndexCounterUpdate {
    key: QueryIndexCounterKey,
    value: Option<u64>,
}

struct QueryIndexMaintenancePlan {
    transitions: Vec<(QueryQuad, bool)>,
    mappings: Vec<(TermId, QueryTermId)>,
    counters: Vec<QueryIndexCounterUpdate>,
    header: Option<QueryIndexHeader>,
}

enum QueryIndexCounterRead {
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
            Self::Delete(graph) => graph_search_delete_key(graph).to_vec(),
        }
    }
}

/// FTS queue keys a batch owes, deduplicated but kept in enqueue order.
///
/// Order is load-bearing: acknowledgement compares tokens, so a whole-graph
/// reindex enqueued after some subjects must outrank them and clear them.
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

/// A batch's durable half: the staged fjall writes plus the FTS queue keys
/// whose tokens are minted when it publishes.
struct DurableCommit {
    batch: fjall::OwnedWriteBatch,
    pending_fts: PendingFts,
}

pub struct WriteBatch {
    inner: fjall::OwnedWriteBatch,
    /// Uncommitted dot sets, so later operations in the same batch read the
    /// batch-local state instead of the (still stale) durable one. `None` means
    /// "written empty", i.e. the quad is dead. Keyed by the fixed-size quad key
    /// so no per-quad `Vec` is allocated.
    pending_quad_states: HashMap<QuadKey, Option<Vec<Dot>>>,
    pending_terms: HashMap<TermId, String>,
    publish: PendingPublish,
    /// Queue keys this batch dirtied. Their tokens are minted and their entries
    /// staged when the batch commits, under the queue lock, so no
    /// acknowledgement can be reading them at the time.
    pending_fts: PendingFts,
}

impl WriteBatch {
    fn new(inner: fjall::OwnedWriteBatch) -> Self {
        Self {
            inner,
            pending_quad_states: HashMap::new(),
            pending_terms: HashMap::new(),
            publish: PendingPublish::default(),
            pending_fts: PendingFts::default(),
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
    quad_subjects: BoundedCache<(TermId, TermId, u64), Arc<Vec<(TermId, TermId)>>>,
    object_order: ObjectOrderCache,
    planner_distinct: BoundedCache<(u64, Option<QueryTermId>, DistinctDomain), usize>,
    generations: HashMap<TermId, u64>,
    /// Per-graph clocks as published by each graph's last commit. A missing
    /// entry is the empty clock, which is what the durable read yields for a
    /// graph that has never committed.
    clocks: HashMap<TermId, VectorClock>,
}

impl Default for IndexState {
    fn default() -> Self {
        Self {
            quad_subjects: BoundedCache::new(QUAD_SUBJECT_CACHE_CAP, QUAD_SUBJECT_CACHE_BYTES),
            object_order: ObjectOrderCache::default(),
            planner_distinct: BoundedCache::new(
                PLANNER_DISTINCT_CACHE_CAP,
                PLANNER_DISTINCT_CACHE_BYTES,
            ),
            generations: HashMap::new(),
            clocks: HashMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DistinctDomain {
    Subject,
    Object,
}

type ObjectOrderKey = (TermId, TermId, TermId);
type ObjectOrderValues = Arc<Vec<TermId>>;

/// `(graph, subject, predicate)` → objects in decoded-term order.
///
/// Repopulation decodes outside the lock, so an entry computed from an index a
/// commit has since invalidated must not be installed. `generation` moves on
/// every invalidation and a repopulating reader only installs what it computed
/// while the count has not moved.
struct ObjectOrderCache {
    entries: BoundedCache<(ObjectOrderKey, u64), ObjectOrderValues>,
}

impl Default for ObjectOrderCache {
    fn default() -> Self {
        Self {
            entries: BoundedCache::new(OBJECT_ORDER_CACHE_CAP, OBJECT_ORDER_CACHE_BYTES),
        }
    }
}

impl ObjectOrderCache {
    fn get(&mut self, key: &ObjectOrderKey, generation: u64) -> Option<ObjectOrderValues> {
        self.entries.get_cloned(&(*key, generation))
    }

    fn invalidate(&mut self, key: &ObjectOrderKey) {
        self.entries.remove_where(|(cached, _)| cached == key);
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
    fn publish(&mut self, publish: &PendingPublish) {
        let mut changed_graphs = HashSet::new();
        for mutation in &publish.quad_mutations {
            let quad = match mutation {
                QuadMutation::Insert(quad) | QuadMutation::Remove(quad) => *quad,
            };
            self.quad_subjects.remove_where(|(graph, subject, _)| {
                *graph == quad.graph && *subject == quad.subject
            });
            self.object_order
                .invalidate(&(quad.graph, quad.subject, quad.predicate));
            changed_graphs.insert(quad.graph);
        }
        for graph in changed_graphs {
            let generation = self.generations.entry(graph).or_default();
            *generation = generation.wrapping_add(1);
        }

        for (&graph_id, clock) in &publish.clocks {
            match clock {
                Some(clock) => self.clocks.insert(graph_id, clock.clone()),
                None => self.clocks.remove(&graph_id),
            };
        }
    }
}

/// The in-memory half of a commit, staged alongside the durable batch and
/// published once that batch lands.
#[derive(Default)]
struct PendingPublish {
    quad_mutations: Vec<QuadMutation>,
    /// `None` clears the mirror entry, which is what removing a graph's clock
    /// key means.
    clocks: HashMap<TermId, Option<VectorClock>>,
}

impl PendingPublish {
    fn is_empty(&self) -> bool {
        self.quad_mutations.is_empty() && self.clocks.is_empty()
    }
}

/// Diagnostics as persisted under `'O' || graph_id`, tagged with the graph's
/// vector clock at the moment they were computed.
///
/// The tag is what makes the cache self-checking: every quad-mutating commit
/// advances the graph clock (`set_vector_clock` is part of the same batch), so
/// `at_clock != current clock` proves the record describes an older state and
/// must be recomputed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StoredDiagnostics {
    diagnostics: GraphDiagnostics,
    at_clock: VectorClock,
}

/// The interned ids of the four vocabulary terms orphan detection matches on.
///
/// `None` means the term was never interned, so no stored quad can mention it.
struct OrphanVocab {
    rdf_type: Option<TermId>,
    /// `schema:Dataset` and `schema:MediaObject` — the two types that make a
    /// non-root subject a data entity.
    data_types: [Option<TermId>; 2],
    has_part: Option<TermId>,
}

pub struct GraphStore {
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
    qv2_term_to_query: Keyspace,
    qv2_query_to_term: Keyspace,
    qv2_meta: Keyspace,
    /// Guards first-write-wins term interning, sharded by term id.
    term_locks: Vec<Mutex<()>>,
    /// Guards whole read→write→commit cycles of one graph's CRDT state; see
    /// [`GraphStore::graph_commit_guard`].
    commit_locks: Vec<Mutex<()>>,
    /// One qv maintainer may stage global counters at a time. Other graph
    /// commits never wait: they mark qv degraded and commit source state.
    qv_commit_state: AtomicU64,
    qv_degraded: AtomicBool,
    qv_pending_commits: AtomicU64,
    qv_catchup_failed: AtomicBool,
    #[cfg(feature = "shacl-core")]
    binding_lock: Mutex<()>,
    #[cfg(feature = "shacl-core")]
    binding_lock_wait_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    binding_lock_hold_ns: AtomicU64,
    #[cfg(feature = "shacl-core")]
    graph_commit_lock_wait_ns: AtomicU64,
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
    status_full_shape_scans: AtomicU64,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_stall: Mutex<Duration>,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_active: std::sync::atomic::AtomicUsize,
    #[cfg(all(test, feature = "shacl-core"))]
    validation_max_active: std::sync::atomic::AtomicUsize,
    indexes: RwLock<IndexState>,
    /// Memory mirror of the persisted `'O'` records; always carries the clock
    /// tag so a reader can tell a fresh entry from a stale one.
    diagnostics_cache: RwLock<HashMap<TermId, StoredDiagnostics>>,
    /// Global term-id → term cache. Term ids are content hashes, so entries do
    /// not need invalidation; capacity and bytes are bounded independently.
    term_decode_cache: RwLock<BoundedCache<TermId, Arc<EncodedTerm>>>,
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
    commit_stall_max_active: std::sync::atomic::AtomicUsize,
    /// Makes the next durable batch fail immediately before fjall commits.
    #[cfg(test)]
    commit_failure: std::sync::atomic::AtomicBool,
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
    /// Set by a test to stall a graph delete between its queue scan and the
    /// commit; `delete_stalled` publishes that the window has been entered.
    #[cfg(test)]
    delete_stall: Mutex<Option<std::time::Duration>>,
    #[cfg(test)]
    delete_stalled: std::sync::atomic::AtomicBool,
    /// Serializes every durable mutation of the FTS queues: minting a dirty
    /// token and staging its entry, the acknowledgement check-and-remove, and
    /// the queue clears. Without it an enqueue can land between an
    /// acknowledgement's token read and its committed removal and be erased
    /// without ever being indexed (G7).
    ///
    /// **Lock order: innermost.** Take it after the graph commit guard, hold it
    /// only across the queue read plus the commit that acts on it, and take no
    /// other `GraphStore` lock while it is held.
    fts_queue_lock: Mutex<()>,
    dirty_counter: AtomicU64,
    /// How many times this store instance has recomputed graph diagnostics.
    /// Tests use it to prove a reopen served the persisted record instead of
    /// recomputing, and that a stale record was repaired at open.
    diagnostics_computed: AtomicU64,
    /// Metadata point reads performed by the O(1) qv2 admission gate.
    #[cfg(test)]
    query_index_admission_probes: AtomicU64,
    #[cfg(test)]
    query_index_verification_runs: AtomicU64,
    /// Explicit persists so far. Tests use it to pin a durability call that
    /// leaves no other trace inside one process.
    #[cfg(test)]
    persists: AtomicU64,
}

// ── Frozen WS0 parameter structs ────────────────────────────────────────────

/// RAII guard serializing one graph's read→write cycles (dot sets, log heads,
/// vector clock, meta, diagnostics tag). Sharded by graph term hash.
///
/// **Lock order: graph commit guard ▸ term shard locks.** Never take a second
/// commit guard while holding one — `std::sync::Mutex` is not reentrant. Any
/// method that calls `graph_commit_guard` itself is therefore off limits while
/// one is held; batch-taking methods do not lock and require the caller to hold it.
///
/// Poison is recovered: the protected state lives in fjall, not behind the mutex.
pub(crate) struct GraphCommitGuard<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);

struct QueryIndexCommitReset<'a>(&'a AtomicU64);

impl Drop for QueryIndexCommitReset<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

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
pub(crate) struct PendingQueueRepairStatistics {
    pub(crate) binding_records_scanned: u64,
    pub(crate) pending_queue_entries_scanned: u64,
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

/// An OR-Set add: contributes exactly one unique dot to the quad's dot set (G1).
pub struct QuadAdd {
    pub quad: EncodedQuad,
    pub dot: Dot,
}

/// An OR-Set remove: deletes exactly the dots contained in the witnessed clock
/// and can never kill a dot it did not witness (G1).
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

fn encode_dirty_tokens(tokens: DirtyTokens) -> [u8; 16] {
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&tokens.oldest.to_be_bytes());
    value[8..].copy_from_slice(&tokens.latest.to_be_bytes());
    value
}

/// Decode a queue entry's tokens, accepting the single-token form a store
/// written before `oldest` existed still holds: that token was the latest, and
/// with nothing older recorded it is also the oldest unindexed one.
fn decode_dirty_tokens(bytes: &[u8], context: &'static str) -> Result<DirtyTokens> {
    if bytes.len() == 8 {
        let token = decode_u64_bytes(bytes, context)?;
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
        oldest: decode_u64_bytes(&bytes[..8], context)?,
        latest: decode_u64_bytes(&bytes[8..], context)?,
    })
}

fn decode_u64_bytes(bytes: &[u8], context: &'static str) -> Result<u64> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| StoreError::InvalidEncoding {
        context,
        message: format!("expected 8 bytes, found {}", bytes.len()),
    })?;
    Ok(u64::from_be_bytes(raw))
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

/// Is a stored dot payload the empty set?
///
/// Both encodings start with one header byte — the `DOT_ENCODING_TAG` for the
/// packed form, a postcard length prefix of `0` for the legacy form — so any
/// payload of one byte or less carries no dots and the quad is dead.
fn dot_payload_is_empty(bytes: &[u8]) -> bool {
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

fn graph_dirty_graph_prefix(graph: TermId) -> [u8; 17] {
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

fn graph_search_delete_key(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = GRAPH_SEARCH_DELETE_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
}

fn graph_search_delete_prefix() -> [u8; 1] {
    [GRAPH_SEARCH_DELETE_PREFIX]
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

fn cursor_repair_audit_key(audit: &crate::sync::TopicCursorRepairAudit) -> [u8; 97] {
    let mut key = [0u8; 97];
    key[0] = CURSOR_REPAIR_AUDIT_PREFIX;
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

/// Confirm that the bytes already stored under a term id really are `term`.
///
/// Compares raw bytes and only decodes on mismatch, i.e. on the
/// (astronomically unlikely) hash-collision path that needs a message.
fn confirm_stored_term(stored: &[u8], term: &EncodedTerm) -> Result<()> {
    if stored == term.0.as_bytes() {
        return Ok(());
    }
    Err(StoreError::TermCollision {
        attempted: term.0.clone(),
        existing: decode_term_utf8(stored)?,
    })
}

fn decode_term_utf8(bytes: &[u8]) -> Result<String> {
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

fn decode_query_index_u64(bytes: &[u8]) -> Option<u64> {
    let raw: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

fn query_index_failure_code_is_valid(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= QUERY_INDEX_FAILURE_MAX_BYTES
        && code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn encode_query_index_header(header: &QueryIndexHeader) -> Vec<u8> {
    let (state_tag, failure) = match &header.state {
        StoredQueryIndexState::Building => (1, ""),
        StoredQueryIndexState::Ready => (2, ""),
        StoredQueryIndexState::Failed(reason) => (3, reason.as_str()),
    };
    let failure =
        if query_index_failure_code_is_valid(failure) || (failure.is_empty() && state_tag != 3) {
            failure
        } else {
            "metadata-malformed"
        };
    let mut bytes = Vec::with_capacity(QUERY_INDEX_HEADER_BASE_LEN + failure.len());
    bytes.extend_from_slice(&QUERY_INDEX_HEADER_MAGIC);
    bytes.extend_from_slice(&QUERY_INDEX_SCHEMA_VERSION.to_be_bytes());
    bytes.push(state_tag);
    bytes.extend_from_slice(&[0, 0, 0]);
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

fn decode_query_index_header(bytes: &[u8]) -> QueryIndexHeaderRead {
    if bytes.len() < QUERY_INDEX_HEADER_BASE_LEN
        || bytes[0..4] != QUERY_INDEX_HEADER_MAGIC
        || bytes[8] == 0
        || bytes[9..12] != [0, 0, 0]
    {
        return QueryIndexHeaderRead::Malformed;
    }
    let schema_version = u32::from_be_bytes(
        bytes[4..8]
            .try_into()
            .expect("fixed query-index header slice"),
    );
    if schema_version != QUERY_INDEX_SCHEMA_VERSION {
        return QueryIndexHeaderRead::Malformed;
    }
    let Some(source_epoch) = decode_query_index_u64(&bytes[12..20]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(index_epoch) = decode_query_index_u64(&bytes[20..28]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(source_live_quads) = decode_query_index_u64(&bytes[28..36]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(indexed_quads) = decode_query_index_u64(&bytes[36..44]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(last_build_sequence) = decode_query_index_u64(&bytes[44..52]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(query_id_generation) = decode_query_index_u64(&bytes[52..60]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let Some(next_query_id) = decode_query_index_u64(&bytes[60..68]) else {
        return QueryIndexHeaderRead::Malformed;
    };
    let failure_len = u16::from_be_bytes(
        bytes[68..70]
            .try_into()
            .expect("fixed query-index header slice"),
    ) as usize;
    if failure_len > QUERY_INDEX_FAILURE_MAX_BYTES
        || bytes.len() != QUERY_INDEX_HEADER_BASE_LEN + failure_len
    {
        return QueryIndexHeaderRead::Malformed;
    }
    let failure = std::str::from_utf8(&bytes[QUERY_INDEX_HEADER_BASE_LEN..]).ok();
    let state = match (bytes[8], failure) {
        (1, Some("")) => StoredQueryIndexState::Building,
        (2, Some("")) => StoredQueryIndexState::Ready,
        (3, Some(reason)) if query_index_failure_code_is_valid(reason) => {
            StoredQueryIndexState::Failed(reason.to_owned())
        }
        _ => return QueryIndexHeaderRead::Malformed,
    };
    QueryIndexHeaderRead::Valid(QueryIndexHeader {
        state,
        source_epoch,
        index_epoch,
        source_live_quads,
        indexed_quads,
        last_build_sequence,
        query_id_generation,
        next_query_id,
    })
}

fn query_index_term_at(bytes: &[u8], offset: usize) -> QueryTermId {
    QueryTermId::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed query-index term slice"),
    )
}

fn decode_query_index_counter_key(bytes: &[u8]) -> QueryIndexCounterKeyRead {
    match bytes.first().copied() {
        Some(b'H') if bytes.len() == 1 => QueryIndexCounterKeyRead::Header,
        Some(b'H') => QueryIndexCounterKeyRead::InvalidLength,
        Some(b'T') if bytes.len() == 1 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::Total)
        }
        Some(b'T') => QueryIndexCounterKeyRead::InvalidLength,
        Some(QUERY_INDEX_UNION_DUPLICATE_FREE_TAG) if bytes.len() == 1 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::UnionDuplicateFree)
        }
        Some(QUERY_INDEX_UNION_DUPLICATE_FREE_TAG) => QueryIndexCounterKeyRead::InvalidLength,
        Some(QUERY_INDEX_GRAPH_COUNT_TAG) if bytes.len() == 9 => QueryIndexCounterKeyRead::Counter(
            QueryIndexCounterKey::Graph(query_index_term_at(bytes, 1)),
        ),
        Some(QUERY_INDEX_PREDICATE_COUNT_TAG) if bytes.len() == 9 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::Predicate(query_index_term_at(
                bytes, 1,
            )))
        }
        Some(QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG) if bytes.len() == 17 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::GraphPredicate(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 9),
            ))
        }
        Some(QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG) if bytes.len() == 17 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::PredicateObject(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 9),
            ))
        }
        Some(QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG) if bytes.len() == 25 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::GraphPredicateObject(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 9),
                query_index_term_at(bytes, 17),
            ))
        }
        Some(
            QUERY_INDEX_GRAPH_COUNT_TAG
            | QUERY_INDEX_PREDICATE_COUNT_TAG
            | QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG
            | QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG
            | QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG,
        ) => QueryIndexCounterKeyRead::InvalidLength,
        Some(_) => QueryIndexCounterKeyRead::UnknownTag,
        None => QueryIndexCounterKeyRead::InvalidLength,
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

fn qv2_gspo_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.subject, quad.predicate, quad.object])
}

fn qv2_gpos_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.predicate, quad.object, quad.subject])
}

fn qv2_spog_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.subject, quad.predicate, quad.object, quad.graph])
}

fn qv2_posg_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.predicate, quad.object, quad.subject, quad.graph])
}

fn qv2_ospg_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.object, quad.subject, quad.predicate, quad.graph])
}

fn qv2_gosp_key(quad: QueryQuad) -> QueryQuadKey {
    query_index_key([quad.graph, quad.object, quad.subject, quad.predicate])
}

fn decode_qv2_gspo_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: query_index_term_at(bytes, 0),
        subject: query_index_term_at(bytes, 8),
        predicate: query_index_term_at(bytes, 16),
        object: query_index_term_at(bytes, 24),
    })
}

fn decode_qv2_gpos_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: query_index_term_at(bytes, 0),
        predicate: query_index_term_at(bytes, 8),
        object: query_index_term_at(bytes, 16),
        subject: query_index_term_at(bytes, 24),
    })
}

fn decode_qv2_spog_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        subject: query_index_term_at(bytes, 0),
        predicate: query_index_term_at(bytes, 8),
        object: query_index_term_at(bytes, 16),
        graph: query_index_term_at(bytes, 24),
    })
}

fn decode_qv2_posg_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        predicate: query_index_term_at(bytes, 0),
        object: query_index_term_at(bytes, 8),
        subject: query_index_term_at(bytes, 16),
        graph: query_index_term_at(bytes, 24),
    })
}

fn decode_qv2_ospg_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        object: query_index_term_at(bytes, 0),
        subject: query_index_term_at(bytes, 8),
        predicate: query_index_term_at(bytes, 16),
        graph: query_index_term_at(bytes, 24),
    })
}

fn decode_qv2_gosp_key(bytes: &[u8]) -> Option<QueryQuad> {
    (bytes.len() == 32).then(|| QueryQuad {
        graph: query_index_term_at(bytes, 0),
        object: query_index_term_at(bytes, 8),
        subject: query_index_term_at(bytes, 16),
        predicate: query_index_term_at(bytes, 24),
    })
}

fn source_term_at(bytes: &[u8], offset: usize) -> TermId {
    TermId::from_be_bytes(
        bytes[offset..offset + 16]
            .try_into()
            .expect("fixed source term slice"),
    )
}

fn decode_query_term_id_value(bytes: &[u8], context: &'static str) -> Result<QueryTermId> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StoreError::InvalidQueryIndexEncoding {
            context,
            message: format!("expected 8 bytes, found {}", bytes.len()),
        })?;
    Ok(QueryTermId::from_be_bytes(raw))
}

fn decode_query_source_term_value(bytes: &[u8], context: &'static str) -> Result<TermId> {
    let raw: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StoreError::InvalidQueryIndexEncoding {
            context,
            message: format!("expected 16 bytes, found {}", bytes.len()),
        })?;
    Ok(TermId::from_be_bytes(raw))
}

fn decode_source_quad_key(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        graph: source_term_at(bytes, 0),
        subject: source_term_at(bytes, 16),
        predicate: source_term_at(bytes, 32),
        object: source_term_at(bytes, 48),
    })
}

fn coalesced_query_index_transitions(mutations: &[QuadMutation]) -> Vec<NetQuadTransition> {
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

fn query_index_live_counter_keys(quad: QueryQuad) -> [QueryIndexCounterKey; 6] {
    [
        QueryIndexCounterKey::Total,
        QueryIndexCounterKey::Graph(quad.graph),
        QueryIndexCounterKey::Predicate(quad.predicate),
        QueryIndexCounterKey::GraphPredicate(quad.graph, quad.predicate),
        QueryIndexCounterKey::PredicateObject(quad.predicate, quad.object),
        QueryIndexCounterKey::GraphPredicateObject(quad.graph, quad.predicate, quad.object),
    ]
}

struct QueryIndexVerificationBuilder {
    report: QueryIndexVerification,
}

#[derive(Clone, Copy)]
enum QueryIndexVerificationExpectation {
    Ready,
    BuildingCandidate,
}

#[derive(Clone, Copy)]
enum QueryIndexKeyOrder {
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
pub(crate) enum QueryIndexCursorOrder {
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
    pub(crate) query_id_upper_bound: Option<u64>,
    pub(crate) fallback_reason: Option<&'static str>,
    pub(crate) header_reads: u64,
    pub(crate) counter_reads: u64,
}

/// One immutable, publication-coherent durable read view.
///
/// It deliberately owns only the Fjall snapshot. Callers receive opaque
/// cursor and metadata operations rather than Fjall objects or keyspaces.
#[derive(Clone)]
pub(crate) struct StoreReadSnapshot {
    snapshot: Snapshot,
}

impl QueryIndexVerificationBuilder {
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
        if self.report.problems.len() < QUERY_INDEX_PROBLEM_LIMIT
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

    pub(crate) fn raw_quad_cursor(
        &self,
        store: &GraphStore,
        pattern: crate::rdf_read::QuadPattern,
    ) -> crate::query_cursor::RawQuadCursor {
        store
            .current_derived_raw_cursor(self.sequence(), pattern)
            .unwrap_or_else(|| {
                crate::query_cursor::RawQuadCursor::new(
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
    ) -> crate::query_cursor::RawQuadCursor {
        crate::query_cursor::RawQuadCursor::new(self.snapshot.clone(), &store.quads, pattern)
    }

    pub(crate) fn raw_quad_point(
        &self,
        store: &GraphStore,
        quad: EncodedQuad,
    ) -> Result<Option<crate::query_cursor::RawQuadCandidate>> {
        crate::query_cursor::point_candidate(&self.snapshot, &store.quads, quad)
    }

    pub(crate) fn query_index_cursor(
        &self,
        store: &GraphStore,
        order: QueryIndexCursorOrder,
        pattern: crate::rdf_read::QuadPattern,
    ) -> Result<crate::query_cursor::RawQuadCursor> {
        let Some((keyspace, prefix)) = store.query_index_range(&self.snapshot, order, pattern)?
        else {
            return Ok(crate::query_cursor::RawQuadCursor::empty());
        };
        Ok(crate::query_cursor::RawQuadCursor::query_index(
            self.snapshot.clone(),
            keyspace,
            &store.qv2_query_to_term,
            order,
            prefix,
        ))
    }

    pub(crate) fn query_index_key_cursor(
        &self,
        store: &GraphStore,
        order: QueryIndexCursorOrder,
        pattern: crate::rdf_read::QuadPattern,
        query_id_upper_bound: u64,
    ) -> Result<Option<crate::query_cursor::RawQueryIndexKeyCursor>> {
        let resolve = |term: Option<TermId>| -> Result<Option<Option<QueryTermId>>> {
            match term {
                Some(term) => Ok(store
                    .query_term_id_from_snapshot(&self.snapshot, term)?
                    .map(Some)),
                None => Ok(Some(None)),
            }
        };
        let Some(graph) = resolve(pattern.graph)? else {
            return Ok(None);
        };
        let Some(subject) = resolve(pattern.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = resolve(pattern.predicate)? else {
            return Ok(None);
        };
        let Some(object) = resolve(pattern.object)? else {
            return Ok(None);
        };
        let Some((keyspace, prefix)) = store.query_index_range(&self.snapshot, order, pattern)?
        else {
            return Ok(None);
        };
        let filter =
            crate::query_cursor::RawQueryIndexPattern::new(graph, subject, predicate, object)
                .without_prefix(order, prefix.len() / 8);
        Ok(Some(crate::query_cursor::RawQueryIndexKeyCursor::new(
            self.snapshot.clone(),
            keyspace,
            &store.qv2_query_to_term,
            order,
            prefix,
            filter,
            query_id_upper_bound,
        )))
    }

    pub(crate) fn query_index_admission(&self, store: &GraphStore) -> Result<QueryIndexAdmission> {
        store.snapshot_admission(&self.snapshot)
    }

    pub(crate) fn query_term_id(
        &self,
        store: &GraphStore,
        term: TermId,
    ) -> Result<Option<QueryTermId>> {
        store.query_term_id_from_snapshot(&self.snapshot, term)
    }

    pub(crate) fn qv_g_count(&self, store: &GraphStore, graph: TermId) -> Result<Option<u64>> {
        let Some(graph) = store.query_term_id_from_snapshot(&self.snapshot, graph)? else {
            return Ok(Some(0));
        };
        self.qv_count(store, QueryIndexCounterKey::Graph(graph), false)
    }

    pub(crate) fn qv_total_count(&self, store: &GraphStore) -> Result<Option<u64>> {
        self.qv_count(store, QueryIndexCounterKey::Total, false)
    }

    pub(crate) fn qv_union_duplicate_free(&self, store: &GraphStore) -> Result<Option<bool>> {
        Ok(
            match store.query_index_counter_from_snapshot(
                &self.snapshot,
                QueryIndexCounterKey::UnionDuplicateFree,
            )? {
                QueryIndexCounterRead::Value(0) => Some(false),
                QueryIndexCounterRead::Value(1) => Some(true),
                QueryIndexCounterRead::Missing
                | QueryIndexCounterRead::Malformed
                | QueryIndexCounterRead::Value(_) => None,
            },
        )
    }

    pub(crate) fn qv_p_count(&self, store: &GraphStore, predicate: TermId) -> Result<Option<u64>> {
        let Some(predicate) = store.query_term_id_from_snapshot(&self.snapshot, predicate)? else {
            return Ok(Some(0));
        };
        self.qv_count(store, QueryIndexCounterKey::Predicate(predicate), true)
    }

    pub(crate) fn qv_po_count(
        &self,
        store: &GraphStore,
        predicate: TermId,
        object: TermId,
    ) -> Result<Option<u64>> {
        let Some(predicate) = store.query_term_id_from_snapshot(&self.snapshot, predicate)? else {
            return Ok(Some(0));
        };
        let Some(object) = store.query_term_id_from_snapshot(&self.snapshot, object)? else {
            return Ok(Some(0));
        };
        self.qv_count(
            store,
            QueryIndexCounterKey::PredicateObject(predicate, object),
            true,
        )
    }

    pub(crate) fn qv_gp_count(
        &self,
        store: &GraphStore,
        graph: TermId,
        predicate: TermId,
    ) -> Result<Option<u64>> {
        let Some(graph) = store.query_term_id_from_snapshot(&self.snapshot, graph)? else {
            return Ok(Some(0));
        };
        let Some(predicate) = store.query_term_id_from_snapshot(&self.snapshot, predicate)? else {
            return Ok(Some(0));
        };
        self.qv_count(
            store,
            QueryIndexCounterKey::GraphPredicate(graph, predicate),
            true,
        )
    }

    pub(crate) fn qv_gpo_count(
        &self,
        store: &GraphStore,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<Option<u64>> {
        let Some(graph) = store.query_term_id_from_snapshot(&self.snapshot, graph)? else {
            return Ok(Some(0));
        };
        let Some(predicate) = store.query_term_id_from_snapshot(&self.snapshot, predicate)? else {
            return Ok(Some(0));
        };
        let Some(object) = store.query_term_id_from_snapshot(&self.snapshot, object)? else {
            return Ok(Some(0));
        };
        self.qv_count(
            store,
            QueryIndexCounterKey::GraphPredicateObject(graph, predicate, object),
            true,
        )
    }

    fn qv_count(
        &self,
        store: &GraphStore,
        key: QueryIndexCounterKey,
        zero_missing: bool,
    ) -> Result<Option<u64>> {
        match store.query_index_counter_from_snapshot(&self.snapshot, key)? {
            QueryIndexCounterRead::Value(count) => Ok(Some(count)),
            QueryIndexCounterRead::Missing if zero_missing => Ok(Some(0)),
            QueryIndexCounterRead::Missing | QueryIndexCounterRead::Malformed => Ok(None),
        }
    }

    pub(crate) fn contains_graph_by_id(&self, store: &GraphStore, graph: TermId) -> Result<bool> {
        Ok(self
            .snapshot
            .get(&store.graphs, graph_meta_key(graph))?
            .is_some())
    }

    pub(crate) fn graph_version(&self, store: &GraphStore, graph: TermId) -> Result<[u8; 32]> {
        let clock = store.snapshot_vector_clock(&self.snapshot, graph)?;
        Ok(*blake3::hash(&postcard::to_allocvec(&clock)?).as_bytes())
    }

    pub(crate) fn graph_term_id_iter<'a>(
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
            existing: decode_term_utf8(existing.as_ref())?,
        })
    }

    pub(crate) fn graph_policy(
        &self,
        store: &GraphStore,
        graph: &GraphId,
    ) -> Result<Option<GraphPolicy>> {
        let Some(graph) = self.lookup_term(store, &EncodedTerm::from_named_node(&graph.0))? else {
            return Ok(None);
        };
        let Some(bytes) = self.snapshot.get(&store.graphs, graph_meta_key(graph))? else {
            return Ok(None);
        };
        Ok(Some(
            postcard::from_bytes::<StoredGraphMeta>(bytes.as_ref())?.policy,
        ))
    }

    /// Returns the orphan ids implied by this exact snapshot. A matching
    /// persisted diagnostic record is cheap; stale or absent records are
    /// recomputed from snapshot quads and are never persisted or globally
    /// cached by reads.
    pub(crate) fn orphaned_entity_ids(
        &self,
        store: &GraphStore,
        context: &crate::query_context::ReadContext<'_>,
        graph: TermId,
    ) -> Result<HashSet<TermId>> {
        if !self.contains_graph_by_id(store, graph)? {
            return Ok(HashSet::new());
        }
        let clock = store.snapshot_vector_clock(&self.snapshot, graph)?;
        if let Some(record) = store.snapshot_stored_diagnostics(&self.snapshot, graph)?
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
        store.snapshot_orphaned_entity_ids(&self.snapshot, context, graph, &vocab)
    }
}

impl GraphStore {
    fn term_lock_index(&self, id: TermId) -> usize {
        (id.0 as usize) % self.term_locks.len()
    }

    // ── Locking ────────────────────────────────────────────────────

    /// Serialize the whole read→write→commit cycle for `graph`.
    ///
    /// See [`GraphCommitGuard`] for the lock order and the list of
    /// self-guarding functions that must not be called while this is held.
    pub(crate) fn graph_commit_guard(&self, graph: &GraphId) -> GraphCommitGuard<'_> {
        self.graph_commit_guard_by_id(hash_term(&EncodedTerm::from_named_node(&graph.0)))
    }

    /// Id-keyed twin of [`GraphStore::graph_commit_guard`]. The shard is chosen
    /// from the graph term id, so both entry points map to the same lock.
    pub(crate) fn graph_commit_guard_by_id(&self, graph_id: TermId) -> GraphCommitGuard<'_> {
        let shard = (graph_id.0 as usize) % self.commit_locks.len();
        #[cfg(feature = "shacl-core")]
        let wait_started = Instant::now();
        let guard = self.commit_locks[shard]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(feature = "shacl-core")]
        self.graph_commit_lock_wait_ns
            .fetch_add(elapsed_ns(wait_started.elapsed()), Ordering::Relaxed);
        GraphCommitGuard(guard)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn binding_guard(&self) -> BindingGuard<'_> {
        let wait_started = Instant::now();
        let guard = self
            .binding_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.binding_lock_wait_ns
            .fetch_add(elapsed_ns(wait_started.elapsed()), Ordering::Relaxed);
        BindingGuard {
            guard,
            hold_started: Instant::now(),
            hold_ns: &self.binding_lock_hold_ns,
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
            binding_lock_wait_ns: self.binding_lock_wait_ns.load(Ordering::Relaxed),
            binding_lock_hold_ns: self.binding_lock_hold_ns.load(Ordering::Relaxed),
            graph_commit_lock_wait_ns: self.graph_commit_lock_wait_ns.load(Ordering::Relaxed),
            validation_ns: self.validation_ns.load(Ordering::Relaxed),
            settlement_ns: self.settlement_ns.load(Ordering::Relaxed),
            settlement_failures: self.settlement_failures.load(Ordering::Relaxed),
            status_bindings_read: self.status_bindings_read.load(Ordering::Relaxed),
            status_version_checks: self.status_version_checks.load(Ordering::Relaxed),
            status_shape_compilations: self.status_shape_compilations.load(Ordering::Relaxed),
            status_full_shape_scans: self.status_full_shape_scans.load(Ordering::Relaxed),
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

    fn indexes_read(&self) -> RwLockReadGuard<'_, IndexState> {
        self.indexes.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn indexes_write(&self) -> RwLockWriteGuard<'_, IndexState> {
        self.indexes.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn query_index_header_from_snapshot(
        &self,
        snapshot: &Snapshot,
    ) -> Result<QueryIndexHeaderRead> {
        Ok(
            match snapshot.get(&self.qv2_meta, QUERY_INDEX_HEADER_KEY)? {
                Some(bytes) => decode_query_index_header(bytes.as_ref()),
                None => QueryIndexHeaderRead::Absent,
            },
        )
    }

    fn stage_query_index_header(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        header: &QueryIndexHeader,
    ) {
        batch.insert(
            &self.qv2_meta,
            QUERY_INDEX_HEADER_KEY,
            encode_query_index_header(header),
        );
    }

    fn stage_query_index_failed(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        previous: Option<&QueryIndexHeader>,
        reason: &'static str,
    ) {
        self.stage_query_index_header(batch, &QueryIndexHeader::failed_from(previous, reason));
    }

    fn query_term_id_from_snapshot(
        &self,
        snapshot: &Snapshot,
        term: TermId,
    ) -> Result<Option<QueryTermId>> {
        snapshot
            .get(&self.qv2_term_to_query, term.to_be_bytes())?
            .map(|value| decode_query_term_id_value(value.as_ref(), "term-to-query mapping"))
            .transpose()
    }

    fn source_term_id_from_snapshot(
        &self,
        snapshot: &Snapshot,
        term: QueryTermId,
    ) -> Result<Option<TermId>> {
        snapshot
            .get(&self.qv2_query_to_term, term.to_be_bytes())?
            .map(|value| decode_query_source_term_value(value.as_ref(), "query-to-term mapping"))
            .transpose()
    }

    fn query_quad_from_snapshot(
        &self,
        snapshot: &Snapshot,
        quad: EncodedQuad,
    ) -> Result<Option<QueryQuad>> {
        let Some(graph) = self.query_term_id_from_snapshot(snapshot, quad.graph)? else {
            return Ok(None);
        };
        let Some(subject) = self.query_term_id_from_snapshot(snapshot, quad.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = self.query_term_id_from_snapshot(snapshot, quad.predicate)? else {
            return Ok(None);
        };
        let Some(object) = self.query_term_id_from_snapshot(snapshot, quad.object)? else {
            return Ok(None);
        };
        Ok(Some(QueryQuad {
            graph,
            subject,
            predicate,
            object,
        }))
    }

    fn source_quad_from_snapshot(
        &self,
        snapshot: &Snapshot,
        quad: QueryQuad,
    ) -> Result<Option<EncodedQuad>> {
        let Some(graph) = self.source_term_id_from_snapshot(snapshot, quad.graph)? else {
            return Ok(None);
        };
        let Some(subject) = self.source_term_id_from_snapshot(snapshot, quad.subject)? else {
            return Ok(None);
        };
        let Some(predicate) = self.source_term_id_from_snapshot(snapshot, quad.predicate)? else {
            return Ok(None);
        };
        let Some(object) = self.source_term_id_from_snapshot(snapshot, quad.object)? else {
            return Ok(None);
        };
        Ok(Some(EncodedQuad {
            graph,
            subject,
            predicate,
            object,
        }))
    }

    fn count_live_source_rows(&self, snapshot: &Snapshot) -> Result<u64> {
        let mut rows = 0u64;
        for guard in snapshot.iter(&self.quads) {
            let (_, value) = guard.into_inner()?;
            if !dot_payload_is_empty(value.as_ref()) {
                rows = rows
                    .checked_add(1)
                    .ok_or(StoreError::QueryIndexVerificationFailed(
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
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "index-row-count-overflow",
                ))?;
            well_formed &= key.as_ref().len() == 32 && value.as_ref().is_empty();
        }
        Ok((rows, well_formed))
    }

    fn query_index_keyspaces_are_empty(&self, snapshot: &Snapshot) -> Result<bool> {
        for keyspace in [
            &self.qv2_gspo,
            &self.qv2_gpos,
            &self.qv2_spog,
            &self.qv2_posg,
            &self.qv2_ospg,
            &self.qv2_gosp,
            &self.qv2_term_to_query,
            &self.qv2_query_to_term,
            &self.qv2_meta,
        ] {
            if let Some(guard) = snapshot.iter(keyspace).next() {
                let _ = guard.into_inner()?;
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Captures the durable source/qv authority. Cache publication may follow
    /// it; generation checks keep stale cache entries off this read path.
    pub(crate) fn read_snapshot(&self) -> StoreReadSnapshot {
        StoreReadSnapshot {
            snapshot: self.db.snapshot(),
        }
    }

    fn query_index_snapshot(&self) -> Snapshot {
        self.read_snapshot().snapshot
    }

    /// O(1) qv2 eligibility gate for a single execution snapshot. Full source
    /// and qv cross-checking belongs to open-time verification and explicit
    /// maintenance checks; doing it here would erase the index's query value.
    fn snapshot_admission(&self, snapshot: &Snapshot) -> Result<QueryIndexAdmission> {
        #[cfg(test)]
        self.query_index_admission_probes
            .fetch_add(1, Ordering::Relaxed);
        if self.qv_degraded.load(Ordering::Acquire) {
            return Ok(QueryIndexAdmission {
                trusted: false,
                query_id_generation: None,
                query_id_upper_bound: None,
                fallback_reason: Some("concurrent-source-commit"),
                header_reads: 0,
                counter_reads: 0,
            });
        }
        let header = match self.query_index_header_from_snapshot(snapshot)? {
            QueryIndexHeaderRead::Absent => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    query_id_generation: None,
                    query_id_upper_bound: None,
                    fallback_reason: Some("metadata-missing"),
                    header_reads: 1,
                    counter_reads: 0,
                });
            }
            QueryIndexHeaderRead::Malformed => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    query_id_generation: None,
                    query_id_upper_bound: None,
                    fallback_reason: Some("metadata-malformed"),
                    header_reads: 1,
                    counter_reads: 0,
                });
            }
            QueryIndexHeaderRead::Valid(header) => header,
        };
        self.header_admission(snapshot, &header)
    }

    fn header_admission(
        &self,
        snapshot: &Snapshot,
        header: &QueryIndexHeader,
    ) -> Result<QueryIndexAdmission> {
        let fallback_reason = match &header.state {
            StoredQueryIndexState::Building => Some("index-building"),
            StoredQueryIndexState::Failed(_) => Some("index-failed"),
            StoredQueryIndexState::Ready if !header.ready_is_coherent() => {
                Some("metadata-incoherent")
            }
            StoredQueryIndexState::Ready if !header.is_not_ahead_of_snapshot(snapshot.seqno()) => {
                Some("metadata-ahead-of-snapshot")
            }
            StoredQueryIndexState::Ready => None,
        };
        if let Some(fallback_reason) = fallback_reason {
            return Ok(QueryIndexAdmission {
                trusted: false,
                query_id_generation: None,
                query_id_upper_bound: None,
                fallback_reason: Some(fallback_reason),
                header_reads: 1,
                counter_reads: 0,
            });
        }
        #[cfg(test)]
        self.query_index_admission_probes
            .fetch_add(1, Ordering::Relaxed);
        let (trusted, fallback_reason) = match self
            .query_index_counter_from_snapshot(snapshot, QueryIndexCounterKey::Total)?
        {
            QueryIndexCounterRead::Value(total) if total == header.indexed_quads => (true, None),
            QueryIndexCounterRead::Value(_) => (false, Some("total-counter-mismatch")),
            QueryIndexCounterRead::Missing => (false, Some("total-counter-missing")),
            QueryIndexCounterRead::Malformed => (false, Some("total-counter-malformed")),
        };
        Ok(QueryIndexAdmission {
            trusted,
            query_id_generation: trusted.then_some(header.query_id_generation),
            query_id_upper_bound: trusted.then_some(header.next_query_id),
            fallback_reason,
            header_reads: 1,
            counter_reads: 1,
        })
    }

    fn query_index_range(
        &self,
        snapshot: &Snapshot,
        order: QueryIndexCursorOrder,
        pattern: crate::rdf_read::QuadPattern,
    ) -> Result<Option<(&Keyspace, Vec<u8>)>> {
        let terms = match order {
            QueryIndexCursorOrder::Gspo => match (
                pattern.graph,
                pattern.subject,
                pattern.predicate,
                pattern.object,
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
            QueryIndexCursorOrder::Gpos => match (pattern.graph, pattern.predicate, pattern.object)
            {
                (Some(graph), Some(predicate), Some(object)) => vec![graph, predicate, object],
                (Some(graph), Some(predicate), None) => vec![graph, predicate],
                (Some(graph), None, _) => vec![graph],
                (None, _, _) => Vec::new(),
            },
            QueryIndexCursorOrder::Spog => {
                match (pattern.subject, pattern.predicate, pattern.object) {
                    (Some(subject), Some(predicate), Some(object)) => {
                        vec![subject, predicate, object]
                    }
                    (Some(subject), Some(predicate), None) => vec![subject, predicate],
                    (Some(subject), None, _) => vec![subject],
                    (None, _, _) => Vec::new(),
                }
            }
            QueryIndexCursorOrder::Posg => match (pattern.predicate, pattern.object) {
                (Some(predicate), Some(object)) => vec![predicate, object],
                (Some(predicate), None) => vec![predicate],
                (None, _) => Vec::new(),
            },
            QueryIndexCursorOrder::Ospg => {
                match (pattern.object, pattern.subject, pattern.predicate) {
                    (Some(object), Some(subject), Some(predicate)) => {
                        vec![object, subject, predicate]
                    }
                    (Some(object), Some(subject), None) => vec![object, subject],
                    (Some(object), None, _) => vec![object],
                    (None, _, _) => Vec::new(),
                }
            }
            QueryIndexCursorOrder::Gosp => match (
                pattern.graph,
                pattern.object,
                pattern.subject,
                pattern.predicate,
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
        let keyspace = match order {
            QueryIndexCursorOrder::Gspo => &self.qv2_gspo,
            QueryIndexCursorOrder::Gpos => &self.qv2_gpos,
            QueryIndexCursorOrder::Spog => &self.qv2_spog,
            QueryIndexCursorOrder::Posg => &self.qv2_posg,
            QueryIndexCursorOrder::Ospg => &self.qv2_ospg,
            QueryIndexCursorOrder::Gosp => &self.qv2_gosp,
        };
        let mut query_terms = Vec::with_capacity(terms.len());
        for term in terms {
            let Some(term) = self.query_term_id_from_snapshot(snapshot, term)? else {
                return Ok(None);
            };
            query_terms.push(term);
        }
        Ok(Some((keyspace, query_index_prefix(&query_terms))))
    }

    pub(crate) fn query_index_status(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let snapshot_sequence = snapshot.seqno();
        let header = self.query_index_header_from_snapshot(&snapshot)?;
        let source_live_quads = self.count_live_source_rows(&snapshot)?;
        let (indexed_quads, gpos_well_formed) =
            self.summarize_qv_rows(&snapshot, &self.qv2_gpos)?;
        let (gspo_quads, gspo_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv2_gspo)?;
        let (spog_quads, spog_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv2_spog)?;
        let (posg_quads, posg_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv2_posg)?;
        let (ospg_quads, ospg_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv2_ospg)?;
        let (gosp_quads, gosp_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv2_gosp)?;
        let (state, last_build_sequence, query_id_generation, query_term_ids) = match header {
            QueryIndexHeaderRead::Absent => (QueryIndexState::Missing, 0, 0, 0),
            QueryIndexHeaderRead::Malformed => (
                QueryIndexState::Failed("metadata-malformed".to_owned()),
                0,
                0,
                0,
            ),
            QueryIndexHeaderRead::Valid(header) => {
                let total_matches_header = matches!(
                    self.query_index_counter_from_snapshot(&snapshot, QueryIndexCounterKey::Total)?,
                    QueryIndexCounterRead::Value(total) if total == header.indexed_quads
                );
                let ready_matches_snapshot = header.ready_is_coherent()
                    && header.is_not_ahead_of_snapshot(snapshot_sequence)
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
                if matches!(header.state, StoredQueryIndexState::Ready) && !ready_matches_snapshot {
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
            schema_version: QUERY_INDEX_SCHEMA_VERSION,
            state,
            query_id_generation,
            query_term_ids,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        })
    }

    pub(crate) fn query_index_status_fast(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let (
            state,
            query_id_generation,
            query_term_ids,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        ) = match self.query_index_header_from_snapshot(&snapshot)? {
            QueryIndexHeaderRead::Absent => (QueryIndexState::Missing, 0, 0, 0, 0, 0),
            QueryIndexHeaderRead::Malformed => (
                QueryIndexState::Failed("metadata-malformed".to_owned()),
                0,
                0,
                0,
                0,
                0,
            ),
            QueryIndexHeaderRead::Valid(header) => {
                #[cfg(test)]
                self.query_index_admission_probes
                    .fetch_add(1, Ordering::Relaxed);
                if self.qv_degraded.load(Ordering::Acquire) {
                    return Ok(QueryIndexStatus {
                        schema_version: QUERY_INDEX_SCHEMA_VERSION,
                        state: QueryIndexState::Failed("concurrent-source-commit".to_owned()),
                        query_id_generation: header.query_id_generation,
                        query_term_ids: header.next_query_id,
                        source_live_quads: header.source_live_quads,
                        indexed_quads: header.indexed_quads,
                        last_build_sequence: header.last_build_sequence,
                    });
                }
                let admission = self.header_admission(&snapshot, &header)?;
                let state =
                    if matches!(header.state, StoredQueryIndexState::Ready) && !admission.trusted {
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
            schema_version: QUERY_INDEX_SCHEMA_VERSION,
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
        mode: impl Into<QueryIndexVerificationMode>,
    ) -> Result<QueryIndexVerification> {
        let snapshot = self.query_index_snapshot();
        let mode = mode.into();
        self.verify_query_index_snapshot(
            &snapshot,
            matches!(mode, QueryIndexVerificationMode::Full),
            QueryIndexVerificationExpectation::Ready,
        )
    }

    fn initialize_query_indexes_at_open(&self) -> Result<()> {
        let snapshot = self.db.snapshot();
        match self.query_index_header_from_snapshot(&snapshot)? {
            QueryIndexHeaderRead::Absent => {
                let source_live_quads = self.count_live_source_rows(&snapshot)?;
                if source_live_quads != 0 {
                    return Ok(());
                }
                if !self.query_index_keyspaces_are_empty(&snapshot)? {
                    let mut batch = self.buffered_batch();
                    self.stage_query_index_failed(
                        &mut batch,
                        None,
                        "metadata-missing-with-derived-residue",
                    );
                    return self.commit_fjall_batch(batch);
                }
                let mut batch = self.buffered_batch();
                self.stage_query_index_header(&mut batch, &QueryIndexHeader::empty_ready());
                batch.insert(&self.qv2_meta, QUERY_INDEX_TOTAL_KEY, 0u64.to_be_bytes());
                batch.insert(
                    &self.qv2_meta,
                    QueryIndexCounterKey::UnionDuplicateFree.bytes(),
                    1u64.to_be_bytes(),
                );
                self.commit_fjall_batch(batch)
            }
            QueryIndexHeaderRead::Malformed => {
                let mut batch = self.buffered_batch();
                self.stage_query_index_failed(&mut batch, None, "metadata-malformed");
                self.commit_fjall_batch(batch)
            }
            QueryIndexHeaderRead::Valid(header) => {
                if !matches!(header.state, StoredQueryIndexState::Ready) {
                    return Ok(());
                }
                let admission = self.snapshot_admission(&snapshot)?;
                if admission.trusted {
                    return Ok(());
                }
                let mut batch = self.buffered_batch();
                self.stage_query_index_failed(&mut batch, Some(&header), "open-admission-failed");
                self.commit_fjall_batch(batch)
            }
        }
    }

    fn query_index_row_is_sampled(full: bool, checked: u64) -> bool {
        full || checked < QUERY_INDEX_SAMPLE_ROWS
    }

    fn qv_row_is_present_and_empty(
        &self,
        snapshot: &Snapshot,
        keyspace: &Keyspace,
        key: QueryQuadKey,
    ) -> Result<bool> {
        Ok(snapshot
            .get(keyspace, key)?
            .is_some_and(|value| value.as_ref().is_empty()))
    }

    fn verify_source_to_qv_rows(
        &self,
        snapshot: &Snapshot,
        full: bool,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        for guard in snapshot.iter(&self.quads) {
            let (key, value) = guard.into_inner()?;
            if dot_payload_is_empty(value.as_ref()) {
                continue;
            }
            report.report.source_live_quads =
                report.report.source_live_quads.checked_add(1).ok_or(
                    StoreError::QueryIndexVerificationFailed("source-row-count-overflow"),
                )?;
            let Some(quad) = decode_source_quad_key(key.as_ref()) else {
                report.problem("source-key-length");
                continue;
            };
            if !Self::query_index_row_is_sampled(full, report.report.checked_source_rows) {
                continue;
            }
            report.report.checked_source_rows =
                report.report.checked_source_rows.checked_add(1).ok_or(
                    StoreError::QueryIndexVerificationFailed("source-check-count-overflow"),
                )?;
            let Some(quad) = self.query_quad_from_snapshot(snapshot, quad)? else {
                report.problem("source-query-id-mapping-missing");
                continue;
            };
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_gspo, qv2_gspo_key(quad))? {
                report.problem("source-gspo-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_gpos, qv2_gpos_key(quad))? {
                report.problem("source-gpos-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_spog, qv2_spog_key(quad))? {
                report.problem("source-spog-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_posg, qv2_posg_key(quad))? {
                report.problem("source-posg-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_ospg, qv2_ospg_key(quad))? {
                report.problem("source-ospg-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv2_gosp, qv2_gosp_key(quad))? {
                report.problem("source-gosp-missing-or-nonempty");
            }
        }
        Ok(())
    }

    fn verify_qv_rows(
        &self,
        snapshot: &Snapshot,
        order: QueryIndexKeyOrder,
        full: bool,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<u64> {
        let (keyspace, key_problem, value_problem, source_problem) = match order {
            QueryIndexKeyOrder::Gspo => (
                &self.qv2_gspo,
                "qv-gspo-key-length",
                "qv-gspo-value-nonempty",
                "qv-gspo-source-missing",
            ),
            QueryIndexKeyOrder::Gpos => (
                &self.qv2_gpos,
                "qv-gpos-key-length",
                "qv-gpos-value-nonempty",
                "qv-gpos-source-missing",
            ),
            QueryIndexKeyOrder::Spog => (
                &self.qv2_spog,
                "qv-spog-key-length",
                "qv-spog-value-nonempty",
                "qv-spog-source-missing",
            ),
            QueryIndexKeyOrder::Posg => (
                &self.qv2_posg,
                "qv-posg-key-length",
                "qv-posg-value-nonempty",
                "qv-posg-source-missing",
            ),
            QueryIndexKeyOrder::Ospg => (
                &self.qv2_ospg,
                "qv-ospg-key-length",
                "qv-ospg-value-nonempty",
                "qv-ospg-source-missing",
            ),
            QueryIndexKeyOrder::Gosp => (
                &self.qv2_gosp,
                "qv-gosp-key-length",
                "qv-gosp-value-nonempty",
                "qv-gosp-source-missing",
            ),
        };
        let mut rows = 0u64;
        let mut checked_in_this_keyspace = 0u64;
        for guard in snapshot.iter(keyspace) {
            let (key, value) = guard.into_inner()?;
            rows = rows
                .checked_add(1)
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "index-row-count-overflow",
                ))?;
            if !Self::query_index_row_is_sampled(full, checked_in_this_keyspace) {
                continue;
            }
            checked_in_this_keyspace = checked_in_this_keyspace.checked_add(1).ok_or(
                StoreError::QueryIndexVerificationFailed("index-check-count-overflow"),
            )?;
            report.report.checked_index_rows =
                report.report.checked_index_rows.checked_add(1).ok_or(
                    StoreError::QueryIndexVerificationFailed("index-check-count-overflow"),
                )?;
            if !value.as_ref().is_empty() {
                report.problem(value_problem);
            }
            let quad = match order {
                QueryIndexKeyOrder::Gspo => decode_qv2_gspo_key(key.as_ref()),
                QueryIndexKeyOrder::Gpos => decode_qv2_gpos_key(key.as_ref()),
                QueryIndexKeyOrder::Spog => decode_qv2_spog_key(key.as_ref()),
                QueryIndexKeyOrder::Posg => decode_qv2_posg_key(key.as_ref()),
                QueryIndexKeyOrder::Ospg => decode_qv2_ospg_key(key.as_ref()),
                QueryIndexKeyOrder::Gosp => decode_qv2_gosp_key(key.as_ref()),
            };
            let Some(quad) = quad else {
                report.problem(key_problem);
                continue;
            };
            let Some(quad) = self.source_quad_from_snapshot(snapshot, quad)? else {
                report.problem("qv-query-id-mapping-missing");
                continue;
            };
            let source_key = Self::quad_key(quad.graph, quad.subject, quad.predicate, quad.object);
            let source_is_live = snapshot
                .get(&self.quads, source_key)?
                .is_some_and(|source| !dot_payload_is_empty(source.as_ref()));
            if !source_is_live {
                report.problem(source_problem);
            }
        }
        Ok(rows)
    }

    fn verify_expected_query_index_counter(
        &self,
        snapshot: &Snapshot,
        key: QueryIndexCounterKey,
        expected: u64,
        missing_problem: &'static str,
        mismatch_problem: &'static str,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        match snapshot.get(&self.qv2_meta, key.bytes())? {
            None => report.problem(missing_problem),
            Some(value) => match decode_query_index_u64(value.as_ref()) {
                Some(actual) if actual == expected => {}
                _ => report.problem(mismatch_problem),
            },
        }
        Ok(())
    }

    fn verify_gpos_counter_group(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        terms: [QueryTermId; 3],
        expected: u64,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let (key, missing_problem, mismatch_problem) = match dimension {
            1 => (
                QueryIndexCounterKey::Graph(terms[0]),
                "counter-g-missing",
                "counter-g-mismatch",
            ),
            2 => (
                QueryIndexCounterKey::GraphPredicate(terms[0], terms[1]),
                "counter-gp-missing",
                "counter-gp-mismatch",
            ),
            3 => (
                QueryIndexCounterKey::GraphPredicateObject(terms[0], terms[1], terms[2]),
                "counter-gpo-missing",
                "counter-gpo-mismatch",
            ),
            _ => unreachable!("only GPOS counter dimensions are used"),
        };
        self.verify_expected_query_index_counter(
            snapshot,
            key,
            expected,
            missing_problem,
            mismatch_problem,
            report,
        )
    }

    fn verify_gpos_counter_dimension(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let mut current = None::<[QueryTermId; 3]>;
        let mut count = 0u64;
        for guard in snapshot.iter(&self.qv2_gpos) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_qv2_gpos_key(key.as_ref()) else {
                continue;
            };
            let terms = [quad.graph, quad.predicate, quad.object];
            if let Some(previous) = current
                && previous[..dimension] != terms[..dimension]
            {
                self.verify_gpos_counter_group(snapshot, dimension, previous, count, report)?;
                count = 0;
            }
            current = Some(terms);
            count = count
                .checked_add(1)
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "counter-count-overflow",
                ))?;
        }
        if let Some(previous) = current {
            self.verify_gpos_counter_group(snapshot, dimension, previous, count, report)?;
        }
        Ok(())
    }

    fn verify_posg_counter_group(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        terms: [QueryTermId; 2],
        expected: u64,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let (key, missing_problem, mismatch_problem) = match dimension {
            1 => (
                QueryIndexCounterKey::Predicate(terms[0]),
                "counter-p-missing",
                "counter-p-mismatch",
            ),
            2 => (
                QueryIndexCounterKey::PredicateObject(terms[0], terms[1]),
                "counter-po-missing",
                "counter-po-mismatch",
            ),
            _ => unreachable!("only POSG counter dimensions are used"),
        };
        self.verify_expected_query_index_counter(
            snapshot,
            key,
            expected,
            missing_problem,
            mismatch_problem,
            report,
        )
    }

    fn verify_posg_counter_dimension(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let mut current = None::<[QueryTermId; 2]>;
        let mut count = 0u64;
        for guard in snapshot.iter(&self.qv2_posg) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_qv2_posg_key(key.as_ref()) else {
                continue;
            };
            let terms = [quad.predicate, quad.object];
            if let Some(previous) = current
                && previous[..dimension] != terms[..dimension]
            {
                self.verify_posg_counter_group(snapshot, dimension, previous, count, report)?;
                count = 0;
            }
            current = Some(terms);
            count = count
                .checked_add(1)
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "counter-count-overflow",
                ))?;
        }
        if let Some(previous) = current {
            self.verify_posg_counter_group(snapshot, dimension, previous, count, report)?;
        }
        Ok(())
    }

    fn query_index_counter_has_rows(
        &self,
        snapshot: &Snapshot,
        key: QueryIndexCounterKey,
    ) -> Result<bool> {
        let mut rows = match key {
            QueryIndexCounterKey::Graph(graph) => {
                snapshot.prefix(&self.qv2_gpos, query_index_prefix(&[graph]))
            }
            QueryIndexCounterKey::Predicate(predicate) => {
                snapshot.prefix(&self.qv2_posg, query_index_prefix(&[predicate]))
            }
            QueryIndexCounterKey::GraphPredicate(graph, predicate) => {
                snapshot.prefix(&self.qv2_gpos, query_index_prefix(&[graph, predicate]))
            }
            QueryIndexCounterKey::PredicateObject(predicate, object) => {
                snapshot.prefix(&self.qv2_posg, query_index_prefix(&[predicate, object]))
            }
            QueryIndexCounterKey::GraphPredicateObject(graph, predicate, object) => snapshot
                .prefix(
                    &self.qv2_gpos,
                    query_index_prefix(&[graph, predicate, object]),
                ),
            QueryIndexCounterKey::Total | QueryIndexCounterKey::UnionDuplicateFree => {
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

    fn verify_query_index_meta_records(
        &self,
        snapshot: &Snapshot,
        header: Option<&QueryIndexHeader>,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let mut headers = 0u64;
        let mut totals = 0u64;
        let mut union_proofs = 0u64;
        for guard in snapshot.iter(&self.qv2_meta) {
            let (key, value) = guard.into_inner()?;
            match decode_query_index_counter_key(key.as_ref()) {
                QueryIndexCounterKeyRead::Header => {
                    headers =
                        headers
                            .checked_add(1)
                            .ok_or(StoreError::QueryIndexVerificationFailed(
                                "metadata-count-overflow",
                            ))?;
                    if !matches!(
                        decode_query_index_header(value.as_ref()),
                        QueryIndexHeaderRead::Valid(_)
                    ) {
                        report.problem("meta-header-malformed");
                    }
                }
                QueryIndexCounterKeyRead::Counter(counter) => {
                    let Some(value) = decode_query_index_u64(value.as_ref()) else {
                        report.problem("meta-counter-value-length");
                        continue;
                    };
                    match counter {
                        QueryIndexCounterKey::Total => {
                            totals = totals.checked_add(1).ok_or(
                                StoreError::QueryIndexVerificationFailed("metadata-count-overflow"),
                            )?;
                            match header {
                                Some(header) if value == header.indexed_quads => {}
                                Some(_) => report.problem("meta-total-mismatch"),
                                None => report.problem("meta-total-without-header"),
                            }
                        }
                        QueryIndexCounterKey::UnionDuplicateFree => {
                            union_proofs = union_proofs.checked_add(1).ok_or(
                                StoreError::QueryIndexVerificationFailed("metadata-count-overflow"),
                            )?;
                            if value > 1 {
                                report.problem("union-proof-value-invalid");
                            } else if value == 1
                                && !self.query_index_union_duplicate_free(snapshot)?
                            {
                                report.problem("union-proof-mismatch");
                            }
                        }
                        _ => {
                            if value == 0 {
                                report.problem("meta-counter-zero");
                            }
                            if !self.query_index_counter_has_rows(snapshot, counter)? {
                                report.problem("meta-counter-orphan");
                            }
                        }
                    }
                }
                QueryIndexCounterKeyRead::UnknownTag => report.problem("meta-unknown-tag"),
                QueryIndexCounterKeyRead::InvalidLength => {
                    report.problem("meta-counter-key-length")
                }
            }
        }
        if headers != 1 {
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

    fn verify_query_id_mappings(
        &self,
        snapshot: &Snapshot,
        header: Option<&QueryIndexHeader>,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let mut forward_count = 0u64;
        for guard in snapshot.iter(&self.qv2_term_to_query) {
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
                    .ok_or(StoreError::QueryIndexVerificationFailed(
                        "query-id-mapping-count-overflow",
                    ))?;
            match snapshot.get(&self.qv2_query_to_term, query.to_be_bytes())? {
                Some(reverse) if reverse.as_ref() == term.to_be_bytes() => {}
                _ => report.problem("term-to-query-reverse-mismatch"),
            }
            if header.is_some_and(|header| query.0 >= header.next_query_id) {
                report.problem("query-id-outside-header-range");
            }
        }

        let mut reverse_count = 0u64;
        for guard in snapshot.iter(&self.qv2_query_to_term) {
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
                    .ok_or(StoreError::QueryIndexVerificationFailed(
                        "query-id-mapping-count-overflow",
                    ))?;
            match snapshot.get(&self.qv2_term_to_query, term.to_be_bytes())? {
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

    fn verify_query_index_snapshot(
        &self,
        snapshot: &Snapshot,
        full: bool,
        expected_state: QueryIndexVerificationExpectation,
    ) -> Result<QueryIndexVerification> {
        #[cfg(test)]
        self.query_index_verification_runs
            .fetch_add(1, Ordering::Relaxed);
        let header_read = self.query_index_header_from_snapshot(snapshot)?;
        let snapshot_sequence = snapshot.seqno();
        let header = match &header_read {
            QueryIndexHeaderRead::Valid(header) => Some(header),
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => None,
        };
        let mut report = QueryIndexVerificationBuilder::new(full);
        self.verify_source_to_qv_rows(snapshot, full, &mut report)?;
        let gspo_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Gspo, full, &mut report)?;
        let gpos_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Gpos, full, &mut report)?;
        let spog_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Spog, full, &mut report)?;
        let posg_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Posg, full, &mut report)?;
        let ospg_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Ospg, full, &mut report)?;
        let gosp_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Gosp, full, &mut report)?;
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
                QueryIndexHeaderRead::Absent => report.problem("meta-header-missing"),
                QueryIndexHeaderRead::Malformed => report.problem("meta-header-malformed"),
                QueryIndexHeaderRead::Valid(_) => unreachable!("valid header was retained"),
            },
            Some(header) => {
                let expected_state_matches = match expected_state {
                    QueryIndexVerificationExpectation::Ready => {
                        matches!(header.state, StoredQueryIndexState::Ready)
                    }
                    QueryIndexVerificationExpectation::BuildingCandidate => {
                        matches!(header.state, StoredQueryIndexState::Building)
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
            self.verify_gpos_counter_dimension(snapshot, 1, &mut report)?;
            self.verify_gpos_counter_dimension(snapshot, 2, &mut report)?;
            self.verify_gpos_counter_dimension(snapshot, 3, &mut report)?;
            self.verify_posg_counter_dimension(snapshot, 1, &mut report)?;
            self.verify_posg_counter_dimension(snapshot, 2, &mut report)?;
            self.verify_query_index_meta_records(snapshot, header, &mut report)?;
            self.verify_query_id_mappings(snapshot, header, &mut report)?;
        }
        Ok(report.finish())
    }

    fn pending_term_in_batch<'a>(
        &self,
        batch: Option<&'a WriteBatch>,
        id: TermId,
    ) -> Option<&'a str> {
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

        if let Some(existing) = self.pending_term_in_batch(batch.as_deref(), id) {
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
        if let Some(existing) = self.pending_term_in_batch(batch.as_deref(), id) {
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

    fn read_graph_meta_by_id(&self, graph: TermId) -> Result<Option<StoredGraphMeta>> {
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

    fn clear_query_index_keyspace(&self, keyspace: &Keyspace, retain_header: bool) -> Result<()> {
        let snapshot = self.db.snapshot();
        let mut batch = self.buffered_batch();
        let mut pending = 0usize;
        for guard in snapshot.iter(keyspace) {
            let (key, _) = guard.into_inner()?;
            if retain_header && key.as_ref() == QUERY_INDEX_HEADER_KEY {
                continue;
            }
            batch.remove(keyspace, key);
            pending += 1;
            if pending == QUERY_INDEX_BUILD_CHUNK_ROWS {
                self.commit_fjall_batch(batch)?;
                batch = self.buffered_batch();
                pending = 0;
            }
        }
        if pending != 0 {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    fn clear_query_index_derived_data(&self) -> Result<()> {
        self.clear_query_index_keyspace(&self.qv2_gspo, false)?;
        self.clear_query_index_keyspace(&self.qv2_gpos, false)?;
        self.clear_query_index_keyspace(&self.qv2_spog, false)?;
        self.clear_query_index_keyspace(&self.qv2_posg, false)?;
        self.clear_query_index_keyspace(&self.qv2_ospg, false)?;
        self.clear_query_index_keyspace(&self.qv2_gosp, false)?;
        self.clear_query_index_keyspace(&self.qv2_term_to_query, false)?;
        self.clear_query_index_keyspace(&self.qv2_query_to_term, false)?;
        self.clear_query_index_keyspace(&self.qv2_meta, true)
    }

    fn rebuild_query_term_id(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        allocated: &mut HashMap<TermId, QueryTermId>,
        next_query_id: &mut u64,
        term: TermId,
    ) -> Result<QueryTermId> {
        if let Some(query) = allocated.get(&term) {
            return Ok(*query);
        }
        if let Some(value) = self.qv2_term_to_query.get(term.to_be_bytes())? {
            let query = decode_query_term_id_value(value.as_ref(), "term-to-query mapping")?;
            let Some(reverse) = self.qv2_query_to_term.get(query.to_be_bytes())? else {
                return Err(StoreError::QueryIndexVerificationFailed(
                    "rebuild-query-id-reverse-missing",
                ));
            };
            if reverse.as_ref() != term.to_be_bytes() {
                return Err(StoreError::QueryIndexVerificationFailed(
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
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "query-id-space-exhausted",
                ))?;
        if self.qv2_query_to_term.get(query.to_be_bytes())?.is_some() {
            return Err(StoreError::QueryIndexVerificationFailed(
                "rebuild-query-id-already-used",
            ));
        }
        batch.insert(
            &self.qv2_term_to_query,
            term.to_be_bytes(),
            query.to_be_bytes(),
        );
        batch.insert(
            &self.qv2_query_to_term,
            query.to_be_bytes(),
            term.to_be_bytes(),
        );
        allocated.insert(term, query);
        Ok(query)
    }

    fn build_query_index_chunk(
        &self,
        quads: &[EncodedQuad],
        next_query_id: &mut u64,
    ) -> Result<()> {
        let mut increments = BTreeMap::<Vec<u8>, (QueryIndexCounterKey, u64)>::new();
        let mut allocated = HashMap::new();
        let mut batch = self.buffered_batch();
        for quad in quads {
            let quad = QueryQuad {
                graph: self.rebuild_query_term_id(
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.graph,
                )?,
                subject: self.rebuild_query_term_id(
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.subject,
                )?,
                predicate: self.rebuild_query_term_id(
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.predicate,
                )?,
                object: self.rebuild_query_term_id(
                    &mut batch,
                    &mut allocated,
                    next_query_id,
                    quad.object,
                )?,
            };
            for (keyspace, key) in [
                (&self.qv2_gspo, qv2_gspo_key(quad)),
                (&self.qv2_gpos, qv2_gpos_key(quad)),
                (&self.qv2_spog, qv2_spog_key(quad)),
                (&self.qv2_posg, qv2_posg_key(quad)),
                (&self.qv2_ospg, qv2_ospg_key(quad)),
                (&self.qv2_gosp, qv2_gosp_key(quad)),
            ] {
                batch.insert(keyspace, key, Vec::<u8>::new());
            }
            for counter in query_index_live_counter_keys(quad) {
                let entry = increments.entry(counter.bytes()).or_insert((counter, 0));
                entry.1 =
                    entry
                        .1
                        .checked_add(1)
                        .ok_or(StoreError::QueryIndexVerificationFailed(
                            "rebuild-counter-overflow",
                        ))?;
            }
        }
        for (_, (counter, increment)) in increments {
            let current = match self.qv2_meta.get(counter.bytes())? {
                None => 0,
                Some(value) => decode_query_index_u64(value.as_ref()).ok_or(
                    StoreError::QueryIndexVerificationFailed("rebuild-counter-malformed"),
                )?,
            };
            let next =
                current
                    .checked_add(increment)
                    .ok_or(StoreError::QueryIndexVerificationFailed(
                        "rebuild-counter-overflow",
                    ))?;
            batch.insert(&self.qv2_meta, counter.bytes(), next.to_be_bytes());
        }
        self.commit_fjall_batch(batch)
    }

    fn build_query_index_rows(&self, snapshot: &Snapshot) -> Result<(u64, u64)> {
        let mut rows = 0u64;
        let mut next_query_id = 0u64;
        let mut chunk = Vec::with_capacity(QUERY_INDEX_BUILD_CHUNK_ROWS);
        for guard in snapshot.iter(&self.quads) {
            let (key, value) = guard.into_inner()?;
            if dot_payload_is_empty(value.as_ref()) {
                continue;
            }
            let quad = decode_source_quad_key(key.as_ref()).ok_or(
                StoreError::QueryIndexVerificationFailed("rebuild-source-key-malformed"),
            )?;
            rows = rows
                .checked_add(1)
                .ok_or(StoreError::QueryIndexVerificationFailed(
                    "rebuild-source-count-overflow",
                ))?;
            chunk.push(quad);
            if chunk.len() == QUERY_INDEX_BUILD_CHUNK_ROWS {
                self.build_query_index_chunk(&chunk, &mut next_query_id)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            self.build_query_index_chunk(&chunk, &mut next_query_id)?;
        }
        Ok((rows, next_query_id))
    }

    fn query_index_union_duplicate_free(&self, snapshot: &Snapshot) -> Result<bool> {
        let mut previous = None;
        for guard in snapshot.iter(&self.qv2_spog) {
            let (key, _) = guard.into_inner()?;
            let quad = decode_qv2_spog_key(key.as_ref()).ok_or(
                StoreError::QueryIndexVerificationFailed("union-proof-row-malformed"),
            )?;
            let current = (quad.subject, quad.predicate, quad.object);
            if previous == Some(current) {
                return Ok(false);
            }
            previous = Some(current);
        }
        Ok(true)
    }

    fn mark_query_index_rebuild_failed(&self, reason: &'static str) -> Result<()> {
        let snapshot = self.db.snapshot();
        let previous = match self.query_index_header_from_snapshot(&snapshot)? {
            QueryIndexHeaderRead::Valid(header) => Some(header),
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        self.stage_query_index_failed(&mut batch, previous.as_ref(), reason);
        self.commit_fjall_batch(batch)
    }

    pub(crate) fn rebuild_query_indexes(&self) -> Result<()> {
        if self
            .qv_commit_state
            .compare_exchange(0, QV_COMMIT_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(StoreError::QueryIndexUnavailable(
                "query-index rebuild overlaps a graph commit",
            ));
        }
        let _qv_reset = QueryIndexCommitReset(&self.qv_commit_state);
        let initial_snapshot = self.db.snapshot();
        let previous = match self.query_index_header_from_snapshot(&initial_snapshot)? {
            QueryIndexHeaderRead::Valid(header) => Some(header),
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => None,
        };
        let mut building = previous
            .clone()
            .unwrap_or_else(QueryIndexHeader::empty_ready);
        building.state = StoredQueryIndexState::Building;
        {
            let mut batch = self.buffered_batch();
            self.stage_query_index_header(&mut batch, &building);
            self.commit_fjall_batch(batch)?;
        }

        let result = (|| -> Result<()> {
            self.clear_query_index_derived_data()?;
            let source_snapshot = self.db.snapshot();
            let source_sequence = source_snapshot.seqno();
            let last_build_sequence = previous
                .as_ref()
                .filter(|header| header.last_build_sequence <= source_sequence)
                .and_then(|header| header.last_build_sequence.checked_add(1))
                .map(|next| next.max(source_sequence))
                .unwrap_or(source_sequence);
            let source_epoch = previous
                .as_ref()
                .filter(|header| {
                    header.source_epoch <= source_sequence && header.index_epoch <= source_sequence
                })
                .map(|header| header.source_epoch.max(header.index_epoch))
                .and_then(|prior| prior.max(source_sequence).checked_add(1))
                .unwrap_or(source_sequence);
            let query_id_generation = previous
                .as_ref()
                .and_then(|header| header.query_id_generation.checked_add(1))
                .unwrap_or(1);
            let (source_live_quads, next_query_id) =
                self.build_query_index_rows(&source_snapshot)?;
            let union_duplicate_free =
                self.query_index_union_duplicate_free(&self.db.snapshot())?;
            let candidate = QueryIndexHeader {
                state: StoredQueryIndexState::Building,
                source_epoch,
                index_epoch: source_epoch,
                source_live_quads,
                indexed_quads: source_live_quads,
                last_build_sequence,
                query_id_generation,
                next_query_id,
            };
            {
                let mut batch = self.buffered_batch();
                batch.insert(
                    &self.qv2_meta,
                    QUERY_INDEX_TOTAL_KEY,
                    source_live_quads.to_be_bytes(),
                );
                batch.insert(
                    &self.qv2_meta,
                    QueryIndexCounterKey::UnionDuplicateFree.bytes(),
                    u64::from(union_duplicate_free).to_be_bytes(),
                );
                self.stage_query_index_header(&mut batch, &candidate);
                self.commit_fjall_batch(batch)?;
            }
            let verification_snapshot = self.db.snapshot();
            let report = self.verify_query_index_snapshot(
                &verification_snapshot,
                true,
                QueryIndexVerificationExpectation::BuildingCandidate,
            )?;
            if !report.valid {
                return Err(StoreError::QueryIndexVerificationFailed(
                    "rebuild-verification-failed",
                ));
            }
            let mut ready = candidate;
            ready.state = StoredQueryIndexState::Ready;
            let mut batch = self.buffered_batch();
            self.stage_query_index_header(&mut batch, &ready);
            self.commit_fjall_batch(batch)
        })();
        if result.is_err() {
            let _ = self.mark_query_index_rebuild_failed("rebuild-failed");
        }
        let clean = self.finish_query_index_commit();
        if result.is_ok() && clean {
            self.qv_degraded.store(false, Ordering::Release);
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
            self.commit_stall_max_active
                .fetch_max(active, Ordering::SeqCst);
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
        self.commit_stall_max_active.store(0, Ordering::SeqCst);
    }

    /// Whether a commit is inside its post-durable publication stall.
    #[cfg(test)]
    pub(crate) fn commit_stalled(&self) -> bool {
        self.commit_stalled.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn commit_stall_max_active(&self) -> usize {
        self.commit_stall_max_active.load(Ordering::SeqCst)
    }

    /// Make exactly the next durable batch commit fail. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_commit_failure(&self) {
        self.commit_failure.store(true, Ordering::SeqCst);
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
    fn stall_in_fts_ack(&self) {
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
    pub(crate) fn set_fts_ack_stall(&self, delay: std::time::Duration) {
        *self
            .fts_ack_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    fn query_index_counter_from_snapshot(
        &self,
        snapshot: &Snapshot,
        key: QueryIndexCounterKey,
    ) -> Result<QueryIndexCounterRead> {
        Ok(match snapshot.get(&self.qv2_meta, key.bytes())? {
            None => QueryIndexCounterRead::Missing,
            Some(value) => match decode_query_index_u64(value.as_ref()) {
                Some(value) => QueryIndexCounterRead::Value(value),
                None => QueryIndexCounterRead::Malformed,
            },
        })
    }

    fn adjusted_query_index_counter(current: u64, delta: i128) -> Option<u64> {
        if delta >= 0 {
            current.checked_add(u64::try_from(delta).ok()?)
        } else {
            current.checked_sub(u64::try_from(delta.checked_neg()?).ok()?)
        }
    }

    fn resolve_maintenance_query_term(
        &self,
        snapshot: &Snapshot,
        term: TermId,
        allow_allocate: bool,
        resolved: &mut HashMap<TermId, QueryTermId>,
        mappings: &mut Vec<(TermId, QueryTermId)>,
        next_query_id: &mut u64,
    ) -> Result<Option<QueryTermId>> {
        if let Some(query) = resolved.get(&term) {
            return Ok(Some(*query));
        }
        if let Some(value) = snapshot.get(&self.qv2_term_to_query, term.to_be_bytes())? {
            let Ok(raw) = <[u8; 8]>::try_from(value.as_ref()) else {
                return Ok(None);
            };
            let query = QueryTermId::from_be_bytes(raw);
            if query.0 >= *next_query_id {
                return Ok(None);
            }
            let Some(reverse) = snapshot.get(&self.qv2_query_to_term, query.to_be_bytes())? else {
                return Ok(None);
            };
            if reverse.as_ref() != term.to_be_bytes() {
                return Ok(None);
            }
            resolved.insert(term, query);
            return Ok(Some(query));
        }
        if !allow_allocate {
            return Ok(None);
        }
        let query = QueryTermId(*next_query_id);
        let Some(next) = next_query_id.checked_add(1) else {
            return Ok(None);
        };
        if snapshot
            .get(&self.qv2_query_to_term, query.to_be_bytes())?
            .is_some()
        {
            return Ok(None);
        }
        *next_query_id = next;
        resolved.insert(term, query);
        mappings.push((term, query));
        Ok(Some(query))
    }

    fn query_index_spo_exists(&self, snapshot: &Snapshot, quad: QueryQuad) -> Result<bool> {
        let key = qv2_spog_key(quad);
        let mut prefix = [0u8; 24];
        prefix.copy_from_slice(&key[..24]);
        match snapshot.prefix(&self.qv2_spog, prefix).next() {
            Some(guard) => {
                let _ = guard.into_inner()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn insertions_preserve_union_uniqueness(
        &self,
        snapshot: &Snapshot,
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
            if self.query_index_spo_exists(snapshot, *quad)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn single_transition_preserves_union_uniqueness(
        &self,
        snapshot: &Snapshot,
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
        Ok(!self.query_index_spo_exists(snapshot, quad)?)
    }

    fn plan_ready_query_index_maintenance(
        &self,
        snapshot: &Snapshot,
        header: &QueryIndexHeader,
        transitions: Vec<NetQuadTransition>,
    ) -> Result<Option<QueryIndexMaintenancePlan>> {
        let total =
            match self.query_index_counter_from_snapshot(snapshot, QueryIndexCounterKey::Total)? {
                QueryIndexCounterRead::Value(total) if total == header.indexed_quads => total,
                QueryIndexCounterRead::Missing
                | QueryIndexCounterRead::Malformed
                | QueryIndexCounterRead::Value(_) => return Ok(None),
            };

        let mut resolved = HashMap::new();
        let mut mappings = Vec::new();
        let mut next_query_id = header.next_query_id;
        let mut query_transitions = Vec::with_capacity(transitions.len());
        for transition in transitions {
            let allow_allocate = transition.is_live;
            let Some(graph) = self.resolve_maintenance_query_term(
                snapshot,
                transition.quad.graph,
                allow_allocate,
                &mut resolved,
                &mut mappings,
                &mut next_query_id,
            )?
            else {
                return Ok(None);
            };
            let Some(subject) = self.resolve_maintenance_query_term(
                snapshot,
                transition.quad.subject,
                allow_allocate,
                &mut resolved,
                &mut mappings,
                &mut next_query_id,
            )?
            else {
                return Ok(None);
            };
            let Some(predicate) = self.resolve_maintenance_query_term(
                snapshot,
                transition.quad.predicate,
                allow_allocate,
                &mut resolved,
                &mut mappings,
                &mut next_query_id,
            )?
            else {
                return Ok(None);
            };
            let Some(object) = self.resolve_maintenance_query_term(
                snapshot,
                transition.quad.object,
                allow_allocate,
                &mut resolved,
                &mut mappings,
                &mut next_query_id,
            )?
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
            let mut all_at_prior_state = true;
            for (keyspace, key) in [
                (&self.qv2_gspo, qv2_gspo_key(quad)),
                (&self.qv2_gpos, qv2_gpos_key(quad)),
                (&self.qv2_spog, qv2_spog_key(quad)),
                (&self.qv2_posg, qv2_posg_key(quad)),
                (&self.qv2_ospg, qv2_ospg_key(quad)),
                (&self.qv2_gosp, qv2_gosp_key(quad)),
            ] {
                let current = snapshot.get(keyspace, key)?;
                let present = match current {
                    None => false,
                    Some(value) if value.as_ref().is_empty() => true,
                    Some(_) => return Ok(None),
                };
                already_desired &= present == transition.is_live;
                all_at_prior_state &= present != transition.is_live;
            }
            if already_desired {
                continue;
            }
            if !all_at_prior_state {
                return Ok(None);
            }
            query_transitions.push((quad, transition.is_live));
        }

        if query_transitions.is_empty() {
            return Ok(Some(QueryIndexMaintenancePlan {
                transitions: query_transitions,
                mappings,
                counters: Vec::new(),
                header: None,
            }));
        }

        let union_duplicate_free = match self
            .query_index_counter_from_snapshot(snapshot, QueryIndexCounterKey::UnionDuplicateFree)?
        {
            QueryIndexCounterRead::Value(0) => false,
            QueryIndexCounterRead::Value(1) => true,
            QueryIndexCounterRead::Missing
            | QueryIndexCounterRead::Malformed
            | QueryIndexCounterRead::Value(_) => return Ok(None),
        };
        let union_uniqueness_preserved = !union_duplicate_free
            || if let [transition] = query_transitions.as_slice() {
                self.single_transition_preserves_union_uniqueness(snapshot, *transition, &mappings)?
            } else {
                let new_terms = mappings.iter().map(|(_, query)| *query).collect();
                self.insertions_preserve_union_uniqueness(snapshot, &query_transitions, &new_terms)?
            };

        let mut deltas = BTreeMap::<Vec<u8>, (QueryIndexCounterKey, i128)>::new();
        for (quad, is_live) in &query_transitions {
            let delta = if *is_live { 1 } else { -1 };
            for counter in query_index_live_counter_keys(*quad) {
                let entry = deltas.entry(counter.bytes()).or_insert((counter, 0));
                let Some(next) = entry.1.checked_add(delta) else {
                    return Ok(None);
                };
                entry.1 = next;
            }
        }

        let mut counters = Vec::with_capacity(deltas.len() + 1);
        for (counter, delta) in deltas.values() {
            let has_rows = !matches!(counter, QueryIndexCounterKey::Total)
                && self.query_index_counter_has_rows(snapshot, *counter)?;
            let current = match self.query_index_counter_from_snapshot(snapshot, *counter)? {
                QueryIndexCounterRead::Missing
                    if !matches!(counter, QueryIndexCounterKey::Total) =>
                {
                    if has_rows {
                        return Ok(None);
                    }
                    0
                }
                QueryIndexCounterRead::Value(value)
                    if matches!(counter, QueryIndexCounterKey::Total)
                        || (value != 0 && has_rows) =>
                {
                    value
                }
                QueryIndexCounterRead::Missing
                | QueryIndexCounterRead::Malformed
                | QueryIndexCounterRead::Value(_) => return Ok(None),
            };
            let Some(next) = Self::adjusted_query_index_counter(current, *delta) else {
                return Ok(None);
            };
            counters.push(QueryIndexCounterUpdate {
                key: *counter,
                value: if matches!(counter, QueryIndexCounterKey::Total) || next != 0 {
                    Some(next)
                } else {
                    None
                },
            });
        }
        if union_duplicate_free && !union_uniqueness_preserved {
            counters.push(QueryIndexCounterUpdate {
                key: QueryIndexCounterKey::UnionDuplicateFree,
                value: Some(0),
            });
        }

        let total_delta = deltas
            .get(QUERY_INDEX_TOTAL_KEY.as_slice())
            .map(|(_, delta)| *delta)
            .unwrap_or(0);
        let Some(source_live_quads) =
            Self::adjusted_query_index_counter(header.source_live_quads, total_delta)
        else {
            return Ok(None);
        };
        let Some(indexed_quads) =
            Self::adjusted_query_index_counter(header.indexed_quads, total_delta)
        else {
            return Ok(None);
        };
        let Some(source_epoch) = header.source_epoch.checked_add(1) else {
            return Ok(None);
        };

        let Some(updated_total) = counters
            .iter()
            .find(|update| matches!(update.key, QueryIndexCounterKey::Total))
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
        Ok(Some(QueryIndexMaintenancePlan {
            transitions: query_transitions,
            mappings,
            counters,
            header: Some(QueryIndexHeader {
                state: StoredQueryIndexState::Ready,
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

    fn stage_query_index_maintenance_plan(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        plan: QueryIndexMaintenancePlan,
    ) {
        for (term, query) in plan.mappings {
            batch.insert(
                &self.qv2_term_to_query,
                term.to_be_bytes(),
                query.to_be_bytes(),
            );
            batch.insert(
                &self.qv2_query_to_term,
                query.to_be_bytes(),
                term.to_be_bytes(),
            );
        }
        for (quad, is_live) in plan.transitions {
            let keys = [
                (&self.qv2_gspo, qv2_gspo_key(quad)),
                (&self.qv2_gpos, qv2_gpos_key(quad)),
                (&self.qv2_spog, qv2_spog_key(quad)),
                (&self.qv2_posg, qv2_posg_key(quad)),
                (&self.qv2_ospg, qv2_ospg_key(quad)),
                (&self.qv2_gosp, qv2_gosp_key(quad)),
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
                Some(value) => {
                    batch.insert(&self.qv2_meta, update.key.bytes(), value.to_be_bytes())
                }
                None => batch.remove(&self.qv2_meta, update.key.bytes()),
            }
        }
        if let Some(header) = plan.header {
            self.stage_query_index_header(batch, &header);
        }
    }

    fn stage_query_index_maintenance(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        publish: &PendingPublish,
    ) -> Result<()> {
        let snapshot = self.db.snapshot();
        match self.query_index_header_from_snapshot(&snapshot)? {
            QueryIndexHeaderRead::Absent => Ok(()),
            QueryIndexHeaderRead::Malformed => {
                self.stage_query_index_failed(batch, None, "metadata-malformed");
                Ok(())
            }
            QueryIndexHeaderRead::Valid(header) => match header.state {
                StoredQueryIndexState::Building | StoredQueryIndexState::Failed(_) => Ok(()),
                StoredQueryIndexState::Ready => {
                    if !header.ready_is_coherent()
                        || !header.is_not_ahead_of_snapshot(snapshot.seqno())
                    {
                        self.stage_query_index_failed(
                            batch,
                            Some(&header),
                            "ready-metadata-inconsistent",
                        );
                        return Ok(());
                    }
                    let transitions = coalesced_query_index_transitions(&publish.quad_mutations);
                    match self.plan_ready_query_index_maintenance(
                        &snapshot,
                        &header,
                        transitions,
                    )? {
                        Some(plan) => self.stage_query_index_maintenance_plan(batch, plan),
                        None => self.stage_query_index_failed(
                            batch,
                            Some(&header),
                            "maintenance-anomaly",
                        ),
                    }
                    Ok(())
                }
            },
        }
    }

    fn begin_query_index_commit(&self) -> bool {
        loop {
            let state = self.qv_commit_state.load(Ordering::Acquire);
            if state & QV_COMMIT_ACTIVE == 0 {
                if self
                    .qv_commit_state
                    .compare_exchange(state, QV_COMMIT_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
                continue;
            }
            if state & QV_COMMIT_DIRTY == 0
                && self
                    .qv_commit_state
                    .compare_exchange(
                        state,
                        state | QV_COMMIT_DIRTY,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
            {
                continue;
            }
            if self.qv_pending_commits.fetch_add(1, Ordering::AcqRel) == 0 {
                self.qv_catchup_failed.store(false, Ordering::Release);
            }
            self.qv_degraded.store(true, Ordering::Release);
            return false;
        }
    }

    fn finish_query_index_commit(&self) -> bool {
        loop {
            let state = self.qv_commit_state.load(Ordering::Acquire);
            if self
                .qv_commit_state
                .compare_exchange(state, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return state == QV_COMMIT_ACTIVE;
            }
        }
    }

    fn catch_up_query_index(&self, publish: &PendingPublish) -> Result<()> {
        loop {
            if self
                .qv_commit_state
                .compare_exchange(0, QV_COMMIT_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            std::thread::yield_now();
        }
        let _reset = QueryIndexCommitReset(&self.qv_commit_state);
        let mut batch = self.buffered_batch();
        self.stage_query_index_maintenance(&mut batch, publish)?;
        self.commit_fjall_batch(batch)
    }

    fn finish_pending_query_index_commit(&self, caught_up: bool) {
        if !caught_up {
            self.qv_catchup_failed.store(true, Ordering::Release);
        }
        if self.qv_pending_commits.fetch_sub(1, Ordering::AcqRel) == 1
            && !self.qv_catchup_failed.load(Ordering::Acquire)
        {
            self.qv_degraded.store(false, Ordering::Release);
        }
    }

    /// Commit without holding the global cache lock, then publish only the
    /// affected cache generations under a short write section.
    fn commit_with_index(&self, mut commit: DurableCommit, publish: &PendingPublish) -> Result<()> {
        let maintains_query_index = self.begin_query_index_commit();
        if maintains_query_index {
            if let Err(error) = self.stage_query_index_maintenance(&mut commit.batch, publish) {
                let _ = self.finish_query_index_commit();
                return Err(error);
            }
        }
        let committed = self.commit_durable(commit);
        let published = if committed.is_ok() {
            #[cfg(test)]
            self.stall_after_commit();
            self.indexes_write().publish(publish);
            true
        } else {
            false
        };
        if maintains_query_index {
            self.finish_query_index_commit();
        } else {
            let caught_up = if committed.is_ok() {
                match self.catch_up_query_index(publish) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "query-index catch-up failed after a durable source commit"
                        );
                        self.mark_query_index_rebuild_failed("concurrent-catch-up-failed")
                            .is_ok()
                    }
                }
            } else {
                true
            };
            self.finish_pending_query_index_commit(caught_up);
        }
        committed?;
        debug_assert!(published, "successful durable commit publishes cache state");
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
        let terms = self
            .term_decode_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .statistics();
        [quad, object, terms]
    }

    #[cfg(test)]
    pub(crate) fn query_index_admission_probe_count(&self) -> u64 {
        self.query_index_admission_probes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn index_verify_count(&self) -> u64 {
        self.query_index_verification_runs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn fail_test_indexes(&self) {
        let snapshot = self.db.snapshot();
        let previous = match self.query_index_header_from_snapshot(&snapshot).unwrap() {
            QueryIndexHeaderRead::Valid(header) => Some(header),
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        self.stage_query_index_failed(&mut batch, previous.as_ref(), "test-failure");
        self.commit_fjall_batch(batch).unwrap();
    }

    #[cfg(test)]
    pub(crate) fn set_test_query_index_state(&self, state: QueryIndexState) {
        let snapshot = self.db.snapshot();
        let QueryIndexHeaderRead::Valid(mut header) =
            self.query_index_header_from_snapshot(&snapshot).unwrap()
        else {
            panic!("query-index header must be present before degrading it");
        };
        let mut batch = self.buffered_batch();
        match state {
            QueryIndexState::Missing => batch.remove(&self.qv2_meta, QUERY_INDEX_HEADER_KEY),
            QueryIndexState::Building => {
                header.state = StoredQueryIndexState::Building;
                self.stage_query_index_header(&mut batch, &header);
            }
            QueryIndexState::Failed(reason) => {
                header.state = StoredQueryIndexState::Failed(reason);
                self.stage_query_index_header(&mut batch, &header);
            }
            QueryIndexState::Ready => panic!("test helper only degrades query indexes"),
        }
        self.commit_fjall_batch(batch).unwrap();
    }

    /// The vocabulary term ids orphan detection matches on.
    ///
    /// `None` means the term was never interned, so no stored quad can mention
    /// it and the branch that tests for it simply never fires.
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

    /// Term ids of the graph's orphaned data entities.
    ///
    /// Evaluates [`crate::rules::orphaned_data_entities`] entirely on term ids
    /// against the durable graph prefix: nothing is decoded, so the cost is a
    /// handful of integer comparisons per stored triple. The rule is the specification and the two are
    /// cross-checked on generated graph shapes by
    /// `orphan_ids_match`; recomputation is on the hot path of
    /// every write that defers its diagnostics refresh, where the decoding
    /// version cost 74ms on a 10,000-entity crate.
    ///
    /// The crate root is the graph term itself, so its term id *is* `graph_id`.
    fn orphaned_entity_ids(
        &self,
        graph_id: TermId,
        vocab: &OrphanVocab,
    ) -> Result<HashSet<TermId>> {
        let mut data_entities: HashSet<TermId> = HashSet::new();
        let mut adjacency: HashMap<TermId, Vec<TermId>> = HashMap::new();
        self.for_each_stored_quad(graph_id, |quad, _| {
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

    /// Snapshot-only twin of [`GraphStore::orphaned_entity_ids`]. Reads use it
    /// when the persisted diagnostic record is absent or tagged for another
    /// clock, so visibility cannot mix qv/source rows from one commit with
    /// orphan state from another. It intentionally does not persist or update
    /// the global diagnostic cache.
    fn snapshot_orphaned_entity_ids(
        &self,
        snapshot: &Snapshot,
        context: &crate::query_context::ReadContext<'_>,
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
            if dot_payload_is_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            context.record_key_fields_extracted(4);
            context.increment_encoded_quad_constructions();
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

    /// Open-time repair pass for the persisted diagnostics.
    ///
    /// A record whose clock tag still matches the graph's clock describes the
    /// current state and is simply loaded into the memory cache; anything else
    /// (missing record, or a tag left behind by a crash between the quad commit
    /// and the diagnostics write) is recomputed and re-persisted right here,
    /// not lazily.
    ///
    /// Doubles as the seeding pass for the clock mirror, which every later
    /// freshness check reads. Nothing else holds the store yet, so seeding a
    /// graph's clock before its diagnostics are looked at is enough ordering.
    fn repair_graph_diagnostics_at_open(&self) -> Result<()> {
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();

        for graph_id in self.graph_term_ids()? {
            let clock = self.durable_vector_clock(graph_id)?;
            self.indexes_write().clocks.insert(graph_id, clock.clone());
            let stored = self.read_stored_diagnostics(graph_id)?;
            if let Some(record) = stored.as_ref().filter(|record| record.at_clock == clock) {
                self.diagnostics_cache
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(graph_id, record.clone());
                continue;
            }

            let previous = stored.map(|record| record.diagnostics).unwrap_or_default();
            let repaired = self.compute_tagged_diagnostics(graph_id)?;
            // Re-queue first, record second, in that order and as separate
            // commits: a crash or a failed enqueue in between must leave the
            // older baseline so the next open re-queues again (G7).
            self.requeue_orphan_changes(graph_id, (&previous, &repaired.diagnostics))?;
            self.store_diagnostics_record(graph_id, repaired)?;
        }
        Ok(())
    }

    /// Re-queue for search every entity whose orphan status changed during a
    /// repair, since orphaned entities are invisible to search.
    ///
    /// Without it a crash between a quad commit and its diagnostics write
    /// strands an entity as searchable, or wrongly hidden, until it is dirtied.
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
            if dot_payload_is_empty(value.as_ref()) {
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

    fn planner_count(&self, count: Result<Option<u64>>, fallback: impl FnOnce() -> usize) -> usize {
        count
            .ok()
            .flatten()
            .and_then(|count| usize::try_from(count).ok())
            .unwrap_or_else(fallback)
    }

    fn source_pattern_count(
        &self,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> usize {
        self.quads_for_pattern(None, subject, predicate, object)
            .map(|quads| quads.len())
            .unwrap_or(usize::MAX)
    }

    fn query_index_pattern_count(
        &self,
        order: QueryIndexCursorOrder,
        pattern: crate::rdf_read::QuadPattern,
    ) -> Option<usize> {
        let snapshot = self.read_snapshot();
        if !snapshot.query_index_admission(self).ok()?.trusted {
            return None;
        }
        let mut cursor = snapshot.query_index_cursor(self, order, pattern).ok()?;
        let mut count = 0usize;
        while let Some(candidate) = cursor.next_candidate() {
            let candidate = candidate.ok()?;
            if candidate.live && pattern.matches(candidate.quad) {
                count = count.saturating_add(1);
            }
        }
        Some(count)
    }

    fn query_index_distinct_count(
        &self,
        predicate: Option<TermId>,
        domain: DistinctDomain,
    ) -> Option<usize> {
        let snapshot = self.db.snapshot();
        if !self.snapshot_admission(&snapshot).ok()?.trusted {
            return None;
        }
        let QueryIndexHeaderRead::Valid(header) =
            self.query_index_header_from_snapshot(&snapshot).ok()?
        else {
            return None;
        };
        let predicate = match predicate {
            Some(predicate) => match self
                .query_term_id_from_snapshot(&snapshot, predicate)
                .ok()?
            {
                Some(predicate) => Some(predicate),
                None => return Some(0),
            },
            None => None,
        };
        let cache_key = (header.source_epoch, predicate, domain);
        if let Some(count) = self.indexes_write().planner_distinct.get_cloned(&cache_key) {
            return Some(count);
        }

        let count = match (predicate, domain) {
            (predicate, DistinctDomain::Subject) => {
                let mut subject = None;
                let mut matched = false;
                let mut count = 0usize;
                for guard in snapshot.iter(&self.qv2_spog) {
                    let (key, _) = guard.into_inner().ok()?;
                    let quad = decode_qv2_spog_key(key.as_ref())?;
                    if subject != Some(quad.subject) {
                        count = count.saturating_add(usize::from(matched));
                        subject = Some(quad.subject);
                        matched = false;
                    }
                    matched |= predicate.is_none_or(|expected| quad.predicate == expected);
                }
                count.saturating_add(usize::from(matched))
            }
            (Some(predicate), DistinctDomain::Object) => {
                let mut object = None;
                let mut count = 0usize;
                for guard in snapshot.prefix(&self.qv2_posg, predicate.to_be_bytes()) {
                    let (key, _) = guard.into_inner().ok()?;
                    let quad = decode_qv2_posg_key(key.as_ref())?;
                    if object != Some(quad.object) {
                        object = Some(quad.object);
                        count = count.saturating_add(1);
                    }
                }
                count
            }
            (None, DistinctDomain::Object) => {
                let mut object = None;
                let mut count = 0usize;
                for guard in snapshot.iter(&self.qv2_ospg) {
                    let (key, _) = guard.into_inner().ok()?;
                    let quad = decode_qv2_ospg_key(key.as_ref())?;
                    if object != Some(quad.object) {
                        object = Some(quad.object);
                        count = count.saturating_add(1);
                    }
                }
                count
            }
        };
        self.indexes_write().planner_distinct.insert(
            cache_key,
            count,
            std::mem::size_of_val(&count),
        );
        Some(count)
    }

    /// Approximate corpus-wide counts used only for planning. qv counters are
    /// preferred; source scans remain the correctness-preserving fallback.
    pub(crate) fn stat_predicate_object_count(&self, predicate: TermId, object: TermId) -> usize {
        let snapshot = self.read_snapshot();
        self.planner_count(snapshot.qv_po_count(self, predicate, object), || {
            self.source_pattern_count(None, Some(predicate), Some(object))
        })
    }

    pub(crate) fn stat_predicate_count(&self, predicate: TermId) -> usize {
        let snapshot = self.read_snapshot();
        self.planner_count(snapshot.qv_p_count(self, predicate), || {
            self.source_pattern_count(None, Some(predicate), None)
        })
    }

    pub(crate) fn predicate_subject_count(&self, predicate: TermId) -> usize {
        self.query_index_distinct_count(Some(predicate), DistinctDomain::Subject)
            .unwrap_or_else(|| self.stat_predicate_count(predicate))
    }

    pub(crate) fn predicate_object_count(&self, predicate: TermId) -> usize {
        self.query_index_distinct_count(Some(predicate), DistinctDomain::Object)
            .unwrap_or_else(|| self.stat_predicate_count(predicate))
    }

    pub(crate) fn stat_object_count(&self, object: TermId) -> usize {
        let pattern = crate::rdf_read::QuadPattern {
            object: Some(object),
            ..crate::rdf_read::QuadPattern::default()
        };
        self.query_index_pattern_count(QueryIndexCursorOrder::Ospg, pattern)
            .unwrap_or_else(|| self.source_pattern_count(None, None, Some(object)))
    }

    pub(crate) fn stat_subject_count(&self, subject: TermId) -> usize {
        let pattern = crate::rdf_read::QuadPattern {
            subject: Some(subject),
            ..crate::rdf_read::QuadPattern::default()
        };
        self.query_index_pattern_count(QueryIndexCursorOrder::Spog, pattern)
            .unwrap_or_else(|| self.source_pattern_count(Some(subject), None, None))
    }

    pub(crate) fn distinct_subject_count(&self) -> usize {
        self.query_index_distinct_count(None, DistinctDomain::Subject)
            .unwrap_or_else(|| self.stat_total_quads())
    }

    pub(crate) fn distinct_object_count(&self) -> usize {
        self.query_index_distinct_count(None, DistinctDomain::Object)
            .unwrap_or_else(|| self.stat_total_quads())
    }

    pub(crate) fn stat_total_quads(&self) -> usize {
        let snapshot = self.read_snapshot();
        self.planner_count(snapshot.qv_total_count(self), || {
            self.source_pattern_count(None, None, None)
        })
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

    pub(crate) fn decode_query_index_key(
        order: QueryIndexCursorOrder,
        bytes: &[u8],
    ) -> Result<QueryQuad> {
        let quad = match order {
            QueryIndexCursorOrder::Gspo => decode_qv2_gspo_key(bytes),
            QueryIndexCursorOrder::Gpos => decode_qv2_gpos_key(bytes),
            QueryIndexCursorOrder::Spog => decode_qv2_spog_key(bytes),
            QueryIndexCursorOrder::Posg => decode_qv2_posg_key(bytes),
            QueryIndexCursorOrder::Ospg => decode_qv2_ospg_key(bytes),
            QueryIndexCursorOrder::Gosp => decode_qv2_gosp_key(bytes),
        };
        quad.ok_or_else(|| StoreError::InvalidQueryIndexEncoding {
            context: "qv2 query index key",
            message: format!("expected 32 bytes, found {}", bytes.len()),
        })
    }

    pub(crate) fn decode_query_source_term(
        snapshot: &Snapshot,
        query_to_term: &Keyspace,
        term: QueryTermId,
    ) -> Result<TermId> {
        let value = snapshot.get(query_to_term, term.to_be_bytes())?.ok_or(
            StoreError::QueryIndexVerificationFailed("query-to-term-mapping-missing"),
        )?;
        decode_query_source_term_value(value.as_ref(), "query-to-term mapping")
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

    pub(crate) fn quad_value_is_live(bytes: &[u8]) -> bool {
        !dot_payload_is_empty(bytes)
    }

    fn count_objects_for_ids(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
    ) -> Result<usize> {
        Ok(self
            .subject_entries((graph, subject), None)?
            .into_iter()
            .filter(|(candidate_predicate, _)| *candidate_predicate == predicate)
            .count())
    }

    /// Objects of `(graph, subject, predicate)` in decoded-term order.
    ///
    /// Decoding happens with no lock held, so a commit can invalidate the entry
    /// while it runs. The generation snapshot taken before the index is read is
    /// what stops the result being cached in that case: an ordering installed
    /// over a newer one would be served indefinitely on a quiescent graph and
    /// silently omit a new `hasPart` child from every export page (G6).
    fn ordered_objects_for_subject_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
    ) -> Result<Arc<Vec<TermId>>> {
        let key = (graph, subject, predicate);
        let generation = {
            let mut indexes = self.indexes_write();
            let generation = indexes.generations.get(&graph).copied().unwrap_or(0);
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
        if indexes.generations.get(&graph).copied().unwrap_or(0) == generation {
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
        Self::open_with_persist_mode(path, PersistMode::Buffer)
    }

    pub fn open_with_persist_mode(
        path: impl AsRef<Path>,
        persist_mode: PersistMode,
    ) -> Result<Self> {
        let worker_threads = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .min(32);
        // `max_write_buffer_size` is `#[deprecated = "todo"]` and `#[doc(hidden)]`
        // upstream (fjall 3.1.6) — a knob whose behaviour is not settled. We
        // stop setting it rather than pin
        // durability behaviour to an unstable option; per-keyspace
        // `max_memtable_size` below still bounds memtable growth, so the
        // durability contract (G10) is unchanged.
        //
        // `max_journaling_size` and `max_memtable_size` only bound replay
        // *together*: measured over a 40,000-graph corpus, capping the journal
        // alone left 218 MiB of journal and an 8.7 s reopen, because the
        // eviction that enforces the cap only runs after a flush and the 1 GiB
        // memtables never produced one. See [`MAX_JOURNALING_BYTES`].
        let db = Database::builder(path.as_ref())
            .manual_journal_persist(true)
            .cache_size(recommended_db_cache_bytes())
            .journal_compression(CompressionType::None)
            .max_journaling_size(MAX_JOURNALING_BYTES)
            .worker_threads(worker_threads)
            .open()?;
        Self::with_persist_mode(db, persist_mode)
    }

    pub fn from_database(db: Database) -> Result<Self> {
        Self::with_persist_mode(db, PersistMode::Buffer)
    }

    /// Build a store on an already-open database with an explicit durability
    /// mode; [`GraphStore::open_with_persist_mode`] opens the database first.
    pub fn with_persist_mode(db: Database, persist_mode: PersistMode) -> Result<Self> {
        let point_read_heavy = || {
            KeyspaceCreateOptions::default()
                .expect_point_read_hits(true)
                .max_memtable_size(POINT_READ_MEMTABLE_BYTES)
        };
        let write_heavy = || {
            KeyspaceCreateOptions::default()
                .data_block_compression_policy(CompressionPolicy::disabled())
                .index_block_compression_policy(CompressionPolicy::disabled())
                .compaction_strategy(Arc::new(
                    Leveled::default()
                        .with_l0_threshold(WRITE_HEAVY_L0_THRESHOLD)
                        .with_table_target_size(WRITE_HEAVY_TABLE_TARGET_BYTES)
                        .with_level_ratio_policy(vec![WRITE_HEAVY_LEVEL_RATIO]),
                ))
                .max_memtable_size(WRITE_HEAVY_MEMTABLE_BYTES)
        };

        let store = Self {
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
            qv2_term_to_query: db.keyspace("qv2_term_to_query", point_read_heavy)?,
            qv2_query_to_term: db.keyspace("qv2_query_to_term", point_read_heavy)?,
            qv2_meta: db.keyspace("qv2_meta", point_read_heavy)?,
            db,
            persist_mode,
            term_locks: (0..TERM_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            commit_locks: (0..COMMIT_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            qv_commit_state: AtomicU64::new(0),
            qv_degraded: AtomicBool::new(false),
            qv_pending_commits: AtomicU64::new(0),
            qv_catchup_failed: AtomicBool::new(false),
            #[cfg(feature = "shacl-core")]
            binding_lock: Mutex::new(()),
            #[cfg(feature = "shacl-core")]
            binding_lock_wait_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            binding_lock_hold_ns: AtomicU64::new(0),
            #[cfg(feature = "shacl-core")]
            graph_commit_lock_wait_ns: AtomicU64::new(0),
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
            status_full_shape_scans: AtomicU64::new(0),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_stall: Mutex::new(Duration::ZERO),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_active: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, feature = "shacl-core"))]
            validation_max_active: std::sync::atomic::AtomicUsize::new(0),
            indexes: RwLock::new(IndexState::default()),
            diagnostics_cache: RwLock::new(HashMap::new()),
            term_decode_cache: RwLock::new(BoundedCache::new(
                TERM_DECODE_CACHE_CAP,
                TERM_DECODE_CACHE_BYTES,
            )),
            #[cfg(test)]
            commit_stall: Mutex::new(None),
            #[cfg(test)]
            commit_stalled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            commit_stall_active: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            commit_stall_max_active: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            commit_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fts_ack_stall: Mutex::new(None),
            #[cfg(test)]
            rebuild_stall: Mutex::new(None),
            #[cfg(test)]
            rebuild_stalled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            delete_stall: Mutex::new(None),
            #[cfg(test)]
            delete_stalled: std::sync::atomic::AtomicBool::new(false),
            fts_queue_lock: Mutex::new(()),
            dirty_counter: AtomicU64::new(1),
            diagnostics_computed: AtomicU64::new(0),
            #[cfg(test)]
            query_index_admission_probes: AtomicU64::new(0),
            #[cfg(test)]
            query_index_verification_runs: AtomicU64::new(0),
            #[cfg(test)]
            persists: AtomicU64::new(0),
        };

        store.ensure_disk_format()?;
        store.initialize_query_indexes_at_open()?;
        store.restore_dirty_counter()?;
        store.repair_graph_diagnostics_at_open()?;
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
            None if self.authoritative_keyspaces_are_empty()? => {
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

    fn authoritative_keyspaces_are_empty(&self) -> Result<bool> {
        for keyspace in [&self.terms, &self.quads, &self.log] {
            if keyspace.iter().next().is_some() {
                return Ok(false);
            }
        }
        for guard in self.graphs.iter() {
            let (key, _) = guard.into_inner()?;
            if key.as_ref() != DISK_FORMAT_KEY {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Restore the FTS queue token counter across restarts.
    ///
    /// Acknowledgement is token-comparison based: a reindex/delete entry clears
    /// every subject entry whose token is `<=` its own. If the counter restarted
    /// at 1, a subject enqueued *after* a restart would get a token below a
    /// reindex token issued *before* it and be silently dropped without tantivy
    /// ever having processed it — a silent search-index data loss (G7). Seeding
    /// the counter past every live token makes tokens strictly increasing across
    /// restarts, which is exactly what the acknowledgement rule assumes.
    fn restore_dirty_counter(&self) -> Result<()> {
        let mut highest = 0u64;
        for prefix in [
            graph_dirty_prefix(),
            graph_reindex_prefix(),
            graph_search_delete_prefix(),
        ] {
            for guard in self.graphs.prefix(prefix) {
                let (_, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "fts queue tokens")?;
                highest = highest.max(tokens.latest);
            }
        }
        self.dirty_counter.store(highest + 1, Ordering::SeqCst);
        Ok(())
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn persist_mode(&self) -> PersistMode {
        self.persist_mode
    }

    /// Flush every keyspace, then compact it.
    ///
    /// The rotation is load-bearing: compaction has no input until a memtable
    /// is flushed, and a memtable only flushes on its own once it exceeds a
    /// ceiling that fjall persists per keyspace at creation. A store created
    /// before that ceiling was lowered keeps the old one, so this call was
    /// otherwise a no-op on exactly the stores needing it (C1/C2). Depends on
    /// fjall's `#[doc(hidden)]` `rotate_memtable_and_wait`.
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
            &self.qv2_term_to_query,
            &self.qv2_query_to_term,
            &self.qv2_meta,
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
            Some(bytes) => Ok(EncodedTerm(decode_term_utf8(bytes.as_ref())?)),
            None => Err(StoreError::TermNotFound(id.0)),
        }
    }

    /// Decode a term id through the global term cache.
    ///
    /// Term ids are content hashes of immutable term bytes, so a cached entry
    /// can never become stale and needs no invalidation path. Returns an `Arc`
    /// so hot paths share one allocation instead of cloning the string.
    pub(crate) fn decode_term_arc(&self, id: TermId) -> Result<Arc<EncodedTerm>> {
        if let Some(term) = self
            .term_decode_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .get_cloned(&id)
        {
            return Ok(term);
        }

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
            existing: decode_term_utf8(existing.as_ref())?,
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
        if self.read_graph_meta_by_id(graph_id)?.is_none() {
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
        if self.read_graph_meta_by_id(graph_id)?.is_none() {
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
        self.contains_graph_by_id(graph_id)
    }

    /// O(1) existence probe that never decodes the metadata record.
    pub(crate) fn contains_graph_by_id(&self, graph_id: TermId) -> Result<bool> {
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
    pub(crate) fn pending_shacl_queue_repair_required(&self) -> Result<bool> {
        Ok(self
            .graphs
            .get(SHACL_PENDING_QUEUE_SCHEMA_KEY)?
            .is_none_or(|value| value.as_ref() != [SHACL_PENDING_QUEUE_SCHEMA_VERSION]))
    }

    /// Rebuild the durable pending queue from all binding records.
    #[cfg(feature = "shacl-core")]
    pub(crate) fn repair_pending_shacl_queue(&self) -> Result<PendingQueueRepairStatistics> {
        let mut batch = self.new_batch();
        let mut pending_queue_entries_scanned = 0u64;
        for guard in self.graphs.prefix(shacl_pending_prefix()) {
            let (key, _) = guard.into_inner()?;
            pending_queue_entries_scanned += 1;
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
        batch.insert(
            &self.graphs,
            SHACL_PENDING_QUEUE_SCHEMA_KEY,
            [SHACL_PENDING_QUEUE_SCHEMA_VERSION],
        );
        self.commit(batch)?;
        Ok(PendingQueueRepairStatistics {
            binding_records_scanned,
            pending_queue_entries_scanned,
        })
    }

    /// Scan only the durable pending queue, optionally stopping at a replay budget.
    #[cfg(feature = "shacl-core")]
    pub(crate) fn pending_shacl_queue_bounded(
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
        Ok(self.pending_shacl_queue_bounded(usize::MAX, None)?.graphs)
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
    pub(crate) fn shacl_graph_is_pending(&self, graph: &GraphId) -> Result<bool> {
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
        self.delete_graph_inner(graph, None)
    }

    /// Delete a graph and persist its tombstone in the same durable batch.
    pub(crate) fn delete_graph_tombstoned(&self, tombstone: &GraphTombstone) -> Result<()> {
        self.delete_graph_inner(&tombstone.graph, Some(tombstone))
    }

    fn delete_graph_inner(
        &self,
        graph: &GraphId,
        tombstone: Option<&GraphTombstone>,
    ) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.binding_guard();
        let mut batch = self.new_batch();
        let graph_id = match self.graph_id_for(graph)? {
            Some(graph_id) => graph_id,
            None if tombstone.is_some() => self
                .encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?,
            None => return Ok(()),
        };
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
        }
        self.for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
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
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
        }

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

        let reindex_key = graph_reindex_key(graph_id);
        if self.graphs.get(reindex_key)?.is_some() {
            batch.remove(&self.graphs, reindex_key);
        }

        batch.pending_fts.push(FtsQueueKey::Delete(graph_id));

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
        self.sweep_graph_queue(graph_id)?;
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&graph_id);
        let mut indexes = self.indexes_write();
        indexes
            .quad_subjects
            .remove_where(|(graph, _, _)| *graph == graph_id);
        indexes.object_order.drop_graph(graph_id);
        let generation = indexes.generations.entry(graph_id).or_default();
        *generation = generation.wrapping_add(1);
        Ok(())
    }

    /// Drop the subject and reindex queue entries of a graph that is gone.
    ///
    /// The delete scans those keys without the queue lock, so an enqueue
    /// landing before its commit would otherwise outlive the graph.
    fn sweep_graph_queue(&self, graph_id: TermId) -> Result<()> {
        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        let reindex_key = graph_reindex_key(graph_id);
        if self.graphs.get(reindex_key)?.is_some() {
            batch.remove(&self.graphs, reindex_key);
            dirty = true;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
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
            if dot_payload_is_empty(value.as_ref()) {
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
            if !dot_payload_is_empty(value.as_ref()) {
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
        self.graph_term_id_iter().collect()
    }

    /// Lazily streams the graph term ids of [`GraphStore::graph_term_ids`],
    /// so short-circuiting consumers (ASK, LIMIT) stop without scanning the
    /// full graph list.
    pub fn graph_term_id_iter(&self) -> impl Iterator<Item = Result<TermId>> {
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

    // ── Persisted, clock-tagged diagnostics ────────────

    fn read_stored_diagnostics(&self, graph_id: TermId) -> Result<Option<StoredDiagnostics>> {
        self.graphs
            .get(graph_diagnostics_key(graph_id))?
            .map(|bytes| postcard::from_bytes(bytes.as_ref()))
            .transpose()
            .map_err(Into::into)
    }

    /// The orphan set as last *persisted*, without verifying its clock tag and
    /// without recomputing.
    ///
    /// This is the set the search index currently reflects, which is what a
    /// re-queue must diff against. Every other reader wants
    /// [`GraphStore::graph_diagnostics`], which refuses to serve a stale record;
    /// this one is deliberately allowed to return one, so callers must not use
    /// it to decide visibility.
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
        let diagnostics = record.diagnostics.clone();
        // Guards the in-memory mirror of the persisted 'O' records.
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(graph_id, record);
        Ok(diagnostics)
    }

    /// Recompute a graph's diagnostics from the store, tagged with the clock.
    ///
    /// The clock is read *before* computing so a concurrent commit can only
    /// make the tag look older than it is. That direction is safe: it triggers
    /// one more recomputation, never a stale record wrongly accepted as fresh
    /// (G6).
    fn compute_tagged_diagnostics(&self, graph_id: TermId) -> Result<StoredDiagnostics> {
        let at_clock = self.get_vector_clock_by_id(graph_id)?;
        let graph = self.graph_name_by_id(graph_id)?;
        Ok(StoredDiagnostics {
            diagnostics: self.compute_graph_diagnostics(&graph)?,
            at_clock,
        })
    }

    fn graph_name_by_id(&self, graph_id: TermId) -> Result<GraphId> {
        let term = self.decode_term_arc(graph_id)?;
        term.to_named_node()
            .map(GraphId)
            .ok_or_else(|| StoreError::InvalidEncoding {
                context: "graph term",
                message: term.0.clone(),
            })
    }

    /// Persist `diagnostics` for `graph`, tagged with the graph's current
    /// vector clock, and refresh the memory cache.
    ///
    /// **Call while holding the graph commit guard** so no commit can slip
    /// between the state the caller measured and the clock recorded here;
    /// otherwise the record can be tagged with a clock newer than the state it
    /// describes and readers would accept it as fresh.
    pub fn set_graph_diagnostics(
        &self,
        graph: &GraphId,
        diagnostics: &GraphDiagnostics,
    ) -> Result<()> {
        let graph_id = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let record = StoredDiagnostics {
            diagnostics: diagnostics.clone(),
            at_clock: self.get_vector_clock_by_id(graph_id)?,
        };
        self.store_diagnostics_record(graph_id, record)?;
        Ok(())
    }

    pub fn graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphDiagnostics::default());
        };
        self.graph_diagnostics_by_id(graph_id)
    }

    /// Like [`GraphStore::graph_diagnostics`] but keyed by term id. Verifies the
    /// clock tag on every read and recomputes inline on a mismatch, so a stale
    /// set is never served.
    ///
    /// **A read never persists.** The stored record is the baseline the search
    /// re-queue diffs against, so a reader that saved its recomputation would
    /// erase the difference a later rebuild must act on (G7).
    pub fn graph_diagnostics_by_id(&self, graph_id: TermId) -> Result<GraphDiagnostics> {
        let clock = self.get_vector_clock_by_id(graph_id)?;

        if let Some(record) = self
            .diagnostics_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&graph_id)
            && record.at_clock == clock
        {
            return Ok(record.diagnostics.clone());
        }

        if let Some(record) = self.read_stored_diagnostics(graph_id)?
            && record.at_clock == clock
        {
            let diagnostics = record.diagnostics.clone();
            self.diagnostics_cache
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(graph_id, record);
            return Ok(diagnostics);
        }

        if !self.contains_graph_by_id(graph_id)? {
            return Ok(GraphDiagnostics::default());
        }

        let record = self.compute_tagged_diagnostics(graph_id)?;
        let diagnostics = record.diagnostics.clone();
        // Guards the in-memory mirror of the persisted 'O' records.
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(graph_id, record);
        Ok(diagnostics)
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn set_tagged_graph_policy(
        &self,
        graph: &GraphId,
        tagged: &TaggedGraphPolicy,
    ) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        #[cfg(feature = "shacl-core")]
        let _binding_guard = self.binding_guard();
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
        meta.policy = tagged.policy.clone().normalized();
        meta.policy_tag = tagged.tag;
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        self.commit(batch)
    }

    #[cfg(test)]
    pub fn set_graph_policy(&self, graph: &GraphId, policy: &GraphPolicy) -> Result<()> {
        let current = self.graph_tagged_policy(graph)?;
        self.set_tagged_graph_policy(
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

    pub fn graph_tagged_policy(&self, graph: &GraphId) -> Result<TaggedGraphPolicy> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(TaggedGraphPolicy {
                policy: GraphPolicy::default(),
                tag: PolicyTag::default(),
            });
        };
        let meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
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
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .irokle_topic)
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn set_irokle_topic_id(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        self.set_topic_guarded(graph, topic_id)
    }

    /// Caller holds this graph's [`GraphCommitGuard`].
    pub(crate) fn set_topic_guarded(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
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
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .rocrate_context)
    }

    /// Read the raw root `license` JSON and the graph digest it describes.
    pub fn graph_license(&self, graph: &GraphId) -> Result<Option<(String, [u8; 32])>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        let meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
        Ok(meta.rocrate_license.zip(meta.rocrate_license_digest))
    }

    /// Read the last-write-wins ordering tag for a graph's stored `@context`.
    /// Returns [`ContextTag::GENESIS`] when the graph has no explicit context.
    pub fn graph_context_tag(&self, graph: &GraphId) -> Result<ContextTag> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(ContextTag::GENESIS);
        };
        Ok(self
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .context_tag)
    }

    /// Persist the raw RO-Crate render hints and their ordering tag.
    ///
    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
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
            &TaggedRoCrateRenderHints {
                hints: RoCrateRenderHints {
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
        tagged: &TaggedRoCrateRenderHints,
    ) -> Result<bool> {
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
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

    pub fn set_applied_topic_clock(&self, topic_id: &[u8; 32], clock: &[u8]) -> Result<()> {
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
        repaired_at_unix_nanos: i64,
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
            repaired_at_unix_nanos,
        };
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, key, replacement);
        batch.insert(
            &self.graphs,
            cursor_repair_audit_key(&audit),
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
            if dot_payload_is_empty(value.as_ref()) {
                continue;
            }
            let quad = Self::decode_quad_key(key.as_ref())?;
            quads.push(SnapshotQuadState {
                subject: self.decode_term_arc(quad.subject)?.as_ref().clone(),
                predicate: self.decode_term_arc(quad.predicate)?.as_ref().clone(),
                object: self.decode_term_arc(quad.object)?.as_ref().clone(),
                dots: decode_dots(value.as_ref())?,
            });
        }

        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            clock: vector_clock,
            quads,
        })
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
            if dot_payload_is_empty(value.as_ref()) {
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

    pub fn subject_triple_count_by_ids(&self, graph: TermId, subject: TermId) -> Result<usize> {
        Ok(self.subject_entries((graph, subject), None)?.len())
    }

    /// OR-Set add: stage `add.dot` into the quad's dot set (G1).
    ///
    /// Does not lock — the caller must hold the graph commit guard, otherwise
    /// two concurrent adds can read the same dot set and one add is lost.
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

    /// OR-Set remove: drop exactly the dots contained in the witnessed clock,
    /// never a dot the remover did not witness (G1).
    ///
    /// Does not lock — the caller must hold the graph commit guard.
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
            .is_some_and(|value| !dot_payload_is_empty(value.as_ref())))
    }

    /// The token the next FTS queue entry will receive, pinned under the queue
    /// lock so every entry below it is already durable.
    ///
    /// That is what makes it usable as a flush bound: read it outside the lock
    /// and a commit could be mid-flight, holding a lower token that the drain
    /// would not yet see.
    pub(crate) fn current_dirty_token(&self) -> u64 {
        let _queue = self.fts_queue_guard();
        self.dirty_counter.load(Ordering::SeqCst)
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

    /// Returns an in-memory range only when it still describes `snapshot_seqno`.
    /// A newer commit falls back to the caller-owned durable snapshot instead
    /// of mixing two execution states.
    fn current_derived_raw_cursor(
        &self,
        _snapshot_seqno: u64,
        _pattern: crate::rdf_read::QuadPattern,
    ) -> Option<crate::query_cursor::RawQuadCursor> {
        None
    }

    pub fn for_each_quad_in_graph<E, F>(
        &self,
        graph: TermId,
        mut visit: F,
    ) -> std::result::Result<(), E>
    where
        E: From<StoreError>,
        F: FnMut(EncodedQuad) -> std::result::Result<(), E>,
    {
        for quad in self.graph_scan(graph, None, None)? {
            visit(quad)?;
        }
        Ok(())
    }

    /// Stream a graph's quads straight off the durable `quads` keyspace,
    /// handing each one to `visit` together with its raw dot-set bytes.
    ///
    /// One sequential prefix scan replaces "in-memory scan + one point read per
    /// quad to fetch its dots". Reads committed state only.
    pub(crate) fn for_each_stored_quad<F>(&self, graph: TermId, mut visit: F) -> Result<()>
    where
        F: FnMut(EncodedQuad, &[u8]) -> Result<()>,
    {
        for guard in self.quads.prefix(graph.to_be_bytes()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 64 {
                continue;
            }
            let dots = value.as_ref();
            if dot_payload_is_empty(dots) {
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
        self.get_vector_clock_by_id(graph_id)
    }

    /// A graph's vector clock as published by its last commit.
    ///
    /// Reads the in-memory mirror, never the `'K'` key: a fjall batch becomes
    /// visible key by key, so the durable clock still reads pre-commit while
    /// the same batch's quads are already visible. A freshness check trusting
    /// that clock accepts the pre-write orphan set as current (G6).
    pub(crate) fn get_vector_clock_by_id(&self, graph_id: TermId) -> Result<VectorClock> {
        Ok(self
            .indexes_read()
            .clocks
            .get(&graph_id)
            .cloned()
            .unwrap_or_default())
    }

    /// Read a graph's vector clock from its own `'K'` key. Open-time only —
    /// everything else reads the mirror seeded from this.
    ///
    /// Falls back to the clock embedded in the legacy metadata record when no
    /// `'K'` key exists yet, which is the one-time migration path for stores
    /// written before the split; the first [`GraphStore::set_vector_clock`]
    /// writes `'K'` and the legacy copy is ignored from then on.
    fn durable_vector_clock(&self, graph_id: TermId) -> Result<VectorClock> {
        if let Some(bytes) = self.graphs.get(graph_clock_key(graph_id))? {
            return Ok(postcard::from_bytes(bytes.as_ref())?);
        }
        Ok(self
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .clock)
    }

    /// Write **only** the graph's clock key; the metadata record (policy,
    /// context, topic binding) is never rewritten, so a commit cannot clobber a
    /// concurrent policy or context write.
    ///
    /// The mirror update is staged, not applied: the batch can still fail, and
    /// only [`GraphStore::commit_with_index`] knows when the clock is really
    /// visible.
    ///
    /// Does not lock — the caller must hold the graph commit guard, which is
    /// what makes the read-clock → advance → write-clock cycle atomic (G2).
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

    /// Next per-(graph, actor) event counter, staged into `batch`.
    ///
    /// The caller MUST hold the graph commit guard: the read-then-write of the
    /// log head is what guarantees two concurrent local writes never mint the
    /// same dot (G1).
    pub fn next_counter(&self, batch: &mut WriteBatch, key: CounterKey) -> Result<u64> {
        let head = log_head_key(key.graph_id, &key.actor);
        let counter = match self.log.get(head)? {
            Some(value) => decode_u64_bytes(value.as_ref(), "log head")? + 1,
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

    /// Queue one subject for search reindexing, in the same durable batch as the
    /// store mutation that dirtied it (G7).
    ///
    /// The token is minted when the batch commits, not here: a token handed out
    /// now but made durable later could sit below a bound a flush pinned in
    /// between, and the flush would return without the entry ever being visible
    /// to its drain.
    pub fn enqueue_fts(&self, batch: &mut WriteBatch, key: FtsSubject) -> Result<()> {
        batch.pending_fts.push(FtsQueueKey::Subject {
            graph: key.graph_id,
            subject: key.subject,
        });
        Ok(())
    }

    /// Queue a set of subjects, collapsing to a whole-graph reindex only when
    /// the rescan is genuinely the cheaper of the two.
    ///
    /// See [`GraphStore::fts_reindex_is_cheaper`] for the rule and for why
    /// picking the per-subject branch cannot lose search freshness (G7).
    pub fn enqueue_fts_subjects(&self, batch: &mut WriteBatch, req: FtsEnqueue<'_>) -> Result<()> {
        if req.subjects.is_empty() {
            return Ok(());
        }
        if self.fts_reindex_is_cheaper(req.graph_id, req.subjects.len())? {
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

    /// Whether rescanning the whole graph beats queueing `subjects` dirty
    /// entries: the batch must be large *and* cover much of the graph.
    ///
    /// Safe to take the per-subject branch (G7) because callers pass exactly the
    /// subjects their write changed, and orphan-status flips on untouched
    /// subjects are queued separately by the diagnostics settle.
    fn fts_reindex_is_cheaper(&self, graph_id: TermId, subjects: usize) -> Result<bool> {
        Ok(subjects >= FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD
            && subjects * 2 >= self.graph_subject_count(graph_id)?)
    }

    pub fn enqueue_fts_reindex(&self, batch: &mut WriteBatch, graph_id: TermId) -> Result<()> {
        batch.pending_fts.push(FtsQueueKey::Reindex(graph_id));
        Ok(())
    }

    pub fn drain_fts_queue(&self, limit: usize) -> Result<Vec<DirtySubject>> {
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

    pub fn drain_fts_reindex_queue(&self, limit: usize) -> Result<Vec<DirtyGraph>> {
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

    pub fn drain_fts_delete_queue(&self, limit: usize) -> Result<Vec<DirtyGraph>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_search_delete_prefix()) {
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

    /// Drop the subject entries the indexer just covered, keeping any that were
    /// re-dirtied since it read them.
    ///
    /// The queue lock spans the token read and the commit: an enqueue landing
    /// in between would otherwise be erased by a removal that never covered it.
    pub fn acknowledge_fts_queue(&self, queued: &[DirtySubject]) -> Result<()> {
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
        self.stall_in_fts_ack();
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub fn acknowledge_fts_reindex_queue(&self, queued: &[DirtyGraph]) -> Result<()> {
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

    pub fn acknowledge_fts_delete_queue(&self, queued: &[DirtyGraph]) -> Result<()> {
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
                    key: graph_search_delete_key(graph_id).to_vec(),
                    covered: entry.tokens.latest,
                },
            )?;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub fn acknowledge_fts_subjects_for_reindexed_graphs(
        &self,
        queued: &[DirtyGraph],
    ) -> Result<()> {
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
            for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "graph dirty tokens")?;
                // `latest`: a subject dirtied past the reindex token is not
                // covered by the scan that token bounded.
                if tokens.latest <= entry.tokens.latest {
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

    pub fn acknowledge_fts_queues_for_deleted_graphs(&self, queued: &[DirtyGraph]) -> Result<()> {
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
            for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let tokens = decode_dirty_tokens(value.as_ref(), "graph dirty tokens")?;
                if tokens.latest <= delete_token {
                    batch.remove(&self.graphs, key);
                    dirty = true;
                }
            }

            let reindex_key = graph_reindex_key(graph_id);
            if self.graphs.get(reindex_key)?.is_some_and(|current| {
                decode_dirty_tokens(current.as_ref(), "graph reindex tokens")
                    .is_ok_and(|tokens| tokens.latest <= delete_token)
            }) {
                batch.remove(&self.graphs, reindex_key);
                dirty = true;
            }
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    /// Retire this graph's queue entries up to `upto`, the token a scan pinned
    /// before it started reading.
    ///
    /// Entries dirtied past that token survive: the scan never saw those
    /// writes, and clearing them would leave the subjects unindexed with
    /// nothing left to re-queue them (G7).
    pub fn clear_fts_queue_for_graph(&self, graph: &GraphId, upto: u64) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let _queue = self.fts_queue_guard();
        let mut batch = self.buffered_batch();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            keys.push(key.to_vec());
        }
        keys.push(graph_reindex_key(graph_id).to_vec());
        keys.push(graph_search_delete_key(graph_id).to_vec());

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
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        for guard in self.graphs.prefix(graph_reindex_prefix()) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        for guard in self.graphs.prefix(graph_search_delete_prefix()) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
            dirty = true;
        }
        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
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

    /// Retire the part of a queue entry an indexing pass just covered.
    ///
    /// Removes the entry when nothing was queued since the drain read it.
    /// Otherwise the entry stays — the newer write still owes an index — but
    /// its `oldest` moves past the covered tokens. Leaving it whole instead
    /// would keep it matching the bound a flush pinned, and a writer that
    /// keeps dirtying the same subject would hold that flush open forever.
    ///
    /// Every token at or below `covered` was durable before the drain read
    /// the entry, so the pass that followed indexed it.
    ///
    /// The caller MUST hold the FTS queue lock.
    fn settle_fts_entry(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        entry: AckedEntry,
    ) -> Result<bool> {
        let Some(current) = self.graphs.get(&entry.key)? else {
            return Ok(false);
        };
        let stored = decode_dirty_tokens(current.as_ref(), "fts queue tokens")?;
        if stored.latest <= entry.covered {
            batch.remove(&self.graphs, entry.key);
        } else {
            let narrowed = DirtyTokens {
                oldest: entry.covered + 1,
                latest: stored.latest,
            };
            batch.insert(&self.graphs, entry.key, encode_dirty_tokens(narrowed));
        }
        Ok(true)
    }

    /// Take the FTS queue lock, recovering from poison: the state it guards
    /// lives in fjall, not behind the mutex.
    fn fts_queue_guard(&self) -> MutexGuard<'_, ()> {
        self.fts_queue_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Stamp `key` with a fresh dirty token and stage its queue entry,
    /// coalescing into whatever the queue already holds for that key.
    ///
    /// Keeps the older `oldest`. Coalescing a second enqueue by overwriting
    /// would lift the entry above a bound a flush pinned between the two, and
    /// the drain would filter out work the flush promised to index. Widening
    /// the other way only ever over-includes, which costs a reindex.
    ///
    /// The caller MUST hold the FTS queue lock: it makes this read-modify-write
    /// atomic against acknowledgement, and minting under it is what makes
    /// "every entry below the token a flush pinned is already durable" true.
    fn stage_fts_entry(&self, batch: &mut fjall::OwnedWriteBatch, key: FtsQueueKey) -> Result<()> {
        let token = self.dirty_counter.fetch_add(1, Ordering::SeqCst);
        let bytes = key.bytes();
        let tokens = match self.graphs.get(&bytes)? {
            Some(current) => {
                let stored = decode_dirty_tokens(current.as_ref(), "fts queue tokens")?;
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
        Ok(())
    }

    /// Publish a batch together with the queue entries it owes.
    fn commit_durable(&self, commit: DurableCommit) -> Result<()> {
        let DurableCommit {
            mut batch,
            pending_fts,
        } = commit;
        let _queue = self.fts_queue_guard();
        for key in pending_fts.order {
            self.stage_fts_entry(&mut batch, key)?;
        }
        self.commit_fjall_batch(batch)
    }

    pub fn commit(&self, batch: WriteBatch) -> Result<()> {
        let WriteBatch {
            inner,
            pending_quad_states: _,
            pending_terms: _,
            publish,
            pending_fts,
        } = batch;
        let commit = DurableCommit {
            batch: inner,
            pending_fts,
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
        let generation = self
            .indexes_read()
            .generations
            .get(&graph)
            .copied()
            .unwrap_or(0);
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
                    if dot_payload_is_empty(value.as_ref()) {
                        continue;
                    }
                    let quad = Self::decode_quad_key(quad_key.as_ref())?;
                    entries.push((quad.predicate, quad.object));
                }
                let entries = Arc::new(entries);
                let mut indexes = self.indexes_write();
                if indexes.generations.get(&graph).copied().unwrap_or(0) == generation {
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

    pub fn triples_for_subject_excluding_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        excluded_predicate: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.decode_entries(self.subject_entries((graph, subject), Some(excluded_predicate))?)
    }

    pub fn count_objects_for_subject_predicate(
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
        self.count_objects_for_ids(graph_id, subject_id, predicate_id)
    }

    /// One page of the objects of `(graph, subject, predicate)`, in the stable
    /// order defined by the decoded object terms.
    ///
    /// Returns `(total, page)`; `total` is the full object count for both
    /// cursor kinds, so an `After` caller can still report progress.
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

        let object_ids =
            self.ordered_objects_for_subject_predicate(graph_id, subject_id, predicate_id)?;
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
    fn corrupt_index_for_test(&self, quad: EncodedQuad) {
        let mut entries = self
            .subject_entries((quad.graph, quad.subject), None)
            .unwrap();
        entries.retain(|entry| *entry != (quad.predicate, quad.object));
        let mut indexes = self.indexes_write();
        let generation = indexes.generations.get(&quad.graph).copied().unwrap_or(0);
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
mod tests {
    use super::*;
    use crate::query_context::{QueryReadMode, ReadContext};
    use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
    use crate::search_queue::{QueueBound, drain_upto};

    fn setup_store() -> (tempfile::TempDir, GraphStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        (dir, store)
    }

    fn seed_raw_graph_record(path: &Path, key: &[u8], value: &[u8]) {
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
    fn disk_format_marker_is_written_and_reopened() {
        let dir = tempfile::tempdir().unwrap();
        GraphStore::open(dir.path()).unwrap().persist().unwrap();
        GraphStore::open(dir.path()).unwrap();
    }

    #[test]
    fn future_disk_format_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        seed_raw_graph_record(
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
    fn malformed_disk_format_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        seed_raw_graph_record(dir.path(), DISK_FORMAT_KEY, &[1, 2, 3]);

        assert!(matches!(
            GraphStore::open(dir.path()),
            Err(StoreError::InvalidAuthoritativeFormat)
        ));
    }

    #[test]
    fn unmarked_nonempty_authoritative_store_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        seed_raw_graph_record(dir.path(), b"Mlegacy", b"value");

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
    fn pending_queue_scan_is_independent_of_binding_count() {
        let (_dir, store) = setup_store();
        let mut previous = 0;
        for count in [0, 100, 1_000, 10_000] {
            stage_binding_records(&store, previous, count);
            let started = Instant::now();
            let scan = store.pending_shacl_queue_bounded(usize::MAX, None).unwrap();
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
        let scan = store.pending_shacl_queue_bounded(usize::MAX, None).unwrap();
        assert_eq!(scan.entries_scanned, 1);
        assert_eq!(scan.graphs, vec![data]);
    }

    #[cfg(feature = "shacl-core")]
    #[test]
    fn explicit_queue_repair_restores_missing_and_malformed_entries() {
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
        assert!(store.pending_shacl_queue_repair_required().unwrap());
        let repair = store.repair_pending_shacl_queue().unwrap();
        assert_eq!(repair.binding_records_scanned, 100);
        assert_eq!(repair.pending_queue_entries_scanned, 1);
        assert_eq!(store.pending_shacl_queue().unwrap(), vec![data.clone()]);
        assert!(!store.pending_shacl_queue_repair_required().unwrap());

        let mut batch = store.new_batch();
        let data_id = store.graph_id_for(&data).unwrap().unwrap();
        batch.remove(&store.graphs, shacl_pending_key(data_id));
        store.commit(batch).unwrap();
        assert!(store.pending_shacl_queue().unwrap().is_empty());
        let repair = store.repair_pending_shacl_queue().unwrap();
        assert_eq!(repair.binding_records_scanned, 100);
        assert_eq!(store.pending_shacl_queue().unwrap(), vec![data]);
    }

    #[test]
    fn open_with_persist_mode_tracks_configured_mode() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:persist-mode:sync-all");

        {
            let store =
                GraphStore::open_with_persist_mode(dir.path(), PersistMode::SyncAll).unwrap();
            assert_eq!(PersistMode::SyncAll, store.persist_mode());
            store.create_graph(&graph).unwrap();
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_eq!(PersistMode::Buffer, reopened.persist_mode());
        assert!(reopened.contains_graph(&graph).unwrap());
    }

    #[test]
    fn graph_context_defaults_survive_reopen() {
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
        let mut clock = store.get_vector_clock_by_id(quad.graph).unwrap();
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

    fn query_index_header_for_test(store: &GraphStore) -> QueryIndexHeader {
        let snapshot = store.db.snapshot();
        match store.query_index_header_from_snapshot(&snapshot).unwrap() {
            QueryIndexHeaderRead::Valid(header) => header,
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => {
                panic!("query-index header must be present and valid")
            }
        }
    }

    fn query_index_counter_for_test(store: &GraphStore, key: QueryIndexCounterKey) -> Option<u64> {
        let snapshot = store.db.snapshot();
        snapshot
            .get(&store.qv2_meta, key.bytes())
            .unwrap()
            .map(|value| decode_query_index_u64(value.as_ref()).unwrap())
    }

    fn query_term_id_for_test(store: &GraphStore, term: TermId) -> QueryTermId {
        let snapshot = store.db.snapshot();
        store
            .query_term_id_from_snapshot(&snapshot, term)
            .unwrap()
            .expect("live query-index term must have a dense id")
    }

    fn query_quad_for_test(store: &GraphStore, quad: EncodedQuad) -> QueryQuad {
        let snapshot = store.db.snapshot();
        store
            .query_quad_from_snapshot(&snapshot, quad)
            .unwrap()
            .expect("live query-index quad must have dense ids")
    }

    fn assert_query_index_ready(store: &GraphStore, source_rows: u64) {
        let status = store.query_index_status().unwrap();
        assert_eq!(status.state, QueryIndexState::Ready);
        assert_eq!(status.source_live_quads, source_rows);
        assert_eq!(status.indexed_quads, source_rows);
        assert!(store.verify_query_indexes(true).unwrap().valid);
    }

    fn assert_query_index_problem(report: &QueryIndexVerification, problem: &str) {
        assert!(
            report.problems.iter().any(|current| current == problem),
            "expected query-index problem {problem}, got {:?}",
            report.problems
        );
    }

    fn stage_query_index_header_for_test(store: &GraphStore, header: &QueryIndexHeader) {
        let mut batch = store.buffered_batch();
        store.stage_query_index_header(&mut batch, header);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn stage_query_index_value_for_test(
        store: &GraphStore,
        keyspace: &Keyspace,
        key: impl Into<fjall::UserKey>,
        value: impl Into<fjall::UserValue>,
    ) {
        let mut batch = store.buffered_batch();
        batch.insert(keyspace, key, value);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn remove_query_index_key_for_test(
        store: &GraphStore,
        keyspace: &Keyspace,
        key: impl Into<fjall::UserKey>,
    ) {
        let mut batch = store.buffered_batch();
        batch.remove(keyspace, key);
        store.commit_fjall_batch(batch).unwrap();
    }

    fn read_rows_for_test(
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
    fn deterministic_term_ids_round_trip() {
        let (_dir, store) = setup_store();
        let term = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:term"));
        let id = store.encode_term(&term).unwrap();
        assert_eq!(Some(id), store.lookup_term(&term).unwrap());
        assert_eq!(term, store.decode_term(id).unwrap());
    }

    #[test]
    fn query_index_fresh_empty_is_ready_and_old_source_without_header_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:old-source");
        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_query_index_ready(&store, 0);

            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            assert_query_index_ready(&store, 1);

            remove_query_index_key_for_test(&store, &store.qv2_meta, QUERY_INDEX_HEADER_KEY);
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
    fn query_index_v2_keys_and_disk_space_are_smaller_than_u128_layout() {
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
                qv2_gspo_key(query_quad),
                qv2_gpos_key(query_quad),
                qv2_spog_key(query_quad),
                qv2_posg_key(query_quad),
                qv2_ospg_key(query_quad),
                qv2_gosp_key(query_quad),
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

        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            QueryIndexCounterKey::GraphPredicate(
                query_term_id_for_test(&store, quad.graph),
                query_term_id_for_test(&store, quad.predicate),
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

        let mut failed = query_index_header_for_test(&store);
        failed.state = StoredQueryIndexState::Failed("test-failed".to_owned());
        stage_query_index_header_for_test(&store, &failed);
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
    fn qv_reads_fall_back_for_every_spo_binding_shape_when_metadata_is_untrusted() {
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
            .map(|pattern| read_rows_for_test(&store, GraphSelector::Named(first.graph), pattern))
            .collect();
        let ready = query_index_header_for_test(&store);

        remove_query_index_key_for_test(&store, &store.qv2_meta, QUERY_INDEX_HEADER_KEY);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_rows_for_test(&store, GraphSelector::Named(first.graph), *shape),
                "Missing changed binding shape {shape:?}"
            );
        }
        stage_query_index_header_for_test(&store, &ready);

        let mut building = ready.clone();
        building.state = StoredQueryIndexState::Building;
        stage_query_index_header_for_test(&store, &building);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_rows_for_test(&store, GraphSelector::Named(first.graph), *shape),
                "Building changed binding shape {shape:?}"
            );
        }
        stage_query_index_header_for_test(&store, &ready);

        let mut failed = ready.clone();
        failed.state = StoredQueryIndexState::Failed("test-failed".to_owned());
        stage_query_index_header_for_test(&store, &failed);
        for (shape, expected) in patterns.iter().zip(&trusted) {
            assert_eq!(
                expected,
                &read_rows_for_test(&store, GraphSelector::Named(first.graph), *shape),
                "Failed changed binding shape {shape:?}"
            );
        }
        stage_query_index_header_for_test(&store, &ready);
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
        let ready = query_index_header_for_test(&store);

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

        remove_query_index_key_for_test(&store, &store.qv2_meta, QUERY_INDEX_HEADER_KEY);
        assert_unavailable("Missing");
        stage_query_index_header_for_test(&store, &ready);

        let mut building = ready.clone();
        building.state = StoredQueryIndexState::Building;
        stage_query_index_header_for_test(&store, &building);
        assert_unavailable("Building");
        stage_query_index_header_for_test(&store, &ready);

        let mut failed = ready.clone();
        failed.state = StoredQueryIndexState::Failed("test-failed".to_owned());
        stage_query_index_header_for_test(&store, &failed);
        assert_unavailable("Failed");
        stage_query_index_header_for_test(&store, &ready);
    }

    #[test]
    fn qv_malformed_and_stale_headers_fall_back_before_cursor_output() {
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
        let expected = read_rows_for_test(&store, GraphSelector::Named(quad.graph), pattern);
        let ready = query_index_header_for_test(&store);

        stage_query_index_value_for_test(&store, &store.qv2_meta, QUERY_INDEX_HEADER_KEY, [0_u8]);
        assert_eq!(
            expected,
            read_rows_for_test(&store, GraphSelector::Named(quad.graph), pattern)
        );
        stage_query_index_header_for_test(&store, &ready);

        let mut stale = ready.clone();
        stale.index_epoch = stale.index_epoch.saturating_add(1);
        stage_query_index_header_for_test(&store, &stale);
        assert_eq!(
            expected,
            read_rows_for_test(&store, GraphSelector::Named(quad.graph), pattern)
        );
    }

    #[test]
    fn selected_qv_row_corruption_is_terminal_without_a_fallback_restart() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:terminal-corruption");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p1", "urn:test:o1"));
        let second = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p2", "urn:test:o2"));
        commit_add(&store, &graph, first);
        commit_add(&store, &graph, second);
        settle_diagnostics(&store, &graph);

        let (first, corrupt) = if qv2_gspo_key(query_quad_for_test(&store, first))
            < qv2_gspo_key(query_quad_for_test(&store, second))
        {
            (first, second)
        } else {
            (second, first)
        };
        stage_query_index_value_for_test(
            &store,
            &store.qv2_gspo,
            qv2_gspo_key(query_quad_for_test(&store, corrupt)),
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
            Some(Err(StoreError::InvalidQueryIndexEncoding { .. }))
        ));
        assert!(
            cursor.next().is_none(),
            "a qv error must finish this cursor"
        );
    }

    #[test]
    fn query_index_insert_and_delete_survive_restart_ready() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:restart");
        let (quad, query_quad, dot) = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            let dot = commit_add(&store, &graph, quad);
            assert_query_index_ready(&store, 1);
            let query_quad = query_quad_for_test(&store, quad);
            let header = query_index_header_for_test(&store);
            assert_eq!(header.next_query_id, 4);
            store.persist().unwrap();
            (quad, query_quad, dot)
        };

        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_query_index_ready(&store, 1);
            assert_eq!(query_quad_for_test(&store, quad), query_quad);
            let mut witnessed = VectorClock::new();
            witnessed.advance(dot.actor, dot.counter);
            commit_remove(&store, &graph, quad, &witnessed);
            assert_query_index_ready(&store, 0);
            assert_eq!(query_quad_for_test(&store, quad), query_quad);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_query_index_ready(&reopened, 0);
        assert_eq!(query_quad_for_test(&reopened, quad), query_quad);
    }

    #[test]
    fn query_index_coalesces_live_dot_transitions_and_last_dot_removal() {
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
        assert_query_index_ready(&store, 1);

        let mut first_only = VectorClock::new();
        first_only.advance(first.actor, first.counter);
        commit_remove(&store, &graph, quad, &first_only);
        assert_query_index_ready(&store, 1);

        let mut all_dots = VectorClock::new();
        all_dots.advance(first.actor, first.counter);
        all_dots.advance(second.actor, second.counter);
        commit_remove(&store, &graph, quad, &all_dots);
        assert_query_index_ready(&store, 0);
    }

    #[test]
    fn query_index_concurrent_cross_graph_writes_keep_exact_counters() {
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

        assert_query_index_ready(&store, 2);
        let first_query = query_quad_for_test(&store, first);
        let second_query = query_quad_for_test(&store, second);
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Total),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(first_query.graph)),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(second_query.graph)),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::Predicate(first_query.predicate),
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::PredicateObject(first_query.predicate, first_query.object)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(first_query.graph, first_query.predicate)
            ),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(second_query.graph, second_query.predicate)
            ),
            Some(1)
        );
    }

    #[test]
    fn query_index_coalesces_repeated_crossings_in_one_batch() {
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
        let header = query_index_header_for_test(&store);
        assert_eq!(header.source_epoch, 1);
        assert_query_index_ready(&store, 1);

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
        assert_eq!(query_index_header_for_test(&store).source_epoch, 1);
        assert_query_index_ready(&store, 1);
    }

    #[test]
    fn query_index_union_duplicate_proof_falls_back_and_rebuilds() {
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
            query_index_counter_for_test(&store, QueryIndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        commit_add(&store, &first_graph, first);
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        commit_add(&store, &second_graph, second);
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::UnionDuplicateFree),
            Some(0)
        );

        let clock = store.get_vector_clock(&second_graph).unwrap();
        commit_remove(&store, &second_graph, second, &clock);
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::UnionDuplicateFree),
            Some(0)
        );
        store.rebuild_query_indexes().unwrap();
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::UnionDuplicateFree),
            Some(1)
        );
        assert_query_index_ready(&store, 1);
    }

    #[test]
    fn query_index_tracks_exact_dimensions_and_removes_zero_counters() {
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
        assert_query_index_ready(&store, 4);
        let one_query = query_quad_for_test(&store, one);
        let three_query = query_quad_for_test(&store, three);

        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Total),
            Some(4)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(one_query.graph)),
            Some(3)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::Predicate(one_query.predicate),
            ),
            Some(3)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(one_query.graph, one_query.predicate)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::PredicateObject(one_query.predicate, one_query.object)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicateObject(
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
        assert_query_index_ready(&store, 3);
        for key in [
            QueryIndexCounterKey::Predicate(three_query.predicate),
            QueryIndexCounterKey::GraphPredicate(three_query.graph, three_query.predicate),
            QueryIndexCounterKey::PredicateObject(three_query.predicate, three_query.object),
            QueryIndexCounterKey::GraphPredicateObject(
                three_query.graph,
                three_query.predicate,
                three_query.object,
            ),
        ] {
            assert_eq!(query_index_counter_for_test(&store, key), None);
        }
    }

    #[test]
    fn query_index_removing_last_row_keeps_ready_and_removes_zero_dimensions() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:last-row");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        let dot = commit_add(&store, &graph, quad);
        let query_quad = query_quad_for_test(&store, quad);

        let mut witnessed = VectorClock::new();
        witnessed.advance(dot.actor, dot.counter);
        commit_remove(&store, &graph, quad, &witnessed);

        assert_query_index_ready(&store, 0);
        let header = query_index_header_for_test(&store);
        assert_eq!(header.source_live_quads, 0);
        assert_eq!(header.indexed_quads, 0);
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Total),
            Some(0)
        );
        for key in [
            QueryIndexCounterKey::Graph(query_quad.graph),
            QueryIndexCounterKey::Predicate(query_quad.predicate),
            QueryIndexCounterKey::GraphPredicate(query_quad.graph, query_quad.predicate),
            QueryIndexCounterKey::PredicateObject(query_quad.predicate, query_quad.object),
            QueryIndexCounterKey::GraphPredicateObject(
                query_quad.graph,
                query_quad.predicate,
                query_quad.object,
            ),
        ] {
            assert_eq!(query_index_counter_for_test(&store, key), None);
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
    fn query_index_keys_are_fixed_order_and_empty() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:keys");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let snapshot = store.db.snapshot();
        let query_quad = query_quad_for_test(&store, quad);
        for (keyspace, key) in [
            (&store.qv2_gspo, qv2_gspo_key(query_quad)),
            (&store.qv2_gpos, qv2_gpos_key(query_quad)),
            (&store.qv2_spog, qv2_spog_key(query_quad)),
            (&store.qv2_posg, qv2_posg_key(query_quad)),
            (&store.qv2_ospg, qv2_ospg_key(query_quad)),
            (&store.qv2_gosp, qv2_gosp_key(query_quad)),
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
    fn query_index_status_rejects_mismatched_qv_rows_or_total() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:status-qv-rows");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);

        remove_query_index_key_for_test(
            &store,
            &store.qv2_spog,
            qv2_spog_key(query_quad_for_test(&store, quad)),
        );
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_query_index_value_for_test(
            &store,
            &store.qv2_spog,
            qv2_spog_key(query_quad_for_test(&store, quad)),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(
            &store,
            &store.qv2_posg,
            qv2_posg_key(query_quad_for_test(&store, quad)),
            vec![1],
        );
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_query_index_value_for_test(
            &store,
            &store.qv2_posg,
            qv2_posg_key(query_quad_for_test(&store, quad)),
            Vec::<u8>::new(),
        );
        remove_query_index_key_for_test(
            &store,
            &store.qv2_spog,
            qv2_spog_key(query_quad_for_test(&store, quad)),
        );
        stage_query_index_value_for_test(&store, &store.qv2_spog, vec![0; 31], Vec::<u8>::new());
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        remove_query_index_key_for_test(&store, &store.qv2_spog, vec![0; 31]);
        stage_query_index_value_for_test(
            &store,
            &store.qv2_spog,
            qv2_spog_key(query_quad_for_test(&store, quad)),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            QUERY_INDEX_TOTAL_KEY,
            2u64.to_be_bytes(),
        );
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
        let probes_before = reopened.query_index_admission_probe_count();
        let status = reopened.query_index_status_fast().unwrap();
        assert_eq!(QueryIndexState::Ready, status.state);
        assert_eq!(1, status.source_live_quads);
        assert_eq!(1, status.indexed_quads);
        assert_eq!(
            2,
            reopened.query_index_admission_probe_count() - probes_before,
            "fast status reads only the header and total counter"
        );
        assert_eq!(0, reopened.index_verify_count());

        let sampled = reopened
            .verify_query_indexes(QueryIndexVerificationMode::Sample)
            .unwrap();
        assert!(sampled.valid);
        assert!(!sampled.full);
        assert_eq!(1, reopened.index_verify_count());
    }

    #[test]
    fn query_index_rebuild_after_restart_recovers_missing_and_advances_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:rebuild-restart");
        {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            remove_query_index_key_for_test(&store, &store.qv2_meta, QUERY_INDEX_HEADER_KEY);
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
            let first = query_index_header_for_test(&store);
            assert!(matches!(first.state, StoredQueryIndexState::Ready));
            assert!(first.last_build_sequence >= before_rebuild_sequence);
            assert!(first.source_epoch >= before_rebuild_sequence);
            assert_eq!(first.source_live_quads, 1);
            assert_eq!(first.indexed_quads, 1);
            assert_query_index_ready(&store, 1);

            store.rebuild_query_indexes().unwrap();
            let second = query_index_header_for_test(&store);
            assert!(second.last_build_sequence > first.last_build_sequence);
            assert!(second.source_epoch > first.source_epoch);
            assert!(second.query_id_generation > first.query_id_generation);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_query_index_ready(&reopened, 1);
    }

    #[test]
    fn query_index_interrupted_building_reopens_without_promoting_derived_rows() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:interrupted-rebuild");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            let mut header = query_index_header_for_test(&store);
            header.state = StoredQueryIndexState::Building;
            stage_query_index_header_for_test(&store, &header);
            remove_query_index_key_for_test(
                &store,
                &store.qv2_posg,
                qv2_posg_key(query_quad_for_test(&store, quad)),
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
            assert_query_index_ready(&store, 1);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_query_index_ready(&reopened, 1);
    }

    #[test]
    fn query_index_full_and_sample_verification_are_deterministic() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:verify-sample");
        store.create_graph(&graph).unwrap();
        let rows = QUERY_INDEX_SAMPLE_ROWS + 1;
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
        assert_eq!(sample.checked_source_rows, QUERY_INDEX_SAMPLE_ROWS);
        assert_eq!(sample.checked_index_rows, QUERY_INDEX_SAMPLE_ROWS * 6);

        let full = store.verify_query_indexes(true).unwrap();
        assert!(full.valid);
        assert!(full.full);
        assert_eq!(full.source_live_quads, rows);
        assert_eq!(full.indexed_quads, rows);
        assert_eq!(full.checked_source_rows, rows);
        assert_eq!(full.checked_index_rows, rows * 6);
    }

    #[test]
    fn query_index_verification_detects_qv_and_metadata_corruption() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:verification-corruption");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let query_quad = query_quad_for_test(&store, quad);
        let extra = QueryQuad {
            subject: QueryTermId(query_index_header_for_test(&store).next_query_id),
            ..query_quad
        };

        stage_query_index_value_for_test(
            &store,
            &store.qv2_gpos,
            qv2_gpos_key(extra),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(
            &store,
            &store.qv2_gpos,
            qv2_gpos_key(query_quad),
            vec![1],
        );
        remove_query_index_key_for_test(&store, &store.qv2_spog, qv2_spog_key(query_quad));
        stage_query_index_value_for_test(&store, &store.qv2_posg, vec![0; 31], Vec::<u8>::new());
        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            QUERY_INDEX_TOTAL_KEY,
            vec![0; 7],
        );
        stage_query_index_value_for_test(&store, &store.qv2_meta, vec![b'Z'], 0u64.to_be_bytes());
        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            vec![b'G', 0],
            0u64.to_be_bytes(),
        );
        let orphan_graph = QueryTermId(query_index_header_for_test(&store).next_query_id);
        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            QueryIndexCounterKey::Graph(orphan_graph).bytes(),
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
            assert_query_index_problem(&report, problem);
        }
    }

    #[test]
    fn query_index_maintenance_anomaly_commits_source_and_fails_derived_state() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-anomaly");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, first);
        remove_query_index_key_for_test(
            &store,
            &store.qv2_meta,
            QueryIndexCounterKey::Predicate(query_term_id_for_test(&store, first.predicate))
                .bytes(),
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
    fn query_index_maintenance_rejects_ahead_header_without_losing_source_write() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-ahead-header");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, first);
        let mut header = query_index_header_for_test(&store);
        let ahead = store.db.snapshot().seqno().checked_add(100).unwrap();
        header.source_epoch = ahead;
        header.index_epoch = ahead;
        stage_query_index_header_for_test(&store, &header);

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
    fn query_index_maintenance_rejects_orphan_counter_without_losing_source_write() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-orphan-counter");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p1", "urn:test:o"));
        commit_add(&store, &graph, first);
        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p2", "urn:test:o"));
        let orphan_predicate = query_index_header_for_test(&store)
            .next_query_id
            .checked_add(1)
            .unwrap();
        stage_query_index_value_for_test(
            &store,
            &store.qv2_meta,
            QueryIndexCounterKey::Predicate(QueryTermId(orphan_predicate)).bytes(),
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
    fn query_index_malformed_metadata_or_counter_is_never_trusted_ready() {
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
            stage_query_index_value_for_test(
                &store,
                &store.qv2_meta,
                QUERY_INDEX_HEADER_KEY,
                vec![0],
            );
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
            stage_query_index_value_for_test(
                &store,
                &store.qv2_meta,
                QUERY_INDEX_TOTAL_KEY,
                vec![0; 7],
            );
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
    fn query_index_epoch_mismatch_fails_open_and_explicit_rebuild_preserves_source() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:epoch-mismatch");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            let mut header = query_index_header_for_test(&store);
            header.index_epoch = header.index_epoch.checked_add(1).unwrap();
            stage_query_index_header_for_test(&store, &header);
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
        assert_query_index_ready(&store, 1);
        assert_eq!(
            store.quads_for_pattern(None, None, None, None).unwrap(),
            source_before_rebuild,
            "rebuild must never mutate canonical source rows"
        );
    }

    #[test]
    fn query_index_rebuild_discards_ahead_header_hints() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:ahead-hints");
        let first = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, first);
            let mut header = query_index_header_for_test(&store);
            header.source_epoch = u64::MAX;
            header.index_epoch = u64::MAX;
            header.last_build_sequence = u64::MAX;
            stage_query_index_header_for_test(&store, &header);
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
        assert_query_index_problem(&failed_report, "meta-epoch-ahead-of-snapshot");
        assert_query_index_problem(&failed_report, "meta-build-sequence-ahead-of-snapshot");
        assert_eq!(
            store.quads_for_pattern(None, None, None, None).unwrap(),
            vec![first]
        );

        store.rebuild_query_indexes().unwrap();
        assert_query_index_ready(&store, 1);
        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, second);
        assert_query_index_ready(&store, 2);
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
    fn query_index_manual_compaction_preserves_all_keyspaces_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphId::new("urn:test:qv:manual-compact");
        let quad = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            commit_add(&store, &graph, quad);
            store.manual_compact().unwrap();
            let snapshot = store.db.snapshot();
            let query_quad = query_quad_for_test(&store, quad);
            for (keyspace, key) in [
                (&store.qv2_gspo, qv2_gspo_key(query_quad)),
                (&store.qv2_gpos, qv2_gpos_key(query_quad)),
                (&store.qv2_spog, qv2_spog_key(query_quad)),
                (&store.qv2_posg, qv2_posg_key(query_quad)),
                (&store.qv2_ospg, qv2_ospg_key(query_quad)),
                (&store.qv2_gosp, qv2_gosp_key(query_quad)),
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
                    .get(&store.qv2_meta, QUERY_INDEX_HEADER_KEY)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                snapshot
                    .get(&store.qv2_meta, QUERY_INDEX_TOTAL_KEY)
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &1u64.to_be_bytes()
            );
            store.persist().unwrap();
            quad
        };

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_query_index_ready(&reopened, 1);
        assert_eq!(
            reopened.quads_for_pattern(None, None, None, None).unwrap(),
            vec![quad]
        );
    }

    #[test]
    fn graph_queries_use_durable_source() {
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
    fn durable_pattern_reads_track_commits() {
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
    fn fts_dirty_set_deduplicates_subjects() {
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

    /// Re-dirtying a subject must not lift its entry above a bound pinned
    /// before it: the flush holding that bound promised to index the first
    /// write, and a drain filters on the token the entry carries.
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
        let bound = QueueBound {
            chunk: 10,
            max_token: Some(store.current_dirty_token()),
        };
        // Carry the counter past the bound, then dirty the subject again.
        enqueue(other);
        enqueue(subject);

        let drained = drain_upto(&bound, |chunk| store.drain_fts_queue(chunk)).unwrap();

        assert!(drained.iter().any(|entry| entry.subject == subject));
    }

    /// An enqueue landing between an acknowledgement's token read and its
    /// commit must survive: the removal only ever covered the older token, so
    /// erasing the entry would leave that write unindexed for good.
    #[test]
    fn ack_keeps_racing_enqueue() {
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

        store.set_fts_ack_stall(std::time::Duration::from_millis(300));
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
    fn fts_graph_reindex_queue_round_trips() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let mut batch = store.new_batch();
        store.enqueue_fts_reindex(&mut batch, graph_id).unwrap();
        store.commit(batch).unwrap();

        let queued = store.drain_fts_reindex_queue(10).unwrap();
        assert_eq!(1, queued.len());
        assert_eq!(queued[0].graph, graph);
        store.acknowledge_fts_reindex_queue(&queued).unwrap();
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
    }

    // ── W14: the reindex collapse is relative to graph size ─────────────

    /// Give `graph` exactly `count` distinct subjects, in one batch.
    ///
    /// Returns the graph's term id together with its subject ids, in ascending
    /// seed order, so a caller can enqueue a prefix of them.
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
        let reindexes = store.drain_fts_reindex_queue(usize::MAX).unwrap().len();
        (per_subject, reindexes)
    }

    /// The collapse still fires when the rescan really is the cheaper option:
    /// the batch is large *and* covers half the graph, so re-reading the graph
    /// costs no more than the per-subject entries it replaces.
    #[test]
    fn enqueue_collapses_batch() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:half");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) =
            seed_subjects(&store, &graph, FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD * 2);
        let batch = &subjects[..FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD];

        assert_eq!((0, 1), enqueue_and_count(&store, graph_id, batch));
    }

    /// One subject past the halfway mark the rescan is the more expensive
    /// option, and the enqueue must stay per-subject.
    ///
    /// The absolute rule alone turned this write into a rescan of a graph twice
    /// its size — and, in a batched ingest, once per batch.
    #[test]
    fn enqueue_below_ratio() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:dwarfed");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) =
            seed_subjects(&store, &graph, FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD * 2 + 1);
        let batch = &subjects[..FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD];

        // Every affected subject is queued: the relative rule may only move
        // work off the rescan branch, never drop it (G7).
        assert_eq!(
            (FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD, 0),
            enqueue_and_count(&store, graph_id, batch)
        );
    }

    /// A batch that is the whole graph still stays per-subject while it is
    /// small: the absolute bound survives the relative one.
    #[test]
    fn enqueue_below_threshold() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:w14:small");
        store.create_graph(&graph).unwrap();

        let (graph_id, subjects) = seed_subjects(&store, &graph, 100);

        assert_eq!((100, 0), enqueue_and_count(&store, graph_id, &subjects));
    }

    // ── Commit guards and cache publication ─────────────────────────────

    /// Concurrent adds to one quad must each contribute a distinct dot (G1).
    ///
    /// Without the commit guard the `next_counter` read-then-write and the
    /// `insert_quad` read-modify-write of the dot set interleave, so two writers
    /// mint the same counter and one add is lost.
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

        // Every minted counter is reflected in the graph clock (G2).
        let clock = store.get_vector_clock(&graph).unwrap();
        let clocked: u64 = clock.0.values().sum();
        assert_eq!(dots.len() as u64, clocked);
    }

    #[test]
    fn global_commit_lock_concurrency() {
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
            store.commit_stall_max_active() >= 2,
            "independent durable commits were serialized by one cache lock"
        );
        assert_query_index_ready(&store, 2);
    }

    #[test]
    fn commit_failure_publishes_no_cache_state() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:failed-commit-cache");
        store.create_graph(&graph).unwrap();
        let seeded = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:seeded"));
        let rejected = encode_quad(&store, &graph, ("urn:s", "urn:p", "urn:rejected"));
        commit_add(&store, &graph, seeded);
        assert!(store.index_contains(seeded));
        let generation = store.indexes_read().generations.get(&seeded.graph).copied();

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

        assert_eq!(
            generation,
            store.indexes_read().generations.get(&seeded.graph).copied()
        );
        assert!(store.index_contains(seeded));
        assert!(!store.contains_quad(rejected).unwrap());
    }

    #[test]
    fn crash_after_durable_commit_rebuilds_cache_state() {
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
            } = batch;
            let mut durable = DurableCommit {
                batch: inner,
                pending_fts,
            };
            assert!(store.begin_query_index_commit());
            store
                .stage_query_index_maintenance(&mut durable.batch, &publish)
                .unwrap();
            store.commit_durable(durable).unwrap();
            assert!(store.finish_query_index_commit());
            store.persist().unwrap();
            // Deliberately omit `indexes.publish(&publish)`: this is the crash
            // window after durable commit and before cache publication.
        }

        let reopened = GraphStore::open(directory.path()).unwrap();
        assert!(reopened.contains_quad(quad).unwrap());
        assert_eq!(
            1,
            reopened
                .subject_triple_count_by_ids(quad.graph, quad.subject)
                .unwrap()
        );
    }

    #[test]
    fn bounded_store_cache_statistics() {
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

    /// The self-guarding store functions take their own guard, so calling them
    /// concurrently — including on graphs that share a lock shard — must make
    /// progress rather than deadlock.
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
                            .set_irokle_topic_id(&graph, [(index * 32 + round as usize) as u8; 32])
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
    fn commit_generation_invalidates_corrupt_cache() {
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
        store.corrupt_index_for_test(removed);
        store.corrupt_index_for_test(collateral);
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

    // ── FTS queue tokens across restarts ────────────────────────────────

    /// A reindex token issued before a restart must never acknowledge a
    /// subject entry queued after it. With the counter restarting at 1 the
    /// post-restart entry gets a lower token and is silently dropped without
    /// tantivy ever having indexed the subject.
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

            let queued = store.drain_fts_reindex_queue(10).unwrap();
            assert_eq!(1, queued.len());
            // The pre-restart subject entries are legitimately covered.
            store
                .acknowledge_fts_subjects_for_reindexed_graphs(&queued)
                .unwrap();
            assert!(store.drain_fts_queue(10).unwrap().is_empty());
            queued[0].tokens.latest
        };

        let store = GraphStore::open(dir.path()).unwrap();
        assert!(
            store.current_dirty_token() > reindex_token,
            "the token counter must resume past every live queue token"
        );

        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject = store.resolve_term(&named("urn:post-restart")).unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts(&mut batch, FtsSubject { graph_id, subject })
            .unwrap();
        store.commit(batch).unwrap();

        let reindex_queued = store.drain_fts_reindex_queue(10).unwrap();
        assert_eq!(1, reindex_queued.len());
        assert_eq!(graph, reindex_queued[0].graph);
        assert_eq!(reindex_token, reindex_queued[0].tokens.latest);
        store
            .acknowledge_fts_subjects_for_reindexed_graphs(&reindex_queued)
            .unwrap();

        let remaining = store.drain_fts_queue(10).unwrap();
        assert_eq!(
            1,
            remaining.len(),
            "a subject queued after the reindex must survive its acknowledgement"
        );
        assert_eq!(subject, remaining[0].subject);
    }

    // ── Vector-clock key split ──────────────────────────────────────────

    /// Open is the only moment a store written before the split can still be
    /// carrying its clock inside the metadata record, so that is where the
    /// fallback runs and seeds the mirror every later read uses.
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
        let mut meta = store.read_graph_meta_by_id(graph_id).unwrap().unwrap();
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
        let meta_after = store.read_graph_meta_by_id(graph_id).unwrap().unwrap();
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

    // ── Persisted, clock-tagged diagnostics ─────────────────────────────

    /// Attach `entity` to the graph as a data entity that is *not* reachable
    /// from the root, i.e. an orphan.
    /// Persist a graph's diagnostics the way a committing writer does.
    ///
    /// Reads deliberately do not persist what they recompute (see
    /// [`GraphStore::graph_diagnostics_by_id`]), so a fixture that needs a
    /// stored record has to settle it here, holding the guard the writer holds.
    fn settle_diagnostics(store: &GraphStore, graph: &GraphId) {
        let _commit_guard = store.graph_commit_guard(graph);
        let diagnostics = store.compute_graph_diagnostics(graph).unwrap();
        store.set_graph_diagnostics(graph, &diagnostics).unwrap();
    }

    /// A reader that sees the post-commit clock must not then read an index
    /// that predates it: it would compute a pre-write orphan set, tag it with
    /// the post-write clock, and every later reader would accept that as fresh
    /// until the next write (G6).
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
            .for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
                indexed.push(quad);
                Ok(())
            })
            .unwrap();
        let mut stored = Vec::new();
        store
            .for_each_stored_quad(graph_id, |quad, _| {
                stored.push(quad);
                Ok(())
            })
            .unwrap();
        let key = |quad: &EncodedQuad| (quad.subject, quad.predicate, quad.object);
        indexed.sort_by_key(key);
        stored.sort_by_key(key);
        (indexed, stored)
    }

    /// A rebuild scans the durable quads and then installs what it read. While
    /// the scan ran unlocked, a commit landing inside that window was erased
    /// from the index by the install, yet kept the clock it had published.
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

    /// Repopulating the object-order cache reads the index, then decodes and
    /// sorts with no lock held. An invalidation landing entirely inside that
    /// window used to be undone by the ordering the reader had already
    /// computed — stored untagged, so nothing ever rechecked it, and a graph
    /// that then went quiet kept paging exports missing the newest `hasPart`
    /// child (G6).
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
                .count_objects_for_subject_predicate(&graph, &root, &predicate)
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
                        // The writer only ever appends, so the paged total has
                        // to fall inside the counts sandwiching the read.
                        let before = total_objects();
                        let paged = first_page();
                        let after = total_objects();
                        assert!(
                            (before..=after).contains(&paged),
                            "paging reported {paged} objects, outside the {before}..={after} the \
                             index held during the read"
                        );
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

    /// The id-based orphan pass in [`GraphStore::compute_graph_diagnostics`] is
    /// an optimisation of `rules::orphaned_data_entities`, so it has to agree
    /// with it on every shape that distinguishes them: reachable and unreachable
    /// entities, chains, `hasPart` cycles, entities that are only ever a
    /// `hasPart` object, typed non-data entities, and the root itself (which is
    /// never an orphan). Consistency outranks speed — if these ever diverge, the
    /// rule is right and this test is the thing that says so.
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

    /// Quads committed without a diagnostics refresh — what a crash between the
    /// quad commit and the diagnostics write leaves behind — must be repaired
    /// promptly: at open, and by any read that sees the stale tag.
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
            let mut clock = store.get_vector_clock_by_id(quad.graph).unwrap();
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

            // Repair on read: dirty the graph again without refreshing
            // diagnostics, then read. The clock tag no longer matches, so the
            // reader must recompute rather than serve the stale set.
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

            // The read repaired what it served, not what is stored: the record
            // is the search re-queue's baseline, so only a committing writer
            // moves it.
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

    /// A read may not touch the persisted diagnostics record.
    ///
    /// The record names the orphan set the search index was last brought in
    /// step with, and `rebuild_graph_diagnostics` re-queues exactly the
    /// difference against it. A reader that persisted its own recomputation —
    /// the search worker is such a reader — would erase that difference without
    /// indexing anything, and the entity whose visibility changed would never
    /// be re-queued (G7).
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

    /// A repair that changes the orphan set must re-queue the affected
    /// subjects, because orphans are invisible to search: otherwise the index
    /// keeps showing an entity the store now hides (G6/G7).
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

    /// The other direction: an entity the root adopted since the record was
    /// written has to come *back* to search at open, or the restart leaves it
    /// hidden (G7).
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

    // ── Durability under the fjall configuration (G10) ──────────────────

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
            let mut clock = store.get_vector_clock_by_id(graph_id).unwrap();
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
    fn clear_fts_queue_for_graph_removes_subject_and_reindex_entries() {
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
            .clear_fts_queue_for_graph(&graph, store.current_dirty_token())
            .unwrap();
        assert!(store.drain_fts_queue(10).unwrap().is_empty());
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
    }

    /// The delete scans the graph's queue keys without the queue lock, so an
    /// enqueue landing before its commit used to outlive the deleted graph and
    /// re-index a subject of a graph that no longer exists.
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
            store.drain_fts_delete_queue(10).unwrap().len(),
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

    /// `manual_compact` must flush, not just request a compaction: without the
    /// rotation there is nothing on disk to compact and the journal is never
    /// reclaimed (C1/C2).
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
}
