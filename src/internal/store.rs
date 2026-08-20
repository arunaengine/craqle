use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque, hash_map::Entry};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::core::*;
use crate::search_queue::{DirtyGraph, DirtySubject, DirtyTokens};
use crate::{
    QueryIndexState, QueryIndexStatus, QueryIndexVerification, QueryIndexVerificationMode,
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
    #[error("query index unavailable: {0}")]
    QueryIndexUnavailable(&'static str),
}

impl StoreError {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodedQuad {
    pub graph: TermId,
    pub subject: TermId,
    pub predicate: TermId,
    pub object: TermId,
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
/// Per-graph vector clock, split out of the graph meta record so a
/// commit writes only the clock and never rewrites policy/context/topic bytes.
const GRAPH_CLOCK_PREFIX: u8 = b'K';
/// Persisted, clock-tagged graph diagnostics.
const GRAPH_DIAGNOSTICS_PREFIX: u8 = b'O';
const TERM_LOCK_SHARDS: usize = 64;
const COMMIT_LOCK_SHARDS: usize = 64;
/// Upper bound on the global term-decode cache. Term ids are content hashes so
/// entries are never invalidated; the cache is simply cleared when it hits the
/// cap.
const TERM_DECODE_CACHE_CAP: usize = 1_000_000;
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

const QUERY_INDEX_SCHEMA_VERSION: u32 = 1;
const QUERY_INDEX_HEADER_KEY: [u8; 1] = *b"H";
const QUERY_INDEX_TOTAL_KEY: [u8; 1] = *b"T";
const QUERY_INDEX_HEADER_MAGIC: [u8; 4] = *b"QVI1";
const QUERY_INDEX_HEADER_BASE_LEN: usize = 54;
const QUERY_INDEX_FAILURE_MAX_BYTES: usize = 256;
const QUERY_INDEX_BUILD_CHUNK_ROWS: usize = 1_024;
const QUERY_INDEX_SAMPLE_ROWS: u64 = 128;
const QUERY_INDEX_PROBLEM_LIMIT: usize = 32;

const QUERY_INDEX_GRAPH_COUNT_TAG: u8 = b'G';
const QUERY_INDEX_PREDICATE_COUNT_TAG: u8 = b'P';
const QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG: u8 = b'A';
const QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG: u8 = b'O';
const QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG: u8 = b'X';
const QUERY_INDEX_PREDICATE_MUTATION_EPOCH_TAG: u8 = b'M';

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
    Graph(TermId),
    Predicate(TermId),
    GraphPredicate(TermId, TermId),
    PredicateObject(TermId, TermId),
    GraphPredicateObject(TermId, TermId, TermId),
    PredicateMutationEpoch(TermId),
}

impl QueryIndexCounterKey {
    fn bytes(self) -> Vec<u8> {
        let mut key = match self {
            Self::Total => return QUERY_INDEX_TOTAL_KEY.to_vec(),
            Self::Graph(_) | Self::Predicate(_) | Self::PredicateMutationEpoch(_) => vec![0; 17],
            Self::GraphPredicate(_, _) | Self::PredicateObject(_, _) => vec![0; 33],
            Self::GraphPredicateObject(_, _, _) => vec![0; 49],
        };
        match self {
            Self::Graph(graph) => {
                key[0] = QUERY_INDEX_GRAPH_COUNT_TAG;
                key[1..17].copy_from_slice(&graph.to_be_bytes());
            }
            Self::Predicate(predicate) => {
                key[0] = QUERY_INDEX_PREDICATE_COUNT_TAG;
                key[1..17].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::GraphPredicate(graph, predicate) => {
                key[0] = QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG;
                key[1..17].copy_from_slice(&graph.to_be_bytes());
                key[17..33].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::PredicateObject(predicate, object) => {
                key[0] = QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG;
                key[1..17].copy_from_slice(&predicate.to_be_bytes());
                key[17..33].copy_from_slice(&object.to_be_bytes());
            }
            Self::GraphPredicateObject(graph, predicate, object) => {
                key[0] = QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG;
                key[1..17].copy_from_slice(&graph.to_be_bytes());
                key[17..33].copy_from_slice(&predicate.to_be_bytes());
                key[33..49].copy_from_slice(&object.to_be_bytes());
            }
            Self::PredicateMutationEpoch(predicate) => {
                key[0] = QUERY_INDEX_PREDICATE_MUTATION_EPOCH_TAG;
                key[1..17].copy_from_slice(&predicate.to_be_bytes());
            }
            Self::Total => unreachable!("total counter returned before allocating a key"),
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
    transitions: Vec<NetQuadTransition>,
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

/// Outcome of applying one quad mutation to a derived index.
///
/// `Anomaly` means the index disagreed with the durable `quads` keyspace, which
/// is the source of truth: an insert found the `(predicate, object)` pair
/// already present, or a remove found nothing to remove. Both are impossible
/// while every read→write cycle of a graph is serialized by its commit guard,
/// so seeing one means the index has drifted and must be rebuilt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexApply {
    Ok,
    Anomaly,
}

/// Every in-memory structure a reader correlates with a graph's vector clock,
/// behind one lock so a commit publishes all of them at once.
///
/// Splitting them cost consistency: `fjall::Batch::commit` applies a batch's
/// keys to the memtables one at a time, so the durable clock still reads
/// pre-commit while the same batch's quads are already visible. Freshness
/// checks therefore read [`IndexState::clocks`], and a read that observes a
/// published clock has by construction waited for the index, the derived
/// mirror and the order cache that clock describes (G6).
#[derive(Default)]
struct IndexState {
    graph_subjects: HashMap<TermId, HashSet<TermId>>,
    by_graph_subject: HashMap<(TermId, TermId), HashSet<(TermId, TermId)>>,
    /// Planner/cross-graph mirror, built on first use.
    derived: Option<DerivedIndexState>,
    object_order: ObjectOrderCache,
    /// Per-graph clocks as published by each graph's last commit. A missing
    /// entry is the empty clock, which is what the durable read yields for a
    /// graph that has never committed.
    clocks: HashMap<TermId, VectorClock>,
}

type ObjectOrderKey = (TermId, TermId, TermId);
type ObjectOrderValues = Arc<Vec<TermId>>;

/// `(graph, subject, predicate)` → objects in decoded-term order.
///
/// Repopulation decodes outside the lock, so an entry computed from an index a
/// commit has since invalidated must not be installed. `generation` moves on
/// every invalidation and a repopulating reader only installs what it computed
/// while the count has not moved.
#[derive(Default)]
struct ObjectOrderCache {
    entries: HashMap<ObjectOrderKey, ObjectOrderValues>,
    generation: u64,
}

impl ObjectOrderCache {
    fn get(&self, key: &ObjectOrderKey) -> Option<ObjectOrderValues> {
        self.entries.get(key).cloned()
    }

    fn invalidate(&mut self, key: &ObjectOrderKey) {
        self.entries.remove(key);
        self.generation += 1;
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.generation += 1;
    }

    /// Drop every entry belonging to `graph`, e.g. when the graph is deleted.
    fn drop_graph(&mut self, graph: TermId) {
        self.entries.retain(|(cached, _, _), _| *cached != graph);
        self.generation += 1;
    }

    /// Install `objects` only if nothing was invalidated since `generation`.
    fn install(&mut self, entry: OrderEntry, generation: u64) {
        if self.generation == generation {
            self.entries.insert(entry.key, entry.objects);
        }
    }
}

/// One `(graph, subject, predicate)` ordering, decoded and sorted.
struct OrderEntry {
    key: ObjectOrderKey,
    objects: ObjectOrderValues,
}

impl IndexState {
    fn insert_quad(&mut self, quad: EncodedQuad) -> IndexApply {
        let entries = self
            .by_graph_subject
            .entry((quad.graph, quad.subject))
            .or_default();
        if !entries.insert((quad.predicate, quad.object)) {
            return IndexApply::Anomaly;
        }
        if entries.len() == 1 {
            self.graph_subjects
                .entry(quad.graph)
                .or_default()
                .insert(quad.subject);
        }
        IndexApply::Ok
    }

    fn remove_quad(&mut self, quad: EncodedQuad) -> IndexApply {
        let Entry::Occupied(mut entry) = self.by_graph_subject.entry((quad.graph, quad.subject))
        else {
            return IndexApply::Anomaly;
        };
        let removed = entry.get_mut().remove(&(quad.predicate, quad.object));
        if entry.get().is_empty() {
            entry.remove();
            if let Entry::Occupied(mut subjects) = self.graph_subjects.entry(quad.graph) {
                subjects.get_mut().remove(&quad.subject);
                if subjects.get().is_empty() {
                    subjects.remove();
                }
            }
        }
        if removed {
            IndexApply::Ok
        } else {
            IndexApply::Anomaly
        }
    }

    /// Apply one commit's whole in-memory half, reporting whether any mutation
    /// disagreed with the durable `quads` keyspace.
    ///
    /// The clocks land last only for readability — the caller holds the write
    /// lock across all of it, so nothing observes an intermediate state.
    fn publish(&mut self, publish: &PendingPublish) -> bool {
        let mut anomaly = false;
        for mutation in &publish.quad_mutations {
            let (quad, inserting) = match mutation {
                QuadMutation::Insert(quad) => (*quad, true),
                QuadMutation::Remove(quad) => (*quad, false),
            };
            let outcome = if inserting {
                self.insert_quad(quad)
            } else {
                self.remove_quad(quad)
            };
            anomaly |= outcome == IndexApply::Anomaly;
            if let Some(derived) = self.derived.as_mut() {
                if inserting {
                    derived.insert_quad(quad);
                } else {
                    derived.remove_quad(quad);
                }
            }
            self.object_order
                .invalidate(&(quad.graph, quad.subject, quad.predicate));
        }

        for (&graph_id, clock) in &publish.clocks {
            match clock {
                Some(clock) => self.clocks.insert(graph_id, clock.clone()),
                None => self.clocks.remove(&graph_id),
            };
        }
        anomaly
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

#[derive(Default)]
struct DerivedIndexState {
    by_subject: HashMap<TermId, HashSet<(TermId, TermId, TermId)>>,
    by_predicate_object: HashMap<(TermId, TermId), PredicateObjectSubjects>,
    by_object: HashMap<TermId, HashMap<TermId, ObjectEntries>>,
    /// predicate → graph → live quad count, so a predicate-only pattern can be
    /// answered by scanning just the graphs that actually contain it instead of
    /// every subject in the corpus. Counts are needed so a graph is
    /// dropped only when its last quad for that predicate goes away.
    predicate_graph_counts: HashMap<TermId, HashMap<TermId, usize>>,
    // Approximate corpus-wide cardinalities for the query planner; quad
    // instances are counted per graph (no cross-graph triple dedup).
    predicate_object_counts: HashMap<(TermId, TermId), usize>,
    predicate_counts: HashMap<TermId, usize>,
    predicate_subject_term_counts: HashMap<TermId, HashMap<TermId, usize>>,
    predicate_object_term_counts: HashMap<TermId, HashMap<TermId, usize>>,
    object_counts: HashMap<TermId, usize>,
    total_quads: usize,
}

type PredicateObjectSubjects = HashMap<TermId, Arc<Vec<TermId>>>;
type ObjectEntries = Arc<BTreeSet<(TermId, TermId)>>;

impl DerivedIndexState {
    fn insert_quad(&mut self, quad: EncodedQuad) {
        let is_new = self.by_subject.entry(quad.subject).or_default().insert((
            quad.predicate,
            quad.object,
            quad.graph,
        ));
        let subjects = self
            .by_predicate_object
            .entry((quad.predicate, quad.object))
            .or_default()
            .entry(quad.graph)
            .or_default();
        let subjects = Arc::make_mut(subjects);
        if let Err(index) = subjects.binary_search(&quad.subject) {
            subjects.insert(index, quad.subject);
        }
        let entries = self
            .by_object
            .entry(quad.object)
            .or_default()
            .entry(quad.graph)
            .or_default();
        Arc::make_mut(entries).insert((quad.subject, quad.predicate));
        if is_new {
            *self
                .predicate_object_counts
                .entry((quad.predicate, quad.object))
                .or_default() += 1;
            *self.predicate_counts.entry(quad.predicate).or_default() += 1;
            *self
                .predicate_subject_term_counts
                .entry(quad.predicate)
                .or_default()
                .entry(quad.subject)
                .or_default() += 1;
            *self
                .predicate_object_term_counts
                .entry(quad.predicate)
                .or_default()
                .entry(quad.object)
                .or_default() += 1;
            *self.object_counts.entry(quad.object).or_default() += 1;
            *self
                .predicate_graph_counts
                .entry(quad.predicate)
                .or_default()
                .entry(quad.graph)
                .or_default() += 1;
            self.total_quads += 1;
        }
    }

    fn remove_quad(&mut self, quad: EncodedQuad) {
        let mut was_present = false;
        if let Entry::Occupied(mut entry) = self.by_subject.entry(quad.subject) {
            was_present = entry
                .get_mut()
                .remove(&(quad.predicate, quad.object, quad.graph));
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        if was_present {
            if let Entry::Occupied(mut count) = self
                .predicate_object_counts
                .entry((quad.predicate, quad.object))
            {
                *count.get_mut() = count.get().saturating_sub(1);
                if *count.get() == 0 {
                    count.remove();
                }
            }
            if let Entry::Occupied(mut count) = self.predicate_counts.entry(quad.predicate) {
                *count.get_mut() = count.get().saturating_sub(1);
                if *count.get() == 0 {
                    count.remove();
                }
            }
            decrement_nested_count(
                &mut self.predicate_subject_term_counts,
                quad.predicate,
                quad.subject,
            );
            decrement_nested_count(
                &mut self.predicate_object_term_counts,
                quad.predicate,
                quad.object,
            );
            if let Entry::Occupied(mut count) = self.object_counts.entry(quad.object) {
                *count.get_mut() = count.get().saturating_sub(1);
                if *count.get() == 0 {
                    count.remove();
                }
            }
            if let Entry::Occupied(mut graphs) = self.predicate_graph_counts.entry(quad.predicate) {
                if let Entry::Occupied(mut count) = graphs.get_mut().entry(quad.graph) {
                    *count.get_mut() = count.get().saturating_sub(1);
                    if *count.get() == 0 {
                        count.remove();
                    }
                }
                if graphs.get().is_empty() {
                    graphs.remove();
                }
            }
            self.total_quads = self.total_quads.saturating_sub(1);
        }

        if let Entry::Occupied(mut entry) = self
            .by_predicate_object
            .entry((quad.predicate, quad.object))
        {
            if let Entry::Occupied(mut graphs) = entry.get_mut().entry(quad.graph) {
                let subjects = Arc::make_mut(graphs.get_mut());
                if let Ok(index) = subjects.binary_search(&quad.subject) {
                    subjects.remove(index);
                }
                if subjects.is_empty() {
                    graphs.remove();
                }
            }
            if entry.get().is_empty() {
                entry.remove();
            }
        }

        if let Entry::Occupied(mut entry) = self.by_object.entry(quad.object) {
            if let Entry::Occupied(mut graphs) = entry.get_mut().entry(quad.graph) {
                let empty = {
                    let entries = Arc::make_mut(graphs.get_mut());
                    entries.remove(&(quad.subject, quad.predicate));
                    entries.is_empty()
                };
                if empty {
                    graphs.remove();
                }
            }
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }
}

fn decrement_nested_count(
    counts: &mut HashMap<TermId, HashMap<TermId, usize>>,
    outer: TermId,
    inner: TermId,
) {
    if let Entry::Occupied(mut outer_entry) = counts.entry(outer) {
        if let Entry::Occupied(mut count) = outer_entry.get_mut().entry(inner) {
            *count.get_mut() = count.get().saturating_sub(1);
            if *count.get() == 0 {
                count.remove();
            }
        }
        if outer_entry.get().is_empty() {
            outer_entry.remove();
        }
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
    qv1_gpos: Keyspace,
    qv1_spog: Keyspace,
    qv1_posg: Keyspace,
    qv1_meta: Keyspace,
    /// Guards first-write-wins term interning, sharded by term id.
    term_locks: Vec<Mutex<()>>,
    /// Guards whole read→write→commit cycles of one graph's CRDT state; see
    /// [`GraphStore::graph_commit_guard`].
    commit_locks: Vec<Mutex<()>>,
    indexes: RwLock<IndexState>,
    /// Memory mirror of the persisted `'O'` records; always carries the clock
    /// tag so a reader can tell a fresh entry from a stale one.
    diagnostics_cache: RwLock<HashMap<TermId, StoredDiagnostics>>,
    /// Global term-id → term cache. Term ids are content hashes, so an entry
    /// can never become wrong; the map is bounded by clearing at
    /// [`TERM_DECODE_CACHE_CAP`].
    term_decode_cache: RwLock<HashMap<TermId, Arc<EncodedTerm>>>,
    /// Set by a test to stall between the durable commit and the index apply,
    /// widening a window that is otherwise microseconds wide.
    #[cfg(test)]
    commit_stall: Mutex<Option<std::time::Duration>>,
    /// True while the test-only post-durable commit stall holds `indexes`
    /// write-locked.
    #[cfg(test)]
    commit_stalled: std::sync::atomic::AtomicBool,
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
    /// **Lock order: innermost.** Take it last — after the graph commit guard
    /// and after `indexes` — hold it only across the queue read plus the commit
    /// that acts on it, and take no other `GraphStore` lock while it is held.
    fts_queue_lock: Mutex<()>,
    dirty_counter: AtomicU64,
    /// How many times this store instance has recomputed graph diagnostics.
    /// Tests use it to prove a reopen served the persisted record instead of
    /// recomputing, and that a stale record was repaired at open.
    diagnostics_computed: AtomicU64,
    /// Metadata point reads performed by the O(1) qv1 admission gate.
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
    let failure_len = u16::from_be_bytes(
        bytes[52..54]
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
    })
}

fn query_index_term_at(bytes: &[u8], offset: usize) -> TermId {
    TermId::from_be_bytes(
        bytes[offset..offset + 16]
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
        Some(QUERY_INDEX_GRAPH_COUNT_TAG) if bytes.len() == 17 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::Graph(query_index_term_at(
                bytes, 1,
            )))
        }
        Some(QUERY_INDEX_PREDICATE_COUNT_TAG) if bytes.len() == 17 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::Predicate(query_index_term_at(
                bytes, 1,
            )))
        }
        Some(QUERY_INDEX_PREDICATE_MUTATION_EPOCH_TAG) if bytes.len() == 17 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::PredicateMutationEpoch(
                query_index_term_at(bytes, 1),
            ))
        }
        Some(QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG) if bytes.len() == 33 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::GraphPredicate(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 17),
            ))
        }
        Some(QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG) if bytes.len() == 33 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::PredicateObject(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 17),
            ))
        }
        Some(QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG) if bytes.len() == 49 => {
            QueryIndexCounterKeyRead::Counter(QueryIndexCounterKey::GraphPredicateObject(
                query_index_term_at(bytes, 1),
                query_index_term_at(bytes, 17),
                query_index_term_at(bytes, 33),
            ))
        }
        Some(
            QUERY_INDEX_GRAPH_COUNT_TAG
            | QUERY_INDEX_PREDICATE_COUNT_TAG
            | QUERY_INDEX_PREDICATE_MUTATION_EPOCH_TAG
            | QUERY_INDEX_GRAPH_PREDICATE_COUNT_TAG
            | QUERY_INDEX_PREDICATE_OBJECT_COUNT_TAG
            | QUERY_INDEX_GRAPH_PREDICATE_OBJECT_COUNT_TAG,
        ) => QueryIndexCounterKeyRead::InvalidLength,
        Some(_) => QueryIndexCounterKeyRead::UnknownTag,
        None => QueryIndexCounterKeyRead::InvalidLength,
    }
}

fn query_index_key(parts: [TermId; 4]) -> QuadKey {
    let mut key = [0u8; 64];
    for (index, term) in parts.into_iter().enumerate() {
        key[index * 16..(index + 1) * 16].copy_from_slice(&term.to_be_bytes());
    }
    key
}

fn query_index_prefix(parts: &[TermId]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(parts.len() * 16);
    for term in parts {
        prefix.extend_from_slice(&term.to_be_bytes());
    }
    prefix
}

fn qv1_gpos_key(quad: EncodedQuad) -> QuadKey {
    query_index_key([quad.graph, quad.predicate, quad.object, quad.subject])
}

fn qv1_spog_key(quad: EncodedQuad) -> QuadKey {
    query_index_key([quad.subject, quad.predicate, quad.object, quad.graph])
}

fn qv1_posg_key(quad: EncodedQuad) -> QuadKey {
    query_index_key([quad.predicate, quad.object, quad.subject, quad.graph])
}

fn decode_qv1_gpos_key(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        graph: query_index_term_at(bytes, 0),
        predicate: query_index_term_at(bytes, 16),
        object: query_index_term_at(bytes, 32),
        subject: query_index_term_at(bytes, 48),
    })
}

fn decode_qv1_spog_key(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        subject: query_index_term_at(bytes, 0),
        predicate: query_index_term_at(bytes, 16),
        object: query_index_term_at(bytes, 32),
        graph: query_index_term_at(bytes, 48),
    })
}

fn decode_qv1_posg_key(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        predicate: query_index_term_at(bytes, 0),
        object: query_index_term_at(bytes, 16),
        subject: query_index_term_at(bytes, 32),
        graph: query_index_term_at(bytes, 48),
    })
}

fn decode_source_quad_key(bytes: &[u8]) -> Option<EncodedQuad> {
    (bytes.len() == 64).then(|| EncodedQuad {
        graph: query_index_term_at(bytes, 0),
        subject: query_index_term_at(bytes, 16),
        predicate: query_index_term_at(bytes, 32),
        object: query_index_term_at(bytes, 48),
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

fn query_index_live_counter_keys(quad: EncodedQuad) -> [QueryIndexCounterKey; 6] {
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
    Gpos,
    Spog,
    Posg,
}

/// The physical order selected for one trusted qv1 range. This remains
/// crate-private so query readers never learn Fjall keyspace details.
#[derive(Clone, Copy)]
pub(crate) enum QueryIndexCursorOrder {
    Gpos,
    Spog,
    Posg,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryIndexAdmission {
    pub(crate) trusted: bool,
    pub(crate) fallback_reason: Option<&'static str>,
    pub(crate) header_reads: u64,
    pub(crate) counter_reads: u64,
}

/// One immutable, publication-coherent durable read view.
///
/// It deliberately owns only the Fjall snapshot. Callers receive opaque
/// cursor and metadata operations rather than Fjall objects or keyspaces.
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
    ) -> crate::query_cursor::RawQuadCursor {
        let (keyspace, prefix) = store.query_index_range(order, pattern);
        crate::query_cursor::RawQuadCursor::query_index(
            self.snapshot.clone(),
            keyspace,
            order,
            prefix,
        )
    }

    pub(crate) fn query_index_admission(&self, store: &GraphStore) -> Result<QueryIndexAdmission> {
        store.query_index_snapshot_admission(&self.snapshot)
    }

    pub(crate) fn contains_graph_by_id(&self, store: &GraphStore, graph: TermId) -> Result<bool> {
        Ok(self
            .snapshot
            .get(&store.graphs, graph_meta_key(graph))?
            .is_some())
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
        GraphCommitGuard(
            self.commit_locks[shard]
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
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
            match snapshot.get(&self.qv1_meta, QUERY_INDEX_HEADER_KEY)? {
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
            &self.qv1_meta,
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
            well_formed &= key.as_ref().len() == 64 && value.as_ref().is_empty();
        }
        Ok((rows, well_formed))
    }

    fn query_index_keyspaces_are_empty(&self, snapshot: &Snapshot) -> Result<bool> {
        for keyspace in [
            &self.qv1_gpos,
            &self.qv1_spog,
            &self.qv1_posg,
            &self.qv1_meta,
        ] {
            if let Some(guard) = snapshot.iter(keyspace).next() {
                let _ = guard.into_inner()?;
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Captures one durable snapshot only after a concurrent commit has made
    /// both its Fjall batch and in-memory publication visible. The returned
    /// object never retains this lock across cursor iteration.
    pub(crate) fn read_snapshot(&self) -> StoreReadSnapshot {
        let _publication = self.indexes_read();
        StoreReadSnapshot {
            snapshot: self.db.snapshot(),
        }
    }

    fn query_index_snapshot(&self) -> Snapshot {
        self.read_snapshot().snapshot
    }

    /// O(1) qv1 eligibility gate for a single execution snapshot. Full source
    /// and qv cross-checking belongs to open-time verification and explicit
    /// maintenance checks; doing it here would erase the index's query value.
    fn query_index_snapshot_admission(&self, snapshot: &Snapshot) -> Result<QueryIndexAdmission> {
        #[cfg(test)]
        self.query_index_admission_probes
            .fetch_add(1, Ordering::Relaxed);
        let header = match self.query_index_header_from_snapshot(snapshot)? {
            QueryIndexHeaderRead::Absent => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    fallback_reason: Some("metadata-missing"),
                    header_reads: 1,
                    counter_reads: 0,
                });
            }
            QueryIndexHeaderRead::Malformed => {
                return Ok(QueryIndexAdmission {
                    trusted: false,
                    fallback_reason: Some("metadata-malformed"),
                    header_reads: 1,
                    counter_reads: 0,
                });
            }
            QueryIndexHeaderRead::Valid(header) => header,
        };
        self.query_index_admission_for_header(snapshot, &header)
    }

    fn query_index_admission_for_header(
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
            fallback_reason,
            header_reads: 1,
            counter_reads: 1,
        })
    }

    fn query_index_range(
        &self,
        order: QueryIndexCursorOrder,
        pattern: crate::rdf_read::QuadPattern,
    ) -> (&Keyspace, Vec<u8>) {
        let terms = match order {
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
        };
        let keyspace = match order {
            QueryIndexCursorOrder::Gpos => &self.qv1_gpos,
            QueryIndexCursorOrder::Spog => &self.qv1_spog,
            QueryIndexCursorOrder::Posg => &self.qv1_posg,
        };
        (keyspace, query_index_prefix(&terms))
    }

    pub(crate) fn query_index_status(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let snapshot_sequence = snapshot.seqno();
        let header = self.query_index_header_from_snapshot(&snapshot)?;
        let source_live_quads = self.count_live_source_rows(&snapshot)?;
        let (indexed_quads, gpos_well_formed) =
            self.summarize_qv_rows(&snapshot, &self.qv1_gpos)?;
        let (spog_quads, spog_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv1_spog)?;
        let (posg_quads, posg_well_formed) = self.summarize_qv_rows(&snapshot, &self.qv1_posg)?;
        let (state, last_build_sequence) = match header {
            QueryIndexHeaderRead::Absent => (QueryIndexState::Missing, 0),
            QueryIndexHeaderRead::Malformed => {
                (QueryIndexState::Failed("metadata-malformed".to_owned()), 0)
            }
            QueryIndexHeaderRead::Valid(header) => {
                let total_matches_header = matches!(
                    self.query_index_counter_from_snapshot(&snapshot, QueryIndexCounterKey::Total)?,
                    QueryIndexCounterRead::Value(total) if total == header.indexed_quads
                );
                let ready_matches_snapshot = header.ready_is_coherent()
                    && header.is_not_ahead_of_snapshot(snapshot_sequence)
                    && header.source_live_quads == source_live_quads
                    && header.indexed_quads == indexed_quads
                    && indexed_quads == spog_quads
                    && indexed_quads == posg_quads
                    && gpos_well_formed
                    && spog_well_formed
                    && posg_well_formed
                    && total_matches_header;
                if matches!(header.state, StoredQueryIndexState::Ready) && !ready_matches_snapshot {
                    (
                        QueryIndexState::Failed("ready-status-mismatch".to_owned()),
                        header.last_build_sequence,
                    )
                } else {
                    (header.state(), header.last_build_sequence)
                }
            }
        };
        Ok(QueryIndexStatus {
            schema_version: QUERY_INDEX_SCHEMA_VERSION,
            state,
            source_live_quads,
            indexed_quads,
            last_build_sequence,
        })
    }

    pub(crate) fn query_index_status_fast(&self) -> Result<QueryIndexStatus> {
        let snapshot = self.query_index_snapshot();
        let (state, source_live_quads, indexed_quads, last_build_sequence) = match self
            .query_index_header_from_snapshot(&snapshot)?
        {
            QueryIndexHeaderRead::Absent => (QueryIndexState::Missing, 0, 0, 0),
            QueryIndexHeaderRead::Malformed => (
                QueryIndexState::Failed("metadata-malformed".to_owned()),
                0,
                0,
                0,
            ),
            QueryIndexHeaderRead::Valid(header) => {
                #[cfg(test)]
                self.query_index_admission_probes
                    .fetch_add(1, Ordering::Relaxed);
                let admission = self.query_index_admission_for_header(&snapshot, &header)?;
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
                    header.source_live_quads,
                    header.indexed_quads,
                    header.last_build_sequence,
                )
            }
        };
        Ok(QueryIndexStatus {
            schema_version: QUERY_INDEX_SCHEMA_VERSION,
            state,
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
        let _indexes = self.indexes_write();
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
                batch.insert(&self.qv1_meta, QUERY_INDEX_TOTAL_KEY, 0u64.to_be_bytes());
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
                let admission = self.query_index_snapshot_admission(&snapshot)?;
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
        key: QuadKey,
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
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv1_gpos, qv1_gpos_key(quad))? {
                report.problem("source-gpos-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv1_spog, qv1_spog_key(quad))? {
                report.problem("source-spog-missing-or-nonempty");
            }
            if !self.qv_row_is_present_and_empty(snapshot, &self.qv1_posg, qv1_posg_key(quad))? {
                report.problem("source-posg-missing-or-nonempty");
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
            QueryIndexKeyOrder::Gpos => (
                &self.qv1_gpos,
                "qv-gpos-key-length",
                "qv-gpos-value-nonempty",
                "qv-gpos-source-missing",
            ),
            QueryIndexKeyOrder::Spog => (
                &self.qv1_spog,
                "qv-spog-key-length",
                "qv-spog-value-nonempty",
                "qv-spog-source-missing",
            ),
            QueryIndexKeyOrder::Posg => (
                &self.qv1_posg,
                "qv-posg-key-length",
                "qv-posg-value-nonempty",
                "qv-posg-source-missing",
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
                QueryIndexKeyOrder::Gpos => decode_qv1_gpos_key(key.as_ref()),
                QueryIndexKeyOrder::Spog => decode_qv1_spog_key(key.as_ref()),
                QueryIndexKeyOrder::Posg => decode_qv1_posg_key(key.as_ref()),
            };
            let Some(quad) = quad else {
                report.problem(key_problem);
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
        match snapshot.get(&self.qv1_meta, key.bytes())? {
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
        terms: [TermId; 3],
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
        let mut current = None::<[TermId; 3]>;
        let mut count = 0u64;
        for guard in snapshot.iter(&self.qv1_gpos) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_qv1_gpos_key(key.as_ref()) else {
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

    fn verify_predicate_mutation_epoch(
        &self,
        snapshot: &Snapshot,
        predicate: TermId,
        source_epoch: Option<u64>,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let Some(source_epoch) = source_epoch else {
            report.problem("mutation-epoch-without-header");
            return Ok(());
        };
        match snapshot.get(
            &self.qv1_meta,
            QueryIndexCounterKey::PredicateMutationEpoch(predicate).bytes(),
        )? {
            None => report.problem("mutation-epoch-missing"),
            Some(value) => match decode_query_index_u64(value.as_ref()) {
                Some(epoch) if epoch != 0 && epoch <= source_epoch => {}
                _ => report.problem("mutation-epoch-invalid"),
            },
        }
        Ok(())
    }

    fn verify_posg_counter_group(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        terms: [TermId; 2],
        expected: u64,
        source_epoch: Option<u64>,
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
        )?;
        if dimension == 1 {
            self.verify_predicate_mutation_epoch(snapshot, terms[0], source_epoch, report)?;
        }
        Ok(())
    }

    fn verify_posg_counter_dimension(
        &self,
        snapshot: &Snapshot,
        dimension: usize,
        source_epoch: Option<u64>,
        report: &mut QueryIndexVerificationBuilder,
    ) -> Result<()> {
        let mut current = None::<[TermId; 2]>;
        let mut count = 0u64;
        for guard in snapshot.iter(&self.qv1_posg) {
            let (key, _) = guard.into_inner()?;
            let Some(quad) = decode_qv1_posg_key(key.as_ref()) else {
                continue;
            };
            let terms = [quad.predicate, quad.object];
            if let Some(previous) = current
                && previous[..dimension] != terms[..dimension]
            {
                self.verify_posg_counter_group(
                    snapshot,
                    dimension,
                    previous,
                    count,
                    source_epoch,
                    report,
                )?;
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
            self.verify_posg_counter_group(
                snapshot,
                dimension,
                previous,
                count,
                source_epoch,
                report,
            )?;
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
                snapshot.prefix(&self.qv1_gpos, query_index_prefix(&[graph]))
            }
            QueryIndexCounterKey::Predicate(predicate)
            | QueryIndexCounterKey::PredicateMutationEpoch(predicate) => {
                snapshot.prefix(&self.qv1_posg, query_index_prefix(&[predicate]))
            }
            QueryIndexCounterKey::GraphPredicate(graph, predicate) => {
                snapshot.prefix(&self.qv1_gpos, query_index_prefix(&[graph, predicate]))
            }
            QueryIndexCounterKey::PredicateObject(predicate, object) => {
                snapshot.prefix(&self.qv1_posg, query_index_prefix(&[predicate, object]))
            }
            QueryIndexCounterKey::GraphPredicateObject(graph, predicate, object) => snapshot
                .prefix(
                    &self.qv1_gpos,
                    query_index_prefix(&[graph, predicate, object]),
                ),
            QueryIndexCounterKey::Total => return Ok(true),
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
        for guard in snapshot.iter(&self.qv1_meta) {
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
                        QueryIndexCounterKey::PredicateMutationEpoch(_) => {
                            let source_epoch = header.map(|header| header.source_epoch);
                            if value == 0 || source_epoch.is_none() || value > source_epoch.unwrap()
                            {
                                report.problem("mutation-epoch-invalid");
                            }
                            if !self.query_index_counter_has_rows(snapshot, counter)? {
                                report.problem("meta-counter-orphan");
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
        let gpos_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Gpos, full, &mut report)?;
        let spog_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Spog, full, &mut report)?;
        let posg_rows =
            self.verify_qv_rows(snapshot, QueryIndexKeyOrder::Posg, full, &mut report)?;
        report.report.indexed_quads = gpos_rows;
        if gpos_rows != spog_rows || gpos_rows != posg_rows {
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
            let source_epoch = header.map(|header| header.source_epoch);
            self.verify_posg_counter_dimension(snapshot, 1, source_epoch, &mut report)?;
            self.verify_posg_counter_dimension(snapshot, 2, source_epoch, &mut report)?;
            self.verify_query_index_meta_records(snapshot, header, &mut report)?;
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

    fn build_indexes(&self) -> Result<IndexState> {
        let mut indexes = IndexState::default();
        for guard in self.quads.iter() {
            let (key, value) = guard.into_inner()?;
            if key.len() != 64 {
                continue;
            }
            if dot_payload_is_empty(value.as_ref()) {
                continue;
            }
            indexes.insert_quad(Self::decode_quad_key(key.as_ref())?);
        }
        Ok(indexes)
    }

    /// Rebuild every derived structure from the durable `quads` keyspace, the
    /// source of truth. The clock mirror is kept: those commits did land.
    ///
    /// Scan and install share the write lock, so no commit can publish between
    /// them and be erased. The scan takes no other lock, so it cannot deadlock.
    fn rebuild_indexes(&self) -> Result<()> {
        {
            let mut indexes = self.indexes_write();
            let rebuilt = self.build_indexes()?;
            #[cfg(test)]
            self.stall_in_rebuild();
            indexes.graph_subjects = rebuilt.graph_subjects;
            indexes.by_graph_subject = rebuilt.by_graph_subject;
            indexes.derived = None;
            indexes.object_order.clear();
        }
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
        self.clear_query_index_keyspace(&self.qv1_gpos, false)?;
        self.clear_query_index_keyspace(&self.qv1_spog, false)?;
        self.clear_query_index_keyspace(&self.qv1_posg, false)?;
        self.clear_query_index_keyspace(&self.qv1_meta, true)
    }

    fn build_query_index_chunk(&self, quads: &[EncodedQuad], source_epoch: u64) -> Result<()> {
        let mut increments = BTreeMap::<Vec<u8>, (QueryIndexCounterKey, u64)>::new();
        let mut predicates = BTreeSet::new();
        let mut batch = self.buffered_batch();
        for quad in quads {
            batch.insert(&self.qv1_gpos, qv1_gpos_key(*quad), Vec::<u8>::new());
            batch.insert(&self.qv1_spog, qv1_spog_key(*quad), Vec::<u8>::new());
            batch.insert(&self.qv1_posg, qv1_posg_key(*quad), Vec::<u8>::new());
            predicates.insert(quad.predicate);
            for counter in query_index_live_counter_keys(*quad) {
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
            let current = match self.qv1_meta.get(counter.bytes())? {
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
            batch.insert(&self.qv1_meta, counter.bytes(), next.to_be_bytes());
        }
        for predicate in predicates {
            batch.insert(
                &self.qv1_meta,
                QueryIndexCounterKey::PredicateMutationEpoch(predicate).bytes(),
                source_epoch.to_be_bytes(),
            );
        }
        self.commit_fjall_batch(batch)
    }

    fn build_query_index_rows(&self, snapshot: &Snapshot, source_epoch: u64) -> Result<u64> {
        let mut rows = 0u64;
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
                self.build_query_index_chunk(&chunk, source_epoch)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            self.build_query_index_chunk(&chunk, source_epoch)?;
        }
        Ok(rows)
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
        let _indexes = self.indexes_write();
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
            let source_live_quads = self.build_query_index_rows(&source_snapshot, source_epoch)?;
            let candidate = QueryIndexHeader {
                state: StoredQueryIndexState::Building,
                source_epoch,
                index_epoch: source_epoch,
                source_live_quads,
                indexed_quads: source_live_quads,
                last_build_sequence,
            };
            {
                let mut batch = self.buffered_batch();
                batch.insert(
                    &self.qv1_meta,
                    QUERY_INDEX_TOTAL_KEY,
                    source_live_quads.to_be_bytes(),
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
        result
    }

    /// Commit a batch and publish its in-memory half.
    ///
    /// An [`IndexApply::Anomaly`] means the index drifted from the store, so it
    /// is rebuilt before this returns rather than left inconsistent.
    fn apply_commit(&self, commit: DurableCommit, publish: PendingPublish) -> Result<()> {
        if publish.is_empty() {
            return self.commit_durable(commit);
        }

        if self.commit_with_index(commit, &publish)? {
            tracing::warn!(
                "index anomaly detected while applying a commit; rebuilding indexes from the store"
            );
            return self.rebuild_indexes();
        }
        Ok(())
    }

    /// Stall inside the publish window. Test-only.
    #[cfg(test)]
    fn stall_after_commit(&self) {
        let stall = *self
            .commit_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(delay) = stall {
            self.commit_stalled.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
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
    }

    /// Whether a commit is inside its post-durable publication stall.
    #[cfg(test)]
    pub(crate) fn commit_stalled(&self) -> bool {
        self.commit_stalled.load(Ordering::SeqCst)
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
        Ok(match snapshot.get(&self.qv1_meta, key.bytes())? {
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

        for transition in &transitions {
            for (keyspace, key) in [
                (&self.qv1_gpos, qv1_gpos_key(transition.quad)),
                (&self.qv1_spog, qv1_spog_key(transition.quad)),
                (&self.qv1_posg, qv1_posg_key(transition.quad)),
            ] {
                let current = snapshot.get(keyspace, key)?;
                let expected = if transition.is_live {
                    current.is_none()
                } else {
                    current.is_some_and(|value| value.as_ref().is_empty())
                };
                if !expected {
                    return Ok(None);
                }
            }
        }

        if transitions.is_empty() {
            return Ok(Some(QueryIndexMaintenancePlan {
                transitions,
                counters: Vec::new(),
                header: None,
            }));
        }

        let mut deltas = BTreeMap::<Vec<u8>, (QueryIndexCounterKey, i128)>::new();
        let mut touched_predicates = BTreeSet::new();
        for transition in &transitions {
            let delta = if transition.is_live { 1 } else { -1 };
            touched_predicates.insert(transition.quad.predicate);
            for counter in query_index_live_counter_keys(transition.quad) {
                let entry = deltas.entry(counter.bytes()).or_insert((counter, 0));
                let Some(next) = entry.1.checked_add(delta) else {
                    return Ok(None);
                };
                entry.1 = next;
            }
        }

        let mut current_values = BTreeMap::<Vec<u8>, u64>::new();
        let mut counters = Vec::with_capacity(deltas.len() + touched_predicates.len());
        for (bytes, (counter, delta)) in &deltas {
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
            current_values.insert(bytes.clone(), current);
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

        for predicate in touched_predicates {
            let predicate_counter = QueryIndexCounterKey::Predicate(predicate);
            let predicate_bytes = predicate_counter.bytes();
            let Some(previous_predicate_rows) = current_values.get(&predicate_bytes).copied()
            else {
                return Ok(None);
            };
            match self.query_index_counter_from_snapshot(
                snapshot,
                QueryIndexCounterKey::PredicateMutationEpoch(predicate),
            )? {
                QueryIndexCounterRead::Value(epoch)
                    if previous_predicate_rows != 0
                        && epoch != 0
                        && epoch <= header.source_epoch => {}
                QueryIndexCounterRead::Missing if previous_predicate_rows == 0 => {}
                QueryIndexCounterRead::Missing
                | QueryIndexCounterRead::Malformed
                | QueryIndexCounterRead::Value(_) => return Ok(None),
            }
            let Some(predicate_update) = counters
                .iter()
                .find(|update| update.key == predicate_counter)
            else {
                return Ok(None);
            };
            let next_predicate_rows = predicate_update.value.unwrap_or(0);
            counters.push(QueryIndexCounterUpdate {
                key: QueryIndexCounterKey::PredicateMutationEpoch(predicate),
                value: (next_predicate_rows != 0).then_some(source_epoch),
            });
        }

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
            transitions,
            counters,
            header: Some(QueryIndexHeader {
                state: StoredQueryIndexState::Ready,
                source_epoch,
                index_epoch: source_epoch,
                source_live_quads,
                indexed_quads,
                last_build_sequence: header.last_build_sequence,
            }),
        }))
    }

    fn stage_query_index_maintenance_plan(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        plan: QueryIndexMaintenancePlan,
    ) {
        for transition in plan.transitions {
            let keys = [
                (&self.qv1_gpos, qv1_gpos_key(transition.quad)),
                (&self.qv1_spog, qv1_spog_key(transition.quad)),
                (&self.qv1_posg, qv1_posg_key(transition.quad)),
            ];
            for (keyspace, key) in keys {
                if transition.is_live {
                    batch.insert(keyspace, key, Vec::<u8>::new());
                } else {
                    batch.remove(keyspace, key);
                }
            }
        }
        for update in plan.counters {
            match update.value {
                Some(value) => {
                    batch.insert(&self.qv1_meta, update.key.bytes(), value.to_be_bytes())
                }
                None => batch.remove(&self.qv1_meta, update.key.bytes()),
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

    /// Publish the durable batch and the index together, reporting anomalies.
    /// A reader past the new clock never sees state predating it.
    ///
    /// Only the queue lock nests inside, and the fjall reads made here take no
    /// further lock, so the section cannot deadlock.
    fn commit_with_index(
        &self,
        mut commit: DurableCommit,
        publish: &PendingPublish,
    ) -> Result<bool> {
        let mut indexes = self.indexes_write();
        self.stage_query_index_maintenance(&mut commit.batch, publish)?;
        self.commit_durable(commit)?;
        #[cfg(test)]
        self.stall_after_commit();
        Ok(indexes.publish(publish))
    }

    fn build_derived_indexes(indexes: &IndexState) -> DerivedIndexState {
        let mut derived = DerivedIndexState::default();
        for (&(graph, subject), entries) in &indexes.by_graph_subject {
            for &(predicate, object) in entries {
                derived.insert_quad(EncodedQuad {
                    graph,
                    subject,
                    predicate,
                    object,
                });
            }
        }
        derived
    }

    pub fn ensure_derived_indexes(&self) {
        self.with_derived_indexes(|_| ());
    }

    /// Runs `f` under the index lock, so `f` must not call back into the store:
    /// any store lock it takes self-deadlocks.
    fn with_derived_indexes<R>(&self, f: impl FnOnce(&DerivedIndexState) -> R) -> R {
        {
            let indexes = self.indexes_read();
            if let Some(derived) = indexes.derived.as_ref() {
                return f(derived);
            }
        }

        let mut indexes = self.indexes_write();
        if indexes.derived.is_none() {
            let derived = Self::build_derived_indexes(&indexes);
            indexes.derived = Some(derived);
        }
        f(indexes
            .derived
            .as_ref()
            .expect("derived indexes initialized"))
    }

    /// Diagnostics recomputations performed by this store instance.
    #[cfg(test)]
    pub(crate) fn diagnostics_compute_count(&self) -> u64 {
        self.diagnostics_computed.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn query_index_admission_probe_count(&self) -> u64 {
        self.query_index_admission_probes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn query_index_verification_run_count(&self) -> u64 {
        self.query_index_verification_runs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn fail_query_indexes_for_test(&self) {
        let _indexes = self.indexes_write();
        let snapshot = self.db.snapshot();
        let previous = match self.query_index_header_from_snapshot(&snapshot).unwrap() {
            QueryIndexHeaderRead::Valid(header) => Some(header),
            QueryIndexHeaderRead::Absent | QueryIndexHeaderRead::Malformed => None,
        };
        let mut batch = self.buffered_batch();
        self.stage_query_index_failed(&mut batch, previous.as_ref(), "test-failure");
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
    /// against the in-memory index: nothing is decoded, so the cost is a handful
    /// of integer comparisons per stored triple instead of three `String` clones
    /// plus hashing of full IRIs. The rule is the specification and the two are
    /// cross-checked on generated graph shapes by
    /// `orphan_ids_match`; recomputation is on the hot path of
    /// every write that defers its diagnostics refresh, where the decoding
    /// version cost 74ms on a 10,000-entity crate.
    ///
    /// The crate root is the graph term itself, so its term id *is* `graph_id`.
    fn orphaned_entity_ids(&self, graph_id: TermId, vocab: &OrphanVocab) -> HashSet<TermId> {
        let mut data_entities: HashSet<TermId> = HashSet::new();
        let mut adjacency: HashMap<TermId, Vec<TermId>> = HashMap::new();
        {
            // Guards IndexState for the single pass over the graph's triples.
            let indexes = self.indexes_read();
            let Some(subjects) = indexes.graph_subjects.get(&graph_id) else {
                return HashSet::new();
            };
            for &subject in subjects {
                let Some(entries) = indexes.by_graph_subject.get(&(graph_id, subject)) else {
                    continue;
                };
                for &(predicate, object) in entries {
                    if vocab.has_part == Some(predicate) {
                        adjacency.entry(subject).or_default().push(object);
                        if subject != graph_id {
                            data_entities.insert(subject);
                        }
                        if object != graph_id {
                            data_entities.insert(object);
                        }
                    }
                    if vocab.rdf_type == Some(predicate)
                        && subject != graph_id
                        && vocab.data_types.contains(&Some(object))
                    {
                        data_entities.insert(subject);
                    }
                }
            }
        }

        if data_entities.is_empty() {
            return HashSet::new();
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
        data_entities
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
        let orphans = self.orphaned_entity_ids(graph_id, &self.orphan_vocab()?);
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
    ) -> Vec<EncodedQuad> {
        let indexes = self.indexes_read();
        let Some(subjects) = indexes.graph_subjects.get(&graph) else {
            return Vec::new();
        };

        let mut quads = Vec::new();
        for &subject in subjects {
            let Some(entries) = indexes.by_graph_subject.get(&(graph, subject)) else {
                continue;
            };
            for &(candidate_predicate, candidate_object) in entries {
                if predicate.is_some_and(|expected| expected != candidate_predicate) {
                    continue;
                }
                if object.is_some_and(|expected| expected != candidate_object) {
                    continue;
                }
                quads.push(EncodedQuad {
                    graph,
                    subject,
                    predicate: candidate_predicate,
                    object: candidate_object,
                });
            }
        }
        quads
    }

    /// Approximate corpus-wide quad counts used by the query planner. All are
    /// O(1) reads against the lazily built derived indexes; values count quad
    /// instances per graph (no cross-graph triple dedup), which is good
    /// enough for relative selectivity ordering.
    pub(crate) fn stat_predicate_object_count(&self, predicate: TermId, object: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes
                .predicate_object_counts
                .get(&(predicate, object))
                .copied()
                .unwrap_or(0)
        })
    }

    pub(crate) fn stat_predicate_count(&self, predicate: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes
                .predicate_counts
                .get(&predicate)
                .copied()
                .unwrap_or(0)
        })
    }

    pub(crate) fn stat_predicate_distinct_subject_count(&self, predicate: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes
                .predicate_subject_term_counts
                .get(&predicate)
                .map(HashMap::len)
                .unwrap_or(0)
        })
    }

    pub(crate) fn stat_predicate_distinct_object_count(&self, predicate: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes
                .predicate_object_term_counts
                .get(&predicate)
                .map(HashMap::len)
                .unwrap_or(0)
        })
    }

    pub(crate) fn stat_object_count(&self, object: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes.object_counts.get(&object).copied().unwrap_or(0)
        })
    }

    pub(crate) fn stat_subject_count(&self, subject: TermId) -> usize {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_subject
                .get(&subject)
                .map(HashSet::len)
                .unwrap_or(0)
        })
    }

    pub(crate) fn stat_distinct_subject_count(&self) -> usize {
        self.with_derived_indexes(|indexes| indexes.by_subject.len())
    }

    pub(crate) fn stat_distinct_object_count(&self) -> usize {
        self.with_derived_indexes(|indexes| indexes.object_counts.len())
    }

    pub(crate) fn stat_total_quads(&self) -> usize {
        self.with_derived_indexes(|indexes| indexes.total_quads)
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
    ) -> Result<EncodedQuad> {
        let quad = match order {
            QueryIndexCursorOrder::Gpos => decode_qv1_gpos_key(bytes),
            QueryIndexCursorOrder::Spog => decode_qv1_spog_key(bytes),
            QueryIndexCursorOrder::Posg => decode_qv1_posg_key(bytes),
        };
        quad.ok_or_else(|| StoreError::InvalidEncoding {
            context: "qv1 query index key",
            message: format!("expected 64 bytes, found {}", bytes.len()),
        })
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

    fn count_objects_for_ids(&self, graph: TermId, subject: TermId, predicate: TermId) -> usize {
        self.indexes
            .read()
            .unwrap()
            .by_graph_subject
            .get(&(graph, subject))
            .map(|entries| {
                entries
                    .iter()
                    .filter(|(candidate_predicate, _)| *candidate_predicate == predicate)
                    .count()
            })
            .unwrap_or(0)
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
        let (generation, object_ids) = {
            let indexes = self.indexes_read();
            if let Some(cached) = indexes.object_order.get(&key) {
                return Ok(cached);
            }
            let object_ids = indexes
                .by_graph_subject
                .get(&(graph, subject))
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|(candidate_predicate, object)| {
                            (*candidate_predicate == predicate).then_some(*object)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (indexes.object_order.generation, object_ids)
        };

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
        self.indexes_write().object_order.install(
            OrderEntry {
                key,
                objects: Arc::clone(&objects),
            },
            generation,
        );
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
            qv1_gpos: db.keyspace("qv1_gpos", write_heavy)?,
            qv1_spog: db.keyspace("qv1_spog", write_heavy)?,
            qv1_posg: db.keyspace("qv1_posg", write_heavy)?,
            qv1_meta: db.keyspace("qv1_meta", point_read_heavy)?,
            db,
            persist_mode,
            term_locks: (0..TERM_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            commit_locks: (0..COMMIT_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            indexes: RwLock::new(IndexState::default()),
            diagnostics_cache: RwLock::new(HashMap::new()),
            term_decode_cache: RwLock::new(HashMap::new()),
            #[cfg(test)]
            commit_stall: Mutex::new(None),
            #[cfg(test)]
            commit_stalled: std::sync::atomic::AtomicBool::new(false),
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

        store.rebuild_indexes()?;
        store.initialize_query_indexes_at_open()?;
        store.restore_dirty_counter()?;
        store.repair_graph_diagnostics_at_open()?;
        Ok(store)
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
            &self.qv1_gpos,
            &self.qv1_spog,
            &self.qv1_posg,
            &self.qv1_meta,
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
    /// can never become stale and needs no invalidation path — the only bound
    /// is the cap, at which the whole map is cleared. Returns an `Arc` so hot
    /// paths (visibility checks, snapshot/fingerprint scans) share one
    /// allocation instead of cloning the string per lookup.
    pub(crate) fn decode_term_arc(&self, id: TermId) -> Result<Arc<EncodedTerm>> {
        if let Some(term) = self
            .term_decode_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
        {
            return Ok(term.clone());
        }

        let term = Arc::new(self.read_term(id)?);
        // Guards the term-id → term cache.
        let mut cache = self
            .term_decode_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if cache.len() >= TERM_DECODE_CACHE_CAP {
            cache.clear();
        }
        Ok(cache.entry(id).or_insert(term).clone())
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

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn delete_graph(&self, graph: &GraphId) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let mut batch = self.new_batch();
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
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
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
        self.indexes_write().object_order.drop_graph(graph_id);
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
        Ok(self.graph_subject_count(graph_id) == 0)
    }

    /// Live subject count for a graph, straight off the in-memory index.
    pub(crate) fn graph_subject_count(&self, graph_id: TermId) -> usize {
        self.indexes_read()
            .graph_subjects
            .get(&graph_id)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    pub fn contains_subject(&self, graph: &GraphId, subject: &EncodedTerm) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(false);
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(false);
        };

        let indexes = self.indexes_read();
        Ok(indexes
            .by_graph_subject
            .contains_key(&(graph_id, subject_id)))
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
    pub fn set_graph_policy(&self, graph: &GraphId, policy: &GraphPolicy) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        let mut batch = self.new_batch();
        let graph_id =
            self.encode_term_internal(Some(&mut batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
        meta.policy = policy.clone().normalized();
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        self.commit(batch)
    }

    pub fn graph_policy(&self, graph: &GraphId) -> Result<GraphPolicy> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphPolicy::default());
        };
        Ok(self
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .policy)
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
    /// while a commit guard is held (see [`GraphCommitGuard`]). In particular
    /// `IrokleGraphSync::ensure_graph_topic` reaches this, so any publish that
    /// may bind a topic must run *before* the caller takes its guard.
    pub fn set_irokle_topic_id(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
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
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
        meta.rocrate_context = context.map(str::to_string);
        meta.rocrate_license = license.map(str::to_string);
        meta.rocrate_license_digest = license_digest;
        meta.context_tag = tag;
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        self.commit(batch)
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

    pub fn graph_tombstoned(&self, graph: &GraphId) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(false);
        };
        Ok(self.graphs.get(graph_tombstone_key(graph_id))?.is_some())
    }

    /// Self-guarding: takes the graph commit guard itself. Must NOT be called
    /// while a commit guard is held (see [`GraphCommitGuard`]).
    pub fn set_graph_tombstone(&self, graph: &GraphId) -> Result<()> {
        let _commit_guard = self.graph_commit_guard(graph);
        let graph_id = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        let mut batch = self.buffered_batch();
        batch.insert(&self.graphs, graph_tombstone_key(graph_id), []);
        batch.commit()?;
        Ok(())
    }

    pub fn graph_snapshot(&self, graph: &GraphId) -> Result<GraphReplicaSnapshot> {
        // Held across the clock read and the scan, so both describe the
        // same committed state and no torn batch is visible.
        let indexes = self.indexes_read();
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphReplicaSnapshot {
                graph: graph.clone(),
                clock: VectorClock::new(),
                quads: Vec::new(),
            });
        };
        let vector_clock = indexes.clocks.get(&graph_id).cloned().unwrap_or_default();

        // One prefix scan yields both the quads and their dot sets, instead of
        // an index scan plus a point read per quad.
        let mut quads = Vec::new();
        self.for_each_stored_quad(graph_id, |quad, dots| {
            quads.push(SnapshotQuadState {
                subject: self.decode_term_arc(quad.subject)?.as_ref().clone(),
                predicate: self.decode_term_arc(quad.predicate)?.as_ref().clone(),
                object: self.decode_term_arc(quad.object)?.as_ref().clone(),
                dots: decode_dots(dots)?,
            });
            Ok(())
        })?;

        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            clock: vector_clock,
            quads,
        })
    }

    pub fn graph_fingerprint(&self, graph: &GraphId) -> Result<(u64, [u8; 32], [u8; 32])> {
        // Commits publish under this lock, so holding it keeps the scan
        // from observing a batch that is still being applied.
        let _indexes = self.indexes_read();
        let Some(graph_id) = self.graph_id_for(graph)? else {
            let empty = *blake3::hash(&[]).as_bytes();
            return Ok((0, empty, empty));
        };

        let mut count = 0u64;
        let mut xor = [0u8; 32];
        let mut sum = [0u8; 32];
        self.for_each_stored_quad(graph_id, |quad, _dots| {
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
            Ok(())
        })?;
        Ok((count, xor, sum))
    }

    pub fn subject_triple_count_by_ids(&self, graph: TermId, subject: TermId) -> Result<usize> {
        Ok(self
            .indexes
            .read()
            .unwrap()
            .by_graph_subject
            .get(&(graph, subject))
            .map(HashSet::len)
            .unwrap_or(0))
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

    /// Is this exact quad live? O(1) against the derived indexes, committed
    /// state only — uncommitted batch state is invisible here.
    pub fn contains_quad(&self, quad: EncodedQuad) -> bool {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_subject
                .get(&quad.subject)
                .is_some_and(|entries| entries.contains(&(quad.predicate, quad.object, quad.graph)))
        })
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
        snapshot_seqno: u64,
        pattern: crate::rdf_read::QuadPattern,
    ) -> Option<crate::query_cursor::RawQuadCursor> {
        let uses_predicate_object = matches!(
            (
                pattern.graph,
                pattern.subject,
                pattern.predicate,
                pattern.object
            ),
            (Some(_), None, Some(_), Some(_))
        );
        let uses_object = matches!(
            (
                pattern.graph,
                pattern.subject,
                pattern.predicate,
                pattern.object
            ),
            (Some(_), None, None, Some(_))
        );
        if !uses_predicate_object && !uses_object {
            return None;
        }

        self.ensure_derived_indexes();
        let indexes = self.indexes_read();
        if self.db.snapshot().seqno() != snapshot_seqno {
            return None;
        }
        let derived = indexes
            .derived
            .as_ref()
            .expect("ensure_derived_indexes initialized the derived index");
        match (
            pattern.graph,
            pattern.subject,
            pattern.predicate,
            pattern.object,
        ) {
            (Some(graph), None, Some(predicate), Some(object)) => Some(
                derived
                    .by_predicate_object
                    .get(&(predicate, object))
                    .and_then(|graphs| graphs.get(&graph))
                    .cloned()
                    .map(|subjects| {
                        crate::query_cursor::RawQuadCursor::predicate_object(
                            subjects, graph, predicate, object,
                        )
                    })
                    .unwrap_or_else(crate::query_cursor::RawQuadCursor::empty),
            ),
            (Some(graph), None, None, Some(object)) => Some(
                derived
                    .by_object
                    .get(&object)
                    .and_then(|graphs| graphs.get(&graph))
                    .cloned()
                    .map(|entries| {
                        crate::query_cursor::RawQuadCursor::object(entries, graph, object)
                    })
                    .unwrap_or_else(crate::query_cursor::RawQuadCursor::empty),
            ),
            _ => None,
        }
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
        for quad in self.graph_scan(graph, None, None) {
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
        if self.fts_reindex_is_cheaper(req.graph_id, req.subjects.len()) {
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
    fn fts_reindex_is_cheaper(&self, graph_id: TermId, subjects: usize) -> bool {
        subjects >= FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD
            && subjects * 2 >= self.graph_subject_count(graph_id)
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

    /// Copy a subject's `(predicate, object)` id pairs out of the index,
    /// dropping `excluded` **while the read lock is held**.
    ///
    /// Filtering by term id before anything is decoded means excluding a
    /// high-cardinality predicate (`hasPart` on a crate root) never copies or
    /// decodes those entries at all.
    fn subject_entries(
        &self,
        key: (TermId, TermId),
        excluded: Option<TermId>,
    ) -> Vec<(TermId, TermId)> {
        // Guards IndexState for the duration of the filtered copy.
        let indexes = self.indexes_read();
        let Some(entries) = indexes.by_graph_subject.get(&key) else {
            return Vec::new();
        };
        entries
            .iter()
            .copied()
            .filter(|(predicate, _)| Some(*predicate) != excluded)
            .collect()
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
        self.decode_entries(self.subject_entries((graph, subject), None))
    }

    pub fn triples_for_subject_excluding_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        excluded_predicate: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        self.decode_entries(self.subject_entries((graph, subject), Some(excluded_predicate)))
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
        Ok(self.count_objects_for_ids(graph_id, subject_id, predicate_id))
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

    /// Test-only hook: drop a live quad from the in-memory index without
    /// touching the store, simulating index drift so tests can prove the next
    /// commit repairs it.
    #[cfg(test)]
    fn corrupt_index_for_test(&self, quad: EncodedQuad) {
        let mut indexes = self.indexes_write();
        indexes.remove_quad(quad);
    }

    #[cfg(test)]
    fn index_contains(&self, quad: EncodedQuad) -> bool {
        self.indexes_read()
            .by_graph_subject
            .get(&(quad.graph, quad.subject))
            .is_some_and(|entries| entries.contains(&(quad.predicate, quad.object)))
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
    fn planner_predicate_distinct_counts_track_live_rows() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:planner-distinct");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o:1"));
        let second = encode_quad(&store, &graph, ("urn:s:1", "urn:p", "urn:o:2"));
        let third = encode_quad(&store, &graph, ("urn:s:2", "urn:p", "urn:o:1"));
        commit_add(&store, &graph, first);
        commit_add(&store, &graph, second);
        commit_add(&store, &graph, third);

        assert_eq!(
            store.stat_predicate_distinct_subject_count(first.predicate),
            2
        );
        assert_eq!(
            store.stat_predicate_distinct_object_count(first.predicate),
            2
        );

        let clock = store.get_vector_clock(&graph).unwrap();
        commit_remove(&store, &graph, third, &clock);
        assert_eq!(
            store.stat_predicate_distinct_subject_count(first.predicate),
            1
        );
        assert_eq!(
            store.stat_predicate_distinct_object_count(first.predicate),
            2
        );

        let clock = store.get_vector_clock(&graph).unwrap();
        commit_remove(&store, &graph, first, &clock);
        assert_eq!(
            store.stat_predicate_distinct_subject_count(first.predicate),
            1
        );
        assert_eq!(
            store.stat_predicate_distinct_object_count(first.predicate),
            1
        );
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
            .get(&store.qv1_meta, key.bytes())
            .unwrap()
            .map(|value| decode_query_index_u64(value.as_ref()).unwrap())
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

            remove_query_index_key_for_test(&store, &store.qv1_meta, QUERY_INDEX_HEADER_KEY);
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

        remove_query_index_key_for_test(&store, &store.qv1_meta, QUERY_INDEX_HEADER_KEY);
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
    fn default_union_untrusted_states_fail_before_scanning_every_spo_binding_shape() {
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

        remove_query_index_key_for_test(&store, &store.qv1_meta, QUERY_INDEX_HEADER_KEY);
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

        stage_query_index_value_for_test(&store, &store.qv1_meta, QUERY_INDEX_HEADER_KEY, [0_u8]);
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

        let (first, corrupt) = if qv1_spog_key(first) < qv1_spog_key(second) {
            (first, second)
        } else {
            (second, first)
        };
        stage_query_index_value_for_test(&store, &store.qv1_spog, qv1_spog_key(corrupt), [1_u8]);

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
            Some(Err(StoreError::InvalidEncoding { .. }))
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
        let (quad, dot) = {
            let store = GraphStore::open(dir.path()).unwrap();
            store.create_graph(&graph).unwrap();
            let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
            let dot = commit_add(&store, &graph, quad);
            assert_query_index_ready(&store, 1);
            store.persist().unwrap();
            (quad, dot)
        };

        {
            let store = GraphStore::open(dir.path()).unwrap();
            assert_query_index_ready(&store, 1);
            let mut witnessed = VectorClock::new();
            witnessed.advance(dot.actor, dot.counter);
            commit_remove(&store, &graph, quad, &witnessed);
            assert_query_index_ready(&store, 0);
            store.persist().unwrap();
        }

        let reopened = GraphStore::open(dir.path()).unwrap();
        assert_query_index_ready(&reopened, 0);
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
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Total),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(first.graph)),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(second.graph)),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Predicate(first.predicate)),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::PredicateObject(first.predicate, first.object)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(first.graph, first.predicate)
            ),
            Some(1)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(second.graph, second.predicate)
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
    fn query_index_tracks_exact_dimensions_and_removes_last_predicate_epoch() {
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

        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Total),
            Some(4)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Graph(one.graph)),
            Some(3)
        );
        assert_eq!(
            query_index_counter_for_test(&store, QueryIndexCounterKey::Predicate(one.predicate)),
            Some(3)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicate(one.graph, one.predicate)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::PredicateObject(one.predicate, one.object)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::GraphPredicateObject(one.graph, one.predicate, one.object,)
            ),
            Some(2)
        );
        assert_eq!(
            query_index_counter_for_test(
                &store,
                QueryIndexCounterKey::PredicateMutationEpoch(one.predicate)
            ),
            Some(query_index_header_for_test(&store).source_epoch)
        );

        let mut witnessed = VectorClock::new();
        witnessed.advance(three_dot.actor, three_dot.counter);
        commit_remove(&store, &graph_one, three, &witnessed);
        assert_query_index_ready(&store, 3);
        for key in [
            QueryIndexCounterKey::Predicate(three.predicate),
            QueryIndexCounterKey::GraphPredicate(three.graph, three.predicate),
            QueryIndexCounterKey::PredicateObject(three.predicate, three.object),
            QueryIndexCounterKey::GraphPredicateObject(three.graph, three.predicate, three.object),
            QueryIndexCounterKey::PredicateMutationEpoch(three.predicate),
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
            QueryIndexCounterKey::Graph(quad.graph),
            QueryIndexCounterKey::Predicate(quad.predicate),
            QueryIndexCounterKey::GraphPredicate(quad.graph, quad.predicate),
            QueryIndexCounterKey::PredicateObject(quad.predicate, quad.object),
            QueryIndexCounterKey::GraphPredicateObject(quad.graph, quad.predicate, quad.object),
            QueryIndexCounterKey::PredicateMutationEpoch(quad.predicate),
        ] {
            assert_eq!(query_index_counter_for_test(&store, key), None);
        }
        let snapshot = store.db.snapshot();
        for keyspace in [&store.qv1_gpos, &store.qv1_spog, &store.qv1_posg] {
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
        for (keyspace, key) in [
            (&store.qv1_gpos, qv1_gpos_key(quad)),
            (&store.qv1_spog, qv1_spog_key(quad)),
            (&store.qv1_posg, qv1_posg_key(quad)),
        ] {
            let value = snapshot.get(keyspace, key).unwrap().unwrap();
            assert!(value.as_ref().is_empty());
            let (stored_key, stored_value) = snapshot
                .iter(keyspace)
                .next()
                .unwrap()
                .into_inner()
                .unwrap();
            assert_eq!(stored_key.as_ref().len(), 64);
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

        remove_query_index_key_for_test(&store, &store.qv1_spog, qv1_spog_key(quad));
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_query_index_value_for_test(
            &store,
            &store.qv1_spog,
            qv1_spog_key(quad),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(&store, &store.qv1_posg, qv1_posg_key(quad), vec![1]);
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        stage_query_index_value_for_test(
            &store,
            &store.qv1_posg,
            qv1_posg_key(quad),
            Vec::<u8>::new(),
        );
        remove_query_index_key_for_test(&store, &store.qv1_spog, qv1_spog_key(quad));
        stage_query_index_value_for_test(&store, &store.qv1_spog, vec![0; 63], Vec::<u8>::new());
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );

        remove_query_index_key_for_test(&store, &store.qv1_spog, vec![0; 63]);
        stage_query_index_value_for_test(
            &store,
            &store.qv1_spog,
            qv1_spog_key(quad),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(
            &store,
            &store.qv1_meta,
            QUERY_INDEX_TOTAL_KEY,
            2u64.to_be_bytes(),
        );
        assert_eq!(
            store.query_index_status().unwrap().state,
            QueryIndexState::Failed("ready-status-mismatch".to_owned())
        );
    }

    #[test]
    fn query_index_fast_status_and_normal_reopen_skip_full_verification() {
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
        assert_eq!(0, reopened.query_index_verification_run_count());
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
        assert_eq!(0, reopened.query_index_verification_run_count());

        let sampled = reopened
            .verify_query_indexes(QueryIndexVerificationMode::Sample)
            .unwrap();
        assert!(sampled.valid);
        assert!(!sampled.full);
        assert_eq!(1, reopened.query_index_verification_run_count());
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
            remove_query_index_key_for_test(&store, &store.qv1_meta, QUERY_INDEX_HEADER_KEY);
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
            remove_query_index_key_for_test(&store, &store.qv1_posg, qv1_posg_key(quad));
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
        assert_eq!(sample.checked_index_rows, QUERY_INDEX_SAMPLE_ROWS * 3);

        let full = store.verify_query_indexes(true).unwrap();
        assert!(full.valid);
        assert!(full.full);
        assert_eq!(full.source_live_quads, rows);
        assert_eq!(full.indexed_quads, rows);
        assert_eq!(full.checked_source_rows, rows);
        assert_eq!(full.checked_index_rows, rows * 3);
    }

    #[test]
    fn query_index_verification_detects_qv_and_metadata_corruption() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:verification-corruption");
        store.create_graph(&graph).unwrap();
        let quad = encode_quad(&store, &graph, ("urn:test:s", "urn:test:p", "urn:test:o"));
        commit_add(&store, &graph, quad);
        let extra = EncodedQuad {
            subject: store
                .resolve_term(&named("urn:test:qv:extra-subject"))
                .unwrap(),
            ..quad
        };

        stage_query_index_value_for_test(
            &store,
            &store.qv1_gpos,
            qv1_gpos_key(extra),
            Vec::<u8>::new(),
        );
        stage_query_index_value_for_test(&store, &store.qv1_gpos, qv1_gpos_key(quad), vec![1]);
        remove_query_index_key_for_test(&store, &store.qv1_spog, qv1_spog_key(quad));
        stage_query_index_value_for_test(&store, &store.qv1_posg, vec![0; 63], Vec::<u8>::new());
        stage_query_index_value_for_test(
            &store,
            &store.qv1_meta,
            QUERY_INDEX_TOTAL_KEY,
            vec![0; 7],
        );
        stage_query_index_value_for_test(&store, &store.qv1_meta, vec![b'Z'], 0u64.to_be_bytes());
        stage_query_index_value_for_test(
            &store,
            &store.qv1_meta,
            vec![b'G', 0],
            0u64.to_be_bytes(),
        );
        let orphan_graph = store
            .resolve_term(&named("urn:test:qv:orphan-counter-graph"))
            .unwrap();
        stage_query_index_value_for_test(
            &store,
            &store.qv1_meta,
            QueryIndexCounterKey::Graph(orphan_graph).bytes(),
            1u64.to_be_bytes(),
        );

        let report = store.verify_query_indexes(true).unwrap();
        assert!(!report.valid);
        for problem in [
            "source-gpos-missing-or-nonempty",
            "source-spog-missing-or-nonempty",
            "qv-gpos-value-nonempty",
            "qv-gpos-source-missing",
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
            &store.qv1_meta,
            QueryIndexCounterKey::Predicate(first.predicate).bytes(),
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
        assert_eq!(snapshot.iter(&store.qv1_gpos).count(), 1);
        assert!(
            snapshot
                .get(&store.qv1_gpos, qv1_gpos_key(second))
                .unwrap()
                .is_none()
        );
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
        assert_eq!(snapshot.iter(&store.qv1_gpos).count(), 1);
        assert!(
            snapshot
                .get(&store.qv1_gpos, qv1_gpos_key(second))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn query_index_maintenance_rejects_orphan_counter_without_losing_source_write() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:qv:maintenance-orphan-counter");
        store.create_graph(&graph).unwrap();
        let first = encode_quad(&store, &graph, ("urn:test:s1", "urn:test:p1", "urn:test:o"));
        commit_add(&store, &graph, first);
        let second = encode_quad(&store, &graph, ("urn:test:s2", "urn:test:p2", "urn:test:o"));
        stage_query_index_value_for_test(
            &store,
            &store.qv1_meta,
            QueryIndexCounterKey::Predicate(second.predicate).bytes(),
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
        assert_eq!(snapshot.iter(&store.qv1_gpos).count(), 1);
        assert!(
            snapshot
                .get(&store.qv1_gpos, qv1_gpos_key(second))
                .unwrap()
                .is_none()
        );
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
                &store.qv1_meta,
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
                &store.qv1_meta,
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
            for (keyspace, key) in [
                (&store.qv1_gpos, qv1_gpos_key(quad)),
                (&store.qv1_spog, qv1_spog_key(quad)),
                (&store.qv1_posg, qv1_posg_key(quad)),
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
                    .get(&store.qv1_meta, QUERY_INDEX_HEADER_KEY)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                snapshot
                    .get(&store.qv1_meta, QUERY_INDEX_TOTAL_KEY)
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
    fn graph_queries_use_in_memory_indexes() {
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
    fn derived_indexes_track_commits_incrementally() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        let subject = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:s"));
        let predicate =
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:p"));
        let object = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("urn:test:o"));

        // Built before any write: later commits must maintain it in place.
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

        assert_eq!(count, store.graph_subject_count(graph_id));
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

    // ── Commit guard and prompt index repair ────────────────────────────

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

    /// Prompt-repair proof: a commit that trips the anomaly check rebuilds the
    /// index from the store before it returns, so unrelated drift is gone by
    /// the time the caller sees the result — not "at next restart".
    #[test]
    fn commit_repairs_anomaly() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:index-anomaly");
        store.create_graph(&graph).unwrap();

        let removed = encode_quad(&store, &graph, ("urn:s1", "urn:p", "urn:o1"));
        let collateral = encode_quad(&store, &graph, ("urn:s2", "urn:p", "urn:o2"));
        let removed_dot = commit_add(&store, &graph, removed);
        commit_add(&store, &graph, collateral);
        assert!(store.index_contains(removed));
        assert!(store.index_contains(collateral));

        // Simulate drift: both quads vanish from the index while the store
        // still holds them.
        store.corrupt_index_for_test(removed);
        store.corrupt_index_for_test(collateral);
        assert!(!store.index_contains(removed));
        assert!(!store.index_contains(collateral));

        // Retracting `removed` makes the index remove find nothing -> anomaly.
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
            "the detecting commit must rebuild the index, restoring unrelated drift"
        );
        assert!(
            !store.index_contains(removed),
            "the rebuilt index must match the store, where the quad is gone"
        );
        assert!(!store.contains_quad(removed));
        assert!(store.contains_quad(collateral));
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
