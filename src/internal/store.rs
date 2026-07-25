use std::collections::{BTreeSet, HashMap, HashSet, VecDeque, hash_map::Entry};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::core::*;
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode, compaction::Leveled,
    config::CompressionPolicy,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
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

pub struct WriteBatch {
    inner: fjall::OwnedWriteBatch,
    /// Uncommitted dot sets, so later operations in the same batch read the
    /// batch-local state instead of the (still stale) durable one. `None` means
    /// "written empty", i.e. the quad is dead. Keyed by the fixed-size quad key
    /// so no per-quad `Vec` is allocated.
    pending_quad_states: HashMap<QuadKey, Option<Vec<Dot>>>,
    pending_terms: HashMap<TermId, String>,
    quad_mutations: Vec<QuadMutation>,
}

impl WriteBatch {
    fn new(inner: fjall::OwnedWriteBatch) -> Self {
        Self {
            inner,
            pending_quad_states: HashMap::new(),
            pending_terms: HashMap::new(),
            quad_mutations: Vec::new(),
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

#[derive(Default)]
struct IndexState {
    graph_subjects: HashMap<TermId, HashSet<TermId>>,
    by_graph_subject: HashMap<(TermId, TermId), HashSet<(TermId, TermId)>>,
}

type ObjectOrderKey = (TermId, TermId, TermId);
type ObjectOrderValues = Arc<Vec<TermId>>;
type ObjectOrderCache = HashMap<ObjectOrderKey, ObjectOrderValues>;

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
}

#[derive(Default)]
struct DerivedIndexState {
    by_subject: HashMap<TermId, HashSet<(TermId, TermId, TermId)>>,
    by_predicate_object: HashMap<(TermId, TermId), HashMap<TermId, HashSet<TermId>>>,
    by_object: HashMap<TermId, HashMap<TermId, HashSet<(TermId, TermId)>>>,
    /// predicate → graph → live quad count, so a predicate-only pattern can be
    /// answered by scanning just the graphs that actually contain it instead of
    /// every subject in the corpus. Counts are needed so a graph is
    /// dropped only when its last quad for that predicate goes away.
    predicate_graph_counts: HashMap<TermId, HashMap<TermId, usize>>,
    // Approximate corpus-wide cardinalities for the query planner; quad
    // instances are counted per graph (no cross-graph triple dedup).
    predicate_object_counts: HashMap<(TermId, TermId), usize>,
    predicate_counts: HashMap<TermId, usize>,
    object_counts: HashMap<TermId, usize>,
    total_quads: usize,
}

impl DerivedIndexState {
    fn insert_quad(&mut self, quad: EncodedQuad) {
        let is_new = self.by_subject.entry(quad.subject).or_default().insert((
            quad.predicate,
            quad.object,
            quad.graph,
        ));
        self.by_predicate_object
            .entry((quad.predicate, quad.object))
            .or_default()
            .entry(quad.graph)
            .or_default()
            .insert(quad.subject);
        self.by_object
            .entry(quad.object)
            .or_default()
            .entry(quad.graph)
            .or_default()
            .insert((quad.subject, quad.predicate));
        if is_new {
            *self
                .predicate_object_counts
                .entry((quad.predicate, quad.object))
                .or_default() += 1;
            *self.predicate_counts.entry(quad.predicate).or_default() += 1;
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
                graphs.get_mut().remove(&quad.subject);
                if graphs.get().is_empty() {
                    graphs.remove();
                }
            }
            if entry.get().is_empty() {
                entry.remove();
            }
        }

        if let Entry::Occupied(mut entry) = self.by_object.entry(quad.object) {
            if let Entry::Occupied(mut graphs) = entry.get_mut().entry(quad.graph) {
                graphs.get_mut().remove(&(quad.subject, quad.predicate));
                if graphs.get().is_empty() {
                    graphs.remove();
                }
            }
            if entry.get().is_empty() {
                entry.remove();
            }
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
    /// Guards first-write-wins term interning, sharded by term id.
    term_locks: Vec<Mutex<()>>,
    /// Guards whole read→write→commit cycles of one graph's CRDT state; see
    /// [`GraphStore::graph_commit_guard`].
    commit_locks: Vec<Mutex<()>>,
    indexes: RwLock<IndexState>,
    derived_indexes: RwLock<Option<DerivedIndexState>>,
    object_order_cache: RwLock<ObjectOrderCache>,
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
    dirty_counter: AtomicU64,
    /// How many times this store instance has recomputed graph diagnostics.
    /// Tests use it to prove a reopen served the persisted record instead of
    /// recomputing, and that a stale record was repaired at open.
    diagnostics_computed: AtomicU64,
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
                batch.quad_mutations.push(QuadMutation::Insert(quad));
                Ok(true)
            }
            (true, false) => {
                batch.quad_mutations.push(QuadMutation::Remove(quad));
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
    /// source of truth. This is the repair action for derived-state register
    /// rows 1–3 and 6.
    fn rebuild_indexes(&self) -> Result<()> {
        let indexes = self.build_indexes()?;
        // Order matters only in that all derived state is dropped before the
        // fresh index becomes visible; every consumer rebuilds lazily.
        *self.indexes_write() = indexes;
        *self
            .derived_indexes
            .write()
            .unwrap_or_else(PoisonError::into_inner) = None;
        self.object_order_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.term_decode_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Ok(())
    }

    /// Commit a batch and mirror its quad mutations into the in-memory indexes.
    ///
    /// An [`IndexApply::Anomaly`] means the index drifted from the store, so it
    /// is rebuilt before this returns rather than left inconsistent.
    fn apply_quad_mutations(
        &self,
        batch: fjall::OwnedWriteBatch,
        mutations: Vec<QuadMutation>,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(batch.commit()?);
        }

        let anomaly = self.commit_with_index(batch, &mutations)?;

        if anomaly {
            // Drop the mirror and the order cache with it; rebuild_indexes
            // resets both from the store.
            tracing::warn!(
                "index anomaly detected while applying a commit; rebuilding indexes from the store"
            );
            return self.rebuild_indexes();
        }

        // Guards the lazily built planner/cross-graph mirror of IndexState.
        if let Some(derived) = self
            .derived_indexes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            for mutation in &mutations {
                match mutation {
                    QuadMutation::Insert(quad) => derived.insert_quad(*quad),
                    QuadMutation::Remove(quad) => derived.remove_quad(*quad),
                }
            }
        }

        // Guards the (graph, subject, predicate) → sorted objects cache.
        let mut cache = self
            .object_order_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for mutation in &mutations {
            let quad = match mutation {
                QuadMutation::Insert(quad) | QuadMutation::Remove(quad) => *quad,
            };
            cache.remove(&(quad.graph, quad.subject, quad.predicate));
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
            std::thread::sleep(delay);
        }
    }

    /// Make every later commit stall between the durable write and the index
    /// apply. Test-only.
    #[cfg(test)]
    pub(crate) fn set_commit_stall(&self, delay: std::time::Duration) {
        *self
            .commit_stall
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    /// Publish the durable batch and the index together, reporting anomalies.
    ///
    /// The lock spans the commit: a reader that sees the new clock must not
    /// then read an index predating it (G6).
    fn commit_with_index(
        &self,
        batch: fjall::OwnedWriteBatch,
        mutations: &[QuadMutation],
    ) -> Result<bool> {
        // Guards IndexState: the (graph, subject) → (predicate, object) map.
        let mut indexes = self.indexes_write();
        batch.commit()?;
        #[cfg(test)]
        self.stall_after_commit();

        let mut anomaly = false;
        for mutation in mutations {
            let outcome = match mutation {
                QuadMutation::Insert(quad) => indexes.insert_quad(*quad),
                QuadMutation::Remove(quad) => indexes.remove_quad(*quad),
            };
            anomaly |= outcome == IndexApply::Anomaly;
        }
        Ok(anomaly)
    }

    fn build_derived_indexes(&self) -> DerivedIndexState {
        let indexes = self.indexes_read();
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

    fn with_derived_indexes<R>(&self, f: impl FnOnce(&DerivedIndexState) -> R) -> R {
        {
            let guard = self
                .derived_indexes
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(derived) = guard.as_ref() {
                return f(derived);
            }
        }

        let mut guard = self
            .derived_indexes
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if guard.is_none() {
            *guard = Some(self.build_derived_indexes());
        }
        f(guard.as_ref().expect("derived indexes initialized"))
    }

    /// Diagnostics recomputations performed by this store instance.
    #[cfg(test)]
    pub(crate) fn diagnostics_compute_count(&self) -> u64 {
        self.diagnostics_computed.load(Ordering::Relaxed)
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
    fn repair_graph_diagnostics_at_open(&self) -> Result<()> {
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();

        for graph_id in self.graph_term_ids()? {
            let clock = self.get_vector_clock_by_id(graph_id)?;
            let stored = self.read_stored_diagnostics(graph_id)?;
            if let Some(record) = stored.filter(|record| record.at_clock == clock) {
                self.diagnostics_cache
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(graph_id, record);
                continue;
            }

            let previous = self
                .read_stored_diagnostics(graph_id)?
                .map(|record| record.diagnostics)
                .unwrap_or_default();
            let repaired = self.recompute_graph_diagnostics(graph_id)?;
            self.requeue_orphan_changes(graph_id, (&previous, &repaired))?;
        }
        Ok(())
    }

    /// Re-queue for search every entity whose orphan status changed during a
    /// repair.
    ///
    /// Orphaned entities are invisible to search (G6), so a repair that flips
    /// an entity in or out of the orphan set leaves the search index disagreeing
    /// with the store until that subject is re-indexed. Without this, a crash
    /// between a quad commit and its diagnostics write would keep an entity
    /// searchable — or wrongly hidden — until something unrelated happened to
    /// dirty it (G7).
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
            // `from_subject_id`, not `from_named_node`: diagnostics store a
            // blank node as `_:b0`, and re-encoding that as the IRI `<_:b0>`
            // would miss the lookup and silently never re-index it (G6, G7).
            let term = EncodedTerm::from_subject_id(entity.as_str());
            if let Some(subject) = self.lookup_term(&term)? {
                subjects.insert(subject);
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

    fn graph_subject_quads(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Vec<EncodedQuad> {
        let indexes = self.indexes_read();
        let Some(entries) = indexes.by_graph_subject.get(&(graph, subject)) else {
            return Vec::new();
        };

        let mut quads = Vec::new();
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
        quads
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

    fn cross_graph_subject_scan(
        &self,
        subject: TermId,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Vec<EncodedQuad> {
        self.with_derived_indexes(|indexes| {
            let Some(entries) = indexes.by_subject.get(&subject) else {
                return Vec::new();
            };

            let mut quads = Vec::new();
            for &(candidate_predicate, candidate_object, graph) in entries {
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
            quads
        })
    }

    fn predicate_object_scan(
        &self,
        graph: Option<TermId>,
        predicate: TermId,
        object: TermId,
    ) -> Vec<EncodedQuad> {
        self.with_derived_indexes(|indexes| {
            let Some(graphs) = indexes.by_predicate_object.get(&(predicate, object)) else {
                return Vec::new();
            };

            let mut quads = Vec::new();
            let mut push_graph = |g: TermId, subjects: &HashSet<TermId>| {
                quads.extend(subjects.iter().map(|&subject| EncodedQuad {
                    graph: g,
                    subject,
                    predicate,
                    object,
                }));
            };
            match graph {
                Some(g) => {
                    if let Some(subjects) = graphs.get(&g) {
                        push_graph(g, subjects);
                    }
                }
                None => {
                    for (&g, subjects) in graphs {
                        push_graph(g, subjects);
                    }
                }
            }
            quads
        })
    }

    /// Graphs containing at least one quad matching (predicate, object), so
    /// union readers can stream graph-at-a-time and short-circuit (ASK/LIMIT)
    /// after checking visibility per graph instead of materializing the full
    /// cross-corpus match set.
    pub(crate) fn predicate_object_graphs(&self, predicate: TermId, object: TermId) -> Vec<TermId> {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_predicate_object
                .get(&(predicate, object))
                .map(|graphs| graphs.keys().copied().collect())
                .unwrap_or_default()
        })
    }

    pub(crate) fn predicate_object_subjects_in_graph(
        &self,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Vec<TermId> {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_predicate_object
                .get(&(predicate, object))
                .and_then(|graphs| graphs.get(&graph))
                .map(|subjects| subjects.iter().copied().collect())
                .unwrap_or_default()
        })
    }

    pub(crate) fn object_graphs(&self, object: TermId) -> Vec<TermId> {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_object
                .get(&object)
                .map(|graphs| graphs.keys().copied().collect())
                .unwrap_or_default()
        })
    }

    pub(crate) fn object_entries_in_graph(
        &self,
        graph: TermId,
        object: TermId,
    ) -> Vec<(TermId, TermId)> {
        self.with_derived_indexes(|indexes| {
            indexes
                .by_object
                .get(&object)
                .and_then(|graphs| graphs.get(&graph))
                .map(|entries| entries.iter().copied().collect())
                .unwrap_or_default()
        })
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

    pub(crate) fn stat_total_quads(&self) -> usize {
        self.with_derived_indexes(|indexes| indexes.total_quads)
    }

    /// Term ids of all graphs that currently hold at least one quad, from the
    /// in-memory index (no store reads). Suitable for quad iteration; use
    /// [`GraphStore::graph_term_id_iter`] when empty graphs must be included.
    pub(crate) fn populated_graph_ids(&self) -> Vec<TermId> {
        self.indexes
            .read()
            .unwrap()
            .graph_subjects
            .keys()
            .copied()
            .collect()
    }

    fn object_scan(&self, graph: Option<TermId>, object: TermId) -> Vec<EncodedQuad> {
        self.with_derived_indexes(|indexes| {
            let Some(graphs) = indexes.by_object.get(&object) else {
                return Vec::new();
            };

            let mut quads = Vec::new();
            let mut push_graph = |g: TermId, entries: &HashSet<(TermId, TermId)>| {
                quads.extend(entries.iter().map(|&(subject, predicate)| EncodedQuad {
                    graph: g,
                    subject,
                    predicate,
                    object,
                }));
            };
            match graph {
                Some(g) => {
                    if let Some(entries) = graphs.get(&g) {
                        push_graph(g, entries);
                    }
                }
                None => {
                    for (&g, entries) in graphs {
                        push_graph(g, entries);
                    }
                }
            }
            quads
        })
    }

    fn decode_quad_key(bytes: &[u8]) -> Result<EncodedQuad> {
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

    fn quad_key(graph: TermId, subject: TermId, predicate: TermId, object: TermId) -> [u8; 64] {
        let mut key = [0u8; 64];
        key[0..16].copy_from_slice(&graph.to_be_bytes());
        key[16..32].copy_from_slice(&subject.to_be_bytes());
        key[32..48].copy_from_slice(&predicate.to_be_bytes());
        key[48..64].copy_from_slice(&object.to_be_bytes());
        key
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

    fn ordered_objects_for_subject_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
    ) -> Result<Arc<Vec<TermId>>> {
        if let Some(cached) = self
            .object_order_cache
            .read()
            .unwrap()
            .get(&(graph, subject, predicate))
            .cloned()
        {
            return Ok(cached);
        }

        let object_ids = self
            .indexes
            .read()
            .unwrap()
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

        let mut ordered = object_ids
            .into_iter()
            .map(|object| Ok((self.decode_term(object)?.0, object)))
            .collect::<Result<Vec<_>>>()?;
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        let ordered = Arc::new(
            ordered
                .into_iter()
                .map(|(_, object)| object)
                .collect::<Vec<_>>(),
        );
        self.object_order_cache
            .write()
            .unwrap()
            .insert((graph, subject, predicate), ordered.clone());
        Ok(ordered)
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
        Self::from_database_with_persist_mode(db, persist_mode)
    }

    pub fn from_database(db: Database) -> Result<Self> {
        Self::from_database_with_persist_mode(db, PersistMode::Buffer)
    }

    pub fn from_database_with_persist_mode(
        db: Database,
        persist_mode: PersistMode,
    ) -> Result<Self> {
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
            db,
            persist_mode,
            term_locks: (0..TERM_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            commit_locks: (0..COMMIT_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            indexes: RwLock::new(IndexState::default()),
            derived_indexes: RwLock::new(None),
            object_order_cache: RwLock::new(HashMap::new()),
            diagnostics_cache: RwLock::new(HashMap::new()),
            term_decode_cache: RwLock::new(HashMap::new()),
            #[cfg(test)]
            commit_stall: Mutex::new(None),
            dirty_counter: AtomicU64::new(1),
            diagnostics_computed: AtomicU64::new(0),
        };

        store.rebuild_indexes()?;
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
                highest = highest.max(decode_u64_bytes(value.as_ref(), "fts queue token")?);
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
        for keyspace in [&self.terms, &self.quads, &self.graphs, &self.log] {
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
        // The clock and the diagnostics record live under their own keys since
        // A recreated graph must start from a fresh clock, not
        // inherit the deleted one.
        batch.remove(&self.graphs, graph_clock_key(graph_id));
        batch.remove(&self.graphs, graph_diagnostics_key(graph_id));
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
        }

        let reindex_key = graph_reindex_key(graph_id);
        if self.graphs.get(reindex_key)?.is_some() {
            batch.remove(&self.graphs, reindex_key);
        }

        // `Relaxed` for the same reason as `enqueue_fts`.
        let delete_token = self.dirty_counter.fetch_add(1, Ordering::Relaxed);
        batch.insert(
            &self.graphs,
            graph_search_delete_key(graph_id),
            delete_token.to_be_bytes(),
        );

        for guard in self.log.prefix(log_head_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.log, key);
        }
        for guard in self.log.prefix(log_batch_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.log, key);
        }

        self.commit(batch)?;
        self.diagnostics_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&graph_id);
        self.object_order_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(graph_term, _, _), _| *graph_term != graph_id);
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

    /// Recompute a graph's diagnostics and persist them. Only writers that own
    /// the search re-queue may call this; see [`GraphStore::graph_diagnostics_by_id`].
    fn recompute_graph_diagnostics(&self, graph_id: TermId) -> Result<GraphDiagnostics> {
        let record = self.compute_tagged_diagnostics(graph_id)?;
        self.store_diagnostics_record(graph_id, record)
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
        let vector_clock = self.get_vector_clock(graph)?;
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphReplicaSnapshot {
                graph: graph.clone(),
                clock: vector_clock,
                quads: Vec::new(),
            });
        };

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

    /// Graphs holding at least one quad with `predicate`, so a predicate-only
    /// pattern scans those graphs instead of every subject in the corpus.
    pub(crate) fn predicate_graphs(&self, predicate: TermId) -> Vec<TermId> {
        self.with_derived_indexes(|indexes| {
            indexes
                .predicate_graph_counts
                .get(&predicate)
                .map(|graphs| graphs.keys().copied().collect())
                .unwrap_or_default()
        })
    }

    /// The token the next FTS queue entry will receive. The reindex-threshold
    /// policy uses it to reason about queue ordering.
    pub(crate) fn current_dirty_token(&self) -> u64 {
        self.dirty_counter.load(Ordering::SeqCst)
    }

    pub fn quads_for_pattern(
        &self,
        graph: Option<TermId>,
        subject: Option<TermId>,
        predicate: Option<TermId>,
        object: Option<TermId>,
    ) -> Result<Vec<EncodedQuad>> {
        Ok(match (graph, subject, predicate, object) {
            (Some(graph), Some(subject), predicate, object) => {
                self.graph_subject_quads(graph, subject, predicate, object)
            }
            (Some(graph), None, Some(predicate), Some(object)) => {
                self.predicate_object_scan(Some(graph), predicate, object)
            }
            (Some(graph), None, Some(predicate), None) => {
                self.graph_scan(graph, Some(predicate), None)
            }
            (Some(graph), None, None, Some(object)) => self.object_scan(Some(graph), object),
            (Some(graph), None, None, None) => self.graph_scan(graph, None, None),
            (None, Some(subject), predicate, object) => {
                self.cross_graph_subject_scan(subject, predicate, object)
            }
            (None, None, Some(predicate), Some(object)) => {
                self.predicate_object_scan(None, predicate, object)
            }
            (None, None, None, Some(object)) => self.object_scan(None, object),
            (None, None, Some(predicate), None) => {
                let mut quads = Vec::new();
                for graph in self.predicate_graphs(predicate) {
                    quads.extend(self.graph_scan(graph, Some(predicate), None));
                }
                quads
            }
            (None, None, None, None) => {
                let graph_ids = self
                    .indexes_read()
                    .graph_subjects
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>();
                let mut quads = Vec::new();
                for graph in graph_ids {
                    quads.extend(self.graph_scan(graph, None, None));
                }
                quads
            }
        })
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

    /// Read a graph's vector clock from its own `'K'` key.
    ///
    /// Falls back to the clock embedded in the legacy metadata record when no
    /// `'K'` key exists yet, which is the one-time migration path for stores
    /// written before the split; the first [`GraphStore::set_vector_clock`]
    /// writes `'K'` and the legacy copy is ignored from then on.
    pub(crate) fn get_vector_clock_by_id(&self, graph_id: TermId) -> Result<VectorClock> {
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
    /// Does not lock — the caller must hold the graph commit guard, which is
    /// what makes the read-clock → advance → write-clock cycle atomic (G2).
    pub fn set_vector_clock(&self, batch: &mut WriteBatch, update: ClockUpdate<'_>) -> Result<()> {
        batch.insert(
            &self.graphs,
            graph_clock_key(update.graph_id),
            postcard::to_allocvec(update.clock)?,
        );
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
    /// store mutation that dirtied it (G7). The token comes from a counter seeded
    /// past every live queue token at open.
    ///
    /// `Relaxed` suffices: the entry's visibility comes from the fjall batch, and
    /// `current_dirty_token` only needs single-location read-read coherence.
    pub fn enqueue_fts(&self, batch: &mut WriteBatch, key: FtsSubject) -> Result<()> {
        let token = self.dirty_counter.fetch_add(1, Ordering::Relaxed);
        batch.insert(
            &self.graphs,
            graph_dirty_key(key.graph_id, key.subject),
            token.to_be_bytes(),
        );
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
        // `Relaxed` for the same reason as `enqueue_fts`.
        let token = self.dirty_counter.fetch_add(1, Ordering::Relaxed);
        batch.insert(
            &self.graphs,
            graph_reindex_key(graph_id),
            token.to_be_bytes(),
        );
        Ok(())
    }

    pub fn drain_fts_queue(&self, limit: usize) -> Result<Vec<(GraphId, TermId, u64)>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_dirty_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 33 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph dirty graph")?;
            let subject_id = decode_term_id(&key[17..33], "graph dirty subject")?;
            let token = decode_u64_bytes(value.as_ref(), "graph dirty token")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push((graph, subject_id, token));
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    pub fn drain_fts_reindex_queue(&self, limit: usize) -> Result<Vec<(GraphId, u64)>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_reindex_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph reindex graph")?;
            let token = decode_u64_bytes(value.as_ref(), "graph reindex token")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push((graph, token));
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    pub fn drain_fts_delete_queue(&self, limit: usize) -> Result<Vec<(GraphId, u64)>> {
        let mut result = Vec::new();
        let mut term_cache = HashMap::new();

        for guard in self.graphs.prefix(graph_search_delete_prefix()) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph search delete graph")?;
            let token = decode_u64_bytes(value.as_ref(), "graph search delete token")?;
            let graph = self
                .decode_term_cached(&mut term_cache, graph_id)?
                .to_named_node()
                .map(GraphId);
            if let Some(graph) = graph {
                result.push((graph, token));
            }
            if result.len() >= limit {
                break;
            }
        }

        Ok(result)
    }

    pub fn acknowledge_fts_queue(&self, queued: &[(GraphId, TermId, u64)]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        for (graph, subject, token) in queued {
            let Some(graph_id) = self.graph_id_for(graph)? else {
                continue;
            };
            let key = graph_dirty_key(graph_id, *subject);
            if self.graphs.get(key)?.is_some_and(|current| {
                decode_u64_bytes(current.as_ref(), "graph dirty token").ok() == Some(*token)
            }) {
                batch.remove(&self.graphs, key);
            }
        }
        self.commit_fjall_batch(batch)?;
        Ok(())
    }

    pub fn acknowledge_fts_reindex_queue(&self, queued: &[(GraphId, u64)]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        for (graph, token) in queued {
            let Some(graph_id) = self.graph_id_for(graph)? else {
                continue;
            };
            let key = graph_reindex_key(graph_id);
            if self.graphs.get(key)?.is_some_and(|current| {
                decode_u64_bytes(current.as_ref(), "graph reindex token").ok() == Some(*token)
            }) {
                batch.remove(&self.graphs, key);
            }
        }
        self.commit_fjall_batch(batch)?;
        Ok(())
    }

    pub fn acknowledge_fts_delete_queue(&self, queued: &[(GraphId, u64)]) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        for (graph, token) in queued {
            let Some(graph_id) = self.graph_id_for(graph)? else {
                continue;
            };
            let key = graph_search_delete_key(graph_id);
            if self.graphs.get(key)?.is_some_and(|current| {
                decode_u64_bytes(current.as_ref(), "graph search delete token").ok() == Some(*token)
            }) {
                batch.remove(&self.graphs, key);
            }
        }
        self.commit_fjall_batch(batch)?;
        Ok(())
    }

    pub fn acknowledge_fts_subjects_for_reindexed_graphs(
        &self,
        queued: &[(GraphId, u64)],
    ) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for (graph, reindex_token) in queued {
            let Some(graph_id) = self.graph_id_for(graph)? else {
                continue;
            };
            for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let token = decode_u64_bytes(value.as_ref(), "graph dirty token")?;
                if token <= *reindex_token {
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

    pub fn acknowledge_fts_queues_for_deleted_graphs(
        &self,
        queued: &[(GraphId, u64)],
    ) -> Result<()> {
        if queued.is_empty() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for (graph, delete_token) in queued {
            let Some(graph_id) = self.graph_id_for(graph)? else {
                continue;
            };
            for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
                let (key, value) = guard.into_inner()?;
                let token = decode_u64_bytes(value.as_ref(), "graph dirty token")?;
                if token <= *delete_token {
                    batch.remove(&self.graphs, key);
                    dirty = true;
                }
            }

            let reindex_key = graph_reindex_key(graph_id);
            if self.graphs.get(reindex_key)?.is_some_and(|current| {
                decode_u64_bytes(current.as_ref(), "graph reindex token")
                    .ok()
                    .is_some_and(|token| token <= *delete_token)
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

    pub fn clear_fts_queue_for_graph(&self, graph: &GraphId) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

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

        let delete_key = graph_search_delete_key(graph_id);
        if self.graphs.get(delete_key)?.is_some() {
            batch.remove(&self.graphs, delete_key);
            dirty = true;
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub fn clear_fts_reindex_for_graph(&self, graph: &GraphId) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let reindex_key = graph_reindex_key(graph_id);
        if self.graphs.get(reindex_key)?.is_none() {
            return Ok(());
        }

        let mut batch = self.buffered_batch();
        batch.remove(&self.graphs, reindex_key);
        self.commit_fjall_batch(batch)?;
        Ok(())
    }

    pub fn clear_fts_queue_subjects(
        &self,
        graph: &GraphId,
        subjects: &[EncodedTerm],
    ) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let mut batch = self.buffered_batch();
        let mut dirty = false;
        for subject in subjects {
            let Some(subject_id) = self.lookup_term(subject)? else {
                continue;
            };
            let key = graph_dirty_key(graph_id, subject_id);
            if self.graphs.get(key)?.is_some() {
                batch.remove(&self.graphs, key);
                dirty = true;
            }
        }

        if dirty {
            self.commit_fjall_batch(batch)?;
        }
        Ok(())
    }

    pub fn clear_fts_queue(&self) -> Result<()> {
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
        Ok(())
    }

    fn commit_fjall_batch(&self, batch: fjall::OwnedWriteBatch) -> Result<()> {
        batch.commit()?;
        Ok(())
    }

    pub fn commit(&self, batch: WriteBatch) -> Result<()> {
        let WriteBatch {
            inner,
            pending_quad_states: _,
            pending_terms: _,
            quad_mutations,
        } = batch;
        self.apply_quad_mutations(inner, quad_mutations)
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
        assert_eq!(queued[0].0, graph);
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
            queued[0].1
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
        assert_eq!(vec![(graph.clone(), reindex_token)], reindex_queued);
        store
            .acknowledge_fts_subjects_for_reindexed_graphs(&reindex_queued)
            .unwrap();

        let remaining = store.drain_fts_queue(10).unwrap();
        assert_eq!(
            1,
            remaining.len(),
            "a subject queued after the reindex must survive its acknowledgement"
        );
        assert_eq!(subject, remaining[0].1);
    }

    // ── Vector-clock key split ──────────────────────────────────────────

    #[test]
    fn clock_split_migration() {
        let (_dir, store) = setup_store();
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
            .map(|(_, subject, _)| subject)
            .collect();
        assert_eq!(
            vec![subject_id],
            queued,
            "the newly orphaned entity must be re-queued for search at open"
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

        store.clear_fts_queue_for_graph(&graph).unwrap();
        assert!(store.drain_fts_queue(10).unwrap().is_empty());
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
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
