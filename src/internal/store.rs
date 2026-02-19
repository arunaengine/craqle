use std::collections::{BTreeSet, HashMap, HashSet, hash_map::Entry};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::core::*;
use chrono::{DateTime, Utc};
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
    #[error("snapshot import failed: {0}")]
    SnapshotImport(String),
    #[error("invalid stored encoding for {context}: {message}")]
    InvalidEncoding {
        context: &'static str,
        message: String,
    },
}

pub type Result<T> = std::result::Result<T, StoreError>;

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
const BATCH_LOG_ENCODING_TAG: u8 = b'B';
const GRAPH_META_PREFIX: u8 = b'M';
const GRAPH_DIRTY_PREFIX: u8 = b'D';
const GRAPH_REINDEX_PREFIX: u8 = b'R';
const LOG_HEAD_PREFIX: u8 = b'H';
const LOG_BATCH_PREFIX: u8 = b'B';
const TERM_LOCK_SHARDS: usize = 64;
const FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD: usize = 10_000;
const DEFAULT_DB_CACHE_BYTES: u64 = 1_024 * 1_024 * 1_024;
const MAX_DB_CACHE_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;
const WRITE_HEAVY_MEMTABLE_BYTES: u64 = 1_024 * 1_024 * 1_024;
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum StoredQuadOp {
    Add {
        subject: TermId,
        predicate: TermId,
        object: TermId,
        dot: Dot,
    },
    Remove {
        subject: TermId,
        predicate: TermId,
        object: TermId,
        witnessed: VectorClock,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredBatch {
    pub(crate) actor: ActorId,
    pub(crate) counter: u64,
    pub(crate) base_clock: VectorClock,
    pub(crate) ops: Vec<StoredQuadOp>,
    pub(crate) timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct StoredGraphMeta {
    policy: GraphPolicy,
    clock: VectorClock,
}

#[derive(Debug, Clone)]
enum QuadMutation {
    Insert(EncodedQuad),
    Remove(EncodedQuad),
}

pub struct WriteBatch {
    inner: fjall::OwnedWriteBatch,
    pending_quad_states: HashMap<Vec<u8>, Option<Vec<Dot>>>,
    pending_terms: HashMap<TermId, String>,
    quad_mutations: Vec<QuadMutation>,
    touched_graphs: HashSet<TermId>,
}

impl WriteBatch {
    fn new(inner: fjall::OwnedWriteBatch) -> Self {
        Self {
            inner,
            pending_quad_states: HashMap::new(),
            pending_terms: HashMap::new(),
            quad_mutations: Vec::new(),
            touched_graphs: HashSet::new(),
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

#[derive(Default)]
struct IndexState {
    graph_subjects: HashMap<TermId, Vec<TermId>>,
    by_graph_subject: HashMap<(TermId, TermId), Vec<(TermId, TermId)>>,
}

type ObjectOrderKey = (TermId, TermId, TermId);
type ObjectOrderValues = Arc<Vec<TermId>>;
type ObjectOrderCache = HashMap<ObjectOrderKey, ObjectOrderValues>;

impl IndexState {
    fn insert_quad(&mut self, quad: EncodedQuad) {
        match self.by_graph_subject.entry((quad.graph, quad.subject)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push((quad.predicate, quad.object));
            }
            Entry::Vacant(entry) => {
                self.graph_subjects
                    .entry(quad.graph)
                    .or_default()
                    .push(quad.subject);
                entry.insert(vec![(quad.predicate, quad.object)]);
            }
        }
    }

    fn remove_quad(&mut self, quad: EncodedQuad) {
        if let Some(entries) = self.by_graph_subject.get_mut(&(quad.graph, quad.subject))
            && let Some(index) = entries
                .iter()
                .position(|entry| *entry == (quad.predicate, quad.object))
        {
            entries.swap_remove(index);
        }
    }
}

#[derive(Default)]
struct DerivedIndexState {
    by_subject: HashMap<TermId, Vec<(TermId, TermId, TermId)>>,
    by_predicate_object: HashMap<(TermId, TermId), Vec<(TermId, TermId)>>,
    by_object: HashMap<TermId, Vec<(TermId, TermId, TermId)>>,
}

impl DerivedIndexState {
    fn insert_quad(&mut self, quad: EncodedQuad) {
        self.by_subject.entry(quad.subject).or_default().push((
            quad.predicate,
            quad.object,
            quad.graph,
        ));
        self.by_predicate_object
            .entry((quad.predicate, quad.object))
            .or_default()
            .push((quad.graph, quad.subject));
        self.by_object.entry(quad.object).or_default().push((
            quad.graph,
            quad.subject,
            quad.predicate,
        ));
    }

    fn remove_quad(&mut self, quad: EncodedQuad) {
        if let Some(entries) = self.by_subject.get_mut(&quad.subject) {
            if let Some(index) = entries
                .iter()
                .position(|entry| *entry == (quad.predicate, quad.object, quad.graph))
            {
                entries.swap_remove(index);
            }
            if entries.is_empty() {
                self.by_subject.remove(&quad.subject);
            }
        }

        if let Some(entries) = self
            .by_predicate_object
            .get_mut(&(quad.predicate, quad.object))
        {
            if let Some(index) = entries
                .iter()
                .position(|entry| *entry == (quad.graph, quad.subject))
            {
                entries.swap_remove(index);
            }
            if entries.is_empty() {
                self.by_predicate_object
                    .remove(&(quad.predicate, quad.object));
            }
        }

        if let Some(entries) = self.by_object.get_mut(&quad.object) {
            if let Some(index) = entries
                .iter()
                .position(|entry| *entry == (quad.graph, quad.subject, quad.predicate))
            {
                entries.swap_remove(index);
            }
            if entries.is_empty() {
                self.by_object.remove(&quad.object);
            }
        }
    }
}

pub struct GraphStore {
    db: Database,
    terms: Keyspace,
    quads: Keyspace,
    graphs: Keyspace,
    log: Keyspace,
    term_locks: Vec<Mutex<()>>,
    indexes: RwLock<IndexState>,
    derived_indexes: RwLock<Option<DerivedIndexState>>,
    object_order_cache: RwLock<ObjectOrderCache>,
    diagnostics_cache: RwLock<HashMap<TermId, GraphDiagnostics>>,
    dirty_counter: AtomicU64,
}

fn decode_u64_bytes(bytes: &[u8], context: &'static str) -> Result<u64> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| StoreError::InvalidEncoding {
        context,
        message: format!("expected 8 bytes, found {}", bytes.len()),
    })?;
    Ok(u64::from_be_bytes(raw))
}

fn encode_dots(dots: &[Dot]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + dots.len() * 24);
    bytes.push(DOT_ENCODING_TAG);
    for dot in dots {
        bytes.extend_from_slice(dot.actor.0.as_bytes());
        bytes.extend_from_slice(&dot.counter.to_be_bytes());
    }
    bytes
}

fn decode_dots(bytes: &[u8]) -> Result<Vec<Dot>> {
    if bytes.first().copied() != Some(DOT_ENCODING_TAG) {
        return Ok(postcard::from_bytes(bytes)?);
    }
    if !(bytes.len() - 1).is_multiple_of(24) {
        return Err(StoreError::InvalidEncoding {
            context: "quad dots",
            message: format!("invalid dot payload length {}", bytes.len()),
        });
    }

    let mut dots = Vec::with_capacity((bytes.len() - 1) / 24);
    for chunk in bytes[1..].chunks_exact(24) {
        dots.push(Dot {
            actor: ActorId(uuid::Uuid::from_bytes(chunk[..16].try_into().unwrap())),
            counter: u64::from_be_bytes(chunk[16..24].try_into().unwrap()),
        });
    }
    Ok(dots)
}

fn normalize_dots(dots: &mut Vec<Dot>) {
    dots.sort_by_key(|dot| (dot.actor, dot.counter));
    dots.dedup();
}

fn encode_stored_batch(stored_batch: &StoredBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(1 + stored_batch.ops.len() * 64);
    bytes.push(BATCH_LOG_ENCODING_TAG);
    bytes.extend_from_slice(&postcard::to_allocvec(stored_batch)?);
    Ok(bytes)
}

fn hash_term(term: &EncodedTerm) -> TermId {
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

fn graph_meta_prefix() -> [u8; 1] {
    [GRAPH_META_PREFIX]
}

fn log_head_key(graph: TermId, actor: &ActorId) -> [u8; 33] {
    let mut key = [0u8; 33];
    key[0] = LOG_HEAD_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..33].copy_from_slice(actor.0.as_bytes());
    key
}

fn log_batch_key(graph: TermId, actor: &ActorId, counter: u64) -> [u8; 41] {
    let mut key = [0u8; 41];
    key[0] = LOG_BATCH_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..33].copy_from_slice(actor.0.as_bytes());
    key[33..41].copy_from_slice(&counter.to_be_bytes());
    key
}

fn log_batch_prefix(graph: TermId) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[0] = LOG_BATCH_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key
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
            let existing = String::from_utf8(existing.to_vec()).map_err(|error| {
                StoreError::InvalidEncoding {
                    context: "terms",
                    message: error.to_string(),
                }
            })?;
            if existing == term.0 {
                return Ok(id);
            }
            return Err(StoreError::TermCollision {
                attempted: term.0.clone(),
                existing,
            });
        }

        let _guard = self.term_locks[self.term_lock_index(id)].lock().unwrap();
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
            let existing = String::from_utf8(existing.to_vec()).map_err(|error| {
                StoreError::InvalidEncoding {
                    context: "terms",
                    message: error.to_string(),
                }
            })?;
            if existing == term.0 {
                return Ok(id);
            }
            return Err(StoreError::TermCollision {
                attempted: term.0.clone(),
                existing,
            });
        }

        if let Some(batch) = batch {
            batch.insert(&self.terms, key, term.0.as_bytes());
            batch.pending_terms.insert(id, term.0.clone());
        } else {
            self.terms.insert(key, term.0.as_bytes())?;
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

    fn write_graph_meta_immediate(&self, graph: TermId, meta: &StoredGraphMeta) -> Result<()> {
        self.graphs
            .insert(graph_meta_key(graph), postcard::to_allocvec(meta)?)?;
        Ok(())
    }

    fn read_quad_dots(&self, key: &[u8]) -> Result<Vec<Dot>> {
        match self.quads.get(key)? {
            Some(bytes) => decode_dots(bytes.as_ref()),
            None => Ok(Vec::new()),
        }
    }

    fn current_quad_dots(&self, batch: &WriteBatch, key: &[u8]) -> Result<Vec<Dot>> {
        if let Some(state) = batch.pending_quad_states.get(key) {
            return Ok(state.clone().unwrap_or_default());
        }
