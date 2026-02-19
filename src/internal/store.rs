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

        batch.touched_graphs.insert(quad.graph);
        batch.pending_quad_states.insert(
            key.to_vec(),
            if is_live { Some(dots.clone()) } else { None },
        );

        if is_live {
            batch.insert(&self.quads, key, encode_dots(&dots));
        } else {
            batch.remove(&self.quads, key);
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
            if decode_dots(value.as_ref())?.is_empty() {
                continue;
            }
            indexes.insert_quad(Self::decode_quad_key(key.as_ref())?);
        }
        Ok(indexes)
    }

    fn rebuild_indexes(&self) -> Result<()> {
        let indexes = self.build_indexes()?;
        *self.indexes.write().unwrap() = indexes;
        *self.derived_indexes.write().unwrap() = None;
        self.object_order_cache.write().unwrap().clear();
        self.diagnostics_cache.write().unwrap().clear();
        Ok(())
    }

    fn apply_quad_mutations(&self, mutations: Vec<QuadMutation>, _touched_graphs: HashSet<TermId>) {
        if mutations.is_empty() {
            return;
        }

        let mut indexes = self.indexes.write().unwrap();
        for mutation in &mutations {
            match mutation {
                QuadMutation::Insert(quad) => indexes.insert_quad(*quad),
                QuadMutation::Remove(quad) => indexes.remove_quad(*quad),
            }
        }
        drop(indexes);

        if let Some(derived) = self.derived_indexes.write().unwrap().as_mut() {
            for mutation in &mutations {
                match mutation {
                    QuadMutation::Insert(quad) => derived.insert_quad(*quad),
                    QuadMutation::Remove(quad) => derived.remove_quad(*quad),
                }
            }
        }

        let mut cache = self.object_order_cache.write().unwrap();
        for mutation in &mutations {
            let quad = match mutation {
                QuadMutation::Insert(quad) | QuadMutation::Remove(quad) => *quad,
            };
            cache.remove(&(quad.graph, quad.subject, quad.predicate));
        }
    }

    fn build_derived_indexes(&self) -> DerivedIndexState {
        let indexes = self.indexes.read().unwrap();
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

    fn with_derived_indexes<R>(&self, f: impl FnOnce(&DerivedIndexState) -> R) -> R {
        {
            let guard = self.derived_indexes.read().unwrap();
            if let Some(derived) = guard.as_ref() {
                return f(derived);
            }
        }

        let mut guard = self.derived_indexes.write().unwrap();
        if guard.is_none() {
            *guard = Some(self.build_derived_indexes());
        }
        f(guard.as_ref().expect("derived indexes initialized"))
    }

    fn compute_graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        let snapshot = crate::rules::GraphSnapshot::from_store(self, graph)?;
        Ok(GraphDiagnostics::from_orphaned_entities(
            crate::rules::orphaned_data_entities(&snapshot)
                .into_iter()
                .map(|term| {
                    term.to_named_node()
                        .map(|named_node| named_node.as_str().to_string())
                        .unwrap_or(term.0)
                })
                .collect(),
        ))
    }

    fn prime_graph_diagnostics(&self) -> Result<()> {
        let mut cache = self.diagnostics_cache.write().unwrap();
        cache.clear();
        for graph in self.graphs()? {
            if let Some(graph_id) = self.graph_id_for(&graph)? {
                cache.insert(graph_id, self.compute_graph_diagnostics(&graph)?);
            }
        }
        Ok(())
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
        let indexes = self.indexes.read().unwrap();
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
        let subject_ids = self
            .indexes
            .read()
            .unwrap()
            .graph_subjects
            .get(&graph)
            .cloned()
            .unwrap_or_default();

        let mut quads = Vec::new();
        for subject in subject_ids {
            quads.extend(self.graph_subject_quads(graph, subject, predicate, object));
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
            let Some(entries) = indexes.by_predicate_object.get(&(predicate, object)) else {
                return Vec::new();
            };

            let mut quads = Vec::new();
            for &(candidate_graph, subject) in entries {
                if graph.is_some_and(|expected| expected != candidate_graph) {
                    continue;
                }
                quads.push(EncodedQuad {
                    graph: candidate_graph,
                    subject,
                    predicate,
                    object,
                });
            }
            quads
        })
    }

    fn object_scan(&self, graph: Option<TermId>, object: TermId) -> Vec<EncodedQuad> {
        self.with_derived_indexes(|indexes| {
            let Some(entries) = indexes.by_object.get(&object) else {
                return Vec::new();
            };

            let mut quads = Vec::new();
            for &(candidate_graph, subject, predicate) in entries {
                if graph.is_some_and(|expected| expected != candidate_graph) {
                    continue;
                }
                quads.push(EncodedQuad {
                    graph: candidate_graph,
                    subject,
                    predicate,
                    object,
                });
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
