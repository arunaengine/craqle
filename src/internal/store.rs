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
const GRAPH_SEARCH_DELETE_PREFIX: u8 = b'X';
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
    #[serde(default)]
    irokle_topic: Option<[u8; 32]>,
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
    for chunk in bytes[1..].chunks_exact(40) {
        dots.push(Dot {
            actor: ActorId::from_bytes(chunk[..32].try_into().unwrap()),
            counter: u64::from_be_bytes(chunk[32..40].try_into().unwrap()),
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

fn log_head_key(graph: TermId, actor: &ActorId) -> [u8; 49] {
    let mut key = [0u8; 49];
    key[0] = LOG_HEAD_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..49].copy_from_slice(actor.as_bytes());
    key
}

fn log_batch_key(graph: TermId, actor: &ActorId, counter: u64) -> [u8; 57] {
    let mut key = [0u8; 57];
    key[0] = LOG_BATCH_PREFIX;
    key[1..17].copy_from_slice(&graph.to_be_bytes());
    key[17..49].copy_from_slice(actor.as_bytes());
    key[49..57].copy_from_slice(&counter.to_be_bytes());
    key
}

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
        let worker_threads = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .min(32);
        #[allow(deprecated)]
        let db = Database::builder(path.as_ref())
            .manual_journal_persist(true)
            .cache_size(recommended_db_cache_bytes())
            .journal_compression(CompressionType::None)
            .max_journaling_size(16 * 1_024 * 1_024 * 1_024)
            .max_write_buffer_size(Some(24 * 1_024 * 1_024 * 1_024))
            .worker_threads(worker_threads)
            .open()?;
        Self::from_database(db)
    }

    pub fn from_database(db: Database) -> Result<Self> {
        let point_read_heavy = || {
            KeyspaceCreateOptions::default()
                .expect_point_read_hits(true)
                .max_memtable_size(256 * 1_024 * 1_024)
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
            term_locks: (0..TERM_LOCK_SHARDS).map(|_| Mutex::new(())).collect(),
            indexes: RwLock::new(IndexState::default()),
            derived_indexes: RwLock::new(None),
            object_order_cache: RwLock::new(HashMap::new()),
            diagnostics_cache: RwLock::new(HashMap::new()),
            dirty_counter: AtomicU64::new(1),
        };

        store.rebuild_indexes()?;
        store.prime_graph_diagnostics()?;
        Ok(store)
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn manual_compact(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncData)?;
        for keyspace in [&self.terms, &self.quads, &self.graphs, &self.log] {
            keyspace.major_compact()?;
        }
        self.db.persist(PersistMode::SyncData)?;
        Ok(())
    }

    pub fn encode_term(&self, term: &EncodedTerm) -> Result<TermId> {
        self.encode_term_internal(None, term)
    }

    pub fn resolve_term_cached(
        &self,
        batch: &mut WriteBatch,
        cache: &mut HashMap<String, TermId>,
        term: &EncodedTerm,
    ) -> Result<TermId> {
        if let Some(&id) = cache.get(term.0.as_str()) {
            return Ok(id);
        }
        let id = self.encode_term_internal(Some(batch), term)?;
        cache.insert(term.0.clone(), id);
        Ok(id)
    }

    pub fn seed_term_cache<'a, I>(
        &self,
        batch: &mut WriteBatch,
        cache: &mut HashMap<String, TermId>,
        terms: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a EncodedTerm>,
    {
        for term in terms {
            if cache.contains_key(term.0.as_str()) {
                continue;
            }
            let id = self.encode_term_internal(Some(batch), term)?;
            cache.insert(term.0.clone(), id);
        }
        Ok(())
    }

    pub fn decode_term(&self, id: TermId) -> Result<EncodedTerm> {
        match self.terms.get(id.to_be_bytes())? {
            Some(bytes) => Ok(EncodedTerm(String::from_utf8(bytes.to_vec()).map_err(
                |error| StoreError::InvalidEncoding {
                    context: "terms",
                    message: error.to_string(),
                },
            )?)),
            None => Err(StoreError::TermNotFound(id.0)),
        }
    }

    pub fn lookup_term(&self, term: &EncodedTerm) -> Result<Option<TermId>> {
        let id = hash_term(term);
        let Some(existing) = self.terms.get(id.to_be_bytes())? else {
            return Ok(None);
        };
        let existing =
            String::from_utf8(existing.to_vec()).map_err(|error| StoreError::InvalidEncoding {
                context: "terms",
                message: error.to_string(),
            })?;
        if existing == term.0 {
            return Ok(Some(id));
        }
        Err(StoreError::TermCollision {
            attempted: term.0.clone(),
            existing,
        })
    }

    pub fn resolve_term(&self, term: &EncodedTerm) -> Result<TermId> {
        self.encode_term(term)
    }

    pub fn create_graph(&self, graph: &GraphId) -> Result<()> {
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
        Ok(self.read_graph_meta_by_id(graph_id)?.is_some())
    }

    pub fn delete_graph(&self, graph: &GraphId) -> Result<()> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(());
        };

        let mut batch = self.new_batch();
        self.for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
            self.write_quad_state(&mut batch, quad, Vec::new())?;
            Ok(())
        })?;

        batch.remove(&self.graphs, graph_meta_key(graph_id));
        for guard in self.graphs.prefix(graph_dirty_graph_prefix(graph_id)) {
            let (key, _) = guard.into_inner()?;
            batch.remove(&self.graphs, key);
        }

        let reindex_key = graph_reindex_key(graph_id);
        if self.graphs.get(reindex_key)?.is_some() {
            batch.remove(&self.graphs, reindex_key);
        }

        let delete_token = self.dirty_counter.fetch_add(1, Ordering::SeqCst);
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
        self.diagnostics_cache.write().unwrap().remove(&graph_id);
        self.object_order_cache
            .write()
            .unwrap()
            .retain(|(graph_term, _, _), _| *graph_term != graph_id);
        Ok(())
    }

    pub fn graph_is_empty(&self, graph: &GraphId) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(true);
        };
        let indexes = self.indexes.read().unwrap();
        Ok(indexes
            .graph_subjects
            .get(&graph_id)
            .is_none_or(Vec::is_empty))
    }

    pub fn contains_subject(&self, graph: &GraphId, subject: &EncodedTerm) -> Result<bool> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(false);
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(false);
        };

        let indexes = self.indexes.read().unwrap();
        Ok(indexes
            .by_graph_subject
            .contains_key(&(graph_id, subject_id)))
    }

    pub fn graphs(&self) -> Result<Vec<GraphId>> {
        let mut graphs = Vec::new();
        for guard in self.graphs.prefix(graph_meta_prefix()) {
            let (key, _) = guard.into_inner()?;
            if key.len() != 17 {
                continue;
            }
            let graph_id = decode_term_id(&key[1..17], "graph meta key")?;
            let term = self.decode_term(graph_id)?;
            if let Some(named_node) = term.to_named_node() {
                graphs.push(GraphId(named_node));
            }
        }
        Ok(graphs)
    }

    pub fn set_graph_diagnostics(
        &self,
        graph: &GraphId,
        diagnostics: &GraphDiagnostics,
    ) -> Result<()> {
        let graph_id = self.encode_term(&EncodedTerm::from_named_node(&graph.0))?;
        self.diagnostics_cache
            .write()
            .unwrap()
            .insert(graph_id, diagnostics.clone());
        Ok(())
    }

    pub fn graph_diagnostics(&self, graph: &GraphId) -> Result<GraphDiagnostics> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphDiagnostics::default());
        };

        if let Some(cached) = self.diagnostics_cache.read().unwrap().get(&graph_id) {
            return Ok(cached.clone());
        }

        let diagnostics = self.compute_graph_diagnostics(graph)?;
        self.diagnostics_cache
            .write()
            .unwrap()
            .insert(graph_id, diagnostics.clone());
        Ok(diagnostics)
    }

    pub fn set_graph_policy(&self, graph: &GraphId, policy: &GraphPolicy) -> Result<()> {
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

    pub fn set_irokle_topic_id(&self, graph: &GraphId, topic_id: [u8; 32]) -> Result<()> {
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
        self.commit(batch)
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

        let mut quads = Vec::new();
        let mut term_cache = HashMap::new();
        self.for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
            quads.push(SnapshotQuadState {
                subject: self.decode_term_cached(&mut term_cache, quad.subject)?,
                predicate: self.decode_term_cached(&mut term_cache, quad.predicate)?,
                object: self.decode_term_cached(&mut term_cache, quad.object)?,
                dots: self.read_quad_dots(&Self::quad_key(
                    graph_id,
                    quad.subject,
                    quad.predicate,
                    quad.object,
                ))?,
            });
            Ok(())
        })?;

        Ok(GraphReplicaSnapshot {
            graph: graph.clone(),
            clock: vector_clock,
            quads,
        })
    }

    pub fn compact_graph_snapshot(&self, graph: &GraphId) -> Result<GraphReplicaCompactSnapshot> {
        let vector_clock = self.get_vector_clock(graph)?;
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(GraphReplicaCompactSnapshot {
                graph: graph.clone(),
                clock: vector_clock,
                terms: Vec::new(),
                quads: Vec::new(),
            });
        };

        let mut term_to_index = HashMap::new();
        let mut terms = Vec::new();
        let mut quads = Vec::new();
        let mut decode_cache = HashMap::new();
        self.for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
            let subject = self.decode_term_cached(&mut decode_cache, quad.subject)?;
            let predicate = self.decode_term_cached(&mut decode_cache, quad.predicate)?;
            let object = self.decode_term_cached(&mut decode_cache, quad.object)?;
            quads.push(CompactSnapshotQuadState {
                subject: intern_snapshot_term(&mut term_to_index, &mut terms, subject),
                predicate: intern_snapshot_term(&mut term_to_index, &mut terms, predicate),
                object: intern_snapshot_term(&mut term_to_index, &mut terms, object),
                dots: self.read_quad_dots(&Self::quad_key(
                    graph_id,
                    quad.subject,
                    quad.predicate,
                    quad.object,
                ))?,
            });
            Ok(())
        })?;

        Ok(GraphReplicaCompactSnapshot {
            graph: graph.clone(),
            clock: vector_clock,
            terms,
            quads,
        })
    }

    pub fn import_graph_snapshot(&self, snapshot: &GraphReplicaSnapshot) -> Result<()> {
        if self.contains_graph(&snapshot.graph)? {
            let existing = self.graph_snapshot(&snapshot.graph)?;
            if !existing.quads.is_empty() || !existing.clock.0.is_empty() {
                return Err(StoreError::SnapshotImport(format!(
                    "graph `{}` already contains data",
                    snapshot.graph.as_str()
                )));
            }
        } else {
            self.create_graph(&snapshot.graph)?;
        }

        let mut batch = self.new_batch();
        let graph_id = self.resolve_term(&EncodedTerm::from_named_node(&snapshot.graph.0))?;
        let mut term_cache = HashMap::new();
        self.seed_term_cache(
            &mut batch,
            &mut term_cache,
            snapshot
                .quads
                .iter()
                .flat_map(|quad| [&quad.subject, &quad.predicate, &quad.object]),
        )?;

        for quad in &snapshot.quads {
            let state = EncodedQuad {
                graph: graph_id,
                subject: self.resolve_term_cached(&mut batch, &mut term_cache, &quad.subject)?,
                predicate: self.resolve_term_cached(
                    &mut batch,
                    &mut term_cache,
                    &quad.predicate,
                )?,
                object: self.resolve_term_cached(&mut batch, &mut term_cache, &quad.object)?,
            };
            self.write_quad_state(&mut batch, state, quad.dots.clone())?;
        }

        self.enqueue_fts_reindex(&mut batch, &snapshot.graph)?;
        self.set_vector_clock(&mut batch, &snapshot.graph, &snapshot.clock)?;
        self.commit(batch)
    }

    pub fn import_compact_graph_snapshot(
        &self,
        snapshot: &GraphReplicaCompactSnapshot,
    ) -> Result<()> {
        if self.contains_graph(&snapshot.graph)? {
            let existing = self.graph_snapshot(&snapshot.graph)?;
            if !existing.quads.is_empty() || !existing.clock.0.is_empty() {
                return Err(StoreError::SnapshotImport(format!(
                    "graph `{}` already contains data",
                    snapshot.graph.as_str()
                )));
            }
        } else {
            self.create_graph(&snapshot.graph)?;
        }

        let mut batch = self.new_batch();
        let graph_id = self.resolve_term(&EncodedTerm::from_named_node(&snapshot.graph.0))?;
        let mut term_cache = HashMap::new();
        self.seed_term_cache(&mut batch, &mut term_cache, snapshot.terms.iter())?;

        let mut term_ids = Vec::with_capacity(snapshot.terms.len());
        for term in &snapshot.terms {
            term_ids.push(self.resolve_term_cached(&mut batch, &mut term_cache, term)?);
        }

        for quad in &snapshot.quads {
            let state = EncodedQuad {
                graph: graph_id,
                subject: *term_ids.get(quad.subject as usize).ok_or_else(|| {
                    StoreError::SnapshotImport(format!(
                        "quad subject index {} out of bounds for {} terms",
                        quad.subject,
                        term_ids.len()
                    ))
                })?,
                predicate: *term_ids.get(quad.predicate as usize).ok_or_else(|| {
                    StoreError::SnapshotImport(format!(
                        "quad predicate index {} out of bounds for {} terms",
                        quad.predicate,
                        term_ids.len()
                    ))
                })?,
                object: *term_ids.get(quad.object as usize).ok_or_else(|| {
                    StoreError::SnapshotImport(format!(
                        "quad object index {} out of bounds for {} terms",
                        quad.object,
                        term_ids.len()
                    ))
                })?,
            };
            self.write_quad_state(&mut batch, state, quad.dots.clone())?;
        }

        self.enqueue_fts_reindex(&mut batch, &snapshot.graph)?;
        self.set_vector_clock(&mut batch, &snapshot.graph, &snapshot.clock)?;
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
        let mut term_cache = HashMap::new();
        self.for_each_quad_in_graph::<StoreError, _>(graph_id, |quad| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(
                self.decode_term_cached(&mut term_cache, quad.subject)?
                    .0
                    .as_bytes(),
            );
            hasher.update(&[0]);
            hasher.update(
                self.decode_term_cached(&mut term_cache, quad.predicate)?
                    .0
                    .as_bytes(),
            );
            hasher.update(&[0]);
            hasher.update(
                self.decode_term_cached(&mut term_cache, quad.object)?
                    .0
                    .as_bytes(),
            );
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
            .map(Vec::len)
            .unwrap_or(0))
    }

    pub fn predicate_object_count_by_ids(
        &self,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<usize> {
        Ok(self.with_derived_indexes(|indexes| {
            indexes
                .by_predicate_object
                .get(&(predicate, object))
                .map(|entries| entries.iter().filter(|(g, _)| *g == graph).count())
                .unwrap_or(0)
        }))
    }

    pub fn apply_subject_predicate_count_deltas(
        &self,
        _batch: &mut WriteBatch,
        _deltas: &HashMap<(TermId, TermId, TermId), i64>,
    ) -> Result<()> {
        Ok(())
    }

    pub fn insert_quad(
        &self,
        batch: &mut WriteBatch,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        dot: &Dot,
    ) -> Result<bool> {
        let key = Self::quad_key(graph, subject, predicate, object);
        let mut dots = self.current_quad_dots(batch, &key)?;
        if dots.contains(dot) {
            return Ok(false);
        }
        dots.push(*dot);
        self.write_quad_state(
            batch,
            EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            },
            dots,
        )
    }

    pub fn remove_quad(
        &self,
        batch: &mut WriteBatch,
        graph: TermId,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        witnessed: &VectorClock,
    ) -> Result<bool> {
        let key = Self::quad_key(graph, subject, predicate, object);
        let mut dots = self.current_quad_dots(batch, &key)?;
        let before = dots.len();
        dots.retain(|dot| !witnessed.contains(dot));
        if before == dots.len() {
            return Ok(false);
        }
        self.write_quad_state(
            batch,
            EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            },
            dots,
        )
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
            (None, None, Some(predicate), None) => self.with_derived_indexes(|indexes| {
                let mut quads = Vec::new();
                for (&subject, entries) in &indexes.by_subject {
                    for &(candidate_predicate, candidate_object, graph) in entries {
                        if candidate_predicate != predicate {
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
            }),
            (None, None, None, None) => {
                let graph_ids = self
                    .indexes
                    .read()
                    .unwrap()
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
        let subjects = self
            .indexes
            .read()
            .unwrap()
            .graph_subjects
            .get(&graph)
            .cloned()
            .unwrap_or_default();
        for subject in subjects {
            for quad in self.graph_subject_quads(graph, subject, None, None) {
                visit(quad)?;
            }
        }
        Ok(())
    }

    pub fn get_vector_clock(&self, graph: &GraphId) -> Result<VectorClock> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(VectorClock::new());
        };
        Ok(self
            .read_graph_meta_by_id(graph_id)?
            .unwrap_or_default()
            .clock)
    }

    pub fn set_vector_clock(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        clock: &VectorClock,
    ) -> Result<()> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        let mut meta = self.read_graph_meta_by_id(graph_id)?.unwrap_or_default();
        meta.clock = clock.clone();
        batch.insert(
            &self.graphs,
            graph_meta_key(graph_id),
            postcard::to_allocvec(&meta)?,
        );
        Ok(())
    }

    pub fn next_counter(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        actor: &ActorId,
    ) -> Result<u64> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        let key = log_head_key(graph_id, actor);
        let counter = match self.log.get(key)? {
            Some(value) => decode_u64_bytes(value.as_ref(), "log head")? + 1,
            None => 1,
        };
        batch.insert(&self.log, key, counter.to_be_bytes());
        Ok(counter)
    }

    pub(crate) fn append_compact_batch_log(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        stored_batch: &StoredBatch,
    ) -> Result<()> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        batch.insert(
            &self.log,
            log_batch_key(graph_id, &stored_batch.actor, stored_batch.counter),
            encode_stored_batch(stored_batch)?,
        );
        Ok(())
    }

    pub fn batch_log_entry(
        &self,
        graph: &GraphId,
        actor: ActorId,
        counter: u64,
    ) -> Result<Option<crate::core::Batch>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(None);
        };
        self.log
            .get(log_batch_key(graph_id, &actor, counter))?
            .map(|bytes| self.decode_batch_log_bytes(graph, bytes.as_ref()))
            .transpose()
    }

    pub fn batches_beyond_vector_clock(
        &self,
        graph: &GraphId,
        vector_clock: &VectorClock,
    ) -> Result<Vec<crate::core::Batch>> {
        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(Vec::new());
        };

        let mut batches = Vec::new();
        let mut term_cache = HashMap::new();
        for guard in self.log.prefix(log_batch_prefix(graph_id)) {
            let (key, value) = guard.into_inner()?;
            if key.len() != 57 {
                continue;
            }
            let actor = ActorId::from_bytes(key[17..49].try_into().unwrap());
            let counter = u64::from_be_bytes(key[49..57].try_into().unwrap());
            if vector_clock.contains(&Dot { actor, counter }) {
                continue;
            }
            batches.push(self.decode_batch_log_bytes_with_cache(
                graph,
                value.as_ref(),
                &mut term_cache,
            )?);
        }
        Ok(batches)
    }

    fn decode_batch_log_bytes(&self, graph: &GraphId, bytes: &[u8]) -> Result<crate::core::Batch> {
        let mut term_cache = HashMap::new();
        self.decode_batch_log_bytes_with_cache(graph, bytes, &mut term_cache)
    }

    fn decode_batch_log_bytes_with_cache(
        &self,
        graph: &GraphId,
        bytes: &[u8],
        term_cache: &mut HashMap<TermId, EncodedTerm>,
    ) -> Result<crate::core::Batch> {
        if bytes.first().copied() != Some(BATCH_LOG_ENCODING_TAG) {
            return Ok(postcard::from_bytes(bytes)?);
        }

        let stored: StoredBatch = postcard::from_bytes(&bytes[1..])?;
        let mut ops = Vec::with_capacity(stored.ops.len());
        for op in stored.ops {
            match op {
                StoredQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => ops.push(QuadOp::Add {
                    subject: self.decode_term_cached(term_cache, subject)?,
                    predicate: self.decode_term_cached(term_cache, predicate)?,
                    object: self.decode_term_cached(term_cache, object)?,
                    dot,
                }),
                StoredQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => ops.push(QuadOp::Remove {
                    subject: self.decode_term_cached(term_cache, subject)?,
                    predicate: self.decode_term_cached(term_cache, predicate)?,
                    object: self.decode_term_cached(term_cache, object)?,
                    witnessed,
                }),
            }
        }

        Ok(crate::core::Batch {
            graph: graph.clone(),
            actor: stored.actor,
            counter: stored.counter,
            base_clock: stored.base_clock,
            ops,
            timestamp: stored.timestamp,
        })
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

    pub fn enqueue_fts(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        subject: TermId,
    ) -> Result<()> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        self.enqueue_fts_by_graph_id(batch, graph_id, subject)
    }

    fn enqueue_fts_by_graph_id(
        &self,
        batch: &mut WriteBatch,
        graph_id: TermId,
        subject: TermId,
    ) -> Result<()> {
        let token = self.dirty_counter.fetch_add(1, Ordering::SeqCst);
        batch.insert(
            &self.graphs,
            graph_dirty_key(graph_id, subject),
            token.to_be_bytes(),
        );
        Ok(())
    }

    pub fn enqueue_fts_subjects(
        &self,
        batch: &mut WriteBatch,
        graph: &GraphId,
        subjects: &HashSet<TermId>,
    ) -> Result<()> {
        if subjects.is_empty() {
            return Ok(());
        }

        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        if subjects.len() >= FTS_GRAPH_REINDEX_SUBJECT_THRESHOLD {
            return self.enqueue_fts_reindex_by_graph_id(batch, graph_id);
        }

        for subject in subjects {
            self.enqueue_fts_by_graph_id(batch, graph_id, *subject)?;
        }
        Ok(())
    }

    pub fn enqueue_fts_reindex(&self, batch: &mut WriteBatch, graph: &GraphId) -> Result<()> {
        let graph_id =
            self.encode_term_internal(Some(batch), &EncodedTerm::from_named_node(&graph.0))?;
        self.enqueue_fts_reindex_by_graph_id(batch, graph_id)
    }

    fn enqueue_fts_reindex_by_graph_id(
        &self,
        batch: &mut WriteBatch,
        graph_id: TermId,
    ) -> Result<()> {
        let token = self.dirty_counter.fetch_add(1, Ordering::SeqCst);
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
        self.db.persist(PersistMode::SyncData)?;
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
            touched_graphs,
        } = batch;
        inner.commit()?;
        self.apply_quad_mutations(quad_mutations, touched_graphs);
        Ok(())
    }

    pub fn triples_for_subject(
        &self,
        graph: TermId,
        subject: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        let predicates = self
            .indexes
            .read()
            .unwrap()
            .by_graph_subject
            .get(&(graph, subject))
            .cloned()
            .unwrap_or_default();
        let mut triples = Vec::new();
        for (predicate, object) in predicates {
            let predicate_term = self.decode_term(predicate)?;
            triples.push((predicate_term, self.decode_term(object)?));
        }
        Ok(triples)
    }

    pub fn triples_for_subject_excluding_predicate(
        &self,
        graph: TermId,
        subject: TermId,
        excluded_predicate: TermId,
    ) -> Result<Vec<(EncodedTerm, EncodedTerm)>> {
        let predicates = self
            .indexes
            .read()
            .unwrap()
            .by_graph_subject
            .get(&(graph, subject))
            .cloned()
            .unwrap_or_default();
        let mut triples = Vec::new();
        for (predicate, object) in predicates {
            if predicate == excluded_predicate {
                continue;
            }
            let predicate_term = self.decode_term(predicate)?;
            triples.push((predicate_term, self.decode_term(object)?));
        }
        Ok(triples)
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

    pub fn objects_for_subject_predicate_page(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        offset: usize,
        limit: usize,
    ) -> Result<(usize, Vec<EncodedTerm>)> {
        if limit == 0 {
            return Ok((0, Vec::new()));
        }

        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok((0, Vec::new()));
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok((0, Vec::new()));
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok((0, Vec::new()));
        };

        let object_ids =
            self.ordered_objects_for_subject_predicate(graph_id, subject_id, predicate_id)?;
        let total = object_ids.len();
        let page = object_ids
            .iter()
            .skip(offset)
            .take(limit)
            .map(|object| self.decode_term(*object))
            .collect::<Result<Vec<_>>>()?;
        Ok((total, page))
    }

    pub fn objects_for_subject_predicate_page_after(
        &self,
        graph: &GraphId,
        subject: &EncodedTerm,
        predicate: &EncodedTerm,
        after: Option<&EncodedTerm>,
        limit: usize,
    ) -> Result<Vec<EncodedTerm>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let Some(graph_id) = self.graph_id_for(graph)? else {
            return Ok(Vec::new());
        };
        let Some(subject_id) = self.lookup_term(subject)? else {
            return Ok(Vec::new());
        };
        let Some(predicate_id) = self.lookup_term(predicate)? else {
            return Ok(Vec::new());
        };
        let object_ids =
            self.ordered_objects_for_subject_predicate(graph_id, subject_id, predicate_id)?;
        let start = match after {
            Some(after) => match self.lookup_term(after)? {
                Some(after_id) => object_ids
                    .iter()
                    .position(|object| *object == after_id)
                    .map(|index| index + 1)
                    .unwrap_or(0),
                None => 0,
            },
            None => 0,
        };

        object_ids
            .iter()
            .skip(start)
            .take(limit)
            .map(|object| self.decode_term(*object))
            .collect()
    }
}

fn intern_snapshot_term(
    term_to_index: &mut HashMap<EncodedTerm, u32>,
    terms: &mut Vec<EncodedTerm>,
    term: EncodedTerm,
) -> u32 {
    if let Some(&index) = term_to_index.get(&term) {
        return index;
    }

    let index = terms.len() as u32;
    term_to_index.insert(term.clone(), index);
    terms.push(term);
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_store() -> (tempfile::TempDir, GraphStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        (dir, store)
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
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject_id = store.resolve_term(subject).unwrap();
        let predicate_id = store.resolve_term(predicate).unwrap();
        let object_id = store.resolve_term(object).unwrap();
        store
            .insert_quad(
                &mut batch,
                graph_id,
                subject_id,
                predicate_id,
                object_id,
                &dot,
            )
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
    fn fts_dirty_set_deduplicates_subjects() {
        let (_dir, store) = setup_store();
        let graph = GraphId::new("urn:test:graph");
        store.create_graph(&graph).unwrap();
        let subject = store
            .resolve_term(&EncodedTerm::from_named_node(
                &oxrdf::NamedNode::new_unchecked("urn:test:subject"),
            ))
            .unwrap();

        let mut batch = store.new_batch();
        store.enqueue_fts(&mut batch, &graph, subject).unwrap();
        store.enqueue_fts(&mut batch, &graph, subject).unwrap();
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

        let mut batch = store.new_batch();
        store.enqueue_fts_reindex(&mut batch, &graph).unwrap();
        store.commit(batch).unwrap();

        let queued = store.drain_fts_reindex_queue(10).unwrap();
        assert_eq!(1, queued.len());
        assert_eq!(queued[0].0, graph);
        store.acknowledge_fts_reindex_queue(&queued).unwrap();
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
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

        let mut batch = store.new_batch();
        store.enqueue_fts(&mut batch, &graph, subject).unwrap();
        store.enqueue_fts_reindex(&mut batch, &graph).unwrap();
        store.commit(batch).unwrap();

        store.clear_fts_queue_for_graph(&graph).unwrap();
        assert!(store.drain_fts_queue(10).unwrap().is_empty());
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
    }
}
