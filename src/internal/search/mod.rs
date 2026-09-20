//! Indexes visible RDF documents and drains durable search repair queues.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

pub(crate) mod queue;

use std::borrow::Cow;
use std::collections::BinaryHeap;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
#[cfg(test)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, RwLock};

use tantivy::TERMINATED;
#[cfg(test)]
use tantivy::collector::{BytesFilterCollector, TopDocs};
use tantivy::query::{BooleanQuery, EnableScoring, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    FAST, Field, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT, TextFieldIndexing,
    Value,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{DocAddress, DocId, DocSet, Index, IndexReader, IndexWriter, Score, Searcher};
use tantivy::{TantivyDocument, Term};

use crate::core::{EncodedTerm, GraphId};
use crate::memory::MemoryBudget;
#[cfg(test)]
use crate::search::queue::GraphGeneration;
use crate::search::queue::{
    CleanupFailure, CleanupScan, DirtySubject, DrainControl, DrainProgress, DrainRequest,
    GenerationId, GenerationRequest, GenerationSwitch, GraphScan, ManifestScan, OversizedEntry,
    QueueCursor, QueueId, QueueKind, QueuePage, QueueScan, RebuildRequest, RebuildScan, RetryState,
    StageFailure, StageJob, StageRequest, SubjectScan,
};
pub(crate) use crate::search::queue::{DrainFailure, QueueBound};
use crate::store::{GraphStore, SearchSnapshot, TermId};

const MAX_RETRY_ATTEMPTS: u32 = 8;
const RETRY_BASE_MS: u64 = 250;
const RETRY_MAX_MS: u64 = 60_000;
const SOURCE_PAGE_ROWS: usize = 2_048;
const STAGE_SOURCE_BYTES: usize = 256;
/// Rebuild-lock shards. Comfortably above the indexer's concurrency while
/// staying a fixed, tiny allocation.
const REBUILD_SHARDS: usize = 64;
const ALL_TEXT_TOKENIZER: &str = "craqle_text_v2";
const INDEX_VERSION_FIELD: &str = "_craqle_search_index_v5";
const INDEX_ID_FILE: &str = ".craqle-index-id";
const DIRECT_GENERATION: GenerationId = GenerationId(1);
const GENERATION_FIELD: &str = "doc_generation";
const GENERATION_SCOPE_FIELD: &str = "generation_scope";
const STABLE_KEY_FIELD: &str = "stable_key";

/// Cached predicates whose objects contribute searchable document text.
static SEARCHABLE_PREDICATES: LazyLock<[EncodedTerm; 4]> = LazyLock::new(|| {
    [
        EncodedTerm::from_named_node(&crate::vocab::schema_name()),
        EncodedTerm::from_named_node(&crate::vocab::schema_description()),
        EncodedTerm::from_named_node(&crate::vocab::schema_keywords()),
        EncodedTerm::from_named_node(&crate::vocab::schema_identifier()),
    ]
});

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query parse: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("search maintenance cancelled")]
    Cancelled,
    #[error("search index is not bound to its durable generation state")]
    Unbound,
    #[error("search item uses {bytes} bytes, limit is {limit}")]
    ItemTooLarge { bytes: usize, limit: usize },
    #[error("search item uses {rows} source rows, limit is {limit}")]
    SourceTooLarge { rows: usize, limit: usize },
}

impl SearchError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::QueryParse(_) => crate::CraqleErrorKind::InvalidInput,
            Self::Cancelled => crate::CraqleErrorKind::Cancelled,
            Self::ItemTooLarge { .. } | Self::SourceTooLarge { .. } => {
                crate::CraqleErrorKind::QueryLimit
            }
            Self::Tantivy(_) | Self::Io(_) | Self::Unbound => crate::CraqleErrorKind::Storage,
            Self::Store(error) => error.kind(),
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, SearchError>;

#[derive(Default)]
struct QueueCursors {
    deletes: Option<QueueCursor>,
    reindexes: Option<QueueCursor>,
    subjects: Option<QueueCursor>,
    cleanup: Option<GenerationId>,
}

struct ScanSave<'a, T> {
    kind: QueueKind,
    page: &'a QueuePage<T>,
    started: bool,
}

/// One drain pass: what it may read, and what it has covered so far.
struct DrainPass<'a> {
    store: &'a GraphStore,
    bound: QueueBound,
    control: DrainControl,
    progress: DrainProgress,
    byte_limit: usize,
    advanced: bool,
}

struct GraphWork<'a> {
    store: &'a GraphStore,
    graph: &'a GraphId,
    control: &'a DrainControl,
    byte_limit: usize,
}

struct QueueInput<'a> {
    kind: QueueKind,
    bound: &'a QueueBound,
    byte_limit: usize,
}

struct FailureInput<'a> {
    id: &'a QueueId,
    owed_from: u64,
    target: u64,
}

struct FailedItem<'a> {
    input: FailureInput<'a>,
    error: SearchError,
}

#[derive(Clone, Copy)]
enum FailureClass {
    ItemPermanent,
    ItemRetryable,
    Global,
    Rebuild,
}

#[derive(Clone, Default)]
struct GenerationView {
    by_graph: HashMap<String, GenerationId>,
    active: HashSet<Vec<u8>>,
}

impl GenerationView {
    #[cfg(test)]
    fn from_rows(index_id: [u8; 16], rows: Vec<GraphGeneration>) -> Self {
        let mut view = Self::default();
        view.extend(index_id, rows);
        view
    }

    #[cfg(test)]
    fn extend(&mut self, index_id: [u8; 16], rows: Vec<GraphGeneration>) {
        self.by_graph.extend(rows.into_iter().filter_map(|row| {
            row.active
                .map(|generation| (row.graph.as_str().to_string(), generation))
        }));
        self.active = self
            .by_graph
            .iter()
            .map(|(graph, generation)| generation_scope(index_id, graph, *generation))
            .collect();
    }
}

struct SearchView {
    searcher: Searcher,
    generations: Arc<GenerationView>,
    bound: bool,
}

struct ManifestState {
    generations: GenerationView,
    count: u64,
    hash: [u8; 32],
    epoch: u64,
    valid: bool,
    digest_match: bool,
}

#[cfg(test)]
struct TopRequest<'a> {
    view: &'a SearchView,
    query: &'a dyn Query,
    limit: usize,
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

#[derive(Clone, Debug)]
struct RankedDoc {
    score: Score,
    stable: [u8; 32],
    address: DocAddress,
}

impl PartialEq for RankedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score).is_eq()
            && self.stable == other.stable
            && self.address == other.address
    }
}

impl Eq for RankedDoc {}

impl PartialOrd for RankedDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.stable.cmp(&other.stable))
            .then_with(|| self.address.cmp(&other.address))
    }
}

struct StageSource {
    snapshot: SearchSnapshot,
    orphaned: HashSet<TermId>,
    bytes: usize,
}

enum StageOutcome {
    Pending(usize),
    Covered(u64),
}

struct StageSubject<'a> {
    store: &'a GraphStore,
    source: &'a StageSource,
    graph: &'a GraphId,
    graph_tid: TermId,
    subject: TermId,
    generation: GenerationId,
    control: &'a DrainControl,
    byte_limit: usize,
}

struct PreparedStage {
    op: PreparedDocOp,
    rows: usize,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct SearchHit {
    /// Graph that contained the matching subject.
    pub graph_id: String,
    /// Matching RDF subject IRI.
    pub subject_iri: String,
    /// Tantivy relevance score.
    pub score: f32,
}

/// Minimal Tantivy index containing only document identity plus aggregated text.
pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    view: RwLock<Arc<SearchView>>,
    /// Single Tantivy writer, held only for index mutation and poison recovery.
    writer: Mutex<IndexWriter>,
    /// Epoch assigned under the writer lock to every index mutation.
    write_epoch: AtomicU64,
    /// Highest write epoch committed durably and visible to readers.
    committed_epoch: AtomicU64,
    /// Serializes commits after rebuild shards and before the writer lock.
    commit_lock: Mutex<()>,
    /// Graph mutation shards precede commit and writer locks in ascending order.
    rebuild_shards: [Mutex<()>; REBUILD_SHARDS],
    /// Set when a poisoned writer was rolled back and the index therefore owes
    /// the store a full re-derivation. Cleared once that reindex is queued.
    rebuild_owed: AtomicBool,
    scan_cursors: Mutex<QueueCursors>,
    retry_now: AtomicU64,
    fair_cursor: AtomicU64,
    prepared_bytes: usize,
    work_lock: Mutex<()>,
    stage_sources: Mutex<HashMap<GenerationId, Arc<StageSource>>>,
    session: [u8; 16],
    #[cfg(test)]
    hooks: TestHooks,
    needs_rebuild: AtomicBool,
    index_id: [u8; 16],
    f_doc_key: Field,
    f_graph_id: Field,
    f_subject_iri: Field,
    f_all_text: Field,
    f_generation_key: Field,
    f_doc_generation: Field,
    f_generation_scope: Field,
    f_stable_key: Field,
}

/// Interleaving hooks a test arms to pin down a race. Per-index rather than
/// global so concurrent tests cannot arm each other's workers.
#[cfg(test)]
#[derive(Default)]
struct TestHooks {
    /// Makes the next indexer drain cycle panic, proving the worker survives one.
    drain_panic: AtomicBool,
    /// Pauses a rebuild between its clear and its refill.
    rebuild: StallHook,
    /// Holds a completed staged rebuild before publication.
    stage: GateHook,
    /// Holds a committed stage page before its durable cursor advances.
    page: GateHook,
    /// Holds a durable generation switch before queue acknowledgement.
    switch: GateHook,
    /// Holds committed cleanup deletes before durable acknowledgement.
    cleanup: GateHook,
    /// Pauses a commit just before the Tantivy commit it is about to run.
    commit: StallHook,
    /// Index searches run, so a test can prove one request runs one search.
    searches: std::sync::atomic::AtomicUsize,
    /// Stored documents decoded, so a test can bound what a page retains.
    decoded: std::sync::atomic::AtomicUsize,
    /// Every queue entry naming this graph fails, modelling a bad item.
    fail_graph: Mutex<Option<String>>,
}

#[cfg(test)]
#[derive(Default)]
struct GateState {
    armed: bool,
    entered: bool,
    released: bool,
}

#[cfg(test)]
#[derive(Default)]
struct GateHook {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[cfg(test)]
impl GateHook {
    fn arm(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        *state = GateState {
            armed: true,
            entered: false,
            released: false,
        };
    }

    fn run(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.armed {
            return;
        }
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, std::time::Duration::from_secs(10))
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
            assert!(!timeout.timed_out(), "staged rebuild was not released");
        }
        state.armed = false;
    }

    fn wait(&self) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, std::time::Duration::from_secs(10), |state| {
                !state.entered
            })
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.entered && !timeout.timed_out(),
            "staged rebuild did not start"
        );
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.released = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
impl TestHooks {
    fn fail_item(&self, graph: &GraphId) -> Result<()> {
        let armed = self
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if armed.as_deref() == Some(graph.as_str()) {
            return Err(SearchError::Store(
                crate::store::StoreError::InvalidSearchState("search-diagnostics-stale"),
            ));
        }
        Ok(())
    }
}

/// A one-shot pause. `run` sleeps once armed and publishes `entered` for the
/// duration, so another thread can act strictly inside the window.
#[cfg(test)]
#[derive(Default)]
struct StallHook {
    delay: Mutex<Option<std::time::Duration>>,
    entered: AtomicBool,
}

#[cfg(test)]
impl StallHook {
    fn arm(&self, delay: std::time::Duration) {
        *self.delay.lock().unwrap_or_else(PoisonError::into_inner) = Some(delay);
    }

    fn run(&self) {
        let delay = self
            .delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(delay) = delay {
            self.entered.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
            self.entered.store(false, Ordering::SeqCst);
        }
    }

    /// Spin until a stalling thread is inside the window. Panics rather than
    /// hanging if that thread died before entering it.
    fn wait_entered(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !self.entered.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "stall window was never entered"
            );
            std::hint::spin_loop();
            std::thread::yield_now();
        }
    }
}

/// One document to add or replace.
struct ResourceDoc<'a> {
    graph_id: &'a str,
    subject_iri: &'a str,
    all_text: Option<&'a str>,
    /// Delete any existing document with the same key first. Skipped by bulk
    /// reindex, which already dropped every document of the graph.
    delete_existing: bool,
}

/// Full-text query restricted to an explicit set of graph IRIs.
pub struct GraphSetQuery<'a> {
    pub graphs: &'a [GraphId],
    pub query: &'a str,
    pub limit: usize,
}

/// A subject update read from the store, ready to apply to the index.
///
/// Produced with no writer lock held so the (potentially very large) store
/// read phase does not block searches or other index writers.
enum PreparedDocOp {
    /// The subject is gone, orphaned, or its graph is unknown: drop it.
    Delete { doc: DocIdentity },
    /// Replace the subject's document with freshly read text.
    Upsert {
        doc: DocIdentity,
        all_text: Option<String>,
    },
}

impl PreparedDocOp {
    /// Prepared text this op holds, charged against the pass byte budget.
    fn text_bytes(&self) -> usize {
        match self {
            Self::Delete { .. } => 0,
            Self::Upsert { all_text, .. } => all_text.as_ref().map_or(0, String::len),
        }
    }
}

struct DocIdentity {
    graph_iri: String,
    subject_iri: String,
}

/// Store reads needed to prepare one subject's index update.
struct PrepareSubject<'a> {
    store: &'a GraphStore,
    graph: &'a GraphId,
    subject: TermId,
}

#[derive(Default)]
struct StoreSyncCaches {
    orphaned_subjects: HashMap<GraphId, HashSet<String>>,
    graph_terms: HashMap<GraphId, Option<TermId>>,
}

fn build_schema() -> Schema {
    let mut builder = SchemaBuilder::default();
    builder.add_text_field("doc_key", STRING | STORED);
    builder.add_text_field("graph_id", STRING | STORED);
    builder.add_text_field("subject_iri", STRING | STORED);
    builder.add_text_field(
        "all_text",
        TEXT.set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(ALL_TEXT_TOKENIZER)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        ),
    );
    builder.add_text_field(INDEX_VERSION_FIELD, STRING);
    builder.build()
}

fn schema_fields(schema: &Schema) -> tantivy::Result<(Field, Field, Field, Field)> {
    schema.get_field(INDEX_VERSION_FIELD)?;
    Ok((
        schema.get_field("doc_key")?,
        schema.get_field("graph_id")?,
        schema.get_field("subject_iri")?,
        schema.get_field("all_text")?,
    ))
}

fn register_text_analyzer(index: &Index) {
    index.tokenizers().register(
        ALL_TEXT_TOKENIZER,
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build(),
    );
}

fn create_index_dir(dir: &Path, schema: &Schema) -> tantivy::Result<Index> {
    std::fs::create_dir_all(dir).map_err(|e| {
        tantivy::TantivyError::SystemError(format!("failed to create index directory: {e}"))
    })?;
    Index::create_in_dir(dir, schema.clone())
}

fn recreate_index_dir(dir: &Path, schema: &Schema) -> tantivy::Result<Index> {
    if dir.exists() {
        std::fs::remove_dir_all(dir).map_err(|e| {
            tantivy::TantivyError::SystemError(format!("failed to recreate index directory: {e}"))
        })?;
    }
    create_index_dir(dir, schema)
}

impl SearchIndex {
    pub fn ensure_available(&self) -> Result<()> {
        Ok(())
    }

    /// Create or open a persistent index at the given directory path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let schema = build_schema();

        let dir = path.as_ref();
        let (index, needs_rebuild) = if dir.join("meta.json").exists() {
            let index = Index::open_in_dir(dir)?;
            if schema_fields(&index.schema()).is_ok() {
                (index, false)
            } else {
                (recreate_index_dir(dir, &schema)?, true)
            }
        } else {
            (create_index_dir(dir, &schema)?, true)
        };

        register_text_analyzer(&index);
        let (f_doc_key, f_graph_id, f_subject_iri, f_all_text) = schema_fields(&index.schema())?;
        let reader = index.reader()?;
        let writer = index.writer(DISK_INDEX_WRITER_HEAP_BYTES)?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            write_epoch: AtomicU64::new(0),
            committed_epoch: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
            rebuild_shards: std::array::from_fn(|_| Mutex::new(())),
            rebuild_owed: AtomicBool::new(false),
            item_failures: Mutex::new(HashMap::new()),
            #[cfg(test)]
            hooks: TestHooks::default(),
            needs_rebuild,
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
        })
    }

    /// Create an in-memory index (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let schema = build_schema();

        let (f_doc_key, f_graph_id, f_subject_iri, f_all_text) = schema_fields(&schema)?;
        let index = Index::create_in_ram(schema);
        register_text_analyzer(&index);
        let reader = index.reader()?;
        let writer = index.writer(MEMORY_INDEX_WRITER_HEAP_BYTES)?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            write_epoch: AtomicU64::new(0),
            committed_epoch: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
            rebuild_shards: std::array::from_fn(|_| Mutex::new(())),
            rebuild_owed: AtomicBool::new(false),
            item_failures: Mutex::new(HashMap::new()),
            #[cfg(test)]
            hooks: TestHooks::default(),
            needs_rebuild: false,
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
        })
    }

    /// Returns `true` when the on-disk index had to be created or migrated.
    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild
    }

    /// Makes the next indexer drain cycle panic. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_drain_panic(&self) {
        self.hooks.drain_panic.store(true, Ordering::SeqCst);
    }

    /// Consumes a pending injected panic, reporting whether one was armed.
    #[cfg(test)]
    pub(crate) fn take_drain_panic(&self) -> bool {
        self.hooks.drain_panic.swap(false, Ordering::SeqCst)
    }

    /// Pauses the next rebuild between its clear and its refill. Test-only.
    #[cfg(test)]
    pub(crate) fn arm_rebuild_stall(&self, delay: std::time::Duration) {
        self.hooks.rebuild.arm(delay);
    }

    /// Spins until a rebuild is inside that pause. Test-only.
    #[cfg(test)]
    pub(crate) fn await_rebuild_stall(&self) {
        self.hooks.rebuild.wait_entered();
    }

    #[cfg(test)]
    fn arm_stage_gate(&self) {
        self.hooks.stage.arm();
    }

    #[cfg(test)]
    fn await_stage_gate(&self) {
        self.hooks.stage.wait();
    }

    #[cfg(test)]
    fn release_stage_gate(&self) {
        self.hooks.stage.release();
    }

    #[cfg(test)]
    fn arm_page_gate(&self) {
        self.hooks.page.arm();
    }

    #[cfg(test)]
    fn await_page_gate(&self) {
        self.hooks.page.wait();
    }

    #[cfg(test)]
    fn release_page_gate(&self) {
        self.hooks.page.release();
    }

    #[cfg(test)]
    pub(crate) fn arm_switch_gate(&self) {
        self.hooks.switch.arm();
    }

    #[cfg(test)]
    pub(crate) fn await_switch_gate(&self) {
        self.hooks.switch.wait();
    }

    #[cfg(test)]
    pub(crate) fn arm_cleanup_gate(&self) {
        self.hooks.cleanup.arm();
    }

    #[cfg(test)]
    pub(crate) fn await_cleanup_gate(&self) {
        self.hooks.cleanup.wait();
    }

    /// Lock one graph's rebuild shard. See `rebuild_shards` for the order this
    /// must be taken in.
    fn lock_graph(&self, graph_iri: &str) -> MutexGuard<'_, ()> {
        self.rebuild_shards[rebuild_shard(graph_iri)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Lock every shard the given graphs map onto, ascending, so two callers
    /// with overlapping graph sets cannot deadlock against each other.
    fn lock_graphs<'a>(&self, graphs: impl Iterator<Item = &'a str>) -> Vec<MutexGuard<'_, ()>> {
        let mut shards: Vec<usize> = graphs.map(rebuild_shard).collect();
        shards.sort_unstable();
        shards.dedup();
        shards
            .into_iter()
            .map(|shard| {
                self.rebuild_shards[shard]
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
            })
            .collect()
    }

    /// Lock the Tantivy writer, repairing it if a panicking thread poisoned
    /// the mutex.
    ///
    /// `lock().unwrap()` made a single panic anywhere in the process fatal to
    /// the search index for the lifetime of that process: every later lock
    /// panicked in turn, the background indexer died with the first of them,
    /// and the index stopped converging with the store until a restart. The
    /// index is derived state, and derived state gets a prompt automatic
    /// repair — "fixed at next restart" is not a repair.
    fn writer(&self) -> Result<MutexGuard<'_, IndexWriter>> {
        match self.writer.lock() {
            Ok(guard) => Ok(guard),
            Err(poisoned) => self.recover_writer(poisoned.into_inner()),
        }
    }

    /// Roll a poisoned writer back to its last commit and record that the index
    /// owes the store a re-derivation.
    ///
    /// The panic unwound at an unknown point, so uncommitted writer state cannot
    /// be trusted. `rollback` discards it and builds a fresh writer from the same
    /// `Index`, moving the directory lock across. The debt is recorded first, so a
    /// failing rollback leaves the mutex poisoned and the repair is retried.
    fn recover_writer<'a>(
        &'a self,
        mut guard: MutexGuard<'a, IndexWriter>,
    ) -> Result<MutexGuard<'a, IndexWriter>> {
        self.rebuild_owed.store(true, Ordering::SeqCst);
        guard.rollback()?;
        self.writer.clear_poison();
        Ok(guard)
    }

    /// Repair a poisoned writer and durably queue the reindex it owes.
    ///
    /// Runs at the top of every drain, so the indexer's one-second tick is the
    /// detection point. The reindex is queued rather than run inline so it stays
    /// crash-safe and keeps G7's acknowledge-after-commit rule.
    ///
    /// The rebuild it queues carries tokens above the caller's bound, so the
    /// raised target is returned alongside the bound and travels back to the
    /// owning flush in [`DrainProgress::recovery`]. Widening only this call's
    /// local bound lost that obligation as soon as a higher-priority queue
    /// class ended the pass, and the next pass rebuilt the original cutoff.
    fn settle_poisoned_writer(
        &self,
        store: &GraphStore,
        bound: QueueBound,
    ) -> Result<(QueueBound, Option<u64>)> {
        if self.writer.is_poisoned() {
            drop(self.writer()?);
        }
        if !self.rebuild_owed.swap(false, Ordering::SeqCst) {
            return Ok((bound, None));
        }

        if let Err(error) = self.enqueue_full_rebuild(store) {
            // Put the debt back: the next pass, one tick later, retries it.
            self.rebuild_owed.store(true, Ordering::SeqCst);
            return Err(error);
        }

        let raised = bound.max_token.map(|_| store.current_dirty_token());
        Ok((
            QueueBound {
                max_token: raised.or(bound.max_token),
                ..bound
            },
            raised,
        ))
    }

    /// Note that one entry failed, and report it with its attempt count.
    fn record_failure(&self, key: FailureKey, error: &SearchError) -> DrainFailure {
        let mut failures = self
            .item_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = failures.entry(key.clone()).or_insert_with(|| DrainFailure {
            graph: GraphId::new(&key.graph),
            attempts: 0,
            diagnostic: String::new(),
        });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.diagnostic = error.to_string();
        entry.clone()
    }

    fn clear_failure(&self, key: &FailureKey) {
        self.item_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
    }

    fn enqueue_full_rebuild(&self, store: &GraphStore) -> Result<()> {
        let mut batch = store.new_batch();
        for graph_id in store.graph_term_ids()? {
            store.enqueue_fts_reindex(&mut batch, graph_id)?;
        }
        store.commit(batch)?;
        Ok(store.persist()?)
    }

    /// Add or update a document for the given resource.
    ///
    /// Deletes any existing document with the same `subject_iri` in the same
    /// `graph_id` before inserting the new one.
    pub fn index_resource(
        &self,
        graph_id: &str,
        subject_iri: &str,
        all_text: Option<&str>,
    ) -> Result<()> {
        let _rebuild = self.lock_graph(graph_id);
        let mut writer = self.writer()?;
        self.add_document(
            &mut writer,
            ResourceDoc {
                graph_id,
                subject_iri,
                all_text,
                delete_existing: true,
            },
        )
    }

    /// Adds a second document under an existing key, bypassing the delete
    /// that normally keeps one document per key. Test seeding only.
    #[cfg(test)]
    pub(crate) fn seed_duplicate(&self, graph_id: &str, subject_iri: &str) -> Result<()> {
        let _rebuild = self.lock_graph(graph_id);
        let mut writer = self.writer()?;
        self.add_document(
            &mut writer,
            ResourceDoc {
                graph_id,
                subject_iri,
                all_text: None,
                delete_existing: false,
            },
        )
    }

    /// Add `doc` to the index, optionally replacing the document with the same
    /// `(graph, subject)` key first.
    fn add_document(&self, writer: &mut IndexWriter, doc: ResourceDoc<'_>) -> Result<()> {
        let key = doc_key(doc.graph_id, doc.subject_iri);
        if doc.delete_existing {
            writer.delete_term(Term::from_field_text(self.f_doc_key, &key));
        }

        let mut all_text_parts: Vec<&str> = vec![doc.graph_id, doc.subject_iri];
        if let Some(extra) = doc.all_text {
            all_text_parts.push(extra);
        }
        let all_text = all_text_parts.join(" ");

        let mut document = TantivyDocument::default();
        document.add_text(self.f_doc_key, key);
        document.add_text(self.f_graph_id, doc.graph_id);
        document.add_text(self.f_subject_iri, doc.subject_iri);
        document.add_text(self.f_all_text, &all_text);

        writer.add_document(document)?;
        self.write_epoch.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Full-text search across all graphs.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = query_parser.parse_query(&sanitize_query(query))?;

        self.collect_top_docs(&parsed, limit)
    }

    /// Full-text search restricted to a single graph.
    pub fn search_in_graph(
        &self,
        graph_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = query_parser.parse_query(&sanitize_query(query))?;
        let graph_filter = TermQuery::new(
            Term::from_field_text(self.f_graph_id, graph_id),
            IndexRecordOption::Basic,
        );
        let combined = BooleanQuery::new(vec![
            (Occur::Must, parsed),
            (Occur::Must, Box::new(graph_filter)),
        ]);

        self.collect_top_docs(&combined, limit)
    }

    /// Full-text search restricted to an explicit set of graphs.
    ///
    /// One top-k collection over a graph-set filter, instead of one full
    /// search per graph. Callers must have authorized every graph in the set
    /// against the *stored* policy first: this filter only narrows the
    /// candidate set, it is not an authorization check (G8).
    pub fn search_in_graphs(&self, req: GraphSetQuery<'_>) -> Result<Vec<SearchHit>> {
        if req.graphs.is_empty() {
            return Ok(Vec::new());
        }

        // Sanitized exactly as in `search` and `search_in_graph`: this is the
        // arm `search_graphs` picks above a graph-count threshold, and a raw
        // parse there made the same user query mean something else — or fail
        // outright — purely because one more graph was readable (G8).
        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = query_parser.parse_query(&sanitize_query(req.query))?;

        let graph_clauses: Vec<(Occur, Box<dyn Query>)> = req
            .graphs
            .iter()
            .map(|graph| {
                let term = TermQuery::new(
                    Term::from_field_text(self.f_graph_id, graph.as_str()),
                    IndexRecordOption::Basic,
                );
                (Occur::Should, Box::new(term) as Box<dyn Query>)
            })
            .collect();

        let combined = BooleanQuery::new(vec![
            (Occur::Must, parsed),
            (Occur::Must, Box::new(BooleanQuery::new(graph_clauses))),
        ]);

        self.collect_top_docs(&combined, req.limit)
    }

    fn collect_top_docs(&self, query: &dyn Query, limit: usize) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let top_docs = searcher.search(query, &TopDocs::with_limit(limit).order_by_score())?;
        #[cfg(test)]
        {
            self.hooks.searches.fetch_add(1, Ordering::SeqCst);
            self.hooks
                .decoded
                .fetch_add(top_docs.len(), Ordering::SeqCst);
        }
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            hits.push(self.doc_to_hit(doc, score));
        }
        Ok(hits)
    }

    /// Commit pending writes and reload the reader so subsequent searches
    /// reflect the latest changes.
    ///
    /// A completion barrier, not a request: returning `Ok` means a commit
    /// covering every write made before the call has finished and the reader
    /// has been reloaded. A plain dirty flag cleared up front let a second
    /// caller see "clean" while the first commit was still in flight and
    /// acknowledge queue entries Tantivy had not yet made durable — if that
    /// commit then failed, the acknowledged work was never indexed (G7).
    pub fn commit(&self) -> Result<()> {
        let target = self.write_epoch.load(Ordering::SeqCst);
        if self.committed_epoch.load(Ordering::SeqCst) >= target {
            return Ok(());
        }

        let _serialized = self
            .commit_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A commit that finished while we queued may already cover us.
        if self.committed_epoch.load(Ordering::SeqCst) >= target {
            return Ok(());
        }

        #[cfg(test)]
        self.hooks.commit.run();

        // Every epoch bump happens under the writer lock, so nothing can slip
        // in between reading the epoch and committing it.
        let covered = {
            let mut writer = self.writer()?;
            let covered = self.write_epoch.load(Ordering::SeqCst);
            writer.commit()?;
            covered
        };
        self.reader.reload()?;
        self.committed_epoch.store(covered, Ordering::SeqCst);
        Ok(())
    }

    /// Sync queued updates from the RDF store into Tantivy.
    ///
    /// All three durable queues get a share of the pass, in delete, then
    /// whole-graph, then per-subject order. Returning as soon as one class had
    /// work let a steady stream of deletes starve every queued subject, and a
    /// single failing entry aborted the pass before any commit, so the work
    /// prepared ahead of it was thrown away and redone on every retry.
    ///
    /// Each branch commits the index *before* acknowledging the entries it
    /// covered: a crash in between only re-does work, whereas acknowledging
    /// first would silently drop updates Tantivy never committed (G7). An
    /// entry that fails is left unacknowledged and named in the result, so a
    /// flush covering it cannot report success.
    /// Entries this drain committed and acknowledged.
    ///
    /// A count only. [`SearchIndex::drain_queues`] carries the rest of the
    /// outcome: work still owed, entries that failed, and a recovery target a
    /// repair raised.
    pub fn process_queued_updates(&self, store: &GraphStore, bound: QueueBound) -> Result<usize> {
        Ok(self.drain_queues(store, bound)?.covered)
    }

    pub fn drain_queues(&self, store: &GraphStore, bound: QueueBound) -> Result<DrainProgress> {
        let (bound, recovery) = self.settle_poisoned_writer(store, bound)?;
        // One share per class, so no class can consume the whole pass.
        let quota = bound.chunk.div_ceil(3).max(1);
        let mut pass = DrainPass {
            store,
            bound: QueueBound {
                chunk: quota,
                ..bound
            },
            progress: DrainProgress {
                recovery,
                ..DrainProgress::default()
            },
        };

        self.drain_deleted_graphs(&mut pass)?;
        self.drain_rebuilt_graphs(&mut pass)?;
        self.drain_dirty_subjects(&mut pass)?;
        Ok(pass.progress)
    }

    /// Settle the graphs whose search documents a removal invalidated.
    fn drain_deleted_graphs(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let slice = drain_upto(&pass.bound, |chunk| {
            pass.store.drain_fts_delete_queue(chunk)
        })?;
        pass.progress.remaining |= slice.remaining;
        if slice.entries.is_empty() {
            return Ok(());
        }

        let mut covered = Vec::with_capacity(slice.entries.len());
        for entry in &slice.entries {
            let key = FailureKey::graph(&entry.graph);
            match self.settle_deleted_graph(pass.store, &entry.graph) {
                Ok(()) => {
                    self.clear_failure(&key);
                    covered.push(entry.clone());
                }
                Err(error) => pass
                    .progress
                    .failures
                    .push(self.record_failure(key, &error)),
            }
        }
        if covered.is_empty() {
            return Ok(());
        }

        self.commit()?;
        pass.store
            .acknowledge_fts_queues_for_deleted_graphs(&covered)?;
        pass.store.acknowledge_fts_delete_queue(&covered)?;
        pass.progress.covered += covered.len();
        Ok(())
    }

    /// Drop or re-derive one graph whose removal was queued.
    fn settle_deleted_graph(&self, store: &GraphStore, graph: &GraphId) -> Result<()> {
        #[cfg(test)]
        self.hooks.fail_item(graph)?;
        // Taken before the probe: read outside the shard, the answer can
        // already be stale by the time its branch runs.
        let _rebuild = self.lock_graph(graph.as_str());
        if store.contains_graph(graph)? {
            self.reindex_locked(store, graph)?;
        } else {
            self.delete_documents_locked(graph.as_str())?;
        }
        Ok(())
    }

    /// Re-derive the graphs queued for a whole-graph rebuild.
    fn drain_rebuilt_graphs(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let slice = drain_upto(&pass.bound, |chunk| {
            pass.store.drain_fts_reindex_queue(chunk)
        })?;
        pass.progress.remaining |= slice.remaining;
        if slice.entries.is_empty() {
            return Ok(());
        }

        let mut covered = Vec::with_capacity(slice.entries.len());
        for entry in &slice.entries {
            let key = FailureKey::graph(&entry.graph);
            match self.rebuild_queued_graph(pass.store, &entry.graph) {
                Ok(()) => {
                    self.clear_failure(&key);
                    covered.push(entry.clone());
                }
                Err(error) => pass
                    .progress
                    .failures
                    .push(self.record_failure(key, &error)),
            }
        }
        if covered.is_empty() {
            return Ok(());
        }

        self.commit()?;
        pass.store
            .acknowledge_fts_subjects_for_reindexed_graphs(&covered)?;
        pass.store.acknowledge_fts_reindex_queue(&covered)?;
        pass.progress.covered += covered.len();
        Ok(())
    }

    /// Re-derive one queued graph, unless it has since been removed.
    fn rebuild_queued_graph(&self, store: &GraphStore, graph: &GraphId) -> Result<()> {
        #[cfg(test)]
        self.hooks.fail_item(graph)?;
        // A graph removed after its rebuild was queued stays removed: a scan
        // that ran anyway would republish documents for a deleted graph.
        if !store.contains_graph(graph)? {
            let _rebuild = self.lock_graph(graph.as_str());
            return self.delete_documents_locked(graph.as_str());
        }
        self.reindex_from_store(store, graph)?;
        Ok(())
    }

    /// Apply the queued per-subject updates.
    fn drain_dirty_subjects(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let slice = drain_upto(&pass.bound, |chunk| pass.store.drain_fts_queue(chunk))?;
        pass.progress.remaining |= slice.remaining;
        if slice.entries.is_empty() {
            return Ok(());
        }

        // Held across both phases: a rebuild of one of these graphs must not
        // clear and refill it from a scan that straddles the read below and
        // the apply that follows it.
        let rebuild_guards =
            self.lock_graphs(slice.entries.iter().map(|entry| entry.graph.as_str()));

        // Phase 1: read every update from the store with NO writer lock held,
        // stopping once the prepared text reaches the pass budget.
        let mut seen = HashSet::with_capacity(slice.entries.len());
        let mut caches = StoreSyncCaches::default();
        let mut prepared: Vec<(PreparedDocOp, DirtySubject)> = Vec::new();
        let mut duplicates = Vec::new();
        let mut prepared_bytes = 0usize;
        for entry in &slice.entries {
            if prepared_bytes >= PREPARED_TEXT_BUDGET {
                // The rest stays queued: acknowledging entries this pass never
                // prepared would drop their updates permanently.
                pass.progress.remaining = true;
                break;
            }
            if !seen.insert((entry.graph.clone(), entry.subject)) {
                duplicates.push(entry.clone());
                continue;
            }
            let key = FailureKey::subject(&entry.graph, entry.subject);
            match self.prepare_queued_entry(
                &mut caches,
                PrepareSubject {
                    store: pass.store,
                    graph: &entry.graph,
                    subject: entry.subject,
                },
            ) {
                Ok(op) => {
                    prepared_bytes = prepared_bytes.saturating_add(op.text_bytes());
                    self.clear_failure(&key);
                    prepared.push((op, entry.clone()));
                }
                Err(error) => pass
                    .progress
                    .failures
                    .push(self.record_failure(key, &error)),
            }
        }

        // Phase 2: apply the prepared ops in queue order under the writer lock.
        let mut covered = Vec::with_capacity(prepared.len());
        {
            // Guards the Tantivy writer; no store reads happen inside.
            let mut writer = self.writer()?;
            for (op, entry) in &prepared {
                let key = FailureKey::subject(&entry.graph, entry.subject);
                match self.apply_prepared_op(&mut writer, op) {
                    Ok(()) => covered.push(entry.clone()),
                    Err(error) => pass
                        .progress
                        .failures
                        .push(self.record_failure(key, &error)),
                }
            }
        }
        drop(rebuild_guards);

        if covered.is_empty() && duplicates.is_empty() {
            return Ok(());
        }
        covered.extend(duplicates);

        self.commit()?;
        pass.store.acknowledge_fts_queue(&covered)?;
        pass.progress.covered += covered.len();
        Ok(())
    }

    /// Read one queued subject, with the per-entry failure hook applied first.
    fn prepare_queued_entry(
        &self,
        caches: &mut StoreSyncCaches,
        req: PrepareSubject<'_>,
    ) -> Result<PreparedDocOp> {
        #[cfg(test)]
        self.hooks.fail_item(req.graph)?;
        prepare_subject_op(req, caches)
    }

    fn apply_prepared_op(&self, writer: &mut IndexWriter, op: &PreparedDocOp) -> Result<()> {
        match op {
            PreparedDocOp::Delete { doc } => {
                self.delete_resource_with_writer(writer, &doc.graph_iri, &doc.subject_iri);
                Ok(())
            }
            PreparedDocOp::Upsert { doc, all_text } => self.add_document(
                writer,
                ResourceDoc {
                    graph_id: &doc.graph_iri,
                    subject_iri: &doc.subject_iri,
                    all_text: all_text.as_deref(),
                    delete_existing: true,
                },
            ),
        }
    }

    /// Reindex all entities in a graph from the RDF store.
    ///
    /// Scans the store for triples with searchable predicates, groups them by
    /// subject, and indexes each subject as a document.
    ///
    /// Returns the number of entities indexed.
    pub fn reindex_from_store(&self, store: &GraphStore, graph: &GraphId) -> Result<usize> {
        // Held across the clear, the scan and the refill, or a concurrent
        // upsert is duplicated by the refill or overwritten by the scan.
        let _rebuild = self.lock_graph(graph.as_str());
        self.reindex_locked(store, graph)
    }

    /// The caller MUST hold this graph's rebuild shard.
    fn reindex_locked(&self, store: &GraphStore, graph: &GraphId) -> Result<usize> {
        let graph_iri = graph.as_str();
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let graph_tid = match store.lookup_term(&graph_term)? {
            Some(tid) => tid,
            None => return Ok(0),
        };

        let orphaned = orphaned_subjects(store, graph)?;
        let mut count = 0usize;
        let mut current_subject: Option<TermId> = None;
        let mut current_subject_iri = String::new();
        let mut current_subject_visible = false;
        let mut current_text = String::new();
        let mut pending_documents = Vec::new();
        {
            // Guards the Tantivy writer for the whole-graph clear only.
            let writer = self.writer()?;
            writer.delete_term(Term::from_field_text(self.f_graph_id, graph_iri));
            self.write_epoch.fetch_add(1, Ordering::SeqCst);
        }

        #[cfg(test)]
        self.hooks.rebuild.run();

        store.for_each_quad_in_graph::<SearchError, _>(graph_tid, |quad| {
            if current_subject != Some(quad.subject) {
                if current_subject_visible {
                    pending_documents.push((
                        std::mem::take(&mut current_subject_iri),
                        (!current_text.is_empty()).then(|| std::mem::take(&mut current_text)),
                    ));
                    count += 1;
                    if pending_documents.len() >= REINDEX_FLUSH_CHUNK {
                        self.flush_pending_documents(graph_iri, &mut pending_documents)?;
                    }
                }

                let subject_term = store.decode_term_arc(quad.subject)?;
                current_subject_iri = term_to_string(&subject_term);
                current_subject_visible = !orphaned.contains(&current_subject_iri);
                current_text.clear();
                current_subject = Some(quad.subject);
            }

            if current_subject_visible {
                let predicate_term = store.decode_term_arc(quad.predicate)?;
                if !is_searchable_predicate(&predicate_term) {
                    return Ok(());
                }
                let object_term = store.decode_term_arc(quad.object)?;
                append_searchable_text(&mut current_text, &object_term);
            }
            Ok(())
        })?;

        if current_subject_visible {
            pending_documents.push((
                current_subject_iri,
                (!current_text.is_empty()).then_some(current_text),
            ));
            count += 1;
        }

        self.flush_pending_documents(graph_iri, &mut pending_documents)?;

        Ok(count)
    }

    fn doc_to_hit(&self, doc: TantivyDocument, score: f32) -> SearchHit {
        let graph_id = first_text(&doc, self.f_graph_id);
        let subject_iri = first_text(&doc, self.f_subject_iri);
        let (graph_id, subject_iri) = match (graph_id, subject_iri) {
            (Some(graph_id), Some(subject_iri)) => (graph_id, subject_iri),
            _ => {
                let doc_key = first_text(&doc, self.f_doc_key).unwrap_or_default();
                split_doc_key(&doc_key).unwrap_or_default()
            }
        };

        SearchHit {
            graph_id,
            subject_iri,
            score,
        }
    }

    fn flush_pending_documents(
        &self,
        graph_iri: &str,
        pending_documents: &mut Vec<(String, Option<String>)>,
    ) -> Result<()> {
        if pending_documents.is_empty() {
            return Ok(());
        }

        // Guards the Tantivy writer. Reindex already dropped every document of
        // this graph, so the per-document delete is unnecessary here.
        let mut writer = self.writer()?;
        for (subject_iri, extra_text) in pending_documents.drain(..) {
            self.add_document(
                &mut writer,
                ResourceDoc {
                    graph_id: graph_iri,
                    subject_iri: &subject_iri,
                    all_text: extra_text.as_deref(),
                    delete_existing: false,
                },
            )?;
        }
        Ok(())
    }

    fn delete_resource_with_writer(
        &self,
        writer: &mut IndexWriter,
        graph_id: &str,
        subject_iri: &str,
    ) {
        writer.delete_term(Term::from_field_text(
            self.f_doc_key,
            &doc_key(graph_id, subject_iri),
        ));
        self.write_epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Drop every document of a graph, leaving the writer uncommitted.
    /// The caller MUST hold this graph's rebuild shard.
    fn delete_documents_locked(&self, graph_id: &str) -> Result<()> {
        let writer = self.writer()?;
        writer.delete_term(Term::from_field_text(self.f_graph_id, graph_id));
        self.write_epoch.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// [`SearchIndex::delete_documents_locked`], taking the shard itself.
    #[cfg(test)]
    fn delete_graph_documents(&self, graph_id: &str) -> Result<()> {
        let _rebuild = self.lock_graph(graph_id);
        self.delete_documents_locked(graph_id)
    }
}

/// Read everything one queued subject needs from the store and decide whether
/// its document should be replaced or dropped.
///
/// Pure store reads: no Tantivy writer lock is held while this runs.
fn prepare_subject_op(
    req: PrepareSubject<'_>,
    caches: &mut StoreSyncCaches,
) -> Result<PreparedDocOp> {
    let subject_term = req.store.decode_term_arc(req.subject)?;
    let doc = DocIdentity {
        graph_iri: req.graph.as_str().to_string(),
        subject_iri: term_to_string(&subject_term),
    };

    // Orphaned entities are invisible to search, exactly as they are to export
    // and SPARQL (G6).
    let orphaned = load_orphaned_subjects(&mut caches.orphaned_subjects, req.store, req.graph)?;
    if orphaned.contains(doc.subject_iri.as_str()) {
        return Ok(PreparedDocOp::Delete { doc });
    }

    let graph_tid = match caches.graph_terms.entry(req.graph.clone()) {
        std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let graph_term = EncodedTerm::from_named_node(&req.graph.0);
            *entry.insert(req.store.lookup_term(&graph_term)?)
        }
    };
    let Some(graph_tid) = graph_tid else {
        return Ok(PreparedDocOp::Delete { doc });
    };

    let triples = req.store.triples_for_subject(graph_tid, req.subject)?;
    if triples.is_empty() {
        return Ok(PreparedDocOp::Delete { doc });
    }

    let mut all_text = String::new();
    for (predicate, object) in triples {
        if is_searchable_predicate(&predicate) {
            append_searchable_text(&mut all_text, &object);
        }
    }

    Ok(PreparedDocOp::Upsert {
        doc,
        all_text: (!all_text.is_empty()).then_some(all_text),
    })
}

/// Extract the first text value for a field from a TantivyDocument.
fn first_text(doc: &TantivyDocument, field: Field) -> Option<String> {
    doc.get_all(field)
        .next()
        .and_then(|value| value.as_str().map(str::to_string))
}

fn doc_key(graph_id: &str, subject_iri: &str) -> String {
    format!("{graph_id}\u{1f}{subject_iri}")
}

fn split_doc_key(doc_key: &str) -> Option<(String, String)> {
    let (graph_id, subject_iri) = doc_key.split_once('\u{1f}')?;
    Some((graph_id.to_string(), subject_iri.to_string()))
}

fn rebuild_shard(graph_iri: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    graph_iri.hash(&mut hasher);
    (hasher.finish() % REBUILD_SHARDS as u64) as usize
}

fn sanitize_query(query: &str) -> String {
    let cleaned: String = query
        .chars()
        .map(|c| {
            if "+-&|!(){}[]^\"~*?:\\/".contains(c) {
                ' '
            } else {
                c
            }
        })
        .collect();
    cleaned
        .split_whitespace()
        .filter(|token| !matches!(*token, "AND" | "OR" | "NOT"))
        .map(|token| token.to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn orphaned_subjects(store: &GraphStore, graph: &GraphId) -> Result<HashSet<String>> {
    Ok(store
        .graph_diagnostics(graph)?
        .orphaned_entities
        .into_iter()
        .collect())
}

/// Convert an EncodedTerm to a plain string (IRI without angle brackets,
/// or the raw string representation for other term types).
fn term_to_string(term: &EncodedTerm) -> String {
    if term.0.starts_with('<') && term.0.ends_with('>') {
        term.0[1..term.0.len() - 1].to_string()
    } else {
        term.0.clone()
    }
}

fn append_searchable_text(buffer: &mut String, term: &EncodedTerm) {
    let Some(value) = searchable_term_text(term) else {
        return;
    };
    if !buffer.is_empty() {
        buffer.push(' ');
    }
    buffer.push_str(&value);
}

/// `https://schema.org/` and `http://schema.org/` name the same predicate; the
/// table is interned in the `http` form, so an `https` term is normalized
/// before comparison rather than being silently dropped from the index.
fn is_searchable_predicate(predicate: &EncodedTerm) -> bool {
    let normalized = predicate
        .0
        .strip_prefix("<https://schema.org/")
        .map(|suffix| format!("<http://schema.org/{suffix}"));
    SEARCHABLE_PREDICATES
        .iter()
        .any(|candidate| candidate == predicate || normalized.as_ref() == Some(&candidate.0))
}

fn searchable_term_text(term: &EncodedTerm) -> Option<Cow<'_, str>> {
    if term.0.starts_with('<') && term.0.ends_with('>') {
        return Some(Cow::Borrowed(&term.0[1..term.0.len() - 1]));
    }
    if term.0.starts_with("_:") {
        return Some(Cow::Borrowed(&term.0[2..]));
    }
    match term.to_term()? {
        oxrdf::Term::Literal(lit) => Some(Cow::Owned(lit.value().to_string())),
        oxrdf::Term::NamedNode(nn) => Some(Cow::Owned(nn.as_str().to_string())),
        oxrdf::Term::BlankNode(bn) => Some(Cow::Owned(bn.as_str().to_string())),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn load_orphaned_subjects<'a>(
    cache: &'a mut HashMap<GraphId, HashSet<String>>,
    store: &GraphStore,
    graph: &GraphId,
) -> Result<&'a HashSet<String>> {
    if !cache.contains_key(graph) {
        cache.insert(graph.clone(), orphaned_subjects(store, graph)?);
    }
    Ok(cache.get(graph).expect("orphan cache inserted"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use tempfile::tempdir;

    fn build_legacy_schema() -> Schema {
        let mut builder = SchemaBuilder::default();
        builder.add_text_field("doc_key", STRING | STORED);
        builder.add_text_field("graph_id", STRING | STORED);
        builder.add_text_field("subject_iri", STRING | STORED);
        builder.add_text_field("all_text", TEXT);
        builder.build()
    }

    #[test]
    fn indexes_and_searches() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Protein Structure Analysis A dataset about protein folding biology protein"),
        )?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity2",
            Some("Climate Data Global temperature measurements climate weather"),
        )?;

        idx.commit()?;

        let hits = idx.search("protein", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "http://example.org/entity1");
        assert_eq!(hits[0].graph_id, "http://example.org/graph1");

        Ok(())
    }

    #[test]
    fn searches_selected_graph() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Protein Data"),
        )?;

        idx.index_resource(
            "http://example.org/graph2",
            "http://example.org/entity2",
            Some("Protein Structures"),
        )?;

        idx.commit()?;

        let hits = idx.search_in_graph("http://example.org/graph1", "protein", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].graph_id, "http://example.org/graph1");

        let all_hits = idx.search("protein", 10)?;
        assert_eq!(all_hits.len(), 2);

        Ok(())
    }

    #[test]
    fn neutralizes_query_operators() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("COVID-19 RNA-seq foo dataset"),
        )?;
        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity2",
            Some("bar"),
        )?;
        idx.commit()?;

        for query in ["COVID-19", "covid", "RNA-seq", "type:dataset"] {
            let hits = idx.search(query, 10)?;
            assert_eq!(hits.len(), 1, "query: {query}");
            assert_eq!(hits[0].subject_iri, "http://example.org/entity1");
        }

        let hits = idx.search("foo AND bar", 10)?;
        let subjects: HashSet<_> = hits.iter().map(|hit| hit.subject_iri.as_str()).collect();
        assert_eq!(subjects.len(), 2);
        assert!(subjects.contains("http://example.org/entity1"));
        assert!(subjects.contains("http://example.org/entity2"));

        Ok(())
    }

    #[test]
    fn folding_matches_diacritics() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Forschung an der Universität"),
        )?;
        idx.commit()?;

        for query in ["universität", "universitat"] {
            let hits = idx.search(query, 10)?;
            assert_eq!(hits.len(), 1, "query: {query}");
            assert_eq!(hits[0].subject_iri, "http://example.org/entity1");
        }

        Ok(())
    }

    /// A write landing after a reindex scan pinned its token must survive the
    /// clear that follows: the scan never saw it, so wiping its queue entry
    /// would leave it unindexed with nothing left to re-queue it.
    #[test]
    fn reindex_preserves_writes() {
        let dir = tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let auth = crate::AllowAllAuthorizer;
        let graph = crate::core::GraphId::new("urn:test:reindex-late-write");

        node.create_crate(
            &auth,
            crate::CreateCrateRequest::new(
                graph.clone(),
                "Reindex Dataset",
                "Baseline crate",
                "2025-01-01",
                None,
                crate::core::GraphPolicy::default(),
            ),
        )
        .unwrap();
        node.flush_search_updates().unwrap();

        let (reached, scanned) = std::sync::mpsc::channel();
        let (release, go) = std::sync::mpsc::channel();
        node.set_reindex_gate(reached, go);
        std::thread::scope(|scope| {
            scope.spawn(|| node.reindex_search().unwrap());
            scanned.recv().unwrap();
            // Keep the background indexer off this write, so what makes it
            // searchable is the queue entry surviving the clear.
            node.search.arm_drain_panic();
            node.add_data_entity_with_triples(
                &auth,
                &graph,
                "data/late.dat",
                "http://schema.org/MediaObject",
                "Latewrite Entity",
                Vec::new(),
            )
            .unwrap();
            release.send(()).unwrap();
        });

        // The first flush may be the one that spends the armed panic.
        let _ = node.flush_search_updates();
        node.flush_search_updates().unwrap();

        let hits = node
            .search(
                &auth,
                crate::SearchRequest {
                    query: "latewrite",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(hits.iter().any(|hit| hit.subject_iri.contains("late.dat")));
    }

    #[test]
    fn https_schema_indexed() {
        let dir = tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let graph = crate::core::GraphId::new("urn:test:https-schema-search");
        let auth = crate::AllowAllAuthorizer;
        let document = serde_json::json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {"description": "https://schema.org/description"}
            ],
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "HTTPS Context Crate",
                    "description": "Contains contextneedle in its description",
                    "http://schema.org/description": "Contains contextneedle in its description",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
                }
            ]
        });

        node.apply_rocrate_document_with_policy(
            &auth,
            graph.clone(),
            &document.to_string(),
            crate::core::GraphPolicy::default(),
        )
        .unwrap();
        node.flush_search_updates().unwrap();

        let hits = node
            .search(
                &auth,
                crate::SearchRequest {
                    query: "contextneedle",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.graph_id == graph.as_str() && hit.subject_iri == graph.as_str())
        );
    }

    #[test]
    fn upsert_replaces_document() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Old Name"),
        )?;
        idx.commit()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("New Name"),
        )?;
        idx.commit()?;

        let hits = idx.search("name", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "http://example.org/entity1");

        Ok(())
    }

    #[test]
    fn subjects_remain_scoped() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/graph1",
            Some("Graph One Root"),
        )?;
        idx.index_resource(
            "http://example.org/graph2",
            "http://example.org/graph2",
            Some("Graph Two Root"),
        )?;
        idx.commit()?;

        let graph1_hits = idx.search_in_graph("http://example.org/graph1", "graph", 10)?;
        let graph2_hits = idx.search_in_graph("http://example.org/graph2", "graph", 10)?;

        assert_eq!(graph1_hits.len(), 1);
        assert_eq!(graph2_hits.len(), 1);
        assert_eq!(graph1_hits[0].graph_id, "http://example.org/graph1");
        assert_eq!(graph2_hits[0].graph_id, "http://example.org/graph2");

        Ok(())
    }

    #[test]
    fn indexes_subject_identifiers() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource("http://example.org/graph1", "urn:test:dataset123", None)?;
        idx.commit()?;

        let hits = idx.search("dataset123", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "urn:test:dataset123");

        Ok(())
    }

    #[test]
    fn persistent_index_reopens() -> Result<()> {
        let dir = tempdir().unwrap();

        let idx = SearchIndex::open(dir.path())?;
        assert!(idx.needs_rebuild());
        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("persisted proteomics record"),
        )?;
        idx.commit()?;
        drop(idx);

        let reopened = SearchIndex::open(dir.path())?;
        assert!(!reopened.needs_rebuild());
        let hits = reopened.search_in_graph("http://example.org/graph1", "proteomics", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "http://example.org/entity1");

        Ok(())
    }

    #[test]
    fn old_analyzer_rebuilt() {
        let dir = tempdir().unwrap();
        let graph = crate::core::GraphId::new("urn:test:search-analyzer-reindex");
        let auth = crate::AllowAllAuthorizer;

        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            node.create_crate(
                &auth,
                crate::CreateCrateRequest::new(
                    graph.clone(),
                    "Analyzer Reindex Crate",
                    "Forschung an der Universität",
                    "2025-01-01",
                    Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                    crate::core::GraphPolicy::default(),
                ),
            )
            .unwrap();
            node.flush_search_updates().unwrap();
        }

        let search_dir = dir.path().join("search");
        std::fs::remove_dir_all(&search_dir).unwrap();
        std::fs::create_dir_all(&search_dir).unwrap();
        let legacy_schema = build_legacy_schema();
        let legacy_index = Index::create_in_dir(&search_dir, legacy_schema.clone()).unwrap();
        let mut writer = legacy_index.writer(MEMORY_INDEX_WRITER_HEAP_BYTES).unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_text(
            legacy_schema.get_field("doc_key").unwrap(),
            doc_key(graph.as_str(), graph.as_str()),
        );
        doc.add_text(legacy_schema.get_field("graph_id").unwrap(), graph.as_str());
        doc.add_text(
            legacy_schema.get_field("subject_iri").unwrap(),
            graph.as_str(),
        );
        doc.add_text(
            legacy_schema.get_field("all_text").unwrap(),
            "Forschung an der Universität",
        );
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        drop(writer);
        drop(legacy_index);

        let reopened = crate::CraqleNode::open(dir.path()).unwrap();
        reopened.flush_search_updates().unwrap();
        let hits = reopened
            .search(
                &auth,
                crate::SearchRequest {
                    query: "universitat",
                    limit: 10,
                },
            )
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.graph_id == graph.as_str() && hit.subject_iri == graph.as_str())
        );
    }

    // ── G7: a poisoned Tantivy writer is derived state, so it self-heals ──

    /// Panic while holding the writer lock, from a thread that is then joined.
    fn poison_writer(index: Arc<SearchIndex>) {
        let panicked = std::thread::spawn(move || {
            let _guard = index.writer.lock().unwrap();
            panic!("panic while holding the Tantivy writer");
        })
        .join();

        assert!(panicked.is_err(), "the poisoning thread must have panicked");
    }

    /// A poisoned writer must not turn every later index write into a panic.
    #[test]
    fn poisoned_writer_recovers() -> Result<()> {
        let index = Arc::new(SearchIndex::open_in_memory()?);
        index.index_resource("urn:g", "urn:before", Some("beforepoison"))?;
        index.commit()?;

        poison_writer(index.clone());
        assert!(index.writer.is_poisoned());

        // The very next write repairs the lock instead of panicking on it.
        index.index_resource("urn:g", "urn:after", Some("afterpoison"))?;
        index.commit()?;

        assert!(
            !index.writer.is_poisoned(),
            "the repair must clear the poison, not paper over it"
        );
        assert_eq!(1, index.search("afterpoison", 10)?.len());
        assert!(
            index.rebuild_owed.load(Ordering::SeqCst),
            "the rollback discarded uncommitted work, so a rebuild is owed"
        );

        Ok(())
    }

    /// A second committer must not report success while the first commit is
    /// still running: the pipeline acknowledges queue entries once `commit`
    /// returns, and those writes are neither durable nor visible yet (G7).
    #[test]
    fn commit_awaits_inflight() -> Result<()> {
        let index = Arc::new(SearchIndex::open_in_memory()?);
        index.index_resource("urn:g", "urn:subject", Some("barriertext"))?;
        index.hooks.commit.arm(Duration::from_millis(300));

        let stalled = {
            let index = index.clone();
            std::thread::spawn(move || index.commit())
        };
        index.hooks.commit.wait_entered();

        index.commit()?;
        assert_eq!(
            1,
            index.search("barriertext", 10)?.len(),
            "commit returned before the write it covers was visible"
        );

        stalled.join().expect("the stalled committer panicked")?;
        Ok(())
    }

    /// The repair is not just "stop panicking": the rollback drops the writer
    /// back to its last commit, so the index has to re-derive from the store,
    /// which is the source of truth. One indexer pass must be enough.
    #[test]
    fn poisoned_writer_reconverges() {
        let dir = tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let graph = crate::core::GraphId::new("urn:test:poison-reconverge");
        let auth = crate::AllowAllAuthorizer;
        let document = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": graph.as_str()}
                },
                {
                    "@id": graph.as_str(),
                    "@type": "Dataset",
                    "name": "Crate holding poisonneedle",
                    "description": "Search writer recovery fixture",
                    "datePublished": "2025-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}
                }
            ]
        });

        node.apply_rocrate_document_with_policy(
            &auth,
            graph.clone(),
            &document.to_string(),
            crate::core::GraphPolicy::default(),
        )
        .unwrap();
        node.flush_search_updates().unwrap();
        let found = |node: &crate::CraqleNode| {
            node.search(
                &auth,
                crate::SearchRequest {
                    query: "poisonneedle",
                    limit: 10,
                },
            )
            .unwrap()
            .len()
        };
        assert_eq!(1, found(&node));

        // Drop the graph's documents behind the store's back, so the index is
        // stale in exactly the way an interrupted writer leaves it. Nothing is
        // queued for it: only a re-derivation from the store can bring it back.
        node.search.delete_graph_documents(graph.as_str()).unwrap();
        node.search.commit().unwrap();
        assert_eq!(0, found(&node));

        poison_writer(node.search.clone());

        node.flush_search_updates().unwrap();
        assert!(!node.search.writer.is_poisoned());
        assert_eq!(
            1,
            found(&node),
            "the recovery must re-derive the index from the store, in one pass"
        );
    }

    fn writer_auth() -> crate::GrantAuthorizer {
        crate::GrantAuthorizer::new(vec![crate::PermissionGrant::new(
            "/t/**",
            crate::PermissionLevel::Write,
        )])
    }

    fn crate_request(
        graph: &GraphId,
        description: &str,
        public: bool,
    ) -> crate::CreateCrateRequest {
        crate::CreateCrateRequest::new(
            graph.clone(),
            "Search Fixture",
            description,
            "2025-01-01",
            None,
            crate::core::GraphPolicy {
                public,
                permission_paths: vec!["/t/search-fixture".to_string()],
            },
        )
    }

    /// One request must run one search over one pinned reader. Escalating the
    /// Tantivy fetch until enough hits survive authorization asks the index for
    /// a multiple of the page the caller wanted, once per pass.
    #[test]
    fn search_runs_once() {
        let dir = tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let writer = writer_auth();

        // Comfortably past the smallest over-fetch, so one pass cannot see the
        // whole matching corpus and call itself exhausted.
        for index in 0..80 {
            let graph = GraphId::new(&format!("urn:test:hidden-{index}"));
            node.create_crate(&writer, crate_request(&graph, "clusterneedle", false))
                .unwrap();
        }
        // Padded, so BM25 length normalization ranks the only readable match
        // below every hidden one.
        let padding = "filler ".repeat(200);
        let visible = GraphId::new("urn:test:visible-match");
        node.create_crate(
            &writer,
            crate_request(&visible, &format!("clusterneedle {padding}"), true),
        )
        .unwrap();
        node.flush_search_updates().unwrap();

        node.search.hooks.searches.store(0, Ordering::SeqCst);
        node.search.hooks.decoded.store(0, Ordering::SeqCst);
        let hits = node
            .search(
                &crate::GrantAuthorizer::default(),
                crate::SearchRequest {
                    query: "clusterneedle",
                    limit: 1,
                },
            )
            .unwrap();

        assert_eq!(1, hits.len(), "the one readable match must be returned");
        assert_eq!(visible.as_str(), hits[0].graph_id);
        assert_eq!(
            1,
            node.search.hooks.searches.load(Ordering::SeqCst),
            "a one-row page must not re-issue the query"
        );
        assert!(
            node.search.hooks.decoded.load(Ordering::SeqCst) <= 8,
            "decoded {} stored documents for a one-row page",
            node.search.hooks.decoded.load(Ordering::SeqCst)
        );
    }

    /// Reopen the store a dropped node left behind, so a drain can be stepped
    /// through without a background indexer racing it.
    fn reopen_store(dir: &Path) -> Arc<GraphStore> {
        Arc::new(GraphStore::open(dir.join("store")).unwrap())
    }

    fn graph_term(store: &GraphStore, graph: &GraphId) -> TermId {
        store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .expect("the fixture graph must be interned")
    }

    /// A flush that triggers its own recovery owes the whole rebuild, not the
    /// one queue entry that happened to land on its original cutoff.
    #[test]
    fn flush_covers_recovery() {
        let dir = tempdir().unwrap();
        let first = GraphId::new("urn:test:recovery-first");
        let second = GraphId::new("urn:test:recovery-second");
        let removed = GraphId::new("urn:test:recovery-removed");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            for graph in [&first, &second, &removed] {
                node.create_crate(&writer_auth(), crate_request(graph, "recoveryneedle", true))
                    .unwrap();
            }
            node.flush_search_updates().unwrap();
        }

        let store = reopen_store(dir.path());
        let search = Arc::new(SearchIndex::open(dir.path().join("search")).unwrap());
        assert_eq!(3, search.search("recoveryneedle", 50).unwrap().len());

        // An older queue entry, so it consumes the first drain pass on its own.
        store.delete_graph(&removed).unwrap();
        // Strip every document behind the store's back: only a re-derivation
        // from the store can bring the survivors back.
        for graph in [&first, &second, &removed] {
            search.delete_graph_documents(graph.as_str()).unwrap();
        }
        search.commit().unwrap();
        assert_eq!(0, search.search("recoveryneedle", 50).unwrap().len());

        poison_writer(search.clone());
        crate::flush_search_queue(&store, &search).unwrap();

        assert_eq!(
            2,
            search.search("recoveryneedle", 50).unwrap().len(),
            "the flush reported success with part of its own rebuild unindexed"
        );
    }

    /// One entry that always fails must not stop a different graph's queued
    /// work, and the flush that covered it must say so.
    #[test]
    fn drain_isolates_failure() {
        let dir = tempdir().unwrap();
        let healthy = GraphId::new("urn:test:isolation-healthy");
        let broken = GraphId::new("urn:test:isolation-broken");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            for graph in [&healthy, &broken] {
                node.create_crate(
                    &writer_auth(),
                    crate_request(graph, "isolationneedle", true),
                )
                .unwrap();
            }
            node.flush_search_updates().unwrap();
        }

        // A fresh index, so only what this drain rebuilds can be found.
        let store = reopen_store(dir.path());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        let mut batch = store.new_batch();
        for graph in [&healthy, &broken] {
            let term = graph_term(&store, graph);
            store.enqueue_fts_reindex(&mut batch, term).unwrap();
        }
        store.commit(batch).unwrap();
        *search
            .hooks
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(broken.as_str().to_string());

        assert!(
            crate::flush_search_queue(&store, &search).is_err(),
            "a flush covering a failed entry must not report success"
        );

        let hits = search.search("isolationneedle", 50).unwrap();
        assert_eq!(
            1,
            hits.len(),
            "the healthy graph stayed unindexed behind a permanently failing entry"
        );
        assert_eq!(healthy.as_str(), hits[0].graph_id);

        for _ in 0..10 {
            assert!(crate::flush_search_queue(&store, &search).is_err());
        }
        *search
            .hooks
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        crate::flush_search_queue(&store, &search)
            .expect("a recovered entry must remain retryable after repeated failures");
        assert_eq!(2, search.search("isolationneedle", 50).unwrap().len());
        assert!(store.drain_fts_reindex_queue(10).unwrap().is_empty());
    }
}
