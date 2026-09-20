//! Indexes visible RDF documents and drains durable search repair queues.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

pub(crate) mod queue;

use std::borrow::Cow;
use std::collections::BinaryHeap;
use std::collections::{BTreeSet, HashMap, HashSet};
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
/// Damaged graphs awaiting a reindex; later detections repeat once these are repaired.
const DAMAGED_GRAPHS: usize = 64;
const ALL_TEXT_TOKENIZER: &str = "craqle_text_v2";
const INDEX_VERSION_FIELD: &str = "_craqle_search_index_v5";
const INDEX_ID_FILE: &str = ".craqle-index-id";
const DIRECT_GENERATION: GenerationId = GenerationId(1);
const GENERATION_FIELD: &str = "doc_generation";
const GENERATION_SCOPE_FIELD: &str = "generation_scope";
const STABLE_KEY_FIELD: &str = "stable_key";
type SchemaFields = (Field, Field, Field, Field, Field, Field, Field, Field);

/// Cached predicates whose objects contribute searchable document text.
static SEARCHABLE_PREDICATES: LazyLock<[EncodedTerm; 4]> = LazyLock::new(|| {
    [
        EncodedTerm::from_named_node(&crate::vocab::schema_name()),
        EncodedTerm::from_named_node(&crate::vocab::schema_description()),
        EncodedTerm::from_named_node(&crate::vocab::schema_keywords()),
        EncodedTerm::from_named_node(&crate::vocab::schema_identifier()),
    ]
});

static SEARCHABLE_IDS: LazyLock<[TermId; 8]> = LazyLock::new(|| {
    let mut predicates = std::array::from_fn(|index| {
        let predicate = &SEARCHABLE_PREDICATES[index / 2];
        if index % 2 == 0 {
            crate::store::hash_term(predicate)
        } else {
            crate::store::hash_term(&EncodedTerm(predicate.0.replacen("http://", "https://", 1)))
        }
    });
    predicates.sort_unstable();
    predicates
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
    #[error("search index document has malformed {detail}")]
    Damaged { detail: &'static str },
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
            Self::Damaged { .. } => crate::CraqleErrorKind::CorruptDerivedData,
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

/// One scored document whose required metadata the collector decodes.
struct ActiveDoc<'a, E> {
    view: &'a SearchView,
    scopes: Option<&'a tantivy::columnar::BytesColumn>,
    stable: Option<&'a tantivy::columnar::BytesColumn>,
    doc: DocId,
    allows: &'a dyn Fn(&str) -> std::result::Result<bool, E>,
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
    damaged: Mutex<BTreeSet<String>>,
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
    /// Fails the next required metadata read, modelling an index I/O error.
    metadata_io: AtomicBool,
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
    generation: GenerationId,
    all_text: Option<&'a str>,
    /// Delete any existing document key in this generation first.
    delete_existing: bool,
}

/// Full-text query restricted to an explicit set of graph IRIs.
pub struct GraphSetQuery<'a> {
    pub graphs: &'a [GraphId],
    pub query: &'a str,
    pub limit: usize,
}

/// Subject update prepared from the store without holding the writer lock.
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
    /// Retained identity and text charged against the pass byte budget.
    fn held_bytes(&self) -> usize {
        match self {
            Self::Delete { doc } => doc.graph_iri.len().saturating_add(doc.subject_iri.len()),
            Self::Upsert { doc, all_text } => doc
                .graph_iri
                .len()
                .saturating_add(doc.subject_iri.len())
                .saturating_add(all_text.as_ref().map_or(0, String::len)),
        }
    }
}

struct DocIdentity {
    graph_iri: String,
    subject_iri: String,
    generation: GenerationId,
}

/// Store reads needed to prepare one subject's index update.
struct PrepareSubject<'a> {
    store: &'a GraphStore,
    graph: &'a GraphId,
    subject: TermId,
    byte_limit: usize,
    generation: GenerationId,
}

struct OrphanInput<'a> {
    graph: &'a GraphId,
    graph_tid: TermId,
    byte_limit: usize,
}

struct StoreSyncCaches {
    snapshot: SearchSnapshot,
    orphaned_subjects: HashMap<GraphId, HashSet<TermId>>,
    orphan_bytes: usize,
    graph_terms: HashMap<GraphId, Option<TermId>>,
}

impl StoreSyncCaches {
    fn new(store: &GraphStore) -> Self {
        Self {
            snapshot: store.search_snapshot(),
            orphaned_subjects: HashMap::new(),
            orphan_bytes: 0,
            graph_terms: HashMap::new(),
        }
    }

    fn orphaned(&mut self, input: OrphanInput<'_>) -> Result<&HashSet<TermId>> {
        if !self.orphaned_subjects.contains_key(input.graph) {
            let artifact_limit = input.byte_limit / 32;
            let orphaned = match self.snapshot.orphaned_ids(input.graph_tid, artifact_limit) {
                Ok(orphaned) => orphaned,
                Err(crate::store::StoreError::LimitExceeded {
                    resource: "search diagnostics rows",
                    limit,
                    actual,
                }) => {
                    return Err(SearchError::SourceTooLarge {
                        rows: usize::try_from(actual).unwrap_or(usize::MAX),
                        limit: usize::try_from(limit).unwrap_or(usize::MAX),
                    });
                }
                Err(crate::store::StoreError::LimitExceeded { limit, actual, .. }) => {
                    return Err(SearchError::ItemTooLarge {
                        bytes: usize::try_from(actual).unwrap_or(usize::MAX),
                        limit: usize::try_from(limit).unwrap_or(usize::MAX),
                    });
                }
                Err(error) => return Err(error.into()),
            };
            let bytes = STAGE_SOURCE_BYTES.saturating_add(
                orphaned
                    .len()
                    .saturating_mul(std::mem::size_of::<TermId>() * 4),
            );
            if bytes > input.byte_limit {
                return Err(SearchError::ItemTooLarge {
                    bytes,
                    limit: input.byte_limit,
                });
            }
            self.orphan_bytes = self.orphan_bytes.saturating_add(bytes);
            self.orphaned_subjects.insert(input.graph.clone(), orphaned);
        }
        Ok(self
            .orphaned_subjects
            .get(input.graph)
            .expect("orphan cache inserted"))
    }
}

fn build_schema() -> Schema {
    let mut builder = SchemaBuilder::default();
    builder.add_text_field("doc_key", STRING | STORED);
    builder.add_text_field("graph_id", STRING | STORED);
    builder.add_text_field("subject_iri", STRING | STORED);
    builder.add_text_field("generation_key", STRING);
    builder.add_u64_field(GENERATION_FIELD, FAST | STORED);
    builder.add_bytes_field(GENERATION_SCOPE_FIELD, FAST);
    builder.add_bytes_field(STABLE_KEY_FIELD, FAST);
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

fn schema_fields(schema: &Schema) -> tantivy::Result<SchemaFields> {
    schema.get_field(INDEX_VERSION_FIELD)?;
    Ok((
        schema.get_field("doc_key")?,
        schema.get_field("graph_id")?,
        schema.get_field("subject_iri")?,
        schema.get_field("all_text")?,
        schema.get_field("generation_key")?,
        schema.get_field(GENERATION_FIELD)?,
        schema.get_field(GENERATION_SCOPE_FIELD)?,
        schema.get_field(STABLE_KEY_FIELD)?,
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

fn load_index_id(dir: &Path) -> Result<Option<[u8; 16]>> {
    let path = dir.join(INDEX_ID_FILE);
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(bytes.try_into().ok())
}

fn store_index_id(dir: &Path, index_id: [u8; 16]) -> Result<()> {
    let pending = dir.join(".craqle-index-id.pending");
    let mut file = std::fs::File::create(&pending)?;
    file.write_all(&index_id)?;
    file.sync_all()?;
    std::fs::rename(pending, dir.join(INDEX_ID_FILE))?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

impl SearchIndex {
    pub fn ensure_available(&self) -> Result<()> {
        Ok(())
    }

    /// Create or open a persistent index at the given directory path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_budget(path, MemoryBudget::default())
    }

    pub fn open_with_budget(path: impl AsRef<Path>, budget: MemoryBudget) -> Result<Self> {
        let schema = build_schema();

        let dir = path.as_ref();
        let (index, mut needs_rebuild) = if dir.join("meta.json").exists() {
            let index = Index::open_in_dir(dir)?;
            if schema_fields(&index.schema()).is_ok() {
                (index, false)
            } else {
                (recreate_index_dir(dir, &schema)?, true)
            }
        } else {
            (create_index_dir(dir, &schema)?, true)
        };
        let index_id = match load_index_id(dir)? {
            Some(index_id) => index_id,
            None => {
                let index_id = *uuid::Uuid::new_v4().as_bytes();
                store_index_id(dir, index_id)?;
                needs_rebuild = true;
                index_id
            }
        };

        register_text_analyzer(&index);
        let (
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
            f_generation_key,
            f_doc_generation,
            f_generation_scope,
            f_stable_key,
        ) = schema_fields(&index.schema())?;
        let reader = index.reader()?;
        let generations = Arc::new(GenerationView::default());
        let view = Arc::new(SearchView {
            searcher: reader.searcher(),
            generations,
            bound: false,
        });
        let writer_bytes = usize::try_from(budget.search_writer_bytes()).map_err(|_| {
            SearchError::ItemTooLarge {
                bytes: usize::MAX,
                limit: usize::MAX,
            }
        })?;
        let prepared_bytes = usize::try_from(budget.prepared_work_bytes()).unwrap_or(usize::MAX);
        let writer = index.writer(writer_bytes)?;

        Ok(Self {
            index,
            reader,
            view: RwLock::new(view),
            writer: Mutex::new(writer),
            write_epoch: AtomicU64::new(0),
            committed_epoch: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
            rebuild_shards: std::array::from_fn(|_| Mutex::new(())),
            rebuild_owed: AtomicBool::new(false),
            scan_cursors: Mutex::new(QueueCursors::default()),
            retry_now: AtomicU64::new(0),
            fair_cursor: AtomicU64::new(0),
            prepared_bytes,
            work_lock: Mutex::new(()),
            stage_sources: Mutex::new(HashMap::new()),
            damaged: Mutex::new(BTreeSet::new()),
            session: *uuid::Uuid::new_v4().as_bytes(),
            #[cfg(test)]
            hooks: TestHooks::default(),
            needs_rebuild: AtomicBool::new(needs_rebuild),
            index_id,
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
            f_generation_key,
            f_doc_generation,
            f_generation_scope,
            f_stable_key,
        })
    }

    /// Create an in-memory index (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        Self::memory_with_budget(MemoryBudget::default())
    }

    pub fn memory_with_budget(budget: MemoryBudget) -> Result<Self> {
        let schema = build_schema();

        let (
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
            f_generation_key,
            f_doc_generation,
            f_generation_scope,
            f_stable_key,
        ) = schema_fields(&schema)?;
        let index = Index::create_in_ram(schema);
        register_text_analyzer(&index);
        let reader = index.reader()?;
        let generations = Arc::new(GenerationView::default());
        let view = Arc::new(SearchView {
            searcher: reader.searcher(),
            generations,
            bound: true,
        });
        let writer_bytes = usize::try_from(budget.search_writer_bytes()).map_err(|_| {
            SearchError::ItemTooLarge {
                bytes: usize::MAX,
                limit: usize::MAX,
            }
        })?;
        let prepared_bytes = usize::try_from(budget.prepared_work_bytes()).unwrap_or(usize::MAX);
        let writer = index.writer(writer_bytes)?;

        Ok(Self {
            index,
            reader,
            view: RwLock::new(view),
            writer: Mutex::new(writer),
            write_epoch: AtomicU64::new(0),
            committed_epoch: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
            rebuild_shards: std::array::from_fn(|_| Mutex::new(())),
            rebuild_owed: AtomicBool::new(false),
            scan_cursors: Mutex::new(QueueCursors::default()),
            retry_now: AtomicU64::new(0),
            fair_cursor: AtomicU64::new(0),
            prepared_bytes,
            work_lock: Mutex::new(()),
            stage_sources: Mutex::new(HashMap::new()),
            damaged: Mutex::new(BTreeSet::new()),
            session: *uuid::Uuid::new_v4().as_bytes(),
            #[cfg(test)]
            hooks: TestHooks::default(),
            needs_rebuild: AtomicBool::new(false),
            index_id: *uuid::Uuid::new_v4().as_bytes(),
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_all_text,
            f_generation_key,
            f_doc_generation,
            f_generation_scope,
            f_stable_key,
        })
    }

    /// Returns `true` when the on-disk index had to be created or migrated.
    pub fn needs_rebuild(&self) -> bool {
        self.needs_rebuild.load(Ordering::SeqCst)
    }

    pub(crate) fn index_id(&self) -> [u8; 16] {
        self.index_id
    }

    pub(crate) fn query_bytes(&self) -> usize {
        self.prepared_bytes / 4
    }

    pub(crate) fn check_query_bytes(&self, bytes: usize) -> crate::Result<()> {
        let limit = self.query_bytes();
        if bytes > limit {
            return Err(SearchError::ItemTooLarge { bytes, limit }.into());
        }
        Ok(())
    }

    fn pin_view(&self) -> Arc<SearchView> {
        self.view
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn publish_searcher(&self) {
        let mut published = self.view.write().unwrap_or_else(PoisonError::into_inner);
        let generations = published.generations.clone();
        let bound = published.bound;
        *published = Arc::new(SearchView {
            searcher: self.reader.searcher(),
            generations,
            bound,
        });
    }

    #[cfg(test)]
    fn publish_rows(&self, rows: Vec<GraphGeneration>) {
        self.publish_generations(GenerationView::from_rows(self.index_id, rows));
    }

    fn publish_generations(&self, generations: GenerationView) {
        let mut published = self.view.write().unwrap_or_else(PoisonError::into_inner);
        *published = Arc::new(SearchView {
            searcher: self.reader.searcher(),
            generations: Arc::new(generations),
            bound: true,
        });
    }

    fn set_generation(&self, graph: &str, generation: Option<GenerationId>) {
        let mut published = self.view.write().unwrap_or_else(PoisonError::into_inner);
        let mut generations = (*published.generations).clone();
        match generation {
            Some(generation) => {
                generations.by_graph.insert(graph.to_string(), generation);
            }
            None => {
                generations.by_graph.remove(graph);
            }
        }
        generations.active = generations
            .by_graph
            .iter()
            .map(|(graph, generation)| generation_scope(self.index_id, graph, *generation))
            .collect();
        *published = Arc::new(SearchView {
            searcher: self.reader.searcher(),
            generations: Arc::new(generations),
            bound: true,
        });
    }

    fn active_generation(&self, graph: &str) -> Option<GenerationId> {
        self.pin_view().generations.by_graph.get(graph).copied()
    }

    fn stage_bytes(&self) -> usize {
        self.stage_sources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .fold(0usize, |bytes, source| bytes.saturating_add(source.bytes))
    }

    fn work_bytes(&self) -> usize {
        self.prepared_bytes.saturating_sub(self.stage_bytes()) / 4
    }

    fn apply_switch(&self, switched: &GenerationSwitch) {
        self.set_generation(switched.graph.as_str(), switched.active);
    }

    pub(crate) fn bind_store(&self, store: &GraphStore) -> Result<Option<u64>> {
        let coverage = store.search_coverage()?;
        let foreign = coverage.is_none_or(|coverage| coverage.index_id != self.index_id);
        if foreign {
            self.purge_index()?;
        }
        let revision = self.index.load_metas()?.opstamp;
        let manifest = match coverage {
            Some(coverage) if !foreign && revision >= coverage.index_revision => {
                // A rebuild keeps the last active generations readable; queued
                // debt still prevents final coverage certification.
                let target = if coverage.rebuild.is_some() {
                    0
                } else {
                    coverage.covered
                };
                Some(self.load_manifest(store, target)?)
            }
            _ => None,
        };
        let manifest_valid = coverage
            .zip(manifest.as_ref())
            .is_some_and(|(coverage, manifest)| {
                manifest.valid
                    && (coverage.rebuild.is_some()
                        || (manifest.digest_match
                            && coverage.manifest_count == manifest.count
                            && coverage.manifest_hash == manifest.hash))
            });
        if manifest_valid {
            self.publish_generations(manifest.expect("manifest checked").generations);
        } else {
            self.publish_generations(GenerationView::default());
        }
        let needs_rebuild = self.needs_rebuild.load(Ordering::SeqCst)
            || coverage.is_none_or(|coverage| {
                coverage.index_id != self.index_id
                    || coverage.rebuild.is_some()
                    || revision < coverage.index_revision
                    || !manifest_valid
            });
        if !needs_rebuild {
            return Ok(None);
        }
        Ok(Some(store.bind_search_rebuild(&RebuildRequest {
            index_id: self.index_id,
            index_revision: revision,
        })?))
    }

    fn load_manifest(&self, store: &GraphStore, target: u64) -> Result<ManifestState> {
        let snapshot = store.search_manifest_snapshot()?;
        let map_limit = self.prepared_bytes / 2;
        let mut generations = GenerationView::default();
        let mut hash = [0u8; 32];
        let mut count = 0u64;
        let mut valid = snapshot.oldest_owed()?.is_none_or(|owed| owed > target);
        let mut after = None;
        let mut retained = 0usize;
        loop {
            let page = store.scan_search_manifest(
                &snapshot,
                &ManifestScan {
                    index_id: self.index_id,
                    after,
                    row_limit: SOURCE_PAGE_ROWS,
                    byte_limit: map_limit.saturating_sub(retained),
                },
            )?;
            if let Some(oversized) = page.oversized {
                return Err(SearchError::ItemTooLarge {
                    bytes: oversized.bytes,
                    limit: oversized.limit,
                });
            }
            retained = retained.saturating_add(page.bytes);
            for row in page.entries {
                let old_debt = row.source_owed.is_some_and(|owed| owed <= target)
                    || row.delete_owed.is_some_and(|owed| owed <= target)
                    || row.stage_target.is_some_and(|owed| owed <= target);
                let missing_live = row.live
                    && row.active.is_none()
                    && row.source_owed.is_none_or(|owed| owed <= target);
                let stale_dead = !row.live
                    && row.active.is_some()
                    && row.delete_owed.is_none_or(|owed| owed <= target);
                if row.wrong_index || old_debt || missing_live || stale_dead {
                    valid = false;
                }
                if let Some(generation) = row.active {
                    let graph = row.graph.as_str();
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(&(graph.len() as u64).to_be_bytes());
                    hasher.update(graph.as_bytes());
                    hasher.update(&generation.0.to_be_bytes());
                    for (current, byte) in hash.iter_mut().zip(hasher.finalize().as_bytes()) {
                        *current ^= byte;
                    }
                    count = count.saturating_add(1);
                    if row.live {
                        generations.by_graph.insert(graph.to_string(), generation);
                    }
                }
            }
            if !page.remaining {
                break;
            }
            after = page.next;
            if after.is_none() {
                return Err(SearchError::Store(
                    crate::store::StoreError::InvalidSearchState(
                        "manifest-scan-continuation-missing",
                    ),
                ));
            }
        }
        generations.active = generations
            .by_graph
            .iter()
            .map(|(graph, generation)| generation_scope(self.index_id, graph, *generation))
            .collect();
        let digest = snapshot.digest(self.index_id)?;
        Ok(ManifestState {
            generations,
            count,
            hash,
            epoch: digest.epoch,
            valid,
            digest_match: digest.count == count && digest.hash == hash,
        })
    }

    fn purge_index(&self) -> Result<()> {
        if self.reader.searcher().num_docs() == 0 {
            return Ok(());
        }
        let _serialized = self
            .commit_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let covered = {
            let mut writer = self.writer()?;
            writer.delete_all_documents()?;
            self.write_epoch.fetch_add(1, Ordering::SeqCst);
            let covered = self.write_epoch.load(Ordering::SeqCst);
            writer.commit()?;
            covered
        };
        self.reader.reload()?;
        self.publish_generations(GenerationView::default());
        self.committed_epoch.store(covered, Ordering::SeqCst);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn coverage_target(&self, store: &GraphStore) -> Result<Option<u64>> {
        self.bind_store(store)
    }

    pub(crate) fn complete_coverage(&self, store: &GraphStore, target: u64) -> Result<()> {
        let current = store.search_coverage()?;
        let full_scan = self.needs_rebuild.load(Ordering::SeqCst)
            || current.is_none_or(|coverage| coverage.rebuild.is_some());
        let (count, hash, epoch, generations) = if full_scan {
            let manifest = self.load_manifest(store, target)?;
            if !manifest.valid {
                return Err(SearchError::Store(
                    crate::store::StoreError::InvalidSearchState(
                        "search-generation-manifest-invalid",
                    ),
                ));
            }
            (
                manifest.count,
                manifest.hash,
                manifest.epoch,
                Some(manifest.generations),
            )
        } else {
            let digest = store.search_manifest_digest(self.index_id)?;
            (digest.count, digest.hash, digest.epoch, None)
        };
        let revision = self.index.load_metas()?.opstamp;
        if let Some(current) = current
            && current.index_id == self.index_id
            && current.covered >= target
            && current.rebuild.is_none()
            && revision >= current.index_revision
            && current.manifest_count == count
            && current.manifest_hash == hash
        {
            self.needs_rebuild.store(false, Ordering::SeqCst);
            return Ok(());
        }
        if let Some(rebuild) = current.and_then(|coverage| coverage.rebuild) {
            if target < rebuild {
                return Err(SearchError::Store(
                    crate::store::StoreError::InvalidSearchState(
                        "search-rebuild-coverage-incomplete",
                    ),
                ));
            }
            store.finish_search_rebuild(&crate::search::queue::SearchCoverage {
                format: crate::search::queue::SEARCH_META_FORMAT,
                index_id: self.index_id,
                index_revision: revision,
                covered: target,
                rebuild: Some(rebuild),
                manifest_count: count,
                manifest_hash: hash,
                manifest_epoch: epoch,
            })?;
        } else {
            store.advance_search_coverage(&crate::search::queue::SearchCoverage {
                format: crate::search::queue::SEARCH_META_FORMAT,
                index_id: self.index_id,
                index_revision: revision,
                covered: target,
                rebuild: None,
                manifest_count: count,
                manifest_hash: hash,
                manifest_epoch: epoch,
            })?;
        }
        if let Some(generations) = generations {
            self.publish_generations(generations);
        }
        self.needs_rebuild.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn check_cancel(&self, control: &DrainControl) -> Result<()> {
        if control.is_cancelled() {
            return Err(SearchError::Cancelled);
        }
        Ok(())
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

    /// Lock the writer and recover poisoned derived state immediately.
    fn writer(&self) -> Result<MutexGuard<'_, IndexWriter>> {
        match self.writer.lock() {
            Ok(guard) => Ok(guard),
            Err(poisoned) => self.recover_writer(poisoned.into_inner()),
        }
    }

    /// Roll back poisoned uncommitted state and retain re-derivation debt.
    fn recover_writer<'a>(
        &'a self,
        mut guard: MutexGuard<'a, IndexWriter>,
    ) -> Result<MutexGuard<'a, IndexWriter>> {
        self.rebuild_owed.store(true, Ordering::SeqCst);
        guard.rollback()?;
        self.writer.clear_poison();
        Ok(guard)
    }

    /// Queue poison recovery durably and return its raised flush target.
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

        let revision = self.index.load_metas()?.opstamp;
        let raised = match store.bind_search_rebuild(&RebuildRequest {
            index_id: self.index_id,
            index_revision: revision,
        }) {
            Ok(target) => bound.max_token.map(|_| target),
            Err(error) => {
                self.rebuild_owed.store(true, Ordering::SeqCst);
                return Err(error.into());
            }
        };
        Ok((
            QueueBound {
                max_token: raised.or(bound.max_token),
                ..bound
            },
            raised,
        ))
    }

    fn now_ms(&self) -> u64 {
        let injected = self.retry_now.load(Ordering::SeqCst);
        if injected != 0 {
            return injected;
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    fn set_retry_now(&self, now_ms: u64) {
        self.retry_now.store(now_ms, Ordering::SeqCst);
    }

    pub(crate) fn retry_wait(&self, retry_at_ms: u64) -> std::time::Duration {
        std::time::Duration::from_millis(retry_at_ms.saturating_sub(self.now_ms()))
    }

    fn retry_failure(
        &self,
        store: &GraphStore,
        input: FailureInput<'_>,
    ) -> Result<Option<DrainFailure>> {
        let Some(state) = store.fts_failure(input.id)? else {
            return Ok(None);
        };
        if state.target != input.target {
            store.clear_fts_failure(input.id)?;
            return Ok(None);
        }
        if state.retry_at_ms == u64::MAX && state.id.kind == QueueKind::Reindex {
            let request = GenerationRequest {
                index_id: self.index_id,
                graph: state.id.graph.clone(),
            };
            let stage = store.search_stage(&request)?;
            store.quarantine_search_failure(&StageFailure {
                index_id: self.index_id,
                state: state.clone(),
                job: stage.clone(),
            })?;
            if let Some(stage) = stage {
                self.stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&stage.generation);
            }
        }
        if state.retry_at_ms <= self.now_ms() && state.attempts < MAX_RETRY_ATTEMPTS {
            return Ok(None);
        }
        Ok(Some(drain_failure(&state, "retry deferred")))
    }

    fn record_failure(&self, store: &GraphStore, failed: FailedItem<'_>) -> Result<DrainFailure> {
        let class = failure_class(&failed.error);
        if matches!(class, FailureClass::Rebuild) {
            self.reset_writer()?;
            return Err(failed.error);
        }
        if matches!(class, FailureClass::Global) {
            return Err(failed.error);
        }
        let previous = store.fts_failure(failed.input.id)?;
        let attempts = previous
            .filter(|state| state.target == failed.input.target)
            .map_or(1, |state| state.attempts.saturating_add(1));
        let code = failure_code(&failed.error).to_string();
        let permanent =
            matches!(class, FailureClass::ItemPermanent) || attempts >= MAX_RETRY_ATTEMPTS;
        let shift = attempts.saturating_sub(1).min(16);
        let delay = RETRY_BASE_MS
            .saturating_mul(1u64 << shift)
            .min(RETRY_MAX_MS);
        let state = RetryState {
            id: failed.input.id.clone(),
            owed_from: failed.input.owed_from,
            target: failed.input.target,
            attempts,
            retry_at_ms: if permanent {
                u64::MAX
            } else {
                self.now_ms().saturating_add(delay)
            },
            code,
            error_kind: failed.error.kind(),
        };
        if permanent && state.id.kind == QueueKind::Reindex {
            let request = GenerationRequest {
                index_id: self.index_id,
                graph: state.id.graph.clone(),
            };
            let stage = store.search_stage(&request)?;
            store.quarantine_search_failure(&StageFailure {
                index_id: self.index_id,
                state: state.clone(),
                job: stage.clone(),
            })?;
            if let Some(stage) = stage {
                self.stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&stage.generation);
            }
        } else {
            store.set_fts_failure(&state)?;
        }
        Ok(drain_failure(&state, &failed.error.to_string()))
    }

    fn reset_writer(&self) -> Result<()> {
        self.rebuild_owed.store(true, Ordering::SeqCst);
        let mut writer = self.writer()?;
        writer.rollback()?;
        let committed = self.committed_epoch.load(Ordering::SeqCst);
        self.write_epoch.store(committed, Ordering::SeqCst);
        Ok(())
    }

    fn clear_failure(&self, store: &GraphStore, id: &QueueId) -> Result<()> {
        Ok(store.clear_fts_failure(id)?)
    }

    fn record_oversized(
        &self,
        store: &GraphStore,
        oversized: &OversizedEntry,
    ) -> Result<DrainFailure> {
        let input = FailureInput {
            id: &oversized.id,
            owed_from: oversized.owed_from,
            target: oversized.target,
        };
        if let Some(failure) = self.retry_failure(store, input)? {
            return Ok(failure);
        }
        let error = SearchError::ItemTooLarge {
            bytes: oversized.bytes,
            limit: self.prepared_bytes,
        };
        self.record_failure(
            store,
            FailedItem {
                input: FailureInput {
                    id: &oversized.id,
                    owed_from: oversized.owed_from,
                    target: oversized.target,
                },
                error,
            },
        )
    }

    /// Replace one graph-and-subject document with current searchable text.
    pub fn index_resource(
        &self,
        graph_id: &str,
        subject_iri: &str,
        all_text: Option<&str>,
    ) -> Result<()> {
        let _rebuild = self.lock_graph(graph_id);
        let generation = self.active_generation(graph_id).unwrap_or_else(|| {
            self.set_generation(graph_id, Some(DIRECT_GENERATION));
            DIRECT_GENERATION
        });
        let mut writer = self.writer()?;
        self.add_document(
            &mut writer,
            ResourceDoc {
                graph_id,
                subject_iri,
                generation,
                all_text,
                delete_existing: true,
            },
        )
    }

    /// Seed a duplicate document without the normal identity replacement.
    #[cfg(test)]
    pub(crate) fn seed_duplicate(&self, graph_id: &str, subject_iri: &str) -> Result<()> {
        let _rebuild = self.lock_graph(graph_id);
        let generation = self.active_generation(graph_id).unwrap_or_else(|| {
            self.set_generation(graph_id, Some(DIRECT_GENERATION));
            DIRECT_GENERATION
        });
        let mut writer = self.writer()?;
        self.add_document(
            &mut writer,
            ResourceDoc {
                graph_id,
                subject_iri,
                generation,
                all_text: None,
                delete_existing: false,
            },
        )
    }

    /// Add `doc` to the index, optionally replacing the document with the same
    /// `(graph, subject)` key first.
    fn add_document(&self, writer: &mut IndexWriter, doc: ResourceDoc<'_>) -> Result<()> {
        let key = doc_key(doc.graph_id, doc.generation, doc.subject_iri);
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
        document.add_text(
            self.f_generation_key,
            generation_key(self.index_id, doc.graph_id, doc.generation),
        );
        document.add_u64(self.f_doc_generation, doc.generation.0);
        let scope = generation_scope(self.index_id, doc.graph_id, doc.generation);
        document.add_bytes(self.f_generation_scope, &scope);
        let stable = stable_hit_key(doc.graph_id, doc.subject_iri);
        document.add_bytes(self.f_stable_key, &stable);
        document.add_text(self.f_all_text, &all_text);

        writer.add_document(document)?;
        self.write_epoch.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Full-text search across all graphs.
    pub(crate) fn search_authorized(
        &self,
        req: AuthorizedQuery<'_>,
    ) -> crate::Result<Vec<SearchHit>> {
        let check = || Ok::<(), crate::CraqleError>(());
        self.collect_filtered(FilterQuery {
            query: req.query,
            limit: req.limit,
            subject: req.subject,
            allows: req.allows,
            check: &check,
        })
    }

    pub(crate) fn collect_filtered<E>(
        &self,
        req: FilterQuery<'_, E>,
    ) -> std::result::Result<Vec<SearchHit>, E>
    where
        E: From<SearchError>,
    {
        if req.limit == 0 {
            return Ok(Vec::new());
        }
        if req.query.len() > self.query_bytes() {
            return Err(E::from(SearchError::ItemTooLarge {
                bytes: req.query.len(),
                limit: self.query_bytes(),
            }));
        }
        let view = self.pin_view();
        self.check_bound(&view).map_err(E::from)?;
        let parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = parser
            .parse_query(&sanitize_query(req.query))
            .map_err(SearchError::from)
            .map_err(E::from)?;
        let query: Box<dyn Query> = if let Some(subject) = req.subject {
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, parsed),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.f_subject_iri, subject),
                        IndexRecordOption::Basic,
                    )),
                ),
            ]))
        } else {
            parsed
        };
        let native_limit = self.query_bytes() / 2;
        let per_rank = std::mem::size_of::<RankedDoc>().saturating_add(64);
        let rank_bytes = req.limit.saturating_mul(per_rank);
        if rank_bytes > native_limit {
            return Err(E::from(SearchError::ItemTooLarge {
                bytes: rank_bytes,
                limit: native_limit,
            }));
        }
        let weight = query
            .weight(EnableScoring::enabled_from_searcher(&view.searcher))
            .map_err(SearchError::from)
            .map_err(E::from)?;
        let mut ranked: BinaryHeap<RankedDoc> = BinaryHeap::with_capacity(req.limit);
        let mut retained_keys = HashSet::with_capacity(req.limit);
        for (segment, reader) in view.searcher.segment_readers().iter().enumerate() {
            let scopes = reader
                .fast_fields()
                .bytes(GENERATION_SCOPE_FIELD)
                .map_err(SearchError::from)
                .map_err(E::from)?;
            let stable = reader
                .fast_fields()
                .bytes(STABLE_KEY_FIELD)
                .map_err(SearchError::from)
                .map_err(E::from)?;
            let mut scorer = weight
                .scorer(reader, 1.0)
                .map_err(SearchError::from)
                .map_err(E::from)?;
            while scorer.doc() != TERMINATED {
                (req.check)()?;
                let doc = scorer.doc();
                if reader
                    .alive_bitset()
                    .is_none_or(|alive| alive.is_alive(doc))
                    && let Some(stable) = self.active_key(ActiveDoc {
                        view: &view,
                        scopes: scopes.as_ref(),
                        stable: stable.as_ref(),
                        doc,
                        allows: req.allows,
                    })?
                {
                    let score = scorer.score();
                    if !score.is_finite() {
                        return Err(E::from(SearchError::Tantivy(
                            tantivy::TantivyError::SystemError(
                                "search produced a non-finite score".to_string(),
                            ),
                        )));
                    }
                    let candidate = RankedDoc {
                        score,
                        stable,
                        address: DocAddress::new(segment as u32, doc),
                    };
                    if retained_keys.contains(&candidate.stable) {
                        if ranked
                            .iter()
                            .find(|current| current.stable == candidate.stable)
                            .is_some_and(|current| candidate < *current)
                        {
                            let mut values = std::mem::take(&mut ranked).into_vec();
                            if let Some(current) = values
                                .iter_mut()
                                .find(|current| current.stable == candidate.stable)
                            {
                                *current = candidate;
                            }
                            ranked = BinaryHeap::from(values);
                        }
                        let _ = scorer.advance();
                        continue;
                    }
                    if ranked.len() < req.limit {
                        retained_keys.insert(candidate.stable);
                        ranked.push(candidate);
                    } else if ranked.peek().is_some_and(|worst| candidate < *worst) {
                        if let Some(removed) = ranked.pop() {
                            retained_keys.remove(&removed.stable);
                        }
                        retained_keys.insert(candidate.stable);
                        ranked.push(candidate);
                    }
                }
                let _ = scorer.advance();
            }
        }
        let mut ranked = ranked.into_vec();
        ranked.sort();
        #[cfg(test)]
        {
            self.hooks.searches.fetch_add(1, Ordering::SeqCst);
            self.hooks.decoded.fetch_add(ranked.len(), Ordering::SeqCst);
        }
        let mut retained = rank_bytes;
        let mut hits = Vec::with_capacity(ranked.len());
        for ranked in ranked {
            (req.check)()?;
            let doc: TantivyDocument = view
                .searcher
                .doc(ranked.address)
                .map_err(SearchError::from)
                .map_err(E::from)?;
            let hit = self.doc_to_hit(doc, ranked.score).map_err(E::from)?;
            if stable_hit_key(&hit.graph_id, &hit.subject_iri) != ranked.stable {
                if let Some(graph) = scope_graph_at(&view, ranked.address) {
                    self.mark_damaged(&graph);
                }
                return Err(E::from(SearchError::Damaged {
                    detail: "stored identity",
                }));
            }
            retained = retained
                .saturating_add(hit.graph_id.len())
                .saturating_add(hit.subject_iri.len());
            if retained > native_limit {
                return Err(E::from(SearchError::ItemTooLarge {
                    bytes: retained,
                    limit: native_limit,
                }));
            }
            hits.push(hit);
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| {
                    stable_hit_key(&left.graph_id, &left.subject_iri)
                        .cmp(&stable_hit_key(&right.graph_id, &right.subject_iri))
                })
                .then_with(|| left.graph_id.cmp(&right.graph_id))
                .then_with(|| left.subject_iri.cmp(&right.subject_iri))
        });
        Ok(hits)
    }

    /// Full-text search across all graphs.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let allows = |_: &str| Ok::<bool, SearchError>(true);
        let check = || Ok::<(), SearchError>(());
        self.collect_filtered(FilterQuery {
            query,
            limit,
            subject: None,
            allows: &allows,
            check: &check,
        })
    }

    /// Full-text search restricted to a single graph.
    pub fn search_in_graph(
        &self,
        graph_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let allows = |candidate: &str| Ok::<bool, SearchError>(candidate == graph_id);
        let check = || Ok::<(), SearchError>(());
        self.collect_filtered(FilterQuery {
            query,
            limit,
            subject: None,
            allows: &allows,
            check: &check,
        })
    }

    /// Search one top-k collection restricted to an already authorized graph set.
    pub fn search_in_graphs(&self, req: GraphSetQuery<'_>) -> Result<Vec<SearchHit>> {
        if req.graphs.is_empty() {
            return Ok(Vec::new());
        }
        let allows = |candidate: &str| {
            Ok::<bool, SearchError>(req.graphs.iter().any(|graph| graph.as_str() == candidate))
        };
        let check = || Ok::<(), SearchError>(());
        self.collect_filtered(FilterQuery {
            query: req.query,
            limit: req.limit,
            subject: None,
            allows: &allows,
            check: &check,
        })
    }

    #[cfg(test)]
    fn collect_top_docs(&self, req: TopRequest<'_>) -> Result<Vec<SearchHit>> {
        let generations = req.view.generations.clone();
        let collector = BytesFilterCollector::new(
            GENERATION_SCOPE_FIELD.to_string(),
            move |scope: &[u8]| generations.active.contains(scope),
            TopDocs::with_limit(req.limit).order_by_score(),
        );
        let top_docs = req.view.searcher.search(req.query, &collector)?;
        #[cfg(test)]
        {
            self.hooks.searches.fetch_add(1, Ordering::SeqCst);
            self.hooks
                .decoded
                .fetch_add(top_docs.len(), Ordering::SeqCst);
        }
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = req.view.searcher.doc(doc_address)?;
            hits.push(self.doc_to_hit(doc, score)?);
        }
        Ok(hits)
    }

    fn check_bound(&self, view: &SearchView) -> Result<()> {
        if !view.bound {
            return Err(SearchError::Unbound);
        }
        Ok(())
    }

    /// Commit every preceding write and reload the reader before returning.
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
        let committed = {
            let mut writer = self.writer()?;
            let covered = self.write_epoch.load(Ordering::SeqCst);
            writer.commit().map(|_| covered)
        };
        let covered = match committed {
            Ok(covered) => covered,
            Err(error) => {
                self.reset_writer()?;
                return Err(error.into());
            }
        };
        if let Err(error) = self.reader.reload() {
            self.reset_writer()?;
            return Err(error.into());
        }
        self.publish_searcher();
        self.committed_epoch.store(covered, Ordering::SeqCst);
        Ok(())
    }

    /// Drain every queue class, committing before acknowledgement.
    /// Returns only the acknowledged count; [`Self::drain_queues`] retains detail.
    #[cfg(test)]
    pub(crate) fn process_queued_updates(
        &self,
        store: &GraphStore,
        bound: QueueBound,
    ) -> Result<usize> {
        Ok(self
            .drain_queues(
                store,
                DrainRequest {
                    bound,
                    control: DrainControl::default(),
                },
            )?
            .covered)
    }

    pub(crate) fn drain_queues(
        &self,
        store: &GraphStore,
        request: DrainRequest,
    ) -> Result<DrainProgress> {
        let _work = self
            .work_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let coverage = store.search_coverage()?;
        if !self.pin_view().bound
            || coverage.is_none_or(|coverage| coverage.index_id != self.index_id)
        {
            self.bind_store(store)?;
        }
        self.check_cancel(&request.control)?;
        let (bound, recovery) = self.settle_poisoned_writer(store, request.bound)?;
        if bound.chunk == 0 {
            return Ok(DrainProgress {
                recovery,
                remaining: true,
                ..DrainProgress::default()
            });
        }
        let quotas = self.drain_quotas(bound.chunk);
        let mut pass = DrainPass {
            store,
            control: request.control,
            bound: QueueBound { chunk: 0, ..bound },
            progress: DrainProgress {
                recovery,
                ..DrainProgress::default()
            },
            byte_limit: self.work_bytes(),
            advanced: false,
        };

        pass.byte_limit = self.work_bytes();
        pass.bound.chunk = quotas[1];
        self.drain_deleted_graphs(&mut pass)?;
        pass.byte_limit = self.work_bytes();
        pass.bound.chunk = quotas[2];
        self.drain_rebuilt_graphs(&mut pass)?;
        pass.byte_limit = self.work_bytes();
        pass.bound.chunk = quotas[3];
        self.drain_dirty_subjects(&mut pass)?;
        pass.byte_limit = self.work_bytes();
        pass.bound.chunk = quotas[4];
        self.drain_cleanup(&mut pass)?;
        pass.byte_limit = self.work_bytes();
        pass.bound.chunk = quotas[0];
        self.queue_rebuild(&mut pass)?;
        // Durable failures are reported after independent bounded work drains;
        // otherwise one bad item can hide a healthy inactive generation.
        if pass.progress.remaining && (pass.progress.covered != 0 || pass.advanced) {
            pass.progress.failures.clear();
        }
        Ok(pass.progress)
    }

    fn drain_quotas(&self, chunk: usize) -> [usize; 5] {
        let mut quotas = [chunk / 5; 5];
        let start = (self.fair_cursor.fetch_add(1, Ordering::SeqCst) % 5) as usize;
        for offset in 0..(chunk % 5) {
            quotas[(start + offset) % 5] += 1;
        }
        quotas
    }

    fn queue_rebuild(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let Some(coverage) = pass.store.search_coverage()? else {
            return Ok(());
        };
        if coverage.index_id != self.index_id || coverage.rebuild.is_none() {
            return Ok(());
        }
        let page = pass.store.queue_search_rebuild(&RebuildScan {
            index_id: self.index_id,
            row_limit: pass.bound.chunk,
            byte_limit: pass.byte_limit,
        })?;
        pass.progress.rows_read = pass.progress.rows_read.saturating_add(page.rows);
        pass.advanced |= page.rows != 0;
        pass.progress.queue_bytes = pass.progress.queue_bytes.saturating_add(page.bytes);
        pass.progress.remaining |= page.remaining;
        let target = pass
            .bound
            .max_token
            .map_or(page.target, |current| current.max(page.target));
        pass.bound.max_token = Some(target);
        pass.progress.recovery = Some(
            pass.progress
                .recovery
                .map_or(target, |current| current.max(target)),
        );
        if let Some(oversized) = page.oversized {
            return Err(SearchError::ItemTooLarge {
                bytes: oversized.bytes,
                limit: oversized.limit,
            });
        }
        Ok(())
    }

    fn drain_cleanup(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let after = self
            .scan_cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cleanup;
        let page = pass.store.scan_search_cleanup(&CleanupScan {
            after,
            row_limit: pass.bound.chunk,
            byte_limit: pass.byte_limit,
        })?;
        self.scan_cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cleanup = page.remaining.then_some(page.next).flatten();
        pass.progress.rows_read = pass.progress.rows_read.saturating_add(page.rows);
        pass.progress.queue_bytes = pass.progress.queue_bytes.saturating_add(page.bytes);
        pass.progress.remaining |= page.remaining;
        if let Some(oversized) = page.oversized {
            pass.progress.cleanup_failures.push(CleanupFailure {
                generation: oversized.generation,
                bytes: oversized.bytes,
                limit: oversized.limit,
            });
        }
        if page.entries.is_empty() {
            return Ok(());
        }
        {
            let writer = self.writer()?;
            for job in &page.entries {
                self.check_cancel(&pass.control)?;
                writer.delete_term(Term::from_field_text(
                    self.f_generation_key,
                    &generation_key(self.index_id, job.graph.as_str(), job.generation),
                ));
                self.write_epoch.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.commit()?;
        #[cfg(test)]
        self.hooks.cleanup.run();
        pass.store.ack_search_cleanup(&page.entries)?;
        Ok(())
    }

    fn queue_scan(&self, input: QueueInput<'_>) -> QueueScan {
        let cursors = self
            .scan_cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let cursor = match input.kind {
            QueueKind::Delete => cursors.deletes,
            QueueKind::Reindex => cursors.reindexes,
            QueueKind::Subject => cursors.subjects,
        };
        let after = cursor.filter(|cursor| {
            input
                .bound
                .max_token
                .is_none_or(|max_token| cursor.token <= max_token)
        });
        QueueScan {
            max_token: input.bound.max_token,
            after,
            row_limit: input.bound.chunk,
            byte_limit: input.byte_limit,
        }
    }

    fn save_scan<T>(&self, save: ScanSave<'_, T>) -> bool {
        let mut cursors = self
            .scan_cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let next = save.page.remaining.then_some(save.page.next).flatten();
        match save.kind {
            QueueKind::Delete => cursors.deletes = next,
            QueueKind::Reindex => cursors.reindexes = next,
            QueueKind::Subject => cursors.subjects = next,
        }
        !save.page.remaining && save.started
    }

    /// Settle the graphs whose search documents a removal invalidated.
    fn drain_deleted_graphs(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let scan = self.queue_scan(QueueInput {
            kind: QueueKind::Delete,
            bound: &pass.bound,
            byte_limit: pass.byte_limit,
        });
        let started = scan.after.is_some();
        let page = pass.store.scan_fts_deletes(&scan)?;
        pass.progress.remaining |= self.save_scan(ScanSave {
            kind: QueueKind::Delete,
            page: &page,
            started,
        });
        pass.progress.rows_read = pass.progress.rows_read.saturating_add(page.rows);
        pass.progress.queue_bytes = pass.progress.queue_bytes.saturating_add(page.bytes);
        pass.progress.remaining |= page.remaining;
        if let Some(id) = page.oversized.as_ref() {
            pass.progress
                .failures
                .push(self.record_oversized(pass.store, id)?);
        }
        if page.entries.is_empty() {
            return Ok(());
        }

        let mut covered = Vec::with_capacity(page.entries.len());
        for entry in &page.entries {
            self.check_cancel(&pass.control)?;
            let id = graph_queue_id(QueueKind::Delete, &entry.graph);
            let input = FailureInput {
                id: &id,
                owed_from: entry.tokens.oldest,
                target: entry.tokens.latest,
            };
            if let Some(failure) = self.retry_failure(pass.store, input)? {
                pass.progress.failures.push(failure);
                continue;
            }
            match self.settle_deleted_graph(
                GraphWork {
                    store: pass.store,
                    graph: &entry.graph,
                    control: &pass.control,
                    byte_limit: pass.byte_limit.saturating_sub(page.bytes),
                },
                entry.tokens.latest,
            ) {
                Ok(StageOutcome::Covered(target)) => {
                    self.clear_failure(pass.store, &id)?;
                    let mut settled = entry.clone();
                    settled.tokens.latest = target;
                    covered.push(settled);
                }
                Ok(StageOutcome::Pending(_)) => {
                    pass.advanced = true;
                    pass.progress.remaining = true;
                }
                Err(error) if matches!(error, SearchError::Cancelled) => return Err(error),
                Err(error) => {
                    let input = FailureInput {
                        id: &id,
                        owed_from: entry.tokens.oldest,
                        target: entry.tokens.latest,
                    };
                    pass.progress
                        .failures
                        .push(self.record_failure(pass.store, FailedItem { input, error })?);
                }
            }
        }
        if covered.is_empty() {
            return Ok(());
        }

        self.commit()?;
        pass.store.acknowledge_deleted(&covered)?;
        pass.store.acknowledge_deletes(&covered)?;
        pass.progress.covered += covered.len();
        Ok(())
    }

    /// Drop or re-derive one graph whose removal was queued.
    fn settle_deleted_graph(&self, work: GraphWork<'_>, target: u64) -> Result<StageOutcome> {
        #[cfg(test)]
        self.hooks.fail_item(work.graph)?;
        self.check_cancel(work.control)?;
        // Taken before the probe: read outside the shard, the answer can
        // already be stale by the time its branch runs.
        let _rebuild = self.lock_graph(work.graph.as_str());
        if work.store.contains_graph(work.graph)? {
            return self.stage_graph(work, target);
        } else {
            let request = GenerationRequest {
                index_id: self.index_id,
                graph: work.graph.clone(),
            };
            let stage = work.store.search_stage(&request)?;
            let switched =
                work.store
                    .delete_search_graph(&crate::search::queue::DeleteGeneration {
                        index_id: self.index_id,
                        graph: work.graph.clone(),
                        covered: target,
                    })?;
            if let Some(stage) = stage {
                self.stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&stage.generation);
            }
            self.apply_switch(&switched);
            self.delete_documents_locked(work.graph.as_str())?;
            self.commit()?;
        }
        Ok(StageOutcome::Covered(target))
    }

    /// Re-derive the graphs queued for a whole-graph rebuild.
    fn drain_rebuilt_graphs(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let scan = self.queue_scan(QueueInput {
            kind: QueueKind::Reindex,
            bound: &pass.bound,
            byte_limit: pass.byte_limit / 2,
        });
        let started = scan.after.is_some();
        let page = pass.store.scan_fts_reindexes(&scan)?;
        pass.progress.remaining |= self.save_scan(ScanSave {
            kind: QueueKind::Reindex,
            page: &page,
            started,
        });
        pass.progress.rows_read = pass.progress.rows_read.saturating_add(page.rows);
        pass.progress.queue_bytes = pass.progress.queue_bytes.saturating_add(page.bytes);
        pass.progress.remaining |= page.remaining;
        if let Some(id) = page.oversized.as_ref() {
            pass.progress
                .failures
                .push(self.record_oversized(pass.store, id)?);
        }
        if page.entries.is_empty() {
            return Ok(());
        }

        let mut covered = Vec::with_capacity(page.entries.len());
        for entry in &page.entries {
            self.check_cancel(&pass.control)?;
            let id = graph_queue_id(QueueKind::Reindex, &entry.graph);
            let input = FailureInput {
                id: &id,
                owed_from: entry.tokens.oldest,
                target: entry.tokens.latest,
            };
            if let Some(failure) = self.retry_failure(pass.store, input)? {
                pass.progress.failures.push(failure);
                continue;
            }
            match self.rebuild_queued_graph(
                GraphWork {
                    store: pass.store,
                    graph: &entry.graph,
                    control: &pass.control,
                    byte_limit: pass.byte_limit.saturating_sub(page.bytes),
                },
                entry.tokens.latest,
            ) {
                Ok(StageOutcome::Covered(target)) => {
                    self.clear_failure(pass.store, &id)?;
                    let mut settled = entry.clone();
                    settled.tokens.latest = target;
                    covered.push(settled);
                }
                Ok(StageOutcome::Pending(_)) => {
                    pass.advanced = true;
                    pass.progress.remaining = true;
                }
                Err(error) if matches!(error, SearchError::Cancelled) => return Err(error),
                Err(error) => {
                    let input = FailureInput {
                        id: &id,
                        owed_from: entry.tokens.oldest,
                        target: entry.tokens.latest,
                    };
                    pass.progress
                        .failures
                        .push(self.record_failure(pass.store, FailedItem { input, error })?);
                }
            }
        }
        if covered.is_empty() {
            return Ok(());
        }

        pass.store.acknowledge_reindexed(&covered)?;
        pass.store.acknowledge_reindex(&covered)?;
        pass.progress.covered += covered.len();
        Ok(())
    }

    /// Re-derive one queued graph, unless it has since been removed.
    fn rebuild_queued_graph(&self, work: GraphWork<'_>, target: u64) -> Result<StageOutcome> {
        #[cfg(test)]
        self.hooks.fail_item(work.graph)?;
        self.check_cancel(work.control)?;
        // A graph removed after its rebuild was queued stays removed: a scan
        // that ran anyway would republish documents for a deleted graph.
        if !work.store.contains_graph(work.graph)? {
            let _rebuild = self.lock_graph(work.graph.as_str());
            let request = GenerationRequest {
                index_id: self.index_id,
                graph: work.graph.clone(),
            };
            let stage = work.store.search_stage(&request)?;
            let switched =
                work.store
                    .delete_search_graph(&crate::search::queue::DeleteGeneration {
                        index_id: self.index_id,
                        graph: work.graph.clone(),
                        covered: target,
                    })?;
            if let Some(stage) = stage {
                self.stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&stage.generation);
            }
            self.apply_switch(&switched);
            self.delete_documents_locked(work.graph.as_str())?;
            self.commit()?;
            return Ok(StageOutcome::Covered(target));
        }
        let _rebuild = self.lock_graph(work.graph.as_str());
        self.stage_graph(work, target)
    }

    /// Apply the queued per-subject updates.
    fn drain_dirty_subjects(&self, pass: &mut DrainPass<'_>) -> Result<()> {
        let scan = self.queue_scan(QueueInput {
            kind: QueueKind::Subject,
            bound: &pass.bound,
            byte_limit: pass.byte_limit / 2,
        });
        let started = scan.after.is_some();
        let page = pass.store.scan_fts_subjects(&scan)?;
        pass.progress.remaining |= self.save_scan(ScanSave {
            kind: QueueKind::Subject,
            page: &page,
            started,
        });
        pass.progress.rows_read = pass.progress.rows_read.saturating_add(page.rows);
        pass.progress.queue_bytes = pass.progress.queue_bytes.saturating_add(page.bytes);
        pass.progress.remaining |= page.remaining;
        if let Some(id) = page.oversized.as_ref() {
            pass.progress
                .failures
                .push(self.record_oversized(pass.store, id)?);
        }
        if page.entries.is_empty() {
            return Ok(());
        }

        // Hold graph shards across read and apply so rebuild cannot straddle them.
        let rebuild_guards =
            self.lock_graphs(page.entries.iter().map(|entry| entry.graph.as_str()));

        // Phase 1: read every update from the store with NO writer lock held,
        // stopping once the prepared text reaches the pass budget.
        let mut seen = HashSet::with_capacity(page.entries.len());
        let mut caches = StoreSyncCaches::new(pass.store);
        let mut prepared: Vec<(PreparedDocOp, DirtySubject)> = Vec::new();
        let mut prepared_bytes = 0usize;
        let prepare_limit = pass.byte_limit.saturating_sub(page.bytes);
        let rebuild_pending = pass.store.search_coverage()?.is_none_or(|coverage| {
            coverage.index_id != self.index_id || coverage.rebuild.is_some()
        });
        for entry in &page.entries {
            self.check_cancel(&pass.control)?;
            if prepared_bytes.saturating_add(caches.orphan_bytes) >= prepare_limit {
                // The rest stays queued: acknowledging entries this pass never
                // prepared would drop their updates permanently.
                pass.progress.remaining = true;
                break;
            }
            if !seen.insert((entry.graph.clone(), entry.subject)) {
                pass.progress.remaining = true;
                continue;
            }
            let id = subject_queue_id(&entry.graph, entry.subject);
            let input = FailureInput {
                id: &id,
                owed_from: entry.tokens.oldest,
                target: entry.tokens.latest,
            };
            if let Some(failure) = self.retry_failure(pass.store, input)? {
                pass.progress.failures.push(failure);
                continue;
            }
            if pass
                .store
                .search_stage(&GenerationRequest {
                    index_id: self.index_id,
                    graph: entry.graph.clone(),
                })?
                .is_some()
            {
                pass.progress.remaining = true;
                continue;
            }
            if rebuild_pending && self.active_generation(entry.graph.as_str()).is_none() {
                let (recovery, queued) = pass.store.ensure_reindex(&entry.graph)?;
                pass.progress.recovery = Some(
                    pass.progress
                        .recovery
                        .map_or(recovery, |current| current.max(recovery)),
                );
                pass.advanced |= queued;
                pass.progress.remaining = true;
                continue;
            }
            let switched = pass.store.ensure_search_generation(&GenerationRequest {
                index_id: self.index_id,
                graph: entry.graph.clone(),
            })?;
            self.apply_switch(&switched);
            let Some(generation) = switched.active else {
                pass.progress.remaining = true;
                continue;
            };
            let orphan_bytes = caches.orphan_bytes;
            let byte_limit = prepare_limit
                .saturating_sub(prepared_bytes)
                .saturating_sub(orphan_bytes);
            match self.prepare_queued_entry(
                &mut caches,
                PrepareSubject {
                    store: pass.store,
                    graph: &entry.graph,
                    subject: entry.subject,
                    byte_limit,
                    generation,
                },
            ) {
                Ok(op) => {
                    prepared_bytes = prepared_bytes.saturating_add(op.held_bytes());
                    prepared.push((op, entry.clone()));
                }
                Err(error) if matches!(error, SearchError::Cancelled) => return Err(error),
                Err(error) => {
                    let input = FailureInput {
                        id: &id,
                        owed_from: entry.tokens.oldest,
                        target: entry.tokens.latest,
                    };
                    pass.progress
                        .failures
                        .push(self.record_failure(pass.store, FailedItem { input, error })?);
                }
            }
        }

        // Phase 2: apply the prepared ops in queue order under the writer lock.
        let mut applied = Vec::with_capacity(prepared.len());
        let mut apply_error = None;
        {
            // Guards the Tantivy writer; no store reads happen inside.
            let mut writer = self.writer()?;
            for (op, entry) in &prepared {
                self.check_cancel(&pass.control)?;
                match self.apply_prepared_op(&mut writer, op) {
                    Ok(()) => applied.push(entry.clone()),
                    Err(error) => {
                        apply_error = Some((entry.clone(), error));
                        break;
                    }
                }
            }
        }
        if let Some((entry, error)) = apply_error {
            let id = subject_queue_id(&entry.graph, entry.subject);
            pass.progress.failures.push(self.record_failure(
                pass.store,
                FailedItem {
                    input: FailureInput {
                        id: &id,
                        owed_from: entry.tokens.oldest,
                        target: entry.tokens.latest,
                    },
                    error,
                },
            )?);
        }
        let mut covered = Vec::with_capacity(applied.len());
        for entry in applied {
            let id = subject_queue_id(&entry.graph, entry.subject);
            self.clear_failure(pass.store, &id)?;
            covered.push(entry);
        }
        drop(rebuild_guards);

        if covered.is_empty() {
            return Ok(());
        }

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
                self.delete_doc(writer, doc);
                Ok(())
            }
            PreparedDocOp::Upsert { doc, all_text } => self.add_document(
                writer,
                ResourceDoc {
                    graph_id: &doc.graph_iri,
                    subject_iri: &doc.subject_iri,
                    generation: doc.generation,
                    all_text: all_text.as_deref(),
                    delete_existing: true,
                },
            ),
        }
    }

    fn stage_graph(&self, work: GraphWork<'_>, target: u64) -> Result<StageOutcome> {
        let request = GenerationRequest {
            index_id: self.index_id,
            graph: work.graph.clone(),
        };
        let current = work.store.search_generation(&request)?;
        let prior = work.store.search_stage(&request)?;
        if current.active.is_some() && current.covered >= target {
            self.set_generation(work.graph.as_str(), current.active);
            return Ok(StageOutcome::Covered(target));
        }

        let graph_term = EncodedTerm::from_named_node(&work.graph.0);
        let Some(graph_tid) = work.store.lookup_term(&graph_term)? else {
            let stage = work.store.search_stage(&request)?;
            let switched =
                work.store
                    .delete_search_graph(&crate::search::queue::DeleteGeneration {
                        index_id: self.index_id,
                        graph: work.graph.clone(),
                        covered: target,
                    })?;
            if let Some(stage) = stage {
                self.stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&stage.generation);
            }
            self.apply_switch(&switched);
            self.delete_documents_locked(work.graph.as_str())?;
            self.commit()?;
            return Ok(StageOutcome::Covered(target));
        };
        let stage_target = prior.as_ref().map_or(target, |job| job.target);
        let mut job = work.store.begin_search_stage(&StageRequest {
            index_id: self.index_id,
            graph: work.graph.clone(),
            target: stage_target,
            session: self.session,
        })?;
        if let Some(prior) = prior.filter(|prior| prior.generation != job.generation) {
            self.stage_sources
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&prior.generation);
        }
        if job.complete {
            return self.switch_stage(work.store, &job);
        }
        if job.session != self.session {
            return Err(SearchError::Store(
                crate::store::StoreError::InvalidSearchState("stage-session-mismatch"),
            ));
        }
        let work_limit = work.byte_limit.min(self.work_bytes());
        let source = {
            let sources = self
                .stage_sources
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            sources.get(&job.generation).cloned()
        };
        let (source, source_bytes) = match source {
            Some(source) => (source, 0),
            None => {
                let snapshot = work.store.search_snapshot();
                let mut sources = self
                    .stage_sources
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let source_limit = work_limit / 2;
                if source_limit < STAGE_SOURCE_BYTES {
                    return Err(SearchError::ItemTooLarge {
                        bytes: STAGE_SOURCE_BYTES,
                        limit: source_limit,
                    });
                }
                let artifact_limit = source_limit / 32;
                let orphaned = match snapshot.orphaned_ids(graph_tid, artifact_limit) {
                    Ok(orphaned) => orphaned,
                    Err(crate::store::StoreError::LimitExceeded {
                        resource: "search diagnostics rows",
                        limit,
                        actual,
                    }) => {
                        return Err(SearchError::SourceTooLarge {
                            rows: usize::try_from(actual).unwrap_or(usize::MAX),
                            limit: usize::try_from(limit).unwrap_or(usize::MAX),
                        });
                    }
                    Err(crate::store::StoreError::LimitExceeded { limit, actual, .. }) => {
                        return Err(SearchError::ItemTooLarge {
                            bytes: usize::try_from(actual).unwrap_or(usize::MAX),
                            limit: usize::try_from(limit).unwrap_or(usize::MAX),
                        });
                    }
                    Err(error) => return Err(error.into()),
                };
                let bytes = STAGE_SOURCE_BYTES.saturating_add(
                    orphaned
                        .len()
                        .saturating_mul(std::mem::size_of::<TermId>() * 4),
                );
                if bytes > source_limit {
                    return Err(SearchError::ItemTooLarge {
                        bytes,
                        limit: source_limit,
                    });
                }
                let source = Arc::new(StageSource {
                    snapshot,
                    orphaned,
                    bytes,
                });
                sources.insert(job.generation, source.clone());
                (source, bytes)
            }
        };

        self.check_cancel(work.control)?;
        let page_limit = work_limit.saturating_sub(source_bytes);
        let page = source.snapshot.scan_graph(&GraphScan {
            graph: graph_tid,
            after: job.cursor,
            row_limit: 1,
            byte_limit: page_limit / 2,
        })?;
        if let Some(oversized) = page.oversized {
            return Err(SearchError::ItemTooLarge {
                bytes: oversized.bytes,
                limit: oversized.limit,
            });
        }
        let Some(quad) = page.entries.first().copied() else {
            job.complete = true;
            work.store.finish_search_stage(&job)?;
            #[cfg(test)]
            self.hooks.stage.run();
            self.check_cancel(work.control)?;
            return self.switch_stage(work.store, &job);
        };

        let prepared = self.prepare_stage(StageSubject {
            store: work.store,
            source: &source,
            graph: work.graph,
            graph_tid,
            subject: quad.subject,
            generation: job.generation,
            control: work.control,
            byte_limit: page_limit.saturating_sub(page.bytes),
        })?;
        let indexed = usize::from(matches!(&prepared.op, PreparedDocOp::Upsert { .. }));
        {
            let mut writer = self.writer()?;
            self.apply_prepared_op(&mut writer, &prepared.op)?;
        }
        self.commit()?;
        #[cfg(test)]
        self.hooks.page.run();
        self.check_cancel(work.control)?;
        job.cursor = Some(subject_cursor(graph_tid, quad.subject));
        job.rows = job.rows.saturating_add(
            u64::try_from(prepared.rows.saturating_add(page.rows)).unwrap_or(u64::MAX),
        );
        job.bytes = job.bytes.saturating_add(
            u64::try_from(prepared.bytes.saturating_add(page.bytes)).unwrap_or(u64::MAX),
        );
        work.store.advance_search_stage(&job)?;

        #[cfg(test)]
        self.hooks.rebuild.run();

        Ok(StageOutcome::Pending(indexed))
    }

    fn switch_stage(&self, store: &GraphStore, job: &StageJob) -> Result<StageOutcome> {
        let switched = store.switch_search_stage(job)?;
        self.stage_sources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&job.generation);
        self.apply_switch(&switched);
        #[cfg(test)]
        self.hooks.switch.run();
        Ok(StageOutcome::Covered(job.target))
    }

    fn prepare_stage(&self, req: StageSubject<'_>) -> Result<PreparedStage> {
        let subject_term = req.store.decode_term_arc(req.subject)?;
        let doc = DocIdentity {
            graph_iri: req.graph.as_str().to_string(),
            subject_iri: term_to_string(&subject_term),
            generation: req.generation,
        };
        let identity_bytes = doc.graph_iri.len().saturating_add(doc.subject_iri.len());
        if identity_bytes > req.byte_limit {
            return Err(SearchError::ItemTooLarge {
                bytes: identity_bytes,
                limit: req.byte_limit,
            });
        }
        let remaining = req.byte_limit.saturating_sub(identity_bytes);
        let source_limit = remaining / 2;
        let text_limit = remaining.saturating_sub(source_limit);
        if req.source.orphaned.contains(&req.subject) {
            return Ok(PreparedStage {
                op: PreparedDocOp::Delete { doc },
                rows: 0,
                bytes: 0,
            });
        }

        let mut all_text = String::new();
        let mut after = None;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut found = false;
        loop {
            self.check_cancel(req.control)?;
            let page = req.source.snapshot.scan_subject(
                &SubjectScan {
                    graph: req.graph_tid,
                    subject: req.subject,
                    after,
                    row_limit: SOURCE_PAGE_ROWS,
                    byte_limit: source_limit.saturating_sub(bytes),
                },
                &SEARCHABLE_IDS,
            )?;
            if let Some(oversized) = page.oversized {
                return Err(SearchError::ItemTooLarge {
                    bytes: oversized.bytes,
                    limit: oversized.limit,
                });
            }
            found |= !page.entries.is_empty();
            rows = rows.saturating_add(page.rows);
            bytes = bytes.saturating_add(page.bytes);
            for (_, object) in page.entries {
                append_searchable_text(&mut all_text, &object, text_limit)?;
            }
            if !page.remaining {
                break;
            }
            if rows >= SOURCE_PAGE_ROWS {
                return Err(SearchError::SourceTooLarge {
                    rows: rows.saturating_add(1),
                    limit: SOURCE_PAGE_ROWS,
                });
            }
            if bytes >= source_limit {
                return Err(SearchError::ItemTooLarge {
                    bytes: bytes.saturating_add(1),
                    limit: source_limit,
                });
            }
            after = page.next;
            if after.is_none() {
                return Err(SearchError::Store(
                    crate::store::StoreError::InvalidSearchState(
                        "stage-subject-continuation-missing",
                    ),
                ));
            }
        }
        let op = if found
            || req
                .source
                .snapshot
                .subject_present(req.graph_tid, req.subject)?
        {
            PreparedDocOp::Upsert {
                doc,
                all_text: (!all_text.is_empty()).then_some(all_text),
            }
        } else {
            PreparedDocOp::Delete { doc }
        };
        Ok(PreparedStage { op, rows, bytes })
    }

    /// Reindex searchable graph subjects from RDF source and return their count.
    pub fn reindex_from_store(&self, store: &GraphStore, graph: &GraphId) -> Result<usize> {
        let _work = self
            .work_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let control = DrainControl::default();
        let mut count = 0usize;
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        if let Some(graph_tid) = store.lookup_term(&graph_term)? {
            let mut batch = store.new_batch();
            store.enqueue_fts_reindex(&mut batch, graph_tid)?;
            store.commit(batch)?;
        }
        let target = store.current_dirty_token();
        loop {
            let outcome = {
                let _rebuild = self.lock_graph(graph.as_str());
                self.stage_graph(
                    GraphWork {
                        store,
                        graph,
                        control: &control,
                        byte_limit: self.work_bytes(),
                    },
                    target,
                )?
            };
            match outcome {
                StageOutcome::Pending(indexed) => count = count.saturating_add(indexed),
                StageOutcome::Covered(_) => {
                    store.clear_graph_queue(graph, target)?;
                    return Ok(count);
                }
            }
        }
    }

    fn doc_to_hit(&self, doc: TantivyDocument, score: f32) -> Result<SearchHit> {
        let graph_id = first_text(&doc, self.f_graph_id);
        let subject_iri = first_text(&doc, self.f_subject_iri);
        let (graph_id, subject_iri) = match (graph_id, subject_iri) {
            (Some(graph_id), Some(subject_iri)) => (graph_id, subject_iri),
            _ => first_text(&doc, self.f_doc_key)
                .as_deref()
                .and_then(split_doc_key)
                .ok_or(SearchError::Damaged {
                    detail: "stored identity",
                })?,
        };
        if graph_id.is_empty() || subject_iri.is_empty() {
            return Err(SearchError::Damaged {
                detail: "stored identity",
            });
        }
        Ok(SearchHit {
            graph_id,
            subject_iri,
            score,
        })
    }

    /// Returns the stable key of an active, allowed document, or `None` for skipped ones.
    fn active_key<E>(&self, req: ActiveDoc<'_, E>) -> std::result::Result<Option<[u8; 32]>, E>
    where
        E: From<SearchError>,
    {
        #[cfg(test)]
        if self.hooks.metadata_io.swap(false, Ordering::SeqCst) {
            return Err(E::from(SearchError::Io(std::io::Error::other(
                "injected metadata read failure",
            ))));
        }
        let scope = column_bytes(req.scopes, req.doc).map_err(E::from)?;
        let graph = scope_graph(&scope).ok_or(E::from(SearchError::Damaged {
            detail: "generation scope",
        }))?;
        if !req.view.generations.active.contains(scope.as_slice()) || !(req.allows)(graph)? {
            return Ok(None);
        }
        let stable = column_bytes(req.stable, req.doc).map_err(E::from)?;
        match <[u8; 32]>::try_from(stable.as_slice()) {
            Ok(stable) => Ok(Some(stable)),
            Err(_) => {
                self.mark_damaged(graph);
                Err(E::from(SearchError::Damaged {
                    detail: "stable key",
                }))
            }
        }
    }

    /// Records a readable damaged graph for the maintenance worker to reindex.
    fn mark_damaged(&self, graph: &str) {
        let mut damaged = self.damaged.lock().unwrap_or_else(PoisonError::into_inner);
        if damaged.len() < DAMAGED_GRAPHS {
            damaged.insert(graph.to_owned());
        }
    }

    /// Takes the damaged graphs recorded since the previous call.
    pub(crate) fn take_damaged(&self) -> BTreeSet<String> {
        std::mem::take(&mut *self.damaged.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn delete_doc(&self, writer: &mut IndexWriter, doc: &DocIdentity) {
        writer.delete_term(Term::from_field_text(
            self.f_doc_key,
            &doc_key(&doc.graph_iri, doc.generation, &doc.subject_iri),
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

/// Prepare one subject replacement or deletion without the writer lock.
fn prepare_subject_op(
    req: PrepareSubject<'_>,
    caches: &mut StoreSyncCaches,
) -> Result<PreparedDocOp> {
    let subject_term = req.store.decode_term_arc(req.subject)?;
    let doc = DocIdentity {
        graph_iri: req.graph.as_str().to_string(),
        subject_iri: term_to_string(&subject_term),
        generation: req.generation,
    };
    let identity_bytes = doc.graph_iri.len().saturating_add(doc.subject_iri.len());
    if identity_bytes > req.byte_limit {
        return Err(SearchError::ItemTooLarge {
            bytes: identity_bytes,
            limit: req.byte_limit,
        });
    }
    let text_limit = req.byte_limit.saturating_sub(identity_bytes);

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
    let cache_before = caches.orphan_bytes;
    let hidden = caches
        .orphaned(OrphanInput {
            graph: req.graph,
            graph_tid,
            byte_limit: text_limit,
        })?
        .contains(&req.subject);
    if hidden {
        return Ok(PreparedDocOp::Delete { doc });
    }
    let remaining = text_limit.saturating_sub(caches.orphan_bytes.saturating_sub(cache_before));
    let source_limit = remaining / 2;
    let text_limit = remaining.saturating_sub(source_limit);

    let mut all_text = String::new();
    let mut after = None;
    let mut found = false;
    let mut source_bytes = 0usize;
    let mut source_rows = 0usize;
    loop {
        let page = caches.snapshot.scan_subject(
            &SubjectScan {
                graph: graph_tid,
                subject: req.subject,
                after,
                row_limit: SOURCE_PAGE_ROWS,
                byte_limit: source_limit.saturating_sub(source_bytes),
            },
            &SEARCHABLE_IDS,
        )?;
        if let Some(oversized) = page.oversized {
            return Err(SearchError::ItemTooLarge {
                bytes: oversized.bytes,
                limit: oversized.limit,
            });
        }
        found |= !page.entries.is_empty();
        source_rows = source_rows.saturating_add(page.rows);
        source_bytes = source_bytes.saturating_add(page.bytes);
        for (_, object) in page.entries {
            append_searchable_text(&mut all_text, &object, text_limit)?;
        }
        if !page.remaining {
            break;
        }
        if source_rows >= SOURCE_PAGE_ROWS {
            return Err(SearchError::SourceTooLarge {
                rows: source_rows.saturating_add(1),
                limit: SOURCE_PAGE_ROWS,
            });
        }
        if source_bytes >= source_limit {
            return Err(SearchError::ItemTooLarge {
                bytes: source_bytes.saturating_add(1),
                limit: source_limit,
            });
        }
        after = page.next;
        if after.is_none() {
            return Err(SearchError::Store(
                crate::store::StoreError::InvalidSearchState("subject-scan-continuation-missing"),
            ));
        }
    }
    if !found && !caches.snapshot.subject_present(graph_tid, req.subject)? {
        return Ok(PreparedDocOp::Delete { doc });
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

fn doc_key(graph_id: &str, generation: GenerationId, subject_iri: &str) -> String {
    format!("{graph_id}\u{1f}{}\u{1f}{subject_iri}", generation.0)
}

fn split_doc_key(doc_key: &str) -> Option<(String, String)> {
    let (graph_id, remainder) = doc_key.split_once('\u{1f}')?;
    let (_, subject_iri) = remainder.split_once('\u{1f}')?;
    Some((graph_id.to_string(), subject_iri.to_string()))
}

fn generation_key(index_id: [u8; 16], graph_id: &str, generation: GenerationId) -> String {
    format!(
        "{}\u{1f}{graph_id}\u{1f}{}",
        uuid::Uuid::from_bytes(index_id),
        generation.0
    )
}

fn generation_scope(index_id: [u8; 16], graph_id: &str, generation: GenerationId) -> Vec<u8> {
    let mut scope = Vec::with_capacity(16 + graph_id.len() + 8);
    scope.extend_from_slice(&index_id);
    scope.extend_from_slice(graph_id.as_bytes());
    scope.extend_from_slice(&generation.0.to_be_bytes());
    scope
}

fn scope_graph(scope: &[u8]) -> Option<&str> {
    if scope.len() < 24 {
        return None;
    }
    std::str::from_utf8(&scope[16..scope.len() - 8]).ok()
}

/// Reads the graph named by one document's generation scope.
fn scope_graph_at(view: &SearchView, address: DocAddress) -> Option<String> {
    let reader = view.searcher.segment_reader(address.segment_ord);
    let scopes = reader.fast_fields().bytes(GENERATION_SCOPE_FIELD).ok()?;
    let scope = column_bytes(scopes.as_ref(), address.doc_id).ok()?;
    scope_graph(&scope).map(str::to_owned)
}

/// Reads a required single-value bytes field that every current document carries.
fn column_bytes(column: Option<&tantivy::columnar::BytesColumn>, doc: DocId) -> Result<Vec<u8>> {
    let damaged = || SearchError::Damaged {
        detail: "metadata column",
    };
    let column = column.ok_or_else(damaged)?;
    let ord = column.term_ords(doc).next().ok_or_else(damaged)?;
    let mut bytes = Vec::new();
    column
        .ord_to_bytes(ord, &mut bytes)?
        .then_some(bytes)
        .ok_or_else(damaged)
}

pub(crate) fn stable_hit_key(graph_id: &str, subject_iri: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(graph_id.len() as u64).to_be_bytes());
    hasher.update(graph_id.as_bytes());
    hasher.update(&(subject_iri.len() as u64).to_be_bytes());
    hasher.update(subject_iri.as_bytes());
    *hasher.finalize().as_bytes()
}

fn subject_cursor(graph: TermId, subject: TermId) -> [u8; 64] {
    let mut cursor = [u8::MAX; 64];
    cursor[..16].copy_from_slice(&graph.to_be_bytes());
    cursor[16..32].copy_from_slice(&subject.to_be_bytes());
    cursor
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

/// Convert an EncodedTerm to a plain string (IRI without angle brackets,
/// or the raw string representation for other term types).
fn term_to_string(term: &EncodedTerm) -> String {
    if term.0.starts_with('<') && term.0.ends_with('>') {
        term.0[1..term.0.len() - 1].to_string()
    } else {
        term.0.clone()
    }
}

fn append_searchable_text(buffer: &mut String, term: &EncodedTerm, limit: usize) -> Result<()> {
    let Some(value) = searchable_term_text(term) else {
        return Ok(());
    };
    let separator = usize::from(!buffer.is_empty());
    let bytes = buffer
        .len()
        .saturating_add(separator)
        .saturating_add(value.len());
    if bytes > limit {
        return Err(SearchError::ItemTooLarge { bytes, limit });
    }
    if !buffer.is_empty() {
        buffer.push(' ');
    }
    buffer.push_str(&value);
    Ok(())
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

fn failure_code(error: &SearchError) -> &'static str {
    match error {
        SearchError::ItemTooLarge { .. } => "item-too-large",
        SearchError::SourceTooLarge { .. } => "item-too-large",
        SearchError::Cancelled => "cancelled",
        SearchError::Unbound => "index-unbound",
        SearchError::Store(error) if error.rejects_record() => "item-invalid",
        SearchError::Store(_) => "store-transient",
        SearchError::Tantivy(_) => "index-transient",
        SearchError::Damaged { .. } => "index-damaged",
        SearchError::QueryParse(_) => "query-invalid",
        SearchError::Io(_) => "io-transient",
    }
}

fn failure_class(error: &SearchError) -> FailureClass {
    match error {
        SearchError::ItemTooLarge { .. } | SearchError::SourceTooLarge { .. } => {
            FailureClass::ItemPermanent
        }
        SearchError::Store(error) if error.rejects_record() => FailureClass::ItemPermanent,
        SearchError::Store(crate::store::StoreError::InvalidSearchState(code))
            if matches!(
                *code,
                "search-diagnostics-missing" | "search-diagnostics-stale"
            ) =>
        {
            FailureClass::ItemRetryable
        }
        SearchError::Store(crate::store::StoreError::GraphNotFound(_)) => {
            FailureClass::ItemRetryable
        }
        SearchError::Tantivy(_) | SearchError::Damaged { .. } => FailureClass::Rebuild,
        SearchError::Store(_)
        | SearchError::QueryParse(_)
        | SearchError::Io(_)
        | SearchError::Cancelled
        | SearchError::Unbound => FailureClass::Global,
    }
}

fn graph_queue_id(kind: QueueKind, graph: &GraphId) -> QueueId {
    QueueId {
        kind,
        graph: graph.clone(),
        subject: None,
    }
}

fn subject_queue_id(graph: &GraphId, subject: TermId) -> QueueId {
    QueueId {
        kind: QueueKind::Subject,
        graph: graph.clone(),
        subject: Some(subject),
    }
}

fn drain_failure(state: &RetryState, diagnostic: &str) -> DrainFailure {
    DrainFailure {
        kind: state.id.kind,
        error_kind: state.error_kind,
        graph: state.id.graph.clone(),
        owed_from: state.owed_from,
        target: state.target,
        attempts: state.attempts,
        retry_at_ms: state.retry_at_ms,
        code: state.code.clone(),
        diagnostic: diagnostic.to_string(),
    }
}

#[cfg(test)]
#[path = "bench.rs"]
mod search_bench;

#[cfg(test)]
#[path = "recovery.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn text_limit_rejects() {
        let mut text = String::new();
        let term = EncodedTerm("\"oversized\"".to_string());

        let error = append_searchable_text(&mut text, &term, 4).unwrap_err();

        assert!(matches!(error, SearchError::ItemTooLarge { limit: 4, .. }));
        assert!(text.is_empty(), "rejection must not retain partial text");
    }

    #[test]
    fn search_skips_fanout() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:search-fanout");
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        node.create_crate(
            &writer_auth(),
            crate_request(&graph, "originalneedle", true),
        )
        .unwrap();
        let graph_tid = graph_term(&node.store, &graph);
        let subject = EncodedTerm::from_named_node(&graph.0);
        let scan = SubjectScan {
            graph: graph_tid,
            subject: graph_tid,
            after: None,
            row_limit: SOURCE_PAGE_ROWS,
            byte_limit: usize::MAX,
        };
        let baseline = node
            .store
            .search_snapshot()
            .scan_subject(&scan, &SEARCHABLE_IDS)
            .unwrap();
        let parts = (0..3_101)
            .map(|index| {
                (
                    subject.clone(),
                    EncodedTerm::from_named_node(&crate::vocab::schema_has_part()),
                    EncodedTerm(format!("<urn:test:part-{index}>")),
                )
            })
            .collect();
        node.insert_quads(&writer_auth(), &graph, parts).unwrap();
        let snapshot = node.store.search_snapshot();
        let page = snapshot.scan_subject(&scan, &SEARCHABLE_IDS).unwrap();
        assert_eq!(baseline.entries, page.entries);
        assert_eq!(baseline.rows, page.rows);
        assert_eq!(baseline.bytes, page.bytes);
        assert!(!page.remaining);

        let mut first_scan = scan.clone();
        first_scan.row_limit = 1;
        let first = snapshot.scan_subject(&first_scan, &SEARCHABLE_IDS).unwrap();
        assert!(first.remaining);
        node.insert_quads(
            &writer_auth(),
            &graph,
            vec![(
                subject.clone(),
                EncodedTerm("<https://schema.org/description>".to_string()),
                EncodedTerm("\"laterneedle\"".to_string()),
            )],
        )
        .unwrap();
        first_scan.after = first.next;
        first_scan.row_limit = SOURCE_PAGE_ROWS;
        let next = snapshot.scan_subject(&first_scan, &SEARCHABLE_IDS).unwrap();
        let mut pinned = first.entries;
        pinned.extend(next.entries);
        assert_eq!(baseline.entries, pinned, "pages must retain one snapshot");

        let source = StageSource {
            snapshot,
            orphaned: HashSet::new(),
            bytes: 0,
        };
        let prepared = node
            .search
            .prepare_stage(StageSubject {
                store: &node.store,
                source: &source,
                graph: &graph,
                graph_tid,
                subject: graph_tid,
                generation: DIRECT_GENERATION,
                control: &DrainControl::default(),
                byte_limit: node.search.work_bytes(),
            })
            .unwrap();
        assert_eq!(baseline.rows, prepared.rows);
        assert!(
            matches!(prepared.op, PreparedDocOp::Upsert { all_text: Some(text), .. }
            if text.contains("originalneedle") && !text.contains("laterneedle"))
        );
        let mut caches = StoreSyncCaches::new(&node.store);
        let current = prepare_subject_op(
            PrepareSubject {
                store: &node.store,
                graph: &graph,
                subject: graph_tid,
                byte_limit: node.search.work_bytes(),
                generation: DIRECT_GENERATION,
            },
            &mut caches,
        )
        .unwrap();
        assert!(
            matches!(current, PreparedDocOp::Upsert { all_text: Some(text), .. }
            if text.contains("originalneedle") && text.contains("laterneedle"))
        );
        node.flush_search_updates().unwrap();
        assert_eq!(1, node.search.search("laterneedle", 10).unwrap().len());
    }

    #[test]
    fn search_limits_values() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:search-value-limit");
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        node.create_crate(&writer_auth(), crate_request(&graph, "boundedneedle", true))
            .unwrap();
        let graph_tid = graph_term(&node.store, &graph);
        let subject = EncodedTerm::from_named_node(&graph.0);
        let values = (0..SOURCE_PAGE_ROWS)
            .map(|index| {
                (
                    subject.clone(),
                    SEARCHABLE_PREDICATES[index % 4].clone(),
                    EncodedTerm(format!("\"value-{index}\"")),
                )
            })
            .collect();
        node.insert_quads(&writer_auth(), &graph, values).unwrap();
        let snapshot = node.store.search_snapshot();
        let scan = SubjectScan {
            graph: graph_tid,
            subject: graph_tid,
            after: None,
            row_limit: SOURCE_PAGE_ROWS,
            byte_limit: usize::MAX,
        };
        let page = snapshot.scan_subject(&scan, &SEARCHABLE_IDS).unwrap();
        assert_eq!(SOURCE_PAGE_ROWS, page.rows);
        assert!(
            page.remaining,
            "all predicate prefixes must share one row cap"
        );
        let mut caches = StoreSyncCaches::new(&node.store);
        let error = prepare_subject_op(
            PrepareSubject {
                store: &node.store,
                graph: &graph,
                subject: graph_tid,
                byte_limit: node.search.work_bytes(),
                generation: DIRECT_GENERATION,
            },
            &mut caches,
        );
        assert!(matches!(
            error,
            Err(SearchError::SourceTooLarge {
                limit: SOURCE_PAGE_ROWS,
                ..
            })
        ));
        let source = StageSource {
            snapshot: snapshot.clone(),
            orphaned: HashSet::new(),
            bytes: 0,
        };
        let staged = node.search.prepare_stage(StageSubject {
            store: &node.store,
            source: &source,
            graph: &graph,
            graph_tid,
            subject: graph_tid,
            generation: DIRECT_GENERATION,
            control: &DrainControl::default(),
            byte_limit: node.search.work_bytes(),
        });
        assert!(matches!(
            staged,
            Err(SearchError::SourceTooLarge {
                limit: SOURCE_PAGE_ROWS,
                ..
            })
        ));
        let mut scan = scan;
        scan.byte_limit = page.bytes - 1;
        let bounded = snapshot.scan_subject(&scan, &SEARCHABLE_IDS).unwrap();
        assert!(bounded.remaining);
        assert!(bounded.bytes <= scan.byte_limit);
        assert!(bounded.rows < page.rows);
    }

    #[test]
    fn search_keeps_identifiers() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:search-identity");
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        node.create_crate(
            &writer_auth(),
            crate_request(&graph, "identityneedle", true),
        )
        .unwrap();
        let subject = EncodedTerm::from_subject_id("ro-crate-metadata.json");
        let subject = node.store.lookup_term(&subject).unwrap().unwrap();
        let mut caches = StoreSyncCaches::new(&node.store);
        let prepared = prepare_subject_op(
            PrepareSubject {
                store: &node.store,
                graph: &graph,
                subject,
                byte_limit: node.search.work_bytes(),
                generation: DIRECT_GENERATION,
            },
            &mut caches,
        )
        .unwrap();
        assert!(matches!(
            prepared,
            PreparedDocOp::Upsert { all_text: None, .. }
        ));
        let absent = node
            .store
            .encode_term(&EncodedTerm::from_subject_id("urn:test:absent"))
            .unwrap();
        let prepared = prepare_subject_op(
            PrepareSubject {
                store: &node.store,
                graph: &graph,
                subject: absent,
                byte_limit: node.search.work_bytes(),
                generation: DIRECT_GENERATION,
            },
            &mut caches,
        )
        .unwrap();
        assert!(matches!(prepared, PreparedDocOp::Delete { .. }));
    }

    #[test]
    fn rebuild_recovers_late() {
        let dir = tempdir().unwrap();
        let store = GraphStore::open(dir.path()).unwrap();
        let search = SearchIndex::open_in_memory().unwrap();
        search.bind_store(&store).unwrap();
        let initial = store
            .queue_search_rebuild(&RebuildScan {
                index_id: search.index_id,
                row_limit: 1,
                byte_limit: search.work_bytes(),
            })
            .unwrap();
        assert!(!initial.remaining);

        let graph = GraphId::new("urn:test:late-rebuild-graph");
        store.create_graph(&graph).unwrap();
        let graph_tid = graph_term(&store, &graph);
        let subject = graph_tid;
        let predicate = store
            .encode_term(&EncodedTerm::from_named_node(&crate::vocab::schema_name()))
            .unwrap();
        let object = store
            .encode_term(&EncodedTerm("\"late rebuild needle\"".to_string()))
            .unwrap();
        let actor = crate::ActorId::from_bytes([42; 32]);
        let mut clock = crate::VectorClock::new();
        clock.advance(actor, 1);
        let mut batch = store.new_batch();
        store
            .insert_quad(
                &mut batch,
                crate::store::QuadAdd {
                    quad: crate::store::EncodedQuad {
                        graph: graph_tid,
                        subject,
                        predicate,
                        object,
                    },
                    dot: crate::Dot { actor, counter: 1 },
                },
            )
            .unwrap();
        store
            .set_vector_clock(
                &mut batch,
                crate::store::ClockUpdate {
                    graph_id: graph_tid,
                    clock: &clock,
                },
            )
            .unwrap();
        store
            .enqueue_fts(
                &mut batch,
                crate::store::FtsSubject {
                    graph_id: graph_tid,
                    subject,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        store
            .set_graph_diagnostics(&graph, &crate::GraphDiagnostics::default())
            .unwrap();

        let mut target = store.current_dirty_token();
        let mut completed = false;
        for _ in 0..8 {
            let progress = search
                .drain_queues(
                    &store,
                    DrainRequest {
                        bound: QueueBound {
                            chunk: 50,
                            max_token: Some(target),
                        },
                        control: DrainControl::default(),
                    },
                )
                .unwrap();
            if let Some(recovery) = progress.recovery {
                target = target.max(recovery);
            }
            if store
                .search_stage(&GenerationRequest {
                    index_id: search.index_id,
                    graph: graph.clone(),
                })
                .unwrap()
                .is_none()
                && search.active_generation(graph.as_str()).is_none()
            {
                let before = store.current_dirty_token();
                assert_eq!((before, false), store.ensure_reindex(&graph).unwrap());
                assert_eq!(before, store.current_dirty_token());
            }
            if !progress.remaining {
                completed = true;
                break;
            }
        }
        assert!(completed, "late graph debt made no rebuild progress");
        search.complete_coverage(&store, target).unwrap();
        assert!(store.search_coverage().unwrap().unwrap().rebuild.is_none());
        let hits = search.search("late rebuild needle", 10).unwrap();
        assert_eq!(1, hits.len());
        assert_eq!(graph.as_str(), hits[0].graph_id);
    }

    #[test]
    fn failure_tracks_oldest() {
        let graph = GraphId::new("urn:test:failure-oldest");
        let state = RetryState {
            id: graph_queue_id(QueueKind::Reindex, &graph),
            owed_from: 5,
            target: 100,
            attempts: 1,
            retry_at_ms: u64::MAX,
            code: "item-too-large".to_string(),
            error_kind: crate::CraqleErrorKind::QueryLimit,
        };

        let failure = drain_failure(&state, "oversized");
        assert_eq!(5, failure.owed_from);
        assert_eq!(100, failure.target);
        assert!(failure.owed_from <= 50);
        assert!(failure.owed_from > 4);
    }

    #[test]
    fn generation_isolates_graphs() {
        let index = SearchIndex::open_in_memory().unwrap();
        let first = GraphId::new("urn:test:generation-first");
        let second = GraphId::new("urn:test:generation-second");
        index.publish_rows(vec![
            GraphGeneration {
                graph: first.clone(),
                active: Some(GenerationId(2)),
                covered: 1,
            },
            GraphGeneration {
                graph: second.clone(),
                active: Some(GenerationId(3)),
                covered: 1,
            },
        ]);
        {
            let mut writer = index.writer().unwrap();
            index
                .add_document(
                    &mut writer,
                    ResourceDoc {
                        graph_id: first.as_str(),
                        subject_iri: "urn:test:stale",
                        generation: GenerationId(3),
                        all_text: Some("collisionneedle"),
                        delete_existing: true,
                    },
                )
                .unwrap();
            index
                .add_document(
                    &mut writer,
                    ResourceDoc {
                        graph_id: second.as_str(),
                        subject_iri: "urn:test:active",
                        generation: GenerationId(3),
                        all_text: Some("collisionneedle"),
                        delete_existing: true,
                    },
                )
                .unwrap();
        }
        index.commit().unwrap();

        assert!(
            index
                .search_in_graph(first.as_str(), "collisionneedle", 10)
                .unwrap()
                .is_empty()
        );
        let hits = index.search("collisionneedle", 10).unwrap();
        assert_eq!(1, hits.len());
        assert_eq!(second.as_str(), hits[0].graph_id);
    }

    #[test]
    fn authorization_precedes_topk() {
        let index = SearchIndex::open_in_memory().unwrap();
        for value in 0..20 {
            let graph = format!("urn:test:hidden:{value:02}");
            index
                .index_resource(
                    &graph,
                    &format!("urn:test:item:{value:02}"),
                    Some("rankneedle"),
                )
                .unwrap();
        }
        let visible = "urn:test:visible";
        index
            .index_resource(visible, "urn:test:wanted", Some("rankneedle"))
            .unwrap();
        index.commit().unwrap();
        let allows = |graph: &str| Ok(graph == visible);

        let hits = index
            .search_authorized(AuthorizedQuery {
                query: "rankneedle",
                limit: 1,
                subject: None,
                allows: &allows,
            })
            .unwrap();
        assert_eq!(1, hits.len());
        assert_eq!(visible, hits[0].graph_id);
    }

    #[test]
    fn authorized_ties_stable() {
        let index = SearchIndex::open_in_memory().unwrap();
        for value in (0..20).rev() {
            index
                .index_resource(
                    "urn:test:stable",
                    &format!("urn:test:item:{value:02}"),
                    Some("stableneedle"),
                )
                .unwrap();
        }
        index.commit().unwrap();
        let allows = |_: &str| Ok(true);
        let first = index
            .search_authorized(AuthorizedQuery {
                query: "stableneedle",
                limit: 1,
                subject: None,
                allows: &allows,
            })
            .unwrap();
        let page = index
            .search_authorized(AuthorizedQuery {
                query: "stableneedle",
                limit: 5,
                subject: None,
                allows: &allows,
            })
            .unwrap();

        assert_eq!(first[0].subject_iri, page[0].subject_iri);
        assert!(page.windows(2).all(|pair| {
            stable_hit_key(&pair[0].graph_id, &pair[0].subject_iri)
                < stable_hit_key(&pair[1].graph_id, &pair[1].subject_iri)
        }));
    }

    /// Field values for a hand-built document whose metadata may be missing or malformed.
    struct RawDoc<'a> {
        scope: Option<Vec<u8>>,
        stable: Option<Vec<u8>>,
        identity: Option<(&'a str, &'a str)>,
    }

    const RAW_GRAPH: &str = "urn:test:raw-graph";
    const RAW_SUBJECT: &str = "urn:test:raw-subject";

    impl RawDoc<'_> {
        fn valid(index: &SearchIndex) -> Self {
            Self {
                scope: Some(generation_scope(
                    index.index_id,
                    RAW_GRAPH,
                    DIRECT_GENERATION,
                )),
                stable: Some(stable_hit_key(RAW_GRAPH, RAW_SUBJECT).to_vec()),
                identity: Some((RAW_GRAPH, RAW_SUBJECT)),
            }
        }
    }

    fn write_raw(index: &SearchIndex, raw: RawDoc<'_>) {
        let mut document = TantivyDocument::default();
        document.add_text(index.f_doc_key, "unsplittable");
        if let Some((graph, subject)) = raw.identity {
            document.add_text(index.f_graph_id, graph);
            document.add_text(index.f_subject_iri, subject);
        }
        if let Some(scope) = raw.scope {
            document.add_bytes(index.f_generation_scope, &scope);
        }
        if let Some(stable) = raw.stable {
            document.add_bytes(index.f_stable_key, &stable);
        }
        document.add_text(index.f_all_text, "rawneedle");
        index.writer().unwrap().add_document(document).unwrap();
        index.write_epoch.fetch_add(1, Ordering::SeqCst);
        index.commit().unwrap();
    }

    fn raw_search(index: &SearchIndex) -> Result<Vec<SearchHit>> {
        index.search_in_graphs(GraphSetQuery {
            graphs: &[GraphId::new(RAW_GRAPH)],
            query: "rawneedle",
            limit: 8,
        })
    }

    #[test]
    fn damaged_metadata_errors() {
        type Damage = fn(&mut RawDoc<'static>);
        let cases: [(&str, Damage, &str, bool); 6] = [
            (
                "missing scope",
                |raw| raw.scope = None,
                "metadata column",
                false,
            ),
            (
                "missing stable",
                |raw| raw.stable = None,
                "metadata column",
                false,
            ),
            (
                "short scope",
                |raw| raw.scope = Some(vec![0; 8]),
                "generation scope",
                false,
            ),
            (
                "short stable",
                |raw| raw.stable = Some(vec![0; 16]),
                "stable key",
                true,
            ),
            (
                "no identity",
                |raw| raw.identity = None,
                "stored identity",
                false,
            ),
            (
                "foreign identity",
                |raw| raw.identity = Some(("urn:test:elsewhere", RAW_SUBJECT)),
                "stored identity",
                true,
            ),
        ];
        for (label, damage, expected, repairs) in cases {
            let index = SearchIndex::open_in_memory().unwrap();
            index.set_generation(RAW_GRAPH, Some(DIRECT_GENERATION));
            let mut raw = RawDoc::valid(&index);
            damage(&mut raw);
            write_raw(&index, raw);
            let error = raw_search(&index).expect_err(label);
            assert!(
                matches!(error, SearchError::Damaged { detail } if detail == expected),
                "{label}: {error:?}"
            );
            assert_eq!(error.kind(), crate::CraqleErrorKind::CorruptDerivedData);
            let damaged = index.take_damaged();
            assert_eq!(damaged.contains(RAW_GRAPH), repairs, "{label}");
        }
    }

    #[test]
    fn inactive_generation_skipped() {
        let index = SearchIndex::open_in_memory().unwrap();
        index.set_generation(RAW_GRAPH, Some(DIRECT_GENERATION));
        write_raw(&index, RawDoc::valid(&index));
        let mut stale = RawDoc::valid(&index);
        stale.scope = Some(generation_scope(
            index.index_id,
            RAW_GRAPH,
            GenerationId(99),
        ));
        stale.stable = Some(vec![0; 16]);
        write_raw(&index, stale);
        let hits = raw_search(&index).unwrap();
        assert_eq!(1, hits.len());
        assert_eq!(RAW_SUBJECT, hits[0].subject_iri);
        assert!(index.take_damaged().is_empty());
    }

    #[test]
    fn metadata_io_fails() {
        let index = SearchIndex::open_in_memory().unwrap();
        index.set_generation(RAW_GRAPH, Some(DIRECT_GENERATION));
        write_raw(&index, RawDoc::valid(&index));
        index.hooks.metadata_io.store(true, Ordering::SeqCst);
        let error = raw_search(&index).unwrap_err();
        assert!(matches!(error, SearchError::Io(_)), "{error:?}");
        assert_eq!(1, raw_search(&index).unwrap().len());
    }

    #[test]
    fn damaged_graph_repaired() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:damaged-repair");
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let auth = crate::AllowAllAuthorizer;
        node.create_crate(&auth, crate_request(&graph, "repairneedle", true))
            .unwrap();
        node.flush_search_updates().unwrap();
        let search = |node: &crate::CraqleNode| {
            node.search(
                &auth,
                crate::SearchRequest {
                    query: "repairneedle",
                    limit: 10,
                },
            )
        };
        let expected = search(&node).unwrap();
        assert!(!expected.is_empty());
        let generation = node.search.active_generation(graph.as_str()).unwrap();
        let mut document = TantivyDocument::default();
        document.add_text(node.search.f_graph_id, graph.as_str());
        document.add_text(node.search.f_subject_iri, "urn:test:damaged");
        let scope = generation_scope(node.search.index_id, graph.as_str(), generation);
        document.add_bytes(node.search.f_generation_scope, &scope);
        document.add_bytes(node.search.f_stable_key, &[0; 16]);
        document.add_text(node.search.f_all_text, "repairneedle");
        node.search
            .writer()
            .unwrap()
            .add_document(document)
            .unwrap();
        node.search.write_epoch.fetch_add(1, Ordering::SeqCst);
        node.search.commit().unwrap();
        let pinned = node.search.pin_view();

        let error = search(&node).unwrap_err();
        assert_eq!(error.kind(), crate::CraqleErrorKind::CorruptDerivedData);
        // The worker queues the reindex on its next pass; a flush then drains it.
        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        let repaired = loop {
            node.flush_search_updates().unwrap();
            if let Ok(hits) = search(&node) {
                break hits;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "damage was never repaired"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let identities = |hits: &[SearchHit]| {
            hits.iter()
                .map(|hit| (hit.graph_id.clone(), hit.subject_iri.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(identities(&expected), identities(&repaired));
        assert!(pinned.generations.active.contains(scope.as_slice()));
    }

    #[test]
    fn duplicate_keeps_score() {
        let index = SearchIndex::open_in_memory().unwrap();
        let graph = "urn:test:duplicate-score";
        index.set_generation(graph, Some(DIRECT_GENERATION));
        {
            let mut writer = index.writer().unwrap();
            for text in ["boostneedle", "boostneedle boostneedle boostneedle"] {
                index
                    .add_document(
                        &mut writer,
                        ResourceDoc {
                            graph_id: graph,
                            subject_iri: "urn:test:subject",
                            generation: DIRECT_GENERATION,
                            all_text: Some(text),
                            delete_existing: false,
                        },
                    )
                    .unwrap();
            }
        }
        index.commit().unwrap();
        let view = index.pin_view();
        let parser = QueryParser::for_index(&index.index, vec![index.f_all_text]);
        let query = parser.parse_query("boostneedle").unwrap();
        let raw = index
            .collect_top_docs(TopRequest {
                view: &view,
                query: &query,
                limit: 8,
            })
            .unwrap();
        let expected = raw
            .iter()
            .map(|hit| hit.score)
            .max_by(f32::total_cmp)
            .unwrap();
        let allows = |_: &str| Ok(true);
        let hits = index
            .search_authorized(AuthorizedQuery {
                query: "boostneedle",
                limit: 1,
                subject: None,
                allows: &allows,
            })
            .unwrap();

        assert_eq!(1, hits.len());
        assert_eq!(
            std::cmp::Ordering::Equal,
            hits[0].score.total_cmp(&expected)
        );
    }

    #[test]
    fn tail_scan_wraps() {
        let index = SearchIndex::open_in_memory().unwrap();
        let cursor = QueueCursor {
            token: 7,
            kind: QueueKind::Reindex,
            graph: TermId(9),
            subject: None,
        };
        index
            .scan_cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reindexes = Some(cursor);
        let page = QueuePage::<()> {
            entries: Vec::new(),
            next: Some(cursor),
            remaining: false,
            rows: 0,
            bytes: 0,
            oversized: None,
        };

        assert!(index.save_scan(ScanSave {
            kind: QueueKind::Reindex,
            page: &page,
            started: true,
        }));
        assert!(
            index
                .scan_cursors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .reindexes
                .is_none()
        );
        assert!(!index.save_scan(ScanSave {
            kind: QueueKind::Reindex,
            page: &page,
            started: false,
        }));
    }

    #[test]
    fn pinned_view_survives() {
        let index = SearchIndex::open_in_memory().unwrap();
        let graph = GraphId::new("urn:test:pinned-generation");
        index
            .index_resource(graph.as_str(), "urn:test:old", Some("oldneedle"))
            .unwrap();
        index.commit().unwrap();
        let pinned = index.pin_view();

        {
            let mut writer = index.writer().unwrap();
            index
                .add_document(
                    &mut writer,
                    ResourceDoc {
                        graph_id: graph.as_str(),
                        subject_iri: "urn:test:new",
                        generation: GenerationId(2),
                        all_text: Some("newneedle"),
                        delete_existing: true,
                    },
                )
                .unwrap();
        }
        index.commit().unwrap();
        assert_eq!(1, index.search("oldneedle", 10).unwrap().len());
        assert!(index.search("newneedle", 10).unwrap().is_empty());

        index.set_generation(graph.as_str(), Some(GenerationId(2)));
        assert!(index.search("oldneedle", 10).unwrap().is_empty());
        assert_eq!(1, index.search("newneedle", 10).unwrap().len());
        {
            let writer = index.writer().unwrap();
            writer.delete_term(Term::from_field_text(
                index.f_generation_key,
                &generation_key(index.index_id, graph.as_str(), DIRECT_GENERATION),
            ));
            index.write_epoch.fetch_add(1, Ordering::SeqCst);
        }
        index.commit().unwrap();

        let parser = QueryParser::for_index(&index.index, vec![index.f_all_text]);
        let parsed = parser.parse_query("oldneedle").unwrap();
        let hits = index
            .collect_top_docs(TopRequest {
                view: &pinned,
                query: &parsed,
                limit: 10,
            })
            .unwrap();
        assert_eq!(1, hits.len());
        assert_eq!("urn:test:old", hits[0].subject_iri);
    }

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

    /// Queue entries newer than a reindex cutoff must survive its clear.
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
        let index_id = idx.index_id();
        assert!(idx.needs_rebuild());
        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("persisted proteomics record"),
        )?;
        idx.commit()?;
        drop(idx);

        let reopened = SearchIndex::open(dir.path())?;
        assert_eq!(index_id, reopened.index_id());
        assert!(!reopened.needs_rebuild());
        reopened.set_generation("http://example.org/graph1", Some(DIRECT_GENERATION));
        let hits = reopened.search_in_graph("http://example.org/graph1", "proteomics", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "http://example.org/entity1");

        Ok(())
    }

    #[test]
    fn foreign_index_rebuilds() {
        let dir = tempdir().unwrap();
        let store = GraphStore::open(dir.path().join("store")).unwrap();
        let first = SearchIndex::open(dir.path().join("first")).unwrap();
        store
            .advance_search_coverage(&crate::search::queue::SearchCoverage {
                format: crate::search::queue::SEARCH_META_FORMAT,
                index_id: first.index_id(),
                index_revision: first.index.load_metas().unwrap().opstamp,
                covered: store.current_dirty_token(),
                rebuild: None,
                manifest_count: 0,
                manifest_hash: [0; 32],
                manifest_epoch: 0,
            })
            .unwrap();

        let foreign_path = dir.path().join("foreign");
        let seeded = SearchIndex::open(&foreign_path).unwrap();
        seeded
            .index_resource(
                "urn:test:foreign",
                "urn:test:foreign",
                Some("foreignneedle"),
            )
            .unwrap();
        seeded.commit().unwrap();
        let foreign_id = seeded.index_id();
        drop(seeded);
        let foreign = SearchIndex::open(&foreign_path).unwrap();
        assert_eq!(foreign_id, foreign.index_id());
        assert!(foreign.coverage_target(&store).unwrap().is_some());
        assert_eq!(0, foreign.reader.searcher().num_docs());
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
        let writer_bytes = usize::try_from(MemoryBudget::default().search_writer_bytes()).unwrap();
        let mut writer = legacy_index.writer(writer_bytes).unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_text(
            legacy_schema.get_field("doc_key").unwrap(),
            format!("{}\u{1f}{}", graph.as_str(), graph.as_str()),
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

    // Poisoned derived state must repair itself.

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

    /// A second committer cannot finish before the in-flight commit it needs.
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

    /// Poison recovery must re-derive rolled-back index state in one pass.
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

        // Simulate unqueued stale index state that only re-derivation can repair.
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

    /// One authorized request uses one search over one pinned reader.
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

    #[test]
    fn page_replay_idempotent() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:page-replay");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            node.create_crate(&writer_auth(), crate_request(&graph, "currentneedle", true))
                .unwrap();
            node.flush_search_updates().unwrap();
        }

        let store = reopen_store(dir.path());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        search
            .index_resource(graph.as_str(), graph.as_str(), Some("legacyneedle"))
            .unwrap();
        search.commit().unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts_reindex(&mut batch, graph_term(&store, &graph))
            .unwrap();
        store.commit(batch).unwrap();
        let target = store.current_dirty_token();
        search.arm_page_gate();
        let control = DrainControl::default();

        let worker = {
            let store = store.clone();
            let search = search.clone();
            let graph = graph.clone();
            let control = control.clone();
            std::thread::spawn(move || {
                let _guard = search.lock_graph(graph.as_str());
                search.stage_graph(
                    GraphWork {
                        store: &store,
                        graph: &graph,
                        control: &control,
                        byte_limit: search.work_bytes(),
                    },
                    target,
                )
            })
        };
        search.await_page_gate();
        control.cancel();
        assert_eq!(1, search.search("legacyneedle", 10).unwrap().len());
        search.release_page_gate();
        assert!(matches!(
            worker.join().unwrap(),
            Err(SearchError::Cancelled)
        ));
        let request = GenerationRequest {
            index_id: search.index_id,
            graph: graph.clone(),
        };
        assert_eq!(
            None,
            store.search_stage(&request).unwrap().unwrap().cursor,
            "cursor must not move past an unrecorded committed page"
        );

        let resumed = DrainControl::default();
        loop {
            let outcome = {
                let _guard = search.lock_graph(graph.as_str());
                search
                    .stage_graph(
                        GraphWork {
                            store: &store,
                            graph: &graph,
                            control: &resumed,
                            byte_limit: search.work_bytes(),
                        },
                        target,
                    )
                    .unwrap()
            };
            if matches!(outcome, StageOutcome::Covered(_)) {
                break;
            }
        }
        assert!(search.search("legacyneedle", 10).unwrap().is_empty());
        assert_eq!(1, search.search("currentneedle", 10).unwrap().len());
    }

    #[test]
    fn cancel_keeps_waiter() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:shared-waiter");
        let node = Arc::new(crate::CraqleNode::open(dir.path()).unwrap());
        node.create_crate(&writer_auth(), crate_request(&graph, "sharedneedle", true))
            .unwrap();
        node.flush_search_updates().unwrap();
        let mut batch = node.store.new_batch();
        node.store
            .enqueue_fts_reindex(&mut batch, graph_term(&node.store, &graph))
            .unwrap();
        node.store.commit(batch).unwrap();
        node.search.arm_page_gate();

        let cancellation = crate::QueryCancellation::new();
        let first = {
            let node = node.clone();
            let cancellation = cancellation.clone();
            std::thread::spawn(move || {
                node.flush_search(&crate::SearchFlushOptions {
                    timeout: None,
                    cancellation,
                })
            })
        };
        node.search.await_page_gate();
        let (ready, submitted) = std::sync::mpsc::channel();
        let second = {
            let node = node.clone();
            std::thread::spawn(move || {
                ready.send(()).unwrap();
                node.flush_search(&crate::SearchFlushOptions::default())
            })
        };
        submitted.recv().unwrap();
        cancellation.cancel();
        assert!(first.join().unwrap().is_err());
        node.search.release_page_gate();

        second.join().unwrap().unwrap();
        assert!(node.store.drain_reindex_queue(8).unwrap().is_empty());
        assert_eq!(1, node.search.search("sharedneedle", 10).unwrap().len());
    }

    #[test]
    fn complete_stage_reopens() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:complete-stage");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            node.create_crate(&writer_auth(), crate_request(&graph, "currentneedle", true))
                .unwrap();
            node.flush_search_updates().unwrap();
        }

        let store = reopen_store(dir.path());
        let search_path = dir.path().join("staged-search");
        let search = Arc::new(SearchIndex::open(&search_path).unwrap());
        assert!(search.bind_store(&store).unwrap().is_some());
        let switched = store
            .ensure_search_generation(&GenerationRequest {
                index_id: search.index_id,
                graph: graph.clone(),
            })
            .unwrap();
        search.apply_switch(&switched);
        {
            let mut writer = search.writer().unwrap();
            search
                .add_document(
                    &mut writer,
                    ResourceDoc {
                        graph_id: graph.as_str(),
                        subject_iri: graph.as_str(),
                        generation: switched.active.unwrap(),
                        all_text: Some("legacyneedle"),
                        delete_existing: true,
                    },
                )
                .unwrap();
        }
        search.commit().unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts_reindex(&mut batch, graph_term(&store, &graph))
            .unwrap();
        store.commit(batch).unwrap();
        let target = store.current_dirty_token();
        search.arm_stage_gate();
        let control = DrainControl::default();

        let worker = {
            let store = store.clone();
            let search = search.clone();
            let graph = graph.clone();
            let control = control.clone();
            std::thread::spawn(move || {
                loop {
                    let outcome = {
                        let _guard = search.lock_graph(graph.as_str());
                        search.stage_graph(
                            GraphWork {
                                store: &store,
                                graph: &graph,
                                control: &control,
                                byte_limit: search.work_bytes(),
                            },
                            target,
                        )?
                    };
                    if matches!(outcome, StageOutcome::Covered(_)) {
                        return Ok(());
                    }
                }
            })
        };
        search.await_stage_gate();
        control.cancel();
        search.release_stage_gate();
        assert!(matches!(
            worker.join().unwrap(),
            Err(SearchError::Cancelled)
        ));
        assert_eq!(1, search.search("legacyneedle", 10).unwrap().len());
        drop(search);

        let reopened = SearchIndex::open(&search_path).unwrap();
        reopened.bind_store(&store).unwrap();
        assert_eq!(1, reopened.search("legacyneedle", 10).unwrap().len());
        let resumed = DrainControl::default();
        let outcome = {
            let _guard = reopened.lock_graph(graph.as_str());
            reopened
                .stage_graph(
                    GraphWork {
                        store: &store,
                        graph: &graph,
                        control: &resumed,
                        byte_limit: reopened.work_bytes(),
                    },
                    target,
                )
                .unwrap()
        };
        assert!(matches!(outcome, StageOutcome::Covered(_)));
        assert!(reopened.search("legacyneedle", 10).unwrap().is_empty());
        assert_eq!(1, reopened.search("currentneedle", 10).unwrap().len());
        let active = store
            .search_generation(&GenerationRequest {
                index_id: reopened.index_id,
                graph: graph.clone(),
            })
            .unwrap()
            .active;
        drop(reopened);

        let replay = SearchIndex::open(&search_path).unwrap();
        replay.bind_store(&store).unwrap();
        assert_eq!(
            active,
            store
                .search_generation(&GenerationRequest {
                    index_id: replay.index_id,
                    graph: graph.clone(),
                })
                .unwrap()
                .active
        );
        crate::flush_search_queue(&store, &replay).unwrap();
        assert!(
            store
                .search_generation(&GenerationRequest {
                    index_id: replay.index_id,
                    graph: graph.clone(),
                })
                .unwrap()
                .active
                .is_some()
        );
        assert!(replay.search("legacyneedle", 10).unwrap().is_empty());
        assert_eq!(1, replay.search("currentneedle", 10).unwrap().len());
        assert!(store.drain_reindex_queue(8).unwrap().is_empty());
    }

    #[test]
    fn delete_cancels_stage() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:delete-stage");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            node.create_crate(&writer_auth(), crate_request(&graph, "currentneedle", true))
                .unwrap();
            node.flush_search_updates().unwrap();
        }

        let store = reopen_store(dir.path());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        search
            .index_resource(graph.as_str(), graph.as_str(), Some("legacyneedle"))
            .unwrap();
        search.commit().unwrap();
        let mut batch = store.new_batch();
        store
            .enqueue_fts_reindex(&mut batch, graph_term(&store, &graph))
            .unwrap();
        store.commit(batch).unwrap();
        let target = store.current_dirty_token();
        search.arm_page_gate();
        let control = DrainControl::default();

        let worker = {
            let store = store.clone();
            let search = search.clone();
            let graph = graph.clone();
            let control = control.clone();
            std::thread::spawn(move || {
                let _guard = search.lock_graph(graph.as_str());
                search.stage_graph(
                    GraphWork {
                        store: &store,
                        graph: &graph,
                        control: &control,
                        byte_limit: search.work_bytes(),
                    },
                    target,
                )
            })
        };
        search.await_page_gate();
        store.delete_graph(&graph).unwrap();
        control.cancel();
        search.release_page_gate();
        assert!(matches!(
            worker.join().unwrap(),
            Err(SearchError::Cancelled)
        ));

        let delete_target = store.current_dirty_token();
        search
            .process_queued_updates(
                &store,
                QueueBound {
                    chunk: 8,
                    max_token: Some(delete_target),
                },
            )
            .unwrap();
        let request = GenerationRequest {
            index_id: search.index_id,
            graph: graph.clone(),
        };
        assert!(store.search_stage(&request).unwrap().is_none());
        assert!(store.search_generation(&request).unwrap().active.is_none());
        assert!(search.search("legacyneedle", 10).unwrap().is_empty());
        assert!(search.search("currentneedle", 10).unwrap().is_empty());
    }

    #[test]
    fn cancel_keeps_reader() {
        let dir = tempdir().unwrap();
        let graph = GraphId::new("urn:test:cancelled-rebuild");
        {
            let node = crate::CraqleNode::open(dir.path()).unwrap();
            node.create_crate(&writer_auth(), crate_request(&graph, "oldneedle", true))
                .unwrap();
            node.flush_search_updates().unwrap();
        }

        let store = reopen_store(dir.path());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        search
            .index_resource(graph.as_str(), graph.as_str(), Some("oldneedle"))
            .unwrap();
        search.commit().unwrap();
        search.arm_stage_gate();
        let control = DrainControl::default();

        let worker = {
            let store = store.clone();
            let search = search.clone();
            let graph = graph.clone();
            let control = control.clone();
            std::thread::spawn(move || {
                let target = store.current_dirty_token();
                loop {
                    let outcome = {
                        let _guard = search.lock_graph(graph.as_str());
                        search.stage_graph(
                            GraphWork {
                                store: &store,
                                graph: &graph,
                                control: &control,
                                byte_limit: search.work_bytes(),
                            },
                            target,
                        )?
                    };
                    if matches!(outcome, StageOutcome::Covered(_)) {
                        return Ok(());
                    }
                }
            })
        };
        search.await_stage_gate();
        control.cancel();
        assert_eq!(1, search.search("oldneedle", 10).unwrap().len());
        search.release_stage_gate();

        assert!(matches!(
            worker.join().unwrap(),
            Err(SearchError::Cancelled)
        ));
        assert_eq!(1, search.search("oldneedle", 10).unwrap().len());
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
        search.bind_store(&store).unwrap();
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
    fn failure_retry_bounded() {
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
        search.set_retry_now(1);

        let first_error = crate::flush_search_queue(&store, &search)
            .expect_err("a flush covering a failed entry must not report success");
        assert!(
            first_error.to_string().contains(broken.as_str())
                && first_error.to_string().contains("store-transient"),
            "unexpected first flush error: {first_error}"
        );

        let hits = search.search("isolationneedle", 50).unwrap();
        assert_eq!(
            1,
            hits.len(),
            "the healthy graph stayed unindexed behind a permanently failing entry"
        );
        assert_eq!(healthy.as_str(), hits[0].graph_id);

        for attempt in 1..MAX_RETRY_ATTEMPTS {
            search.set_retry_now(u64::from(attempt).saturating_mul(RETRY_MAX_MS + 1));
            assert!(crate::flush_search_queue(&store, &search).is_err());
        }
        let id = graph_queue_id(QueueKind::Reindex, &broken);
        let failure = store
            .fts_failure(&id)
            .unwrap()
            .expect("failure state must survive the retry cap");
        assert_eq!(MAX_RETRY_ATTEMPTS, failure.attempts);
        assert_eq!(u64::MAX, failure.retry_at_ms);
        assert_eq!(failure.code, "store-transient");
        assert_eq!(
            failure.error_kind,
            crate::CraqleErrorKind::CorruptDerivedData
        );
        *search
            .hooks
            .fail_graph
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        assert!(crate::flush_search_queue(&store, &search).is_err());

        let mut batch = store.new_batch();
        store
            .enqueue_fts_reindex(&mut batch, graph_term(&store, &broken))
            .unwrap();
        store.commit(batch).unwrap();
        crate::flush_search_queue(&store, &search)
            .expect("newer debt resets the capped failure identity");
        assert_eq!(2, search.search("isolationneedle", 50).unwrap().len());
        assert!(store.drain_reindex_queue(10).unwrap().is_empty());
    }
}
