use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT, TextFieldIndexing, Value,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

use crate::core::{EncodedTerm, GraphId};
pub(crate) use crate::search_queue::QueueBound;
use crate::search_queue::drain_upto;
use crate::store::{GraphStore, TermId};

const DISK_INDEX_WRITER_HEAP_BYTES: usize = 256_000_000;
const MEMORY_INDEX_WRITER_HEAP_BYTES: usize = 64_000_000;
const REINDEX_FLUSH_CHUNK: usize = 2_048;
const ALL_TEXT_TOKENIZER: &str = "craqle_text_v2";
const INDEX_VERSION_FIELD: &str = "_craqle_search_index_v2";

/// Predicates whose objects contribute to a document's searchable text.
///
/// Built once instead of per synced subject: the previous per-call constructor
/// allocated four `NamedNode`s plus four `EncodedTerm`s for every subject the
/// worker touched.
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
}

pub(crate) type Result<T> = std::result::Result<T, SearchError>;

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
    /// Guards the single Tantivy writer. Held only around `add`/`delete`
    /// calls — never across store reads. One writer, rather
    /// than concurrent writers behind an `RwLock`, keeps delete/add
    /// interleaving deterministic instead of timing-dependent (G7).
    ///
    /// Never locked directly: go through [`SearchIndex::writer`], which turns
    /// a poisoned lock into a recovery instead of a panic.
    writer: Mutex<IndexWriter>,
    dirty: AtomicBool,
    /// Set when a poisoned writer was rolled back and the index therefore owes
    /// the store a full re-derivation. Cleared once that reindex is queued.
    rebuild_owed: AtomicBool,
    /// Set by a test to make the next indexer drain cycle panic, proving the
    /// worker survives one. Per-index rather than global so concurrent tests
    /// cannot arm each other's workers.
    #[cfg(test)]
    armed_drain_panic: AtomicBool,
    needs_rebuild: bool,
    f_doc_key: Field,
    f_graph_id: Field,
    f_subject_iri: Field,
    f_all_text: Field,
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
            dirty: AtomicBool::new(false),
            rebuild_owed: AtomicBool::new(false),
            #[cfg(test)]
            armed_drain_panic: AtomicBool::new(false),
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
            dirty: AtomicBool::new(false),
            rebuild_owed: AtomicBool::new(false),
            #[cfg(test)]
            armed_drain_panic: AtomicBool::new(false),
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
        self.armed_drain_panic.store(true, Ordering::SeqCst);
    }

    /// Consumes a pending injected panic, reporting whether one was armed.
    #[cfg(test)]
    pub(crate) fn take_armed_drain_panic(&self) -> bool {
        self.armed_drain_panic.swap(false, Ordering::SeqCst)
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
    /// crash-safe and keeps G7's acknowledge-after-commit rule. The returned
    /// bound is widened once, on this pass only, so a caller's own flush cannot
    /// return while the rebuild it triggered is still pending.
    fn settle_poisoned_writer(&self, store: &GraphStore, bound: QueueBound) -> Result<QueueBound> {
        if self.writer.is_poisoned() {
            drop(self.writer()?);
        }
        if !self.rebuild_owed.swap(false, Ordering::SeqCst) {
            return Ok(bound);
        }

        if let Err(error) = self.enqueue_full_rebuild(store) {
            // Put the debt back: the next pass, one tick later, retries it.
            self.rebuild_owed.store(true, Ordering::SeqCst);
            return Err(error);
        }

        Ok(QueueBound {
            chunk: bound.chunk,
            max_token: bound.max_token.map(|_| store.current_dirty_token()),
        })
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
        self.dirty.store(true, Ordering::SeqCst);
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

        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = query_parser.parse_query(req.query)?;

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
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            hits.push(self.doc_to_hit(doc, score));
        }
        Ok(hits)
    }

    /// Commit pending writes and reload the reader so subsequent searches
    /// reflect the latest changes.
    pub fn commit(&self) -> Result<()> {
        if !self.dirty.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        {
            let mut writer = self.writer()?;
            writer.commit()?;
        }
        self.reader.reload()?;
        Ok(())
    }

    /// Sync queued subject updates from the RDF store into Tantivy.
    ///
    /// The three durable queues are drained in priority order: graph deletes,
    /// whole-graph reindexes, then individual subjects. Each branch commits the
    /// index *before* acknowledging the queue entries it covered — a crash in
    /// between only re-does work, whereas acknowledging first would silently
    /// drop updates Tantivy never committed (G7).
    pub fn process_queued_updates(&self, store: &GraphStore, bound: QueueBound) -> Result<usize> {
        let bound = self.settle_poisoned_writer(store, bound)?;

        let queued_deletes = drain_upto(&bound, |chunk| store.drain_fts_delete_queue(chunk))?;
        if !queued_deletes.is_empty() {
            for (graph, _) in &queued_deletes {
                if store.contains_graph(graph)? {
                    self.reindex_from_store(store, graph)?;
                } else {
                    self.delete_graph_documents_uncommitted(graph.as_str())?;
                }
            }

            self.commit()?;
            store.acknowledge_fts_queues_for_deleted_graphs(&queued_deletes)?;
            store.acknowledge_fts_delete_queue(&queued_deletes)?;
            return Ok(queued_deletes.len());
        }

        let queued_graphs = drain_upto(&bound, |chunk| store.drain_fts_reindex_queue(chunk))?;
        if !queued_graphs.is_empty() {
            for (graph, _) in &queued_graphs {
                self.reindex_from_store(store, graph)?;
            }

            self.commit()?;
            store.acknowledge_fts_subjects_for_reindexed_graphs(&queued_graphs)?;
            store.acknowledge_fts_reindex_queue(&queued_graphs)?;
            return Ok(queued_graphs.len());
        }

        let queued = drain_upto(&bound, |chunk| store.drain_fts_queue(chunk))?;
        if queued.is_empty() {
            return Ok(0);
        }

        // Phase 1: read every update from the store with NO writer lock held.
        // This walks up to `bound.chunk` subjects and used to run under the
        // Tantivy writer mutex, blocking every other indexer for the whole
        // scan.
        let mut seen = HashSet::with_capacity(queued.len());
        let mut caches = StoreSyncCaches::default();
        let mut prepared = Vec::with_capacity(queued.len());
        for (graph, subject, _) in &queued {
            if !seen.insert((graph.clone(), *subject)) {
                continue;
            }
            prepared.push(prepare_subject_op(
                PrepareSubject {
                    store,
                    graph,
                    subject: *subject,
                },
                &mut caches,
            )?);
        }

        // Phase 2: apply the prepared ops in queue order under the writer lock.
        {
            // Guards the Tantivy writer; no store reads happen inside.
            let mut writer = self.writer()?;
            for op in &prepared {
                self.apply_prepared_op(&mut writer, op)?;
            }
        }

        self.commit()?;
        store.acknowledge_fts_queue(&queued)?;
        Ok(prepared.len())
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
            self.dirty.store(true, Ordering::SeqCst);
        }

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
        self.dirty.store(true, Ordering::SeqCst);
    }

    fn delete_graph_documents_uncommitted(&self, graph_id: &str) -> Result<()> {
        let writer = self.writer()?;
        writer.delete_term(Term::from_field_text(self.f_graph_id, graph_id));
        self.dirty.store(true, Ordering::SeqCst);
        Ok(())
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
    fn test_index_and_search() -> Result<()> {
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
    fn test_search_in_graph() -> Result<()> {
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
    fn test_raw_query_operators_are_neutralized() -> Result<()> {
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
    fn test_ascii_folding_matches_diacritics() -> Result<()> {
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
    fn test_upsert_replaces_old_document() -> Result<()> {
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
    fn test_same_subject_in_multiple_graphs_do_not_collide() -> Result<()> {
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
    fn test_search_indexes_subject_ids() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource("http://example.org/graph1", "urn:test:dataset123", None)?;
        idx.commit()?;

        let hits = idx.search("dataset123", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "urn:test:dataset123");

        Ok(())
    }

    #[test]
    fn test_persistent_index_roundtrips_across_reopen() -> Result<()> {
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
        node.search
            .delete_graph_documents_uncommitted(graph.as_str())
            .unwrap();
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
}
