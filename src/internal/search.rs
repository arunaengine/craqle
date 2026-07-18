use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT, TextFieldIndexing, Value,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

const DISK_INDEX_WRITER_HEAP_BYTES: usize = 256_000_000;
const MEMORY_INDEX_WRITER_HEAP_BYTES: usize = 64_000_000;
const REINDEX_FLUSH_CHUNK: usize = 2_048;
const ALL_TEXT_TOKENIZER: &str = "craqle_text_v2";
const INDEX_VERSION_FIELD: &str = "_craqle_search_index_v2";

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query parse: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

pub type Result<T> = std::result::Result<T, SearchError>;

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
    writer: Mutex<IndexWriter>,
    dirty: AtomicBool,
    empty: AtomicBool,
    needs_rebuild: bool,
    f_doc_key: Field,
    f_graph_id: Field,
    f_subject_iri: Field,
    f_all_text: Field,
}

#[derive(Default)]
struct StoreSyncCaches {
    orphaned_subjects: HashMap<crate::core::GraphId, HashSet<String>>,
    graph_terms: HashMap<crate::core::GraphId, Option<crate::store::TermId>>,
    terms: HashMap<crate::store::TermId, crate::core::EncodedTerm>,
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
            empty: AtomicBool::new(needs_rebuild),
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
            empty: AtomicBool::new(true),
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
        let mut writer = self.writer.lock().unwrap();
        self.add_resource_document(&mut writer, graph_id, subject_iri, all_text)
    }

    pub fn replace_graph_documents<I>(&self, graph_id: &str, documents: I) -> Result<()>
    where
        I: IntoIterator<Item = (String, Option<String>)>,
    {
        {
            let mut writer = self.writer.lock().unwrap();
            writer.delete_term(Term::from_field_text(self.f_graph_id, graph_id));
            self.dirty.store(true, Ordering::SeqCst);
            for (subject_iri, all_text) in documents {
                self.add_resource_document_with_delete(
                    &mut writer,
                    graph_id,
                    &subject_iri,
                    all_text.as_deref(),
                    false,
                )?;
            }
        }
        self.commit()
    }

    pub fn upsert_resource_documents<I>(&self, graph_id: &str, documents: I) -> Result<()>
    where
        I: IntoIterator<Item = (String, Option<String>)>,
    {
        {
            let mut writer = self.writer.lock().unwrap();
            for (subject_iri, all_text) in documents {
                self.add_resource_document_with_delete(
                    &mut writer,
                    graph_id,
                    &subject_iri,
                    all_text.as_deref(),
                    true,
                )?;
            }
        }
        self.commit()
    }

    pub fn sync_subjects_from_store(
        &self,
        store: &crate::store::GraphStore,
        graph: &crate::core::GraphId,
        subjects: &[crate::core::EncodedTerm],
    ) -> Result<()> {
        let mut seen = HashSet::new();
        let mut caches = StoreSyncCaches::default();
        {
            let mut writer = self.writer.lock().unwrap();
            for subject in subjects {
                let subject_iri = term_to_string(subject);
                if !seen.insert(subject_iri.clone()) {
                    continue;
                }

                let Some(subject_tid) = store.lookup_term(subject)? else {
                    self.delete_resource_with_writer(&mut writer, graph.as_str(), &subject_iri);
                    continue;
                };

                self.sync_subject_from_store_cached(
                    &mut writer,
                    store,
                    graph,
                    subject_tid,
                    &mut caches,
                )?;
            }
        }
        self.commit()
    }

    fn add_resource_document(
        &self,
        writer: &mut IndexWriter,
        graph_id: &str,
        subject_iri: &str,
        all_text: Option<&str>,
    ) -> Result<()> {
        self.add_resource_document_with_delete(writer, graph_id, subject_iri, all_text, true)
    }

    fn add_resource_document_with_delete(
        &self,
        writer: &mut IndexWriter,
        graph_id: &str,
        subject_iri: &str,
        all_text: Option<&str>,
        delete_existing: bool,
    ) -> Result<()> {
        if delete_existing {
            writer.delete_term(Term::from_field_text(
                self.f_doc_key,
                &doc_key(graph_id, subject_iri),
            ));
        }

        self.add_resource_document_without_delete(writer, graph_id, subject_iri, all_text)
    }

    fn add_resource_document_without_delete(
        &self,
        writer: &mut IndexWriter,
        graph_id: &str,
        subject_iri: &str,
        all_text: Option<&str>,
    ) -> Result<()> {
        let mut all_text_parts: Vec<&str> = vec![graph_id, subject_iri];
        if let Some(extra) = all_text {
            all_text_parts.push(extra);
        }
        let all_text = all_text_parts.join(" ");

        let mut doc = TantivyDocument::default();
        doc.add_text(self.f_doc_key, doc_key(graph_id, subject_iri));
        doc.add_text(self.f_graph_id, graph_id);
        doc.add_text(self.f_subject_iri, subject_iri);
        doc.add_text(self.f_all_text, &all_text);

        writer.add_document(doc)?;
        self.dirty.store(true, Ordering::SeqCst);
        self.empty.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Delete a document for the given graph/resource pair.
    pub fn delete_resource(&self, graph_id: &str, subject_iri: &str) -> Result<()> {
        let writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_text(
            self.f_doc_key,
            &doc_key(graph_id, subject_iri),
        ));
        self.dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Full-text search across all graphs.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let parsed = query_parser.parse_query(&sanitize_query(query))?;

        let top_docs = searcher.search(&parsed, &TopDocs::with_limit(limit).order_by_score())?;
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            hits.push(self.doc_to_hit(doc, score));
        }
        Ok(hits)
    }

    /// Full-text search restricted to a single graph.
    pub fn search_in_graph(
        &self,
        graph_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
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

        let top_docs = searcher.search(&combined, &TopDocs::with_limit(limit).order_by_score())?;
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
            let mut writer = self.writer.lock().unwrap();
            writer.commit()?;
        }
        self.reader.reload()?;
        Ok(())
    }

    /// Sync queued subject updates from the RDF store into Tantivy.
    pub fn process_queued_updates(
        &self,
        store: &crate::store::GraphStore,
        limit: usize,
    ) -> Result<usize> {
        let queued_deletes = store.drain_fts_delete_queue(limit)?;
        if !queued_deletes.is_empty() {
            let mut processed = 0usize;
            for (graph, _) in &queued_deletes {
                if store.contains_graph(graph)? {
                    self.reindex_from_store(store, graph)?;
                } else {
                    self.delete_graph_documents_uncommitted(graph.as_str());
                }
                processed += 1;
            }

            if processed > 0 {
                self.commit()?;
                store.acknowledge_fts_queues_for_deleted_graphs(&queued_deletes)?;
                store.acknowledge_fts_delete_queue(&queued_deletes)?;
            }
            return Ok(processed);
        }

        let queued_graphs = store.drain_fts_reindex_queue(limit)?;
        if !queued_graphs.is_empty() {
            let mut processed = 0usize;
            for (graph, _) in &queued_graphs {
                self.reindex_from_store(store, graph)?;
                processed += 1;
            }

            if processed > 0 {
                self.commit()?;
                store.acknowledge_fts_subjects_for_reindexed_graphs(&queued_graphs)?;
                store.acknowledge_fts_reindex_queue(&queued_graphs)?;
            }
            return Ok(processed);
        }

        let queued = store.drain_fts_queue(limit)?;
        if queued.is_empty() {
            return Ok(0);
        }

        let mut seen = HashSet::with_capacity(queued.len());
        let mut caches = StoreSyncCaches::default();
        let mut writer = self.writer.lock().unwrap();
        let mut processed = 0usize;

        for (graph, subject_tid, _) in &queued {
            if !seen.insert((graph.clone(), *subject_tid)) {
                continue;
            }
            self.sync_subject_from_store_cached(
                &mut writer,
                store,
                graph,
                *subject_tid,
                &mut caches,
            )?;
            processed += 1;
        }

        drop(writer);

        if processed > 0 {
            self.commit()?;
            store.acknowledge_fts_queue(&queued)?;
        }
        Ok(processed)
    }

    pub fn sync_subject_from_store(
        &self,
        store: &crate::store::GraphStore,
        graph: &crate::core::GraphId,
        subject: &crate::core::EncodedTerm,
    ) -> Result<()> {
        let Some(subject_tid) = store.lookup_term(subject)? else {
            return self.delete_resource(graph.as_str(), &term_to_string(subject));
        };
        let mut writer = self.writer.lock().unwrap();
        let mut caches = StoreSyncCaches::default();
        self.sync_subject_from_store_cached(&mut writer, store, graph, subject_tid, &mut caches)
    }

    /// Reindex all entities in a graph from the RDF store.
    ///
    /// Scans the store for triples with searchable predicates, groups them by
    /// subject, and indexes each subject as a document.
    ///
    /// Returns the number of entities indexed.
    pub fn reindex_from_store(
        &self,
        store: &crate::store::GraphStore,
        graph: &crate::core::GraphId,
    ) -> Result<usize> {
        let graph_iri = graph.as_str();
        let graph_term = crate::core::EncodedTerm::from_named_node(&graph.0);
        let graph_tid = match store.lookup_term(&graph_term)? {
            Some(tid) => tid,
            None => return Ok(0),
        };

        let orphaned = orphaned_subjects(store, graph)?;
        let searchable_predicates = searchable_predicates();
        let mut count = 0usize;
        let mut current_subject: Option<crate::store::TermId> = None;
        let mut current_subject_iri = String::new();
        let mut current_subject_visible = false;
        let mut current_text = String::new();
        let mut pending_documents = Vec::new();
        let mut term_cache = HashMap::new();
        let delete_existing = false;
        {
            let writer = self.writer.lock().unwrap();
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
                        self.flush_pending_documents(
                            graph_iri,
                            delete_existing,
                            &mut pending_documents,
                        )?;
                    }
                }

                let subject_term = store.decode_term_cached(&mut term_cache, quad.subject)?;
                current_subject_iri = term_to_string(&subject_term);
                current_subject_visible = !orphaned.contains(&current_subject_iri);
                current_text.clear();
                current_subject = Some(quad.subject);
            }

            if current_subject_visible {
                let predicate_term = store.decode_term_cached(&mut term_cache, quad.predicate)?;
                if !is_searchable_predicate(&predicate_term, &searchable_predicates) {
                    return Ok(());
                }
                let object_term = store.decode_term_cached(&mut term_cache, quad.object)?;
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

        self.flush_pending_documents(graph_iri, delete_existing, &mut pending_documents)?;

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
        delete_existing: bool,
        pending_documents: &mut Vec<(String, Option<String>)>,
    ) -> Result<()> {
        if pending_documents.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer.lock().unwrap();
        for (subject_iri, extra_text) in pending_documents.drain(..) {
            self.add_resource_document_with_delete(
                &mut writer,
                graph_iri,
                &subject_iri,
                extra_text.as_deref(),
                delete_existing,
            )?;
        }
        Ok(())
    }

    fn sync_subject_from_store_cached(
        &self,
        writer: &mut IndexWriter,
        store: &crate::store::GraphStore,
        graph: &crate::core::GraphId,
        subject_tid: crate::store::TermId,
        caches: &mut StoreSyncCaches,
    ) -> Result<()> {
        let graph_iri = graph.as_str();
        let subject = store.decode_term_cached(&mut caches.terms, subject_tid)?;
        let subject_iri = term_to_string(&subject);
        let orphaned = load_orphaned_subjects(&mut caches.orphaned_subjects, store, graph)?;
        if orphaned.contains(subject_iri.as_str()) {
            self.delete_resource_with_writer(writer, graph_iri, &subject_iri);
            return Ok(());
        }

        let graph_tid = match caches.graph_terms.get(graph) {
            Some(cached) => *cached,
            None => {
                let graph_term = crate::core::EncodedTerm::from_named_node(&graph.0);
                let resolved = store.lookup_term(&graph_term)?;
                caches.graph_terms.insert(graph.clone(), resolved);
                resolved
            }
        };
        let Some(graph_tid) = graph_tid else {
            self.delete_resource_with_writer(writer, graph_iri, &subject_iri);
            return Ok(());
        };

        let triples = store.triples_for_subject(graph_tid, subject_tid)?;
        if triples.is_empty() {
            self.delete_resource_with_writer(writer, graph_iri, &subject_iri);
            return Ok(());
        }

        let searchable_predicates = searchable_predicates();
        let mut extra_text = String::new();
        for (predicate, object) in triples {
            if !is_searchable_predicate(&predicate, &searchable_predicates) {
                continue;
            }
            append_searchable_text(&mut extra_text, &object);
        }

        self.add_resource_document_with_delete(
            writer,
            graph_iri,
            &subject_iri,
            (!extra_text.is_empty()).then_some(extra_text).as_deref(),
            true,
        )
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

    fn delete_graph_documents_uncommitted(&self, graph_id: &str) {
        let writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_text(self.f_graph_id, graph_id));
        self.dirty.store(true, Ordering::SeqCst);
    }
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

fn orphaned_subjects(
    store: &crate::store::GraphStore,
    graph: &crate::core::GraphId,
) -> Result<std::collections::HashSet<String>> {
    Ok(store
        .graph_diagnostics(graph)?
        .orphaned_entities
        .into_iter()
        .collect())
}

/// Convert an EncodedTerm to a plain string (IRI without angle brackets,
/// or the raw string representation for other term types).
fn term_to_string(term: &crate::core::EncodedTerm) -> String {
    if term.0.starts_with('<') && term.0.ends_with('>') {
        term.0[1..term.0.len() - 1].to_string()
    } else {
        term.0.clone()
    }
}

fn append_searchable_text(buffer: &mut String, term: &crate::core::EncodedTerm) {
    let Some(value) = searchable_term_text(term) else {
        return;
    };
    if !buffer.is_empty() {
        buffer.push(' ');
    }
    buffer.push_str(&value);
}

fn searchable_predicates() -> [crate::core::EncodedTerm; 4] {
    [
        crate::core::EncodedTerm::from_named_node(&crate::vocab::schema_name()),
        crate::core::EncodedTerm::from_named_node(&crate::vocab::schema_description()),
        crate::core::EncodedTerm::from_named_node(&crate::vocab::schema_keywords()),
        crate::core::EncodedTerm::from_named_node(&crate::vocab::schema_identifier()),
    ]
}

fn is_searchable_predicate(
    predicate: &crate::core::EncodedTerm,
    searchable_predicates: &[crate::core::EncodedTerm],
) -> bool {
    let normalized = predicate
        .0
        .strip_prefix("<https://schema.org/")
        .map(|suffix| format!("<http://schema.org/{suffix}"));
    searchable_predicates
        .iter()
        .any(|candidate| candidate == predicate || normalized.as_ref() == Some(&candidate.0))
}

fn searchable_term_text(term: &crate::core::EncodedTerm) -> Option<Cow<'_, str>> {
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
    cache: &'a mut HashMap<crate::core::GraphId, HashSet<String>>,
    store: &crate::store::GraphStore,
    graph: &crate::core::GraphId,
) -> Result<&'a HashSet<String>> {
    if !cache.contains_key(graph) {
        cache.insert(graph.clone(), orphaned_subjects(store, graph)?);
    }
    Ok(cache.get(graph).expect("orphan cache inserted"))
}

#[cfg(test)]
mod tests {
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
    fn test_https_schema_description_is_indexed() {
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

        let hits = node.search(&auth, "contextneedle", 10).unwrap();
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
    fn test_old_analyzer_index_is_rebuilt_on_node_open() {
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
                    "https://creativecommons.org/licenses/by/4.0/",
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
        let hits = reopened.search(&auth, "universitat", 10).unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.graph_id == graph.as_str() && hit.subject_iri == graph.as_str())
        );
    }
}
