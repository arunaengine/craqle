use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT, Value,
};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

const DISK_INDEX_WRITER_HEAP_BYTES: usize = 256_000_000;
const MEMORY_INDEX_WRITER_HEAP_BYTES: usize = 64_000_000;
const REINDEX_FLUSH_CHUNK: usize = 2_048;

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
    builder.add_text_field("all_text", TEXT);
    builder.build()
}

fn schema_fields(schema: &Schema) -> tantivy::Result<(Field, Field, Field, Field)> {
    Ok((
        schema.get_field("doc_key")?,
        schema.get_field("graph_id")?,
        schema.get_field("subject_iri")?,
        schema.get_field("all_text")?,
    ))
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
        let parsed = query_parser.parse_query(query)?;

        let top_docs = searcher.search(&parsed, &TopDocs::with_limit(limit))?;
        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            hits.push(self.doc_to_hit(doc, score));
        }
        Ok(hits)
    }
