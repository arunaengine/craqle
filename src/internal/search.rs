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
