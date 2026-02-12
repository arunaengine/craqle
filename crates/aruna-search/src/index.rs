use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser};
use tantivy::schema::{Field, STORED, STRING, Schema, SchemaBuilder, TEXT, Value};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

use aruna_core::vocab;

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query parse: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("store: {0}")]
    Store(#[from] aruna_rdf_store::StoreError),
}

pub type Result<T> = std::result::Result<T, SearchError>;

// ── Search Hit ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub graph_id: String,
    pub subject_iri: String,
    pub score: f32,
    pub name: Option<String>,
    pub description: Option<String>,
}

// ── Search Index ────────────────────────────────────────────────────────────

pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter>,
    dirty: AtomicBool,
    // Field handles
    f_doc_key: Field,
    f_graph_id: Field,
    f_subject_iri: Field,
    f_name: Field,
    f_description: Field,
    f_keywords: Field,
    f_all_text: Field,
}

fn build_schema() -> (Schema, Field, Field, Field, Field, Field, Field, Field) {
    let mut builder = SchemaBuilder::default();
    let f_doc_key = builder.add_text_field("doc_key", STRING | STORED);
    let f_graph_id = builder.add_text_field("graph_id", STRING | STORED);
    let f_subject_iri = builder.add_text_field("subject_iri", STRING | STORED);
    let f_name = builder.add_text_field("name", TEXT | STORED);
    let f_description = builder.add_text_field("description", TEXT | STORED);
    let f_keywords = builder.add_text_field("keywords", TEXT | STORED);
    let f_all_text = builder.add_text_field("all_text", TEXT);
    let schema = builder.build();
    (
        schema,
        f_doc_key,
        f_graph_id,
        f_subject_iri,
        f_name,
        f_description,
        f_keywords,
        f_all_text,
    )
}

impl SearchIndex {
    /// Create or open a persistent index at the given directory path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let (
            schema,
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_name,
            f_description,
            f_keywords,
            f_all_text,
        ) = build_schema();

        let dir = path.as_ref();
        let index = if dir.join("meta.json").exists() {
            Index::open_in_dir(dir)?
        } else {
            std::fs::create_dir_all(dir).map_err(|e| {
                tantivy::TantivyError::SystemError(format!("failed to create index directory: {e}"))
            })?;
            Index::create_in_dir(dir, schema)?
        };

        let reader = index.reader()?;
        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            dirty: AtomicBool::new(false),
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_name,
            f_description,
            f_keywords,
            f_all_text,
        })
    }

    /// Create an in-memory index (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let (
            schema,
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_name,
            f_description,
            f_keywords,
            f_all_text,
        ) = build_schema();

        let index = Index::create_in_ram(schema);
        let reader = index.reader()?;
        let writer = index.writer(15_000_000)?;

        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            dirty: AtomicBool::new(false),
            f_doc_key,
            f_graph_id,
            f_subject_iri,
            f_name,
            f_description,
            f_keywords,
            f_all_text,
        })
    }

    /// Add or update a document for the given resource.
    ///
    /// Deletes any existing document with the same `subject_iri` in the same
    /// `graph_id` before inserting the new one.
    pub fn index_resource(
        &self,
        graph_id: &str,
        subject_iri: &str,
        name: Option<&str>,
        description: Option<&str>,
        keywords: Option<&str>,
    ) -> Result<()> {
        let writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_text(
            self.f_doc_key,
            &doc_key(graph_id, subject_iri),
        ));

        // Build the concatenated all_text field.
        let mut all_text_parts: Vec<&str> = Vec::new();
        if let Some(n) = name {
            all_text_parts.push(n);
        }
        if let Some(d) = description {
            all_text_parts.push(d);
        }
        if let Some(k) = keywords {
            all_text_parts.push(k);
        }
        let all_text = all_text_parts.join(" ");

        let mut doc = TantivyDocument::default();
        doc.add_text(self.f_doc_key, doc_key(graph_id, subject_iri));
        doc.add_text(self.f_graph_id, graph_id);
        doc.add_text(self.f_subject_iri, subject_iri);
        doc.add_text(self.f_name, name.unwrap_or(""));
        doc.add_text(self.f_description, description.unwrap_or(""));
        doc.add_text(self.f_keywords, keywords.unwrap_or(""));
        doc.add_text(self.f_all_text, &all_text);

        writer.add_document(doc)?;
        self.dirty.store(true, Ordering::SeqCst);
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

    /// Full-text search restricted to a single graph.
    pub fn search_in_graph(
        &self,
        graph_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.f_all_text]);
        let text_query = query_parser.parse_query(query)?;

        let graph_term = Term::from_field_text(self.f_graph_id, graph_id);
        let graph_query =
            tantivy::query::TermQuery::new(graph_term, tantivy::schema::IndexRecordOption::Basic);

        let combined = BooleanQuery::new(vec![
            (Occur::Must, Box::new(graph_query)),
            (Occur::Must, text_query),
        ]);

        let top_docs = searcher.search(&combined, &TopDocs::with_limit(limit))?;
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
        store: &aruna_rdf_store::GraphStore,
        limit: usize,
    ) -> Result<usize> {
        let queued = store.drain_fts_queue(limit)?;
        if queued.is_empty() {
            return Ok(0);
        }

        let mut seen = std::collections::HashSet::new();
        let mut processed = 0usize;

        for (graph, subject_tid) in queued {
            let subject = store.decode_term(subject_tid)?;
            let key = (graph.as_str().to_string(), subject.0.clone());
            if !seen.insert(key) {
                continue;
            }
            self.sync_subject_from_store(store, &graph, &subject)?;
            processed += 1;
        }

        if processed > 0 {
            self.commit()?;
        }
        Ok(processed)
    }

    pub fn sync_subject_from_store(
        &self,
        store: &aruna_rdf_store::GraphStore,
        graph: &aruna_core::GraphId,
        subject: &aruna_core::EncodedTerm,
    ) -> Result<()> {
        let graph_iri = graph.as_str();
        let subject_iri = term_to_string(subject);
        let graph_term = aruna_core::EncodedTerm::from_named_node(&graph.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return self.delete_resource(graph_iri, &subject_iri);
        };
        let Some(subject_tid) = store.lookup_term(subject)? else {
            return self.delete_resource(graph_iri, &subject_iri);
        };

        let triples = store.triples_for_subject(graph_tid, subject_tid)?;
        if triples.is_empty() {
            return self.delete_resource(graph_iri, &subject_iri);
        }

        let pred_name = aruna_core::EncodedTerm::from_named_node(&vocab::schema_name());
        let pred_desc = aruna_core::EncodedTerm::from_named_node(&vocab::schema_description());
        let pred_kw = aruna_core::EncodedTerm::from_named_node(&vocab::schema_keywords());

        let mut names = Vec::new();
        let mut descriptions = Vec::new();
        let mut keywords = Vec::new();

        for (predicate, object) in triples {
            if predicate == pred_name {
                if let Some(value) = literal_value(&object) {
                    names.push(value);
                }
            } else if predicate == pred_desc {
                if let Some(value) = literal_value(&object) {
                    descriptions.push(value);
                }
            } else if predicate == pred_kw {
                if let Some(value) = literal_value(&object) {
                    keywords.push(value);
                }
            }
        }

        if names.is_empty() && descriptions.is_empty() && keywords.is_empty() {
            return self.delete_resource(graph_iri, &subject_iri);
        }

        let name = join_values(&names);
        let description = join_values(&descriptions);
        let keyword_text = join_values(&keywords);

        self.index_resource(
            graph_iri,
            &subject_iri,
            name.as_deref(),
            description.as_deref(),
            keyword_text.as_deref(),
        )
    }

    /// Reindex all entities in a graph from the RDF store.
    ///
    /// Scans the store for triples with predicates `schema:name`,
    /// `schema:description`, and `schema:keywords`, groups them by subject,
    /// and indexes each subject as a document.
    ///
    /// Returns the number of entities indexed.
    pub fn reindex_from_store(
        &self,
        store: &aruna_rdf_store::GraphStore,
        graph: &aruna_core::GraphId,
    ) -> Result<usize> {
        let graph_iri = graph.as_str();
        let graph_term = aruna_core::EncodedTerm::from_named_node(&graph.0);
        let graph_tid = match store.lookup_term(&graph_term)? {
            Some(tid) => tid,
            None => return Ok(0),
        };

        let pred_name = aruna_core::EncodedTerm::from_named_node(&vocab::schema_name());
        let pred_desc = aruna_core::EncodedTerm::from_named_node(&vocab::schema_description());
        let pred_kw = aruna_core::EncodedTerm::from_named_node(&vocab::schema_keywords());

        // subject -> (names, descriptions, keywords) — accumulate all values
        let mut subjects: HashMap<String, (Vec<String>, Vec<String>, Vec<String>)> = HashMap::new();

        // Helper: query by predicate, collect results grouped by subject.
        let predicates: [(
            &aruna_core::EncodedTerm,
            fn(&mut (Vec<String>, Vec<String>, Vec<String>), String),
        ); 3] = [
            (&pred_name, |entry, val| entry.0.push(val)),
            (&pred_desc, |entry, val| entry.1.push(val)),
            (&pred_kw, |entry, val| entry.2.push(val)),
        ];

        for (pred_encoded, setter) in &predicates {
            let pred_tid = match store.lookup_term(pred_encoded)? {
                Some(tid) => tid,
                None => continue,
            };

            let quads = store.quads_for_pattern(Some(graph_tid), None, Some(pred_tid), None)?;

            for quad in quads {
                let subj_term = store.decode_term(quad.subject)?;
                let obj_term = store.decode_term(quad.object)?;

                let subj_str = term_to_string(&subj_term);
                let obj_str = literal_value(&obj_term);

                if let Some(val) = obj_str {
                    let entry = subjects
                        .entry(subj_str)
                        .or_insert_with(|| (Vec::new(), Vec::new(), Vec::new()));
                    setter(entry, val);
                }
            }
        }

        let count = subjects.len();
        for (subject_iri, (names, descriptions, keywords)) in &subjects {
            self.index_resource(
                graph_iri,
                subject_iri,
                join_values(names).as_deref(),
                join_values(descriptions).as_deref(),
                join_values(keywords).as_deref(),
            )?;
        }

        Ok(count)
    }

    // ── Private helpers ─────────────────────────────────────────────────

    fn doc_to_hit(&self, doc: TantivyDocument, score: f32) -> SearchHit {
        let graph_id = first_text(&doc, self.f_graph_id).unwrap_or_default();
        let subject_iri = first_text(&doc, self.f_subject_iri).unwrap_or_default();
        let name = first_text(&doc, self.f_name).filter(|s| !s.is_empty());
        let description = first_text(&doc, self.f_description).filter(|s| !s.is_empty());

        SearchHit {
            graph_id,
            subject_iri,
            score,
            name,
            description,
        }
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

fn join_values(values: &[String]) -> Option<String> {
    if values.is_empty() {
        None
    } else {
        Some(values.join(" "))
    }
}

/// Convert an EncodedTerm to a plain string (IRI without angle brackets,
/// or the raw string representation for other term types).
fn term_to_string(term: &aruna_core::EncodedTerm) -> String {
    if term.0.starts_with('<') && term.0.ends_with('>') {
        term.0[1..term.0.len() - 1].to_string()
    } else {
        term.0.clone()
    }
}

/// Extract the literal value from an EncodedTerm.
/// Returns `Some(value)` for literal terms, `None` otherwise.
fn literal_value(term: &aruna_core::EncodedTerm) -> Option<String> {
    if let Some(oxterm) = term.to_term() {
        match oxterm {
            oxrdf::Term::Literal(lit) => Some(lit.value().to_string()),
            // For named nodes / blank nodes used as keyword values, return
            // the IRI / blank node id.
            oxrdf::Term::NamedNode(nn) => Some(nn.as_str().to_string()),
            oxrdf::Term::BlankNode(bn) => Some(bn.as_str().to_string()),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_and_search() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Protein Structure Analysis"),
            Some("A dataset about protein folding"),
            Some("biology protein"),
        )?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity2",
            Some("Climate Data"),
            Some("Global temperature measurements"),
            Some("climate weather"),
        )?;

        idx.commit()?;

        let hits = idx.search("protein", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].subject_iri, "http://example.org/entity1");
        assert!(hits[0].name.as_deref() == Some("Protein Structure Analysis"));

        Ok(())
    }

    #[test]
    fn test_search_in_graph() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Protein Data"),
            None,
            None,
        )?;

        idx.index_resource(
            "http://example.org/graph2",
            "http://example.org/entity2",
            Some("Protein Structures"),
            None,
            None,
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
    fn test_upsert_replaces_old_document() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("Old Name"),
            None,
            None,
        )?;
        idx.commit()?;

        // Update the same entity
        idx.index_resource(
            "http://example.org/graph1",
            "http://example.org/entity1",
            Some("New Name"),
            None,
            None,
        )?;
        idx.commit()?;

        // Should only find one result
        let hits = idx.search("name", 10)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name.as_deref(), Some("New Name"));

        Ok(())
    }

    #[test]
    fn test_same_subject_in_multiple_graphs_do_not_collide() -> Result<()> {
        let idx = SearchIndex::open_in_memory()?;

        idx.index_resource(
            "http://example.org/graph1",
            "./",
            Some("Graph One Root"),
            None,
            None,
        )?;
        idx.index_resource(
            "http://example.org/graph2",
            "./",
            Some("Graph Two Root"),
            None,
            None,
        )?;
        idx.commit()?;

        let graph1_hits = idx.search_in_graph("http://example.org/graph1", "graph", 10)?;
        let graph2_hits = idx.search_in_graph("http://example.org/graph2", "graph", 10)?;

        assert_eq!(graph1_hits.len(), 1);
        assert_eq!(graph2_hits.len(), 1);
        assert_eq!(graph1_hits[0].name.as_deref(), Some("Graph One Root"));
        assert_eq!(graph2_hits[0].name.as_deref(), Some("Graph Two Root"));

        Ok(())
    }
}
