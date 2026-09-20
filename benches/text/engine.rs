//! Maintains experimental persisted postings and exact corpus statistics.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Readable, Snapshot,
};
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::query::{Bm25StatisticsProvider as Bm25Stats, Bm25Weight};
use tantivy::schema::Field;
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, TokenStream,
};

const FORMAT: u16 = 1;
const STATE_KEY: &[u8] = b"state";
const KEY_LIMIT: usize = 65_535;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("codec: {0}")]
    Codec(#[from] postcard::Error),
    #[error("unsupported query syntax: {0}")]
    Query(String),
    #[error("{resource} uses {actual}, limit is {limit}")]
    Limit {
        resource: &'static str,
        limit: usize,
        actual: usize,
    },
    #[error("corrupt experimental text index: {0}")]
    Corrupt(&'static str),
    #[error("experimental text index layout differs from requested layout")]
    Layout,
    #[error("experimental text index counter overflow")]
    Overflow,
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostingLayout {
    Simple,
    Block { docs: u16 },
}

#[derive(Clone, Copy, Debug)]
pub struct EngineOptions {
    pub layout: PostingLayout,
    pub max_doc_bytes: usize,
    pub max_terms: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            layout: PostingLayout::Simple,
            max_doc_bytes: 1 << 20,
            max_terms: 65_536,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DocumentInput<'a> {
    pub graph: &'a str,
    pub subject: &'a str,
    pub text: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct DocumentKey<'a> {
    pub graph: &'a str,
    pub subject: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct QueryBounds {
    pub terms: usize,
    pub postings: usize,
    pub candidates: usize,
    pub bytes: usize,
}

impl Default for QueryBounds {
    fn default() -> Self {
        Self {
            terms: 32,
            postings: 1_000_000,
            candidates: 250_000,
            bytes: 64 << 20,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SearchRequest<'a> {
    pub query: &'a str,
    pub limit: usize,
    pub bounds: QueryBounds,
    pub allows: Option<&'a dyn Fn(&str) -> bool>,
}

#[derive(Clone, Copy)]
pub struct ScoreRequest<'a> {
    pub query: &'a str,
    pub documents: &'a [DocumentKey<'a>],
    pub bounds: QueryBounds,
}

#[derive(Clone, Copy, Debug)]
pub struct StatsRequest {
    pub field: Field,
    pub max_terms: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub graph: String,
    pub subject: String,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HitIdentity {
    pub graph: String,
    pub subject: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkCount {
    pub pruning_fallback: bool,
    pub posting_rows: usize,
    pub candidate_docs: usize,
    pub scored_docs: usize,
    pub rejected_docs: usize,
    pub metadata_reads: usize,
    pub final_reads: usize,
    pub deduplicated_docs: usize,
    pub bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchReport {
    pub generation: u64,
    pub hits: Vec<SearchHit>,
    pub work: WorkCount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateWork {
    pub generation: u64,
    pub terms: usize,
    pub postings: usize,
    pub bytes: usize,
}

#[derive(Clone)]
pub struct ActiveStats {
    field: Field,
    docs: u64,
    tokens: u64,
    bytes: usize,
    freqs: HashMap<Vec<u8>, u64>,
}

impl Bm25Stats for ActiveStats {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        Ok(if field == self.field { self.tokens } else { 0 })
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if term.field() != self.field {
            return Ok(0);
        }
        Ok(self
            .freqs
            .get(term.serialized_value_bytes())
            .copied()
            .unwrap_or(0))
    }
}

impl ActiveStats {
    pub fn docs(&self) -> u64 {
        self.docs
    }

    pub fn tokens(&self) -> u64 {
        self.tokens
    }

    pub fn terms(&self) -> usize {
        self.freqs.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IndexState {
    format: u16,
    layout: PostingLayout,
    generation: u64,
    next_doc: u64,
    docs: u64,
    tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DocRecord {
    id: u64,
    graph: String,
    subject: String,
    length: u32,
    norm: u8,
    active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TermFreq {
    term: String,
    freq: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Posting {
    doc: u64,
    freq: u32,
}

pub struct FjallBm25 {
    db: Database,
    docs: Keyspace,
    forward: Keyspace,
    postings: Keyspace,
    stats: Keyspace,
    meta: Keyspace,
    options: EngineOptions,
    writer: Mutex<()>,
}

impl FjallBm25 {
    pub fn open(path: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
        if matches!(options.layout, PostingLayout::Block { docs: 0 }) {
            return Err(EngineError::Layout);
        }
        let db = Database::builder(path).open()?;
        let docs = db.keyspace("text_documents", KeyspaceCreateOptions::default)?;
        let forward = db.keyspace("text_forward", KeyspaceCreateOptions::default)?;
        let postings = db.keyspace("text_postings", KeyspaceCreateOptions::default)?;
        let stats = db.keyspace("text_stats", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("text_metadata", KeyspaceCreateOptions::default)?;
        let engine = Self {
            db,
            docs,
            forward,
            postings,
            stats,
            meta,
            options,
            writer: Mutex::new(()),
        };
        match engine.read_state(&engine.db.snapshot())? {
            Some(state) if state.format == FORMAT && state.layout == options.layout => {}
            Some(state) if state.format != FORMAT => {
                return Err(EngineError::Corrupt("unsupported format"));
            }
            Some(_) => return Err(EngineError::Layout),
            None => {
                let state = IndexState {
                    format: FORMAT,
                    layout: options.layout,
                    generation: 0,
                    next_doc: 0,
                    docs: 0,
                    tokens: 0,
                };
                let mut batch = engine.db.batch();
                batch.insert(&engine.meta, STATE_KEY, postcard::to_allocvec(&state)?);
                batch.commit()?;
            }
        }
        Ok(engine)
    }

    pub fn generation(&self) -> Result<u64> {
        Ok(self
            .read_state(&self.db.snapshot())?
            .ok_or(EngineError::Corrupt("state missing"))?
            .generation)
    }

    pub fn persist(&self, mode: PersistMode) -> Result<()> {
        self.db.persist(mode)?;
        Ok(())
    }

    pub fn upsert(&self, input: DocumentInput<'_>) -> Result<UpdateWork> {
        let input_bytes = input
            .graph
            .len()
            .saturating_add(input.subject.len())
            .saturating_add(input.text.len())
            .saturating_add(2);
        check_limit("document bytes", self.options.max_doc_bytes, input_bytes)?;
        let key = identity_key(DocumentKey {
            graph: input.graph,
            subject: input.subject,
        })?;
        let mut all_text = String::with_capacity(input_bytes);
        all_text.push_str(input.graph);
        all_text.push(' ');
        all_text.push_str(input.subject);
        all_text.push(' ');
        all_text.push_str(input.text);
        let terms = analyze(&all_text)?;
        check_limit("document terms", self.options.max_terms, terms.len())?;
        let length = terms.iter().try_fold(0u32, |count, term| {
            count.checked_add(term.freq).ok_or(EngineError::Overflow)
        })?;
        let _guard = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let old_id = self.read_identity(&key)?;
        let old = match old_id {
            Some(id) => Some(
                self.read_doc(id)?
                    .ok_or(EngineError::Corrupt("identity lacks document"))?,
            ),
            None => None,
        };
        let old_terms = match old.as_ref().filter(|doc| doc.active) {
            Some(doc) => self.read_forward(doc.id)?,
            None => Vec::new(),
        };
        let mut state = self
            .read_state(&self.db.snapshot())?
            .ok_or(EngineError::Corrupt("state missing"))?;
        let id = match old.as_ref() {
            Some(doc) => doc.id,
            None => {
                let id = state.next_doc;
                state.next_doc = state.next_doc.checked_add(1).ok_or(EngineError::Overflow)?;
                id
            }
        };
        if old.as_ref().is_none_or(|doc| !doc.active) {
            state.docs = state.docs.checked_add(1).ok_or(EngineError::Overflow)?;
        }
        let old_len = old
            .as_ref()
            .filter(|doc| doc.active)
            .map_or(0, |doc| u64::from(doc.length));
        state.tokens = state
            .tokens
            .checked_sub(old_len)
            .and_then(|tokens| tokens.checked_add(u64::from(length)))
            .ok_or(EngineError::Corrupt("token total invalid"))?;
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(EngineError::Overflow)?;

        let changes = merge_terms(&old_terms, &terms);
        let mut batch = self.db.batch();
        for change in &changes {
            self.write_posting(&mut batch, PostingChange { id, change })?;
            self.write_stat(&mut batch, change)?;
        }
        let record = DocRecord {
            id,
            graph: input.graph.to_string(),
            subject: input.subject.to_string(),
            length,
            norm: FieldNormReader::fieldnorm_to_id(length),
            active: true,
        };
        batch.insert(&self.docs, key, id.to_be_bytes());
        batch.insert(&self.docs, record_key(id), postcard::to_allocvec(&record)?);
        batch.insert(
            &self.forward,
            id.to_be_bytes(),
            postcard::to_allocvec(&terms)?,
        );
        batch.insert(&self.meta, STATE_KEY, postcard::to_allocvec(&state)?);
        batch.commit()?;
        Ok(UpdateWork {
            generation: state.generation,
            terms: changes.len(),
            postings: changes.len(),
            bytes: input_bytes,
        })
    }

    pub fn delete(&self, key: DocumentKey<'_>) -> Result<UpdateWork> {
        let key = identity_key(key)?;
        let _guard = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(id) = self.read_identity(&key)? else {
            return Ok(UpdateWork {
                generation: self.generation()?,
                ..UpdateWork::default()
            });
        };
        let mut record = self
            .read_doc(id)?
            .ok_or(EngineError::Corrupt("identity lacks document"))?;
        if !record.active {
            return Ok(UpdateWork {
                generation: self.generation()?,
                ..UpdateWork::default()
            });
        }
        let terms = self.read_forward(record.id)?;
        let mut state = self
            .read_state(&self.db.snapshot())?
            .ok_or(EngineError::Corrupt("state missing"))?;
        state.docs = state
            .docs
            .checked_sub(1)
            .ok_or(EngineError::Corrupt("doc total invalid"))?;
        state.tokens = state
            .tokens
            .checked_sub(u64::from(record.length))
            .ok_or(EngineError::Corrupt("token total invalid"))?;
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(EngineError::Overflow)?;
        let changes = merge_terms(&terms, &[]);
        let mut batch = self.db.batch();
        for change in &changes {
            self.write_posting(
                &mut batch,
                PostingChange {
                    id: record.id,
                    change,
                },
            )?;
            self.write_stat(&mut batch, change)?;
        }
        record.active = false;
        batch.insert(
            &self.docs,
            record_key(record.id),
            postcard::to_allocvec(&record)?,
        );
        batch.remove(&self.forward, record.id.to_be_bytes());
        batch.insert(&self.meta, STATE_KEY, postcard::to_allocvec(&state)?);
        batch.commit()?;
        Ok(UpdateWork {
            generation: state.generation,
            terms: changes.len(),
            postings: changes.len(),
            bytes: 0,
        })
    }

    pub fn search(&self, request: SearchRequest<'_>) -> Result<SearchReport> {
        check_limit("query bytes", request.bounds.bytes, request.query.len())?;
        let terms = parse_query(request.query, request.bounds.terms)?;
        if request.limit == 0 || terms.is_empty() {
            return Ok(SearchReport {
                generation: self.generation()?,
                hits: Vec::new(),
                work: WorkCount::default(),
            });
        }
        let snapshot = self.db.snapshot();
        let state = self
            .read_state(&snapshot)?
            .ok_or(EngineError::Corrupt("state missing"))?;
        if state.docs == 0 {
            return Ok(SearchReport {
                generation: state.generation,
                hits: Vec::new(),
                work: WorkCount::default(),
            });
        }
        let mut work = WorkCount {
            bytes: request.query.len(),
            ..WorkCount::default()
        };
        let mut candidates = BTreeMap::new();
        let mut rejected = BTreeSet::new();
        let average = state.tokens as f32 / state.docs as f32;
        for term in &terms {
            let frequency = self.read_df(&snapshot, term)?;
            if frequency == 0 {
                continue;
            }
            let weight = Bm25Weight::for_one_term_without_explain(frequency, state.docs, average);
            self.scan_postings(PostingScan {
                snapshot: &snapshot,
                term,
                weight: &weight,
                bounds: request.bounds,
                allows: request.allows,
                candidates: &mut candidates,
                rejected: &mut rejected,
                work: &mut work,
            })?;
        }
        let hits = rank_hits(candidates, request.limit);
        Ok(SearchReport {
            generation: state.generation,
            hits,
            work,
        })
    }

    pub fn score_candidates(&self, request: ScoreRequest<'_>) -> Result<SearchReport> {
        check_limit("query bytes", request.bounds.bytes, request.query.len())?;
        check_limit(
            "candidate documents",
            request.bounds.candidates,
            request.documents.len(),
        )?;
        let terms = parse_query(request.query, request.bounds.terms)?;
        let snapshot = self.db.snapshot();
        let state = self
            .read_state(&snapshot)?
            .ok_or(EngineError::Corrupt("state missing"))?;
        let mut weights = HashMap::new();
        if state.docs > 0 {
            let average = state.tokens as f32 / state.docs as f32;
            for term in &terms {
                let frequency = self.read_df(&snapshot, term)?;
                if frequency > 0 {
                    weights.insert(
                        term.clone(),
                        Bm25Weight::for_one_term_without_explain(frequency, state.docs, average),
                    );
                }
            }
        }
        let mut work = WorkCount {
            bytes: request.query.len(),
            ..WorkCount::default()
        };
        let mut candidates = BTreeMap::new();
        for key in request.documents {
            let encoded = identity_key(*key)?;
            charge_bytes(&mut work, request.bounds.bytes, encoded.len())?;
            let Some(id) = self.read_identity_at(&snapshot, &encoded)? else {
                continue;
            };
            if let Some(bytes) = snapshot.size_of(&self.docs, record_key(id))? {
                charge_bytes(&mut work, request.bounds.bytes, bytes as usize)?;
            }
            let Some(record) = self.read_doc_at(&snapshot, id)? else {
                continue;
            };
            if !record.active {
                continue;
            }
            if let Some(bytes) = snapshot.size_of(&self.forward, id.to_be_bytes())? {
                charge_bytes(&mut work, request.bounds.bytes, bytes as usize)?;
            }
            let forward = self.read_forward_at(&snapshot, record.id)?;
            let by_term: HashMap<_, _> = forward
                .into_iter()
                .map(|entry| (entry.term, entry.freq))
                .collect();
            let mut score = 0.0f32;
            let mut matched = false;
            for (term, weight) in &weights {
                if let Some(freq) = by_term.get(term) {
                    score += weight.score(record.norm, *freq);
                    work.posting_rows = work.posting_rows.saturating_add(1);
                    check_limit("posting rows", request.bounds.postings, work.posting_rows)?;
                    matched = true;
                }
            }
            if !matched {
                continue;
            }
            work.candidate_docs = work.candidate_docs.saturating_add(1);
            work.scored_docs = work.scored_docs.saturating_add(1);
            work.metadata_reads = work.metadata_reads.saturating_add(2);
            candidates.insert(record.id, Candidate { record, score });
        }
        Ok(SearchReport {
            generation: state.generation,
            hits: rank_hits(candidates, request.documents.len()),
            work,
        })
    }

    pub fn load_stats(&self, request: StatsRequest) -> Result<ActiveStats> {
        let snapshot = self.db.snapshot();
        let state = self
            .read_state(&snapshot)?
            .ok_or(EngineError::Corrupt("state missing"))?;
        let mut freqs = HashMap::new();
        let mut bytes = 0usize;
        for guard in snapshot.iter(&self.stats) {
            let (key, value) = guard.into_inner()?;
            check_limit(
                "statistics terms",
                request.max_terms,
                freqs.len().saturating_add(1),
            )?;
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
            check_limit("statistics bytes", request.max_bytes, bytes)?;
            freqs.insert(key.to_vec(), decode_u64(value.as_ref())?);
        }
        Ok(ActiveStats {
            field: request.field,
            docs: state.docs,
            tokens: state.tokens,
            bytes,
            freqs,
        })
    }

    fn read_state(&self, snapshot: &Snapshot) -> Result<Option<IndexState>> {
        snapshot
            .get(&self.meta, STATE_KEY)?
            .map(|value| {
                postcard::from_bytes::<IndexState>(value.as_ref()).map_err(EngineError::from)
            })
            .transpose()
    }

    fn read_identity(&self, key: &[u8]) -> Result<Option<u64>> {
        self.docs
            .get(key)?
            .map(|value| decode_u64(value.as_ref()))
            .transpose()
    }

    fn read_identity_at(&self, snapshot: &Snapshot, key: &[u8]) -> Result<Option<u64>> {
        snapshot
            .get(&self.docs, key)?
            .map(|value| decode_u64(value.as_ref()))
            .transpose()
    }

    fn read_doc(&self, id: u64) -> Result<Option<DocRecord>> {
        self.docs
            .get(record_key(id))?
            .map(|value| {
                postcard::from_bytes::<DocRecord>(value.as_ref()).map_err(EngineError::from)
            })
            .transpose()
    }

    fn read_doc_at(&self, snapshot: &Snapshot, id: u64) -> Result<Option<DocRecord>> {
        snapshot
            .get(&self.docs, record_key(id))?
            .map(|value| {
                postcard::from_bytes::<DocRecord>(value.as_ref()).map_err(EngineError::from)
            })
            .transpose()
    }

    fn read_forward(&self, id: u64) -> Result<Vec<TermFreq>> {
        self.forward
            .get(id.to_be_bytes())?
            .map(|value| {
                postcard::from_bytes::<Vec<TermFreq>>(value.as_ref()).map_err(EngineError::from)
            })
            .transpose()?
            .ok_or(EngineError::Corrupt("forward row missing"))
    }
}
