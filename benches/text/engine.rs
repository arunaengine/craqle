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

    fn read_forward_at(&self, snapshot: &Snapshot, id: u64) -> Result<Vec<TermFreq>> {
        snapshot
            .get(&self.forward, id.to_be_bytes())?
            .map(|value| {
                postcard::from_bytes::<Vec<TermFreq>>(value.as_ref()).map_err(EngineError::from)
            })
            .transpose()?
            .ok_or(EngineError::Corrupt("forward row missing"))
    }

    fn read_df(&self, snapshot: &Snapshot, term: &str) -> Result<u64> {
        snapshot
            .get(&self.stats, term.as_bytes())?
            .map(|value| decode_u64(value.as_ref()))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn write_stat(&self, batch: &mut OwnedWriteBatch, change: &TermChange) -> Result<()> {
        if change.old.is_some() == change.new.is_some() {
            return Ok(());
        }
        let current = self
            .stats
            .get(change.term.as_bytes())?
            .map(|value| decode_u64(value.as_ref()))
            .transpose()?
            .unwrap_or_default();
        let updated = if change.new.is_some() {
            current.checked_add(1).ok_or(EngineError::Overflow)?
        } else {
            current
                .checked_sub(1)
                .ok_or(EngineError::Corrupt("document frequency invalid"))?
        };
        if updated == 0 {
            batch.remove(&self.stats, change.term.as_bytes());
        } else {
            batch.insert(&self.stats, change.term.as_bytes(), updated.to_be_bytes());
        }
        Ok(())
    }

    fn write_posting(&self, batch: &mut OwnedWriteBatch, req: PostingChange<'_>) -> Result<()> {
        match self.options.layout {
            PostingLayout::Simple => {
                let key = posting_key(&req.change.term, req.id)?;
                if let Some(freq) = req.change.new {
                    batch.insert(&self.postings, key, freq.to_be_bytes());
                } else {
                    batch.remove(&self.postings, key);
                }
            }
            PostingLayout::Block { docs } => {
                let block = req.id / u64::from(docs);
                let key = block_key(&req.change.term, block)?;
                let mut entries: Vec<Posting> = self
                    .postings
                    .get(&key)?
                    .map(|value| postcard::from_bytes(value.as_ref()).map_err(EngineError::from))
                    .transpose()?
                    .unwrap_or_default();
                entries.retain(|entry| entry.doc != req.id);
                if let Some(freq) = req.change.new {
                    entries.push(Posting { doc: req.id, freq });
                    entries.sort_unstable_by_key(|entry| entry.doc);
                }
                if entries.len() > usize::from(docs) {
                    return Err(EngineError::Corrupt("posting block exceeded bound"));
                }
                if entries.is_empty() {
                    batch.remove(&self.postings, key);
                } else {
                    batch.insert(&self.postings, key, postcard::to_allocvec(&entries)?);
                }
            }
        }
        Ok(())
    }

    fn scan_postings(&self, mut scan: PostingScan<'_>) -> Result<()> {
        let prefix = term_prefix(scan.term)?;
        for guard in scan.snapshot.prefix(&self.postings, &prefix) {
            let (key, value) = guard.into_inner()?;
            charge_bytes(
                scan.work,
                scan.bounds.bytes,
                key.len().saturating_add(value.len()),
            )?;
            match self.options.layout {
                PostingLayout::Simple => {
                    let id = decode_posting(key.as_ref(), prefix.len())?;
                    let freq = decode_u32(value.as_ref())?;
                    self.score_posting(&mut scan, Posting { doc: id, freq })?;
                }
                PostingLayout::Block { docs } => {
                    let entries: Vec<Posting> = postcard::from_bytes(value.as_ref())?;
                    if entries.len() > usize::from(docs) {
                        return Err(EngineError::Corrupt("posting block exceeded bound"));
                    }
                    for posting in entries {
                        self.score_posting(&mut scan, posting)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn score_posting(&self, scan: &mut PostingScan<'_>, posting: Posting) -> Result<()> {
        scan.work.posting_rows = scan.work.posting_rows.saturating_add(1);
        check_limit("posting rows", scan.bounds.postings, scan.work.posting_rows)?;
        if scan.rejected.contains(&posting.doc) {
            return Ok(());
        }
        if let Some(candidate) = scan.candidates.get_mut(&posting.doc) {
            candidate.score += scan.weight.score(candidate.record.norm, posting.freq);
            return Ok(());
        }
        check_limit(
            "candidate documents",
            scan.bounds.candidates,
            scan.candidates.len().saturating_add(1),
        )?;
        let value = scan
            .snapshot
            .get(&self.docs, record_key(posting.doc))?
            .ok_or(EngineError::Corrupt("active posting lacks document"))?;
        charge_bytes(scan.work, scan.bounds.bytes, value.len())?;
        let record: DocRecord = postcard::from_bytes(value.as_ref())?;
        scan.work.metadata_reads = scan.work.metadata_reads.saturating_add(1);
        if !record.active {
            return Err(EngineError::Corrupt("inactive document has posting"));
        }
        if scan.allows.is_some_and(|allows| !allows(&record.graph)) {
            scan.work.rejected_docs = scan.work.rejected_docs.saturating_add(1);
            scan.rejected.insert(posting.doc);
            return Ok(());
        }
        let score = scan.weight.score(record.norm, posting.freq);
        scan.work.candidate_docs = scan.work.candidate_docs.saturating_add(1);
        scan.work.scored_docs = scan.work.scored_docs.saturating_add(1);
        scan.candidates
            .insert(posting.doc, Candidate { record, score });
        Ok(())
    }
}

impl Default for UpdateWork {
    fn default() -> Self {
        Self {
            generation: 0,
            terms: 0,
            postings: 0,
            bytes: 0,
        }
    }
}

struct PostingChange<'a> {
    id: u64,
    change: &'a TermChange,
}

struct PostingScan<'a> {
    snapshot: &'a Snapshot,
    term: &'a str,
    weight: &'a Bm25Weight,
    bounds: QueryBounds,
    allows: Option<&'a dyn Fn(&str) -> bool>,
    candidates: &'a mut BTreeMap<u64, Candidate>,
    rejected: &'a mut BTreeSet<u64>,
    work: &'a mut WorkCount,
}

struct Candidate {
    record: DocRecord,
    score: f32,
}

struct TermChange {
    term: String,
    old: Option<u32>,
    new: Option<u32>,
}

fn analyze(text: &str) -> Result<Vec<TermFreq>> {
    let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build();
    let mut frequencies = BTreeMap::new();
    let mut overflow = false;
    analyzer.token_stream(text).process(&mut |token| {
        let entry = frequencies.entry(token.text.clone()).or_insert(0u32);
        if let Some(updated) = entry.checked_add(1) {
            *entry = updated;
        } else {
            overflow = true;
        }
    });
    if overflow {
        return Err(EngineError::Overflow);
    }
    Ok(frequencies
        .into_iter()
        .map(|(term, freq)| TermFreq { term, freq })
        .collect())
}

fn parse_query(query: &str, max_terms: usize) -> Result<Vec<String>> {
    let parts: Vec<_> = query.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(Vec::new());
    }
    if parts.len() % 2 == 0 {
        return Err(EngineError::Query(query.to_string()));
    }
    let mut terms = BTreeSet::new();
    for (index, part) in parts.into_iter().enumerate() {
        if index % 2 == 1 {
            if !part.eq_ignore_ascii_case("OR") {
                return Err(EngineError::Query(query.to_string()));
            }
            continue;
        }
        if part.eq_ignore_ascii_case("OR")
            || part
                .bytes()
                .any(|byte| b"\"():+-*?[]{}^~\\".contains(&byte))
        {
            return Err(EngineError::Query(query.to_string()));
        }
        let analyzed = analyze(part)?;
        if analyzed.len() != 1 || analyzed[0].freq != 1 {
            return Err(EngineError::Query(query.to_string()));
        }
        terms.insert(analyzed[0].term.clone());
        check_limit("query terms", max_terms, terms.len())?;
    }
    Ok(terms.into_iter().collect())
}

fn merge_terms(old: &[TermFreq], new: &[TermFreq]) -> Vec<TermChange> {
    let mut changes = BTreeMap::new();
    for term in old {
        changes.insert(term.term.clone(), (Some(term.freq), None));
    }
    for term in new {
        changes
            .entry(term.term.clone())
            .and_modify(|entry| entry.1 = Some(term.freq))
            .or_insert((None, Some(term.freq)));
    }
    changes
        .into_iter()
        .map(|(term, (old, new))| TermChange { term, old, new })
        .collect()
}

fn identity_key(key: DocumentKey<'_>) -> Result<Vec<u8>> {
    let graph_len = u32::try_from(key.graph.len()).map_err(|_| EngineError::Limit {
        resource: "graph bytes",
        limit: u32::MAX as usize,
        actual: key.graph.len(),
    })?;
    let mut encoded = Vec::with_capacity(5 + key.graph.len() + key.subject.len());
    encoded.push(0);
    encoded.extend_from_slice(&graph_len.to_be_bytes());
    encoded.extend_from_slice(key.graph.as_bytes());
    encoded.extend_from_slice(key.subject.as_bytes());
    check_limit("document key bytes", KEY_LIMIT, encoded.len())?;
    Ok(encoded)
}

fn record_key(id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = 1;
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

fn term_prefix(term: &str) -> Result<Vec<u8>> {
    let len = u16::try_from(term.len()).map_err(|_| EngineError::Limit {
        resource: "term bytes",
        limit: u16::MAX as usize,
        actual: term.len(),
    })?;
    let mut key = Vec::with_capacity(2 + term.len());
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(term.as_bytes());
    Ok(key)
}

fn posting_key(term: &str, doc: u64) -> Result<Vec<u8>> {
    let mut key = term_prefix(term)?;
    key.extend_from_slice(&doc.to_be_bytes());
    Ok(key)
}

fn block_key(term: &str, block: u64) -> Result<Vec<u8>> {
    posting_key(term, block)
}

fn decode_posting(key: &[u8], prefix_len: usize) -> Result<u64> {
    let bytes = key
        .get(prefix_len..)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .ok_or(EngineError::Corrupt("posting key invalid"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| EngineError::Corrupt("u64 invalid"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_u32(bytes: &[u8]) -> Result<u32> {
    let bytes = <[u8; 4]>::try_from(bytes).map_err(|_| EngineError::Corrupt("u32 invalid"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn check_limit(resource: &'static str, limit: usize, actual: usize) -> Result<()> {
    if actual > limit {
        return Err(EngineError::Limit {
            resource,
            limit,
            actual,
        });
    }
    Ok(())
}

fn charge_bytes(work: &mut WorkCount, limit: usize, bytes: usize) -> Result<()> {
    work.bytes = work.bytes.saturating_add(bytes);
    check_limit("query bytes", limit, work.bytes)
}

fn rank_hits(candidates: BTreeMap<u64, Candidate>, limit: usize) -> Vec<SearchHit> {
    let mut hits: Vec<_> = candidates
        .into_values()
        .map(|candidate| SearchHit {
            graph: candidate.record.graph,
            subject: candidate.record.subject,
            score: candidate.score,
        })
        .collect();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| {
                stable_key(&left.graph, &left.subject)
                    .cmp(&stable_key(&right.graph, &right.subject))
            })
            .then_with(|| left.graph.cmp(&right.graph))
            .then_with(|| left.subject.cmp(&right.subject))
    });
    hits.truncate(limit);
    hits
}

pub fn stable_key(graph: &str, subject: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(graph.len() as u64).to_be_bytes());
    hasher.update(graph.as_bytes());
    hasher.update(&(subject.len() as u64).to_be_bytes());
    hasher.update(subject.as_bytes());
    *hasher.finalize().as_bytes()
}

pub fn next_down(score: f32) -> f32 {
    if score.is_nan() || score == f32::NEG_INFINITY {
        return score;
    }
    if score == 0.0 {
        return -f32::from_bits(1);
    }
    let bits = score.to_bits();
    if score > 0.0 {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    }
}
