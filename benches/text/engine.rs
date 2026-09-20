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
