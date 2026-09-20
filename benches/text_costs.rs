//! Compares persisted local search, Tantivy pruning, and Fjall text scoring.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

#[cfg(feature = "allocation-metrics")]
#[path = "allocation.rs"]
mod allocation;
#[path = "text/joins.rs"]
mod joins;
#[path = "text/mod.rs"]
mod text;

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use craqle::{
    CraqleNode, CreateCrateRequest, GrantAuthorizer, GraphId, GraphPolicy, NewDataEntity,
    PermissionGrant, PermissionLevel, QueryResults, SearchRequest as LocalSearch, SearchStorage,
};
use oxrdf::{NamedNode, Term};
use serde_json::json;
use tantivy::columnar::{BytesColumn, Column};
use tantivy::query::QueryParser;
use tantivy::schema::{
    FAST, Field, IndexRecordOption, STRING, SchemaBuilder, TEXT, TextFieldIndexing,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{DocAddress, Index, IndexReader, IndexWriter, TantivyDocument, Term as IndexTerm};
use text::{
    DocumentInput, DocumentKey, EngineOptions, FjallBm25, HitIdentity, PostingLayout, QueryBounds,
    ScoreRequest, SearchHit as BenchHit, SearchRequest as FjallSearch, StatsRequest, WorkCount,
};

#[cfg(feature = "allocation-metrics")]
use allocation::{AllocationInterval, AllocationSample};

const ANALYZER: &str = "craqle_text_v2";
const COMMON: &str = "commonneedle";
const RARE: &str = "rareneedle";
const TIE: &str = "tieneedle";

#[derive(Clone, Copy, Debug)]
enum Access {
    All,
    Half,
    One,
}

impl Access {
    fn allows(self, graph: usize) -> bool {
        matches!(self, Self::All)
            || matches!(self, Self::Half) && graph % 2 == 0
            || matches!(self, Self::One) && graph == 0
    }
    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Half => "half",
            Self::One => "one",
        }
    }
}

struct Config {
    seed: u64,
    docs: usize,
    graphs: usize,
    access: Access,
    limit: usize,
    samples: usize,
    common: String,
    rare: String,
    common_mod: u64,
    rare_mod: u64,
    tolerance: f32,
    layout: PostingLayout,
}

#[derive(Clone)]
struct PreparedDoc {
    graph_index: usize,
    graph: String,
    subject: String,
    body: String,
    entity: Option<NewDataEntity>,
}

struct Corpus {
    docs: Vec<PreparedDoc>,
    graphs: Vec<GraphId>,
}
struct LocalFixture {
    root: tempfile::TempDir,
    node: CraqleNode,
}
struct Fields {
    key: Field,
    graph: Field,
    subject: Field,
    graph_ord: Field,
    stable: Field,
    text: Field,
}
struct Baseline {
    root: tempfile::TempDir,
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    fields: Fields,
}
struct SegmentMeta {
    graph: BytesColumn,
    subject: BytesColumn,
    stable: BytesColumn,
    graph_ord: Column<u64>,
}
struct RunCtx<'a> {
    config: &'a Config,
    node: &'a CraqleNode,
    reader: &'a GrantAuthorizer,
    engine: &'a FjallBm25,
    baseline: &'a Baseline,
    stats: &'a text::ActiveStats,
    eligible: &'a [DocumentKey<'a>],
}

fn main() {
    let config = read_config();
    emit(json!({"kind":"manifest",
        "production_scorer":"weight.scorer plus sequential advance; generation and auth precede K",
        "corrected_tantivy":"Weight::for_each_pruning with exact-active statistics",
        "pruning_guard":"falls back to exhaustive unless one segment and provider physical statistics are equal",
        "tantivy_identity":"FAST graph ordinal plus stable key per callback; FAST graph and subject decode only final winners",
        "tantivy_work_scope":"posting_rows counts callbacks, not physical postings skipped by pruning",
        "wand_claim":"none until the parsed weight specialization is inspected",
        "fjall_search":"exhaustive bounded posting accumulation before stable top-k",
        "candidate_scoring":"benchmark-only RDF-first interface; service limits unchanged",
        "prepared_text":"body excludes graph and subject; B and C prepend each once; metadata body is empty",
        "score_tolerance":config.tolerance,
        "allocation_profile":if cfg!(feature="allocation-metrics") {
            "separate untimed process-wide intervals; background workers may contribute"
        } else {
            "disabled; counting allocator is not compiled into this binary"
        },
        "proc_io_scope":"process-wide write_bytes deltas around serial builds; zero is not logical write amplification"}));
    run(config);
}

fn run(config: Config) {
    assert!(config.docs > 0 && config.graphs > 0 && config.limit > 0 && config.samples > 0);
    let corpus = build_corpus(&config);
    assert_token_parity(&corpus);
    let io0 = io_bytes();
    let started = Instant::now();
    let local = build_local(&corpus);
    let local_build = (started.elapsed().as_nanos(), io_delta(io0));
    let fjall_root = tempfile::tempdir().unwrap();
    let io0 = io_bytes();
    let started = Instant::now();
    let engine = FjallBm25::open(
        fjall_root.path(),
        EngineOptions {
            layout: config.layout,
            ..EngineOptions::default()
        },
    )
    .unwrap();
    for doc in &corpus.docs {
        engine.upsert(input(doc)).unwrap();
    }
    engine.persist(fjall::PersistMode::SyncAll).unwrap();
    let fjall_build = (started.elapsed().as_nanos(), io_delta(io0));
    let io0 = io_bytes();
    let started = Instant::now();
    let mut baseline = build_tantivy(&corpus);
    let tantivy_build = (started.elapsed().as_nanos(), io_delta(io0));
    let started = Instant::now();
    let stats = engine
        .load_stats(StatsRequest {
            field: baseline.fields.text,
            max_terms: 1_000_000,
            max_bytes: 64 << 20,
        })
        .unwrap();
    let stats_ns = started.elapsed().as_nanos();
    emit(
        json!({"kind":"build", "seed":config.seed, "documents":config.docs,
        "indexed_documents":corpus.docs.len(), "graphs":config.graphs,
        "permissions":config.access.label(), "limit":config.limit,
        "common_mod":config.common_mod, "rare_mod":config.rare_mod,
        "layout":format!("{:?}", config.layout), "local_ns":local_build.0,
        "local_write_bytes":local_build.1, "fjall_ns":fjall_build.0,
        "fjall_write_bytes":fjall_build.1, "tantivy_ns":tantivy_build.0,
        "tantivy_write_bytes":tantivy_build.1, "stats_ns":stats_ns,
        "stats_docs":stats.docs(), "stats_tokens":stats.tokens(), "stats_terms":stats.terms(),
        "stats_bytes":stats.bytes(),
        "local_disk_bytes":dir_bytes(local.root.path()), "fjall_disk_bytes":dir_bytes(fjall_root.path()),
        "tantivy_disk_bytes":dir_bytes(baseline.root.path()),
        "local_allocated_bytes":dir_blocks(local.root.path()),
        "fjall_allocated_bytes":dir_blocks(fjall_root.path()),
        "tantivy_allocated_bytes":dir_blocks(baseline.root.path())}),
    );
    let reader = reader_auth(config.access, config.graphs);
    let eligible = corpus
        .docs
        .iter()
        .filter(|doc| config.access.allows(doc.graph_index))
        .map(|doc| key(doc))
        .collect::<Vec<_>>();
    {
        let ctx = RunCtx {
            config: &config,
            node: &local.node,
            reader: &reader,
            engine: &engine,
            baseline: &baseline,
            stats: &stats,
            eligible: &eligible,
        };
        let combined = format!("{} OR {}", config.common, config.rare);
        for query in [
            config.common.as_str(),
            config.rare.as_str(),
            TIE,
            combined.as_str(),
        ] {
            run_query(&ctx, query);
        }
        run_joins(&ctx);
    }
    update_costs(&config, &corpus, (&engine, &mut baseline));
}
