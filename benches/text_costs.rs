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

fn run_query(ctx: &RunCtx<'_>, query: &str) {
    let bounds = QueryBounds {
        terms: 4,
        postings: 2_000_000,
        candidates: 1_000_000,
        bytes: 128 << 20,
    };
    let searcher = ctx.baseline.reader.searcher();
    let parser = QueryParser::for_index(&ctx.baseline.index, vec![ctx.baseline.fields.text]);
    let allows = |graph: &str| graph_allowed(graph, ctx.config.access);
    let mut oracle = Vec::new();
    for sample in 0..ctx.config.samples {
        let started = Instant::now();
        let public = ctx
            .node
            .search(
                ctx.reader,
                LocalSearch {
                    query,
                    limit: ctx.config.limit,
                },
            )
            .unwrap();
        let public_ns = started.elapsed().as_nanos();
        validate_public(&public, ctx.config);
        let started = Instant::now();
        let hydrated = ctx.node.hydrate_search_hits(ctx.reader, &public).unwrap();
        let hydrate_ns = started.elapsed().as_nanos();
        assert_hydrated(&public, &hydrated);
        let started = Instant::now();
        let resources = ctx
            .node
            .search_resources(
                ctx.reader,
                LocalSearch {
                    query,
                    limit: ctx.config.limit,
                },
            )
            .unwrap();
        let resources_ns = started.elapsed().as_nanos();
        assert_eq!(public_ids(&public), hydrated_ids(&resources));
        let started = Instant::now();
        let parsed = parser.parse_query(query).unwrap();
        let parse_ns = started.elapsed().as_nanos();
        let started = Instant::now();
        let metadata = load_meta(&searcher).unwrap();
        let metadata_ns = started.elapsed().as_nanos();
        let candidate = |address| candidate_key(&metadata, address, ctx.config.access);
        let identity = |address| decode_identity(&metadata, address);
        let generation = ctx.engine.generation().unwrap();
        let full = || {
            text::collect_full(text::TantivyRequest {
                searcher: &searcher,
                query: parsed.as_ref(),
                statistics: ctx.stats,
                candidate: &candidate,
                identity: &identity,
                generation,
                limit: ctx.config.limit,
                bounds,
            })
        };
        let pruned = || {
            text::collect_pruned(text::TantivyRequest {
                searcher: &searcher,
                query: parsed.as_ref(),
                statistics: ctx.stats,
                candidate: &candidate,
                identity: &identity,
                generation,
                limit: ctx.config.limit,
                bounds,
            })
        };
        let fjall = || {
            ctx.engine.search(FjallSearch {
                query,
                limit: ctx.config.limit,
                bounds,
                allows: Some(&allows),
            })
        };
        let (a, b, c) = match sample % 3 {
            0 => (timed(full), timed(pruned), timed(fjall)),
            1 => {
                let b = timed(pruned);
                let c = timed(fjall);
                let a = timed(full);
                (a, b, c)
            }
            _ => {
                let c = timed(fjall);
                let a = timed(full);
                let b = timed(pruned);
                (a, b, c)
            }
        };
        let (full, full_ns) = a;
        let (pruned, pruned_ns) = b;
        let (fjall, fjall_ns) = c;
        assert_reports(&full, &pruned, 0.0);
        assert_reports(&full, &fjall, ctx.config.tolerance);
        let started = Instant::now();
        let mut candidate = ctx
            .engine
            .score_candidates(ScoreRequest {
                query,
                documents: ctx.eligible,
                bounds,
            })
            .unwrap();
        candidate.hits.truncate(ctx.config.limit);
        let candidate_ns = started.elapsed().as_nanos();
        assert_reports(&full, &candidate, ctx.config.tolerance);
        let outer_started = Instant::now();
        let outer_query = parser.parse_query(query).unwrap();
        let outer_meta = load_meta(&searcher).unwrap();
        let outer_candidate = |address| candidate_key(&outer_meta, address, ctx.config.access);
        let outer_identity = |address| decode_identity(&outer_meta, address);
        let outer = text::collect_pruned(text::TantivyRequest {
            searcher: &searcher,
            query: outer_query.as_ref(),
            statistics: ctx.stats,
            candidate: &outer_candidate,
            identity: &outer_identity,
            generation,
            limit: ctx.config.limit,
            bounds,
        })
        .unwrap();
        let outer_ns = outer_started.elapsed().as_nanos();
        assert_reports(&pruned, &outer, 0.0);
        let ids = hit_ids(&full.hits);
        if sample == 0 {
            oracle = ids;
        } else {
            assert_eq!(oracle, ids);
        }
        emit_parity(ctx.config, (query, sample), (&public, &full.hits));
        metric(
            ctx.config,
            (query, sample),
            ("production_hits", public_ns, public.len(), None),
        );
        metric(
            ctx.config,
            (query, sample),
            ("tantivy_parse", parse_ns, 0, None),
        );
        metric(
            ctx.config,
            (query, sample),
            ("hydrate_only", hydrate_ns, hydrated.len(), None),
        );
        total_metric(
            ctx.config,
            (query, sample),
            (
                "tantivy_full_total",
                parse_ns.saturating_add(metadata_ns).saturating_add(full_ns),
            ),
        );
        total_metric(
            ctx.config,
            (query, sample),
            (
                "tantivy_pruned_total",
                parse_ns
                    .saturating_add(metadata_ns)
                    .saturating_add(pruned_ns),
            ),
        );
        outer_metric(ctx.config, (query, sample), (outer_ns, &outer));
        metric(
            ctx.config,
            (query, sample),
            ("search_resources", resources_ns, resources.len(), None),
        );
        metric(
            ctx.config,
            (query, sample),
            ("tantivy_identity_prepare", metadata_ns, 0, None),
        );
        metric(
            ctx.config,
            (query, sample),
            ("tantivy_full", full_ns, full.hits.len(), Some(full.work)),
        );
        metric(
            ctx.config,
            (query, sample),
            (
                "tantivy_pruned",
                pruned_ns,
                pruned.hits.len(),
                Some(pruned.work),
            ),
        );
        metric(
            ctx.config,
            (query, sample),
            (
                "fjall_posting_topk",
                fjall_ns,
                fjall.hits.len(),
                Some(fjall.work),
            ),
        );
        metric(
            ctx.config,
            (query, sample),
            (
                "fjall_candidate_score",
                candidate_ns,
                candidate.hits.len(),
                Some(candidate.work),
            ),
        );
    }
    #[cfg(feature = "allocation-metrics")]
    allocation_query(ctx, query);
}

#[cfg(feature = "allocation-metrics")]
fn allocation_query(ctx: &RunCtx<'_>, query: &str) {
    let bounds = QueryBounds {
        terms: 4,
        postings: 2_000_000,
        candidates: 1_000_000,
        bytes: 128 << 20,
    };
    let allows = |graph: &str| graph_allowed(graph, ctx.config.access);

    let interval = AllocationInterval::begin();
    let public = ctx
        .node
        .search(
            ctx.reader,
            LocalSearch {
                query,
                limit: ctx.config.limit,
            },
        )
        .unwrap();
    let sample = interval.finish();
    validate_public(&public, ctx.config);
    emit_alloc(
        ctx.config,
        (query, "production_hits"),
        (public.len(), sample),
    );

    let interval = AllocationInterval::begin();
    let hydrated = ctx.node.hydrate_search_hits(ctx.reader, &public).unwrap();
    let sample = interval.finish();
    assert_hydrated(&public, &hydrated);
    emit_alloc(
        ctx.config,
        (query, "hydrate_only"),
        (hydrated.len(), sample),
    );

    let searcher = ctx.baseline.reader.searcher();
    let parser = QueryParser::for_index(&ctx.baseline.index, vec![ctx.baseline.fields.text]);
    let generation = ctx.engine.generation().unwrap();
    let interval = AllocationInterval::begin();
    let parsed = parser.parse_query(query).unwrap();
    let metadata = load_meta(&searcher).unwrap();
    let candidate = |address| candidate_key(&metadata, address, ctx.config.access);
    let identity = |address| decode_identity(&metadata, address);
    let corrected = text::collect_pruned(text::TantivyRequest {
        searcher: &searcher,
        query: parsed.as_ref(),
        statistics: ctx.stats,
        candidate: &candidate,
        identity: &identity,
        generation,
        limit: ctx.config.limit,
        bounds,
    })
    .unwrap();
    let sample = interval.finish();
    emit_alloc(
        ctx.config,
        (query, "tantivy_pruned_e2e"),
        (corrected.hits.len(), sample),
    );

    let interval = AllocationInterval::begin();
    let fjall = ctx
        .engine
        .search(FjallSearch {
            query,
            limit: ctx.config.limit,
            bounds,
            allows: Some(&allows),
        })
        .unwrap();
    let sample = interval.finish();
    assert_reports(&corrected, &fjall, ctx.config.tolerance);
    emit_alloc(
        ctx.config,
        (query, "fjall_posting_topk"),
        (fjall.hits.len(), sample),
    );

    let interval = AllocationInterval::begin();
    let mut direct = ctx
        .engine
        .score_candidates(ScoreRequest {
            query,
            documents: ctx.eligible,
            bounds,
        })
        .unwrap();
    direct.hits.truncate(ctx.config.limit);
    let sample = interval.finish();
    assert_reports(&corrected, &direct, ctx.config.tolerance);
    emit_alloc(
        ctx.config,
        (query, "fjall_candidate_score"),
        (direct.hits.len(), sample),
    );
}

#[cfg(feature = "allocation-metrics")]
fn emit_alloc(config: &Config, case: (&str, &str), result: (usize, AllocationSample)) {
    let (query, variant) = case;
    let (hits, sample) = result;
    emit(json!({
        "kind":"allocation", "seed":config.seed, "documents":config.docs,
        "graphs":config.graphs, "permissions":config.access.label(), "query":query,
        "limit":config.limit, "sample":0, "variant":variant, "hits":hits,
        "allocations":sample.allocations, "allocated_bytes":sample.allocated_bytes,
        "peak_delta_bytes":sample.peak_delta_bytes, "counted_as_timing":false,
        "scope":"process-wide interval; background workers may contribute",
        "interpretation":"separate allocation profile only; not timing evidence",
    }));
}

struct JoinCase<'a> {
    label: &'a str,
    query: &'a str,
    sparql: String,
}

struct RdfCandidates {
    keys: Vec<(String, String)>,
    query_ns: u128,
    map_ns: u128,
    rows: usize,
}

fn run_joins(ctx: &RunCtx<'_>) {
    let broad = JoinCase {
        label: "rare_text_broad_rdf",
        query: &ctx.config.rare,
        sparql: "SELECT ?s ?g WHERE { GRAPH ?g { ?s <http://schema.org/identifier> ?id } }"
            .to_string(),
    };
    let selective = JoinCase {
        label: "common_text_selective_rdf",
        query: &ctx.config.common,
        sparql: "SELECT ?s ?g WHERE { GRAPH ?g { ?s <http://schema.org/identifier> \"DOC-000000000000\" } }"
            .to_string(),
    };
    run_join(ctx, &broad);
    run_join(ctx, &selective);
}
