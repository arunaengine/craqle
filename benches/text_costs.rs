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
            || matches!(self, Self::Half) && graph.is_multiple_of(2)
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

fn run_join(ctx: &RunCtx<'_>, case: &JoinCase<'_>) {
    let rdf = rdf_candidates(ctx, &case.sparql);
    let rdf_set: BTreeSet<_> = rdf.keys.iter().cloned().collect();
    assert_eq!(rdf.rows, rdf_set.len(), "RDF candidates must be a set");
    let rdf_keys = rdf
        .keys
        .iter()
        .map(|(graph, subject)| DocumentKey { graph, subject })
        .collect::<Vec<_>>();
    let stable: BTreeSet<_> = rdf_keys
        .iter()
        .map(|key| text::stable_key(key.graph, key.subject))
        .collect();
    let bounds = QueryBounds {
        terms: 4,
        postings: 2_000_000,
        candidates: 1_000_000,
        bytes: 128 << 20,
    };
    let started = Instant::now();
    let searcher = ctx.baseline.reader.searcher();
    let metadata = load_meta(&searcher).unwrap();
    let parser = QueryParser::for_index(&ctx.baseline.index, vec![ctx.baseline.fields.text]);
    let query = parser.parse_query(case.query).unwrap();
    let prepare_ns = started.elapsed().as_nanos();
    let auth_candidate = |address| candidate_key(&metadata, address, ctx.config.access);
    let identity = |address| decode_identity(&metadata, address);
    let generation = ctx.engine.generation().unwrap();

    let started = Instant::now();
    let mut reference = text::collect_full(text::TantivyRequest {
        searcher: &searcher,
        query: query.as_ref(),
        statistics: ctx.stats,
        candidate: &auth_candidate,
        identity: &identity,
        generation,
        limit: ctx.config.docs.saturating_add(ctx.config.graphs * 2),
        bounds,
    })
    .unwrap();
    reference
        .hits
        .retain(|hit| rdf_set.contains(&(hit.graph.clone(), hit.subject.clone())));
    reference.hits.truncate(ctx.config.limit);
    let reference_ns = started.elapsed().as_nanos();

    let started = Instant::now();
    let rdf_first = joins::score_candidates(joins::JoinRequest {
        searcher: &searcher,
        query: query.as_ref(),
        statistics: ctx.stats,
        key_field: ctx.baseline.fields.key,
        candidates: &rdf_keys,
        generation,
        limit: ctx.config.limit,
        bounds,
    })
    .unwrap();
    let rdf_first_ns = started.elapsed().as_nanos();

    let combined_candidate = |address| {
        Ok::<_, text::EngineError>(
            candidate_key(&metadata, address, ctx.config.access)?
                .filter(|stable_key| stable.contains(stable_key)),
        )
    };
    let started = Instant::now();
    let combined = text::collect_pruned(text::TantivyRequest {
        searcher: &searcher,
        query: query.as_ref(),
        statistics: ctx.stats,
        candidate: &combined_candidate,
        identity: &identity,
        generation,
        limit: ctx.config.limit,
        bounds,
    })
    .unwrap();
    let combined_ns = started.elapsed().as_nanos();

    let started = Instant::now();
    let mut fjall = ctx
        .engine
        .score_candidates(ScoreRequest {
            query: case.query,
            documents: &rdf_keys,
            bounds,
        })
        .unwrap();
    fjall.hits.truncate(ctx.config.limit);
    let fjall_ns = started.elapsed().as_nanos();
    assert_reports(&reference, &rdf_first, 0.0);
    assert_reports(&reference, &combined, 0.0);
    assert_reports(&reference, &fjall, ctx.config.tolerance);
    for (variant, score_ns, report) in [
        ("text_first_exhaustive", reference_ns, &reference),
        ("rdf_first_tantivy", rdf_first_ns, &rdf_first),
        ("combined_pruned", combined_ns, &combined),
        ("rdf_first_fjall", fjall_ns, &fjall),
    ] {
        emit_join(
            ctx,
            case,
            JoinMetric {
                rdf: &rdf,
                variant,
                prepare_ns,
                score_ns,
                report,
            },
        );
    }
}

fn rdf_candidates(ctx: &RunCtx<'_>, sparql: &str) -> RdfCandidates {
    let started = Instant::now();
    let result = ctx.node.query(ctx.reader, sparql).unwrap();
    let query_ns = started.elapsed().as_nanos();
    let QueryResults::Solutions(rows) = result else {
        panic!("RDF candidate query must return solutions");
    };
    let row_count = rows.len();
    let started = Instant::now();
    let keys = rows
        .into_iter()
        .map(|row| {
            let graph = row.get("g").and_then(|term| term.to_named_node()).unwrap();
            let subject = row.get("s").and_then(|term| term.to_named_node()).unwrap();
            (graph.as_str().to_string(), subject.as_str().to_string())
        })
        .collect();
    RdfCandidates {
        keys,
        query_ns,
        map_ns: started.elapsed().as_nanos(),
        rows: row_count,
    }
}

struct JoinMetric<'a> {
    rdf: &'a RdfCandidates,
    variant: &'a str,
    prepare_ns: u128,
    score_ns: u128,
    report: &'a text::SearchReport,
}

fn emit_join(ctx: &RunCtx<'_>, case: &JoinCase<'_>, metric: JoinMetric<'_>) {
    let JoinMetric {
        rdf,
        variant,
        prepare_ns,
        score_ns,
        report,
    } = metric;
    emit(json!({
        "kind":"rdf_text_join", "scenario":case.label, "variant":variant,
        "query":case.query, "permissions":ctx.config.access.label(),
        "rdf_rows":rdf.rows, "rdf_query_ns":rdf.query_ns, "rdf_map_ns":rdf.map_ns,
        "text_prepare_ns":prepare_ns, "text_score_ns":score_ns,
        "outer_ns":rdf.query_ns.saturating_add(rdf.map_ns).saturating_add(prepare_ns).saturating_add(score_ns),
        "outer_composed_from_nonoverlapping_spans":true,
        "hits":report.hits.len(),
        "work":{"pruning_fallback":report.work.pruning_fallback,
            "posting_rows":report.work.posting_rows,"candidate_docs":report.work.candidate_docs,
            "scored_docs":report.work.scored_docs,"rejected_docs":report.work.rejected_docs,
            "metadata_reads":report.work.metadata_reads,"final_reads":report.work.final_reads,
            "deduplicated_docs":report.work.deduplicated_docs,"bytes":report.work.bytes},
        "semantics":"Top K text matches satisfying the RDF set; distinct from public SERVICE pre-join limits"
    }));
}

fn update_costs(config: &Config, corpus: &Corpus, targets: (&FjallBm25, &mut Baseline)) {
    let (engine, baseline) = targets;
    let doc = corpus.docs.iter().find(|doc| doc.entity.is_some()).unwrap();
    let changed = format!("{} updateprobe", doc.body);
    let io = io_bytes();
    let started = Instant::now();
    let work = engine
        .upsert(DocumentInput {
            graph: &doc.graph,
            subject: &doc.subject,
            text: &changed,
        })
        .unwrap();
    engine.persist(fjall::PersistMode::SyncAll).unwrap();
    emit_update(
        "fjall_upsert",
        (started.elapsed().as_nanos(), io_delta(io)),
        update_json(work),
    );
    let io = io_bytes();
    let started = Instant::now();
    let work = engine.delete(key(doc)).unwrap();
    engine.persist(fjall::PersistMode::SyncAll).unwrap();
    emit_update(
        "fjall_delete",
        (started.elapsed().as_nanos(), io_delta(io)),
        update_json(work),
    );
    let io = io_bytes();
    let started = Instant::now();
    let work = engine.upsert(input(doc)).unwrap();
    engine.persist(fjall::PersistMode::SyncAll).unwrap();
    emit_update(
        "fjall_reinsert",
        (started.elapsed().as_nanos(), io_delta(io)),
        update_json(work),
    );
    let io = io_bytes();
    let started = Instant::now();
    replace_tantivy(baseline, doc, Some(&changed));
    emit_update(
        "tantivy_upsert",
        (started.elapsed().as_nanos(), io_delta(io)),
        json!({"documents":1,"input_bytes":changed.len()}),
    );
    let io = io_bytes();
    let started = Instant::now();
    replace_tantivy(baseline, doc, None);
    emit_update(
        "tantivy_delete",
        (started.elapsed().as_nanos(), io_delta(io)),
        json!({"documents":1,"input_bytes":0}),
    );
    let io = io_bytes();
    let started = Instant::now();
    add_tantivy(&mut baseline.writer, &baseline.fields, (doc, &doc.body));
    commit_tantivy(baseline);
    emit_update(
        "tantivy_reinsert",
        (started.elapsed().as_nanos(), io_delta(io)),
        json!({"documents":1,"input_bytes":doc.body.len()}),
    );
    let stats = engine
        .load_stats(StatsRequest {
            field: baseline.fields.text,
            max_terms: 1_000_000,
            max_bytes: 64 << 20,
        })
        .unwrap();
    let searcher = baseline.reader.searcher();
    let metadata = load_meta(&searcher).unwrap();
    let parser = QueryParser::for_index(&baseline.index, vec![baseline.fields.text]);
    let query = parser.parse_query(&config.common).unwrap();
    let allows = |_: &str| true;
    let candidate = |address| candidate_all(&metadata, address);
    let identity = |address| decode_identity(&metadata, address);
    let bounds = QueryBounds {
        terms: 4,
        postings: 2_000_000,
        candidates: 1_000_000,
        bytes: 128 << 20,
    };
    let left = text::collect_full(text::TantivyRequest {
        searcher: &searcher,
        query: query.as_ref(),
        statistics: &stats,
        candidate: &candidate,
        identity: &identity,
        generation: engine.generation().unwrap(),
        limit: config.limit,
        bounds,
    })
    .unwrap();
    let right = engine
        .search(FjallSearch {
            query: &config.common,
            limit: config.limit,
            bounds,
            allows: Some(&allows),
        })
        .unwrap();
    assert_reports(&left, &right, config.tolerance);
}

fn build_corpus(config: &Config) -> Corpus {
    let graphs = (0..config.graphs)
        .map(|i| GraphId::new(&format!("urn:text:graph:{i:06}")))
        .collect::<Vec<_>>();
    let mut docs = Vec::with_capacity(config.docs + config.graphs * 2);
    for (i, graph) in graphs.iter().enumerate() {
        let name = format!("Text Graph {i:06}");
        let desc = "Deterministic text benchmark graph";
        docs.push(PreparedDoc {
            graph_index: i,
            graph: graph.as_str().to_string(),
            subject: graph.as_str().to_string(),
            body: format!("{name} {desc}"),
            entity: None,
        });
        docs.push(PreparedDoc {
            graph_index: i,
            graph: graph.as_str().to_string(),
            subject: "ro-crate-metadata.json".to_string(),
            body: String::new(),
            entity: None,
        });
    }
    let mut state = config.seed;
    for i in 0..config.docs {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let gi = i % config.graphs;
        let graph = graphs[gi].as_str().to_string();
        let subject = format!("urn:text:doc:{i:012}");
        let name = format!("Document {i:012}");
        let desc = format!("deterministic benchmark record {i:012}");
        let ident = format!("DOC-{i:012}");
        let mut terms = vec![format!("bucket{:04}", state % 127)];
        if state.is_multiple_of(config.common_mod) || i == 0 {
            terms.push(config.common.clone());
        }
        if state.is_multiple_of(config.rare_mod) || i == 1 {
            terms.push(config.rare.clone());
        }
        if i < config.graphs * 2 {
            terms.push(TIE.to_string());
        }
        let keywords = terms.join(" ");
        let body = format!("{name} {desc} {keywords} {ident}");
        let entity = NewDataEntity {
            entity_id: subject.clone(),
            entity_type: "http://schema.org/MediaObject".to_string(),
            name,
            additional_triples: vec![
                literal("description", &desc),
                literal("keywords", &keywords),
                literal("identifier", &ident),
            ],
        };
        docs.push(PreparedDoc {
            graph_index: gi,
            graph,
            subject,
            body,
            entity: Some(entity),
        });
    }
    Corpus { docs, graphs }
}

fn build_local(corpus: &Corpus) -> LocalFixture {
    let root = tempfile::tempdir().unwrap();
    let node = CraqleNode::open_with_options(
        root.path(),
        craqle::CraqleOptions::new().with_search_storage(SearchStorage::Disk),
    )
    .unwrap();
    let writer = GrantAuthorizer::new(vec![PermissionGrant::new(
        "/bench/**",
        PermissionLevel::Write,
    )]);
    for (i, graph) in corpus.graphs.iter().enumerate() {
        node.create_crate(
            &writer,
            CreateCrateRequest::new(
                graph.clone(),
                format!("Text Graph {i:06}"),
                "Deterministic text benchmark graph",
                "2026-01-01",
                None,
                GraphPolicy {
                    public: false,
                    permission_paths: vec![format!("/bench/g/{i}")],
                },
            ),
        )
        .unwrap();
        let entities = corpus
            .docs
            .iter()
            .filter(|doc| doc.graph_index == i)
            .filter_map(|doc| doc.entity.clone())
            .collect();
        node.append_new_root_data_entities(&writer, graph, entities)
            .unwrap();
    }
    node.flush_search_updates().unwrap();
    LocalFixture { root, node }
}

fn build_tantivy(corpus: &Corpus) -> Baseline {
    let root = tempfile::tempdir().unwrap();
    let mut schema = SchemaBuilder::default();
    let fields = Fields {
        key: schema.add_text_field("key", STRING),
        graph: schema.add_bytes_field("graph", FAST),
        subject: schema.add_bytes_field("subject", FAST),
        graph_ord: schema.add_u64_field("graph_ord", FAST),
        stable: schema.add_bytes_field("stable", FAST),
        text: schema.add_text_field(
            "all_text",
            TEXT.set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(ANALYZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        ),
    };
    let index = Index::create_in_dir(root.path(), schema.build()).unwrap();
    index.tokenizers().register(ANALYZER, analyzer());
    let mut writer = index.writer_with_num_threads(1, 64 << 20).unwrap();
    for doc in &corpus.docs {
        add_tantivy(&mut writer, &fields, (doc, &doc.body));
    }
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    reader.reload().unwrap();
    Baseline {
        root,
        index,
        writer,
        reader,
        fields,
    }
}

fn add_tantivy(writer: &mut IndexWriter, fields: &Fields, input: (&PreparedDoc, &str)) {
    let (doc, body) = input;
    let text = full_text(doc, body);
    let mut value = TantivyDocument::default();
    value.add_text(fields.key, doc_key(doc));
    value.add_bytes(fields.graph, doc.graph.as_bytes());
    value.add_bytes(fields.subject, doc.subject.as_bytes());
    value.add_bytes(fields.stable, &text::stable_key(&doc.graph, &doc.subject));
    value.add_u64(fields.graph_ord, doc.graph_index as u64);
    value.add_text(fields.text, text);
    writer.add_document(value).unwrap();
}
fn full_text(doc: &PreparedDoc, body: &str) -> String {
    format!("{} {} {}", doc.graph, doc.subject, body)
}
fn engine_text(doc: &PreparedDoc, body: &str) -> String {
    let mut text = String::with_capacity(doc.graph.len() + doc.subject.len() + body.len() + 2);
    text.push_str(&doc.graph);
    text.push(' ');
    text.push_str(&doc.subject);
    text.push(' ');
    text.push_str(body);
    text
}
fn analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build()
}
fn analyzed(text: &str) -> Vec<String> {
    let mut analyzer = analyzer();
    let mut tokens = Vec::new();
    analyzer
        .token_stream(text)
        .process(&mut |token| tokens.push(token.text.clone()));
    tokens
}
fn assert_token_parity(corpus: &Corpus) {
    for doc in &corpus.docs {
        assert_eq!(
            analyzed(&full_text(doc, &doc.body)),
            analyzed(&engine_text(doc, &doc.body)),
            "prepared text mismatch for {} {}",
            doc.graph,
            doc.subject
        );
    }
}
fn replace_tantivy(base: &mut Baseline, doc: &PreparedDoc, text: Option<&str>) {
    base.writer
        .delete_term(IndexTerm::from_field_text(base.fields.key, &doc_key(doc)));
    if let Some(text) = text {
        add_tantivy(&mut base.writer, &base.fields, (doc, text));
    }
    commit_tantivy(base);
}
fn commit_tantivy(base: &mut Baseline) {
    base.writer.commit().unwrap();
    base.reader.reload().unwrap();
}

fn load_meta(searcher: &tantivy::Searcher) -> Result<Vec<SegmentMeta>, text::EngineError> {
    searcher
        .segment_readers()
        .iter()
        .map(|reader| {
            Ok(SegmentMeta {
                graph: reader
                    .fast_fields()
                    .bytes("graph")?
                    .ok_or(text::EngineError::Corrupt("graph fast field"))?,
                subject: reader
                    .fast_fields()
                    .bytes("subject")?
                    .ok_or(text::EngineError::Corrupt("subject fast field"))?,
                stable: reader
                    .fast_fields()
                    .bytes("stable")?
                    .ok_or(text::EngineError::Corrupt("stable fast field"))?,
                graph_ord: reader.fast_fields().u64("graph_ord")?,
            })
        })
        .collect()
}
fn candidate_key(
    metadata: &[SegmentMeta],
    address: DocAddress,
    access: Access,
) -> Result<Option<[u8; 32]>, text::EngineError> {
    let meta = &metadata[address.segment_ord as usize];
    if !meta
        .graph_ord
        .first(address.doc_id)
        .is_some_and(|i| usize::try_from(i).is_ok_and(|i| access.allows(i)))
    {
        return Ok(None);
    }
    stable_key(meta, address.doc_id)
}
fn candidate_all(
    metadata: &[SegmentMeta],
    address: DocAddress,
) -> Result<Option<[u8; 32]>, text::EngineError> {
    stable_key(&metadata[address.segment_ord as usize], address.doc_id)
}
fn stable_key(meta: &SegmentMeta, doc: u32) -> Result<Option<[u8; 32]>, text::EngineError> {
    let Some(stable) = first_bytes(Some(&meta.stable), doc) else {
        return Ok(None);
    };
    let stable = stable
        .try_into()
        .map_err(|_| text::EngineError::Corrupt("stable key length"))?;
    Ok(Some(stable))
}
fn decode_identity(
    metadata: &[SegmentMeta],
    address: DocAddress,
) -> Result<HitIdentity, text::EngineError> {
    identity_fast(&metadata[address.segment_ord as usize], address.doc_id)?
        .ok_or(text::EngineError::Corrupt("tantivy identity missing"))
}
fn identity_fast(meta: &SegmentMeta, doc: u32) -> Result<Option<HitIdentity>, text::EngineError> {
    let graph = first_bytes(Some(&meta.graph), doc);
    let subject = first_bytes(Some(&meta.subject), doc);
    match (graph, subject) {
        (Some(graph), Some(subject)) => Ok(Some(HitIdentity {
            graph: String::from_utf8(graph)
                .map_err(|_| text::EngineError::Corrupt("graph utf8"))?,
            subject: String::from_utf8(subject)
                .map_err(|_| text::EngineError::Corrupt("subject utf8"))?,
        })),
        _ => Ok(None),
    }
}
fn first_bytes(column: Option<&tantivy::columnar::BytesColumn>, doc: u32) -> Option<Vec<u8>> {
    let column = column?;
    let ord = column.term_ords(doc).next()?;
    let mut bytes = Vec::new();
    column
        .ord_to_bytes(ord, &mut bytes)
        .ok()
        .and_then(|found| found.then_some(bytes))
}

fn timed<F>(run: F) -> (text::SearchReport, u128)
where
    F: FnOnce() -> Result<text::SearchReport, text::EngineError>,
{
    let started = Instant::now();
    let report = run().unwrap();
    (report, started.elapsed().as_nanos())
}
fn assert_reports(left: &text::SearchReport, right: &text::SearchReport, tolerance: f32) {
    assert_eq!(hit_ids(&left.hits), hit_ids(&right.hits));
    for (left, right) in left.hits.iter().zip(&right.hits) {
        assert!((left.score - right.score).abs() <= tolerance);
    }
}
fn validate_public(hits: &[craqle::SearchHit], config: &Config) {
    assert!(hits.len() <= config.limit);
    let mut seen = BTreeSet::new();
    for hit in hits {
        assert!(graph_allowed(&hit.graph_id, config.access));
        assert!(seen.insert((&hit.graph_id, &hit.subject_iri)));
    }
}
fn assert_hydrated(raw: &[craqle::SearchHit], full: &[craqle::HydratedSearchHit]) {
    assert_eq!(raw.len(), full.len());
    for (raw, full) in raw.iter().zip(full) {
        assert_eq!(
            (&raw.graph_id, &raw.subject_iri, raw.score.to_bits()),
            (
                &full.hit.graph_id,
                &full.hit.subject_iri,
                full.hit.score.to_bits()
            )
        );
    }
}
fn emit_parity(config: &Config, case: (&str, usize), reports: (&[craqle::SearchHit], &[BenchHit])) {
    let (query, sample) = case;
    let (public, corrected) = reports;
    let p = public
        .iter()
        .map(|h| (h.graph_id.clone(), h.subject_iri.clone()))
        .collect::<Vec<_>>();
    let c = hit_ids(corrected);
    let ps: BTreeSet<_> = p.iter().cloned().collect();
    let cs: BTreeSet<_> = c.iter().cloned().collect();
    let mut delta = 0.0f32;
    for hit in public {
        if let Some(other) = corrected
            .iter()
            .find(|o| o.graph == hit.graph_id && o.subject == hit.subject_iri)
        {
            delta = delta.max((hit.score - other.score).abs());
        }
    }
    emit(
        json!({"kind":"public_corrected_parity","query":query,"sample":sample,"identity_order_equal":p==c,
        "identity_overlap":ps.intersection(&cs).count(),"public_hits":p.len(),"corrected_hits":c.len(),
        "max_shared_score_delta":delta,"permissions":config.access.label(),
        "interpretation":"potential scoring-population or document-text differences; public scores are not an equivalent corrected-corpus comparison"}),
    );
}
fn metric(config: &Config, case: (&str, usize), m: (&str, u128, usize, Option<WorkCount>)) {
    let (query, sample) = case;
    let (variant, wall, hits, work) = m;
    emit(
        json!({"kind":"query","seed":config.seed,"documents":config.docs,
        "graphs":config.graphs,"permissions":config.access.label(),"query":query,"limit":config.limit,
        "sample":sample,"rotation":sample%3,"variant":variant,"wall_ns":wall,"hits":hits,
        "work":work.map(|w|json!({"pruning_fallback":w.pruning_fallback,
            "posting_rows":w.posting_rows,"candidate_docs":w.candidate_docs,
            "scored_docs":w.scored_docs,"rejected_docs":w.rejected_docs,"metadata_reads":w.metadata_reads,
            "final_reads":w.final_reads,"deduplicated_docs":w.deduplicated_docs,"bytes":w.bytes}))}),
    );
}

fn total_metric(config: &Config, case: (&str, usize), timing: (&str, u128)) {
    let (query, sample) = case;
    emit(
        json!({"kind":"query_total","seed":config.seed,"documents":config.docs,
        "graphs":config.graphs,"permissions":config.access.label(),"query":query,
        "limit":config.limit,"sample":sample,"variant":timing.0,"wall_ns":timing.1,
        "timing":"composed from nonoverlapping parse, metadata, and collection spans"}),
    );
}

fn outer_metric(config: &Config, case: (&str, usize), result: (u128, &text::SearchReport)) {
    let (query, sample) = case;
    let (wall_ns, report) = result;
    emit(
        json!({"kind":"query_total","seed":config.seed,"documents":config.docs,
        "graphs":config.graphs,"permissions":config.access.label(),"query":query,
        "limit":config.limit,"sample":sample,"variant":"tantivy_pruned_e2e",
        "wall_ns":wall_ns,"timing":"actual outer parse plus metadata plus collection",
        "hits":report.hits.len(),"work":{"pruning_fallback":report.work.pruning_fallback,
            "posting_rows":report.work.posting_rows,
            "candidate_docs":report.work.candidate_docs,"scored_docs":report.work.scored_docs,
            "rejected_docs":report.work.rejected_docs,"metadata_reads":report.work.metadata_reads,
            "final_reads":report.work.final_reads,"deduplicated_docs":report.work.deduplicated_docs,
            "bytes":report.work.bytes}}),
    );
}

fn reader_auth(access: Access, graphs: usize) -> GrantAuthorizer {
    GrantAuthorizer::new(
        (0..graphs)
            .filter(|i| access.allows(*i))
            .map(|i| PermissionGrant::new(format!("/bench/g/{i}"), PermissionLevel::Read))
            .collect(),
    )
}
fn graph_allowed(graph: &str, access: Access) -> bool {
    graph
        .rsplit(':')
        .next()
        .and_then(|v| v.parse().ok())
        .is_some_and(|i| access.allows(i))
}
fn literal(predicate: &str, value: &str) -> (NamedNode, Term) {
    (
        NamedNode::new_unchecked(format!("http://schema.org/{predicate}")),
        Term::Literal(oxrdf::Literal::new_simple_literal(value)),
    )
}
fn input(doc: &PreparedDoc) -> DocumentInput<'_> {
    DocumentInput {
        graph: &doc.graph,
        subject: &doc.subject,
        text: &doc.body,
    }
}
fn key(doc: &PreparedDoc) -> DocumentKey<'_> {
    DocumentKey {
        graph: &doc.graph,
        subject: &doc.subject,
    }
}
fn doc_key(doc: &PreparedDoc) -> String {
    format!("{}\u{1f}{}", doc.graph, doc.subject)
}
fn hit_ids(hits: &[BenchHit]) -> Vec<(String, String)> {
    hits.iter()
        .map(|h| (h.graph.clone(), h.subject.clone()))
        .collect()
}
fn public_ids(hits: &[craqle::SearchHit]) -> Vec<(String, String)> {
    hits.iter()
        .map(|h| (h.graph_id.clone(), h.subject_iri.clone()))
        .collect()
}
fn hydrated_ids(hits: &[craqle::HydratedSearchHit]) -> Vec<(String, String)> {
    hits.iter()
        .map(|h| (h.hit.graph_id.clone(), h.hit.subject_iri.clone()))
        .collect()
}
fn update_json(work: text::UpdateWork) -> serde_json::Value {
    json!({"generation":work.generation,"terms":work.terms,"postings":work.postings,"bytes":work.bytes})
}
fn emit_update(variant: &str, timing: (u128, Option<u64>), work: serde_json::Value) {
    emit(
        json!({"kind":"update","variant":variant,"wall_ns":timing.0,"write_bytes":timing.1,"work":work}),
    );
}
fn emit(value: serde_json::Value) {
    println!("{value}");
}
fn io_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/io").ok().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("write_bytes: ")?.parse().ok())
    })
}
fn io_delta(before: Option<u64>) -> Option<u64> {
    before
        .zip(io_bytes())
        .map(|(before, after)| after.saturating_sub(before))
}
fn dir_bytes(path: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
            } else {
                total += entry.metadata().unwrap().len();
            }
        }
    }
    total
}

#[cfg(target_os = "linux")]
fn dir_blocks(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let mut blocks = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let metadata = entry.metadata().ok()?;
            blocks = blocks.saturating_add(metadata.blocks());
            if metadata.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Some(blocks.saturating_mul(512))
}

#[cfg(not(target_os = "linux"))]
fn dir_blocks(_: &Path) -> Option<u64> {
    None
}

fn read_config() -> Config {
    Config {
        seed: env("CRAQLE_TEXT_SEED", 7),
        docs: env("CRAQLE_TEXT_DOCS", 200),
        graphs: env("CRAQLE_TEXT_GRAPHS", 8),
        limit: env("CRAQLE_TEXT_K", 10),
        samples: env("CRAQLE_TEXT_SAMPLES", 3),
        common: std::env::var("CRAQLE_TEXT_COMMON_TERM").unwrap_or_else(|_| COMMON.to_string()),
        rare: std::env::var("CRAQLE_TEXT_RARE_TERM").unwrap_or_else(|_| RARE.to_string()),
        common_mod: env::<u64>("CRAQLE_TEXT_COMMON_MOD", 2).max(1),
        rare_mod: env::<u64>("CRAQLE_TEXT_RARE_MOD", 97).max(1),
        tolerance: env("CRAQLE_TEXT_SCORE_TOLERANCE", 0.00001),
        access: match std::env::var("CRAQLE_TEXT_PERMISSIONS")
            .as_deref()
            .unwrap_or("all")
        {
            "all" => Access::All,
            "half" => Access::Half,
            "one" => Access::One,
            other => panic!("unknown permission profile {other}"),
        },
        layout: match std::env::var("CRAQLE_TEXT_LAYOUT") {
            Ok(v) if v != "simple" => PostingLayout::Block {
                docs: v
                    .strip_prefix("block:")
                    .and_then(|v| v.parse().ok())
                    .expect("layout must be simple or block:<docs>"),
            },
            _ => PostingLayout::Simple,
        },
    }
}
fn env<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("invalid numeric environment {key}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("non-Unicode numeric environment {key}")
        }
    }
}
