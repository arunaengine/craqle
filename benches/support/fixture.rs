//! Bounded, deterministic benchmark fixture construction for SPARQL reads.
//!
//! The corpus iterator itself stays numeric and streaming. This module owns
//! the temporary database and retains only graph metadata plus a handful of
//! stable query terms after setup.

use std::env;
use std::fs;
use std::path::Path;
use std::time::Duration;

use craqle::{
    ActorId, CraqleNode, CraqleOptions, EncodedTerm, GraphId, MaterializedQuadChange, QueryResults,
};
use oxrdf::Term;

use super::{
    CORPUS_VERSION, CorpusConfig, CorpusShape, DEFAULT_SEED, DeterministicCorpus, GRAPHS_32,
    GraphVisibility, ObjectSpec, PredicateKind, QUADS_10K, QUADS_10M, graph_visibility,
};

/// Maximum buffered changes across every graph partition. A push that reaches
/// this cap flushes its graph-scoped partition before another record is read.
pub const LOAD_BATCH_SIZE: usize = 512;

const SELECT_LIMIT: usize = 10;
const DIRECTORY_SIZE_ENTRY_LIMIT: usize = 100_000;
const CRAQLE_COMMIT: &str = match option_env!("CRAQLE_GIT_COMMIT") {
    Some(commit) => commit,
    None => "unknown",
};

#[derive(Debug, Clone, Copy)]
pub struct BenchConfig {
    pub corpus: CorpusConfig,
    pub warm_up: Duration,
    pub measurement: Duration,
}

impl BenchConfig {
    pub fn from_environment() -> Self {
        let quads = env_usize("CRAQLE_BENCH_QUADS", QUADS_10K);
        let graphs = env_usize("CRAQLE_BENCH_GRAPHS", GRAPHS_32);
        let duplicate_percent = env_u8("CRAQLE_BENCH_DUPLICATE_PERCENT", 25);
        let corpus = CorpusConfig::new(quads, graphs, duplicate_percent, DEFAULT_SEED)
            .unwrap_or_else(|error| {
                panic!(
                    "invalid CRAQLE benchmark corpus configuration \
                     (CRAQLE_BENCH_QUADS/GRAPHS/DUPLICATE_PERCENT): {error}"
                )
            });

        if quads == QUADS_10M {
            eprintln!(
                "sparql_hot_path: 10M corpus explicitly selected with CRAQLE_BENCH_QUADS; \
                 setup may take substantial time"
            );
        }

        Self {
            corpus,
            warm_up: env_duration("CRAQLE_BENCH_WARMUP_SECS", 1),
            measurement: env_duration("CRAQLE_BENCH_MEASUREMENT_SECS", 5),
        }
    }
}

pub struct Fixture {
    node: CraqleNode,
    // Drop the node before removing the directory it has open handles into.
    _database: tempfile::TempDir,
    config: BenchConfig,
    all_graphs: Vec<GraphId>,
    visible_graphs: Vec<GraphId>,
    terms: QueryTerms,
    cases: Vec<QueryCase>,
    duplicate_queries: Option<DuplicateQueries>,
    hidden_query: Option<String>,
    metrics: FixtureMetrics,
}

/// Stable terms selected from a complete visible canonical star.
#[derive(Clone)]
pub struct QueryTerms {
    pub graph: GraphId,
    pub subject: EncodedTerm,
    pub common_predicate: EncodedTerm,
    pub common_object: EncodedTerm,
    pub rare_predicate: EncodedTerm,
    pub rare_object: EncodedTerm,
}

struct QueryCase {
    label: &'static str,
    kind: QueryKind,
    sparql: String,
    expected: Expected,
}

#[derive(Clone, Copy)]
enum QueryKind {
    AskHit,
    AskMiss,
    SelectLimit,
    Count,
    PropertyStar,
    RareToCommon,
    CommonToRare,
}

enum Expected {
    Boolean(bool),
    Rows {
        count: usize,
        bindings: &'static [&'static str],
    },
    Count {
        expected: usize,
    },
}

struct DuplicateQueries {
    named: String,
    union: String,
}

#[derive(Clone)]
struct Probe {
    graph: GraphId,
    subject: EncodedTerm,
    predicate: EncodedTerm,
    object: EncodedTerm,
}

struct FixtureMetrics {
    inserted_data_quads: usize,
    setup_term_allocations: usize,
    setup_term_bytes: usize,
    database: DirectoryBytes,
}

struct DirectoryBytes {
    bytes: u64,
    entries: usize,
    complete: bool,
}

#[derive(Default)]
pub struct SemanticReport {
    pub ask_hit: bool,
    pub ask_miss: bool,
    pub select_rows: usize,
    pub count: usize,
    pub property_star_rows: usize,
    pub rare_to_common_rows: usize,
    pub common_to_rare_rows: usize,
    pub named_duplicate_rows: Option<usize>,
    pub union_duplicate_rows: Option<usize>,
    pub hidden_all_rows: Option<usize>,
    pub hidden_visible_rows: Option<usize>,
}
