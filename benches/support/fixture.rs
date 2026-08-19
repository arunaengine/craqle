//! Bounded, deterministic benchmark fixture construction for SPARQL reads.
//!
//! The corpus iterator itself stays numeric and streaming. This module owns
//! the temporary database and retains only graph metadata plus a handful of
//! stable query terms after setup.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
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
        tolerate_union_duplicates: bool,
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
    encoded_terms_constructed: usize,
    encoded_term_payload_bytes: usize,
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

impl Fixture {
    pub fn from_environment() -> Self {
        Self::load(BenchConfig::from_environment())
    }

    fn load(config: BenchConfig) -> Self {
        let database = tempfile::tempdir().expect("create benchmark temporary database");
        let node = CraqleNode::open_with_options(
            database.path(),
            CraqleOptions::new().with_actor(ActorId::from_bytes([0x43; 32])),
        )
        .expect("open benchmark node");

        let all_graphs: Vec<_> = (0..config.corpus.graphs).map(graph_id).collect();
        let visible_graphs: Vec<_> = all_graphs
            .iter()
            .enumerate()
            .filter(|(index, _)| graph_visibility(config.corpus.graphs, *index as u32).is_visible())
            .map(|(_, graph)| graph.clone())
            .collect();
        assert!(
            !visible_graphs.is_empty(),
            "the selected corpus configuration must expose a graph"
        );

        let mut loader = GraphPartitionedLoader::new(&node, &all_graphs);
        let mut graph_records = vec![0usize; config.corpus.graphs];
        let mut visible_common_records = 0usize;
        let mut inserted_data_quads = 0usize;
        let mut encoded_terms_constructed = 0usize;
        let mut encoded_term_payload_bytes = 0usize;
        let mut star_probe = None;
        let mut duplicate_probe = None;
        let mut hidden_probe = None;
        let mut visible_hot_pair_records = 0usize;

        for record in DeterministicCorpus::new(config.corpus)
            .expect("validated benchmark corpus")
            .iter()
        {
            let graph_index = record.graph as usize;
            assert!(
                graph_index < all_graphs.len(),
                "corpus emitted a graph outside its configuration"
            );
            let visibility = graph_visibility(config.corpus.graphs, record.graph);
            assert_eq!(
                visibility, record.visibility,
                "corpus graph visibility must be derivable from the graph index"
            );

            let subject = subject_term(record.subject);
            let predicate = predicate_term(record.predicate);
            let object = object_term(record.object);
            encoded_terms_constructed += 3;
            encoded_term_payload_bytes += subject.0.len() + predicate.0.len() + object.0.len();

            graph_records[graph_index] += 1;
            inserted_data_quads += 1;
            if visibility.is_visible() && matches!(record.predicate, PredicateKind::Common(0)) {
                visible_common_records += 1;
            }
            if visibility.is_visible()
                && matches!(record.predicate, PredicateKind::Common(0))
                && matches!(record.object, ObjectSpec::Literal(0))
            {
                visible_hot_pair_records += 1;
            }

            // A star selected after the duplicate prefix is wholly canonical:
            // its seven sibling records share the canonical graph locality.
            let star_start = record.ordinal - record.ordinal % 8;
            if star_probe.is_none()
                && !record.duplicate
                && star_start >= config.corpus.duplicate_quads()
                && visibility == GraphVisibility::Visible
                && record.shape == CorpusShape::SameSubjectStar
                && record.predicate.is_rare()
            {
                star_probe = Some(Probe {
                    graph: all_graphs[graph_index].clone(),
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                });
            }
            if duplicate_probe.is_none() && record.duplicate {
                duplicate_probe = Some(Probe {
                    graph: all_graphs[graph_index].clone(),
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                });
            }
            if hidden_probe.is_none() && visibility == GraphVisibility::Hidden {
                hidden_probe = Some(Probe {
                    graph: all_graphs[graph_index].clone(),
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: object.clone(),
                });
            }

            loader.push(
                graph_index,
                MaterializedQuadChange::Insert {
                    graph: all_graphs[graph_index].clone(),
                    subject,
                    predicate,
                    object,
                },
            );
        }
        loader.finish();

        assert_eq!(
            inserted_data_quads, config.corpus.quads,
            "the streaming loader must ingest every corpus record"
        );
        assert!(
            graph_records.iter().all(|count| *count > 0),
            "the selected corpus configuration must cover every graph"
        );
        assert!(
            visible_hot_pair_records >= SELECT_LIMIT,
            "the fixed predicate-object SELECT needs at least ten visible matches"
        );
        assert_eq!(
            node.graphs().expect("list benchmark graphs").len(),
            all_graphs.len(),
            "each generated graph must have been loaded"
        );

        // Do not rebuild graph diagnostics here. This synthetic corpus has no
        // RO-Crate reachability scaffold, so rebuilding would classify linked
        // synthetic subjects as orphans. Query benchmarks do not require it.
        node.ensure_query_indexes();
        node.flush_search_updates()
            .expect("settle search/index work before query timing");
        node.persist_fjall().expect("persist benchmark fixture");

        let star_probe = star_probe.expect("find a complete visible canonical star");
        let common_predicate = predicate_term(PredicateKind::Common(0));
        let common_object = object_term(ObjectSpec::Literal(0));
        assert!(
            visible_common_records > 0,
            "the visible union must contain a common predicate"
        );

        let terms = QueryTerms {
            graph: star_probe.graph.clone(),
            subject: star_probe.subject.clone(),
            common_predicate: common_predicate.clone(),
            common_object: common_object.clone(),
            rare_predicate: star_probe.predicate.clone(),
            rare_object: star_probe.object.clone(),
        };
        let mut cases = query_cases(&terms);
        let count_case = cases
            .iter()
            .position(|case| matches!(case.kind, QueryKind::Count))
            .expect("construct COUNT benchmark case");
        let count = count_value(
            node.query_graphs(&visible_graphs, &cases[count_case].sparql)
                .expect("run untimed COUNT semantic baseline"),
            cases[count_case].label,
        );
        assert!(
            count > 0 && count <= visible_common_records,
            "COUNT semantic baseline must describe loaded common-predicate data"
        );
        cases[count_case].expected = Expected::Count { expected: count };

        let duplicate_queries = duplicate_probe.map(|probe| DuplicateQueries {
            named: format!(
                "SELECT ?g ?s WHERE {{ GRAPH ?g {{ ?s {} {} }} }}",
                probe.predicate.0, probe.object.0
            ),
            union: format!(
                "SELECT ?s WHERE {{ ?s {} {} }}",
                probe.predicate.0, probe.object.0
            ),
        });
        let hidden_query = hidden_probe.map(|probe| {
            format!(
                "SELECT ?s WHERE {{ GRAPH <{}> {{ ?s {} {} }} }}",
                probe.graph.as_str(),
                probe.predicate.0,
                probe.object.0
            )
        });
        let database_bytes = directory_bytes_bounded(database.path())
            .expect("measure bounded benchmark database directory size");

        Self {
            node,
            _database: database,
            config,
            all_graphs,
            visible_graphs,
            terms,
            cases,
            duplicate_queries,
            hidden_query,
            metrics: FixtureMetrics {
                inserted_data_quads,
                encoded_terms_constructed,
                encoded_term_payload_bytes,
                database: database_bytes,
            },
        }
    }

    pub fn config(&self) -> BenchConfig {
        self.config
    }

    pub fn query_terms(&self) -> &QueryTerms {
        &self.terms
    }

    /// Visibility comes only from the generated graph-id index, never from
    /// diagnostics produced by the synthetic data.
    pub fn graph_is_visible(&self, graph: &GraphId) -> bool {
        self.all_graphs
            .iter()
            .position(|known| known == graph)
            .is_some_and(|index| {
                graph_visibility(self.config.corpus.graphs, index as u32).is_visible()
            })
    }

    pub fn hot_path_count(&self) -> usize {
        self.cases.len()
    }

    pub fn hot_path_label(&self, index: usize) -> &'static str {
        self.cases
            .get(index)
            .unwrap_or_else(|| panic!("unknown hot-path benchmark case {index}"))
            .label
    }

    /// Runs one full public query call. The current API returns fully collected
    /// results, so timing is time-to-completion, not first-row latency.
    pub fn run_hot_path(&self, index: usize) -> QueryResults {
        let case = self
            .cases
            .get(index)
            .unwrap_or_else(|| panic!("unknown hot-path benchmark case {index}"));
        self.node
            .query_graphs(&self.visible_graphs, &case.sparql)
            .unwrap_or_else(|_| panic!("{} query failed", case.label))
    }

    /// Runs a full public query call over the generated visible-graph scope.
    pub fn run_visible_query(&self, sparql: &str, label: &str) -> QueryResults {
        self.node
            .query_graphs(&self.visible_graphs, sparql)
            .unwrap_or_else(|_| panic!("{label} query failed"))
    }

    /// Runs a full public query call over every generated graph, including the
    /// corpus's deliberately hidden graphs for duplicate-baseline inspection.
    pub fn run_all_graph_query(&self, sparql: &str, label: &str) -> QueryResults {
        self.node
            .query_graphs(&self.all_graphs, sparql)
            .unwrap_or_else(|_| panic!("{label} query failed"))
    }

    /// Untimed semantic sweep and deterministic cache warm-up.
    pub fn assert_semantics(&self) -> SemanticReport {
        let mut report = SemanticReport::default();
        for index in 0..self.cases.len() {
            let case = &self.cases[index];
            match (case.kind, self.assert_case(case)) {
                (QueryKind::AskHit, CheckValue::Boolean(value)) => report.ask_hit = value,
                (QueryKind::AskMiss, CheckValue::Boolean(value)) => report.ask_miss = value,
                (QueryKind::SelectLimit, CheckValue::Rows(value)) => report.select_rows = value,
                (QueryKind::Count, CheckValue::Count(value)) => report.count = value,
                (QueryKind::PropertyStar, CheckValue::Rows(value)) => {
                    report.property_star_rows = value
                }
                (QueryKind::RareToCommon, CheckValue::Rows(value)) => {
                    report.rare_to_common_rows = value
                }
                (QueryKind::CommonToRare, CheckValue::Rows(value)) => {
                    report.common_to_rare_rows = value
                }
                _ => panic!("hot-path case result form did not match its assertion"),
            }
        }

        if let Some(duplicate) = &self.duplicate_queries {
            let named_rows =
                self.solution_rows(&self.all_graphs, &duplicate.named, "named duplicate");
            assert!(
                named_rows >= 2,
                "named duplicate smoke must retain graph-specific multiplicity"
            );
            let union_rows = self.solution_rows(
                &self.all_graphs,
                &duplicate.union,
                "union duplicate baseline",
            );
            report.named_duplicate_rows = Some(named_rows);
            // Deliberately do not assert the union count: it is a known current
            // baseline defect, so this number is evidence rather than a target.
            report.union_duplicate_rows = Some(union_rows);
        }

        if let Some(hidden_query) = &self.hidden_query {
            let all_rows = self.solution_rows(&self.all_graphs, hidden_query, "hidden graph setup");
            let visible_rows = self.solution_rows(
                &self.visible_graphs,
                hidden_query,
                "hidden graph visibility",
            );
            assert!(all_rows > 0, "hidden graph setup must contain the probe");
            assert_eq!(
                visible_rows, 0,
                "the generated visibility predicate must hide the graph"
            );
            report.hidden_all_rows = Some(all_rows);
            report.hidden_visible_rows = Some(visible_rows);
        }

        report
    }

    pub fn print_report(&self, report: &SemanticReport) {
        let metadata = self.config.corpus.metadata();
        let craqle_commit = repository_commit();
        println!(
            "sparql_hot_path fixture: corpus_version={} seed={:#x} quads={} graphs={} \
             duplicate_percent={} visible_graphs={} hidden_graphs={} inserted_data_quads={} \
             load_batch_max_changes={} craqle_commit={}",
            CORPUS_VERSION,
            metadata.seed,
            metadata.quads,
            metadata.graphs,
            metadata.duplicate_percent,
            metadata.visible_graphs,
            metadata.hidden_graphs,
            self.metrics.inserted_data_quads,
            LOAD_BATCH_SIZE,
            craqle_commit,
        );
        if self.metrics.database.complete {
            println!(
                "sparql_hot_path fixture: database_directory_bytes={} entries={}",
                self.metrics.database.bytes, self.metrics.database.entries
            );
        } else {
            println!(
                "sparql_hot_path fixture: database_directory_bytes_at_least={} entries_scanned={} \
                 walk_entry_cap={}",
                self.metrics.database.bytes,
                self.metrics.database.entries,
                DIRECTORY_SIZE_ENTRY_LIMIT,
            );
        }
        println!(
            "sparql_hot_path fixture: encoded_terms_constructed={} encoded_term_payload_bytes={} \
             (payload bytes are not allocator measurements)",
            self.metrics.encoded_terms_constructed, self.metrics.encoded_term_payload_bytes
        );
        println!(
            "sparql_hot_path semantic rows: ask_hit={} ask_miss={} select_limit={} count={} \
             property_star={} rare_to_common={} common_to_rare={}",
            report.ask_hit,
            report.ask_miss,
            report.select_rows,
            report.count,
            report.property_star_rows,
            report.rare_to_common_rows,
            report.common_to_rare_rows,
        );
        match (report.named_duplicate_rows, report.union_duplicate_rows) {
            (Some(named), Some(union)) => println!(
                "sparql_hot_path duplicate baseline: named_rows={} union_rows={} \
                 (observed only; union cardinality is not asserted as intended semantics)",
                named, union
            ),
            _ => println!(
                "sparql_hot_path duplicate baseline: no cross-graph duplicate requested by this configuration"
            ),
        }
        match (report.hidden_all_rows, report.hidden_visible_rows) {
            (Some(all), Some(visible)) => println!(
                "sparql_hot_path visibility smoke: hidden_graph_all_rows={} hidden_graph_visible_rows={}",
                all, visible
            ),
            _ => {
                println!("sparql_hot_path visibility smoke: no hidden graph in this configuration")
            }
        }
        println!(
            "sparql_hot_path measurement: each sample is complete, fully collected QueryResults; \
             it is not first-row latency. Capture Rust/profile/features with rustc -Vv, cargo bench, \
             and cargo tree -e features -p craqle."
        );
    }

    fn assert_case(&self, case: &QueryCase) -> CheckValue {
        let results = self
            .node
            .query_graphs(&self.visible_graphs, &case.sparql)
            .unwrap_or_else(|_| panic!("{} semantic query failed", case.label));
        match &case.expected {
            Expected::Boolean(expected) => match results {
                QueryResults::Boolean(actual) => {
                    assert_eq!(actual, *expected, "{} boolean result changed", case.label);
                    CheckValue::Boolean(actual)
                }
                _ => panic!("{} must return a boolean", case.label),
            },
            Expected::Rows {
                count,
                bindings,
                tolerate_union_duplicates,
            } => match results {
                QueryResults::Solutions(rows) => {
                    assert!(
                        rows.iter()
                            .all(|row| bindings.iter().all(|binding| row.contains_key(*binding))),
                        "{} returned an incomplete binding",
                        case.label
                    );
                    if *tolerate_union_duplicates {
                        let unique = rows
                            .iter()
                            .map(|row| {
                                bindings
                                    .iter()
                                    .map(|binding| row[*binding].clone())
                                    .collect::<Vec<_>>()
                            })
                            .collect::<BTreeSet<_>>();
                        assert_eq!(
                            unique.len(),
                            *count,
                            "{} unique solution count changed",
                            case.label
                        );
                    } else {
                        assert_eq!(rows.len(), *count, "{} row count changed", case.label);
                    }
                    CheckValue::Rows(rows.len())
                }
                _ => panic!("{} must return solution rows", case.label),
            },
            Expected::Count { expected } => {
                let actual = count_value(results, case.label);
                assert_eq!(actual, *expected, "{} count changed", case.label);
                CheckValue::Count(actual)
            }
        }
    }

    fn solution_rows(&self, graphs: &[GraphId], sparql: &str, label: &str) -> usize {
        match self
            .node
            .query_graphs(graphs, sparql)
            .unwrap_or_else(|_| panic!("{label} query failed"))
        {
            QueryResults::Solutions(rows) => rows.len(),
            _ => panic!("{label} must return solution rows"),
        }
    }
}

enum CheckValue {
    Boolean(bool),
    Rows(usize),
    Count(usize),
}

struct GraphPartitionedLoader<'a> {
    node: &'a CraqleNode,
    graphs: &'a [GraphId],
    partitions: Vec<Vec<MaterializedQuadChange>>,
    pending_changes: usize,
}

impl<'a> GraphPartitionedLoader<'a> {
    fn new(node: &'a CraqleNode, graphs: &'a [GraphId]) -> Self {
        Self {
            node,
            graphs,
            partitions: (0..graphs.len()).map(|_| Vec::new()).collect(),
            pending_changes: 0,
        }
    }

    fn push(&mut self, graph_index: usize, change: MaterializedQuadChange) {
        self.partitions[graph_index].push(change);
        self.pending_changes += 1;
        if self.partitions[graph_index].len() >= LOAD_BATCH_SIZE
            || self.pending_changes >= LOAD_BATCH_SIZE
        {
            self.flush_graph(graph_index);
        }
        assert!(
            self.pending_changes < LOAD_BATCH_SIZE,
            "the pending graph-partitioned loader buffer exceeded its fixed cap"
        );
    }

    fn finish(&mut self) {
        for graph_index in 0..self.partitions.len() {
            self.flush_graph(graph_index);
        }
        assert_eq!(self.pending_changes, 0, "flush every graph partition");
    }

    fn flush_graph(&mut self, graph_index: usize) {
        let changes = std::mem::take(&mut self.partitions[graph_index]);
        if changes.is_empty() {
            return;
        }
        self.pending_changes -= changes.len();
        self.node
            .apply_changes_bulk_unchecked(&self.graphs[graph_index], changes)
            .expect("apply graph-scoped bounded benchmark batch");
    }
}

fn query_cases(terms: &QueryTerms) -> Vec<QueryCase> {
    let type_predicate = predicate_term(PredicateKind::Type);
    vec![
        QueryCase {
            label: "bound_ask_hit",
            kind: QueryKind::AskHit,
            sparql: format!(
                "ASK WHERE {{ {} {} {} }}",
                terms.subject.0, terms.rare_predicate.0, terms.rare_object.0
            ),
            expected: Expected::Boolean(true),
        },
        QueryCase {
            label: "bound_ask_miss",
            kind: QueryKind::AskMiss,
            sparql: format!(
                "ASK WHERE {{ <urn:craqle:bench:performance-corpus-v1:missing> {} {} }}",
                terms.rare_predicate.0, terms.rare_object.0
            ),
            expected: Expected::Boolean(false),
        },
        QueryCase {
            label: "fixed_predicate_object_select_limit10",
            kind: QueryKind::SelectLimit,
            sparql: format!(
                "SELECT ?s WHERE {{ ?s {} {} }} LIMIT {SELECT_LIMIT}",
                terms.common_predicate.0, terms.common_object.0
            ),
            expected: Expected::Rows {
                count: SELECT_LIMIT,
                bindings: &["s"],
                tolerate_union_duplicates: false,
            },
        },
        QueryCase {
            label: "exact_count_common_predicate",
            kind: QueryKind::Count,
            sparql: format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ ?s {} ?o }}",
                terms.common_predicate.0
            ),
            expected: Expected::Count { expected: 0 },
        },
        QueryCase {
            label: "same_subject_property_star",
            kind: QueryKind::PropertyStar,
            sparql: format!(
                "SELECT ?type ?common ?rare WHERE {{ {} {} ?type ; {} ?common ; {} ?rare }}",
                terms.subject.0, type_predicate.0, terms.common_predicate.0, terms.rare_predicate.0,
            ),
            expected: Expected::Rows {
                count: 1,
                bindings: &["type", "common", "rare"],
                tolerate_union_duplicates: true,
            },
        },
        QueryCase {
            label: "rare_to_common_join",
            kind: QueryKind::RareToCommon,
            sparql: format!(
                "SELECT ?s ?common WHERE {{ ?s {} {} . ?s {} ?common }}",
                terms.rare_predicate.0, terms.rare_object.0, terms.common_predicate.0,
            ),
            expected: Expected::Rows {
                count: 1,
                bindings: &["s", "common"],
                tolerate_union_duplicates: true,
            },
        },
        QueryCase {
            label: "common_to_rare_written_order",
            kind: QueryKind::CommonToRare,
            sparql: format!(
                "SELECT ?s ?common WHERE {{ ?s {} ?common . ?s {} {} }}",
                terms.common_predicate.0, terms.rare_predicate.0, terms.rare_object.0,
            ),
            expected: Expected::Rows {
                count: 1,
                bindings: &["s", "common"],
                tolerate_union_duplicates: true,
            },
        },
    ]
}

fn graph_id(index: usize) -> GraphId {
    GraphId::new(&format!(
        "urn:craqle:bench:performance-corpus-v1:graph:{index}"
    ))
}

fn subject_term(subject: u64) -> EncodedTerm {
    EncodedTerm(format!(
        "<urn:craqle:bench:performance-corpus-v1:subject:{subject:016x}>"
    ))
}

fn predicate_term(predicate: PredicateKind) -> EncodedTerm {
    match predicate {
        PredicateKind::Type => {
            EncodedTerm("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string())
        }
        PredicateKind::Common(index) => EncodedTerm(format!(
            "<urn:craqle:bench:performance-corpus-v1:predicate:common:{index}>"
        )),
        PredicateKind::Rare(index) => EncodedTerm(format!(
            "<urn:craqle:bench:performance-corpus-v1:predicate:rare:{index}>"
        )),
        PredicateKind::Chain => {
            EncodedTerm("<urn:craqle:bench:performance-corpus-v1:predicate:chain>".to_string())
        }
    }
}

fn object_term(object: ObjectSpec) -> EncodedTerm {
    match object {
        ObjectSpec::Iri(value) => EncodedTerm(format!(
            "<urn:craqle:bench:performance-corpus-v1:object:{value:016x}>"
        )),
        ObjectSpec::Literal(value) => EncodedTerm(format!("\"{value:016x}\"")),
    }
}

fn count_value(results: QueryResults, label: &str) -> usize {
    let QueryResults::Solutions(rows) = results else {
        panic!("{label} must return a count solution row");
    };
    assert_eq!(rows.len(), 1, "{label} must return exactly one count row");
    let term = rows[0]
        .get("count")
        .unwrap_or_else(|| panic!("{label} must bind ?count"));
    match term.to_term() {
        Some(Term::Literal(value)) => value
            .value()
            .parse()
            .unwrap_or_else(|_| panic!("{label} must bind an integer count")),
        _ => panic!("{label} must bind a literal count"),
    }
}

fn directory_bytes_bounded(root: &Path) -> std::io::Result<DirectoryBytes> {
    let mut paths = vec![root.to_path_buf()];
    let mut entries = 0usize;
    let mut bytes = 0u64;

    while let Some(path) = paths.pop() {
        for entry in fs::read_dir(path)? {
            if entries == DIRECTORY_SIZE_ENTRY_LIMIT {
                return Ok(DirectoryBytes {
                    bytes,
                    entries,
                    complete: false,
                });
            }
            let entry = entry?;
            entries += 1;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                paths.push(entry.path());
            } else if file_type.is_file() {
                bytes = bytes.saturating_add(entry.metadata()?.len());
            }
        }
    }

    Ok(DirectoryBytes {
        bytes,
        entries,
        complete: true,
    })
}

fn repository_commit() -> String {
    env::var("CRAQLE_GIT_COMMIT")
        .ok()
        .or_else(|| option_env!("CRAQLE_GIT_COMMIT").map(str::to_owned))
        .or_else(|| {
            let output = Command::new("git")
                .args(["rev-parse", "--verify", "HEAD"])
                .output()
                .ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        })
        .filter(|commit| !commit.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn env_u8(name: &str, default: u8) -> u8 {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

fn env_duration(name: &str, default_seconds: u64) -> Duration {
    let seconds = match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("{name} must be whole seconds")),
        Err(env::VarError::NotPresent) => default_seconds,
        Err(env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    };
    assert!(seconds > 0, "{name} must be greater than zero");
    Duration::from_secs(seconds)
}
