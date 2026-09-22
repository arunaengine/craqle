//! Evaluates authorized SPARQL queries and materializes update changes.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange};
use crate::planner::{JoinKind, JoinMode, PlannedJoin, PlannerTrace};
use crate::query::budget::BudgetShape;
pub(crate) use crate::query::budget::{QueryBudget, QueryLimitExceeded};
use crate::query::context::{
    MAX_QUERY_REGISTRATIONS, QueryCancellation, QueryReadMode, ReadContext, ReadStatistics,
    RequestOutcome,
};
use crate::query::cursor::{DenseResolver, DenseTerm, RawIndexPattern};
use crate::query::deadline::{MAX_DEADLINE_REGISTRATIONS, RequestClock};
use crate::rdf_read::{DenseScan, GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::search::SearchIndex;
use crate::sparql_fast_path::{
    FastPathPlan, QueryFastPathKind as FastPathKind, QueryFastPathMode as FastPathMode,
};
use crate::store::{GraphStore, QueryTermId, StoreError, StoreReadSnapshot, TermId};
use oxrdf::{
    BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode as GraphNode, Term, Triple, Variable,
};
use spareval::{
    DeleteInsertQuad, ExpressionTerm, InternalQuad, QueryEvaluationError, QueryEvaluator,
    QueryableDataset,
};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, GraphTarget};
use spargebra::term::{GraphNamePattern, GroundTerm, NamedNodePattern, TermPattern};
use spargebra::{GraphUpdateOperation, Query, SparqlParser};

#[derive(Debug, thiserror::Error)]
pub enum SparqlError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("evaluation error: {0}")]
    Evaluation(String),
    #[error("query planning error: {0}")]
    Planning(String),
    #[error("query limit exceeded: {resource} limit is {limit}")]
    QueryLimit {
        resource: &'static str,
        limit: usize,
    },
    #[error("SPARQL query cancelled")]
    Cancelled,
    #[error("unsupported SPARQL feature: {0}")]
    Unsupported(String),
    #[error("invalid RDF term: {0}")]
    InvalidTerm(String),
    #[error(transparent)]
    UnsupportedRdfStarTerm(#[from] crate::UnsupportedRdfStarTerm),
    #[error("authorization: {0}")]
    Authorization(#[from] crate::AuthorizationError),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("search error: {0}")]
    Search(#[from] crate::search::SearchError),
}

impl SparqlError {
    pub(crate) fn kind(&self) -> crate::CraqleErrorKind {
        match self {
            Self::Parse(_) | Self::Evaluation(_) | Self::Planning(_) | Self::InvalidTerm(_) => {
                crate::CraqleErrorKind::InvalidInput
            }
            Self::QueryLimit { .. } => crate::CraqleErrorKind::QueryLimit,
            Self::Authorization(_) => crate::CraqleErrorKind::Unauthorized,
            Self::Unsupported(_) => crate::CraqleErrorKind::Unsupported,
            Self::UnsupportedRdfStarTerm(_) => crate::CraqleErrorKind::Unsupported,
            Self::Cancelled => crate::CraqleErrorKind::Cancelled,
            Self::Store(error) => error.kind(),
            Self::Search(error) => error.kind(),
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, SparqlError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResults {
    Solutions(Vec<HashMap<String, EncodedTerm>>),
    Boolean(bool),
    Graph(Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>),
}

/// A reusable parsed query without a snapshot or visibility decision.
/// Rewriting, planning, and query ID resolution use current state per execution.
#[derive(Clone)]
pub struct PreparedQuery {
    query: Arc<Query>,
    query_bytes: usize,
    /// Hash of the caller's query text; parsed aggregates get fresh variable names.
    source_fingerprint: Arc<str>,
}

#[derive(Clone, Copy)]
pub(crate) struct QueryRun<'a> {
    pub(crate) sparql: &'a str,
    pub(crate) options: &'a QueryOptions,
}

pub(crate) struct GraphQuery<'a> {
    pub(crate) auth: &'a dyn crate::Authorizer,
    pub(crate) graphs: &'a [GraphId],
    pub(crate) sparql: &'a str,
    pub(crate) options: &'a QueryOptions,
}

impl fmt::Debug for PreparedQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedQuery")
            .finish_non_exhaustive()
    }
}

/// Bounds parsing, adapter reads, native operators, and returned results.
/// Locked evaluator buffers observe cancellation cooperatively and are not byte-metered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryLimits {
    pub max_query_bytes: usize,
    pub max_result_rows: usize,
    pub max_result_cells: usize,
    pub max_result_bytes: usize,
    pub max_graph_triples: usize,
    pub max_intermediate_rows: usize,
    pub max_hash_entries: usize,
    pub max_hash_bytes: usize,
    pub max_property_path_edges: usize,
    pub max_property_path_depth: usize,
    pub deadline: Option<Duration>,
}

impl QueryLimits {
    pub fn production() -> Self {
        Self {
            max_query_bytes: 1_048_576,
            max_result_rows: 100_000,
            max_result_cells: 1_000_000,
            max_result_bytes: 64 * 1_048_576,
            max_graph_triples: 100_000,
            max_intermediate_rows: 1_000_000,
            max_hash_entries: 1_000_000,
            max_hash_bytes: 128 * 1_048_576,
            max_property_path_edges: 1_000_000,
            max_property_path_depth: 64,
            deadline: Some(Duration::from_secs(30)),
        }
    }

    /// Disables enforceable query limits for trusted semantic-oracle executions.
    /// Cancellation inside locked evaluator buffers remains best effort.
    pub fn unbounded() -> Self {
        Self {
            max_query_bytes: usize::MAX,
            max_result_rows: usize::MAX,
            max_result_cells: usize::MAX,
            max_result_bytes: usize::MAX,
            max_graph_triples: usize::MAX,
            max_intermediate_rows: usize::MAX,
            max_hash_entries: usize::MAX,
            max_hash_bytes: usize::MAX,
            max_property_path_edges: usize::MAX,
            max_property_path_depth: usize::MAX,
            deadline: None,
        }
    }
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self::production()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryFeatures {
    budget: BudgetShape,
    /// Statically derivable row estimate, not a measured count. Leaves whose
    /// cardinality only the store knows count as one row.
    estimated_rows: usize,
}

impl From<QueryLimitExceeded> for SparqlError {
    fn from(error: QueryLimitExceeded) -> Self {
        match error {
            QueryLimitExceeded::Limit { resource, limit } => Self::QueryLimit { resource, limit },
            QueryLimitExceeded::Cancelled => Self::Cancelled,
        }
    }
}

impl RequestClock {
    fn check_stage(&self) -> Result<()> {
        match self.outcome() {
            RequestOutcome::Active => Ok(()),
            RequestOutcome::Explicit => Err(SparqlError::Cancelled),
            RequestOutcome::Deadline => Err(QueryLimitExceeded::Limit {
                resource: "query deadline",
                limit: 0,
            }
            .into()),
            RequestOutcome::QueryCapacity => Err(QueryLimitExceeded::Limit {
                resource: "active query registrations",
                limit: MAX_QUERY_REGISTRATIONS,
            }
            .into()),
            RequestOutcome::DeadlineCapacity => Err(QueryLimitExceeded::Limit {
                resource: "active deadline registrations",
                limit: MAX_DEADLINE_REGISTRATIONS,
            }
            .into()),
            RequestOutcome::DeadlineUnavailable => Err(QueryLimitExceeded::Limit {
                resource: "deadline service",
                limit: 0,
            }
            .into()),
        }
    }

    fn cancel_error(&self) -> SparqlError {
        match self.outcome() {
            RequestOutcome::Explicit | RequestOutcome::Active => SparqlError::Cancelled,
            RequestOutcome::Deadline => QueryLimitExceeded::Limit {
                resource: "query deadline",
                limit: 0,
            }
            .into(),
            RequestOutcome::QueryCapacity => QueryLimitExceeded::Limit {
                resource: "active query registrations",
                limit: MAX_QUERY_REGISTRATIONS,
            }
            .into(),
            RequestOutcome::DeadlineCapacity => QueryLimitExceeded::Limit {
                resource: "active deadline registrations",
                limit: MAX_DEADLINE_REGISTRATIONS,
            }
            .into(),
            RequestOutcome::DeadlineUnavailable => QueryLimitExceeded::Limit {
                resource: "deadline service",
                limit: 0,
            }
            .into(),
        }
    }
}

fn query_features(query: &Query) -> QueryFeatures {
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    pattern_features(pattern)
}

fn merge_features(left: QueryFeatures, right: QueryFeatures) -> QueryFeatures {
    let estimated_rows = left.estimated_rows.saturating_add(right.estimated_rows);
    QueryFeatures {
        budget: BudgetShape {
            property_path: left.budget.property_path || right.budget.property_path,
            property_path_depth: left
                .budget
                .property_path_depth
                .max(right.budget.property_path_depth),
            guarded_hash: left.budget.guarded_hash || right.budget.guarded_hash,
            estimated_rows,
        },
        estimated_rows,
    }
}

fn pattern_features(pattern: &GraphPattern) -> QueryFeatures {
    match pattern {
        GraphPattern::Bgp { patterns } => QueryFeatures {
            budget: BudgetShape {
                guarded_hash: patterns.len() > 1,
                estimated_rows: 1,
                ..BudgetShape::default()
            },
            estimated_rows: 1,
        },
        GraphPattern::Path { path, .. } => QueryFeatures {
            budget: BudgetShape {
                property_path: true,
                property_path_depth: property_path_depth(path),
                guarded_hash: true,
                estimated_rows: 1,
            },
            estimated_rows: 1,
        },
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Minus { left, right } => {
            let sides = (pattern_features(left), pattern_features(right));
            let mut features = merge_features(sides.0, sides.1);
            features.budget.guarded_hash = true;
            // Independent sides multiply. Sharing a variable cannot produce
            // more rows than the larger side under this estimate.
            features.estimated_rows = if shares_variable(left, right) {
                sides.0.estimated_rows.max(sides.1.estimated_rows)
            } else {
                sides
                    .0
                    .estimated_rows
                    .saturating_mul(sides.1.estimated_rows)
            };
            features.budget.estimated_rows = features.estimated_rows;
            if let GraphPattern::LeftJoin {
                expression: Some(expression),
                ..
            } = pattern
            {
                features = merge_features(features, expression_features(expression));
            }
            features
        }
        GraphPattern::Union { left, right } => {
            merge_features(pattern_features(left), pattern_features(right))
        }
        GraphPattern::Filter { expr, inner } => {
            merge_features(pattern_features(inner), expression_features(expr))
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Service { inner, .. } => pattern_features(inner),
        GraphPattern::Extend {
            inner, expression, ..
        } => merge_features(pattern_features(inner), expression_features(expression)),
        GraphPattern::Values { bindings, .. } => QueryFeatures {
            budget: BudgetShape {
                estimated_rows: bindings.len(),
                ..BudgetShape::default()
            },
            estimated_rows: bindings.len(),
        },
        GraphPattern::OrderBy { inner, expression } => {
            let mut features = pattern_features(inner);
            features.budget.guarded_hash = true;
            for expression in expression {
                let expression = match expression {
                    spargebra::algebra::OrderExpression::Asc(expression)
                    | spargebra::algebra::OrderExpression::Desc(expression) => expression,
                };
                features = merge_features(features, expression_features(expression));
            }
            features
        }
        GraphPattern::Distinct { inner } => {
            let mut features = pattern_features(inner);
            features.budget.guarded_hash = true;
            features
        }
        GraphPattern::Reduced { inner } => pattern_features(inner),
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            let mut features = pattern_features(inner);
            features.budget.guarded_hash = true;
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    features = merge_features(features, expression_features(expr));
                }
            }
            features
        }
        #[allow(unreachable_patterns)]
        _ => QueryFeatures {
            budget: BudgetShape {
                guarded_hash: true,
                estimated_rows: 1,
                ..BudgetShape::default()
            },
            estimated_rows: 1,
        },
    }
}

/// Whether a join of these sides can multiply instead of matching.
fn shares_variable(left: &GraphPattern, right: &GraphPattern) -> bool {
    let mut bound = HashSet::new();
    left.on_in_scope_variable(|variable| {
        bound.insert(variable.as_str());
    });
    let mut shared = false;
    right.on_in_scope_variable(|variable| {
        shared = shared || bound.contains(variable.as_str());
    });
    shared
}

fn expression_features(expression: &Expression) -> QueryFeatures {
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => QueryFeatures::default(),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            merge_features(expression_features(left), expression_features(right))
        }
        Expression::In(left, right) => right
            .iter()
            .fold(expression_features(left), |features, expression| {
                merge_features(features, expression_features(expression))
            }),
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            expression_features(inner)
        }
        Expression::Exists(pattern) => {
            let mut features = pattern_features(pattern);
            features.budget.guarded_hash = true;
            features
        }
        Expression::If(condition, left, right) => merge_features(
            expression_features(condition),
            merge_features(expression_features(left), expression_features(right)),
        ),
        Expression::Coalesce(expressions) => expressions
            .iter()
            .fold(QueryFeatures::default(), |features, expression| {
                merge_features(features, expression_features(expression))
            }),
        Expression::FunctionCall(_, expressions) => expressions
            .iter()
            .fold(QueryFeatures::default(), |features, expression| {
                merge_features(features, expression_features(expression))
            }),
        #[allow(unreachable_patterns)]
        _ => QueryFeatures {
            budget: BudgetShape {
                guarded_hash: true,
                ..BudgetShape::default()
            },
            ..QueryFeatures::default()
        },
    }
}

fn property_path_depth(path: &spargebra::algebra::PropertyPathExpression) -> usize {
    use spargebra::algebra::PropertyPathExpression;

    match path {
        PropertyPathExpression::NamedNode(_) | PropertyPathExpression::NegatedPropertySet(_) => 1,
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => {
            1usize.saturating_add(property_path_depth(inner))
        }
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            1usize.saturating_add(property_path_depth(left).max(property_path_depth(right)))
        }
    }
}

/// Per-execution controls for a prepared SPARQL query.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct QueryOptions {
    pub cancellation: QueryCancellation,
    pub read_mode: QueryReadMode,
    pub optimize: bool,
    pub join_mode: JoinMode,
    pub fast_paths: FastPathMode,
    /// Collects request-local storage cost counters for this execution.
    pub collect_costs: bool,
    /// Collects per-operator evaluator timings and row counts.
    /// Enabled by default; disable it for results-only execution.
    pub collect_plan_statistics: bool,
    pub limits: QueryLimits,
}

impl QueryOptions {
    /// Default limits and planning without per-operator statistics, the cheap choice
    /// for callers that need rows but not an analyzed plan.
    #[must_use]
    pub fn results_only() -> Self {
        Self {
            collect_plan_statistics: false,
            ..Self::default()
        }
    }
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            cancellation: QueryCancellation::new(),
            read_mode: QueryReadMode::Auto,
            optimize: planner_enabled(),
            join_mode: JoinMode::Auto,
            fast_paths: FastPathMode::Auto,
            collect_costs: false,
            collect_plan_statistics: true,
            limits: QueryLimits::default(),
        }
    }
}

/// Limits applied while parsing and materializing a SPARQL update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateLimits {
    pub max_update_bytes: usize,
    pub max_materialized_bindings: usize,
    pub max_changes: usize,
    pub max_graphs: usize,
    pub deadline: Option<Duration>,
}

impl UpdateLimits {
    pub fn production() -> Self {
        Self {
            max_update_bytes: 1_048_576,
            max_materialized_bindings: 100_000,
            max_changes: 1_000_000,
            max_graphs: 16,
            deadline: Some(Duration::from_secs(30)),
        }
    }

    /// Disables update materialization and deadline limits for trusted input.
    /// Fixed parser nesting protection remains active.
    pub fn unbounded() -> Self {
        Self {
            max_update_bytes: usize::MAX,
            max_materialized_bindings: usize::MAX,
            max_changes: usize::MAX,
            max_graphs: usize::MAX,
            deadline: None,
        }
    }

    fn is_unbounded(&self) -> bool {
        *self == Self::unbounded()
    }
}

impl Default for UpdateLimits {
    fn default() -> Self {
        Self::production()
    }
}

/// Per-request controls for SPARQL Update.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
#[non_exhaustive]
pub struct UpdateOptions {
    pub limits: UpdateLimits,
}

/// Complete query output and diagnostics from the same execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryExecution {
    pub results: QueryResults,
    pub statistics: QueryExecutionStatistics,
}

/// Serializable logical and physical plan for one prepared query.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryPlan {
    pub fingerprint: String,
    pub root: QueryPlanNode,
}

/// One operator in a Craqle query plan.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryPlanNode {
    pub logical_operator: QueryLogicalOperator,
    pub physical_operator: QueryPhysicalOperator,
    pub access_paths: Vec<crate::query::context::ReadAccessPath>,
    pub estimated_rows: Option<u64>,
    pub actual_rows: Option<u64>,
    pub index_seeks: u64,
    pub candidate_rows: u64,
    pub output_rows: u64,
    pub elapsed_time: Duration,
    pub children: Vec<QueryPlanNode>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum QueryLogicalOperator {
    Ask,
    #[default]
    Select,
    Construct,
    Describe,
    Join,
    Evaluation,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum QueryPhysicalOperator {
    #[default]
    Generic,
    FastPath(FastPathKind),
    PlannedJoin(JoinKind),
    Evaluator(String),
    /// The caller may not read every graph, so the physical plan is not reported.
    Withheld,
}

/// Work and stage timings for one complete query execution. A caller that cannot read every
/// graph gets a reduced view; see [`QueryExecutionStatistics::details_withheld`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryExecutionStatistics {
    pub parse_time: Duration,
    pub rewrite_time: Duration,
    pub planning_time: Duration,
    pub execution_time: Duration,
    pub result_collection_time: Duration,
    pub time_to_first_internal_result: Option<Duration>,
    pub fast_path: Option<FastPathKind>,
    pub planned_joins: Vec<PlannedJoin>,
    pub selected_access_paths: Vec<crate::query::context::ReadAccessPath>,
    pub plan_fingerprint: String,
    pub index_seeks: u64,
    pub qv_admission_checks: u64,
    pub qv_header_reads: u64,
    pub qv_counter_reads: u64,
    pub qv_trusted: bool,
    pub query_id_generation: Option<u64>,
    pub fallback_reason: Option<String>,
    pub source_keys_read: u64,
    pub source_bytes_read: u64,
    pub qv_keys_read: u64,
    pub qv_bytes_read: u64,
    pub reverse_mapping_reads: u64,
    pub reverse_mapping_bytes: u64,
    pub forward_mapping_reads: u64,
    pub forward_mapping_bytes: u64,
    pub planner_index_entries: u64,
    pub planner_point_reads: u64,
    pub planner_cache_hits: u64,
    pub planner_cache_misses: u64,
    pub planner_memo_hits: u64,
    pub planner_memo_misses: u64,
    pub candidate_quads: u64,
    pub matching_quads: u64,
    pub graphs_considered: u64,
    pub orphan_checks: u64,
    pub duplicate_groups: u64,
    pub duplicate_copies_skipped: u64,
    pub key_fields_extracted: u64,
    pub authoritative_terms_decoded: u64,
    pub result_terms_decoded: u64,
    pub encoded_quad_constructions: u64,
    pub terms_decoded: u64,
    pub intermediate_rows: u64,
    /// Whether `intermediate_rows` was measured for this execution.
    pub intermediate_rows_available: bool,
    pub result_rows: u64,
    pub result_cells: u64,
    pub plan: QueryPlan,
}

impl QueryExecutionStatistics {
    /// Keeps only timings, the caller's own result counts, and the query-derived plan.
    /// Every other field may reflect unreadable graphs and returns to its default.
    pub(crate) fn withhold_details(&mut self, prepared: &PreparedQuery) {
        let full = std::mem::take(self);
        let mut plan = full.plan;
        plan.withhold_details(prepared);
        *self = Self {
            parse_time: full.parse_time,
            rewrite_time: full.rewrite_time,
            planning_time: full.planning_time,
            execution_time: full.execution_time,
            result_collection_time: full.result_collection_time,
            time_to_first_internal_result: full.time_to_first_internal_result,
            plan_fingerprint: plan.fingerprint.clone(),
            result_rows: full.result_rows,
            result_cells: full.result_cells,
            plan,
            ..Self::default()
        };
    }

    /// Whether details that could reflect unreadable graphs were withheld from the caller.
    pub fn details_withheld(&self) -> bool {
        self.plan.details_withheld()
    }
}

impl QueryPlan {
    /// Replaces the physical plan with its query form, result rows, and query text fingerprint.
    pub(crate) fn withhold_details(&mut self, prepared: &PreparedQuery) {
        let root = std::mem::take(&mut self.root);
        *self = Self {
            fingerprint: prepared.source_fingerprint.to_string(),
            root: QueryPlanNode {
                logical_operator: root.logical_operator,
                physical_operator: QueryPhysicalOperator::Withheld,
                actual_rows: root.actual_rows,
                output_rows: root.output_rows,
                elapsed_time: root.elapsed_time,
                ..QueryPlanNode::default()
            },
        };
    }

    /// Whether the physical plan and its estimates were withheld from the caller.
    pub fn details_withheld(&self) -> bool {
        self.root.physical_operator == QueryPhysicalOperator::Withheld
    }
}

pub(crate) struct SparqlEngine {
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
}

pub(crate) type VisibleFn<'a> = dyn Fn(&GraphId) -> bool + 'a;
pub(crate) type SnapshotVisibleFn<'a> = dyn Fn(&StoreReadSnapshot, &GraphId) -> bool + 'a;

/// Which graphs a query may see. `Predicate` defers the decision to a
/// callback evaluated lazily per touched graph (memoized per query).
#[derive(Clone, Copy)]
enum GraphScope<'a> {
    /// Test-only: every graph is visible.
    #[cfg(test)]
    All,
    List(&'a [GraphId]),
    Predicate(&'a VisibleFn<'a>),
}

#[cfg(test)]
thread_local! {
    /// Evaluations on this thread that collected per-operator statistics.
    static DETAILED_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Native graph-level evaluations on this thread.
    pub(crate) static GRAPH_DISTINCT_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Small graph sets populate spareval's named graph list. Larger sets use the
/// union view filtered by graph term ID.
const EXPLICIT_GRAPH_LIMIT: usize = 32;

const COMMON_PREFIXES: &str = "\
PREFIX schema: <http://schema.org/>\n\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
PREFIX fts: <urn:craqle:fts:>\n";

/// Escape hatch for the craqle plan optimizer: set `CRAQLE_QUERY_OPT` to
/// `0`/`off`/`false` to evaluate raw sparopt plans (debugging aid).
fn planner_enabled() -> bool {
    !matches!(
        std::env::var("CRAQLE_QUERY_OPT").as_deref(),
        Ok("0") | Ok("off") | Ok("OFF") | Ok("false") | Ok("FALSE")
    )
}

const FTS_SERVICE_IRI: &str = "urn:craqle:fts";
const FTS_QUERY_IRI: &str = "urn:craqle:fts:query";
const FTS_LIMIT_IRI: &str = "urn:craqle:fts:limit";
const FTS_SCORE_IRI: &str = "urn:craqle:fts:score";
const FTS_GRAPH_IRI: &str = "urn:craqle:fts:graph";
const FTS_COMPLETE_IRI: &str = "urn:craqle:fts:complete";

impl SparqlEngine {
    pub(crate) fn new(store: Arc<GraphStore>, search: Arc<SearchIndex>) -> Self {
        Self { store, search }
    }

    #[cfg(test)]
    pub(crate) fn query(&self, sparql: &str) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::All, planner_enabled())
    }

    pub(crate) fn prepare_query(&self, sparql: &str) -> Result<PreparedQuery> {
        self.prepare_with_limits(sparql, &QueryLimits::production())
    }

    pub(crate) fn prepare_with_limits(
        &self,
        sparql: &str,
        limits: &QueryLimits,
    ) -> Result<PreparedQuery> {
        Ok(parse_prepared_query(sparql, limits)?.0)
    }

    pub(crate) fn explain_prepared_graphs(
        &self,
        auth: &dyn crate::Authorizer,
        prepared: &PreparedQuery,
        graphs: &[GraphId],
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        self.explain_prepared_scope(
            prepared,
            GraphScope::List(graphs),
            None,
            options,
            Some(auth),
        )
    }

    pub(crate) fn explain_prepared_snapshot(
        &self,
        prepared: &PreparedQuery,
        policy_visible: &SnapshotVisibleFn<'_>,
        options: &QueryOptions,
    ) -> Result<QueryPlan> {
        let view = StoreReadView::with_read_mode(&self.store, options.read_mode);
        let visible = |graph: &GraphId| policy_visible(view.snapshot(), graph);
        self.explain_prepared_scope(
            prepared,
            GraphScope::Predicate(&visible),
            Some((self.store.as_ref(), policy_visible)),
            options,
            None,
        )
    }

    fn explain_prepared_scope(
        &self,
        prepared: &PreparedQuery,
        scope: GraphScope<'_>,
        post_raw_visibility: Option<(&GraphStore, &SnapshotVisibleFn<'_>)>,
        options: &QueryOptions,
        explicit_auth: Option<&dyn crate::Authorizer>,
    ) -> Result<QueryPlan> {
        enforce_query_bytes(prepared.query_bytes, &options.limits)?;
        let clock = request_clock(options, Duration::ZERO);
        let view = StoreReadView::with_read_mode(&self.store, options.read_mode);
        authorize_graph_scope(&view, scope, explicit_auth)?;
        let mut query = prepared.query.as_ref().clone();
        rewrite_fts_query(
            &mut query,
            FtsRewriteCtx {
                search: self.search.as_ref(),
                scope,
                post_raw_visibility,
                clock: &clock,
                limits: &options.limits,
            },
        )?;
        clock.check_stage()?;
        let fast_path = fast_path_plan(&query, options);
        let features = query_features(&query);
        QueryBudget::new(features.budget, options.limits, clock.clone())?;
        let planner_trace = plan_query(
            &mut query,
            PlanRequest {
                store: &self.store,
                options,
                fast_path: fast_path.as_ref(),
                graph: single_graph(scope),
                graph_distinct: false,
            },
        )?;
        let fast_path = select_fast_path(fast_path, &planner_trace);
        clock.check_stage()?;
        let features = query_features(&query);
        QueryBudget::new(features.budget, options.limits, clock.clone())?;
        let plan = explain_query_plan(
            &query,
            query_fingerprint(&query),
            &planner_trace,
            fast_path.as_ref(),
        );
        clock.check_stage()?;
        Ok(plan)
    }

    #[cfg(test)]
    pub(crate) fn query_with_graphs(
        &self,
        sparql: &str,
        graphs: &[GraphId],
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::List(graphs), planner_enabled())
    }

    #[cfg(test)]
    pub(crate) fn query_graph_mode(
        &self,
        sparql: &str,
        graphs: &[GraphId],
        read_mode: QueryReadMode,
    ) -> Result<(QueryResults, ReadStatistics)> {
        self.run_query_mode(
            sparql,
            GraphScope::List(graphs),
            planner_enabled(),
            read_mode,
        )
    }

    pub(crate) fn execute_prepared_graphs(
        &self,
        auth: &dyn crate::Authorizer,
        prepared: &PreparedQuery,
        graphs: &[GraphId],
        options: &QueryOptions,
    ) -> Result<QueryExecution> {
        self.execute_prepared_scope(
            prepared,
            GraphScope::List(graphs),
            options,
            Duration::ZERO,
            true,
            Some(auth),
        )
        .map(|(execution, _)| execution)
    }

    #[cfg(test)]
    pub(crate) fn query_with_visibility(
        &self,
        sparql: &str,
        visible: &VisibleFn<'_>,
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::Predicate(visible), planner_enabled())
    }

    pub(crate) fn query_snapshot(
        &self,
        sparql: &str,
        policy_visible: &SnapshotVisibleFn<'_>,
    ) -> Result<QueryResults> {
        let options = QueryOptions::default();
        let (prepared, parse_time) = parse_prepared_query(sparql, &options.limits)?;
        self.execute_prepared_snapshot(&prepared, policy_visible, &options, parse_time, false)
            .map(|execution| execution.results)
    }

    /// Parses and executes one query, returning the parsed query for diagnostic scoping.
    pub(crate) fn query_with_options(
        &self,
        request: QueryRun<'_>,
        policy_visible: &SnapshotVisibleFn<'_>,
    ) -> Result<(PreparedQuery, QueryExecution)> {
        let (prepared, parse_time) = parse_prepared_query(request.sparql, &request.options.limits)?;
        let execution = self.execute_prepared_snapshot(
            &prepared,
            policy_visible,
            request.options,
            parse_time,
            true,
        )?;
        Ok((prepared, execution))
    }

    pub(crate) fn query_graphs_options(
        &self,
        request: GraphQuery<'_>,
    ) -> Result<(PreparedQuery, QueryExecution)> {
        let (prepared, parse_time) = parse_prepared_query(request.sparql, &request.options.limits)?;
        let (execution, _) = self.execute_prepared_scope(
            &prepared,
            GraphScope::List(request.graphs),
            request.options,
            parse_time,
            true,
            Some(request.auth),
        )?;
        Ok((prepared, execution))
    }

    pub(crate) fn execute_prepared_snapshot(
        &self,
        prepared: &PreparedQuery,
        policy_visible: &SnapshotVisibleFn<'_>,
        options: &QueryOptions,
        parse_time: Duration,
        collect_plan_statistics: bool,
    ) -> Result<QueryExecution> {
        enforce_query_bytes(prepared.query_bytes, &options.limits)?;
        let clock = request_clock(options, parse_time);
        let mut query = prepared.query.as_ref().clone();
        let view = StoreReadView::with_read_mode(&self.store, options.read_mode);
        let visible = |graph: &GraphId| policy_visible(view.snapshot(), graph);
        let scope = GraphScope::Predicate(&visible);

        let rewrite_started = Instant::now();
        rewrite_fts_query(
            &mut query,
            FtsRewriteCtx {
                search: self.search.as_ref(),
                scope,
                post_raw_visibility: Some((self.store.as_ref(), policy_visible)),
                clock: &clock,
                limits: &options.limits,
            },
        )?;
        let rewrite_time = rewrite_started.elapsed();
        clock.check_stage()?;
        let fast_path = fast_path_plan(&query, options);
        let graph_distinct = graph_distinct_plan(&query, options, fast_path.as_ref());
        let features = query_features(&query);
        QueryBudget::new(features.budget, options.limits, clock.clone())?;
        let planning_started = Instant::now();
        let planner_trace = plan_query(
            &mut query,
            PlanRequest {
                store: &self.store,
                options,
                fast_path: fast_path.as_ref(),
                graph: single_graph(scope),
                graph_distinct: graph_distinct.is_some(),
            },
        )?;
        let fast_path = select_fast_path(fast_path, &planner_trace);
        if options.optimize {
            tracing::trace!(target: "craqle::planner", plan = %query, "craqle-optimized query");
        }
        let craqle_planning_time = planning_started.elapsed();
        clock.check_stage()?;
        let plan_fingerprint = query_fingerprint(&query);
        let logical_operator = query_logical_operator(&query);
        self.execute_query(
            query,
            scope,
            &view,
            options,
            QueryStageStatistics {
                clock,
                parse_time,
                rewrite_time,
                craqle_planning_time,
                plan_fingerprint,
                planner_trace,
                fast_path,
                graph_distinct,
                logical_operator,
            },
            collect_plan_statistics,
        )
        .map(|(execution, _)| execution)
    }

    #[cfg(test)]
    fn run_query(
        &self,
        sparql: &str,
        scope: GraphScope<'_>,
        optimize: bool,
    ) -> Result<QueryResults> {
        Ok(self
            .run_query_mode(sparql, scope, optimize, QueryReadMode::Auto)?
            .0)
    }

    #[cfg(test)]
    fn run_query_mode(
        &self,
        sparql: &str,
        scope: GraphScope<'_>,
        optimize: bool,
        read_mode: QueryReadMode,
    ) -> Result<(QueryResults, ReadStatistics)> {
        let options = QueryOptions {
            cancellation: QueryCancellation::new(),
            read_mode,
            optimize,
            join_mode: JoinMode::Auto,
            fast_paths: FastPathMode::Auto,
            collect_costs: false,
            collect_plan_statistics: true,
            limits: QueryLimits::default(),
        };
        let (prepared, parse_time) = parse_prepared_query(sparql, &options.limits)?;
        let (execution, read_statistics) =
            self.execute_prepared_scope(&prepared, scope, &options, parse_time, false, None)?;
        Ok((execution.results, read_statistics))
    }

    fn execute_prepared_scope(
        &self,
        prepared: &PreparedQuery,
        scope: GraphScope<'_>,
        options: &QueryOptions,
        parse_time: Duration,
        collect_plan_statistics: bool,
        explicit_auth: Option<&dyn crate::Authorizer>,
    ) -> Result<(QueryExecution, ReadStatistics)> {
        enforce_query_bytes(prepared.query_bytes, &options.limits)?;
        let clock = request_clock(options, parse_time);
        let view = StoreReadView::with_read_mode(&self.store, options.read_mode);
        authorize_graph_scope(&view, scope, explicit_auth)?;
        let mut query = prepared.query.as_ref().clone();
        let rewrite_started = Instant::now();
        rewrite_fts_query(
            &mut query,
            FtsRewriteCtx {
                search: self.search.as_ref(),
                scope,
                post_raw_visibility: None,
                clock: &clock,
                limits: &options.limits,
            },
        )?;
        let rewrite_time = rewrite_started.elapsed();
        clock.check_stage()?;
        let fast_path = fast_path_plan(&query, options);
        let graph_distinct = graph_distinct_plan(&query, options, fast_path.as_ref());
        let features = query_features(&query);
        QueryBudget::new(features.budget, options.limits, clock.clone())?;
        let planning_started = Instant::now();
        let planner_trace = plan_query(
            &mut query,
            PlanRequest {
                store: &self.store,
                options,
                fast_path: fast_path.as_ref(),
                graph: single_graph(scope),
                graph_distinct: graph_distinct.is_some(),
            },
        )?;
        let fast_path = select_fast_path(fast_path, &planner_trace);
        if options.optimize {
            tracing::trace!(target: "craqle::planner", plan = %query, "craqle-optimized query");
        }
        let craqle_planning_time = planning_started.elapsed();
        clock.check_stage()?;
        let plan_fingerprint = query_fingerprint(&query);
        let logical_operator = query_logical_operator(&query);
        self.execute_query(
            query,
            scope,
            &view,
            options,
            QueryStageStatistics {
                clock,
                parse_time,
                rewrite_time,
                craqle_planning_time,
                plan_fingerprint,
                planner_trace,
                fast_path,
                graph_distinct,
                logical_operator,
            },
            collect_plan_statistics,
        )
    }

    fn execute_query(
        &self,
        mut query: Query,
        scope: GraphScope<'_>,
        view: &StoreReadView<'_>,
        options: &QueryOptions,
        mut stages: QueryStageStatistics,
        collect_plan_statistics: bool,
    ) -> Result<(QueryExecution, ReadStatistics)> {
        let collect_plan_statistics = collect_plan_statistics && options.collect_plan_statistics;
        let (mut context, named_graphs) =
            scope_read_context(scope, view, options.cancellation.clone())?;
        if options.collect_costs {
            context.enable_costs();
        }
        context.check_cancelled()?;
        let features = query_features(&query);
        let budget = Arc::new(QueryBudget::new(
            features.budget,
            options.limits,
            stages.clock.clone(),
        )?);
        let mut known_terms = HashMap::new();
        if let Some(plan) = stages.graph_distinct.take()
            && context.validation_graph().is_none()
            && let Some(relation) = crate::graph_distinct::execute(
                &plan,
                crate::graph_join::JoinInput {
                    view,
                    context: &context,
                    budget: &budget,
                },
            )?
        {
            #[cfg(test)]
            GRAPH_DISTINCT_RUNS.with(|runs| runs.set(runs.get() + 1));
            let work = &relation.stats;
            tracing::debug!(target: "craqle::graph_distinct", ?work, "native graph-level relation");
            crate::graph_distinct::substitute(&plan, &mut query, relation.values);
            known_terms = relation.known;
        }
        if let Some(plan) = stages.fast_path.take() {
            let outcome = crate::sparql_fast_path::execute(&plan, view, &context, &budget)?;
            let read_statistics = context.snapshot();
            let mut statistics = build_execution_statistics(
                stages,
                read_statistics.clone(),
                outcome.execution_time,
                CollectionMetrics {
                    collection_time: outcome.collection_time,
                    first_result_time: outcome.first_result_time,
                    result_rows: outcome.result_rows,
                    result_cells: outcome.result_cells,
                    ..CollectionMetrics::default()
                },
                ExplanationMetrics {
                    intermediate_rows: outcome.intermediate_rows,
                    rows_available: true,
                    ..ExplanationMetrics::default()
                },
            );
            statistics.fast_path = Some(outcome.kind);
            statistics.plan.root.physical_operator = QueryPhysicalOperator::FastPath(outcome.kind);
            return Ok((
                QueryExecution {
                    results: outcome.results,
                    statistics,
                },
                read_statistics,
            ));
        }

        let mut evaluator = QueryEvaluator::new().with_cancellation_token(stages.clock.evaluator());
        if collect_plan_statistics {
            #[cfg(test)]
            DETAILED_RUNS.with(|runs| runs.set(runs.get() + 1));
            evaluator = evaluator.compute_statistics();
        }
        let mut prepared = evaluator.prepare(&query);
        let default_union_marker = BlankNode::default();
        let source_default_graphs = if matches!(options.read_mode, QueryReadMode::ForceSource)
            || !view.query_ids_trusted(&context)?
        {
            match scope {
                GraphScope::List(graphs) => {
                    let mut default_graphs = Vec::with_capacity(graphs.len());
                    for graph in graphs {
                        if view.contains_graph(graph)? {
                            default_graphs.push(GraphName::NamedNode(graph.0.clone()));
                        }
                    }
                    Some(default_graphs)
                }
                #[cfg(test)]
                GraphScope::All => None,
                GraphScope::Predicate(_) => None,
            }
        } else {
            None
        };
        if let Some(source_default_graphs) = source_default_graphs {
            prepared
                .dataset_mut()
                .set_default_graph(source_default_graphs);
        } else {
            prepared
                .dataset_mut()
                .set_default_graph(vec![GraphName::BlankNode(default_union_marker.clone())]);
        }
        if let Some(named_graphs) = named_graphs {
            prepared
                .dataset_mut()
                .set_available_named_graphs(named_graphs);
        }
        let execution_started = Instant::now();
        let (results, explanation) = prepared.explain(
            StoreDataset::with_query_budget(
                view,
                &context,
                default_union_marker,
                Arc::clone(&budget),
            )
            .with_known_terms(known_terms),
        );
        let initial_execution_time = execution_started.elapsed();
        let results = results.map_err(|error| map_eval_error(error, &stages.clock))?;
        let (results, collection) = collect_query_results(
            results,
            execution_started,
            &context,
            &budget,
            collect_plan_statistics || options.collect_costs,
        )?;
        let read_statistics = context.snapshot();
        let explanation_metrics = if collect_plan_statistics {
            read_explanation_metrics(&explanation)?
        } else {
            ExplanationMetrics::default()
        };
        let statistics = build_execution_statistics(
            stages,
            read_statistics.clone(),
            initial_execution_time,
            collection,
            explanation_metrics,
        );
        Ok((
            QueryExecution {
                results,
                statistics,
            },
            read_statistics,
        ))
    }

    pub(crate) fn evaluate_update(
        &self,
        auth: &dyn crate::Authorizer,
        sparql: &str,
        options: &UpdateOptions,
    ) -> Result<Vec<MaterializedQuadChange>> {
        if sparql.len() > options.limits.max_update_bytes {
            return Err(SparqlError::QueryLimit {
                resource: "update bytes",
                limit: options.limits.max_update_bytes,
            });
        }
        check_query_shape(sparql)?;
        let started = Instant::now();
        let cancellation = QueryCancellation::new();
        let clock = RequestClock::start(options.limits.deadline, cancellation.clone(), started);
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let update = SparqlParser::new()
            .parse_update(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;
        clock.check_stage()?;

        let view = StoreReadView::new(&self.store);
        let readable_graphs = readable_update_graphs(&view, auth)?;
        let mut changes = Vec::new();
        let mut changed_graphs = HashSet::new();
        for operation in &update.operations {
            clock.check_stage()?;
            match operation {
                GraphUpdateOperation::InsertData { data } => {
                    for quad in data {
                        let change = quad_to_insert(quad)?;
                        authorize_materialized_change(&view, auth, &change)?;
                        push_update_change(
                            &mut changes,
                            &mut changed_graphs,
                            change,
                            &options.limits,
                            started,
                        )?;
                    }
                }
                GraphUpdateOperation::DeleteData { data } => {
                    for quad in data {
                        let change = ground_quad_delete(quad)?;
                        authorize_materialized_change(&view, auth, &change)?;
                        push_update_change(
                            &mut changes,
                            &mut changed_graphs,
                            change,
                            &options.limits,
                            started,
                        )?;
                    }
                }
                GraphUpdateOperation::DeleteInsert {
                    delete,
                    insert,
                    using,
                    pattern,
                } => {
                    authorize_update_dataset(&view, auth, using.as_ref())?;
                    authorize_update_pattern(&view, auth, pattern)?;
                    for quad in delete {
                        authorize_template_graph(&view, auth, &quad.graph_name)?;
                    }
                    for quad in insert {
                        authorize_template_graph(&view, auth, &quad.graph_name)?;
                    }
                    let template_width = delete.len().saturating_add(insert.len()).max(1);
                    let max_materialized_quads = options
                        .limits
                        .max_materialized_bindings
                        .saturating_mul(template_width);
                    let features = pattern_features(pattern);
                    // A statically known product must fit what this update is
                    // allowed to materialize at all.
                    if features.estimated_rows > max_materialized_quads {
                        return Err(SparqlError::QueryLimit {
                            resource: "materialized update bindings",
                            limit: options.limits.max_materialized_bindings,
                        });
                    }
                    let budget = Arc::new(QueryBudget::new(
                        features.budget,
                        update_read_limits(&options.limits),
                        clock.clone(),
                    )?);
                    let evaluator =
                        QueryEvaluator::new().with_cancellation_token(clock.evaluator());
                    let mut prepared = evaluator.prepare_delete_insert(
                        delete.clone(),
                        insert.clone(),
                        None,
                        using.clone(),
                        pattern,
                    );
                    let default_union_marker = BlankNode::default();
                    if using.is_none() {
                        prepared
                            .dataset_mut()
                            .set_default_graph(vec![GraphName::BlankNode(
                                default_union_marker.clone(),
                            )]);
                    }
                    let context = ReadContext::with_visible_graphs(
                        cancellation.clone(),
                        readable_graphs.iter().cloned(),
                    );
                    let iter = prepared
                        .execute(StoreDataset::with_query_budget(
                            &view,
                            &context,
                            default_union_marker,
                            Arc::clone(&budget),
                        ))
                        .map_err(|error| map_eval_error(error, &clock))?;

                    let mut materialized_quads = 0_usize;
                    for quad in iter {
                        budget.check()?;
                        materialized_quads = materialized_quads.saturating_add(1);
                        if materialized_quads > max_materialized_quads {
                            return Err(SparqlError::QueryLimit {
                                resource: "materialized update bindings",
                                limit: options.limits.max_materialized_bindings,
                            });
                        }
                        let change = update_quad_change(
                            quad.map_err(|error| map_eval_error(error, &clock))?,
                        )?;
                        authorize_materialized_change(&view, auth, &change)?;
                        push_update_change(
                            &mut changes,
                            &mut changed_graphs,
                            change,
                            &options.limits,
                            started,
                        )?;
                    }
                }
                GraphUpdateOperation::Clear { graph, .. }
                | GraphUpdateOperation::Drop { graph, .. } => {
                    let target_graphs =
                        update_target_graphs(&self.store, graph, options.limits.max_graphs)?;
                    for graph in &target_graphs {
                        authorize_update_graph(&view, auth, graph, crate::Action::Write, false)?;
                    }
                    materialize_removals(
                        &self.store,
                        target_graphs,
                        &mut changes,
                        &mut changed_graphs,
                        &options.limits,
                        started,
                    )?;
                }
                GraphUpdateOperation::Create { graph, .. } => {
                    let graph = GraphId(graph.clone());
                    authorize_update_graph(&view, auth, &graph, crate::Action::Write, false)?;
                    if self.store.contains_graph(&graph)? {
                        continue;
                    }
                    return Err(SparqlError::Unsupported(
                        "CREATE is not supported because the write pipeline only materializes quad deltas".into(),
                    ));
                }
                GraphUpdateOperation::Load { .. } => {
                    return Err(SparqlError::Unsupported(
                        "LOAD is not supported by the local materialized-delta pipeline".into(),
                    ));
                }
            }
        }

        Ok(changes)
    }
}

/// Query-shaped limits for an update's read side, under the update deadline.
fn update_read_limits(limits: &UpdateLimits) -> QueryLimits {
    if limits.is_unbounded() {
        return QueryLimits::unbounded();
    }
    QueryLimits {
        deadline: limits.deadline,
        ..QueryLimits::production()
    }
}

fn check_update_deadline(started: Instant, limits: &UpdateLimits) -> Result<()> {
    if limits
        .deadline
        .is_some_and(|deadline| started.elapsed() >= deadline)
    {
        return Err(SparqlError::QueryLimit {
            resource: "update deadline",
            limit: 0,
        });
    }
    Ok(())
}

fn push_update_change(
    changes: &mut Vec<MaterializedQuadChange>,
    graphs: &mut HashSet<GraphId>,
    change: MaterializedQuadChange,
    limits: &UpdateLimits,
    started: Instant,
) -> Result<()> {
    check_update_deadline(started, limits)?;
    if changes.len() >= limits.max_changes {
        return Err(SparqlError::QueryLimit {
            resource: "update changes",
            limit: limits.max_changes,
        });
    }
    let graph = match &change {
        MaterializedQuadChange::Insert { graph, .. }
        | MaterializedQuadChange::Delete { graph, .. } => graph,
    };
    graphs.insert(graph.clone());
    if graphs.len() > limits.max_graphs {
        return Err(SparqlError::QueryLimit {
            resource: "update graphs",
            limit: limits.max_graphs,
        });
    }
    changes.push(change);
    Ok(())
}

fn authorize_update_graph(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    graph: &GraphId,
    action: crate::Action,
    deny_missing: bool,
) -> Result<()> {
    let policy = view.snapshot().graph_policy(view.store(), graph)?;
    if deny_missing && policy.is_none() {
        return Err(crate::AuthorizationError::PermissionDenied {
            action,
            graph: graph.as_str().to_owned(),
        }
        .into());
    }
    auth.authorize(graph, &policy.unwrap_or_default(), action)?;
    Ok(())
}

fn authorize_materialized_change(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    change: &MaterializedQuadChange,
) -> Result<()> {
    let graph = match change {
        MaterializedQuadChange::Insert { graph, .. }
        | MaterializedQuadChange::Delete { graph, .. } => graph,
    };
    authorize_update_graph(view, auth, graph, crate::Action::Write, false)
}

fn readable_update_graphs(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
) -> Result<HashSet<GraphId>> {
    let mut readable = HashSet::new();
    for graph in view.store().graphs()? {
        let Some(policy) = view.snapshot().graph_policy(view.store(), &graph)? else {
            continue;
        };
        match auth.authorize(&graph, &policy, crate::Action::Read) {
            Ok(()) => {
                readable.insert(graph);
            }
            Err(crate::AuthorizationError::PermissionDenied { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(readable)
}

fn authorize_update_dataset(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    dataset: Option<&spargebra::algebra::QueryDataset>,
) -> Result<()> {
    let Some(dataset) = dataset else {
        return Ok(());
    };
    for graph in &dataset.default {
        authorize_update_graph(
            view,
            auth,
            &GraphId(graph.clone()),
            crate::Action::Read,
            true,
        )?;
    }
    if let Some(named) = &dataset.named {
        for graph in named {
            authorize_update_graph(
                view,
                auth,
                &GraphId(graph.clone()),
                crate::Action::Read,
                true,
            )?;
        }
    }
    Ok(())
}

fn authorize_template_graph(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    graph: &GraphNamePattern,
) -> Result<()> {
    match graph {
        GraphNamePattern::NamedNode(graph) => authorize_update_graph(
            view,
            auth,
            &GraphId(graph.clone()),
            crate::Action::Write,
            false,
        ),
        GraphNamePattern::DefaultGraph => Err(SparqlError::Unsupported(
            "default graph updates are not supported; use GRAPH <iri> { ... }".into(),
        )),
        GraphNamePattern::Variable(_) => Ok(()),
    }
}

fn authorize_update_pattern(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    pattern: &GraphPattern,
) -> Result<()> {
    match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            Ok(())
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            authorize_update_pattern(view, auth, left)?;
            authorize_update_pattern(view, auth, right)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            authorize_update_pattern(view, auth, left)?;
            authorize_update_pattern(view, auth, right)?;
            if let Some(expression) = expression {
                authorize_update_expression(view, auth, expression)?;
            }
            Ok(())
        }
        GraphPattern::Filter { expr, inner } => {
            authorize_update_pattern(view, auth, inner)?;
            authorize_update_expression(view, auth, expr)
        }
        GraphPattern::Graph { name, inner } => {
            if let NamedNodePattern::NamedNode(graph) = name {
                authorize_update_graph(
                    view,
                    auth,
                    &GraphId(graph.clone()),
                    crate::Action::Read,
                    true,
                )?;
            }
            authorize_update_pattern(view, auth, inner)
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            authorize_update_pattern(view, auth, inner)?;
            authorize_update_expression(view, auth, expression)
        }
        GraphPattern::OrderBy { inner, expression } => {
            authorize_update_pattern(view, auth, inner)?;
            for expression in expression {
                let expression = match expression {
                    spargebra::algebra::OrderExpression::Asc(expression)
                    | spargebra::algebra::OrderExpression::Desc(expression) => expression,
                };
                authorize_update_expression(view, auth, expression)?;
            }
            Ok(())
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => authorize_update_pattern(view, auth, inner),
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            authorize_update_pattern(view, auth, inner)?;
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    authorize_update_expression(view, auth, expr)?;
                }
            }
            Ok(())
        }
        GraphPattern::Service { .. } => Err(SparqlError::Unsupported(
            "SERVICE is not supported by SPARQL Update".into(),
        )),
        #[allow(unreachable_patterns)]
        _ => Err(SparqlError::Unsupported(
            "unsupported SPARQL Update graph pattern".into(),
        )),
    }
}

fn authorize_update_expression(
    view: &StoreReadView<'_>,
    auth: &dyn crate::Authorizer,
    expression: &Expression,
) -> Result<()> {
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => Ok(()),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            authorize_update_expression(view, auth, left)?;
            authorize_update_expression(view, auth, right)
        }
        Expression::In(left, right) => {
            authorize_update_expression(view, auth, left)?;
            for expression in right {
                authorize_update_expression(view, auth, expression)?;
            }
            Ok(())
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            authorize_update_expression(view, auth, inner)
        }
        Expression::Exists(pattern) => authorize_update_pattern(view, auth, pattern),
        Expression::If(condition, left, right) => {
            authorize_update_expression(view, auth, condition)?;
            authorize_update_expression(view, auth, left)?;
            authorize_update_expression(view, auth, right)
        }
        Expression::Coalesce(expressions) | Expression::FunctionCall(_, expressions) => {
            for expression in expressions {
                authorize_update_expression(view, auth, expression)?;
            }
            Ok(())
        }
        #[allow(unreachable_patterns)]
        _ => Err(SparqlError::Unsupported(
            "unsupported SPARQL Update expression".into(),
        )),
    }
}

fn update_target_graphs(
    store: &GraphStore,
    target: &GraphTarget,
    max_graphs: usize,
) -> Result<Vec<GraphId>> {
    match target {
        GraphTarget::NamedNode(graph) => Ok(vec![GraphId(graph.clone())]),
        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => {
            let mut graphs = Vec::new();
            for graph_id in store.graph_term_iter() {
                if graphs.len() >= max_graphs {
                    return Err(SparqlError::QueryLimit {
                        resource: "update graphs",
                        limit: max_graphs,
                    });
                }
                let term = store.decode_term(graph_id?)?;
                if let Some(graph) = term.to_named_node() {
                    graphs.push(GraphId(graph));
                }
            }
            Ok(graphs)
        }
        GraphTarget::DefaultGraph => Err(SparqlError::Unsupported(
            "default graph updates are not supported; use GRAPH <iri>".into(),
        )),
    }
}

fn scope_read_context<'scope>(
    scope: GraphScope<'scope>,
    view: &StoreReadView<'_>,
    cancellation: QueryCancellation,
) -> Result<(ReadContext<'scope>, Option<Vec<GraphNode>>)> {
    match scope {
        #[cfg(test)]
        GraphScope::All => Ok((ReadContext::new(cancellation), None)),
        GraphScope::Predicate(visible) => {
            // Union view with lazy visibility: the predicate runs at most once
            // per touched graph.
            Ok((
                ReadContext::with_graph_visibility(cancellation, visible),
                None,
            ))
        }
        GraphScope::List(graphs) if graphs.len() <= EXPLICIT_GRAPH_LIMIT => {
            // Named-graph enumeration uses the metadata record, while default
            // patterns retain the sentinel union selected by the evaluator.
            let mut seen = HashSet::with_capacity(graphs.len());
            let mut names: Vec<NamedNode> = Vec::with_capacity(graphs.len());
            for graph in graphs {
                if seen.insert(graph.as_str()) && view.contains_graph(graph)? {
                    names.push(graph.0.clone());
                }
            }
            Ok((
                ReadContext::with_visible_graphs(cancellation, graphs.iter().cloned()),
                Some(names.into_iter().map(Into::into).collect()),
            ))
        }
        GraphScope::List(graphs) => Ok((
            ReadContext::with_visible_graphs(cancellation, graphs.iter().cloned()),
            None,
        )),
    }
}

fn authorize_graph_scope(
    view: &StoreReadView<'_>,
    scope: GraphScope<'_>,
    auth: Option<&dyn crate::Authorizer>,
) -> Result<()> {
    let (GraphScope::List(graphs), Some(auth)) = (scope, auth) else {
        return Ok(());
    };
    let mut seen = HashSet::with_capacity(graphs.len());
    for graph in graphs {
        if !seen.insert(graph.as_str()) {
            continue;
        }
        let Some(policy) = view.snapshot().graph_policy(view.store(), graph)? else {
            return Err(crate::AuthorizationError::PermissionDenied {
                action: crate::Action::Read,
                graph: graph.as_str().to_owned(),
            }
            .into());
        };
        auth.authorize(graph, &policy, crate::Action::Read)?;
    }
    Ok(())
}

struct QueryStageStatistics {
    clock: RequestClock,
    parse_time: Duration,
    rewrite_time: Duration,
    craqle_planning_time: Duration,
    plan_fingerprint: String,
    planner_trace: PlannerTrace,
    fast_path: Option<FastPathPlan>,
    graph_distinct: Option<crate::graph_distinct::GraphDistinctPlan>,
    logical_operator: QueryLogicalOperator,
}

#[derive(Default)]
struct ExplanationMetrics {
    planning_time: Duration,
    intermediate_rows: u64,
    rows_available: bool,
    plan: Option<QueryPlanNode>,
}

#[derive(Default)]
struct CollectionMetrics {
    execution_time: Duration,
    collection_time: Duration,
    first_result_time: Option<Duration>,
    result_rows: u64,
    result_cells: u64,
}

/// Starts the request deadline with the already measured parse time charged.
fn request_clock(options: &QueryOptions, parse_time: Duration) -> RequestClock {
    let now = Instant::now();
    RequestClock::start(
        options.limits.deadline,
        options.cancellation.clone(),
        now.checked_sub(parse_time).unwrap_or(now),
    )
}

fn parse_prepared_query(sparql: &str, limits: &QueryLimits) -> Result<(PreparedQuery, Duration)> {
    enforce_query_bytes(sparql.len(), limits)?;
    check_query_shape(sparql)?;
    let started = Instant::now();
    let full = format!("{COMMON_PREFIXES}{sparql}");
    let query = SparqlParser::new()
        .parse_query(&full)
        .map_err(|error| SparqlError::Parse(error.to_string()))?;
    Ok((
        PreparedQuery {
            query: Arc::new(query),
            query_bytes: sparql.len(),
            source_fingerprint: blake3::hash(sparql.as_bytes()).to_hex().as_str().into(),
        },
        started.elapsed(),
    ))
}

/// Nesting the parser is allowed to build. Mid-parse cancellation does not
/// exist, so the input shape is bounded before parsing starts.
const MAX_PARSE_DEPTH: usize = 64;

fn check_query_shape(sparql: &str) -> Result<()> {
    let bytes = sparql.as_bytes();
    let mut index = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut iri = false;
    let mut comment = false;
    let mut depth = 0_usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if comment {
            comment = byte != b'\n';
        } else if let Some((delimiter, width)) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == delimiter
                && (width == 1
                    || (bytes.get(index + 1) == Some(&delimiter)
                        && bytes.get(index + 2) == Some(&delimiter)))
            {
                quote = None;
                index += width - 1;
            }
        } else if iri {
            iri = byte != b'>';
        } else if byte == b'#' {
            comment = true;
        } else if byte == b'\'' || byte == b'"' {
            let width =
                if bytes.get(index + 1) == Some(&byte) && bytes.get(index + 2) == Some(&byte) {
                    3
                } else {
                    1
                };
            quote = Some((byte, width));
            index += width - 1;
        } else if byte == b'<' && bytes.get(index + 1) == Some(&b'<') {
            return Err(crate::UnsupportedRdfStarTerm {
                term: "SPARQL quoted triple".to_owned(),
            }
            .into());
        } else if byte == b'<' {
            iri = true;
        } else if matches!(byte, b'{' | b'(' | b'[') {
            depth += 1;
            if depth > MAX_PARSE_DEPTH {
                return Err(SparqlError::QueryLimit {
                    resource: "query nesting depth",
                    limit: MAX_PARSE_DEPTH,
                });
            }
        } else if matches!(byte, b'}' | b')' | b']') {
            depth = depth.saturating_sub(1);
        }
        index += 1;
    }
    Ok(())
}

fn enforce_query_bytes(bytes: usize, limits: &QueryLimits) -> Result<()> {
    if bytes > limits.max_query_bytes {
        return Err(SparqlError::QueryLimit {
            resource: "query bytes",
            limit: limits.max_query_bytes,
        });
    }
    Ok(())
}

fn query_fingerprint(query: &Query) -> String {
    blake3::hash(query.to_string().as_bytes())
        .to_hex()
        .to_string()
}

/// What one planning pass may read: statistics, options, the admitted fast path, and scope.
struct PlanRequest<'a> {
    store: &'a GraphStore,
    options: &'a QueryOptions,
    fast_path: Option<&'a FastPathPlan>,
    /// The one explicitly selected graph, whose own counters replace store-wide ones.
    graph: Option<TermId>,
    /// A native graph-level plan replaces the subtree the planner would reorder.
    graph_distinct: bool,
}

/// The only graph an explicit scope selects, after removing duplicate names.
fn single_graph(scope: GraphScope<'_>) -> Option<TermId> {
    let GraphScope::List([first, rest @ ..]) = scope else {
        return None;
    };
    rest.iter()
        .all(|graph| graph == first)
        .then(|| crate::store::hash_term(&EncodedTerm::from_named_node(&first.0)))
}

fn plan_query(query: &mut Query, request: PlanRequest<'_>) -> Result<PlannerTrace> {
    let PlanRequest {
        store,
        options,
        fast_path,
        graph,
        graph_distinct,
    } = request;
    if (graph_distinct || fast_path.is_some_and(|plan| !plan.is_hash_join()))
        && matches!(options.join_mode, JoinMode::Auto)
    {
        return Ok(PlannerTrace::default());
    }
    if matches!(options.join_mode, JoinMode::ForcePropertyStar) {
        return if fast_path.is_some_and(FastPathPlan::is_property_star) {
            Ok(PlannerTrace::default())
        } else {
            Err(SparqlError::Planning(
                "forced join mode ForcePropertyStar cannot represent this query".to_owned(),
            ))
        };
    }
    if !options.optimize {
        return if matches!(options.join_mode, JoinMode::Auto) {
            Ok(PlannerTrace::default())
        } else {
            Err(SparqlError::Planning(format!(
                "forced join mode {:?} requires query optimization",
                options.join_mode
            )))
        };
    }
    crate::planner::optimize_with_costs(
        query,
        store,
        crate::planner::PlanMode {
            join: options.join_mode,
            collect_costs: options.collect_costs,
            graph,
        },
    )
    .map_err(|error| SparqlError::Planning(error.to_string()))
}

fn fast_path_plan(query: &Query, options: &QueryOptions) -> Option<FastPathPlan> {
    if matches!(options.fast_paths, FastPathMode::Disabled) {
        return None;
    }
    let plan = crate::sparql_fast_path::analyze(query)?;
    match options.join_mode {
        JoinMode::Auto => Some(plan),
        JoinMode::ForceHash if plan.is_hash_join() => Some(plan),
        JoinMode::ForcePropertyStar if plan.is_property_star() => Some(plan),
        JoinMode::ForceLateral | JoinMode::ForceHash | JoinMode::ForcePropertyStar => None,
    }
}

fn graph_distinct_plan(
    query: &Query,
    options: &QueryOptions,
    fast_path: Option<&FastPathPlan>,
) -> Option<crate::graph_distinct::GraphDistinctPlan> {
    if fast_path.is_some()
        || matches!(options.fast_paths, FastPathMode::Disabled)
        || !matches!(options.join_mode, JoinMode::Auto)
    {
        return None;
    }
    crate::graph_distinct::analyze(query)
}

fn select_fast_path(
    plan: Option<FastPathPlan>,
    planner_trace: &PlannerTrace,
) -> Option<FastPathPlan> {
    match plan {
        Some(plan) if plan.is_hash_join() => planner_trace
            .joins
            .iter()
            .any(|join| join.physical_operator == JoinKind::Hash)
            .then_some(plan),
        plan => plan,
    }
}

fn read_explanation_metrics(
    explanation: &spareval::QueryExplanation,
) -> Result<ExplanationMetrics> {
    let mut output = Vec::new();
    explanation
        .write_in_json(&mut output)
        .map_err(|error| SparqlError::Evaluation(error.to_string()))?;
    let value: serde_json::Value = serde_json::from_slice(&output)
        .map_err(|error| SparqlError::Evaluation(error.to_string()))?;
    let planning_time = value
        .get("planning duration in seconds")
        .and_then(serde_json::Value::as_f64)
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or_default();
    let intermediate_rows = value
        .get("plan")
        .map(|plan| explanation_descendant_rows(plan, true))
        .unwrap_or_default();
    let plan = value.get("plan").map(explanation_plan_node);
    Ok(ExplanationMetrics {
        planning_time,
        intermediate_rows,
        rows_available: true,
        plan,
    })
}

fn explanation_plan_node(node: &serde_json::Value) -> QueryPlanNode {
    let actual_rows = node
        .get("number of results")
        .and_then(serde_json::Value::as_u64);
    let elapsed_time = node
        .get("duration in seconds")
        .and_then(serde_json::Value::as_f64)
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or_default();
    QueryPlanNode {
        logical_operator: QueryLogicalOperator::Evaluation,
        physical_operator: QueryPhysicalOperator::Evaluator(
            node.get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        ),
        actual_rows,
        output_rows: actual_rows.unwrap_or_default(),
        elapsed_time,
        children: node
            .get("children")
            .and_then(serde_json::Value::as_array)
            .map(|children| children.iter().map(explanation_plan_node).collect())
            .unwrap_or_default(),
        ..QueryPlanNode::default()
    }
}

fn explanation_descendant_rows(node: &serde_json::Value, root: bool) -> u64 {
    let own = if root {
        0
    } else {
        node.get("number of results")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
    };
    node.get("children")
        .and_then(serde_json::Value::as_array)
        .map(|children| {
            children.iter().fold(own, |total, child| {
                total.saturating_add(explanation_descendant_rows(child, false))
            })
        })
        .unwrap_or(own)
}

fn build_execution_statistics(
    stages: QueryStageStatistics,
    reads: ReadStatistics,
    initial_execution_time: Duration,
    collection: CollectionMetrics,
    explanation: ExplanationMetrics,
) -> QueryExecutionStatistics {
    let planner_costs = stages.planner_trace.costs;
    let execution_time = initial_execution_time
        .saturating_sub(explanation.planning_time)
        .saturating_add(collection.execution_time);
    let estimated_rows = stages
        .planner_trace
        .joins
        .last()
        .map(|join| join.estimated_output_rows);
    let mut plan_children: Vec<_> = stages
        .planner_trace
        .joins
        .iter()
        .map(planned_join_node)
        .collect();
    if let Some(plan) = explanation.plan {
        plan_children.push(plan);
    }
    let plan = QueryPlan {
        fingerprint: stages.plan_fingerprint.clone(),
        root: QueryPlanNode {
            logical_operator: stages.logical_operator,
            physical_operator: QueryPhysicalOperator::Generic,
            access_paths: reads.selected_access_paths.clone(),
            estimated_rows,
            actual_rows: Some(collection.result_rows),
            index_seeks: reads.index_seeks,
            candidate_rows: reads.candidate_quads,
            output_rows: collection.result_rows,
            elapsed_time: execution_time.saturating_add(collection.collection_time),
            children: plan_children,
        },
    };
    QueryExecutionStatistics {
        parse_time: stages.parse_time,
        rewrite_time: stages.rewrite_time,
        planning_time: stages
            .craqle_planning_time
            .saturating_add(explanation.planning_time),
        execution_time,
        result_collection_time: collection.collection_time,
        time_to_first_internal_result: collection.first_result_time,
        fast_path: None,
        planned_joins: stages.planner_trace.joins,
        selected_access_paths: reads.selected_access_paths,
        plan_fingerprint: stages.plan_fingerprint,
        index_seeks: reads.index_seeks,
        qv_admission_checks: reads.qv_admission_checks,
        qv_header_reads: reads.qv_header_reads,
        qv_counter_reads: reads.qv_counter_reads,
        qv_trusted: reads.qv_trusted,
        query_id_generation: reads.query_id_generation,
        fallback_reason: reads.fallback_reason,
        source_keys_read: reads.source_keys_read,
        source_bytes_read: reads.source_bytes_read,
        qv_keys_read: reads.qv_keys_read,
        qv_bytes_read: reads.qv_bytes_read,
        reverse_mapping_reads: reads
            .reverse_mapping_reads
            .saturating_add(planner_costs.reverse_mapping_reads),
        reverse_mapping_bytes: reads
            .reverse_mapping_bytes
            .saturating_add(planner_costs.reverse_mapping_bytes),
        forward_mapping_reads: reads
            .forward_mapping_reads
            .saturating_add(planner_costs.forward_mapping_reads),
        forward_mapping_bytes: reads
            .forward_mapping_bytes
            .saturating_add(planner_costs.forward_mapping_bytes),
        planner_index_entries: planner_costs.planner_index_entries,
        planner_point_reads: planner_costs.planner_point_reads,
        planner_cache_hits: planner_costs.planner_cache_hits,
        planner_cache_misses: planner_costs.planner_cache_misses,
        planner_memo_hits: planner_costs.planner_memo_hits,
        planner_memo_misses: planner_costs.planner_memo_misses,
        candidate_quads: reads.candidate_quads,
        matching_quads: reads.matching_quads,
        graphs_considered: reads.graphs_considered,
        orphan_checks: reads.orphan_checks,
        duplicate_groups: reads.duplicate_groups,
        duplicate_copies_skipped: reads.duplicate_copies_skipped,
        key_fields_extracted: reads.key_fields_extracted,
        authoritative_terms_decoded: reads.authoritative_terms_decoded,
        result_terms_decoded: reads.result_terms_decoded,
        encoded_quad_constructions: reads.encoded_quad_constructions,
        terms_decoded: reads.terms_decoded,
        intermediate_rows: explanation.intermediate_rows,
        intermediate_rows_available: explanation.rows_available,
        result_rows: collection.result_rows,
        result_cells: collection.result_cells,
        plan,
    }
}

fn query_logical_operator(query: &Query) -> QueryLogicalOperator {
    match query {
        Query::Ask { .. } => QueryLogicalOperator::Ask,
        Query::Select { .. } => QueryLogicalOperator::Select,
        Query::Construct { .. } => QueryLogicalOperator::Construct,
        Query::Describe { .. } => QueryLogicalOperator::Describe,
    }
}

fn explain_query_plan(
    query: &Query,
    fingerprint: String,
    planner_trace: &PlannerTrace,
    fast_path: Option<&FastPathPlan>,
) -> QueryPlan {
    QueryPlan {
        fingerprint,
        root: QueryPlanNode {
            logical_operator: query_logical_operator(query),
            physical_operator: fast_path.map_or(QueryPhysicalOperator::Generic, |plan| {
                QueryPhysicalOperator::FastPath(plan.kind())
            }),
            estimated_rows: planner_trace
                .joins
                .last()
                .map(|join| join.estimated_output_rows),
            children: planner_trace.joins.iter().map(planned_join_node).collect(),
            ..QueryPlanNode::default()
        },
    }
}

fn planned_join_node(join: &PlannedJoin) -> QueryPlanNode {
    QueryPlanNode {
        logical_operator: QueryLogicalOperator::Join,
        physical_operator: QueryPhysicalOperator::PlannedJoin(join.physical_operator),
        estimated_rows: Some(join.estimated_output_rows),
        ..QueryPlanNode::default()
    }
}

#[derive(Debug, Clone)]
enum FtsSubjectPattern {
    Variable(Variable),
    NamedNode(NamedNode),
}

#[derive(Debug, Clone)]
enum FtsGraphBinding {
    Variable(Variable),
    Fixed(NamedNode),
}

#[derive(Debug, Clone, Default)]
struct FtsServiceSpec {
    subject: Option<FtsSubjectPattern>,
    query: Option<String>,
    limit: usize,
    /// The caller asked for more than [`crate::MAX_SEARCH_LIMIT`] hits.
    limit_clamped: bool,
    score_var: Option<Variable>,
    graph: Option<FtsGraphBinding>,
    /// Every match within the intermediate-row budget, unranked, instead of the top hits.
    complete: bool,
    limit_set: bool,
}

/// Everything the FTS SERVICE rewrite needs: the index it reads and the
/// caller's graph scope.
#[derive(Clone, Copy)]
struct FtsRewriteCtx<'a> {
    search: &'a SearchIndex,
    scope: GraphScope<'a>,
    post_raw_visibility: Option<(&'a GraphStore, &'a SnapshotVisibleFn<'a>)>,
    clock: &'a RequestClock,
    limits: &'a QueryLimits,
}

fn rewrite_fts_query(query: &mut Query, cx: FtsRewriteCtx<'_>) -> Result<()> {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Construct { pattern, .. } => {
            let current = std::mem::replace(pattern, GraphPattern::Bgp { patterns: vec![] });
            *pattern = rewrite_graph_pattern(current, cx)?;
        }
    }
    Ok(())
}

fn rewrite_graph_pattern(pattern: GraphPattern, cx: FtsRewriteCtx<'_>) -> Result<GraphPattern> {
    Ok(match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            pattern
        }
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_graph_pattern(*left, cx)?),
            right: Box::new(rewrite_graph_pattern(*right, cx)?),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_graph_pattern(*left, cx)?),
            right: Box::new(rewrite_graph_pattern(*right, cx)?),
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_graph_pattern(*left, cx)?),
            right: Box::new(rewrite_graph_pattern(*right, cx)?),
        },
        GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
            left: Box::new(rewrite_graph_pattern(*left, cx)?),
            right: Box::new(rewrite_graph_pattern(*right, cx)?),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_graph_pattern(*left, cx)?),
            right: Box::new(rewrite_graph_pattern(*right, cx)?),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
            variables,
            aggregates,
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => match name {
            NamedNodePattern::NamedNode(node) if node.as_str() == FTS_SERVICE_IRI => {
                rewrite_fts_service(*inner, cx)?
            }
            other => GraphPattern::Service {
                name: other,
                inner: Box::new(rewrite_graph_pattern(*inner, cx)?),
                silent,
            },
        },
    })
}

/// Bounds graph-policy memoization for one pinned SERVICE request.
struct FtsGraphVisibility<'a> {
    scope: GraphScope<'a>,
    listed: Option<HashSet<&'a str>>,
    memo: RefCell<crate::cache::BoundedCache<String, bool>>,
}

impl<'a> FtsGraphVisibility<'a> {
    fn new(scope: GraphScope<'a>, bytes: usize) -> Result<Self> {
        let listed = match scope {
            GraphScope::List(graphs) => {
                let needed = graphs.len().saturating_mul(std::mem::size_of::<&str>() * 4);
                if needed > bytes {
                    return Err(SparqlError::QueryLimit {
                        resource: "fts graph scope bytes",
                        limit: bytes,
                    });
                }
                Some(graphs.iter().map(GraphId::as_str).collect())
            }
            _ => None,
        };
        Ok(Self {
            scope,
            listed,
            memo: RefCell::new(crate::cache::BoundedCache::new(1_024, bytes)),
        })
    }

    fn allows(&self, graph_iri: &str) -> bool {
        match self.scope {
            #[cfg(test)]
            GraphScope::All => true,
            GraphScope::List(_) => self
                .listed
                .as_ref()
                .is_some_and(|listed| listed.contains(graph_iri)),
            GraphScope::Predicate(visible) => {
                if let Some(allowed) = self.memo.borrow_mut().get_cloned(graph_iri) {
                    return allowed;
                }
                let allowed = visible(&GraphId::new(graph_iri));
                self.memo.borrow_mut().insert(
                    graph_iri.to_owned(),
                    allowed,
                    graph_iri.len().saturating_mul(2),
                );
                allowed
            }
        }
    }
}

struct FtsHitFilter<'a> {
    visibility: &'a FtsGraphVisibility<'a>,
    post_raw_visibility: Option<(&'a GraphStore, &'a SnapshotVisibleFn<'a>)>,
    subject: Option<&'a str>,
}

/// One FTS SERVICE lookup: what to search for, how many rows the caller asked
/// for, and which hits it is allowed to keep.
struct FtsSearchRequest<'a> {
    query: &'a str,
    limit: usize,
    clock: &'a RequestClock,
    /// A filled page under a clamped limit is an incomplete answer.
    clamped: bool,
    /// Every match up to `limit`; more matches are an error, never a truncated answer.
    complete: bool,
    /// `Some` restricts the index query to a single, already-visible graph.
    graph: Option<&'a str>,
    filter: FtsHitFilter<'a>,
}

/// Authorizes before ranking and checks current raw policies before returning rows.
fn search_visible_hits(
    search: &SearchIndex,
    request: &FtsSearchRequest<'_>,
) -> Result<Vec<crate::search::SearchHit>> {
    request.clock.check_stage()?;
    let allows = |graph: &str| {
        request.clock.check_stage()?;
        Ok(request.graph.is_none_or(|selected| graph == selected)
            && request.filter.visibility.allows(graph))
    };
    let check = || request.clock.check_stage();
    let query = crate::search::FilterQuery::<SparqlError> {
        query: request.query,
        limit: request.limit,
        subject: request.filter.subject,
        allows: &allows,
        check: &check,
    };
    let raw = if request.complete {
        search.collect_complete(query)?
    } else {
        search.collect_filtered(query)?
    };
    request.clock.check_stage()?;
    if request.complete && raw.len() > request.limit {
        return Err(SparqlError::QueryLimit {
            resource: "fts matches",
            limit: request.limit,
        });
    }
    if request.clamped && raw.len() == request.limit {
        return Err(SparqlError::QueryLimit {
            resource: "fts hits",
            limit: request.limit,
        });
    }
    let current = request
        .filter
        .post_raw_visibility
        .map(|(store, _)| store.read_snapshot());
    let mut memo = crate::cache::BoundedCache::new(1_024, search.query_bytes() / 4);
    let mut kept = Vec::with_capacity(raw.len());
    for hit in raw {
        if let (Some((_, visible)), Some(current)) =
            (request.filter.post_raw_visibility, current.as_ref())
        {
            let allowed = if let Some(allowed) = memo.get_cloned(hit.graph_id.as_str()) {
                allowed
            } else {
                let allowed = visible(current, &GraphId::new(&hit.graph_id));
                memo.insert(
                    hit.graph_id.clone(),
                    allowed,
                    hit.graph_id.len().saturating_mul(2),
                );
                allowed
            };
            if !allowed {
                continue;
            }
        }
        kept.push(hit);
    }
    Ok(kept)
}

/// Rewrites FTS against the last committed index state into `VALUES`.
/// Callers needing read-your-writes must flush search first.
fn rewrite_fts_service(pattern: GraphPattern, cx: FtsRewriteCtx<'_>) -> Result<GraphPattern> {
    let mut spec = parse_fts_spec(pattern)?;
    if spec.complete {
        spec.limit = cx.limits.max_intermediate_rows;
    }
    if spec.limit == 0 {
        return Ok(GraphPattern::Values {
            variables: requested_fts_variables(&spec),
            bindings: Vec::new(),
        });
    }

    let variables = requested_fts_variables(&spec);
    if variables.is_empty() {
        return Err(SparqlError::Unsupported(
            "FTS SERVICE must bind at least one variable".into(),
        ));
    }
    if spec.limit > cx.limits.max_intermediate_rows {
        return Err(SparqlError::QueryLimit {
            resource: "intermediate rows",
            limit: cx.limits.max_intermediate_rows,
        });
    }
    cx.search.ensure_available()?;
    let visibility = FtsGraphVisibility::new(cx.scope, cx.search.query_bytes() / 4)?;
    let graph = match &spec.graph {
        Some(FtsGraphBinding::Fixed(graph)) => {
            if !visibility.allows(graph.as_str()) {
                return Ok(GraphPattern::Values {
                    variables,
                    bindings: Vec::new(),
                });
            }
            Some(graph.as_str())
        }
        _ => None,
    };

    let hits = search_visible_hits(
        cx.search,
        &FtsSearchRequest {
            query: spec.query.as_deref().unwrap_or(""),
            limit: spec.limit,
            clock: cx.clock,
            clamped: spec.limit_clamped,
            complete: spec.complete,
            graph,
            filter: FtsHitFilter {
                visibility: &visibility,
                post_raw_visibility: cx.post_raw_visibility,
                subject: match &spec.subject {
                    Some(FtsSubjectPattern::NamedNode(node)) => Some(node.as_str()),
                    _ => None,
                },
            },
        },
    )?;

    let mut bindings = Vec::with_capacity(hits.len());
    for hit in &hits {
        bindings.push(fts_binding_row(&variables, &spec, hit)?);
    }

    Ok(GraphPattern::Values {
        variables,
        bindings,
    })
}

/// Reads FTS arguments and clamps `fts:limit` to [`crate::MAX_SEARCH_LIMIT`].
/// A truncating clamp is reported by [`search_visible_hits`].
fn parse_fts_spec(pattern: GraphPattern) -> Result<FtsServiceSpec> {
    let GraphPattern::Bgp { patterns } = pattern else {
        return Err(SparqlError::Unsupported(
            "FTS SERVICE currently supports only basic graph patterns".into(),
        ));
    };

    let mut spec = FtsServiceSpec {
        limit: 20,
        ..Default::default()
    };

    for pattern in patterns {
        let predicate = match pattern.predicate {
            NamedNodePattern::NamedNode(node) => node,
            NamedNodePattern::Variable(_) => {
                return Err(SparqlError::Unsupported(
                    "FTS SERVICE does not support variable predicates".into(),
                ));
            }
        };

        set_fts_subject(&mut spec, pattern.subject)?;

        match predicate.as_str() {
            FTS_QUERY_IRI => {
                let TermPattern::Literal(literal) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:query must be bound to a string literal".into(),
                    ));
                };
                spec.query = Some(literal.value().to_string());
            }
            FTS_LIMIT_IRI => {
                let TermPattern::Literal(literal) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:limit must be bound to an integer literal".into(),
                    ));
                };
                // Large limits are clamped. A full page reports possible
                // truncation.
                let requested = literal.value().parse::<usize>().map_err(|_| {
                    SparqlError::Unsupported("fts:limit must be a positive integer".into())
                })?;
                spec.limit = requested.min(crate::MAX_SEARCH_LIMIT);
                spec.limit_clamped = requested > spec.limit;
                spec.limit_set = true;
            }
            FTS_COMPLETE_IRI => {
                let TermPattern::Literal(literal) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:complete must be bound to a boolean literal".into(),
                    ));
                };
                spec.complete = match (literal.datatype(), literal.value()) {
                    (oxrdf::vocab::xsd::BOOLEAN, "true" | "1") => true,
                    (oxrdf::vocab::xsd::BOOLEAN, "false" | "0") => false,
                    _ => {
                        return Err(SparqlError::Unsupported(
                            "fts:complete must be bound to a boolean literal".into(),
                        ));
                    }
                };
            }
            FTS_SCORE_IRI => {
                let TermPattern::Variable(variable) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:score must bind to a variable".into(),
                    ));
                };
                spec.score_var = Some(variable);
            }
            FTS_GRAPH_IRI => {
                spec.graph = Some(match pattern.object {
                    TermPattern::Variable(variable) => FtsGraphBinding::Variable(variable),
                    TermPattern::NamedNode(node) => FtsGraphBinding::Fixed(node),
                    _ => {
                        return Err(SparqlError::Unsupported(
                            "fts:graph must bind to a variable or graph IRI".into(),
                        ));
                    }
                });
            }
            other => {
                return Err(SparqlError::Unsupported(format!(
                    "unsupported FTS predicate `{other}`"
                )));
            }
        }
    }

    if spec.subject.is_none() {
        return Err(SparqlError::Unsupported(
            "FTS SERVICE must specify a subject binding".into(),
        ));
    }
    if spec.query.is_none() {
        return Err(SparqlError::Unsupported(
            "FTS SERVICE requires an fts:query literal".into(),
        ));
    }
    // A complete match has no ranking, so a page size or score would be meaningless.
    if spec.complete && (spec.limit_set || spec.score_var.is_some()) {
        return Err(SparqlError::Unsupported(
            "fts:complete cannot be combined with fts:limit or fts:score".into(),
        ));
    }

    Ok(spec)
}

fn set_fts_subject(spec: &mut FtsServiceSpec, subject: TermPattern) -> Result<()> {
    let subject = match subject {
        TermPattern::Variable(variable) => FtsSubjectPattern::Variable(variable),
        TermPattern::NamedNode(node) => FtsSubjectPattern::NamedNode(node),
        _ => {
            return Err(SparqlError::Unsupported(
                "FTS SERVICE subject must be a variable or named node".into(),
            ));
        }
    };

    match (&spec.subject, &subject) {
        (None, _) => {
            spec.subject = Some(subject);
            Ok(())
        }
        (Some(FtsSubjectPattern::Variable(left)), FtsSubjectPattern::Variable(right))
            if left == right =>
        {
            Ok(())
        }
        (Some(FtsSubjectPattern::NamedNode(left)), FtsSubjectPattern::NamedNode(right))
            if left == right =>
        {
            Ok(())
        }
        _ => Err(SparqlError::Unsupported(
            "all triples inside an FTS SERVICE must share the same subject".into(),
        )),
    }
}

fn requested_fts_variables(spec: &FtsServiceSpec) -> Vec<Variable> {
    let mut variables = Vec::new();
    if let Some(FtsSubjectPattern::Variable(variable)) = &spec.subject {
        variables.push(variable.clone());
    }
    if let Some(FtsGraphBinding::Variable(variable)) = &spec.graph {
        variables.push(variable.clone());
    }
    if let Some(variable) = &spec.score_var {
        variables.push(variable.clone());
    }
    variables
}

fn fts_binding_row(
    variables: &[Variable],
    spec: &FtsServiceSpec,
    hit: &crate::search::SearchHit,
) -> Result<Vec<Option<GroundTerm>>> {
    let mut row = Vec::with_capacity(variables.len());
    for variable in variables {
        let value = if matches!(&spec.subject, Some(FtsSubjectPattern::Variable(bound)) if bound == variable)
        {
            Some(ground_named_node(&hit.subject_iri))
        } else if matches!(&spec.graph, Some(FtsGraphBinding::Variable(bound)) if bound == variable)
        {
            Some(ground_named_node(&hit.graph_id))
        } else if spec
            .score_var
            .as_ref()
            .is_some_and(|bound| bound == variable)
        {
            Some(GroundTerm::Literal(Literal::from(hit.score as f64)))
        } else {
            None
        };
        row.push(value);
    }
    Ok(row)
}

fn ground_named_node(iri: &str) -> GroundTerm {
    GroundTerm::NamedNode(NamedNode::new_unchecked(iri))
}

#[derive(Debug, Clone)]
enum StoreTerm {
    Source(TermId),
    Mapped {
        source: TermId,
        dense: DenseTerm,
    },
    Dense(DenseTerm),
    Missing(EncodedTerm),
    /// Claimed exactly once while spareval encodes the default graph marker.
    DefaultUnion,
}

impl PartialEq for StoreTerm {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Source(left), Self::Source(right)) => left == right,
            (
                Self::Mapped { dense: left, .. } | Self::Dense(left),
                Self::Mapped { dense: right, .. } | Self::Dense(right),
            ) => left == right,
            (Self::Missing(left), Self::Missing(right)) => left == right,
            (Self::DefaultUnion, Self::DefaultUnion) => true,
            _ => false,
        }
    }
}

impl Eq for StoreTerm {}

impl Hash for StoreTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Source(source) => {
                0u8.hash(state);
                source.hash(state);
            }
            Self::Mapped { dense, .. } | Self::Dense(dense) => {
                1u8.hash(state);
                dense.hash(state);
            }
            Self::Missing(term) => {
                2u8.hash(state);
                term.hash(state);
            }
            Self::DefaultUnion => 3u8.hash(state),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum StoreDatasetError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("query limit exceeded: {0:?}")]
    QueryLimit(#[from] QueryLimitExceeded),
    #[error("invalid RDF term: {0}")]
    InvalidTerm(String),
    #[error(transparent)]
    UnsupportedStarTerm(#[from] crate::UnsupportedRdfStarTerm),
}

#[derive(Clone, Copy)]
enum ResolvedPatternTerm {
    Any,
    Existing {
        source: Option<TermId>,
        dense: Option<DenseTerm>,
    },
    Missing,
    DefaultUnion,
}

#[derive(Clone, Copy)]
struct TermWriter<'store, 'context, 'visibility> {
    view: &'context StoreReadView<'store>,
    context: &'context ReadContext<'visibility>,
    dense_scope: Option<u64>,
}

impl TermWriter<'_, '_, '_> {
    fn stored_term(
        &self,
        source: TermId,
        require_query_id: bool,
    ) -> std::result::Result<StoreTerm, StoreDatasetError> {
        let query = if self.view.query_ids_trusted(self.context)? {
            let query = self.view.query_term_id(self.context, source)?;
            if require_query_id && query.is_none() {
                return Err(
                    StoreError::IndexVerificationFailed("term-to-query-mapping-missing").into(),
                );
            }
            query
        } else {
            None
        };
        Ok(match (query, self.dense_scope) {
            (Some(query), Some(scope)) => StoreTerm::Mapped {
                source,
                dense: DenseTerm::new(query, scope),
            },
            _ => StoreTerm::Source(source),
        })
    }
}

struct StoreDataset<'store, 'context, 'visibility> {
    view: &'context StoreReadView<'store>,
    context: &'context ReadContext<'visibility>,
    default_union_marker: Option<BlankNode>,
    union_marker_pending: Cell<bool>,
    query_budget: Option<Arc<QueryBudget>>,
    dense_resolver: RefCell<Option<DenseResolver>>,
    dense_scope: Option<u64>,
    /// The single selected graph and its dense ID, resolved once per dataset.
    scoped_graph: Cell<Option<(TermId, Option<DenseTerm>)>>,
    /// Set once a broad cross-graph scan has considered reading graph records by range.
    graph_visits_prepared: Cell<bool>,
    /// Stored identities a native operator already resolved for this query.
    known_terms: HashMap<String, (TermId, QueryTermId)>,
}

static NEXT_DENSE_SCOPE: AtomicU64 = AtomicU64::new(1);

fn next_dense_scope() -> Option<u64> {
    NEXT_DENSE_SCOPE
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |scope| {
            scope.checked_add(1)
        })
        .ok()
}

impl<'store, 'context, 'visibility> StoreDataset<'store, 'context, 'visibility> {
    #[cfg(test)]
    fn new(
        view: &'context StoreReadView<'store>,
        context: &'context ReadContext<'visibility>,
    ) -> Self {
        Self {
            view,
            context,
            default_union_marker: None,
            union_marker_pending: Cell::new(false),
            query_budget: None,
            dense_resolver: RefCell::new(None),
            dense_scope: next_dense_scope(),
            scoped_graph: Cell::new(None),
            graph_visits_prepared: Cell::new(false),
            known_terms: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn mark_default_union(
        view: &'context StoreReadView<'store>,
        context: &'context ReadContext<'visibility>,
        marker: BlankNode,
    ) -> Self {
        Self {
            view,
            context,
            default_union_marker: Some(marker),
            union_marker_pending: Cell::new(true),
            query_budget: None,
            dense_resolver: RefCell::new(None),
            dense_scope: next_dense_scope(),
            scoped_graph: Cell::new(None),
            graph_visits_prepared: Cell::new(false),
            known_terms: HashMap::new(),
        }
    }

    fn with_query_budget(
        view: &'context StoreReadView<'store>,
        context: &'context ReadContext<'visibility>,
        marker: BlankNode,
        query_budget: Arc<QueryBudget>,
    ) -> Self {
        Self {
            view,
            context,
            default_union_marker: Some(marker),
            union_marker_pending: Cell::new(true),
            query_budget: Some(query_budget),
            dense_resolver: RefCell::new(None),
            dense_scope: next_dense_scope(),
            scoped_graph: Cell::new(None),
            graph_visits_prepared: Cell::new(false),
            known_terms: HashMap::new(),
        }
    }

    fn with_known_terms(mut self, known_terms: HashMap<String, (TermId, QueryTermId)>) -> Self {
        self.known_terms = known_terms;
        self
    }

    /// Reads graph records by range once when a predicate scan crosses many graphs.
    fn prepare_graph_visits(
        &self,
        predicate: ResolvedPatternTerm,
    ) -> std::result::Result<(), StoreDatasetError> {
        let ResolvedPatternTerm::Existing { source, dense } = predicate else {
            return Ok(());
        };
        if self.graph_visits_prepared.replace(true) {
            return Ok(());
        }
        let predicate = self.source_term(source, dense)?;
        if let Some(expected) = self.view.qv_p_count(self.context, predicate)? {
            self.view.prepare_graph_visits(expected)?;
        }
        Ok(())
    }

    /// The one graph an explicit scope selects, with its dense ID when query IDs apply.
    fn scoped_graph(&self) -> crate::store::Result<Option<(TermId, Option<DenseTerm>)>> {
        let Some(graph) = self.context.single_graph() else {
            return Ok(None);
        };
        if let Some(scoped) = self.scoped_graph.get() {
            return Ok(Some(scoped));
        }
        let dense = match self.dense_scope {
            Some(scope) if self.view.query_ids_trusted(self.context)? => self
                .view
                .query_term_id(self.context, graph)?
                .map(|query| DenseTerm::new(query, scope)),
            _ => None,
        };
        self.scoped_graph.set(Some((graph, dense)));
        Ok(Some((graph, dense)))
    }

    fn resolve_pattern_term(&self, term: Option<&StoreTerm>) -> ResolvedPatternTerm {
        match term {
            None => ResolvedPatternTerm::Any,
            Some(StoreTerm::Source(source)) => ResolvedPatternTerm::Existing {
                source: Some(*source),
                dense: None,
            },
            Some(StoreTerm::Mapped { source, dense }) => ResolvedPatternTerm::Existing {
                source: Some(*source),
                dense: Some(*dense),
            },
            Some(StoreTerm::Dense(dense)) => ResolvedPatternTerm::Existing {
                source: None,
                dense: Some(*dense),
            },
            Some(StoreTerm::Missing(_)) => ResolvedPatternTerm::Missing,
            Some(StoreTerm::DefaultUnion) => ResolvedPatternTerm::DefaultUnion,
        }
    }

    fn term_identity(term: &StoreTerm) -> Option<(Option<TermId>, Option<DenseTerm>)> {
        match term {
            StoreTerm::Source(source) => Some((Some(*source), None)),
            StoreTerm::Mapped { source, dense } => Some((Some(*source), Some(*dense))),
            StoreTerm::Dense(dense) => Some((None, Some(*dense))),
            StoreTerm::Missing(_) | StoreTerm::DefaultUnion => None,
        }
    }

    fn decode_term(&self, id: TermId) -> std::result::Result<Arc<EncodedTerm>, StoreDatasetError> {
        self.view
            .decode_term_arc(self.context, id)
            .map_err(Into::into)
    }

    fn source_term(
        &self,
        source: Option<TermId>,
        dense: Option<DenseTerm>,
    ) -> std::result::Result<TermId, StoreDatasetError> {
        if let Some(dense) = dense {
            if Some(dense.scope()) != self.dense_scope {
                return Err(
                    StoreError::IndexVerificationFailed("dense-term-scope-mismatch").into(),
                );
            }
            if let Some(resolver) = self.dense_resolver.borrow().as_ref() {
                if dense.scope() != resolver.scope() {
                    return Err(
                        StoreError::IndexVerificationFailed("dense-term-scope-mismatch").into(),
                    );
                }
                if let Some(source) = source {
                    return Ok(source);
                }
                return resolver.source(dense).map_err(Into::into);
            }
        }
        source.ok_or_else(|| {
            StoreError::IndexVerificationFailed("stored-term-identity-missing").into()
        })
    }

    fn accept_resolver(
        &self,
        resolver: DenseResolver,
        terms: [Option<DenseTerm>; 4],
    ) -> std::result::Result<(), StoreDatasetError> {
        let Some(scope) = self.dense_scope else {
            return Err(StoreError::IndexVerificationFailed("dense-scope-exhausted").into());
        };
        if resolver.scope() != scope
            || terms
                .into_iter()
                .flatten()
                .any(|term| term.scope() != scope)
        {
            return Err(StoreError::IndexVerificationFailed("dense-term-scope-mismatch").into());
        }
        let mut current = self.dense_resolver.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|current| current.space() != resolver.space())
        {
            return Err(
                StoreError::IndexVerificationFailed("dense-resolver-snapshot-mismatch").into(),
            );
        }
        if current.is_none() {
            *current = Some(resolver);
        }
        Ok(())
    }

    fn dense_store_term(term: DenseTerm, source: Option<TermId>) -> StoreTerm {
        match source {
            Some(source) => StoreTerm::Mapped {
                source,
                dense: term,
            },
            None => StoreTerm::Dense(term),
        }
    }

    fn stored_term(
        &self,
        source: TermId,
        require_query_id: bool,
    ) -> std::result::Result<StoreTerm, StoreDatasetError> {
        self.term_writer().stored_term(source, require_query_id)
    }

    fn term_writer(&self) -> TermWriter<'store, 'context, 'visibility> {
        TermWriter {
            view: self.view,
            context: self.context,
            dense_scope: self.dense_scope,
        }
    }

    fn externalize_encoded_term(
        &self,
        term: &EncodedTerm,
    ) -> std::result::Result<Term, StoreDatasetError> {
        term.to_term()
            .ok_or_else(|| StoreDatasetError::InvalidTerm(term.0.clone()))
    }

    fn externalize_store_term(
        &self,
        term: StoreTerm,
    ) -> std::result::Result<Term, StoreDatasetError> {
        match term {
            StoreTerm::Source(source) => {
                let decoded = self.decode_term(source)?;
                self.externalize_encoded_term(&decoded)
            }
            StoreTerm::Mapped { source, dense } => {
                let decoded = self.decode_term(self.source_term(Some(source), Some(dense))?)?;
                self.externalize_encoded_term(&decoded)
            }
            StoreTerm::Dense(dense) => {
                let decoded = self.decode_term(self.source_term(None, Some(dense))?)?;
                self.externalize_encoded_term(&decoded)
            }
            StoreTerm::Missing(term) => self.externalize_encoded_term(&term),
            StoreTerm::DefaultUnion => self
                .default_union_marker
                .as_ref()
                .cloned()
                .map(Term::BlankNode)
                .ok_or_else(|| {
                    StoreDatasetError::InvalidTerm(
                        "internal default-union marker escaped evaluation".to_owned(),
                    )
                }),
        }
    }
}

impl<'store, 'context, 'visibility> QueryableDataset<'context>
    for StoreDataset<'store, 'context, 'visibility>
where
    'store: 'context,
    'visibility: 'context,
{
    type InternalTerm = StoreTerm;
    type Error = StoreDatasetError;

    #[allow(refining_impl_trait)]
    fn internal_quads_for_pattern(
        &self,
        subject: Option<&Self::InternalTerm>,
        predicate: Option<&Self::InternalTerm>,
        object: Option<&Self::InternalTerm>,
        graph_name: Option<Option<&Self::InternalTerm>>,
    ) -> Box<
        dyn Iterator<Item = std::result::Result<InternalQuad<Self::InternalTerm>, Self::Error>>
            + 'context,
    > {
        if let Some(query_budget) = &self.query_budget
            && let Err(error) = query_budget.check()
        {
            return Box::new(std::iter::once(Err(error.into())));
        }
        let subject = self.resolve_pattern_term(subject);
        let predicate = self.resolve_pattern_term(predicate);
        let object = self.resolve_pattern_term(object);

        if matches!(subject, ResolvedPatternTerm::Missing)
            || matches!(predicate, ResolvedPatternTerm::Missing)
            || matches!(object, ResolvedPatternTerm::Missing)
            || matches!(subject, ResolvedPatternTerm::DefaultUnion)
            || matches!(predicate, ResolvedPatternTerm::DefaultUnion)
            || matches!(object, ResolvedPatternTerm::DefaultUnion)
        {
            return Box::new(std::iter::empty());
        }

        let source = |term: ResolvedPatternTerm| match term {
            ResolvedPatternTerm::Any => Ok(None),
            ResolvedPatternTerm::Existing { source, dense } => {
                self.source_term(source, dense).map(Some)
            }
            ResolvedPatternTerm::Missing | ResolvedPatternTerm::DefaultUnion => {
                unreachable!("non-stored terms short-circuit above")
            }
        };
        let dense = |term: ResolvedPatternTerm| match term {
            ResolvedPatternTerm::Any => Some(None),
            ResolvedPatternTerm::Existing { dense, .. } => dense.map(|term| Some(term.query())),
            ResolvedPatternTerm::Missing | ResolvedPatternTerm::DefaultUnion => {
                unreachable!("non-stored terms short-circuit above")
            }
        };
        let shape = |term: ResolvedPatternTerm| match term {
            ResolvedPatternTerm::Any => None,
            ResolvedPatternTerm::Existing { source, .. } => Some(source.unwrap_or(TermId(0))),
            ResolvedPatternTerm::Missing | ResolvedPatternTerm::DefaultUnion => {
                unreachable!("non-stored terms short-circuit above")
            }
        };
        let source_hint = |term: ResolvedPatternTerm| match term {
            ResolvedPatternTerm::Existing { source, dense } => dense
                .zip(source)
                .map(|(dense, source)| (dense.query(), source)),
            ResolvedPatternTerm::Any
            | ResolvedPatternTerm::Missing
            | ResolvedPatternTerm::DefaultUnion => None,
        };
        let (selector, graph_dense, graph_source) = match graph_name {
            Some(Some(
                graph @ (StoreTerm::Source(_) | StoreTerm::Mapped { .. } | StoreTerm::Dense(_)),
            )) => {
                let (source, dense) = Self::term_identity(graph).expect("stored graph term");
                let graph_source = match self.source_term(source, dense) {
                    Ok(graph) => graph,
                    Err(error) => return Box::new(std::iter::once(Err(error))),
                };
                (
                    GraphSelector::Named(graph_source),
                    dense,
                    Some(graph_source),
                )
            }
            Some(Some(StoreTerm::Missing(_))) => return Box::new(std::iter::empty()),
            // Compatibility callers use `Some(None)` for the distinct union
            // default; the cursor owns its constant-state semantics.
            Some(Some(StoreTerm::DefaultUnion)) | Some(None) => match self.scoped_graph() {
                // One graph holds each triple once, so its range is the distinct union.
                Ok(Some((graph, dense))) => (GraphSelector::Named(graph), dense, Some(graph)),
                Ok(None) => (GraphSelector::DefaultUnion, None, None),
                Err(error) => return Box::new(std::iter::once(Err(error.into()))),
            },
            None => (GraphSelector::Union, None, None),
        };
        let dense_terms = [
            graph_dense,
            match subject {
                ResolvedPatternTerm::Existing { dense, .. } => dense,
                _ => None,
            },
            match predicate {
                ResolvedPatternTerm::Existing { dense, .. } => dense,
                _ => None,
            },
            match object {
                ResolvedPatternTerm::Existing { dense, .. } => dense,
                _ => None,
            },
        ];
        if let (Some(subject_id), Some(predicate_id), Some(object_id)) =
            (dense(subject), dense(predicate), dense(object))
            && (graph_dense.is_some() || !matches!(selector, GraphSelector::Named(_)))
            && let Some(scope) = self.dense_scope
        {
            if subject_id.is_none()
                && object_id.is_none()
                && !matches!(selector, GraphSelector::Named(_))
                && let Err(error) = self.prepare_graph_visits(predicate)
            {
                return Box::new(std::iter::once(Err(error)));
            }
            let (cache_entries, cache_bytes) = self
                .query_budget
                .as_ref()
                .map_or((0, 0), |budget| budget.dense_cache_limits());
            let scan = DenseScan {
                selector,
                shape: QuadPattern {
                    subject: shape(subject),
                    predicate: shape(predicate),
                    object: shape(object),
                    ..QuadPattern::default()
                },
                pattern: RawIndexPattern::from_terms([
                    graph_dense.map(DenseTerm::query),
                    subject_id,
                    predicate_id,
                    object_id,
                ]),
                source_hints: [
                    graph_dense
                        .zip(graph_source)
                        .map(|(dense, source)| (dense.query(), source)),
                    source_hint(subject),
                    source_hint(predicate),
                    source_hint(object),
                ],
                scope,
                resolver: self.dense_resolver.borrow().clone(),
                cache_entries,
                cache_bytes,
                graphs: None,
            };
            match self.view.dense_keys(self.context, scan) {
                Ok(Some(cursor)) => {
                    if let Err(error) = self.accept_resolver(cursor.resolver(), dense_terms) {
                        return Box::new(std::iter::once(Err(error)));
                    }
                    let query_budget = self.query_budget.clone();
                    let has_graph =
                        !matches!(graph_name, Some(None) | Some(Some(StoreTerm::DefaultUnion)));
                    return Box::new(cursor.map(move |quad| {
                        if let Some(query_budget) = &query_budget {
                            query_budget.observe_intermediate(1)?;
                        }
                        let quad = quad.map_err(StoreDatasetError::from)?;
                        Ok(InternalQuad {
                            subject: Self::dense_store_term(quad.subject, quad.subject_source()),
                            predicate: Self::dense_store_term(quad.predicate, None),
                            object: Self::dense_store_term(quad.object, quad.object_source()),
                            graph_name: has_graph.then(|| {
                                Self::dense_store_term(quad.graph, Some(quad.graph_source))
                            }),
                        })
                    }));
                }
                Ok(None) => {}
                Err(error) => return Box::new(std::iter::once(Err(error.into()))),
            }
        }
        let pattern = QuadPattern {
            subject: match source(subject) {
                Ok(term) => term,
                Err(error) => return Box::new(std::iter::once(Err(error))),
            },
            predicate: match source(predicate) {
                Ok(term) => term,
                Err(error) => return Box::new(std::iter::once(Err(error))),
            },
            object: match source(object) {
                Ok(term) => term,
                Err(error) => return Box::new(std::iter::once(Err(error))),
            },
            ..QuadPattern::default()
        };
        let quads = match self.view.scan(self.context, selector, pattern) {
            Ok(quads) => quads,
            Err(error) => return Box::new(std::iter::once(Err(error.into()))),
        };
        let query_budget = self.query_budget.clone();
        let quads = quads.map(move |quad| {
            if let Some(query_budget) = &query_budget {
                query_budget.observe_intermediate(1)?;
            }
            quad.map_err(StoreDatasetError::from)
        });
        let writer = self.term_writer();

        match graph_name {
            Some(Some(StoreTerm::DefaultUnion)) => Box::new(quads.map(move |quad| {
                let quad = quad?;
                Ok(InternalQuad {
                    subject: writer.stored_term(quad.subject, true)?,
                    predicate: writer.stored_term(quad.predicate, true)?,
                    object: writer.stored_term(quad.object, true)?,
                    graph_name: None,
                })
            })),
            Some(Some(StoreTerm::Source(_) | StoreTerm::Mapped { .. } | StoreTerm::Dense(_))) => {
                Box::new(quads.map(move |quad| {
                    let quad = quad?;
                    Ok(InternalQuad {
                        subject: writer.stored_term(quad.subject, true)?,
                        predicate: writer.stored_term(quad.predicate, true)?,
                        object: writer.stored_term(quad.object, true)?,
                        graph_name: Some(writer.stored_term(quad.graph, true)?),
                    })
                }))
            }
            Some(Some(StoreTerm::Missing(_))) => unreachable!("missing graph short-circuits above"),
            Some(None) => Box::new(quads.map(move |quad| {
                let quad = quad?;
                Ok(InternalQuad {
                    subject: writer.stored_term(quad.subject, true)?,
                    predicate: writer.stored_term(quad.predicate, true)?,
                    object: writer.stored_term(quad.object, true)?,
                    graph_name: None,
                })
            })),
            None => Box::new(quads.map(move |quad| {
                let quad = quad?;
                Ok(InternalQuad {
                    subject: writer.stored_term(quad.subject, true)?,
                    predicate: writer.stored_term(quad.predicate, true)?,
                    object: writer.stored_term(quad.object, true)?,
                    graph_name: Some(writer.stored_term(quad.graph, true)?),
                })
            })),
        }
    }

    #[allow(refining_impl_trait)]
    fn internal_named_graphs(
        &self,
    ) -> Box<dyn Iterator<Item = std::result::Result<Self::InternalTerm, Self::Error>> + 'context>
    {
        let view = self.view;
        let context = self.context;
        let writer = self.term_writer();
        let query_budget = self.query_budget.clone();
        Box::new(view.graph_term_iter().filter_map(move |graph_id| {
            match graph_id {
                Ok(graph_id) => match view.graph_is_visible(context, graph_id) {
                    Ok(true) => Some(
                        query_budget
                            .as_ref()
                            .map_or(Ok(()), |budget| budget.check().map_err(Into::into))
                            .and_then(|()| writer.stored_term(graph_id, false)),
                    ),
                    Ok(false) => None,
                    Err(error) => Some(Err(error.into())),
                },
                Err(error) => Some(Err(error.into())),
            }
        }))
    }

    /// Graph existence requires visible metadata, including for empty graphs.
    /// This matches explicit datasets instead of probing for a visible quad.
    fn contains_internal_graph_name(
        &self,
        graph_name: &Self::InternalTerm,
    ) -> std::result::Result<bool, Self::Error> {
        let Some((source, dense)) = Self::term_identity(graph_name) else {
            // The marker is never a named graph, and a missing term was never
            // a graph in this execution snapshot.
            return Ok(false);
        };
        let graph = self.source_term(source, dense)?;
        Ok(self.view.contains_graph_id(graph)?
            && self.view.graph_is_visible(self.context, graph)?)
    }

    fn internalize_term(&self, term: Term) -> std::result::Result<Self::InternalTerm, Self::Error> {
        if self.union_marker_pending.get()
            && let Term::BlankNode(node) = &term
            && self
                .default_union_marker
                .as_ref()
                .is_some_and(|marker| marker == node)
        {
            // Claim the evaluator's first default-graph encoding only.
            // A later matching user term remains ordinary data.
            self.union_marker_pending.set(false);
            return Ok(StoreTerm::DefaultUnion);
        }
        let encoded = EncodedTerm::from_term(&term)?;
        if let Some((source, dense)) = self.known_terms.get(&encoded.0) {
            return Ok(match self.dense_scope {
                Some(scope) => StoreTerm::Mapped {
                    source: *source,
                    dense: DenseTerm::new(*dense, scope),
                },
                None => StoreTerm::Source(*source),
            });
        }
        Ok(match self.view.lookup_term(self.context, &encoded)? {
            Some(id) => self.stored_term(id, false)?,
            None => StoreTerm::Missing(encoded),
        })
    }

    fn externalize_term(&self, term: Self::InternalTerm) -> std::result::Result<Term, Self::Error> {
        self.externalize_store_term(term)
    }

    /// These hooks keep term conversion on the cached dataset path.
    /// The inherited EBV hook preserves spareval's private truth table.
    fn internalize_expression_term(
        &self,
        term: ExpressionTerm,
    ) -> std::result::Result<Self::InternalTerm, Self::Error> {
        self.internalize_term(term.into())
    }

    fn externalize_expression_term(
        &self,
        term: Self::InternalTerm,
    ) -> std::result::Result<ExpressionTerm, Self::Error> {
        Ok(self.externalize_store_term(term)?.into())
    }
}

fn collect_query_results(
    results: spareval::QueryResults<'_>,
    execution_started: Instant,
    context: &ReadContext<'_>,
    budget: &QueryBudget,
    collect_metrics: bool,
) -> Result<(QueryResults, CollectionMetrics)> {
    match results {
        spareval::QueryResults::Solutions(mut solutions) => {
            // Solutions yield only bound pairs, avoiding projected-variable
            // scans and repeated name clones.
            let mut rows = Vec::new();
            let mut metrics = CollectionMetrics::default();
            loop {
                budget.check()?;
                let execution = collect_metrics.then(Instant::now);
                let solution = solutions.next();
                if let Some(execution) = execution {
                    metrics.execution_time =
                        metrics.execution_time.saturating_add(execution.elapsed());
                }
                let Some(solution) = solution else {
                    break;
                };
                if collect_metrics && metrics.first_result_time.is_none() {
                    metrics.first_result_time = Some(execution_started.elapsed());
                }
                let solution = solution.map_err(|error| map_eval_error(error, budget.clock()))?;
                let collecting = collect_metrics.then(Instant::now);
                let mut row = HashMap::with_capacity(solution.len());
                for (variable, term) in solution.iter() {
                    row.insert(variable.as_str().to_string(), EncodedTerm::from_term(term)?);
                    context.increment_result_decodes();
                }
                metrics.result_rows = metrics.result_rows.saturating_add(1);
                metrics.result_cells = metrics
                    .result_cells
                    .saturating_add(u64::try_from(row.len()).unwrap_or(u64::MAX));
                budget.observe_solution(&row)?;
                let previous_capacity = rows.capacity();
                rows.push(row);
                budget.observe_capacity::<HashMap<String, EncodedTerm>>(
                    previous_capacity,
                    rows.capacity(),
                )?;
                if let Some(collecting) = collecting {
                    metrics.collection_time =
                        metrics.collection_time.saturating_add(collecting.elapsed());
                }
            }
            Ok((QueryResults::Solutions(rows), metrics))
        }
        spareval::QueryResults::Boolean(value) => {
            budget.observe_boolean()?;
            Ok((
                QueryResults::Boolean(value),
                CollectionMetrics {
                    first_result_time: Some(execution_started.elapsed()),
                    result_rows: 1,
                    result_cells: 1,
                    ..CollectionMetrics::default()
                },
            ))
        }
        spareval::QueryResults::Graph(mut triples) => {
            let mut graph = Vec::new();
            let mut metrics = CollectionMetrics::default();
            loop {
                budget.check()?;
                let execution = collect_metrics.then(Instant::now);
                let triple = triples.next();
                if let Some(execution) = execution {
                    metrics.execution_time =
                        metrics.execution_time.saturating_add(execution.elapsed());
                }
                let Some(triple) = triple else {
                    break;
                };
                if collect_metrics && metrics.first_result_time.is_none() {
                    metrics.first_result_time = Some(execution_started.elapsed());
                }
                let Triple {
                    subject,
                    predicate,
                    object,
                } = triple.map_err(|error| map_eval_error(error, budget.clock()))?;
                let collecting = collect_metrics.then(Instant::now);
                let triple = (
                    EncodedTerm::from(&subject),
                    EncodedTerm::from_named_node(&predicate),
                    EncodedTerm::from_term(&object)?,
                );
                budget.observe_graph(&triple)?;
                let previous_capacity = graph.capacity();
                graph.push(triple);
                budget.observe_capacity::<(EncodedTerm, EncodedTerm, EncodedTerm)>(
                    previous_capacity,
                    graph.capacity(),
                )?;
                for _ in 0..3 {
                    context.increment_result_decodes();
                }
                metrics.result_rows = metrics.result_rows.saturating_add(1);
                metrics.result_cells = metrics.result_cells.saturating_add(3);
                if let Some(collecting) = collecting {
                    metrics.collection_time =
                        metrics.collection_time.saturating_add(collecting.elapsed());
                }
            }
            Ok((QueryResults::Graph(graph), metrics))
        }
    }
}

fn map_eval_error(error: QueryEvaluationError, clock: &RequestClock) -> SparqlError {
    match error {
        QueryEvaluationError::Cancelled => clock.cancel_error(),
        QueryEvaluationError::Dataset(error)
            if error
                .downcast_ref::<StoreDatasetError>()
                .is_some_and(|error| matches!(error, StoreDatasetError::QueryLimit(_))) =>
        {
            let StoreDatasetError::QueryLimit(error) = error
                .downcast_ref::<StoreDatasetError>()
                .expect("query-limit dataset error was matched")
            else {
                unreachable!("query-limit dataset error was matched")
            };
            (*error).into()
        }
        QueryEvaluationError::Dataset(error)
            if error
                .downcast_ref::<StoreDatasetError>()
                .is_some_and(|error| {
                    matches!(error, StoreDatasetError::Store(StoreError::Cancelled))
                }) =>
        {
            SparqlError::Cancelled
        }
        QueryEvaluationError::Dataset(error)
            if error
                .downcast_ref::<StoreDatasetError>()
                .is_some_and(|error| {
                    matches!(error, StoreDatasetError::UnsupportedStarTerm(_))
                }) =>
        {
            let StoreDatasetError::UnsupportedStarTerm(error) = error
                .downcast_ref::<StoreDatasetError>()
                .expect("RDF-star dataset error was matched")
            else {
                unreachable!("RDF-star dataset error was matched")
            };
            error.clone().into()
        }
        error => SparqlError::Evaluation(error.to_string()),
    }
}

fn quad_to_insert(quad: &spargebra::term::Quad) -> Result<MaterializedQuadChange> {
    Ok(MaterializedQuadChange::Insert {
        graph: spargebra_graph_id(&quad.graph_name)?,
        subject: EncodedTerm::from(&quad.subject),
        predicate: EncodedTerm::from_named_node(&quad.predicate),
        object: EncodedTerm::from_term(&quad.object)?,
    })
}

fn ground_quad_delete(quad: &spargebra::term::GroundQuad) -> Result<MaterializedQuadChange> {
    Ok(MaterializedQuadChange::Delete {
        graph: spargebra_graph_id(&quad.graph_name)?,
        subject: EncodedTerm::from_named_node(&quad.subject),
        predicate: EncodedTerm::from_named_node(&quad.predicate),
        object: encode_ground_term(&quad.object)?,
    })
}

fn update_quad_change(quad: DeleteInsertQuad) -> Result<MaterializedQuadChange> {
    match quad {
        DeleteInsertQuad::Delete(quad) => Ok(MaterializedQuadChange::Delete {
            graph: oxrdf_graph_id(&quad.graph_name)?,
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object)?,
        }),
        DeleteInsertQuad::Insert(quad) => Ok(MaterializedQuadChange::Insert {
            graph: oxrdf_graph_id(&quad.graph_name)?,
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object)?,
        }),
    }
}

fn spargebra_graph_id(graph_name: &spargebra::term::GraphName) -> Result<GraphId> {
    match graph_name {
        spargebra::term::GraphName::NamedNode(node) => Ok(GraphId(node.clone())),
        spargebra::term::GraphName::DefaultGraph => Err(SparqlError::Unsupported(
            "default graph updates are not supported; use GRAPH <iri> { ... }".into(),
        )),
    }
}

fn oxrdf_graph_id(graph_name: &oxrdf::GraphName) -> Result<GraphId> {
    match graph_name {
        oxrdf::GraphName::NamedNode(node) => Ok(GraphId(node.clone())),
        oxrdf::GraphName::BlankNode(node) => Err(SparqlError::Unsupported(format!(
            "blank node graph names are not supported: _:{}",
            node.as_str()
        ))),
        oxrdf::GraphName::DefaultGraph => Err(SparqlError::Unsupported(
            "default graph updates are not supported; use GRAPH <iri> { ... }".into(),
        )),
    }
}

fn encode_ground_term(term: &spargebra::term::GroundTerm) -> Result<EncodedTerm> {
    Ok(match term {
        spargebra::term::GroundTerm::NamedNode(node) => EncodedTerm::from_named_node(node),
        spargebra::term::GroundTerm::Literal(literal) => EncodedTerm(literal.to_string()),
        spargebra::term::GroundTerm::Triple(_) => {
            return Err(crate::UnsupportedRdfStarTerm {
                term: term.to_string(),
            }
            .into());
        }
    })
}

fn materialize_removals(
    store: &GraphStore,
    graphs: Vec<GraphId>,
    changes: &mut Vec<MaterializedQuadChange>,
    changed_graphs: &mut HashSet<GraphId>,
    limits: &UpdateLimits,
    started: Instant,
) -> Result<()> {
    for graph in graphs {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = store.lookup_term(&graph_term)? else {
            continue;
        };

        store.visit_graph_quads::<SparqlError, _>(graph_id, |quad| {
            push_update_change(
                changes,
                changed_graphs,
                MaterializedQuadChange::Delete {
                    graph: graph.clone(),
                    subject: store.decode_term(quad.subject)?,
                    predicate: store.decode_term(quad.predicate)?,
                    object: store.decode_term(quad.object)?,
                },
                limits,
                started,
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::core::{ActorId, Dot, GraphDiagnostics};
    use crate::query::context::ReadAccessPath;
    use crate::store::{EncodedQuad, FtsSubject, QuadAdd, QueryTermId};
    use oxrdf::{Literal, Term};

    fn setup_engine() -> (
        tempfile::TempDir,
        Arc<GraphStore>,
        Arc<SearchIndex>,
        SparqlEngine,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(GraphStore::open(dir.path()).unwrap());
        let search = Arc::new(SearchIndex::open_in_memory().unwrap());
        let engine = SparqlEngine::new(store.clone(), search.clone());
        (dir, store, search, engine)
    }

    #[test]
    fn query_terms_compare() {
        let first = StoreTerm::Mapped {
            source: TermId(1),
            dense: DenseTerm::new(QueryTermId(7), 3),
        };
        let same_query_id = StoreTerm::Mapped {
            source: TermId(2),
            dense: DenseTerm::new(QueryTermId(7), 3),
        };
        assert_eq!(first, same_query_id);
        assert_eq!(HashSet::from([first.clone(), same_query_id]).len(), 1);

        let source_first = StoreTerm::Source(TermId(1));
        let source_second = StoreTerm::Source(TermId(2));
        assert_ne!(source_first, source_second);
        assert_ne!(StoreTerm::Source(TermId(1)), first);

        let other_scope = StoreTerm::Mapped {
            source: TermId(1),
            dense: DenseTerm::new(QueryTermId(7), 4),
        };
        assert_ne!(first, other_scope);

        #[allow(dead_code)]
        enum LegacyTerm {
            Existing(TermId, Option<QueryTermId>),
            Missing(EncodedTerm),
            DefaultUnion,
        }
        assert_eq!(
            std::mem::size_of::<StoreTerm>(),
            std::mem::size_of::<LegacyTerm>() + std::mem::size_of::<DenseTerm>(),
            "scope fencing adds one dense identity to each binding"
        );
        assert_eq!(
            std::mem::size_of::<DenseTerm>(),
            std::mem::size_of::<QueryTermId>() + std::mem::size_of::<u64>()
        );
        assert_eq!(
            std::mem::size_of::<crate::query::cursor::DenseQuad>(),
            4 * std::mem::size_of::<DenseTerm>() + 4 * std::mem::size_of::<TermId>()
        );
    }

    fn insert_quad(
        store: &GraphStore,
        graph: &GraphId,
        subject: &str,
        predicate: &str,
        object: EncodedTerm,
    ) {
        if !store.contains_graph(graph).unwrap() {
            store.create_graph(graph).unwrap();
        }
        let mut batch = store.new_batch();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject_id = store
            .resolve_term(&EncodedTerm::from_named_node(
                &oxrdf::NamedNode::new_unchecked(subject),
            ))
            .unwrap();
        let predicate_id = store
            .resolve_term(&EncodedTerm::from_named_node(
                &oxrdf::NamedNode::new_unchecked(predicate),
            ))
            .unwrap();
        let object_id = store.resolve_term(&object).unwrap();
        store
            .insert_quad(
                &mut batch,
                QuadAdd {
                    quad: EncodedQuad {
                        graph: graph_id,
                        subject: subject_id,
                        predicate: predicate_id,
                        object: object_id,
                    },
                    dot: Dot {
                        actor: ActorId::random(),
                        counter: 1,
                    },
                },
            )
            .unwrap();
        store
            .enqueue_fts(
                &mut batch,
                FtsSubject {
                    graph_id,
                    subject: subject_id,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
    }

    fn settle_diagnostics(store: &GraphStore, graph: &GraphId) {
        let diagnostics = store.graph_diagnostics(graph).unwrap();
        store.set_graph_diagnostics(graph, &diagnostics).unwrap();
    }

    #[test]
    fn dense_store_identity() {
        let (_left_dir, left, _, _) = setup_engine();
        let (_right_dir, right, _, _) = setup_engine();
        let graph = GraphId::new("urn:test:dense-store");
        insert_quad(
            &left,
            &graph,
            "urn:test:dense-store:left",
            "urn:test:dense-store:p",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:dense-store:o")),
        );
        insert_quad(
            &right,
            &graph,
            "urn:test:dense-store:right",
            "urn:test:dense-store:p",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:dense-store:o")),
        );
        settle_diagnostics(&left, &graph);
        settle_diagnostics(&right, &graph);
        let left_view = StoreReadView::new(&left);
        let right_view = StoreReadView::new(&right);
        assert_eq!(
            left_view.snapshot().sequence(),
            right_view.snapshot().sequence()
        );
        assert_eq!(
            left.index_status_fast().unwrap().query_id_generation,
            right.index_status_fast().unwrap().query_id_generation
        );
        let left_context = ReadContext::default();
        let right_context = ReadContext::default();
        let left_data = StoreDataset::new(&left_view, &left_context);
        let right_data = StoreDataset::new(&right_view, &right_context);
        let left_term = left_data
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dense-store:left",
            )))
            .unwrap();
        let right_term = right_data
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dense-store:right",
            )))
            .unwrap();
        let StoreTerm::Mapped {
            dense: left_dense, ..
        } = left_term
        else {
            panic!("left term must use the trusted dense dictionary");
        };
        let StoreTerm::Mapped {
            dense: right_dense, ..
        } = right_term
        else {
            panic!("right term must use the trusted dense dictionary");
        };
        let left_collision = DenseTerm::new(QueryTermId(7), left_dense.scope());
        let right_collision = DenseTerm::new(QueryTermId(7), right_dense.scope());
        assert_ne!(left_collision, right_collision);
        assert!(matches!(
            left_data.source_term(None, Some(right_collision)),
            Err(StoreDatasetError::Store(
                StoreError::IndexVerificationFailed("dense-term-scope-mismatch")
            ))
        ));
    }

    fn solution_rows(results: QueryResults) -> Vec<HashMap<String, EncodedTerm>> {
        match results {
            QueryResults::Solutions(rows) => rows,
            other => panic!("expected solutions, got {other:?}"),
        }
    }

    #[test]
    fn dataset_stops_early() {
        let (_dir, store, _search, _engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:early-stop");
        for index in 0..64 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:early-stop:{index:03}"),
                "urn:test:dataset:early-stop:p",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        settle_diagnostics(&store, &graph);

        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let dataset = StoreDataset::new(&view, &context);
        let predicate = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dataset:early-stop:p",
            )))
            .unwrap();
        let mut rows = dataset.internal_quads_for_pattern(None, Some(&predicate), None, None);
        let row = rows.next().unwrap().unwrap();
        assert!(matches!(
            dataset.externalize_term(row.subject).unwrap(),
            Term::NamedNode(_)
        ));
        drop(rows);

        let statistics = context.snapshot();
        assert_eq!(statistics.index_seeks, 1);
        assert_eq!(statistics.matching_quads, 1);
        assert_eq!(statistics.terms_decoded, 1);
        assert!(
            statistics.candidate_quads < 64,
            "the first accepted row must not drain the matching range: {statistics:?}"
        );
    }

    #[test]
    fn cost_collection_toggles() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:query-costs");
        insert_quad(
            &store,
            &graph,
            "urn:test:query-costs:s",
            "urn:test:query-costs:p",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("value"))),
        );
        settle_diagnostics(&store, &graph);
        let prepared = engine
            .prepare_query(
                "SELECT ?s WHERE { ?s <urn:test:query-costs:p> \"value\" . \
                 ?s <urn:test:query-costs:p> ?other }",
            )
            .unwrap();
        let run = |collect_costs| {
            let options = QueryOptions {
                collect_costs,
                fast_paths: FastPathMode::Disabled,
                ..QueryOptions::default()
            };
            engine
                .execute_prepared_graphs(
                    &crate::AllowAllAuthorizer,
                    &prepared,
                    std::slice::from_ref(&graph),
                    &options,
                )
                .unwrap()
                .statistics
        };
        // Counting runs first, before shared source mappings are cached.
        let enabled = run(true);
        assert!(enabled.reverse_mapping_reads > 0);
        assert!(enabled.forward_mapping_reads > 0);
        assert!(enabled.planner_point_reads > 0);
        assert!(enabled.planner_cache_misses > 0);

        let disabled = run(false);
        assert_eq!(disabled.reverse_mapping_reads, 0);
        assert_eq!(disabled.forward_mapping_reads, 0);
        assert_eq!(disabled.planner_point_reads, 0);
        assert_eq!(disabled.planner_cache_hits, 0);
        assert_eq!(disabled.planner_cache_misses, 0);
    }

    #[test]
    fn plan_stats_toggle() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:plan-stats");
        insert_quad(
            &store,
            &graph,
            "urn:test:plan-stats:s",
            "urn:test:plan-stats:left",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:plan-stats:key")),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:plan-stats:s",
            "urn:test:plan-stats:right",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:plan-stats:key")),
        );
        settle_diagnostics(&store, &graph);
        let prepared = engine
            .prepare_query(
                "SELECT ?s ?key WHERE { \
                 ?s <urn:test:plan-stats:left> ?key . \
                 ?s <urn:test:plan-stats:right> ?key }",
            )
            .unwrap();
        let run = |collect_plan_statistics| {
            let options = QueryOptions {
                fast_paths: FastPathMode::Disabled,
                join_mode: JoinMode::ForceHash,
                collect_plan_statistics,
                ..QueryOptions::default()
            };
            engine
                .execute_prepared_graphs(
                    &crate::AllowAllAuthorizer,
                    &prepared,
                    std::slice::from_ref(&graph),
                    &options,
                )
                .unwrap()
        };
        let enabled = run(true);
        let disabled = run(false);
        assert_eq!(enabled.results, disabled.results);
        assert_eq!(
            enabled.statistics.plan_fingerprint,
            disabled.statistics.plan_fingerprint
        );
        assert_eq!(
            enabled.statistics.planned_joins,
            disabled.statistics.planned_joins
        );
        assert!(enabled.statistics.intermediate_rows_available);
        assert!(enabled.statistics.intermediate_rows > 0);
        assert!(!disabled.statistics.intermediate_rows_available);
    }

    #[test]
    fn dense_mapping_work() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dense-mapping");
        for index in 0..64 {
            let subject = format!("urn:test:dense-mapping:{index:03}");
            insert_quad(
                &store,
                &graph,
                &subject,
                "urn:test:dense-mapping:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:dense-mapping:shared",
                )),
            );
            insert_quad(
                &store,
                &graph,
                &subject,
                "urn:test:dense-mapping:q",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        settle_diagnostics(&store, &graph);
        let prepared = engine
            .prepare_query(
                "SELECT ?s ?value WHERE { \
                 ?s <urn:test:dense-mapping:p> <urn:test:dense-mapping:shared> . \
                 ?s <urn:test:dense-mapping:q> ?value }",
            )
            .unwrap();
        let options = QueryOptions {
            collect_costs: true,
            fast_paths: FastPathMode::Disabled,
            join_mode: JoinMode::ForceLateral,
            read_mode: QueryReadMode::ForceQv,
            ..QueryOptions::default()
        };
        let execution = engine
            .execute_prepared_graphs(
                &crate::AllowAllAuthorizer,
                &prepared,
                std::slice::from_ref(&graph),
                &options,
            )
            .unwrap();
        assert_eq!(solution_rows(execution.results).len(), 64);
        let statistics = execution.statistics;
        assert!(statistics.candidate_quads >= 128, "{statistics:?}");
        assert_eq!(statistics.encoded_quad_constructions, 0);
        assert!(
            statistics.forward_mapping_reads < statistics.candidate_quads,
            "candidate rows must not be mapped back into dense IDs: {statistics:?}"
        );
        assert!(
            statistics.reverse_mapping_reads <= statistics.candidate_quads * 2,
            "only visibility and final decoding may resolve dense IDs: {statistics:?}"
        );
    }

    #[test]
    fn dense_orphan_work() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dense-orphans");
        for index in 0..64 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dense-orphans:{index:03}"),
                "urn:test:dense-orphans:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:dense-orphans:o")),
            );
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dense-orphans:{index:03}"),
                "urn:test:dense-orphans:q",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        settle_diagnostics(&store, &graph);
        let prepared = engine
            .prepare_query(&format!(
                "SELECT (COUNT(*) AS ?count) WHERE {{ GRAPH <{}> {{ \
                 ?s <urn:test:dense-orphans:p> ?o . \
                 ?s <urn:test:dense-orphans:q> ?value }} }}",
                graph.as_str()
            ))
            .unwrap();
        let options = QueryOptions {
            collect_costs: true,
            fast_paths: FastPathMode::Disabled,
            join_mode: JoinMode::ForceLateral,
            read_mode: QueryReadMode::ForceQv,
            ..QueryOptions::default()
        };
        let run = || {
            engine
                .execute_prepared_graphs(
                    &crate::AllowAllAuthorizer,
                    &prepared,
                    std::slice::from_ref(&graph),
                    &options,
                )
                .unwrap()
        };
        let clean = run();
        assert_eq!(clean.statistics.candidate_quads, 128);
        assert_eq!(clean.statistics.reverse_mapping_reads, 0);
        assert_eq!(clean.statistics.encoded_quad_constructions, 0);

        store
            .set_graph_diagnostics(
                &graph,
                &GraphDiagnostics::from_orphaned_entities(vec![
                    "urn:test:dense-orphans:000".to_owned(),
                ]),
            )
            .unwrap();
        let filtered = run();
        assert_eq!(filtered.statistics.candidate_quads, 127);
        assert_eq!(
            filtered.statistics.reverse_mapping_reads,
            64 + 1 + 63,
            "orphan filtering should resolve each unique subject and object once"
        );
        assert!(
            solution_rows(clean.results)[0]["count"]
                .0
                .starts_with("\"64\"")
        );
        assert!(
            solution_rows(filtered.results)[0]["count"]
                .0
                .starts_with("\"63\"")
        );
    }

    #[test]
    fn dense_fanout_memo() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dense-fanout");
        for index in 0..512 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dense-fanout:s:{index:03}"),
                "urn:test:dense-fanout:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(format!(
                    "urn:test:dense-fanout:o:{index:03}"
                ))),
            );
        }
        settle_diagnostics(&store, &graph);
        let branch = format!("{{ GRAPH <{}> {{ ?s ?p ?o }} }}", graph.as_str());
        let union = std::iter::repeat_n(branch, 64)
            .collect::<Vec<_>>()
            .join(" UNION ");
        let prepared = engine
            .prepare_query(&format!("SELECT ?s ?p ?o WHERE {{ {union} }}"))
            .unwrap();
        let options = QueryOptions {
            collect_costs: true,
            fast_paths: FastPathMode::Disabled,
            read_mode: QueryReadMode::ForceQv,
            ..QueryOptions::default()
        };
        let execution = engine
            .execute_prepared_graphs(
                &crate::AllowAllAuthorizer,
                &prepared,
                std::slice::from_ref(&graph),
                &options,
            )
            .unwrap();
        assert_eq!(execution.statistics.candidate_quads, 32_768);
        assert_eq!(execution.statistics.reverse_mapping_reads, 1_025);
        assert_eq!(execution.statistics.encoded_quad_constructions, 0);
        let rows = solution_rows(execution.results);
        assert_eq!(rows.len(), 32_768);
        let mut counts = HashMap::new();
        for row in rows {
            *counts.entry(row["s"].clone()).or_insert(0usize) += 1;
        }
        assert_eq!(counts.len(), 512);
        assert!(counts.values().all(|count| *count == 64));
    }

    #[test]
    fn dense_generation_refresh() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dense-generation");
        insert_quad(
            &store,
            &graph,
            "urn:test:dense-generation:s",
            "urn:test:dense-generation:p",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:dense-generation:o")),
        );
        settle_diagnostics(&store, &graph);
        let prepared = engine
            .prepare_query("SELECT ?s ?o WHERE { ?s <urn:test:dense-generation:p> ?o }")
            .unwrap();
        let options = QueryOptions {
            fast_paths: FastPathMode::Disabled,
            read_mode: QueryReadMode::ForceQv,
            ..QueryOptions::default()
        };
        let first = engine
            .execute_prepared_graphs(
                &crate::AllowAllAuthorizer,
                &prepared,
                std::slice::from_ref(&graph),
                &options,
            )
            .unwrap();
        store.rebuild_query_indexes().unwrap();
        let second = engine
            .execute_prepared_graphs(
                &crate::AllowAllAuthorizer,
                &prepared,
                std::slice::from_ref(&graph),
                &options,
            )
            .unwrap();
        assert_eq!(first.results, second.results);
        assert_ne!(
            first.statistics.query_id_generation,
            second.statistics.query_id_generation
        );
    }

    #[test]
    fn dense_visibility_multiset() {
        let (_dir, store, _search, engine) = setup_engine();
        let hidden = GraphId::new("urn:test:dense-copy:hidden");
        let visible = GraphId::new("urn:test:dense-copy:visible");
        for graph in [&hidden, &visible] {
            insert_quad(
                &store,
                graph,
                "urn:test:dense-copy:s",
                "urn:test:dense-copy:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:dense-copy:o")),
            );
            settle_diagnostics(&store, graph);
        }
        let options = QueryOptions {
            fast_paths: FastPathMode::Disabled,
            ..QueryOptions::default()
        };
        let named = solution_rows(
            engine
                .query_with_options(
                    QueryRun {
                        sparql: "SELECT ?g WHERE { GRAPH ?g { ?s <urn:test:dense-copy:p> ?o } }",
                        options: &options,
                    },
                    &|_, _: &GraphId| true,
                )
                .unwrap()
                .1
                .results,
        );
        assert_eq!(
            named.len(),
            2,
            "named graph copies must remain multiplicative"
        );

        let default = solution_rows(
            engine
                .query_with_options(
                    QueryRun {
                        sparql: "SELECT ?s WHERE { ?s <urn:test:dense-copy:p> ?o }",
                        options: &options,
                    },
                    &|_, graph: &GraphId| graph != &hidden,
                )
                .unwrap()
                .1
                .results,
        );
        assert_eq!(
            default.len(),
            1,
            "a hidden first copy must not suppress the visible union row"
        );
    }

    #[test]
    fn read_modes_equivalent() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:read-mode");
        insert_quad(
            &store,
            &graph,
            "urn:test:read-mode:s",
            "urn:test:read-mode:p",
            EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:read-mode:o")),
        );
        settle_diagnostics(&store, &graph);
        let query = format!(
            "SELECT ?s WHERE {{ GRAPH <{}> {{ ?s <urn:test:read-mode:p> ?o }} }}",
            graph.as_str()
        );

        let (auto_results, auto) = engine
            .query_graph_mode(&query, std::slice::from_ref(&graph), QueryReadMode::Auto)
            .unwrap();
        let (source_results, source) = engine
            .query_graph_mode(
                &query,
                std::slice::from_ref(&graph),
                QueryReadMode::ForceSource,
            )
            .unwrap();
        let (qv_results, qv) = engine
            .query_graph_mode(&query, std::slice::from_ref(&graph), QueryReadMode::ForceQv)
            .unwrap();

        assert_eq!(auto_results, source_results);
        assert_eq!(auto_results, qv_results);
        assert_eq!(vec![ReadAccessPath::QvGpos], auto.selected_access_paths);
        assert_eq!(
            vec![ReadAccessPath::SourceGspo],
            source.selected_access_paths
        );
        assert_eq!(vec![ReadAccessPath::QvGpos], qv.selected_access_paths);
        assert_eq!(1, source.source_keys_read);
        assert_eq!(1, qv.qv_keys_read);
    }

    #[test]
    fn degraded_counts_match() {
        fn fixture(
            state: Option<crate::QueryIndexState>,
        ) -> (
            tempfile::TempDir,
            Arc<GraphStore>,
            SparqlEngine,
            Vec<GraphId>,
            GraphId,
        ) {
            let (directory, store, _search, engine) = setup_engine();
            let primary = GraphId::new("urn:test:count-distinct:primary");
            let duplicate = GraphId::new("urn:test:count-distinct:duplicate");
            let orphan = GraphId::new("urn:test:count-distinct:orphan");
            let hidden = GraphId::new("urn:test:count-distinct:hidden");

            for (subject, object) in [
                (
                    "urn:test:count-distinct:s:a",
                    "urn:test:count-distinct:o:shared",
                ),
                (
                    "urn:test:count-distinct:s:b",
                    "urn:test:count-distinct:o:unique",
                ),
                (
                    "urn:test:count-distinct:s:c",
                    "urn:test:count-distinct:o:shared",
                ),
            ] {
                insert_quad(
                    &store,
                    &primary,
                    subject,
                    "urn:test:count-distinct:p",
                    EncodedTerm::from_named_node(&NamedNode::new_unchecked(object)),
                );
            }
            insert_quad(
                &store,
                &duplicate,
                "urn:test:count-distinct:s:a",
                "urn:test:count-distinct:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:count-distinct:o:shared",
                )),
            );
            insert_quad(
                &store,
                &hidden,
                "urn:test:count-distinct:s:hidden",
                "urn:test:count-distinct:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:count-distinct:o:hidden",
                )),
            );
            insert_quad(
                &store,
                &orphan,
                orphan.as_str(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "http://schema.org/Dataset",
                )),
            );
            insert_quad(
                &store,
                &orphan,
                "urn:test:count-distinct:s:orphan",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "http://schema.org/MediaObject",
                )),
            );
            insert_quad(
                &store,
                &orphan,
                "urn:test:count-distinct:s:orphan",
                "urn:test:count-distinct:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked(
                    "urn:test:count-distinct:o:orphan",
                )),
            );
            settle_diagnostics(&store, &primary);
            settle_diagnostics(&store, &duplicate);
            settle_diagnostics(&store, &orphan);
            settle_diagnostics(&store, &hidden);

            if let Some(state) = state {
                store.set_test_index(state);
            }
            (
                directory,
                store,
                engine,
                vec![primary.clone(), duplicate, orphan],
                primary,
            )
        }

        #[allow(clippy::field_reassign_with_default)]
        fn execute(
            engine: &SparqlEngine,
            graphs: &[GraphId],
            query: &str,
            read_mode: QueryReadMode,
            fast_paths: FastPathMode,
            max_hash_entries: usize,
            cancellation: QueryCancellation,
        ) -> Result<QueryExecution> {
            let prepared = engine.prepare_query(query)?;
            let mut options = QueryOptions::default();
            options.read_mode = read_mode;
            options.fast_paths = fast_paths;
            options.limits.max_hash_entries = max_hash_entries;
            options.cancellation = cancellation;
            engine
                .execute_prepared_scope(
                    &prepared,
                    GraphScope::List(graphs),
                    &options,
                    Duration::ZERO,
                    false,
                    None,
                )
                .map(|(execution, _)| execution)
        }

        let states = [
            (None, QueryReadMode::Auto, "Ready"),
            (
                Some(crate::QueryIndexState::Missing),
                QueryReadMode::Auto,
                "Missing",
            ),
            (
                Some(crate::QueryIndexState::Building),
                QueryReadMode::Auto,
                "Building",
            ),
            (
                Some(crate::QueryIndexState::Failed("test-failed".to_owned())),
                QueryReadMode::Auto,
                "Failed",
            ),
            (None, QueryReadMode::ForceSource, "forced source"),
        ];
        for (state, read_mode, label) in states {
            let (_directory, _store, engine, graphs, primary) = fixture(state);
            let queries = [
                "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s <urn:test:count-distinct:p> ?o }".to_owned(),
                "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s <urn:test:count-distinct:p> ?o }".to_owned(),
                format!(
                    "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE {{ GRAPH <{}> {{ ?s <urn:test:count-distinct:p> ?o }} }}",
                    primary.as_str()
                ),
                "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s <urn:test:count-distinct:missing> ?o }".to_owned(),
            ];
            for query in queries {
                let fast = execute(
                    &engine,
                    &graphs,
                    &query,
                    read_mode,
                    FastPathMode::Auto,
                    usize::MAX,
                    QueryCancellation::new(),
                )
                .unwrap();
                let generic = execute(
                    &engine,
                    &graphs,
                    &query,
                    read_mode,
                    FastPathMode::Disabled,
                    usize::MAX,
                    QueryCancellation::new(),
                )
                .unwrap();
                assert_eq!(fast.results, generic.results, "{label}: {query}");
                assert!(fast.statistics.fast_path.is_some(), "{label}: {query}");
            }
        }

        let (_directory, _store, engine, graphs, _primary) = fixture(None);
        let object_query =
            "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s <urn:test:count-distinct:p> ?o }";
        let error = execute(
            &engine,
            &graphs,
            object_query,
            QueryReadMode::ForceSource,
            FastPathMode::Auto,
            1,
            QueryCancellation::new(),
        )
        .unwrap_err();
        assert!(matches!(error, SparqlError::QueryLimit { .. }));

        let cancellation = QueryCancellation::new();
        cancellation.cancel();
        let error = execute(
            &engine,
            &graphs,
            object_query,
            QueryReadMode::ForceSource,
            FastPathMode::Auto,
            usize::MAX,
            cancellation,
        )
        .unwrap_err();
        assert_eq!(error.kind(), crate::CraqleErrorKind::Cancelled);
    }

    #[test]
    fn named_cursor_lazy() {
        let (_dir, store, _search, _engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:named");
        for index in 0..24 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:named:{index:03}"),
                "urn:test:dataset:named:p",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        insert_quad(
            &store,
            &graph,
            "urn:test:dataset:named:other",
            "urn:test:dataset:named:other-p",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("other"))),
        );
        settle_diagnostics(&store, &graph);

        let graph_id = store
            .lookup_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap()
            .unwrap();
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let dataset = StoreDataset::new(&view, &context);
        let graph_term = dataset
            .internalize_term(Term::NamedNode(graph.0.clone()))
            .unwrap();
        let subject = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dataset:named:004",
            )))
            .unwrap();
        let predicate = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dataset:named:p",
            )))
            .unwrap();
        let object = dataset
            .internalize_term(Term::Literal(Literal::new_simple_literal("4")))
            .unwrap();
        let term_id = |term: Option<&StoreTerm>| match term {
            Some(
                term @ (StoreTerm::Source(_) | StoreTerm::Mapped { .. } | StoreTerm::Dense(_)),
            ) => StoreDataset::term_identity(term).and_then(|(source, _)| source),
            Some(StoreTerm::Missing(_) | StoreTerm::DefaultUnion) => {
                panic!("fixture term should be interned")
            }
            None => None,
        };

        for (subject, predicate, object) in [
            (None, None, None),
            (Some(&subject), None, None),
            (None, Some(&predicate), None),
            (None, None, Some(&object)),
            (None, Some(&predicate), Some(&object)),
        ] {
            let mut streamed: Vec<_> = dataset
                .internal_quads_for_pattern(subject, predicate, object, Some(Some(&graph_term)))
                .map(|quad| {
                    let quad = quad.unwrap();
                    let subject = StoreDataset::term_identity(&quad.subject).unwrap();
                    let predicate = StoreDataset::term_identity(&quad.predicate).unwrap();
                    let object = StoreDataset::term_identity(&quad.object).unwrap();
                    (
                        dataset.source_term(subject.0, subject.1).unwrap(),
                        dataset.source_term(predicate.0, predicate.1).unwrap(),
                        dataset.source_term(object.0, object.1).unwrap(),
                    )
                })
                .collect();
            let mut collected: Vec<_> = store
                .quads_for_pattern(
                    Some(graph_id),
                    term_id(subject),
                    term_id(predicate),
                    term_id(object),
                )
                .unwrap()
                .into_iter()
                .map(|quad| (quad.subject, quad.predicate, quad.object))
                .collect();
            streamed.sort_unstable();
            collected.sort_unstable();
            assert_eq!(streamed, collected);
        }
        drop(dataset);

        let context = ReadContext::default();
        let dataset = StoreDataset::new(&view, &context);
        let graph_term = dataset
            .internalize_term(Term::NamedNode(graph.0.clone()))
            .unwrap();
        let predicate = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dataset:named:p",
            )))
            .unwrap();
        let mut rows = dataset.internal_quads_for_pattern(
            None,
            Some(&predicate),
            None,
            Some(Some(&graph_term)),
        );
        rows.next()
            .expect("named cursor must yield one row")
            .expect("named cursor row must remain valid in the new request scope");
        drop(rows);
        let statistics = context.snapshot();
        assert_eq!(statistics.index_seeks, 1);
        assert_eq!(statistics.matching_quads, 1);
        assert!(
            statistics.candidate_quads < 24,
            "a named scan must not drain its matching range: {statistics:?}"
        );
    }

    #[test]
    fn dataset_memoizes_visibility() {
        let (_dir, store, _search, _engine) = setup_engine();
        let visible_graph = GraphId::new("urn:test:dataset:visible");
        let hidden_graph = GraphId::new("urn:test:dataset:hidden");
        let predicate = "urn:test:dataset:visibility:p";
        let object =
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("shared")));
        insert_quad(
            &store,
            &visible_graph,
            "urn:test:dataset:visible:kept",
            predicate,
            object.clone(),
        );
        insert_quad(
            &store,
            &visible_graph,
            "./data/orphan.txt",
            predicate,
            object.clone(),
        );
        insert_quad(
            &store,
            &hidden_graph,
            "urn:test:dataset:hidden:row",
            predicate,
            object.clone(),
        );
        store
            .set_graph_diagnostics(
                &visible_graph,
                &GraphDiagnostics::from_orphaned_entities(vec!["./data/orphan.txt".to_string()]),
            )
            .unwrap();

        let calls: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
        let visible = |graph: &GraphId| {
            *calls
                .borrow_mut()
                .entry(graph.as_str().to_string())
                .or_insert(0) += 1;
            graph == &visible_graph
        };
        let view = StoreReadView::new(&store);
        let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &visible);
        let dataset = StoreDataset::new(&view, &context);
        let predicate = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(predicate)))
            .unwrap();
        let object = dataset
            .internalize_term(Term::Literal(Literal::new_simple_literal("shared")))
            .unwrap();
        let rows: Vec<_> = dataset
            .internal_quads_for_pattern(None, Some(&predicate), Some(&object), None)
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);

        let statistics = context.snapshot();
        drop(dataset);
        drop(context);
        let calls = calls.into_inner();
        assert_eq!(calls.len(), 2);
        assert!(calls.values().all(|&count| count == 1), "{calls:?}");
        assert_eq!(statistics.graphs_considered, 2);
        assert_eq!(statistics.matching_quads, 1);
        assert_eq!(statistics.candidate_quads, 3);
    }

    #[test]
    fn union_multiplicity_preserved() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:dataset:copies:1");
        let graph2 = GraphId::new("urn:test:dataset:copies:2");
        let object =
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("same")));
        for graph in [&graph1, &graph2] {
            insert_quad(
                &store,
                graph,
                "urn:test:dataset:copy",
                "urn:test:dataset:copy:p",
                object.clone(),
            );
        }

        let public_rows = solution_rows(
            engine
                .query("SELECT ?s WHERE { ?s <urn:test:dataset:copy:p> \"same\" }")
                .unwrap(),
        );
        assert_eq!(public_rows.len(), 1);

        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let dataset = StoreDataset::new(&view, &context);
        let predicate = dataset
            .internalize_term(Term::NamedNode(NamedNode::new_unchecked(
                "urn:test:dataset:copy:p",
            )))
            .unwrap();
        let object = dataset
            .internalize_term(Term::Literal(Literal::new_simple_literal("same")))
            .unwrap();
        let named_copies: Vec<_> = dataset
            .internal_quads_for_pattern(None, Some(&predicate), Some(&object), None)
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(named_copies.len(), 2);
        assert!(named_copies.iter().all(|quad| quad.graph_name.is_some()));

        let direct_default: Vec<_> = dataset
            .internal_quads_for_pattern(None, Some(&predicate), Some(&object), Some(None))
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(direct_default.len(), 1);
        assert!(direct_default.iter().all(|quad| quad.graph_name.is_none()));

        let mixed_rows = solution_rows(
            engine
                .query(
                    "SELECT ?s ?g WHERE { \
                     ?s <urn:test:dataset:copy:p> \"same\" . \
                     GRAPH ?g { ?s <urn:test:dataset:copy:p> \"same\" } \
                     }",
                )
                .unwrap(),
        );
        assert_eq!(2, mixed_rows.len());
        assert!(mixed_rows.iter().all(|row| {
            matches!(
                row.get("g").and_then(EncodedTerm::to_term),
                Some(Term::NamedNode(ref graph))
                    if graph == &graph1.0 || graph == &graph2.0
            )
        }));
    }

    #[test]
    fn union_marker_private() {
        let (_dir, store, _search, _engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:marker");
        insert_quad(
            &store,
            &graph,
            "urn:test:dataset:marker:s",
            "urn:test:dataset:marker:p",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("marker"))),
        );
        let marker = BlankNode::default();
        let marker_id = store
            .encode_term(&EncodedTerm::from_plain_term(&Term::BlankNode(
                marker.clone(),
            )))
            .unwrap();
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let dataset = StoreDataset::mark_default_union(&view, &context, marker.clone());

        assert!(matches!(
            dataset
                .internalize_term(Term::BlankNode(marker.clone()))
                .unwrap(),
            StoreTerm::DefaultUnion
        ));
        let stored_marker = dataset
            .internalize_term(Term::BlankNode(marker.clone()))
            .unwrap();
        assert!(matches!(
            stored_marker,
            StoreTerm::Source(source) | StoreTerm::Mapped { source, .. } if source == marker_id
        ));
        assert_eq!(
            Term::BlankNode(marker.clone()),
            dataset.externalize_term(StoreTerm::DefaultUnion).unwrap()
        );
        assert_eq!(
            Term::BlankNode(marker),
            dataset.externalize_term(stored_marker).unwrap()
        );
        assert!(
            !dataset
                .contains_internal_graph_name(&StoreTerm::DefaultUnion)
                .unwrap()
        );
        assert!(
            dataset
                .internal_named_graphs()
                .all(|graph| StoreDataset::term_identity(&graph.unwrap()).is_some())
        );
    }

    #[test]
    fn bounded_queries_supported() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:limit");
        for index in 0..12 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:limit:{index:03}"),
                "urn:test:dataset:limit:p",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        settle_diagnostics(&store, &graph);

        assert_eq!(
            engine
                .query("ASK { ?s <urn:test:dataset:limit:p> ?o }")
                .unwrap(),
            QueryResults::Boolean(true)
        );
        assert_eq!(
            engine
                .query("ASK { <urn:test:dataset:missing> <urn:test:dataset:limit:p> ?o }")
                .unwrap(),
            QueryResults::Boolean(false)
        );
        assert_eq!(
            solution_rows(
                engine
                    .query("SELECT ?s WHERE { ?s <urn:test:dataset:limit:p> ?o } LIMIT 10")
                    .unwrap(),
            )
            .len(),
            10
        );

        let ask = SparqlParser::new()
            .parse_query(&format!(
                "{COMMON_PREFIXES}ASK {{ ?s <urn:test:dataset:limit:p> ?o }}"
            ))
            .unwrap();
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let evaluator = QueryEvaluator::new();
        let mut prepared = evaluator.prepare(&ask);
        let default_union_marker = BlankNode::default();
        prepared
            .dataset_mut()
            .set_default_graph(vec![GraphName::BlankNode(default_union_marker.clone())]);
        assert!(matches!(
            prepared
                .execute(StoreDataset::mark_default_union(
                    &view,
                    &context,
                    default_union_marker,
                ))
                .unwrap(),
            spareval::QueryResults::Boolean(true)
        ));
        let statistics = context.snapshot();
        assert_eq!(statistics.index_seeks, 1);
        assert_eq!(statistics.candidate_quads, 1);
        assert_eq!(statistics.matching_quads, 1);

        let limit = SparqlParser::new()
            .parse_query(&format!(
                "{COMMON_PREFIXES}SELECT ?s WHERE {{ ?s <urn:test:dataset:limit:p> ?o }} LIMIT 10"
            ))
            .unwrap();
        let context = ReadContext::default();
        let evaluator = QueryEvaluator::new();
        let mut prepared = evaluator.prepare(&limit);
        let default_union_marker = BlankNode::default();
        prepared
            .dataset_mut()
            .set_default_graph(vec![GraphName::BlankNode(default_union_marker.clone())]);
        let budget = Arc::new(
            QueryBudget::new(
                query_features(&limit).budget,
                QueryLimits::default(),
                RequestClock::start(None, QueryCancellation::new(), Instant::now()),
            )
            .unwrap(),
        );
        let rows = collect_query_results(
            prepared
                .execute(StoreDataset::with_query_budget(
                    &view,
                    &context,
                    default_union_marker,
                    Arc::clone(&budget),
                ))
                .unwrap(),
            Instant::now(),
            &context,
            &budget,
            true,
        )
        .unwrap()
        .0;
        assert_eq!(solution_rows(rows).len(), 10);
        let statistics = context.snapshot();
        assert_eq!(statistics.index_seeks, 1);
        assert_eq!(statistics.candidate_quads, 10);
        assert_eq!(statistics.matching_quads, 10);
    }

    #[test]
    fn select_defaults_union() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset One",
            ))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset Two",
            ))),
        );

        let rows = solution_rows(
            engine
                .query("SELECT ?s ?name WHERE { ?s schema:name ?name }")
                .unwrap(),
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn graph_scopes_authorize() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset One",
            ))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset Two",
            ))),
        );

        let rows = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?s ?name WHERE { ?s schema:name ?name }",
                    std::slice::from_ref(&graph1),
                )
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").unwrap().0, "\"Dataset One\"");

        let named_rows = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?g ?name WHERE { GRAPH ?g { ?s schema:name ?name } }",
                    std::slice::from_ref(&graph1),
                )
                .unwrap(),
        );
        assert_eq!(named_rows.len(), 1);
        assert_eq!(named_rows[0].get("g").unwrap().0, "<urn:test:g1>");

        let missing = GraphId::new("urn:test:missing");
        let empty_rows = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?s WHERE { ?s schema:name ?name }",
                    std::slice::from_ref(&missing),
                )
                .unwrap(),
        );
        assert!(empty_rows.is_empty());
    }

    #[test]
    fn exact_scopes_skip() {
        let (_dir, store, _search, engine) = setup_engine();
        let mut graphs = Vec::new();
        for index in 0..10 {
            let graph = GraphId::new(&format!("urn:test:exact-skip:{index}"));
            for predicate in ["urn:test:exact-skip:p", "urn:test:exact-skip:q"] {
                insert_quad(
                    &store,
                    &graph,
                    &format!("urn:test:exact-skip:s{index}"),
                    predicate,
                    EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                        "same",
                    ))),
                );
            }
            graphs.push(graph);
        }
        let selected = [graphs[2].clone(), graphs[7].clone()];
        let (results, statistics) = engine
            .query_graph_mode(
                "SELECT ?s WHERE { ?s <urn:test:exact-skip:p> ?o ; <urn:test:exact-skip:q> ?o }",
                &selected,
                QueryReadMode::Auto,
            )
            .unwrap();
        let mut subjects: Vec<String> = solution_rows(results)
            .into_iter()
            .map(|row| row["s"].0.clone())
            .collect();
        subjects.sort();
        assert_eq!(
            subjects,
            ["<urn:test:exact-skip:s2>", "<urn:test:exact-skip:s7>"]
        );
        // Keys of unlisted graphs are skipped before any visibility decision.
        assert_eq!(statistics.graphs_considered, 2);
    }

    #[test]
    fn explicit_scopes_preserved() {
        let (_dir, store, _search, engine) = setup_engine();
        let mut graphs = Vec::new();
        for index in 0..=EXPLICIT_GRAPH_LIMIT {
            let graph_name = format!("urn:test:dataset-boundary:{index:02}");
            let graph = GraphId::new(&graph_name);
            insert_quad(
                &store,
                &graph,
                "urn:test:dataset-boundary:s",
                "urn:test:dataset-boundary:p",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("same"))),
            );
            graphs.push(graph);
        }

        for count in [1, 2, EXPLICIT_GRAPH_LIMIT, EXPLICIT_GRAPH_LIMIT + 1] {
            let selected = &graphs[..count];
            assert_eq!(
                1,
                solution_rows(
                    engine
                        .query_with_graphs(
                            "SELECT ?s WHERE { ?s <urn:test:dataset-boundary:p> \"same\" }",
                            selected,
                        )
                        .unwrap(),
                )
                .len(),
                "default union must remain distinct for {count} graphs"
            );
            assert_eq!(
                count,
                solution_rows(
                    engine
                        .query_with_graphs(
                            "SELECT ?g WHERE { GRAPH ?g { \
                             ?s <urn:test:dataset-boundary:p> \"same\" } }",
                            selected,
                        )
                        .unwrap(),
                )
                .len(),
                "named graph copies must remain multiplicative for {count} graphs"
            );
        }
    }

    #[test]
    fn union_marker_propagates() {
        let (_dir, store, _search, engine) = setup_engine();
        let first_graph = GraphId::new("urn:test:marker-seam:first");
        let second_graph = GraphId::new("urn:test:marker-seam:second");
        for graph in [&first_graph, &second_graph] {
            insert_quad(
                &store,
                graph,
                "urn:test:marker-seam:s",
                "urn:test:marker-seam:p",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:marker-seam:m")),
            );
            insert_quad(
                &store,
                graph,
                "urn:test:marker-seam:m",
                "urn:test:marker-seam:q",
                EncodedTerm::from_named_node(&NamedNode::new_unchecked("urn:test:marker-seam:o")),
            );
        }

        assert_eq!(
            1,
            solution_rows(
                engine
                    .query(
                        "SELECT ?o WHERE { \
                         <urn:test:marker-seam:s> \
                         <urn:test:marker-seam:p>/<urn:test:marker-seam:q> ?o }",
                    )
                    .unwrap(),
            )
            .len()
        );
        assert!(matches!(
            engine.query("DESCRIBE <urn:test:marker-seam:s>").unwrap(),
            QueryResults::Graph(ref triples) if triples.len() == 1
        ));
        assert!(matches!(
            engine
                .query(
                    "CONSTRUCT { ?s ?p ?o } WHERE { \
                     ?s ?p ?o FILTER(?s = <urn:test:marker-seam:s> || \
                     ?s = <urn:test:marker-seam:m>) }",
                )
                .unwrap(),
            QueryResults::Graph(ref triples) if triples.len() == 2
        ));

        let changes = engine
            .evaluate_update(
                &crate::AllowAllAuthorizer,
                "DELETE { GRAPH <urn:test:marker-seam:first> { \
                 ?s <urn:test:marker-seam:p> ?o } } \
                 INSERT { GRAPH <urn:test:marker-seam:first> { \
                 ?s <urn:test:marker-seam:updated> ?o } } \
                 WHERE { ?s <urn:test:marker-seam:p> ?o }",
                &UpdateOptions::default(),
            )
            .unwrap();
        assert_eq!(2, changes.len());
    }

    #[test]
    fn large_scopes_filter() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_GRAPH_LIMIT + 8;
        let mut graphs = Vec::with_capacity(total);
        let shared_subject = "urn:test:large:shared";
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:large:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:large:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Dataset {idx:03}"
                )))),
            );
            insert_quad(
                &store,
                &graph,
                shared_subject,
                "http://schema.org/position",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    idx.to_string(),
                ))),
            );
            graphs.push(graph);
        }
        let hidden = GraphId::new("urn:test:large:hidden");
        insert_quad(
            &store,
            &hidden,
            "urn:test:hidden:e",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden Dataset",
            ))),
        );
        insert_quad(
            &store,
            &hidden,
            shared_subject,
            "http://schema.org/position",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("hidden"))),
        );

        let rows = solution_rows(
            engine
                .query_with_graphs("SELECT ?s ?name WHERE { ?s schema:name ?name }", &graphs)
                .unwrap(),
        );
        assert_eq!(rows.len(), total);
        assert!(
            rows.iter()
                .all(|row| row.get("name").unwrap().0 != "\"Hidden Dataset\"")
        );

        let named_rows = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?g WHERE { GRAPH ?g { ?s schema:name ?name } }",
                    &graphs,
                )
                .unwrap(),
        );
        assert_eq!(named_rows.len(), total);
        assert!(
            named_rows
                .iter()
                .all(|row| row.get("g").unwrap().0 != "<urn:test:large:hidden>")
        );

        let enumerated = solution_rows(
            engine
                .query_with_graphs("SELECT ?g WHERE { GRAPH ?g {} }", &graphs)
                .unwrap(),
        );
        assert_eq!(enumerated.len(), total);
        assert!(
            enumerated
                .iter()
                .all(|row| row.get("g").unwrap().0 != "<urn:test:large:hidden>")
        );

        let fixed_hidden = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?name WHERE { GRAPH <urn:test:large:hidden> { ?s schema:name ?name } }",
                    &graphs,
                )
                .unwrap(),
        );
        assert!(fixed_hidden.is_empty());

        let subject_rows = solution_rows(
            engine
                .query_with_graphs(
                    "SELECT ?pos WHERE { <urn:test:large:shared> <http://schema.org/position> ?pos }",
                    &graphs,
                )
                .unwrap(),
        );
        assert_eq!(subject_rows.len(), total);
        assert!(
            subject_rows
                .iter()
                .all(|row| row.get("pos").unwrap().0 != "\"hidden\"")
        );

        assert_eq!(
            engine
                .query_with_graphs("ASK { ?s ?p ?o }", &graphs)
                .unwrap(),
            QueryResults::Boolean(true)
        );
        assert_eq!(
            engine
                .query_with_graphs("ASK { ?s ?p ?o }", &[GraphId::new("urn:test:absent")])
                .unwrap(),
            QueryResults::Boolean(false)
        );
    }

    #[test]
    fn large_scopes_hide() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_GRAPH_LIMIT + 4;
        let mut graphs = Vec::with_capacity(total);
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:orphan:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:orphan:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Visible {idx:03}"
                )))),
            );
            graphs.push(graph);
        }
        insert_quad(
            &store,
            &graphs[0],
            "./data/orphan.txt",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Orphaned File",
            ))),
        );
        store
            .set_graph_diagnostics(
                &graphs[0],
                &GraphDiagnostics::from_orphaned_entities(vec!["./data/orphan.txt".to_string()]),
            )
            .unwrap();

        let rows = solution_rows(
            engine
                .query_with_graphs("SELECT ?name WHERE { ?s schema:name ?name }", &graphs)
                .unwrap(),
        );
        assert_eq!(rows.len(), total);
        assert!(
            rows.iter()
                .all(|row| row.get("name").unwrap().0 != "\"Orphaned File\"")
        );
    }

    #[test]
    fn visibility_filters_union() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_GRAPH_LIMIT + 8;
        let shared_subject = "urn:test:pred:shared";
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:pred:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:pred:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Dataset {idx:03}"
                )))),
            );
            insert_quad(
                &store,
                &graph,
                shared_subject,
                "http://schema.org/position",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                    idx.to_string(),
                ))),
            );
        }
        let hidden = GraphId::new("urn:test:pred:hidden");
        insert_quad(
            &store,
            &hidden,
            "urn:test:hidden:e",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden Dataset",
            ))),
        );
        insert_quad(
            &store,
            &hidden,
            shared_subject,
            "http://schema.org/position",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("hidden"))),
        );

        let visible = |graph: &GraphId| graph.as_str() != "urn:test:pred:hidden";

        let rows = solution_rows(
            engine
                .query_with_visibility("SELECT ?s ?name WHERE { ?s schema:name ?name }", &visible)
                .unwrap(),
        );
        assert_eq!(rows.len(), total);
        assert!(
            rows.iter()
                .all(|row| row.get("name").unwrap().0 != "\"Hidden Dataset\"")
        );

        let named_rows = solution_rows(
            engine
                .query_with_visibility(
                    "SELECT ?g WHERE { GRAPH ?g { ?s schema:name ?name } }",
                    &visible,
                )
                .unwrap(),
        );
        assert_eq!(named_rows.len(), total);
        assert!(
            named_rows
                .iter()
                .all(|row| row.get("g").unwrap().0 != "<urn:test:pred:hidden>")
        );

        let enumerated = solution_rows(
            engine
                .query_with_visibility("SELECT ?g WHERE { GRAPH ?g {} }", &visible)
                .unwrap(),
        );
        assert_eq!(enumerated.len(), total);
        assert!(
            enumerated
                .iter()
                .all(|row| row.get("g").unwrap().0 != "<urn:test:pred:hidden>")
        );

        let fixed_hidden = solution_rows(
            engine
                .query_with_visibility(
                    "SELECT ?name WHERE { GRAPH <urn:test:pred:hidden> { ?s schema:name ?name } }",
                    &visible,
                )
                .unwrap(),
        );
        assert!(fixed_hidden.is_empty());

        let subject_rows = solution_rows(
            engine
                .query_with_visibility(
                    "SELECT ?pos WHERE { <urn:test:pred:shared> <http://schema.org/position> ?pos }",
                    &visible,
                )
                .unwrap(),
        );
        assert_eq!(subject_rows.len(), total);
        assert!(
            subject_rows
                .iter()
                .all(|row| row.get("pos").unwrap().0 != "\"hidden\"")
        );

        assert_eq!(
            engine
                .query_with_visibility("ASK { ?s ?p ?o }", &visible)
                .unwrap(),
            QueryResults::Boolean(true)
        );
        assert_eq!(
            engine
                .query_with_visibility("ASK { ?s ?p ?o }", &|_: &GraphId| false)
                .unwrap(),
            QueryResults::Boolean(false)
        );
    }

    #[test]
    fn visibility_hides_orphans() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_GRAPH_LIMIT + 4;
        let mut graphs = Vec::with_capacity(total);
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:predorphan:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:predorphan:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Visible {idx:03}"
                )))),
            );
            graphs.push(graph);
        }
        insert_quad(
            &store,
            &graphs[0],
            "./data/orphan.txt",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Orphaned File",
            ))),
        );
        store
            .set_graph_diagnostics(
                &graphs[0],
                &GraphDiagnostics::from_orphaned_entities(vec!["./data/orphan.txt".to_string()]),
            )
            .unwrap();

        let rows = solution_rows(
            engine
                .query_with_visibility(
                    "SELECT ?name WHERE { ?s schema:name ?name }",
                    &|_: &GraphId| true,
                )
                .unwrap(),
        );
        assert_eq!(rows.len(), total);
        assert!(
            rows.iter()
                .all(|row| row.get("name").unwrap().0 != "\"Orphaned File\"")
        );
    }

    #[test]
    fn visibility_memoizes_graphs() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_GRAPH_LIMIT + 8;
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:memo:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:memo:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Dataset {idx:03}"
                )))),
            );
        }

        let calls: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
        let visible = |graph: &GraphId| {
            *calls
                .borrow_mut()
                .entry(graph.as_str().to_string())
                .or_insert(0) += 1;
            true
        };

        let rows = solution_rows(
            engine
                .query_with_visibility("SELECT ?s ?name WHERE { ?s schema:name ?name }", &visible)
                .unwrap(),
        );
        assert_eq!(rows.len(), total);

        let calls = calls.into_inner();
        assert_eq!(calls.len(), total);
        assert!(calls.values().all(|&count| count == 1), "{calls:?}");
    }

    #[test]
    fn visibility_isolates_graphs() {
        let (_dir, store, _search, engine) = setup_engine();
        let visible_graph = GraphId::new("urn:test:join:visible");
        let hidden_graph = GraphId::new("urn:test:join:hidden");
        insert_quad(
            &store,
            &visible_graph,
            "urn:test:join:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset One",
            ))),
        );
        insert_quad(
            &store,
            &hidden_graph,
            "urn:test:join:e1",
            "http://schema.org/hidden",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("true"))),
        );

        let query = "SELECT ?name WHERE { ?s schema:name ?name . \
                     FILTER NOT EXISTS { ?s <http://schema.org/hidden> ?h } }";

        let rows = solution_rows(
            engine
                .query_with_visibility(query, &|graph: &GraphId| {
                    graph.as_str() != "urn:test:join:hidden"
                })
                .unwrap(),
        );
        assert_eq!(rows.len(), 1, "invisible graph must not feed NOT EXISTS");

        let rows = solution_rows(
            engine
                .query_with_visibility(query, &|_: &GraphId| true)
                .unwrap(),
        );
        assert!(rows.is_empty());
    }

    #[test]
    fn supports_algebra_combinations() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset One",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/description",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Primary record",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset Two",
            ))),
        );

        let query = r#"
            SELECT ?s ?label ?desc
            WHERE {
                {
                    GRAPH <urn:test:g1> {
                        ?s schema:name ?label .
                        OPTIONAL { ?s schema:description ?desc }
                        FILTER(?label = "Dataset One")
                    }
                }
                UNION
                {
                    GRAPH <urn:test:g1> {
                        ?s schema:name ?label .
                        OPTIONAL { ?s schema:description ?desc }
                        FILTER(?label = "Dataset Two")
                    }
                }
                BIND(CONCAT(STR(?label), "!") AS ?tag)
                FILTER(CONTAINS(?tag, "Dataset"))
            }
        "#;

        let rows = solution_rows(engine.query(query).unwrap());
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.contains_key("desc")));
    }

    #[test]
    fn supports_ask_construct() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Dataset One",
            ))),
        );

        assert_eq!(
            engine
                .query("ASK { GRAPH <urn:test:g1> { <urn:test:e1> schema:name \"Dataset One\" } }")
                .unwrap(),
            QueryResults::Boolean(true)
        );

        let graph = engine
            .query(
                "CONSTRUCT { ?s <urn:test:derived> ?name } WHERE { GRAPH <urn:test:g1> { ?s schema:name ?name } }",
            )
            .unwrap();
        match graph {
            QueryResults::Graph(triples) => {
                assert_eq!(triples.len(), 1);
                assert!(triples[0].1.0.contains("urn:test:derived"));
            }
            other => panic!("expected graph results, got {other:?}"),
        }
    }

    #[test]
    fn supports_nested_select() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("Alpha"))),
        );
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/keywords",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("omics"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("Beta"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/keywords",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("omics"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/keywords",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal("proteomics"))),
        );

        let query = r#"
            SELECT ?s ?name ?kwCount
            WHERE {
                {
                    SELECT ?s (COUNT(?kw) AS ?kwCount)
                    WHERE {
                        ?s schema:keywords ?kw .
                    }
                    GROUP BY ?s
                    HAVING(COUNT(?kw) >= 1)
                }
                ?s schema:name ?name .
            }
            ORDER BY DESC(?kwCount) ?name
            LIMIT 2
        "#;

        let rows = solution_rows(engine.query(query).unwrap());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name").unwrap().0, "\"Beta\"");
        assert_eq!(rows[1].get("name").unwrap().0, "\"Alpha\"");
    }

    #[test]
    fn selects_hide_orphans() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            graph.as_str(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                "http://schema.org/Dataset",
            )),
        );
        insert_quad(
            &store,
            &graph,
            graph.as_str(),
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Root Dataset",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "./data/",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                "http://schema.org/Dataset",
            )),
        );
        insert_quad(
            &store,
            &graph,
            "./data/",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden Dataset",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "./data/file.txt",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                "http://schema.org/MediaObject",
            )),
        );
        insert_quad(
            &store,
            &graph,
            "./data/file.txt",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden File",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "./data/",
            "http://schema.org/hasPart",
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked("./data/file.txt")),
        );
        store
            .set_graph_diagnostics(
                &graph,
                &GraphDiagnostics::from_orphaned_entities(vec![
                    "./data/".to_string(),
                    "./data/file.txt".to_string(),
                ]),
            )
            .unwrap();

        let rows = solution_rows(
            engine
                .query("SELECT ?name WHERE { GRAPH <urn:test:g1> { ?s schema:name ?name } }")
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").unwrap().0, "\"Root Dataset\"");

        let hidden_rows = solution_rows(
            engine
                .query(
                    "SELECT ?s ?child WHERE { GRAPH <urn:test:g1> { ?s schema:hasPart ?child } }",
                )
                .unwrap(),
        );
        assert!(hidden_rows.is_empty());
    }

    #[test]
    fn updates_materialize_changes() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/position",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::from(0_i32))),
        );

        let changes = engine
            .evaluate_update(
                &crate::AllowAllAuthorizer,
                "DELETE { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o } } \
                 INSERT { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o2 } } \
                 WHERE { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o . BIND(?o + 1 AS ?o2) } }",
                &UpdateOptions::default(),
            )
            .unwrap();

        assert_eq!(changes.len(), 2);
        assert!(matches!(changes[0], MaterializedQuadChange::Delete { .. }));
        assert!(matches!(changes[1], MaterializedQuadChange::Insert { .. }));
    }

    /// Needs a real tantivy index: the `search`-off stub returns no hits,
    /// which would make the FTS SERVICE clause bind nothing at all.
    #[cfg(feature = "search")]
    #[test]
    fn service_binds_hits() {
        let (_dir, store, search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Atlas",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/description",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Large-scale proteomics experiment",
            ))),
        );
        settle_diagnostics(&store, &graph);
        crate::flush_search_queue(&store, &search).unwrap();

        let query = r#"
            SELECT ?s ?g ?score ?name
            WHERE {
                SERVICE <urn:craqle:fts> {
                    ?s fts:query "proteomics" .
                    ?s fts:score ?score .
                    ?s fts:graph ?g .
                    ?s fts:limit 5 .
                }
                GRAPH ?g {
                    ?s schema:name ?name .
                }
            }
        "#;

        let rows = solution_rows(engine.query(query).unwrap());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("s").unwrap().0, "<urn:test:e1>");
        assert_eq!(rows[0].get("g").unwrap().0, "<urn:test:g1>");
        assert_eq!(rows[0].get("name").unwrap().0, "\"Proteomics Atlas\"");
        assert!(
            rows[0]
                .get("score")
                .unwrap()
                .0
                .contains("http://www.w3.org/2001/XMLSchema#double")
        );
    }

    /// Complete matching returns every match beyond a ranked page, and a match count over
    /// the query budget fails instead of truncating.
    #[cfg(feature = "search")]
    #[test]
    fn complete_matches_bounded() {
        let (_dir, store, search, engine) = setup_engine();
        for index in 0..30 {
            let graph = GraphId::new(&format!("urn:test:fts-complete:g{index}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:fts-complete:e{index}"),
                "http://schema.org/name",
                EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Zephyr survey {index}"
                )))),
            );
            settle_diagnostics(&store, &graph);
        }
        crate::flush_search_queue(&store, &search).unwrap();
        let service = |arguments: &str| {
            format!(
                "SELECT ?s WHERE {{ SERVICE <urn:craqle:fts> {{ \
                 ?s fts:query \"zephyr\" ; {arguments} }} }}"
            )
        };
        let run = |sparql: &str, limits: QueryLimits| {
            let options = QueryOptions {
                limits,
                ..QueryOptions::default()
            };
            engine
                .query_with_options(
                    QueryRun {
                        sparql,
                        options: &options,
                    },
                    &|_, _| true,
                )
                .map(|(_, execution)| solution_rows(execution.results).len())
        };

        let ranked = run(&service("fts:limit 5"), QueryLimits::default()).unwrap();
        assert_eq!(ranked, 5);
        let complete = run(&service("fts:complete true"), QueryLimits::default()).unwrap();
        assert_eq!(complete, 30);
        let limits = QueryLimits {
            max_intermediate_rows: 10,
            ..QueryLimits::default()
        };
        assert!(matches!(
            run(&service("fts:complete true"), limits),
            Err(SparqlError::QueryLimit { .. })
        ));
        assert!(matches!(
            run(
                &service("fts:complete true ; fts:limit 5"),
                QueryLimits::default()
            ),
            Err(SparqlError::Unsupported(_))
        ));
    }

    /// Needs a real tantivy index: the `search`-off stub returns no hits,
    /// which would make the FTS SERVICE clause bind nothing at all.
    #[cfg(feature = "search")]
    #[test]
    fn service_respects_visibility() {
        let (_dir, store, search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:fts:g1");
        let graph2 = GraphId::new("urn:test:fts:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:fts:e1",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Atlas",
            ))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:fts:e2",
            "http://schema.org/name",
            EncodedTerm::from_plain_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Archive",
            ))),
        );
        settle_diagnostics(&store, &graph1);
        settle_diagnostics(&store, &graph2);
        crate::flush_search_queue(&store, &search).unwrap();

        let query = r#"
            SELECT ?s ?g
            WHERE {
                SERVICE <urn:craqle:fts> {
                    ?s fts:query "proteomics" .
                    ?s fts:graph ?g .
                    ?s fts:limit 5 .
                }
            }
        "#;

        let rows = solution_rows(
            engine
                .query_with_visibility(query, &|graph: &GraphId| {
                    graph.as_str() != "urn:test:fts:g2"
                })
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("g").unwrap().0, "<urn:test:fts:g1>");

        let all_rows = solution_rows(
            engine
                .query_with_visibility(query, &|_: &GraphId| true)
                .unwrap(),
        );
        assert_eq!(all_rows.len(), 2);

        let fixed_hidden = r#"
            SELECT ?s
            WHERE {
                SERVICE <urn:craqle:fts> {
                    ?s fts:query "proteomics" .
                    ?s fts:graph <urn:test:fts:g2> .
                    ?s fts:limit 5 .
                }
            }
        "#;
        let hidden_rows = solution_rows(
            engine
                .query_with_visibility(fixed_hidden, &|graph: &GraphId| {
                    graph.as_str() != "urn:test:fts:g2"
                })
                .unwrap(),
        );
        assert!(hidden_rows.is_empty());
    }

    #[test]
    fn graph_skips_timings() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::CraqleNode::open(dir.path()).unwrap();
        let graph = GraphId::new("urn:test:graph-results");
        let insert = |subject: &str, predicate: &str| MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: EncodedTerm(format!("<{subject}>")),
            predicate: EncodedTerm(format!("<{predicate}>")),
            object: EncodedTerm("<urn:test:graph-results:key>".to_owned()),
        };
        node.apply_changes(
            &crate::AllowAllAuthorizer,
            &graph,
            vec![
                insert("urn:test:graph-results:a", "urn:test:graph-results:left"),
                insert("urn:test:graph-results:b", "urn:test:graph-results:right"),
            ],
        )
        .unwrap();
        let sparql = "SELECT ?a ?b WHERE { ?a <urn:test:graph-results:left> ?key . \
                      ?b <urn:test:graph-results:right> ?key }";
        let graphs = std::slice::from_ref(&graph);
        let runs = || DETAILED_RUNS.with(std::cell::Cell::get);
        let before = runs();
        let results = node
            .query_in_graphs(&crate::AllowAllAuthorizer, graphs, sparql)
            .unwrap();
        assert_eq!(before, runs());
        let detailed = node
            .query_in_graphs_with_options(
                &crate::AllowAllAuthorizer,
                graphs,
                sparql,
                &QueryOptions::default(),
            )
            .unwrap();
        assert_eq!(before + 1, runs());
        assert_eq!(results, detailed.results);
    }
}
