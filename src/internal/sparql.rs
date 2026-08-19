use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange};
use crate::query_context::{QueryCancellation, QueryReadMode, ReadContext, ReadStatistics};
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::search::SearchIndex;
use crate::store::{GraphStore, StoreError, StoreReadSnapshot, TermId};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Term, Triple, Variable};
use spareval::{
    DeleteInsertQuad, ExpressionTerm, InternalQuad, QueryEvaluationError, QueryEvaluator,
    QueryableDataset,
};
use spargebra::algebra::{GraphPattern, GraphTarget};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern};
use spargebra::{GraphUpdateOperation, Query, SparqlParser};

#[derive(Debug, thiserror::Error)]
pub enum SparqlError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("evaluation error: {0}")]
    Evaluation(String),
    #[error("unsupported SPARQL feature: {0}")]
    Unsupported(String),
    #[error("invalid RDF term: {0}")]
    InvalidTerm(String),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("search error: {0}")]
    Search(#[from] crate::search::SearchError),
}

pub(crate) type Result<T> = std::result::Result<T, SparqlError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResults {
    Solutions(Vec<HashMap<String, EncodedTerm>>),
    Boolean(bool),
    Graph(Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>),
}

pub(crate) struct SparqlEngine {
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
    evaluator: QueryEvaluator,
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

/// Visible-graph counts up to this limit populate spareval's available named
/// graph list. Larger sets avoid O(graphs) metadata reads and use the same
/// union view filtered by graph term id.
const EXPLICIT_DATASET_GRAPH_LIMIT: usize = 32;

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

/// Over-fetch factor for the FTS SERVICE. Graph visibility is decided per hit
/// *after* tantivy has ranked them, so asking the index for exactly `fts:limit`
/// hits silently returns fewer authorized rows than the caller requested.
const FTS_OVERFETCH_FACTOR: usize = 4;
/// Floor for the first over-fetch, so a small `fts:limit` still survives a run
/// of unreadable top-ranked hits without another index round trip.
const FTS_MIN_FETCH: usize = 64;

impl SparqlEngine {
    pub(crate) fn new(store: Arc<GraphStore>, search: Arc<SearchIndex>) -> Self {
        Self {
            store,
            search,
            evaluator: QueryEvaluator::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn query(&self, sparql: &str) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::All, planner_enabled())
    }

    pub(crate) fn query_with_graphs(
        &self,
        sparql: &str,
        graphs: &[GraphId],
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::List(graphs), planner_enabled())
    }

    pub(crate) fn query_with_graphs_read_mode(
        &self,
        sparql: &str,
        graphs: &[GraphId],
        read_mode: QueryReadMode,
    ) -> Result<(QueryResults, ReadStatistics)> {
        self.run_query_with_read_mode(
            sparql,
            GraphScope::List(graphs),
            planner_enabled(),
            read_mode,
        )
    }

    pub(crate) fn query_with_visibility(
        &self,
        sparql: &str,
        visible: &VisibleFn<'_>,
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::Predicate(visible), planner_enabled())
    }

    pub(crate) fn query_with_snapshot_visibility(
        &self,
        sparql: &str,
        policy_visible: &SnapshotVisibleFn<'_>,
    ) -> Result<QueryResults> {
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let mut query = SparqlParser::new()
            .parse_query(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;
        let view = StoreReadView::new(&self.store);
        let visible = |graph: &GraphId| policy_visible(view.snapshot(), graph);
        let scope = GraphScope::Predicate(&visible);

        rewrite_fts_query(
            &mut query,
            FtsRewriteCtx {
                search: self.search.as_ref(),
                scope,
                post_raw_visibility: Some((self.store.as_ref(), policy_visible)),
            },
        )?;
        if planner_enabled() {
            crate::planner::optimize_query(&mut query, &self.store);
            tracing::trace!(target: "craqle::planner", plan = %query, "craqle-optimized query");
        }
        self.execute_query(query, scope, &view)
    }

    /// Like [`SparqlEngine::query_with_visibility`] with explicit control over
    /// the craqle plan optimizer (used by tests and as a debugging hatch).
    pub(crate) fn query_with_visibility_planned(
        &self,
        sparql: &str,
        visible: &VisibleFn<'_>,
        optimize: bool,
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::Predicate(visible), optimize)
    }

    fn run_query(
        &self,
        sparql: &str,
        scope: GraphScope<'_>,
        optimize: bool,
    ) -> Result<QueryResults> {
        Ok(self
            .run_query_with_read_mode(sparql, scope, optimize, QueryReadMode::Auto)?
            .0)
    }

    fn run_query_with_read_mode(
        &self,
        sparql: &str,
        scope: GraphScope<'_>,
        optimize: bool,
        read_mode: QueryReadMode,
    ) -> Result<(QueryResults, ReadStatistics)> {
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let mut query = SparqlParser::new()
            .parse_query(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;

        rewrite_fts_query(
            &mut query,
            FtsRewriteCtx {
                search: self.search.as_ref(),
                scope,
                post_raw_visibility: None,
            },
        )?;
        if optimize {
            crate::planner::optimize_query(&mut query, &self.store);
            tracing::trace!(target: "craqle::planner", plan = %query, "craqle-optimized query");
        }

        let view = StoreReadView::with_read_mode(&self.store, read_mode);
        self.execute_query_with_statistics(query, scope, &view)
    }

    fn execute_query(
        &self,
        query: Query,
        scope: GraphScope<'_>,
        view: &StoreReadView<'_>,
    ) -> Result<QueryResults> {
        Ok(self.execute_query_with_statistics(query, scope, view)?.0)
    }

    fn execute_query_with_statistics(
        &self,
        query: Query,
        scope: GraphScope<'_>,
        view: &StoreReadView<'_>,
    ) -> Result<(QueryResults, ReadStatistics)> {
        let mut prepared = self.evaluator.prepare(&query);
        let default_union_marker = BlankNode::default();
        prepared
            .dataset_mut()
            .set_default_graph(vec![GraphName::BlankNode(default_union_marker.clone())]);
        let context = match scope {
            #[cfg(test)]
            GraphScope::All => ReadContext::new(QueryCancellation::new()),
            GraphScope::Predicate(visible) => {
                // Union view with lazy visibility: the predicate runs at most
                // once per touched graph, so the per-query cost scales with
                // the graphs evaluation actually reaches, not the corpus.
                ReadContext::with_graph_visibility(QueryCancellation::new(), visible)
            }
            GraphScope::List(graphs) if graphs.len() <= EXPLICIT_DATASET_GRAPH_LIMIT => {
                // Restrict named-graph enumeration to the visible graph list;
                // default-graph patterns still use the sentinel union view.
                //
                // Membership is decided by the *metadata* record, not by the
                // term table: a deleted graph's IRI survives interning, so
                // filtering on `lookup_term` would resurrect it here while the
                // union regime (which reads graph metadata) rightly omits it.
                // Both regimes must answer graph existence identically (G9).
                let mut seen = HashSet::with_capacity(graphs.len());
                let mut names: Vec<NamedNode> = Vec::with_capacity(graphs.len());
                for graph in graphs {
                    if seen.insert(graph.as_str()) && view.contains_graph(graph)? {
                        names.push(graph.0.clone());
                    }
                }
                let named_graphs: Vec<NamedOrBlankNode> =
                    names.into_iter().map(Into::into).collect();
                prepared
                    .dataset_mut()
                    .set_available_named_graphs(named_graphs);
                ReadContext::with_visible_graphs(QueryCancellation::new(), graphs.iter().cloned())
            }
            GraphScope::List(graphs) => {
                // Large graph sets: evaluate once over the union view;
                // the shared read context filters quads against the visible
                // graph term ids in O(1) per candidate.
                ReadContext::with_visible_graphs(QueryCancellation::new(), graphs.iter().cloned())
            }
        };
        let results = prepared
            .execute(StoreDataset::with_default_union_marker(
                view,
                &context,
                default_union_marker,
            ))
            .map_err(map_eval_error)?;

        let results = collect_query_results(results)?;
        Ok((results, context.snapshot()))
    }

    pub(crate) fn evaluate_update(&self, sparql: &str) -> Result<Vec<MaterializedQuadChange>> {
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let update = SparqlParser::new()
            .parse_update(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;

        let mut changes = Vec::new();
        for operation in &update.operations {
            match operation {
                GraphUpdateOperation::InsertData { data } => {
                    for quad in data {
                        changes.push(quad_to_insert(quad)?);
                    }
                }
                GraphUpdateOperation::DeleteData { data } => {
                    for quad in data {
                        changes.push(ground_quad_to_delete(quad)?);
                    }
                }
                GraphUpdateOperation::DeleteInsert {
                    delete,
                    insert,
                    using,
                    pattern,
                } => {
                    let mut prepared = self.evaluator.prepare_delete_insert(
                        delete.clone(),
                        insert.clone(),
                        None,
                        using.clone(),
                        pattern,
                    );
                    let view = StoreReadView::new(&self.store);
                    let default_union_marker = BlankNode::default();
                    prepared
                        .dataset_mut()
                        .set_default_graph(vec![GraphName::BlankNode(
                            default_union_marker.clone(),
                        )]);
                    let context = ReadContext::new(QueryCancellation::new());
                    let iter = prepared
                        .execute(StoreDataset::with_default_union_marker(
                            &view,
                            &context,
                            default_union_marker,
                        ))
                        .map_err(map_eval_error)?;

                    for quad in iter {
                        changes.push(delete_insert_quad_to_change(quad.map_err(map_eval_error)?)?);
                    }
                }
                GraphUpdateOperation::Clear { graph, .. }
                | GraphUpdateOperation::Drop { graph, .. } => {
                    changes.extend(materialize_graph_target_removals(&self.store, graph)?);
                }
                GraphUpdateOperation::Create { graph, .. } => {
                    if self.store.contains_graph(&GraphId(graph.clone()))? {
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
    score_var: Option<Variable>,
    graph: Option<FtsGraphBinding>,
}

/// Everything the FTS SERVICE rewrite needs: the index it reads and the
/// caller's graph scope.
#[derive(Clone, Copy)]
struct FtsRewriteCtx<'a> {
    search: &'a SearchIndex,
    scope: GraphScope<'a>,
    post_raw_visibility: Option<(&'a GraphStore, &'a SnapshotVisibleFn<'a>)>,
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

/// Graph-visibility verdicts for FTS hits, memoized by graph IRI.
///
/// Over-fetching surfaces many hits from the same graph and the `Predicate`
/// scope's callback costs a policy read per call, so each graph is decided at
/// most once per SERVICE clause.
struct FtsGraphVisibility<'a> {
    scope: GraphScope<'a>,
    listed: Option<HashSet<&'a str>>,
    memo: RefCell<HashMap<String, bool>>,
}

impl<'a> FtsGraphVisibility<'a> {
    fn new(scope: GraphScope<'a>) -> Self {
        let listed = match scope {
            GraphScope::List(graphs) => Some(graphs.iter().map(GraphId::as_str).collect()),
            _ => None,
        };
        Self {
            scope,
            listed,
            memo: RefCell::new(HashMap::new()),
        }
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
                if let Some(&allowed) = self.memo.borrow().get(graph_iri) {
                    return allowed;
                }
                let allowed = visible(&GraphId::new(graph_iri));
                self.memo.borrow_mut().insert(graph_iri.to_owned(), allowed);
                allowed
            }
        }
    }
}

/// Post-search filter applied to every hit tantivy returns.
struct FtsHitFilter<'a> {
    visibility: &'a FtsGraphVisibility<'a>,
    post_raw_visibility: Option<(&'a GraphStore, &'a SnapshotVisibleFn<'a>)>,
    /// Set when the SERVICE pinned its subject to a concrete IRI.
    subject: Option<&'a str>,
}

impl FtsHitFilter<'_> {
    fn keeps(
        &self,
        hit: &crate::search::SearchHit,
        current: Option<&StoreReadSnapshot>,
        current_memo: &mut HashMap<String, bool>,
    ) -> bool {
        if !self.visibility.allows(&hit.graph_id) {
            return false;
        }
        if let (Some((_, visible)), Some(current)) = (self.post_raw_visibility, current) {
            let allowed = *current_memo
                .entry(hit.graph_id.clone())
                .or_insert_with(|| visible(current, &GraphId::new(&hit.graph_id)));
            if !allowed {
                return false;
            }
        }
        self.subject
            .is_none_or(|subject| hit.subject_iri == subject)
    }
}

/// One FTS SERVICE lookup: what to search for, how many rows the caller asked
/// for, and which hits it is allowed to keep.
struct FtsSearchRequest<'a> {
    query: &'a str,
    limit: usize,
    /// `Some` restricts the index query to a single, already-visible graph.
    graph: Option<&'a str>,
    filter: FtsHitFilter<'a>,
}

/// Collects up to the requested number of authorized, deduplicated hits,
/// widening its over-fetch until the page fills or the index runs out.
fn search_visible_hits(
    search: &SearchIndex,
    request: &FtsSearchRequest<'_>,
) -> Result<Vec<crate::search::SearchHit>> {
    let mut fetch = request
        .limit
        .saturating_mul(FTS_OVERFETCH_FACTOR)
        .max(FTS_MIN_FETCH);
    loop {
        let raw = match request.graph {
            Some(graph) => search.search_in_graph(graph, request.query, fetch)?,
            None => search.search(request.query, fetch)?,
        };
        let raw_len = raw.len();
        let current = request
            .filter
            .post_raw_visibility
            .map(|(store, _)| store.read_snapshot());
        let mut current_memo = HashMap::new();

        let mut seen = crate::SeenHits::default();
        let mut kept = Vec::with_capacity(request.limit.min(raw_len));
        for hit in raw {
            if !seen.admits(&hit)
                || !request
                    .filter
                    .keeps(&hit, current.as_ref(), &mut current_memo)
            {
                continue;
            }
            kept.push(hit);
            if kept.len() == request.limit {
                return Ok(kept);
            }
        }

        // Short of `limit`. If the index returned fewer hits than we asked
        // for it has no more matches, so this is the complete answer.
        if raw_len < fetch {
            return Ok(kept);
        }
        match fetch.checked_mul(FTS_OVERFETCH_FACTOR) {
            Some(next) => fetch = next,
            None => return Ok(kept),
        }
    }
}

/// Rewrites an FTS SERVICE clause into an inline `VALUES` block.
///
/// The index is read at its **last committed state**: FTS updates still
/// sitting in the durable queue (drained by the search worker, G7) are not
/// visible to this clause. Callers that need read-your-writes must flush the
/// search worker first.
fn rewrite_fts_service(pattern: GraphPattern, cx: FtsRewriteCtx<'_>) -> Result<GraphPattern> {
    let spec = parse_fts_service_spec(pattern)?;
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

    let visibility = FtsGraphVisibility::new(cx.scope);
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
            graph,
            filter: FtsHitFilter {
                visibility: &visibility,
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

/// Read one FTS SERVICE block's arguments. `fts:limit` is clamped to
/// [`crate::MAX_SEARCH_LIMIT`] (10_000), never rejected.
fn parse_fts_service_spec(pattern: GraphPattern) -> Result<FtsServiceSpec> {
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

        set_or_check_subject(&mut spec, pattern.subject)?;

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
                // Clamped, not rejected: a large limit is a legitimate "give
                // me everything" and the other fts: arguments only error on
                // input they cannot interpret at all.
                spec.limit = literal
                    .value()
                    .parse::<usize>()
                    .map_err(|_| {
                        SparqlError::Unsupported("fts:limit must be a positive integer".into())
                    })?
                    .min(crate::MAX_SEARCH_LIMIT);
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

    Ok(spec)
}

fn set_or_check_subject(spec: &mut FtsServiceSpec, subject: TermPattern) -> Result<()> {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StoreTerm {
    Existing(TermId),
    Missing(EncodedTerm),
}

#[derive(Debug, thiserror::Error)]
enum StoreDatasetError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("invalid RDF term: {0}")]
    InvalidTerm(String),
}

enum ResolvedPatternTerm {
    Any,
    Existing(TermId),
    Missing,
}

struct StoreDataset<'store, 'context, 'visibility> {
    view: &'context StoreReadView<'store>,
    context: &'context ReadContext<'visibility>,
}

impl<'store, 'context, 'visibility> StoreDataset<'store, 'context, 'visibility> {
    fn new(
        view: &'context StoreReadView<'store>,
        context: &'context ReadContext<'visibility>,
    ) -> Self {
        Self { view, context }
    }

    fn resolve_pattern_term(&self, term: Option<&StoreTerm>) -> ResolvedPatternTerm {
        match term {
            None => ResolvedPatternTerm::Any,
            Some(StoreTerm::Existing(id)) => ResolvedPatternTerm::Existing(*id),
            Some(StoreTerm::Missing(_)) => ResolvedPatternTerm::Missing,
        }
    }

    fn decode_term(&self, id: TermId) -> std::result::Result<Arc<EncodedTerm>, StoreDatasetError> {
        self.view
            .decode_term_arc(self.context, id)
            .map_err(Into::into)
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
            StoreTerm::Existing(id) => {
                let decoded = self.decode_term(id)?;
                self.externalize_encoded_term(&decoded)
            }
            StoreTerm::Missing(term) => self.externalize_encoded_term(&term),
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
        let subject = self.resolve_pattern_term(subject);
        let predicate = self.resolve_pattern_term(predicate);
        let object = self.resolve_pattern_term(object);

        if matches!(subject, ResolvedPatternTerm::Missing)
            || matches!(predicate, ResolvedPatternTerm::Missing)
            || matches!(object, ResolvedPatternTerm::Missing)
        {
            return Box::new(std::iter::empty());
        }

        let bound = |term: ResolvedPatternTerm| match term {
            ResolvedPatternTerm::Any => None,
            ResolvedPatternTerm::Existing(id) => Some(id),
            ResolvedPatternTerm::Missing => unreachable!("missing terms short-circuit above"),
        };
        let pattern = QuadPattern {
            subject: bound(subject),
            predicate: bound(predicate),
            object: bound(object),
            ..QuadPattern::default()
        };
        let selector = match graph_name {
            Some(Some(StoreTerm::Existing(graph))) => GraphSelector::Named(*graph),
            Some(Some(StoreTerm::Missing(_))) => return Box::new(std::iter::empty()),
            Some(None) | None => GraphSelector::Union,
        };
        let quads = match self.view.scan(self.context, selector, pattern) {
            Ok(quads) => quads,
            Err(error) => return Box::new(std::iter::once(Err(error.into()))),
        };

        match graph_name {
            // Preserve the legacy direct default-union branch: it suppresses
            // duplicate triples across named graph copies.
            Some(None) => {
                let mut seen = HashSet::new();
                Box::new(quads.filter_map(move |quad| {
                    let quad = match quad {
                        Ok(quad) => quad,
                        Err(error) => return Some(Err(error.into())),
                    };
                    let key = (quad.subject, quad.predicate, quad.object);
                    seen.insert(key).then_some(Ok(InternalQuad {
                        subject: StoreTerm::Existing(quad.subject),
                        predicate: StoreTerm::Existing(quad.predicate),
                        object: StoreTerm::Existing(quad.object),
                        graph_name: None,
                    }))
                }))
            }
            Some(Some(StoreTerm::Existing(_))) => Box::new(quads.map(|quad| {
                let quad = quad.map_err(StoreDatasetError::from)?;
                Ok(InternalQuad {
                    subject: StoreTerm::Existing(quad.subject),
                    predicate: StoreTerm::Existing(quad.predicate),
                    object: StoreTerm::Existing(quad.object),
                    graph_name: Some(StoreTerm::Existing(quad.graph)),
                })
            })),
            Some(Some(StoreTerm::Missing(_))) => unreachable!("missing graph short-circuits above"),
            None => Box::new(quads.map(|quad| {
                let quad = quad.map_err(StoreDatasetError::from)?;
                Ok(InternalQuad {
                    subject: StoreTerm::Existing(quad.subject),
                    predicate: StoreTerm::Existing(quad.predicate),
                    object: StoreTerm::Existing(quad.object),
                    graph_name: Some(StoreTerm::Existing(quad.graph)),
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
        Box::new(self.view.store().graph_term_id_iter().filter_map(
            move |graph_id| match graph_id {
                Ok(graph_id) => match view.graph_is_visible(context, graph_id) {
                    Ok(true) => Some(Ok(StoreTerm::Existing(graph_id))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error.into())),
                },
                Err(error) => Some(Err(error.into())),
            },
        ))
    }

    /// Graph existence for `GRAPH <g> { ... }` (charter G9).
    ///
    /// A named graph exists iff its metadata record exists **and** the caller
    /// may see it. spareval's default implementation instead probes for one
    /// visible quad, which makes an empty graph — or one whose entities are
    /// all orphan-hidden — report as non-existent, and which disagrees with
    /// the explicit-dataset regime used for small visible sets.
    fn contains_internal_graph_name(
        &self,
        graph_name: &Self::InternalTerm,
    ) -> std::result::Result<bool, Self::Error> {
        let StoreTerm::Existing(graph) = graph_name else {
            // The IRI is not even interned, so no graph was ever created for it.
            return Ok(false);
        };
        Ok(self.view.store().contains_graph_by_id(*graph)?
            && self.view.graph_is_visible(self.context, *graph)?)
    }

    fn internalize_term(&self, term: Term) -> std::result::Result<Self::InternalTerm, Self::Error> {
        let encoded = EncodedTerm::from_term(&term);
        Ok(match self.view.lookup_term(self.context, &encoded)? {
            Some(id) => StoreTerm::Existing(id),
            None => StoreTerm::Missing(encoded),
        })
    }

    fn externalize_term(&self, term: Self::InternalTerm) -> std::result::Result<Term, Self::Error> {
        self.externalize_store_term(term)
    }

    /// Expression-term hooks: pinned to our cached decode/lookup path rather
    /// than left to spareval's defaults, which are defined in terms of
    /// `externalize_term`/`internalize_term` and would silently change shape
    /// if the trait's defaults ever do.
    ///
    /// `internal_term_effective_boolean_value` is deliberately *not*
    /// overridden: spareval defines it as
    /// `externalize_expression_term(term)?.effective_boolean_value()`, and
    /// `ExpressionTerm::effective_boolean_value` is crate-private. Any override
    /// would have to restate spareval's EBV table by hand and could drift from
    /// it — silently changing FILTER results. Inheriting the default keeps EBV
    /// exact and still routes through the cached externalization below.
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

fn collect_query_results(results: spareval::QueryResults<'_>) -> Result<QueryResults> {
    match results {
        spareval::QueryResults::Solutions(solutions) => {
            // Each solution carries its own (variable, term) pairs and yields
            // only the bound ones, so building the row from them is exactly
            // the old "for every projected variable, look it up" loop without
            // the per-cell linear scan and per-cell name clone.
            let mut rows = Vec::new();
            for solution in solutions {
                let solution = solution.map_err(map_eval_error)?;
                let mut row = HashMap::with_capacity(solution.len());
                for (variable, term) in solution.iter() {
                    row.insert(variable.as_str().to_string(), EncodedTerm::from_term(term));
                }
                rows.push(row);
            }
            Ok(QueryResults::Solutions(rows))
        }
        spareval::QueryResults::Boolean(value) => Ok(QueryResults::Boolean(value)),
        spareval::QueryResults::Graph(triples) => {
            let mut graph = Vec::new();
            for triple in triples {
                let Triple {
                    subject,
                    predicate,
                    object,
                } = triple.map_err(map_eval_error)?;
                graph.push((
                    EncodedTerm::from(&subject),
                    EncodedTerm::from_named_node(&predicate),
                    EncodedTerm::from_term(&object),
                ));
            }
            Ok(QueryResults::Graph(graph))
        }
    }
}

fn map_eval_error(error: QueryEvaluationError) -> SparqlError {
    SparqlError::Evaluation(error.to_string())
}

fn quad_to_insert(quad: &spargebra::term::Quad) -> Result<MaterializedQuadChange> {
    Ok(MaterializedQuadChange::Insert {
        graph: spargebra_graph_name_to_graph_id(&quad.graph_name)?,
        subject: EncodedTerm::from(&quad.subject),
        predicate: EncodedTerm::from_named_node(&quad.predicate),
        object: EncodedTerm::from_term(&quad.object),
    })
}

fn ground_quad_to_delete(quad: &spargebra::term::GroundQuad) -> Result<MaterializedQuadChange> {
    Ok(MaterializedQuadChange::Delete {
        graph: spargebra_graph_name_to_graph_id(&quad.graph_name)?,
        subject: EncodedTerm::from_named_node(&quad.subject),
        predicate: EncodedTerm::from_named_node(&quad.predicate),
        object: ground_term_to_encoded(&quad.object),
    })
}

fn delete_insert_quad_to_change(quad: DeleteInsertQuad) -> Result<MaterializedQuadChange> {
    match quad {
        DeleteInsertQuad::Delete(quad) => Ok(MaterializedQuadChange::Delete {
            graph: oxrdf_graph_name_to_graph_id(&quad.graph_name)?,
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object),
        }),
        DeleteInsertQuad::Insert(quad) => Ok(MaterializedQuadChange::Insert {
            graph: oxrdf_graph_name_to_graph_id(&quad.graph_name)?,
            subject: EncodedTerm::from(&quad.subject),
            predicate: EncodedTerm::from_named_node(&quad.predicate),
            object: EncodedTerm::from_term(&quad.object),
        }),
    }
}

fn spargebra_graph_name_to_graph_id(graph_name: &spargebra::term::GraphName) -> Result<GraphId> {
    match graph_name {
        spargebra::term::GraphName::NamedNode(node) => Ok(GraphId(node.clone())),
        spargebra::term::GraphName::DefaultGraph => Err(SparqlError::Unsupported(
            "default graph updates are not supported; use GRAPH <iri> { ... }".into(),
        )),
    }
}

fn oxrdf_graph_name_to_graph_id(graph_name: &oxrdf::GraphName) -> Result<GraphId> {
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

fn ground_term_to_encoded(term: &spargebra::term::GroundTerm) -> EncodedTerm {
    #[allow(unreachable_patterns)]
    match term {
        spargebra::term::GroundTerm::NamedNode(node) => EncodedTerm::from_named_node(node),
        spargebra::term::GroundTerm::Literal(literal) => EncodedTerm(literal.to_string()),
        _ => EncodedTerm(term.to_string()),
    }
}

fn materialize_graph_target_removals(
    store: &GraphStore,
    target: &GraphTarget,
) -> Result<Vec<MaterializedQuadChange>> {
    let graphs = match target {
        GraphTarget::NamedNode(node) => vec![GraphId(node.clone())],
        GraphTarget::DefaultGraph => Vec::new(),
        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => store.graphs()?,
    };

    let mut changes = Vec::new();
    for graph in graphs {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_id) = store.lookup_term(&graph_term)? else {
            continue;
        };

        store.for_each_quad_in_graph::<SparqlError, _>(graph_id, |quad| {
            changes.push(MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: store.decode_term(quad.subject)?,
                predicate: store.decode_term(quad.predicate)?,
                object: store.decode_term(quad.object)?,
            });
            Ok(())
        })?;
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ActorId, Dot, GraphDiagnostics};
    #[cfg(feature = "search")]
    use crate::search::QueueBound;
    use crate::store::{EncodedQuad, FtsSubject, QuadAdd};
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

    fn solution_rows(results: QueryResults) -> Vec<HashMap<String, EncodedTerm>> {
        match results {
            QueryResults::Solutions(rows) => rows,
            other => panic!("expected solutions, got {other:?}"),
        }
    }

    #[test]
    fn dataset_cursor_stops_after_the_first_accepted_row() {
        let (_dir, store, _search, _engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:early-stop");
        for index in 0..64 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:early-stop:{index:03}"),
                "urn:test:dataset:early-stop:p",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }

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
    fn named_dataset_cursor_is_lazy_and_matches_the_compatibility_collector() {
        let (_dir, store, _search, _engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:named");
        for index in 0..24 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:named:{index:03}"),
                "urn:test:dataset:named:p",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }
        insert_quad(
            &store,
            &graph,
            "urn:test:dataset:named:other",
            "urn:test:dataset:named:other-p",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("other"))),
        );

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
            Some(StoreTerm::Existing(id)) => Some(*id),
            Some(StoreTerm::Missing(_)) => panic!("fixture term should be interned"),
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
                    let StoreTerm::Existing(subject) = quad.subject else {
                        panic!("stored subject should be interned");
                    };
                    let StoreTerm::Existing(predicate) = quad.predicate else {
                        panic!("stored predicate should be interned");
                    };
                    let StoreTerm::Existing(object) = quad.object else {
                        panic!("stored object should be interned");
                    };
                    (subject, predicate, object)
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
        let mut rows = dataset.internal_quads_for_pattern(
            None,
            Some(&predicate),
            None,
            Some(Some(&graph_term)),
        );
        assert!(rows.next().unwrap().is_ok());
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
    fn shared_dataset_visibility_memoizes_and_hides_orphans() {
        let (_dir, store, _search, _engine) = setup_engine();
        let visible_graph = GraphId::new("urn:test:dataset:visible");
        let hidden_graph = GraphId::new("urn:test:dataset:hidden");
        let predicate = "urn:test:dataset:visibility:p";
        let object = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("shared")));
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
        assert_eq!(statistics.candidate_quads, 2);
    }

    #[test]
    fn union_copy_multiplicity_and_direct_default_dedup_remain_distinct() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:dataset:copies:1");
        let graph2 = GraphId::new("urn:test:dataset:copies:2");
        let object = EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("same")));
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
        assert_eq!(public_rows.len(), 2);

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
    }

    #[test]
    fn ask_hit_miss_and_limit_ten_remain_supported() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:dataset:limit");
        for index in 0..12 {
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:dataset:limit:{index:03}"),
                "urn:test:dataset:limit:p",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                    index.to_string(),
                ))),
            );
        }

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
        prepared.dataset_mut().set_default_graph_as_union();
        assert!(matches!(
            prepared
                .execute(StoreDataset::new(&view, &context))
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
        prepared.dataset_mut().set_default_graph_as_union();
        let rows = collect_query_results(
            prepared
                .execute(StoreDataset::new(&view, &context))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(solution_rows(rows).len(), 10);
        let statistics = context.snapshot();
        assert_eq!(statistics.index_seeks, 1);
        assert_eq!(statistics.candidate_quads, 10);
        assert_eq!(statistics.matching_quads, 10);
    }

    #[test]
    fn select_queries_use_union_default_graph() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset One"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset Two"))),
        );

        let rows = solution_rows(
            engine
                .query("SELECT ?s ?name WHERE { ?s schema:name ?name }")
                .unwrap(),
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn query_with_graphs_limits_default_and_named_graphs_to_visible_set() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset One"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset Two"))),
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
    fn large_visible_graph_sets_filter_through_union_view() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_DATASET_GRAPH_LIMIT + 8;
        let mut graphs = Vec::with_capacity(total);
        let shared_subject = "urn:test:large:shared";
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:large:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:large:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Dataset {idx:03}"
                )))),
            );
            insert_quad(
                &store,
                &graph,
                shared_subject,
                "http://schema.org/position",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden Dataset",
            ))),
        );
        insert_quad(
            &store,
            &hidden,
            shared_subject,
            "http://schema.org/position",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("hidden"))),
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
    fn large_visible_graph_sets_hide_orphaned_entities() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_DATASET_GRAPH_LIMIT + 4;
        let mut graphs = Vec::with_capacity(total);
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:orphan:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:orphan:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(format!(
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Orphaned File"))),
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
    fn predicate_visibility_filters_union_view() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_DATASET_GRAPH_LIMIT + 8;
        let shared_subject = "urn:test:pred:shared";
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:pred:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:pred:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(format!(
                    "Dataset {idx:03}"
                )))),
            );
            insert_quad(
                &store,
                &graph,
                shared_subject,
                "http://schema.org/position",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Hidden Dataset",
            ))),
        );
        insert_quad(
            &store,
            &hidden,
            shared_subject,
            "http://schema.org/position",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("hidden"))),
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
    fn predicate_visibility_hides_orphaned_entities() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_DATASET_GRAPH_LIMIT + 4;
        let mut graphs = Vec::with_capacity(total);
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:predorphan:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:predorphan:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(format!(
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Orphaned File"))),
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
    fn predicate_visibility_is_memoized_per_graph() {
        let (_dir, store, _search, engine) = setup_engine();
        let total = EXPLICIT_DATASET_GRAPH_LIMIT + 8;
        for idx in 0..total {
            let graph = GraphId::new(&format!("urn:test:memo:{idx:03}"));
            insert_quad(
                &store,
                &graph,
                &format!("urn:test:memo:{idx:03}:e"),
                "http://schema.org/name",
                EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(format!(
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
    fn predicate_visibility_blocks_cross_graph_influence() {
        let (_dir, store, _search, engine) = setup_engine();
        let visible_graph = GraphId::new("urn:test:join:visible");
        let hidden_graph = GraphId::new("urn:test:join:hidden");
        insert_quad(
            &store,
            &visible_graph,
            "urn:test:join:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset One"))),
        );
        insert_quad(
            &store,
            &hidden_graph,
            "urn:test:join:e1",
            "http://schema.org/hidden",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("true"))),
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
    fn query_supports_union_optional_bind_and_filter() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset One"))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/description",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Primary record",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset Two"))),
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
    fn ask_and_construct_queries_are_supported() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Dataset One"))),
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
    fn select_queries_support_group_order_and_subqueries() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:g1");
        let graph2 = GraphId::new("urn:test:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Alpha"))),
        );
        insert_quad(
            &store,
            &graph1,
            "urn:test:e1",
            "http://schema.org/keywords",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("omics"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Beta"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/keywords",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("omics"))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:e2",
            "http://schema.org/keywords",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("proteomics"))),
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
    fn orphaned_entities_are_hidden_from_select_queries() {
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Root Dataset"))),
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
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
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal("Hidden File"))),
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
    fn delete_insert_where_materializes_concrete_changes() {
        let (_dir, store, _search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/position",
            EncodedTerm::from_term(&Term::Literal(Literal::from(0_i32))),
        );

        let changes = engine
            .evaluate_update(
                "DELETE { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o } } \
                 INSERT { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o2 } } \
                 WHERE { GRAPH <urn:test:g1> { ?s <http://schema.org/position> ?o . BIND(?o + 1 AS ?o2) } }",
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
    fn service_fts_binds_hits_and_scores() {
        let (_dir, store, search, engine) = setup_engine();
        let graph = GraphId::new("urn:test:g1");
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Atlas",
            ))),
        );
        insert_quad(
            &store,
            &graph,
            "urn:test:e1",
            "http://schema.org/description",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Large-scale proteomics experiment",
            ))),
        );
        while search
            .process_queued_updates(
                &store,
                QueueBound {
                    chunk: 50_000,
                    max_token: None,
                },
            )
            .unwrap()
            != 0
        {}

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

    /// Needs a real tantivy index: the `search`-off stub returns no hits,
    /// which would make the FTS SERVICE clause bind nothing at all.
    #[cfg(feature = "search")]
    #[test]
    fn service_fts_respects_visibility_predicate() {
        let (_dir, store, search, engine) = setup_engine();
        let graph1 = GraphId::new("urn:test:fts:g1");
        let graph2 = GraphId::new("urn:test:fts:g2");
        insert_quad(
            &store,
            &graph1,
            "urn:test:fts:e1",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Atlas",
            ))),
        );
        insert_quad(
            &store,
            &graph2,
            "urn:test:fts:e2",
            "http://schema.org/name",
            EncodedTerm::from_term(&Term::Literal(Literal::new_simple_literal(
                "Proteomics Archive",
            ))),
        );
        while search
            .process_queued_updates(
                &store,
                QueueBound {
                    chunk: 50_000,
                    max_token: None,
                },
            )
            .unwrap()
            != 0
        {}

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
}
