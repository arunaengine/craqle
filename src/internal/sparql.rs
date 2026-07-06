use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange};
use crate::search::SearchIndex;
use crate::store::{GraphStore, StoreError, TermId};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term, Triple, Variable};
use spareval::{
    DeleteInsertQuad, InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset,
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

pub type Result<T> = std::result::Result<T, SparqlError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResults {
    Solutions(Vec<HashMap<String, EncodedTerm>>),
    Boolean(bool),
    Graph(Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>),
}

pub struct SparqlEngine {
    store: Arc<GraphStore>,
    search: Arc<SearchIndex>,
    evaluator: QueryEvaluator,
}

type VisibleGraphSet = Option<HashSet<TermId>>;

pub type VisibleFn<'a> = dyn Fn(&GraphId) -> bool + 'a;

/// Which graphs a query may see. `Predicate` defers the decision to a
/// callback evaluated lazily per touched graph (memoized per query).
#[derive(Clone, Copy)]
enum GraphScope<'a> {
    #[cfg_attr(not(test), allow(dead_code))]
    All,
    List(&'a [GraphId]),
    Predicate(&'a VisibleFn<'a>),
}

/// Visible-graph counts up to this limit are scoped through an explicit
/// spareval dataset spec (planned as per-graph index lookups). Larger sets
/// are evaluated once over a union view filtered by graph term id, since the
/// dataset spec costs O(graphs) store reads and per-pattern iterator setup.
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

impl SparqlEngine {
    pub fn new(store: Arc<GraphStore>, search: Arc<SearchIndex>) -> Self {
        Self {
            store,
            search,
            evaluator: QueryEvaluator::new(),
        }
    }

    #[cfg(test)]
    pub fn query(&self, sparql: &str) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::All, planner_enabled())
    }

    pub fn query_with_graphs(&self, sparql: &str, graphs: &[GraphId]) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::List(graphs), planner_enabled())
    }

    pub fn query_with_visibility(
        &self,
        sparql: &str,
        visible: &VisibleFn<'_>,
    ) -> Result<QueryResults> {
        self.run_query(sparql, GraphScope::Predicate(visible), planner_enabled())
    }

    /// Like [`SparqlEngine::query_with_visibility`] with explicit control over
    /// the craqle plan optimizer (used by tests and as a debugging hatch).
    pub fn query_with_visibility_planned(
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
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let mut query = SparqlParser::new()
            .parse_query(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;

        rewrite_fts_query(&mut query, &self.search, &self.store, scope)?;
        if optimize {
            crate::planner::optimize_query(&mut query, &self.store);
            tracing::trace!(target: "craqle::planner", plan = %query, "craqle-optimized query");
        }

        let mut prepared = self.evaluator.prepare(&query);
        let dataset = match scope {
            GraphScope::All => {
                prepared.dataset_mut().set_default_graph_as_union();
                StoreDataset::new(&self.store, None)
            }
            GraphScope::Predicate(visible) => {
                // Union view with lazy visibility: the predicate runs at most
                // once per touched graph, so the per-query cost scales with
                // the graphs evaluation actually reaches, not the corpus.
                prepared.dataset_mut().set_default_graph_as_union();
                StoreDataset::with_predicate(&self.store, visible)
            }
            GraphScope::List(graphs) if graphs.len() <= EXPLICIT_DATASET_GRAPH_LIMIT => {
                // Scope the dataset to the visible graph list so patterns are
                // planned as graph-specific lookups instead of union scans.
                let mut seen = HashSet::with_capacity(graphs.len());
                let mut names: Vec<NamedNode> = Vec::with_capacity(graphs.len());
                for graph in graphs {
                    if seen.insert(graph.as_str())
                        && self
                            .store
                            .lookup_term(&EncodedTerm::from_named_node(&graph.0))?
                            .is_some()
                    {
                        names.push(graph.0.clone());
                    }
                }
                let default_graphs: Vec<GraphName> =
                    names.iter().cloned().map(Into::into).collect();
                let named_graphs: Vec<NamedOrBlankNode> =
                    names.into_iter().map(Into::into).collect();
                prepared.dataset_mut().set_default_graph(default_graphs);
                prepared
                    .dataset_mut()
                    .set_available_named_graphs(named_graphs);
                StoreDataset::new(&self.store, Some(hash_graph_list(graphs)))
            }
            GraphScope::List(graphs) => {
                // Large graph sets: evaluate once over the union view;
                // StoreDataset filters quads against the visible graph term
                // ids in O(1) per quad.
                prepared.dataset_mut().set_default_graph_as_union();
                StoreDataset::new(&self.store, Some(hash_graph_list(graphs)))
            }
        };
        let results = prepared.execute(dataset).map_err(map_eval_error)?;

        collect_query_results(results)
    }

    pub fn evaluate_update(&self, sparql: &str) -> Result<Vec<MaterializedQuadChange>> {
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
                    prepared.dataset_mut().set_default_graph_as_union();
                    let iter = prepared
                        .execute(StoreDataset::new(&self.store, None))
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

fn hash_graph_list(graphs: &[GraphId]) -> HashSet<TermId> {
    graphs
        .iter()
        .map(|graph| crate::store::hash_term(&EncodedTerm::from_named_node(&graph.0)))
        .collect()
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

fn rewrite_fts_query(
    query: &mut Query,
    search: &SearchIndex,
    store: &GraphStore,
    scope: GraphScope<'_>,
) -> Result<()> {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Construct { pattern, .. } => {
            let current = std::mem::replace(pattern, GraphPattern::Bgp { patterns: vec![] });
            *pattern = rewrite_graph_pattern(current, search, store, scope)?;
        }
    }
    Ok(())
}

fn rewrite_graph_pattern(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
    scope: GraphScope<'_>,
) -> Result<GraphPattern> {
    Ok(match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            pattern
        }
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_graph_pattern(*left, search, store, scope)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store, scope)?),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_graph_pattern(*left, search, store, scope)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store, scope)?),
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_graph_pattern(*left, search, store, scope)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store, scope)?),
        },
        GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
            left: Box::new(rewrite_graph_pattern(*left, search, store, scope)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store, scope)?),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_graph_pattern(*left, search, store, scope)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store, scope)?),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
            variables,
            aggregates,
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => match name {
            NamedNodePattern::NamedNode(node) if node.as_str() == FTS_SERVICE_IRI => {
                rewrite_fts_service(*inner, search, store, scope)?
            }
            other => GraphPattern::Service {
                name: other,
                inner: Box::new(rewrite_graph_pattern(*inner, search, store, scope)?),
                silent,
            },
        },
    })
}

fn rewrite_fts_service(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
    scope: GraphScope<'_>,
) -> Result<GraphPattern> {
    let spec = parse_fts_service_spec(pattern)?;
    if spec.limit == 0 {
        return Ok(GraphPattern::Values {
            variables: requested_fts_variables(&spec),
            bindings: Vec::new(),
        });
    }

    // Built only when a SERVICE clause exists; hit counts are bounded by
    // `spec.limit`, so the predicate path checks each hit's graph directly.
    let visible_set: Option<HashSet<&str>> = match scope {
        GraphScope::List(graphs) => Some(graphs.iter().map(GraphId::as_str).collect()),
        _ => None,
    };
    let graph_visible = |iri: &str| match scope {
        GraphScope::All => true,
        GraphScope::List(_) => visible_set
            .as_ref()
            .is_some_and(|visible| visible.contains(iri)),
        GraphScope::Predicate(visible) => visible(&GraphId::new(iri)),
    };

    flush_queued_search_updates(search, store)?;
    let hits = match &spec.graph {
        Some(FtsGraphBinding::Fixed(graph)) => {
            if !graph_visible(graph.as_str()) {
                Vec::new()
            } else {
                search.search_in_graph(
                    graph.as_str(),
                    spec.query.as_deref().unwrap_or(""),
                    spec.limit,
                )?
            }
        }
        _ => search.search(spec.query.as_deref().unwrap_or(""), spec.limit)?,
    };

    let variables = requested_fts_variables(&spec);
    if variables.is_empty() {
        return Err(SparqlError::Unsupported(
            "FTS SERVICE must bind at least one variable".into(),
        ));
    }

    let subject_filter = match &spec.subject {
        Some(FtsSubjectPattern::NamedNode(node)) => Some(node.as_str()),
        _ => None,
    };

    let mut bindings = Vec::new();
    for hit in hits {
        if !graph_visible(hit.graph_id.as_str()) {
            continue;
        }
        if subject_filter.is_some_and(|subject| hit.subject_iri != subject) {
            continue;
        }
        bindings.push(fts_binding_row(&variables, &spec, &hit)?);
    }

    Ok(GraphPattern::Values {
        variables,
        bindings,
    })
}

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
                spec.limit = literal.value().parse::<usize>().map_err(|_| {
                    SparqlError::Unsupported("fts:limit must be a positive integer".into())
                })?;
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

fn flush_queued_search_updates(_search: &SearchIndex, _store: &GraphStore) -> Result<()> {
    Ok(())
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

struct StoreDataset<'a> {
    store: &'a GraphStore,
    visibility: QuadVisibility<'a>,
}

/// How the union view decides graph visibility. `Predicate` resolves it
/// lazily: the first quad touched in a graph decodes the graph IRI, asks the
/// callback once, and memoizes the verdict by term id for the query.
#[derive(Clone)]
enum GraphFilter<'a> {
    All,
    Set {
        members: Rc<HashSet<TermId>>,
        ordered: Rc<Vec<TermId>>,
    },
    Predicate {
        visible: &'a VisibleFn<'a>,
        memo: Rc<RefCell<HashMap<TermId, bool>>>,
    },
}

/// Cheap-to-clone visibility filter shared with lazy quad iterators, which
/// must not borrow the dataset itself (`QueryableDataset` iterators may only
/// capture `'a`).
#[derive(Clone)]
struct QuadVisibility<'a> {
    store: &'a GraphStore,
    filter: GraphFilter<'a>,
    orphan_cache: Rc<RefCell<HashMap<TermId, Rc<HashSet<TermId>>>>>,
}

impl<'a> QuadVisibility<'a> {
    fn graph_is_visible(&self, graph: TermId) -> std::result::Result<bool, StoreDatasetError> {
        match &self.filter {
            GraphFilter::All => Ok(true),
            GraphFilter::Set { members, .. } => Ok(members.contains(&graph)),
            GraphFilter::Predicate { visible, memo } => {
                if let Some(&allowed) = memo.borrow().get(&graph) {
                    return Ok(allowed);
                }
                let term = self.store.decode_graph_term(graph)?;
                // Non-IRI graph terms fail closed.
                let allowed = if term.0.starts_with('<') && term.0.ends_with('>') {
                    let mut iri = term.0;
                    iri.pop();
                    iri.remove(0);
                    visible(&GraphId(NamedNode::new_unchecked(iri)))
                } else {
                    false
                };
                memo.borrow_mut().insert(graph, allowed);
                Ok(allowed)
            }
        }
    }

    fn orphaned_subjects_for_graph(
        &self,
        graph: TermId,
    ) -> std::result::Result<Rc<HashSet<TermId>>, StoreDatasetError> {
        if let Some(cached) = self.orphan_cache.borrow().get(&graph) {
            return Ok(cached.clone());
        }

        let diagnostics = self.store.graph_diagnostics_by_id(graph)?;
        let mut orphaned = HashSet::with_capacity(diagnostics.orphaned_entities.len());
        for entity_id in diagnostics.orphaned_entities {
            let term = EncodedTerm::from_named_node(&NamedNode::new_unchecked(&entity_id));
            if let Some(term_id) = self.store.lookup_term(&term)? {
                orphaned.insert(term_id);
            }
        }

        let orphaned = Rc::new(orphaned);
        self.orphan_cache
            .borrow_mut()
            .insert(graph, orphaned.clone());
        Ok(orphaned)
    }

    fn quad_is_visible(
        &self,
        quad: &crate::store::EncodedQuad,
    ) -> std::result::Result<bool, StoreDatasetError> {
        if !self.graph_is_visible(quad.graph)? {
            return Ok(false);
        }
        let orphaned = self.orphaned_subjects_for_graph(quad.graph)?;
        Ok(!orphaned.contains(&quad.subject) && !orphaned.contains(&quad.object))
    }
}

enum ResolvedPatternTerm {
    Any,
    Existing(TermId),
    Missing,
}

enum EitherIter<L, R> {
    Left(L),
    Right(R),
}

impl<L, R, T> Iterator for EitherIter<L, R>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            Self::Left(left) => left.next(),
            Self::Right(right) => right.next(),
        }
    }
}

type QuadResultIter<'a> = Box<
    dyn Iterator<Item = std::result::Result<crate::store::EncodedQuad, StoreDatasetError>> + 'a,
>;

/// Quads of all visible graphs matching the pattern, evaluated lazily so that
/// short-circuiting consumers (ASK, LIMIT) stop after a few graphs instead of
/// materializing the whole union. Streams graph-at-a-time wherever an index
/// can enumerate candidate graphs, checking visibility per graph before any
/// per-quad work so the cost tracks the graphs evaluation actually consumes.
fn union_quads_for_pattern<'a>(
    store: &'a GraphStore,
    visibility: &QuadVisibility<'a>,
    subject: Option<TermId>,
    predicate: Option<TermId>,
    object: Option<TermId>,
) -> QuadResultIter<'a> {
    if subject.is_none() {
        let candidate_graphs = match (&visibility.filter, object) {
            // Bound object: only graphs containing a matching quad.
            (_, Some(object_id)) => Some(match predicate {
                Some(predicate_id) => store.predicate_object_graphs(predicate_id, object_id),
                None => store.object_graphs(object_id),
            }),
            // Unbound object: every populated graph is a candidate.
            (GraphFilter::Set { ordered, .. }, None) => Some(ordered.as_ref().clone()),
            (GraphFilter::Predicate { .. }, None) => Some(store.populated_graph_ids()),
            (GraphFilter::All, None) => None,
        };
        if let Some(graphs) = candidate_graphs {
            let visibility = visibility.clone();
            return Box::new(graphs.into_iter().flat_map(move |graph| {
                let visible = match visibility.graph_is_visible(graph) {
                    Ok(visible) => visible,
                    Err(error) => return EitherIter::Right(std::iter::once(Err(error))),
                };
                if !visible {
                    return EitherIter::Left(Vec::new().into_iter().map(Ok));
                }
                let quads = match (predicate, object) {
                    (Some(predicate_id), Some(object_id)) => store
                        .predicate_object_subjects_in_graph(graph, predicate_id, object_id)
                        .into_iter()
                        .map(|subject| crate::store::EncodedQuad {
                            graph,
                            subject,
                            predicate: predicate_id,
                            object: object_id,
                        })
                        .collect::<Vec<_>>(),
                    (None, Some(object_id)) => store
                        .object_entries_in_graph(graph, object_id)
                        .into_iter()
                        .map(|(subject, predicate)| crate::store::EncodedQuad {
                            graph,
                            subject,
                            predicate,
                            object: object_id,
                        })
                        .collect::<Vec<_>>(),
                    (_, None) => {
                        match store.quads_for_pattern(Some(graph), None, predicate, None) {
                            Ok(quads) => quads,
                            Err(error) => {
                                return EitherIter::Right(std::iter::once(Err(error.into())));
                            }
                        }
                    }
                };
                EitherIter::Left(quads.into_iter().map(Ok))
            }));
        }
    }

    match store.quads_for_pattern(None, subject, predicate, object) {
        Ok(quads) => Box::new(quads.into_iter().map(Ok)),
        Err(error) => Box::new(std::iter::once(Err(error.into()))),
    }
}

impl<'a> StoreDataset<'a> {
    fn new(store: &'a GraphStore, visible_graphs: VisibleGraphSet) -> Self {
        let filter = match visible_graphs {
            None => GraphFilter::All,
            Some(members) => {
                let mut ordered: Vec<TermId> = members.iter().copied().collect();
                ordered.sort_unstable();
                GraphFilter::Set {
                    members: Rc::new(members),
                    ordered: Rc::new(ordered),
                }
            }
        };
        Self::with_filter(store, filter)
    }

    fn with_predicate(store: &'a GraphStore, visible: &'a VisibleFn<'a>) -> Self {
        Self::with_filter(
            store,
            GraphFilter::Predicate {
                visible,
                memo: Rc::new(RefCell::new(HashMap::new())),
            },
        )
    }

    fn with_filter(store: &'a GraphStore, filter: GraphFilter<'a>) -> Self {
        Self {
            store,
            visibility: QuadVisibility {
                store,
                filter,
                orphan_cache: Rc::new(RefCell::new(HashMap::new())),
            },
        }
    }

    fn resolve_pattern_term(&self, term: Option<&StoreTerm>) -> ResolvedPatternTerm {
        match term {
            None => ResolvedPatternTerm::Any,
            Some(StoreTerm::Existing(id)) => ResolvedPatternTerm::Existing(*id),
            Some(StoreTerm::Missing(_)) => ResolvedPatternTerm::Missing,
        }
    }

    fn decode_term(&self, id: TermId) -> std::result::Result<EncodedTerm, StoreDatasetError> {
        self.store.decode_term(id).map_err(Into::into)
    }

    fn externalize_encoded_term(
        &self,
        term: EncodedTerm,
    ) -> std::result::Result<Term, StoreDatasetError> {
        term.to_term().ok_or(StoreDatasetError::InvalidTerm(term.0))
    }

    fn externalize_store_term(
        &self,
        term: StoreTerm,
    ) -> std::result::Result<Term, StoreDatasetError> {
        match term {
            StoreTerm::Existing(id) => self.externalize_encoded_term(self.decode_term(id)?),
            StoreTerm::Missing(term) => self.externalize_encoded_term(term),
        }
    }
}

impl<'a> QueryableDataset<'a> for StoreDataset<'a> {
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
            + 'a,
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

        let subject = match subject {
            ResolvedPatternTerm::Any => None,
            ResolvedPatternTerm::Existing(id) => Some(id),
            ResolvedPatternTerm::Missing => unreachable!(),
        };
        let predicate = match predicate {
            ResolvedPatternTerm::Any => None,
            ResolvedPatternTerm::Existing(id) => Some(id),
            ResolvedPatternTerm::Missing => unreachable!(),
        };
        let object = match object {
            ResolvedPatternTerm::Any => None,
            ResolvedPatternTerm::Existing(id) => Some(id),
            ResolvedPatternTerm::Missing => unreachable!(),
        };

        let visibility = self.visibility.clone();
        match graph_name {
            Some(None) => {
                let quads =
                    union_quads_for_pattern(self.store, &visibility, subject, predicate, object);
                let mut seen = HashSet::new();
                Box::new(quads.filter_map(move |quad| {
                    let quad = match quad {
                        Ok(quad) => quad,
                        Err(error) => return Some(Err(error)),
                    };
                    match visibility.quad_is_visible(&quad) {
                        Ok(true) => {
                            let key = (quad.subject, quad.predicate, quad.object);
                            if seen.insert(key) {
                                Some(Ok(InternalQuad {
                                    subject: StoreTerm::Existing(quad.subject),
                                    predicate: StoreTerm::Existing(quad.predicate),
                                    object: StoreTerm::Existing(quad.object),
                                    graph_name: None,
                                }))
                            } else {
                                None
                            }
                        }
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    }
                }))
            }
            Some(Some(StoreTerm::Existing(graph))) => {
                match visibility.graph_is_visible(*graph) {
                    Ok(true) => {}
                    Ok(false) => return Box::new(std::iter::empty()),
                    Err(error) => return Box::new(std::iter::once(Err(error))),
                }
                match self
                    .store
                    .quads_for_pattern(Some(*graph), subject, predicate, object)
                {
                    Ok(quads) => Box::new(quads.into_iter().filter_map(move |quad| {
                        match visibility.quad_is_visible(&quad) {
                            Ok(true) => Some(Ok(InternalQuad {
                                subject: StoreTerm::Existing(quad.subject),
                                predicate: StoreTerm::Existing(quad.predicate),
                                object: StoreTerm::Existing(quad.object),
                                graph_name: Some(StoreTerm::Existing(quad.graph)),
                            })),
                            Ok(false) => None,
                            Err(error) => Some(Err(error)),
                        }
                    })),
                    Err(error) => Box::new(std::iter::once(Err(error.into()))),
                }
            }
            Some(Some(StoreTerm::Missing(_))) => Box::new(std::iter::empty()),
            None => {
                let quads =
                    union_quads_for_pattern(self.store, &visibility, subject, predicate, object);
                Box::new(quads.filter_map(move |quad| {
                    let quad = match quad {
                        Ok(quad) => quad,
                        Err(error) => return Some(Err(error)),
                    };
                    match visibility.quad_is_visible(&quad) {
                        Ok(true) => Some(Ok(InternalQuad {
                            subject: StoreTerm::Existing(quad.subject),
                            predicate: StoreTerm::Existing(quad.predicate),
                            object: StoreTerm::Existing(quad.object),
                            graph_name: Some(StoreTerm::Existing(quad.graph)),
                        })),
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    }
                }))
            }
        }
    }

    #[allow(refining_impl_trait)]
    fn internal_named_graphs(
        &self,
    ) -> Box<dyn Iterator<Item = std::result::Result<Self::InternalTerm, Self::Error>> + 'a> {
        let visibility = self.visibility.clone();
        Box::new(
            self.store
                .graph_term_id_iter()
                .filter_map(move |graph_id| match graph_id {
                    Ok(graph_id) => match visibility.graph_is_visible(graph_id) {
                        Ok(true) => Some(Ok(StoreTerm::Existing(graph_id))),
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    },
                    Err(error) => Some(Err(error.into())),
                }),
        )
    }

    fn internalize_term(&self, term: Term) -> std::result::Result<Self::InternalTerm, Self::Error> {
        let encoded = EncodedTerm::from_term(&term);
        Ok(match self.store.lookup_term(&encoded)? {
            Some(id) => StoreTerm::Existing(id),
            None => StoreTerm::Missing(encoded),
        })
    }

    fn externalize_term(&self, term: Self::InternalTerm) -> std::result::Result<Term, Self::Error> {
        self.externalize_store_term(term)
    }
}

fn collect_query_results(results: spareval::QueryResults<'_>) -> Result<QueryResults> {
    match results {
        spareval::QueryResults::Solutions(solutions) => {
            let variables: Vec<String> = solutions
                .variables()
                .iter()
                .map(|variable| variable.as_str().to_string())
                .collect();

            let mut rows = Vec::new();
            for solution in solutions {
                let solution = solution.map_err(map_eval_error)?;
                let mut row = HashMap::new();
                for variable in &variables {
                    if let Some(term) = solution.get(variable.as_str()) {
                        row.insert(variable.clone(), EncodedTerm::from_term(term));
                    }
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
                graph_id,
                subject_id,
                predicate_id,
                object_id,
                &Dot {
                    actor: ActorId::random(),
                    counter: 1,
                },
            )
            .unwrap();
        store.enqueue_fts(&mut batch, graph, subject_id).unwrap();
        store.commit(batch).unwrap();
    }

    fn solution_rows(results: QueryResults) -> Vec<HashMap<String, EncodedTerm>> {
        match results {
            QueryResults::Solutions(rows) => rows,
            other => panic!("expected solutions, got {other:?}"),
        }
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
        while search.process_queued_updates(&store, 50_000).unwrap() != 0 {}

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
        while search.process_queued_updates(&store, 50_000).unwrap() != 0 {}

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
