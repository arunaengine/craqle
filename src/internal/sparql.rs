use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange};
use crate::search::SearchIndex;
use crate::store::{GraphStore, StoreError, TermId};
use oxrdf::{Literal, NamedNode, Term, Triple, Variable};
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
type VisibleGraphIris = Option<HashSet<String>>;

const COMMON_PREFIXES: &str = "\
PREFIX schema: <http://schema.org/>\n\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
PREFIX fts: <urn:craqle:fts:>\n";

const FTS_SERVICE_IRI: &str = "urn:craqle:fts";
const FTS_QUERY_IRI: &str = "urn:craqle:fts:query";
const FTS_LIMIT_IRI: &str = "urn:craqle:fts:limit";
const FTS_SCORE_IRI: &str = "urn:craqle:fts:score";
const FTS_GRAPH_IRI: &str = "urn:craqle:fts:graph";
const FTS_QUEUE_FLUSH_CHUNK: usize = 50_000;

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
        self.query_with_visible_graphs(sparql, None)
    }

    pub fn query_with_graphs(&self, sparql: &str, graphs: &[GraphId]) -> Result<QueryResults> {
        let visible = self.resolve_visible_graphs(graphs)?;
        self.query_with_visible_graphs(sparql, visible)
    }

    fn query_with_visible_graphs(
        &self,
        sparql: &str,
        visible_graphs: VisibleGraphSet,
    ) -> Result<QueryResults> {
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let mut query = SparqlParser::new()
            .parse_query(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;

        let visible_graph_iris = visible_graph_iris(&self.store, &visible_graphs)?;
        rewrite_fts_query(
            &mut query,
            &self.search,
            &self.store,
            visible_graph_iris.as_ref(),
        )?;

        let mut prepared = self.evaluator.prepare(&query);
        prepared.dataset_mut().set_default_graph_as_union();
        let results = prepared
            .execute(StoreDataset::new(&self.store, visible_graphs))
            .map_err(map_eval_error)?;

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

    fn resolve_visible_graphs(&self, graphs: &[GraphId]) -> Result<VisibleGraphSet> {
        let mut visible = HashSet::with_capacity(graphs.len());
        for graph in graphs {
            let encoded = EncodedTerm::from_named_node(&graph.0);
            if let Some(term_id) = self.store.lookup_term(&encoded)? {
                visible.insert(term_id);
            }
        }
        Ok(Some(visible))
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

fn rewrite_fts_query(
    query: &mut Query,
    search: &SearchIndex,
    store: &GraphStore,
    visible_graphs: Option<&HashSet<String>>,
) -> Result<()> {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Construct { pattern, .. } => {
            let current = std::mem::replace(pattern, GraphPattern::Bgp { patterns: vec![] });
            *pattern = rewrite_graph_pattern(current, search, store, visible_graphs)?;
        }
    }
    Ok(())
}

fn rewrite_graph_pattern(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
    visible_graphs: Option<&HashSet<String>>,
) -> Result<GraphPattern> {
    Ok(match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            pattern
        }
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_graph_pattern(*left, search, store, visible_graphs)?),
            right: Box::new(rewrite_graph_pattern(
                *right,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_graph_pattern(*left, search, store, visible_graphs)?),
            right: Box::new(rewrite_graph_pattern(
                *right,
                search,
                store,
                visible_graphs,
            )?),
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_graph_pattern(*left, search, store, visible_graphs)?),
            right: Box::new(rewrite_graph_pattern(
                *right,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_graph_pattern(*left, search, store, visible_graphs)?),
            right: Box::new(rewrite_graph_pattern(
                *right,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_graph_pattern(
                *inner,
                search,
                store,
                visible_graphs,
            )?),
            variables,
            aggregates,
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => match name {
            NamedNodePattern::NamedNode(node) if node.as_str() == FTS_SERVICE_IRI => {
                rewrite_fts_service(*inner, search, store, visible_graphs)?
            }
            other => GraphPattern::Service {
                name: other,
                inner: Box::new(rewrite_graph_pattern(
                    *inner,
                    search,
                    store,
                    visible_graphs,
                )?),
                silent,
            },
        },
    })
}

fn rewrite_fts_service(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
    visible_graphs: Option<&HashSet<String>>,
) -> Result<GraphPattern> {
    let spec = parse_fts_service_spec(pattern)?;
    if spec.limit == 0 {
        return Ok(GraphPattern::Values {
            variables: requested_fts_variables(&spec),
            bindings: Vec::new(),
        });
    }

    flush_queued_search_updates(search, store)?;
    let hits = match &spec.graph {
        Some(FtsGraphBinding::Fixed(graph)) => {
            if visible_graphs.is_some_and(|visible| !visible.contains(graph.as_str())) {
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
        if visible_graphs.is_some_and(|visible| !visible.contains(hit.graph_id.as_str())) {
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

fn visible_graph_iris(
    store: &GraphStore,
    visible_graphs: &VisibleGraphSet,
) -> Result<VisibleGraphIris> {
    let Some(visible_graphs) = visible_graphs else {
        return Ok(None);
    };

    let mut iris = HashSet::with_capacity(visible_graphs.len());
    for graph_id in visible_graphs {
        let graph_term = store.decode_term(*graph_id)?;
        let Some(graph_name) = graph_term.to_named_node() else {
            return Err(SparqlError::InvalidTerm(graph_term.0));
        };
        iris.insert(graph_name.as_str().to_string());
    }
    Ok(Some(iris))
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
