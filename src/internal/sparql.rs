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
