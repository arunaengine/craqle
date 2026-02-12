use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aruna_core::{EncodedTerm, GraphId, MaterializedQuadChange};
use aruna_rdf_store::{GraphStore, StoreError, TermId};
use aruna_search::SearchIndex;
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
    Store(#[from] aruna_rdf_store::StoreError),
    #[error("search error: {0}")]
    Search(#[from] aruna_search::SearchError),
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

const COMMON_PREFIXES: &str = "\
PREFIX schema: <http://schema.org/>\n\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
PREFIX fts: <urn:aruna:fts:>\n";

const FTS_SERVICE_IRI: &str = "urn:aruna:fts";
const FTS_QUERY_IRI: &str = "urn:aruna:fts:query";
const FTS_LIMIT_IRI: &str = "urn:aruna:fts:limit";
const FTS_SCORE_IRI: &str = "urn:aruna:fts:score";
const FTS_GRAPH_IRI: &str = "urn:aruna:fts:graph";
const FTS_NAME_IRI: &str = "urn:aruna:fts:name";
const FTS_DESCRIPTION_IRI: &str = "urn:aruna:fts:description";
const FTS_QUEUE_FLUSH_CHUNK: usize = 50_000;

impl SparqlEngine {
    pub fn new(store: Arc<GraphStore>, search: Arc<SearchIndex>) -> Self {
        Self {
            store,
            search,
            evaluator: QueryEvaluator::new(),
        }
    }

    pub fn query(&self, sparql: &str) -> Result<QueryResults> {
        let full = format!("{COMMON_PREFIXES}{sparql}");
        let mut query = SparqlParser::new()
            .parse_query(&full)
            .map_err(|e| SparqlError::Parse(e.to_string()))?;

        rewrite_fts_query(&mut query, &self.search, &self.store)?;

        let mut prepared = self.evaluator.prepare(&query);
        prepared.dataset_mut().set_default_graph_as_union();
        let results = prepared
            .execute(StoreDataset::new(&self.store))
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
                        .execute(StoreDataset::new(&self.store))
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
    name_var: Option<Variable>,
    description_var: Option<Variable>,
}

fn rewrite_fts_query(query: &mut Query, search: &SearchIndex, store: &GraphStore) -> Result<()> {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Construct { pattern, .. } => {
            let current = std::mem::replace(pattern, GraphPattern::Bgp { patterns: vec![] });
            *pattern = rewrite_graph_pattern(current, search, store)?;
        }
    }
    Ok(())
}

fn rewrite_graph_pattern(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
) -> Result<GraphPattern> {
    Ok(match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {
            pattern
        }
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_graph_pattern(*left, search, store)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store)?),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_graph_pattern(*left, search, store)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store)?),
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_graph_pattern(*left, search, store)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store)?),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
            variable,
            expression,
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_graph_pattern(*left, search, store)?),
            right: Box::new(rewrite_graph_pattern(*right, search, store)?),
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
            variables,
            aggregates,
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => match name {
            NamedNodePattern::NamedNode(node) if node.as_str() == FTS_SERVICE_IRI => {
                rewrite_fts_service(*inner, search, store)?
            }
            other => GraphPattern::Service {
                name: other,
                inner: Box::new(rewrite_graph_pattern(*inner, search, store)?),
                silent,
            },
        },
    })
}

fn rewrite_fts_service(
    pattern: GraphPattern,
    search: &SearchIndex,
    store: &GraphStore,
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
        Some(FtsGraphBinding::Fixed(graph)) => search.search_in_graph(
            graph.as_str(),
            spec.query.as_deref().unwrap_or(""),
            spec.limit,
        )?,
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
            FTS_NAME_IRI => {
                let TermPattern::Variable(variable) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:name must bind to a variable".into(),
                    ));
                };
                spec.name_var = Some(variable);
            }
            FTS_DESCRIPTION_IRI => {
                let TermPattern::Variable(variable) = pattern.object else {
                    return Err(SparqlError::Unsupported(
                        "fts:description must bind to a variable".into(),
                    ));
                };
                spec.description_var = Some(variable);
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
    if let Some(variable) = &spec.name_var {
        variables.push(variable.clone());
    }
    if let Some(variable) = &spec.description_var {
        variables.push(variable.clone());
    }
    variables
}

fn fts_binding_row(
    variables: &[Variable],
    spec: &FtsServiceSpec,
    hit: &aruna_search::SearchHit,
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
        } else if spec
            .name_var
            .as_ref()
            .is_some_and(|bound| bound == variable)
        {
            hit.name
                .as_ref()
                .map(|value| GroundTerm::Literal(Literal::new_simple_literal(value.clone())))
        } else if spec
            .description_var
            .as_ref()
            .is_some_and(|bound| bound == variable)
        {
            hit.description
                .as_ref()
                .map(|value| GroundTerm::Literal(Literal::new_simple_literal(value.clone())))
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

fn flush_queued_search_updates(search: &SearchIndex, store: &GraphStore) -> Result<()> {
    loop {
        let processed = search.process_queued_updates(store, FTS_QUEUE_FLUSH_CHUNK)?;
        if processed == 0 {
            break;
        }
    }
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
}

enum ResolvedPatternTerm {
    Any,
    Existing(TermId),
    Missing,
}

impl<'a> StoreDataset<'a> {
    fn new(store: &'a GraphStore) -> Self {
        Self { store }
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
        term.to_term()
            .ok_or_else(|| StoreDatasetError::InvalidTerm(term.0))
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

    fn all_named_graph_terms(&self) -> Vec<std::result::Result<StoreTerm, StoreDatasetError>> {
        match self.store.graphs() {
            Ok(graphs) => graphs
                .into_iter()
                .map(|graph| {
                    let encoded = EncodedTerm::from_named_node(&graph.0);
                    match self.store.lookup_term(&encoded)? {
                        Some(id) => Ok(StoreTerm::Existing(id)),
                        None => Err(StoreDatasetError::InvalidTerm(graph.as_str().to_string())),
                    }
                })
                .collect(),
            Err(error) => vec![Err(error.into())],
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
    ) -> std::vec::IntoIter<std::result::Result<InternalQuad<Self::InternalTerm>, Self::Error>>
    {
        let subject = self.resolve_pattern_term(subject);
        let predicate = self.resolve_pattern_term(predicate);
        let object = self.resolve_pattern_term(object);

        if matches!(subject, ResolvedPatternTerm::Missing)
            || matches!(predicate, ResolvedPatternTerm::Missing)
            || matches!(object, ResolvedPatternTerm::Missing)
        {
            return Vec::<std::result::Result<InternalQuad<Self::InternalTerm>, Self::Error>>::new(
            )
            .into_iter();
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

        let rows = match graph_name {
            Some(None) => match self
                .store
                .quads_for_pattern(None, subject, predicate, object)
            {
                Ok(quads) => {
                    let mut seen = HashSet::new();
                    quads
                        .into_iter()
                        .filter_map(move |quad| {
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
                        })
                        .collect()
                }
                Err(error) => vec![Err(error.into())],
            },
            Some(Some(StoreTerm::Existing(graph))) => {
                match self
                    .store
                    .quads_for_pattern(Some(*graph), subject, predicate, object)
                {
                    Ok(quads) => quads
                        .into_iter()
                        .map(|quad| {
                            Ok(InternalQuad {
                                subject: StoreTerm::Existing(quad.subject),
                                predicate: StoreTerm::Existing(quad.predicate),
                                object: StoreTerm::Existing(quad.object),
                                graph_name: Some(StoreTerm::Existing(quad.graph)),
                            })
                        })
                        .collect(),
                    Err(error) => vec![Err(error.into())],
                }
            }
            Some(Some(StoreTerm::Missing(_))) => Vec::new(),
            None => match self
                .store
                .quads_for_pattern(None, subject, predicate, object)
            {
                Ok(quads) => quads
                    .into_iter()
                    .map(|quad| {
                        Ok(InternalQuad {
                            subject: StoreTerm::Existing(quad.subject),
                            predicate: StoreTerm::Existing(quad.predicate),
                            object: StoreTerm::Existing(quad.object),
                            graph_name: Some(StoreTerm::Existing(quad.graph)),
                        })
                    })
                    .collect(),
                Err(error) => vec![Err(error.into())],
            },
        };

        rows.into_iter()
    }

    #[allow(refining_impl_trait)]
    fn internal_named_graphs(
        &self,
    ) -> std::vec::IntoIter<std::result::Result<Self::InternalTerm, Self::Error>> {
        self.all_named_graph_terms().into_iter()
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

        for quad in store.quads_for_pattern(Some(graph_id), None, None, None)? {
            changes.push(MaterializedQuadChange::Delete {
                graph: graph.clone(),
                subject: store.decode_term(quad.subject)?,
                predicate: store.decode_term(quad.predicate)?,
                object: store.decode_term(quad.object)?,
            });
        }
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::{ActorId, Dot};
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
        let (_dir, store, _search, engine) = setup_engine();
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

        let query = r#"
            SELECT ?s ?g ?score ?name
            WHERE {
                SERVICE <urn:aruna:fts> {
                    ?s fts:query "proteomics" .
                    ?s fts:score ?score .
                    ?s fts:graph ?g .
                    ?s fts:name ?name .
                    ?s fts:limit 5 .
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
}
