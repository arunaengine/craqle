use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use oxrdf::{Literal, Term, Variable};
use spargebra::Query;
use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::sparql::{QueryResults, Result};
use crate::store::{EncodedQuad, TermId};

/// Same-binary control for guarded SPARQL fast paths.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryFastPathMode {
    #[default]
    Auto,
    Disabled,
}

/// Guarded executor used for a complete query result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFastPathKind {
    Ask,
    SelectLimit,
    NamedCount,
    UnionCount,
    CountDistinctSubject,
    PropertyStar,
    HashJoinCount,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum PatternTerm {
    Constant(EncodedTerm),
    Variable(String),
}

#[derive(Clone)]
enum PatternGraph {
    DefaultUnion,
    Named(EncodedTerm),
}

#[derive(Clone)]
pub(crate) struct TriplePlan {
    graph: PatternGraph,
    subject: PatternTerm,
    predicate: PatternTerm,
    object: PatternTerm,
}

#[derive(Clone)]
pub(crate) enum FastPathPlan {
    Ask(TriplePlan),
    SelectLimit {
        triple: TriplePlan,
        variables: Vec<String>,
        limit: usize,
    },
    Count {
        triple: TriplePlan,
        output: String,
        distinct_subject: bool,
    },
    PropertyStar {
        triples: Vec<TriplePlan>,
        subject: PatternTerm,
        variables: Vec<String>,
        limit: usize,
    },
    HashJoinCount {
        left: TriplePlan,
        right: TriplePlan,
        output: String,
        join_variables: Vec<String>,
    },
}

impl FastPathPlan {
    pub(crate) fn is_property_star(&self) -> bool {
        matches!(self, Self::PropertyStar { .. })
    }

    pub(crate) fn is_hash_join(&self) -> bool {
        matches!(self, Self::HashJoinCount { .. })
    }
}

pub(crate) struct FastPathOutcome {
    pub(crate) results: QueryResults,
    pub(crate) kind: QueryFastPathKind,
    pub(crate) execution_time: Duration,
    pub(crate) collection_time: Duration,
    pub(crate) time_to_first_result: Option<Duration>,
    pub(crate) intermediate_rows: u64,
    pub(crate) result_rows: u64,
    pub(crate) result_cells: u64,
}

pub(crate) fn analyze(query: &Query) -> Option<FastPathPlan> {
    match query {
        Query::Ask {
            dataset: None,
            pattern,
            ..
        } => {
            let pattern = match pattern {
                GraphPattern::Project { inner, variables } if variables.is_empty() => {
                    inner.as_ref()
                }
                pattern => pattern,
            };
            Some(FastPathPlan::Ask(single_triple(pattern)?))
        }
        Query::Select {
            dataset: None,
            pattern,
            ..
        } => {
            if let Some(plan) = count_plan(pattern) {
                return Some(plan);
            }
            let (inner, variables, limit) = projection(pattern)?;
            if let Some(triple) = single_triple(inner) {
                return Some(FastPathPlan::SelectLimit {
                    triple,
                    variables: variables
                        .iter()
                        .map(|variable| variable.as_str().to_owned())
                        .collect(),
                    limit: limit?,
                });
            }
            property_star_plan(
                inner,
                variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned())
                    .collect(),
                limit.unwrap_or(usize::MAX),
            )
        }
        _ => None,
    }
}

fn projection(pattern: &GraphPattern) -> Option<(&GraphPattern, &[Variable], Option<usize>)> {
    match pattern {
        GraphPattern::Project { inner, variables } => {
            Some((inner.as_ref(), variables.as_slice(), None))
        }
        GraphPattern::Slice {
            inner,
            start: 0,
            length: Some(limit),
        } => {
            let GraphPattern::Project { inner, variables } = inner.as_ref() else {
                return None;
            };
            Some((inner.as_ref(), variables.as_slice(), Some(*limit)))
        }
        _ => None,
    }
}

fn property_star_plan(
    pattern: &GraphPattern,
    variables: Vec<String>,
    limit: usize,
) -> Option<FastPathPlan> {
    let (graph, patterns) = match pattern {
        GraphPattern::Bgp { patterns } => (PatternGraph::DefaultUnion, patterns.as_slice()),
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(graph),
            inner,
        } => {
            let GraphPattern::Bgp { patterns } = inner.as_ref() else {
                return None;
            };
            (
                PatternGraph::Named(EncodedTerm::from_named_node(graph)),
                patterns.as_slice(),
            )
        }
        _ => return None,
    };
    if patterns.len() < 2 {
        return None;
    }
    let subject = pattern_term(&patterns[0].subject)?;
    let mut object_variables = HashSet::new();
    let mut triples = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        if pattern_term(&pattern.subject).as_ref() != Some(&subject)
            || !matches!(&pattern.predicate, NamedNodePattern::NamedNode(_))
        {
            return None;
        }
        if let TermPattern::Variable(variable) = &pattern.object
            && (matches!(&subject, PatternTerm::Variable(subject) if variable.as_str() == subject)
                || !object_variables.insert(variable.as_str()))
        {
            return None;
        }
        triples.push(triple_plan(graph.clone(), pattern)?);
    }
    Some(FastPathPlan::PropertyStar {
        triples,
        subject,
        variables,
        limit,
    })
}

fn count_plan(pattern: &GraphPattern) -> Option<FastPathPlan> {
    let GraphPattern::Project { inner, variables } = pattern else {
        return None;
    };
    let [output] = variables.as_slice() else {
        return None;
    };
    let GraphPattern::Extend {
        inner,
        variable,
        expression: Expression::Variable(aggregate_result),
    } = inner.as_ref()
    else {
        return None;
    };
    if variable != output {
        return None;
    }
    let GraphPattern::Group {
        inner,
        variables,
        aggregates,
    } = inner.as_ref()
    else {
        return None;
    };
    if !variables.is_empty() {
        return None;
    }
    let [(aggregate_variable, aggregate)] = aggregates.as_slice() else {
        return None;
    };
    if aggregate_variable != aggregate_result {
        return None;
    }
    let count_all = matches!(
        aggregate,
        AggregateExpression::CountSolutions { distinct: false }
    );
    if count_all && let Some((left, right, join_variables)) = two_joined_triples(inner) {
        return Some(FastPathPlan::HashJoinCount {
            left,
            right,
            output: output.as_str().to_owned(),
            join_variables,
        });
    }
    let triple = single_triple(inner)?;
    let distinct_subject = match aggregate {
        AggregateExpression::CountSolutions { distinct: false } => false,
        AggregateExpression::FunctionCall {
            name: AggregateFunction::Count,
            expr: Expression::Variable(variable),
            distinct: true,
        } if triple.distinct_subject_is_ordered(variable) => true,
        _ => return None,
    };
    Some(FastPathPlan::Count {
        triple,
        output: output.as_str().to_owned(),
        distinct_subject,
    })
}

fn two_joined_triples(pattern: &GraphPattern) -> Option<(TriplePlan, TriplePlan, Vec<String>)> {
    let (graph, patterns) = match pattern {
        GraphPattern::Bgp { patterns } => (PatternGraph::DefaultUnion, patterns.as_slice()),
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(graph),
            inner,
        } => {
            let GraphPattern::Bgp { patterns } = inner.as_ref() else {
                return None;
            };
            (
                PatternGraph::Named(EncodedTerm::from_named_node(graph)),
                patterns.as_slice(),
            )
        }
        _ => return None,
    };
    let [left, right] = patterns else {
        return None;
    };
    if !matches!(&left.predicate, NamedNodePattern::NamedNode(_))
        || !matches!(&right.predicate, NamedNodePattern::NamedNode(_))
    {
        return None;
    }
    let left = triple_plan(graph.clone(), left)?;
    let right = triple_plan(graph, right)?;
    let left_variables = triple_variable_names(&left);
    let right_variables = triple_variable_names(&right);
    let mut join_variables: Vec<_> = left_variables
        .intersection(&right_variables)
        .cloned()
        .collect();
    join_variables.sort();
    (!join_variables.is_empty()).then_some((left, right, join_variables))
}

fn triple_variable_names(triple: &TriplePlan) -> HashSet<String> {
    [&triple.subject, &triple.predicate, &triple.object]
        .into_iter()
        .filter_map(|term| match term {
            PatternTerm::Variable(variable) => Some(variable.clone()),
            PatternTerm::Constant(_) => None,
        })
        .collect()
}

fn single_triple(pattern: &GraphPattern) -> Option<TriplePlan> {
    match pattern {
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            triple_plan(PatternGraph::DefaultUnion, &patterns[0])
        }
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(graph),
            inner,
        } => {
            let GraphPattern::Bgp { patterns } = inner.as_ref() else {
                return None;
            };
            if patterns.len() != 1 {
                return None;
            }
            triple_plan(
                PatternGraph::Named(EncodedTerm::from_named_node(graph)),
                &patterns[0],
            )
        }
        _ => None,
    }
}

fn triple_plan(graph: PatternGraph, pattern: &TriplePattern) -> Option<TriplePlan> {
    let subject = pattern_term(&pattern.subject)?;
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(node) => {
            PatternTerm::Constant(EncodedTerm::from_named_node(node))
        }
        NamedNodePattern::Variable(variable) => PatternTerm::Variable(variable.as_str().to_owned()),
    };
    let object = pattern_term(&pattern.object)?;
    let variables: Vec<_> = [&subject, &predicate, &object]
        .into_iter()
        .filter_map(|term| match term {
            PatternTerm::Variable(variable) => Some(variable.as_str()),
            PatternTerm::Constant(_) => None,
        })
        .collect();
    if variables.iter().copied().collect::<HashSet<_>>().len() != variables.len() {
        return None;
    }
    Some(TriplePlan {
        graph,
        subject,
        predicate,
        object,
    })
}

fn pattern_term(term: &TermPattern) -> Option<PatternTerm> {
    match term {
        TermPattern::NamedNode(node) => {
            Some(PatternTerm::Constant(EncodedTerm::from_named_node(node)))
        }
        TermPattern::Literal(literal) => Some(PatternTerm::Constant(EncodedTerm::from_term(
            &Term::Literal(literal.clone()),
        ))),
        TermPattern::Variable(variable) => {
            Some(PatternTerm::Variable(variable.as_str().to_owned()))
        }
        TermPattern::BlankNode(_) => None,
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct ResolvedTriple<'a> {
    selector: GraphSelector,
    pattern: QuadPattern,
    terms: [&'a PatternTerm; 3],
}

impl TriplePlan {
    fn distinct_subject_is_ordered(&self, variable: &Variable) -> bool {
        matches!(
            &self.subject,
            PatternTerm::Variable(subject) if subject == variable.as_str()
        ) && matches!(&self.predicate, PatternTerm::Constant(_))
            && matches!(&self.object, PatternTerm::Constant(_))
    }

    fn resolve<'a>(
        &'a self,
        view: &StoreReadView<'_>,
        context: &ReadContext<'_>,
    ) -> Result<Option<ResolvedTriple<'a>>> {
        let selector = match &self.graph {
            PatternGraph::DefaultUnion => GraphSelector::DefaultUnion,
            PatternGraph::Named(graph) => {
                let Some(graph) = view.lookup_term(context, graph)? else {
                    return Ok(None);
                };
                GraphSelector::Named(graph)
            }
        };
        let Some(subject) = resolve_pattern_term(&self.subject, view, context)? else {
            return Ok(None);
        };
        let Some(predicate) = resolve_pattern_term(&self.predicate, view, context)? else {
            return Ok(None);
        };
        let Some(object) = resolve_pattern_term(&self.object, view, context)? else {
            return Ok(None);
        };
        Ok(Some(ResolvedTriple {
            selector,
            pattern: QuadPattern {
                subject,
                predicate,
                object,
                ..QuadPattern::default()
            },
            terms: [&self.subject, &self.predicate, &self.object],
        }))
    }
}

fn resolve_pattern_term(
    term: &PatternTerm,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
) -> Result<Option<Option<TermId>>> {
    match term {
        PatternTerm::Constant(term) => Ok(view.lookup_term(context, term)?.map(Some)),
        PatternTerm::Variable(_) => Ok(Some(None)),
    }
}
