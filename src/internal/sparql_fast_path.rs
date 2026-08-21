use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use oxrdf::{Literal, Term, Variable};
use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::sparql::{QueryResults, Result};
use crate::store::{EncodedQuad, QueryTermId, TermId};

const CANCELLATION_CHECK_INTERVAL: usize = 1_024;

fn enforce_hash_entries(entries: usize, limits: &crate::sparql::QueryLimits) -> Result<()> {
    if entries > limits.max_hash_entries {
        return Err(crate::sparql::SparqlError::QueryLimit {
            resource: "hash keys",
            limit: limits.max_hash_entries,
        });
    }
    Ok(())
}

/// Same-binary control for guarded SPARQL fast paths.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryFastPathMode {
    #[default]
    Auto,
    Disabled,
}

/// Guarded executor used for a complete query result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryFastPathKind {
    Ask,
    Projection,
    SelectLimit,
    NamedCount,
    UnionCount,
    CountDistinctSubject,
    CountDistinctObject,
    SubjectStarCount,
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
    TriangleAsk(TriplePlan),
    SelectLimit {
        triple: TriplePlan,
        variables: Vec<String>,
        limit: usize,
    },
    Projection {
        triple: TriplePlan,
        variables: Vec<String>,
    },
    Count {
        triple: TriplePlan,
        output: String,
        domain: crate::count_plan::CountValueDomain,
    },
    SubjectStarCount {
        triples: Vec<TriplePlan>,
        output: String,
    },
    OptionalSubjectStarCount {
        mandatory: Vec<TriplePlan>,
        optional: Vec<TriplePlan>,
        output: String,
    },
    SubjectSetCount {
        outer: Vec<TriplePlan>,
        inner: Vec<TriplePlan>,
        output: String,
        mode: crate::count_plan::SubjectSetMode,
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
    pub(crate) fn kind(&self) -> QueryFastPathKind {
        match self {
            Self::Ask(_) | Self::TriangleAsk(_) => QueryFastPathKind::Ask,
            Self::Projection { .. } => QueryFastPathKind::Projection,
            Self::SelectLimit { .. } => QueryFastPathKind::SelectLimit,
            Self::Count {
                domain: crate::count_plan::CountValueDomain::Subject,
                ..
            } => QueryFastPathKind::CountDistinctSubject,
            Self::Count {
                domain: crate::count_plan::CountValueDomain::Object,
                ..
            } => QueryFastPathKind::CountDistinctObject,
            Self::Count { triple, .. } => match &triple.graph {
                PatternGraph::Named(_) => QueryFastPathKind::NamedCount,
                PatternGraph::DefaultUnion => QueryFastPathKind::UnionCount,
            },
            Self::SubjectStarCount { .. } | Self::OptionalSubjectStarCount { .. } => {
                QueryFastPathKind::SubjectStarCount
            }
            Self::SubjectSetCount { .. } => QueryFastPathKind::HashJoinCount,
            Self::PropertyStar { .. } => QueryFastPathKind::PropertyStar,
            Self::HashJoinCount { .. } => QueryFastPathKind::HashJoinCount,
        }
    }

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
                GraphPattern::Project { inner, .. } => inner.as_ref(),
                pattern => pattern,
            };
            if let Some(triple) = single_triple(pattern) {
                Some(FastPathPlan::Ask(triple))
            } else {
                Some(FastPathPlan::TriangleAsk(triangle_triple(pattern)?))
            }
        }
        Query::Select {
            dataset: None,
            pattern,
            ..
        } => {
            if let Some(plan) = crate::count_plan::analyze(pattern) {
                return Some(plan);
            }
            let (inner, variables, limit) = projection(pattern)?;
            if let Some(triple) = single_triple(inner) {
                let variables = variables
                    .iter()
                    .map(|variable| variable.as_str().to_owned())
                    .collect();
                return Some(match limit {
                    Some(limit) => FastPathPlan::SelectLimit {
                        triple,
                        variables,
                        limit,
                    },
                    None => FastPathPlan::Projection { triple, variables },
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
    let (subject, triples) = same_subject_triples(pattern)?;
    Some(FastPathPlan::PropertyStar {
        triples,
        subject,
        variables,
        limit,
    })
}

pub(crate) fn same_subject_triples(
    pattern: &GraphPattern,
) -> Option<(PatternTerm, Vec<TriplePlan>)> {
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
    Some((subject, triples))
}

pub(crate) fn optional_subject_triples(
    pattern: &GraphPattern,
) -> Option<(Vec<TriplePlan>, Vec<TriplePlan>)> {
    let (graph, inner) = match pattern {
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(graph),
            inner,
        } => (
            PatternGraph::Named(EncodedTerm::from_named_node(graph)),
            inner.as_ref(),
        ),
        pattern => (PatternGraph::DefaultUnion, pattern),
    };
    let GraphPattern::LeftJoin {
        left,
        right,
        expression: None,
    } = inner
    else {
        return None;
    };
    let (GraphPattern::Bgp { patterns: left }, GraphPattern::Bgp { patterns: right }) =
        (left.as_ref(), right.as_ref())
    else {
        return None;
    };
    subject_relation_triples(graph, left, right, false)
}

pub(crate) fn subject_set_triples(
    pattern: &GraphPattern,
) -> Option<(
    Vec<TriplePlan>,
    Vec<TriplePlan>,
    crate::count_plan::SubjectSetMode,
)> {
    let (graph, inner) = match pattern {
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(graph),
            inner,
        } => (
            PatternGraph::Named(EncodedTerm::from_named_node(graph)),
            inner.as_ref(),
        ),
        pattern => (PatternGraph::DefaultUnion, pattern),
    };
    let (left, right, mode, require_variable_subject) = match inner {
        GraphPattern::Filter {
            expr: Expression::Exists(right),
            inner: left,
        } => (
            left.as_ref(),
            right.as_ref(),
            crate::count_plan::SubjectSetMode::Include,
            false,
        ),
        GraphPattern::Filter {
            expr: Expression::Not(expression),
            inner: left,
        } => {
            let Expression::Exists(right) = expression.as_ref() else {
                return None;
            };
            (
                left.as_ref(),
                right.as_ref(),
                crate::count_plan::SubjectSetMode::Exclude,
                false,
            )
        }
        GraphPattern::Minus { left, right } => (
            left.as_ref(),
            right.as_ref(),
            crate::count_plan::SubjectSetMode::Exclude,
            true,
        ),
        _ => return None,
    };
    let (GraphPattern::Bgp { patterns: left }, GraphPattern::Bgp { patterns: right }) =
        (left, right)
    else {
        return None;
    };
    let (left, right) = subject_relation_triples(
        graph,
        left.as_slice(),
        right.as_slice(),
        require_variable_subject,
    )?;
    Some((left, right, mode))
}

fn subject_relation_triples(
    graph: PatternGraph,
    left: &[TriplePattern],
    right: &[TriplePattern],
    require_variable_subject: bool,
) -> Option<(Vec<TriplePlan>, Vec<TriplePlan>)> {
    let first = left.first()?;
    if right.is_empty() {
        return None;
    }
    let subject = pattern_term(&first.subject)?;
    if require_variable_subject && !matches!(subject, PatternTerm::Variable(_)) {
        return None;
    }
    let mut object_variables = HashSet::new();
    let mut build = |patterns: &[TriplePattern]| {
        let mut triples = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            if pattern_term(&pattern.subject).as_ref() != Some(&subject)
                || !matches!(&pattern.predicate, NamedNodePattern::NamedNode(_))
            {
                return None;
            }
            if let TermPattern::Variable(variable) = &pattern.object
                && (matches!(&subject, PatternTerm::Variable(subject) if variable.as_str() == subject)
                    || !object_variables.insert(variable.as_str().to_owned()))
            {
                return None;
            }
            triples.push(triple_plan(graph.clone(), pattern)?);
        }
        Some(triples)
    };
    Some((build(left)?, build(right)?))
}

pub(crate) fn two_joined_triples(
    pattern: &GraphPattern,
) -> Option<(TriplePlan, TriplePlan, Vec<String>)> {
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

pub(crate) fn single_triple(pattern: &GraphPattern) -> Option<TriplePlan> {
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

fn triangle_triple(pattern: &GraphPattern) -> Option<TriplePlan> {
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
    let [first, _, _] = patterns else {
        return None;
    };
    let NamedNodePattern::NamedNode(predicate) = &first.predicate else {
        return None;
    };
    let mut subjects = HashSet::new();
    let mut objects = HashSet::new();
    for pattern in patterns {
        if !matches!(
            &pattern.predicate,
            NamedNodePattern::NamedNode(candidate) if candidate == predicate
        ) {
            return None;
        }
        let (TermPattern::Variable(subject), TermPattern::Variable(object)) =
            (&pattern.subject, &pattern.object)
        else {
            return None;
        };
        if subject == object {
            return None;
        }
        subjects.insert(subject.as_str());
        objects.insert(object.as_str());
    }
    if subjects.len() != 3 || subjects != objects {
        return None;
    }
    triple_plan(graph, first)
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
    pub(crate) fn binds(&self, variable: &Variable) -> bool {
        [&self.subject, &self.predicate, &self.object]
            .into_iter()
            .any(|term| matches!(term, PatternTerm::Variable(bound) if bound == variable.as_str()))
    }

    pub(crate) fn distinct_subject_order(&self, variable: &Variable) -> bool {
        matches!(
            &self.subject,
            PatternTerm::Variable(subject) if subject == variable.as_str()
        ) && matches!(&self.object, PatternTerm::Constant(_))
    }

    pub(crate) fn distinct_object_order(&self, variable: &Variable) -> bool {
        matches!(
            &self.object,
            PatternTerm::Variable(object) if object == variable.as_str()
        ) && matches!(&self.predicate, PatternTerm::Constant(_))
    }

    fn count_value_domain(&self, variable: &str) -> Option<crate::count_plan::CountValueDomain> {
        if matches!(&self.subject, PatternTerm::Variable(subject) if subject == variable) {
            Some(crate::count_plan::CountValueDomain::Subject)
        } else if matches!(&self.object, PatternTerm::Variable(object) if object == variable) {
            Some(crate::count_plan::CountValueDomain::Object)
        } else {
            None
        }
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

pub(crate) fn execute(
    plan: &FastPathPlan,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
) -> Result<FastPathOutcome> {
    let started = Instant::now();
    match plan {
        FastPathPlan::Ask(triple) => {
            let value = match triple.resolve(view, context)? {
                Some(triple) => view.exists(context, triple.selector, triple.pattern)?,
                None => false,
            };
            Ok(FastPathOutcome {
                results: QueryResults::Boolean(value),
                kind: QueryFastPathKind::Ask,
                execution_time: started.elapsed(),
                collection_time: Duration::ZERO,
                time_to_first_result: Some(started.elapsed()),
                intermediate_rows: u64::from(value),
                result_rows: 1,
                result_cells: 1,
            })
        }
        FastPathPlan::TriangleAsk(triple) => triangle_ask(triple, view, context, limits, started),
        FastPathPlan::SelectLimit {
            triple,
            variables,
            limit,
        } => execute_projection(
            triple,
            variables,
            *limit,
            QueryFastPathKind::SelectLimit,
            view,
            context,
            started,
        ),
        FastPathPlan::Projection { triple, variables } => execute_projection(
            triple,
            variables,
            usize::MAX,
            QueryFastPathKind::Projection,
            view,
            context,
            started,
        ),
        FastPathPlan::Count {
            triple,
            output,
            domain,
        } => {
            let mut count = 0_u64;
            if let Some(triple) = triple.resolve(view, context)? {
                if let Some(exact) = crate::count_exec::single_pattern_count(
                    view,
                    context,
                    triple.selector,
                    triple.pattern,
                    *domain,
                )? {
                    count = exact.get();
                } else {
                    let mut last_value = None;
                    let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
                    for quad in &mut cursor {
                        let quad = quad?;
                        let value = match domain {
                            crate::count_plan::CountValueDomain::Scalar => None,
                            crate::count_plan::CountValueDomain::Subject => Some(quad.subject),
                            crate::count_plan::CountValueDomain::Object => Some(quad.object),
                        };
                        if value.is_none() || last_value != value {
                            count = count.checked_add(1).ok_or_else(|| {
                                crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                            })?;
                            last_value = value;
                        }
                    }
                }
            }
            Ok(count_outcome(
                output,
                count,
                match domain {
                    crate::count_plan::CountValueDomain::Subject => {
                        QueryFastPathKind::CountDistinctSubject
                    }
                    crate::count_plan::CountValueDomain::Object => {
                        QueryFastPathKind::CountDistinctObject
                    }
                    crate::count_plan::CountValueDomain::Scalar => match &triple.graph {
                        PatternGraph::Named(_) => QueryFastPathKind::NamedCount,
                        PatternGraph::DefaultUnion => QueryFastPathKind::UnionCount,
                    },
                },
                count,
                started,
            ))
        }
        FastPathPlan::SubjectStarCount { triples, output } => {
            subject_star_count(triples, output, view, context, limits, started)
        }
        FastPathPlan::OptionalSubjectStarCount {
            mandatory,
            optional,
            output,
        } => {
            optional_subject_star_count(mandatory, optional, output, view, context, limits, started)
        }
        FastPathPlan::SubjectSetCount {
            outer,
            inner,
            output,
            mode,
        } => subject_set_count(outer, inner, output, *mode, view, context, limits),
        FastPathPlan::PropertyStar {
            triples,
            subject,
            variables,
            limit,
        } => execute_property_star(triples, subject, variables, *limit, view, context, started),
        FastPathPlan::HashJoinCount {
            left,
            right,
            output,
            join_variables,
        } => hash_join_count(left, right, output, join_variables, view, context, limits),
    }
}

fn execute_projection(
    triple: &TriplePlan,
    variables: &[String],
    limit: usize,
    kind: QueryFastPathKind,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    started: Instant,
) -> Result<FastPathOutcome> {
    let mut rows = Vec::with_capacity(limit.min(1_024));
    let mut collection_time = Duration::ZERO;
    let mut time_to_first_result = None;
    if let Some(triple) = triple.resolve(view, context)? {
        let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
        while rows.len() < limit {
            let Some(quad) = cursor.next() else {
                break;
            };
            let quad = quad?;
            if time_to_first_result.is_none() {
                time_to_first_result = Some(started.elapsed());
            }
            let collecting = Instant::now();
            rows.push(collect_row(view, context, &triple, quad, variables)?);
            collection_time = collection_time.saturating_add(collecting.elapsed());
        }
    }
    let result_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let result_cells = rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(u64::try_from(row.len()).unwrap_or(u64::MAX))
    });
    Ok(FastPathOutcome {
        results: QueryResults::Solutions(rows),
        kind,
        execution_time: started.elapsed().saturating_sub(collection_time),
        collection_time,
        time_to_first_result,
        intermediate_rows: result_rows,
        result_rows,
        result_cells,
    })
}

fn triangle_ask(
    triple: &TriplePlan,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
    started: Instant,
) -> Result<FastPathOutcome> {
    let Some(triple) = triple.resolve(view, context)? else {
        return Ok(boolean_ask_outcome(false, 0, started));
    };
    let (value, intermediate_rows) =
        if let Some(mut edges) = raw_triangle_edges(view, context, triple, limits)? {
            let intermediate_rows = u64::try_from(edges.len()).unwrap_or(u64::MAX);
            (
                sorted_edges_contain_triangle(&mut edges, context)?,
                intermediate_rows,
            )
        } else {
            let mut edges = Vec::new();
            let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
            for quad in &mut cursor {
                let quad = quad?;
                edges.push((quad.subject, quad.object));
                enforce_hash_entries(edges.len(), limits)?;
            }
            let intermediate_rows = u64::try_from(edges.len()).unwrap_or(u64::MAX);
            (
                sorted_edges_contain_triangle(&mut edges, context)?,
                intermediate_rows,
            )
        };
    Ok(boolean_ask_outcome(value, intermediate_rows, started))
}

fn raw_triangle_edges(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    triple: ResolvedTriple<'_>,
    limits: &crate::sparql::QueryLimits,
) -> Result<Option<Vec<(QueryTermId, QueryTermId)>>> {
    let Some(mut cursor) = view.raw_query_index_keys(context, triple.selector, triple.pattern)?
    else {
        return Ok(None);
    };
    let mut edges = Vec::new();
    let mut work = 0usize;
    match triple.selector {
        GraphSelector::Named(graph) => {
            let orphaned = view.orphaned_ids(context, graph)?;
            while let Some(key) = cursor.next_key() {
                let key = key?;
                context.increment_candidate_quads();
                context.record_qv_read(key.bytes_read);
                work += 1;
                if work == CANCELLATION_CHECK_INTERVAL {
                    work = 0;
                    context.check_cancelled()?;
                }
                let (matches, extracted) = cursor.matches(key);
                context.record_key_fields_extracted(extracted);
                if !matches {
                    continue;
                }
                context.record_key_fields_extracted(2);
                let edge = (key.subject(), key.object());
                if !orphaned.is_empty()
                    && (orphaned.contains(&cursor.source_term(edge.0)?)
                        || orphaned.contains(&cursor.source_term(edge.1)?))
                {
                    continue;
                }
                context.increment_matching_quads();
                edges.push(edge);
                enforce_hash_entries(edges.len(), limits)?;
            }
        }
        GraphSelector::DefaultUnion => {
            let mut graph_cache = HashMap::<QueryTermId, Option<Rc<HashSet<TermId>>>>::new();
            let mut current_edge = None;
            let mut edge_emitted = false;
            while let Some(key) = cursor.next_key() {
                let key = key?;
                context.increment_candidate_quads();
                context.record_qv_read(key.bytes_read);
                work += 1;
                if work == CANCELLATION_CHECK_INTERVAL {
                    work = 0;
                    context.check_cancelled()?;
                }
                let (matches, extracted) = cursor.matches(key);
                context.record_key_fields_extracted(extracted);
                if !matches {
                    continue;
                }
                context.record_key_fields_extracted(2);
                let edge = (key.subject(), key.object());
                if current_edge != Some(edge) {
                    current_edge = Some(edge);
                    edge_emitted = false;
                    context.increment_duplicate_groups();
                } else if edge_emitted {
                    context.increment_skipped_copies();
                    continue;
                }

                context.record_key_fields_extracted(1);
                let query_graph = key.graph();
                let orphaned = if let Some(orphaned) = graph_cache.get(&query_graph) {
                    orphaned.clone()
                } else {
                    let graph = cursor.source_term(query_graph)?;
                    let orphaned = if view.graph_is_visible(context, graph)? {
                        Some(view.orphaned_ids(context, graph)?)
                    } else {
                        None
                    };
                    graph_cache.insert(query_graph, orphaned.clone());
                    orphaned
                };
                let Some(orphaned) = orphaned else {
                    continue;
                };
                if !orphaned.is_empty()
                    && (orphaned.contains(&cursor.source_term(edge.0)?)
                        || orphaned.contains(&cursor.source_term(edge.1)?))
                {
                    continue;
                }
                edge_emitted = true;
                context.increment_matching_quads();
                edges.push(edge);
                enforce_hash_entries(edges.len(), limits)?;
            }
        }
        GraphSelector::Union => return Ok(None),
    }
    Ok(Some(edges))
}

fn sorted_edges_contain_triangle<T: Copy + Ord>(
    edges: &mut Vec<(T, T)>,
    context: &ReadContext<'_>,
) -> Result<bool> {
    edges.sort_unstable();
    edges.dedup();
    let mut work = 0usize;
    for &(first, second) in edges.iter() {
        let start = edges.partition_point(|(subject, _)| *subject < second);
        let end = edges.partition_point(|(subject, _)| *subject <= second);
        for &(_, third) in &edges[start..end] {
            work += 1;
            if work == CANCELLATION_CHECK_INTERVAL {
                work = 0;
                context.check_cancelled()?;
            }
            if edges.binary_search(&(third, first)).is_ok() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn boolean_ask_outcome(value: bool, intermediate_rows: u64, started: Instant) -> FastPathOutcome {
    FastPathOutcome {
        results: QueryResults::Boolean(value),
        kind: QueryFastPathKind::Ask,
        execution_time: started.elapsed(),
        collection_time: Duration::ZERO,
        time_to_first_result: Some(started.elapsed()),
        intermediate_rows,
        result_rows: 1,
        result_cells: 1,
    }
}

fn subject_star_count(
    triples: &[TriplePlan],
    output: &str,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
    started: Instant,
) -> Result<FastPathOutcome> {
    let mut resolved = Vec::with_capacity(triples.len());
    for triple in triples {
        let Some(triple) = triple.resolve(view, context)? else {
            return Ok(count_outcome(
                output,
                0,
                QueryFastPathKind::SubjectStarCount,
                0,
                started,
            ));
        };
        resolved.push(triple);
    }
    let patterns: Vec<_> = resolved
        .iter()
        .map(|triple| (triple.selector, triple.pattern))
        .collect();
    if let Some((count, intermediate_rows)) =
        crate::count_exec::subject_star_count(view, context, &patterns, limits.max_hash_entries)?
    {
        return Ok(count_outcome(
            output,
            count.get(),
            QueryFastPathKind::SubjectStarCount,
            intermediate_rows,
            started,
        ));
    }

    let mut relations = Vec::with_capacity(resolved.len());
    let mut intermediate_rows = 0_u64;
    let mut hash_entries = 0usize;
    for triple in resolved {
        let mut relation = HashMap::<TermId, u64>::new();
        let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
        for quad in &mut cursor {
            let before = relation.len();
            let count = relation.entry(quad?.subject).or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
            })?;
            if relation.len() != before {
                hash_entries = hash_entries.saturating_add(1);
                enforce_hash_entries(hash_entries, limits)?;
            }
            intermediate_rows = intermediate_rows.saturating_add(1);
        }
        relations.push(relation);
    }
    let Some((base_index, base)) = relations
        .iter()
        .enumerate()
        .min_by_key(|(_, relation)| relation.len())
    else {
        unreachable!("subject-star plans contain at least two triples")
    };
    let mut count = 0_u64;
    for (subject, multiplicity) in base {
        let mut product = *multiplicity;
        for (index, relation) in relations.iter().enumerate() {
            if index != base_index {
                product = product
                    .checked_mul(relation.get(subject).copied().unwrap_or(0))
                    .ok_or_else(|| {
                        crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                    })?;
            }
        }
        count = count
            .checked_add(product)
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
    }
    Ok(count_outcome(
        output,
        count,
        QueryFastPathKind::SubjectStarCount,
        intermediate_rows,
        started,
    ))
}

fn optional_subject_star_count(
    mandatory: &[TriplePlan],
    optional: &[TriplePlan],
    output: &str,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
    started: Instant,
) -> Result<FastPathOutcome> {
    let mut resolved = Vec::with_capacity(mandatory.len() + optional.len());
    for triple in mandatory {
        let Some(triple) = triple.resolve(view, context)? else {
            return Ok(count_outcome(
                output,
                0,
                QueryFastPathKind::SubjectStarCount,
                0,
                started,
            ));
        };
        resolved.push(triple);
    }
    for triple in optional {
        let Some(triple) = triple.resolve(view, context)? else {
            return subject_star_count(mandatory, output, view, context, limits, started);
        };
        resolved.push(triple);
    }
    let patterns: Vec<_> = resolved
        .iter()
        .map(|triple| (triple.selector, triple.pattern))
        .collect();
    if let Some((count, intermediate_rows)) = crate::count_exec::optional_subject_star_count(
        view,
        context,
        &patterns[..mandatory.len()],
        &patterns[mandatory.len()..],
        limits.max_hash_entries,
    )? {
        return Ok(count_outcome(
            output,
            count.get(),
            QueryFastPathKind::SubjectStarCount,
            intermediate_rows,
            started,
        ));
    }

    let mut relations = Vec::with_capacity(resolved.len());
    let mut intermediate_rows = 0_u64;
    let mut hash_entries = 0usize;
    for triple in resolved {
        let mut relation = HashMap::<TermId, u64>::new();
        let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
        for quad in &mut cursor {
            let before = relation.len();
            let count = relation.entry(quad?.subject).or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
            })?;
            if relation.len() != before {
                hash_entries = hash_entries.saturating_add(1);
                enforce_hash_entries(hash_entries, limits)?;
            }
            intermediate_rows = intermediate_rows.saturating_add(1);
        }
        relations.push(relation);
    }
    let (mandatory_relations, optional_relations) = relations.split_at(mandatory.len());
    let Some((base_index, base)) = mandatory_relations
        .iter()
        .enumerate()
        .min_by_key(|(_, relation)| relation.len())
    else {
        unreachable!("optional subject-star plans have a mandatory relation")
    };
    let mut count = 0_u64;
    for (subject, multiplicity) in base {
        if mandatory_relations
            .iter()
            .enumerate()
            .any(|(index, relation)| index != base_index && !relation.contains_key(subject))
        {
            continue;
        }
        let mut mandatory_product = *multiplicity;
        for (index, relation) in mandatory_relations.iter().enumerate() {
            if index != base_index {
                mandatory_product = mandatory_product
                    .checked_mul(relation.get(subject).copied().unwrap_or(0))
                    .ok_or_else(|| {
                        crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                    })?;
            }
        }
        let mut optional_product = 1_u64;
        if optional_relations
            .iter()
            .all(|relation| relation.contains_key(subject))
        {
            for relation in optional_relations {
                optional_product = optional_product
                    .checked_mul(relation.get(subject).copied().unwrap_or(0))
                    .ok_or_else(|| {
                        crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                    })?;
            }
        }
        count = count
            .checked_add(
                mandatory_product
                    .checked_mul(optional_product)
                    .ok_or_else(|| {
                        crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                    })?,
            )
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
    }
    Ok(count_outcome(
        output,
        count,
        QueryFastPathKind::SubjectStarCount,
        intermediate_rows,
        started,
    ))
}

fn subject_set_count(
    outer: &[TriplePlan],
    inner: &[TriplePlan],
    output: &str,
    mode: crate::count_plan::SubjectSetMode,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
) -> Result<FastPathOutcome> {
    let started = Instant::now();
    let mut resolved = Vec::with_capacity(outer.len() + inner.len());
    for triple in outer {
        let Some(triple) = triple.resolve(view, context)? else {
            return Ok(count_outcome(
                output,
                0,
                QueryFastPathKind::HashJoinCount,
                0,
                started,
            ));
        };
        resolved.push(triple);
    }
    for triple in inner {
        let Some(triple) = triple.resolve(view, context)? else {
            return match mode {
                crate::count_plan::SubjectSetMode::Include => Ok(count_outcome(
                    output,
                    0,
                    QueryFastPathKind::HashJoinCount,
                    0,
                    started,
                )),
                crate::count_plan::SubjectSetMode::Exclude => {
                    let mut outcome =
                        subject_star_count(outer, output, view, context, limits, started)?;
                    outcome.kind = QueryFastPathKind::HashJoinCount;
                    Ok(outcome)
                }
            };
        };
        resolved.push(triple);
    }
    let patterns: Vec<_> = resolved
        .iter()
        .map(|triple| (triple.selector, triple.pattern))
        .collect();
    if let Some((count, intermediate_rows)) = crate::count_exec::subject_set_count(
        view,
        context,
        &patterns[..outer.len()],
        &patterns[outer.len()..],
        mode,
        limits.max_hash_entries,
    )? {
        return Ok(count_outcome(
            output,
            count.get(),
            QueryFastPathKind::HashJoinCount,
            intermediate_rows,
            started,
        ));
    }

    let mut outer_relations = Vec::with_capacity(outer.len());
    let mut inner_relations = Vec::with_capacity(inner.len());
    let mut intermediate_rows = 0_u64;
    let mut hash_entries = 0usize;
    for (index, triple) in resolved.into_iter().enumerate() {
        let mut relation = HashMap::<TermId, u64>::new();
        let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
        for quad in &mut cursor {
            let before = relation.len();
            let count = relation.entry(quad?.subject).or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
            })?;
            if relation.len() != before {
                hash_entries = hash_entries.saturating_add(1);
                enforce_hash_entries(hash_entries, limits)?;
            }
            intermediate_rows = intermediate_rows.saturating_add(1);
        }
        if index < outer.len() {
            outer_relations.push(relation);
        } else {
            inner_relations.push(relation);
        }
    }
    let Some((base_index, base)) = outer_relations
        .iter()
        .enumerate()
        .min_by_key(|(_, relation)| relation.len())
    else {
        unreachable!("subject-set plans have an outer relation")
    };
    let mut count = 0_u64;
    for (subject, multiplicity) in base {
        if outer_relations
            .iter()
            .enumerate()
            .any(|(index, relation)| index != base_index && !relation.contains_key(subject))
        {
            continue;
        }
        let inner_matches = inner_relations
            .iter()
            .all(|relation| relation.contains_key(subject));
        if inner_matches != matches!(mode, crate::count_plan::SubjectSetMode::Include) {
            continue;
        }
        let mut product = *multiplicity;
        for (index, relation) in outer_relations.iter().enumerate() {
            if index != base_index {
                product = product
                    .checked_mul(relation.get(subject).copied().unwrap_or(0))
                    .ok_or_else(|| {
                        crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned())
                    })?;
            }
        }
        count = count
            .checked_add(product)
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
    }
    Ok(count_outcome(
        output,
        count,
        QueryFastPathKind::HashJoinCount,
        intermediate_rows,
        started,
    ))
}

fn hash_join_count(
    left: &TriplePlan,
    right: &TriplePlan,
    output: &str,
    join_variables: &[String],
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    limits: &crate::sparql::QueryLimits,
) -> Result<FastPathOutcome> {
    let started = Instant::now();
    let (Some(left_resolved), Some(right_resolved)) =
        (left.resolve(view, context)?, right.resolve(view, context)?)
    else {
        return Ok(count_outcome(
            output,
            0,
            QueryFastPathKind::HashJoinCount,
            0,
            started,
        ));
    };
    let (build_plan, build, probe_plan, probe) =
        if estimated_rows(view, left_resolved) <= estimated_rows(view, right_resolved) {
            (left, left_resolved, right, right_resolved)
        } else {
            (right, right_resolved, left, left_resolved)
        };
    if let [join_variable] = join_variables {
        let raw = match (
            build_plan.count_value_domain(join_variable),
            probe_plan.count_value_domain(join_variable),
        ) {
            (
                Some(crate::count_plan::CountValueDomain::Subject),
                Some(crate::count_plan::CountValueDomain::Subject),
            ) => crate::count_exec::subject_join_count(
                view,
                context,
                build.selector,
                build.pattern,
                probe.selector,
                probe.pattern,
                limits.max_hash_entries,
            )?,
            (
                Some(crate::count_plan::CountValueDomain::Object),
                Some(crate::count_plan::CountValueDomain::Object),
            ) => crate::count_exec::object_join_count(
                view,
                context,
                build.selector,
                build.pattern,
                probe.selector,
                probe.pattern,
                limits.max_hash_entries,
            )?,
            (
                Some(crate::count_plan::CountValueDomain::Object),
                Some(crate::count_plan::CountValueDomain::Subject),
            ) => crate::count_exec::object_subject_join_count(
                view,
                context,
                build.selector,
                build.pattern,
                probe.selector,
                probe.pattern,
                limits.max_hash_entries,
            )?,
            (
                Some(crate::count_plan::CountValueDomain::Subject),
                Some(crate::count_plan::CountValueDomain::Object),
            ) => crate::count_exec::subject_object_join_count(
                view,
                context,
                build.selector,
                build.pattern,
                probe.selector,
                probe.pattern,
                limits.max_hash_entries,
            )?,
            _ => None,
        };
        if let Some((count, intermediate_rows)) = raw {
            return Ok(count_outcome(
                output,
                count.get(),
                QueryFastPathKind::HashJoinCount,
                intermediate_rows,
                started,
            ));
        }
    }
    let mut table: HashMap<Vec<TermId>, u64> = HashMap::new();
    let mut intermediate_rows = 0_u64;
    let mut build_cursor = view.scan(context, build.selector, build.pattern)?;
    for quad in &mut build_cursor {
        let key = join_key(build_plan, quad?, join_variables);
        let before = table.len();
        let multiplicity = table.entry(key).or_default();
        *multiplicity = multiplicity
            .checked_add(1)
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
        if table.len() != before {
            enforce_hash_entries(table.len(), limits)?;
        }
        intermediate_rows = intermediate_rows.saturating_add(1);
    }
    let mut count = 0_u64;
    let mut probe_cursor = view.scan(context, probe.selector, probe.pattern)?;
    for quad in &mut probe_cursor {
        let key = join_key(probe_plan, quad?, join_variables);
        count = count
            .checked_add(table.get(&key).copied().unwrap_or(0))
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
        intermediate_rows = intermediate_rows.saturating_add(1);
    }
    Ok(count_outcome(
        output,
        count,
        QueryFastPathKind::HashJoinCount,
        intermediate_rows,
        started,
    ))
}

fn estimated_rows(view: &StoreReadView<'_>, triple: ResolvedTriple<'_>) -> usize {
    if let (Some(predicate), Some(object)) = (triple.pattern.predicate, triple.pattern.object) {
        view.store().stat_predicate_object_count(predicate, object)
    } else if let Some(predicate) = triple.pattern.predicate {
        view.store().stat_predicate_count(predicate)
    } else {
        view.store().stat_total_quads()
    }
}

fn join_key(triple: &TriplePlan, quad: EncodedQuad, join_variables: &[String]) -> Vec<TermId> {
    join_variables
        .iter()
        .map(|variable| {
            if matches!(&triple.subject, PatternTerm::Variable(subject) if subject == variable) {
                quad.subject
            } else if matches!(&triple.predicate, PatternTerm::Variable(predicate) if predicate == variable)
            {
                quad.predicate
            } else {
                debug_assert!(
                    matches!(&triple.object, PatternTerm::Variable(object) if object == variable)
                );
                quad.object
            }
        })
        .collect()
}

fn count_outcome(
    output: &str,
    count: u64,
    kind: QueryFastPathKind,
    intermediate_rows: u64,
    started: Instant,
) -> FastPathOutcome {
    let collecting = Instant::now();
    let row = HashMap::from([(
        output.to_owned(),
        EncodedTerm::from_term(&Term::Literal(Literal::from(count))),
    )]);
    let collection_time = collecting.elapsed();
    FastPathOutcome {
        results: QueryResults::Solutions(vec![row]),
        kind,
        execution_time: started.elapsed().saturating_sub(collection_time),
        collection_time,
        time_to_first_result: Some(started.elapsed()),
        intermediate_rows,
        result_rows: 1,
        result_cells: 1,
    }
}

fn execute_property_star(
    triples: &[TriplePlan],
    subject_term: &PatternTerm,
    projected: &[String],
    limit: usize,
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    started: Instant,
) -> Result<FastPathOutcome> {
    let mut resolved = Vec::with_capacity(triples.len());
    for triple in triples {
        let Some(triple) = triple.resolve(view, context)? else {
            return Ok(empty_property_star(started));
        };
        resolved.push(triple);
    }
    if limit == 0 {
        return Ok(empty_property_star(started));
    }
    let seed = resolved
        .iter()
        .enumerate()
        .min_by_key(|(_, triple)| {
            if let (Some(predicate), Some(object)) =
                (triple.pattern.predicate, triple.pattern.object)
            {
                view.store().stat_predicate_object_count(predicate, object)
            } else if let Some(predicate) = triple.pattern.predicate {
                view.store().stat_predicate_count(predicate)
            } else {
                usize::MAX
            }
        })
        .map(|(index, _)| index)
        .expect("a property star has at least two patterns");
    let mut candidates = if resolved[0].pattern.subject.is_none() {
        Some(view.scan(context, resolved[seed].selector, resolved[seed].pattern)?)
    } else {
        None
    };
    let mut fixed_subject = resolved[0].pattern.subject;
    let mut visited = HashSet::new();
    let mut rows = Vec::new();
    let mut collection_time = Duration::ZERO;
    let mut time_to_first_result = None;
    let mut intermediate_rows = 0_u64;

    while rows.len() < limit {
        let subject = if let Some(subject) = fixed_subject.take() {
            subject
        } else {
            let Some(candidate) = candidates.as_mut().and_then(|cursor| cursor.next()) else {
                break;
            };
            candidate?.subject
        };
        if !visited.insert(subject) {
            continue;
        }
        let mut binding = HashMap::new();
        if let PatternTerm::Variable(subject_variable) = subject_term {
            binding.insert(subject_variable.as_str(), subject);
        }
        let mut bindings = vec![binding];
        for (plan, triple) in triples.iter().zip(&resolved) {
            let mut pattern = triple.pattern;
            pattern.subject = Some(subject);
            let values = match &plan.object {
                PatternTerm::Constant(_) => {
                    if view.exists(context, triple.selector, pattern)? {
                        vec![None]
                    } else {
                        Vec::new()
                    }
                }
                PatternTerm::Variable(_) => {
                    let mut values = Vec::new();
                    let mut cursor = view.scan(context, triple.selector, pattern)?;
                    while values.len() < limit {
                        let Some(quad) = cursor.next() else {
                            break;
                        };
                        values.push(Some(quad?.object));
                    }
                    values
                }
            };
            if values.is_empty() {
                bindings.clear();
                break;
            }
            let mut next = Vec::with_capacity(bindings.len().saturating_mul(values.len()));
            for binding in bindings {
                for value in &values {
                    let mut binding = binding.clone();
                    if let (PatternTerm::Variable(variable), Some(value)) = (&plan.object, value) {
                        binding.insert(variable.as_str(), *value);
                    }
                    next.push(binding);
                }
            }
            intermediate_rows =
                intermediate_rows.saturating_add(u64::try_from(next.len()).unwrap_or(u64::MAX));
            bindings = next;
        }
        for binding in bindings {
            if rows.len() == limit {
                break;
            }
            if time_to_first_result.is_none() {
                time_to_first_result = Some(started.elapsed());
            }
            let collecting = Instant::now();
            rows.push(decode_binding(view, context, &binding, projected)?);
            collection_time = collection_time.saturating_add(collecting.elapsed());
        }
    }
    let result_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let result_cells = rows.iter().fold(0_u64, |total, row| {
        total.saturating_add(u64::try_from(row.len()).unwrap_or(u64::MAX))
    });
    Ok(FastPathOutcome {
        results: QueryResults::Solutions(rows),
        kind: QueryFastPathKind::PropertyStar,
        execution_time: started.elapsed().saturating_sub(collection_time),
        collection_time,
        time_to_first_result,
        intermediate_rows,
        result_rows,
        result_cells,
    })
}

fn empty_property_star(started: Instant) -> FastPathOutcome {
    FastPathOutcome {
        results: QueryResults::Solutions(Vec::new()),
        kind: QueryFastPathKind::PropertyStar,
        execution_time: started.elapsed(),
        collection_time: Duration::ZERO,
        time_to_first_result: None,
        intermediate_rows: 0,
        result_rows: 0,
        result_cells: 0,
    }
}

fn decode_binding(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    binding: &HashMap<&str, TermId>,
    projected: &[String],
) -> Result<HashMap<String, EncodedTerm>> {
    let mut row = HashMap::with_capacity(projected.len());
    for variable in projected {
        if let Some(value) = binding.get(variable.as_str()) {
            row.insert(variable.clone(), view.decode_result_term(context, *value)?);
        }
    }
    Ok(row)
}

fn collect_row(
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    triple: &ResolvedTriple<'_>,
    quad: EncodedQuad,
    projected: &[String],
) -> Result<HashMap<String, EncodedTerm>> {
    let mut bindings = HashMap::with_capacity(3);
    for (term, value) in triple
        .terms
        .iter()
        .zip([quad.subject, quad.predicate, quad.object])
    {
        if let PatternTerm::Variable(variable) = term {
            bindings.insert(variable.as_str(), value);
        }
    }
    let mut row = HashMap::with_capacity(projected.len());
    for variable in projected {
        if let Some(value) = bindings.get(variable.as_str()) {
            row.insert(variable.clone(), view.decode_result_term(context, *value)?);
        }
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spargebra::SparqlParser;

    #[test]
    fn recognizes_guarded_algebra_shapes() {
        for query in [
            "ASK { <urn:s> <urn:p> <urn:o> }",
            "SELECT ?s WHERE { ?s <urn:p> ?o } LIMIT 10",
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?o }",
            "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s <urn:p> <urn:o> }",
            "SELECT (COUNT(?s) AS ?count) WHERE { ?s <urn:p> ?o }",
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?a ; <urn:q> ?b ; <urn:r> ?c }",
            "SELECT ?s ?a ?b WHERE { ?s <urn:p> ?o ; <urn:a> ?a ; <urn:b> ?b }",
            "SELECT ?a ?b WHERE { <urn:s> <urn:a> ?a ; <urn:b> ?b }",
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?key . ?s <urn:q> ?key }",
        ] {
            let parsed = SparqlParser::new().parse_query(query).unwrap();
            assert!(analyze(&parsed).is_some(), "{query}\n{parsed:#?}");
        }
    }
}
