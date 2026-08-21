use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use oxrdf::{Literal, Term, Variable};
use spargebra::Query;
use spargebra::algebra::GraphPattern;
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryFastPathKind {
    Ask,
    SelectLimit,
    NamedCount,
    UnionCount,
    CountDistinctSubject,
    CountDistinctObject,
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
        domain: crate::count_plan::CountValueDomain,
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
            Self::Ask(_) => QueryFastPathKind::Ask,
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
            if let Some(plan) = crate::count_plan::analyze(pattern) {
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
        FastPathPlan::SelectLimit {
            triple,
            variables,
            limit,
        } => {
            let mut rows = Vec::with_capacity(*limit);
            let mut collection_time = Duration::ZERO;
            let mut time_to_first_result = None;
            if let Some(triple) = triple.resolve(view, context)? {
                let mut cursor = view.scan(context, triple.selector, triple.pattern)?;
                while rows.len() < *limit {
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
                kind: QueryFastPathKind::SelectLimit,
                execution_time: started.elapsed().saturating_sub(collection_time),
                collection_time,
                time_to_first_result,
                intermediate_rows: result_rows,
                result_rows,
                result_cells,
            })
        }
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
        } => hash_join_count(left, right, output, join_variables, view, context, started),
    }
}

fn hash_join_count(
    left: &TriplePlan,
    right: &TriplePlan,
    output: &str,
    join_variables: &[String],
    view: &StoreReadView<'_>,
    context: &ReadContext<'_>,
    started: Instant,
) -> Result<FastPathOutcome> {
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
        let multiplicity = table.entry(key).or_default();
        *multiplicity = multiplicity
            .checked_add(1)
            .ok_or_else(|| crate::sparql::SparqlError::Evaluation("COUNT overflow".to_owned()))?;
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
            "SELECT ?s ?a ?b WHERE { ?s <urn:p> ?o ; <urn:a> ?a ; <urn:b> ?b }",
            "SELECT ?a ?b WHERE { <urn:s> <urn:a> ?a ; <urn:b> ?b }",
            "SELECT (COUNT(*) AS ?count) WHERE { ?s <urn:p> ?key . ?s <urn:q> ?key }",
        ] {
            let parsed = SparqlParser::new().parse_query(query).unwrap();
            assert!(analyze(&parsed).is_some(), "{query}\n{parsed:#?}");
        }
    }
}
