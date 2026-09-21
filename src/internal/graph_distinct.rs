//! Evaluates DISTINCT graph and grouped COUNT(DISTINCT graph) shapes over dense query IDs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::RefCell;
use std::collections::HashMap;

use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, Term, Variable};
use spargebra::Query;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::graph_join::{
    GraphReader, JoinInput, MAX_VARIABLES, Method, Range, ReadCounts, Resolved, RowSet, Rows, Step,
    join_key,
};
use crate::rdf_read::{GraphSelector, RdfReadView};
use crate::sparql::Result;
use crate::store::QueryTermId;

/// Bounds planning work, which grows with the square of the pattern count.
const MAX_PATTERNS: usize = 32;

#[derive(Clone, Debug)]
enum Slot {
    Constant(EncodedTerm),
    Variable(usize),
}

#[derive(Clone, Debug)]
enum Output {
    /// Distinct key tuples, used below `DISTINCT` or `REDUCED`.
    Distinct,
    /// The number of distinct graphs per key tuple, once per aggregate variable.
    Count(Vec<Variable>),
}

/// A `GRAPH ?g { BGP }` whose consumer only needs distinct key tuples per graph.
#[derive(Clone, Debug)]
pub(crate) struct GraphDistinctPlan {
    /// Unary operators between the query root and the replaced node.
    depth: usize,
    graph: usize,
    patterns: Vec<[Slot; 3]>,
    variables: usize,
    keys: Vec<(Variable, usize)>,
    output: Output,
}

/// Recognizes a query whose graph-level result can replace one subtree.
pub(crate) fn analyze(query: &Query) -> Option<GraphDistinctPlan> {
    let Query::Select {
        dataset: None,
        pattern,
        ..
    } = query
    else {
        return None;
    };
    let mut node = pattern;
    let mut depth = 0;
    loop {
        match node {
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => return group_plan(inner, variables, aggregates).map(|plan| plan.at(depth)),
            GraphPattern::Distinct { inner } | GraphPattern::Reduced { inner } => {
                if let Some((plan, offset)) = distinct_plan(inner) {
                    return Some(plan.at(depth + 1 + offset));
                }
            }
            _ => {}
        }
        node = unary_inner(node)?;
        depth += 1;
    }
}

/// Replaces the planned subtree with the computed relation.
pub(crate) fn substitute(plan: &GraphDistinctPlan, query: &mut Query, values: GraphPattern) {
    let Query::Select { pattern, .. } = query else {
        return;
    };
    let mut node = pattern;
    for _ in 0..plan.depth {
        let Some(inner) = unary_inner_mut(node) else {
            return;
        };
        node = inner;
    }
    *node = values;
}

impl GraphDistinctPlan {
    fn at(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }
}

fn unary_inner(pattern: &GraphPattern) -> Option<&GraphPattern> {
    match pattern {
        GraphPattern::Slice { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Filter { inner, .. } => Some(inner),
        _ => None,
    }
}

fn unary_inner_mut(pattern: &mut GraphPattern) -> Option<&mut GraphPattern> {
    match pattern {
        GraphPattern::Slice { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::Filter { inner, .. } => Some(inner),
        _ => None,
    }
}

/// `DISTINCT(PROJECT(ORDER BY*(GRAPH)))`, where ordering reads projected variables only.
fn distinct_plan(pattern: &GraphPattern) -> Option<(GraphDistinctPlan, usize)> {
    let GraphPattern::Project { inner, variables } = pattern else {
        return None;
    };
    let mut node = inner.as_ref();
    let mut offset = 1;
    while let GraphPattern::OrderBy { inner, expression } = node {
        let projected = expression.iter().all(|order| {
            let (OrderExpression::Asc(Expression::Variable(variable))
            | OrderExpression::Desc(Expression::Variable(variable))) = order
            else {
                return false;
            };
            variables.contains(variable)
        });
        if !projected {
            return None;
        }
        node = inner;
        offset += 1;
    }
    let plan = graph_plan(node, variables, Output::Distinct)?;
    Some((plan, offset))
}

fn group_plan(
    inner: &GraphPattern,
    variables: &[Variable],
    aggregates: &[(Variable, AggregateExpression)],
) -> Option<GraphDistinctPlan> {
    let GraphPattern::Graph {
        name: NamedNodePattern::Variable(graph),
        ..
    } = inner
    else {
        return None;
    };
    let mut outputs = Vec::with_capacity(aggregates.len());
    for (output, aggregate) in aggregates {
        let AggregateExpression::FunctionCall {
            name: AggregateFunction::Count,
            expr: Expression::Variable(counted),
            distinct: true,
        } = aggregate
        else {
            return None;
        };
        if counted != graph {
            return None;
        }
        outputs.push(output.clone());
    }
    if outputs.is_empty() {
        return None;
    }
    graph_plan(inner, variables, Output::Count(outputs))
}

fn graph_plan(
    pattern: &GraphPattern,
    keys: &[Variable],
    output: Output,
) -> Option<GraphDistinctPlan> {
    let GraphPattern::Graph {
        name: NamedNodePattern::Variable(graph),
        inner,
    } = pattern
    else {
        return None;
    };
    let GraphPattern::Bgp { patterns } = inner.as_ref() else {
        return None;
    };
    if patterns.is_empty() || patterns.len() > MAX_PATTERNS {
        return None;
    }
    let mut names = Names::default();
    let graph_index = names.variable(graph);
    let patterns = patterns
        .iter()
        .map(|pattern| names.triple(pattern))
        .collect::<Option<Vec<_>>>()?;
    // Every key must be bound by each solution, so unbound keys never reach the relation.
    let keys = keys
        .iter()
        .map(|key| Some((key.clone(), names.bound(key)?)))
        .collect::<Option<Vec<_>>>()?;
    if names.count > MAX_VARIABLES {
        return None;
    }
    Some(GraphDistinctPlan {
        depth: 0,
        graph: graph_index,
        patterns,
        variables: names.count,
        keys,
        output,
    })
}

#[derive(Default)]
struct Names {
    variables: HashMap<Variable, usize>,
    blank_nodes: HashMap<BlankNode, usize>,
    count: usize,
}

impl Names {
    fn variable(&mut self, variable: &Variable) -> usize {
        let next = self.count;
        let index = *self.variables.entry(variable.clone()).or_insert(next);
        self.count = self.count.max(index + 1);
        index
    }

    fn bound(&self, variable: &Variable) -> Option<usize> {
        self.variables.get(variable).copied()
    }

    fn triple(&mut self, pattern: &TriplePattern) -> Option<[Slot; 3]> {
        let predicate = match &pattern.predicate {
            NamedNodePattern::NamedNode(node) => Slot::Constant(EncodedTerm::from_named_node(node)),
            NamedNodePattern::Variable(variable) => Slot::Variable(self.variable(variable)),
        };
        Some([
            self.term(&pattern.subject)?,
            predicate,
            self.term(&pattern.object)?,
        ])
    }

    fn term(&mut self, term: &TermPattern) -> Option<Slot> {
        Some(match term {
            TermPattern::NamedNode(node) => Slot::Constant(EncodedTerm::from_named_node(node)),
            TermPattern::Literal(literal) => Slot::Constant(EncodedTerm::from_literal(literal)),
            TermPattern::Variable(variable) => Slot::Variable(self.variable(variable)),
            // Blank nodes in a basic graph pattern act as undistinguished variables.
            TermPattern::BlankNode(node) => {
                let next = self.count;
                let index = *self.blank_nodes.entry(node.clone()).or_insert(next);
                self.count = self.count.max(index + 1);
                Slot::Variable(index)
            }
            #[allow(unreachable_patterns)]
            _ => return None,
        })
    }
}

/// Relative cost of opening one index range, in units of one visited key.
const SEEK_COST: u64 = 10;
/// Relative cost of one key in a whole-store range, where the graph changes on most keys.
const SCAN_KEY_COST: u64 = 2;
/// Row counts from which the planner probes a few real rows instead of trusting counters.
const OBSERVE_ROWS: usize = 64;
const OBSERVE_SAMPLES: usize = 4;
/// Matches counted per observed probe.
const OBSERVE_CAP: usize = 1_024;

#[cfg(test)]
thread_local! {
    /// Forces one access method wherever it is applicable, so tests cover each.
    static FORCED_METHOD: std::cell::Cell<Option<Method>> =
        const { std::cell::Cell::new(None) };
}

/// Work counts of one native graph-level evaluation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphDistinctStats {
    pub(crate) reads: ReadCounts,
    pub(crate) peak_rows: u64,
    pub(crate) retained: u64,
    /// Pattern, access method, filter step, and row counts before and after each step.
    pub(crate) steps: Vec<(usize, Method, bool, usize, usize)>,
}

/// The computed replacement relation.
pub(crate) struct GraphDistinctRelation {
    pub(crate) values: GraphPattern,
    pub(crate) stats: GraphDistinctStats,
}

/// Computes the replacement relation, or `None` when the generic evaluator must run instead:
/// a key `VALUES` cannot hold, or no trusted dense query IDs.
pub(crate) fn execute(
    plan: &GraphDistinctPlan,
    input: JoinInput<'_, '_, '_>,
) -> Result<Option<GraphDistinctRelation>> {
    input.context.check_cancelled()?;
    input.budget.check()?;
    if !input.view.query_ids_trusted(input.context)? {
        return Ok(None);
    }
    let mut run = Run {
        reader: GraphReader::new(input, plan.graph),
        plan,
        scan_estimates: RefCell::new(vec![None; plan.patterns.len()]),
        samples: RefCell::new(vec![None; plan.patterns.len()]),
        stats: RefCell::new(GraphDistinctStats::default()),
    };
    let rows = if run.resolve()? {
        match run.evaluate()? {
            Some(rows) => rows,
            None => return Ok(None),
        }
    } else {
        Rows::default()
    };
    run.values(&rows)
}

fn ground_term(term: &EncodedTerm) -> Option<GroundTerm> {
    match term.to_term()? {
        Term::NamedNode(node) => Some(GroundTerm::NamedNode(node)),
        Term::Literal(literal) => Some(GroundTerm::Literal(literal)),
        _ => None,
    }
}

fn count_term(count: u64) -> GroundTerm {
    GroundTerm::Literal(Literal::new_typed_literal(count.to_string(), xsd::INTEGER))
}

/// Chooses a pattern order and access methods from counters and observed rows.
struct Run<'a, 'view, 'context> {
    reader: GraphReader<'a, 'view, 'context>,
    plan: &'a GraphDistinctPlan,
    scan_estimates: RefCell<Vec<Option<u64>>>,
    samples: RefCell<Vec<Option<(u64, u64)>>>,
    stats: RefCell<GraphDistinctStats>,
}

impl Run<'_, '_, '_> {
    /// Resolves constants to dense IDs; `false` means a constant is absent, so nothing matches.
    fn resolve(&mut self) -> Result<bool> {
        let JoinInput { view, context, .. } = self.reader.input;
        for pattern in &self.plan.patterns {
            let mut resolved = [Resolved::Variable(self.plan.graph); 4];
            for (slot, target) in pattern.iter().zip(&mut resolved[1..]) {
                *target = match slot {
                    Slot::Variable(index) => Resolved::Variable(*index),
                    Slot::Constant(term) => {
                        let Some(source) = view.lookup_term(context, term)? else {
                            return Ok(false);
                        };
                        let Some(dense) = view.query_term_id(context, source)? else {
                            return Ok(false);
                        };
                        Resolved::Constant(dense, source)
                    }
                };
            }
            self.reader.patterns.push(resolved);
        }
        Ok(true)
    }

    fn record(&self, update: impl FnOnce(&mut GraphDistinctStats)) {
        update(&mut self.stats.borrow_mut());
    }

    /// Evaluates every pattern; `None` means dense reads became unavailable.
    fn evaluate(&self) -> Result<Option<Rows>> {
        let mut pending: Vec<usize> = (0..self.plan.patterns.len()).collect();
        let mut rows = Rows::unit();
        while !pending.is_empty() && !rows.data.is_empty() {
            self.reader.input.context.check_cancelled()?;
            let (position, method) = self.choose(&rows, &pending)?;
            let pattern = pending.remove(position);
            let order: Vec<usize> = std::iter::once(pattern)
                .chain(pending.iter().copied())
                .collect();
            let mut step = self.step(&rows, &order);
            step.method = method;
            let Some(next) = self.reader.run(&rows, &step)? else {
                return Ok(None);
            };
            let before = rows.data.len();
            rows = next;
            let count = u64::try_from(rows.data.len()).unwrap_or(u64::MAX);
            self.record(|stats| {
                stats.peak_rows = stats.peak_rows.max(count);
                stats
                    .steps
                    .push((pattern, method, step.filter, before, rows.data.len()));
            });
        }
        if !pending.is_empty() {
            rows = Rows::default();
        }
        Ok(Some(rows))
    }

    fn variables(&self, pattern: usize) -> impl Iterator<Item = usize> + '_ {
        self.reader.patterns[pattern]
            .iter()
            .filter_map(|slot| match slot {
                Resolved::Variable(variable) => Some(*variable),
                Resolved::Constant(..) => None,
            })
    }

    /// Plans `order[0]` as the next pattern, with `order[1..]` still pending.
    fn step(&self, rows: &Rows, order: &[usize]) -> Step {
        let (pattern, pending) = (order[0], &order[1..]);
        let mut needed = vec![false; self.plan.variables];
        for (_, key) in &self.plan.keys {
            needed[*key] = true;
        }
        if !pending.is_empty() || matches!(self.plan.output, Output::Count(_)) {
            needed[self.plan.graph] = true;
        }
        for other in pending {
            for variable in self.variables(*other) {
                needed[variable] = true;
            }
        }
        let mut join: Vec<usize> = self
            .variables(pattern)
            .filter(|variable| rows.columns.contains(variable))
            .collect();
        join.sort_unstable();
        join.dedup();
        let mut columns: Vec<usize> = rows
            .columns
            .iter()
            .copied()
            .chain(self.variables(pattern))
            .filter(|variable| needed[*variable])
            .collect();
        columns.sort_unstable();
        columns.dedup();
        let kept: Vec<(usize, bool)> = columns
            .iter()
            .map(|column| (*column, rows.columns.contains(column)))
            .collect();
        let filter = kept.iter().all(|(_, from_row)| *from_row);
        Step {
            pattern,
            join,
            columns,
            kept,
            filter,
            method: Method::Scan,
        }
    }

    /// Picks the cheapest next pattern and access method from counters and current rows.
    fn choose(&self, rows: &Rows, pending: &[usize]) -> Result<(usize, Method)> {
        let graph_bound = rows.columns.contains(&self.plan.graph);
        let current = u64::try_from(rows.data.len()).unwrap_or(u64::MAX);
        let graphs = u64::try_from(self.outputs(rows, &[self.plan.graph])).unwrap_or(u64::MAX);
        let mut best = (u64::MAX, 0, Method::Scan);
        for (position, pattern) in pending.iter().enumerate() {
            let order: Vec<usize> = std::iter::once(*pattern)
                .chain(pending.iter().copied().filter(|other| other != pattern))
                .collect();
            let step = self.step(rows, &order);
            let scan = self.scan_estimate(*pattern)?;
            let scan_cost = SEEK_COST.saturating_add(scan.saturating_mul(SCAN_KEY_COST));
            let (cost, method, fanout) = if graph_bound {
                let (fanout, tries) = self.fanout(rows, *pattern)?;
                let per_graph = self.sample_counts(rows, *pattern)?.1;
                let graph_scan_cost = graphs.saturating_mul(SEEK_COST.saturating_add(per_graph));
                let probe_cost = if step.filter {
                    // Probing stops at the first match for each distinct output row.
                    let outputs =
                        u64::try_from(self.outputs(rows, &step.columns)).unwrap_or(u64::MAX);
                    let tries = tries.min(current / outputs.max(1)).max(1);
                    outputs
                        .saturating_mul(tries)
                        .saturating_mul(SEEK_COST.saturating_add(1))
                } else {
                    current.saturating_mul(SEEK_COST.saturating_add(fanout))
                };
                [
                    (probe_cost, Method::Probe, fanout),
                    (graph_scan_cost, Method::GraphScan, fanout),
                    (scan_cost, Method::Scan, fanout),
                ]
                .into_iter()
                .min_by_key(|(cost, ..)| *cost)
                .expect("three methods")
            } else {
                (scan_cost, Method::Scan, scan)
            };
            let output = if step.filter {
                current
            } else {
                current.saturating_mul(fanout.max(1))
            };
            let score = cost.saturating_add(output);
            if score < best.0 {
                best = (score, position, method);
            }
        }
        #[cfg(test)]
        if let Some(method) = FORCED_METHOD.with(std::cell::Cell::get)
            && (graph_bound || method == Method::Scan)
        {
            best.2 = method;
        }
        Ok((best.1, best.2))
    }

    /// Visible keys a whole-store scan of the pattern would read, from raw counters.
    fn scan_estimate(&self, pattern: usize) -> Result<u64> {
        if let Some(estimate) = self.scan_estimates.borrow()[pattern] {
            return Ok(estimate);
        }
        let JoinInput { view, context, .. } = self.reader.input;
        let [_, _, predicate, object] = self.reader.patterns[pattern];
        let estimate = match (predicate, object) {
            (Resolved::Constant(_, predicate), Resolved::Constant(_, object)) => {
                view.qv_po_count(context, predicate, object)?
            }
            (Resolved::Constant(_, predicate), _) => view.qv_p_count(context, predicate)?,
            _ => None,
        }
        .unwrap_or(u64::MAX / 4);
        self.scan_estimates.borrow_mut()[pattern] = Some(estimate);
        Ok(estimate)
    }

    /// Distinct rows after projecting onto `columns`.
    fn outputs(&self, rows: &Rows, columns: &[usize]) -> usize {
        rows.data
            .iter()
            .map(|row| join_key(row, columns))
            .collect::<RowSet>()
            .len()
    }

    /// Expected matches per probe and expected probes until one succeeds. Large row sets
    /// probe a few real rows; smaller ones use one sampled graph's counters.
    fn fanout(&self, rows: &Rows, pattern: usize) -> Result<(u64, u64)> {
        const FEW: u64 = 2;
        if rows.data.len() >= OBSERVE_ROWS
            && let Some(observed) = self.observe(rows, pattern)?
        {
            return Ok(observed);
        }
        let slots = self.reader.patterns[pattern];
        let bound = |slot: &Resolved| match slot {
            Resolved::Constant(..) => true,
            Resolved::Variable(variable) => rows.columns.contains(variable),
        };
        let (per_predicate, per_object) = self.sample_counts(rows, pattern)?;
        let fanout = match (bound(&slots[1]), bound(&slots[3])) {
            (true, true) => 1,
            // The graph itself as subject is usually the root, which holds all such facts.
            (true, false) if matches!(slots[1], Resolved::Variable(graph) if graph == self.plan.graph) => {
                per_predicate
            }
            (true, false) => FEW,
            (false, true) if matches!(slots[3], Resolved::Constant(..)) => per_object,
            (false, true) => FEW,
            (false, false) => per_predicate,
        };
        let tries = if bound(&slots[1]) && matches!(slots[3], Resolved::Constant(..)) {
            per_predicate.div_ceil(per_object.max(1))
        } else {
            1
        };
        Ok((fanout.max(1), tries.max(1)))
    }

    /// Probes the first rows for real; `None` means dense reads are unavailable.
    fn observe(&self, rows: &Rows, pattern: usize) -> Result<Option<(u64, u64)>> {
        let slots = self.reader.patterns[pattern];
        let (mut matches, mut successes) = (0_u64, 0_u64);
        let samples = rows.data.iter().take(OBSERVE_SAMPLES);
        let count = u64::try_from(samples.len()).unwrap_or(1);
        for row in samples {
            let terms = slots.map(|slot| match slot {
                Resolved::Constant(term, _) => Some(term),
                Resolved::Variable(variable) => rows.value(row, variable),
            });
            let Some(source) = terms[0].and_then(|graph| self.reader.graph_source(graph)) else {
                return Ok(None);
            };
            let range = Range {
                selector: GraphSelector::Named(source),
                terms,
                graphs: None,
            };
            let Some(cursor) = self.reader.cursor(range)? else {
                return Ok(None);
            };
            let before = matches;
            for quad in cursor.take(OBSERVE_CAP) {
                if self.reader.bind(pattern, &quad?).is_some() {
                    matches += 1;
                }
            }
            successes += u64::from(matches > before);
        }
        let fanout = matches.div_ceil(count).max(1);
        let tries = count.div_ceil(successes.max(1)).max(1);
        Ok(Some((
            fanout,
            if successes == 0 { count + 1 } else { tries },
        )))
    }

    /// The sampled graph's (graph, predicate) and (graph, predicate, object) counters.
    fn sample_counts(&self, rows: &Rows, pattern: usize) -> Result<(u64, u64)> {
        const UNKNOWN: u64 = u64::MAX / 4;
        if let Some(counts) = self.samples.borrow()[pattern] {
            return Ok(counts);
        }
        let source = rows
            .data
            .first()
            .and_then(|row| rows.value(row, self.plan.graph))
            .and_then(|graph| self.reader.graph_source(graph));
        let JoinInput { view, context, .. } = self.reader.input;
        let slots = self.reader.patterns[pattern];
        let counts = match (source, slots[2], slots[3]) {
            (Some(graph), Resolved::Constant(_, predicate), object) => {
                let per_predicate = view
                    .qv_gp_count(context, graph, predicate)?
                    .unwrap_or(UNKNOWN);
                let per_object = match object {
                    Resolved::Constant(_, object) => view
                        .qv_gpo_count(context, graph, predicate, object)?
                        .unwrap_or(UNKNOWN),
                    Resolved::Variable(_) => per_predicate,
                };
                (per_predicate, per_object)
            }
            _ => (UNKNOWN, UNKNOWN),
        };
        self.samples.borrow_mut()[pattern] = Some(counts);
        Ok(counts)
    }

    fn values(&self, rows: &Rows) -> Result<Option<GraphDistinctRelation>> {
        let JoinInput { view, context, .. } = self.reader.input;
        let plan = self.plan;
        let mut tuples: HashMap<Vec<QueryTermId>, u64> = HashMap::new();
        for row in &rows.data {
            let key: Vec<QueryTermId> = plan
                .keys
                .iter()
                .map(|(_, index)| rows.value(row, *index).expect("keys are kept columns"))
                .collect();
            *tuples.entry(key).or_insert(0) += 1;
        }
        let mut variables: Vec<Variable> = plan.keys.iter().map(|(key, _)| key.clone()).collect();
        let counts = match &plan.output {
            Output::Distinct => &[][..],
            Output::Count(outputs) => outputs.as_slice(),
        };
        variables.extend(counts.iter().cloned());
        let mut decoded: HashMap<QueryTermId, GroundTerm> = HashMap::new();
        let mut bindings = Vec::with_capacity(tuples.len());
        for (key, count) in &tuples {
            let mut row = Vec::with_capacity(variables.len());
            for term in key {
                let ground = match decoded.get(term) {
                    Some(ground) => ground.clone(),
                    None => {
                        let source = self.reader.source(*term)?;
                        let encoded = view.decode_result_term(context, source)?;
                        let Some(ground) = ground_term(&encoded) else {
                            return Ok(None);
                        };
                        decoded.insert(*term, ground.clone());
                        ground
                    }
                };
                row.push(Some(ground));
            }
            row.extend(counts.iter().map(|_| Some(count_term(*count))));
            bindings.push(row);
        }
        // Without group keys, an empty input still forms one group with a zero count.
        if bindings.is_empty() && plan.keys.is_empty() && !counts.is_empty() {
            bindings.push(counts.iter().map(|_| Some(count_term(0))).collect());
        }
        context.check_cancelled()?;
        let mut stats = self.stats.borrow().clone();
        stats.reads = self.reader.counts();
        stats.retained = u64::try_from(tuples.len()).unwrap_or(u64::MAX);
        Ok(Some(GraphDistinctRelation {
            values: GraphPattern::Values {
                variables,
                bindings,
            },
            stats,
        }))
    }
}
