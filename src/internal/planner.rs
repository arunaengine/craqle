//! Optimizes query plans using store cardinalities, safe term folds, and bounded limit pushdown.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Term};
use spargebra::Query;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::query::context::{CostStatistics, QueryCost};
use crate::store::{GraphStore, PlannerEstimate, PlannerStat, TermId};

/// Test and benchmark control for connected BGP join selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum JoinMode {
    #[default]
    Auto,
    ForceLateral,
    ForceHash,
    ForcePropertyStar,
}

/// Physical join selected for one connected BGP edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinKind {
    IndexedLateral,
    Hash,
    PropertyStar,
}

/// Planner estimates recorded for one physical join decision.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlannedJoin {
    pub physical_operator: JoinKind,
    pub estimated_left_rows: u64,
    pub estimated_right_rows: u64,
    pub estimated_distinct_join_keys: u64,
    pub estimated_output_rows: u64,
    pub estimated_lateral_cost: u64,
    pub estimated_hash_cost: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlannerError {
    #[error("forced join mode {0:?} cannot represent this query")]
    ForcedModeUnavailable(JoinMode),
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
}

#[derive(Default)]
pub(crate) struct PlannerTrace {
    pub(crate) joins: Vec<PlannedJoin>,
    pub(crate) costs: CostStatistics,
}

#[derive(Clone, Copy)]
pub(crate) struct PlanMode {
    pub(crate) join: JoinMode,
    pub(crate) collect_costs: bool,
    /// The one graph a query reads, whose own counters replace store-wide estimates.
    pub(crate) graph: Option<TermId>,
}

/// Per-pass planner state that memoizes constant term ids but never transient errors.
struct PlanCtx<'a> {
    store: &'a GraphStore,
    term_ids: RefCell<HashMap<EncodedTerm, Option<u128>>>,
    join_mode: JoinMode,
    forced_mode_used: Cell<bool>,
    error: RefCell<Option<PlannerError>>,
    planned_joins: RefCell<Vec<PlannedJoin>>,
    costs: QueryCost,
    stats: RefCell<HashMap<PlannerStat, PlannerEstimate>>,
    row_demand: Cell<Option<u64>>,
    graph: Option<TermId>,
    graph_var: RefCell<Option<String>>,
    /// Every enclosing graph variable, bound or not; folding one would drop its graph constraint.
    graph_names: RefCell<Vec<String>>,
}

impl<'a> PlanCtx<'a> {
    fn new(store: &'a GraphStore, mode: PlanMode) -> Self {
        Self {
            store,
            term_ids: RefCell::new(HashMap::new()),
            join_mode: mode.join,
            forced_mode_used: Cell::new(false),
            error: RefCell::new(None),
            planned_joins: RefCell::new(Vec::new()),
            costs: QueryCost::planner(mode.collect_costs),
            stats: RefCell::new(HashMap::new()),
            row_demand: Cell::new(None),
            graph: mode.graph,
            graph_var: RefCell::new(None),
            graph_names: RefCell::new(Vec::new()),
        }
    }

    fn stat(&self, stat: PlannerStat) -> PlannerEstimate {
        // Exact per-graph counters exist for these; other estimates stay store-wide bounds.
        let stat = match (self.graph, stat) {
            (Some(graph), PlannerStat::Predicate(predicate)) => {
                PlannerStat::GraphPredicate(graph, predicate)
            }
            (Some(graph), PlannerStat::PredicateObject(predicate, object)) => {
                PlannerStat::GraphPredicateObject(graph, predicate, object)
            }
            (Some(graph), PlannerStat::Total) => PlannerStat::Graph(graph),
            (_, stat) => stat,
        };
        if let Some(value) = self.stats.borrow().get(&stat).copied() {
            self.costs.planner_memo(true);
            return value;
        }
        self.costs.planner_memo(false);
        let value = self.store.planner_estimate(stat, &self.costs);
        self.stats.borrow_mut().insert(stat, value);
        value
    }

    fn rows(&self, stat: PlannerStat) -> u64 {
        u64::try_from(self.stat(stat).row_upper()).unwrap_or(u64::MAX)
    }

    fn distinct(&self, stat: PlannerStat) -> u64 {
        u64::try_from(self.stat(stat).distinct_lower()).unwrap_or(u64::MAX)
    }
}

/// Relative costs for patterns whose selective variable is already bound by a lateral chain.
const COST_S_PO: u64 = 1;
const COST_S_P: u64 = 3;
const COST_BOUND_S: u64 = 6;
const COST_P_O: u64 = 4;
const COST_BOUND_O: u64 = 8;
const COST_ONLY_P: u64 = 1 << 20;

#[cfg(test)]
pub(crate) fn optimize_query(query: &mut Query, store: &GraphStore) {
    optimize_with_mode(query, store, JoinMode::Auto)
        .expect("automatic join planning must always have a fallback");
}

#[cfg(test)]
pub(crate) fn optimize_with_mode(
    query: &mut Query,
    store: &GraphStore,
    join_mode: JoinMode,
) -> Result<PlannerTrace, PlannerError> {
    optimize_with_costs(
        query,
        store,
        PlanMode {
            join: join_mode,
            collect_costs: false,
            graph: None,
        },
    )
}

pub(crate) fn optimize_with_costs(
    query: &mut Query,
    store: &GraphStore,
    mode: PlanMode,
) -> Result<PlannerTrace, PlannerError> {
    let cx = PlanCtx::new(store, mode);
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Construct { pattern, .. } => {
            let current = std::mem::replace(
                pattern,
                GraphPattern::Bgp {
                    patterns: Vec::new(),
                },
            );
            *pattern = optimize_pattern(current, &HashSet::new(), &cx);
        }
    }
    if let Some(error) = cx.error.into_inner() {
        return Err(error);
    }
    if !matches!(mode.join, JoinMode::Auto) && !cx.forced_mode_used.get() {
        return Err(PlannerError::ForcedModeUnavailable(mode.join));
    }
    Ok(PlannerTrace {
        joins: cx.planned_joins.into_inner(),
        costs: cx.costs.snapshot(),
    })
}

/// Variables and blank nodes share a key space; blank nodes in query position
/// behave as variables (and the engine maps them to variables query-wide).
fn term_var_key(term: &TermPattern) -> Option<String> {
    match term {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        _ => None,
    }
}

fn predicate_var_key(predicate: &NamedNodePattern) -> Option<String> {
    match predicate {
        NamedNodePattern::Variable(v) => Some(v.as_str().to_string()),
        NamedNodePattern::NamedNode(_) => None,
    }
}

/// Triple variables plus the enclosing unbound graph variable, which every pattern binds.
fn pattern_var_keys(pattern: &TriplePattern, cx: &PlanCtx<'_>) -> Vec<String> {
    let mut keys = triple_var_keys(pattern);
    keys.extend(cx.graph_var.borrow().clone());
    keys
}

fn triple_var_keys(pattern: &TriplePattern) -> Vec<String> {
    let mut keys = Vec::with_capacity(3);
    if let Some(key) = term_var_key(&pattern.subject) {
        keys.push(key);
    }
    if let Some(key) = predicate_var_key(&pattern.predicate) {
        keys.push(key);
    }
    if let Some(key) = term_var_key(&pattern.object) {
        keys.push(key);
    }
    keys
}

/// All variable keys a pattern can mention (superset of certainly-bound);
/// used only for ordering heuristics, never for semantic decisions.
fn collect_pattern_vars(pattern: &GraphPattern, out: &mut HashSet<String>) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                out.extend(triple_var_keys(triple));
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            out.extend(term_var_key(subject));
            out.extend(term_var_key(object));
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_pattern_vars(left, out);
            collect_pattern_vars(right, out);
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => collect_pattern_vars(inner, out),
        GraphPattern::Graph { name, inner } => {
            out.extend(predicate_var_key(name));
            collect_pattern_vars(inner, out);
        }
        GraphPattern::Extend {
            inner, variable, ..
        } => {
            out.insert(variable.as_str().to_string());
            collect_pattern_vars(inner, out);
        }
        GraphPattern::Values { variables, .. } => {
            out.extend(variables.iter().map(|v| v.as_str().to_string()));
        }
        GraphPattern::Project { variables, .. } => {
            out.extend(variables.iter().map(|v| v.as_str().to_string()));
        }
        GraphPattern::Group {
            variables,
            aggregates,
            ..
        } => {
            out.extend(variables.iter().map(|v| v.as_str().to_string()));
            out.extend(aggregates.iter().map(|(v, _)| v.as_str().to_string()));
        }
        GraphPattern::Service { inner, .. } => collect_pattern_vars(inner, out),
    }
}

fn supports_demand(pattern: &GraphPattern) -> bool {
    match pattern {
        GraphPattern::Project { inner, .. } | GraphPattern::Graph { inner, .. } => {
            supports_demand(inner)
        }
        GraphPattern::Bgp { patterns } => {
            let [left, right] = patterns.as_slice() else {
                return false;
            };
            let Some(left) = pattern_variables(left) else {
                return false;
            };
            let Some(right) = pattern_variables(right) else {
                return false;
            };
            left.iter().any(|variable| right.contains(variable))
        }
        _ => false,
    }
}

fn optimize_pattern(
    pattern: GraphPattern,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> GraphPattern {
    match pattern {
        GraphPattern::Bgp { patterns } => reorder_bgp(patterns, bound, cx),
        GraphPattern::Path { .. } | GraphPattern::Values { .. } => pattern,
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(optimize_pattern(*left, bound, cx)),
            right: Box::new(optimize_pattern(*right, bound, cx)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left = optimize_pattern(*left, bound, cx);
            // sparopt evaluates fit OPTIONAL bodies as for-loop left joins
            // with the outer row bound, so order the body accordingly.
            let mut right_bound = bound.clone();
            collect_pattern_vars(&left, &mut right_bound);
            let right = optimize_pattern(*right, &right_bound, cx);
            let mut expr_bound = right_bound.clone();
            collect_pattern_vars(&right, &mut expr_bound);
            GraphPattern::LeftJoin {
                left: Box::new(left),
                right: Box::new(right),
                expression: expression.map(|e| optimize_expression(e, &expr_bound, cx)),
            }
        }
        GraphPattern::Lateral { left, right } => {
            let left = optimize_pattern(*left, bound, cx);
            let mut right_bound = bound.clone();
            collect_pattern_vars(&left, &mut right_bound);
            GraphPattern::Lateral {
                right: Box::new(optimize_pattern(*right, &right_bound, cx)),
                left: Box::new(left),
            }
        }
        GraphPattern::Filter { expr, inner } => {
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            let expr = optimize_expression(expr, &expr_bound, cx);
            if let GraphPattern::Bgp { patterns } = *inner {
                rewrite_bgp_filter(FilterOverBgp { expr, patterns }, bound, cx)
            } else {
                GraphPattern::Filter {
                    expr,
                    inner: Box::new(optimize_pattern(*inner, bound, cx)),
                }
            }
        }
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(optimize_pattern(*left, bound, cx)),
            right: Box::new(optimize_pattern(*right, bound, cx)),
        },
        GraphPattern::Graph { name, inner } => {
            // An unbound graph variable joins every inner pattern and is bound by the first match.
            let free = predicate_var_key(&name).filter(|key| !bound.contains(key));
            let outer = cx.graph_var.replace(free);
            let named = predicate_var_key(&name);
            cx.graph_names.borrow_mut().extend(named.clone());
            let inner = optimize_pattern(*inner, bound, cx);
            if named.is_some() {
                cx.graph_names.borrow_mut().pop();
            }
            cx.graph_var.replace(outer);
            GraphPattern::Graph {
                name,
                inner: Box::new(inner),
            }
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let inner = optimize_pattern(*inner, bound, cx);
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            GraphPattern::Extend {
                expression: optimize_expression(expression, &expr_bound, cx),
                inner: Box::new(inner),
                variable,
            }
        }
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(optimize_pattern(*left, bound, cx)),
            right: Box::new(optimize_pattern(*right, bound, cx)),
        },
        GraphPattern::OrderBy { inner, expression } => {
            let inner = optimize_pattern(*inner, bound, cx);
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            GraphPattern::OrderBy {
                inner: Box::new(inner),
                expression: expression
                    .into_iter()
                    .map(|order| match order {
                        OrderExpression::Asc(e) => {
                            OrderExpression::Asc(optimize_expression(e, &expr_bound, cx))
                        }
                        OrderExpression::Desc(e) => {
                            OrderExpression::Desc(optimize_expression(e, &expr_bound, cx))
                        }
                    })
                    .collect(),
            }
        }
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(optimize_pattern(*inner, bound, cx)),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(optimize_pattern(*inner, bound, cx)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(optimize_pattern(*inner, bound, cx)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let demand = length
                .filter(|length| *length != 0)
                .map(|length| start.saturating_add(length))
                .filter(|_| supports_demand(&inner))
                .map(|demand| u64::try_from(demand).unwrap_or(u64::MAX));
            let previous = demand.map(|demand| cx.row_demand.replace(Some(demand)));
            let mut inner = optimize_pattern(*inner, bound, cx);
            if let Some(previous) = previous {
                cx.row_demand.set(previous);
            }
            if let Some(length) = length {
                inner = push_slice_cap(inner, start.saturating_add(length));
            }
            GraphPattern::Slice {
                inner: Box::new(inner),
                start,
                length,
            }
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let inner = optimize_pattern(*inner, bound, cx);
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            GraphPattern::Group {
                inner: Box::new(inner),
                variables,
                aggregates: aggregates
                    .into_iter()
                    .map(|(variable, aggregate)| {
                        let aggregate = match aggregate {
                            AggregateExpression::CountSolutions { distinct } => {
                                AggregateExpression::CountSolutions { distinct }
                            }
                            AggregateExpression::FunctionCall {
                                name,
                                expr,
                                distinct,
                            } => AggregateExpression::FunctionCall {
                                name,
                                expr: optimize_expression(expr, &expr_bound, cx),
                                distinct,
                            },
                        };
                        (variable, aggregate)
                    })
                    .collect(),
            }
        }
        // SERVICE bodies run remotely (the FTS service is rewritten away
        // before this pass); never touch them.
        GraphPattern::Service { .. } => pattern,
    }
}

fn optimize_expression(
    expression: Expression,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> Expression {
    let walk = |e: Box<Expression>| Box::new(optimize_expression(*e, bound, cx));
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => expression,
        Expression::Or(a, b) => Expression::Or(walk(a), walk(b)),
        Expression::And(a, b) => Expression::And(walk(a), walk(b)),
        Expression::Equal(a, b) => Expression::Equal(walk(a), walk(b)),
        Expression::SameTerm(a, b) => Expression::SameTerm(walk(a), walk(b)),
        Expression::Greater(a, b) => Expression::Greater(walk(a), walk(b)),
        Expression::GreaterOrEqual(a, b) => Expression::GreaterOrEqual(walk(a), walk(b)),
        Expression::Less(a, b) => Expression::Less(walk(a), walk(b)),
        Expression::LessOrEqual(a, b) => Expression::LessOrEqual(walk(a), walk(b)),
        Expression::In(e, list) => Expression::In(
            walk(e),
            list.into_iter()
                .map(|e| optimize_expression(e, bound, cx))
                .collect(),
        ),
        Expression::Add(a, b) => Expression::Add(walk(a), walk(b)),
        Expression::Subtract(a, b) => Expression::Subtract(walk(a), walk(b)),
        Expression::Multiply(a, b) => Expression::Multiply(walk(a), walk(b)),
        Expression::Divide(a, b) => Expression::Divide(walk(a), walk(b)),
        Expression::UnaryPlus(e) => Expression::UnaryPlus(walk(e)),
        Expression::UnaryMinus(e) => Expression::UnaryMinus(walk(e)),
        Expression::Not(e) => Expression::Not(walk(e)),
        // EXISTS bodies are never join-reordered by sparopt; the outer row's
        // variables are bound when the body runs.
        Expression::Exists(inner) => {
            Expression::Exists(Box::new(optimize_pattern(*inner, bound, cx)))
        }
        Expression::If(a, b, c) => Expression::If(walk(a), walk(b), walk(c)),
        Expression::Coalesce(list) => Expression::Coalesce(
            list.into_iter()
                .map(|e| optimize_expression(e, bound, cx))
                .collect(),
        ),
        Expression::FunctionCall(function, args) => Expression::FunctionCall(
            function,
            args.into_iter()
                .map(|e| optimize_expression(e, bound, cx))
                .collect(),
        ),
    }
}

// --- FILTER equality folding -------------------------------------------------

enum FoldableConstant {
    Iri(NamedNode),
    StringLiteral(Literal),
    /// sameTerm only: exact term identity for any literal without language tag.
    TypedLiteral(Literal),
}

fn flatten_and(expression: Expression, out: &mut Vec<Expression>) {
    if let Expression::And(a, b) = expression {
        flatten_and(*a, out);
        flatten_and(*b, out);
    } else {
        out.push(expression);
    }
}

fn and_all(mut conjuncts: Vec<Expression>) -> Option<Expression> {
    let mut result = conjuncts.pop()?;
    while let Some(next) = conjuncts.pop() {
        result = Expression::And(Box::new(next), Box::new(result));
    }
    Some(result)
}

fn foldable_equality(conjunct: &Expression) -> Option<(oxrdf::Variable, FoldableConstant)> {
    let (variable, constant, is_same_term) = match conjunct {
        Expression::Equal(a, b) => match (a.as_ref(), b.as_ref()) {
            (Expression::Variable(v), c) | (c, Expression::Variable(v)) => (v, c, false),
            _ => return None,
        },
        Expression::SameTerm(a, b) => match (a.as_ref(), b.as_ref()) {
            (Expression::Variable(v), c) | (c, Expression::Variable(v)) => (v, c, true),
            _ => return None,
        },
        _ => return None,
    };
    let constant = match constant {
        Expression::NamedNode(node) => FoldableConstant::Iri(node.clone()),
        Expression::Literal(literal) => {
            if literal.language().is_some() {
                // Language tag casing differences between query and store
                // spelling cannot be ruled out; do not fold.
                return None;
            }
            if literal.datatype() == xsd::STRING {
                FoldableConstant::StringLiteral(literal.clone())
            } else if is_same_term {
                FoldableConstant::TypedLiteral(literal.clone())
            } else {
                // `=` does value comparison for typed literals; a bound
                // pattern would do term comparison. Not equivalent.
                return None;
            }
        }
        _ => return None,
    };
    Some((variable.clone(), constant))
}

/// True when a non-canonical spelling of the same string value exists in the
/// term table; folding would then miss value-equal rows the filter matches.
fn has_noncanonical_spelling(cx: &PlanCtx<'_>, literal: &Literal) -> bool {
    let alternate = EncodedTerm(format!(
        "{}^^<http://www.w3.org/2001/XMLSchema#string>",
        literal
    ));
    match cx.store.lookup_term(&alternate) {
        Ok(found) => found.is_some(),
        Err(error) => {
            *cx.error.borrow_mut() = Some(PlannerError::Store(error));
            true
        }
    }
}

fn fold_variable_patterns(
    patterns: &mut [TriplePattern],
    variable: &oxrdf::Variable,
    constant: &FoldableConstant,
) -> bool {
    let key = variable.as_str();
    let occurs_as =
        |slot: &TermPattern| matches!(slot, TermPattern::Variable(v) if v.as_str() == key);
    let occurs_as_predicate =
        |p: &NamedNodePattern| matches!(p, NamedNodePattern::Variable(v) if v.as_str() == key);

    let literal_constant = !matches!(constant, FoldableConstant::Iri(_));
    let mut occurs_anywhere = false;
    for pattern in patterns.iter() {
        if occurs_as(&pattern.subject) || occurs_as_predicate(&pattern.predicate) {
            // Literals cannot sit in subject/predicate position; refusing
            // (rather than substituting) keeps raw-store edge cases identical.
            if literal_constant {
                return false;
            }
            occurs_anywhere = true;
        }
        if occurs_as(&pattern.object) {
            occurs_anywhere = true;
        }
    }
    if !occurs_anywhere {
        return false;
    }

    let term_pattern: TermPattern = match constant {
        FoldableConstant::Iri(node) => TermPattern::NamedNode(node.clone()),
        FoldableConstant::StringLiteral(literal) | FoldableConstant::TypedLiteral(literal) => {
            TermPattern::Literal(literal.clone())
        }
    };
    for pattern in patterns.iter_mut() {
        if occurs_as(&pattern.subject) {
            pattern.subject = term_pattern.clone();
        }
        if occurs_as_predicate(&pattern.predicate)
            && let FoldableConstant::Iri(node) = constant
        {
            pattern.predicate = NamedNodePattern::NamedNode(node.clone());
        }
        if occurs_as(&pattern.object) {
            pattern.object = term_pattern.clone();
        }
    }
    true
}

/// A `FILTER` applied directly over a BGP that shape equality folding rewrites.
struct FilterOverBgp {
    expr: Expression,
    patterns: Vec<TriplePattern>,
}

fn rewrite_bgp_filter(
    filter: FilterOverBgp,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> GraphPattern {
    let FilterOverBgp { expr, mut patterns } = filter;
    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);

    let mut bindings: Vec<(oxrdf::Variable, FoldableConstant)> = Vec::new();
    let mut remaining = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        let folded = foldable_equality(&conjunct).and_then(|(variable, constant)| {
            if cx
                .graph_names
                .borrow()
                .iter()
                .any(|name| name == variable.as_str())
            {
                return None;
            }
            match &constant {
                FoldableConstant::StringLiteral(literal)
                    if has_noncanonical_spelling(cx, literal) =>
                {
                    return None;
                }
                _ => {}
            }
            fold_variable_patterns(&mut patterns, &variable, &constant)
                .then_some((variable, constant))
        });
        match folded {
            Some(binding) => bindings.push(binding),
            None => remaining.push(conjunct),
        }
    }

    let mut node = reorder_bgp(patterns, bound, cx);
    for (variable, constant) in bindings {
        let expression = match constant {
            FoldableConstant::Iri(node) => Expression::NamedNode(node),
            FoldableConstant::StringLiteral(literal) | FoldableConstant::TypedLiteral(literal) => {
                Expression::Literal(literal)
            }
        };
        node = GraphPattern::Extend {
            inner: Box::new(node),
            variable,
            expression,
        };
    }
    match and_all(remaining) {
        Some(expr) => GraphPattern::Filter {
            expr,
            inner: Box::new(node),
        },
        None => node,
    }
}

// --- BGP reordering ----------------------------------------------------------

enum Slot {
    /// Constant term; `None` when absent from the term table (no match).
    Const(Option<u128>),
    BoundVar,
    FreeVar,
    Unsupported,
}

fn term_slot(term: &TermPattern, bound: &HashSet<String>, cx: &PlanCtx<'_>) -> Slot {
    match term {
        TermPattern::NamedNode(node) => const_slot(cx, &EncodedTerm::from_named_node(node)),
        TermPattern::Literal(literal) => const_slot(
            cx,
            &EncodedTerm::from_plain_term(&Term::Literal(literal.clone())),
        ),
        TermPattern::Variable(v) => {
            if bound.contains(v.as_str()) {
                Slot::BoundVar
            } else {
                Slot::FreeVar
            }
        }
        TermPattern::BlankNode(b) => {
            if bound.contains(&format!("_:{}", b.as_str())) {
                Slot::BoundVar
            } else {
                Slot::FreeVar
            }
        }
        #[allow(unreachable_patterns)]
        _ => Slot::Unsupported,
    }
}

fn const_slot(cx: &PlanCtx<'_>, term: &EncodedTerm) -> Slot {
    if let Some(&id) = cx.term_ids.borrow().get(term) {
        cx.costs.planner_cache(true);
        return Slot::Const(id);
    }
    cx.costs.planner_cache(false);
    cx.costs.planner_points(1);
    match cx.store.lookup_term(term) {
        Ok(id) => {
            let id = id.map(|id| id.0);
            cx.term_ids.borrow_mut().insert(term.clone(), id);
            Slot::Const(id)
        }
        Err(_) => Slot::Unsupported,
    }
}

fn predicate_slot(predicate: &NamedNodePattern, bound: &HashSet<String>, cx: &PlanCtx<'_>) -> Slot {
    match predicate {
        NamedNodePattern::NamedNode(node) => const_slot(cx, &EncodedTerm::from_named_node(node)),
        NamedNodePattern::Variable(v) => {
            if bound.contains(v.as_str()) {
                Slot::BoundVar
            } else {
                Slot::FreeVar
            }
        }
    }
}

/// Estimate matches from corpus counts for free slots and constants for bound index slots.
fn estimate_pattern(
    pattern: &TriplePattern,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> Option<u64> {
    use crate::store::TermId;
    let subject = term_slot(&pattern.subject, bound, cx);
    let predicate = predicate_slot(&pattern.predicate, bound, cx);
    let object = term_slot(&pattern.object, bound, cx);
    if matches!(subject, Slot::Unsupported)
        || matches!(predicate, Slot::Unsupported)
        || matches!(object, Slot::Unsupported)
    {
        return None;
    }
    if matches!(subject, Slot::Const(None))
        || matches!(predicate, Slot::Const(None))
        || matches!(object, Slot::Const(None))
    {
        return Some(0);
    }

    Some(match (subject, predicate, object) {
        (Slot::Const(Some(s)), predicate, object) => {
            let mut estimate = cx.rows(PlannerStat::Subject(TermId(s)));
            match (&predicate, &object) {
                (Slot::Const(Some(p)), Slot::Const(Some(o))) => {
                    let pair = cx.rows(PlannerStat::PredicateObject(TermId(*p), TermId(*o)));
                    estimate = estimate.min(pair).min(1);
                }
                (Slot::Const(Some(p)), _) => {
                    estimate = estimate.min(cx.rows(PlannerStat::Predicate(TermId(*p))));
                }
                (_, Slot::Const(Some(o))) => {
                    estimate = estimate.min(cx.rows(PlannerStat::Object(TermId(*o))));
                }
                _ => {}
            }
            estimate
        }
        (Slot::BoundVar, Slot::Const(_), Slot::Const(_) | Slot::BoundVar) => COST_S_PO,
        (Slot::BoundVar, Slot::Const(_), _) => COST_S_P,
        (Slot::BoundVar, _, _) => COST_BOUND_S,
        (Slot::FreeVar, Slot::Const(Some(p)), Slot::Const(Some(o))) => {
            cx.rows(PlannerStat::PredicateObject(TermId(p), TermId(o)))
        }
        (Slot::FreeVar, Slot::Const(_), Slot::BoundVar) => COST_P_O,
        (Slot::FreeVar, Slot::Const(Some(p)), Slot::FreeVar) => {
            cx.rows(PlannerStat::Predicate(TermId(p)))
        }
        (Slot::FreeVar, _, Slot::Const(Some(o))) => cx.rows(PlannerStat::Object(TermId(o))),
        (Slot::FreeVar, _, Slot::BoundVar) => COST_BOUND_O,
        (Slot::FreeVar, Slot::BoundVar, Slot::FreeVar) => {
            COST_ONLY_P.min(cx.rows(PlannerStat::Total))
        }
        (Slot::FreeVar, Slot::FreeVar, Slot::FreeVar) => cx.rows(PlannerStat::Total),
        // Const(None) and Unsupported handled above.
        _ => cx.rows(PlannerStat::Total),
    })
}

const INDEXED_LOOKUP_COST: u64 = 8;
const HASH_OUTER_MIN: u64 = 256;
/// First visit to a graph reads its visibility and orphan records, about 16 scanned rows.
const GRAPH_VISIT_COST: u64 = 16;

fn pattern_variables(pattern: &TriplePattern) -> Option<Vec<oxrdf::Variable>> {
    let mut variables = HashMap::new();
    let mut insert_term = |term: &TermPattern| match term {
        TermPattern::Variable(variable) => {
            variables.insert(variable.as_str().to_owned(), variable.clone());
            true
        }
        TermPattern::BlankNode(_) => false,
        _ => true,
    };
    if !insert_term(&pattern.subject) || !insert_term(&pattern.object) {
        return None;
    }
    if let NamedNodePattern::Variable(variable) = &pattern.predicate {
        variables.insert(variable.as_str().to_owned(), variable.clone());
    }
    let mut variables: Vec<_> = variables.into_values().collect();
    variables.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    Some(variables)
}

fn projected_pattern(pattern: TriplePattern, variables: Vec<oxrdf::Variable>) -> GraphPattern {
    GraphPattern::Project {
        inner: Box::new(GraphPattern::Bgp {
            patterns: vec![pattern],
        }),
        variables,
    }
}

fn projected_node(node: GraphPattern, mut variables: Vec<oxrdf::Variable>) -> GraphPattern {
    variables.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    variables.dedup_by(|left, right| left.as_str() == right.as_str());
    GraphPattern::Project {
        inner: Box::new(node),
        variables,
    }
}

fn include_bound_variables(variables: &mut Vec<oxrdf::Variable>, bound: &HashSet<String>) {
    variables.extend(
        bound
            .iter()
            .filter_map(|name| oxrdf::Variable::new(name).ok()),
    );
    variables.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    variables.dedup_by(|left, right| left.as_str() == right.as_str());
}

fn estimate_variable_distinct(
    pattern: &TriplePattern,
    variable: &str,
    rows: u64,
    cx: &PlanCtx<'_>,
) -> u64 {
    let subject = term_var_key(&pattern.subject).is_some_and(|key| key == variable);
    let object = term_var_key(&pattern.object).is_some_and(|key| key == variable);
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(node) => {
            match const_slot(cx, &EncodedTerm::from_named_node(node)) {
                Slot::Const(Some(id)) => Some(crate::store::TermId(id)),
                _ => None,
            }
        }
        NamedNodePattern::Variable(_) => None,
    };
    let estimate = match (subject, object) {
        (true, false) => predicate.map_or_else(
            || cx.distinct(PlannerStat::DistinctSubjects),
            |predicate| cx.distinct(PlannerStat::PredicateSubjects(predicate)),
        ),
        (false, true) => predicate.map_or_else(
            || cx.distinct(PlannerStat::DistinctObjects),
            |predicate| cx.distinct(PlannerStat::PredicateObjects(predicate)),
        ),
        _ => rows,
    };
    estimate.min(rows).max(u64::from(rows > 0))
}

fn ceil_div(left: u64, right: u64) -> u64 {
    if left == 0 {
        return 0;
    }
    left.saturating_add(right.saturating_sub(1)) / right.max(1)
}

fn join_estimate(
    left_rows: u64,
    right_rows: u64,
    left_distinct: u64,
    right_distinct: u64,
) -> PlannedJoin {
    let distinct_keys = left_distinct.min(left_rows).max(u64::from(left_rows > 0));
    let right_per_key = ceil_div(right_rows, right_distinct.max(1));
    let output_rows = left_rows.saturating_mul(right_per_key);
    let lateral_cost = left_rows
        .saturating_add(left_rows.saturating_mul(INDEXED_LOOKUP_COST))
        .saturating_add(output_rows);
    let hash_cost = left_rows
        .saturating_add(right_rows)
        .saturating_add(output_rows);
    PlannedJoin {
        physical_operator: if hash_cost < lateral_cost {
            JoinKind::Hash
        } else {
            JoinKind::IndexedLateral
        },
        estimated_left_rows: left_rows,
        estimated_right_rows: right_rows,
        estimated_distinct_join_keys: distinct_keys,
        estimated_output_rows: output_rows,
        estimated_lateral_cost: lateral_cost,
        estimated_hash_cost: hash_cost,
    }
}

fn apply_demand(estimate: &mut PlannedJoin, right_distinct: u64, demand: u64) {
    if demand >= estimate.estimated_output_rows {
        return;
    }
    let output = demand;
    let left_per_probe = ceil_div(
        estimate.estimated_left_rows,
        estimate.estimated_distinct_join_keys.max(1),
    );
    let probe_rows = ceil_div(output, left_per_probe.max(1)).min(estimate.estimated_right_rows);
    estimate.estimated_hash_cost = estimate
        .estimated_left_rows
        .saturating_add(probe_rows)
        .saturating_add(output);

    let right_per_left = ceil_div(estimate.estimated_right_rows, right_distinct.max(1));
    let outer_rows = ceil_div(output, right_per_left.max(1)).min(estimate.estimated_left_rows);
    estimate.estimated_lateral_cost = outer_rows
        .saturating_add(outer_rows.saturating_mul(INDEXED_LOOKUP_COST))
        .saturating_add(output);
    estimate.physical_operator = if estimate.estimated_hash_cost < estimate.estimated_lateral_cost {
        JoinKind::Hash
    } else {
        JoinKind::IndexedLateral
    };
}

fn physical_chain(
    patterns: Vec<TriplePattern>,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> GraphPattern {
    let mut patterns = patterns.into_iter();
    let first = patterns.next().expect("non-empty pattern chain");
    let mut left_rows = estimate_pattern(&first, bound, cx).unwrap_or(u64::MAX);
    let mut left_distinct = HashMap::new();
    let mut projected = bound.clone();
    projected.extend(cx.graph_var.borrow().clone());
    for key in pattern_var_keys(&first, cx) {
        let estimate = if matches!(cx.join_mode, JoinMode::ForceHash) || left_rows >= HASH_OUTER_MIN
        {
            estimate_variable_distinct(&first, &key, left_rows, cx)
        } else {
            left_rows
        };
        left_distinct.insert(key, estimate);
    }
    let mut left_variables = pattern_variables(&first).map(|mut variables| {
        include_bound_variables(&mut variables, &projected);
        variables
    });
    let mut node = GraphPattern::Bgp {
        patterns: vec![first],
    };

    for pattern in patterns {
        let right_rows = estimate_pattern(&pattern, bound, cx).unwrap_or(u64::MAX);
        let right_variables = pattern_variables(&pattern).map(|mut variables| {
            include_bound_variables(&mut variables, &projected);
            variables
        });
        let right_keys: HashSet<_> = pattern_var_keys(&pattern, cx).into_iter().collect();
        let join_keys: Vec<_> = right_keys
            .iter()
            .filter(|key| left_distinct.contains_key(*key))
            .cloned()
            .collect();
        let left_key_count = join_keys
            .iter()
            .filter_map(|key| left_distinct.get(key).copied())
            .min()
            .unwrap_or(left_rows);
        let consider_hash = matches!(cx.join_mode, JoinMode::ForceHash)
            || (matches!(cx.join_mode, JoinMode::Auto) && left_rows >= HASH_OUTER_MIN);
        let right_key_count = if consider_hash {
            join_keys
                .iter()
                .map(|key| estimate_variable_distinct(&pattern, key, right_rows, cx))
                .min()
                .unwrap_or(right_rows)
        } else {
            right_rows
        };
        let mut estimate = join_estimate(left_rows, right_rows, left_key_count, right_key_count);
        if matches!(cx.join_mode, JoinMode::Auto)
            && let Some(demand) = cx.row_demand.get()
        {
            apply_demand(&mut estimate, right_key_count, demand);
        }
        // A hash side on the graph variable may visit graphs the left side never reached.
        if let Some(graph) = cx.graph_var.borrow().as_ref()
            && join_keys.contains(graph)
        {
            let visited = left_distinct.get(graph).copied().unwrap_or(left_rows);
            let reached = estimate_variable_distinct(&pattern, graph, right_rows, cx);
            let unvisited = reached.saturating_sub(visited);
            estimate.estimated_hash_cost = estimate
                .estimated_hash_cost
                .saturating_add(unvisited.saturating_mul(GRAPH_VISIT_COST));
            if estimate.estimated_hash_cost >= estimate.estimated_lateral_cost {
                estimate.physical_operator = JoinKind::IndexedLateral;
            }
        }
        let hash_eligible =
            !join_keys.is_empty() && left_variables.is_some() && right_variables.is_some();
        estimate.physical_operator = match cx.join_mode {
            JoinMode::Auto if hash_eligible && consider_hash => estimate.physical_operator,
            JoinMode::Auto => JoinKind::IndexedLateral,
            JoinMode::ForceLateral => {
                cx.forced_mode_used.set(true);
                JoinKind::IndexedLateral
            }
            JoinMode::ForceHash if hash_eligible => {
                cx.forced_mode_used.set(true);
                JoinKind::Hash
            }
            JoinMode::ForceHash | JoinMode::ForcePropertyStar => {
                *cx.error.borrow_mut() = Some(PlannerError::ForcedModeUnavailable(cx.join_mode));
                JoinKind::IndexedLateral
            }
        };

        let right_node = GraphPattern::Bgp {
            patterns: vec![pattern.clone()],
        };
        node = match estimate.physical_operator {
            JoinKind::IndexedLateral | JoinKind::PropertyStar => GraphPattern::Lateral {
                left: Box::new(node),
                right: Box::new(right_node),
            },
            JoinKind::Hash => {
                let projected_left = projected_node(
                    node,
                    left_variables.clone().expect("checked hash variables"),
                );
                let projected_right = projected_pattern(
                    pattern.clone(),
                    right_variables.clone().expect("checked hash variables"),
                );
                GraphPattern::Join {
                    left: Box::new(projected_left),
                    right: Box::new(projected_right),
                }
            }
        };
        cx.planned_joins.borrow_mut().push(estimate.clone());
        left_rows = estimate.estimated_output_rows;
        for key in right_keys {
            let right = if consider_hash {
                estimate_variable_distinct(&pattern, &key, right_rows, cx)
            } else {
                right_rows
            };
            left_distinct
                .entry(key)
                .and_modify(|left| *left = (*left).min(right).min(left_rows))
                .or_insert(right.min(left_rows));
        }
        if let (Some(left), Some(right)) = (&mut left_variables, right_variables) {
            left.extend(right);
            left.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            left.dedup_by(|a, b| a.as_str() == b.as_str());
        }
    }
    node
}

/// Stats-driven greedy ordering of a BGP with one physical decision per
/// connected edge. Disconnected components remain ordinary joins.
fn reorder_bgp(
    patterns: Vec<TriplePattern>,
    bound: &HashSet<String>,
    cx: &PlanCtx<'_>,
) -> GraphPattern {
    if patterns.len() < 2 {
        return GraphPattern::Bgp { patterns };
    }

    let estimates: Vec<Option<u64>> = patterns
        .iter()
        .map(|pattern| estimate_pattern(pattern, bound, cx))
        .collect();
    if estimates.iter().any(Option::is_none) {
        return GraphPattern::Bgp { patterns };
    }

    let free_vars: Vec<HashSet<String>> = patterns
        .iter()
        .map(|pattern| {
            pattern_var_keys(pattern, cx)
                .into_iter()
                .filter(|key| !bound.contains(key))
                .collect()
        })
        .collect();

    // Connected components over shared free variables.
    let mut component_of: Vec<Option<usize>> = vec![None; patterns.len()];
    let mut components: Vec<Vec<usize>> = Vec::new();
    for start in 0..patterns.len() {
        if component_of[start].is_some() {
            continue;
        }
        let component_id = components.len();
        let mut stack = vec![start];
        let mut members = Vec::new();
        component_of[start] = Some(component_id);
        while let Some(idx) = stack.pop() {
            members.push(idx);
            for other in 0..patterns.len() {
                if component_of[other].is_none() && !free_vars[idx].is_disjoint(&free_vars[other]) {
                    component_of[other] = Some(component_id);
                    stack.push(other);
                }
            }
        }
        members.sort_unstable();
        components.push(members);
    }

    // Greedy chain per component: start at the smallest estimate, then keep
    // appending the cheapest pattern connected to the already-bound set.
    let mut chains: Vec<(u64, usize, Vec<TriplePattern>)> = Vec::new();
    for members in components {
        let mut local_bound = bound.clone();
        let mut remaining = members;
        let mut chain = Vec::with_capacity(remaining.len());
        let mut chain_cost = u64::MAX;
        let mut first_index = usize::MAX;
        while !remaining.is_empty() {
            let connected = |idx: usize| free_vars[idx].iter().any(|key| local_bound.contains(key));
            let candidate = remaining
                .iter()
                .copied()
                .filter(|&idx| chain.is_empty() || connected(idx))
                .map(|idx| {
                    (
                        estimate_pattern(&patterns[idx], &local_bound, cx).unwrap_or(u64::MAX),
                        idx,
                    )
                })
                .min()
                .or_else(|| {
                    remaining
                        .iter()
                        .copied()
                        .map(|idx| {
                            (
                                estimate_pattern(&patterns[idx], &local_bound, cx)
                                    .unwrap_or(u64::MAX),
                                idx,
                            )
                        })
                        .min()
                });
            let Some((cost, idx)) = candidate else { break };
            if chain.is_empty() {
                chain_cost = cost;
                first_index = idx;
            }
            remaining.retain(|&other| other != idx);
            local_bound.extend(free_vars[idx].iter().cloned());
            chain.push(patterns[idx].clone());
        }
        chains.push((chain_cost, first_index, chain));
    }

    // Most selective component first; stable on original position.
    chains.sort_by_key(|(cost, first_index, _)| (*cost, *first_index));
    chains
        .into_iter()
        .map(|(_, _, chain)| physical_chain(chain, bound, cx))
        .reduce(|left, right| GraphPattern::Join {
            left: Box::new(left),
            right: Box::new(right),
        })
        .expect("non-empty BGP")
}

/// Push an upper row bound through row-preserving operators while retaining the outer slice.
fn push_slice_cap(pattern: GraphPattern, cap: usize) -> GraphPattern {
    match pattern {
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(push_slice_cap(*inner, cap)),
            variables,
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(push_slice_cap(*inner, cap)),
            variable,
            expression,
        },
        GraphPattern::Union { left, right } => {
            let cap_branch = |branch: GraphPattern| match branch {
                GraphPattern::Slice {
                    inner,
                    start: 0,
                    length: Some(existing),
                } if existing <= cap => GraphPattern::Slice {
                    inner,
                    start: 0,
                    length: Some(existing),
                },
                other => GraphPattern::Slice {
                    inner: Box::new(push_slice_cap(other, cap)),
                    start: 0,
                    length: Some(cap),
                },
            };
            GraphPattern::Union {
                left: Box::new(cap_branch(*left)),
                right: Box::new(cap_branch(*right)),
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ActorId, Dot, GraphId};
    use crate::store::{EncodedQuad, QuadAdd};
    use spargebra::SparqlParser;
    use std::sync::Arc;

    fn setup_store() -> (tempfile::TempDir, Arc<GraphStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(GraphStore::open(dir.path()).unwrap());
        (dir, store)
    }

    fn insert(store: &GraphStore, graph: &str, subject: &str, predicate: &str, object: &str) {
        let graph = GraphId::new(graph);
        if !store.contains_graph(&graph).unwrap() {
            store.create_graph(&graph).unwrap();
        }
        let mut batch = store.new_batch();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let subject_id = store
            .resolve_term(&EncodedTerm(subject.to_string()))
            .unwrap();
        let predicate_id = store
            .resolve_term(&EncodedTerm(predicate.to_string()))
            .unwrap();
        let object_id = store
            .resolve_term(&EncodedTerm(object.to_string()))
            .unwrap();
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
        store.commit(batch).unwrap();
    }

    fn seeded_store() -> (tempfile::TempDir, Arc<GraphStore>) {
        let (dir, store) = setup_store();
        for idx in 0..50 {
            let graph = format!("urn:g:{idx}");
            let dataset = format!("<urn:d:{idx}>");
            insert(
                &store,
                &graph,
                &dataset,
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://schema.org/Dataset>",
            );
            insert(
                &store,
                &graph,
                &dataset,
                "<http://schema.org/name>",
                &format!("\"Dataset {idx}\""),
            );
        }
        (dir, store)
    }

    fn parse(query: &str) -> Query {
        SparqlParser::new().parse_query(query).unwrap()
    }

    fn select_pattern(query: &Query) -> &GraphPattern {
        match query {
            Query::Select { pattern, .. } => pattern,
            _ => panic!("expected SELECT"),
        }
    }

    fn first_lateral_leaf(pattern: &GraphPattern) -> Option<&TriplePattern> {
        match pattern {
            GraphPattern::Lateral { left, .. } => first_lateral_leaf(left),
            GraphPattern::Bgp { patterns } if patterns.len() == 1 => Some(&patterns[0]),
            GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::Filter { inner, .. }
            | GraphPattern::Extend { inner, .. } => first_lateral_leaf(inner),
            _ => None,
        }
    }

    #[test]
    fn selective_patterns_lead() {
        let (_dir, store) = seeded_store();
        for written in [
            "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> . ?d <http://schema.org/name> \"Dataset 7\" }",
            "SELECT ?d WHERE { ?d <http://schema.org/name> \"Dataset 7\" . ?d a <http://schema.org/Dataset> }",
        ] {
            let mut query = parse(written);
            optimize_query(&mut query, &store);
            let leaf = first_lateral_leaf(select_pattern(&query)).expect("lateral chain");
            assert!(
                matches!(&leaf.object, TermPattern::Literal(l) if l.value() == "Dataset 7"),
                "selective name pattern must run first, got {leaf}"
            );
        }
    }

    #[test]
    fn string_equality_folds() {
        let (_dir, store) = seeded_store();
        let mut query = parse(
            "SELECT ?d ?n WHERE { ?d <http://schema.org/name> ?n . FILTER(?n = \"Dataset 7\") }",
        );
        optimize_query(&mut query, &store);
        let mut pattern = select_pattern(&query);
        loop {
            match pattern {
                GraphPattern::Project { inner, .. } => pattern = inner,
                GraphPattern::Extend {
                    inner,
                    variable,
                    expression,
                } => {
                    assert_eq!(variable.as_str(), "n");
                    assert!(matches!(expression, Expression::Literal(_)));
                    pattern = inner;
                }
                GraphPattern::Bgp { patterns } => {
                    assert_eq!(patterns.len(), 1);
                    assert!(matches!(&patterns[0].object, TermPattern::Literal(_)));
                    return;
                }
                other => panic!("unexpected node: {other:?}"),
            }
        }
    }

    #[test]
    fn numeric_equality_remains() {
        let (_dir, store) = seeded_store();
        let mut query =
            parse("SELECT ?d WHERE { ?d <http://schema.org/version> ?v . FILTER(?v = 1) }");
        optimize_query(&mut query, &store);
        fn has_filter(pattern: &GraphPattern) -> bool {
            match pattern {
                GraphPattern::Filter { .. } => true,
                GraphPattern::Project { inner, .. } => has_filter(inner),
                _ => false,
            }
        }
        assert!(has_filter(select_pattern(&query)));
    }

    #[test]
    fn missing_terms_lead() {
        let (_dir, store) = seeded_store();
        let mut query = parse(
            "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> . ?d <http://schema.org/name> \"No Such Name\" }",
        );
        optimize_query(&mut query, &store);
        let leaf = first_lateral_leaf(select_pattern(&query)).expect("lateral chain");
        assert!(matches!(&leaf.object, TermPattern::Literal(_)));
    }

    #[test]
    fn disconnected_patterns_join() {
        let (_dir, store) = seeded_store();
        let mut query = parse(
            "SELECT * WHERE { ?a <http://schema.org/name> ?n . ?b a <http://schema.org/Dataset> }",
        );
        optimize_query(&mut query, &store);
        fn has_join(pattern: &GraphPattern) -> bool {
            match pattern {
                GraphPattern::Join { .. } => true,
                GraphPattern::Project { inner, .. } => has_join(inner),
                _ => false,
            }
        }
        assert!(has_join(select_pattern(&query)));
    }

    #[test]
    fn compare_join_costs() {
        for outer_rows in [1, 10, 1_000, 100_000] {
            for distinct_keys in [1, 10, 1_000, 100_000] {
                for right_rows in [100, 100_000, 10_000_000] {
                    let estimate = join_estimate(
                        outer_rows,
                        right_rows,
                        distinct_keys.min(outer_rows),
                        right_rows.min(100_000),
                    );
                    assert!(estimate.estimated_hash_cost >= estimate.estimated_output_rows);
                    assert!(estimate.estimated_lateral_cost >= estimate.estimated_output_rows);
                }
            }
        }

        assert_eq!(
            join_estimate(10, 100_000, 10, 100_000).physical_operator,
            JoinKind::IndexedLateral
        );
        assert_eq!(
            join_estimate(100_000, 100_000, 100_000, 100_000).physical_operator,
            JoinKind::Hash
        );
    }

    #[test]
    fn stat_memo_reuses() {
        let (_dir, store) = seeded_store();
        let cx = PlanCtx::new(
            &store,
            PlanMode {
                join: JoinMode::Auto,
                collect_costs: true,
                graph: None,
            },
        );
        let first = cx.stat(PlannerStat::Total);
        assert_eq!(cx.stat(PlannerStat::Total), first);
        let costs = cx.costs.snapshot();
        assert_eq!(costs.planner_memo_misses, 1);
        assert_eq!(costs.planner_memo_hits, 1);
    }

    #[test]
    fn forced_join_shapes() {
        let (_dir, store) = seeded_store();
        let text = "SELECT ?d ?n WHERE { ?d a <http://schema.org/Dataset> . ?d <http://schema.org/name> ?n }";

        let mut hash = parse(text);
        let hash_trace = optimize_with_mode(&mut hash, &store, JoinMode::ForceHash).unwrap();
        assert_eq!(hash_trace.joins.len(), 1);
        assert_eq!(hash_trace.joins[0].physical_operator, JoinKind::Hash);
        fn has_guarded_hash(pattern: &GraphPattern) -> bool {
            match pattern {
                GraphPattern::Join { left, right }
                    if matches!(left.as_ref(), GraphPattern::Project { .. })
                        && matches!(right.as_ref(), GraphPattern::Project { .. }) =>
                {
                    true
                }
                GraphPattern::Project { inner, .. } => has_guarded_hash(inner),
                _ => false,
            }
        }
        assert!(has_guarded_hash(select_pattern(&hash)));

        let mut lateral = parse(text);
        let lateral_trace =
            optimize_with_mode(&mut lateral, &store, JoinMode::ForceLateral).unwrap();
        assert_eq!(lateral_trace.joins.len(), 1);
        assert_eq!(
            lateral_trace.joins[0].physical_operator,
            JoinKind::IndexedLateral
        );
        assert!(first_lateral_leaf(select_pattern(&lateral)).is_some());
    }

    /// Inside `GRAPH ?g`, a hash side that reaches unvisited graphs loses to lateral probes.
    #[test]
    fn graph_visits_priced() {
        let (_dir, store) = setup_store();
        for idx in 0..600 {
            let graph = format!("urn:g:{idx}");
            let root = format!("<{graph}>");
            insert(
                &store,
                &graph,
                &format!("<urn:d:{idx}>"),
                "<urn:about>",
                &root,
            );
            insert(&store, &graph, &root, "<urn:name>", "\"n\"");
            if idx % 2 == 0 {
                insert(&store, &graph, &root, "<urn:author>", "<urn:person>");
            }
        }
        let plan = |text: &str| {
            let mut query = parse(text);
            let trace = optimize_with_mode(&mut query, &store, JoinMode::Auto).unwrap();
            trace.joins[0].physical_operator
        };
        assert_eq!(
            plan(
                "SELECT ?g WHERE { GRAPH ?g { ?d <urn:about> ?g . ?g <urn:author> <urn:person> } }"
            ),
            JoinKind::IndexedLateral
        );
        assert_eq!(
            plan("SELECT ?g WHERE { GRAPH ?g { ?d <urn:about> ?g . ?g <urn:name> ?n } }"),
            JoinKind::Hash
        );
    }

    #[test]
    fn forced_join_fallback() {
        let (_dir, store) = seeded_store();
        let mut query = parse("SELECT ?d WHERE { ?d a <http://schema.org/Dataset> }");
        assert!(matches!(
            optimize_with_mode(&mut query, &store, JoinMode::ForceHash),
            Err(PlannerError::ForcedModeUnavailable(JoinMode::ForceHash))
        ));

        let mut blank_join = parse(
            "SELECT * WHERE { _:d a <http://schema.org/Dataset> . _:d <http://schema.org/name> ?n }",
        );
        assert!(matches!(
            optimize_with_mode(&mut blank_join, &store, JoinMode::ForceHash),
            Err(PlannerError::ForcedModeUnavailable(JoinMode::ForceHash))
        ));
    }
}
