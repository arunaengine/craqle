//! Craqle-owned query plan optimization.
//!
//! Rewrites the spargebra AST before it is handed to spareval, using the
//! store's real cardinality statistics instead of sparopt's static guesses:
//!
//! * Triple patterns inside each BGP are reordered by estimated cardinality
//!   and emitted as an explicit left-deep `Lateral` chain. sparopt never
//!   reorders across lateral boundaries, so the chain locks both the join
//!   order and the for-loop (index nested loop) join strategy. This also
//!   makes OPTIONAL bodies "fit" for sparopt's ForLoopLeftJoin conversion
//!   (multi-pattern bodies otherwise stay hash joins that materialize their
//!   full build side) and fixes EXISTS bodies, which sparopt never reorders
//!   at all (they otherwise run as per-row cartesian products).
//! * `FILTER(?v = <iri>)`, `FILTER(?v = "string")` and `FILTER(sameTerm(...))`
//!   over a BGP are folded into the patterns as bound terms (index lookups),
//!   with an Extend re-binding the variable. Numeric/value equality is never
//!   folded (`"01"^^xsd:integer = "1"^^xsd:integer` is value-equal but not
//!   term-equal), and string folds are skipped when a non-canonical
//!   `^^xsd:string` spelling of the same value exists in the term table.
//! * LIMIT caps are pushed through row-preserving operators (Project/Extend)
//!   into UNION branches.
//!
//! Everything else (OPTIONAL scoping, MINUS, DISTINCT/ORDER interactions,
//! property paths, sub-SELECTs, SERVICE bodies) is left untouched: the pass
//! only recurses into those nodes, it never moves work across them.

use std::collections::HashSet;

use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Term};
use spargebra::Query;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::store::GraphStore;

/// Per-row cost guesses for patterns whose selective position is a variable
/// that will already be bound when the pattern runs inside a lateral chain.
/// They only need to compare correctly against real corpus counts.
const COST_BOUND_S_CONST_PO: u64 = 1;
const COST_BOUND_S_CONST_P: u64 = 3;
const COST_BOUND_S: u64 = 6;
const COST_CONST_P_BOUND_O: u64 = 4;
const COST_BOUND_O: u64 = 8;
const COST_BOUND_ONLY_P: u64 = 1 << 20;

pub(crate) fn optimize_query(query: &mut Query, store: &GraphStore) {
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
            *pattern = optimize_pattern(current, &HashSet::new(), store);
        }
    }
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

fn optimize_pattern(
    pattern: GraphPattern,
    bound: &HashSet<String>,
    store: &GraphStore,
) -> GraphPattern {
    match pattern {
        GraphPattern::Bgp { patterns } => reorder_bgp(patterns, bound, store),
        GraphPattern::Path { .. } | GraphPattern::Values { .. } => pattern,
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(optimize_pattern(*left, bound, store)),
            right: Box::new(optimize_pattern(*right, bound, store)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left = optimize_pattern(*left, bound, store);
            // sparopt evaluates fit OPTIONAL bodies as for-loop left joins
            // with the outer row bound, so order the body accordingly.
            let mut right_bound = bound.clone();
            collect_pattern_vars(&left, &mut right_bound);
            let right = optimize_pattern(*right, &right_bound, store);
            let mut expr_bound = right_bound.clone();
            collect_pattern_vars(&right, &mut expr_bound);
            GraphPattern::LeftJoin {
                left: Box::new(left),
                right: Box::new(right),
                expression: expression.map(|e| optimize_expression(e, &expr_bound, store)),
            }
        }
        GraphPattern::Lateral { left, right } => {
            let left = optimize_pattern(*left, bound, store);
            let mut right_bound = bound.clone();
            collect_pattern_vars(&left, &mut right_bound);
            GraphPattern::Lateral {
                right: Box::new(optimize_pattern(*right, &right_bound, store)),
                left: Box::new(left),
            }
        }
        GraphPattern::Filter { expr, inner } => {
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            let expr = optimize_expression(expr, &expr_bound, store);
            if let GraphPattern::Bgp { patterns } = *inner {
                rewrite_filter_over_bgp(expr, patterns, bound, store)
            } else {
                GraphPattern::Filter {
                    expr,
                    inner: Box::new(optimize_pattern(*inner, bound, store)),
                }
            }
        }
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(optimize_pattern(*left, bound, store)),
            right: Box::new(optimize_pattern(*right, bound, store)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(optimize_pattern(*inner, bound, store)),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let inner = optimize_pattern(*inner, bound, store);
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            GraphPattern::Extend {
                expression: optimize_expression(expression, &expr_bound, store),
                inner: Box::new(inner),
                variable,
            }
        }
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(optimize_pattern(*left, bound, store)),
            right: Box::new(optimize_pattern(*right, bound, store)),
        },
        GraphPattern::OrderBy { inner, expression } => {
            let inner = optimize_pattern(*inner, bound, store);
            let mut expr_bound = bound.clone();
            collect_pattern_vars(&inner, &mut expr_bound);
            GraphPattern::OrderBy {
                inner: Box::new(inner),
                expression: expression
                    .into_iter()
                    .map(|order| match order {
                        OrderExpression::Asc(e) => {
                            OrderExpression::Asc(optimize_expression(e, &expr_bound, store))
                        }
                        OrderExpression::Desc(e) => {
                            OrderExpression::Desc(optimize_expression(e, &expr_bound, store))
                        }
                    })
                    .collect(),
            }
        }
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(optimize_pattern(*inner, bound, store)),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(optimize_pattern(*inner, bound, store)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(optimize_pattern(*inner, bound, store)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let mut inner = optimize_pattern(*inner, bound, store);
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
            let inner = optimize_pattern(*inner, bound, store);
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
                                expr: optimize_expression(expr, &expr_bound, store),
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
    store: &GraphStore,
) -> Expression {
    let walk = |e: Box<Expression>| Box::new(optimize_expression(*e, bound, store));
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
                .map(|e| optimize_expression(e, bound, store))
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
            Expression::Exists(Box::new(optimize_pattern(*inner, bound, store)))
        }
        Expression::If(a, b, c) => Expression::If(walk(a), walk(b), walk(c)),
        Expression::Coalesce(list) => Expression::Coalesce(
            list.into_iter()
                .map(|e| optimize_expression(e, bound, store))
                .collect(),
        ),
        Expression::FunctionCall(function, args) => Expression::FunctionCall(
            function,
            args.into_iter()
                .map(|e| optimize_expression(e, bound, store))
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
fn has_non_canonical_string_spelling(store: &GraphStore, literal: &Literal) -> bool {
    let alternate = EncodedTerm(format!(
        "{}^^<http://www.w3.org/2001/XMLSchema#string>",
        literal
    ));
    matches!(store.lookup_term(&alternate), Ok(Some(_)))
}

fn fold_variable_into_patterns(
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

fn rewrite_filter_over_bgp(
    expr: Expression,
    mut patterns: Vec<TriplePattern>,
    bound: &HashSet<String>,
    store: &GraphStore,
) -> GraphPattern {
    let mut conjuncts = Vec::new();
    flatten_and(expr, &mut conjuncts);

    let mut bindings: Vec<(oxrdf::Variable, FoldableConstant)> = Vec::new();
    let mut remaining = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        let folded = foldable_equality(&conjunct).and_then(|(variable, constant)| {
            match &constant {
                FoldableConstant::StringLiteral(literal)
                    if has_non_canonical_string_spelling(store, literal) =>
                {
                    return None;
                }
                _ => {}
            }
            fold_variable_into_patterns(&mut patterns, &variable, &constant)
                .then_some((variable, constant))
        });
        match folded {
            Some(binding) => bindings.push(binding),
            None => remaining.push(conjunct),
        }
    }

    let mut node = reorder_bgp(patterns, bound, store);
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

fn term_slot(term: &TermPattern, bound: &HashSet<String>, store: &GraphStore) -> Slot {
    match term {
        TermPattern::NamedNode(node) => const_slot(store, &EncodedTerm::from_named_node(node)),
        TermPattern::Literal(literal) => const_slot(
            store,
            &EncodedTerm::from_term(&Term::Literal(literal.clone())),
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

fn const_slot(store: &GraphStore, term: &EncodedTerm) -> Slot {
    match store.lookup_term(term) {
        Ok(id) => Slot::Const(id.map(|id| id.0)),
        Err(_) => Slot::Unsupported,
    }
}

fn predicate_slot(
    predicate: &NamedNodePattern,
    bound: &HashSet<String>,
    store: &GraphStore,
) -> Slot {
    match predicate {
        NamedNodePattern::NamedNode(node) => const_slot(store, &EncodedTerm::from_named_node(node)),
        NamedNodePattern::Variable(v) => {
            if bound.contains(v.as_str()) {
                Slot::BoundVar
            } else {
                Slot::FreeVar
            }
        }
    }
}

/// Approximate match count for one triple pattern given the variables that
/// will already be bound when it executes. Real corpus counts for free
/// patterns, small constants for index-addressable bound positions.
fn estimate_pattern(
    pattern: &TriplePattern,
    bound: &HashSet<String>,
    store: &GraphStore,
) -> Option<u64> {
    use crate::store::TermId;
    let subject = term_slot(&pattern.subject, bound, store);
    let predicate = predicate_slot(&pattern.predicate, bound, store);
    let object = term_slot(&pattern.object, bound, store);
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
            let mut estimate = store.stat_subject_count(TermId(s)) as u64;
            match (&predicate, &object) {
                (Slot::Const(Some(p)), Slot::Const(Some(o))) => {
                    let pair = store.stat_predicate_object_count(TermId(*p), TermId(*o)) as u64;
                    estimate = estimate.min(pair).min(1);
                }
                (Slot::Const(Some(p)), _) => {
                    estimate = estimate.min(store.stat_predicate_count(TermId(*p)) as u64);
                }
                (_, Slot::Const(Some(o))) => {
                    estimate = estimate.min(store.stat_object_count(TermId(*o)) as u64);
                }
                _ => {}
            }
            estimate
        }
        (Slot::BoundVar, Slot::Const(_), Slot::Const(_) | Slot::BoundVar) => COST_BOUND_S_CONST_PO,
        (Slot::BoundVar, Slot::Const(_), _) => COST_BOUND_S_CONST_P,
        (Slot::BoundVar, _, _) => COST_BOUND_S,
        (Slot::FreeVar, Slot::Const(Some(p)), Slot::Const(Some(o))) => {
            store.stat_predicate_object_count(TermId(p), TermId(o)) as u64
        }
        (Slot::FreeVar, Slot::Const(_), Slot::BoundVar) => COST_CONST_P_BOUND_O,
        (Slot::FreeVar, Slot::Const(Some(p)), Slot::FreeVar) => {
            store.stat_predicate_count(TermId(p)) as u64
        }
        (Slot::FreeVar, _, Slot::Const(Some(o))) => store.stat_object_count(TermId(o)) as u64,
        (Slot::FreeVar, _, Slot::BoundVar) => COST_BOUND_O,
        (Slot::FreeVar, Slot::BoundVar, Slot::FreeVar) => {
            COST_BOUND_ONLY_P.min(store.stat_total_quads() as u64)
        }
        (Slot::FreeVar, Slot::FreeVar, Slot::FreeVar) => store.stat_total_quads() as u64,
        // Const(None) and Unsupported handled above.
        _ => store.stat_total_quads() as u64,
    })
}

fn lateral_chain(patterns: Vec<TriplePattern>) -> GraphPattern {
    patterns
        .into_iter()
        .map(|pattern| GraphPattern::Bgp {
            patterns: vec![pattern],
        })
        .reduce(|left, right| GraphPattern::Lateral {
            left: Box::new(left),
            right: Box::new(right),
        })
        .expect("non-empty pattern chain")
}

/// Stats-driven greedy ordering of a BGP, emitted as an explicit lateral
/// chain per connected component (components joined by Join nodes so large
/// disconnected products keep sparopt's hash/cartesian strategy).
fn reorder_bgp(
    patterns: Vec<TriplePattern>,
    bound: &HashSet<String>,
    store: &GraphStore,
) -> GraphPattern {
    if patterns.len() < 2 {
        return GraphPattern::Bgp { patterns };
    }

    let estimates: Vec<Option<u64>> = patterns
        .iter()
        .map(|pattern| estimate_pattern(pattern, bound, store))
        .collect();
    if estimates.iter().any(Option::is_none) {
        return GraphPattern::Bgp { patterns };
    }

    let free_vars: Vec<HashSet<String>> = patterns
        .iter()
        .map(|pattern| {
            triple_var_keys(pattern)
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
                        estimate_pattern(&patterns[idx], &local_bound, store).unwrap_or(u64::MAX),
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
                                estimate_pattern(&patterns[idx], &local_bound, store)
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
        .map(|(_, _, chain)| lateral_chain(chain))
        .reduce(|left, right| GraphPattern::Join {
            left: Box::new(left),
            right: Box::new(right),
        })
        .expect("non-empty BGP")
}

// --- LIMIT pushdown ----------------------------------------------------------

/// Pushes an upper row bound through row-preserving operators into UNION
/// branches. The outer Slice stays in place; this only caps how much each
/// branch may produce.
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
    fn bgp_reorder_puts_selective_pattern_first_regardless_of_written_order() {
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
    fn filter_string_equality_folds_into_index_lookup() {
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
    fn filter_numeric_equality_is_not_folded() {
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
    fn missing_terms_estimate_to_zero_and_lead_the_chain() {
        let (_dir, store) = seeded_store();
        let mut query = parse(
            "SELECT ?d WHERE { ?d a <http://schema.org/Dataset> . ?d <http://schema.org/name> \"No Such Name\" }",
        );
        optimize_query(&mut query, &store);
        let leaf = first_lateral_leaf(select_pattern(&query)).expect("lateral chain");
        assert!(matches!(&leaf.object, TermPattern::Literal(_)));
    }

    #[test]
    fn disconnected_patterns_stay_joined_not_lateral() {
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
}
