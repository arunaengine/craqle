//! Evaluates DISTINCT graph and grouped COUNT(DISTINCT graph) shapes over dense query IDs.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::collections::HashMap;

use oxrdf::{BlankNode, Variable};
use spargebra::Query;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use crate::core::EncodedTerm;
use crate::graph_join::MAX_VARIABLES;

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
