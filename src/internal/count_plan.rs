//! Type-safe count planning is kept separate from execution so eligibility
//! decisions cannot be confused with exact stored counts.

use spargebra::algebra::{AggregateExpression, AggregateFunction, Expression, GraphPattern};

use crate::sparql_fast_path::{
    FastPathPlan, same_subject_triples, single_triple, two_joined_triples,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CountValueDomain {
    Scalar,
    Subject,
    Object,
}

pub(crate) fn analyze(pattern: &GraphPattern) -> Option<FastPathPlan> {
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
    if count_all
        && let Some((_, triples)) = same_subject_triples(inner)
        && triples.len() > 2
    {
        return Some(FastPathPlan::SubjectStarCount {
            triples,
            output: output.as_str().to_owned(),
        });
    }
    if count_all && let Some((left, right, join_variables)) = two_joined_triples(inner) {
        return Some(FastPathPlan::HashJoinCount {
            left,
            right,
            output: output.as_str().to_owned(),
            join_variables,
        });
    }
    let triple = single_triple(inner)?;
    let domain = match aggregate {
        AggregateExpression::CountSolutions { distinct: false } => CountValueDomain::Scalar,
        AggregateExpression::FunctionCall {
            name: AggregateFunction::Count,
            expr: Expression::Variable(variable),
            distinct: false,
        } if triple.binds(variable) => CountValueDomain::Scalar,
        AggregateExpression::FunctionCall {
            name: AggregateFunction::Count,
            expr: Expression::Variable(variable),
            distinct: true,
        } if triple.distinct_subject_order(variable) => CountValueDomain::Subject,
        AggregateExpression::FunctionCall {
            name: AggregateFunction::Count,
            expr: Expression::Variable(variable),
            distinct: true,
        } if triple.distinct_object_order(variable) => CountValueDomain::Object,
        _ => return None,
    };
    Some(FastPathPlan::Count {
        triple,
        output: output.as_str().to_owned(),
        domain,
    })
}
