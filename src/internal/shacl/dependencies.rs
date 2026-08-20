use std::collections::BTreeSet;

use crate::EncodedTerm;

use super::model::{ConstraintPlan, PathPlan, ShapeDependencies, ShapeId, TargetPlan};

pub(crate) fn analyze(
    targets: &[TargetPlan],
    path: Option<&PathPlan>,
    constraints: &[ConstraintPlan],
    property_shapes: &[ShapeId],
) -> ShapeDependencies {
    let mut forward = BTreeSet::new();
    let mut inverse = BTreeSet::new();
    let mut classes = BTreeSet::new();
    let mut nested: BTreeSet<ShapeId> = property_shapes.iter().copied().collect();
    let mut reads_rdf_type = false;
    let mut reads_all_outgoing = false;
    let mut has_transitive_path = false;

    for target in targets {
        match target {
            TargetPlan::Class(class) | TargetPlan::ImplicitClass(class) => {
                classes.insert(class.clone());
                reads_rdf_type = true;
            }
            TargetPlan::SubjectsOf(predicate) => {
                forward.insert(predicate.clone());
            }
            TargetPlan::ObjectsOf(predicate) => {
                inverse.insert(predicate.clone());
            }
            TargetPlan::Node(_) => {}
        }
    }
    if let Some(path) = path {
        collect_path(
            path,
            false,
            &mut forward,
            &mut inverse,
            &mut has_transitive_path,
        );
    }
    for constraint in constraints {
        match constraint {
            ConstraintPlan::Class(_) => reads_rdf_type = true,
            ConstraintPlan::Equals(predicate)
            | ConstraintPlan::Disjoint(predicate)
            | ConstraintPlan::LessThan(predicate)
            | ConstraintPlan::LessThanOrEquals(predicate) => {
                forward.insert(predicate.clone());
            }
            ConstraintPlan::Or(shapes)
            | ConstraintPlan::And(shapes)
            | ConstraintPlan::Xone(shapes) => nested.extend(shapes.iter().copied()),
            ConstraintPlan::Not(shape) | ConstraintPlan::Node(shape) => {
                nested.insert(*shape);
            }
            ConstraintPlan::QualifiedValueShape {
                shape, siblings, ..
            } => {
                nested.insert(*shape);
                nested.extend(siblings.iter().copied());
            }
            ConstraintPlan::Closed { .. } => reads_all_outgoing = true,
            _ => {}
        }
    }

    ShapeDependencies {
        forward_predicates: forward.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        inverse_predicates: inverse.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        target_classes: classes.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        nested_shapes: nested.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        reads_rdf_type,
        reads_all_outgoing_predicates: reads_all_outgoing,
        has_transitive_path,
        requires_global_work: false,
    }
}

fn collect_path(
    path: &PathPlan,
    inverted: bool,
    forward: &mut BTreeSet<EncodedTerm>,
    inverse: &mut BTreeSet<EncodedTerm>,
    transitive: &mut bool,
) {
    match path {
        PathPlan::Predicate(predicate) => {
            if inverted {
                inverse.insert(predicate.clone());
            } else {
                forward.insert(predicate.clone());
            }
        }
        PathPlan::Alternative(paths) | PathPlan::Sequence(paths) => {
            for path in paths {
                collect_path(path, inverted, forward, inverse, transitive);
            }
        }
        PathPlan::Inverse(path) => collect_path(path, !inverted, forward, inverse, transitive),
        PathPlan::ZeroOrMore(path) | PathPlan::OneOrMore(path) => {
            *transitive = true;
            collect_path(path, inverted, forward, inverse, transitive);
        }
        PathPlan::ZeroOrOne(path) => {
            collect_path(path, inverted, forward, inverse, transitive);
        }
    }
}
