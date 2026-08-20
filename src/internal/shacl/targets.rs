use std::collections::BTreeSet;

use crate::Result;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView};
use crate::store::TermId;

use super::resolve::{ResolvedSchema, ResolvedTarget};

#[derive(Default)]
pub(crate) struct TargetWork {
    pub(crate) candidates: u64,
}

pub(crate) fn resolve_targets<V: RdfReadView + ?Sized>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    rdf_type: TermId,
    schema: &ResolvedSchema,
    work: &mut TargetWork,
) -> Result<Vec<BTreeSet<TermId>>> {
    let mut targets = Vec::with_capacity(schema.shapes.len());
    for shape in &schema.shapes {
        let mut focus_nodes = BTreeSet::new();
        for target in &shape.targets {
            match target {
                ResolvedTarget::Node(node) => {
                    work.candidates = work.candidates.saturating_add(1);
                    focus_nodes.insert(*node);
                }
                ResolvedTarget::Class(class) | ResolvedTarget::ImplicitClass(class) => {
                    let cursor = view.scan(
                        context,
                        GraphSelector::Named(graph),
                        QuadPattern {
                            predicate: Some(rdf_type),
                            object: Some(*class),
                            ..QuadPattern::default()
                        },
                    )?;
                    for quad in cursor {
                        let quad = quad?;
                        work.candidates = work.candidates.saturating_add(1);
                        focus_nodes.insert(quad.subject);
                    }
                }
                ResolvedTarget::SubjectsOf(predicate) => {
                    let cursor = view.scan(
                        context,
                        GraphSelector::Named(graph),
                        QuadPattern {
                            predicate: Some(*predicate),
                            ..QuadPattern::default()
                        },
                    )?;
                    for quad in cursor {
                        let quad = quad?;
                        work.candidates = work.candidates.saturating_add(1);
                        focus_nodes.insert(quad.subject);
                    }
                }
                ResolvedTarget::ObjectsOf(predicate) => {
                    let cursor = view.scan(
                        context,
                        GraphSelector::Named(graph),
                        QuadPattern {
                            predicate: Some(*predicate),
                            ..QuadPattern::default()
                        },
                    )?;
                    for quad in cursor {
                        let quad = quad?;
                        work.candidates = work.candidates.saturating_add(1);
                        focus_nodes.insert(quad.object);
                    }
                }
            }
        }
        targets.push(focus_nodes);
    }

    loop {
        let mut changed = false;
        for shape in &schema.portable.shapes {
            if shape.deactivated {
                continue;
            }
            let inherited = targets[shape.id.0 as usize].clone();
            for property in &shape.property_shapes {
                let property_targets = &mut targets[property.0 as usize];
                let before = property_targets.len();
                property_targets.extend(inherited.iter().copied());
                changed |= property_targets.len() != before;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(targets)
}
