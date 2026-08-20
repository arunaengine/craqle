use std::collections::BTreeSet;

use crate::Result;
use crate::query_context::ReadContext;
use crate::rdf_read::{GraphSelector, RdfReadView};
use crate::shacl::{ShaclError, ShaclValidationOptions};
use crate::store::TermId;

use super::resolve::ResolvedPath;

#[derive(Default)]
pub(crate) struct PathWork {
    pub(crate) edges_read: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn values<V: RdfReadView + ?Sized>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    focus: TermId,
    path: &ResolvedPath,
    cap: Option<usize>,
    options: &ShaclValidationOptions,
    work: &mut PathWork,
) -> Result<BTreeSet<TermId>> {
    walk(view, context, graph, focus, path, false, cap, options, work)
}

#[allow(clippy::too_many_arguments)]
fn walk<V: RdfReadView + ?Sized>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    focus: TermId,
    path: &ResolvedPath,
    inverted: bool,
    cap: Option<usize>,
    options: &ShaclValidationOptions,
    work: &mut PathWork,
) -> Result<BTreeSet<TermId>> {
    context.check_cancelled()?;
    if cap == Some(0) {
        return Ok(BTreeSet::new());
    }
    match path {
        ResolvedPath::Predicate(predicate) => predicate_values(
            view, context, graph, focus, *predicate, inverted, cap, options, work,
        ),
        ResolvedPath::Alternative(paths) => {
            let mut output = BTreeSet::new();
            for path in paths {
                let remaining = cap.map(|cap| cap.saturating_sub(output.len()));
                output.extend(walk(
                    view, context, graph, focus, path, inverted, remaining, options, work,
                )?);
                if cap.is_some_and(|cap| output.len() >= cap) {
                    break;
                }
            }
            Ok(output)
        }
        ResolvedPath::Sequence(paths) => {
            let mut frontier = BTreeSet::from([focus]);
            let length = paths.len();
            for (index, path_index) in (0..length).enumerate() {
                let path_index = if inverted {
                    length - 1 - path_index
                } else {
                    path_index
                };
                let mut next = BTreeSet::new();
                for node in frontier {
                    let final_step = index + 1 == length;
                    let remaining = if final_step {
                        cap.map(|cap| cap.saturating_sub(next.len()))
                    } else {
                        None
                    };
                    next.extend(walk(
                        view,
                        context,
                        graph,
                        node,
                        &paths[path_index],
                        inverted,
                        remaining,
                        options,
                        work,
                    )?);
                    if final_step && cap.is_some_and(|cap| next.len() >= cap) {
                        break;
                    }
                }
                frontier = next;
                if frontier.is_empty() {
                    break;
                }
            }
            Ok(frontier)
        }
        ResolvedPath::Inverse(path) => walk(
            view, context, graph, focus, path, !inverted, cap, options, work,
        ),
        ResolvedPath::ZeroOrOne(path) => {
            let mut output = BTreeSet::from([focus]);
            if cap != Some(1) {
                let remaining = cap.map(|cap| cap.saturating_sub(1));
                output.extend(walk(
                    view, context, graph, focus, path, inverted, remaining, options, work,
                )?);
            }
            Ok(output)
        }
        ResolvedPath::ZeroOrMore(path) => repeat(
            view, context, graph, focus, path, inverted, true, cap, options, work,
        ),
        ResolvedPath::OneOrMore(path) => repeat(
            view, context, graph, focus, path, inverted, false, cap, options, work,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn predicate_values<V: RdfReadView + ?Sized>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    focus: TermId,
    predicate: TermId,
    inverted: bool,
    cap: Option<usize>,
    options: &ShaclValidationOptions,
    work: &mut PathWork,
) -> Result<BTreeSet<TermId>> {
    let selector = GraphSelector::Named(graph);
    let cursor = if inverted {
        view.inverse_predicate(context, selector, predicate, focus)?
    } else {
        view.forward_predicate(context, selector, focus, predicate)?
    };
    let mut output = BTreeSet::new();
    for quad in cursor {
        let quad = quad?;
        work.edges_read = work.edges_read.saturating_add(1);
        if work.edges_read > options.max_path_edges {
            return Err(ShaclError::PathBudgetExceeded {
                limit: options.max_path_edges,
            }
            .into());
        }
        output.insert(if inverted { quad.subject } else { quad.object });
        if cap.is_some_and(|cap| output.len() >= cap) {
            break;
        }
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn repeat<V: RdfReadView + ?Sized>(
    view: &V,
    context: &ReadContext<'_>,
    graph: TermId,
    focus: TermId,
    path: &ResolvedPath,
    inverted: bool,
    include_focus: bool,
    cap: Option<usize>,
    options: &ShaclValidationOptions,
    work: &mut PathWork,
) -> Result<BTreeSet<TermId>> {
    let mut visited = BTreeSet::from([focus]);
    let mut output = if include_focus {
        BTreeSet::from([focus])
    } else {
        BTreeSet::new()
    };
    let mut frontier = BTreeSet::from([focus]);
    let mut depth = 0usize;
    while !frontier.is_empty() && !cap.is_some_and(|cap| output.len() >= cap) {
        if depth >= options.max_path_depth {
            return Err(ShaclError::PathDepthExceeded {
                limit: options.max_path_depth,
            }
            .into());
        }
        depth += 1;
        let mut next = BTreeSet::new();
        for node in frontier {
            for value in walk(
                view, context, graph, node, path, inverted, None, options, work,
            )? {
                if visited.insert(value) {
                    next.insert(value);
                    output.insert(value);
                    if cap.is_some_and(|cap| output.len() >= cap) {
                        break;
                    }
                }
            }
            if cap.is_some_and(|cap| output.len() >= cap) {
                break;
            }
        }
        frontier = next;
    }
    Ok(output)
}
