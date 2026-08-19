use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;

use crate::core::{EncodedTerm, GraphId};
use crate::query_context::{GraphVisibility, ReadContext};
use crate::query_cursor::QueryCursor;
use crate::store::{EncodedQuad, GraphStore, Result, TermId};

/// A quad pattern represented entirely by internally interned term ids.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QuadPattern {
    pub(crate) graph: Option<TermId>,
    pub(crate) subject: Option<TermId>,
    pub(crate) predicate: Option<TermId>,
    pub(crate) object: Option<TermId>,
}

impl QuadPattern {
    pub(crate) fn matches(self, quad: EncodedQuad) -> bool {
        self.graph.is_none_or(|term| term == quad.graph)
            && self.subject.is_none_or(|term| term == quad.subject)
            && self.predicate.is_none_or(|term| term == quad.predicate)
            && self.object.is_none_or(|term| term == quad.object)
    }
}

/// The graph component of an RDF read.
#[derive(Clone, Copy, Debug)]
pub(crate) enum GraphSelector {
    Named(TermId),
    /// Visits visible named-graph copies. It intentionally preserves each
    /// source graph id in the returned [`EncodedQuad`]; distinct union grouping
    /// is a later SPARQL-layer responsibility, not a constant-memory cursor
    /// operation in this foundation.
    Union,
}

impl GraphSelector {
    fn apply(self, mut pattern: QuadPattern) -> Option<QuadPattern> {
        if let Self::Named(graph) = self {
            if pattern.graph.is_some_and(|expected| expected != graph) {
                return None;
            }
            pattern.graph = Some(graph);
        }
        Some(pattern)
    }
}

pub(crate) fn decode_term(
    store: &GraphStore,
    context: &ReadContext<'_>,
    term: TermId,
) -> Result<EncodedTerm> {
    context.increment_terms_decoded();
    store.decode_term(term)
}

pub(crate) fn graph_is_visible(
    store: &GraphStore,
    context: &ReadContext<'_>,
    graph: TermId,
) -> Result<bool> {
    if let Some(visible) = context.graph_visibility(graph) {
        return Ok(visible);
    }

    context.increment_graphs_considered();
    let visible = if !store.contains_graph_by_id(graph)? {
        false
    } else {
        match &context.visibility {
            GraphVisibility::All => true,
            GraphVisibility::Exact(graphs) => graphs.contains(&graph),
            GraphVisibility::Predicate(visible) => {
                let term = decode_term(store, context, graph)?;
                term.to_named_node()
                    .map(|named| visible(&GraphId(named)))
                    .unwrap_or(false)
            }
        }
    };
    context.remember_graph_visibility(graph, visible);
    Ok(visible)
}

fn orphaned_for_graph(
    store: &GraphStore,
    context: &ReadContext<'_>,
    graph: TermId,
) -> Result<Rc<HashSet<TermId>>> {
    if let Some(orphaned) = context.orphaned(graph) {
        return Ok(orphaned);
    }

    // `graph_diagnostics_by_id` is deliberately clock-aware and remains the
    // source for this read path; it recomputes stale records without persisting.
    let diagnostics = store.graph_diagnostics_by_id(graph)?;
    let mut orphaned = HashSet::with_capacity(diagnostics.orphaned_entities.len());
    for entity in diagnostics.orphaned_entities {
        if let Some(term) = store.lookup_term(&EncodedTerm::from_subject_id(&entity))? {
            orphaned.insert(term);
        }
    }
    let orphaned = Rc::new(orphaned);
    context.remember_orphaned(graph, orphaned.clone());
    Ok(orphaned)
}

pub(crate) fn quad_is_visible(
    store: &GraphStore,
    context: &ReadContext<'_>,
    quad: EncodedQuad,
) -> Result<bool> {
    if !graph_is_visible(store, context, quad.graph)? {
        return Ok(false);
    }
    let orphaned = orphaned_for_graph(store, context, quad.graph)?;
    Ok(!orphaned.contains(&quad.subject) && !orphaned.contains(&quad.object))
}
