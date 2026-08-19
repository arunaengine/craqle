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

/// The shared behavior surface for durable RDF reads.
pub(crate) trait RdfReadView {
    fn scan<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>>;

    fn exists(
        &self,
        context: &ReadContext<'_>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<bool>;

    fn count_up_to(
        &self,
        context: &ReadContext<'_>,
        selector: GraphSelector,
        pattern: QuadPattern,
        cap: u64,
    ) -> Result<u64>;

    fn forward_predicate<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        subject: TermId,
        predicate: TermId,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>>;

    fn inverse_predicate<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        predicate: TermId,
        object: TermId,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>>;

    fn lookup_term(&self, context: &ReadContext<'_>, term: &EncodedTerm) -> Result<Option<TermId>>;

    fn decode_term(&self, context: &ReadContext<'_>, term: TermId) -> Result<EncodedTerm>;

    fn terms_equal(&self, context: &ReadContext<'_>, left: TermId, right: TermId) -> Result<bool>;

    fn compare_terms(
        &self,
        context: &ReadContext<'_>,
        left: TermId,
        right: TermId,
    ) -> Result<Ordering>;

    fn graph_is_visible(&self, context: &ReadContext<'_>, graph: TermId) -> Result<bool>;

    fn quad_is_visible(&self, context: &ReadContext<'_>, quad: EncodedQuad) -> Result<bool>;
}

/// Durable-source implementation of [`RdfReadView`].
pub(crate) struct StoreReadView<'store> {
    store: &'store GraphStore,
}

impl<'store> StoreReadView<'store> {
    pub(crate) fn new(store: &'store GraphStore) -> Self {
        Self { store }
    }
}

impl RdfReadView for StoreReadView<'_> {
    fn scan<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        context.check_cancelled()?;
        let Some(pattern) = selector.apply(pattern) else {
            return Ok(QueryCursor::empty(self.store, context, pattern));
        };
        context.increment_index_seeks();
        Ok(QueryCursor::new(
            self.store,
            context,
            self.store.raw_quad_cursor(pattern),
            pattern,
        ))
    }

    fn exists(
        &self,
        context: &ReadContext<'_>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<bool> {
        context.check_cancelled()?;
        let Some(pattern) = selector.apply(pattern) else {
            return Ok(false);
        };
        if let (GraphSelector::Named(graph), Some(subject), Some(predicate), Some(object)) =
            (selector, pattern.subject, pattern.predicate, pattern.object)
        {
            let quad = EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            };
            context.increment_index_seeks();
            let Some(candidate) = self.store.raw_quad_point(quad)? else {
                return Ok(false);
            };
            context.increment_candidate_quads();
            if !candidate.live || !pattern.matches(candidate.quad) {
                return Ok(false);
            }
            if quad_is_visible(self.store, context, candidate.quad)? {
                context.increment_matching_quads();
                return Ok(true);
            }
            return Ok(false);
        }

        let mut cursor = self.scan(context, selector, pattern)?;
        match cursor.next() {
            Some(Ok(_)) => Ok(true),
            Some(Err(error)) => Err(error),
            None => Ok(false),
        }
    }

    fn count_up_to(
        &self,
        context: &ReadContext<'_>,
        selector: GraphSelector,
        pattern: QuadPattern,
        cap: u64,
    ) -> Result<u64> {
        if cap == 0 {
            return Ok(0);
        }
        let mut cursor = self.scan(context, selector, pattern)?;
        let mut count = 0;
        while count < cap {
            match cursor.next() {
                Some(Ok(_)) => count += 1,
                Some(Err(error)) => return Err(error),
                None => break,
            }
        }
        Ok(count)
    }

    fn forward_predicate<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        subject: TermId,
        predicate: TermId,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        self.scan(
            context,
            selector,
            QuadPattern {
                subject: Some(subject),
                predicate: Some(predicate),
                ..QuadPattern::default()
            },
        )
    }

    fn inverse_predicate<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        predicate: TermId,
        object: TermId,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        self.scan(
            context,
            selector,
            QuadPattern {
                predicate: Some(predicate),
                object: Some(object),
                ..QuadPattern::default()
            },
        )
    }

    fn lookup_term(&self, context: &ReadContext<'_>, term: &EncodedTerm) -> Result<Option<TermId>> {
        context.check_cancelled()?;
        self.store.lookup_term(term)
    }

    fn decode_term(&self, context: &ReadContext<'_>, term: TermId) -> Result<EncodedTerm> {
        context.check_cancelled()?;
        decode_term(self.store, context, term)
    }

    fn terms_equal(&self, context: &ReadContext<'_>, left: TermId, right: TermId) -> Result<bool> {
        context.check_cancelled()?;
        Ok(left == right)
    }

    fn compare_terms(
        &self,
        context: &ReadContext<'_>,
        left: TermId,
        right: TermId,
    ) -> Result<Ordering> {
        Ok(self
            .decode_term(context, left)?
            .0
            .cmp(&self.decode_term(context, right)?.0))
    }

    fn graph_is_visible(&self, context: &ReadContext<'_>, graph: TermId) -> Result<bool> {
        context.check_cancelled()?;
        graph_is_visible(self.store, context, graph)
    }

    fn quad_is_visible(&self, context: &ReadContext<'_>, quad: EncodedQuad) -> Result<bool> {
        context.check_cancelled()?;
        quad_is_visible(self.store, context, quad)
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
