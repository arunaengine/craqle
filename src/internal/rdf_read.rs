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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::core::{ActorId, Dot};
    use crate::query_context::{QueryCancellation, ReadContext};
    use crate::store::{ClockUpdate, CounterKey, QuadAdd, StoreError};

    use super::*;

    fn named(iri: &str) -> EncodedTerm {
        EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(iri))
    }

    fn setup_store() -> (tempfile::TempDir, GraphStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = GraphStore::open(directory.path()).unwrap();
        (directory, store)
    }

    fn add_quad(
        store: &GraphStore,
        graph: &GraphId,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> EncodedQuad {
        if !store.contains_graph(graph).unwrap() {
            store.create_graph(graph).unwrap();
        }
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let quad = EncodedQuad {
            graph: graph_id,
            subject: store.resolve_term(&named(subject)).unwrap(),
            predicate: store.resolve_term(&named(predicate)).unwrap(),
            object: store.resolve_term(&named(object)).unwrap(),
        };
        let _guard = store.graph_commit_guard(graph);
        let actor = ActorId::random();
        let mut batch = store.new_batch();
        let counter = store
            .next_counter(&mut batch, CounterKey { graph_id, actor })
            .unwrap();
        assert!(
            store
                .insert_quad(
                    &mut batch,
                    QuadAdd {
                        quad,
                        dot: Dot { actor, counter },
                    },
                )
                .unwrap()
        );
        let mut clock = store.get_vector_clock_by_id(graph_id).unwrap();
        clock.advance(actor, counter);
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id,
                    clock: &clock,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        quad
    }

    fn add_many(store: &GraphStore, graph: &GraphId, count: usize) -> TermId {
        store.create_graph(graph).unwrap();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        let predicate = store.resolve_term(&named("urn:test:read:p")).unwrap();
        let _guard = store.graph_commit_guard(graph);
        let actor = ActorId::random();
        let mut batch = store.new_batch();
        let mut clock = store.get_vector_clock_by_id(graph_id).unwrap();
        for index in 0..count {
            let subject = store
                .resolve_term(&named(&format!("urn:test:read:s{index}")))
                .unwrap();
            let object = store
                .resolve_term(&named(&format!("urn:test:read:o{index}")))
                .unwrap();
            let counter = store
                .next_counter(&mut batch, CounterKey { graph_id, actor })
                .unwrap();
            assert!(
                store
                    .insert_quad(
                        &mut batch,
                        QuadAdd {
                            quad: EncodedQuad {
                                graph: graph_id,
                                subject,
                                predicate,
                                object,
                            },
                            dot: Dot { actor, counter },
                        },
                    )
                    .unwrap()
            );
            clock.advance(actor, counter);
        }
        store
            .set_vector_clock(
                &mut batch,
                ClockUpdate {
                    graph_id,
                    clock: &clock,
                },
            )
            .unwrap();
        store.commit(batch).unwrap();
        graph_id
    }

    fn collect_rows(
        cursor: impl Iterator<Item = crate::store::Result<EncodedQuad>>,
    ) -> Vec<EncodedQuad> {
        cursor.collect::<crate::store::Result<Vec<_>>>().unwrap()
    }

    fn raw_rows(store: &GraphStore, pattern: QuadPattern) -> Vec<EncodedQuad> {
        let mut cursor = store.raw_quad_cursor(pattern);
        let mut rows = Vec::new();
        while let Some(candidate) = cursor.next_candidate() {
            let candidate = candidate.unwrap();
            if candidate.live && pattern.matches(candidate.quad) {
                rows.push(candidate.quad);
            }
        }
        rows
    }

    fn sorted(mut rows: Vec<EncodedQuad>) -> Vec<EncodedQuad> {
        rows.sort_by_key(|quad| (quad.graph, quad.subject, quad.predicate, quad.object));
        rows
    }

    fn spin_until(entered: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !entered() {
            assert!(
                std::time::Instant::now() < deadline,
                "the commit stall was never entered"
            );
            std::hint::spin_loop();
            std::thread::yield_now();
        }
    }

    #[test]
    fn raw_cursor_matches_vector_wrapper_for_every_binding_shape() {
        let (_directory, store) = setup_store();
        let first_graph = GraphId::new("urn:test:raw:first");
        let second_graph = GraphId::new("urn:test:raw:second");
        let first = add_quad(
            &store,
            &first_graph,
            "urn:test:raw:s1",
            "urn:test:raw:p1",
            "urn:test:raw:o1",
        );
        add_quad(
            &store,
            &first_graph,
            "urn:test:raw:s1",
            "urn:test:raw:p2",
            "urn:test:raw:o2",
        );
        add_quad(
            &store,
            &second_graph,
            "urn:test:raw:s2",
            "urn:test:raw:p1",
            "urn:test:raw:o1",
        );

        for bindings in 0..16 {
            let pattern = QuadPattern {
                graph: (bindings & 1 != 0).then_some(first.graph),
                subject: (bindings & 2 != 0).then_some(first.subject),
                predicate: (bindings & 4 != 0).then_some(first.predicate),
                object: (bindings & 8 != 0).then_some(first.object),
            };
            assert_eq!(
                sorted(raw_rows(&store, pattern)),
                sorted(
                    store
                        .quads_for_pattern(
                            pattern.graph,
                            pattern.subject,
                            pattern.predicate,
                            pattern.object,
                        )
                        .unwrap()
                ),
                "binding shape {bindings:04b}"
            );
        }
    }

    #[test]
    fn cursor_stops_after_the_first_consumed_row() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:early-stop");
        let first = add_quad(
            &store,
            &graph,
            "urn:test:early:s1",
            "urn:test:early:p",
            "urn:test:early:o1",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:early:s2",
            "urn:test:early:p",
            "urn:test:early:o2",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:early:s3",
            "urn:test:early:p",
            "urn:test:early:o3",
        );

        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern::default(),
            )
            .unwrap();
        assert!(cursor.next().unwrap().is_ok());
        drop(cursor);
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(1, context.snapshot().candidate_quads);
        assert_eq!(1, context.snapshot().matching_quads);
    }

    #[test]
    fn exists_uses_one_point_candidate_and_stops_on_a_hit() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:exists");
        let quad = add_quad(
            &store,
            &graph,
            "urn:test:exists:s",
            "urn:test:exists:p",
            "urn:test:exists:o",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:exists:s2",
            "urn:test:exists:p",
            "urn:test:exists:o2",
        );
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let pattern = QuadPattern {
            subject: Some(quad.subject),
            predicate: Some(quad.predicate),
            object: Some(quad.object),
            ..QuadPattern::default()
        };

        assert!(
            view.exists(&context, GraphSelector::Named(quad.graph), pattern)
                .unwrap()
        );
        let statistics = context.snapshot();
        assert_eq!(1, statistics.index_seeks);
        assert_eq!(1, statistics.candidate_quads);
        assert_eq!(1, statistics.matching_quads);

        let missing = store
            .resolve_term(&named("urn:test:exists:missing"))
            .unwrap();
        let context = ReadContext::default();
        assert!(
            !view
                .exists(
                    &context,
                    GraphSelector::Named(quad.graph),
                    QuadPattern {
                        subject: Some(quad.subject),
                        predicate: Some(quad.predicate),
                        object: Some(missing),
                        ..QuadPattern::default()
                    },
                )
                .unwrap()
        );
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(0, context.snapshot().candidate_quads);
    }

    #[test]
    fn count_up_to_zero_and_two_stop_at_the_requested_cap() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:count-up-to");
        let quad = add_quad(
            &store,
            &graph,
            "urn:test:count:s1",
            "urn:test:count:p",
            "urn:test:count:o1",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:count:s2",
            "urn:test:count:p",
            "urn:test:count:o2",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:count:s3",
            "urn:test:count:p",
            "urn:test:count:o3",
        );
        let view = StoreReadView::new(&store);

        let zero = ReadContext::default();
        assert_eq!(
            0,
            view.count_up_to(
                &zero,
                GraphSelector::Named(quad.graph),
                QuadPattern::default(),
                0,
            )
            .unwrap()
        );
        assert_eq!(0, zero.snapshot().index_seeks);
        assert_eq!(0, zero.snapshot().candidate_quads);

        let two = ReadContext::default();
        assert_eq!(
            2,
            view.count_up_to(
                &two,
                GraphSelector::Named(quad.graph),
                QuadPattern::default(),
                2,
            )
            .unwrap()
        );
        assert_eq!(1, two.snapshot().index_seeks);
        assert_eq!(2, two.snapshot().candidate_quads);
        assert_eq!(2, two.snapshot().matching_quads);
    }

}
