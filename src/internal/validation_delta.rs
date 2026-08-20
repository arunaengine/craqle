use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use crate::query_context::ReadContext;
use crate::query_cursor::{CandidateStorage, QueryCursor, RawQuadCandidate, RawQuadCursor};
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::store::{EncodedQuad, GraphStore, Result, StoreError, TermId, hash_term};

/// The parts of a selected graph a change set can affect. It deliberately
/// excludes operations for every other graph before recording any impact.
#[derive(Debug, Default, Clone)]
pub(crate) struct DeltaImpact {
    pub(crate) changed_subjects: HashSet<TermId>,
    pub(crate) changed_objects: HashSet<TermId>,
    pub(crate) changed_predicates: HashSet<TermId>,
    /// `(subject, class)` pairs changed through `rdf:type` operations. The
    /// adapter does not classify vocabulary; downstream rule engines can.
    pub(crate) changed_types: HashSet<(TermId, TermId)>,
    pub(crate) has_deletions: bool,
}

impl DeltaImpact {
    fn record(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        inserted: bool,
        rdf_type: TermId,
    ) {
        self.changed_subjects.insert(subject);
        self.changed_objects.insert(object);
        self.changed_predicates.insert(predicate);
        if !inserted {
            self.has_deletions = true;
        }
        if predicate == rdf_type {
            self.changed_types.insert((subject, object));
        }
    }
}

/// An internal key so the delta map has deterministic iteration without
/// exposing term ids outside the crate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct QuadKey {
    graph: TermId,
    subject: TermId,
    predicate: TermId,
    object: TermId,
}

impl QuadKey {
    fn from_quad(quad: EncodedQuad) -> Self {
        Self {
            graph: quad.graph,
            subject: quad.subject,
            predicate: quad.predicate,
            object: quad.object,
        }
    }

    fn quad(self) -> EncodedQuad {
        EncodedQuad {
            graph: self.graph,
            subject: self.subject,
            predicate: self.predicate,
            object: self.object,
        }
    }
}

/// Last-change-wins state for one selected graph. Its term map is bounded by
/// the caller's delta (one graph id, fixed vocabulary, and at most three
/// terms per operation), and no term is interned while building it.
#[derive(Debug)]
pub(crate) struct DeltaIndex {
    graph: TermId,
    states: BTreeMap<QuadKey, bool>,
    /// The same bounded final states ordered for inverse predicate-object
    /// walks. The durable store has an equivalent copy-on-write index.
    by_predicate_object: BTreeMap<PredicateObjectKey, bool>,
    terms: HashMap<TermId, EncodedTerm>,
    impact: DeltaImpact,
}

impl DeltaIndex {
    pub(crate) fn build(
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
    ) -> Result<Self> {
        let mut index = Self {
            graph: TermId(0),
            states: BTreeMap::new(),
            by_predicate_object: BTreeMap::new(),
            terms: HashMap::new(),
            impact: DeltaImpact::default(),
        };
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        index.graph = index.remember_term(store, &graph_term)?;
        let rdf_type_term = EncodedTerm::from_named_node(&vocab::rdf_type());
        let rdf_type = index.remember_term(store, &rdf_type_term)?;

        for change in delta {
            let (change_graph, subject, predicate, object, present) = match change {
                MaterializedQuadChange::Insert {
                    graph,
                    subject,
                    predicate,
                    object,
                } => (graph, subject, predicate, object, true),
                MaterializedQuadChange::Delete {
                    graph,
                    subject,
                    predicate,
                    object,
                } => (graph, subject, predicate, object, false),
            };
            if change_graph != graph {
                continue;
            }
            let subject = index.remember_term(store, subject)?;
            let predicate = index.remember_term(store, predicate)?;
            let object = index.remember_term(store, object)?;
            let key = QuadKey {
                graph: index.graph,
                subject,
                predicate,
                object,
            };
            // Last writer wins regardless of whether the durable base had the
            // quad, so delete→insert is live and insert→delete is absent.
            index.states.insert(key, present);
            index
                .by_predicate_object
                .insert(PredicateObjectKey::from(key), present);
            index
                .impact
                .record(subject, predicate, object, present, rdf_type);
        }
        Ok(index)
    }

    pub(crate) fn graph(&self) -> TermId {
        self.graph
    }

    pub(crate) fn impact(&self) -> &DeltaImpact {
        &self.impact
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    fn state(&self, quad: EncodedQuad) -> Option<bool> {
        self.states.get(&QuadKey::from_quad(quad)).copied()
    }

    fn delta_term(&self, term: TermId) -> Option<&EncodedTerm> {
        self.terms.get(&term)
    }

    fn lookup_delta_term(&self, term: &EncodedTerm) -> Result<Option<TermId>> {
        let id = hash_term(term);
        match self.delta_term(id) {
            Some(existing) if existing == term => Ok(Some(id)),
            Some(existing) => Err(StoreError::TermCollision {
                attempted: term.0.clone(),
                existing: existing.0.clone(),
            }),
            None => Ok(None),
        }
    }

    fn remember_term(&mut self, store: &GraphStore, term: &EncodedTerm) -> Result<TermId> {
        let id = hash_term(term);
        if let Some(existing) = self.terms.get(&id) {
            if existing != term {
                return Err(StoreError::TermCollision {
                    attempted: term.0.clone(),
                    existing: existing.0.clone(),
                });
            }
        } else {
            self.terms.insert(id, term.clone());
        }

        // `lookup_term` compares bytes on hash agreement, which makes a
        // collision with a durable term explicit without mutating the store.
        let _ = store.lookup_term(term)?;
        Ok(id)
    }

    fn overlay(&self, pattern: QuadPattern) -> OverlayCursor<'_> {
        let minimum = TermId(0);
        let maximum = TermId(u128::MAX);
        if let Some(subject) = pattern.subject {
            let (lower, upper) = match (pattern.predicate, pattern.object) {
                (Some(predicate), Some(object)) => (
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate,
                        object,
                    },
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate,
                        object,
                    },
                ),
                (Some(predicate), None) => (
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate,
                        object: minimum,
                    },
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate,
                        object: maximum,
                    },
                ),
                (None, _) => (
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate: minimum,
                        object: minimum,
                    },
                    QuadKey {
                        graph: self.graph,
                        subject,
                        predicate: maximum,
                        object: maximum,
                    },
                ),
            };
            return OverlayCursor::Primary(self.states.range(lower..=upper));
        }
        if let Some(predicate) = pattern.predicate {
            let (lower, upper) = match pattern.object {
                Some(object) => (
                    PredicateObjectKey {
                        predicate,
                        object,
                        subject: minimum,
                    },
                    PredicateObjectKey {
                        predicate,
                        object,
                        subject: maximum,
                    },
                ),
                None => (
                    PredicateObjectKey {
                        predicate,
                        object: minimum,
                        subject: minimum,
                    },
                    PredicateObjectKey {
                        predicate,
                        object: maximum,
                        subject: maximum,
                    },
                ),
            };
            return OverlayCursor::PredicateObject {
                iterator: self.by_predicate_object.range(lower..=upper),
                graph: self.graph,
            };
        }
        OverlayCursor::Primary(self.states.range(..))
    }
}

/// A secondary ordering for the delta's bounded inverse walk. `graph` is
/// omitted because one [`DeltaIndex`] always belongs to one graph.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PredicateObjectKey {
    predicate: TermId,
    object: TermId,
    subject: TermId,
}

impl From<QuadKey> for PredicateObjectKey {
    fn from(quad: QuadKey) -> Self {
        Self {
            predicate: quad.predicate,
            object: quad.object,
            subject: quad.subject,
        }
    }
}

enum OverlayCursor<'delta> {
    Primary(btree_map::Range<'delta, QuadKey, bool>),
    PredicateObject {
        iterator: btree_map::Range<'delta, PredicateObjectKey, bool>,
        graph: TermId,
    },
}

impl OverlayCursor<'_> {
    fn next(&mut self) -> Option<(QuadKey, bool)> {
        match self {
            Self::Primary(iterator) => iterator.next().map(|(key, present)| (*key, *present)),
            Self::PredicateObject { iterator, graph } => iterator.next().map(|(key, present)| {
                (
                    QuadKey {
                        graph: *graph,
                        subject: key.subject,
                        predicate: key.predicate,
                        object: key.object,
                    },
                    *present,
                )
            }),
        }
    }
}

/// The two-source state machine behind a delta read. It never copies the base
/// graph: only base rows touched by a final live delta state are remembered,
/// so the seen set is bounded by the delta.
pub(crate) struct DeltaQuadCursor<'delta> {
    base: Option<RawQuadCursor>,
    overlay: OverlayCursor<'delta>,
    index: &'delta DeltaIndex,
    base_live_delta_rows: HashSet<QuadKey>,
}

impl<'delta> DeltaQuadCursor<'delta> {
    pub(crate) fn new(
        base: RawQuadCursor,
        index: &'delta DeltaIndex,
        pattern: QuadPattern,
    ) -> Self {
        Self {
            base: Some(base),
            overlay: index.overlay(pattern),
            index,
            base_live_delta_rows: HashSet::new(),
        }
    }

    pub(crate) fn next_candidate(&mut self) -> Option<Result<RawQuadCandidate>> {
        if let Some(base) = self.base.as_mut() {
            match base.next_candidate() {
                Some(Ok(mut candidate)) => {
                    if candidate.live {
                        match self.index.state(candidate.quad) {
                            Some(false) => candidate.live = false,
                            Some(true) => {
                                self.base_live_delta_rows
                                    .insert(QuadKey::from_quad(candidate.quad));
                            }
                            None => {}
                        }
                    }
                    return Some(Ok(candidate));
                }
                Some(Err(error)) => return Some(Err(error)),
                None => self.base = None,
            }
        }

        let (key, present) = self.overlay.next()?;
        Some(Ok(RawQuadCandidate {
            quad: key.quad(),
            // A deletion or a row already emitted from the base remains a
            // candidate for exact accounting, never a matching row.
            live: present && !self.base_live_delta_rows.contains(&key),
            storage: CandidateStorage::Delta,
            bytes_read: 0,
        }))
    }
}

/// Post-change RDF view layered over the durable read view. The base remains
/// authoritative; this adapter only supplies the candidate write's final
/// last-change-wins overlay.
pub(crate) struct DeltaReadView<'store, 'delta> {
    base: StoreReadView<'store>,
    index: &'delta DeltaIndex,
}

impl<'store, 'delta> DeltaReadView<'store, 'delta> {
    pub(crate) fn new(base: StoreReadView<'store>, index: &'delta DeltaIndex) -> Self {
        Self { base, index }
    }

    pub(crate) fn graph(&self) -> TermId {
        self.index.graph()
    }

    /// Whether the durable pre-state had any row for `subject`. Rules use this
    /// only to preserve their existing "newly introduced untyped subject"
    /// scope; all normal candidate reads use the final delta view.
    pub(crate) fn base_subject_exists(
        &self,
        context: &ReadContext<'_>,
        subject: TermId,
    ) -> Result<bool> {
        self.base.exists(
            context,
            GraphSelector::Named(self.graph()),
            QuadPattern {
                subject: Some(subject),
                ..QuadPattern::default()
            },
        )
    }

    fn selected_pattern(
        &self,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Option<QuadPattern> {
        let mut pattern = selector.apply(pattern)?;
        if pattern.graph.is_some_and(|graph| graph != self.graph()) {
            return None;
        }
        pattern.graph = Some(self.graph());
        Some(pattern)
    }
}

impl RdfReadView for DeltaReadView<'_, '_> {
    fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        self.base.contains_graph(graph)
    }

    fn scan<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        context.check_cancelled()?;
        let Some(pattern) = self.selected_pattern(selector, pattern) else {
            return Ok(QueryCursor::empty(
                self.base.store(),
                self.base.snapshot(),
                context,
                pattern,
            ));
        };
        // One durable range plus one bounded in-memory delta range, each
        // counted exactly once when opened.
        context.increment_index_seeks();
        if !self.index.is_empty() {
            context.increment_index_seeks();
        }
        let base = self
            .base
            .snapshot()
            .raw_quad_cursor(self.base.store(), pattern);
        Ok(QueryCursor::delta(
            self.base.store(),
            self.base.snapshot(),
            context,
            DeltaQuadCursor::new(base, self.index, pattern),
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
        if matches!(selector, GraphSelector::Named(_)) {
            let Some(pattern) = self.selected_pattern(selector, pattern) else {
                return Ok(false);
            };
            if let (Some(graph), Some(subject), Some(predicate), Some(object)) = (
                pattern.graph,
                pattern.subject,
                pattern.predicate,
                pattern.object,
            ) {
                let quad = EncodedQuad {
                    graph,
                    subject,
                    predicate,
                    object,
                };
                if let Some(present) = self.index.state(quad) {
                    // A final delta state is an exact in-memory probe. It has
                    // the same accounting shape as the durable point path.
                    context.increment_index_seeks();
                    context.increment_candidate_quads();
                    if !present {
                        return Ok(false);
                    }
                    if self.base.quad_is_visible(context, quad)? {
                        context.increment_matching_quads();
                        return Ok(true);
                    }
                    return Ok(false);
                }
                // No overlay verdict: retain StoreReadView's exact durable
                // point probe instead of widening to a graph-subject scan.
                return self.base.exists(context, selector, pattern);
            }
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
        match self.index.lookup_delta_term(term)? {
            Some(term) => Ok(Some(term)),
            None => self.base.lookup_term(context, term),
        }
    }

    fn decode_term(&self, context: &ReadContext<'_>, term: TermId) -> Result<EncodedTerm> {
        context.check_cancelled()?;
        if let Some(decoded) = self.index.delta_term(term) {
            context.increment_terms_decoded();
            return Ok(decoded.clone());
        }
        self.base.decode_term(context, term)
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
        self.base.graph_is_visible(context, graph)
    }

    fn quad_is_visible(&self, context: &ReadContext<'_>, quad: EncodedQuad) -> Result<bool> {
        self.base.quad_is_visible(context, quad)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{ActorId, Dot};
    use crate::query_context::QueryCancellation;
    use crate::store::{ClockUpdate, CounterKey, QuadAdd};

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

    fn insert(
        graph: &GraphId,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> MaterializedQuadChange {
        MaterializedQuadChange::Insert {
            graph: graph.clone(),
            subject: named(subject),
            predicate: named(predicate),
            object: named(object),
        }
    }

    fn delete(
        graph: &GraphId,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> MaterializedQuadChange {
        MaterializedQuadChange::Delete {
            graph: graph.clone(),
            subject: named(subject),
            predicate: named(predicate),
            object: named(object),
        }
    }

    fn rows(view: &DeltaReadView<'_, '_>, context: &ReadContext<'_>) -> Vec<EncodedQuad> {
        view.scan(
            context,
            GraphSelector::Named(view.graph()),
            QuadPattern::default(),
        )
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap()
    }

    fn ids(
        view: &DeltaReadView<'_, '_>,
        context: &ReadContext<'_>,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> (TermId, TermId, TermId) {
        (
            view.lookup_term(context, &named(subject)).unwrap().unwrap(),
            view.lookup_term(context, &named(predicate))
                .unwrap()
                .unwrap(),
            view.lookup_term(context, &named(object)).unwrap().unwrap(),
        )
    }

    #[test]
    fn delta_only_insert_is_visible_without_store_mutation() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:delta-only");
        let changes = vec![insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o")];
        assert!(store.lookup_term(&named("urn:test:s")).unwrap().is_none());
        assert!(!store.contains_graph(&graph).unwrap());

        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let (subject, predicate, object) =
            ids(&view, &context, "urn:test:s", "urn:test:p", "urn:test:o");

        assert_eq!(
            rows(&view, &context),
            vec![EncodedQuad {
                graph: view.graph(),
                subject,
                predicate,
                object,
            }]
        );
        assert!(store.lookup_term(&named("urn:test:s")).unwrap().is_none());
        assert!(store.lookup_term(&named("urn:test:p")).unwrap().is_none());
        assert!(store.lookup_term(&named("urn:test:o")).unwrap().is_none());
        assert!(!store.contains_graph(&graph).unwrap());
    }

    #[test]
    fn deletes_and_reordered_changes_have_base_aware_final_state() {
        for base_present in [false, true] {
            let (_directory, store) = setup_store();
            let graph = GraphId::new(if base_present {
                "urn:test:lww:base"
            } else {
                "urn:test:lww:empty"
            });
            if base_present {
                add_quad(&store, &graph, "urn:test:s", "urn:test:p", "urn:test:o");
            }

            let delete_insert = vec![
                delete(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
                insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
            ];
            let index = DeltaIndex::build(&store, &graph, &delete_insert).unwrap();
            let view = DeltaReadView::new(StoreReadView::new(&store), &index);
            let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
            let (subject, predicate, object) =
                ids(&view, &context, "urn:test:s", "urn:test:p", "urn:test:o");
            assert!(
                view.exists(
                    &context,
                    GraphSelector::Named(view.graph()),
                    QuadPattern {
                        subject: Some(subject),
                        predicate: Some(predicate),
                        object: Some(object),
                        ..QuadPattern::default()
                    },
                )
                .unwrap()
            );
            assert_eq!(1, context.snapshot().index_seeks);
            assert_eq!(1, context.snapshot().candidate_quads);
            assert_eq!(1, context.snapshot().matching_quads);

            let insert_delete = vec![
                insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
                delete(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
            ];
            let index = DeltaIndex::build(&store, &graph, &insert_delete).unwrap();
            let view = DeltaReadView::new(StoreReadView::new(&store), &index);
            let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
            let (subject, predicate, object) =
                ids(&view, &context, "urn:test:s", "urn:test:p", "urn:test:o");
            assert!(
                !view
                    .exists(
                        &context,
                        GraphSelector::Named(view.graph()),
                        QuadPattern {
                            subject: Some(subject),
                            predicate: Some(predicate),
                            object: Some(object),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap()
            );
            assert_eq!(1, context.snapshot().index_seeks);
            assert_eq!(1, context.snapshot().candidate_quads);
            assert_eq!(0, context.snapshot().matching_quads);
        }
    }

    #[test]
    fn repeated_operations_never_duplicate_or_underflow() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:repeated");
        let inserts = vec![
            insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
            insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
        ];
        let index = DeltaIndex::build(&store, &graph, &inserts).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert_eq!(1, rows(&view, &context).len());

        add_quad(&store, &graph, "urn:test:s", "urn:test:p", "urn:test:o");
        let deletes = vec![
            delete(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
            delete(&graph, "urn:test:s", "urn:test:p", "urn:test:o"),
        ];
        let index = DeltaIndex::build(&store, &graph, &deletes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert!(rows(&view, &context).is_empty());
    }

    #[test]
    fn base_present_final_insert_is_emitted_once() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:base-present");
        add_quad(&store, &graph, "urn:test:s", "urn:test:p", "urn:test:o");
        let index = DeltaIndex::build(
            &store,
            &graph,
            &[insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o")],
        )
        .unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert_eq!(1, rows(&view, &context).len());
        assert_eq!(1, context.snapshot().matching_quads);
    }

    #[test]
    fn foreign_graph_changes_do_not_affect_the_selected_view_or_impact() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:selected");
        let foreign = GraphId::new("urn:test:foreign");
        let changes = vec![delete(&foreign, "urn:test:s", "urn:test:p", "urn:test:o")];
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        assert!(index.is_empty());
        assert!(index.impact().changed_subjects.is_empty());
        assert!(!index.impact().has_deletions);
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert!(rows(&view, &context).is_empty());
    }

    #[test]
    fn impact_is_id_based_and_limited_to_the_selected_graph() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:impact");
        let foreign = GraphId::new("urn:test:impact:foreign");
        let changes = vec![
            insert(
                &graph,
                "urn:test:impact:subject",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "urn:test:impact:type",
            ),
            delete(
                &graph,
                "urn:test:impact:subject",
                "urn:test:impact:predicate",
                "urn:test:impact:object",
            ),
            insert(
                &foreign,
                "urn:test:impact:foreign-subject",
                "urn:test:impact:foreign-predicate",
                "urn:test:impact:foreign-object",
            ),
        ];
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let impact = index.impact();
        let subject = hash_term(&named("urn:test:impact:subject"));
        let class = hash_term(&named("urn:test:impact:type"));
        assert_eq!(HashSet::from([subject]), impact.changed_subjects);
        assert!(
            impact
                .changed_objects
                .contains(&hash_term(&named("urn:test:impact:object")))
        );
        assert!(
            impact
                .changed_predicates
                .contains(&hash_term(&named("urn:test:impact:predicate")))
        );
        assert_eq!(HashSet::from([(subject, class)]), impact.changed_types);
        assert!(impact.has_deletions);
    }

    #[test]
    fn delta_terms_decode_and_collisions_fail_explicitly() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:delta-terms");
        let changes = vec![insert(&graph, "urn:test:s", "urn:test:p", "urn:test:o")];
        let mut index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let subject = named("urn:test:s");
        let subject_id = view.lookup_term(&context, &subject).unwrap().unwrap();
        assert_eq!(subject, view.decode_term(&context, subject_id).unwrap());
        assert_eq!(1, context.snapshot().terms_decoded);
        drop(view);

        let attempted = named("urn:test:collision:attempted");
        index
            .terms
            .insert(hash_term(&attempted), named("urn:test:collision:existing"));
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert!(matches!(
            view.lookup_term(&context, &attempted),
            Err(StoreError::TermCollision { .. })
        ));
    }

    #[test]
    fn exists_and_bounded_count_stop_after_their_cap() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:early-stop");
        for index in 0..3 {
            add_quad(
                &store,
                &graph,
                &format!("urn:test:s{index}"),
                "urn:test:p",
                &format!("urn:test:o{index}"),
            );
        }
        let changes = vec![insert(
            &graph,
            "urn:test:overlay",
            "urn:test:q",
            "urn:test:r",
        )];
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);

        let point = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let (subject, predicate, object) =
            ids(&view, &point, "urn:test:s0", "urn:test:p", "urn:test:o0");
        assert!(
            view.exists(
                &point,
                GraphSelector::Named(view.graph()),
                QuadPattern {
                    subject: Some(subject),
                    predicate: Some(predicate),
                    object: Some(object),
                    ..QuadPattern::default()
                },
            )
            .unwrap()
        );
        assert_eq!(1, point.snapshot().index_seeks);
        assert_eq!(1, point.snapshot().candidate_quads);
        assert_eq!(1, point.snapshot().matching_quads);

        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert!(
            view.exists(
                &context,
                GraphSelector::Named(view.graph()),
                QuadPattern::default(),
            )
            .unwrap()
        );
        assert_eq!(1, context.snapshot().candidate_quads);
        assert_eq!(1, context.snapshot().matching_quads);
        assert_eq!(2, context.snapshot().index_seeks);

        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert_eq!(
            2,
            view.count_up_to(
                &context,
                GraphSelector::Named(view.graph()),
                QuadPattern::default(),
                2,
            )
            .unwrap()
        );
        assert_eq!(2, context.snapshot().candidate_quads);
        assert_eq!(2, context.snapshot().matching_quads);
        assert_eq!(2, context.snapshot().index_seeks);

        let zero = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert_eq!(
            0,
            view.count_up_to(
                &zero,
                GraphSelector::Named(view.graph()),
                QuadPattern::default(),
                0,
            )
            .unwrap()
        );
        assert_eq!(0, zero.snapshot().index_seeks);
        assert_eq!(0, zero.snapshot().candidate_quads);
    }

    #[test]
    fn overlay_ranges_stop_after_two_matching_count_forward_and_inverse_rows() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:overlay-ranges");
        let forward_subject = "urn:test:forward:subject";
        let forward_predicate = "urn:test:forward:predicate";
        let inverse_predicate = "urn:test:inverse:predicate";
        let inverse_object = "urn:test:inverse:object";
        let mut changes: Vec<_> = (0..128)
            .map(|index| {
                insert(
                    &graph,
                    &format!("urn:test:unrelated:subject:{index}"),
                    "urn:test:unrelated:predicate",
                    &format!("urn:test:unrelated:object:{index}"),
                )
            })
            .collect();
        for index in 0..3 {
            changes.push(insert(
                &graph,
                forward_subject,
                forward_predicate,
                &format!("urn:test:forward:object:{index}"),
            ));
            changes.push(insert(
                &graph,
                &format!("urn:test:inverse:subject:{index}"),
                inverse_predicate,
                inverse_object,
            ));
        }
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let lookup = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let forward_subject = view
            .lookup_term(&lookup, &named(forward_subject))
            .unwrap()
            .unwrap();
        let forward_predicate = view
            .lookup_term(&lookup, &named(forward_predicate))
            .unwrap()
            .unwrap();
        let inverse_predicate = view
            .lookup_term(&lookup, &named(inverse_predicate))
            .unwrap()
            .unwrap();
        let inverse_object = view
            .lookup_term(&lookup, &named(inverse_object))
            .unwrap()
            .unwrap();

        let count = ReadContext::for_validation(QueryCancellation::new(), &graph);
        assert_eq!(
            2,
            view.count_up_to(
                &count,
                GraphSelector::Named(view.graph()),
                QuadPattern {
                    subject: Some(forward_subject),
                    ..QuadPattern::default()
                },
                2,
            )
            .unwrap()
        );
        assert_eq!(2, count.snapshot().candidate_quads);
        assert_eq!(2, count.snapshot().matching_quads);

        let forward = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let mut cursor = view
            .forward_predicate(
                &forward,
                GraphSelector::Named(view.graph()),
                forward_subject,
                forward_predicate,
            )
            .unwrap();
        assert!(cursor.next().unwrap().is_ok());
        assert!(cursor.next().unwrap().is_ok());
        assert_eq!(2, forward.snapshot().candidate_quads);
        assert_eq!(2, forward.snapshot().matching_quads);

        let inverse = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let mut cursor = view
            .inverse_predicate(
                &inverse,
                GraphSelector::Named(view.graph()),
                inverse_predicate,
                inverse_object,
            )
            .unwrap();
        assert!(cursor.next().unwrap().is_ok());
        assert!(cursor.next().unwrap().is_ok());
        assert_eq!(2, inverse.snapshot().candidate_quads);
        assert_eq!(2, inverse.snapshot().matching_quads);
        assert!(changes.len() > 2);
    }

    #[test]
    fn cancellation_stops_a_large_overlay_scan() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:overlay-cancellation");
        let changes: Vec<_> = (0..1_025)
            .map(|index| {
                insert(
                    &graph,
                    &format!("urn:test:s{index}"),
                    "urn:test:p",
                    &format!("urn:test:o{index}"),
                )
            })
            .collect();
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let cancellation = QueryCancellation::new();
        let context = ReadContext::for_validation(cancellation.clone(), &graph);
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Named(view.graph()),
                QuadPattern::default(),
            )
            .unwrap();
        for _ in 0..1_024 {
            assert!(cursor.next().unwrap().is_ok());
        }
        cancellation.cancel();
        assert!(matches!(cursor.next(), Some(Err(StoreError::Cancelled))));
        assert_eq!(1_024, context.snapshot().candidate_quads);
        assert!(cursor.next().is_none());
    }

    #[test]
    fn final_forward_and_inverse_walks_include_the_overlay() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:walks");
        add_quad(
            &store,
            &graph,
            "urn:test:root",
            "urn:test:hasPart",
            "urn:test:old",
        );
        let changes = vec![
            delete(&graph, "urn:test:root", "urn:test:hasPart", "urn:test:old"),
            insert(&graph, "urn:test:root", "urn:test:hasPart", "urn:test:new"),
        ];
        let index = DeltaIndex::build(&store, &graph, &changes).unwrap();
        let view = DeltaReadView::new(StoreReadView::new(&store), &index);
        let context = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let (root, predicate, new) = ids(
            &view,
            &context,
            "urn:test:root",
            "urn:test:hasPart",
            "urn:test:new",
        );
        let forward: Vec<_> = view
            .forward_predicate(
                &context,
                GraphSelector::Named(view.graph()),
                root,
                predicate,
            )
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(1, forward.len());
        assert_eq!(new, forward[0].object);

        let inverse: Vec<_> = view
            .inverse_predicate(&context, GraphSelector::Named(view.graph()), predicate, new)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(1, inverse.len());
        assert_eq!(root, inverse[0].subject);
    }

    #[test]
    fn validation_visibility_includes_orphans_without_weakening_normal_reads() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:validation-orphan");
        let orphan = add_quad(
            &store,
            &graph,
            "urn:test:orphan",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://schema.org/MediaObject",
        );
        assert!(store.graph_diagnostics(&graph).unwrap().has_orphans());
        let view = StoreReadView::new(&store);

        let normal = ReadContext::default();
        let normal_rows: Vec<_> = view
            .scan(
                &normal,
                GraphSelector::Named(orphan.graph),
                QuadPattern::default(),
            )
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert!(normal_rows.is_empty());

        let validation = ReadContext::for_validation(QueryCancellation::new(), &graph);
        let validation_rows: Vec<_> = view
            .scan(
                &validation,
                GraphSelector::Named(orphan.graph),
                QuadPattern::default(),
            )
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(vec![orphan], validation_rows);

        let proposed_graph = GraphId::new("urn:test:proposed-orphan");
        let index = DeltaIndex::build(
            &store,
            &proposed_graph,
            &[insert(
                &proposed_graph,
                "urn:test:proposed-orphan",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://schema.org/MediaObject",
            )],
        )
        .unwrap();
        let proposed = DeltaReadView::new(StoreReadView::new(&store), &index);
        let validation = ReadContext::for_validation(QueryCancellation::new(), &proposed_graph);
        assert_eq!(1, rows(&proposed, &validation).len());
    }
}
