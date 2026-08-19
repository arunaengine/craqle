use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::core::{EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use crate::query_context::ReadContext;
use crate::query_cursor::{QueryCursor, RawQuadCandidate, RawQuadCursor};
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
    fn scan<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        context.check_cancelled()?;
        let Some(pattern) = self.selected_pattern(selector, pattern) else {
            return Ok(QueryCursor::empty(self.base.store(), context, pattern));
        };
        // One durable range plus one bounded in-memory delta range, each
        // counted exactly once when opened.
        context.increment_index_seeks();
        if !self.index.is_empty() {
            context.increment_index_seeks();
        }
        let base = self.base.store().raw_quad_cursor(pattern);
        Ok(QueryCursor::delta(
            self.base.store(),
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
