use std::cell::OnceCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use crate::core::{EncodedTerm, GraphId};
use crate::query_context::{GraphVisibility, QueryReadMode, ReadAccessPath, ReadContext};
use crate::query_cursor::QueryCursor;
use crate::store::{
    EncodedQuad, GraphStore, QueryIndexAdmission, QueryIndexCursorOrder, Result, StoreError,
    StoreReadSnapshot, TermId,
};

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
    /// Visits visible named-graph copies. It intentionally preserves every
    /// source graph id in the returned [`EncodedQuad`].
    Union,
    /// Visits the distinct default union. This is intentionally separate from
    /// [`GraphSelector::Union`]: an unbound named graph must preserve copies.
    DefaultUnion,
}

impl GraphSelector {
    pub(crate) fn apply(self, mut pattern: QuadPattern) -> Option<QuadPattern> {
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
    fn contains_graph(&self, graph: &GraphId) -> Result<bool>;

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

    fn decode_term_arc(&self, context: &ReadContext<'_>, term: TermId) -> Result<Arc<EncodedTerm>> {
        Ok(Arc::new(self.decode_term(context, term)?))
    }

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
#[derive(Clone)]
pub(crate) struct StoreReadView<'store> {
    store: &'store GraphStore,
    snapshot: StoreReadSnapshot,
    read_mode: QueryReadMode,
    qv_admission: Rc<OnceCell<QueryIndexAdmission>>,
}

impl<'store> StoreReadView<'store> {
    pub(crate) fn new(store: &'store GraphStore) -> Self {
        Self {
            store,
            snapshot: store.read_snapshot(),
            read_mode: QueryReadMode::Auto,
            qv_admission: Rc::new(OnceCell::new()),
        }
    }

    pub(crate) fn with_read_mode(store: &'store GraphStore, read_mode: QueryReadMode) -> Self {
        Self {
            store,
            snapshot: store.read_snapshot(),
            read_mode,
            qv_admission: Rc::new(OnceCell::new()),
        }
    }

    pub(crate) fn store(&self) -> &'store GraphStore {
        self.store
    }

    pub(crate) fn snapshot(&self) -> &StoreReadSnapshot {
        &self.snapshot
    }

    fn auto_access_path(selector: GraphSelector, pattern: QuadPattern) -> ReadAccessPath {
        match selector {
            GraphSelector::Named(_) if pattern.subject.is_some() => ReadAccessPath::SourceGspo,
            GraphSelector::Named(_) if pattern.predicate.is_some() => ReadAccessPath::QvGpos,
            GraphSelector::Named(_) => ReadAccessPath::SourceGspo,
            GraphSelector::Union | GraphSelector::DefaultUnion if pattern.subject.is_some() => {
                ReadAccessPath::QvSpog
            }
            GraphSelector::Union | GraphSelector::DefaultUnion if pattern.predicate.is_some() => {
                ReadAccessPath::QvPosg
            }
            GraphSelector::Union | GraphSelector::DefaultUnion => ReadAccessPath::QvSpog,
        }
    }

    fn force_qv_path(selector: GraphSelector, pattern: QuadPattern) -> ReadAccessPath {
        match selector {
            GraphSelector::Named(_) if pattern.predicate.is_some() => ReadAccessPath::QvGpos,
            GraphSelector::Named(_) if pattern.subject.is_some() => ReadAccessPath::QvSpog,
            GraphSelector::Named(_) => ReadAccessPath::QvGpos,
            GraphSelector::Union | GraphSelector::DefaultUnion if pattern.subject.is_some() => {
                ReadAccessPath::QvSpog
            }
            GraphSelector::Union | GraphSelector::DefaultUnion => ReadAccessPath::QvPosg,
        }
    }

    fn qv_admission(&self, context: &ReadContext<'_>) -> Result<QueryIndexAdmission> {
        if let Some(admission) = self.qv_admission.get().copied() {
            context.observe_qv_admission(admission.trusted, admission.fallback_reason);
            return Ok(admission);
        }
        let admission = self.snapshot.query_index_admission(self.store)?;
        context.record_qv_admission(
            admission.trusted,
            admission.fallback_reason,
            admission.header_reads,
            admission.counter_reads,
        );
        let _ = self.qv_admission.set(admission);
        Ok(admission)
    }

    fn qv_order(path: ReadAccessPath) -> QueryIndexCursorOrder {
        match path {
            ReadAccessPath::QvGpos => QueryIndexCursorOrder::Gpos,
            ReadAccessPath::QvSpog => QueryIndexCursorOrder::Spog,
            ReadAccessPath::QvPosg => QueryIndexCursorOrder::Posg,
            ReadAccessPath::SourceGspo | ReadAccessPath::Empty => {
                unreachable!("only qv access paths have qv orders")
            }
        }
    }

    pub(crate) fn contains_graph_by_id(&self, graph: TermId) -> Result<bool> {
        self.snapshot.contains_graph_by_id(self.store, graph)
    }

    pub(crate) fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        let Some(graph) = self
            .snapshot
            .lookup_term(self.store, &EncodedTerm::from_named_node(&graph.0))?
        else {
            return Ok(false);
        };
        self.contains_graph_by_id(graph)
    }

    pub(crate) fn graph_term_id_iter(&self) -> impl Iterator<Item = Result<TermId>> + '_ {
        self.snapshot.graph_term_id_iter(self.store)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn qv_g_count(
        &self,
        context: &ReadContext<'_>,
        graph: TermId,
    ) -> Result<Option<u64>> {
        if !self.qv_ready(context)? {
            return Ok(None);
        }
        context.record_qv_meta();
        self.snapshot.qv_g_count(self.store, graph)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn qv_gp_count(
        &self,
        context: &ReadContext<'_>,
        graph: TermId,
        predicate: TermId,
    ) -> Result<Option<u64>> {
        if !self.qv_ready(context)? {
            return Ok(None);
        }
        context.record_qv_meta();
        self.snapshot.qv_gp_count(self.store, graph, predicate)
    }

    #[cfg(feature = "shacl-core")]
    pub(crate) fn qv_gpo_count(
        &self,
        context: &ReadContext<'_>,
        graph: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<Option<u64>> {
        if !self.qv_ready(context)? {
            return Ok(None);
        }
        context.record_qv_meta();
        self.snapshot
            .qv_gpo_count(self.store, graph, predicate, object)
    }

    #[cfg(feature = "shacl-core")]
    fn qv_ready(&self, context: &ReadContext<'_>) -> Result<bool> {
        Ok(self.qv_admission(context)?.trusted)
    }
}

impl RdfReadView for StoreReadView<'_> {
    fn contains_graph(&self, graph: &GraphId) -> Result<bool> {
        StoreReadView::contains_graph(self, graph)
    }

    fn scan<'store, 'context, 'visibility>(
        &'store self,
        context: &'context ReadContext<'visibility>,
        selector: GraphSelector,
        pattern: QuadPattern,
    ) -> Result<QueryCursor<'store, 'context, 'visibility>> {
        context.check_cancelled()?;
        let Some(pattern) = selector.apply(pattern) else {
            context.record_access_path(ReadAccessPath::Empty);
            return Ok(QueryCursor::empty(
                self.store,
                &self.snapshot,
                context,
                pattern,
            ));
        };
        if let GraphSelector::Named(graph) = selector
            && !self.graph_is_visible(context, graph)?
        {
            context.record_access_path(ReadAccessPath::Empty);
            return Ok(QueryCursor::empty(
                self.store,
                &self.snapshot,
                context,
                pattern,
            ));
        }
        if let (GraphSelector::Named(graph), Some(subject), Some(predicate), Some(object)) =
            (selector, pattern.subject, pattern.predicate, pattern.object)
        {
            let quad = EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            };
            context.record_access_path(ReadAccessPath::SourceGspo);
            context.increment_index_seeks();
            let candidate = self.snapshot.raw_quad_point(self.store, quad)?;
            return Ok(QueryCursor::new(
                self.store,
                &self.snapshot,
                context,
                crate::query_cursor::RawQuadCursor::single(candidate),
                pattern,
            ));
        }

        let requested = match self.read_mode {
            QueryReadMode::Auto => Self::auto_access_path(selector, pattern),
            QueryReadMode::ForceSource => ReadAccessPath::SourceGspo,
            QueryReadMode::ForceQv => Self::force_qv_path(selector, pattern),
        };
        if matches!(requested, ReadAccessPath::SourceGspo) {
            if matches!(selector, GraphSelector::DefaultUnion) {
                return Err(StoreError::QueryIndexUnavailable(
                    "default-union-source-dedup-is-not-bounded",
                ));
            }
            context.record_access_path(ReadAccessPath::SourceGspo);
            context.increment_index_seeks();
            return Ok(QueryCursor::new(
                self.store,
                &self.snapshot,
                context,
                self.snapshot.source_quad_cursor(self.store, pattern),
                pattern,
            ));
        }

        let admission = self.qv_admission(context)?;
        if !admission.trusted {
            if matches!(self.read_mode, QueryReadMode::ForceQv)
                || matches!(selector, GraphSelector::DefaultUnion)
            {
                return Err(StoreError::QueryIndexUnavailable(
                    admission.fallback_reason.unwrap_or("qv1-not-trusted"),
                ));
            }
            context.record_access_path(ReadAccessPath::SourceGspo);
            context.increment_index_seeks();
            return Ok(QueryCursor::new(
                self.store,
                &self.snapshot,
                context,
                self.snapshot.source_quad_cursor(self.store, pattern),
                pattern,
            ));
        }

        context.record_access_path(requested);
        context.increment_index_seeks();
        let raw = self
            .snapshot
            .query_index_cursor(self.store, Self::qv_order(requested), pattern);
        if matches!(selector, GraphSelector::DefaultUnion) {
            Ok(QueryCursor::default_union(
                self.store,
                &self.snapshot,
                context,
                raw,
                pattern,
            ))
        } else {
            Ok(QueryCursor::new(
                self.store,
                &self.snapshot,
                context,
                raw,
                pattern,
            ))
        }
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
        if let GraphSelector::Named(graph) = selector
            && !self.graph_is_visible(context, graph)?
        {
            return Ok(false);
        }
        if let (GraphSelector::Named(graph), Some(subject), Some(predicate), Some(object)) =
            (selector, pattern.subject, pattern.predicate, pattern.object)
        {
            let quad = EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            };
            context.record_access_path(ReadAccessPath::SourceGspo);
            context.increment_index_seeks();
            let Some(candidate) = self.snapshot.raw_quad_point(self.store, quad)? else {
                return Ok(false);
            };
            context.increment_candidate_quads();
            context.record_source_read(candidate.bytes_read);
            if !candidate.live || !pattern.matches(candidate.quad) {
                return Ok(false);
            }
            if quad_is_visible(self.store, &self.snapshot, context, candidate.quad)? {
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
        self.snapshot.lookup_term(self.store, term)
    }

    fn decode_term(&self, context: &ReadContext<'_>, term: TermId) -> Result<EncodedTerm> {
        context.check_cancelled()?;
        decode_term(self.store, context, term)
    }

    fn decode_term_arc(&self, context: &ReadContext<'_>, term: TermId) -> Result<Arc<EncodedTerm>> {
        context.check_cancelled()?;
        let decoded = self.store.decode_term_arc(term)?;
        context.increment_terms_decoded();
        Ok(decoded)
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
        let visible = graph_is_visible(self.store, &self.snapshot, context, graph)?;
        context.check_cancelled()?;
        Ok(visible)
    }

    fn quad_is_visible(&self, context: &ReadContext<'_>, quad: EncodedQuad) -> Result<bool> {
        context.check_cancelled()?;
        quad_is_visible(self.store, &self.snapshot, context, quad)
    }
}

pub(crate) fn decode_term(
    store: &GraphStore,
    context: &ReadContext<'_>,
    term: TermId,
) -> Result<EncodedTerm> {
    let decoded = store.decode_term(term)?;
    context.increment_terms_decoded();
    Ok(decoded)
}

pub(crate) fn graph_is_visible(
    store: &GraphStore,
    snapshot: &StoreReadSnapshot,
    context: &ReadContext<'_>,
    graph: TermId,
) -> Result<bool> {
    if let Some(visible) = context.graph_visibility(graph) {
        return Ok(visible);
    }

    context.increment_graphs_considered();
    let visible = if let Some(validation_graph) = context.validation_graph() {
        graph == validation_graph
    } else if !snapshot.contains_graph_by_id(store, graph)? {
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
    snapshot: &StoreReadSnapshot,
    context: &ReadContext<'_>,
    graph: TermId,
) -> Result<Rc<HashSet<TermId>>> {
    if let Some(orphaned) = context.orphaned(graph) {
        return Ok(orphaned);
    }

    let orphaned = Rc::new(snapshot.orphaned_entity_ids(store, context, graph)?);
    context.remember_orphaned(graph, orphaned.clone());
    Ok(orphaned)
}

pub(crate) fn quad_is_visible(
    store: &GraphStore,
    snapshot: &StoreReadSnapshot,
    context: &ReadContext<'_>,
    quad: EncodedQuad,
) -> Result<bool> {
    let visible = graph_is_visible(store, snapshot, context, quad.graph)?;
    context.check_cancelled()?;
    if !visible {
        return Ok(false);
    }
    if context.validation_graph().is_some() {
        return Ok(true);
    }
    let orphaned = orphaned_for_graph(store, snapshot, context, quad.graph)?;
    context.increment_orphan_checks();
    Ok(!orphaned.contains(&quad.subject) && !orphaned.contains(&quad.object))
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::core::{ActorId, Dot, GraphDiagnostics};
    use crate::query_context::{QueryCancellation, QueryReadMode, ReadAccessPath, ReadContext};
    use crate::store::{ClockUpdate, CounterKey, QuadAdd, QuadRemove, StoreError, hash_term};

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

    fn add_subject_graphs(store: &GraphStore, graph_count: usize) -> (GraphId, EncodedQuad) {
        let target_iri = format!("urn:test:named-subject:target:{graph_count}");
        let target = GraphId::new(&target_iri);
        store.create_graph(&target).unwrap();
        let target_id = store
            .resolve_term(&EncodedTerm::from_named_node(&target.0))
            .unwrap();
        let subject = store
            .resolve_term(&named("urn:test:named-subject:s"))
            .unwrap();
        let predicate = store
            .resolve_term(&named("urn:test:named-subject:p"))
            .unwrap();
        let object = store
            .resolve_term(&named("urn:test:named-subject:o"))
            .unwrap();
        let actor = ActorId::random();
        let mut batch = store.new_batch();
        let mut target_quad = None;
        for index in 0..graph_count {
            let graph = if index == 0 {
                target_id
            } else {
                hash_term(&named(&format!("urn:test:named-subject:copy:{index}")))
            };
            let quad = EncodedQuad {
                graph,
                subject,
                predicate,
                object,
            };
            assert!(
                store
                    .insert_quad(
                        &mut batch,
                        QuadAdd {
                            quad,
                            dot: Dot {
                                actor,
                                counter: (index + 1) as u64,
                            },
                        },
                    )
                    .unwrap()
            );
            if index == 0 {
                target_quad = Some(quad);
            }
        }
        store.commit(batch).unwrap();
        settle_diagnostics(store, &target);
        (target, target_quad.unwrap())
    }

    fn settle_diagnostics(store: &GraphStore, graph: &GraphId) {
        let diagnostics = store.graph_diagnostics(graph).unwrap();
        store.set_graph_diagnostics(graph, &diagnostics).unwrap();
    }

    fn remove_quad(store: &GraphStore, graph: &GraphId, quad: EncodedQuad) {
        let witnessed = store.get_vector_clock_by_id(quad.graph).unwrap();
        let _guard = store.graph_commit_guard(graph);
        let mut batch = store.new_batch();
        assert!(
            store
                .remove_quad(
                    &mut batch,
                    QuadRemove {
                        quad,
                        witnessed: &witnessed,
                    },
                )
                .unwrap()
        );
        store.commit(batch).unwrap();
    }

    fn collect_rows(
        cursor: impl Iterator<Item = crate::store::Result<EncodedQuad>>,
    ) -> Vec<EncodedQuad> {
        cursor.collect::<crate::store::Result<Vec<_>>>().unwrap()
    }

    fn raw_rows(store: &GraphStore, pattern: QuadPattern) -> Vec<EncodedQuad> {
        let snapshot = store.read_snapshot();
        let mut cursor = snapshot.raw_quad_cursor(store, pattern);
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
        settle_diagnostics(&store, &graph);

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
        settle_diagnostics(&store, &graph);
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
        settle_diagnostics(&store, &graph);
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

    #[test]
    fn cancellation_stops_before_a_scan_and_after_a_bound_union_candidate() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:cancellation");
        let graph_id = add_many(&store, &graph, 1_025);
        settle_diagnostics(&store, &graph);
        let view = StoreReadView::new(&store);

        let cancelled = QueryCancellation::new();
        cancelled.cancel();
        let context = ReadContext::new(cancelled);
        assert!(matches!(
            view.scan(
                &context,
                GraphSelector::Named(graph_id),
                QuadPattern::default()
            ),
            Err(StoreError::Cancelled)
        ));
        assert_eq!(0, context.snapshot().candidate_quads);
        assert_eq!(0, context.snapshot().index_seeks);

        let cancellation = QueryCancellation::new();
        let calls = Cell::new(0);
        let visibility = |_: &GraphId| {
            calls.set(calls.get() + 1);
            cancellation.cancel();
            false
        };
        let context = ReadContext::with_graph_visibility(cancellation.clone(), &visibility);
        let mut subjects: Vec<_> = (0..1_025)
            .map(|index| {
                store
                    .lookup_term(&named(&format!("urn:test:read:s{index}")))
                    .unwrap()
                    .unwrap()
            })
            .collect();
        subjects.sort_unstable();
        let late_subject = subjects[1_023];
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Union,
                QuadPattern {
                    subject: Some(late_subject),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(matches!(cursor.next(), Some(Err(StoreError::Cancelled))));
        assert_eq!(1, calls.get());
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(1, context.snapshot().candidate_quads);
        assert_eq!(1, context.snapshot().graphs_considered);
        assert_eq!(1, context.snapshot().terms_decoded);
        assert!(cursor.next().is_none());
    }

    #[test]
    fn default_union_cancellation_is_observed_at_1024_skipped_duplicate_copies() {
        let (_directory, store) = setup_store();
        let mut first = None;
        for index in 0..1_025 {
            let graph_name = format!("urn:test:default-union-cancel:{index}");
            let graph = GraphId::new(&graph_name);
            let quad = add_quad(
                &store,
                &graph,
                "urn:test:default-union-cancel:s",
                "urn:test:default-union-cancel:p",
                "urn:test:default-union-cancel:o",
            );
            first.get_or_insert(quad);
        }
        let first = first.expect("the fixture has one duplicate group");
        let view = StoreReadView::new(&store);
        let cancellation = QueryCancellation::new();
        let cancellation_in_visibility = cancellation.clone();
        let calls = Cell::new(0);
        let visibility = |_: &GraphId| {
            let next = calls.get() + 1;
            calls.set(next);
            if next == 1_024 {
                cancellation_in_visibility.cancel();
            }
            false
        };
        let context = ReadContext::with_graph_visibility(cancellation, &visibility);
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::DefaultUnion,
                QuadPattern {
                    predicate: Some(first.predicate),
                    object: Some(first.object),
                    ..QuadPattern::default()
                },
            )
            .unwrap();

        assert!(matches!(cursor.next(), Some(Err(StoreError::Cancelled))));
        assert_eq!(1_024, calls.get());
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(1_024, context.snapshot().candidate_quads);
        assert_eq!(0, context.snapshot().matching_quads);
        assert!(cursor.next().is_none());
    }

    #[test]
    fn hidden_named_graph_stops_before_ranges_and_point_probes() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:hidden-named");
        let graph_id = add_many(&store, &graph, 1_025);
        let view = StoreReadView::new(&store);

        let range_calls = Cell::new(0);
        let hidden = |_: &GraphId| {
            range_calls.set(range_calls.get() + 1);
            false
        };
        let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &hidden);
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Named(graph_id),
                QuadPattern::default(),
            )
            .unwrap();
        assert!(cursor.next().is_none());
        assert_eq!(1, range_calls.get());
        let statistics = context.snapshot();
        assert_eq!(1, statistics.graphs_considered);
        assert_eq!(0, statistics.index_seeks);
        assert_eq!(0, statistics.candidate_quads);

        let point_calls = Cell::new(0);
        let hidden = |_: &GraphId| {
            point_calls.set(point_calls.get() + 1);
            false
        };
        let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &hidden);
        assert!(
            !view
                .exists(
                    &context,
                    GraphSelector::Named(graph_id),
                    QuadPattern {
                        subject: Some(
                            store
                                .lookup_term(&named("urn:test:read:s0"))
                                .unwrap()
                                .unwrap(),
                        ),
                        predicate: Some(
                            store
                                .lookup_term(&named("urn:test:read:p"))
                                .unwrap()
                                .unwrap(),
                        ),
                        object: Some(
                            store
                                .lookup_term(&named("urn:test:read:o0"))
                                .unwrap()
                                .unwrap(),
                        ),
                        ..QuadPattern::default()
                    },
                )
                .unwrap()
        );
        assert_eq!(1, point_calls.get());
        let statistics = context.snapshot();
        assert_eq!(1, statistics.graphs_considered);
        assert_eq!(0, statistics.index_seeks);
        assert_eq!(0, statistics.candidate_quads);
    }

    #[test]
    fn visibility_cancellation_stops_before_named_and_union_reads() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:visibility-cancellation");
        let quad = add_quad(
            &store,
            &graph,
            "urn:test:visibility-cancellation:s",
            "urn:test:visibility-cancellation:p",
            "urn:test:visibility-cancellation:o",
        );
        let view = StoreReadView::new(&store);

        let cancellation = QueryCancellation::new();
        let visible = |_: &GraphId| {
            cancellation.cancel();
            true
        };
        let context = ReadContext::with_graph_visibility(cancellation.clone(), &visible);
        assert!(matches!(
            view.scan(
                &context,
                GraphSelector::Named(quad.graph),
                QuadPattern::default(),
            ),
            Err(StoreError::Cancelled)
        ));
        assert_eq!(1, context.snapshot().graphs_considered);
        assert_eq!(0, context.snapshot().index_seeks);
        assert_eq!(0, context.snapshot().candidate_quads);

        let cancellation = QueryCancellation::new();
        let visible = |_: &GraphId| {
            cancellation.cancel();
            true
        };
        let context = ReadContext::with_graph_visibility(cancellation.clone(), &visible);
        assert!(matches!(
            view.exists(
                &context,
                GraphSelector::Named(quad.graph),
                QuadPattern {
                    subject: Some(quad.subject),
                    predicate: Some(quad.predicate),
                    object: Some(quad.object),
                    ..QuadPattern::default()
                },
            ),
            Err(StoreError::Cancelled)
        ));
        assert_eq!(1, context.snapshot().graphs_considered);
        assert_eq!(0, context.snapshot().index_seeks);
        assert_eq!(0, context.snapshot().candidate_quads);

        let cancellation = QueryCancellation::new();
        let visible = |_: &GraphId| {
            cancellation.cancel();
            true
        };
        let context = ReadContext::with_graph_visibility(cancellation.clone(), &visible);
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Union,
                QuadPattern {
                    predicate: Some(quad.predicate),
                    object: Some(quad.object),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(matches!(cursor.next(), Some(Err(StoreError::Cancelled))));
        assert_eq!(1, context.snapshot().graphs_considered);
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(1, context.snapshot().candidate_quads);

        let cancellation = QueryCancellation::new();
        let visible = |_: &GraphId| {
            cancellation.cancel();
            true
        };
        let context = ReadContext::with_graph_visibility(cancellation.clone(), &visible);
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Union,
                QuadPattern {
                    subject: Some(quad.subject),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(matches!(cursor.next(), Some(Err(StoreError::Cancelled))));
        assert_eq!(1, context.snapshot().graphs_considered);
        assert_eq!(1, context.snapshot().index_seeks);
        assert_eq!(1, context.snapshot().candidate_quads);
        assert_eq!(0, context.snapshot().matching_quads);
    }

    #[test]
    fn union_respects_set_and_predicate_visibility_once_per_graph() {
        let (_directory, store) = setup_store();
        let first_graph = GraphId::new("urn:test:visible:first");
        let second_graph = GraphId::new("urn:test:visible:second");
        let first = add_quad(
            &store,
            &first_graph,
            "urn:test:visible:s1",
            "urn:test:visible:p",
            "urn:test:visible:o1",
        );
        let second = add_quad(
            &store,
            &second_graph,
            "urn:test:visible:s2",
            "urn:test:visible:p",
            "urn:test:visible:o2",
        );
        settle_diagnostics(&store, &first_graph);
        settle_diagnostics(&store, &second_graph);
        let view = StoreReadView::new(&store);

        let context =
            ReadContext::with_visible_graphs(QueryCancellation::new(), vec![first_graph.clone()]);
        let rows = collect_rows(
            view.scan(&context, GraphSelector::Union, QuadPattern::default())
                .unwrap(),
        );
        assert_eq!(vec![first], rows);
        assert_eq!(1, context.snapshot().index_seeks);

        let calls = RefCell::new(HashMap::<String, usize>::new());
        let visibility = |graph: &GraphId| {
            *calls
                .borrow_mut()
                .entry(graph.as_str().to_string())
                .or_default() += 1;
            graph == &first_graph
        };
        let context = ReadContext::with_graph_visibility(QueryCancellation::new(), &visibility);
        let rows = collect_rows(
            view.scan(&context, GraphSelector::Union, QuadPattern::default())
                .unwrap(),
        );
        assert_eq!(vec![first], rows);
        assert_eq!(Some(&1), calls.borrow().get(first_graph.as_str()));
        assert_eq!(Some(&1), calls.borrow().get(second_graph.as_str()));
        assert_eq!(2, context.snapshot().graphs_considered);
        assert_eq!(2, context.snapshot().terms_decoded);
        assert_ne!(first.graph, second.graph);
    }

    #[test]
    fn union_qv_ranges_use_one_boundary() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:union-candidate-index");
        let quad = add_quad(
            &store,
            &graph,
            "urn:test:union-candidate:s",
            "urn:test:union-candidate:p",
            "urn:test:union-candidate:o",
        );
        settle_diagnostics(&store, &graph);
        let view = StoreReadView::new(&store);

        let context = ReadContext::default();
        assert_eq!(
            vec![quad],
            collect_rows(
                view.scan(
                    &context,
                    GraphSelector::Union,
                    QuadPattern {
                        predicate: Some(quad.predicate),
                        object: Some(quad.object),
                        ..QuadPattern::default()
                    },
                )
                .unwrap(),
            )
        );
        let statistics = context.snapshot();
        assert_eq!(1, statistics.index_seeks);
        assert_eq!(1, statistics.candidate_quads);

        let context = ReadContext::default();
        assert_eq!(
            vec![quad],
            collect_rows(
                view.scan(
                    &context,
                    GraphSelector::Union,
                    QuadPattern {
                        object: Some(quad.object),
                        ..QuadPattern::default()
                    },
                )
                .unwrap(),
            )
        );
        let statistics = context.snapshot();
        assert_eq!(1, statistics.index_seeks);
        assert_eq!(1, statistics.candidate_quads);
    }

    #[test]
    fn qv_admission_cached() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:qv-admission");
        add_many(&store, &graph, 1_025);
        settle_diagnostics(&store, &graph);
        let predicate = store
            .lookup_term(&named("urn:test:read:p"))
            .unwrap()
            .unwrap();
        let before = store.query_index_admission_probe_count();
        let view = StoreReadView::new(&store);
        let second_view = view.clone();
        let context = ReadContext::default();
        let mut cursor = view
            .scan(
                &context,
                GraphSelector::Union,
                QuadPattern {
                    predicate: Some(predicate),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(cursor.next().unwrap().is_ok());
        drop(cursor);

        let mut second = second_view
            .scan(
                &context,
                GraphSelector::Union,
                QuadPattern {
                    predicate: Some(predicate),
                    ..QuadPattern::default()
                },
            )
            .unwrap();
        assert!(second.next().unwrap().is_ok());
        drop(second);

        assert_eq!(
            2,
            store.query_index_admission_probe_count() - before,
            "the hot path reads only the Ready header and exact total counter"
        );
        let statistics = context.snapshot();
        assert_eq!(1, statistics.qv_admission_checks);
        assert_eq!(1, statistics.qv_header_reads);
        assert_eq!(1, statistics.qv_counter_reads);
        assert!(statistics.qv_trusted);
        assert_eq!(2, statistics.candidate_quads);
        assert_eq!(2, statistics.qv_keys_read);
        assert_eq!(
            vec![ReadAccessPath::QvPosg],
            statistics.selected_access_paths
        );
    }

    #[test]
    fn named_subject_scaling() {
        for graph_count in [1, 32, 1_000] {
            let (_directory, store) = setup_store();
            let (graph, expected) = add_subject_graphs(&store, graph_count);

            let auto = StoreReadView::with_read_mode(&store, QueryReadMode::Auto);
            let auto_context = ReadContext::default();
            assert_eq!(
                vec![expected],
                collect_rows(
                    auto.scan(
                        &auto_context,
                        GraphSelector::Named(expected.graph),
                        QuadPattern {
                            subject: Some(expected.subject),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
                )
            );
            let auto_statistics = auto_context.snapshot();
            assert_eq!(1, auto_statistics.source_keys_read, "{graph_count} graphs");
            assert_eq!(1, auto_statistics.candidate_quads, "{graph_count} graphs");
            assert_eq!(
                vec![ReadAccessPath::SourceGspo],
                auto_statistics.selected_access_paths
            );

            let forced = StoreReadView::with_read_mode(&store, QueryReadMode::ForceQv);
            let forced_context = ReadContext::default();
            assert_eq!(
                vec![expected],
                collect_rows(
                    forced
                        .scan(
                            &forced_context,
                            GraphSelector::Named(expected.graph),
                            QuadPattern {
                                subject: Some(expected.subject),
                                ..QuadPattern::default()
                            },
                        )
                        .unwrap(),
                )
            );
            assert_eq!(
                graph_count as u64,
                forced_context.snapshot().qv_keys_read,
                "forced SPOG demonstrates the cross-graph work avoided for {graph}"
            );
        }
    }

    #[test]
    fn union_fallback_linear() {
        for graph_count in [10, 100] {
            let (_directory, store) = setup_store();
            let (_, expected) = add_subject_graphs(&store, graph_count);
            store.fail_test_indexes();

            let view = StoreReadView::with_read_mode(&store, QueryReadMode::Auto);
            let context = ReadContext::default();
            let error = match view.scan(
                &context,
                GraphSelector::DefaultUnion,
                QuadPattern {
                    subject: Some(expected.subject),
                    ..QuadPattern::default()
                },
            ) {
                Ok(_) => panic!("untrusted default union must not use a source rescan"),
                Err(error) => error,
            };
            assert!(matches!(error, StoreError::QueryIndexUnavailable(_)));

            let context = ReadContext::default();
            let rows = collect_rows(
                view.scan(
                    &context,
                    GraphSelector::Union,
                    QuadPattern {
                        subject: Some(expected.subject),
                        ..QuadPattern::default()
                    },
                )
                .unwrap(),
            );
            assert_eq!(vec![expected], rows);
            assert_eq!(graph_count as u64, context.snapshot().source_keys_read);
            assert_eq!(graph_count as u64, context.snapshot().candidate_quads);
        }

        let (_directory, store) = setup_store();
        let (_, expected) = add_subject_graphs(&store, 2);
        let source = StoreReadView::with_read_mode(&store, QueryReadMode::ForceSource);
        assert!(matches!(
            source.scan(
                &ReadContext::default(),
                GraphSelector::DefaultUnion,
                QuadPattern {
                    subject: Some(expected.subject),
                    ..QuadPattern::default()
                }
            ),
            Err(StoreError::QueryIndexUnavailable(_))
        ));
    }

    #[test]
    fn default_union_groups_qv_copies_but_named_union_preserves_them() {
        let (_directory, store) = setup_store();
        let first_graph = GraphId::new("urn:test:default-union:first");
        let second_graph = GraphId::new("urn:test:default-union:second");
        let first = add_quad(
            &store,
            &first_graph,
            "urn:test:default-union:s",
            "urn:test:default-union:p",
            "urn:test:default-union:o",
        );
        let second = add_quad(
            &store,
            &second_graph,
            "urn:test:default-union:s",
            "urn:test:default-union:p",
            "urn:test:default-union:o",
        );
        settle_diagnostics(&store, &first_graph);
        settle_diagnostics(&store, &second_graph);
        let view = StoreReadView::new(&store);
        let pattern = QuadPattern {
            predicate: Some(first.predicate),
            object: Some(first.object),
            ..QuadPattern::default()
        };

        let default_context = ReadContext::default();
        let default_rows = collect_rows(
            view.scan(&default_context, GraphSelector::DefaultUnion, pattern)
                .unwrap(),
        );
        assert_eq!(1, default_rows.len());
        assert_eq!(first.subject, default_rows[0].subject);
        assert_eq!(first.predicate, default_rows[0].predicate);
        assert_eq!(first.object, default_rows[0].object);
        assert_eq!(1, default_context.snapshot().matching_quads);
        assert_eq!(2, default_context.snapshot().candidate_quads);

        let named_context = ReadContext::default();
        assert_eq!(
            sorted(vec![first, second]),
            sorted(collect_rows(
                view.scan(&named_context, GraphSelector::Union, pattern)
                    .unwrap(),
            ))
        );
    }

    #[test]
    fn default_union_skips_hidden_and_orphaned_copies_before_emitting_one() {
        let (_directory, store) = setup_store();
        let orphan_graph = GraphId::new("urn:test:default-union:orphan");
        let hidden_graph = GraphId::new("urn:test:default-union:hidden");
        let visible_graph = GraphId::new("urn:test:default-union:visible");
        let subject = "urn:test:default-union:filtered:s";
        let predicate = "urn:test:default-union:filtered:p";
        let object = "urn:test:default-union:filtered:o";
        let orphan = add_quad(&store, &orphan_graph, subject, predicate, object);
        add_quad(&store, &hidden_graph, subject, predicate, object);
        let visible = add_quad(&store, &visible_graph, subject, predicate, object);
        store
            .set_graph_diagnostics(
                &orphan_graph,
                &GraphDiagnostics::from_orphaned_entities(vec![subject.to_owned()]),
            )
            .unwrap();
        settle_diagnostics(&store, &hidden_graph);
        settle_diagnostics(&store, &visible_graph);

        let view = StoreReadView::new(&store);
        let context = ReadContext::with_visible_graphs(
            QueryCancellation::new(),
            vec![orphan_graph, visible_graph],
        );
        assert_eq!(
            vec![visible],
            collect_rows(
                view.scan(
                    &context,
                    GraphSelector::DefaultUnion,
                    QuadPattern {
                        predicate: Some(orphan.predicate),
                        object: Some(orphan.object),
                        ..QuadPattern::default()
                    },
                )
                .unwrap(),
            )
        );
        assert_eq!(3, context.snapshot().candidate_quads);
        assert_eq!(1, context.snapshot().matching_quads);
    }

    #[test]
    fn orphaned_subjects_and_objects_are_filtered_from_normal_diagnostics() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:orphan-filter");
        add_quad(
            &store,
            &graph,
            graph.as_str(),
            "http://schema.org/hasPart",
            "urn:test:orphan-filter:reachable",
        );
        let orphan_type = add_quad(
            &store,
            &graph,
            "urn:test:orphan-filter:orphan",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://schema.org/MediaObject",
        );
        let orphan_object = store
            .lookup_term(&named("urn:test:orphan-filter:orphan"))
            .unwrap()
            .unwrap();
        add_quad(
            &store,
            &graph,
            "urn:test:orphan-filter:visible",
            "urn:test:orphan-filter:references",
            "urn:test:orphan-filter:orphan",
        );
        assert!(
            store
                .graph_diagnostics(&graph)
                .unwrap()
                .orphaned_entities
                .contains(&"urn:test:orphan-filter:orphan".to_string())
        );

        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let rows = collect_rows(
            view.scan(
                &context,
                GraphSelector::Named(orphan_type.graph),
                QuadPattern::default(),
            )
            .unwrap(),
        );
        assert!(
            rows.iter()
                .all(|quad| quad.subject != orphan_type.subject && quad.object != orphan_object)
        );
    }

    #[test]
    fn captured_view_keeps_an_orphan_hidden_after_a_live_adoption() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:snapshot-orphan-adoption");
        let entity = "urn:test:snapshot-orphan-adoption:entity";
        let typed = add_quad(
            &store,
            &graph,
            entity,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://schema.org/MediaObject",
        );
        let captured = StoreReadView::new(&store);

        add_quad(
            &store,
            &graph,
            graph.as_str(),
            "http://schema.org/hasPart",
            entity,
        );

        let captured_context = ReadContext::default();
        assert!(
            collect_rows(
                captured
                    .scan(
                        &captured_context,
                        GraphSelector::Named(typed.graph),
                        QuadPattern {
                            subject: Some(typed.subject),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
            )
            .is_empty()
        );

        let fresh = StoreReadView::new(&store);
        let fresh_context = ReadContext::default();
        assert_eq!(
            vec![typed],
            collect_rows(
                fresh
                    .scan(
                        &fresh_context,
                        GraphSelector::Named(typed.graph),
                        QuadPattern {
                            subject: Some(typed.subject),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
            )
        );
    }

    #[test]
    fn captured_view_keeps_a_reachable_entity_visible_after_a_live_unlink() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:snapshot-orphan-unlink");
        let entity = "urn:test:snapshot-orphan-unlink:entity";
        let typed = add_quad(
            &store,
            &graph,
            entity,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://schema.org/MediaObject",
        );
        let link = add_quad(
            &store,
            &graph,
            graph.as_str(),
            "http://schema.org/hasPart",
            entity,
        );
        let captured = StoreReadView::new(&store);

        remove_quad(&store, &graph, link);

        let captured_context = ReadContext::default();
        assert_eq!(
            vec![typed],
            collect_rows(
                captured
                    .scan(
                        &captured_context,
                        GraphSelector::Named(typed.graph),
                        QuadPattern {
                            subject: Some(typed.subject),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
            )
        );

        let fresh = StoreReadView::new(&store);
        let fresh_context = ReadContext::default();
        assert!(
            collect_rows(
                fresh
                    .scan(
                        &fresh_context,
                        GraphSelector::Named(typed.graph),
                        QuadPattern {
                            subject: Some(typed.subject),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
            )
            .is_empty()
        );
    }

    #[test]
    fn stale_snapshot_orphan_recompute_stops_before_a_large_source_scan_when_cancelled() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:snapshot-orphan-cancel");
        let graph_id = add_many(&store, &graph, 1_025);
        let view = StoreReadView::new(&store);
        let cancellation = QueryCancellation::new();
        cancellation.cancel();
        let context = ReadContext::new(cancellation);

        assert!(matches!(
            view.snapshot()
                .orphaned_entity_ids(&store, &context, graph_id),
            Err(StoreError::Cancelled)
        ));
        assert_eq!(0, context.snapshot().index_seeks);
        assert_eq!(0, context.snapshot().candidate_quads);
    }

    #[test]
    fn forward_and_inverse_walks_delegate_to_pattern_scans() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:walks");
        let first = add_quad(
            &store,
            &graph,
            "urn:test:walks:s1",
            "urn:test:walks:p",
            "urn:test:walks:o1",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:walks:s1",
            "urn:test:walks:p",
            "urn:test:walks:o2",
        );
        add_quad(
            &store,
            &graph,
            "urn:test:walks:s2",
            "urn:test:walks:p",
            "urn:test:walks:o1",
        );
        let view = StoreReadView::new(&store);

        let context = ReadContext::default();
        let forward = collect_rows(
            view.forward_predicate(
                &context,
                GraphSelector::Named(first.graph),
                first.subject,
                first.predicate,
            )
            .unwrap(),
        );
        let context = ReadContext::default();
        let equivalent = collect_rows(
            view.scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern {
                    subject: Some(first.subject),
                    predicate: Some(first.predicate),
                    ..QuadPattern::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(sorted(forward), sorted(equivalent));

        let context = ReadContext::default();
        let inverse = collect_rows(
            view.inverse_predicate(
                &context,
                GraphSelector::Named(first.graph),
                first.predicate,
                first.object,
            )
            .unwrap(),
        );
        let context = ReadContext::default();
        let equivalent = collect_rows(
            view.scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern {
                    predicate: Some(first.predicate),
                    object: Some(first.object),
                    ..QuadPattern::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(sorted(inverse), sorted(equivalent));
    }

    #[test]
    fn term_operations_use_graph_store_and_count_requested_decodes() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:terms");
        let quad = add_quad(
            &store,
            &graph,
            "urn:test:terms:s",
            "urn:test:terms:p",
            "urn:test:terms:o",
        );
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let subject = named("urn:test:terms:s");

        assert_eq!(
            Some(quad.subject),
            view.lookup_term(&context, &subject).unwrap()
        );
        assert_eq!(subject, view.decode_term(&context, quad.subject).unwrap());
        assert!(
            view.terms_equal(&context, quad.subject, quad.subject)
                .unwrap()
        );
        assert!(
            !view
                .terms_equal(&context, quad.subject, quad.object)
                .unwrap()
        );
        assert_ne!(
            Ordering::Equal,
            view.compare_terms(&context, quad.subject, quad.object)
                .unwrap()
        );
        assert_eq!(3, context.snapshot().terms_decoded);

        assert!(view.decode_term(&context, TermId(u128::MAX)).is_err());
        assert_eq!(3, context.snapshot().terms_decoded);
    }

    #[test]
    fn snapshot_cursor_does_not_hold_the_publication_lock_or_see_later_writes() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:snapshot-barrier");
        let first = add_quad(
            &store,
            &graph,
            "urn:test:snapshot:s1",
            "urn:test:snapshot:p",
            "urn:test:snapshot:o1",
        );
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let cursor = view
            .scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern::default(),
            )
            .unwrap();

        let (done, received) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                add_quad(
                    &store,
                    &graph,
                    "urn:test:snapshot:s2",
                    "urn:test:snapshot:p",
                    "urn:test:snapshot:o2",
                );
                done.send(()).unwrap();
            });
            received.recv_timeout(Duration::from_secs(2)).unwrap();
        });

        assert_eq!(vec![first], collect_rows(cursor));
    }

    #[test]
    fn predicate_object_cursor_is_lazy_and_copy_on_write_stable() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:predicate-object-snapshot");
        let first = add_quad(
            &store,
            &graph,
            "urn:test:predicate-object:s1",
            "urn:test:predicate-object:p",
            "urn:test:predicate-object:o",
        );
        settle_diagnostics(&store, &graph);
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let cursor = view
            .scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern {
                    predicate: Some(first.predicate),
                    object: Some(first.object),
                    ..QuadPattern::default()
                },
            )
            .unwrap();

        let (done, received) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                add_quad(
                    &store,
                    &graph,
                    "urn:test:predicate-object:s2",
                    "urn:test:predicate-object:p",
                    "urn:test:predicate-object:o",
                );
                done.send(()).unwrap();
            });
            received.recv_timeout(Duration::from_secs(2)).unwrap();
        });

        assert_eq!(vec![first], collect_rows(cursor));
        assert_eq!(1, context.snapshot().candidate_quads);
        assert_eq!(
            2,
            store
                .quads_for_pattern(
                    Some(first.graph),
                    None,
                    Some(first.predicate),
                    Some(first.object),
                )
                .unwrap()
                .len()
        );
    }

    #[test]
    fn object_cursor_is_lazy_and_copy_on_write_stable_across_add_and_remove() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:object-snapshot");
        let first = add_quad(
            &store,
            &graph,
            "urn:test:object:s1",
            "urn:test:object:p1",
            "urn:test:object:o",
        );
        let removed = add_quad(
            &store,
            &graph,
            "urn:test:object:s2",
            "urn:test:object:p2",
            "urn:test:object:o",
        );
        let view = StoreReadView::new(&store);
        let context = ReadContext::default();
        let cursor = view
            .scan(
                &context,
                GraphSelector::Named(first.graph),
                QuadPattern {
                    object: Some(first.object),
                    ..QuadPattern::default()
                },
            )
            .unwrap();

        let added = {
            let (done, received) = mpsc::channel();
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let added = add_quad(
                        &store,
                        &graph,
                        "urn:test:object:s3",
                        "urn:test:object:p3",
                        "urn:test:object:o",
                    );
                    let witnessed = store.get_vector_clock_by_id(removed.graph).unwrap();
                    let _guard = store.graph_commit_guard(&graph);
                    let mut batch = store.new_batch();
                    assert!(
                        store
                            .remove_quad(
                                &mut batch,
                                QuadRemove {
                                    quad: removed,
                                    witnessed: &witnessed,
                                },
                            )
                            .unwrap()
                    );
                    store.commit(batch).unwrap();
                    done.send(added).unwrap();
                });
                received.recv_timeout(Duration::from_secs(2)).unwrap()
            })
        };

        assert_eq!(sorted(vec![first, removed]), sorted(collect_rows(cursor)));
        let fresh_context = ReadContext::default();
        let fresh_view = StoreReadView::new(&store);
        assert_eq!(
            sorted(vec![first, added]),
            sorted(collect_rows(
                fresh_view
                    .scan(
                        &fresh_context,
                        GraphSelector::Named(first.graph),
                        QuadPattern {
                            object: Some(first.object),
                            ..QuadPattern::default()
                        },
                    )
                    .unwrap(),
            ))
        );
    }

    #[test]
    fn raw_cursor_waits_for_publication_and_snapshots_the_complete_batch() {
        let (_directory, store) = setup_store();
        let graph = GraphId::new("urn:test:raw-publication-barrier");
        store.create_graph(&graph).unwrap();
        let graph_id = store
            .resolve_term(&EncodedTerm::from_named_node(&graph.0))
            .unwrap();
        store.set_commit_stall(Duration::from_millis(500));

        std::thread::scope(|scope| {
            let store = &store;
            let graph = &graph;
            scope.spawn(|| {
                add_many(store, graph, 2);
            });
            spin_until(|| store.commit_stalled());

            let (started, wait_for_start) = mpsc::channel();
            let (done, wait_for_done) = mpsc::channel();
            scope.spawn(move || {
                started.send(()).unwrap();
                let rows = raw_rows(
                    store,
                    QuadPattern {
                        graph: Some(graph_id),
                        ..QuadPattern::default()
                    },
                );
                done.send(rows).unwrap();
            });
            wait_for_start.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(
                wait_for_done
                    .recv_timeout(Duration::from_millis(50))
                    .is_err(),
                "a raw cursor created during publication must wait for the barrier"
            );
            assert_eq!(
                2,
                wait_for_done
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .len(),
                "the cursor snapshot must contain the entire published batch"
            );
        });
    }
}
