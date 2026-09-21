//! Tracks query visibility, cancellation, budgets, and read statistics.
// Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
// SPDX-License-Identifier: MIT

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::{EncodedTerm, GraphId};
use crate::store::{Result, StoreError, TermId, hash_term};

/// Cancellation shared by Craqle reads without exposing evaluator or storage
/// cancellation primitives through the public API.
#[derive(Clone)]
pub struct QueryCancellation {
    inner: Arc<QueryCancellationInner>,
}

struct QueryCancellationInner {
    cancelled: AtomicBool,
    registry: Mutex<CancellationRegistry>,
}

#[derive(Default)]
struct CancellationRegistry {
    next_id: u128,
    evaluators: HashMap<u128, Arc<RequestState>>,
}

pub(crate) struct RequestState {
    cause: Mutex<RequestOutcome>,
    evaluator: spareval::CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestOutcome {
    Active,
    Explicit,
    Deadline,
    QueryCapacity,
    DeadlineCapacity,
    DeadlineUnavailable,
}

pub(crate) struct CancelRegistration {
    inner: Arc<QueryCancellationInner>,
    id: Option<u128>,
    state: Arc<RequestState>,
}

pub(crate) const MAX_QUERY_REGISTRATIONS: usize = 65_536;

impl Default for QueryCancellation {
    fn default() -> Self {
        Self {
            inner: Arc::new(QueryCancellationInner {
                cancelled: AtomicBool::new(false),
                registry: Mutex::new(CancellationRegistry::default()),
            }),
        }
    }
}

impl QueryCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        let states = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evaluators
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for state in states {
            state.mark_explicit();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn register(&self) -> CancelRegistration {
        self.register_with_limit(MAX_QUERY_REGISTRATIONS)
    }

    fn register_with_limit(&self, limit: usize) -> CancelRegistration {
        let state = Arc::new(RequestState::new());
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = if self.is_cancelled() || registry.evaluators.len() >= limit {
            None
        } else if let Some(next_id) = registry.next_id.checked_add(1) {
            let id = registry.next_id;
            registry.next_id = next_id;
            registry.evaluators.insert(id, Arc::clone(&state));
            Some(id)
        } else {
            None
        };
        let outcome = if self.is_cancelled() {
            RequestOutcome::Explicit
        } else if id.is_none() {
            RequestOutcome::QueryCapacity
        } else {
            RequestOutcome::Active
        };
        drop(registry);
        state.mark(outcome);
        CancelRegistration {
            inner: Arc::clone(&self.inner),
            id,
            state,
        }
    }
}

impl CancelRegistration {
    pub(crate) fn evaluator(&self) -> spareval::CancellationToken {
        self.state.evaluator.clone()
    }

    pub(crate) fn state(&self) -> Arc<RequestState> {
        Arc::clone(&self.state)
    }

    pub(crate) fn outcome(&self) -> RequestOutcome {
        self.state.outcome()
    }
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .evaluators
            .get(&id)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            registry.evaluators.remove(&id);
        }
    }
}

impl RequestState {
    pub(crate) fn new() -> Self {
        Self {
            cause: Mutex::new(RequestOutcome::Active),
            evaluator: spareval::CancellationToken::new(),
        }
    }

    pub(crate) fn mark_deadline(&self) {
        let mut cause = self
            .cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *cause == RequestOutcome::Active {
            *cause = RequestOutcome::Deadline;
        }
        drop(cause);
        self.evaluator.cancel();
    }

    pub(crate) fn mark_capacity(&self) {
        self.mark(RequestOutcome::DeadlineCapacity);
    }

    pub(crate) fn mark_unavailable(&self) {
        self.mark(RequestOutcome::DeadlineUnavailable);
    }

    fn mark_explicit(&self) {
        self.mark(RequestOutcome::Explicit);
    }

    fn mark(&self, outcome: RequestOutcome) {
        let mut cause = self
            .cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if outcome == RequestOutcome::Explicit || *cause == RequestOutcome::Active {
            *cause = outcome;
        }
        drop(cause);
        if outcome != RequestOutcome::Active {
            self.evaluator.cancel();
        }
    }

    pub(crate) fn outcome(&self) -> RequestOutcome {
        *self
            .cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for QueryCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Counters collected during one RDF read execution.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadStatistics {
    pub index_seeks: u64,
    pub qv_admission_checks: u64,
    pub qv_header_reads: u64,
    pub qv_counter_reads: u64,
    pub qv_trusted: bool,
    pub query_id_generation: Option<u64>,
    pub fallback_reason: Option<String>,
    pub selected_access_paths: Vec<ReadAccessPath>,
    pub source_keys_read: u64,
    pub source_bytes_read: u64,
    pub qv_keys_read: u64,
    pub qv_bytes_read: u64,
    #[serde(default)]
    pub reverse_mapping_reads: u64,
    #[serde(default)]
    pub reverse_mapping_bytes: u64,
    #[serde(default)]
    pub forward_mapping_reads: u64,
    #[serde(default)]
    pub forward_mapping_bytes: u64,
    pub candidate_quads: u64,
    pub matching_quads: u64,
    pub graphs_considered: u64,
    pub orphan_checks: u64,
    pub duplicate_groups: u64,
    pub duplicate_copies_skipped: u64,
    pub key_fields_extracted: u64,
    pub authoritative_terms_decoded: u64,
    pub result_terms_decoded: u64,
    pub encoded_quad_constructions: u64,
    pub terms_decoded: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CostStatistics {
    pub(crate) reverse_mapping_reads: u64,
    pub(crate) reverse_mapping_bytes: u64,
    pub(crate) forward_mapping_reads: u64,
    pub(crate) forward_mapping_bytes: u64,
    pub(crate) planner_index_entries: u64,
    pub(crate) planner_point_reads: u64,
    pub(crate) planner_cache_hits: u64,
    pub(crate) planner_cache_misses: u64,
    pub(crate) planner_memo_hits: u64,
    pub(crate) planner_memo_misses: u64,
}

#[derive(Debug, Default)]
struct CostCounters {
    reverse_mapping_reads: AtomicU64,
    reverse_mapping_bytes: AtomicU64,
    forward_mapping_reads: AtomicU64,
    forward_mapping_bytes: AtomicU64,
    planner_index_entries: AtomicU64,
    planner_point_reads: AtomicU64,
    planner_cache_hits: AtomicU64,
    planner_cache_misses: AtomicU64,
    planner_memo_hits: AtomicU64,
    planner_memo_misses: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryCost {
    counters: Option<Arc<CostCounters>>,
    planner: bool,
}

impl QueryCost {
    pub(crate) fn enabled(enabled: bool) -> Self {
        Self {
            counters: enabled.then(|| Arc::new(CostCounters::default())),
            planner: false,
        }
    }

    pub(crate) fn planner(enabled: bool) -> Self {
        Self {
            counters: enabled.then(|| Arc::new(CostCounters::default())),
            planner: true,
        }
    }

    pub(crate) fn as_planner(&self) -> Self {
        Self {
            counters: self.counters.clone(),
            planner: true,
        }
    }

    pub(crate) fn reverse_mapping(&self, bytes: u64) {
        if let Some(counters) = &self.counters {
            Self::increment(&counters.reverse_mapping_reads);
            Self::add(&counters.reverse_mapping_bytes, bytes);
            if self.planner {
                Self::increment(&counters.planner_point_reads);
            }
        }
    }

    pub(crate) fn forward_mapping(&self, bytes: u64) {
        if let Some(counters) = &self.counters {
            Self::increment(&counters.forward_mapping_reads);
            Self::add(&counters.forward_mapping_bytes, bytes);
            if self.planner {
                Self::increment(&counters.planner_point_reads);
            }
        }
    }

    pub(crate) fn planner_entries(&self, count: u64) {
        if self.planner
            && let Some(counters) = &self.counters
        {
            Self::add(&counters.planner_index_entries, count);
        }
    }

    pub(crate) fn planner_points(&self, count: u64) {
        if self.planner
            && let Some(counters) = &self.counters
        {
            Self::add(&counters.planner_point_reads, count);
        }
    }

    pub(crate) fn planner_cache(&self, hit: bool) {
        if self.planner
            && let Some(counters) = &self.counters
        {
            Self::increment(if hit {
                &counters.planner_cache_hits
            } else {
                &counters.planner_cache_misses
            });
        }
    }

    pub(crate) fn planner_memo(&self, hit: bool) {
        if self.planner
            && let Some(counters) = &self.counters
        {
            Self::increment(if hit {
                &counters.planner_memo_hits
            } else {
                &counters.planner_memo_misses
            });
        }
    }

    pub(crate) fn snapshot(&self) -> CostStatistics {
        self.counters
            .as_ref()
            .map_or_else(CostStatistics::default, |counters| CostStatistics {
                reverse_mapping_reads: counters.reverse_mapping_reads.load(Ordering::Relaxed),
                reverse_mapping_bytes: counters.reverse_mapping_bytes.load(Ordering::Relaxed),
                forward_mapping_reads: counters.forward_mapping_reads.load(Ordering::Relaxed),
                forward_mapping_bytes: counters.forward_mapping_bytes.load(Ordering::Relaxed),
                planner_index_entries: counters.planner_index_entries.load(Ordering::Relaxed),
                planner_point_reads: counters.planner_point_reads.load(Ordering::Relaxed),
                planner_cache_hits: counters.planner_cache_hits.load(Ordering::Relaxed),
                planner_cache_misses: counters.planner_cache_misses.load(Ordering::Relaxed),
                planner_memo_hits: counters.planner_memo_hits.load(Ordering::Relaxed),
                planner_memo_misses: counters.planner_memo_misses.load(Ordering::Relaxed),
            })
    }

    fn increment(counter: &AtomicU64) {
        Self::add(counter, 1);
    }

    fn add(counter: &AtomicU64, amount: u64) {
        let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(amount))
        });
    }
}

/// Storage access selected for an RDF pattern scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum ReadAccessPath {
    SourceGspo,
    QvGspo,
    QvGpos,
    QvSpog,
    QvPosg,
    QvOspg,
    QvGosp,
    Empty,
}

/// Test and benchmark control for Craqle's RDF read source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryReadMode {
    #[default]
    Auto,
    ForceSource,
    ForceQv,
}

#[derive(Default)]
struct ReadCounters {
    index_seeks: Cell<u64>,
    qv_admission_checks: Cell<u64>,
    qv_header_reads: Cell<u64>,
    qv_counter_reads: Cell<u64>,
    qv_trusted: Cell<bool>,
    query_id_generation: Cell<Option<u64>>,
    fallback_reason: RefCell<Option<String>>,
    selected_access_paths: RefCell<Vec<ReadAccessPath>>,
    source_keys_read: Cell<u64>,
    source_bytes_read: Cell<u64>,
    qv_keys_read: Cell<u64>,
    qv_bytes_read: Cell<u64>,
    costs: QueryCost,
    candidate_quads: Cell<u64>,
    matching_quads: Cell<u64>,
    graphs_considered: Cell<u64>,
    orphan_checks: Cell<u64>,
    duplicate_groups: Cell<u64>,
    duplicate_copies_skipped: Cell<u64>,
    key_fields_extracted: Cell<u64>,
    authoritative_terms_decoded: Cell<u64>,
    result_terms_decoded: Cell<u64>,
    encoded_quad_constructions: Cell<u64>,
    terms_decoded: Cell<u64>,
}

impl ReadCounters {
    fn increment(counter: &Cell<u64>) {
        counter.set(counter.get().saturating_add(1));
    }

    fn add(counter: &Cell<u64>, amount: u64) {
        counter.set(counter.get().saturating_add(amount));
    }

    fn snapshot(&self) -> ReadStatistics {
        ReadStatistics {
            index_seeks: self.index_seeks.get(),
            qv_admission_checks: self.qv_admission_checks.get(),
            qv_header_reads: self.qv_header_reads.get(),
            qv_counter_reads: self.qv_counter_reads.get(),
            qv_trusted: self.qv_trusted.get(),
            query_id_generation: self.query_id_generation.get(),
            fallback_reason: self.fallback_reason.borrow().clone(),
            selected_access_paths: self.selected_access_paths.borrow().clone(),
            source_keys_read: self.source_keys_read.get(),
            source_bytes_read: self.source_bytes_read.get(),
            qv_keys_read: self.qv_keys_read.get(),
            qv_bytes_read: self.qv_bytes_read.get(),
            reverse_mapping_reads: self.costs.snapshot().reverse_mapping_reads,
            reverse_mapping_bytes: self.costs.snapshot().reverse_mapping_bytes,
            forward_mapping_reads: self.costs.snapshot().forward_mapping_reads,
            forward_mapping_bytes: self.costs.snapshot().forward_mapping_bytes,
            candidate_quads: self.candidate_quads.get(),
            matching_quads: self.matching_quads.get(),
            graphs_considered: self.graphs_considered.get(),
            orphan_checks: self.orphan_checks.get(),
            duplicate_groups: self.duplicate_groups.get(),
            duplicate_copies_skipped: self.duplicate_copies_skipped.get(),
            key_fields_extracted: self.key_fields_extracted.get(),
            authoritative_terms_decoded: self.authoritative_terms_decoded.get(),
            result_terms_decoded: self.result_terms_decoded.get(),
            encoded_quad_constructions: self.encoded_quad_constructions.get(),
            terms_decoded: self.terms_decoded.get(),
        }
    }
}

pub(crate) enum GraphVisibility<'a> {
    All,
    Exact(HashSet<TermId>),
    Predicate(&'a (dyn Fn(&GraphId) -> bool + 'a)),
}

/// Per-execution state for a Craqle RDF read.
pub(crate) struct ReadContext<'a> {
    cancellation: QueryCancellation,
    pub(crate) visibility: GraphVisibility<'a>,
    /// Validation keeps the proposed graph visible before durable creation or
    /// diagnostic recomputation.
    validation_graph: Option<TermId>,
    counters: ReadCounters,
    graph_visibility: RefCell<HashMap<TermId, bool>>,
    orphaned: RefCell<HashMap<TermId, Rc<HashSet<TermId>>>>,
}

impl<'a> Default for ReadContext<'a> {
    fn default() -> Self {
        Self::new(QueryCancellation::default())
    }
}

impl<'a> ReadContext<'a> {
    #[must_use]
    pub(crate) fn new(cancellation: QueryCancellation) -> Self {
        Self {
            cancellation,
            visibility: GraphVisibility::All,
            validation_graph: None,
            counters: ReadCounters::default(),
            graph_visibility: RefCell::new(HashMap::new()),
            orphaned: RefCell::new(HashMap::new()),
        }
    }

    #[must_use]
    pub(crate) fn with_visible_graphs(
        cancellation: QueryCancellation,
        graphs: impl IntoIterator<Item = GraphId>,
    ) -> Self {
        let exact_graphs = graphs
            .into_iter()
            .map(|graph| hash_term(&EncodedTerm::from_named_node(&graph.0)))
            .collect();
        Self {
            cancellation,
            visibility: GraphVisibility::Exact(exact_graphs),
            validation_graph: None,
            counters: ReadCounters::default(),
            graph_visibility: RefCell::new(HashMap::new()),
            orphaned: RefCell::new(HashMap::new()),
        }
    }

    /// The only graph an exact scope admits, so default-union reads may use its range.
    pub(crate) fn single_graph(&self) -> Option<TermId> {
        match &self.visibility {
            GraphVisibility::Exact(graphs)
                if graphs.len() == 1 && self.validation_graph.is_none() =>
            {
                graphs.iter().next().copied()
            }
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn with_graph_visibility(
        cancellation: QueryCancellation,
        visible: &'a (dyn Fn(&GraphId) -> bool + 'a),
    ) -> Self {
        Self {
            cancellation,
            visibility: GraphVisibility::Predicate(visible),
            validation_graph: None,
            counters: ReadCounters::default(),
            graph_visibility: RefCell::new(HashMap::new()),
            orphaned: RefCell::new(HashMap::new()),
        }
    }

    /// Reads one candidate graph's final state while retaining normal filtering
    /// for ordinary queries.
    #[must_use]
    pub(crate) fn for_validation(cancellation: QueryCancellation, graph: &GraphId) -> Self {
        let graph = hash_term(&EncodedTerm::from_named_node(&graph.0));
        Self {
            cancellation,
            visibility: GraphVisibility::Exact(HashSet::from([graph])),
            validation_graph: Some(graph),
            counters: ReadCounters::default(),
            graph_visibility: RefCell::new(HashMap::new()),
            orphaned: RefCell::new(HashMap::new()),
        }
    }

    /// Returns a value snapshot without exposing the mutable counter cells.
    #[must_use]
    pub(crate) fn snapshot(&self) -> ReadStatistics {
        self.counters.snapshot()
    }

    pub(crate) fn enable_costs(&mut self) {
        self.counters.costs = QueryCost::enabled(true);
    }

    pub(crate) fn costs(&self) -> QueryCost {
        self.counters.costs.clone()
    }

    pub(crate) fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(StoreError::Cancelled);
        }
        Ok(())
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.cancellation.clone()
    }

    pub(crate) fn increment_index_seeks(&self) {
        ReadCounters::increment(&self.counters.index_seeks);
    }

    pub(crate) fn record_qv_admission(
        &self,
        trusted: bool,
        query_id_generation: Option<u64>,
        fallback_reason: Option<&'static str>,
        header_reads: u64,
        counter_reads: u64,
    ) {
        ReadCounters::increment(&self.counters.qv_admission_checks);
        ReadCounters::add(&self.counters.qv_header_reads, header_reads);
        ReadCounters::add(&self.counters.qv_counter_reads, counter_reads);
        self.observe_qv_admission(trusted, query_id_generation, fallback_reason);
    }

    pub(crate) fn record_qv_meta(&self) {
        ReadCounters::increment(&self.counters.qv_counter_reads);
    }

    pub(crate) fn observe_qv_admission(
        &self,
        trusted: bool,
        query_id_generation: Option<u64>,
        fallback_reason: Option<&'static str>,
    ) {
        self.counters.qv_trusted.set(trusted);
        if let Some(generation) = query_id_generation {
            self.counters.query_id_generation.set(Some(generation));
        }
        if self.counters.fallback_reason.borrow().is_none()
            && let Some(reason) = fallback_reason
        {
            *self.counters.fallback_reason.borrow_mut() = Some(reason.to_owned());
        }
    }

    pub(crate) fn record_access_path(&self, path: ReadAccessPath) {
        let mut paths = self.counters.selected_access_paths.borrow_mut();
        if !paths.contains(&path) {
            paths.push(path);
        }
    }

    pub(crate) fn record_source_read(&self, bytes: u64) {
        ReadCounters::increment(&self.counters.source_keys_read);
        ReadCounters::add(&self.counters.source_bytes_read, bytes);
    }

    pub(crate) fn record_qv_read(&self, bytes: u64) {
        ReadCounters::increment(&self.counters.qv_keys_read);
        ReadCounters::add(&self.counters.qv_bytes_read, bytes);
    }

    pub(crate) fn record_qv_reads(&self, count: u64, bytes: u64) {
        ReadCounters::add(&self.counters.qv_keys_read, count);
        ReadCounters::add(&self.counters.qv_bytes_read, bytes);
    }

    pub(crate) fn increment_candidate_quads(&self) {
        ReadCounters::increment(&self.counters.candidate_quads);
    }

    pub(crate) fn record_candidate_quads(&self, count: u64) {
        ReadCounters::add(&self.counters.candidate_quads, count);
    }

    pub(crate) fn increment_matching_quads(&self) {
        ReadCounters::increment(&self.counters.matching_quads);
    }

    pub(crate) fn record_matching_quads(&self, count: u64) {
        ReadCounters::add(&self.counters.matching_quads, count);
    }

    pub(crate) fn increment_graphs_considered(&self) {
        ReadCounters::increment(&self.counters.graphs_considered);
    }

    pub(crate) fn increment_orphan_checks(&self) {
        ReadCounters::increment(&self.counters.orphan_checks);
    }

    pub(crate) fn increment_duplicate_groups(&self) {
        ReadCounters::increment(&self.counters.duplicate_groups);
    }

    pub(crate) fn record_duplicate_groups(&self, count: u64) {
        ReadCounters::add(&self.counters.duplicate_groups, count);
    }

    pub(crate) fn increment_skipped_copies(&self) {
        ReadCounters::increment(&self.counters.duplicate_copies_skipped);
    }

    pub(crate) fn record_skipped_copies(&self, count: u64) {
        ReadCounters::add(&self.counters.duplicate_copies_skipped, count);
    }

    pub(crate) fn record_key_fields(&self, count: u64) {
        ReadCounters::add(&self.counters.key_fields_extracted, count);
    }

    pub(crate) fn increment_quad_builds(&self) {
        ReadCounters::increment(&self.counters.encoded_quad_constructions);
    }

    pub(crate) fn increment_terms_decoded(&self) {
        ReadCounters::increment(&self.counters.authoritative_terms_decoded);
        ReadCounters::increment(&self.counters.terms_decoded);
    }

    pub(crate) fn increment_result_decodes(&self) {
        ReadCounters::increment(&self.counters.result_terms_decoded);
    }

    pub(crate) fn graph_visibility(&self, graph: TermId) -> Option<bool> {
        self.graph_visibility.borrow().get(&graph).copied()
    }

    pub(crate) fn remember_graph_visibility(&self, graph: TermId, visible: bool) {
        self.graph_visibility.borrow_mut().insert(graph, visible);
    }

    pub(crate) fn orphaned(&self, graph: TermId) -> Option<Rc<HashSet<TermId>>> {
        self.orphaned.borrow().get(&graph).cloned()
    }

    pub(crate) fn remember_orphaned(&self, graph: TermId, orphaned: Rc<HashSet<TermId>>) {
        self.orphaned.borrow_mut().insert(graph, orphaned);
    }

    pub(crate) fn validation_graph(&self) -> Option<TermId> {
        self.validation_graph
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_releases_capacity() {
        let cancellation = QueryCancellation::new();
        let first = cancellation.register_with_limit(1);
        assert_eq!(first.outcome(), RequestOutcome::Active);
        let rejected = cancellation.register_with_limit(1);
        assert_eq!(rejected.outcome(), RequestOutcome::QueryCapacity);
        assert!(rejected.evaluator().is_cancelled());
        drop(first);
        let admitted = cancellation.register_with_limit(1);
        assert_eq!(admitted.outcome(), RequestOutcome::Active);

        let exhausted = QueryCancellation::new();
        exhausted.inner.registry.lock().unwrap().next_id = u128::MAX;
        let rejected = exhausted.register_with_limit(1);
        assert_eq!(rejected.outcome(), RequestOutcome::QueryCapacity);
    }

    #[test]
    fn stale_drop_preserves() {
        let cancellation = QueryCancellation::new();
        let registration = cancellation.register_with_limit(1);
        let id = registration.id.expect("registration must have an id");
        let replacement = Arc::new(RequestState::new());
        cancellation
            .inner
            .registry
            .lock()
            .unwrap()
            .evaluators
            .insert(id, Arc::clone(&replacement));
        drop(registration);
        let registry = cancellation.inner.registry.lock().unwrap();
        assert!(
            registry
                .evaluators
                .get(&id)
                .is_some_and(|state| Arc::ptr_eq(state, &replacement))
        );
    }

    #[test]
    fn explicit_wins_race() {
        let cancellation = QueryCancellation::new();
        let registration = cancellation.register();
        registration.state.mark_deadline();
        cancellation.cancel();
        assert_eq!(registration.outcome(), RequestOutcome::Explicit);
        assert!(registration.evaluator().is_cancelled());

        let cancellation = QueryCancellation::new();
        let registration = cancellation.register();
        cancellation.cancel();
        registration.state.mark_deadline();
        assert_eq!(registration.outcome(), RequestOutcome::Explicit);
    }
}
