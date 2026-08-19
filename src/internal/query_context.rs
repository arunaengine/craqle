use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::core::{EncodedTerm, GraphId};
use crate::store::{Result, StoreError, TermId, hash_term};

/// Cancellation shared by one or more Craqle read operations.
///
/// This deliberately belongs to Craqle rather than exposing an evaluator or
/// storage cancellation primitive through the public API.
#[derive(Clone)]
pub struct QueryCancellation {
    inner: Arc<QueryCancellationInner>,
}

struct QueryCancellationInner {
    cancelled: AtomicBool,
    evaluator: spareval::CancellationToken,
}

impl Default for QueryCancellation {
    fn default() -> Self {
        Self {
            inner: Arc::new(QueryCancellationInner {
                cancelled: AtomicBool::new(false),
                evaluator: spareval::CancellationToken::new(),
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
        self.inner.evaluator.cancel();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn evaluator_token(&self) -> spareval::CancellationToken {
        self.inner.evaluator.clone()
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadStatistics {
    pub index_seeks: u64,
    pub qv_admission_checks: u64,
    pub qv_header_reads: u64,
    pub qv_counter_reads: u64,
    pub qv_trusted: bool,
    pub fallback_reason: Option<String>,
    pub selected_access_paths: Vec<ReadAccessPath>,
    pub source_keys_read: u64,
    pub source_bytes_read: u64,
    pub qv_keys_read: u64,
    pub qv_bytes_read: u64,
    pub candidate_quads: u64,
    pub matching_quads: u64,
    pub graphs_considered: u64,
    pub orphan_checks: u64,
    pub duplicate_groups: u64,
    pub duplicate_copies_skipped: u64,
    pub terms_decoded: u64,
}

/// Storage access selected for an RDF pattern scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum ReadAccessPath {
    SourceGspo,
    QvGpos,
    QvSpog,
    QvPosg,
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
    fallback_reason: RefCell<Option<String>>,
    selected_access_paths: RefCell<Vec<ReadAccessPath>>,
    source_keys_read: Cell<u64>,
    source_bytes_read: Cell<u64>,
    qv_keys_read: Cell<u64>,
    qv_bytes_read: Cell<u64>,
    candidate_quads: Cell<u64>,
    matching_quads: Cell<u64>,
    graphs_considered: Cell<u64>,
    orphan_checks: Cell<u64>,
    duplicate_groups: Cell<u64>,
    duplicate_copies_skipped: Cell<u64>,
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
            fallback_reason: self.fallback_reason.borrow().clone(),
            selected_access_paths: self.selected_access_paths.borrow().clone(),
            source_keys_read: self.source_keys_read.get(),
            source_bytes_read: self.source_bytes_read.get(),
            qv_keys_read: self.qv_keys_read.get(),
            qv_bytes_read: self.qv_bytes_read.get(),
            candidate_quads: self.candidate_quads.get(),
            matching_quads: self.matching_quads.get(),
            graphs_considered: self.graphs_considered.get(),
            orphan_checks: self.orphan_checks.get(),
            duplicate_groups: self.duplicate_groups.get(),
            duplicate_copies_skipped: self.duplicate_copies_skipped.get(),
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
    /// Validation is intentionally distinct from ordinary query visibility:
    /// it names the one proposed graph and keeps its rows observable even
    /// before the graph exists durably or diagnostics have been recomputed.
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

    /// Read the final state of exactly one graph while validating a candidate
    /// write. This is crate-private so normal query reads retain their usual
    /// graph and orphan filtering semantics.
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

    pub(crate) fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(StoreError::Cancelled);
        }
        Ok(())
    }

    pub(crate) fn increment_index_seeks(&self) {
        ReadCounters::increment(&self.counters.index_seeks);
    }

    pub(crate) fn record_qv_admission(
        &self,
        trusted: bool,
        fallback_reason: Option<&'static str>,
        header_reads: u64,
        counter_reads: u64,
    ) {
        ReadCounters::increment(&self.counters.qv_admission_checks);
        ReadCounters::add(&self.counters.qv_header_reads, header_reads);
        ReadCounters::add(&self.counters.qv_counter_reads, counter_reads);
        self.observe_qv_admission(trusted, fallback_reason);
    }

    pub(crate) fn observe_qv_admission(
        &self,
        trusted: bool,
        fallback_reason: Option<&'static str>,
    ) {
        self.counters.qv_trusted.set(trusted);
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

    pub(crate) fn increment_candidate_quads(&self) {
        ReadCounters::increment(&self.counters.candidate_quads);
    }

    pub(crate) fn increment_matching_quads(&self) {
        ReadCounters::increment(&self.counters.matching_quads);
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

    pub(crate) fn increment_duplicate_copies_skipped(&self) {
        ReadCounters::increment(&self.counters.duplicate_copies_skipped);
    }

    pub(crate) fn increment_terms_decoded(&self) {
        ReadCounters::increment(&self.counters.terms_decoded);
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
