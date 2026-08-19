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
#[derive(Clone, Default)]
pub struct QueryCancellation {
    cancelled: Arc<AtomicBool>,
}

impl QueryCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Counters collected during one RDF read execution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadStatistics {
    pub index_seeks: u64,
    pub candidate_quads: u64,
    pub matching_quads: u64,
    pub graphs_considered: u64,
    pub terms_decoded: u64,
}

#[derive(Default)]
struct ReadCounters {
    index_seeks: Cell<u64>,
    candidate_quads: Cell<u64>,
    matching_quads: Cell<u64>,
    graphs_considered: Cell<u64>,
    terms_decoded: Cell<u64>,
}

impl ReadCounters {
    fn increment(counter: &Cell<u64>) {
        counter.set(counter.get().saturating_add(1));
    }

    fn snapshot(&self) -> ReadStatistics {
        ReadStatistics {
            index_seeks: self.index_seeks.get(),
            candidate_quads: self.candidate_quads.get(),
            matching_quads: self.matching_quads.get(),
            graphs_considered: self.graphs_considered.get(),
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
    exact_graphs: Option<Rc<Vec<TermId>>>,
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
            exact_graphs: None,
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
        let mut exact_graphs: Vec<_> = graphs
            .into_iter()
            .map(|graph| hash_term(&EncodedTerm::from_named_node(&graph.0)))
            .collect();
        exact_graphs.sort_unstable();
        exact_graphs.dedup();
        let exact_graphs = Rc::new(exact_graphs);
        Self {
            cancellation,
            visibility: GraphVisibility::Exact(exact_graphs.iter().copied().collect()),
            exact_graphs: Some(exact_graphs),
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
            exact_graphs: None,
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
            exact_graphs: Some(Rc::new(vec![graph])),
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

    pub(crate) fn increment_candidate_quads(&self) {
        ReadCounters::increment(&self.counters.candidate_quads);
    }

    pub(crate) fn increment_matching_quads(&self) {
        ReadCounters::increment(&self.counters.matching_quads);
    }

    pub(crate) fn increment_graphs_considered(&self) {
        ReadCounters::increment(&self.counters.graphs_considered);
    }

    pub(crate) fn increment_terms_decoded(&self) {
        ReadCounters::increment(&self.counters.terms_decoded);
    }

    pub(crate) fn graph_visibility(&self, graph: TermId) -> Option<bool> {
        self.graph_visibility.borrow().get(&graph).copied()
    }

    pub(crate) fn exact_graphs(&self) -> Option<Rc<Vec<TermId>>> {
        self.exact_graphs.clone()
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
