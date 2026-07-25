use std::cell::OnceCell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::LazyLock;

use crate::core::{CrateViolation, EncodedTerm, GraphId, MaterializedQuadChange, QuadOp, vocab};
use crate::store::{GraphStore, TermId};

// ── Constant term table ─────────────────────────────────────────────────────
//
// `vocab` hands out freshly parsed `NamedNode`s, so every rule used to re-encode
// the same dozen IRIs on every call. These are interned once per process.

fn encoded_nn(nn: &oxrdf::NamedNode) -> EncodedTerm {
    EncodedTerm::from_named_node(nn)
}

macro_rules! vocab_terms {
    ($($name:ident => $source:path),* $(,)?) => {
        $(static $name: LazyLock<EncodedTerm> = LazyLock::new(|| encoded_nn(&$source()));)*
    };
}

vocab_terms! {
    RDF_TYPE => vocab::rdf_type,
    SCHEMA_DATASET => vocab::schema_dataset,
    SCHEMA_MEDIA_OBJECT => vocab::schema_media_object,
    SCHEMA_CREATIVE_WORK => vocab::schema_creative_work,
    SCHEMA_HAS_PART => vocab::schema_has_part,
    SCHEMA_ABOUT => vocab::schema_about,
    SCHEMA_NAME => vocab::schema_name,
    SCHEMA_DESCRIPTION => vocab::schema_description,
    SCHEMA_DATE_PUBLISHED => vocab::schema_date_published,
    METADATA_DESCRIPTOR => vocab::metadata_descriptor,
}

/// The three properties RO-Crate requires on the root data entity, in the order
/// they are reported. `DeltaSummary::touches_required_root_properties` is
/// indexed by this order.
///
/// `schema:license` is deliberately absent: a crate may be created without one
/// (genesis takes `Option<License>`), and the submitted licence shape is
/// preserved out of band by the store rather than as a required root triple.
static REQUIRED_ROOT_PROPERTIES: LazyLock<[(EncodedTerm, &'static str); 3]> = LazyLock::new(|| {
    [
        (SCHEMA_NAME.clone(), "schema:name"),
        (SCHEMA_DESCRIPTION.clone(), "schema:description"),
        (SCHEMA_DATE_PUBLISHED.clone(), "schema:datePublished"),
    ]
});

/// Index of `schema:datePublished` in [`REQUIRED_ROOT_PROPERTIES`].
const DATE_PUBLISHED_SLOT: usize = 2;

fn graph_root(graph: &GraphId) -> EncodedTerm {
    EncodedTerm::from_named_node(&graph.0)
}

// ── Small key bundles ───────────────────────────────────────────────────────

/// A `(subject, predicate, object)` lookup key. Bundled so every helper that
/// needs a whole triple stays inside the three-parameter budget.
#[derive(Clone, Copy)]
pub struct TripleRef<'a> {
    pub subject: &'a EncodedTerm,
    pub predicate: &'a EncodedTerm,
    pub object: &'a EncodedTerm,
}

/// A `(subject, predicate)` lookup key.
#[derive(Clone, Copy)]
pub struct SubjectPredicate<'a> {
    pub subject: &'a EncodedTerm,
    pub predicate: &'a EncodedTerm,
}

/// The change set under validation: what is being written, to which graph, on
/// top of which store.
#[derive(Clone, Copy)]
pub struct ChangeSet<'a> {
    pub store: &'a GraphStore,
    pub graph: &'a GraphId,
    pub delta: &'a [MaterializedQuadChange],
}

// ── Snapshots ───────────────────────────────────────────────────────────────

/// A snapshot view of a graph for validation.
pub struct GraphSnapshot {
    pub graph: GraphId,
    /// All quads in the graph, as (subject, predicate, object) encoded terms.
    pub triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
    /// Lazily built index over `triples`, so the post-state rules cost O(T)
    /// instead of one full scan per lookup. `triples` is never mutated after
    /// construction, so a single build can never go stale.
    lookup: OnceCell<SnapshotLookup>,
}

#[derive(Default)]
struct SnapshotLookup {
    /// subject → predicate → row indices into `GraphSnapshot::triples`.
    rows: HashMap<EncodedTerm, HashMap<EncodedTerm, Vec<usize>>>,
}

impl GraphSnapshot {
    fn new(graph: GraphId, triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>) -> Self {
        Self {
            graph,
            triples,
            lookup: OnceCell::new(),
        }
    }

    /// Build a snapshot by reading every quad for a graph from the store.
    pub fn from_store(store: &GraphStore, graph: &GraphId) -> crate::store::Result<Self> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_tid) = store.lookup_term(&graph_term)? else {
            return Ok(Self::new(graph.clone(), Vec::new()));
        };
        let mut triples = Vec::new();
        let mut term_cache = HashMap::new();
        store.for_each_quad_in_graph::<crate::store::StoreError, _>(graph_tid, |q| {
            let s = store.decode_term_cached(&mut term_cache, q.subject)?;
            let p = store.decode_term_cached(&mut term_cache, q.predicate)?;
            let o = store.decode_term_cached(&mut term_cache, q.object)?;
            triples.push((s, p, o));
            Ok(())
        })?;
        Ok(Self::new(graph.clone(), triples))
    }

    fn root(&self) -> EncodedTerm {
        graph_root(&self.graph)
    }

    fn lookup(&self) -> &SnapshotLookup {
        self.lookup.get_or_init(|| {
            let mut rows: HashMap<EncodedTerm, HashMap<EncodedTerm, Vec<usize>>> = HashMap::new();
            for (row, (subject, predicate, _)) in self.triples.iter().enumerate() {
                rows.entry(subject.clone())
                    .or_default()
                    .entry(predicate.clone())
                    .or_default()
                    .push(row);
            }
            SnapshotLookup { rows }
        })
    }

    fn rows_for(&self, key: SubjectPredicate<'_>) -> &[usize] {
        self.lookup()
            .rows
            .get(key.subject)
            .and_then(|predicates| predicates.get(key.predicate))
            .map_or(&[][..], Vec::as_slice)
    }

    /// Count triples matching `(subject, predicate, *)`.
    fn count_sp(&self, key: SubjectPredicate<'_>) -> usize {
        self.rows_for(key).len()
    }

    /// Does this exact triple exist?
    fn has_triple(&self, triple: TripleRef<'_>) -> bool {
        let key = SubjectPredicate {
            subject: triple.subject,
            predicate: triple.predicate,
        };
        self.rows_for(key)
            .iter()
            .any(|&row| &self.triples[row].2 == triple.object)
    }
}

// ── Rules ───────────────────────────────────────────────────────────────────

/// Everything a candidate check may read: the post-delta view of the store plus
/// the pre-computed summary of what the delta touches.
pub struct RuleContext<'a> {
    view: DeltaView<'a>,
    summary: &'a DeltaSummary,
}

impl<'a> RuleContext<'a> {
    fn new(view: DeltaView<'a>, summary: &'a DeltaSummary) -> Self {
        Self { view, summary }
    }

    fn root(&self) -> EncodedTerm {
        graph_root(self.view.graph)
    }

    fn delta_is_empty(&self) -> bool {
        self.view.index.is_empty
    }
}

pub trait Rule: Send + Sync {
    /// Validate against the delta alone where possible. Returning
    /// [`CandidateCheck::NeedSnapshot`] falls back to materialising the whole
    /// post state.
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck>;

    /// Validate the post-state snapshot. Return `Err` if a violation is found.
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateCheck {
    Pass,
    Violation(CrateViolation),
    NeedSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum RuleEvaluationError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("violations: {0:?}")]
    Violations(Vec<CrateViolation>),
}

// ── Delta summary ───────────────────────────────────────────────────────────

/// What a change set touches, in the terms the rules care about. Cheap to
/// compute (one pass over the delta) and shared by every rule.
#[derive(Debug, Default, Clone)]
pub struct DeltaSummary {
    inserted_subjects: BTreeSet<EncodedTerm>,
    type_changed_subjects: BTreeSet<EncodedTerm>,
    /// Entities whose whole `hasPart` subtree may have changed reachability.
    /// Expanded to all descendants before the orphan check.
    reachability_seeds: BTreeSet<EncodedTerm>,
    /// Entities whose *own* data-entity status may have changed but whose
    /// reachability and subtree cannot have. Checked directly, without
    /// expansion — gaining or losing an outgoing `hasPart` edge changes whether
    /// a node counts as a data entity, never whether it is reachable.
    reachability_probes: BTreeSet<EncodedTerm>,
    touches_root_dataset: bool,
    touches_metadata_descriptor: bool,
    touches_required_root_properties: [bool; 3],
    touches_reachability: bool,
}

impl DeltaSummary {
    fn touches_any_required_root_property(&self) -> bool {
        self.touches_required_root_properties.iter().any(|hit| *hit)
    }

    /// True when the change set can move an entity into or out of
    /// `orphaned_data_entities`.
    ///
    /// `orphaned_data_entities` reads exactly two triple shapes — `rdf:type` with
    /// object `schema:Dataset` or `schema:MediaObject`, and `schema:hasPart` —
    /// and those are exactly the shapes that set this flag. A change set that
    /// sets none of them therefore leaves the orphan set bit-for-bit unchanged.
    pub fn touches_reachability(&self) -> bool {
        self.touches_reachability
    }
}

/// One triple a write touches, independent of whether it arrived as a local
/// change or as a replicated op.
struct TouchedTriple<'a> {
    subject: &'a EncodedTerm,
    predicate: &'a EncodedTerm,
    object: &'a EncodedTerm,
    inserted: bool,
}

fn summarize<'t>(
    graph: &GraphId,
    touched: impl Iterator<Item = TouchedTriple<'t>>,
) -> DeltaSummary {
    let root = graph_root(graph);
    let mut summary = DeltaSummary::default();

    for change in touched {
        let TouchedTriple {
            subject,
            predicate,
            object,
            inserted,
        } = change;

        if inserted {
            summary.inserted_subjects.insert(subject.clone());
        }

        if subject == &root {
            for (slot, (required, _)) in summary
                .touches_required_root_properties
                .iter_mut()
                .zip(REQUIRED_ROOT_PROPERTIES.iter())
            {
                if predicate == required {
                    *slot = true;
                }
            }
            if predicate == &*RDF_TYPE && object == &*SCHEMA_DATASET {
                summary.touches_root_dataset = true;
            }
        }

        if subject == &*METADATA_DESCRIPTOR
            && ((predicate == &*RDF_TYPE && object == &*SCHEMA_CREATIVE_WORK)
                || (predicate == &*SCHEMA_ABOUT && object == &root))
        {
            summary.touches_metadata_descriptor = true;
        }

        if predicate == &*SCHEMA_HAS_PART {
            summary.touches_reachability = true;
            summary.reachability_seeds.insert(object.clone());
            summary.reachability_probes.insert(subject.clone());
        }

        if predicate == &*RDF_TYPE {
            summary.type_changed_subjects.insert(subject.clone());
            if object == &*SCHEMA_DATASET || object == &*SCHEMA_MEDIA_OBJECT {
                summary.touches_reachability = true;
                summary.reachability_seeds.insert(subject.clone());
            }
        }
    }

    summary
}

/// Summarize a local change set. Changes targeting another graph are ignored,
/// mirroring every other delta helper.
pub fn summarize_delta(graph: &GraphId, delta: &[MaterializedQuadChange]) -> DeltaSummary {
    summarize(
        graph,
        delta.iter().filter_map(|change| {
            let (change_graph, subject, predicate, object, inserted) = match change {
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
            (change_graph == graph).then_some(TouchedTriple {
                subject,
                predicate,
                object,
                inserted,
            })
        }),
    )
}

/// Summarize a replicated batch's ops. `QuadOp`s carry no graph of their own —
/// they all belong to their batch's graph.
pub fn summarize_ops(graph: &GraphId, ops: &[QuadOp]) -> DeltaSummary {
    summarize(
        graph,
        ops.iter().map(|op| match op {
            QuadOp::Add {
                subject,
                predicate,
                object,
                ..
            } => TouchedTriple {
                subject,
                predicate,
                object,
                inserted: true,
            },
            QuadOp::Remove {
                subject,
                predicate,
                object,
                ..
            } => TouchedTriple {
                subject,
                predicate,
                object,
                inserted: false,
            },
        }),
    )
}

// ── Delta index ─────────────────────────────────────────────────────────────

/// Net effect of a change set, keyed for O(1) lookup.
///
/// Two fold shapes live here and must not be confused:
///
/// * **Last writer wins** — `PredicateDelta::objects` records the *final* entry
///   for each triple, so `Delete` then `Insert` of the same triple leaves it
///   present and `Insert` then `Delete` leaves it absent. Building it means
///   iterating the delta in order and overwriting.
/// * **Order-independent sums** — the `count` fields add `+1`/`-1` per matching
///   change, so any ordering yields the same total. They deliberately do *not*
///   deduplicate, matching the counts the store returns.
#[derive(Debug, Default)]
pub struct DeltaIndex {
    subjects: HashMap<EncodedTerm, SubjectDelta>,
    /// Reverse `hasPart` edge map: object → subject → final state.
    has_part_parents: HashMap<EncodedTerm, HashMap<EncodedTerm, bool>>,
    /// Whether the change set was empty. Several rules only make sense to check
    /// against a delta, and fall back to the post-state snapshot without one.
    is_empty: bool,
}

#[derive(Debug, Default)]
struct SubjectDelta {
    /// Net change in the subject's triple count.
    triple_count: i64,
    predicates: HashMap<EncodedTerm, PredicateDelta>,
}

#[derive(Debug, Default)]
struct PredicateDelta {
    /// Net change in the `(subject, predicate, *)` count.
    count: i64,
    /// Final state of every object touched for this `(subject, predicate)`.
    objects: HashMap<EncodedTerm, bool>,
}

impl DeltaIndex {
    /// Fold a change set into the index. Changes targeting another graph are
    /// ignored, matching the per-change `change_graph == graph` filter every
    /// delta helper used to apply.
    pub fn build(graph: &GraphId, delta: &[MaterializedQuadChange]) -> Self {
        let mut index = Self {
            is_empty: delta.is_empty(),
            ..Self::default()
        };

        for change in delta {
            let (change_graph, subject, predicate, object, inserted) = match change {
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

            let step = if inserted { 1 } else { -1 };
            let subject_delta = index.subjects.entry(subject.clone()).or_default();
            subject_delta.triple_count += step;
            let predicate_delta = subject_delta
                .predicates
                .entry(predicate.clone())
                .or_default();
            predicate_delta.count += step;
            // Last writer wins: a later entry for the same object overwrites.
            predicate_delta.objects.insert(object.clone(), inserted);

            if predicate == &*SCHEMA_HAS_PART {
                index
                    .has_part_parents
                    .entry(object.clone())
                    .or_default()
                    .insert(subject.clone(), inserted);
            }
        }

        index
    }

    fn predicate_delta(&self, key: SubjectPredicate<'_>) -> Option<&PredicateDelta> {
        self.subjects
            .get(key.subject)
            .and_then(|subject| subject.predicates.get(key.predicate))
    }

    /// The delta's final verdict on one triple, or `None` when it says nothing.
    fn triple_state(&self, triple: TripleRef<'_>) -> Option<bool> {
        let key = SubjectPredicate {
            subject: triple.subject,
            predicate: triple.predicate,
        };
        self.predicate_delta(key)?
            .objects
            .get(triple.object)
            .copied()
    }

    fn count_change(&self, key: SubjectPredicate<'_>) -> i64 {
        self.predicate_delta(key).map_or(0, |entry| entry.count)
    }

    fn subject_count_change(&self, subject: &EncodedTerm) -> i64 {
        self.subjects
            .get(subject)
            .map_or(0, |entry| entry.triple_count)
    }

    fn has_part_children(&self, subject: &EncodedTerm) -> Option<&HashMap<EncodedTerm, bool>> {
        let key = SubjectPredicate {
            subject,
            predicate: &SCHEMA_HAS_PART,
        };
        self.predicate_delta(key).map(|entry| &entry.objects)
    }

    fn has_part_parents(&self, object: &EncodedTerm) -> Option<&HashMap<EncodedTerm, bool>> {
        self.has_part_parents.get(object)
    }
}

// ── Post-delta view of the store ────────────────────────────────────────────

/// "The graph as it will be once this change set commits."
///
/// Every read is `store value` combined with the [`DeltaIndex`] entry, so a
/// check costs one store probe plus a hash lookup instead of a full delta scan.
pub struct DeltaView<'a> {
    store: &'a GraphStore,
    graph: &'a GraphId,
    graph_id: Option<TermId>,
    index: &'a DeltaIndex,
}

impl<'a> DeltaView<'a> {
    fn new(change_set: ChangeSet<'a>, index: &'a DeltaIndex) -> crate::store::Result<Self> {
        let graph_term = EncodedTerm::from_named_node(&change_set.graph.0);
        Ok(Self {
            store: change_set.store,
            graph: change_set.graph,
            graph_id: change_set.store.lookup_term(&graph_term)?,
            index,
        })
    }

    /// Does this triple exist after the change set applies?
    fn triple_exists(&self, triple: TripleRef<'_>) -> crate::store::Result<bool> {
        if let Some(present) = self.index.triple_state(triple) {
            return Ok(present);
        }
        self.stored_triple_exists(triple)
    }

    /// How many objects does `(subject, predicate)` have afterwards?
    fn count_sp(&self, key: SubjectPredicate<'_>) -> crate::store::Result<usize> {
        let stored = self.store.count_objects_for_subject_predicate(
            self.graph,
            key.subject,
            key.predicate,
        )?;
        Ok(saturating_total(stored, self.index.count_change(key)))
    }

    /// How many triples does `subject` have afterwards?
    fn subject_triple_count(&self, subject: &EncodedTerm) -> crate::store::Result<usize> {
        let stored = self.stored_subject_triple_count(subject)?;
        Ok(saturating_total(
            stored,
            self.index.subject_count_change(subject),
        ))
    }

    fn stored_subject_triple_count(&self, subject: &EncodedTerm) -> crate::store::Result<usize> {
        let (Some(graph_id), Some(subject_id)) = (self.graph_id, self.store.lookup_term(subject)?)
        else {
            return Ok(0);
        };
        self.store.subject_triple_count_by_ids(graph_id, subject_id)
    }

    fn stored_triple_exists(&self, triple: TripleRef<'_>) -> crate::store::Result<bool> {
        let (Some(graph_id), Some(subject_id), Some(predicate_id), Some(object_id)) = (
            self.graph_id,
            self.store.lookup_term(triple.subject)?,
            self.store.lookup_term(triple.predicate)?,
            self.store.lookup_term(triple.object)?,
        ) else {
            return Ok(false);
        };
        Ok(self.store.contains_quad(crate::store::EncodedQuad {
            graph: graph_id,
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        }))
    }

    /// `hasPart` children of `subject` afterwards.
    fn children(&self, subject: &EncodedTerm) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let mut children = self.stored_children(subject)?;
        apply_edge_changes(&mut children, self.index.has_part_children(subject));
        Ok(children)
    }

    /// Entities with a `hasPart` edge pointing at `entity` afterwards.
    fn parents(&self, entity: &EncodedTerm) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let mut parents = self.stored_parents(entity)?;
        apply_edge_changes(&mut parents, self.index.has_part_parents(entity));
        Ok(parents)
    }

    fn stored_children(
        &self,
        subject: &EncodedTerm,
    ) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let (Some(graph_id), Some(subject_id)) = (self.graph_id, self.store.lookup_term(subject)?)
        else {
            return Ok(BTreeSet::new());
        };
        Ok(self
            .store
            .triples_for_subject(graph_id, subject_id)?
            .into_iter()
            .filter_map(|(predicate, object)| (predicate == *SCHEMA_HAS_PART).then_some(object))
            .collect())
    }

    fn stored_parents(&self, entity: &EncodedTerm) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let (Some(graph_id), Some(predicate_id), Some(object_id)) = (
            self.graph_id,
            self.store.lookup_term(&SCHEMA_HAS_PART)?,
            self.store.lookup_term(entity)?,
        ) else {
            return Ok(BTreeSet::new());
        };

        self.store
            .quads_for_pattern(Some(graph_id), None, Some(predicate_id), Some(object_id))?
            .into_iter()
            .map(|quad| self.store.decode_term(quad.subject))
            .collect()
    }
}

/// `stored + change`, clamped at zero — a delta that deletes more than the
/// store holds must not underflow into a huge count.
fn saturating_total(stored: usize, change: i64) -> usize {
    (stored as i64 + change).max(0) as usize
}

fn apply_edge_changes(
    edges: &mut BTreeSet<EncodedTerm>,
    changes: Option<&HashMap<EncodedTerm, bool>>,
) {
    let Some(changes) = changes else {
        return;
    };
    for (peer, live) in changes {
        if *live {
            edges.insert(peer.clone());
        } else {
            edges.remove(peer);
        }
    }
}

// ── Orphan detection ────────────────────────────────────────────────────────

pub fn orphaned_data_entities(snapshot: &GraphSnapshot) -> BTreeSet<EncodedTerm> {
    let root = snapshot.root();

    let mut data_entities: BTreeSet<EncodedTerm> = snapshot
        .triples
        .iter()
        .filter(|(s, p, o)| {
            p == &*RDF_TYPE && (o == &*SCHEMA_MEDIA_OBJECT || o == &*SCHEMA_DATASET) && s != &root
        })
        .map(|(s, _, _)| s.clone())
        .collect();
    let mut adjacency: HashMap<EncodedTerm, Vec<EncodedTerm>> = HashMap::new();
    for (s, p, o) in &snapshot.triples {
        if p == &*SCHEMA_HAS_PART {
            adjacency.entry(s.clone()).or_default().push(o.clone());
            if s != &root {
                data_entities.insert(s.clone());
            }
            if o != &root {
                data_entities.insert(o.clone());
            }
        }
    }

    if data_entities.is_empty() {
        return BTreeSet::new();
    }

    let mut reachable: HashSet<EncodedTerm> = HashSet::new();
    let mut queue: VecDeque<EncodedTerm> = VecDeque::new();
    queue.push_back(root.clone());
    reachable.insert(root);

    while let Some(current) = queue.pop_front() {
        if let Some(neighbors) = adjacency.get(&current) {
            for neighbor in neighbors {
                if reachable.insert(neighbor.clone()) {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    data_entities
        .into_iter()
        .filter(|entity| !reachable.contains(entity))
        .collect()
}

/// A bounded walk of the `hasPart` graph as it will be after the change set.
///
/// Owns the per-validation memos: children, parents, and settled reachability
/// verdicts are reused across every candidate entity.
struct ReachabilityWalk<'a> {
    view: &'a DeltaView<'a>,
    root: EncodedTerm,
    children: HashMap<EncodedTerm, BTreeSet<EncodedTerm>>,
    parents: HashMap<EncodedTerm, BTreeSet<EncodedTerm>>,
    reachable: HashMap<EncodedTerm, bool>,
    /// Entities on the current depth-first path. A node reached again while it
    /// is still being resolved counts as unreachable *through that edge*, which
    /// is what breaks `hasPart` cycles.
    visiting: HashSet<EncodedTerm>,
}

/// One entry of the explicit reachability stack.
struct ReachabilityFrame {
    entity: EncodedTerm,
    /// Remaining parents to try, in ascending term order.
    parents: std::vec::IntoIter<EncodedTerm>,
    reachable: bool,
    /// Whether reaching this verdict meant treating a node on the current path
    /// as unreachable. A `false` that leant on the cycle break is only valid
    /// *for this path*, so it must not be memoized.
    used_cycle_break: bool,
}

impl<'a> ReachabilityWalk<'a> {
    fn new(view: &'a DeltaView<'a>) -> Self {
        Self {
            view,
            root: graph_root(view.graph),
            children: HashMap::new(),
            parents: HashMap::new(),
            reachable: HashMap::new(),
            visiting: HashSet::new(),
        }
    }

    fn cached_children(
        &mut self,
        subject: &EncodedTerm,
    ) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        if let Some(children) = self.children.get(subject) {
            return Ok(children.clone());
        }
        let children = self.view.children(subject)?;
        self.children.insert(subject.clone(), children.clone());
        Ok(children)
    }

    fn cached_parents(
        &mut self,
        entity: &EncodedTerm,
    ) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        if let Some(parents) = self.parents.get(entity) {
            return Ok(parents.clone());
        }
        let parents = self.view.parents(entity)?;
        self.parents.insert(entity.clone(), parents.clone());
        Ok(parents)
    }

    /// Add `seed` and everything below it to `out`.
    fn collect_descendants(
        &mut self,
        seed: &EncodedTerm,
        out: &mut BTreeSet<EncodedTerm>,
    ) -> crate::store::Result<()> {
        let mut queue = VecDeque::from([seed.clone()]);
        while let Some(subject) = queue.pop_front() {
            if !out.insert(subject.clone()) {
                continue;
            }
            for child in self.cached_children(&subject)? {
                queue.push_back(child);
            }
        }
        Ok(())
    }

    /// An entity counts as a data entity if it is typed as one or takes part in
    /// any `hasPart` edge.
    fn is_data_entity(&mut self, entity: &EncodedTerm) -> crate::store::Result<bool> {
        let typed = self.view.triple_exists(TripleRef {
            subject: entity,
            predicate: &RDF_TYPE,
            object: &SCHEMA_DATASET,
        })? || self.view.triple_exists(TripleRef {
            subject: entity,
            predicate: &RDF_TYPE,
            object: &SCHEMA_MEDIA_OBJECT,
        })?;
        Ok(typed
            || !self.cached_children(entity)?.is_empty()
            || !self.cached_parents(entity)?.is_empty())
    }

    /// Is `entity` reachable from the root over `hasPart` edges?
    ///
    /// Explicit stack rather than recursion: a crate is free to contain a
    /// thousands-deep `hasPart` chain and a recursive walk overflows on it (K3).
    ///
    /// Cycles are broken by treating a node that is still on the current path as
    /// unreachable *through that edge*. Such a verdict is only valid for the path
    /// that produced it, so a `false` that leant on the break is returned but not
    /// memoized: in `root ▸ a`, `a ▸ b`, `b ▸ a`, resolving `a` first would
    /// otherwise cache "b is unreachable" and report a reachable entity as an
    /// orphan. A `true` never depends on the break — it is witnessed by a real
    /// path to the root — so it is always memoized.
    fn is_reachable(&mut self, entity: &EncodedTerm) -> crate::store::Result<bool> {
        if entity == &self.root {
            return Ok(true);
        }
        if let Some(reachable) = self.reachable.get(entity) {
            return Ok(*reachable);
        }
        if self.visiting.contains(entity) {
            return Ok(false);
        }

        let mut answer = false;
        let mut stack = vec![self.open_frame(entity.clone())?];
        while let Some(top) = stack.len().checked_sub(1) {
            if stack[top].reachable {
                let done = stack.pop().expect("stack is non-empty");
                self.settle(&done.entity, true);
                match stack.last_mut() {
                    Some(parent) => parent.reachable = true,
                    None => answer = true,
                }
                continue;
            }

            let Some(candidate) = stack[top].parents.next() else {
                let done = stack.pop().expect("stack is non-empty");
                self.visiting.remove(&done.entity);
                if !done.used_cycle_break {
                    self.reachable.insert(done.entity, false);
                }
                if let Some(parent) = stack.last_mut() {
                    parent.used_cycle_break |= done.used_cycle_break;
                }
                continue;
            };

            if candidate == self.root {
                stack[top].reachable = true;
            } else if let Some(reachable) = self.reachable.get(&candidate) {
                stack[top].reachable |= *reachable;
            } else if self.visiting.contains(&candidate) {
                stack[top].used_cycle_break = true;
            } else {
                let frame = self.open_frame(candidate)?;
                stack.push(frame);
            }
        }

        Ok(answer)
    }

    fn open_frame(&mut self, entity: EncodedTerm) -> crate::store::Result<ReachabilityFrame> {
        let parents = self.cached_parents(&entity)?;
        self.visiting.insert(entity.clone());
        Ok(ReachabilityFrame {
            entity,
            parents: Vec::from_iter(parents).into_iter(),
            reachable: false,
            used_cycle_break: false,
        })
    }

    fn settle(&mut self, entity: &EncodedTerm, reachable: bool) {
        self.visiting.remove(entity);
        self.reachable.insert(entity.clone(), reachable);
    }

    /// First orphan the change set would leave behind, if any.
    fn first_orphan(
        &mut self,
        summary: &DeltaSummary,
    ) -> crate::store::Result<Option<EncodedTerm>> {
        let mut candidates = BTreeSet::new();
        for seed in &summary.reachability_seeds {
            self.collect_descendants(seed, &mut candidates)?;
        }
        candidates.extend(summary.reachability_probes.iter().cloned());

        for entity in candidates {
            if entity == self.root {
                continue;
            }
            if self.is_data_entity(&entity)? && !self.is_reachable(&entity)? {
                return Ok(Some(entity));
            }
        }

        Ok(None)
    }
}

// ── Rule implementations ────────────────────────────────────────────────────

pub struct RootEntityRule;

impl Rule for RootEntityRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if !cx.delta_is_empty() && !cx.summary.touches_root_dataset {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let exists = cx.view.triple_exists(TripleRef {
            subject: &root,
            predicate: &RDF_TYPE,
            object: &SCHEMA_DATASET,
        })?;

        Ok(if exists {
            CandidateCheck::Pass
        } else {
            CandidateCheck::Violation(CrateViolation::missing_root(""))
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = post.root();
        if !post.has_triple(TripleRef {
            subject: &root,
            predicate: &RDF_TYPE,
            object: &SCHEMA_DATASET,
        }) {
            return Err(CrateViolation::missing_root(""));
        }
        Ok(())
    }
}

pub struct MetadataDescriptorRule;

impl Rule for MetadataDescriptorRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if !cx.delta_is_empty() && !cx.summary.touches_metadata_descriptor {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let has_type = cx.view.triple_exists(TripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &RDF_TYPE,
            object: &SCHEMA_CREATIVE_WORK,
        })?;
        let has_about = cx.view.triple_exists(TripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &SCHEMA_ABOUT,
            object: &root,
        })?;

        Ok(if has_type && has_about {
            CandidateCheck::Pass
        } else {
            CandidateCheck::Violation(CrateViolation::missing_descriptor(""))
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = post.root();
        let has_type = post.has_triple(TripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &RDF_TYPE,
            object: &SCHEMA_CREATIVE_WORK,
        });
        let has_about = post.has_triple(TripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &SCHEMA_ABOUT,
            object: &root,
        });

        if !has_type || !has_about {
            return Err(CrateViolation::missing_descriptor(""));
        }
        Ok(())
    }
}

pub struct RequiredRootPropertiesRule;

impl Rule for RequiredRootPropertiesRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if !cx.delta_is_empty() && !cx.summary.touches_any_required_root_property() {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        for ((predicate, label), touched) in REQUIRED_ROOT_PROPERTIES
            .iter()
            .zip(cx.summary.touches_required_root_properties)
        {
            if !cx.delta_is_empty() && !touched {
                continue;
            }
            let key = SubjectPredicate {
                subject: &root,
                predicate,
            };
            if cx.view.count_sp(key)? < 1 {
                return Ok(CandidateCheck::Violation(CrateViolation::missing_property(
                    cx.view.graph.as_str(),
                    label,
                    "",
                )));
            }
        }

        Ok(CandidateCheck::Pass)
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = post.root();
        for (predicate, label) in REQUIRED_ROOT_PROPERTIES.iter() {
            let key = SubjectPredicate {
                subject: &root,
                predicate,
            };
            if post.count_sp(key) < 1 {
                return Err(CrateViolation::missing_property(
                    post.graph.as_str(),
                    label,
                    "",
                ));
            }
        }
        Ok(())
    }
}

pub struct DatePublishedCardinalityRule;

impl Rule for DatePublishedCardinalityRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        let touched = cx.summary.touches_required_root_properties[DATE_PUBLISHED_SLOT];
        if !cx.delta_is_empty() && !touched {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let count = cx.view.count_sp(SubjectPredicate {
            subject: &root,
            predicate: &SCHEMA_DATE_PUBLISHED,
        })?;

        Ok(if count == 1 {
            CandidateCheck::Pass
        } else {
            CandidateCheck::Violation(CrateViolation::invalid_date(count, ""))
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = post.root();
        let count = post.count_sp(SubjectPredicate {
            subject: &root,
            predicate: &SCHEMA_DATE_PUBLISHED,
        });

        if count != 1 {
            return Err(CrateViolation::invalid_date(count, ""));
        }
        Ok(())
    }
}

pub struct EntityTypeRule;

impl Rule for EntityTypeRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if cx.delta_is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }

        // Only a subject whose types changed, or one that did not exist before,
        // can newly be untyped.
        let mut candidates = cx.summary.type_changed_subjects.clone();
        for subject in &cx.summary.inserted_subjects {
            if cx.view.stored_subject_triple_count(subject)? == 0 {
                candidates.insert(subject.clone());
            }
        }

        for subject in candidates {
            if cx.view.subject_triple_count(&subject)? == 0 {
                continue;
            }
            let key = SubjectPredicate {
                subject: &subject,
                predicate: &RDF_TYPE,
            };
            if cx.view.count_sp(key)? == 0 {
                return Ok(CandidateCheck::Violation(CrateViolation::missing_type(
                    subject.0, "",
                )));
            }
        }

        Ok(CandidateCheck::Pass)
    }

    /// Two passes over the triples (collect typed subjects, then scan for an
    /// untyped one) instead of one nested scan per subject.
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let typed: HashSet<&EncodedTerm> = post
            .triples
            .iter()
            .filter(|(_, p, _)| p == &*RDF_TYPE)
            .map(|(s, _, _)| s)
            .collect();

        for (subject, _, _) in &post.triples {
            if !typed.contains(subject) {
                return Err(CrateViolation::missing_type(subject.0.clone(), ""));
            }
        }
        Ok(())
    }
}

pub struct ReachabilityRule;

impl Rule for ReachabilityRule {
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if cx.delta_is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }
        if !cx.summary.touches_reachability {
            return Ok(CandidateCheck::Pass);
        }

        let mut walk = ReachabilityWalk::new(&cx.view);
        Ok(match walk.first_orphan(cx.summary)? {
            Some(entity) => CandidateCheck::Violation(CrateViolation::orphaned(entity.0, "")),
            None => CandidateCheck::Pass,
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        if let Some(entity) = orphaned_data_entities(post).into_iter().next() {
            return Err(CrateViolation::orphaned(entity.0, ""));
        }

        Ok(())
    }
}

/// Create the default set of RO-Crate 1.2 rules.
pub fn default_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(RootEntityRule),
        Box::new(MetadataDescriptorRule),
        Box::new(RequiredRootPropertiesRule),
        Box::new(DatePublishedCardinalityRule),
        Box::new(EntityTypeRule),
        Box::new(ReachabilityRule),
    ]
}

/// Run all rules against the state the change set would produce, collecting
/// violations.
pub fn validate_change_set(
    rules: &[Box<dyn Rule>],
    change_set: ChangeSet<'_>,
) -> std::result::Result<(), RuleEvaluationError> {
    let index = DeltaIndex::build(change_set.graph, change_set.delta);
    let summary = summarize_delta(change_set.graph, change_set.delta);
    let cx = RuleContext::new(DeltaView::new(change_set, &index)?, &summary);

    let mut violations = Vec::new();
    let mut post = None;

    for rule in rules {
        match rule.check_candidate(&cx)? {
            CandidateCheck::Pass => {}
            CandidateCheck::Violation(violation) => violations.push(violation),
            CandidateCheck::NeedSnapshot => {
                // Unreachable today: every rule that returns `NeedSnapshot`
                // guards it behind an empty delta, and this entry point is only
                // called with something to write. The fallback still applies the
                // delta, so a future rule cannot silently validate the pre-state.
                debug_assert!(
                    change_set.delta.is_empty(),
                    "a rule requested a snapshot for a non-empty change set",
                );
                if post.is_none() {
                    post = Some(post_state_after(change_set)?);
                }
                let post = post.as_ref().expect("post state materialized above");
                if let Err(violation) = rule.check_post_state(post) {
                    violations.push(violation);
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(RuleEvaluationError::Violations(violations))
    }
}

pub fn post_merge_violations_from_store(
    store: &GraphStore,
    graph: &GraphId,
) -> crate::store::Result<Vec<CrateViolation>> {
    let rules = default_rules();
    let change_set = ChangeSet {
        store,
        graph,
        delta: &[],
    };
    let index = DeltaIndex::build(graph, &[]);
    let summary = DeltaSummary::default();
    let cx = RuleContext::new(DeltaView::new(change_set, &index)?, &summary);

    let mut violations = Vec::new();
    let mut snapshot = None;

    for rule in &rules {
        match rule.check_candidate(&cx)? {
            CandidateCheck::Pass => {}
            CandidateCheck::Violation(violation) => violations.push(violation),
            CandidateCheck::NeedSnapshot => {
                if snapshot.is_none() {
                    snapshot = Some(GraphSnapshot::from_store(store, graph)?);
                }
                let snapshot = snapshot.as_ref().expect("snapshot materialized above");
                if let Err(violation) = rule.check_post_state(snapshot) {
                    violations.push(violation);
                }
            }
        }
    }

    Ok(violations)
}

/// Materialize the graph as the change set would leave it.
///
/// Only used by the `NeedSnapshot` fallback in [`validate_change_set`]; the
/// candidate checks never pay for this.
fn post_state_after(change_set: ChangeSet<'_>) -> crate::store::Result<GraphSnapshot> {
    let snapshot = GraphSnapshot::from_store(change_set.store, change_set.graph)?;
    let mut triples: HashSet<(EncodedTerm, EncodedTerm, EncodedTerm)> =
        snapshot.triples.into_iter().collect();

    for change in change_set.delta {
        match change {
            MaterializedQuadChange::Insert {
                graph,
                subject,
                predicate,
                object,
            } if graph == change_set.graph => {
                triples.insert((subject.clone(), predicate.clone(), object.clone()));
            }
            MaterializedQuadChange::Delete {
                graph,
                subject,
                predicate,
                object,
            } if graph == change_set.graph => {
                triples.remove(&(subject.clone(), predicate.clone(), object.clone()));
            }
            _ => {}
        }
    }

    Ok(GraphSnapshot::new(
        change_set.graph.clone(),
        triples.into_iter().collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The pre-`DeltaIndex` fold shapes, kept verbatim as an oracle. Each one
    /// re-scans the whole delta, which is exactly what the index replaces.
    mod naive {
        use super::*;

        pub fn triple_exists_after(base: bool, key: TripleRef<'_>, delta: &[Change]) -> bool {
            let mut exists = base;
            for change in delta {
                if change.subject == *key.subject
                    && change.predicate == *key.predicate
                    && change.object == *key.object
                {
                    exists = change.inserted;
                }
            }
            exists
        }

        pub fn count_sp_after(base: usize, key: SubjectPredicate<'_>, delta: &[Change]) -> usize {
            let mut count = base as i64;
            for change in delta {
                if change.subject == *key.subject && change.predicate == *key.predicate {
                    count += if change.inserted { 1 } else { -1 };
                }
            }
            count.max(0) as usize
        }

        pub fn subject_count_after(base: usize, subject: &EncodedTerm, delta: &[Change]) -> usize {
            let mut count = base as i64;
            for change in delta {
                if change.subject == *subject {
                    count += if change.inserted { 1 } else { -1 };
                }
            }
            count.max(0) as usize
        }

        pub fn children_after(
            base: &BTreeSet<EncodedTerm>,
            subject: &EncodedTerm,
            delta: &[Change],
        ) -> BTreeSet<EncodedTerm> {
            let mut children = base.clone();
            for change in delta {
                if change.subject != *subject || change.predicate != *SCHEMA_HAS_PART {
                    continue;
                }
                if change.inserted {
                    children.insert(change.object.clone());
                } else {
                    children.remove(&change.object);
                }
            }
            children
        }

        pub fn parents_after(
            base: &BTreeSet<EncodedTerm>,
            object: &EncodedTerm,
            delta: &[Change],
        ) -> BTreeSet<EncodedTerm> {
            let mut parents = base.clone();
            for change in delta {
                if change.object != *object || change.predicate != *SCHEMA_HAS_PART {
                    continue;
                }
                if change.inserted {
                    parents.insert(change.subject.clone());
                } else {
                    parents.remove(&change.subject);
                }
            }
            parents
        }
    }

    /// A delta entry in the shape the oracles read.
    #[derive(Clone, Debug)]
    struct Change {
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
        inserted: bool,
    }

    fn graph() -> GraphId {
        GraphId::new("urn:test:delta-index")
    }

    fn term(kind: &str, index: u8) -> EncodedTerm {
        EncodedTerm(format!("<urn:test:{kind}-{index}>"))
    }

    /// A small alphabet keeps duplicate and delete-then-reinsert cases dense.
    fn change_strategy() -> impl Strategy<Value = Change> {
        (0u8..3, 0u8..3, 0u8..3, any::<bool>(), any::<bool>()).prop_map(
            |(s, p, o, has_part, inserted)| Change {
                subject: term("s", s),
                predicate: if has_part {
                    SCHEMA_HAS_PART.clone()
                } else {
                    term("p", p)
                },
                object: term("o", o),
                inserted,
            },
        )
    }

    fn to_materialized(changes: &[Change]) -> Vec<MaterializedQuadChange> {
        let graph = graph();
        changes
            .iter()
            .map(|change| {
                let (subject, predicate, object) = (
                    change.subject.clone(),
                    change.predicate.clone(),
                    change.object.clone(),
                );
                if change.inserted {
                    MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject,
                        predicate,
                        object,
                    }
                } else {
                    MaterializedQuadChange::Delete {
                        graph: graph.clone(),
                        subject,
                        predicate,
                        object,
                    }
                }
            })
            .collect()
    }

    fn alphabet() -> Vec<EncodedTerm> {
        let mut terms: Vec<EncodedTerm> = (0..3).map(|i| term("s", i)).collect();
        terms.extend((0..3).map(|i| term("o", i)));
        terms
    }

    proptest! {
        /// Triple state is last-writer-wins: the index must agree with a fold
        /// that overwrites, not with one that counts.
        #[test]
        fn delta_index_matches_naive_triple_state(
            changes in proptest::collection::vec(change_strategy(), 0..24),
            base in any::<bool>(),
        ) {
            let index = DeltaIndex::build(&graph(), &to_materialized(&changes));
            let predicates: Vec<EncodedTerm> =
                std::iter::once(SCHEMA_HAS_PART.clone()).chain((0..3).map(|i| term("p", i))).collect();

            for subject in alphabet() {
                for predicate in &predicates {
                    for object in alphabet() {
                        let key = TripleRef { subject: &subject, predicate, object: &object };
                        let expected = naive::triple_exists_after(base, key, &changes);
                        let actual = index.triple_state(key).unwrap_or(base);
                        prop_assert_eq!(actual, expected);
                    }
                }
            }
        }

        /// Counts are order-independent sums, clamped at zero.
        #[test]
        fn delta_index_matches_naive_counts(
            changes in proptest::collection::vec(change_strategy(), 0..24),
            base in 0usize..4,
        ) {
            let index = DeltaIndex::build(&graph(), &to_materialized(&changes));
            let predicates: Vec<EncodedTerm> =
                std::iter::once(SCHEMA_HAS_PART.clone()).chain((0..3).map(|i| term("p", i))).collect();

            for subject in alphabet() {
                prop_assert_eq!(
                    saturating_total(base, index.subject_count_change(&subject)),
                    naive::subject_count_after(base, &subject, &changes)
                );
                for predicate in &predicates {
                    let key = SubjectPredicate { subject: &subject, predicate };
                    prop_assert_eq!(
                        saturating_total(base, index.count_change(key)),
                        naive::count_sp_after(base, key, &changes)
                    );
                }
            }
        }

        /// `hasPart` edges apply in delta order per edge, in both directions.
        #[test]
        fn delta_index_matches_naive_has_part_edges(
            changes in proptest::collection::vec(change_strategy(), 0..24),
            base_index in 0usize..3,
        ) {
            let index = DeltaIndex::build(&graph(), &to_materialized(&changes));
            let base: BTreeSet<EncodedTerm> = alphabet().into_iter().take(base_index).collect();

            for entity in alphabet() {
                let mut children = base.clone();
                apply_edge_changes(&mut children, index.has_part_children(&entity));
                prop_assert_eq!(children, naive::children_after(&base, &entity, &changes));

                let mut parents = base.clone();
                apply_edge_changes(&mut parents, index.has_part_parents(&entity));
                prop_assert_eq!(parents, naive::parents_after(&base, &entity, &changes));
            }
        }
    }

    /// A delete followed by a re-insert of the same triple leaves it present;
    /// the reverse leaves it absent. Pinning the LWW direction explicitly.
    #[test]
    fn delta_index_last_writer_wins_on_reinsert() {
        let graph = graph();
        let (subject, predicate, object) = (term("s", 0), term("p", 0), term("o", 0));
        let key = TripleRef {
            subject: &subject,
            predicate: &predicate,
            object: &object,
        };

        let delete_then_insert = to_materialized(&[
            Change {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
                inserted: false,
            },
            Change {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
                inserted: true,
            },
        ]);
        let mut reversed = delete_then_insert.clone();
        reversed.reverse();

        assert_eq!(
            DeltaIndex::build(&graph, &delete_then_insert).triple_state(key),
            Some(true)
        );
        assert_eq!(
            DeltaIndex::build(&graph, &reversed).triple_state(key),
            Some(false)
        );
    }

    /// Changes aimed at another graph never enter the index.
    #[test]
    fn delta_index_ignores_foreign_graph_changes() {
        let (subject, predicate, object) = (term("s", 0), term("p", 0), term("o", 0));
        let foreign = vec![MaterializedQuadChange::Insert {
            graph: GraphId::new("urn:test:other"),
            subject: subject.clone(),
            predicate: predicate.clone(),
            object: object.clone(),
        }];
        let index = DeltaIndex::build(&graph(), &foreign);
        assert_eq!(
            index.triple_state(TripleRef {
                subject: &subject,
                predicate: &predicate,
                object: &object
            }),
            None
        );
    }
}
