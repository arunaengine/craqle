use std::cell::OnceCell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::LazyLock;

use crate::core::{CrateViolation, EncodedTerm, GraphId, MaterializedQuadChange, QuadOp, vocab};
use crate::query_context::{QueryCancellation, ReadContext};
use crate::rdf_read::{GraphSelector, QuadPattern, RdfReadView, StoreReadView};
use crate::store::GraphStore;
use crate::validation_delta::{DeltaImpact, DeltaIndex, DeltaReadView};
use crate::RoCrateVersion;

#[cfg(test)]
thread_local! {
    static SNAPSHOT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

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

static DCTERMS_CONFORMS_TO: LazyLock<EncodedTerm> =
    LazyLock::new(|| EncodedTerm("<http://purl.org/dc/terms/conformsTo>".to_string()));

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
pub(crate) struct EncodedTripleRef<'a> {
    pub subject: &'a EncodedTerm,
    pub predicate: &'a EncodedTerm,
    pub object: &'a EncodedTerm,
}

/// A `(subject, predicate)` lookup key.
#[derive(Clone, Copy)]
pub(crate) struct SubjectPredicate<'a> {
    pub subject: &'a EncodedTerm,
    pub predicate: &'a EncodedTerm,
}

/// The change set under validation: what is being written, to which graph, on
/// top of which store.
#[derive(Clone, Copy)]
pub(crate) struct ChangeSet<'a> {
    pub store: &'a GraphStore,
    pub graph: &'a GraphId,
    pub delta: &'a [MaterializedQuadChange],
}

// ── Snapshots ───────────────────────────────────────────────────────────────

/// A snapshot view of a graph for validation.
pub(crate) struct GraphSnapshot {
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

    /// Build a snapshot by streaming the shared validation read interface.
    pub(crate) fn from_store(store: &GraphStore, graph: &GraphId) -> crate::store::Result<Self> {
        let view = StoreReadView::new(store);
        let context = ReadContext::for_validation(QueryCancellation::new(), graph);
        Self::from_read_view(&view, &context, graph)
    }

    fn from_read_view(
        view: &impl RdfReadView,
        context: &ReadContext<'_>,
        graph: &GraphId,
    ) -> crate::store::Result<Self> {
        #[cfg(test)]
        SNAPSHOT_READS.with(|reads| reads.set(reads.get().saturating_add(1)));
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let Some(graph_tid) = view.lookup_term(context, &graph_term)? else {
            return Ok(Self::new(graph.clone(), Vec::new()));
        };
        let mut triples = Vec::new();
        let cursor = view.scan(
            context,
            GraphSelector::Named(graph_tid),
            QuadPattern::default(),
        )?;
        for quad in cursor {
            let q = quad?;
            let s = view.decode_term(context, q.subject)?;
            let p = view.decode_term(context, q.predicate)?;
            let o = view.decode_term(context, q.object)?;
            triples.push((s, p, o));
        }
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
    fn has_triple(&self, triple: EncodedTripleRef<'_>) -> bool {
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
pub(crate) struct RuleContext<'a> {
    view: DeltaReadView<'a, 'a>,
    context: ReadContext<'a>,
    graph: &'a GraphId,
    impact: &'a DeltaImpact,
    summary: &'a DeltaSummary,
    profile: OnceCell<Option<RoCrateVersion>>,
}

impl<'a> RuleContext<'a> {
    fn new(change_set: ChangeSet<'a>, index: &'a DeltaIndex, summary: &'a DeltaSummary) -> Self {
        Self {
            view: DeltaReadView::new(StoreReadView::new(change_set.store), index),
            context: ReadContext::for_validation(QueryCancellation::new(), change_set.graph),
            graph: change_set.graph,
            impact: index.impact(),
            summary,
            profile: OnceCell::new(),
        }
    }

    fn root(&self) -> EncodedTerm {
        graph_root(self.graph)
    }

    fn delta_is_empty(&self) -> bool {
        self.impact.changed_subjects.is_empty()
    }

    fn profile(&self) -> crate::store::Result<Option<RoCrateVersion>> {
        if let Some(profile) = self.profile.get() {
            return Ok(*profile);
        }
        let profile = profile_from_view(self)?;
        let _ = self.profile.set(profile);
        Ok(profile)
    }

    fn triple_exists(&self, triple: EncodedTripleRef<'_>) -> crate::store::Result<bool> {
        let (Some(subject), Some(predicate), Some(object)) = (
            self.view.lookup_term(&self.context, triple.subject)?,
            self.view.lookup_term(&self.context, triple.predicate)?,
            self.view.lookup_term(&self.context, triple.object)?,
        ) else {
            return Ok(false);
        };
        self.view.exists(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            QuadPattern {
                subject: Some(subject),
                predicate: Some(predicate),
                object: Some(object),
                ..QuadPattern::default()
            },
        )
    }

    fn has_subject_predicate(&self, key: SubjectPredicate<'_>) -> crate::store::Result<bool> {
        let (Some(subject), Some(predicate)) = (
            self.view.lookup_term(&self.context, key.subject)?,
            self.view.lookup_term(&self.context, key.predicate)?,
        ) else {
            return Ok(false);
        };
        self.view.exists(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            QuadPattern {
                subject: Some(subject),
                predicate: Some(predicate),
                ..QuadPattern::default()
            },
        )
    }

    fn count_sp(&self, key: SubjectPredicate<'_>) -> crate::store::Result<usize> {
        let (Some(subject), Some(predicate)) = (
            self.view.lookup_term(&self.context, key.subject)?,
            self.view.lookup_term(&self.context, key.predicate)?,
        ) else {
            return Ok(0);
        };
        let count = self.view.count_up_to(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            QuadPattern {
                subject: Some(subject),
                predicate: Some(predicate),
                ..QuadPattern::default()
            },
            u64::MAX,
        )?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    fn subject_has_triples(&self, subject: &EncodedTerm) -> crate::store::Result<bool> {
        let Some(subject) = self.view.lookup_term(&self.context, subject)? else {
            return Ok(false);
        };
        self.view.exists(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            QuadPattern {
                subject: Some(subject),
                ..QuadPattern::default()
            },
        )
    }

    fn subject_existed_before(&self, subject: &EncodedTerm) -> crate::store::Result<bool> {
        let Some(subject) = self.view.lookup_term(&self.context, subject)? else {
            return Ok(false);
        };
        self.view.base_subject_exists(&self.context, subject)
    }

    fn children(&self, subject: &EncodedTerm) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let (Some(subject), Some(predicate)) = (
            self.view.lookup_term(&self.context, subject)?,
            self.view.lookup_term(&self.context, &SCHEMA_HAS_PART)?,
        ) else {
            return Ok(BTreeSet::new());
        };
        let mut children = BTreeSet::new();
        let cursor = self.view.forward_predicate(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            subject,
            predicate,
        )?;
        for quad in cursor {
            children.insert(self.view.decode_term(&self.context, quad?.object)?);
        }
        Ok(children)
    }

    fn parents(&self, object: &EncodedTerm) -> crate::store::Result<BTreeSet<EncodedTerm>> {
        let (Some(predicate), Some(object)) = (
            self.view.lookup_term(&self.context, &SCHEMA_HAS_PART)?,
            self.view.lookup_term(&self.context, object)?,
        ) else {
            return Ok(BTreeSet::new());
        };
        let mut parents = BTreeSet::new();
        let cursor = self.view.inverse_predicate(
            &self.context,
            GraphSelector::Named(self.view.graph()),
            predicate,
            object,
        )?;
        for quad in cursor {
            parents.insert(self.view.decode_term(&self.context, quad?.subject)?);
        }
        Ok(parents)
    }
}

pub(crate) trait Rule: Send + Sync {
    fn rule_id(&self) -> Option<RuleId> {
        None
    }

    /// Validate against the delta alone where possible. Returning
    /// [`CandidateCheck::NeedSnapshot`] falls back to materialising the whole
    /// post state.
    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck>;

    /// Validate the post-state snapshot. Return `Err` if a violation is found.
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuleId {
    Root,
    Descriptor,
    RequiredProperties,
    DatePublished,
    EntityType,
    Reachability,
}

static ROCRATE_RULES: [RuleId; 6] = [
    RuleId::Root,
    RuleId::Descriptor,
    RuleId::RequiredProperties,
    RuleId::DatePublished,
    RuleId::EntityType,
    RuleId::Reachability,
];

fn rule_plan(version: RoCrateVersion) -> &'static [RuleId] {
    match version {
        RoCrateVersion::V1_1 => &ROCRATE_RULES,
        RoCrateVersion::V1_2 => &ROCRATE_RULES,
        RoCrateVersion::V1_3 => &ROCRATE_RULES,
    }
}

fn profile_from_view(cx: &RuleContext<'_>) -> crate::store::Result<Option<RoCrateVersion>> {
    let mut selected = None;
    for subject in [METADATA_DESCRIPTOR.clone(), cx.root()] {
        for version in [
            RoCrateVersion::V1_1,
            RoCrateVersion::V1_2,
            RoCrateVersion::V1_3,
        ] {
            let object = EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                version.specification_url(),
            ));
            if cx.triple_exists(EncodedTripleRef {
                subject: &subject,
                predicate: &DCTERMS_CONFORMS_TO,
                object: &object,
            })? {
                if selected.is_some_and(|current| current != version) {
                    return Ok(None);
                }
                selected = Some(version);
            }
        }
    }
    Ok(selected)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CandidateCheck {
    Pass,
    Violation(CrateViolation),
    NeedSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RuleEvaluationError {
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("violations: {0:?}")]
    Violations(Vec<CrateViolation>),
}

// ── Delta summary ───────────────────────────────────────────────────────────

/// What a change set touches, in the terms the rules care about. Cheap to
/// compute (one pass over the delta) and shared by every rule.
#[derive(Debug, Default, Clone)]
pub(crate) struct DeltaSummary {
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
    pub(crate) fn touches_reachability(&self) -> bool {
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
pub(crate) fn summarize_delta(graph: &GraphId, delta: &[MaterializedQuadChange]) -> DeltaSummary {
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
pub(crate) fn summarize_ops(graph: &GraphId, ops: &[QuadOp]) -> DeltaSummary {
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

// ── Orphan detection ────────────────────────────────────────────────────────

pub(crate) fn orphaned_data_entities(snapshot: &GraphSnapshot) -> BTreeSet<EncodedTerm> {
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
    view: &'a RuleContext<'a>,
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
    fn new(view: &'a RuleContext<'a>) -> Self {
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
        let typed = self.view.triple_exists(EncodedTripleRef {
            subject: entity,
            predicate: &RDF_TYPE,
            object: &SCHEMA_DATASET,
        })? || self.view.triple_exists(EncodedTripleRef {
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

pub(crate) struct RootEntityRule;

impl Rule for RootEntityRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::Root)
    }

    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if !cx.delta_is_empty() && !cx.summary.touches_root_dataset {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let exists = cx.triple_exists(EncodedTripleRef {
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
        if !post.has_triple(EncodedTripleRef {
            subject: &root,
            predicate: &RDF_TYPE,
            object: &SCHEMA_DATASET,
        }) {
            return Err(CrateViolation::missing_root(""));
        }
        Ok(())
    }
}

pub(crate) struct MetadataDescriptorRule;

impl Rule for MetadataDescriptorRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::Descriptor)
    }

    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if !cx.delta_is_empty() && !cx.summary.touches_metadata_descriptor {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let has_type = cx.triple_exists(EncodedTripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &RDF_TYPE,
            object: &SCHEMA_CREATIVE_WORK,
        })?;
        let has_about = cx.triple_exists(EncodedTripleRef {
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
        let has_type = post.has_triple(EncodedTripleRef {
            subject: &METADATA_DESCRIPTOR,
            predicate: &RDF_TYPE,
            object: &SCHEMA_CREATIVE_WORK,
        });
        let has_about = post.has_triple(EncodedTripleRef {
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

pub(crate) struct RequiredRootPropertiesRule;

impl Rule for RequiredRootPropertiesRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::RequiredProperties)
    }

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
            if !cx.has_subject_predicate(key)? {
                return Ok(CandidateCheck::Violation(CrateViolation::missing_property(
                    cx.graph.as_str(),
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

pub(crate) struct DatePublishedCardinalityRule;

impl Rule for DatePublishedCardinalityRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::DatePublished)
    }

    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        let touched = cx.summary.touches_required_root_properties[DATE_PUBLISHED_SLOT];
        if !cx.delta_is_empty() && !touched {
            return Ok(CandidateCheck::Pass);
        }

        let root = cx.root();
        let count = cx.count_sp(SubjectPredicate {
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

pub(crate) struct EntityTypeRule;

impl Rule for EntityTypeRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::EntityType)
    }

    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if cx.delta_is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }

        // Only a subject whose types changed, or one that did not exist before,
        // can newly be untyped.
        let mut candidates = cx.summary.type_changed_subjects.clone();
        for subject in &cx.summary.inserted_subjects {
            if !cx.subject_existed_before(subject)? {
                candidates.insert(subject.clone());
            }
        }

        for subject in candidates {
            if !cx.subject_has_triples(&subject)? {
                continue;
            }
            let key = SubjectPredicate {
                subject: &subject,
                predicate: &RDF_TYPE,
            };
            if !cx.has_subject_predicate(key)? {
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

pub(crate) struct ReachabilityRule;

impl Rule for ReachabilityRule {
    fn rule_id(&self) -> Option<RuleId> {
        Some(RuleId::Reachability)
    }

    fn check_candidate(&self, cx: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
        if cx.delta_is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }
        if !cx.summary.touches_reachability {
            return Ok(CandidateCheck::Pass);
        }

        let mut walk = ReachabilityWalk::new(cx);
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

/// Create the default RO-Crate rule set for the supported versions.
pub(crate) fn default_rules() -> Vec<Box<dyn Rule>> {
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
pub(crate) fn validate_change_set(
    rules: &[Box<dyn Rule>],
    change_set: ChangeSet<'_>,
) -> std::result::Result<(), RuleEvaluationError> {
    let index = DeltaIndex::build(change_set.store, change_set.graph, change_set.delta)?;
    let summary = summarize_delta(change_set.graph, change_set.delta);
    let cx = RuleContext::new(change_set, &index, &summary);
    let plan = if rules.iter().any(|rule| rule.rule_id().is_some()) {
        cx.profile()?.map(rule_plan)
    } else {
        None
    };

    let mut violations = Vec::new();
    let mut post = None;

    for rule in rules {
        if rule
            .rule_id()
            .is_some_and(|id| plan.is_some_and(|plan| !plan.contains(&id)))
        {
            continue;
        }
        match rule.check_candidate(&cx)? {
            CandidateCheck::Pass => {}
            CandidateCheck::Violation(violation) => violations.push(violation),
            CandidateCheck::NeedSnapshot => {
                if post.is_none() {
                    post = Some(post_state_after(&cx)?);
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

pub(crate) fn post_merge_violations_from_store(
    store: &GraphStore,
    graph: &GraphId,
) -> crate::store::Result<Vec<CrateViolation>> {
    let rules = default_rules();
    let change_set = ChangeSet {
        store,
        graph,
        delta: &[],
    };
    let index = DeltaIndex::build(store, graph, &[])?;
    let summary = DeltaSummary::default();
    let cx = RuleContext::new(change_set, &index, &summary);
    let plan = cx.profile()?.map(rule_plan);

    let mut violations = Vec::new();
    let mut snapshot = None;

    for rule in &rules {
        if rule
            .rule_id()
            .is_some_and(|id| plan.is_some_and(|plan| !plan.contains(&id)))
        {
            continue;
        }
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
fn post_state_after(cx: &RuleContext<'_>) -> crate::store::Result<GraphSnapshot> {
    GraphSnapshot::from_read_view(&cx.view, &cx.context, cx.graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// W1 — the diagnostics fast path must agree with a full recompute.
    ///
    /// `ReplicationEngine::settle_diagnostics` skips the recompute whenever
    /// `DeltaSummary::touches_reachability` is clear, and re-stamps the previous
    /// verdict against the new clock instead. A triple shape that
    /// `summarize` fails to recognise would therefore persist a *wrong* orphan
    /// set carrying a matching tag, and nothing ever re-checks such a record —
    /// not the open-time repair pass, not a later read. These tests drive the
    /// engine through the same public entry points the node uses and compare
    /// the verdict it leaves behind against a from-scratch recompute of the
    /// same final graph, while pinning which side of the branch was taken.
    mod diagnostics_fast_path {
        use std::sync::Arc;

        use super::*;
        use crate::core::{ActorId, GraphDiagnostics};
        use crate::replication::ReplicationEngine;
        use crate::search::SearchIndex;
        use crate::sparql::SparqlEngine;
        use crate::store::GraphStore;

        type Triple = (EncodedTerm, EncodedTerm, EncodedTerm);

        const CHILD: &str = "urn:test:fast-path:child";
        const LOOSE: &str = "urn:test:fast-path:loose";
        const EXTRA: &str = "urn:test:fast-path:extra";

        fn iri(value: &str) -> EncodedTerm {
            EncodedTerm(format!("<{value}>"))
        }

        fn text(value: &str) -> EncodedTerm {
            EncodedTerm(format!("\"{value}\""))
        }

        fn engine_at(dir: &std::path::Path) -> (Arc<GraphStore>, ReplicationEngine) {
            let store = Arc::new(GraphStore::open(dir).unwrap());
            let search = Arc::new(SearchIndex::open_in_memory().unwrap());
            let sparql = Arc::new(SparqlEngine::new(store.clone(), search));
            let engine = ReplicationEngine::new(store.clone(), sparql, ActorId::random());
            (store, engine)
        }

        fn inserts(graph: &GraphId, triples: &[Triple]) -> Vec<MaterializedQuadChange> {
            triples
                .iter()
                .map(
                    |(subject, predicate, object)| MaterializedQuadChange::Insert {
                        graph: graph.clone(),
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: object.clone(),
                    },
                )
                .collect()
        }

        fn deletes(graph: &GraphId, triples: &[Triple]) -> Vec<MaterializedQuadChange> {
            triples
                .iter()
                .map(
                    |(subject, predicate, object)| MaterializedQuadChange::Delete {
                        graph: graph.clone(),
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: object.clone(),
                    },
                )
                .collect()
        }

        /// Root Dataset, one reachable child, and one detached data entity, so
        /// every case starts from a graph that already has an orphan and can
        /// gain or lose one in either direction.
        fn seed(graph: &GraphId) -> Vec<Triple> {
            vec![
                (
                    iri(graph.as_str()),
                    RDF_TYPE.clone(),
                    SCHEMA_DATASET.clone(),
                ),
                (iri(graph.as_str()), SCHEMA_HAS_PART.clone(), iri(CHILD)),
                (iri(CHILD), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
                (iri(CHILD), SCHEMA_NAME.clone(), text("child")),
                (iri(LOOSE), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
            ]
        }

        /// The orphan set a store that has never seen a diagnostics record
        /// computes for `triples`. Written through the deferred plan, so the
        /// stored record's tag is stale and the read recomputes from the quads.
        fn recomputed(graph: &GraphId, triples: &[Triple]) -> GraphDiagnostics {
            let dir = tempfile::tempdir().unwrap();
            let (store, engine) = engine_at(dir.path());
            engine
                .local_apply_changes_bulk_unchecked(graph, inserts(graph, triples))
                .unwrap();
            store.graph_diagnostics(graph).unwrap()
        }

        fn apply_change(triples: &mut Vec<Triple>, change: &MaterializedQuadChange) {
            match change {
                MaterializedQuadChange::Insert {
                    subject,
                    predicate,
                    object,
                    ..
                } => {
                    let triple = (subject.clone(), predicate.clone(), object.clone());
                    if !triples.contains(&triple) {
                        triples.push(triple);
                    }
                }
                MaterializedQuadChange::Delete {
                    subject,
                    predicate,
                    object,
                    ..
                } => triples
                    .retain(|held| held != &(subject.clone(), predicate.clone(), object.clone())),
            }
        }

        struct Case {
            label: &'static str,
            changes: Vec<MaterializedQuadChange>,
            fast_path: bool,
        }

        fn cases(graph: &GraphId) -> Vec<Case> {
            let root = iri(graph.as_str());
            vec![
                Case {
                    label: "types a new entity as a MediaObject",
                    changes: inserts(
                        graph,
                        &[(iri(EXTRA), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone())],
                    ),
                    fast_path: false,
                },
                Case {
                    label: "types a new entity as a Dataset",
                    changes: inserts(
                        graph,
                        &[(iri(EXTRA), RDF_TYPE.clone(), SCHEMA_DATASET.clone())],
                    ),
                    fast_path: false,
                },
                Case {
                    label: "attaches the detached entity",
                    changes: inserts(
                        graph,
                        &[(root.clone(), SCHEMA_HAS_PART.clone(), iri(LOOSE))],
                    ),
                    fast_path: false,
                },
                Case {
                    label: "detaches the reachable child",
                    changes: deletes(
                        graph,
                        &[(root.clone(), SCHEMA_HAS_PART.clone(), iri(CHILD))],
                    ),
                    fast_path: false,
                },
                Case {
                    label: "untypes the detached entity",
                    changes: deletes(
                        graph,
                        &[(iri(LOOSE), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone())],
                    ),
                    fast_path: false,
                },
                Case {
                    label: "detaches then re-attaches the same edge",
                    changes: deletes(
                        graph,
                        &[(root.clone(), SCHEMA_HAS_PART.clone(), iri(CHILD))],
                    )
                    .into_iter()
                    .chain(inserts(
                        graph,
                        &[(root.clone(), SCHEMA_HAS_PART.clone(), iri(CHILD))],
                    ))
                    .collect(),
                    fast_path: false,
                },
                Case {
                    label: "touches an unrelated predicate on an existing entity",
                    changes: inserts(
                        graph,
                        &[(iri(CHILD), SCHEMA_DESCRIPTION.clone(), text("about"))],
                    ),
                    fast_path: true,
                },
                Case {
                    label: "touches an unrelated predicate on a new subject",
                    changes: inserts(graph, &[(iri(EXTRA), SCHEMA_NAME.clone(), text("extra"))]),
                    fast_path: true,
                },
                Case {
                    label: "types an entity as something that is not a data entity",
                    changes: inserts(
                        graph,
                        &[(iri(EXTRA), RDF_TYPE.clone(), SCHEMA_CREATIVE_WORK.clone())],
                    ),
                    fast_path: true,
                },
                Case {
                    label: "removes a name and puts it back",
                    changes: deletes(graph, &[(iri(CHILD), SCHEMA_NAME.clone(), text("child"))])
                        .into_iter()
                        .chain(inserts(
                            graph,
                            &[(iri(CHILD), SCHEMA_NAME.clone(), text("renamed"))],
                        ))
                        .collect(),
                    fast_path: true,
                },
            ]
        }

        #[test]
        fn fastpath_matches_recompute() {
            let graph = GraphId::new("urn:test:fast-path");
            for case in cases(&graph) {
                let dir = tempfile::tempdir().unwrap();
                let (store, engine) = engine_at(dir.path());
                let mut triples = seed(&graph);
                engine
                    .local_apply_changes_unchecked(&graph, inserts(&graph, &triples))
                    .unwrap();

                let before = store.diagnostics_compute_count();
                engine
                    .local_apply_changes_unchecked(&graph, case.changes.clone())
                    .unwrap();
                let settled = store.diagnostics_compute_count();
                assert_eq!(
                    case.fast_path,
                    settled == before,
                    "`{}` took the wrong branch: {} recomputes",
                    case.label,
                    settled - before
                );

                // Reading the verdict must not recompute it: the record the
                // fast path persisted has to carry the post-write clock tag.
                let observed = store.graph_diagnostics(&graph).unwrap();
                assert_eq!(
                    settled,
                    store.diagnostics_compute_count(),
                    "`{}` left a record a reader has to repair",
                    case.label
                );

                for change in &case.changes {
                    apply_change(&mut triples, change);
                }
                assert_eq!(
                    recomputed(&graph, &triples),
                    observed,
                    "`{}` persisted an orphan set a full recompute disagrees with",
                    case.label
                );
            }
        }

        /// A validated write that touches reachability derives its orphan set
        /// rather than inheriting one from validation.
        #[test]
        fn write_derives_orphans() {
            let graph = GraphId::new("urn:test:fast-path-validated");
            let dir = tempfile::tempdir().unwrap();
            let (store, engine) = engine_at(dir.path());

            let mut triples: Vec<Triple> = vec![
                (
                    iri(graph.as_str()),
                    RDF_TYPE.clone(),
                    SCHEMA_DATASET.clone(),
                ),
                (iri(graph.as_str()), SCHEMA_HAS_PART.clone(), iri(CHILD)),
                (iri(CHILD), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
            ];
            engine
                .local_apply_changes_unchecked(&graph, inserts(&graph, &triples))
                .unwrap();
            assert!(!store.graph_diagnostics(&graph).unwrap().has_orphans());

            // Reachability *is* touched, so this must recompute. Trusting
            // validation to imply orphan-freeness was unsound: two writes that
            // each validate against the same pre-state can jointly orphan an
            // entity, and stamping clean recorded that as permanent.
            let grow = inserts(
                &graph,
                &[
                    (iri(graph.as_str()), SCHEMA_HAS_PART.clone(), iri(EXTRA)),
                    (iri(EXTRA), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
                ],
            );
            let before = store.diagnostics_compute_count();
            engine.local_apply_changes(&graph, grow.clone()).unwrap();
            assert!(
                store.diagnostics_compute_count() > before,
                "a validated write touching reachability must derive the orphan set, not assume it"
            );

            let observed = store.graph_diagnostics(&graph).unwrap();
            for change in &grow {
                apply_change(&mut triples, change);
            }
            assert_eq!(recomputed(&graph, &triples), observed);
            assert!(!observed.has_orphans());
        }
    }

    mod delta_differential {
        use std::collections::BTreeSet;

        use super::*;
        use crate::core::{ActorId, Dot};
        use crate::store::{ClockUpdate, CounterKey, QuadAdd};
        use proptest::prelude::*;

        type Triple = (EncodedTerm, EncodedTerm, EncodedTerm);

        fn put(store: &GraphStore, graph: &GraphId, triple: &Triple) {
            if !store.contains_graph(graph).unwrap() {
                store.create_graph(graph).unwrap();
            }
            let graph_id = store
                .resolve_term(&EncodedTerm::from_named_node(&graph.0))
                .unwrap();
            let quad = crate::store::EncodedQuad {
                graph: graph_id,
                subject: store.resolve_term(&triple.0).unwrap(),
                predicate: store.resolve_term(&triple.1).unwrap(),
                object: store.resolve_term(&triple.2).unwrap(),
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
        }

        fn literal(value: &str) -> EncodedTerm {
            EncodedTerm(format!("\"{value}\""))
        }

        fn valid_base(graph: &GraphId) -> Vec<Triple> {
            let root = graph_root(graph);
            let child = EncodedTerm("<urn:test:differential:child>".to_string());
            vec![
                (root.clone(), RDF_TYPE.clone(), SCHEMA_DATASET.clone()),
                (root.clone(), SCHEMA_NAME.clone(), literal("base")),
                (root.clone(), SCHEMA_DESCRIPTION.clone(), literal("base")),
                (
                    root.clone(),
                    SCHEMA_DATE_PUBLISHED.clone(),
                    literal("2026-01-01"),
                ),
                (
                    METADATA_DESCRIPTOR.clone(),
                    RDF_TYPE.clone(),
                    SCHEMA_CREATIVE_WORK.clone(),
                ),
                (
                    METADATA_DESCRIPTOR.clone(),
                    SCHEMA_ABOUT.clone(),
                    root.clone(),
                ),
                (root.clone(), SCHEMA_HAS_PART.clone(), child.clone()),
                (child, RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
            ]
        }

        fn version_iri(version: RoCrateVersion) -> EncodedTerm {
            EncodedTerm::from_named_node(&oxrdf::NamedNode::new_unchecked(
                version.specification_url(),
            ))
        }

        #[test]
        fn profiles_use_markers() {
            let directory = tempfile::tempdir().unwrap();
            let store = GraphStore::open(directory.path()).unwrap();

            for version in [
                RoCrateVersion::V1_1,
                RoCrateVersion::V1_2,
                RoCrateVersion::V1_3,
            ] {
                let graph = GraphId::new(&format!("urn:test:rules-profile:{version:?}"));
                let mut triples = valid_base(&graph);
                triples.retain(|(_, predicate, _)| predicate != &*SCHEMA_NAME);
                triples.push((
                    METADATA_DESCRIPTOR.clone(),
                    DCTERMS_CONFORMS_TO.clone(),
                    version_iri(version),
                ));
                for triple in &triples {
                    put(&store, &graph, triple);
                }

                let index = DeltaIndex::build(&store, &graph, &[]).unwrap();
                let summary = DeltaSummary::default();
                let cx = RuleContext::new(
                    ChangeSet {
                        store: &store,
                        graph: &graph,
                        delta: &[],
                    },
                    &index,
                    &summary,
                );
                assert_eq!(cx.profile().unwrap(), Some(version));
                assert_eq!(rule_plan(version), ROCRATE_RULES.as_slice());

                let violations = match validate_change_set(
                    &default_rules(),
                    ChangeSet {
                        store: &store,
                        graph: &graph,
                        delta: &[],
                    },
                ) {
                    Err(RuleEvaluationError::Violations(violations)) => violations,
                    other => panic!("expected missing property violation, got {other:?}"),
                };
                assert_eq!(
                    violations
                        .iter()
                        .map(|violation| violation.code)
                        .collect::<Vec<_>>(),
                    ["missing_required_property"]
                );
            }
        }

        #[test]
        fn profile_uses_delta() {
            let directory = tempfile::tempdir().unwrap();
            let store = GraphStore::open(directory.path()).unwrap();
            let graph = GraphId::new("urn:test:rules-profile-delta");
            let old = (
                METADATA_DESCRIPTOR.clone(),
                DCTERMS_CONFORMS_TO.clone(),
                version_iri(RoCrateVersion::V1_2),
            );
            put(&store, &graph, &old);
            let new = (
                METADATA_DESCRIPTOR.clone(),
                DCTERMS_CONFORMS_TO.clone(),
                version_iri(RoCrateVersion::V1_1),
            );
            let delta = vec![
                materialize(&graph, old, false),
                materialize(&graph, new, true),
            ];
            let index = DeltaIndex::build(&store, &graph, &delta).unwrap();
            let summary = summarize_delta(&graph, &delta);
            let cx = RuleContext::new(
                ChangeSet {
                    store: &store,
                    graph: &graph,
                    delta: &delta,
                },
                &index,
                &summary,
            );
            assert_eq!(cx.profile().unwrap(), Some(RoCrateVersion::V1_1));
            let statistics = cx.context.snapshot();
            assert!(statistics.index_seeks <= 6);
            assert_eq!(statistics.terms_decoded, 0);
        }

        #[test]
        fn profile_rejects_conflict() {
            let directory = tempfile::tempdir().unwrap();
            let store = GraphStore::open(directory.path()).unwrap();
            let graph = GraphId::new("urn:test:rules-profile-conflict");
            for triple in [
                (
                    METADATA_DESCRIPTOR.clone(),
                    DCTERMS_CONFORMS_TO.clone(),
                    version_iri(RoCrateVersion::V1_2),
                ),
                (
                    graph_root(&graph),
                    DCTERMS_CONFORMS_TO.clone(),
                    version_iri(RoCrateVersion::V1_1),
                ),
            ] {
                put(&store, &graph, &triple);
            }
            let index = DeltaIndex::build(&store, &graph, &[]).unwrap();
            let summary = DeltaSummary::default();
            let cx = RuleContext::new(
                ChangeSet {
                    store: &store,
                    graph: &graph,
                    delta: &[],
                },
                &index,
                &summary,
            );
            assert_eq!(cx.profile().unwrap(), None);
        }

        #[test]
        fn profile_is_absent() {
            let directory = tempfile::tempdir().unwrap();
            let store = GraphStore::open(directory.path()).unwrap();
            let graph = GraphId::new("urn:test:rules-profile-absent");
            let index = DeltaIndex::build(&store, &graph, &[]).unwrap();
            let summary = DeltaSummary::default();
            let cx = RuleContext::new(
                ChangeSet {
                    store: &store,
                    graph: &graph,
                    delta: &[],
                },
                &index,
                &summary,
            );
            assert_eq!(cx.profile().unwrap(), None);
        }

        fn palette(graph: &GraphId) -> Vec<Triple> {
            let root = graph_root(graph);
            let child = EncodedTerm("<urn:test:differential:child>".to_string());
            let extra = EncodedTerm("<urn:test:differential:extra>".to_string());
            let untyped = EncodedTerm("<urn:test:differential:untyped>".to_string());
            let mut triples = valid_base(graph);
            triples.extend([
                (root.clone(), SCHEMA_NAME.clone(), literal("other-name")),
                (
                    root.clone(),
                    SCHEMA_DATE_PUBLISHED.clone(),
                    literal("2026-02-02"),
                ),
                (extra.clone(), RDF_TYPE.clone(), SCHEMA_MEDIA_OBJECT.clone()),
                (root.clone(), SCHEMA_HAS_PART.clone(), extra),
                (untyped, SCHEMA_NAME.clone(), literal("untyped")),
                (child, SCHEMA_NAME.clone(), literal("child")),
            ]);
            triples
        }

        fn materialize(graph: &GraphId, triple: Triple, inserted: bool) -> MaterializedQuadChange {
            let (subject, predicate, object) = triple;
            if inserted {
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
        }

        fn final_snapshot(
            graph: &GraphId,
            base: &[Triple],
            changes: &[MaterializedQuadChange],
        ) -> GraphSnapshot {
            let mut triples: BTreeSet<Triple> = base.iter().cloned().collect();
            for change in changes {
                match change {
                    MaterializedQuadChange::Insert {
                        subject,
                        predicate,
                        object,
                        ..
                    } => {
                        triples.insert((subject.clone(), predicate.clone(), object.clone()));
                    }
                    MaterializedQuadChange::Delete {
                        subject,
                        predicate,
                        object,
                        ..
                    } => {
                        triples.remove(&(subject.clone(), predicate.clone(), object.clone()));
                    }
                }
            }
            GraphSnapshot::new(graph.clone(), triples.into_iter().collect())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(32))]

            /// The optimized candidate path must agree with a full, deterministic
            /// post-state evaluation over duplicate, no-op, and reordered writes.
            #[test]
            fn candidate_rules_match_full_post_state(
                operations in proptest::collection::vec((0usize..14, any::<bool>()), 0..24),
            ) {
                let directory = tempfile::tempdir().unwrap();
                let store = GraphStore::open(directory.path()).unwrap();
                let graph = GraphId::new("urn:test:delta-rule-differential");
                let base = valid_base(&graph);
                for triple in &base {
                    put(&store, &graph, triple);
                }
                let palette = palette(&graph);
                let changes: Vec<_> = operations
                    .into_iter()
                    .map(|(slot, inserted)| materialize(&graph, palette[slot % palette.len()].clone(), inserted))
                    .collect();
                let rules = default_rules();

                let actual = match validate_change_set(
                    &rules,
                    ChangeSet {
                        store: &store,
                        graph: &graph,
                        delta: &changes,
                    },
                ) {
                    Ok(()) => Vec::new(),
                    Err(RuleEvaluationError::Violations(violations)) => violations,
                    Err(RuleEvaluationError::Store(error)) => panic!("unexpected store error: {error}"),
                };
                let snapshot = final_snapshot(&graph, &base, &changes);
                let expected: Vec<_> = rules
                    .iter()
                    .filter_map(|rule| rule.check_post_state(&snapshot).err())
                    .collect();
                prop_assert_eq!(actual, expected);
            }
        }

        struct ExplicitSnapshotRule {
            marker: Triple,
        }

        impl Rule for ExplicitSnapshotRule {
            fn check_candidate(&self, _: &RuleContext<'_>) -> crate::store::Result<CandidateCheck> {
                Ok(CandidateCheck::NeedSnapshot)
            }

            fn check_post_state(
                &self,
                post: &GraphSnapshot,
            ) -> std::result::Result<(), CrateViolation> {
                let (subject, predicate, object) = &self.marker;
                if post.has_triple(EncodedTripleRef {
                    subject,
                    predicate,
                    object,
                }) {
                    Ok(())
                } else {
                    Err(CrateViolation::missing_root(""))
                }
            }
        }

        #[test]
        fn snapshot_fallback_applies_delta_but_normal_candidates_do_not_materialize() {
            let directory = tempfile::tempdir().unwrap();
            let store = GraphStore::open(directory.path()).unwrap();
            let graph = GraphId::new("urn:test:snapshot-fallback");
            let base = valid_base(&graph);
            for triple in &base {
                put(&store, &graph, triple);
            }

            let root = graph_root(&graph);
            let ordinary = MaterializedQuadChange::Insert {
                graph: graph.clone(),
                subject: root.clone(),
                predicate: SCHEMA_NAME.clone(),
                object: literal("another-name"),
            };
            SNAPSHOT_READS.with(|reads| reads.set(0));
            assert!(
                validate_change_set(
                    &default_rules(),
                    ChangeSet {
                        store: &store,
                        graph: &graph,
                        delta: &[ordinary],
                    },
                )
                .is_ok()
            );
            assert_eq!(0, SNAPSHOT_READS.with(|reads| reads.get()));

            let marker = (
                root,
                EncodedTerm("<urn:test:snapshot:predicate>".to_string()),
                literal("marker"),
            );
            let delta = vec![materialize(&graph, marker.clone(), true)];
            let rules: Vec<Box<dyn Rule>> = vec![Box::new(ExplicitSnapshotRule {
                marker: marker.clone(),
            })];
            SNAPSHOT_READS.with(|reads| reads.set(0));
            assert!(
                validate_change_set(
                    &rules,
                    ChangeSet {
                        store: &store,
                        graph: &graph,
                        delta: &delta,
                    },
                )
                .is_ok()
            );
            assert_eq!(1, SNAPSHOT_READS.with(|reads| reads.get()));
        }
    }
}
