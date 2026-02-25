use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::core::{CrateViolation, EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use crate::store::GraphStore;

/// A snapshot view of a graph for validation.
pub struct GraphSnapshot {
    /// All quads in the graph, as (subject, predicate, object) encoded terms.
    pub triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
}

impl GraphSnapshot {
    /// Build a snapshot by reading every quad for a graph from the store.
    pub fn from_store(store: &GraphStore, graph: &GraphId) -> crate::store::Result<Self> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let graph_tid = match store.lookup_term(&graph_term)? {
            Some(id) => id,
            None => {
                return Ok(Self {
                    triples: Vec::new(),
                });
            }
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
        Ok(Self { triples })
    }
}

pub trait Rule: Send + Sync {
    fn check_candidate(
        &self,
        _store: &GraphStore,
        _graph: &GraphId,
        _delta: &[MaterializedQuadChange],
    ) -> crate::store::Result<CandidateCheck> {
        Ok(CandidateCheck::NeedSnapshot)
    }

    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        _summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        self.check_candidate(store, graph, delta)
    }

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

#[derive(Debug, Default, Clone)]
pub struct DeltaSummary {
    impacted_subjects: BTreeSet<EncodedTerm>,
    inserted_subjects: BTreeSet<EncodedTerm>,
    type_changed_subjects: BTreeSet<EncodedTerm>,
    reachability_seeds: BTreeSet<EncodedTerm>,
    touches_root_dataset: bool,
    touches_metadata_descriptor: bool,
    touches_root_name: bool,
    touches_root_description: bool,
    touches_root_date_published: bool,
    touches_root_license: bool,
    touches_reachability: bool,
}

#[derive(Debug, Clone)]
struct ReachabilityTerms {
    root: EncodedTerm,
    rdf_type: EncodedTerm,
    has_part: EncodedTerm,
    dataset: EncodedTerm,
    media_object: EncodedTerm,
}

impl ReachabilityTerms {
    fn new() -> Self {
        Self {
            root: encoded_nn(&vocab::root_entity()),
            rdf_type: encoded_nn(&vocab::rdf_type()),
            has_part: encoded_nn(&vocab::schema_has_part()),
            dataset: encoded_nn(&vocab::schema_dataset()),
            media_object: encoded_nn(&vocab::schema_media_object()),
        }
    }
}

#[derive(Default)]
struct ReachabilityCaches {
    children: HashMap<EncodedTerm, BTreeSet<EncodedTerm>>,
    parents: HashMap<EncodedTerm, BTreeSet<EncodedTerm>>,
    reachability: HashMap<EncodedTerm, bool>,
    visiting: HashSet<EncodedTerm>,
}

impl DeltaSummary {
    fn touches_required_root_properties(&self) -> bool {
        self.touches_root_name
            || self.touches_root_description
            || self.touches_root_date_published
            || self.touches_root_license
    }

    fn touched_required_root_properties(&self) -> [bool; 4] {
        [
            self.touches_root_name,
            self.touches_root_description,
            self.touches_root_date_published,
            self.touches_root_license,
        ]
    }
}

fn encoded_nn(nn: &oxrdf::NamedNode) -> EncodedTerm {
    EncodedTerm::from_named_node(nn)
}

/// Apply delta to a snapshot to produce the post-state.
pub fn apply_delta(snapshot: &GraphSnapshot, delta: &[MaterializedQuadChange]) -> GraphSnapshot {
    let mut triple_set: HashSet<(EncodedTerm, EncodedTerm, EncodedTerm)> =
        snapshot.triples.iter().cloned().collect();

    for change in delta {
        match change {
            MaterializedQuadChange::Insert {
                subject,
                predicate,
                object,
                ..
            } => {
                triple_set.insert((subject.clone(), predicate.clone(), object.clone()));
            }
            MaterializedQuadChange::Delete {
                subject,
                predicate,
                object,
                ..
            } => {
                triple_set.remove(&(subject.clone(), predicate.clone(), object.clone()));
            }
        }
    }

    GraphSnapshot {
        triples: triple_set.into_iter().collect(),
    }
}

/// Count triples matching (subject, predicate, *) in a snapshot.
fn count_sp(snapshot: &GraphSnapshot, subject: &EncodedTerm, predicate: &EncodedTerm) -> usize {
    snapshot
        .triples
        .iter()
        .filter(|(s, p, _)| s == subject && p == predicate)
        .count()
}

/// Check whether (subject, predicate, object) exists in a snapshot.
fn has_triple(
    snapshot: &GraphSnapshot,
    subject: &EncodedTerm,
    predicate: &EncodedTerm,
    object: &EncodedTerm,
) -> bool {
    snapshot
        .triples
        .iter()
        .any(|(s, p, o)| s == subject && p == predicate && o == object)
}

pub fn orphaned_data_entities(snapshot: &GraphSnapshot) -> BTreeSet<EncodedTerm> {
    let root = encoded_nn(&vocab::root_entity());
    let rdf_type = encoded_nn(&vocab::rdf_type());
    let has_part = encoded_nn(&vocab::schema_has_part());
    let media_object = encoded_nn(&vocab::schema_media_object());
    let dataset = encoded_nn(&vocab::schema_dataset());

    let mut data_entities: BTreeSet<EncodedTerm> = snapshot
        .triples
        .iter()
        .filter(|(s, p, o)| p == &rdf_type && (o == &media_object || o == &dataset) && s != &root)
        .map(|(s, _, _)| s.clone())
        .collect();
    let mut adjacency: HashMap<EncodedTerm, Vec<EncodedTerm>> = HashMap::new();
    for (s, p, o) in &snapshot.triples {
        if p == &has_part {
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

pub struct RootEntityRule;

impl Rule for RootEntityRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if !delta.is_empty() && !summary.touches_root_dataset {
            return Ok(CandidateCheck::Pass);
        }
        self.check_candidate(store, graph, delta)
    }

    fn check_candidate(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
    ) -> crate::store::Result<CandidateCheck> {
        let root = encoded_nn(&vocab::root_entity());
        let rdf_type = encoded_nn(&vocab::rdf_type());
        let dataset = encoded_nn(&vocab::schema_dataset());

        Ok(
            if triple_exists_after(store, graph, &root, &rdf_type, &dataset, delta)? {
                CandidateCheck::Pass
            } else {
                CandidateCheck::Violation(CrateViolation::MissingRootDataEntity)
            },
        )
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
