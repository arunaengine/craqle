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

