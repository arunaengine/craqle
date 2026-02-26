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
        let root = encoded_nn(&vocab::root_entity());
        let rdf_type = encoded_nn(&vocab::rdf_type());
        let dataset = encoded_nn(&vocab::schema_dataset());

        if !has_triple(post, &root, &rdf_type, &dataset) {
            return Err(CrateViolation::MissingRootDataEntity);
        }
        Ok(())
    }
}

pub struct MetadataDescriptorRule;

impl Rule for MetadataDescriptorRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if !delta.is_empty() && !summary.touches_metadata_descriptor {
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
        let descriptor = encoded_nn(&vocab::metadata_descriptor());
        let rdf_type = encoded_nn(&vocab::rdf_type());
        let creative_work = encoded_nn(&vocab::schema_creative_work());
        let about = encoded_nn(&vocab::schema_about());
        let root = encoded_nn(&vocab::root_entity());

        let has_type =
            triple_exists_after(store, graph, &descriptor, &rdf_type, &creative_work, delta)?;
        let has_about = triple_exists_after(store, graph, &descriptor, &about, &root, delta)?;

        Ok(if has_type && has_about {
            CandidateCheck::Pass
        } else {
            CandidateCheck::Violation(CrateViolation::MissingMetadataDescriptor)
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let descriptor = encoded_nn(&vocab::metadata_descriptor());
        let rdf_type = encoded_nn(&vocab::rdf_type());
        let creative_work = encoded_nn(&vocab::schema_creative_work());
        let about = encoded_nn(&vocab::schema_about());
        let root = encoded_nn(&vocab::root_entity());

        let has_type = has_triple(post, &descriptor, &rdf_type, &creative_work);
        let has_about = has_triple(post, &descriptor, &about, &root);

        if !has_type || !has_about {
            return Err(CrateViolation::MissingMetadataDescriptor);
        }
        Ok(())
    }
}

pub struct RequiredRootPropertiesRule;

impl Rule for RequiredRootPropertiesRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if !delta.is_empty() && !summary.touches_required_root_properties() {
            return Ok(CandidateCheck::Pass);
        }

        let root = encoded_nn(&vocab::root_entity());
        let required: &[(oxrdf::NamedNode, &str)] = &[
            (vocab::schema_name(), "schema:name"),
            (vocab::schema_description(), "schema:description"),
            (vocab::schema_date_published(), "schema:datePublished"),
            (vocab::schema_license(), "schema:license"),
        ];

        for ((nn, label), touched) in required
            .iter()
            .zip(summary.touched_required_root_properties())
        {
            if !delta.is_empty() && !touched {
                continue;
            }
            let pred = encoded_nn(nn);
            if count_sp_after(store, graph, &root, &pred, delta)? < 1 {
                return Ok(CandidateCheck::Violation(
                    CrateViolation::MissingRequiredProperty {
                        entity: "./".to_string(),
                        property: label.to_string(),
                    },
                ));
            }
        }

        Ok(CandidateCheck::Pass)
    }

    fn check_candidate(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
    ) -> crate::store::Result<CandidateCheck> {
        let root = encoded_nn(&vocab::root_entity());
        let required: &[(oxrdf::NamedNode, &str)] = &[
            (vocab::schema_name(), "schema:name"),
            (vocab::schema_description(), "schema:description"),
            (vocab::schema_date_published(), "schema:datePublished"),
            (vocab::schema_license(), "schema:license"),
        ];

        for (nn, label) in required {
            let pred = encoded_nn(nn);
            if count_sp_after(store, graph, &root, &pred, delta)? < 1 {
                return Ok(CandidateCheck::Violation(
                    CrateViolation::MissingRequiredProperty {
                        entity: "./".to_string(),
                        property: label.to_string(),
                    },
                ));
            }
        }

        Ok(CandidateCheck::Pass)
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = encoded_nn(&vocab::root_entity());

        let required: &[(oxrdf::NamedNode, &str)] = &[
            (vocab::schema_name(), "schema:name"),
            (vocab::schema_description(), "schema:description"),
            (vocab::schema_date_published(), "schema:datePublished"),
            (vocab::schema_license(), "schema:license"),
        ];

        for (nn, label) in required {
            let pred = encoded_nn(nn);
            if count_sp(post, &root, &pred) < 1 {
                return Err(CrateViolation::MissingRequiredProperty {
                    entity: "./".to_string(),
                    property: label.to_string(),
                });
            }
        }
        Ok(())
    }
}

pub struct DatePublishedCardinalityRule;

impl Rule for DatePublishedCardinalityRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if !delta.is_empty() && !summary.touches_root_date_published {
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
        let date_pub = encoded_nn(&vocab::schema_date_published());
        let count = count_sp_after(store, graph, &root, &date_pub, delta)?;

        Ok(if count == 1 {
            CandidateCheck::Pass
        } else {
            CandidateCheck::Violation(CrateViolation::InvalidDatePublishedCardinality { count })
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = encoded_nn(&vocab::root_entity());
        let date_pub = encoded_nn(&vocab::schema_date_published());
        let count = count_sp(post, &root, &date_pub);

        if count != 1 {
            return Err(CrateViolation::InvalidDatePublishedCardinality { count });
        }
        Ok(())
    }
}

pub struct EntityTypeRule;

impl Rule for EntityTypeRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if delta.is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }

        let rdf_type = encoded_nn(&vocab::rdf_type());
        let mut candidates = summary.type_changed_subjects.clone();
        for subject in &summary.inserted_subjects {
            if current_subject_triple_count(store, graph, subject)? == 0 {
                candidates.insert(subject.clone());
            }
        }

        if candidates.is_empty() {
            return Ok(CandidateCheck::Pass);
        }

        for subject in candidates {
            let triple_count = subject_triple_count_after(store, graph, &subject, delta)?;
            if triple_count == 0 {
                continue;
            }
            if count_sp_after(store, graph, &subject, &rdf_type, delta)? == 0 {
                return Ok(CandidateCheck::Violation(
                    CrateViolation::EntityMissingType {
                        entity_id: subject.0,
                    },
                ));
            }
        }

        Ok(CandidateCheck::Pass)
    }

    fn check_candidate(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
    ) -> crate::store::Result<CandidateCheck> {
        if delta.is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }

        if let Some(subject) = first_subject_missing_type_after(store, graph, delta)? {
            return Ok(CandidateCheck::Violation(
                CrateViolation::EntityMissingType {
                    entity_id: subject.0,
                },
            ));
        }

        Ok(CandidateCheck::Pass)
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let rdf_type = encoded_nn(&vocab::rdf_type());

        let subjects: HashSet<&EncodedTerm> = post.triples.iter().map(|(s, _, _)| s).collect();

        for subject in &subjects {
            let has_type = post
                .triples
                .iter()
                .any(|(s, p, _)| s == *subject && p == &rdf_type);
            if !has_type {
                let id = subject.0.clone();
                return Err(CrateViolation::EntityMissingType { entity_id: id });
            }
        }
        Ok(())
    }
}

pub struct ReachabilityRule;

impl Rule for ReachabilityRule {
    fn check_candidate_with_summary(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
        summary: &DeltaSummary,
    ) -> crate::store::Result<CandidateCheck> {
        if delta.is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }
        if !summary.touches_reachability {
            return Ok(CandidateCheck::Pass);
        }

        Ok(
            match first_orphan_after_localized(store, graph, delta, summary)? {
                Some(entity) => CandidateCheck::Violation(CrateViolation::OrphanedDataEntity {
                    entity_id: entity.0,
                }),
                None => CandidateCheck::Pass,
            },
        )
    }

    fn check_candidate(
        &self,
        store: &GraphStore,
        graph: &GraphId,
        delta: &[MaterializedQuadChange],
    ) -> crate::store::Result<CandidateCheck> {
        if delta.is_empty() {
            return Ok(CandidateCheck::NeedSnapshot);
        }

        Ok(match first_orphan_after(store, graph, delta)? {
            Some(entity) => CandidateCheck::Violation(CrateViolation::OrphanedDataEntity {
                entity_id: entity.0,
            }),
            None => CandidateCheck::Pass,
        })
    }

    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        if let Some(entity) = orphaned_data_entities(post).into_iter().next() {
            return Err(CrateViolation::OrphanedDataEntity {
                entity_id: entity.0,
            });
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

/// Run all rules against the post-state, collecting violations.
pub fn validate_change_set(
    rules: &[Box<dyn Rule>],
    store: &GraphStore,
    graph: &GraphId,
    delta: &[MaterializedQuadChange],
) -> std::result::Result<(), RuleEvaluationError> {
    let mut violations = Vec::new();
    let mut post = None;
    let summary = summarize_delta(graph, delta);

    for rule in rules {
        match rule.check_candidate_with_summary(store, graph, delta, &summary)? {
            CandidateCheck::Pass => {}
            CandidateCheck::Violation(violation) => violations.push(violation),
            CandidateCheck::NeedSnapshot => {
                if post.is_none() {
                    let snapshot = GraphSnapshot::from_store(store, graph)?;
                    post = Some(apply_delta(&snapshot, delta));
                }
                let post = post.as_ref().unwrap();
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
    let mut violations = Vec::new();
    let mut snapshot = None;

    for rule in &rules {
        match rule.check_candidate(store, graph, &[])? {
            CandidateCheck::Pass => {}
            CandidateCheck::Violation(violation) => violations.push(violation),
            CandidateCheck::NeedSnapshot => {
                if snapshot.is_none() {
                    snapshot = Some(GraphSnapshot::from_store(store, graph)?);
                }
                if let Err(violation) = rule.check_post_state(snapshot.as_ref().unwrap()) {
                    violations.push(violation);
                }
            }
        }
    }

    Ok(violations)
}

fn first_subject_missing_type_after(
    store: &GraphStore,
    graph: &GraphId,
    delta: &[MaterializedQuadChange],
) -> crate::store::Result<Option<EncodedTerm>> {
    let rdf_type = encoded_nn(&vocab::rdf_type());
    for subject in impacted_subjects(delta, graph) {
        let triple_count = subject_triple_count_after(store, graph, &subject, delta)?;
        if triple_count == 0 {
            continue;
        }
        if count_sp_after(store, graph, &subject, &rdf_type, delta)? == 0 {
            return Ok(Some(subject));
        }
    }

    Ok(None)
}

fn first_orphan_after(
    store: &GraphStore,
    graph: &GraphId,
    delta: &[MaterializedQuadChange],
) -> crate::store::Result<Option<EncodedTerm>> {
    let terms = ReachabilityTerms::new();

    let reachable = reachable_entities_after(store, graph, delta, &terms.root, &terms.has_part)?;
    for entity in impacted_reachability_entities(delta, graph, &terms.rdf_type, &terms.has_part)? {
        if entity == terms.root {
            continue;
        }
        if is_data_entity_after(store, graph, delta, &entity, &terms)?
            && !reachable.contains(&entity)
        {
            return Ok(Some(entity));
        }
    }

    Ok(None)
}

fn first_orphan_after_localized(
    store: &GraphStore,
    graph: &GraphId,
    delta: &[MaterializedQuadChange],
    summary: &DeltaSummary,
) -> crate::store::Result<Option<EncodedTerm>> {
    let terms = ReachabilityTerms::new();

    let mut candidate_entities = BTreeSet::new();
    let mut caches = ReachabilityCaches::default();
    for seed in &summary.reachability_seeds {
        collect_descendants_after_cached(
            store,
            graph,
            delta,
            seed,
            &terms.has_part,
            &mut caches.children,
            &mut candidate_entities,
        )?;
    }

    for entity in candidate_entities {
        if entity == terms.root {
            continue;
        }

        if is_data_entity_after_cached(store, graph, delta, &entity, &terms, &mut caches)?
            && !is_reachable_after_cached(store, graph, delta, &entity, &terms, &mut caches)?
        {
            return Ok(Some(entity));
        }
    }

    Ok(None)
}

fn summarize_delta(graph: &GraphId, delta: &[MaterializedQuadChange]) -> DeltaSummary {
    let root = encoded_nn(&vocab::root_entity());
    let metadata = encoded_nn(&vocab::metadata_descriptor());
    let rdf_type = encoded_nn(&vocab::rdf_type());
    let dataset = encoded_nn(&vocab::schema_dataset());
    let media_object = encoded_nn(&vocab::schema_media_object());
    let creative_work = encoded_nn(&vocab::schema_creative_work());
    let about = encoded_nn(&vocab::schema_about());
    let has_part = encoded_nn(&vocab::schema_has_part());
    let root_name = encoded_nn(&vocab::schema_name());
    let root_description = encoded_nn(&vocab::schema_description());
    let root_date_published = encoded_nn(&vocab::schema_date_published());
    let root_license = encoded_nn(&vocab::schema_license());

    let mut summary = DeltaSummary::default();

    for change in delta {
        let (change_graph, subject, predicate, object) = match change {
            MaterializedQuadChange::Insert {
                graph,
                subject,
                predicate,
                object,
            }
            | MaterializedQuadChange::Delete {
                graph,
                subject,
                predicate,
                object,
            } => (graph, subject, predicate, object),
        };

        if change_graph != graph {
            continue;
        }

        summary.impacted_subjects.insert(subject.clone());
        if matches!(change, MaterializedQuadChange::Insert { .. }) {
            summary.inserted_subjects.insert(subject.clone());
        }

        if subject == &root {
            if predicate == &root_name {
                summary.touches_root_name = true;
            }
            if predicate == &root_description {
                summary.touches_root_description = true;
            }
            if predicate == &root_date_published {
                summary.touches_root_date_published = true;
            }
            if predicate == &root_license {
                summary.touches_root_license = true;
            }
            if predicate == &rdf_type && object == &dataset {
                summary.touches_root_dataset = true;
            }
        }

        if subject == &metadata
            && ((predicate == &rdf_type && object == &creative_work)
                || (predicate == &about && object == &root))
        {
            summary.touches_metadata_descriptor = true;
