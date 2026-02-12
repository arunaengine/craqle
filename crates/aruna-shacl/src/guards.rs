use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::{CrateViolation, EncodedTerm, GraphId, MaterializedQuadChange, vocab};
use aruna_rdf_store::GraphStore;

// ---------------------------------------------------------------------------
// GraphSnapshot
// ---------------------------------------------------------------------------

/// A snapshot view of a graph for validation.
pub struct GraphSnapshot {
    /// All quads in the graph, as (subject, predicate, object) encoded terms.
    pub triples: Vec<(EncodedTerm, EncodedTerm, EncodedTerm)>,
}

impl GraphSnapshot {
    /// Build a snapshot by reading every quad for a graph from the store.
    pub fn from_store(store: &GraphStore, graph: &GraphId) -> aruna_rdf_store::Result<Self> {
        let graph_term = EncodedTerm::from_named_node(&graph.0);
        let graph_tid = match store.lookup_term(&graph_term)? {
            Some(id) => id,
            None => {
                return Ok(Self {
                    triples: Vec::new(),
                });
            }
        };
        let quads = store.quads_for_pattern(Some(graph_tid), None, None, None)?;
        let mut triples = Vec::with_capacity(quads.len());
        for q in quads {
            let s = store.decode_term(q.subject)?;
            let p = store.decode_term(q.predicate)?;
            let o = store.decode_term(q.object)?;
            triples.push((s, p, o));
        }
        Ok(Self { triples })
    }
}

// ---------------------------------------------------------------------------
// Guard trait
// ---------------------------------------------------------------------------

pub trait Guard: Send + Sync {
    /// Validate the post-state snapshot. Return `Err` if a violation is found.
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 1. RootDataEntityGuard
// ---------------------------------------------------------------------------

pub struct RootDataEntityGuard;

impl Guard for RootDataEntityGuard {
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

// ---------------------------------------------------------------------------
// 2. MetadataDescriptorGuard
// ---------------------------------------------------------------------------

pub struct MetadataDescriptorGuard;

impl Guard for MetadataDescriptorGuard {
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

// ---------------------------------------------------------------------------
// 3. RequiredRootPropertiesGuard
// ---------------------------------------------------------------------------

pub struct RequiredRootPropertiesGuard;

impl Guard for RequiredRootPropertiesGuard {
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

// ---------------------------------------------------------------------------
// 4. DatePublishedCardinalityGuard
// ---------------------------------------------------------------------------

pub struct DatePublishedCardinalityGuard;

impl Guard for DatePublishedCardinalityGuard {
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

// ---------------------------------------------------------------------------
// 5. EntityTypeGuard
// ---------------------------------------------------------------------------

pub struct EntityTypeGuard;

impl Guard for EntityTypeGuard {
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let rdf_type = encoded_nn(&vocab::rdf_type());

        // Collect all subjects that appear in the post-state.
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

// ---------------------------------------------------------------------------
// 6. HasPartReachabilityGuard
// ---------------------------------------------------------------------------

pub struct HasPartReachabilityGuard;

impl Guard for HasPartReachabilityGuard {
    fn check_post_state(&self, post: &GraphSnapshot) -> std::result::Result<(), CrateViolation> {
        let root = encoded_nn(&vocab::root_entity());
        let rdf_type = encoded_nn(&vocab::rdf_type());
        let has_part = encoded_nn(&vocab::schema_has_part());
        let media_object = encoded_nn(&vocab::schema_media_object());
        let dataset = encoded_nn(&vocab::schema_dataset());

        // Identify data entities: typed schema:MediaObject or schema:Dataset, excluding root.
        let data_entities: HashSet<&EncodedTerm> = post
            .triples
            .iter()
            .filter(|(s, p, o)| {
                p == &rdf_type && (o == &media_object || o == &dataset) && s != &root
            })
            .map(|(s, _, _)| s)
            .collect();

        if data_entities.is_empty() {
            return Ok(());
        }

        // Build adjacency list for schema:hasPart edges.
        let mut adjacency: HashMap<&EncodedTerm, Vec<&EncodedTerm>> = HashMap::new();
        for (s, p, o) in &post.triples {
            if p == &has_part {
                adjacency.entry(s).or_default().push(o);
            }
        }

        // BFS from root via the adjacency list.
        let mut reachable: HashSet<&EncodedTerm> = HashSet::new();
        let mut queue: VecDeque<&EncodedTerm> = VecDeque::new();
        queue.push_back(&root);
        reachable.insert(&root);

        while let Some(current) = queue.pop_front() {
            if let Some(neighbors) = adjacency.get(current) {
                for o in neighbors {
                    if reachable.insert(o) {
                        queue.push_back(o);
                    }
                }
            }
        }

        // Every data entity must be reachable.
        for entity in &data_entities {
            if !reachable.contains(*entity) {
                return Err(CrateViolation::OrphanedDataEntity {
                    entity_id: entity.0.clone(),
                });
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

/// Create the default set of RO-Crate 1.2 guards.
pub fn default_guards() -> Vec<Box<dyn Guard>> {
    vec![
        Box::new(RootDataEntityGuard),
        Box::new(MetadataDescriptorGuard),
        Box::new(RequiredRootPropertiesGuard),
        Box::new(DatePublishedCardinalityGuard),
        Box::new(EntityTypeGuard),
        Box::new(HasPartReachabilityGuard),
    ]
}

/// Run all guards against the post-state, collecting violations.
pub fn pre_execution_validate(
    guards: &[Box<dyn Guard>],
    snapshot: &GraphSnapshot,
    delta: &[MaterializedQuadChange],
) -> std::result::Result<(), Vec<CrateViolation>> {
    // Compute post-state once for all guards.
    let post = apply_delta(snapshot, delta);

    let violations: Vec<CrateViolation> = guards
        .iter()
        .filter_map(|g| g.check_post_state(&post).err())
        .collect();

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// Post-merge check (non-blocking, just reports violations).
pub fn post_merge_check(snapshot: &GraphSnapshot) -> Vec<CrateViolation> {
    let guards = default_guards();
    guards
        .iter()
        .filter_map(|g| g.check_post_state(snapshot).err())
        .collect()
}
