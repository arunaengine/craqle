use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use oxrdf::NamedNode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Identity ────────────────────────────────────────────────────────────────

/// A named-graph IRI identifying one RO-Crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GraphId(pub NamedNode);

impl GraphId {
    pub fn new(iri: &str) -> Self {
        Self(NamedNode::new_unchecked(iri))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for GraphId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for GraphId {
    fn from(iri: &str) -> Self {
        Self::new(iri)
    }
}

/// Unique, stable identifier for a simulated peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorId(pub Uuid);

impl ActorId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── Causality ───────────────────────────────────────────────────────────────

/// A single event identifier: (actor, monotonic counter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub actor: ActorId,
    pub counter: u64,
}

/// Vector clock: highest counter seen from each actor for a given graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier(pub BTreeMap<ActorId, u64>);

impl Frontier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that we have seen up to `counter` from `actor`.
    pub fn advance(&mut self, actor: ActorId, counter: u64) {
        let entry = self.0.entry(actor).or_insert(0);
        *entry = (*entry).max(counter);
    }

    /// Returns true if this frontier has seen the given dot.
    pub fn contains(&self, dot: &Dot) -> bool {
        self.0
            .get(&dot.actor)
            .is_some_and(|&seen| seen >= dot.counter)
    }

    /// Merge another frontier into this one (element-wise max).
    pub fn merge(&mut self, other: &Frontier) {
        for (&actor, &counter) in &other.0 {
            self.advance(actor, counter);
        }
    }
}

// ── Quad Operations (CRDT primitives) ───────────────────────────────────────

/// An RDF term serialized as a string for transport. We use oxrdf's
/// Display/FromStr round-trip via N-Triples syntax.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EncodedTerm(pub String);

impl EncodedTerm {
    pub fn from_named_node(n: &NamedNode) -> Self {
        Self(n.to_string())
    }

    pub fn from_term(t: &oxrdf::Term) -> Self {
        Self(t.to_string())
    }

    /// Parse back to an oxrdf Term (N-Triples syntax).
    pub fn to_term(&self) -> Option<oxrdf::Term> {
        // oxrdf terms Display as N-Triples: <iri>, "lit"^^<dt>, _:bn
        if self.0.starts_with('<') && self.0.ends_with('>') {
            let iri = &self.0[1..self.0.len() - 1];
            Some(oxrdf::Term::NamedNode(NamedNode::new_unchecked(iri)))
        } else if self.0.starts_with('"') {
            // Try parsing as literal: "value"^^<type> or "value"@lang or "value"
            parse_ntriples_literal(&self.0).map(oxrdf::Term::Literal)
        } else if self.0.starts_with("_:") {
            Some(oxrdf::Term::BlankNode(oxrdf::BlankNode::new_unchecked(
                &self.0[2..],
            )))
        } else {
            None
        }
    }

    pub fn to_named_node(&self) -> Option<NamedNode> {
        if self.0.starts_with('<') && self.0.ends_with('>') {
            Some(NamedNode::new_unchecked(&self.0[1..self.0.len() - 1]))
        } else {
            None
        }
    }
}

impl From<&NamedNode> for EncodedTerm {
    fn from(n: &NamedNode) -> Self {
        Self::from_named_node(n)
    }
}

impl From<NamedNode> for EncodedTerm {
    fn from(n: NamedNode) -> Self {
        Self::from_named_node(&n)
    }
}

impl From<&oxrdf::NamedOrBlankNode> for EncodedTerm {
    fn from(subject: &oxrdf::NamedOrBlankNode) -> Self {
        match subject {
            oxrdf::NamedOrBlankNode::NamedNode(node) => Self::from_named_node(node),
            oxrdf::NamedOrBlankNode::BlankNode(node) => Self(format!("_:{}", node.as_str())),
        }
    }
}

fn parse_ntriples_literal(s: &str) -> Option<oxrdf::Literal> {
    // Format: "value"^^<datatype> or "value"@lang or "value"
    let s = s.strip_prefix('"')?;
    // Find the closing quote (handle escaped quotes)
    let mut chars = s.char_indices();
    let mut end = 0;
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next(); // skip escaped char
        } else if c == '"' {
            end = i;
            break;
        }
    }
    let value = &s[..end];
    let rest = &s[end + 1..];

    if let Some(lang) = rest.strip_prefix('@') {
        Some(oxrdf::Literal::new_language_tagged_literal_unchecked(
            value, lang,
        ))
    } else if let Some(dt) = rest.strip_prefix("^^<") {
        let dt = dt.strip_suffix('>')?;
        Some(oxrdf::Literal::new_typed_literal(
            value,
            NamedNode::new_unchecked(dt),
        ))
    } else {
        Some(oxrdf::Literal::new_simple_literal(value))
    }
}

/// CRDT quad operation: either an add (with dot) or a remove (with witnessed frontier).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuadOp {
    Add {
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
        dot: Dot,
    },
    Remove {
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
        witnessed: Frontier,
    },
}

// ── Replication Batch ───────────────────────────────────────────────────────

/// The unit of replication: a committed set of operations on a single graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub graph: GraphId,
    pub actor: ActorId,
    pub counter: u64,
    pub base_frontier: Frontier,
    pub ops: Vec<QuadOp>,
    pub timestamp: DateTime<Utc>,
}

/// Snapshot of a live quad and its OR-Set dot set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotQuadState {
    pub subject: EncodedTerm,
    pub predicate: EncodedTerm,
    pub object: EncodedTerm,
    pub dots: Vec<Dot>,
}

/// Full graph snapshot used for bootstrap/catch-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphReplicaSnapshot {
    pub graph: GraphId,
    pub frontier: Frontier,
    pub quads: Vec<SnapshotQuadState>,
}

// ── Violations ──────────────────────────────────────────────────────────────

/// Structural violations detectable by SHACL guards or post-merge checks.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CrateViolation {
    #[error("missing root data entity (<./> rdf:type schema:Dataset)")]
    MissingRootDataEntity,

    #[error("missing metadata descriptor (ro-crate-metadata.json)")]
    MissingMetadataDescriptor,

    #[error("missing required property `{property}` on entity `{entity}`")]
    MissingRequiredProperty { entity: String, property: String },

    #[error("orphaned data entity `{entity_id}` not reachable from root")]
    OrphanedDataEntity { entity_id: String },

    #[error("datePublished must have exactly one value, found {count}")]
    InvalidDatePublishedCardinality { count: usize },

    #[error("entity `{entity_id}` is missing rdf:type")]
    EntityMissingType { entity_id: String },
}

// ── Materialized Changes (SPARQL evaluator output) ──────────────────────────

/// A concrete quad change produced by SPARQL Update evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializedQuadChange {
    Insert {
        graph: GraphId,
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
    },
    Delete {
        graph: GraphId,
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
    },
}

// ── Search Filter ───────────────────────────────────────────────────────────

/// Predicates that trigger Tantivy reindexing.
#[derive(Debug, Clone)]
pub struct PredicateFilter {
    pub predicates: HashSet<NamedNode>,
}

impl Default for PredicateFilter {
    fn default() -> Self {
        let preds = [
            "http://schema.org/name",
            "http://schema.org/description",
            "http://schema.org/keywords",
        ];
        Self {
            predicates: preds.iter().map(|p| NamedNode::new_unchecked(*p)).collect(),
        }
    }
}

// ── Well-known IRIs ─────────────────────────────────────────────────────────

pub mod vocab {
    use oxrdf::NamedNode;

    pub fn rdf_type() -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    }
    pub fn schema_dataset() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/Dataset")
    }
    pub fn schema_creative_work() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/CreativeWork")
    }
    pub fn schema_name() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/name")
    }
    pub fn schema_description() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/description")
    }
    pub fn schema_date_published() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/datePublished")
    }
    pub fn schema_license() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/license")
    }
    pub fn schema_has_part() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/hasPart")
    }
    pub fn schema_about() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/about")
    }
    pub fn schema_conforms_to() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/conformsTo")
    }
    pub fn schema_media_object() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/MediaObject")
    }
    #[deprecated(note = "renamed to schema_media_object")]
    pub fn schema_file() -> NamedNode {
        schema_media_object()
    }
    pub fn schema_keywords() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/keywords")
    }

    pub fn root_entity() -> NamedNode {
        NamedNode::new_unchecked("./")
    }
    pub fn metadata_descriptor() -> NamedNode {
        NamedNode::new_unchecked("ro-crate-metadata.json")
    }
}
