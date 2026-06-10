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

/// Unique, stable identifier for a replica actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActorId(pub [u8; 32]);

impl ActorId {
    pub fn random() -> Self {
        Self(*blake3::hash(Uuid::new_v4().as_bytes()).as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
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
pub struct VectorClock(pub BTreeMap<ActorId, u64>);

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that we have seen up to `counter` from `actor`.
    pub fn advance(&mut self, actor: ActorId, counter: u64) {
        let entry = self.0.entry(actor).or_insert(0);
        *entry = (*entry).max(counter);
    }

    /// Returns true if this vector clock has seen the given dot.
    pub fn contains(&self, dot: &Dot) -> bool {
        self.0
            .get(&dot.actor)
            .is_some_and(|&seen| seen >= dot.counter)
    }

    /// Merge another vector clock into this one (element-wise max).
    pub fn merge(&mut self, other: &VectorClock) {
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
    let bytes = s.as_bytes();
    if bytes.first().copied()? != b'"' {
        return None;
    }

    let mut value = String::new();
    let mut index = 1usize;
    let mut closed = false;

    while index < s.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                closed = true;
                break;
            }
            b'\\' => {
                index += 1;
                let escaped = *bytes.get(index)?;
                match escaped {
                    b'"' => value.push('"'),
                    b'\\' => value.push('\\'),
                    b'n' => value.push('\n'),
                    b'r' => value.push('\r'),
                    b't' => value.push('\t'),
                    b'b' => value.push('\u{0008}'),
                    b'f' => value.push('\u{000C}'),
                    b'u' => {
                        let code_point = parse_hex_escape(&s[index + 1..], 4)?;
                        value.push(char::from_u32(code_point)?);
                        index += 4;
                    }
                    b'U' => {
                        let code_point = parse_hex_escape(&s[index + 1..], 8)?;
                        value.push(char::from_u32(code_point)?);
                        index += 8;
                    }
                    _ => return None,
                }
                index += 1;
            }
            _ => {
                let ch = s[index..].chars().next()?;
                value.push(ch);
                index += ch.len_utf8();
            }
        }
    }

    if !closed {
        return None;
    }

    let rest = &s[index..];

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

fn parse_hex_escape(value: &str, width: usize) -> Option<u32> {
    if value.len() < width {
        return None;
    }
    u32::from_str_radix(&value[..width], 16).ok()
}

/// CRDT quad operation: either an add (with dot) or a remove (with witnessed vector clock).
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
        witnessed: VectorClock,
    },
}

// ── Replication Batch ───────────────────────────────────────────────────────

/// The unit of replication: a committed set of operations on a single graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub graph: GraphId,
    pub actor: ActorId,
    pub counter: u64,
    pub base_clock: VectorClock,
    pub ops: Vec<QuadOp>,
    pub timestamp: DateTime<Utc>,
}

/// Read-only state of a live quad and its OR-Set dot set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotQuadState {
    pub subject: EncodedTerm,
    pub predicate: EncodedTerm,
    pub object: EncodedTerm,
    pub dots: Vec<Dot>,
}

/// Read-only dump of one graph's quad state for diagnostics and tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphReplicaSnapshot {
    pub graph: GraphId,
    pub clock: VectorClock,
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

    #[error("custom rule `{rule}` failed: {message}")]
    Custom { rule: String, message: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDiagnostics {
    pub orphaned_entities: Vec<String>,
}

impl GraphDiagnostics {
    pub fn from_orphaned_entities(mut orphaned_entities: Vec<String>) -> Self {
        orphaned_entities.sort();
        orphaned_entities.dedup();
        Self { orphaned_entities }
    }

    pub fn has_orphans(&self) -> bool {
        !self.orphaned_entities.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphPolicy {
    pub public: bool,
    pub permission_paths: Vec<String>,
}

impl GraphPolicy {
    pub fn normalized(mut self) -> Self {
        self.permission_paths.sort();
        self.permission_paths.dedup();
        self
    }
}

// ── Materialized Changes (SPARQL evaluator output) ──────────────────────────

/// A concrete quad change produced by SPARQL Update evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn schema_identifier() -> NamedNode {
        NamedNode::new_unchecked("http://schema.org/identifier")
    }

    pub fn metadata_descriptor() -> NamedNode {
        NamedNode::new_unchecked("ro-crate-metadata.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, Term};

    #[test]
    fn encoded_term_round_trips_escaped_literals() {
        let literal = Literal::new_typed_literal(
            "Quote: \" slash: \\\\ newline:\n snowman:\u{2603}",
            NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
        );
        let encoded = EncodedTerm::from_term(&Term::Literal(literal.clone()));
        let decoded = encoded.to_term().unwrap();

        assert_eq!(decoded, Term::Literal(literal));
    }

    #[test]
    fn encoded_term_round_trips_language_literals() {
        let literal = Literal::new_language_tagged_literal_unchecked("bonjour", "fr-ca");
        let encoded = EncodedTerm::from_term(&Term::Literal(literal.clone()));
        let decoded = encoded.to_term().unwrap();

        assert_eq!(decoded, Term::Literal(literal));
    }
}
