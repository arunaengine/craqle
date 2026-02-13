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
